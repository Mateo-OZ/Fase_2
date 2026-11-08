"""Implementación de Raft sobre gRPC con TLS mutuo (Ongaro y Ousterhout, 2014).

Cubre lo que el diseño necesita (secciones 3, 7 y 8):

* Elección de líder con temporizador aleatorio (1 a 2 s) y latidos cada 250 ms.
* Replicación del log; una entrada se confirma cuando la guarda la mayoría
  (2 de 3) y es del término actual.
* Persistencia en disco (estado duro + log en SQLite) y snapshots periódicos
  con InstallSnapshot para seguidores muy atrasados.
* Lecturas atendidas por el líder tras confirmar que sigue siéndolo
  (lease de lectura: mayoría de respuestas recientes) y que aplicó todo lo
  confirmado.

La máquina de estados es determinista: el líder sella la hora en cada comando.
"""
from __future__ import annotations

import json
import logging
import random
import threading
import time
from typing import Callable

import grpc

from dfsha.proto import dfsha_pb2 as pb
from dfsha.proto import dfsha_pb2_grpc as pbg

from .storage import RaftStorage

log = logging.getLogger("raft")

FOLLOWER, CANDIDATE, LEADER = "follower", "candidate", "leader"


class NotLeader(Exception):
    def __init__(self, leader_id: str | None, leader_addr: str | None):
        super().__init__(f"no soy el líder (líder: {leader_id})")
        self.leader_id = leader_id
        self.leader_addr = leader_addr


class ProposalTimeout(Exception):
    pass


class RaftNode:
    def __init__(
        self,
        node_id: str,
        peers: dict[str, str],
        storage: RaftStorage,
        state_machine,
        channel_factory: Callable[[str], grpc.Channel],
        election_timeout: tuple[float, float] = (1.0, 2.0),
        heartbeat_interval: float = 0.25,
        snapshot_threshold: int = 2000,
        on_become_leader: Callable[[], None] | None = None,
        on_lose_leadership: Callable[[], None] | None = None,
    ):
        self.id = node_id
        self.peers_all = dict(peers)  # incluye a este nodo
        self.peers = {k: v for k, v in peers.items() if k != node_id}
        self.storage = storage
        self.sm = state_machine
        self.channel_factory = channel_factory
        self.election_timeout = election_timeout
        self.heartbeat_interval = heartbeat_interval
        self.snapshot_threshold = snapshot_threshold
        self.on_become_leader = on_become_leader
        self.on_lose_leadership = on_lose_leadership

        self.lock = threading.RLock()
        self.cv = threading.Condition(self.lock)
        self.running = True

        # Estado persistente
        self.term, self.voted_for = storage.load_hard_state()
        self.snap_index, self.snap_term, snap_data = storage.load_snapshot()
        if snap_data is not None:
            self.sm.restore(snap_data)
        self.log: list[tuple[int, int, bytes]] = storage.load_log(self.snap_index)

        # Estado volátil
        self.commit_index = self.snap_index
        self.last_applied = self.snap_index
        self.role = FOLLOWER
        self.leader_id: str | None = None
        self.leader_since = 0.0
        self.last_leader_contact = 0.0
        self.leader_start_index = 0
        self.election_deadline = self._new_deadline()

        # Estado del líder
        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}
        self.last_ack: dict[str, float] = {}
        self.triggers: dict[str, threading.Event] = {}

        self.waiters: set[int] = set()
        self.results: dict[int, tuple[int, dict]] = {}
        self._stubs: dict[str, pbg.MetadataStub] = {}

        log.info(
            "nodo %s iniciado: term=%s snapshot=%s entradas_log=%s",
            self.id, self.term, self.snap_index, len(self.log),
        )

    # ------------------------------------------------------------------ util
    def _new_deadline(self) -> float:
        return time.monotonic() + random.uniform(*self.election_timeout)

    def _stub(self, peer: str) -> pbg.MetadataStub:
        stub = self._stubs.get(peer)
        if stub is None:
            stub = pbg.MetadataStub(self.channel_factory(self.peers[peer]))
            self._stubs[peer] = stub
        return stub

    def last_index(self) -> int:
        return self.log[-1][0] if self.log else self.snap_index

    def last_term(self) -> int:
        return self.log[-1][1] if self.log else self.snap_term

    def term_at(self, index: int) -> int:
        if index == self.snap_index:
            return self.snap_term
        if index < self.snap_index or index > self.last_index():
            return -1
        return self.log[index - self.snap_index - 1][1]

    def entry_at(self, index: int) -> tuple[int, int, bytes]:
        return self.log[index - self.snap_index - 1]

    def quorum(self) -> int:
        return len(self.peers_all) // 2 + 1

    def leader_addr(self) -> str | None:
        return self.peers_all.get(self.leader_id) if self.leader_id else None

    def status(self) -> dict:
        with self.lock:
            return {
                "id": self.id,
                "role": self.role,
                "term": self.term,
                "leader": self.leader_id,
                "commit_index": self.commit_index,
                "last_applied": self.last_applied,
                "last_index": self.last_index(),
                "snapshot_index": self.snap_index,
            }

    def is_leader(self) -> bool:
        with self.lock:
            return self.role == LEADER

    # ------------------------------------------------------------- arranque
    def start(self) -> None:
        threading.Thread(target=self._ticker, name="raft-ticker", daemon=True).start()
        threading.Thread(target=self._applier, name="raft-applier", daemon=True).start()

    def stop(self) -> None:
        with self.lock:
            self.running = False
            self.cv.notify_all()

    # ------------------------------------------------- transiciones de rol
    def _persist(self) -> None:
        self.storage.save_hard_state(self.term, self.voted_for)

    def _become_follower(self, term: int, leader: str | None = None) -> None:
        was_leader = self.role == LEADER
        if term > self.term:
            self.term = term
            self.voted_for = None
            self._persist()
        self.role = FOLLOWER
        if leader is not None:
            self.leader_id = leader
        elif was_leader:
            self.leader_id = None
        self.election_deadline = self._new_deadline()
        self.cv.notify_all()
        if was_leader:
            log.warning("dejo de ser líder (term=%s)", self.term)
            if self.on_lose_leadership:
                threading.Thread(target=self.on_lose_leadership, daemon=True).start()

    def _become_leader(self) -> None:
        self.role = LEADER
        self.leader_id = self.id
        self.leader_since = time.monotonic()
        nxt = self.last_index() + 1
        self.next_index = {p: nxt for p in self.peers}
        self.match_index = {p: 0 for p in self.peers}
        self.last_ack = {p: 0.0 for p in self.peers}
        # Entrada no-op del nuevo término: permite confirmar lo heredado.
        self.leader_start_index = self._append_local({"op": "noop", "ts": time.time()})
        log.warning("soy el LÍDER (term=%s, last_index=%s)", self.term, self.last_index())
        term = self.term
        for p in self.peers:
            ev = threading.Event()
            self.triggers[p] = ev
            threading.Thread(target=self._replicator, args=(p, term, ev), daemon=True).start()
        self._advance_commit()
        if self.on_become_leader:
            threading.Thread(target=self.on_become_leader, daemon=True).start()

    # ------------------------------------------------------------ elección
    def _ticker(self) -> None:
        while self.running:
            time.sleep(0.05)
            with self.lock:
                if self.role == LEADER:
                    continue
                if time.monotonic() >= self.election_deadline:
                    self._start_pre_vote()

    def _start_pre_vote(self) -> None:
        """PreVote: antes de subir el término se pregunta si se ganaría la elección.

        Evita que un nodo que estuvo aislado o que se reinicia suba el término y
        derroque a un líder sano al volver.
        """
        self.election_deadline = self._new_deadline()
        self.pre_round = getattr(self, "pre_round", 0) + 1
        rnd, term = self.pre_round, self.term
        req = pb.VoteRequest(term=term + 1, candidate_id=self.id, last_log_index=self.last_index(),
                             last_log_term=self.last_term(), pre_vote=True)
        votes = {self.id}
        if len(votes) >= self.quorum():
            self._start_election()
            return

        def ask(peer: str) -> None:
            try:
                reply = self._stub(peer).RequestVote(req, timeout=0.5)
            except grpc.RpcError:
                return
            with self.lock:
                if reply.term > self.term:
                    self._become_follower(reply.term)
                    return
                if self.role == LEADER or self.pre_round != rnd or self.term != term or not reply.granted:
                    return
                votes.add(peer)
                if len(votes) == self.quorum():
                    self._start_election()

        for p in self.peers:
            threading.Thread(target=ask, args=(p,), daemon=True).start()

    def _start_election(self) -> None:
        self.term += 1
        self.role = CANDIDATE
        self.voted_for = self.id
        self.leader_id = None
        self._persist()
        self.election_deadline = self._new_deadline()
        term = self.term
        votes = {self.id}
        req = pb.VoteRequest(
            term=term, candidate_id=self.id,
            last_log_index=self.last_index(), last_log_term=self.last_term(),
        )
        log.info("inicio elección term=%s", term)
        if len(votes) >= self.quorum():
            self._become_leader()
            return

        def ask(peer: str) -> None:
            try:
                reply = self._stub(peer).RequestVote(req, timeout=0.5)
            except grpc.RpcError:
                return
            with self.lock:
                if reply.term > self.term:
                    self._become_follower(reply.term)
                    return
                if self.role != CANDIDATE or self.term != term or not reply.granted:
                    return
                votes.add(peer)
                if len(votes) >= self.quorum():
                    self._become_leader()

        for p in self.peers:
            threading.Thread(target=ask, args=(p,), daemon=True).start()

    # ------------------------------------------------------- RPC entrantes
    def handle_request_vote(self, req: pb.VoteRequest) -> pb.VoteReply:
        with self.lock:
            up_to_date = (req.last_log_term, req.last_log_index) >= (self.last_term(), self.last_index())
            if req.pre_vote:
                heard = self.role == LEADER or (
                    self.leader_id is not None
                    and time.monotonic() - self.last_leader_contact < self.election_timeout[0])
                return pb.VoteReply(term=self.term, granted=req.term > self.term and up_to_date and not heard)
            if req.term > self.term:
                self._become_follower(req.term)
            granted = False
            if (
                req.term == self.term
                and self.voted_for in (None, req.candidate_id)
                and up_to_date
            ):
                self.voted_for = req.candidate_id
                self._persist()
                self.election_deadline = self._new_deadline()
                granted = True
            return pb.VoteReply(term=self.term, granted=granted)

    def handle_append_entries(self, req: pb.AppendRequest) -> pb.AppendReply:
        with self.lock:
            if req.term < self.term:
                return pb.AppendReply(term=self.term, success=False)
            if req.term > self.term or self.role != FOLLOWER:
                self._become_follower(req.term, req.leader_id)
            self.leader_id = req.leader_id
            self.last_leader_contact = time.monotonic()
            self.election_deadline = self._new_deadline()

            prev = req.prev_log_index
            if prev > self.last_index():
                return pb.AppendReply(term=self.term, success=False, conflict_index=self.last_index() + 1)
            if prev >= self.snap_index and self.term_at(prev) != req.prev_log_term:
                bad_term = self.term_at(prev)
                ci = prev
                while ci - 1 > self.snap_index and self.term_at(ci - 1) == bad_term:
                    ci -= 1
                return pb.AppendReply(term=self.term, success=False, conflict_index=max(ci, self.snap_index + 1))

            new_entries: list[tuple[int, int, bytes]] = []
            for e in req.entries:
                if e.index <= self.snap_index:
                    continue
                if e.index <= self.last_index():
                    if self.term_at(e.index) == e.term:
                        continue
                    # conflicto: borrar desde aquí
                    self.storage.truncate_from(e.index)
                    del self.log[e.index - self.snap_index - 1:]
                new_entries.append((e.index, e.term, bytes(e.command)))
            if new_entries:
                self.storage.append(new_entries)
                self.log.extend(new_entries)
            last_new = prev + len(req.entries)
            if req.leader_commit > self.commit_index:
                self.commit_index = min(req.leader_commit, max(last_new, self.snap_index))
                self.cv.notify_all()
            return pb.AppendReply(term=self.term, success=True, match_index=last_new)

    def handle_install_snapshot(self, req: pb.SnapshotRequest) -> pb.SnapshotReply:
        with self.lock:
            if req.term < self.term:
                return pb.SnapshotReply(term=self.term)
            if req.term > self.term or self.role != FOLLOWER:
                self._become_follower(req.term, req.leader_id)
            self.leader_id = req.leader_id
            self.last_leader_contact = time.monotonic()
            self.election_deadline = self._new_deadline()
            if req.last_index <= self.commit_index:
                return pb.SnapshotReply(term=self.term)
            log.warning("instalando snapshot del líder hasta el índice %s", req.last_index)
            self.sm.restore(bytes(req.data))
            self.storage.save_snapshot(req.last_index, req.last_term, bytes(req.data))
            keep = [e for e in self.log if e[0] > req.last_index]
            if keep and self.term_at(req.last_index) == req.last_term:
                self.log = keep
            else:
                self.log = []
                self.storage.truncate_from(0)
            self.storage.compact_until(req.last_index)
            self.snap_index, self.snap_term = req.last_index, req.last_term
            self.commit_index = max(self.commit_index, req.last_index)
            self.last_applied = req.last_index
            self.cv.notify_all()
            return pb.SnapshotReply(term=self.term)

    # ------------------------------------------------------- replicación
    def _append_local(self, cmd: dict) -> int:
        idx = self.last_index() + 1
        entry = (idx, self.term, json.dumps(cmd, separators=(",", ":")).encode())
        self.log.append(entry)
        self.storage.append([entry])
        for ev in self.triggers.values():
            ev.set()
        return idx

    def _replicator(self, peer: str, term: int, trigger: threading.Event) -> None:
        backoff = self.heartbeat_interval
        while True:
            with self.lock:
                if not self.running or self.role != LEADER or self.term != term:
                    return
                ni = self.next_index[peer]
                if ni <= self.snap_index:
                    _, _, data = self.storage.load_snapshot()
                    req = pb.SnapshotRequest(
                        term=term, leader_id=self.id, last_index=self.snap_index,
                        last_term=self.snap_term, data=data or b"{}",
                    )
                    kind = "snap"
                else:
                    prev = ni - 1
                    batch = self.log[ni - self.snap_index - 1: ni - self.snap_index - 1 + 256]
                    req = pb.AppendRequest(
                        term=term, leader_id=self.id, prev_log_index=prev,
                        prev_log_term=self.term_at(prev),
                        entries=[pb.LogEntry(index=i, term=t, command=c) for i, t, c in batch],
                        leader_commit=self.commit_index,
                    )
                    kind = "append"
            sent_at = time.monotonic()
            try:
                if kind == "snap":
                    reply = self._stub(peer).InstallSnapshot(req, timeout=30)
                else:
                    reply = self._stub(peer).AppendEntries(req, timeout=1.0)
            except grpc.RpcError:
                trigger.wait(backoff)
                trigger.clear()
                backoff = min(backoff * 2, 1.0)
                continue
            backoff = self.heartbeat_interval
            more = False
            with self.lock:
                if reply.term > self.term:
                    self._become_follower(reply.term)
                    return
                if self.role != LEADER or self.term != term:
                    return
                self.last_ack[peer] = sent_at
                if kind == "snap":
                    self.match_index[peer] = max(self.match_index[peer], req.last_index)
                    self.next_index[peer] = req.last_index + 1
                    more = True
                elif reply.success:
                    self.match_index[peer] = max(self.match_index[peer], reply.match_index)
                    self.next_index[peer] = self.match_index[peer] + 1
                    self._advance_commit()
                    more = self.next_index[peer] <= self.last_index()
                else:
                    ci = reply.conflict_index or (ni - 1)
                    self.next_index[peer] = max(1, min(ci, ni - 1))
                    more = True
                self.cv.notify_all()
            if not more:
                trigger.wait(self.heartbeat_interval)
                trigger.clear()

    def _advance_commit(self) -> None:
        for n in range(self.last_index(), self.commit_index, -1):
            if self.term_at(n) != self.term:
                break
            count = 1 + sum(1 for p in self.peers if self.match_index.get(p, 0) >= n)
            if count >= self.quorum():
                self.commit_index = n
                self.cv.notify_all()
                break

    # ------------------------------------------------------------- aplicar
    def _applier(self) -> None:
        while self.running:
            with self.lock:
                while self.running and self.last_applied >= self.commit_index:
                    self.cv.wait(0.5)
                while self.last_applied < self.commit_index:
                    idx = self.last_applied + 1
                    _, term, cmd = self.entry_at(idx)
                    try:
                        result = self.sm.apply(json.loads(cmd))
                    except Exception as exc:  # nunca debe ocurrir; se registra y se sigue
                        log.exception("error aplicando la entrada %s", idx)
                        result = {"status": 500, "error": "APPLY_ERROR", "message": str(exc)}
                    self.last_applied = idx
                    if idx in self.waiters:
                        self.results[idx] = (term, result)
                self.cv.notify_all()
                self._maybe_snapshot()

    def _maybe_snapshot(self) -> None:
        if self.last_applied - self.snap_index < self.snapshot_threshold:
            return
        idx = self.last_applied
        term = self.term_at(idx)
        data = self.sm.snapshot()
        self.storage.save_snapshot(idx, term, data)
        self.log = [e for e in self.log if e[0] > idx]
        self.storage.compact_until(idx)
        self.snap_index, self.snap_term = idx, term
        log.info("snapshot tomado hasta el índice %s", idx)

    # ------------------------------------------------------ API para uso
    def propose(self, cmd: dict, timeout: float = 5.0) -> dict:
        with self.lock:
            if self.role != LEADER:
                raise NotLeader(self.leader_id, self.leader_addr())
            cmd.setdefault("ts", time.time())
            idx = self._append_local(cmd)
            self.waiters.add(idx)
            term = self.term
            if not self.peers:
                self._advance_commit()
            deadline = time.monotonic() + timeout
            try:
                while True:
                    if idx in self.results:
                        t, res = self.results.pop(idx)
                        if t != term:
                            raise NotLeader(self.leader_id, self.leader_addr())
                        return res
                    if self.role != LEADER or self.term != term:
                        raise NotLeader(self.leader_id, self.leader_addr())
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise ProposalTimeout(f"la entrada {idx} no se confirmó a tiempo")
                    self.cv.wait(remaining)
            finally:
                self.waiters.discard(idx)
                self.results.pop(idx, None)

    def read_barrier(self, timeout: float = 3.0) -> None:
        """Garantiza que el líder sigue siéndolo y aplicó todo lo confirmado."""
        lease = self.election_timeout[0] * 0.8
        deadline = time.monotonic() + timeout
        with self.lock:
            while True:
                if self.role != LEADER:
                    raise NotLeader(self.leader_id, self.leader_addr())
                now = time.monotonic()
                acks = 1 + sum(1 for p in self.peers if now - self.last_ack.get(p, 0) < lease)
                if self.commit_index >= self.leader_start_index and acks >= self.quorum():
                    break
                if now >= deadline:
                    raise ProposalTimeout("sin mayoría para confirmar la lectura")
                for ev in self.triggers.values():
                    ev.set()
                self.cv.wait(0.05)
            read_index = self.commit_index
            while self.last_applied < read_index:
                if time.monotonic() >= deadline:
                    raise ProposalTimeout("no se alcanzó a aplicar el log")
                self.cv.wait(0.05)
