"""Servicio gRPC de metadatos: Raft + operaciones + mantenimiento del líder.

Plano de control del diseño (secciones 3 a 9):
* atiende las operaciones que reenvía la puerta de entrada (RF1-RF3),
* recibe latidos (cada 3 s) e inventarios de los nodos de datos,
* como líder: detecta nodos caídos (6 s sospechoso, 10 s caído), vence locks
  y cargas, encola re-replicación y limpieza en RabbitMQ.
"""
from __future__ import annotations

import json
import logging
import threading
import time
import uuid

import grpc

from dfsha.common import config
from dfsha.common.audit import audit
from dfsha.common.crypto import new_wrapped_dek
from dfsha.common.mq import Publisher
from dfsha.common.paths import PathError, authorize, valid_username
from dfsha.common.tls import peer_cn, peer_role
from dfsha.common.tokens import sign_block_token
from dfsha.proto import dfsha_pb2 as pb
from dfsha.proto import dfsha_pb2_grpc as pbg

from .placement import Placement, live_nodes
from .raft import NotLeader, ProposalTimeout, RaftNode
from .state_machine import INF, Err, StateMachine, _overlap, n_blocks

log = logging.getLogger("metadata")


class LockQueue:
    """Cola FIFO por archivo para pedidos de lock con espera (sección 6)."""

    def __init__(self):
        self.lock = threading.Lock()
        self.queues: dict[str, list[dict]] = {}

    def enqueue(self, w: dict) -> None:
        with self.lock:
            self.queues.setdefault(w["file_id"], []).append(w)

    def remove(self, w: dict) -> None:
        with self.lock:
            q = self.queues.get(w["file_id"], [])
            if w in q:
                q.remove(w)

    def ahead_conflict(self, w: dict) -> bool:
        with self.lock:
            for other in self.queues.get(w["file_id"], []):
                if other is w:
                    return False
                if _overlap(other["start"], other["end"], w["start"], w["end"]) and (
                        other["mode"] == "exclusive" or w["mode"] == "exclusive"):
                    return True
            return False

    def notify(self, file_id: str) -> None:
        with self.lock:
            for w in self.queues.get(file_id, []):
                w["event"].set()


def _json(body: dict) -> bytes:
    return json.dumps(body, ensure_ascii=False, default=str).encode()


# Qué rol (CN del certificado) puede invocar cada operación
GATEWAY_OPS = {
    "ls", "stat", "mkdir", "rmdir", "rm", "mv", "manifest", "begin_upload", "realloc_block",
    "commit_upload", "abort_upload", "lock_acquire", "lock_renew", "lock_release",
    "report_replica", "cluster_status",
}
AUTH_OPS = {"get_user", "create_user", "set_password", "list_users"}
WORKER_OPS = {"task_plan", "task_complete"}
DN_OPS = {"report_bad_replica"}
ANY_OPS = {"raft_status"}


class MetadataService(pbg.MetadataServicer):
    def __init__(self, node_id: str, peers: dict[str, str], storage, channel_factory,
                 cert_name: str, enable_mq: bool = True):
        self.node_id = node_id
        self.sm = StateMachine()
        self.live: dict[str, dict] = {}
        self.live_lock = threading.Lock()
        self.placement = Placement()
        self.lockq = LockQueue()
        self.publisher = Publisher(cert_name) if enable_mq else None
        self.published: dict[str, float] = {}
        self.leader_epoch = 0
        self.raft = RaftNode(
            node_id, peers, storage, self.sm, channel_factory,
            election_timeout=(config.env_float("DFSHA_ELECTION_MIN", 1.0), config.env_float("DFSHA_ELECTION_MAX", 2.0)),
            heartbeat_interval=config.env_float("DFSHA_RAFT_HEARTBEAT", 0.25),
            snapshot_threshold=config.env_int("DFSHA_SNAPSHOT_EVERY", 2000),
            on_become_leader=self._on_leader, on_lose_leadership=self._on_follower,
        )

    def start(self) -> None:
        self.raft.start()

    # ================================================================ Raft RPC
    def RequestVote(self, request, context):
        return self.raft.handle_request_vote(request)

    def AppendEntries(self, request, context):
        return self.raft.handle_append_entries(request)

    def InstallSnapshot(self, request, context):
        return self.raft.handle_install_snapshot(request)

    # ============================================================== helpers
    def write(self, cmd: dict, timeout: float = 5.0) -> dict:
        res = self.raft.propose(cmd, timeout=timeout)
        status = res.pop("status", 200)
        if status >= 400:
            raise Err(status, res.pop("error", "ERROR"), res.pop("message", ""), **res)
        return res

    def read(self) -> None:
        self.raft.read_barrier()

    def live_snapshot(self) -> dict:
        with self.live_lock:
            return {k: dict(v) for k, v in self.live.items()}

    def _alive_replicas(self, replicas: list[str], live: dict) -> list[str]:
        alive = set(live_nodes(self.sm.s["nodes"], live, config.DEAD_AFTER))
        out = [dn for dn in replicas if dn in alive]
        out.sort(key=lambda dn: live.get(dn, {}).get("active", 0))
        return out

    def _node_addr(self, dn: str, internal: bool = False) -> str:
        n = self.sm.s["nodes"][dn]
        return n["internal_addr"] if internal else n["client_addr"]

    # ================================================================== Call
    def Call(self, request, context):
        role = peer_role(context)
        op = request.op
        allowed = (
            op in ANY_OPS
            or role == "insecure"
            or (role == "gateway" and op in GATEWAY_OPS)
            or (role == "auth" and op in AUTH_OPS)
            or (role == "worker" and op in WORKER_OPS)
            or (role == "dn" and op in DN_OPS)
        )
        if not allowed:
            audit("denied_internal", peer=peer_cn(context), op=op)
            return pb.MetaReply(status=403, payload=_json({"error": "FORBIDDEN",
                                "message": f"{peer_cn(context)} no puede invocar {op}"}))
        handler = getattr(self, "op_" + op, None)
        if handler is None:
            return pb.MetaReply(status=400, payload=_json({"error": "UNKNOWN_OP", "message": op}))
        try:
            payload = json.loads(request.payload or b"{}")
            status, body = handler(request.user, request.role, payload, request.wait)
        except NotLeader as e:
            return pb.MetaReply(status=421, payload=_json({"error": "NOT_LEADER"}),
                                leader_hint=e.leader_addr or "")
        except ProposalTimeout as e:
            return pb.MetaReply(status=503, payload=_json({"error": "NO_QUORUM", "message": str(e)}))
        except PathError as e:
            if e.status == 403:
                audit("access_denied", user=request.user, op=op, message=e.message)
            return pb.MetaReply(status=e.status, payload=_json({"error": e.code, "message": e.message}))
        except Err as e:
            if e.status == 403:
                audit("access_denied", user=request.user, op=op, message=e.message)
            return pb.MetaReply(status=e.status, payload=_json({"error": e.code, "message": e.message, **e.extra}))
        except (KeyError, ValueError, TypeError) as e:
            return pb.MetaReply(status=400, payload=_json({"error": "BAD_REQUEST", "message": repr(e)}))
        return pb.MetaReply(status=status, payload=_json(body))

    # ------------------------------------------------------------- RF1
    def op_raft_status(self, user, role, p, wait):
        """Estado de Raft de ESTA réplica y huella del estado aplicado (para comparar réplicas)."""
        st = self.raft.status()
        with self.raft.lock:
            data = self.sm.snapshot()
            st["applied_index"] = self.raft.last_applied
        import hashlib
        snap = json.loads(data)
        snap.pop("applied", None)
        st["state_sha256"] = hashlib.sha256(json.dumps(snap, sort_keys=True).encode()).hexdigest()[:16]
        return 200, st

    def op_ls(self, user, role, p, wait):
        path = authorize(user, p["path"])
        self.read()
        with self.sm.lock:
            e = self.sm.entry(path)
            if e is None:
                raise Err(404, "NOT_FOUND", f"No existe {path}")
            if e["type"] == "file":
                return 200, {"path": path, "entries": [self._describe(path, e)]}
            names = sorted(self.sm.s["children"].get(path, {}))
            base = path.rstrip("/")
            entries = [self._describe(f"{base}/{n}", self.sm.s["tree"][f"{base}/{n}"]) for n in names]
        return 200, {"path": path, "entries": entries}

    def _describe(self, path: str, e: dict) -> dict:
        d = {"name": path.rsplit("/", 1)[-1], "path": path, "type": e["type"], "mtime": e["mtime"]}
        if e["type"] == "file":
            f = self.sm.s["files"][e["file_id"]]
            d.update(size=f["size"], version=f["version"], blocks=len(f["blocks"]),
                     block_size=f["block_size"], replication=f["replication"])
        return d

    def op_stat(self, user, role, p, wait):
        path = authorize(user, p["path"])
        self.read()
        live = self.live_snapshot()
        with self.sm.lock:
            e = self.sm.entry(path)
            if e is None:
                raise Err(404, "NOT_FOUND", f"No existe {path}")
            d = self._describe(path, e)
            if e["type"] == "file":
                f = self.sm.s["files"][e["file_id"]]
                d["sha256"] = f["sha256"]
                d["file_id"] = f["file_id"]
                d["block_list"] = []
                for i, bid in enumerate(f["blocks"]):
                    b = self.sm.s["blocks"][bid]
                    d["block_list"].append({
                        "index": i, "block_id": bid, "size": b["size"], "sha256": b["sha256"],
                        "replicas": b["replicas"], "alive": self._alive_replicas(b["replicas"], live),
                    })
                d["locks"] = [
                    {"lock_id": lid, "owner": lk["owner"], "mode": lk["mode"], "start": lk["start"],
                     "end": None if lk["end"] >= INF else lk["end"], "token": lk["token"],
                     "expires_in": round(lk["expires"] - time.time(), 1)}
                    for lid, lk in self.sm.active_locks(f["file_id"], time.time())
                ]
        return 200, d

    def op_mkdir(self, user, role, p, wait):
        path = authorize(user, p["path"])
        return 201, self.write({"op": "mkdir", "path": path, "owner": user, "parents": bool(p.get("parents"))})

    def op_rmdir(self, user, role, p, wait):
        path = authorize(user, p["path"])
        return 204, self.write({"op": "rmdir", "path": path})

    def op_rm(self, user, role, p, wait):
        path = authorize(user, p["path"])
        res = self.write({"op": "rm", "path": path})
        audit("rm", user=user, path=path)
        return 204, res

    def op_mv(self, user, role, p, wait):
        src = authorize(user, p["src"])
        dst = authorize(user, p["dst"])
        return 200, self.write({"op": "mv", "src": src, "dst": dst})

    # ------------------------------------------------------------ RF2/RF3
    def _block_token(self, op: str, bid: str, file_id: str, wrapped: str, ttl: float,
                     user: str, pipeline: list[list[str]] | None = None) -> str:
        claims = {"b": bid, "op": op, "exp": round(time.time() + ttl, 3), "f": file_id, "k": wrapped, "u": user}
        if pipeline is not None:
            claims["p"] = pipeline
        return sign_block_token(claims)

    def op_manifest(self, user, role, p, wait):
        path = authorize(user, p["path"])
        self.read()
        live = self.live_snapshot()
        with self.sm.lock:
            f = self.sm.file_by_path(path)
            if f is None:
                e = self.sm.entry(path)
                if e is not None:
                    raise Err(400, "IS_A_DIRECTORY", f"{path} es un directorio")
                raise Err(404, "NOT_FOUND", f"No existe {path}")
            version = p.get("version")
            if version is None or int(version) == f["version"]:
                blocks, size, sha, version = f["blocks"], f["size"], f["sha256"], f["version"]
            else:
                hist = [h for h in f["history"] if h["version"] == int(version)]
                if not hist:
                    raise Err(410, "VERSION_GONE", f"La versión {version} ya no está disponible")
                h = hist[0]
                blocks, size, sha = h["blocks"], h["size"], h["sha256"]
            out = []
            for i, bid in enumerate(blocks):
                b = self.sm.s["blocks"][bid]
                alive = self._alive_replicas(b["replicas"], live)
                out.append({
                    "index": i, "block_id": bid, "size": b["size"], "sha256": b["sha256"],
                    "replicas": [{"dn_id": dn, "addr": self._node_addr(dn)} for dn in alive],
                    "token": self._block_token("r", bid, f["file_id"], f["wrapped_dek"],
                                               config.READ_TOKEN_TTL, user),
                })
            return 200, {
                "path": path, "file_id": f["file_id"], "version": version, "size": size,
                "block_size": f["block_size"], "replication": f["replication"], "sha256": sha,
                "blocks": out, "token_expires_in": config.READ_TOKEN_TTL,
            }

    def _allocate(self, k: int, bs: int, n: int, exclude=frozenset()) -> list[list[str]]:
        live = self.live_snapshot()
        need = min(config.MIN_REPLICAS, k)
        planned: dict[str, int] = {}
        primaries: dict[str, int] = {}
        with self.sm.lock:
            nodes = self.sm.s["nodes"]
            return [self.placement.choose(nodes, live, k, need, bs, exclude, planned, primaries) for _ in range(n)]

    def op_begin_upload(self, user, role, p, wait):
        path = authorize(user, p["path"])
        mode = p.get("mode", "replace")
        if mode not in ("create", "replace", "append", "modify"):
            raise Err(400, "BAD_REQUEST", f"Modo {mode} no válido")
        size = int(p["size"])
        if size < 0:
            raise Err(400, "BAD_REQUEST", "Tamaño negativo")
        self.read()
        with self.sm.lock:
            e = self.sm.entry(path)
            if e is not None and e["type"] == "dir":
                raise Err(400, "IS_A_DIRECTORY", f"{path} es un directorio")
            f = self.sm.file_by_path(path)
            if f is None and mode in ("append", "modify"):
                raise Err(404, "NOT_FOUND", f"No existe {path}")
            if f is not None and mode == "create":
                raise Err(409, "ALREADY_EXISTS", f"Ya existe {path}")
            if f is not None and mode in ("append", "modify"):
                bs, repl = f["block_size"], f["replication"]
            else:
                bs = int(p.get("block_size") or config.BLOCK_SIZE)
                repl = int(p.get("replication") or (f["replication"] if f else config.REPLICATION))
            if bs < 64 * 1024 or bs > 256 * 1024 * 1024:
                raise Err(400, "BAD_REQUEST", "block_size fuera de rango (64 KiB a 256 MiB)")
            repl = max(1, min(5, repl))
            nb = n_blocks(size, bs)
            if mode in ("create", "replace"):
                indices = list(range(nb))
            else:
                indices = sorted({int(i) for i in p.get("indices", [])})
                if any(i < 0 or i >= nb for i in indices):
                    raise Err(400, "BAD_BLOCK", "Índices de bloque fuera del tamaño indicado")
            file_id = f["file_id"] if f else "f-" + uuid.uuid4().hex[:12]
            wrapped = f["wrapped_dek"] if f else new_wrapped_dek(file_id)
            base_version = f["version"] if f else 0
            fid_expected = f["file_id"] if f else None
        pipelines = self._allocate(repl, bs, len(indices))
        upload_id = "up-" + uuid.uuid4().hex[:12]
        allocations = [{"index": i, "block_id": "b-" + uuid.uuid4().hex, "pipeline": pl}
                       for i, pl in zip(indices, pipelines)]
        self.write({
            "op": "begin_upload", "upload_id": upload_id, "path": path, "owner": user, "mode": mode,
            "size": size, "block_size": bs, "replication": repl, "base_version": base_version,
            "file_id": file_id, "file_id_expected": fid_expected, "wrapped_dek": wrapped,
            "allocations": allocations,
        })
        with self.sm.lock:
            blocks = [self._alloc_reply(a["index"], a["block_id"], a["pipeline"], file_id, wrapped, user)
                      for a in allocations]
        return 201, {"upload_id": upload_id, "block_size": bs, "base_version": base_version,
                     "replication": repl, "file_id": file_id, "blocks": blocks}

    def _alloc_reply(self, index, bid, pipeline, file_id, wrapped, user) -> dict:
        chain = [[dn, self._node_addr(dn, internal=True)] for dn in pipeline]
        return {
            "index": index, "block_id": bid, "nodes": pipeline,
            "pipeline": [self._node_addr(dn) for dn in pipeline],
            "token": self._block_token("w", bid, file_id, wrapped, config.WRITE_TOKEN_TTL, user, chain),
        }

    def op_realloc_block(self, user, role, p, wait):
        """Extensión del Anexo A: pedir nodos de reemplazo para un bloque (escenario 9.3)."""
        self.read()
        with self.sm.lock:
            up = self.sm.s["uploads"].get(p["upload_id"])
            if not up or up["owner"] != user:
                raise Err(404, "UPLOAD_NOT_FOUND", "No existe la carga")
            if up["state"] != "open":
                raise Err(410, "UPLOAD_EXPIRED", "La carga ya no está abierta")
            alloc = up["allocations"].get(str(int(p["index"])))
            if not alloc:
                raise Err(400, "BAD_BLOCK", "Ese índice no pertenece a la carga")
            repl, bs, file_id, wrapped = up["replication"], up["block_size"], up["file_id"], up["wrapped_dek"]
        exclude = set(p.get("exclude", []))
        pipeline = self._allocate(repl, bs, 1, exclude)[0]
        res = self.write({"op": "realloc_block", "upload_id": p["upload_id"], "index": int(p["index"]),
                          "pipeline": pipeline})
        with self.sm.lock:
            return 200, self._alloc_reply(int(p["index"]), res["block_id"], pipeline, file_id, wrapped, user)

    def op_commit_upload(self, user, role, p, wait):
        cmd = {
            "op": "commit_upload", "upload_id": p["upload_id"], "owner": user,
            "session": p.get("session"), "expected_version": p.get("expected_version"),
            "size": int(p["size"]), "blocks": p.get("blocks", []), "file_sha256": p.get("file_sha256"),
            "lock_id": p.get("lock_id"), "fence": p.get("fence"),
        }
        try:
            res = self.write(cmd)
        except Err as e:
            audit("commit_rejected", user=user, upload_id=p["upload_id"], error=e.code, message=e.message)
            raise
        audit("commit", user=user, path=res.get("path"), version=res.get("version"), size=res.get("size"),
              upload_id=p["upload_id"])
        return 200, res

    def op_abort_upload(self, user, role, p, wait):
        return 204, self.write({"op": "abort_upload", "upload_id": p["upload_id"], "owner": user})

    def op_report_replica(self, user, role, p, wait):
        audit("replica_problem", user=user, block_id=p.get("block_id"), dn_id=p.get("dn_id"),
              reason=p.get("reason"))
        if p.get("reason") == "hash_mismatch":
            self.write({"op": "remove_replica", "block_id": p["block_id"], "dn_id": p["dn_id"]})
        return 202, {}

    def op_report_bad_replica(self, user, role, p, wait):
        audit("replica_corrupt", dn_id=p["dn_id"], block_id=p["block_id"], reason=p.get("reason"))
        self.write({"op": "remove_replica", "block_id": p["block_id"], "dn_id": p["dn_id"]})
        return 200, {}

    # ---------------------------------------------------------------- locks
    def op_lock_acquire(self, user, role, p, wait):
        path = authorize(user, p["path"])
        mode = p.get("mode", "exclusive")
        if mode not in ("shared", "exclusive"):
            raise Err(400, "BAD_REQUEST", "mode debe ser shared o exclusive")
        start = int(p.get("start", 0))
        end = p.get("end")
        end_i = INF if end in (None, -1) else int(end)
        if start < 0 or end_i < start:
            raise Err(400, "BAD_REQUEST", "Rango de bloques inválido")
        self.read()
        with self.sm.lock:
            f = self.sm.file_by_path(path)
            if f is None:
                raise Err(404, "NOT_FOUND", f"No existe {path}")
            file_id = f["file_id"]
        wait = max(0.0, min(float(wait or p.get("wait", 0)), 120.0))
        w = {"file_id": file_id, "start": start, "end": end_i, "mode": mode, "event": threading.Event()}
        deadline = time.monotonic() + wait
        if wait > 0:
            self.lockq.enqueue(w)
        try:
            while True:
                if not self.lockq.ahead_conflict(w):
                    try:
                        res = self.write({
                            "op": "lock_acquire", "lock_id": "lk-" + uuid.uuid4().hex[:12], "file_id": file_id,
                            "start": start, "end": end, "mode": mode, "owner": user,
                            "session": p.get("session"), "lease": config.LOCK_LEASE,
                        })
                        res["path"] = path
                        audit("lock", user=user, path=path, mode=mode, start=start, end=end, token=res["token"])
                        return 201, res
                    except Err as e:
                        if e.status != 423 or wait <= 0:
                            raise
                elif wait <= 0:
                    raise Err(423, "LOCKED", "Hay otros clientes esperando ese rango")
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise Err(423, "LOCKED", f"No se obtuvo el lock en {wait:.0f} s")
                w["event"].wait(min(remaining, 1.0))
                w["event"].clear()
        finally:
            self.lockq.remove(w)

    def op_lock_renew(self, user, role, p, wait):
        return 200, self.write({"op": "lock_renew", "lock_id": p["lock_id"], "owner": user,
                                "lease": config.LOCK_LEASE})

    def op_lock_release(self, user, role, p, wait):
        res = self.write({"op": "lock_release", "lock_id": p["lock_id"], "owner": user})
        if res.get("file_id"):
            self.lockq.notify(res["file_id"])
        return 204, res

    # ------------------------------------------------------- autenticación
    def op_get_user(self, user, role, p, wait):
        self.read()
        with self.sm.lock:
            u = self.sm.s["users"].get(p["username"])
            if not u:
                raise Err(404, "NOT_FOUND", "Usuario desconocido")
            return 200, {"username": p["username"], "pw_hash": u["pw_hash"], "role": u.get("role", "user")}

    def op_create_user(self, user, role, p, wait):
        if not valid_username(p["username"]):
            raise Err(400, "BAD_REQUEST", "Nombre de usuario inválido (minúsculas, números, . _ -)")
        res = self.write({"op": "create_user", "username": p["username"], "pw_hash": p["pw_hash"],
                          "role": p.get("role", "user")})
        audit("user_created", username=p["username"], role=p.get("role", "user"))
        return 201, res

    def op_set_password(self, user, role, p, wait):
        return 200, self.write({"op": "set_password", "username": p["username"], "pw_hash": p["pw_hash"]})

    def op_list_users(self, user, role, p, wait):
        self.read()
        with self.sm.lock:
            return 200, {"users": [{"username": u, "role": d.get("role")} for u, d in self.sm.s["users"].items()]}

    # ------------------------------------------------------- administración
    def op_cluster_status(self, user, role, p, wait):
        if role != "admin":
            raise Err(403, "FORBIDDEN", "Solo el administrador puede ver el estado del clúster")
        self.read()
        live = self.live_snapshot()
        now = time.monotonic()
        with self.sm.lock:
            s = self.sm.s
            nodes = []
            for dn, n in sorted(s["nodes"].items()):
                info = live.get(dn)
                age = round(now - info["t"], 1) if info else None
                health = n["state"]
                if n["state"] == "active" and (age is None or age > config.SUSPECT_AFTER):
                    health = "suspect"
                nodes.append({"dn_id": dn, "state": health, "client_addr": n["client_addr"],
                              "zone": n.get("zone"), "last_heartbeat_s": age,
                              "free_gib": round(info["free"] / 2**30, 2) if info else None,
                              "active": info.get("active") if info else None,
                              "blocks": info.get("blocks") if info else None})
            committed = [b for b in s["blocks"].values() if b["state"] == "committed"]
            under = sum(1 for b in committed if len(b["replicas"]) < b.get("replication", 3))
            lost = sum(1 for b in committed if not b["replicas"])
            return 200, {
                "raft": self.raft.status(),
                "nodes": nodes,
                "files": sum(1 for f in s["files"].values() if not f["deleted"]),
                "blocks_committed": len(committed),
                "blocks_under_replicated": under,
                "blocks_without_replica": lost,
                "blocks_garbage": sum(1 for b in s["blocks"].values() if b["state"] == "garbage"),
                "open_uploads": sum(1 for u in s["uploads"].values() if u["state"] == "open"),
                "locks": len(s["locks"]),
                "tasks_pending_count": len(s["tasks"]),
                "tasks_pending": [{"task_id": k, **v} for k, v in list(s["tasks"].items())[:50]],
                "mq_connected": bool(self.publisher and self.publisher.ok),
            }

    # ----------------------------------------------------------- trabajadores
    def op_task_plan(self, user, role, p, wait):
        self.read()
        live = self.live_snapshot()
        with self.sm.lock:
            t = self.sm.s["tasks"].get(p["task_id"])
            if t is None:
                return 200, {"action": "skip", "reason": "tarea desconocida o ya resuelta"}
            b = self.sm.s["blocks"].get(t["block_id"])
            if b is None:
                return 200, {"action": "complete", "reason": "el bloque ya no existe"}
            f = self.sm.s["files"].get(b["file_id"])
            if t["type"] == "replicate":
                if b["state"] != "committed" or f is None:
                    return 200, {"action": "complete", "reason": "el bloque ya no está confirmado"}
                alive = self._alive_replicas(b["replicas"], live)
                if len(alive) >= b.get("replication", 3):
                    return 200, {"action": "complete", "reason": "ya tiene réplicas suficientes"}
                if not alive:
                    return 200, {"action": "complete", "reason": "no hay réplica viva para copiar"}
                nodes = self.sm.s["nodes"]
                try:
                    target = self.placement.choose(nodes, live, 1, 1, b["size"] or 0, set(b["replicas"]))[0]
                except Err as e:
                    return 200, {"action": "retry", "reason": e.message}
                token = self._block_token("rep", t["block_id"], f["file_id"], f["wrapped_dek"], 600, "worker",
                                          [[target, self._node_addr(target, True)]])
                return 200, {"action": "replicate", "block_id": t["block_id"], "target": target,
                             "target_addr": self._node_addr(target, True), "source": alive[0],
                             "source_addr": self._node_addr(alive[0], True), "token": token,
                             "sha256": b["sha256"], "size": b["size"]}
            wrapped, fid = (f["wrapped_dek"], f["file_id"]) if f else ("", "")
            token = self._block_token("d", t["block_id"], fid, wrapped, 600, "worker")
            if t["type"] == "delete_replica":
                dn = t["dn_id"]
                if dn not in self.sm.s["nodes"]:
                    return 200, {"action": "complete", "reason": "nodo desconocido"}
                return 200, {"action": "delete", "targets": [[dn, self._node_addr(dn, True)]], "token": token}
            targets = sorted(set(b["replicas"]) | set(b.get("candidates", [])))
            alive = set(live_nodes(self.sm.s["nodes"], live, config.DEAD_AFTER))
            return 200, {"action": "delete", "token": token,
                         "targets": [[dn, self._node_addr(dn, True)] for dn in targets if dn in alive]}

    def op_task_complete(self, user, role, p, wait):
        res = self.write({"op": "task_complete", "task_id": p["task_id"], "receipt": p.get("receipt")})
        self.published.pop(p["task_id"], None)
        return 200, res

    # ================================================== nodos de datos
    def Heartbeat(self, request, context):
        if peer_role(context) not in ("dn", "insecure"):
            context.abort(grpc.StatusCode.PERMISSION_DENIED, "solo nodos de datos")
        with self.live_lock:
            self.live[request.dn_id] = {
                "t": time.monotonic(), "free": request.free, "capacity": request.capacity,
                "active": request.active_transfers, "blocks": request.block_count,
                "client_addr": request.client_addr, "internal_addr": request.internal_addr,
                "zone": request.zone,
            }
        if not self.raft.is_leader():
            return pb.HeartbeatReply(is_leader=False, leader_hint=self.raft.leader_addr() or "")
        need_report = False
        with self.sm.lock:
            node = self.sm.s["nodes"].get(request.dn_id)
            changed = (node is None or node["state"] != "active"
                       or node["client_addr"] != request.client_addr
                       or node["internal_addr"] != request.internal_addr)
            was_dead = node is not None and node["state"] == "dead"
        if changed:
            try:
                self.write({"op": "register_node", "dn_id": request.dn_id, "client_addr": request.client_addr,
                            "internal_addr": request.internal_addr, "zone": request.zone,
                            "capacity": request.capacity}, timeout=2.0)
                log.warning("nodo %s registrado (%s)", request.dn_id, "regresa" if was_dead else "nuevo/actualizado")
                audit("node_registered", dn_id=request.dn_id, returning=was_dead)
                need_report = True
            except (NotLeader, ProposalTimeout, Err) as exc:
                log.warning("no se pudo registrar %s: %s", request.dn_id, exc)
        return pb.HeartbeatReply(is_leader=True, need_block_report=need_report)

    def BlockReport(self, request, context):
        if peer_role(context) not in ("dn", "insecure"):
            context.abort(grpc.StatusCode.PERMISSION_DENIED, "solo nodos de datos")
        if not self.raft.is_leader():
            return pb.BlockReportReply(accepted=False, leader_hint=self.raft.leader_addr() or "")
        try:
            self.read()
        except (NotLeader, ProposalTimeout):
            return pb.BlockReportReply(accepted=False, leader_hint=self.raft.leader_addr() or "")
        dn = request.dn_id
        delete, add = [], []
        with self.sm.lock:
            node = self.sm.s["nodes"].get(dn)
            if node is None or node["state"] != "active":
                return pb.BlockReportReply(accepted=False)
            for bi in request.blocks:
                b = self.sm.s["blocks"].get(bi.block_id)
                if b is None or b["state"] == "garbage":
                    delete.append(bi.block_id)
                elif b["state"] == "writing":
                    continue
                elif b["sha256"] != bi.sha256:
                    delete.append(bi.block_id)
                elif dn in b["replicas"]:
                    continue
                elif len(b["replicas"]) < b.get("replication", 3):
                    add.append({"block_id": bi.block_id, "dn_id": dn, "sha256": bi.sha256})
                else:
                    delete.append(bi.block_id)
        if add:
            try:
                self.write({"op": "add_replicas", "items": add}, timeout=5.0)
            except (NotLeader, ProposalTimeout, Err):
                return pb.BlockReportReply(accepted=False)
        log.info("inventario de %s: %d bloques, %d aceptados de nuevo, %d a borrar",
                 dn, len(request.blocks), len(add), len(delete))
        audit("block_report", dn_id=dn, blocks=len(request.blocks), readded=len(add), delete=len(delete))
        return pb.BlockReportReply(accepted=True, delete=delete)

    # ============================================ mantenimiento del líder
    def _on_leader(self) -> None:
        self.leader_epoch += 1
        epoch = self.leader_epoch
        self.published.clear()
        self.leader_started = time.monotonic()
        threading.Thread(target=self._maintenance, args=(epoch,), name="maintenance", daemon=True).start()

    def _on_follower(self) -> None:
        self.leader_epoch += 1

    def _maintenance(self, epoch: int) -> None:
        log.info("mantenimiento del líder iniciado")
        tick = 0
        while self.leader_epoch == epoch and self.raft.is_leader():
            time.sleep(1.0)
            tick += 1
            try:
                self._detect_failures()
                self._expire()
                if tick % 3 == 0:
                    self._replication_scan()
                    self._garbage_scan()
                self._publish_tasks()
            except (NotLeader, ProposalTimeout):
                pass
            except Exception:
                log.exception("error en mantenimiento")
        log.info("mantenimiento del líder detenido")

    def _detect_failures(self) -> None:
        live = self.live_snapshot()
        now = time.monotonic()
        dead = []
        with self.sm.lock:
            for dn, node in self.sm.s["nodes"].items():
                if node["state"] != "active":
                    continue
                last = max(live[dn]["t"] if dn in live else 0, self.leader_started)
                if now - last > config.DEAD_AFTER:
                    dead.append(dn)
        for dn in dead:
            res = self.write({"op": "node_dead", "dn_id": dn})
            log.warning("nodo %s CAÍDO (sin latidos > %ss); %s bloques con réplica menos",
                        dn, config.DEAD_AFTER, res.get("affected"))
            audit("node_dead", dn_id=dn, affected_blocks=res.get("affected"))

    def _expire(self) -> None:
        ts = time.time()
        with self.sm.lock:
            s = self.sm.s
            need = (any(lk["expires"] < ts for lk in s["locks"].values())
                    or any(u["state"] == "open" and u["expires"] < ts for u in s["uploads"].values())
                    or any(u["state"] != "open" and u["created"] + 2 * config.UPLOAD_TTL < ts
                           for u in s["uploads"].values())
                    or any(h["superseded_at"] + config.HISTORY_RETENTION < ts
                           for f in s["files"].values() for h in f["history"])
                    or any(f["deleted"] and not f["history"] for f in s["files"].values()))
        if need:
            res = self.write({"op": "expire", "ts": ts})
            for fid in res.get("files_touched", []):
                self.lockq.notify(fid)
            if res.get("locks") or res.get("uploads"):
                log.info("vencimientos: %s locks, %s cargas, %s versiones viejas",
                         res.get("locks"), res.get("uploads"), res.get("history"))

    def _replication_scan(self) -> None:
        live = self.live_snapshot()
        alive = set(live_nodes(self.sm.s["nodes"], live, config.DEAD_AFTER))
        tasks, trims = [], []
        with self.sm.lock:
            pending = {(t["type"], t["block_id"]) for t in self.sm.s["tasks"].values()}
            for bid, b in self.sm.s["blocks"].items():
                if b["state"] != "committed":
                    continue
                want = b.get("replication", 3)
                n = len(b["replicas"])
                if n == 0:
                    continue  # sin réplica viva no hay de dónde copiar; se recupera si el nodo vuelve
                if n < want and ("replicate", bid) not in pending:
                    tasks.append({"task_id": "t-" + uuid.uuid4().hex[:12], "type": "replicate",
                                  "block_id": bid, "priority": 9 - min(n, 9)})
                elif n > want and all(dn in alive for dn in b["replicas"]):
                    extra = sorted(b["replicas"], key=lambda dn: -live.get(dn, {}).get("active", 0))[0]
                    trims.append((bid, extra))
        if tasks:
            tasks.sort(key=lambda t: -t["priority"])  # primero los bloques con menos réplicas
            self.write({"op": "task_create", "tasks": tasks[:500]})
            log.info("re-replicación: %d bloques con menos réplicas de las debidas", len(tasks))
        for bid, dn in trims[:100]:
            self.write({"op": "trim_replica", "block_id": bid, "dn_id": dn,
                        "task_id": "t-" + uuid.uuid4().hex[:12]})

    def _garbage_scan(self) -> None:
        with self.sm.lock:
            s = self.sm.s
            referenced = set()
            for f in s["files"].values():
                referenced.update(f["blocks"])
                for h in f["history"]:
                    referenced.update(h["blocks"])
            open_uploads = {u for u, up in s["uploads"].items() if up["state"] == "open"}
            mark = []
            for bid, b in s["blocks"].items():
                if b["state"] == "committed" and bid not in referenced:
                    mark.append(bid)
                elif b["state"] == "writing" and b.get("upload_id") not in open_uploads:
                    mark.append(bid)
            pending = {t["block_id"] for t in s["tasks"].values() if t["type"] == "delete"}
            garbage = [bid for bid, b in s["blocks"].items()
                       if (b["state"] == "garbage" or bid in mark) and bid not in pending]
        if mark:
            self.write({"op": "mark_garbage", "block_ids": mark})
        if garbage:
            self.write({"op": "task_create", "tasks": [
                {"task_id": "t-" + uuid.uuid4().hex[:12], "type": "delete", "block_id": bid, "priority": 1}
                for bid in garbage[:1000]]})
            log.info("limpieza: %d bloques sin uso encolados para borrar", len(garbage))

    def _publish_tasks(self) -> None:
        if not self.publisher:
            return
        now = time.monotonic()
        with self.sm.lock:
            tasks = [dict(t, task_id=tid) for tid, t in self.sm.s["tasks"].items()]
        for t in tasks:
            last = self.published.get(t["task_id"])
            if last is not None and now - last < 30:
                continue
            if self.publisher.publish({"task_id": t["task_id"], "type": t["type"], "block_id": t["block_id"]},
                                      priority=t.get("priority", 5)):
                self.published[t["task_id"]] = now
