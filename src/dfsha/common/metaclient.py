"""Cliente gRPC del servicio de metadatos que sigue al líder (sección 9.4, paso 5).

Si la réplica consultada no es el líder responde 421 con la dirección del
líder y aquí se reintenta allí, sin que quien llama lo note. Si no hay líder
(elección en curso) se reintenta durante unos segundos y, si no aparece,
se devuelve 503 NO_QUORUM.
"""
from __future__ import annotations

import json
import logging
import threading
import time

import grpc

from dfsha.proto import dfsha_pb2 as pb
from dfsha.proto import dfsha_pb2_grpc as pbg

from .tls import grpc_channel

log = logging.getLogger("metaclient")


class MetaClient:
    def __init__(self, addrs: list[str], cert_name: str, failover_window: float = 8.0):
        if not addrs:
            raise ValueError("Se necesita al menos una dirección de metadatos")
        self.addrs = list(addrs)
        self.cert_name = cert_name
        self.failover_window = failover_window
        self._stubs: dict[str, pbg.MetadataStub] = {}
        self._lock = threading.Lock()
        self.leader: str | None = None
        self._rr = 0

    def stub(self, addr: str) -> pbg.MetadataStub:
        with self._lock:
            s = self._stubs.get(addr)
            if s is None:
                s = pbg.MetadataStub(grpc_channel(addr, self.cert_name))
                self._stubs[addr] = s
            return s

    def _next_addr(self) -> str:
        with self._lock:
            if self.leader:
                return self.leader
            addr = self.addrs[self._rr % len(self.addrs)]
            self._rr += 1
            return addr

    def call(self, op: str, payload: dict | None = None, user: str = "", role: str = "",
             wait: float = 0.0, timeout: float | None = None) -> tuple[int, dict]:
        window = timeout if timeout is not None else self.failover_window
        deadline = time.monotonic() + window + wait
        req = pb.MetaRequest(op=op, user=user, role=role,
                             payload=json.dumps(payload or {}).encode(), wait=wait)
        last_err = "sin respuesta"
        while time.monotonic() < deadline:
            addr = self._next_addr()
            try:
                reply = self.stub(addr).Call(req, timeout=max(2.0, wait + 5.0))
            except grpc.RpcError as exc:
                last_err = f"{addr}: {exc.code().name}"
                with self._lock:
                    if self.leader == addr:
                        self.leader = None
                time.sleep(0.15)
                continue
            if reply.status == 421:
                with self._lock:
                    self.leader = reply.leader_hint or None
                if not reply.leader_hint:
                    time.sleep(0.25)
                last_err = "elección de líder en curso"
                continue
            with self._lock:
                self.leader = addr
            body = json.loads(reply.payload) if reply.payload else {}
            return reply.status, body
        return 503, {"error": "NO_QUORUM", "message": f"Metadatos no tiene líder disponible ({last_err})"}

    def leader_stub(self) -> pbg.MetadataStub:
        return self.stub(self._next_addr())
