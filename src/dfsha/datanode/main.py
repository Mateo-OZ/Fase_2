"""Arranque de un nodo de datos.

Variables de entorno principales:
  DFSHA_DN_ID            dn1 ... dn4 (también nombre del certificado)
  DFSHA_DN_DATA          /var/lib/dfsha/dn
  DFSHA_DN_CLIENT_BIND   0.0.0.0:7001     (clientes, TLS)
  DFSHA_DN_INTERNAL_BIND 0.0.0.0:7002     (servidores, TLS mutuo)
  DFSHA_DN_ADVERTISE     10.20.2.21       (IP privada anunciada a metadatos)
  DFSHA_META_PEERS       meta1=...,meta2=...,meta3=...
  DFSHA_DN_ZONE          us-east-1a
"""
from __future__ import annotations

import signal
import threading
import time
from concurrent import futures

import grpc

from dfsha.common import config
from dfsha.common.audit import audit, setup_logging
from dfsha.common.config import env, env_bool, env_int, parse_peers
from dfsha.common.metaclient import MetaClient
from dfsha.common.tls import SERVER_OPTIONS, add_grpc_port, grpc_channel
from dfsha.proto import dfsha_pb2 as pb
from dfsha.proto import dfsha_pb2_grpc as pbg

from .service import DataNodeService, SharedState
from .store import BlockStore


class DataNode:
    def __init__(self):
        self.dn_id = env("DFSHA_DN_ID", required=True)
        self.log = setup_logging(self.dn_id)
        self.root = env("DFSHA_DN_DATA", "/var/lib/dfsha/dn")
        self.client_bind = env("DFSHA_DN_CLIENT_BIND", "0.0.0.0:7001")
        self.internal_bind = env("DFSHA_DN_INTERNAL_BIND", "0.0.0.0:7002")
        adv = env("DFSHA_DN_ADVERTISE", "127.0.0.1")
        self.client_addr = env("DFSHA_DN_CLIENT_ADDR", f"{adv}:{self.client_bind.rsplit(':', 1)[1]}")
        self.internal_addr = env("DFSHA_DN_INTERNAL_ADDR", f"{adv}:{self.internal_bind.rsplit(':', 1)[1]}")
        self.zone = env("DFSHA_DN_ZONE", "")
        self.peers = parse_peers(env("DFSHA_META_PEERS", required=True))
        self.store = BlockStore(self.root, encrypt=env_bool("DFSHA_ENCRYPT_AT_REST", True))
        self.meta = MetaClient(list(self.peers.values()), self.dn_id)
        self.shared = SharedState(self.dn_id, self.root, self._report_corrupt)
        self.meta_stubs = {addr: pbg.MetadataStub(grpc_channel(addr, self.dn_id)) for addr in self.peers.values()}
        self.report_pending = True
        self.stop = threading.Event()

    # ------------------------------------------------------------ latidos
    def _heartbeat_loop(self) -> None:
        while not self.stop.is_set():
            total, free = self.store.usage()
            req = pb.HeartbeatRequest(
                dn_id=self.dn_id, client_addr=self.client_addr, internal_addr=self.internal_addr,
                zone=self.zone, capacity=total, free=free, active_transfers=self.shared.active,
                block_count=len(self.store.index),
            )
            threads = [threading.Thread(target=self._beat, args=(addr, stub, req), daemon=True)
                       for addr, stub in self.meta_stubs.items()]
            for t in threads:
                t.start()
            self.stop.wait(config.HEARTBEAT_INTERVAL)

    def _beat(self, addr: str, stub, req) -> None:
        try:
            reply = stub.Heartbeat(req, timeout=2.5)
        except grpc.RpcError:
            return
        if reply.is_leader and (reply.need_block_report or self.report_pending):
            self._send_report(stub)

    def _send_report(self, stub) -> None:
        blocks = [pb.BlockInfo(block_id=m["block_id"], sha256=m["sha256"], size=m["size"])
                  for m in self.store.list()]
        try:
            reply = stub.BlockReport(pb.BlockReportRequest(dn_id=self.dn_id, blocks=blocks), timeout=20)
        except grpc.RpcError as exc:
            self.log.warning("no se pudo enviar el inventario: %s", exc.code().name)
            return
        if not reply.accepted:
            return
        self.report_pending = False
        for bid in reply.delete:
            self.store.delete(bid)
        self.log.info("inventario enviado: %d bloques; metadatos pidió borrar %d", len(blocks), len(reply.delete))

    # ----------------------------------------------------------- scrubber
    def _scrub_loop(self) -> None:
        interval = config.SCRUB_INTERVAL
        while not self.stop.wait(interval):
            bad = 0
            for m in self.store.list():
                if not self.store.verify_at_rest(m["block_id"]):
                    bad += 1
                    self._report_corrupt(m["block_id"], "el revisor periódico encontró el bloque dañado")
            self.log.info("revisión periódica terminada: %d bloques dañados", bad)

    def _report_corrupt(self, block_id: str, reason: str) -> None:
        self.log.error("bloque %s dañado: %s", block_id, reason)
        self.store.delete(block_id)
        status, body = self.meta.call("report_bad_replica", {"block_id": block_id, "dn_id": self.dn_id,
                                                             "reason": reason})
        audit("corrupt_block_reported", dn_id=self.dn_id, block_id=block_id, status=status)

    # ---------------------------------------------------------- servidor
    def run(self) -> None:
        workers = futures.ThreadPoolExecutor(max_workers=env_int("DFSHA_DN_THREADS", 64))
        client_srv = grpc.server(workers, options=SERVER_OPTIONS)
        internal_srv = grpc.server(futures.ThreadPoolExecutor(max_workers=env_int("DFSHA_DN_THREADS", 64)),
                                   options=SERVER_OPTIONS)
        pbg.add_DataNodeServicer_to_server(DataNodeService(self.dn_id, self.store, False, self.shared), client_srv)
        pbg.add_DataNodeServicer_to_server(DataNodeService(self.dn_id, self.store, True, self.shared), internal_srv)
        add_grpc_port(client_srv, self.client_bind, self.dn_id, mutual=False)
        add_grpc_port(internal_srv, self.internal_bind, self.dn_id, mutual=True)
        client_srv.start()
        internal_srv.start()
        self.log.info("nodo %s: clientes %s (TLS), servidores %s (TLS mutuo); %d bloques en disco; anuncia %s",
                      self.dn_id, self.client_bind, self.internal_bind, len(self.store.index), self.client_addr)
        threading.Thread(target=self._heartbeat_loop, name="heartbeat", daemon=True).start()
        threading.Thread(target=self._scrub_loop, name="scrubber", daemon=True).start()
        signal.signal(signal.SIGTERM, lambda *_: self.stop.set())
        signal.signal(signal.SIGINT, lambda *_: self.stop.set())
        while not self.stop.is_set():
            time.sleep(0.5)
        client_srv.stop(2)
        internal_srv.stop(2)


def main() -> None:
    DataNode().run()


if __name__ == "__main__":
    main()
