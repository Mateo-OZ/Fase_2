"""Arranque de una réplica del servicio de metadatos.

Variables de entorno principales:
  DFSHA_META_ID       meta1 | meta2 | meta3 (también es el nombre del certificado)
  DFSHA_META_PEERS    meta1=10.20.2.11:7100,meta2=10.20.2.12:7100,meta3=10.20.2.13:7100
  DFSHA_META_BIND     0.0.0.0:7100
  DFSHA_META_DATA     /var/lib/dfsha/meta
"""
from __future__ import annotations

import signal
import threading
from concurrent import futures

import grpc

from dfsha.common.audit import setup_logging
from dfsha.common.config import env, env_bool, env_int, parse_peers
from dfsha.common.tls import SERVER_OPTIONS, add_grpc_port, grpc_channel
from dfsha.proto import dfsha_pb2_grpc as pbg

from .service import MetadataService
from .storage import RaftStorage


def main() -> None:
    node_id = env("DFSHA_META_ID", required=True)
    log = setup_logging(node_id)
    peers = parse_peers(env("DFSHA_META_PEERS", required=True))
    if node_id not in peers:
        raise SystemExit(f"{node_id} no aparece en DFSHA_META_PEERS")
    bind = env("DFSHA_META_BIND", "0.0.0.0:7100")
    storage = RaftStorage(env("DFSHA_META_DATA", "/var/lib/dfsha/meta"))
    service = MetadataService(
        node_id, peers, storage,
        channel_factory=lambda addr: grpc_channel(addr, node_id),
        cert_name=node_id,
        enable_mq=env_bool("DFSHA_MQ_ENABLED", True),
    )
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=env_int("DFSHA_META_THREADS", 64)),
                         options=SERVER_OPTIONS)
    pbg.add_MetadataServicer_to_server(service, server)
    add_grpc_port(server, bind, node_id, mutual=True)
    server.start()
    service.start()
    log.info("metadatos %s escuchando en %s (TLS mutuo); pares: %s", node_id, bind, peers)

    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    stop.wait()
    service.raft.stop()
    server.stop(2)


if __name__ == "__main__":
    main()
