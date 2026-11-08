#!/usr/bin/env python3
"""Consulta cada réplica de metadatos por separado: rol, término, índice aplicado y
huella SHA-256 del estado. Si dos réplicas tienen el mismo applied_index deben tener
la misma huella (consistencia de la máquina de estados replicada).

Uso (en una máquina de control):  docker exec dfsha-meta python /app/scripts/meta-status.py
       o localmente:              DFSHA_CERT_DIR=... DFSHA_META_PEERS=... python scripts/meta-status.py gateway1
"""
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from dfsha.common.config import env, parse_peers  # noqa: E402
from dfsha.common.tls import grpc_channel  # noqa: E402
from dfsha.proto import dfsha_pb2 as pb  # noqa: E402
from dfsha.proto import dfsha_pb2_grpc as pbg  # noqa: E402

import time  # noqa: E402

cert = sys.argv[1] if len(sys.argv) > 1 else env("DFSHA_META_ID", "meta1")
peers = parse_peers(env("DFSHA_META_PEERS", required=True))
for attempt in range(10):  # se repite hasta ver a todas en el mismo índice aplicado
    rows = {}
    for name, addr in peers.items():
        try:
            r = pbg.MetadataStub(grpc_channel(addr, cert)).Call(pb.MetaRequest(op="raft_status"), timeout=3)
            rows[name] = json.loads(r.payload)
        except Exception as exc:  # noqa: BLE001
            rows[name] = {"error": exc.__class__.__name__}
    if len({st.get("applied_index") for st in rows.values() if "error" not in st}) <= 1:
        break
    time.sleep(0.2)
for name, st in rows.items():
    if "error" in st:
        print(f"{name:<6} {peers[name]:<20} NO RESPONDE ({st['error']})")
        continue
    print(f"{name:<6} {peers[name]:<20} rol={st['role']:<9} term={st['term']:<4} líder={st['leader']}  "
          f"commit={st['commit_index']:<6} aplicado={st['applied_index']:<6} estado={st['state_sha256']}")
