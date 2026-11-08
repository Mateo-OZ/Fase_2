"""Lectura de configuración desde variables de entorno.

Toda la configuración de DFSha llega por variables de entorno (archivo
/opt/dfsha/cluster.env en cada máquina EC2). Ningún componente tiene
direcciones de otros nodos escritas en el código.
"""
from __future__ import annotations

import os

MiB = 1024 * 1024


def env(name: str, default: str | None = None, required: bool = False) -> str | None:
    value = os.environ.get(name, default)
    if required and (value is None or value == ""):
        raise SystemExit(f"Falta la variable de entorno obligatoria {name}")
    return value


def env_int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


def env_float(name: str, default: float) -> float:
    return float(os.environ.get(name, default))


def env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in ("1", "true", "yes", "si", "sí", "on")


def parse_peers(raw: str | None) -> dict[str, str]:
    """"meta1=10.20.2.11:7100,meta2=10.20.2.12:7100" -> {"meta1": "10.20.2.11:7100", ...}"""
    peers: dict[str, str] = {}
    if not raw:
        return peers
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        name, _, addr = item.partition("=")
        peers[name.strip()] = addr.strip()
    return peers


def parse_list(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [x.strip() for x in raw.split(",") if x.strip()]


# Parámetros del diseño (sección 4, 6, 7 y 8). Todos se pueden ajustar por entorno.
BLOCK_SIZE = env_int("DFSHA_BLOCK_SIZE", 8 * MiB)          # 8 MiB por defecto
CHUNK_SIZE = env_int("DFSHA_CHUNK_SIZE", 1 * MiB)          # trozos de 1 MiB en el stream
REPLICATION = env_int("DFSHA_REPLICATION", 3)              # 3 réplicas por bloque
MIN_REPLICAS = env_int("DFSHA_MIN_REPLICAS", 2)            # se confirma con 2
HEARTBEAT_INTERVAL = env_float("DFSHA_HEARTBEAT_INTERVAL", 3.0)
SUSPECT_AFTER = env_float("DFSHA_SUSPECT_AFTER", 6.0)
DEAD_AFTER = env_float("DFSHA_DEAD_AFTER", 10.0)
CHUNK_TIMEOUT = env_float("DFSHA_CHUNK_TIMEOUT", 5.0)
LOCK_LEASE = env_float("DFSHA_LOCK_LEASE", 30.0)
LOCK_RENEW = env_float("DFSHA_LOCK_RENEW", 10.0)
READ_TOKEN_TTL = env_float("DFSHA_READ_TOKEN_TTL", 300.0)  # 5 minutos
WRITE_TOKEN_TTL = env_float("DFSHA_WRITE_TOKEN_TTL", 900.0)
UPLOAD_TTL = env_float("DFSHA_UPLOAD_TTL", 3600.0)         # 1 hora
HISTORY_RETENTION = env_float("DFSHA_HISTORY_RETENTION", 600.0)  # 10 minutos
JWT_TTL = env_float("DFSHA_JWT_TTL", 900.0)                # 15 minutos
MAINT_COPIES_PER_NODE = env_int("DFSHA_MAINT_COPIES_PER_NODE", 2)
SCRUB_INTERVAL = env_float("DFSHA_SCRUB_INTERVAL", 24 * 3600.0)
