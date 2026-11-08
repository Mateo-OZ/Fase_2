"""Registro de auditoría (sección 10): inicios de sesión, accesos denegados y commits.

Escribe una línea JSON por evento en stdout (visible con `docker logs`) y, si
DFSHA_AUDIT_FILE está definida, también en ese archivo.
"""
from __future__ import annotations

import json
import logging
import sys
import threading
import time

from .config import env

_lock = threading.Lock()
_file = env("DFSHA_AUDIT_FILE", "")


def setup_logging(component: str) -> logging.Logger:
    level = env("DFSHA_LOG_LEVEL", "INFO")
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format=f"%(asctime)s {component} %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
    )
    logging.getLogger("pika").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    return logging.getLogger(component)


def audit(event: str, **fields) -> None:
    record = {"ts": round(time.time(), 3), "audit": event, **fields}
    line = json.dumps(record, ensure_ascii=False, default=str)
    with _lock:
        print(line, flush=True)
        if _file:
            try:
                with open(_file, "a", encoding="utf-8") as fh:
                    fh.write(line + "\n")
            except OSError:
                pass
