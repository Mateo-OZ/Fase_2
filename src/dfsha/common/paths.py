"""Normalización y autorización de rutas (RF1: cada usuario solo en /home/<usuario>)."""
from __future__ import annotations

import posixpath
import re

_USER_RE = re.compile(r"^[a-z][a-z0-9_.-]{1,31}$")


class PathError(Exception):
    def __init__(self, status: int, code: str, message: str):
        super().__init__(message)
        self.status, self.code, self.message = status, code, message


def valid_username(name: str) -> bool:
    return bool(_USER_RE.match(name or ""))


def normalize(path: str) -> str:
    if not path or not path.startswith("/"):
        raise PathError(400, "INVALID_PATH", f"La ruta debe ser absoluta: {path!r}")
    if "\x00" in path:
        raise PathError(400, "INVALID_PATH", "Ruta con caracteres inválidos")
    norm = posixpath.normpath(path)
    if norm.startswith("//"):
        norm = "/" + norm.lstrip("/")
    return norm


def home_of(user: str) -> str:
    return f"/home/{user}"


def authorize(user: str, path: str) -> str:
    """Devuelve la ruta normalizada o lanza 403 si está fuera de /home/<user>."""
    norm = normalize(path)
    home = home_of(user)
    if norm != home and not norm.startswith(home + "/"):
        raise PathError(403, "FORBIDDEN", f"{user} no puede acceder a {norm}")
    return norm


def parent(path: str) -> str:
    return posixpath.dirname(path) or "/"


def name(path: str) -> str:
    return posixpath.basename(path)
