"""Tokens de bloque y recibos firmados con HMAC-SHA256 (sección 10).

* Token de bloque: lo emite metadatos. Sirve solo para un block_id, una
  operación (r, w, rep, d) y unos minutos. Los nodos de datos lo validan sin
  consultar a metadatos. Lleva la clave del archivo envuelta con la clave
  maestra (el cliente no puede abrirla) y, para escritura, la cadena de
  réplicas.
* Recibo: lo firma el nodo de datos cuando guardó y verificó un bloque. El
  commit necesita al menos 2 recibos válidos (sección 9.3).
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time

from .config import env

_SECRET_RAW = env("DFSHA_BLOCK_TOKEN_SECRET", "")


def _key(purpose: str) -> bytes:
    if not _SECRET_RAW:
        raise RuntimeError("DFSHA_BLOCK_TOKEN_SECRET no está configurada")
    return hmac.new(_SECRET_RAW.encode(), purpose.encode(), hashlib.sha256).digest()


def _b64e(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def _b64d(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


class TokenError(Exception):
    pass


def sign_block_token(claims: dict) -> str:
    body = _b64e(json.dumps(claims, separators=(",", ":"), sort_keys=True).encode())
    sig = _b64e(hmac.new(_key("block-token"), body.encode(), hashlib.sha256).digest())
    return f"{body}.{sig}"


def verify_block_token(token: str, block_id: str, ops: tuple[str, ...]) -> dict:
    try:
        body, sig = token.split(".", 1)
    except ValueError as exc:
        raise TokenError("token mal formado") from exc
    expected = _b64e(hmac.new(_key("block-token"), body.encode(), hashlib.sha256).digest())
    if not hmac.compare_digest(sig, expected):
        raise TokenError("firma inválida")
    claims = json.loads(_b64d(body))
    if claims.get("b") != block_id:
        raise TokenError("el token es de otro bloque")
    if claims.get("op") not in ops:
        raise TokenError(f"operación {claims.get('op')} no permitida aquí")
    if float(claims.get("exp", 0)) < time.time():
        raise TokenError("token vencido")
    return claims


def receipt_signature(dn_id: str, block_id: str, sha256: str, size: int) -> str:
    msg = f"{dn_id}|{block_id}|{sha256}|{size}".encode()
    return hmac.new(_key("receipt"), msg, hashlib.sha256).hexdigest()


def verify_receipt(r: dict) -> bool:
    try:
        expected = receipt_signature(r["dn_id"], r["block_id"], r["sha256"], int(r["size"]))
    except (KeyError, ValueError, TypeError):
        return False
    return hmac.compare_digest(expected, str(r.get("sig", "")))
