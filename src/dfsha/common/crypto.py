"""Cifrado en reposo: AES-256-GCM por bloque con una clave de datos por archivo.

La clave de datos (DEK) se genera en metadatos al crear el archivo y se guarda
cifrada con la clave maestra (KEK, DFSHA_MASTER_KEY). En AWS la KEK puede
venir de KMS; aquí se entrega como secreto a metadatos y nodos de datos.

Formato de un bloque en disco (escritura en streaming, trozo a trozo):

    "DFSHA1E\n" | ( len:uint32 | nonce:12 | ciphertext+tag )*

Cada trozo usa un nonce aleatorio y como dato asociado "<block_id>:<n>", de
modo que no se pueden reordenar ni mezclar trozos de bloques distintos.
"""
from __future__ import annotations

import base64
import os
import struct

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from .config import env

MAGIC_ENC = b"DFSHA1E\n"
MAGIC_PLAIN = b"DFSHA1P\n"


def _master_key() -> bytes:
    raw = env("DFSHA_MASTER_KEY", "")
    if not raw:
        raise RuntimeError("DFSHA_MASTER_KEY no está configurada")
    key = base64.b64decode(raw)
    if len(key) != 32:
        raise RuntimeError("DFSHA_MASTER_KEY debe ser de 32 bytes en base64")
    return key


def new_wrapped_dek(file_id: str) -> str:
    dek = AESGCM.generate_key(bit_length=256)
    nonce = os.urandom(12)
    ct = AESGCM(_master_key()).encrypt(nonce, dek, file_id.encode())
    return base64.b64encode(nonce + ct).decode()


def unwrap_dek(wrapped: str, file_id: str) -> bytes:
    raw = base64.b64decode(wrapped)
    return AESGCM(_master_key()).decrypt(raw[:12], raw[12:], file_id.encode())


class FrameWriter:
    """Escribe trozos cifrados (o en claro si encrypt=False) en un archivo abierto."""

    def __init__(self, fh, block_id: str, dek: bytes | None, encrypt: bool = True):
        self.fh = fh
        self.block_id = block_id
        self.encrypt = encrypt and dek is not None
        self.aes = AESGCM(dek) if self.encrypt else None
        self.n = 0
        fh.write(MAGIC_ENC if self.encrypt else MAGIC_PLAIN)

    def write(self, data: bytes) -> None:
        if self.encrypt:
            nonce = os.urandom(12)
            ct = self.aes.encrypt(nonce, data, f"{self.block_id}:{self.n}".encode())
        else:
            nonce, ct = b"\0" * 12, data
        self.fh.write(struct.pack(">I", len(ct)) + nonce + ct)
        self.n += 1


def iter_frames(fh, block_id: str, dek: bytes | None):
    """Devuelve los trozos en claro de un bloque guardado."""
    magic = fh.read(len(MAGIC_ENC))
    if magic not in (MAGIC_ENC, MAGIC_PLAIN):
        raise ValueError("formato de bloque desconocido")
    encrypted = magic == MAGIC_ENC
    aes = AESGCM(dek) if encrypted else None
    n = 0
    while True:
        head = fh.read(16)
        if not head:
            return
        if len(head) < 16:
            raise ValueError("bloque truncado")
        (length,) = struct.unpack(">I", head[:4])
        nonce = head[4:16]
        ct = fh.read(length)
        if len(ct) < length:
            raise ValueError("bloque truncado")
        if encrypted:
            yield aes.decrypt(nonce, ct, f"{block_id}:{n}".encode())
        else:
            yield ct
        n += 1
