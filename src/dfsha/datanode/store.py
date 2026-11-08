"""Almacén de bloques de un nodo de datos (secciones 7 y 10).

* Cada bloque se escribe en tmp/ y se renombra a blocks/ solo cuando está
  completo y el SHA-256 coincide (renombrado atómico en Linux). Un bloque a
  medio recibir nunca existe para el nodo (escenario 9.3).
* Al arrancar se borran los temporales.
* Los bloques se guardan cifrados con AES-256-GCM (clave del archivo).
* Junto a cada bloque hay un .json con su SHA-256 (contenido original) y el
  SHA-256 del archivo cifrado, que usa el revisor periódico (scrubber).
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import threading
import uuid

from dfsha.common.crypto import FrameWriter, iter_frames


class IntegrityError(Exception):
    pass


class BlockWriter:
    def __init__(self, store: "BlockStore", block_id: str, dek: bytes | None):
        self.store = store
        self.block_id = block_id
        self.tmp = os.path.join(store.tmp_dir, f"{block_id}.{uuid.uuid4().hex[:6]}.part")
        self.fh = open(self.tmp, "wb")
        self.fw = FrameWriter(_HashingFile(self.fh), block_id, dek, store.encrypt)
        self.sha = hashlib.sha256()
        self.size = 0

    def write(self, data: bytes) -> None:
        self.sha.update(data)
        self.size += len(data)
        self.fw.write(data)

    def commit(self, expected_sha: str, expected_size: int) -> tuple[str, int]:
        digest = self.sha.hexdigest()
        if digest != expected_sha or self.size != expected_size:
            self.abort()
            raise IntegrityError(
                f"hash o tamaño no coinciden (sha {digest[:12]} vs {expected_sha[:12]}, "
                f"{self.size} vs {expected_size} bytes)")
        self.fh.flush()
        os.fsync(self.fh.fileno())
        enc_sha = self.fw.fh.digest()
        self.fh.close()
        final = self.store.path(self.block_id)
        os.makedirs(os.path.dirname(final), exist_ok=True)
        meta = {"block_id": self.block_id, "sha256": digest, "size": self.size, "enc_sha256": enc_sha}
        meta_tmp = final + ".json.tmp"
        with open(meta_tmp, "w") as fh:
            json.dump(meta, fh)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(meta_tmp, final + ".json")
        os.replace(self.tmp, final)  # el bloque existe solo desde aquí
        with self.store.lock:
            self.store.index[self.block_id] = meta
        return digest, self.size

    def abort(self) -> None:
        try:
            self.fh.close()
        except OSError:
            pass
        try:
            os.unlink(self.tmp)
        except FileNotFoundError:
            pass


class _HashingFile:
    """Envuelve un archivo y calcula el SHA-256 de lo que se escribe (contenido cifrado)."""

    def __init__(self, fh):
        self._fh = fh
        self._h = hashlib.sha256()

    def write(self, data: bytes) -> None:
        self._h.update(data)
        self._fh.write(data)

    def digest(self) -> str:
        return self._h.hexdigest()


class BlockStore:
    def __init__(self, root: str, encrypt: bool = True):
        self.root = root
        self.encrypt = encrypt
        self.blocks_dir = os.path.join(root, "blocks")
        self.tmp_dir = os.path.join(root, "tmp")
        os.makedirs(self.blocks_dir, exist_ok=True)
        shutil.rmtree(self.tmp_dir, ignore_errors=True)  # descarta bloques a medio recibir
        os.makedirs(self.tmp_dir, exist_ok=True)
        self.lock = threading.Lock()
        self.index: dict[str, dict] = {}
        self._load_index()

    def _load_index(self) -> None:
        for sub in os.listdir(self.blocks_dir):
            d = os.path.join(self.blocks_dir, sub)
            if not os.path.isdir(d):
                continue
            for fname in os.listdir(d):
                if fname.endswith(".json"):
                    bid = fname[:-5]
                    blk = os.path.join(d, bid)
                    if not os.path.exists(blk):
                        os.unlink(os.path.join(d, fname))
                        continue
                    with open(os.path.join(d, fname)) as fh:
                        self.index[bid] = json.load(fh)
                elif fname.endswith(".tmp"):
                    os.unlink(os.path.join(d, fname))

    def path(self, block_id: str) -> str:
        return os.path.join(self.blocks_dir, block_id[-2:], block_id)

    def has(self, block_id: str) -> bool:
        with self.lock:
            return block_id in self.index

    def meta(self, block_id: str) -> dict | None:
        with self.lock:
            return self.index.get(block_id)

    def writer(self, block_id: str, dek: bytes | None) -> BlockWriter:
        return BlockWriter(self, block_id, dek)

    def read(self, block_id: str, dek: bytes | None, offset: int = 0, length: int = 0):
        """Genera los bytes en claro del rango pedido. Verifica el hash si se lee completo."""
        meta = self.meta(block_id)
        if meta is None:
            raise FileNotFoundError(block_id)
        full = offset == 0 and (length == 0 or length >= meta["size"])
        end = meta["size"] if length == 0 else min(meta["size"], offset + length)
        sha = hashlib.sha256() if full else None
        pos = 0
        with open(self.path(block_id), "rb") as fh:
            for frame in iter_frames(fh, block_id, dek):
                f0, f1 = pos, pos + len(frame)
                pos = f1
                if sha:
                    sha.update(frame)
                if f1 <= offset:
                    continue
                if f0 >= end:
                    if not sha:
                        break
                    continue
                yield frame[max(0, offset - f0): min(len(frame), end - f0)]
        if sha and sha.hexdigest() != meta["sha256"]:
            raise IntegrityError(f"el bloque {block_id} está dañado (hash distinto)")

    def delete(self, block_id: str) -> bool:
        with self.lock:
            existed = self.index.pop(block_id, None) is not None
        for p in (self.path(block_id), self.path(block_id) + ".json"):
            try:
                os.unlink(p)
            except FileNotFoundError:
                pass
        return existed

    def list(self) -> list[dict]:
        with self.lock:
            return list(self.index.values())

    def verify_at_rest(self, block_id: str) -> bool:
        """Scrubber: recalcula el SHA-256 del archivo cifrado sin necesitar la clave."""
        meta = self.meta(block_id)
        if meta is None:
            return True
        h = hashlib.sha256()
        try:
            with open(self.path(block_id), "rb") as fh:
                for chunk in iter(lambda: fh.read(1024 * 1024), b""):
                    h.update(chunk)
        except FileNotFoundError:
            return False
        return h.hexdigest() == meta.get("enc_sha256")

    def usage(self) -> tuple[int, int]:
        du = shutil.disk_usage(self.root)
        return du.total, du.free
