"""Servicio gRPC del nodo de datos (secciones 3, 5, 7, 8 y Anexo A).

* WriteBlock: recibe un bloque en trozos de 1 MiB, lo cifra y lo guarda, y a
  la vez lo reenvía al siguiente nodo de la cadena (replicación en cadena).
  Responde con los recibos firmados de los nodos que lo guardaron.
* ReadBlock: devuelve el bloque (o un rango) descifrado y verifica su hash.
* ReplicateBlock: copia un bloque desde otra réplica (lo usan los trabajadores).
* DeleteBlock: borra un bloque (limpieza).

Puerto 7001 (clientes, TLS): WriteBlock y ReadBlock.
Puerto 7002 (servidores, TLS mutuo): las cuatro operaciones.
"""
from __future__ import annotations

import logging
import os
import queue
import threading
import time

import grpc

from dfsha.common import config
from dfsha.common.audit import audit
from dfsha.common.crypto import unwrap_dek
from dfsha.common.tls import grpc_channel
from dfsha.common.tokens import TokenError, receipt_signature, verify_block_token
from dfsha.proto import dfsha_pb2 as pb
from dfsha.proto import dfsha_pb2_grpc as pbg

from .store import BlockStore, IntegrityError

log = logging.getLogger("datanode")


class Forwarder:
    """Reenvía los trozos al siguiente nodo de la cadena mientras llegan."""

    def __init__(self, stub: pbg.DataNodeStub, header: pb.BlockHeader, next_dn: str, n_chunks: int):
        self.next_dn = next_dn
        self.q: queue.Queue = queue.Queue(maxsize=4)
        self.failed = False
        self.reason = ""
        timeout = config.CHUNK_TIMEOUT * (n_chunks + 4)
        self.future = stub.WriteBlock.future(self._gen(header), timeout=timeout)

    def _gen(self, header):
        yield pb.WriteChunk(header=header)
        while True:
            try:
                # La espera aquí depende de quien envía (este nodo o los anteriores), no del
                # siguiente nodo: no se le culpa. El nodo anterior corta la cadena si este se atrasa.
                item = self.q.get(timeout=config.CHUNK_TIMEOUT * 6)
            except queue.Empty:
                fut = getattr(self, "future", None)
                if fut is not None:
                    fut.cancel()
                return
            if item is None:
                return
            yield pb.WriteChunk(data=item)

    def put(self, data: bytes) -> None:
        if self.failed or self.future.done():
            return
        try:
            self.q.put(data, timeout=config.CHUNK_TIMEOUT)
        except queue.Full:
            self.failed, self.reason = True, "el siguiente nodo no recibe (más de 5 s por trozo)"
            self.future.cancel()

    def finish(self) -> tuple[list[pb.Receipt], list[str], str]:
        if not self.failed:
            try:
                self.q.put(None, timeout=config.CHUNK_TIMEOUT)
            except queue.Full:
                self.failed, self.reason = True, "el siguiente nodo no recibe"
        if self.failed:
            self.future.cancel()
            return [], [self.next_dn], self.reason
        try:
            reply = self.future.result(timeout=config.CHUNK_TIMEOUT * 3)
            return list(reply.receipts), list(reply.failed), reply.detail
        except grpc.FutureTimeoutError:
            self.future.cancel()
            return [], [self.next_dn], "el siguiente nodo no confirmó a tiempo"
        except grpc.RpcError as exc:
            return [], [self.next_dn], f"{exc.code().name}: {exc.details()}"
        except grpc.FutureCancelledError:
            return [], [self.next_dn], "cancelado"


class DataNodeService(pbg.DataNodeServicer):
    def __init__(self, dn_id: str, store: BlockStore, internal: bool, shared: "SharedState"):
        self.dn_id = dn_id
        self.store = store
        self.internal = internal
        self.shared = shared

    # ------------------------------------------------------------ helpers
    def _check_token(self, context, token: str, block_id: str, ops: tuple[str, ...]) -> dict:
        try:
            return verify_block_token(token, block_id, ops)
        except TokenError as exc:
            audit("block_token_denied", dn_id=self.dn_id, block_id=block_id, reason=str(exc), peer=context.peer())
            context.abort(grpc.StatusCode.PERMISSION_DENIED, f"token de bloque rechazado: {exc}")

    def _dek(self, claims: dict) -> bytes | None:
        if not claims.get("k"):
            return None
        return unwrap_dek(claims["k"], claims["f"])

    def _receipt(self, block_id: str, sha: str, size: int) -> pb.Receipt:
        return pb.Receipt(dn_id=self.dn_id, block_id=block_id, sha256=sha, size=size,
                          sig=receipt_signature(self.dn_id, block_id, sha, size))

    # --------------------------------------------------------- WriteBlock
    def WriteBlock(self, request_iterator, context):
        first = next(request_iterator, None)
        if first is None or not first.HasField("header"):
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "el primer mensaje debe ser el encabezado")
        h = first.header
        claims = self._check_token(context, h.token, h.block_id, ("w",))
        chain = claims.get("p", [])
        ids = [c[0] for c in chain]
        if self.dn_id not in ids:
            context.abort(grpc.StatusCode.PERMISSION_DENIED, f"{self.dn_id} no está en la cadena del bloque")
        pos = ids.index(self.dn_id)
        if pos > 0 and not self.internal:
            context.abort(grpc.StatusCode.PERMISSION_DENIED, "los saltos de la cadena van por el puerto interno")
        downstream = chain[pos + 1:]
        dek = self._dek(claims)
        fault_after = self.shared.take_fault("crash_after_bytes")
        delay_ms = self.shared.peek_fault("delay_ms")

        writer = self.store.writer(h.block_id, dek)
        n_chunks = max(1, -(-h.size // config.CHUNK_SIZE))
        fwd = None
        if downstream:
            nxt_id, nxt_addr = downstream[0]
            fwd = Forwarder(self.shared.internal_stub(nxt_addr), h, nxt_id, n_chunks)
        self.shared.active += 1
        received = 0
        try:
            for msg in request_iterator:
                data = msg.data
                writer.write(data)
                received += len(data)
                if fwd:
                    fwd.put(data)
                if delay_ms:
                    time.sleep(delay_ms / 1000.0)
                if fault_after and received >= fault_after:
                    log.error("FALLA INYECTADA: el nodo se apaga tras recibir %d bytes del bloque %s",
                              received, h.block_id)
                    os._exit(17)
            try:
                sha, size = writer.commit(h.sha256, h.size)
            except IntegrityError as exc:
                if fwd:
                    fwd.future.cancel()
                context.abort(grpc.StatusCode.DATA_LOSS, str(exc))
            receipts = [self._receipt(h.block_id, sha, size)]
            failed: list[str] = []
            detail = ""
            if fwd:
                r, f, detail = fwd.finish()
                receipts += r
                failed += f
                if f:
                    log.warning("cadena del bloque %s: falló %s (%s)", h.block_id[:12], f, detail)
            return pb.WriteBlockReply(receipts=receipts, failed=failed, detail=detail)
        except Exception:
            writer.abort()
            raise
        finally:
            self.shared.active -= 1

    # ---------------------------------------------------------- ReadBlock
    def ReadBlock(self, request, context):
        claims = self._check_token(context, request.token, request.block_id, ("r", "rep"))
        if claims["op"] == "rep" and not self.internal:
            context.abort(grpc.StatusCode.PERMISSION_DENIED, "copia de mantenimiento solo por el puerto interno")
        if not self.store.has(request.block_id):
            context.abort(grpc.StatusCode.NOT_FOUND, f"{self.dn_id} no tiene el bloque {request.block_id}")
        dek = self._dek(claims)
        self.shared.active += 1
        try:
            buf = bytearray()
            for piece in self.store.read(request.block_id, dek, request.offset, request.length):
                buf += piece
                while len(buf) >= config.CHUNK_SIZE:
                    yield pb.DataChunk(data=bytes(buf[:config.CHUNK_SIZE]))
                    del buf[:config.CHUNK_SIZE]
            if buf:
                yield pb.DataChunk(data=bytes(buf))
        except IntegrityError as exc:
            self.shared.report_corrupt(request.block_id, str(exc))
            context.abort(grpc.StatusCode.DATA_LOSS, str(exc))
        except Exception as exc:  # p. ej. etiqueta GCM inválida
            if isinstance(exc, grpc.RpcError):
                raise
            self.shared.report_corrupt(request.block_id, repr(exc))
            context.abort(grpc.StatusCode.DATA_LOSS, f"no se pudo descifrar el bloque: {exc!r}")
        finally:
            self.shared.active -= 1

    # ----------------------------------------------------- ReplicateBlock
    def ReplicateBlock(self, request, context):
        if not self.internal:
            context.abort(grpc.StatusCode.PERMISSION_DENIED, "solo por el puerto interno")
        claims = self._check_token(context, request.token, request.block_id, ("rep",))
        if self.dn_id not in [c[0] for c in claims.get("p", [])]:
            context.abort(grpc.StatusCode.PERMISSION_DENIED, "este nodo no es el destino de la copia")
        if not self.shared.maint.acquire(timeout=10):
            context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, "el nodo ya atiende 2 copias de mantenimiento")
        try:
            meta = self.store.meta(request.block_id)
            if meta and meta["sha256"] == request.sha256:
                return self._receipt(request.block_id, meta["sha256"], meta["size"])
            dek = self._dek(claims)
            src = self.shared.internal_stub(request.source_addr)
            writer = self.store.writer(request.block_id, dek)
            try:
                for chunk in src.ReadBlock(pb.ReadBlockRequest(block_id=request.block_id, token=request.token),
                                           timeout=config.CHUNK_TIMEOUT * 20):
                    writer.write(chunk.data)
                sha, size = writer.commit(request.sha256, request.size)
            except grpc.RpcError as exc:
                writer.abort()
                context.abort(grpc.StatusCode.UNAVAILABLE, f"no se pudo leer la fuente: {exc.code().name}")
            except IntegrityError as exc:
                context.abort(grpc.StatusCode.DATA_LOSS, str(exc))
            audit("block_replicated", dn_id=self.dn_id, block_id=request.block_id, source=request.source_addr)
            return self._receipt(request.block_id, sha, size)
        finally:
            self.shared.maint.release()

    # -------------------------------------------------------- DeleteBlock
    def DeleteBlock(self, request, context):
        if not self.internal:
            context.abort(grpc.StatusCode.PERMISSION_DENIED, "solo por el puerto interno")
        self._check_token(context, request.token, request.block_id, ("d",))
        existed = self.store.delete(request.block_id)
        return pb.StatusReply(ok=True, detail="borrado" if existed else "no existía")


class SharedState:
    """Estado compartido entre los dos puertos del nodo."""

    def __init__(self, dn_id: str, root: str, report_fn):
        self.dn_id = dn_id
        self.root = root
        self.active = 0
        self.maint = threading.Semaphore(config.MAINT_COPIES_PER_NODE)
        self._stubs: dict[str, pbg.DataNodeStub] = {}
        self._lock = threading.Lock()
        self.report_fn = report_fn

    def internal_stub(self, addr: str) -> pbg.DataNodeStub:
        with self._lock:
            s = self._stubs.get(addr)
            if s is None:
                s = pbg.DataNodeStub(grpc_channel(addr, self.dn_id))
                self._stubs[addr] = s
            return s

    # Inyección de fallas para las pruebas: archivos en el directorio de datos
    def _fault_path(self, name: str) -> str:
        return os.path.join(self.root, f"fault_{name}")

    def take_fault(self, name: str) -> int:
        p = self._fault_path(name)
        try:
            with open(p) as fh:
                value = int(fh.read().strip() or 0)
            os.unlink(p)
            return value
        except (FileNotFoundError, ValueError):
            return 0

    def peek_fault(self, name: str) -> int:
        try:
            with open(self._fault_path(name)) as fh:
                return int(fh.read().strip() or 0)
        except (FileNotFoundError, ValueError):
            return 0

    def report_corrupt(self, block_id: str, reason: str) -> None:
        threading.Thread(target=self.report_fn, args=(block_id, reason), daemon=True).start()
