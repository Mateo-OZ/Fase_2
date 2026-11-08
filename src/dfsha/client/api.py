"""Librería cliente de DFSha (sección 2: "CLI dfs o librería cliente en Python").

El cliente solo conoce el nombre de la puerta de entrada (DFS_GATEWAY, por
defecto https://dfs.internal). La ubicación de los bloques la descubre en cada
operación pidiéndole el manifiesto a metadatos (escenario 9.4, RNF7).

    from dfsha.client.api import DFSClient
    c = DFSClient.from_session()
    c.put("video.mp4", "/home/andrey/video.mp4")
    with c.open("/home/andrey/notas.txt", "rw") as f:
        f.write(10, b"hola")
        print(f.read(0, 100))
"""
from __future__ import annotations

import concurrent.futures as cf
import hashlib
import json
import math
import os
import threading
import time
import uuid

import grpc
import httpx

from dfsha.proto import dfsha_pb2 as pb
from dfsha.proto import dfsha_pb2_grpc as pbg

MiB = 1024 * 1024
CHUNK = int(os.environ.get("DFS_CHUNK_SIZE", MiB))
CHUNK_TIMEOUT = float(os.environ.get("DFS_CHUNK_TIMEOUT", 5.0))
DEFAULT_BLOCK = int(os.environ.get("DFS_BLOCK_SIZE", 8 * MiB))
LOCK_RENEW = float(os.environ.get("DFS_LOCK_RENEW", 10.0))
INF = None


class ChunkTimeout(Exception):
    pass


class DFSError(Exception):
    def __init__(self, status: int, code: str, message: str, body: dict | None = None):
        super().__init__(f"{status} {code}: {message}")
        self.status, self.code, self.message, self.body = status, code, message, body or {}


def _home() -> str:
    return os.path.expanduser(os.environ.get("DFS_HOME", "~/.dfsha"))


def session_path() -> str:
    return os.environ.get("DFS_SESSION") or os.path.join(_home(), "session.json")


def load_session() -> dict:
    try:
        with open(session_path()) as fh:
            return json.load(fh)
    except FileNotFoundError:
        return {}


def save_session(data: dict) -> None:
    p = session_path()
    os.makedirs(os.path.dirname(p), exist_ok=True)
    tmp = p + ".tmp"
    with open(tmp, "w") as fh:
        json.dump(data, fh, indent=2)
    os.chmod(tmp, 0o600)
    os.replace(tmp, p)


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(4 * MiB), b""):
            h.update(chunk)
    return h.hexdigest()


class DFSClient:
    def __init__(self, gateway: str | None = None, ca_file: str | None = None, token: str | None = None,
                 user: str | None = None, cwd: str | None = None):
        self.gateway = (gateway or os.environ.get("DFS_GATEWAY") or "https://dfs.internal").rstrip("/")
        self.tls = os.environ.get("DFS_TLS", "true").lower() not in ("0", "false", "no")
        self.ca_file = ca_file or os.environ.get("DFS_CA_FILE") or os.path.join(_home(), "ca.crt")
        self.token = token
        self.user = user
        self.cwd = cwd or (f"/home/{user}" if user else "/")
        self.session_id = uuid.uuid4().hex[:12]
        verify = self.ca_file if self.tls else False
        self.http = httpx.Client(base_url=self.gateway, verify=verify, timeout=httpx.Timeout(30.0, read=180.0))
        self._stubs: dict[str, pbg.DataNodeStub] = {}
        self._stub_lock = threading.Lock()
        self._ca_bytes = None

    # ------------------------------------------------------------ sesión
    @classmethod
    def from_session(cls) -> "DFSClient":
        s = load_session()
        c = cls(gateway=s.get("gateway"), ca_file=s.get("ca_file"), token=s.get("token"),
                user=s.get("user"), cwd=s.get("cwd"))
        return c

    def save(self) -> None:
        save_session({"gateway": self.gateway, "ca_file": self.ca_file, "token": self.token,
                      "user": self.user, "cwd": self.cwd})

    def login(self, username: str, password: str) -> dict:
        r = self.http.post("/v1/auth/login", json={"username": username, "password": password})
        body = self._json(r)
        if r.status_code != 200:
            raise DFSError(r.status_code, body.get("error", "ERROR"), body.get("message", ""), body)
        self.token, self.user = body["token"], body["username"]
        if not self.cwd or not self.cwd.startswith(f"/home/{self.user}"):
            self.cwd = f"/home/{self.user}"
        return body

    # -------------------------------------------------------------- HTTP
    @staticmethod
    def _json(r: httpx.Response) -> dict:
        try:
            return r.json() if r.content else {}
        except ValueError:
            return {"error": "BAD_RESPONSE", "message": r.text[:200]}

    def request(self, method: str, url: str, *, params=None, body=None, ok=(200, 201, 202, 204)) -> dict:
        for attempt in range(2):
            headers = {"Authorization": f"Bearer {self.token}"} if self.token else {}
            r = self.http.request(method, url, params=params, json=body, headers=headers)
            data = self._json(r)
            if r.status_code == 401 and attempt == 0 and os.environ.get("DFS_PASSWORD") and self.user:
                self.login(self.user, os.environ["DFS_PASSWORD"])
                self.save()
                continue
            if r.status_code not in ok:
                raise DFSError(r.status_code, data.get("error", "ERROR"), data.get("message", r.text[:200]), data)
            return data
        raise DFSError(401, "TOKEN_EXPIRED", "Inicie sesión de nuevo")

    def abspath(self, path: str) -> str:
        if path in ("~", ""):
            return f"/home/{self.user}"
        if path.startswith("~/"):
            path = f"/home/{self.user}/" + path[2:]
        if not path.startswith("/"):
            path = self.cwd.rstrip("/") + "/" + path
        return os.path.normpath(path)

    # --------------------------------------------------------------- RF1
    def ls(self, path: str = ".") -> dict:
        return self.request("GET", "/v1/fs/ls", params={"path": self.abspath(path)})

    def stat(self, path: str) -> dict:
        return self.request("GET", "/v1/fs/stat", params={"path": self.abspath(path)})

    def mkdir(self, path: str, parents: bool = False) -> dict:
        return self.request("POST", "/v1/fs/mkdir", body={"path": self.abspath(path), "parents": parents})

    def rmdir(self, path: str) -> None:
        self.request("DELETE", "/v1/fs/rmdir", params={"path": self.abspath(path)})

    def rm(self, path: str) -> None:
        self.request("DELETE", "/v1/fs/files", params={"path": self.abspath(path)})

    def mv(self, src: str, dst: str) -> dict:
        return self.request("POST", "/v1/fs/mv", body={"src": self.abspath(src), "dst": self.abspath(dst)})

    def cd(self, path: str) -> str:
        target = self.abspath(path)
        info = self.stat(target)
        if info["type"] != "dir":
            raise DFSError(400, "NOT_A_DIRECTORY", f"{target} no es un directorio")
        self.cwd = target
        return target

    def manifest(self, path: str, version: int | None = None) -> dict:
        params = {"path": self.abspath(path)}
        if version is not None:
            params["version"] = version
        return self.request("GET", "/v1/files/manifest", params=params)

    def cluster(self) -> dict:
        return self.request("GET", "/v1/admin/cluster")

    # -------------------------------------------------------- plano de datos
    def _stub(self, addr: str) -> pbg.DataNodeStub:
        with self._stub_lock:
            s = self._stubs.get(addr)
            if s is None:
                opts = [("grpc.max_receive_message_length", 64 * MiB), ("grpc.initial_reconnect_backoff_ms", 200),
                        ("grpc.min_reconnect_backoff_ms", 200), ("grpc.max_reconnect_backoff_ms", 1000)]
                if self.tls:
                    if self._ca_bytes is None:
                        with open(self.ca_file, "rb") as fh:
                            self._ca_bytes = fh.read()
                    ch = grpc.secure_channel(addr, grpc.ssl_channel_credentials(self._ca_bytes), options=opts)
                else:
                    ch = grpc.insecure_channel(addr, options=opts)
                s = pbg.DataNodeStub(ch)
                self._stubs[addr] = s
            return s

    def _write_once(self, alloc: dict, data: bytes, sha: str) -> pb.WriteBlockReply:
        """Envía el bloque al primer nodo de la cadena con límite de 5 s por trozo (sección 8)."""
        header = pb.BlockHeader(block_id=alloc["block_id"], token=alloc["token"], sha256=sha, size=len(data))
        last = [time.monotonic()]
        sent_all = threading.Event()

        def gen():
            yield pb.WriteChunk(header=header)
            mv = memoryview(data)
            for i in range(0, len(data), CHUNK):
                last[0] = time.monotonic()
                yield pb.WriteChunk(data=bytes(mv[i:i + CHUNK]))
            last[0] = time.monotonic()
            sent_all.set()

        n = max(1, math.ceil(len(data) / CHUNK))
        fut = self._stub(alloc["pipeline"][0]).WriteBlock.future(gen(), timeout=CHUNK_TIMEOUT * (n + 6))
        while True:
            try:
                return fut.result(timeout=0.2)
            except grpc.FutureTimeoutError:
                if not sent_all.is_set() and time.monotonic() - last[0] > CHUNK_TIMEOUT:
                    fut.cancel()
                    raise ChunkTimeout(f"{alloc['nodes'][0]} no recibió un trozo en {CHUNK_TIMEOUT:.0f} s")

    def send_block(self, upload_id: str, alloc: dict, data: bytes, replication: int,
                   log=None, bad: set | None = None) -> dict:
        """Envía un bloque por la cadena; si quedan menos de 2 réplicas pide nodos de reemplazo.

        `bad` se comparte entre los bloques de una misma carga: un nodo que ya falló no se
        vuelve a intentar para los bloques siguientes.
        """
        sha = hashlib.sha256(data).hexdigest()
        need = min(2, replication)
        bad = bad if bad is not None else set()
        exclude: set[str] = set(bad)
        if set(alloc["nodes"]) & bad:
            try:
                alloc = self.request("POST", f"/v1/files/uploads/{upload_id}/blocks/{alloc['index']}/replace",
                                     body={"exclude": sorted(exclude)})
            except DFSError as e:
                if e.status != 503:
                    raise
        for attempt in range(5):
            try:
                reply = self._write_once(alloc, data, sha)
                receipts = [{"dn_id": r.dn_id, "block_id": r.block_id, "sha256": r.sha256,
                             "size": r.size, "sig": r.sig} for r in reply.receipts]
                if len(receipts) >= need:
                    return {"index": alloc["index"], "block_id": alloc["block_id"], "sha256": sha,
                            "size": len(data), "receipts": receipts}
                exclude.update(reply.failed)
                bad.update(reply.failed)
                why = f"solo {len(receipts)} réplica(s); fallaron {list(reply.failed)} ({reply.detail})"
            except grpc.RpcError as exc:
                exclude.add(alloc["nodes"][0])
                bad.add(alloc["nodes"][0])
                why = f"{alloc['nodes'][0]} {exc.code().name}"
            except (ChunkTimeout, grpc.FutureCancelledError) as exc:
                exclude.add(alloc["nodes"][0])
                bad.add(alloc["nodes"][0])
                why = str(exc) or "cancelado"
            if log:
                log(f"  bloque {alloc['index']}: {why}; pido nodos de reemplazo")
            try:
                alloc = self.request("POST", f"/v1/files/uploads/{upload_id}/blocks/{alloc['index']}/replace",
                                     body={"exclude": sorted(exclude)})
            except DFSError as e:
                if e.status != 503:
                    raise
                # demasiados excluidos: se vuelve a intentar sin la lista acumulada
                exclude = set()
                alloc = self.request("POST", f"/v1/files/uploads/{upload_id}/blocks/{alloc['index']}/replace",
                                     body={"exclude": []})
        raise DFSError(503, "NO_QUORUM", f"No se pudo guardar el bloque {alloc['index']} en 2 réplicas")

    def read_block(self, path: str, version: int, block: dict, offset: int = 0, length: int = 0,
                   manifest_cache: dict | None = None) -> bytes:
        """Lee un bloque (o un rango) probando réplica por réplica (sección 8)."""
        replicas = list(block["replicas"])
        full = offset == 0 and (length == 0 or length >= block["size"])
        last_err = "sin réplicas vivas"
        for refresh in range(2):
            for rep in replicas:
                try:
                    req = pb.ReadBlockRequest(block_id=block["block_id"], token=block["token"],
                                              offset=offset, length=length)
                    n = max(1, math.ceil((length or block["size"]) / CHUNK))
                    data = b"".join(c.data for c in self._stub(rep["addr"]).ReadBlock(
                        req, timeout=CHUNK_TIMEOUT * (n + 4)))
                except grpc.RpcError as exc:
                    last_err = f"{rep['dn_id']}: {exc.code().name}"
                    if exc.code() == grpc.StatusCode.PERMISSION_DENIED:
                        break  # token vencido: pedir el manifiesto de nuevo
                    self._report(block, rep, "data_loss" if exc.code() == grpc.StatusCode.DATA_LOSS
                                 else "unavailable")
                    continue
                if full and hashlib.sha256(data).hexdigest() != block["sha256"]:
                    last_err = f"{rep['dn_id']}: hash distinto"
                    self._report(block, rep, "hash_mismatch")
                    continue
                return data
            # pedir de nuevo el manifiesto: ubicación y tokens actualizados
            m = self.manifest(path, version)
            if manifest_cache is not None:
                manifest_cache.update(m)
            block = m["blocks"][block["index"]]
            replicas = list(block["replicas"])
        raise DFSError(503, "NO_REPLICA", f"No se pudo leer el bloque {block['index']}: {last_err}")

    def _report(self, block: dict, rep: dict, reason: str) -> None:
        try:
            self.request("POST", "/v1/files/replica-report",
                         body={"block_id": block["block_id"], "dn_id": rep["dn_id"], "reason": reason})
        except Exception:
            pass

    # --------------------------------------------------------------- RF2
    def put(self, local: str, remote: str, block_size: int | None = None, replication: int | None = None,
            parallel: int = 4, overwrite: bool = True, log=None) -> dict:
        remote = self.abspath(remote)
        size = os.path.getsize(local)
        t0 = time.perf_counter()
        body = {"path": remote, "mode": "replace" if overwrite else "create", "size": size}
        if block_size:
            body["block_size"] = block_size
        if replication:
            body["replication"] = replication
        up = self.request("POST", "/v1/files/uploads", body=body)
        bs, repl = up["block_size"], up["replication"]
        if log:
            log(f"carga {up['upload_id']}: {len(up['blocks'])} bloques de {bs // 1024} KiB, "
                f"réplicas={repl}, versión base={up['base_version']}")
        fd = os.open(local, os.O_RDONLY)
        bad: set[str] = set()

        def one(alloc):
            data = os.pread(fd, bs, alloc["index"] * bs)
            res = self.send_block(up["upload_id"], alloc, data, repl, log, bad)
            if log:
                log(f"  bloque {alloc['index']:>3} -> {', '.join(r['dn_id'] for r in res['receipts'])}")
            return res

        try:
            with cf.ThreadPoolExecutor(max_workers=parallel) as ex:
                blocks = list(ex.map(one, up["blocks"]))
        except Exception:
            try:
                self.request("DELETE", f"/v1/files/uploads/{up['upload_id']}")
            except Exception:
                pass
            raise
        finally:
            os.close(fd)
        file_sha = sha256_file(local)
        res = self.request("POST", f"/v1/files/uploads/{up['upload_id']}/commit", body={
            "expected_version": up["base_version"], "size": size, "blocks": blocks,
            "file_sha256": file_sha, "session": self.session_id})
        res.update(sha256=file_sha, seconds=time.perf_counter() - t0, blocks=len(blocks), block_size=bs)
        return res

    def get(self, remote: str, local: str, parallel: int = 4, log=None, version: int | None = None) -> dict:
        remote = self.abspath(remote)
        t0 = time.perf_counter()
        m = self.manifest(remote, version)
        tmp = local + ".dfspart"
        with open(tmp, "wb") as fh:
            fh.truncate(m["size"])
        fd = os.open(tmp, os.O_WRONLY)
        cache: dict = {}

        def one(block):
            data = self.read_block(remote, m["version"], block, manifest_cache=cache)
            os.pwrite(fd, data, block["index"] * m["block_size"])
            if log:
                log(f"  bloque {block['index']:>3} <- {block['replicas'][0]['dn_id'] if block['replicas'] else '?'}")
            return len(data)

        try:
            with cf.ThreadPoolExecutor(max_workers=parallel) as ex:
                list(ex.map(one, m["blocks"]))
        finally:
            os.close(fd)
        sha = sha256_file(tmp)
        if m.get("sha256") and sha != m["sha256"]:
            raise DFSError(500, "DATA_LOSS", f"El archivo recuperado no coincide: {sha} != {m['sha256']}")
        os.replace(tmp, local)
        return {"path": remote, "version": m["version"], "size": m["size"], "sha256": sha,
                "expected_sha256": m.get("sha256"), "seconds": time.perf_counter() - t0,
                "blocks": len(m["blocks"])}

    # --------------------------------------------------------------- RF3
    def open(self, path: str, mode: str = "r", *, auto_lock: bool = True, lock_wait: float = 0.0,
             block_size: int | None = None, replication: int | None = None) -> "DFSFile":
        return DFSFile(self, self.abspath(path), mode, auto_lock=auto_lock, lock_wait=lock_wait,
                       block_size=block_size, replication=replication)

    def lock(self, path: str, start: int = 0, end: int | None = None, mode: str = "exclusive",
             wait: float = 0.0) -> dict:
        return self.request("POST", "/v1/locks", body={
            "path": self.abspath(path), "start": start, "end": end, "mode": mode, "wait": wait,
            "session": self.session_id})

    def renew(self, lock_id: str) -> dict:
        return self.request("PUT", f"/v1/locks/{lock_id}")

    def unlock(self, lock_id: str) -> None:
        self.request("DELETE", f"/v1/locks/{lock_id}")


class DFSFile:
    """Sesión sobre un archivo con semántica close-to-open (sección 6).

    Modos: r (solo lectura), w (crear o reemplazar completo), a (anexar),
    rw (modificación parcial en cualquier posición). Los cambios se acumulan
    en el cliente y se publican de forma atómica en fsync()/close().
    """

    def __init__(self, client: DFSClient, path: str, mode: str, auto_lock: bool, lock_wait: float,
                 block_size: int | None, replication: int | None):
        if mode not in ("r", "w", "a", "rw"):
            raise ValueError("modo debe ser r, w, a o rw")
        self.c, self.path, self.mode = client, path, mode
        self.auto_lock, self.lock_wait = auto_lock, lock_wait
        self.replication = replication
        self.locks: list[dict] = []
        self.dirty: dict[int, bytearray] = {}
        self.closed = False
        self._stop = threading.Event()
        self._renewer: threading.Thread | None = None
        self.replace_all = mode == "w"
        self.base: dict | None = None
        try:
            self.base = client.manifest(path)
        except DFSError as e:
            if e.status != 404 or mode != "w":
                raise
        self.block_size = (self.base["block_size"] if self.base and mode != "w" else None) \
            or block_size or (self.base["block_size"] if self.base else DEFAULT_BLOCK)
        self.size = 0 if mode == "w" else self.base["size"]
        if auto_lock and self.base is not None:
            if mode == "w":
                self.lock(0, None, "exclusive", lock_wait)
            elif mode == "a":
                self.lock(max(0, self._nb(self.size) - 1), None, "exclusive", lock_wait)

    # ------------------------------------------------------------ util
    def _nb(self, size: int) -> int:
        return math.ceil(size / self.block_size) if size > 0 else 0

    def _exp(self, i: int) -> int:
        nb = self._nb(self.size)
        return self.block_size if i < nb - 1 else self.size - (nb - 1) * self.block_size

    @property
    def version(self) -> int:
        return self.base["version"] if self.base else 0

    def _base_blocks(self) -> list[dict]:
        if self.replace_all or not self.base:
            return []
        return self.base["blocks"]

    def _load_block(self, i: int) -> bytearray:
        if i in self.dirty:
            return self.dirty[i]
        blocks = self._base_blocks()
        if i < len(blocks):
            return bytearray(self.c.read_block(self.path, self.version, blocks[i], manifest_cache=self.base))
        return bytearray()

    # ------------------------------------------------------------ locks
    def lock(self, start: int = 0, end: int | None = None, mode: str = "exclusive", wait: float | None = None) -> dict:
        res = self.c.lock(self.path, start, end, mode, self.lock_wait if wait is None else wait)
        res["end"] = res.get("end")
        self.locks.append(res)
        if self._renewer is None:
            self._renewer = threading.Thread(target=self._renew_loop, daemon=True)
            self._renewer.start()
        # close-to-open: tras obtener el lock se relee la última versión confirmada
        if self.mode in ("rw", "a") and not self.dirty:
            self.base = self.c.manifest(self.path)
            self.size = self.base["size"]
        return res

    def unlock(self, lock_id: str) -> None:
        self.c.unlock(lock_id)
        self.locks = [lk for lk in self.locks if lk["lock_id"] != lock_id]

    def _renew_loop(self) -> None:
        while not self._stop.wait(LOCK_RENEW):
            for lk in list(self.locks):
                try:
                    self.c.renew(lk["lock_id"])
                except DFSError:
                    lk["lost"] = True

    def _covering_lock(self, lo: int, hi: int) -> dict | None:
        for lk in self.locks:
            end = lk["end"] if lk["end"] is not None else float("inf")
            if lk["mode"] == "exclusive" and lk["start"] <= lo and hi <= end:
                return lk
        return None

    # -------------------------------------------------------- read/write
    def read(self, offset: int, n: int) -> bytes:
        if self.closed:
            raise ValueError("archivo cerrado")
        end = min(self.size, offset + n)
        out = bytearray()
        pos = offset
        bs = self.block_size
        blocks = self._base_blocks()
        while pos < end:
            i = pos // bs
            in_off = pos - i * bs
            take = min(bs - in_off, end - pos)
            if i in self.dirty:
                buf = self.dirty[i]
                piece = bytes(buf[in_off:in_off + take])
                piece += b"\0" * (take - len(piece))
            elif i < len(blocks):
                piece = self.c.read_block(self.path, self.version, blocks[i], in_off, take,
                                          manifest_cache=self.base)
            else:
                piece = b"\0" * take
            out += piece
            pos += take
        return bytes(out)

    def write(self, offset: int, data: bytes) -> int:
        if self.closed:
            raise ValueError("archivo cerrado")
        if self.mode == "r":
            raise DFSError(400, "READ_ONLY", "El archivo se abrió en modo r")
        if self.mode == "a":
            offset = self.size
        if not data:
            return 0
        bs = self.block_size
        first, last = offset // bs, (offset + len(data) - 1) // bs
        if self.auto_lock and self.mode == "rw" and self.base is not None \
                and self._covering_lock(first, last) is None:
            self.lock(first, last, "exclusive")
        pos = 0
        for i in range(first, last + 1):
            buf = self._load_block(i)
            in_off = max(0, offset - i * bs)
            part = data[pos: pos + (bs - in_off)]
            if len(buf) < in_off:
                buf += b"\0" * (in_off - len(buf))
            buf[in_off:in_off + len(part)] = part
            self.dirty[i] = buf
            pos += len(part)
        self.size = max(self.size, offset + len(data))
        return len(data)

    def truncate(self, size: int) -> None:
        if self.mode == "r":
            raise DFSError(400, "READ_ONLY", "El archivo se abrió en modo r")
        nb = self._nb(size)
        if self.auto_lock and self.mode == "rw" and self.base is not None:
            lo = max(0, nb - 1)
            if self._covering_lock(lo, max(lo, self._nb(self.size) - 1)) is None:
                self.lock(lo, None, "exclusive")
        self.size = size
        for i in [i for i in self.dirty if i >= nb]:
            del self.dirty[i]

    # ------------------------------------------------------------ commit
    def _normalize(self) -> None:
        nb = self._nb(self.size)
        base = self._base_blocks()
        for i in range(nb):
            exp = self._exp(i)
            if i in self.dirty:
                buf = self.dirty[i]
            elif i < len(base) and base[i]["size"] == exp:
                continue
            elif i < len(base):
                buf = self._load_block(i)
            else:
                buf = bytearray()
            if len(buf) < exp:
                buf += b"\0" * (exp - len(buf))
            elif len(buf) > exp:
                del buf[exp:]
            self.dirty[i] = buf

    def fsync(self, log=None) -> dict | None:
        if not self.replace_all and not self.dirty and self.size == (self.base["size"] if self.base else 0):
            return None  # nada que confirmar
        self._normalize()
        mode = "replace" if self.replace_all else ("append" if self.mode == "a" else "modify")
        indices = sorted(self.dirty)
        body = {"path": self.path, "mode": mode, "size": self.size, "indices": indices}
        if self.replace_all:
            body["block_size"] = self.block_size
            if self.replication:
                body["replication"] = self.replication
        up = self.c.request("POST", "/v1/files/uploads", body=body)
        repl = up["replication"]
        allocs = {a["index"]: a for a in up["blocks"]}
        base = self._base_blocks()
        bad: set[str] = set()

        def one(i):
            res = self.c.send_block(up["upload_id"], allocs[i], bytes(self.dirty[i]), repl, log, bad)
            if not self.replace_all:
                res["expected_block_id"] = base[i]["block_id"] if i < len(base) else None
            return res

        with cf.ThreadPoolExecutor(max_workers=4) as ex:
            blocks = list(ex.map(one, indices))
        commit = {"size": self.size, "blocks": blocks, "session": self.c.session_id}
        if self.replace_all:
            commit["expected_version"] = self.version
            whole = hashlib.sha256()
            for i in indices:
                whole.update(self.dirty[i])
            commit["file_sha256"] = whole.hexdigest()
        if indices:
            lk = self._covering_lock(min(indices), max(indices))
            if lk:
                commit["lock_id"], commit["fence"] = lk["lock_id"], lk["token"]
        try:
            res = self.c.request("POST", f"/v1/files/uploads/{up['upload_id']}/commit", body=commit)
        except DFSError:
            try:
                self.c.request("DELETE", f"/v1/files/uploads/{up['upload_id']}")
            except DFSError:
                pass
            raise
        self.dirty.clear()
        self.replace_all = False
        self.base = self.c.manifest(self.path, res["version"])
        self.size = self.base["size"]
        return res

    def close(self) -> dict | None:
        if self.closed:
            return None
        try:
            return self.fsync() if self.mode != "r" else None
        finally:
            self.closed = True
            self._stop.set()
            for lk in list(self.locks):
                try:
                    self.c.unlock(lk["lock_id"])
                except DFSError:
                    pass
            self.locks.clear()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            self.close()
        else:
            self._stop.set()
            for lk in list(self.locks):
                try:
                    self.c.unlock(lk["lock_id"])
                except DFSError:
                    pass
            self.closed = True
