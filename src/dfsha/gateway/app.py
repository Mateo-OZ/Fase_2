"""Puerta de entrada: API REST sobre HTTPS (Anexo A.1).

Sin estado: valida el JWT con la clave pública, revisa que la ruta esté
dentro de /home/<usuario del token> y reenvía la operación al líder de
metadatos por gRPC con TLS mutuo. Se despliegan 2 instancias detrás de un
balanceador (NLB interno) con el nombre dfs.internal.
"""
from __future__ import annotations

import time

import anyio.to_thread
import httpx
import jwt
from fastapi import Depends, FastAPI, Header, Request
from fastapi.responses import JSONResponse, Response

from dfsha.common.audit import audit, setup_logging
from dfsha.common.config import env, parse_list, parse_peers
from dfsha.common.metaclient import MetaClient
from dfsha.common.paths import PathError, authorize
from dfsha.common.tls import client_ssl_context

GW_ID = env("DFSHA_GATEWAY_ID", "gateway1")
log = setup_logging(GW_ID)
app = FastAPI(title="DFSha gateway", version="2.0", docs_url="/docs", redoc_url=None)

_state: dict = {}


def meta() -> MetaClient:
    if "meta" not in _state:
        _state["meta"] = MetaClient(list(parse_peers(env("DFSHA_META_PEERS", required=True)).values()), GW_ID)
    return _state["meta"]


def public_key() -> str:
    if "pub" not in _state:
        with open(env("DFSHA_JWT_PUBLIC_KEY_FILE", "/etc/dfsha/certs/jwt_public.pem")) as fh:
            _state["pub"] = fh.read()
    return _state["pub"]


def auth_client() -> httpx.Client:
    if "auth" not in _state:
        _state["auth"] = httpx.Client(verify=client_ssl_context(GW_ID), timeout=10.0)
    return _state["auth"]


@app.on_event("startup")
def _startup() -> None:
    anyio.to_thread.current_default_thread_limiter().total_tokens = 256


class Principal:
    def __init__(self, user: str, role: str):
        self.user, self.role = user, role


def err(status: int, code: str, message: str, **extra) -> JSONResponse:
    return JSONResponse(status_code=status, content={"error": code, "message": message, **extra})


def principal(request: Request, authorization: str | None = Header(default=None)) -> Principal:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise _HTTPError(401, "TOKEN_EXPIRED", "Falta el token. Inicie sesión con: dfs login")
    token = authorization.split(" ", 1)[1]
    try:
        claims = jwt.decode(token, public_key(), algorithms=["RS256"], issuer="dfsha-auth",
                            options={"require": ["exp", "sub"]})
    except jwt.ExpiredSignatureError:
        raise _HTTPError(401, "TOKEN_EXPIRED", "El token venció. Inicie sesión de nuevo")
    except jwt.InvalidTokenError as exc:
        audit("invalid_token", gateway=GW_ID, client=request.client.host if request.client else None,
              reason=str(exc))
        raise _HTTPError(401, "TOKEN_EXPIRED", f"Token inválido: {exc}")
    return Principal(claims["sub"], claims.get("role", "user"))


class _HTTPError(Exception):
    def __init__(self, status: int, code: str, message: str):
        self.status, self.code, self.message = status, code, message


@app.exception_handler(_HTTPError)
def _http_error(request: Request, exc: _HTTPError):
    return err(exc.status, exc.code, exc.message)


@app.exception_handler(PathError)
def _path_error(request: Request, exc: PathError):
    return err(exc.status, exc.code, exc.message)


def forward(p: Principal, op: str, payload: dict, wait: float = 0.0, check_paths: tuple[str, ...] = ("path",)):
    for key in check_paths:
        if key in payload and payload[key] is not None:
            try:
                payload[key] = authorize(p.user, payload[key])
            except PathError as e:
                audit("access_denied", gateway=GW_ID, user=p.user, op=op, path=payload[key])
                raise e
    status, body = meta().call(op, payload, user=p.user, role=p.role, wait=wait)
    if status == 204:
        return Response(status_code=204)
    return JSONResponse(status_code=status, content=body)


# ------------------------------------------------------------------ auth
@app.post("/v1/auth/login")
def login(body: dict, request: Request):
    urls = parse_list(env("DFSHA_AUTH_URLS", "https://127.0.0.1:8500"))
    last = None
    for url in urls:
        try:
            r = auth_client().post(f"{url}/internal/login", json={
                "username": str(body.get("username", "")), "password": str(body.get("password", "")),
                "client_ip": request.client.host if request.client else None})
            return JSONResponse(status_code=r.status_code, content=r.json())
        except httpx.HTTPError as exc:
            last = exc
    return err(503, "AUTH_UNAVAILABLE", f"El servicio de autenticación no responde: {last}")


@app.get("/v1/auth/whoami")
def whoami(p: Principal = Depends(principal)):
    return {"username": p.user, "role": p.role, "home": f"/home/{p.user}", "gateway": GW_ID}


# ------------------------------------------------------------------- RF1
@app.get("/v1/fs/ls")
def ls(path: str, p: Principal = Depends(principal)):
    return forward(p, "ls", {"path": path})


@app.get("/v1/fs/stat")
def stat(path: str, p: Principal = Depends(principal)):
    return forward(p, "stat", {"path": path})


@app.post("/v1/fs/mkdir")
def mkdir(body: dict, p: Principal = Depends(principal)):
    return forward(p, "mkdir", {"path": body.get("path"), "parents": bool(body.get("parents"))})


@app.delete("/v1/fs/rmdir")
def rmdir(path: str, p: Principal = Depends(principal)):
    return forward(p, "rmdir", {"path": path})


@app.delete("/v1/fs/files")
def rm(path: str, p: Principal = Depends(principal)):
    return forward(p, "rm", {"path": path})


@app.post("/v1/fs/mv")
def mv(body: dict, p: Principal = Depends(principal)):
    return forward(p, "mv", {"src": body.get("src"), "dst": body.get("dst")}, check_paths=("src", "dst"))


# ------------------------------------------------------------- RF2 / RF3
@app.get("/v1/files/manifest")
def manifest(path: str, version: int | None = None, p: Principal = Depends(principal)):
    return forward(p, "manifest", {"path": path, "version": version})


@app.post("/v1/files/uploads")
def begin_upload(body: dict, p: Principal = Depends(principal)):
    return forward(p, "begin_upload", dict(body))


@app.post("/v1/files/uploads/{upload_id}/commit")
def commit(upload_id: str, body: dict, p: Principal = Depends(principal)):
    return forward(p, "commit_upload", dict(body, upload_id=upload_id), check_paths=())


@app.delete("/v1/files/uploads/{upload_id}")
def abort(upload_id: str, p: Principal = Depends(principal)):
    return forward(p, "abort_upload", {"upload_id": upload_id}, check_paths=())


@app.post("/v1/files/uploads/{upload_id}/blocks/{index}/replace")
def replace_nodes(upload_id: str, index: int, body: dict, p: Principal = Depends(principal)):
    return forward(p, "realloc_block", {"upload_id": upload_id, "index": index,
                                        "exclude": body.get("exclude", [])}, check_paths=())


@app.post("/v1/files/replica-report")
def replica_report(body: dict, p: Principal = Depends(principal)):
    return forward(p, "report_replica", dict(body), check_paths=())


# ----------------------------------------------------------------- locks
@app.post("/v1/locks")
def lock(body: dict, p: Principal = Depends(principal)):
    wait = float(body.get("wait", 0) or 0)
    return forward(p, "lock_acquire", dict(body), wait=wait)


@app.put("/v1/locks/{lock_id}")
def renew(lock_id: str, p: Principal = Depends(principal)):
    return forward(p, "lock_renew", {"lock_id": lock_id}, check_paths=())


@app.delete("/v1/locks/{lock_id}")
def unlock(lock_id: str, p: Principal = Depends(principal)):
    return forward(p, "lock_release", {"lock_id": lock_id}, check_paths=())


# ----------------------------------------------------------------- admin
@app.get("/v1/admin/cluster")
def cluster(p: Principal = Depends(principal)):
    if p.role != "admin":
        audit("access_denied", gateway=GW_ID, user=p.user, op="cluster_status")
        return err(403, "FORBIDDEN", "Solo el administrador puede ver el estado del clúster")
    return forward(p, "cluster_status", {}, check_paths=())


@app.get("/healthz")
def healthz():
    return {"status": "ok", "gateway": GW_ID, "time": time.time()}
