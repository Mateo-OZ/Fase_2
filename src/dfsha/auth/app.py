"""Servicio de autenticación (sección 10).

* Las cuentas viven en el servicio de metadatos (replicadas con Raft), con la
  contraseña guardada con bcrypt. Así este servicio no tiene estado.
* El inicio de sesión devuelve un JWT firmado con RS256 que vence en 15 min.
  Las puertas de entrada lo validan con la clave pública, sin llamar aquí.
* Solo acepta conexiones HTTPS con TLS mutuo desde las puertas de entrada.
"""
from __future__ import annotations

import time
import uuid

import bcrypt
import jwt
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from dfsha.common import config
from dfsha.common.audit import audit, setup_logging
from dfsha.common.config import env, parse_peers
from dfsha.common.metaclient import MetaClient

AUTH_ID = env("DFSHA_AUTH_ID", "auth")
log = setup_logging(AUTH_ID)
app = FastAPI(title="DFSha auth", docs_url=None, redoc_url=None)

_meta: MetaClient | None = None
_private_key: str | None = None


def meta() -> MetaClient:
    global _meta
    if _meta is None:
        _meta = MetaClient(list(parse_peers(env("DFSHA_META_PEERS", required=True)).values()), AUTH_ID)
    return _meta


def private_key() -> str:
    global _private_key
    if _private_key is None:
        with open(env("DFSHA_JWT_PRIVATE_KEY_FILE", "/etc/dfsha/certs/jwt_private.pem")) as fh:
            _private_key = fh.read()
    return _private_key


class LoginIn(BaseModel):
    username: str
    password: str
    client_ip: str | None = None


# hash fijo para gastar el mismo tiempo cuando el usuario no existe
_DUMMY = bcrypt.hashpw(b"dummy-password", bcrypt.gensalt(rounds=12))


@app.post("/internal/login")
def login(body: LoginIn):
    status, user = meta().call("get_user", {"username": body.username})
    if status == 503:
        return JSONResponse(status_code=503, content={"error": "NO_QUORUM", "message": user.get("message")})
    pw_hash = user["pw_hash"].encode() if status == 200 else _DUMMY
    ok = bcrypt.checkpw(body.password.encode(), pw_hash) and status == 200
    if not ok:
        audit("login_failed", username=body.username, client_ip=body.client_ip)
        return JSONResponse(status_code=401, content={"error": "INVALID_CREDENTIALS",
                                                      "message": "Usuario o contraseña incorrectos"})
    now = int(time.time())
    claims = {"sub": body.username, "role": user.get("role", "user"), "iat": now,
              "exp": now + int(config.JWT_TTL), "jti": uuid.uuid4().hex, "iss": "dfsha-auth"}
    token = jwt.encode(claims, private_key(), algorithm="RS256")
    audit("login", username=body.username, role=claims["role"], client_ip=body.client_ip)
    return {"token": token, "token_type": "Bearer", "expires_in": int(config.JWT_TTL),
            "username": body.username, "role": claims["role"]}


@app.get("/healthz")
def healthz():
    return {"status": "ok", "service": AUTH_ID}
