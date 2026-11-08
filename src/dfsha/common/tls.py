"""TLS para todos los canales (sección 10 del diseño).

* Cliente -> puerta de entrada: HTTPS (TLS 1.3).
* Cliente -> nodo de datos (7001): gRPC sobre TLS (el cliente valida el
  certificado del nodo con la CA interna).
* Servidor <-> servidor (metadatos, nodos 7002, auth, trabajadores,
  RabbitMQ): TLS mutuo con certificados de la CA interna del proyecto.

Los certificados están en DFSHA_CERT_DIR con la convención
<nombre>.crt / <nombre>.key y la CA en ca.crt. El rol de un par se deduce
del CN de su certificado (meta1 -> meta, dn3 -> dn, gateway2 -> gateway).

DFSHA_TLS=false desactiva TLS en todo el clúster. Solo existe para el
experimento "costo del cifrado" de la sección 11 del diseño.
"""
from __future__ import annotations

import os
import re
import ssl

import grpc

from .config import env, env_bool

TLS_ENABLED = env_bool("DFSHA_TLS", True)
CERT_DIR = env("DFSHA_CERT_DIR", "/etc/dfsha/certs")

GRPC_OPTIONS = [
    ("grpc.max_send_message_length", 512 * 1024 * 1024),
    ("grpc.max_receive_message_length", 512 * 1024 * 1024),
    ("grpc.keepalive_time_ms", 20000),
    ("grpc.keepalive_timeout_ms", 5000),
    ("grpc.http2.max_pings_without_data", 0),
    ("grpc.keepalive_permit_without_calls", 1),
    # reconexión rápida a un par que vuelve (por defecto gRPC espera hasta 120 s)
    ("grpc.initial_reconnect_backoff_ms", 200),
    ("grpc.min_reconnect_backoff_ms", 200),
    ("grpc.max_reconnect_backoff_ms", 1000),
]


# Opciones de los servidores gRPC: aceptan los pings de keepalive de los clientes
SERVER_OPTIONS = [
    ("grpc.max_send_message_length", 512 * 1024 * 1024),
    ("grpc.max_receive_message_length", 512 * 1024 * 1024),
    ("grpc.keepalive_permit_without_calls", 1),
    ("grpc.http2.min_ping_interval_without_data_ms", 10000),
    ("grpc.http2.max_ping_strikes", 0),
]


def _read(path: str) -> bytes:
    with open(path, "rb") as fh:
        return fh.read()


def ca_path() -> str:
    return os.environ.get("DFSHA_CA_FILE") or os.path.join(CERT_DIR, "ca.crt")


def cert_paths(name: str) -> tuple[str, str]:
    return os.path.join(CERT_DIR, f"{name}.crt"), os.path.join(CERT_DIR, f"{name}.key")


def add_grpc_port(server: grpc.Server, bind: str, cert_name: str, mutual: bool) -> None:
    if not TLS_ENABLED:
        server.add_insecure_port(bind)
        return
    crt, key = cert_paths(cert_name)
    creds = grpc.ssl_server_credentials(
        [(_read(key), _read(crt))],
        root_certificates=_read(ca_path()) if mutual else None,
        require_client_auth=mutual,
    )
    server.add_secure_port(bind, creds)


def grpc_channel(target: str, cert_name: str | None = None) -> grpc.Channel:
    """Canal gRPC. Con cert_name se presenta certificado de cliente (TLS mutuo)."""
    if not TLS_ENABLED:
        return grpc.insecure_channel(target, options=GRPC_OPTIONS)
    key = crt = None
    if cert_name:
        crt_p, key_p = cert_paths(cert_name)
        crt, key = _read(crt_p), _read(key_p)
    creds = grpc.ssl_channel_credentials(
        root_certificates=_read(ca_path()), private_key=key, certificate_chain=crt
    )
    return grpc.secure_channel(target, creds, options=GRPC_OPTIONS)


_ROLE_RE = re.compile(r"^([a-z]+)")


def peer_cn(context: grpc.ServicerContext) -> str | None:
    if not TLS_ENABLED:
        return os.environ.get("DFSHA_INSECURE_PEER_CN", "insecure")
    auth = context.auth_context()
    names = auth.get("x509_common_name") or []
    return names[0].decode() if names else None


def peer_role(context: grpc.ServicerContext) -> str | None:
    if not TLS_ENABLED:
        return "insecure"
    cn = peer_cn(context)
    if not cn:
        return None
    m = _ROLE_RE.match(cn)
    return m.group(1) if m else None


def client_ssl_context(cert_name: str | None = None) -> ssl.SSLContext | bool:
    """Contexto TLS para httpx (servidor <-> servidor por HTTPS)."""
    if not TLS_ENABLED:
        return False
    ctx = ssl.create_default_context(cafile=ca_path())
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    if cert_name:
        crt, key = cert_paths(cert_name)
        ctx.load_cert_chain(crt, key)
    return ctx


def uvicorn_ssl_kwargs(cert_name: str, mutual: bool) -> dict:
    if not TLS_ENABLED:
        return {}
    crt, key = cert_paths(cert_name)
    kwargs = {"ssl_certfile": crt, "ssl_keyfile": key}
    if mutual:
        kwargs.update({"ssl_ca_certs": ca_path(), "ssl_cert_reqs": ssl.CERT_REQUIRED})
    return kwargs
