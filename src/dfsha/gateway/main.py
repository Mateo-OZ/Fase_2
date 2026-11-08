"""Arranca la puerta de entrada con HTTPS (TLS 1.2+; TLS 1.3 negociado por defecto)."""
from __future__ import annotations

import uvicorn

from dfsha.common.config import env, env_int
from dfsha.common.tls import uvicorn_ssl_kwargs


def main() -> None:
    name = env("DFSHA_GATEWAY_ID", "gateway1")
    uvicorn.run(
        "dfsha.gateway.app:app",
        host=env("DFSHA_GATEWAY_HOST", "0.0.0.0"),
        port=env_int("DFSHA_GATEWAY_PORT", 443),
        log_level=env("DFSHA_GATEWAY_LOG", "warning"),
        **uvicorn_ssl_kwargs(name, mutual=False),
    )


if __name__ == "__main__":
    main()
