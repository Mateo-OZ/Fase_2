"""Arranca el servicio de autenticación con HTTPS y TLS mutuo (puerto 8500)."""
from __future__ import annotations

import uvicorn

from dfsha.common.config import env, env_int
from dfsha.common.tls import uvicorn_ssl_kwargs


def main() -> None:
    name = env("DFSHA_AUTH_ID", "auth")
    uvicorn.run(
        "dfsha.auth.app:app",
        host=env("DFSHA_AUTH_HOST", "0.0.0.0"),
        port=env_int("DFSHA_AUTH_PORT", 8500),
        log_level="warning",
        **uvicorn_ssl_kwargs(name, mutual=True),
    )


if __name__ == "__main__":
    main()
