"""Herramienta del administrador para crear cuentas (actor "Administrador", sección 2).

Uso (dentro del contenedor auth):
    python -m dfsha.auth.admin create-user andrey
    python -m dfsha.auth.admin create-user admin --admin
    echo 'secreto' | python -m dfsha.auth.admin create-user diego --password-stdin
    python -m dfsha.auth.admin list-users
"""
from __future__ import annotations

import argparse
import getpass
import json
import sys

import bcrypt

from dfsha.common.config import env, parse_peers
from dfsha.common.metaclient import MetaClient


def _password(args) -> str:
    if args.password_stdin:
        return sys.stdin.readline().rstrip("\n")
    p1 = getpass.getpass("Contraseña: ")
    p2 = getpass.getpass("Repita la contraseña: ")
    if p1 != p2:
        raise SystemExit("Las contraseñas no coinciden")
    return p1


def main() -> None:
    ap = argparse.ArgumentParser(prog="dfsha-admin")
    sub = ap.add_subparsers(dest="cmd", required=True)
    cu = sub.add_parser("create-user")
    cu.add_argument("username")
    cu.add_argument("--admin", action="store_true")
    cu.add_argument("--password-stdin", action="store_true")
    sp = sub.add_parser("set-password")
    sp.add_argument("username")
    sp.add_argument("--password-stdin", action="store_true")
    sub.add_parser("list-users")
    args = ap.parse_args()

    meta = MetaClient(list(parse_peers(env("DFSHA_META_PEERS", required=True)).values()),
                      env("DFSHA_AUTH_ID", "auth"))
    if args.cmd == "list-users":
        status, body = meta.call("list_users")
    else:
        pw = _password(args)
        if len(pw) < 8:
            raise SystemExit("La contraseña debe tener al menos 8 caracteres")
        pw_hash = bcrypt.hashpw(pw.encode(), bcrypt.gensalt(rounds=12)).decode()
        if args.cmd == "create-user":
            status, body = meta.call("create_user", {"username": args.username, "pw_hash": pw_hash,
                                                     "role": "admin" if args.admin else "user"})
        else:
            status, body = meta.call("set_password", {"username": args.username, "pw_hash": pw_hash})
    print(json.dumps({"status": status, **body}, ensure_ascii=False, indent=2))
    sys.exit(0 if status < 400 else 1)


if __name__ == "__main__":
    main()
