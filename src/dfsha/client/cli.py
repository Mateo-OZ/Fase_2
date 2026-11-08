"""CLI `dfs` de DFSha (RF1, RF2 y RF3).

    dfs login -u andrey                    # pide la contraseña
    dfs ls / cd / pwd / mkdir / rmdir / rm / mv / stat --blocks
    dfs put video.mp4 /home/andrey/video.mp4
    dfs get /home/andrey/video.mp4 copia.mp4
    dfs cat notas.txt --offset 0 --length 100        # read()
    dfs write notas.txt --offset 10 --data "hola"   # write() parcial (rw)
    dfs append notas.txt --data "más texto"          # write() en modo a
    dfs truncate notas.txt 100
    dfs lock notas.txt --blocks 3-3 --hold 20         # lock() explícito
    dfs shell                                         # modo interactivo
    dfs admin status                                  # estado del clúster (admin)
"""
from __future__ import annotations

import argparse
import getpass
import json
import os
import shlex
import sys
import time

from .api import DFSClient, DFSError, MiB, save_session


def human(n: int | None) -> str:
    if n is None:
        return "-"
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(n) < 1024 or unit == "TiB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return str(n)


def out(args, data, text: str | None = None) -> None:
    if args.json:
        print(json.dumps(data, ensure_ascii=False, indent=2, default=str))
    elif text is not None:
        print(text)


def parse_range(r: str) -> tuple[int, int | None]:
    a, _, b = r.partition("-")
    return int(a), (None if b in ("", "fin", "end") else int(b))


def _data(args) -> bytes:
    if getattr(args, "data", None) is not None:
        return args.data.encode()
    if getattr(args, "from_file", None):
        with open(args.from_file, "rb") as fh:
            return fh.read()
    raise SystemExit("Indique --data TEXTO o --from ARCHIVO")


def run(args, c: DFSClient) -> int:
    log = (lambda m: print(m, file=sys.stderr)) if args.verbose else None
    cmd = args.cmd

    if cmd == "login":
        user = args.user or input("Usuario: ")
        if args.password_stdin:
            pw = sys.stdin.readline().rstrip("\n")
        else:
            pw = os.environ.get("DFS_PASSWORD") or getpass.getpass("Contraseña: ")
        if args.gateway:
            c.__init__(gateway=args.gateway, ca_file=c.ca_file)
        info = c.login(user, pw)
        c.save()
        out(args, info, f"Sesión iniciada como {info['username']} ({info['role']}) en {c.gateway}; "
                        f"el token vence en {info['expires_in'] // 60} min")
    elif cmd == "logout":
        save_session({"gateway": c.gateway, "ca_file": c.ca_file})
        print("Sesión cerrada")
    elif cmd == "whoami":
        info = c.request("GET", "/v1/auth/whoami")
        out(args, info, f"{info['username']} ({info['role']}) — atendido por {info['gateway']}")
    elif cmd == "pwd":
        print(c.cwd)
    elif cmd == "cd":
        c.cd(args.path or "~")
        c.save()
        print(c.cwd)
    elif cmd == "ls":
        res = c.ls(args.path or ".")
        lines = []
        for e in res["entries"]:
            if e["type"] == "dir":
                lines.append(f"d  {'-':>10}  {'':>4}  {e['name']}/")
            else:
                lines.append(f"-  {human(e['size']):>10}  v{e['version']:<3}  {e['name']}")
        out(args, res, "\n".join(lines) if lines else "(vacío)")
    elif cmd == "mkdir":
        c.mkdir(args.path, parents=args.parents)
        print(f"creado {c.abspath(args.path)}")
    elif cmd == "rmdir":
        c.rmdir(args.path)
        print(f"borrado {c.abspath(args.path)}")
    elif cmd == "rm":
        c.rm(args.path)
        print(f"borrado {c.abspath(args.path)}")
    elif cmd == "mv":
        res = c.mv(args.src, args.dst)
        print(f"{res['src']} -> {res['dst']}")
    elif cmd == "stat":
        res = c.stat(args.path)
        if args.json:
            out(args, res)
        else:
            print(f"ruta: {res['path']}\ntipo: {res['type']}")
            if res["type"] == "file":
                print(f"tamaño: {res['size']} bytes ({human(res['size'])})  versión: {res['version']}  "
                      f"bloque: {human(res['block_size'])}  réplicas: {res['replication']}")
                print(f"sha256: {res.get('sha256') or '(no registrado: hubo escrituras parciales)'}")
                if args.blocks:
                    for b in res["block_list"]:
                        dead = sorted(set(b["replicas"]) - set(b["alive"]))
                        extra = f"  (sin latido: {', '.join(dead)})" if dead else ""
                        print(f"  bloque {b['index']:>3}  {human(b['size']):>9}  {b['block_id'][:14]}  "
                              f"réplicas: {', '.join(b['replicas'])}{extra}")
                for lk in res.get("locks", []):
                    end = "fin" if lk["end"] is None else lk["end"]
                    print(f"  lock {lk['mode']} de {lk['owner']} sobre bloques {lk['start']}-{end} "
                          f"(token {lk['token']}, vence en {lk['expires_in']} s)")
    elif cmd == "put":
        remote = args.remote or os.path.basename(args.local)
        res = c.put(args.local, remote, block_size=int(args.block_size * MiB) if args.block_size else None,
                    replication=args.replication, parallel=args.parallel, overwrite=not args.no_overwrite,
                    log=log)
        mbps = res["size"] / MiB / res["seconds"] if res["seconds"] else 0
        out(args, res, f"put {res['path']}: versión {res['version']}, {human(res['size'])} en "
                       f"{res['blocks']} bloques, {res['seconds']:.2f} s ({mbps:.1f} MiB/s)\n"
                       f"sha256 {res['sha256']}")
    elif cmd == "get":
        local = args.local or os.path.basename(args.remote)
        res = c.get(args.remote, local, parallel=args.parallel, log=log, version=args.version)
        mbps = res["size"] / MiB / res["seconds"] if res["seconds"] else 0
        ok = "coincide" if res["expected_sha256"] == res["sha256"] else (
            "sin hash registrado" if not res["expected_sha256"] else "NO coincide")
        out(args, res, f"get {res['path']} (v{res['version']}) -> {local}: {human(res['size'])}, "
                       f"{res['seconds']:.2f} s ({mbps:.1f} MiB/s)\nsha256 {res['sha256']} ({ok})")
    elif cmd == "cat":
        f = c.open(args.path, "r")
        if args.version is not None:
            f.base = c.manifest(f.path, args.version)
            f.size = f.base["size"]
        length = args.length if args.length is not None else f.size - args.offset
        data = f.read(args.offset, length)
        f.close()
        if args.json:
            out(args, {"version": f.version, "offset": args.offset, "data": data.decode(errors="replace")})
        else:
            sys.stdout.buffer.write(data)
            if args.newline:
                sys.stdout.write("\n")
    elif cmd in ("write", "append"):
        mode = "a" if cmd == "append" else args.mode
        f = c.open(args.path, mode, auto_lock=not args.no_lock, lock_wait=args.wait)
        for lk in f.locks:
            print(f"lock {lk['mode']} obtenido sobre bloques {lk['start']}-"
                  f"{'fin' if lk['end'] is None else lk['end']} con token de cercado {lk['token']}",
                  file=sys.stderr)
        data = _data(args)
        before = len(f.locks)
        f.write(0 if mode == "a" else args.offset, data)
        for lk in f.locks[before:]:
            print(f"lock {lk['mode']} obtenido sobre bloques {lk['start']}-"
                  f"{'fin' if lk['end'] is None else lk['end']} con token de cercado {lk['token']}",
                  file=sys.stderr)
        if args.hold:
            print(f"esperando {args.hold} s antes de confirmar...", file=sys.stderr)
            time.sleep(args.hold)
        res = f.close()
        out(args, res, f"confirmado {res['path']}: versión {res['version']}, {res['size']} bytes"
            if res else "sin cambios")
    elif cmd == "truncate":
        f = c.open(args.path, "rw")
        f.truncate(args.size)
        res = f.close()
        out(args, res, f"{res['path']}: versión {res['version']}, {res['size']} bytes" if res else "sin cambios")
    elif cmd == "lock":
        start, end = parse_range(args.blocks)
        res = c.lock(args.path, start, end, "shared" if args.shared else "exclusive", args.wait)
        print(f"lock {res['mode']} {res['lock_id']} sobre bloques {res['start']}-"
              f"{'fin' if res['end'] is None else res['end']}, token de cercado {res['token']}")
        held = 0.0
        try:
            while held < args.hold:
                step = min(10.0, args.hold - held)
                time.sleep(step)
                held += step
                if held < args.hold:
                    c.renew(res["lock_id"])
        finally:
            c.unlock(res["lock_id"])
            print("lock liberado")
    elif cmd == "admin":
        res = c.cluster()
        if args.json:
            out(args, res)
        else:
            r = res["raft"]
            print(f"metadatos: líder={r['leader']} term={r['term']} commit={r['commit_index']}")
            print(f"archivos={res['files']} bloques={res['blocks_committed']} "
                  f"sub-replicados={res['blocks_under_replicated']} sin-réplica-viva={res.get('blocks_without_replica', 0)} basura={res['blocks_garbage']} "
                  f"cargas abiertas={res['open_uploads']} locks={res['locks']} "
                  f"tareas pendientes={res.get('tasks_pending_count', len(res['tasks_pending']))} rabbitmq={'ok' if res['mq_connected'] else 'NO'}")
            for n in res["nodes"]:
                print(f"  {n['dn_id']:<5} {n['state']:<8} {n['client_addr']:<20} latido hace "
                      f"{n['last_heartbeat_s']} s  libre {n['free_gib']} GiB  bloques {n['blocks']}  "
                      f"transferencias {n['active']}")
    elif cmd == "shell":
        shell(c, args)
    return 0


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="dfs", description="Cliente de DFSha")
    ap.add_argument("--json", action="store_true", help="salida en JSON")
    ap.add_argument("-v", "--verbose", action="store_true", help="muestra el detalle por bloque")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("login")
    p.add_argument("-u", "--user")
    p.add_argument("--gateway", help="URL de la puerta (por defecto DFS_GATEWAY o https://dfs.internal)")
    p.add_argument("--password-stdin", action="store_true")
    sub.add_parser("logout")
    sub.add_parser("whoami")
    sub.add_parser("pwd")
    p = sub.add_parser("cd")
    p.add_argument("path", nargs="?")
    p = sub.add_parser("ls")
    p.add_argument("path", nargs="?")
    p = sub.add_parser("mkdir")
    p.add_argument("-p", "--parents", action="store_true")
    p.add_argument("path")
    for name in ("rmdir", "rm"):
        sub.add_parser(name).add_argument("path")
    p = sub.add_parser("mv")
    p.add_argument("src")
    p.add_argument("dst")
    p = sub.add_parser("stat")
    p.add_argument("path")
    p.add_argument("--blocks", action="store_true", help="muestra bloques y réplicas")

    p = sub.add_parser("put", help="send(): sube un archivo completo")
    p.add_argument("local")
    p.add_argument("remote", nargs="?")
    p.add_argument("--block-size", type=float, help="tamaño de bloque en MiB (por defecto 8)")
    p.add_argument("--replication", type=int, help="factor de replicación (por defecto 3)")
    p.add_argument("--parallel", type=int, default=4)
    p.add_argument("--no-overwrite", action="store_true", help="falla si ya existe (modo create)")
    p = sub.add_parser("get", help="recv(): baja un archivo completo y verifica su hash")
    p.add_argument("remote")
    p.add_argument("local", nargs="?")
    p.add_argument("--parallel", type=int, default=4)
    p.add_argument("--version", type=int, help="versión anterior aún retenida")

    p = sub.add_parser("cat", help="read(offset, n)")
    p.add_argument("path")
    p.add_argument("--offset", type=int, default=0)
    p.add_argument("--length", type=int)
    p.add_argument("--version", type=int)
    p.add_argument("-n", "--newline", action="store_true")
    for name in ("write", "append"):
        p = sub.add_parser(name, help="write() con la semántica del modo")
        p.add_argument("path")
        if name == "write":
            p.add_argument("--offset", type=int, default=0)
            p.add_argument("--mode", choices=["rw", "w"], default="rw")
        p.add_argument("--data")
        p.add_argument("--from", dest="from_file")
        p.add_argument("--wait", type=float, default=0.0, help="segundos de espera por el lock (cola FIFO)")
        p.add_argument("--hold", type=float, default=0.0, help="segundos antes de confirmar (pruebas)")
        p.add_argument("--no-lock", action="store_true", help="no pedir lock (solo control optimista)")
    p = sub.add_parser("truncate")
    p.add_argument("path")
    p.add_argument("size", type=int)
    p = sub.add_parser("lock", help="lock() explícito sobre un rango de bloques")
    p.add_argument("path")
    p.add_argument("--blocks", default="0-", help="rango de bloques, ej. 3-3 o 0- (hasta el final)")
    p.add_argument("--shared", action="store_true")
    p.add_argument("--wait", type=float, default=0.0)
    p.add_argument("--hold", type=float, default=30.0)
    p = sub.add_parser("admin", help="estado del clúster (rol admin)")
    p.add_argument("what", nargs="?", default="status")
    sub.add_parser("shell", help="modo interactivo")
    return ap


def shell(c: DFSClient, gargs) -> None:
    print("DFSha shell. Comandos: ls cd pwd mkdir rmdir rm mv stat put get cat write append exit")
    parser = build_parser()
    while True:
        try:
            line = input(f"dfs:{c.cwd}$ ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break
        if not line:
            continue
        if line in ("exit", "quit", "salir"):
            break
        try:
            args = parser.parse_args(shlex.split(line))
            if args.cmd == "shell":
                continue
            run(args, c)
            c.save()
        except SystemExit:
            continue
        except DFSError as e:
            print(f"error {e.status} {e.code}: {e.message}")


def main(argv: list[str] | None = None) -> None:
    import signal
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)  # permite usar | head sin trazas
    args = build_parser().parse_args(argv)
    c = DFSClient.from_session()
    try:
        code = run(args, c)
    except DFSError as e:
        if args.json:
            print(json.dumps({"status": e.status, "error": e.code, "message": e.message, **e.body},
                             ensure_ascii=False, indent=2))
        else:
            print(f"error {e.status} {e.code}: {e.message}", file=sys.stderr)
        code = 2
    except FileNotFoundError as e:
        print(f"error: {e}", file=sys.stderr)
        code = 2
    sys.exit(code)


if __name__ == "__main__":
    main()
