#!/usr/bin/env python3
"""Experimentos de rendimiento de DFSha (sección 11 del diseño).

Cada medición se repite --reps veces (5 por defecto) y se reporta la mediana y
el percentil 95. Los resultados crudos quedan en un CSV.

    python tests/bench.py --experiment size          # 10 MiB, 100 MiB (y 1 GiB con --big)
    python tests/bench.py --experiment replication   # factor 1 frente a 3
    python tests/bench.py --experiment blocksize     # bloque de 1 MiB frente a 8 MiB
    python tests/bench.py --experiment concurrency   # 1, 4 y 16 clientes, archivos distintos y el mismo bloque
    python tests/bench.py --experiment latency       # read de 4 KiB, ls y commit
    python tests/bench.py --experiment all --out resultados.csv

Requiere una sesión iniciada (dfs login) o DFS_PASSWORD para reintentar el login.
"""
from __future__ import annotations

import argparse
import csv
import multiprocessing as mp
import os
import statistics
import sys
import tempfile
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from dfsha.client.api import DFSClient, DFSError, MiB  # noqa: E402

ROWS: list[dict] = []
BASE = "/home/{user}/bench"


def pct(values: list[float], p: float) -> float:
    if not values:
        return float("nan")
    s = sorted(values)
    k = max(0, min(len(s) - 1, int(round(p / 100 * (len(s) - 1)))))
    return s[k]


def record(experiment: str, condition: str, op: str, seconds: float, size: int = 0, extra: str = "") -> None:
    ROWS.append({"experiment": experiment, "condition": condition, "op": op, "seconds": round(seconds, 4),
                 "bytes": size, "mib_s": round(size / MiB / seconds, 2) if size and seconds else "",
                 "extra": extra})


def summary() -> None:
    groups: dict[tuple, list[dict]] = {}
    for r in ROWS:
        groups.setdefault((r["experiment"], r["condition"], r["op"]), []).append(r)
    print(f"\n{'experimento':<12} {'condición':<26} {'op':<10} {'n':>3} {'mediana ms':>11} {'p95 ms':>9} {'MiB/s med':>10}")
    for (exp, cond, op), rows in groups.items():
        secs = [r["seconds"] for r in rows]
        thr = [r["mib_s"] for r in rows if r["mib_s"] != ""]
        print(f"{exp:<12} {cond:<26} {op:<10} {len(rows):>3} {statistics.median(secs) * 1000:>11.1f} "
              f"{pct(secs, 95) * 1000:>9.1f} {statistics.median(thr) if thr else 0:>10.1f}")


def make_file(dirpath: str, size: int) -> str:
    path = os.path.join(dirpath, f"bench_{size}.bin")
    if not os.path.exists(path) or os.path.getsize(path) != size:
        with open(path, "wb") as fh:
            left = size
            while left:
                n = min(left, 8 * MiB)
                fh.write(os.urandom(n))
                left -= n
    return path


def put_get(c: DFSClient, exp: str, cond: str, local: str, remote: str, reps: int, tmp: str, **kw) -> None:
    size = os.path.getsize(local)
    for i in range(reps):
        r = c.put(local, remote, **kw)
        record(exp, cond, "put", r["seconds"], size)
        g = c.get(remote, os.path.join(tmp, "out.bin"))
        ok = g["sha256"] == r["sha256"]
        record(exp, cond, "get", g["seconds"], size, "hash_ok" if ok else "HASH_MISMATCH")
        print(f"  {exp} {cond} rep {i + 1}: put {r['seconds']:.2f}s get {g['seconds']:.2f}s hash {'ok' if ok else 'MAL'}",
              flush=True)
    c.rm(remote)


def exp_size(c, reps, tmp, big):
    sizes = [10 * MiB, 100 * MiB] + ([1024 * MiB] if big else [])
    for s in sizes:
        put_get(c, "size", f"{s // MiB} MiB", make_file(tmp, s), f"{BASE.format(user=c.user)}/s{s}.bin", reps, tmp)


def exp_replication(c, reps, tmp, big):
    f = make_file(tmp, 100 * MiB)
    for rf in (1, 3):
        put_get(c, "replication", f"RF={rf}", f, f"{BASE.format(user=c.user)}/rf{rf}.bin", reps, tmp, replication=rf)


def exp_blocksize(c, reps, tmp, big):
    f = make_file(tmp, 100 * MiB)
    for bs in (1, 8):
        put_get(c, "blocksize", f"bloque {bs} MiB", f, f"{BASE.format(user=c.user)}/bs{bs}.bin", reps, tmp,
                block_size=bs * MiB)


def _conc_worker(args):
    mode, idx, local, remote = args
    c = DFSClient.from_session()
    c.session_id = f"bench{idx}{os.getpid()}"
    t0 = time.perf_counter()
    try:
        if mode == "distinct":
            c.put(local, remote)
            return ("ok", time.perf_counter() - t0, os.path.getsize(local))
        f = c.open(remote, "rw", lock_wait=float(os.environ.get("BENCH_LOCK_WAIT", "60")))
        f.write(0, f"cliente-{idx:03d};".encode())
        f.close()
        return ("ok", time.perf_counter() - t0, 0)
    except DFSError as e:
        return (f"{e.status}", time.perf_counter() - t0, 0)


def exp_concurrency(c, reps, tmp, big):
    f = make_file(tmp, 16 * MiB)
    base = BASE.format(user=c.user)
    ctx = mp.get_context("spawn")
    for n in (1, 4, 16):
        for rep in range(reps):
            jobs = [("distinct", i, f, f"{base}/c{n}_{i}.bin") for i in range(n)]
            t0 = time.perf_counter()
            with ctx.Pool(n) as pool:
                res = pool.map(_conc_worker, jobs)
            wall = time.perf_counter() - t0
            total = sum(r[2] for r in res)
            for r in res:
                record("concurrency", f"{n} clientes, distintos", "put", r[1], r[2], r[0])
            record("concurrency", f"{n} clientes, distintos", "agregado", wall, total)
            print(f"  {n} clientes archivos distintos rep {rep + 1}: {total / MiB / wall:.1f} MiB/s agregados", flush=True)
        # mismo bloque: se serializa con el lock (cola FIFO)
        same = f"{base}/mismo.txt"
        small = os.path.join(tmp, "small.txt")
        with open(small, "w") as fh:
            fh.write("." * 4096)
        c.put(small, same, block_size=64 * 1024)
        for rep in range(reps):
            jobs = [("same", i, None, same) for i in range(n)]
            t0 = time.perf_counter()
            with ctx.Pool(n) as pool:
                res = pool.map(_conc_worker, jobs)
            wall = time.perf_counter() - t0
            codes = [r[0] for r in res]
            for r in res:
                record("concurrency", f"{n} clientes, mismo bloque", "write", r[1], 0, r[0])
            print(f"  {n} clientes mismo bloque rep {rep + 1}: {wall:.2f} s, resultados {sorted(set(codes))} "
                  f"(423: {codes.count('423')}, 409: {codes.count('409')})", flush=True)
        for i in range(n):
            try:
                c.rm(f"{base}/c{n}_{i}.bin")
            except DFSError:
                pass


def exp_latency(c, reps, tmp, big):
    base = BASE.format(user=c.user)
    f = make_file(tmp, 32 * MiB)
    c.put(f, f"{base}/lat.bin")
    h = c.open(f"{base}/lat.bin", "r")
    for i in range(max(reps, 20)):
        off = (i * 7919 * 4096) % (32 * MiB - 4096)
        t0 = time.perf_counter()
        h.read(off, 4096)
        record("latency", "read 4 KiB", "read", time.perf_counter() - t0, 4096)
        t0 = time.perf_counter()
        c.ls(base)
        record("latency", "ls", "ls", time.perf_counter() - t0)
        w = c.open(f"{base}/lat.bin", "rw")
        w.write(off, b"x" * 16)
        t0 = time.perf_counter()
        w.close()
        record("latency", "write 16 B + commit", "commit", time.perf_counter() - t0)


EXPERIMENTS = {"size": exp_size, "replication": exp_replication, "blocksize": exp_blocksize,
               "concurrency": exp_concurrency, "latency": exp_latency}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--experiment", default="all", choices=["all", *EXPERIMENTS])
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--out", default="resultados_bench.csv")
    ap.add_argument("--big", action="store_true", help="incluye el archivo de 1 GiB")
    ap.add_argument("--tmp", default=None)
    args = ap.parse_args()
    c = DFSClient.from_session()
    if not c.token:
        raise SystemExit("Primero inicie sesión: dfs login -u <usuario>")
    try:
        c.mkdir(BASE.format(user=c.user))
    except DFSError:
        pass
    tmp = args.tmp or tempfile.mkdtemp(prefix="dfsha-bench-")
    os.makedirs(tmp, exist_ok=True)
    names = list(EXPERIMENTS) if args.experiment == "all" else [args.experiment]
    for name in names:
        print(f"== experimento {name}", flush=True)
        EXPERIMENTS[name](c, args.reps, tmp, args.big)
    with open(args.out, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(ROWS[0].keys()))
        w.writeheader()
        w.writerows(ROWS)
    summary()
    print(f"\nResultados crudos en {args.out}")


if __name__ == "__main__":
    main()
