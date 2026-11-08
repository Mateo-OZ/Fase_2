#!/usr/bin/env python3
"""Compara read()/write()/append/truncate de DFSha con un modelo en memoria."""
import os
import random
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from dfsha.client.api import DFSClient, MiB  # noqa: E402

c = DFSClient.from_session()
path = f"/home/{c.user}/rf3_modelo.bin"
model = bytearray(os.urandom(3 * MiB + 12345))
src = os.path.join(tempfile.mkdtemp(), "src.bin")
open(src, "wb").write(model)
c.put(src, path, block_size=MiB)
rng = random.Random(int(os.environ.get("SEED", "1")))
for step in range(int(os.environ.get("STEPS", "12"))):
    op = rng.choice(["rw", "rw", "a", "trunc", "w"]) if step > 2 else "rw"
    if op == "rw":
        f = c.open(path, "rw")
        off = rng.randrange(0, len(model) + 1000)
        data = os.urandom(rng.randrange(1, 2 * MiB))
        f.write(off, data)
        if off > len(model):
            model += b"\0" * (off - len(model))
        model[off:off + len(data)] = data
        assert f.read(off, len(data)) == data, "el escritor no ve sus cambios"
        r = f.close()
    elif op == "a":
        f = c.open(path, "a")
        data = os.urandom(rng.randrange(1, MiB))
        f.write(0, data)
        model += data
        r = f.close()
    elif op == "trunc":
        f = c.open(path, "rw")
        n = rng.randrange(0, len(model))
        f.truncate(n)
        del model[n:]
        r = f.close()
    else:
        f = c.open(path, "w")
        data = os.urandom(rng.randrange(0, 3 * MiB))
        f.write(0, data)
        model = bytearray(data)
        r = f.close()
    f = c.open(path, "r")
    assert f.size == len(model), (op, f.size, len(model))
    assert f.read(0, f.size + 10) == bytes(model), op
    o = rng.randrange(0, max(1, len(model)))
    assert f.read(o, 777) == bytes(model[o:o + 777])
    print(f"  paso {step:2} {op:5} -> versión {r['version'] if r else '-'}, {len(model)} bytes: coincide")
c.rm(path)
