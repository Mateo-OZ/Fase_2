"""Pruebas de la máquina de estados de metadatos (sin red)."""
import time

from dfsha.common.tokens import receipt_signature
from dfsha.metadata.state_machine import StateMachine

MiB = 1024 * 1024


def ts():
    return time.time()


def receipt(dn, bid, sha, size):
    return {"dn_id": dn, "block_id": bid, "sha256": sha, "size": size,
            "sig": receipt_signature(dn, bid, sha, size)}


def setup():
    sm = StateMachine()
    sm.apply({"op": "create_user", "username": "ana", "pw_hash": "x", "ts": ts()})
    for dn in ("dn1", "dn2", "dn3"):
        sm.apply({"op": "register_node", "dn_id": dn, "client_addr": f"{dn}:7001",
                  "internal_addr": f"{dn}:7002", "ts": ts()})
    return sm


def upload(sm, path, size, up="u1", fid="f1", mode="replace", indices=None, bs=MiB, fid_expected=None):
    n = -(-size // bs)
    indices = list(range(n)) if indices is None else indices
    allocs = [{"index": i, "block_id": f"{up}-b{i}", "pipeline": ["dn1", "dn2", "dn3"]} for i in indices]
    return sm.apply({"op": "begin_upload", "upload_id": up, "path": path, "owner": "ana", "mode": mode,
                     "size": size, "block_size": bs, "replication": 3, "base_version": 0, "file_id": fid,
                     "file_id_expected": fid_expected, "wrapped_dek": "k", "allocations": allocs, "ts": ts()})


def blocks(up, indices, sizes, nrec=2, expected=None):
    out = []
    for i, size in zip(indices, sizes):
        bid = f"{up}-b{i}"
        b = {"index": i, "block_id": bid, "sha256": "h" + bid, "size": size,
             "receipts": [receipt(dn, bid, "h" + bid, size) for dn in ("dn1", "dn2", "dn3")[:nrec]]}
        if expected is not None:
            b["expected_block_id"] = expected.get(i)
        out.append(b)
    return out


def test_commit_atomico_y_visible_solo_al_confirmar():
    sm = setup()
    assert upload(sm, "/home/ana/a.bin", 2 * MiB + 10)["status"] == 200
    assert sm.entry("/home/ana/a.bin") is None  # no aparece en ls antes del commit
    r = sm.apply({"op": "commit_upload", "upload_id": "u1", "owner": "ana", "size": 2 * MiB + 10,
                  "expected_version": 0, "blocks": blocks("u1", [0, 1, 2], [MiB, MiB, 10]), "ts": ts()})
    assert r["status"] == 200 and r["version"] == 1
    assert sm.file_by_path("/home/ana/a.bin")["blocks"] == ["u1-b0", "u1-b1", "u1-b2"]
    # idempotente
    r2 = sm.apply({"op": "commit_upload", "upload_id": "u1", "owner": "ana", "size": 2 * MiB + 10,
                   "blocks": [], "ts": ts()})
    assert r2["status"] == 200 and r2["idempotent"]


def test_commit_exige_dos_recibos():
    sm = setup()
    upload(sm, "/home/ana/a.bin", 10)
    r = sm.apply({"op": "commit_upload", "upload_id": "u1", "owner": "ana", "size": 10,
                  "blocks": blocks("u1", [0], [10], nrec=1), "ts": ts()})
    assert r["status"] == 422 and r["error"] == "INCOMPLETE_UPLOAD"
    assert sm.entry("/home/ana/a.bin") is None


def test_recibo_falso_no_cuenta():
    sm = setup()
    upload(sm, "/home/ana/a.bin", 10)
    b = blocks("u1", [0], [10])
    b[0]["receipts"][1]["sig"] = "0" * 64
    r = sm.apply({"op": "commit_upload", "upload_id": "u1", "owner": "ana", "size": 10, "blocks": b, "ts": ts()})
    assert r["status"] == 422


def _file_with_blocks(sm):
    upload(sm, "/home/ana/a.bin", 3 * MiB)
    sm.apply({"op": "commit_upload", "upload_id": "u1", "owner": "ana", "size": 3 * MiB,
              "blocks": blocks("u1", [0, 1, 2], [MiB] * 3), "ts": ts()})
    return sm.file_by_path("/home/ana/a.bin")


def test_conflicto_por_block_id_esperado():
    sm = setup()
    f = _file_with_blocks(sm)
    for up in ("u2", "u3"):
        upload(sm, "/home/ana/a.bin", 3 * MiB, up=up, fid=f["file_id"], mode="modify", indices=[1],
               fid_expected=f["file_id"])
    ok = sm.apply({"op": "commit_upload", "upload_id": "u2", "owner": "ana", "size": 3 * MiB,
                   "blocks": blocks("u2", [1], [MiB], expected={1: "u1-b1"}), "ts": ts()})
    assert ok["status"] == 200 and ok["version"] == 2
    late = sm.apply({"op": "commit_upload", "upload_id": "u3", "owner": "ana", "size": 3 * MiB,
                     "blocks": blocks("u3", [1], [MiB], expected={1: "u1-b1"}), "ts": ts()})
    assert late["status"] == 409 and late["error"] == "VERSION_CONFLICT"
    assert sm.file_by_path("/home/ana/a.bin")["blocks"] == ["u1-b0", "u2-b1", "u1-b2"]


def test_locks_y_token_de_cercado():
    sm = setup()
    f = _file_with_blocks(sm)
    now = ts()
    a = sm.apply({"op": "lock_acquire", "lock_id": "L1", "file_id": f["file_id"], "start": 1, "end": 1,
                  "mode": "exclusive", "owner": "ana", "session": "p1", "lease": 30, "ts": now})
    assert a["status"] == 200
    b = sm.apply({"op": "lock_acquire", "lock_id": "L2", "file_id": f["file_id"], "start": 1, "end": 2,
                  "mode": "exclusive", "owner": "ana", "session": "p2", "lease": 30, "ts": now + 1})
    assert b["status"] == 423
    c = sm.apply({"op": "lock_acquire", "lock_id": "L3", "file_id": f["file_id"], "start": 2, "end": 2,
                  "mode": "exclusive", "owner": "ana", "session": "p2", "lease": 30, "ts": now + 1})
    assert c["status"] == 200  # rangos que no se cruzan no compiten
    # el lease de L1 vence y otro cliente obtiene un token mayor
    d = sm.apply({"op": "lock_acquire", "lock_id": "L4", "file_id": f["file_id"], "start": 1, "end": 1,
                  "mode": "exclusive", "owner": "ana", "session": "p3", "lease": 30, "ts": now + 40})
    assert d["status"] == 200 and d["token"] > a["token"]
    upload(sm, "/home/ana/a.bin", 3 * MiB, up="u9", fid=f["file_id"], mode="modify", indices=[1],
           fid_expected=f["file_id"])
    commit = {"op": "commit_upload", "upload_id": "u9", "owner": "ana", "session": "p1", "size": 3 * MiB,
              "lock_id": "L1", "fence": a["token"], "blocks": blocks("u9", [1], [MiB])}
    r = sm.apply(dict(commit, ts=now + 41))
    assert r["status"] == 423  # el nuevo dueño aún tiene el lock
    sm.apply({"op": "lock_release", "lock_id": "L4", "owner": "ana", "ts": now + 42})
    r = sm.apply(dict(commit, ts=now + 43))
    assert r["status"] == 409 and r["error"] == "FENCED"  # token viejo rechazado


def test_rmdir_no_vacio_y_mv():
    sm = setup()
    sm.apply({"op": "mkdir", "path": "/home/ana/d", "owner": "ana", "ts": ts()})
    sm.apply({"op": "mkdir", "path": "/home/ana/d/e", "owner": "ana", "ts": ts()})
    assert sm.apply({"op": "rmdir", "path": "/home/ana/d", "ts": ts()})["error"] == "DIR_NOT_EMPTY"
    assert sm.apply({"op": "mv", "src": "/home/ana/d", "dst": "/home/ana/z", "ts": ts()})["status"] == 200
    assert sm.entry("/home/ana/z/e") is not None and sm.entry("/home/ana/d") is None


def test_snapshot_restaura_igual():
    sm = setup()
    _file_with_blocks(sm)
    other = StateMachine()
    other.restore(sm.snapshot())
    assert other.snapshot() == sm.snapshot()


def test_nodo_caido_quita_replicas():
    sm = setup()
    _file_with_blocks(sm)
    sm.apply({"op": "node_dead", "dn_id": "dn1", "ts": ts()})
    assert all("dn1" not in b["replicas"] for b in sm.s["blocks"].values())
