"""Máquina de estados replicada del servicio de metadatos (secciones 4, 6 y 7).

Todo cambio entra por el log de Raft como un comando JSON y se aplica en el
mismo orden en las tres réplicas. La hora de cada comando (ts) la sella el
líder, así que vencimientos de locks, cargas y versiones son deterministas.

Entidades (tabla de la sección 4):
  tree     ruta -> {type, owner, file_id, mtime}
  children directorio -> {nombre: 1}
  files    file_id -> manifiesto {version, size, block_size, replication,
           blocks[], sha256, wrapped_dek, history[], deleted}
  blocks   block_id -> {size, sha256, replicas[], state, file_id, ...}
  nodes    dn_id -> {client_addr, internal_addr, zone, capacity, state}
  uploads  upload_id -> carga abierta (vence a la hora)
  locks    lock_id -> {file_id, start, end, mode, owner, expires, token}
  fences   file_id -> [[inicio, fin, token]] (tokens de cercado emitidos)
  tasks    task_id -> tarea de mantenimiento pendiente
"""
from __future__ import annotations

import json
import math
import threading

from dfsha.common import config
from dfsha.common.paths import name as pname
from dfsha.common.paths import parent as pparent
from dfsha.common.tokens import verify_receipt


class Err(Exception):
    def __init__(self, status: int, code: str, message: str, **extra):
        super().__init__(message)
        self.status, self.code, self.message, self.extra = status, code, message, extra


INF = 2**62


def _overlap(a0: int, a1: int, b0: int, b1: int) -> bool:
    return a0 <= b1 and b0 <= a1


def n_blocks(size: int, block_size: int) -> int:
    return max(0, math.ceil(size / block_size))


def expected_block_size(index: int, size: int, block_size: int) -> int:
    nb = n_blocks(size, block_size)
    if index < nb - 1:
        return block_size
    return size - (nb - 1) * block_size


class StateMachine:
    def __init__(self):
        self.lock = threading.RLock()
        self.s: dict = {}
        self.reset()

    # ---------------------------------------------------------- snapshot
    def reset(self) -> None:
        self.s = {
            "users": {}, "tree": {}, "children": {}, "files": {}, "blocks": {},
            "nodes": {}, "uploads": {}, "locks": {}, "fences": {}, "fence_seq": 0,
            "tasks": {}, "applied": 0,
        }
        self.s["tree"]["/"] = {"type": "dir", "owner": "root", "mtime": 0}
        self.s["children"]["/"] = {}
        self._add_entry("/home", {"type": "dir", "owner": "root", "mtime": 0})

    def snapshot(self) -> bytes:
        with self.lock:
            return json.dumps(self.s, separators=(",", ":")).encode()

    def restore(self, data: bytes) -> None:
        with self.lock:
            self.s = json.loads(data)

    # ------------------------------------------------------------- apply
    def apply(self, cmd: dict) -> dict:
        with self.lock:
            self.s["applied"] = self.s.get("applied", 0) + 1
            fn = getattr(self, "_op_" + cmd["op"], None)
            if fn is None:
                return {"status": 400, "error": "UNKNOWN_OP", "message": cmd["op"]}
            try:
                out = fn(cmd) or {}
                out.setdefault("status", 200)
                return out
            except Err as e:
                return {"status": e.status, "error": e.code, "message": e.message, **e.extra}

    # ----------------------------------------------------------- helpers
    def _add_entry(self, path: str, entry: dict) -> None:
        self.s["tree"][path] = entry
        par = pparent(path)
        self.s["children"].setdefault(par, {})[pname(path)] = 1
        if entry["type"] == "dir":
            self.s["children"].setdefault(path, {})

    def _del_entry(self, path: str) -> None:
        entry = self.s["tree"].pop(path)
        self.s["children"].get(pparent(path), {}).pop(pname(path), None)
        if entry["type"] == "dir":
            self.s["children"].pop(path, None)

    def entry(self, path: str) -> dict | None:
        return self.s["tree"].get(path)

    def file_by_path(self, path: str) -> dict | None:
        e = self.s["tree"].get(path)
        if not e or e["type"] != "file":
            return None
        return self.s["files"].get(e["file_id"])

    def _require_dir(self, path: str) -> None:
        e = self.s["tree"].get(path)
        if e is None:
            raise Err(404, "NOT_FOUND", f"No existe el directorio {path}")
        if e["type"] != "dir":
            raise Err(400, "NOT_A_DIRECTORY", f"{path} no es un directorio")

    def active_locks(self, file_id: str, ts: float) -> list[tuple[str, dict]]:
        return [
            (lid, lk) for lid, lk in self.s["locks"].items()
            if lk["file_id"] == file_id and lk["expires"] >= ts
        ]

    def max_fence(self, file_id: str, start: int, end: int) -> int:
        best = 0
        for s0, s1, tok in self.s["fences"].get(file_id, []):
            if _overlap(s0, s1, start, end):
                best = max(best, tok)
        return best

    # ------------------------------------------------------------ usuarios
    def _op_noop(self, cmd):
        return {}

    def _op_create_user(self, cmd):
        u = cmd["username"]
        if u in self.s["users"]:
            raise Err(409, "USER_EXISTS", f"El usuario {u} ya existe")
        self.s["users"][u] = {"pw_hash": cmd["pw_hash"], "role": cmd.get("role", "user"), "created": cmd["ts"]}
        home = f"/home/{u}"
        if home not in self.s["tree"]:
            self._add_entry(home, {"type": "dir", "owner": u, "mtime": cmd["ts"]})
        return {"username": u, "home": home}

    def _op_set_password(self, cmd):
        u = cmd["username"]
        if u not in self.s["users"]:
            raise Err(404, "NOT_FOUND", f"No existe el usuario {u}")
        self.s["users"][u]["pw_hash"] = cmd["pw_hash"]
        return {}

    # ---------------------------------------------------------- árbol RF1
    def _op_mkdir(self, cmd):
        path, owner, ts = cmd["path"], cmd["owner"], cmd["ts"]
        if path in self.s["tree"]:
            if cmd.get("parents") and self.s["tree"][path]["type"] == "dir":
                return {"path": path}
            raise Err(409, "ALREADY_EXISTS", f"Ya existe {path}")
        if cmd.get("parents"):
            missing = []
            p = pparent(path)
            while p not in self.s["tree"]:
                missing.append(p)
                p = pparent(p)
            self._require_dir(p)
            for m in reversed(missing):
                self._add_entry(m, {"type": "dir", "owner": owner, "mtime": ts})
        else:
            self._require_dir(pparent(path))
        self._add_entry(path, {"type": "dir", "owner": owner, "mtime": ts})
        return {"path": path}

    def _op_rmdir(self, cmd):
        path = cmd["path"]
        e = self.s["tree"].get(path)
        if e is None:
            raise Err(404, "NOT_FOUND", f"No existe {path}")
        if e["type"] != "dir":
            raise Err(400, "NOT_A_DIRECTORY", f"{path} no es un directorio")
        if path.count("/") <= 2:
            raise Err(403, "FORBIDDEN", "No se puede borrar el directorio raíz del usuario")
        if self.s["children"].get(path):
            raise Err(409, "DIR_NOT_EMPTY", f"El directorio {path} no está vacío")
        self._del_entry(path)
        return {}

    def _op_rm(self, cmd):
        path, ts = cmd["path"], cmd["ts"]
        e = self.s["tree"].get(path)
        if e is None:
            raise Err(404, "NOT_FOUND", f"No existe {path}")
        if e["type"] != "file":
            raise Err(400, "IS_A_DIRECTORY", f"{path} es un directorio, use rmdir")
        f = self.s["files"][e["file_id"]]
        if f["version"] > 0:
            f["history"].append({
                "version": f["version"], "blocks": f["blocks"], "size": f["size"],
                "sha256": f["sha256"], "superseded_at": ts,
            })
        f["deleted"] = True
        f["path"] = None
        f["blocks"] = []
        self._del_entry(path)
        # los locks del archivo dejan de tener sentido
        for lid in [lid for lid, lk in self.s["locks"].items() if lk["file_id"] == f["file_id"]]:
            self.s["locks"].pop(lid)
        return {"file_id": f["file_id"]}

    def _op_mv(self, cmd):
        src, dst, ts = cmd["src"], cmd["dst"], cmd["ts"]
        e = self.s["tree"].get(src)
        if e is None:
            raise Err(404, "NOT_FOUND", f"No existe {src}")
        if src.count("/") <= 2:
            raise Err(403, "FORBIDDEN", "No se puede mover el directorio raíz del usuario")
        d = self.s["tree"].get(dst)
        if d is not None and d["type"] == "dir":
            dst = dst.rstrip("/") + "/" + pname(src)
            d = self.s["tree"].get(dst)
        if d is not None:
            raise Err(409, "ALREADY_EXISTS", f"Ya existe {dst}")
        self._require_dir(pparent(dst))
        if e["type"] == "dir" and (dst == src or dst.startswith(src + "/")):
            raise Err(400, "INVALID_PATH", "No se puede mover un directorio dentro de sí mismo")
        moves = [src]
        if e["type"] == "dir":
            moves += sorted(p for p in self.s["tree"] if p.startswith(src + "/"))
        entries = [(p, self.s["tree"][p]) for p in moves]
        for p, _ in reversed(entries):
            self._del_entry(p)
        for p, ent in entries:
            newp = dst + p[len(src):]
            ent = dict(ent)
            ent["mtime"] = ts
            self._add_entry(newp, ent)
            if ent["type"] == "file":
                self.s["files"][ent["file_id"]]["path"] = newp
        return {"src": src, "dst": dst}

    # ------------------------------------------------------- nodos de datos
    def _op_register_node(self, cmd):
        prev = self.s["nodes"].get(cmd["dn_id"], {})
        self.s["nodes"][cmd["dn_id"]] = {
            "client_addr": cmd["client_addr"], "internal_addr": cmd["internal_addr"],
            "zone": cmd.get("zone", ""), "capacity": cmd.get("capacity", 0),
            "state": "active", "since": cmd["ts"], "registered": prev.get("registered", cmd["ts"]),
        }
        return {"previous_state": prev.get("state")}

    def _op_node_dead(self, cmd):
        dn = cmd["dn_id"]
        node = self.s["nodes"].get(dn)
        if not node or node["state"] == "dead":
            return {"affected": 0}
        node["state"] = "dead"
        node["since"] = cmd["ts"]
        affected = 0
        for b in self.s["blocks"].values():
            if dn in b["replicas"]:
                b["replicas"].remove(dn)
                affected += 1
        return {"affected": affected}

    # ------------------------------------------------------ cargas (RF2/RF3)
    def _op_begin_upload(self, cmd):
        path, owner, mode = cmd["path"], cmd["owner"], cmd["mode"]
        self._require_dir(pparent(path))
        e = self.s["tree"].get(path)
        if e is not None and e["type"] == "dir":
            raise Err(400, "IS_A_DIRECTORY", f"{path} es un directorio")
        if mode == "create" and e is not None:
            raise Err(409, "ALREADY_EXISTS", f"Ya existe {path}")
        if mode in ("append", "modify") and e is None:
            raise Err(404, "NOT_FOUND", f"No existe {path}")
        current_fid = e["file_id"] if e else None
        if cmd.get("file_id_expected") != current_fid:
            raise Err(409, "VERSION_CONFLICT", "El archivo cambió mientras se preparaba la carga")
        up = {
            "upload_id": cmd["upload_id"], "path": path, "owner": owner, "mode": mode,
            "file_id": cmd["file_id"], "new_file": current_fid is None,
            "wrapped_dek": cmd["wrapped_dek"], "size": cmd["size"],
            "block_size": cmd["block_size"], "replication": cmd["replication"],
            "base_version": cmd["base_version"], "allocations": {},
            "state": "open", "expires": cmd["ts"] + config.UPLOAD_TTL, "created": cmd["ts"],
            "result": None,
        }
        for a in cmd["allocations"]:
            up["allocations"][str(a["index"])] = {"block_id": a["block_id"], "pipeline": a["pipeline"]}
            self.s["blocks"][a["block_id"]] = {
                "size": None, "sha256": None, "replicas": [], "state": "writing",
                "file_id": cmd["file_id"], "upload_id": cmd["upload_id"],
                "replication": cmd["replication"], "candidates": list(a["pipeline"]),
                "created": cmd["ts"],
            }
        self.s["uploads"][cmd["upload_id"]] = up
        return {"upload_id": cmd["upload_id"]}

    def _op_realloc_block(self, cmd):
        up = self.s["uploads"].get(cmd["upload_id"])
        if not up or up["state"] != "open":
            raise Err(410, "UPLOAD_EXPIRED", "La carga ya no está abierta")
        alloc = up["allocations"].get(str(cmd["index"]))
        if not alloc:
            raise Err(400, "BAD_BLOCK", "Ese índice no pertenece a la carga")
        alloc["pipeline"] = cmd["pipeline"]
        b = self.s["blocks"][alloc["block_id"]]
        for dn in cmd["pipeline"]:
            if dn not in b["candidates"]:
                b["candidates"].append(dn)
        return {"block_id": alloc["block_id"], "pipeline": cmd["pipeline"]}

    def _op_abort_upload(self, cmd):
        up = self.s["uploads"].get(cmd["upload_id"])
        if not up:
            raise Err(404, "UPLOAD_NOT_FOUND", "No existe la carga")
        if up["owner"] != cmd["owner"]:
            raise Err(403, "FORBIDDEN", "La carga es de otro usuario")
        if up["state"] == "open":
            up["state"] = "aborted"
            self._release_upload_blocks(up)
        return {}

    def _release_upload_blocks(self, up: dict) -> None:
        for a in up["allocations"].values():
            b = self.s["blocks"].get(a["block_id"])
            if b and b["state"] == "writing":
                b["state"] = "garbage"

    def _op_commit_upload(self, cmd):
        ts = cmd["ts"]
        up = self.s["uploads"].get(cmd["upload_id"])
        if not up:
            raise Err(404, "UPLOAD_NOT_FOUND", "No existe la carga")
        if up["owner"] != cmd["owner"]:
            raise Err(403, "FORBIDDEN", "La carga es de otro usuario")
        if up["state"] == "committed":
            return dict(up["result"], idempotent=True)
        if up["state"] != "open":
            raise Err(410, "UPLOAD_EXPIRED", f"La carga está {up['state']}")

        path, mode, bs = up["path"], up["mode"], up["block_size"]
        size = int(cmd["size"])
        nb = n_blocks(size, bs)
        e = self.s["tree"].get(path)
        f = self.s["files"].get(e["file_id"]) if e and e["type"] == "file" else None
        if e is not None and e["type"] != "file":
            raise Err(400, "IS_A_DIRECTORY", f"{path} es un directorio")
        if f is not None and f["file_id"] != up["file_id"]:
            raise Err(409, "VERSION_CONFLICT", "El archivo fue reemplazado por otro", current_version=f["version"])
        if f is None and not up["new_file"]:
            raise Err(409, "VERSION_CONFLICT", "El archivo fue borrado mientras se escribía", current_version=0)
        if f is None:
            self._require_dir(pparent(path))
        if mode == "create" and f is not None:
            raise Err(409, "ALREADY_EXISTS", f"Ya existe {path}")
        cur_version = f["version"] if f else 0
        changes = {int(b["index"]): b for b in cmd.get("blocks", [])}

        # Locks y token de cercado (sección 6)
        # Primero el token de cercado: un cliente cuyo lease venció recibe FENCED
        changed = set(range(nb)) if mode in ("create", "replace") else {int(i) for i in changes}
        if f is not None and changed:
            lo, hi = (0, INF) if mode in ("create", "replace") else (min(changed), max(changed))
            lock_id = cmd.get("lock_id")
            for lid, lk in self.active_locks(f["file_id"], ts):
                if lid == lock_id or (lk["owner"] == cmd["owner"] and lk.get("session") == cmd.get("session")):
                    continue
                if _overlap(lk["start"], lk["end"], lo, hi):
                    raise Err(423, "LOCKED", f"{lk['owner']} tiene un lock {lk['mode']} sobre los bloques "
                              f"{lk['start']}-{'fin' if lk['end'] >= INF else lk['end']}",
                              holder=lk["owner"])
            fence = cmd.get("fence")
            if fence is not None:
                newest = self.max_fence(f["file_id"], lo, hi)
                if int(fence) < newest:
                    raise Err(409, "FENCED", f"Token de cercado {fence} vencido; el vigente es {newest}",
                              current_token=newest)

        ev = cmd.get("expected_version")
        if ev is not None and int(ev) != cur_version:
            raise Err(409, "VERSION_CONFLICT", f"La versión actual es {cur_version}, no {ev}",
                      current_version=cur_version)

        cur_blocks = list(f["blocks"]) if f else []
        if mode in ("create", "replace"):
            new_blocks = [None] * nb
            changed = set(range(nb))
            if set(changes) != changed:
                raise Err(422, "INCOMPLETE_UPLOAD", "Un reemplazo completo debe enviar todos los bloques",
                          missing=sorted(changed - set(changes)))
        else:
            new_blocks = cur_blocks[:nb] + [None] * max(0, nb - len(cur_blocks))
            changed = set(changes)
            for i, b in changes.items():
                if i >= nb:
                    raise Err(400, "BAD_BLOCK", f"El bloque {i} queda fuera del tamaño {size}")
                if "expected_block_id" in b:
                    cur = cur_blocks[i] if i < len(cur_blocks) else None
                    if b["expected_block_id"] != cur:
                        raise Err(409, "VERSION_CONFLICT",
                                  f"El bloque {i} ya cambió (versión actual {cur_version})",
                                  current_version=cur_version, index=i)
            for i in range(nb):
                if new_blocks[i] is None and i not in changes:
                    raise Err(422, "INCOMPLETE_UPLOAD", f"Falta el bloque {i}", missing=[i])

        # Validar bloques nuevos: id asignado, hash y recibos (W >= 2)
        need = min(config.MIN_REPLICAS, up["replication"])
        validated = {}
        for i in sorted(changed):
            b = changes[i]
            alloc = up["allocations"].get(str(i))
            if not alloc or alloc["block_id"] != b["block_id"]:
                raise Err(400, "BAD_BLOCK", f"El bloque {i} no fue asignado a esta carga")
            exp_size = expected_block_size(i, size, bs)
            if int(b["size"]) != exp_size:
                raise Err(400, "SIZE_MISMATCH", f"El bloque {i} mide {b['size']} y debería medir {exp_size}")
            if not b.get("sha256"):
                raise Err(422, "INCOMPLETE_UPLOAD", f"El bloque {i} no tiene hash", index=i)
            stored = []
            for r in b.get("receipts", []):
                if (r.get("block_id") == b["block_id"] and r.get("sha256") == b["sha256"]
                        and int(r.get("size", -1)) == exp_size and verify_receipt(r)
                        and r["dn_id"] in self.s["nodes"] and r["dn_id"] not in stored):
                    stored.append(r["dn_id"])
            if len(stored) < need:
                raise Err(422, "INCOMPLETE_UPLOAD",
                          f"El bloque {i} tiene {len(stored)} réplicas confirmadas; se necesitan {need}",
                          index=i, stored_on=stored)
            validated[i] = (b, stored)
        # Bloques que no cambian: deben tener el tamaño correcto
        for i in range(nb):
            if i in changed:
                continue
            blk = self.s["blocks"].get(new_blocks[i])
            if blk is None or blk["size"] != expected_block_size(i, size, bs):
                raise Err(400, "SIZE_MISMATCH", f"El bloque {i} debe reescribirse para el nuevo tamaño")

        # ----- aplicar (a partir de aquí nada falla) -----
        if f is None:
            f = {
                "file_id": up["file_id"], "path": path, "owner": up["owner"], "version": 0,
                "size": 0, "block_size": bs, "replication": up["replication"], "blocks": [],
                "sha256": None, "wrapped_dek": up["wrapped_dek"], "history": [], "deleted": False,
                "created": ts,
            }
            self.s["files"][f["file_id"]] = f
            self._add_entry(path, {"type": "file", "owner": up["owner"], "file_id": f["file_id"], "mtime": ts})
        elif f["version"] > 0:
            f["history"].append({
                "version": f["version"], "blocks": f["blocks"], "size": f["size"],
                "sha256": f["sha256"], "superseded_at": ts,
            })
        for i, (b, stored) in validated.items():
            new_blocks[i] = b["block_id"]
            blk = self.s["blocks"][b["block_id"]]
            blk.update({"size": int(b["size"]), "sha256": b["sha256"], "replicas": stored,
                        "state": "committed", "upload_id": None, "committed": ts})
        used = set(new_blocks)
        for a in up["allocations"].values():
            if a["block_id"] not in used:
                blk = self.s["blocks"].get(a["block_id"])
                if blk:
                    blk["state"] = "garbage"
        f["blocks"] = new_blocks
        f["size"] = size
        f["version"] += 1
        f["block_size"] = bs
        f["replication"] = up["replication"]
        f["sha256"] = cmd.get("file_sha256") if mode in ("create", "replace") else None
        self.s["tree"][path]["mtime"] = ts
        up["state"] = "committed"
        up["result"] = {"path": path, "version": f["version"], "size": size, "file_id": f["file_id"]}
        under = sum(1 for _, st in validated.values() if len(st) < up["replication"])
        return dict(up["result"], under_replicated=under)

    # -------------------------------------------------------------- locks
    def _op_lock_acquire(self, cmd):
        ts = cmd["ts"]
        fid = cmd["file_id"]
        f = self.s["files"].get(fid)
        if not f or f["deleted"]:
            raise Err(404, "NOT_FOUND", "El archivo no existe")
        start = int(cmd["start"])
        end = INF if cmd.get("end") in (None, -1) else int(cmd["end"])
        mode = cmd["mode"]
        for lid, lk in self.active_locks(fid, ts):
            if not _overlap(lk["start"], lk["end"], start, end):
                continue
            if mode == "shared" and lk["mode"] == "shared":
                continue
            raise Err(423, "LOCKED",
                      f"{lk['owner']} tiene un lock {lk['mode']} sobre los bloques "
                      f"{lk['start']}-{'fin' if lk['end'] >= INF else lk['end']}",
                      holder=lk["owner"], lock_mode=lk["mode"])
        self.s["fence_seq"] += 1
        token = self.s["fence_seq"]
        self.s["locks"][cmd["lock_id"]] = {
            "file_id": fid, "path": f["path"], "start": start, "end": end, "mode": mode,
            "owner": cmd["owner"], "session": cmd.get("session"), "token": token,
            "expires": ts + cmd.get("lease", config.LOCK_LEASE), "granted": ts,
        }
        if mode == "exclusive":
            fences = [x for x in self.s["fences"].get(fid, []) if not (start <= x[0] and x[1] <= end)]
            fences.append([start, end, token])
            self.s["fences"][fid] = fences[-200:]
        return {"lock_id": cmd["lock_id"], "token": token, "start": start,
                "end": None if end >= INF else end, "mode": mode,
                "expires_in": cmd.get("lease", config.LOCK_LEASE)}

    def _op_lock_renew(self, cmd):
        lk = self.s["locks"].get(cmd["lock_id"])
        if not lk or lk["owner"] != cmd["owner"] or lk["expires"] < cmd["ts"]:
            raise Err(410, "LOCK_EXPIRED", "El lock venció o no existe")
        lk["expires"] = cmd["ts"] + cmd.get("lease", config.LOCK_LEASE)
        return {"lock_id": cmd["lock_id"], "token": lk["token"], "expires_in": cmd.get("lease", config.LOCK_LEASE)}

    def _op_lock_release(self, cmd):
        lk = self.s["locks"].get(cmd["lock_id"])
        if lk and lk["owner"] == cmd["owner"]:
            self.s["locks"].pop(cmd["lock_id"])
            return {"released": True, "file_id": lk["file_id"]}
        return {"released": False}

    # ------------------------------------------------- mantenimiento (líder)
    def _op_expire(self, cmd):
        ts = cmd["ts"]
        expired_locks = [lid for lid, lk in self.s["locks"].items() if lk["expires"] < ts]
        files_touched = set()
        for lid in expired_locks:
            files_touched.add(self.s["locks"].pop(lid)["file_id"])
        expired_uploads = 0
        for up in self.s["uploads"].values():
            if up["state"] == "open" and up["expires"] < ts:
                up["state"] = "expired"
                self._release_upload_blocks(up)
                expired_uploads += 1
        # Las cargas cerradas se olvidan después de 2 horas (idempotencia del commit)
        for uid in [u for u, up in self.s["uploads"].items()
                    if up["state"] != "open" and up["created"] + 2 * config.UPLOAD_TTL < ts]:
            self.s["uploads"].pop(uid)
        pruned = 0
        for fid in list(self.s["files"]):
            f = self.s["files"][fid]
            keep = [h for h in f["history"] if h["superseded_at"] + config.HISTORY_RETENTION >= ts]
            pruned += len(f["history"]) - len(keep)
            f["history"] = keep
            if f["deleted"] and not keep:
                self.s["files"].pop(fid)
                self.s["fences"].pop(fid, None)
        return {"locks": len(expired_locks), "uploads": expired_uploads, "history": pruned,
                "files_touched": sorted(files_touched)}

    def _op_mark_garbage(self, cmd):
        n = 0
        for bid in cmd["block_ids"]:
            b = self.s["blocks"].get(bid)
            if b and b["state"] != "garbage":
                b["state"] = "garbage"
                n += 1
        return {"marked": n}

    def _op_add_replicas(self, cmd):
        added = []
        for item in cmd["items"]:
            b = self.s["blocks"].get(item["block_id"])
            node = self.s["nodes"].get(item["dn_id"])
            if (b and node and node["state"] == "active" and b["state"] == "committed"
                    and b["sha256"] == item["sha256"] and item["dn_id"] not in b["replicas"]):
                b["replicas"].append(item["dn_id"])
                added.append(item)
        return {"added": added}

    def _op_remove_replica(self, cmd):
        b = self.s["blocks"].get(cmd["block_id"])
        if b and cmd["dn_id"] in b["replicas"]:
            b["replicas"].remove(cmd["dn_id"])
            return {"removed": True}
        return {"removed": False}

    def _op_trim_replica(self, cmd):
        """Quita una réplica sobrante y encola su borrado físico."""
        out = self._op_remove_replica(cmd)
        if out["removed"]:
            self._op_task_create({"tasks": [{
                "task_id": cmd["task_id"], "type": "delete_replica", "block_id": cmd["block_id"],
                "dn_id": cmd["dn_id"], "priority": 1}], "ts": cmd["ts"]})
        return out

    def _op_task_create(self, cmd):
        existing = {(t["type"], t["block_id"], t.get("dn_id")) for t in self.s["tasks"].values()}
        created = []
        for t in cmd["tasks"]:
            key = (t["type"], t["block_id"], t.get("dn_id"))
            if key in existing:
                continue
            existing.add(key)
            self.s["tasks"][t["task_id"]] = dict(t, created=cmd["ts"])
            created.append(t["task_id"])
        return {"created": created}

    def _op_task_complete(self, cmd):
        t = self.s["tasks"].pop(cmd["task_id"], None)
        if t is None:
            return {"known": False}
        result = {"known": True}
        if t["type"] == "replicate" and cmd.get("receipt"):
            r = cmd["receipt"]
            if verify_receipt(r) and r["block_id"] == t["block_id"]:
                result.update(self._op_add_replicas({"items": [r]}))
        elif t["type"] == "delete":
            self.s["blocks"].pop(t["block_id"], None)
        return result
