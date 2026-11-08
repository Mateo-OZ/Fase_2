"""Almacenamiento durable de Raft: estado duro, log y snapshot (sección 7).

* SQLite (modo WAL, synchronous=FULL) guarda current_term, voted_for y el log.
* El snapshot es un archivo JSON escrito en un temporal y renombrado (atómico).
"""
from __future__ import annotations

import json
import os
import sqlite3


class RaftStorage:
    def __init__(self, data_dir: str):
        os.makedirs(data_dir, exist_ok=True)
        self.dir = data_dir
        self.snap_path = os.path.join(data_dir, "snapshot.json")
        self.db = sqlite3.connect(os.path.join(data_dir, "raft.db"), check_same_thread=False)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT)")
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS log (idx INTEGER PRIMARY KEY, term INTEGER NOT NULL, cmd BLOB NOT NULL)"
        )
        self.db.commit()

    # --- estado duro -----------------------------------------------------
    def load_hard_state(self) -> tuple[int, str | None]:
        rows = dict(self.db.execute("SELECT k, v FROM kv").fetchall())
        term = int(rows.get("term", "0"))
        voted = rows.get("voted_for") or None
        return term, voted

    def save_hard_state(self, term: int, voted_for: str | None) -> None:
        self.db.executemany(
            "INSERT OR REPLACE INTO kv (k, v) VALUES (?, ?)",
            [("term", str(term)), ("voted_for", voted_for or "")],
        )
        self.db.commit()

    # --- log ---------------------------------------------------------------
    def load_log(self, after: int) -> list[tuple[int, int, bytes]]:
        return [
            (int(i), int(t), bytes(c))
            for i, t, c in self.db.execute(
                "SELECT idx, term, cmd FROM log WHERE idx > ? ORDER BY idx", (after,)
            )
        ]

    def append(self, entries: list[tuple[int, int, bytes]]) -> None:
        if not entries:
            return
        self.db.executemany("INSERT OR REPLACE INTO log (idx, term, cmd) VALUES (?, ?, ?)", entries)
        self.db.commit()

    def truncate_from(self, index: int) -> None:
        self.db.execute("DELETE FROM log WHERE idx >= ?", (index,))
        self.db.commit()

    def compact_until(self, index: int) -> None:
        self.db.execute("DELETE FROM log WHERE idx <= ?", (index,))
        self.db.commit()

    # --- snapshot ----------------------------------------------------------
    def load_snapshot(self) -> tuple[int, int, bytes | None]:
        if not os.path.exists(self.snap_path):
            return 0, 0, None
        with open(self.snap_path, "rb") as fh:
            raw = fh.read()
        wrapper = json.loads(raw)
        return int(wrapper["last_index"]), int(wrapper["last_term"]), wrapper["state"].encode()

    def save_snapshot(self, last_index: int, last_term: int, state: bytes) -> None:
        tmp = self.snap_path + ".tmp"
        payload = json.dumps(
            {"last_index": last_index, "last_term": last_term, "state": state.decode()}
        ).encode()
        with open(tmp, "wb") as fh:
            fh.write(payload)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, self.snap_path)
