"""Política de ubicación de réplicas (sección 4).

Se eligen nodos que:
  1. estén vivos (último latido hace menos de DFSHA_SUSPECT_AFTER) y activos;
  2. sean distintos y, si hay varias zonas, queden en zonas distintas;
  3. tengan menos transferencias en curso y más espacio libre.
La primaria rota para repartir la carga. Si no hay 3 nodos se aceptan 2.
"""
from __future__ import annotations

import time

from dfsha.common import config

from .state_machine import Err


def live_nodes(sm_nodes: dict, live: dict, fresh_within: float | None = None) -> list[str]:
    fresh_within = fresh_within if fresh_within is not None else config.SUSPECT_AFTER
    now = time.monotonic()
    out = []
    for dn, node in sm_nodes.items():
        if node.get("state") != "active":
            continue
        info = live.get(dn)
        if info is None or now - info["t"] > fresh_within:
            continue
        out.append(dn)
    return out


class Placement:
    def __init__(self):
        self.primary_count: dict[str, int] = {}

    def choose(self, sm_nodes: dict, live: dict, k: int, need: int, block_size: int,
               exclude: set[str] | frozenset = frozenset(), planned: dict[str, int] | None = None,
               planned_primary: dict[str, int] | None = None) -> list[str]:
        planned = planned if planned is not None else {}
        planned_primary = planned_primary if planned_primary is not None else {}
        alive = [dn for dn in live_nodes(sm_nodes, live) if dn not in exclude]
        with_space = [dn for dn in alive if live[dn].get("free", 0) > 2 * block_size]
        if len(with_space) < need:
            if len(alive) >= need:
                raise Err(507, "NO_SPACE", "No hay nodos de datos con espacio suficiente")
            raise Err(503, "NO_QUORUM",
                      f"Solo hay {len(alive)} nodos de datos disponibles; se necesitan {need}")
        with_space.sort(key=lambda dn: (
            planned.get(dn, 0) + live[dn].get("active", 0), -live[dn].get("free", 0), dn))
        chosen: list[str] = []
        zones: set[str] = set()
        for dn in with_space:  # primera pasada: zonas distintas
            z = sm_nodes[dn].get("zone", "")
            if z not in zones:
                chosen.append(dn)
                zones.add(z)
            if len(chosen) == k:
                break
        for dn in with_space:  # segunda pasada: completar
            if len(chosen) == k:
                break
            if dn not in chosen:
                chosen.append(dn)
        # la primaria rota: primero entre los bloques de esta misma carga, luego en el histórico
        primary = min(chosen, key=lambda dn: (planned_primary.get(dn, 0), self.primary_count.get(dn, 0),
                                              chosen.index(dn)))
        planned_primary[primary] = planned_primary.get(primary, 0) + 1
        chosen.remove(primary)
        chosen.insert(0, primary)
        self.primary_count[primary] = self.primary_count.get(primary, 0) + 1
        for dn in chosen:
            planned[dn] = planned.get(dn, 0) + 1
        return chosen
