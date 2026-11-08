#!/usr/bin/env bash
# Prueba 4 - Replicación y fallos (5.2): caída de un nodo y lectura o
# recuperación según las garantías prometidas (sección 8 del diseño).
source "$(dirname "$0")/lib.sh"
titulo "Prueba 4: caída de nodos de datos y del líder de metadatos"
randfile "$WORK/f64.bin" 64
SHA=$(sha256sum "$WORK/f64.bin" | cut -d' ' -f1)
dfs put "$WORK/f64.bin" f64.bin >/dev/null
admin admin status

paso "A) Se cae dn2 (docker kill): la lectura sigue con las otras réplicas"
nodectl kill dn2
T0=$(date +%s.%N)
dfs get f64.bin "$WORK/f64.out"
[[ "$(sha256sum "$WORK/f64.out" | cut -d' ' -f1)" == "$SHA" ]] && ok "lectura correcta con dn2 caído" || falla "lectura"

paso "B) Escritura con dn2 caído: los bloques nuevos van a otros nodos"
dfs put "$WORK/f64.bin" f64_b.bin | tail -1
dfs stat --blocks f64_b.bin | grep -c dn2 | { read -r n; [[ $n -eq 0 ]] && ok "ningún bloque nuevo en dn2" || echo "  ($n bloques asignados a dn2 antes de detectarse la caída: quedaron con 2 réplicas y se re-replican)"; }

paso "C) Detección (sin latido 10 s => caído) y re-replicación automática hasta 3 copias"
if wait_until 30 is_dead dn2 >/dev/null; then
  ok "dn2 marcado como caído $(elapsed "$T0") s después de la caída"
else falla "dn2 no fue marcado como caído"; fi
if wait_until 120 file_fully_replicated f64.bin >/dev/null && wait_until 120 file_fully_replicated f64_b.bin >/dev/null; then
  ok "los bloques de f64.bin y f64_b.bin volvieron a tener 3 réplicas ($(elapsed "$T0") s desde la caída)"
else falla "quedan bloques sub-replicados"; fi
admin admin status
dfs stat --blocks f64.bin | grep bloque | head -4

paso "D) dn2 vuelve: envía su inventario y metadatos borra las réplicas que sobran"
nodectl start dn2
sleep 8
admin admin status | grep dn2

paso "E) Un nodo falla después de recibir parte de un bloque (escenario 9.3): dn3 se apaga a los 5 MiB"
inject_fault dn3 crash_after_bytes 5242880
dfs -v put "$WORK/f64.bin" f64_c.bin 2>&1 | grep -E "réplica|reemplazo|put " || true
dfs get f64_c.bin "$WORK/f64_c.out" | tail -1
[[ "$(sha256sum "$WORK/f64_c.out" | cut -d' ' -f1)" == "$SHA" ]] && ok "el archivo se publicó completo y correcto" || falla "archivo incompleto"
if [[ "$DFSHA_ENV" == "local" ]]; then sleep 1; nodectl start dn3; else sleep 10; fi  # en AWS Docker lo reinicia solo
sleep 5
on_host dn3 "sudo ls $(dn_data_dir dn3)/tmp | wc -l" | { read -r n; [[ $n -eq 0 ]] && ok "dn3 descartó los bloques a medio recibir al reiniciar" || falla "quedaron $n temporales"; }

paso "F) Se cae el líder de metadatos: Raft elige otro y la puerta reintenta sin que el cliente lo note"
L=$(leader); echo "líder actual: $L"
python3 - <<'EOF' &
import os, sys, time
sys.path.insert(0, os.environ["REPO"] + "/src")
from dfsha.client.api import DFSClient
c = DFSClient.from_session(); end = time.time() + 12; worst = 0; n = 0; err = 0
while time.time() < end:
    t = time.time()
    try: c.ls("."); n += 1
    except Exception as e: err += 1; print("  error:", e)
    worst = max(worst, time.time() - t); time.sleep(0.05)
print(f"  {n} ls correctos, {err} errores; pausa máxima {worst:.2f} s")
EOF
sleep 3; nodectl kill "$L"; wait
NL=$(leader); echo "nuevo líder: $NL"
[[ "$NL" != "$L" && -n "$NL" ]] && ok "nuevo líder $NL" || falla "sin nuevo líder"
nodectl start "$L"; sleep 5
admin admin status | head -1
resumen
