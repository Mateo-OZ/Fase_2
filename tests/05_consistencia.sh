#!/usr/bin/env bash
# Prueba 5 - Consistencia (5.2): comportamiento observable cuando una réplica se
# retrasa o desconecta (escenario 9.2) y garantías de la sección 7.
source "$(dirname "$0")/lib.sh"
titulo "Prueba 5: réplica desconectada o lenta durante una escritura"
randfile "$WORK/f32.bin" 32
SHA=$(sha256sum "$WORK/f32.bin" | cut -d' ' -f1)

paso "A) dn3 se desconecta (docker pause) justo antes de una escritura"
nodectl pause dn3
dfs -v put "$WORK/f32.bin" f32.bin 2>&1 | grep -vE "^  bloque +[0-9]+ ->" || true
dfs stat --blocks f32.bin | grep bloque
N3=$(dfs --json stat f32.bin | python3 -c "import json,sys; print(sum('dn3' in b['replicas'] for b in json.load(sys.stdin)['block_list']))")
[[ "$N3" == "0" ]] && ok "la escritura se confirmó con 2 réplicas; dn3 no figura en ningún bloque nuevo" || falla "dn3 aparece en $N3 bloques"
paso "Los lectores leen la versión nueva desde dn1/dn2/dn4; nunca desde la réplica desactualizada"
dfs get f32.bin "$WORK/f32.out" | tail -1
[[ "$(sha256sum "$WORK/f32.out" | cut -d' ' -f1)" == "$SHA" ]] && ok "lectura correcta" || falla "lectura"
paso "Metadatos detecta la caída y crea la tercera réplica en otro nodo"
wait_until 60 file_fully_replicated f32.bin >/dev/null && ok "todos los bloques de f32.bin recuperaron 3 réplicas" || falla "sub-replicados"
dfs stat --blocks f32.bin | grep bloque
nodectl resume dn3
sleep 6
admin admin status | grep dn3

paso "B) Réplica lenta: dn4 tarda 6 s por trozo (más que el límite de 5 s por trozo)"
inject_fault dn4 delay_ms 6000
T=$(date +%s.%N)
dfs put "$WORK/f32.bin" f32_lenta.bin | tail -2
echo "  tiempo: $(elapsed "$T") s"
on_host dn4 "sudo rm -f $(dn_data_dir dn4)/fault_delay_ms"
dfs stat --blocks f32_lenta.bin | grep bloque
ok "la escritura no espera indefinidamente a la réplica lenta: se confirma con las que respondieron"

paso "C) Close-to-open: un lector que abrió antes del commit sigue viendo su versión completa"
python3 - <<'EOF'
import os, sys
sys.path.insert(0, os.environ["REPO"] + "/src")
from dfsha.client.api import DFSClient
lector, escritor = DFSClient.from_session(), DFSClient.from_session()
p = f"/home/{lector.user}/cto.txt"
open(os.environ["WORK"] + "/v1.txt", "w").write("VERSION-1 " * 10)
open(os.environ["WORK"] + "/v2.txt", "w").write("VERSION-2 " * 10)
escritor.put(os.environ["WORK"] + "/v1.txt", p)
f = lector.open(p, "r"); print(f"  lector abrió v{f.version}: {f.read(0, 9)!r}")
r = escritor.put(os.environ["WORK"] + "/v2.txt", p); print(f"  escritor confirmó v{r['version']}")
print(f"  lector (sin reabrir) sigue en v{f.version}: {f.read(0, 9)!r}")
assert f.read(0, 9) == b"VERSION-1"
g = lector.open(p, "r"); print(f"  lector reabre y ve v{g.version}: {g.read(0, 9)!r}")
assert g.read(0, 9) == b"VERSION-2"
# Dos clientes que hacen ls justo después del commit ven la misma versión
a = [e for e in lector.ls(".")["entries"] if e["name"] == "cto.txt"][0]["version"]
b = [e for e in escritor.ls(".")["entries"] if e["name"] == "cto.txt"][0]["version"]
print(f"  ls desde dos clientes: v{a} y v{b}"); assert a == b == r["version"]
EOF
ok "close-to-open y misma versión para todos tras el commit"

paso "D) Las 3 réplicas de metadatos tienen el mismo estado (huella SHA-256)"
on_host meta1 "$( [[ $DFSHA_ENV == local ]] && echo "cd $REPO && set -a && . .local-cluster/cluster/cluster.env && set +a && DFSHA_CERT_DIR=$REPO/.local-cluster/cluster/certs DFSHA_META_PEERS=meta1=127.0.0.1:7101,meta2=127.0.0.1:7102,meta3=127.0.0.1:7103 python3 scripts/meta-status.py gateway1" || echo "sudo docker exec dfsha-meta python /app/scripts/meta-status.py meta1")"
resumen
