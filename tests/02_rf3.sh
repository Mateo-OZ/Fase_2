#!/usr/bin/env bash
# Prueba 2 - Acceso RF3 (5.2): read()/write() con la semántica declarada.
#   r  : solo lectura de la versión confirmada al abrir
#   w  : creación o reemplazo completo
#   a  : anexar al final
#   rw : modificación parcial en cualquier posición (copia en escritura por bloque)
# Visibilidad close-to-open: los cambios se ven al confirmar (close/fsync).
source "$(dirname "$0")/lib.sh"
titulo "Prueba 2: open/read/write/lock/truncate (RF3)"

paso "Archivo de texto de 200 KiB con bloques de 64 KiB (4 bloques) para ver varios bloques"
python3 - "$WORK/texto.txt" <<'EOF'
import sys
lines = [f"linea {i:05d} ........................................................\n" for i in range(3200)]
open(sys.argv[1], "w").write("".join(lines))
EOF
dfs put "$WORK/texto.txt" rf3.txt --block-size 0.0625
dfs stat --blocks rf3.txt

paso "read(offset=130000, n=64): lee dentro del bloque 1 sin bajar el archivo"
GOT=$(dfs cat rf3.txt --offset 130000 --length 64); echo "$GOT"
EXPECTED=$(tail -c +130001 "$WORK/texto.txt" | head -c 64)
[[ "$GOT" == "$EXPECTED" ]] && ok "read parcial correcto" || falla "read parcial distinto"

paso "write(rw) en offset 131072 (inicio del bloque 2): solo cambia ese bloque"
BEFORE=$(dfs --json stat rf3.txt | python3 -c "import json,sys; print(' '.join(b['block_id'] for b in json.load(sys.stdin)['block_list']))")
dfs write rf3.txt --offset 131072 --data "<<MODIFICADO POR RF3>>"
AFTER=$(dfs --json stat rf3.txt | python3 -c "import json,sys; print(' '.join(b['block_id'] for b in json.load(sys.stdin)['block_list']))")
python3 - "$BEFORE" "$AFTER" <<'EOF'
import sys
b, a = sys.argv[1].split(), sys.argv[2].split()
for i, (x, y) in enumerate(zip(b, a)):
    print(f"  bloque {i}: {x[:14]} -> {y[:14]} {'(nuevo: copia en escritura)' if x != y else '(sin cambios)'}")
EOF
CH=$(python3 -c "import sys; b,a='$BEFORE'.split(),'$AFTER'.split(); print(sum(x!=y for x,y in zip(b,a)))")
[[ "$CH" == "1" ]] && ok "write parcial creó un único bloque nuevo" || falla "cambiaron $CH bloques"
dfs cat rf3.txt --offset 131060 --length 50 -n

paso "append (modo a): anexa al final"
dfs append rf3.txt --data "ULTIMA LINEA AGREGADA"
dfs cat rf3.txt --offset $(( $(stat -c %s "$WORK/texto.txt") - 20 )) -n | tail -c 60; echo

paso "truncate a 100000 bytes"
dfs truncate rf3.txt 100000
SZ=$(dfs --json stat rf3.txt | python3 -c "import json,sys; print(json.load(sys.stdin)['size'])")
[[ "$SZ" == "100000" ]] && ok "tamaño 100000 tras truncate" || falla "tamaño $SZ"

paso "w: reemplazo completo"
dfs write rf3.txt --mode w --data "contenido nuevo completo"
[[ "$(dfs cat rf3.txt)" == "contenido nuevo completo" ]] && ok "reemplazo completo" || falla "reemplazo"
dfs ls

paso "Verificación aleatoria contra un modelo local (rw, a, truncate, w; el escritor ve sus cambios)"
python3 "$TESTS_DIR/rf3_check.py" && ok "12 operaciones aleatorias coinciden byte a byte" || falla "rf3_check"
resumen
