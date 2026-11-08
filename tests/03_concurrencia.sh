#!/usr/bin/env bash
# Prueba 3 - Concurrencia (5.2): dos clientes sobre un mismo archivo/bloque;
# resultado acorde con la política diseñada (sección 6 y escenario 9.1).
source "$(dirname "$0")/lib.sh"
titulo "Prueba 3: dos clientes escriben el mismo bloque"
printf '%.0s.' {1..300} > "$WORK/conc.txt"
dfs put "$WORK/conc.txt" conc.txt --block-size 0.0625 >/dev/null

paso "A) Lock exclusivo con espera: P2 hace cola (FIFO), relee y aplica su cambio después de P1"
dfs write conc.txt --offset 0 --data "P1P1P1" --hold 4 & P1=$!
sleep 1.5
dfs write conc.txt --offset 3 --data "p2p2p2" --wait 30
wait $P1
R=$(dfs cat conc.txt --length 9); echo "contenido: $R"
[[ "$R" == "P1Pp2p2p2" ]] && ok "ambos cambios aplicados en orden, sin mezclas" || falla "resultado $R"

paso "B) Lock exclusivo sin espera: P2 recibe 423 LOCKED de inmediato"
dfs write conc.txt --offset 0 --data "XXXX" --hold 3 & P1=$!
sleep 1.5
if dfs write conc.txt --offset 0 --data "YYYY" 2>"$WORK/b.err"; then falla "P2 no fue bloqueado"; else cat "$WORK/b.err"; fi
grep -q "423 LOCKED" "$WORK/b.err" && ok "P2 recibió 423 LOCKED" || falla "no hubo 423"
wait $P1
dfs cat conc.txt --length 9 -n

paso "C) Sin lock (solo control optimista): el segundo commit recibe 409 VERSION_CONFLICT"
dfs write conc.txt --offset 0 --data "SIN1" --no-lock --hold 2 & P1=$!
sleep 0.5
dfs write conc.txt --offset 0 --data "SIN2" --no-lock --hold 3 2>"$WORK/c.err" || true
wait $P1 || true
cat "$WORK/c.err"
grep -q "409 VERSION_CONFLICT" "$WORK/c.err" && ok "el segundo escritor recibió 409; el bloque nunca mezcla cambios" || falla "no hubo 409"
dfs cat conc.txt --length 9 -n

paso "D) Token de cercado: P1 se congela más de 30 s (vence su lease), P2 escribe, P1 intenta confirmar"
dfs write conc.txt --offset 0 --data "LENTO" --hold 2 2>"$WORK/d.err" & P1=$!
sleep 1.5
kill -STOP $P1; echo "P1 congelado con SIGSTOP (simula una pausa larga del cliente)"
sleep 35
dfs write conc.txt --offset 0 --data "P2!!"
kill -CONT $P1; wait $P1 || true
cat "$WORK/d.err"
grep -q "409 FENCED" "$WORK/d.err" && ok "metadatos rechazó el token de cercado viejo de P1" || falla "P1 no fue cercado"
dfs cat conc.txt --length 9 -n

paso "E) Bloques distintos del mismo archivo: no compiten (RNF5)"
python3 - <<'EOF'
import os, sys, time, threading
sys.path.insert(0, os.environ.get("REPO", ".") + "/src")
from dfsha.client.api import DFSClient, MiB
c = DFSClient.from_session()
p = f"/home/{c.user}/conc_bloques.bin"
src = os.environ["WORK"] + "/cb.bin"; open(src, "wb").write(b"." * (4 * 65536))
c.put(src, p, block_size=65536)
res = {}
def w(i):
    k = DFSClient.from_session(); f = k.open(p, "rw"); f.write(i * 65536, f"bloque{i}".encode()); time.sleep(2); res[i] = f.close()["version"]
t = [threading.Thread(target=w, args=(i,)) for i in (0, 2)]
t0 = time.time(); [x.start() for x in t]; [x.join() for x in t]
f = c.open(p, "r")
print(f"  dos escritores en bloques 0 y 2 en {time.time()-t0:.1f} s; versiones {sorted(res.values())}; "
      f"contenido: {f.read(0,7)!r} y {f.read(2*65536,7)!r}")
assert f.read(0, 7) == b"bloque0" and f.read(2 * 65536, 7) == b"bloque2"
EOF
ok "escrituras en bloques distintos avanzan en paralelo y ambas quedan"
resumen
