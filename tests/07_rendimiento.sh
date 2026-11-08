#!/usr/bin/env bash
# Prueba 7 - Rendimiento (5.2): latencia y throughput bajo varias condiciones
# (sección 11 del diseño). 5 repeticiones, mediana y percentil 95.
source "$(dirname "$0")/lib.sh"
REPS="${REPS:-5}"
titulo "Prueba 7: rendimiento ($REPS repeticiones por condición)"
OUT="${OUT:-$WORK/resultados_rendimiento.csv}"
python3 "$TESTS_DIR/bench.py" --experiment "${EXPERIMENT:-all}" --reps "$REPS" --out "$OUT" --tmp "$WORK/bench" ${BIG:+--big}

paso "Mismo bloque sin espera (lock con espera 0): cuántos reciben 423"
BENCH_LOCK_WAIT=0 python3 "$TESTS_DIR/bench.py" --experiment concurrency --reps 1 --out "$WORK/conc_sin_espera.csv" \
  --tmp "$WORK/bench" | grep "mismo bloque"

paso "Escalabilidad con número de nodos (S(n) = T(1)/T(n)), opcional"
echo "  Con factor de replicación 1, detenga nodos (nodectl kill dnX) y repita:"
echo "  EXPERIMENT=size REPS=5 ./tests/07_rendimiento.sh  con 1, 2 y 4 nodos activos."
echo "Resultados: $OUT"
