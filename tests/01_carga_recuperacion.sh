#!/usr/bin/env bash
# Prueba 1 - Carga y recuperación (5.2): archivo grande particionado en varios
# nodos; archivo recuperado idéntico (hash).
source "$(dirname "$0")/lib.sh"
SIZE_MB="${SIZE_MB:-256}"
titulo "Prueba 1: carga y recuperación de un archivo de ${SIZE_MB} MiB"

paso "Generando archivo aleatorio y su SHA-256 local"
randfile "$WORK/grande.bin" "$SIZE_MB"
LOCAL_SHA=$(sha256sum "$WORK/grande.bin" | cut -d' ' -f1); echo "sha256 local:    $LOCAL_SHA"

paso "dfs put (bloques de 8 MiB, 3 réplicas, 4 bloques en paralelo)"
dfs -v put "$WORK/grande.bin" grande.bin

paso "Distribución de bloques y réplicas (dfs stat --blocks)"
dfs stat --blocks grande.bin | head -n 12
echo "..."
paso "Bloques por nodo de datos"
dfs --json stat grande.bin | python3 -c "
import json,sys,collections
d=json.load(sys.stdin); c=collections.Counter(); p=collections.Counter()
for b in d['block_list']:
    p[b['replicas'][0]]+=1
    for r in b['replicas']: c[r]+=1
for n in sorted(c): print(f'  {n}: {c[n]} réplicas ({p[n]} como primaria)')
print(f'  total bloques: {len(d[\"block_list\"])}, nodos usados: {len(c)}')
" | tee "$WORK/dist.txt"
NODES=$(grep -c "réplicas (" "$WORK/dist.txt")
[[ $NODES -ge 3 ]] && ok "el archivo quedó repartido en $NODES nodos" || falla "solo $NODES nodos"

paso "dfs get y comparación de hashes"
rm -f "$WORK/recuperado.bin"
dfs get grande.bin "$WORK/recuperado.bin"
REMOTE_SHA=$(sha256sum "$WORK/recuperado.bin" | cut -d' ' -f1); echo "sha256 recuperado: $REMOTE_SHA"
[[ "$LOCAL_SHA" == "$REMOTE_SHA" ]] && ok "archivo recuperado idéntico (SHA-256)" || falla "hash distinto"
resumen
