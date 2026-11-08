#!/usr/bin/env bash
# =============================================================================
# DFSha - clúster completo en UNA sola máquina, como procesos separados
# (desarrollo y ensayo de las pruebas antes de ir a AWS).
#
#   ./scripts/run-local.sh start     # 3 metadatos, auth, 2 puertas, 4 nodos, 2 trabajadores
#   ./scripts/run-local.sh status
#   ./scripts/run-local.sh stop
#   ./scripts/run-local.sh kill <servicio>    # p. ej. dn2 o meta1 (simula una caída)
#   ./scripts/run-local.sh start-one <servicio>
#
# Puertos: metadatos 7101-7103, nodos 7011-7014 (clientes) y 7021-7024 (internos),
# auth 8500, puertas 8443 y 8444. RabbitMQ: usa el del sistema en 5671 si
# existe (ver scripts/local-rabbitmq.sh); si no, se desactiva la cola.
# =============================================================================
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RUN="${DFSHA_LOCAL_DIR:-$ROOT/.local-cluster}"
CERTS="$RUN/cluster/certs"
mkdir -p "$RUN/logs" "$RUN/pids" "$RUN/data"

if [[ ! -f "$RUN/cluster/cluster.env" ]]; then
  "$ROOT/scripts/gen-certs.sh" "$ROOT/deploy/inventory/local.env" "$RUN/cluster" >/dev/null
fi

common_env() {
  set -a
  # shellcheck disable=SC1091
  source "$RUN/cluster/cluster.env"
  set +a
  export DFSHA_CERT_DIR="$CERTS"
  export DFSHA_META_PEERS="meta1=127.0.0.1:7101,meta2=127.0.0.1:7102,meta3=127.0.0.1:7103"
  export DFSHA_AUTH_URLS="https://127.0.0.1:8500"
  export DFSHA_JWT_PUBLIC_KEY_FILE="$CERTS/jwt_public.pem"
  export DFSHA_JWT_PRIVATE_KEY_FILE="$CERTS/jwt_private.pem"
  export DFSHA_MQ_HOST=127.0.0.1
  export DFSHA_AUDIT_FILE="$RUN/logs/audit.jsonl"
  export PYTHONUNBUFFERED=1
  if [[ -z "${DFSHA_MQ_ENABLED:-}" ]]; then
    if (exec 3<>/dev/tcp/127.0.0.1/5671) 2>/dev/null; then export DFSHA_MQ_ENABLED=true; else export DFSHA_MQ_ENABLED=false; fi
  fi
}

alive() {  # alive <pid>: vivo y no zombi
  local st; st="$(ps -o stat= -p "$1" 2>/dev/null || true)"
  [[ -n "$st" && "$st" != Z* ]]
}

launch() {  # launch <nombre> <comando...>
  local name="$1"; shift
  if [[ -f "$RUN/pids/$name" ]] && alive "$(cat "$RUN/pids/$name")"; then
    echo "$name ya está corriendo"; return
  fi
  nohup "$@" >>"$RUN/logs/$name.log" 2>&1 &
  echo $! >"$RUN/pids/$name"
  echo "iniciado $name (pid $!)"
}

start_one() {
  common_env
  local s="$1"
  case "$s" in
    meta[1-3])
      local n="${s#meta}"
      DFSHA_META_ID="$s" DFSHA_META_BIND="127.0.0.1:710$n" DFSHA_META_DATA="$RUN/data/$s" \
        launch "$s" python -m dfsha.metadata.main ;;
    dn[1-4])
      local n="${s#dn}"
      DFSHA_DN_ID="$s" DFSHA_DN_DATA="$RUN/data/$s" DFSHA_DN_CLIENT_BIND="127.0.0.1:701$n" \
        DFSHA_DN_INTERNAL_BIND="127.0.0.1:702$n" DFSHA_DN_ADVERTISE=127.0.0.1 DFSHA_DN_ZONE="zona-local" \
        launch "$s" python -m dfsha.datanode.main ;;
    auth)
      DFSHA_AUTH_ID=auth DFSHA_AUTH_HOST=127.0.0.1 DFSHA_AUTH_PORT=8500 launch auth python -m dfsha.auth.main ;;
    gateway1)
      DFSHA_GATEWAY_ID=gateway1 DFSHA_GATEWAY_HOST=127.0.0.1 DFSHA_GATEWAY_PORT=8443 \
        launch gateway1 python -m dfsha.gateway.main ;;
    gateway2)
      DFSHA_GATEWAY_ID=gateway2 DFSHA_GATEWAY_HOST=127.0.0.1 DFSHA_GATEWAY_PORT=8444 \
        launch gateway2 python -m dfsha.gateway.main ;;
    worker[1-2])
      [[ "$DFSHA_MQ_ENABLED" == "true" ]] || { echo "sin RabbitMQ: $s no se inicia"; return; }
      DFSHA_WORKER_ID="$s" launch "$s" python -m dfsha.worker.main ;;
    *) echo "servicio desconocido: $s"; exit 1 ;;
  esac
}

case "${1:-}" in
  start)
    for s in meta1 meta2 meta3; do start_one "$s"; done
    sleep 3
    for s in auth gateway1 gateway2 dn1 dn2 dn3 dn4 worker1 worker2; do start_one "$s"; done
    sleep 4
    echo "CA para el cliente: $CERTS/ca.crt"
    echo "Ejemplo: export DFS_GATEWAY=https://127.0.0.1:8443 DFS_CA_FILE=$CERTS/ca.crt"
    ;;
  start-one) start_one "${2:?servicio}" ;;
  kill)
    s="${2:?servicio}"
    kill -9 "$(cat "$RUN/pids/$s")" 2>/dev/null && echo "$s detenido (kill -9)" || echo "$s no estaba corriendo"
    rm -f "$RUN/pids/$s" ;;
  pause) kill -STOP "$(cat "$RUN/pids/${2:?servicio}")" && echo "${2} pausado (SIGSTOP)" ;;
  resume) kill -CONT "$(cat "$RUN/pids/${2:?servicio}")" && echo "${2} reanudado (SIGCONT)" ;;
  stop)
    for f in "$RUN"/pids/*; do [[ -f "$f" ]] || continue; kill "$(cat "$f")" 2>/dev/null || true; rm -f "$f"; done
    echo "clúster local detenido" ;;
  status)
    for f in "$RUN"/pids/*; do
      [[ -f "$f" ]] || continue
      n="$(basename "$f")"
      if alive "$(cat "$f")"; then echo "  $n  corriendo (pid $(cat "$f"))"; else echo "  $n  CAÍDO"; fi
    done ;;
  admin)  # ./scripts/run-local.sh admin create-user andrey --password-stdin
    shift; common_env; DFSHA_AUTH_ID=auth python -m dfsha.auth.admin "$@" ;;
  clean)
    "$0" stop || true; rm -rf "$RUN"; echo "datos locales borrados" ;;
  *) sed -n '2,16p' "$0"; exit 1 ;;
esac
