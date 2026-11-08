#!/usr/bin/env bash
# =============================================================================
# Utilidades comunes de las pruebas de la Fase II (sección 5.2 del enunciado).
#
# DFSHA_ENV=aws   (por defecto) las pruebas corren en el bastión/cliente y actúan
#                 sobre los nodos por SSH + docker (kill, start, pause, unpause).
# DFSHA_ENV=local las pruebas actúan sobre scripts/run-local.sh.
# =============================================================================
set -eu   # sin pipefail: se usan tuberías con head y se verifica cada resultado aparte
TESTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$TESTS_DIR/.." && pwd)"
DFSHA_ENV="${DFSHA_ENV:-aws}"
export WORK="${WORK:-/tmp/dfsha-pruebas}" REPO
mkdir -p "$WORK"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/labsuser.pem}"
# shellcheck disable=SC1091
source "$REPO/deploy/inventory/aws.env"

# Colores
G=$'\e[32m'; R=$'\e[31m'; Y=$'\e[33m'; B=$'\e[1m'; N=$'\e[0m'
titulo() { echo; echo "${B}==== $* ====${N}"; }
paso()   { echo "${Y}>> $*${N}"; }
ok()     { echo "${G}PASA${N}: $*"; }
falla()  { echo "${R}FALLA${N}: $*"; FALLAS=$((${FALLAS:-0}+1)); }
resumen() { if [[ ${FALLAS:-0} -eq 0 ]]; then echo "${G}${B}Todas las verificaciones pasaron${N}"; else echo "${R}${B}${FALLAS} verificación(es) fallaron${N}"; exit 1; fi; }

host_of() {
  case "$1" in
    meta1|gateway1|auth) echo "$CTRL1_HOST" ;;
    meta2|gateway2) echo "$CTRL2_HOST" ;;
    meta3|rabbitmq|worker1|worker2) echo "$CTRL3_HOST" ;;
    dn1) echo "$DN1_HOST" ;; dn2) echo "$DN2_HOST" ;; dn3) echo "$DN3_HOST" ;; dn4) echo "$DN4_HOST" ;;
    *) echo "desconocido"; return 1 ;;
  esac
}

container_of() {
  case "$1" in
    meta*) echo dfsha-meta ;; gateway*) echo dfsha-gateway ;; auth) echo dfsha-auth ;;
    dn*) echo dfsha-datanode ;; rabbitmq) echo dfsha-rabbitmq ;; worker*) echo "dfsha-$1" ;;
  esac
}

on_host() {  # on_host <servicio> <comando...>   (ejecuta en la máquina del servicio)
  local svc="$1"; shift
  if [[ "$DFSHA_ENV" == "local" ]]; then bash -c "$*"; return; fi
  ssh -i "$SSH_KEY" -o StrictHostKeyChecking=accept-new -o LogLevel=ERROR "ubuntu@$(host_of "$svc")" "$@"
}

nodectl() {  # nodectl <kill|start|pause|resume> <servicio>
  local action="$1" svc="$2"
  if [[ "$DFSHA_ENV" == "local" ]]; then
    case "$action" in
      kill) "$REPO/scripts/run-local.sh" kill "$svc" ;;
      start) "$REPO/scripts/run-local.sh" start-one "$svc" ;;
      pause) "$REPO/scripts/run-local.sh" pause "$svc" ;;
      resume) "$REPO/scripts/run-local.sh" resume "$svc" ;;
    esac
    return
  fi
  local c; c="$(container_of "$svc")"
  case "$action" in
    kill) on_host "$svc" sudo docker kill "$c" >/dev/null && echo "$svc detenido (docker kill $c)" ;;
    start) on_host "$svc" sudo docker start "$c" >/dev/null && echo "$svc iniciado (docker start $c)" ;;
    pause) on_host "$svc" sudo docker pause "$c" >/dev/null && echo "$svc congelado (docker pause $c)" ;;
    resume) on_host "$svc" sudo docker unpause "$c" >/dev/null && echo "$svc reanudado (docker unpause $c)" ;;
  esac
}

dn_data_dir() {  # carpeta de datos del nodo, vista desde la máquina del nodo
  if [[ "$DFSHA_ENV" == "local" ]]; then echo "$REPO/.local-cluster/data/$1"; else echo /var/lib/dfsha/dn; fi
}

inject_fault() {  # inject_fault <dn> <crash_after_bytes|delay_ms> <valor>
  on_host "$1" "echo $3 | sudo tee $(dn_data_dir "$1")/fault_$2 >/dev/null"
  echo "falla inyectada en $1: $2=$3"
}

admin() {  # dfs con la sesión del administrador
  DFS_HOME="${ADMIN_DFS_HOME:-$HOME/.dfsha-admin}" dfs "$@"
}

leader() { admin --json admin status | python3 -c "import json,sys; print(json.load(sys.stdin)['raft']['leader'])"; }

under_replicated() { admin --json admin status | python3 -c "import json,sys; print(json.load(sys.stdin)['blocks_under_replicated'])"; }

wait_until() {  # wait_until <segundos> <comando...>
  local limit="$1"; shift
  local t0=$SECONDS
  until "$@" >/dev/null 2>&1; do
    (( SECONDS - t0 >= limit )) && return 1
    sleep 1
  done
  echo "$((SECONDS - t0))"
}

randfile() {  # randfile <ruta> <MiB>
  [[ -f "$1" && $(stat -c %s "$1") -eq $(( $2 * 1048576 )) ]] || head -c $(( $2 * 1048576 )) /dev/urandom > "$1"
}

node_state() { admin --json admin status | python3 -c "import json,sys; print([n['state'] for n in json.load(sys.stdin)['nodes'] if n['dn_id']=='$1'][0])"; }
is_dead() { [[ "$(node_state "$1")" == dead ]]; }
fully_replicated() { [[ "$(under_replicated)" == 0 ]]; }
elapsed() { python3 -c "print(round($(date +%s.%N)-$1,1))"; }
file_fully_replicated() {  # todos los bloques del archivo tienen al menos 3 réplicas
  dfs --json stat "$1" | python3 -c "import json,sys; d=json.load(sys.stdin); sys.exit(0 if all(len(b['replicas'])>=d['replication'] for b in d['block_list']) else 1)"
}
