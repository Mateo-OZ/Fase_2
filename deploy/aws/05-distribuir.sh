#!/usr/bin/env bash
# =============================================================================
# DFSha en AWS - paso 5 (se ejecuta EN EL BASTIÓN/CLIENTE, dentro del repositorio):
# copia el paquete de certificados de cada máquina y ejecuta node-setup.sh allí.
# Es la versión automática de la sección 5.4 del README (máquina por máquina).
#
#   export REPO_URL=https://github.com/<usuario>/dfsha.git
#   ./deploy/aws/05-distribuir.sh            # todas, en orden
#   ./deploy/aws/05-distribuir.sh dn3        # solo una
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")/../.."
source deploy/inventory/aws.env
KEY="${SSH_KEY:-$HOME/.ssh/labsuser.pem}"
REPO_URL="${REPO_URL:?defina REPO_URL con la URL de su repositorio}"
SSH=(ssh -i "$KEY" -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10)
declare -A HOST=([ctrl1]=$CTRL1_HOST [ctrl2]=$CTRL2_HOST [ctrl3]=$CTRL3_HOST
                 [dn1]=$DN1_HOST [dn2]=$DN2_HOST [dn3]=$DN3_HOST [dn4]=$DN4_HOST)
ROLES=("${@:-ctrl1 ctrl2 ctrl3 dn1 dn2 dn3 dn4}")
# shellcheck disable=SC2206
ROLES=(${ROLES[@]})
for role in "${ROLES[@]}"; do
  ip="${HOST[$role]}"
  echo "================ $role ($ip) ================"
  "${SSH[@]}" "ubuntu@$ip" "rm -rf ~/bundle && mkdir -p ~/bundle"
  scp -i "$KEY" -o StrictHostKeyChecking=accept-new -rq "cluster/bundles/$role/." "ubuntu@$ip:~/bundle/"
  "${SSH[@]}" "ubuntu@$ip" "sudo apt-get update -qq && sudo apt-get install -y -qq git >/dev/null && \
     ( [ -d ~/dfsha/.git ] && git -C ~/dfsha pull --ff-only || git clone $REPO_URL ~/dfsha ) && \
     sudo ~/dfsha/scripts/node-setup.sh $role ~/bundle"
done
