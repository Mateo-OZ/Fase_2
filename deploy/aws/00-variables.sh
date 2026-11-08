#!/usr/bin/env bash
# =============================================================================
# DFSha en AWS - variables comunes. Lo cargan (source) los demás scripts.
# Se ejecutan en la terminal del AWS Academy Learner Lab (o AWS CloudShell)
# con la AWS CLI v2 ya configurada.
# =============================================================================
export AWS_REGION="${AWS_REGION:-us-east-1}"          # Learner Lab: us-east-1 (o us-west-2)
export AWS_DEFAULT_REGION="$AWS_REGION"
export AZ="${AZ:-${AWS_REGION}a}"
export PROJECT=dfsha

# Red (la subred privada aloja TODOS los nodos; la pública solo el bastión/cliente y el NAT)
export VPC_CIDR=10.20.0.0/16
export PUB_CIDR=10.20.1.0/24
export PRIV_CIDR=10.20.2.0/24

# IPs privadas fijas (deben coincidir con deploy/inventory/aws.env)
export CLIENT_IP=10.20.1.10
export CTRL1_IP=10.20.2.11 CTRL2_IP=10.20.2.12 CTRL3_IP=10.20.2.13
export DN1_IP=10.20.2.21 DN2_IP=10.20.2.22 DN3_IP=10.20.2.23 DN4_IP=10.20.2.24
export NLB_IP=10.20.2.100
export GATEWAY_NAME=dfs.internal

# Instancias (Learner Lab permite nano..large y máximo 9 instancias a la vez: aquí son 8)
export KEY_NAME="${KEY_NAME:-vockey}"
export CTRL_TYPE="${CTRL_TYPE:-t3.medium}"
export DATA_TYPE="${DATA_TYPE:-t3.small}"
export CLIENT_TYPE="${CLIENT_TYPE:-t3.small}"
export DISK_GB="${DISK_GB:-30}"
export INSTANCE_PROFILE="${INSTANCE_PROFILE:-LabInstanceProfile}"   # vacío para no adjuntar

# IP pública desde la que usted hará SSH al bastión (por defecto, la de esta terminal).
# Si va a entrar desde su portátil, defínala antes: export MY_IP=$(curl -s https://checkip.amazonaws.com)/32
export MY_IP="${MY_IP:-$(curl -s https://checkip.amazonaws.com)/32}"

# Ubuntu Server 26.04 LTS (Resolute Raccoon) publicado por Canonical, x86_64, raíz gp3
if [[ -z "${AMI_ID:-}" ]]; then
  for v in 26.04 resolute; do
    AMI_ID="$(aws ssm get-parameters --names "/aws/service/canonical/ubuntu/server/${v}/stable/current/amd64/hvm/ebs-gp3/ami-id" \
      --query 'Parameters[0].Value' --output text 2>/dev/null || true)"
    [[ -n "$AMI_ID" && "$AMI_ID" != "None" ]] && break
  done
  export AMI_ID
fi

STATE_FILE="$(dirname "${BASH_SOURCE[0]}")/state.env"
touch "$STATE_FILE"
# shellcheck disable=SC1090
source "$STATE_FILE"
save() { echo "export $1=$2" >> "$STATE_FILE"; export "$1=$2"; echo "   $1=$2"; }
tag() { echo "ResourceType=$1,Tags=[{Key=Name,Value=$2},{Key=Project,Value=$PROJECT}]"; }
