#!/usr/bin/env bash
# =============================================================================
# DFSha - preparación de UNA máquina EC2 (Ubuntu Server 26.04 LTS) y arranque de
# sus servicios. Se ejecuta en cada máquina, dentro del repositorio clonado:
#
#   sudo ./scripts/node-setup.sh <rol> <carpeta_del_paquete>
#
#   rol: ctrl1 | ctrl2 | ctrl3 | dn1 | dn2 | dn3 | dn4
#   carpeta_del_paquete: lo que generó scripts/gen-certs.sh en cluster/bundles/<rol>
#                        (certs/, cluster.env y, en ctrl3, rabbitmq.conf + rabbitmq-certs/)
#
# Pasos: instala Docker Engine + Compose desde el repositorio oficial de Docker,
# sincroniza el reloj con Amazon Time Sync (los leases dependen del reloj),
# instala certificados y secretos con permisos mínimos, escribe
# deploy/compose/.env y levanta los contenedores del rol.
# =============================================================================
set -euo pipefail

ROLE="${1:?Uso: sudo $0 <ctrl1|ctrl2|ctrl3|dn1..dn4> <carpeta_del_paquete>}"
BUNDLE="${2:?Falta la carpeta del paquete (cluster/bundles/<rol>)}"
REPO="$(cd "$(dirname "$0")/.." && pwd)"
[[ $EUID -eq 0 ]] || { echo "Ejecute con sudo"; exit 1; }
[[ -f "$BUNDLE/cluster.env" ]] || { echo "No encuentro $BUNDLE/cluster.env"; exit 1; }

echo ">> [1/6] Paquetes base y reloj (chrony + Amazon Time Sync 169.254.169.123)"
export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y ca-certificates curl gnupg git chrony tcpdump
if ! grep -q 169.254.169.123 /etc/chrony/chrony.conf; then
  sed -i '1i server 169.254.169.123 prefer iburst minpoll 4 maxpoll 4' /etc/chrony/chrony.conf
fi
systemctl restart chrony || systemctl restart chronyd || true

echo ">> [2/6] Docker Engine + Compose (repositorio oficial de Docker)"
if ! command -v docker >/dev/null; then
  install -m 0755 -d /etc/apt/keyrings
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
  chmod a+r /etc/apt/keyrings/docker.asc
  CODENAME="$(. /etc/os-release && echo "${UBUNTU_CODENAME:-$VERSION_CODENAME}")"
  echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu ${CODENAME} stable" \
    > /etc/apt/sources.list.d/docker.list
  apt-get update -y
  apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
fi
systemctl enable --now docker
usermod -aG docker "${SUDO_USER:-ubuntu}" || true

echo ">> [3/6] Certificados y secretos"
install -d -m 0755 /etc/dfsha
install -d -m 0750 -o 10001 -g 10001 /etc/dfsha/certs
install -m 0640 -o 10001 -g 10001 "$BUNDLE"/certs/* /etc/dfsha/certs/
chmod 0600 /etc/dfsha/certs/*.key /etc/dfsha/certs/*.pem 2>/dev/null || true
chmod 0644 /etc/dfsha/certs/ca.crt
install -d -m 0700 /opt/dfsha
install -m 0600 "$BUNDLE/cluster.env" /opt/dfsha/cluster.env
install -d -m 0750 -o 10001 -g 10001 /var/lib/dfsha /var/lib/dfsha/meta /var/lib/dfsha/dn /var/lib/dfsha/audit

# shellcheck disable=SC1091
NODE_IP="$(hostname -I | awk '{print $1}')"
TOKEN="$(curl -s -X PUT http://169.254.169.254/latest/api/token -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' || true)"
ZONE="$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/placement/availability-zone || true)"

case "$ROLE" in
  ctrl1) PROFILE=ctrl1; META_ID=meta1; GATEWAY_ID=gateway1; DN_ID= ;;
  ctrl2) PROFILE=ctrl2; META_ID=meta2; GATEWAY_ID=gateway2; DN_ID= ;;
  ctrl3) PROFILE=ctrl3; META_ID=meta3; GATEWAY_ID=;        DN_ID= ;;
  dn[1-4]) PROFILE=data; META_ID=;     GATEWAY_ID=;        DN_ID="$ROLE" ;;
  *) echo "rol desconocido: $ROLE"; exit 1 ;;
esac

if [[ "$ROLE" == "ctrl3" ]]; then
  echo ">> [3b] RabbitMQ con TLS mutuo"
  install -d -m 0750 -o 999 -g 999 /opt/dfsha/rabbitmq /opt/dfsha/rabbitmq/certs /var/lib/dfsha/rabbitmq
  install -m 0640 -o 999 -g 999 "$BUNDLE"/rabbitmq-certs/* /opt/dfsha/rabbitmq/certs/
  install -m 0640 -o 999 -g 999 "$BUNDLE/rabbitmq.conf" /opt/dfsha/rabbitmq/rabbitmq.conf
fi

echo ">> [4/6] deploy/compose/.env para el rol $ROLE ($NODE_IP, $ZONE)"
cat > "$REPO/deploy/compose/.env" <<EOF
COMPOSE_PROFILES=$PROFILE
META_ID=$META_ID
GATEWAY_ID=$GATEWAY_ID
DN_ID=$DN_ID
NODE_IP=$NODE_IP
ZONE=$ZONE
EOF

echo ">> [5/6] Construyendo la imagen dfsha:2.0"
cd "$REPO/deploy/compose"
docker compose build

echo ">> [6/6] Levantando servicios"
docker compose up -d
if [[ "$ROLE" == "ctrl3" ]]; then
  echo "   esperando a RabbitMQ y fijando la contraseña del usuario dfsha"
  docker exec dfsha-rabbitmq rabbitmqctl await_startup >/dev/null
  MQ_PASS="$(grep '^DFSHA_MQ_PASSWORD=' /opt/dfsha/cluster.env | cut -d= -f2-)"
  docker exec dfsha-rabbitmq rabbitmqctl add_user dfsha "$MQ_PASS" >/dev/null 2>&1 || \
    docker exec dfsha-rabbitmq rabbitmqctl change_password dfsha "$MQ_PASS" >/dev/null
  docker exec dfsha-rabbitmq rabbitmqctl set_permissions -p / dfsha ".*" ".*" ".*" >/dev/null
fi
sleep 3
docker compose ps
echo ">> Listo: $ROLE. Registros: sudo docker compose -f $REPO/deploy/compose/docker-compose.yml logs -f"
