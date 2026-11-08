#!/usr/bin/env bash
# =============================================================================
# DFSha - Generación de la CA interna, certificados TLS y secretos del clúster
# (sección 10 del diseño: "TLS mutuo entre todos los servicios, con
#  certificados de una CA interna del proyecto").
#
# Uso:   ./scripts/gen-certs.sh deploy/inventory/aws.env  [directorio_salida]
#
# Lee el inventario (IPs o nombres de cada máquina) y produce:
#   <salida>/ca/ca.key, ca.crt                 CA interna (ECDSA P-256)
#   <salida>/certs/<servicio>.crt|.key         un certificado por servicio
#   <salida>/certs/jwt_private.pem|jwt_public.pem   llaves RS256 para JWT
#   <salida>/cluster.env                       secretos y direcciones comunes
#   <salida>/bundles/<máquina>/                lo que se copia a cada máquina
# =============================================================================
set -euo pipefail

INVENTORY="${1:?Uso: $0 <inventario.env> [salida]}"
OUT="${2:-./cluster}"
DAYS="${CERT_DAYS:-825}"
# shellcheck disable=SC1090
source "$INVENTORY"

mkdir -p "$OUT/ca" "$OUT/certs" "$OUT/bundles"
umask 077

san_of() {  # convierte "10.0.0.1 nombre ..." en "IP:10.0.0.1,DNS:nombre,..." (sin repetidos)
  local out="" x
  declare -A seen=()
  for x in "$@"; do
    [[ -z "$x" || -n "${seen[$x]:-}" ]] && continue
    seen[$x]=1
    if [[ "$x" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then out+="IP:$x,"; else out+="DNS:$x,"; fi
  done
  echo "${out%,}"
}

# ---------------------------------------------------------------- CA interna
if [[ ! -f "$OUT/ca/ca.key" ]]; then
  echo ">> Creando CA interna"
  openssl ecparam -name prime256v1 -genkey -noout -out "$OUT/ca/ca.key"
  openssl req -x509 -new -key "$OUT/ca/ca.key" -sha256 -days 3650 \
    -subj "/O=DFSha UPB/CN=DFSha Internal CA" -out "$OUT/ca/ca.crt" \
    -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign"
fi
cp "$OUT/ca/ca.crt" "$OUT/certs/ca.crt"

issue() {  # issue <cn> <host1> [host2 ...]
  local cn="$1"; shift
  local san; san="$(san_of "$cn" localhost 127.0.0.1 "$@")"
  openssl ecparam -name prime256v1 -genkey -noout -out "$OUT/certs/$cn.key"
  openssl req -new -key "$OUT/certs/$cn.key" -subj "/O=DFSha UPB/CN=$cn" \
    -addext "subjectAltName=$san" -out "$OUT/certs/$cn.csr"
  openssl x509 -req -in "$OUT/certs/$cn.csr" -CA "$OUT/ca/ca.crt" -CAkey "$OUT/ca/ca.key" \
    -CAcreateserial -days "$DAYS" -sha256 -copy_extensions copyall \
    -extfile <(printf "basicConstraints=CA:FALSE\nkeyUsage=critical,digitalSignature,keyAgreement\nextendedKeyUsage=serverAuth,clientAuth\n") \
    -out "$OUT/certs/$cn.crt" 2>/dev/null
  rm -f "$OUT/certs/$cn.csr"
  echo "   $cn  ->  $san"
}

echo ">> Emitiendo certificados de servicio"
issue meta1    "$CTRL1_HOST" ctrl1
issue meta2    "$CTRL2_HOST" ctrl2
issue meta3    "$CTRL3_HOST" ctrl3
issue gateway1 "$CTRL1_HOST" ctrl1 "$GATEWAY_NAME" ${NLB_IP:-}
issue gateway2 "$CTRL2_HOST" ctrl2 "$GATEWAY_NAME" ${NLB_IP:-}
issue auth     "$CTRL1_HOST" ctrl1
issue rabbitmq "$CTRL3_HOST" ctrl3 rabbitmq
issue worker1  "$CTRL3_HOST" ctrl3
issue worker2  "$CTRL3_HOST" ctrl3
issue dn1      "$DN1_HOST" dn1
issue dn2      "$DN2_HOST" dn2
issue dn3      "$DN3_HOST" dn3
issue dn4      "$DN4_HOST" dn4

# ---------------------------------------------------------- llaves y secretos
if [[ ! -f "$OUT/certs/jwt_private.pem" ]]; then
  echo ">> Llaves RS256 para JWT"
  openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out "$OUT/certs/jwt_private.pem" 2>/dev/null
  openssl pkey -in "$OUT/certs/jwt_private.pem" -pubout -out "$OUT/certs/jwt_public.pem"
fi

if [[ ! -f "$OUT/secrets.env" ]]; then
  echo ">> Secretos del clúster"
  {
    echo "DFSHA_BLOCK_TOKEN_SECRET=$(openssl rand -hex 32)"
    echo "DFSHA_MASTER_KEY=$(openssl rand -base64 32)"
    echo "DFSHA_MQ_PASSWORD=$(openssl rand -hex 16)"
  } > "$OUT/secrets.env"
fi

P="${META_PORT:-7100}"
cat > "$OUT/cluster.env" <<EOF
# Generado por scripts/gen-certs.sh a partir de $INVENTORY
DFSHA_CERT_DIR=/etc/dfsha/certs
DFSHA_META_PEERS=meta1=${CTRL1_HOST}:${P},meta2=${CTRL2_HOST}:${P},meta3=${CTRL3_HOST}:${P}
DFSHA_AUTH_URLS=https://${CTRL1_HOST}:8500
DFSHA_MQ_HOST=${CTRL3_HOST}
DFSHA_MQ_PORT=5671
DFSHA_MQ_USER=dfsha
DFSHA_MQ_TLS_NAME=rabbitmq
DFSHA_JWT_PUBLIC_KEY_FILE=/etc/dfsha/certs/jwt_public.pem
DFSHA_JWT_PRIVATE_KEY_FILE=/etc/dfsha/certs/jwt_private.pem
DFSHA_BLOCK_SIZE=8388608
DFSHA_REPLICATION=3
$(cat "$OUT/secrets.env")
EOF

# ------------------------------------------------------------- RabbitMQ
# shellcheck disable=SC1091
source "$OUT/secrets.env"
ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
mkdir -p "$OUT/rabbitmq-certs"
cp "$OUT/certs/ca.crt" "$OUT/certs/rabbitmq.crt" "$OUT/certs/rabbitmq.key" "$OUT/rabbitmq-certs/"
sed -e "s#__CERT_DIR__#/etc/rabbitmq/certs#" -e "s#__MQ_PASSWORD__#${DFSHA_MQ_PASSWORD}#" \
  "$ROOT_DIR/deploy/rabbitmq/rabbitmq.conf.template" > "$OUT/rabbitmq.conf"
echo "log.console = true" >> "$OUT/rabbitmq.conf"

# ------------------------------------------------- paquetes por cada máquina
bundle() {  # bundle <máquina> <cert1> <cert2> ...
  local host="$1"; shift
  local dir="$OUT/bundles/$host"
  rm -rf "$dir"; mkdir -p "$dir/certs"
  cp "$OUT/certs/ca.crt" "$dir/certs/"
  local c
  for c in "$@"; do cp "$OUT/certs/$c.crt" "$OUT/certs/$c.key" "$dir/certs/"; done
  cp "$OUT/cluster.env" "$dir/cluster.env"
}
bundle ctrl1 meta1 gateway1 auth
cp "$OUT/certs/jwt_private.pem" "$OUT/certs/jwt_public.pem" "$OUT/bundles/ctrl1/certs/"
bundle ctrl2 meta2 gateway2
cp "$OUT/certs/jwt_public.pem" "$OUT/bundles/ctrl2/certs/"
bundle ctrl3 meta3 rabbitmq worker1 worker2
cp -r "$OUT/rabbitmq-certs" "$OUT/rabbitmq.conf" "$OUT/bundles/ctrl3/"
bundle dn1 dn1
bundle dn2 dn2
bundle dn3 dn3
bundle dn4 dn4
# Las réplicas de datos no necesitan la clave de JWT ni la contraseña de RabbitMQ
for h in dn1 dn2 dn3 dn4; do sed -i '/^DFSHA_MQ_PASSWORD=/d' "$OUT/bundles/$h/cluster.env"; done
# El cliente solo necesita la CA para validar la puerta y los nodos de datos
mkdir -p "$OUT/bundles/client"; cp "$OUT/certs/ca.crt" "$OUT/bundles/client/ca.crt"

echo ">> Listo. Paquetes por máquina en $OUT/bundles/: $(ls "$OUT/bundles" | tr '\n' ' ')"
