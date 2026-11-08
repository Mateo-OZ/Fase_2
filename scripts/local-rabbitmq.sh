#!/usr/bin/env bash
# Configura el RabbitMQ del sistema (apt install rabbitmq-server) con TLS mutuo
# para el clúster local de scripts/run-local.sh. Requiere sudo/root.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RUN="${DFSHA_LOCAL_DIR:-$ROOT/.local-cluster}"
[[ -f "$RUN/cluster/cluster.env" ]] || "$ROOT/scripts/gen-certs.sh" "$ROOT/deploy/inventory/local.env" "$RUN/cluster" >/dev/null
# shellcheck disable=SC1091
source "$RUN/cluster/secrets.env"
install -d -o rabbitmq -g rabbitmq -m 0750 /etc/rabbitmq/certs
install -o rabbitmq -g rabbitmq -m 0640 "$RUN/cluster/certs/"{ca.crt,rabbitmq.crt,rabbitmq.key} /etc/rabbitmq/certs/
sed -e "s#__CERT_DIR__#/etc/rabbitmq/certs#" -e "s#__MQ_PASSWORD__#${DFSHA_MQ_PASSWORD}#" \
  "$ROOT/deploy/rabbitmq/rabbitmq.conf.template" > /etc/rabbitmq/rabbitmq.conf
(systemctl restart rabbitmq-server 2>/dev/null || service rabbitmq-server restart)
rabbitmqctl await_startup >/dev/null
# default_user solo se crea en una base nueva: se asegura la contraseña actual
rabbitmqctl add_user dfsha "$DFSHA_MQ_PASSWORD" >/dev/null 2>&1 || rabbitmqctl change_password dfsha "$DFSHA_MQ_PASSWORD" >/dev/null
rabbitmqctl set_permissions -p / dfsha ".*" ".*" ".*" >/dev/null
echo "RabbitMQ con TLS en 5671"
