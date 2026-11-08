#!/usr/bin/env bash
# DFSha en AWS - paso 2: grupos de seguridad (solo tráfico entre componentes conocidos).
#
#   sg-client (bastión/cliente) : 22 desde MY_IP
#   sg-nlb    (balanceador)     : 443 desde sg-client
#   sg-ctrl   (metadatos, puertas, auth, RabbitMQ, trabajadores)
#       8443 desde sg-nlb (y sg-client, solo plan B sin balanceador)
#       7100 desde sg-ctrl y sg-data      (Raft, puerta->líder, latidos de nodos)
#       8500 desde sg-ctrl                (puerta -> auth)
#       5671 desde sg-ctrl                (AMQP TLS: líder y trabajadores)
#       22   desde sg-client
#   sg-data   (nodos de datos)
#       7001 desde sg-client              (plano de datos del cliente, TLS)
#       7002 desde sg-data y sg-ctrl      (cadena de réplicas y trabajadores, TLS mutuo)
#       22   desde sg-client
set -euo pipefail
source "$(dirname "$0")/00-variables.sh"

mk() { aws ec2 create-security-group --group-name "$1" --description "$2" --vpc-id "$VPC_ID" \
        --tag-specifications "$(tag security-group "$1")" --query GroupId --output text; }
allow_sg() {  # allow_sg <grupo> <puerto> <grupo_origen> <descripción>
  aws ec2 authorize-security-group-ingress --group-id "$1" --ip-permissions \
    "IpProtocol=tcp,FromPort=$2,ToPort=$2,UserIdGroupPairs=[{GroupId=$3,Description=\"$4\"}]" >/dev/null
  echo "   $1 <- tcp/$2 desde $3 ($4)"
}

SG_CLIENT=$(mk dfsha-client "Bastion y cliente DFSha"); save SG_CLIENT "$SG_CLIENT"
SG_NLB=$(mk dfsha-nlb "Balanceador interno de las puertas"); save SG_NLB "$SG_NLB"
SG_CTRL=$(mk dfsha-ctrl "Maquinas de control DFSha"); save SG_CTRL "$SG_CTRL"
SG_DATA=$(mk dfsha-data "Nodos de datos DFSha"); save SG_DATA "$SG_DATA"

aws ec2 authorize-security-group-ingress --group-id "$SG_CLIENT" --ip-permissions \
  "IpProtocol=tcp,FromPort=22,ToPort=22,IpRanges=[{CidrIp=$MY_IP,Description=SSH-administrador}]" >/dev/null
echo "   $SG_CLIENT <- tcp/22 desde $MY_IP"
allow_sg "$SG_NLB"  443  "$SG_CLIENT" "cliente a dfs.internal"
allow_sg "$SG_CTRL" 8443 "$SG_NLB"    "NLB a puertas"
allow_sg "$SG_CTRL" 8443 "$SG_CLIENT" "plan B sin NLB"
allow_sg "$SG_CTRL" 7100 "$SG_CTRL"   "Raft y puerta a metadatos"
allow_sg "$SG_CTRL" 7100 "$SG_DATA"   "latidos e inventarios"
allow_sg "$SG_CTRL" 8500 "$SG_CTRL"   "puerta a auth"
allow_sg "$SG_CTRL" 5671 "$SG_CTRL"   "AMQP TLS"
allow_sg "$SG_CTRL" 22   "$SG_CLIENT" "SSH desde bastion"
allow_sg "$SG_DATA" 7001 "$SG_CLIENT" "cliente a nodos (datos)"
allow_sg "$SG_DATA" 7002 "$SG_DATA"   "replicacion en cadena"
allow_sg "$SG_DATA" 7002 "$SG_CTRL"   "trabajadores a nodos"
allow_sg "$SG_DATA" 22   "$SG_CLIENT" "SSH desde bastion"
echo ">> Grupos de seguridad listos"
