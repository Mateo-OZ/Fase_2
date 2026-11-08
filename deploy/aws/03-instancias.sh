#!/usr/bin/env bash
# DFSha en AWS - paso 3: 8 instancias EC2 Ubuntu Server 26.04 LTS con IP privada fija.
#   cliente (bastión, subred pública) + ctrl1..3 + dn1..4 (subred privada, sin IP pública)
set -euo pipefail
source "$(dirname "$0")/00-variables.sh"
[[ -n "$AMI_ID" && "$AMI_ID" != "None" ]] || { echo "No se encontró la AMI de Ubuntu 26.04 (defina AMI_ID)"; exit 1; }
echo ">> AMI Ubuntu Server 26.04 LTS: $AMI_ID"

launch() {  # launch <nombre> <tipo> <subred> <sg> <ip> <ip_pública true|false>
  local profile=()
  [[ -n "$INSTANCE_PROFILE" ]] && profile=(--iam-instance-profile "Name=$INSTANCE_PROFILE")
  aws ec2 run-instances --image-id "$AMI_ID" --instance-type "$2" --key-name "$KEY_NAME" \
    --network-interfaces "[{\"DeviceIndex\":0,\"SubnetId\":\"$3\",\"Groups\":[\"$4\"],\"PrivateIpAddress\":\"$5\",\"AssociatePublicIpAddress\":$6}]" \
    --block-device-mappings "[{\"DeviceName\":\"/dev/sda1\",\"Ebs\":{\"VolumeSize\":$DISK_GB,\"VolumeType\":\"gp3\",\"Encrypted\":true,\"DeleteOnTermination\":true}}]" \
    --metadata-options "HttpTokens=required,HttpEndpoint=enabled" \
    "${profile[@]}" \
    --tag-specifications "$(tag instance "dfsha-$1")" \
    --query 'Instances[0].InstanceId' --output text
}

CLIENT_ID=$(launch cliente "$CLIENT_TYPE" "$PUB_SUBNET" "$SG_CLIENT" "$CLIENT_IP" true); save CLIENT_ID "$CLIENT_ID"
CTRL1_ID=$(launch ctrl1 "$CTRL_TYPE" "$PRIV_SUBNET" "$SG_CTRL" "$CTRL1_IP" false); save CTRL1_ID "$CTRL1_ID"
CTRL2_ID=$(launch ctrl2 "$CTRL_TYPE" "$PRIV_SUBNET" "$SG_CTRL" "$CTRL2_IP" false); save CTRL2_ID "$CTRL2_ID"
CTRL3_ID=$(launch ctrl3 "$CTRL_TYPE" "$PRIV_SUBNET" "$SG_CTRL" "$CTRL3_IP" false); save CTRL3_ID "$CTRL3_ID"
DN1_ID=$(launch dn1 "$DATA_TYPE" "$PRIV_SUBNET" "$SG_DATA" "$DN1_IP" false); save DN1_ID "$DN1_ID"
DN2_ID=$(launch dn2 "$DATA_TYPE" "$PRIV_SUBNET" "$SG_DATA" "$DN2_IP" false); save DN2_ID "$DN2_ID"
DN3_ID=$(launch dn3 "$DATA_TYPE" "$PRIV_SUBNET" "$SG_DATA" "$DN3_IP" false); save DN3_ID "$DN3_ID"
DN4_ID=$(launch dn4 "$DATA_TYPE" "$PRIV_SUBNET" "$SG_DATA" "$DN4_IP" false); save DN4_ID "$DN4_ID"

echo ">> Esperando a que las 8 instancias estén en ejecución..."
aws ec2 wait instance-running --instance-ids "$CLIENT_ID" "$CTRL1_ID" "$CTRL2_ID" "$CTRL3_ID" \
  "$DN1_ID" "$DN2_ID" "$DN3_ID" "$DN4_ID"
CLIENT_PUBLIC_IP=$(aws ec2 describe-instances --instance-ids "$CLIENT_ID" \
  --query 'Reservations[0].Instances[0].PublicIpAddress' --output text); save CLIENT_PUBLIC_IP "$CLIENT_PUBLIC_IP"
aws ec2 describe-instances --filters "Name=tag:Project,Values=$PROJECT" "Name=instance-state-name,Values=running" \
  --query 'Reservations[].Instances[].[Tags[?Key==`Name`]|[0].Value,InstanceId,PrivateIpAddress,PublicIpAddress]' --output table
echo ">> Bastión/cliente: ssh -i labsuser.pem ubuntu@$CLIENT_PUBLIC_IP"
