#!/usr/bin/env bash
# DFSha en AWS - paso 1: VPC con subred pública (bastión + NAT) y subred privada (nodos).
set -euo pipefail
source "$(dirname "$0")/00-variables.sh"

echo ">> VPC $VPC_CIDR"
VPC_ID=$(aws ec2 create-vpc --cidr-block "$VPC_CIDR" --tag-specifications "$(tag vpc dfsha-vpc)" \
  --query Vpc.VpcId --output text); save VPC_ID "$VPC_ID"
aws ec2 modify-vpc-attribute --vpc-id "$VPC_ID" --enable-dns-support '{"Value":true}'
aws ec2 modify-vpc-attribute --vpc-id "$VPC_ID" --enable-dns-hostnames '{"Value":true}'

echo ">> Subredes en $AZ"
PUB_SUBNET=$(aws ec2 create-subnet --vpc-id "$VPC_ID" --cidr-block "$PUB_CIDR" --availability-zone "$AZ" \
  --tag-specifications "$(tag subnet dfsha-publica)" --query Subnet.SubnetId --output text); save PUB_SUBNET "$PUB_SUBNET"
PRIV_SUBNET=$(aws ec2 create-subnet --vpc-id "$VPC_ID" --cidr-block "$PRIV_CIDR" --availability-zone "$AZ" \
  --tag-specifications "$(tag subnet dfsha-privada)" --query Subnet.SubnetId --output text); save PRIV_SUBNET "$PRIV_SUBNET"

echo ">> Internet Gateway y tabla de rutas pública"
IGW_ID=$(aws ec2 create-internet-gateway --tag-specifications "$(tag internet-gateway dfsha-igw)" \
  --query InternetGateway.InternetGatewayId --output text); save IGW_ID "$IGW_ID"
aws ec2 attach-internet-gateway --internet-gateway-id "$IGW_ID" --vpc-id "$VPC_ID"
PUB_RT=$(aws ec2 create-route-table --vpc-id "$VPC_ID" --tag-specifications "$(tag route-table dfsha-rt-publica)" \
  --query RouteTable.RouteTableId --output text); save PUB_RT "$PUB_RT"
aws ec2 create-route --route-table-id "$PUB_RT" --destination-cidr-block 0.0.0.0/0 --gateway-id "$IGW_ID" >/dev/null
aws ec2 associate-route-table --route-table-id "$PUB_RT" --subnet-id "$PUB_SUBNET" >/dev/null

echo ">> NAT Gateway (la subred privada sale a Internet solo para apt, Docker Hub, PyPI y GitHub)"
EIP_ALLOC=$(aws ec2 allocate-address --domain vpc --tag-specifications "$(tag elastic-ip dfsha-nat-eip)" \
  --query AllocationId --output text); save EIP_ALLOC "$EIP_ALLOC"
NAT_ID=$(aws ec2 create-nat-gateway --subnet-id "$PUB_SUBNET" --allocation-id "$EIP_ALLOC" \
  --tag-specifications "$(tag natgateway dfsha-nat)" --query NatGateway.NatGatewayId --output text); save NAT_ID "$NAT_ID"
echo "   esperando a que el NAT quede disponible (1-2 min)..."
aws ec2 wait nat-gateway-available --nat-gateway-ids "$NAT_ID"
PRIV_RT=$(aws ec2 create-route-table --vpc-id "$VPC_ID" --tag-specifications "$(tag route-table dfsha-rt-privada)" \
  --query RouteTable.RouteTableId --output text); save PRIV_RT "$PRIV_RT"
aws ec2 create-route --route-table-id "$PRIV_RT" --destination-cidr-block 0.0.0.0/0 --nat-gateway-id "$NAT_ID" >/dev/null
aws ec2 associate-route-table --route-table-id "$PRIV_RT" --subnet-id "$PRIV_SUBNET" >/dev/null
echo ">> Red lista"
