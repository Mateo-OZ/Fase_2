#!/usr/bin/env bash
# DFSha en AWS - paso 4: balanceador interno (NLB) para las 2 puertas y nombre dfs.internal.
#   cliente --TLS--> dfs.internal:443 (NLB, paso TCP) --> ctrl1:8443 / ctrl2:8443
# El TLS termina en las puertas (el NLB no ve el contenido).
set -euo pipefail
source "$(dirname "$0")/00-variables.sh"

echo ">> Grupo de destino (health check HTTPS /healthz en 8443)"
TG_ARN=$(aws elbv2 create-target-group --name dfsha-gateways --protocol TCP --port 8443 --vpc-id "$VPC_ID" \
  --target-type instance --health-check-protocol HTTPS --health-check-path /healthz \
  --health-check-interval-seconds 10 --healthy-threshold-count 2 --unhealthy-threshold-count 2 \
  --query 'TargetGroups[0].TargetGroupArn' --output text); save TG_ARN "$TG_ARN"
aws elbv2 register-targets --target-group-arn "$TG_ARN" --targets "Id=$CTRL1_ID" "Id=$CTRL2_ID"

echo ">> NLB interno con IP fija $NLB_IP"
NLB_ARN=$(aws elbv2 create-load-balancer --name dfsha-nlb --type network --scheme internal \
  --subnet-mappings "SubnetId=$PRIV_SUBNET,PrivateIPv4Address=$NLB_IP" --security-groups "$SG_NLB" \
  --query 'LoadBalancers[0].LoadBalancerArn' --output text); save NLB_ARN "$NLB_ARN"
aws elbv2 create-listener --load-balancer-arn "$NLB_ARN" --protocol TCP --port 443 \
  --default-actions "Type=forward,TargetGroupArn=$TG_ARN" >/dev/null
aws elbv2 wait load-balancer-available --load-balancer-arns "$NLB_ARN"
read -r NLB_DNS NLB_ZONE < <(aws elbv2 describe-load-balancers --load-balancer-arns "$NLB_ARN" \
  --query 'LoadBalancers[0].[DNSName,CanonicalHostedZoneId]' --output text)
save NLB_DNS "$NLB_DNS"

echo ">> Zona privada de Route 53: $GATEWAY_NAME -> NLB"
if HZ_ID=$(aws route53 create-hosted-zone --name "$GATEWAY_NAME" --caller-reference "dfsha-$(date +%s)" \
      --vpc "VPCRegion=$AWS_REGION,VPCId=$VPC_ID" --hosted-zone-config "PrivateZone=true,Comment=DFSha" \
      --query HostedZone.Id --output text 2>/dev/null); then
  save HZ_ID "$HZ_ID"
  aws route53 change-resource-record-sets --hosted-zone-id "$HZ_ID" --change-batch "{
    \"Changes\":[{\"Action\":\"UPSERT\",\"ResourceRecordSet\":{\"Name\":\"$GATEWAY_NAME\",\"Type\":\"A\",
    \"AliasTarget\":{\"HostedZoneId\":\"$NLB_ZONE\",\"DNSName\":\"$NLB_DNS\",\"EvaluateTargetHealth\":true}}}]}" >/dev/null
  echo "   $GATEWAY_NAME resuelve al NLB dentro de la VPC"
else
  echo "   (!) Route 53 no está permitido en esta cuenta. Plan B en el bastión/cliente:"
  echo "       echo '$NLB_IP $GATEWAY_NAME' | sudo tee -a /etc/hosts"
fi
echo ">> Listo. Estado de los destinos (quedarán 'healthy' cuando las puertas estén arriba):"
aws elbv2 describe-target-health --target-group-arn "$TG_ARN" \
  --query 'TargetHealthDescriptions[].[Target.Id,TargetHealth.State]' --output table
