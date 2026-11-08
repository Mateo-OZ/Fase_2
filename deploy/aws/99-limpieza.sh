#!/usr/bin/env bash
# DFSha en AWS - borra todo lo creado por los scripts 01-04 (para no gastar créditos).
set -uo pipefail
source "$(dirname "$0")/00-variables.sh"
echo ">> Route 53"
if [[ -n "${HZ_ID:-}" ]]; then
  aws route53 change-resource-record-sets --hosted-zone-id "$HZ_ID" --change-batch "{\"Changes\":[{\"Action\":\"DELETE\",
    \"ResourceRecordSet\":$(aws route53 list-resource-record-sets --hosted-zone-id "$HZ_ID" \
    --query "ResourceRecordSets[?Type=='A']|[0]" --output json)}]}" >/dev/null
  aws route53 delete-hosted-zone --id "$HZ_ID" >/dev/null
fi
echo ">> Balanceador"
[[ -n "${NLB_ARN:-}" ]] && aws elbv2 delete-load-balancer --load-balancer-arn "$NLB_ARN" && \
  aws elbv2 wait load-balancers-deleted --load-balancer-arns "$NLB_ARN"
sleep 10
[[ -n "${TG_ARN:-}" ]] && aws elbv2 delete-target-group --target-group-arn "$TG_ARN"
echo ">> Instancias"
IDS=$(aws ec2 describe-instances --filters "Name=tag:Project,Values=$PROJECT" \
  "Name=instance-state-name,Values=pending,running,stopping,stopped" --query 'Reservations[].Instances[].InstanceId' --output text)
if [[ -n "$IDS" ]]; then aws ec2 terminate-instances --instance-ids $IDS >/dev/null; aws ec2 wait instance-terminated --instance-ids $IDS; fi
echo ">> NAT e IP elástica"
[[ -n "${NAT_ID:-}" ]] && aws ec2 delete-nat-gateway --nat-gateway-id "$NAT_ID" >/dev/null && \
  aws ec2 wait nat-gateway-deleted --nat-gateway-ids "$NAT_ID"
[[ -n "${EIP_ALLOC:-}" ]] && aws ec2 release-address --allocation-id "$EIP_ALLOC"
echo ">> Grupos de seguridad"
for sg in "${SG_CTRL:-}" "${SG_DATA:-}" "${SG_NLB:-}" "${SG_CLIENT:-}"; do
  [[ -z "$sg" ]] && continue
  aws ec2 describe-security-groups --group-ids "$sg" --query 'SecurityGroups[0].IpPermissions' --output json > /tmp/perm.json
  [[ "$(cat /tmp/perm.json)" != "[]" ]] && aws ec2 revoke-security-group-ingress --group-id "$sg" --ip-permissions file:///tmp/perm.json >/dev/null
done
for sg in "${SG_CTRL:-}" "${SG_DATA:-}" "${SG_NLB:-}" "${SG_CLIENT:-}"; do [[ -n "$sg" ]] && aws ec2 delete-security-group --group-id "$sg"; done
echo ">> Red"
[[ -n "${IGW_ID:-}" ]] && aws ec2 detach-internet-gateway --internet-gateway-id "$IGW_ID" --vpc-id "$VPC_ID" && \
  aws ec2 delete-internet-gateway --internet-gateway-id "$IGW_ID"
for s in "${PUB_SUBNET:-}" "${PRIV_SUBNET:-}"; do [[ -n "$s" ]] && aws ec2 delete-subnet --subnet-id "$s"; done
for rt in "${PUB_RT:-}" "${PRIV_RT:-}"; do [[ -n "$rt" ]] && aws ec2 delete-route-table --route-table-id "$rt"; done
[[ -n "${VPC_ID:-}" ]] && aws ec2 delete-vpc --vpc-id "$VPC_ID"
mv "$STATE_FILE" "$STATE_FILE.borrado.$(date +%s)"
echo ">> Todo borrado"
