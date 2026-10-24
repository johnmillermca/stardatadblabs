#!/usr/bin/env bash
# =============================================================================
# setup-polaris-iam-role.sh
# =========================
# Creates the polaris-iceberg-role IAM role and wires it into Polaris and
# the K8s bootstrap ConfigMap.
#
# Prerequisites:
#   - AWS CLI configured with credentials that have iam:CreateRole,
#     iam:PutRolePolicy, iam:PutUserPolicy (run from admin workstation)
#   - kubectl pointing at the prod cluster
#
# Usage:
#   bash manifests/tableflow/iam/setup-polaris-iam-role.sh
# =============================================================================
set -euo pipefail

ROLE_NAME="polaris-iceberg-role"
USER_NAME="watsonx-s3-connector"
ACCOUNT_ID="586643076710"
REGION="us-east-2"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "==> [1/5] Creating IAM role: ${ROLE_NAME}"
ROLE_ARN=$(aws iam create-role \
  --role-name "${ROLE_NAME}" \
  --assume-role-policy-document "file://${SCRIPT_DIR}/polaris-iceberg-role-trust-policy.json" \
  --description "Assumed by Polaris to vend temporary S3 credentials for Iceberg tables" \
  --query 'Role.Arn' --output text 2>&1) || {
    # Role may already exist — fetch it instead
    echo "    create-role failed, checking if role already exists..."
    ROLE_ARN=$(aws iam get-role --role-name "${ROLE_NAME}" --query 'Role.Arn' --output text)
  }
echo "    Role ARN: ${ROLE_ARN}"

echo "==> [2/5] Attaching S3 permissions policy to role"
aws iam put-role-policy \
  --role-name "${ROLE_NAME}" \
  --policy-name polaris-iceberg-s3-access \
  --policy-document "file://${SCRIPT_DIR}/polaris-iceberg-role-permissions-policy.json"
echo "    Done."

echo "==> [3/5] Granting sts:AssumeRole to IAM user ${USER_NAME}"
aws iam put-user-policy \
  --user-name "${USER_NAME}" \
  --policy-name allow-assume-polaris-iceberg-role \
  --policy-document "{
    \"Version\": \"2012-10-17\",
    \"Statement\": [{
      \"Effect\": \"Allow\",
      \"Action\": \"sts:AssumeRole\",
      \"Resource\": \"${ROLE_ARN}\"
    }]
  }"
echo "    Done."

echo "==> [4/5] Verifying sts:AssumeRole (needs a few seconds for IAM propagation)"
sleep 10
aws sts assume-role \
  --role-arn "${ROLE_ARN}" \
  --role-session-name polaris-verify \
  --external-id polaris-iceberg \
  --query 'Credentials.AccessKeyId' --output text
echo "    STS assume-role: OK"

echo "==> [5/5] Patching polaris-bootstrap-config in Kubernetes and re-running Job"
kubectl -n prod patch configmap polaris-bootstrap-config \
  --type merge \
  -p "{\"data\":{\"POLARIS_ROLE_ARN\":\"${ROLE_ARN}\"}}"

kubectl -n prod delete job polaris-bootstrap --ignore-not-found
# Re-apply to create a fresh job (ArgoCD will also pick this up)
kubectl apply -f "${SCRIPT_DIR}/../polaris-bootstrap-job.yaml"

echo "    Waiting for bootstrap Job to complete..."
kubectl -n prod wait --for=condition=complete job/polaris-bootstrap --timeout=120s
kubectl -n prod logs job/polaris-bootstrap

echo ""
echo "==> Restarting tableflow deployments"
kubectl -n prod rollout restart \
  deployment/tableflow-postgres \
  deployment/tableflow-oracle \
  deployment/tableflow-mongodb

echo ""
echo "================================================================"
echo " Done. Role ARN: ${ROLE_ARN}"
echo " Watch pods with: kubectl -n prod get pods -l app=tableflow -w"
echo "================================================================"
