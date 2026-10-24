# Polaris IAM Role Setup

One-time AWS setup so Polaris can vend temporary S3 credentials via STS `AssumeRole`.

## Background

Polaris calls `sts:AssumeRole` when a client loads a table. The catalog was originally
configured with an IAM **user** ARN as `roleArn`, which STS cannot assume. This creates
a proper IAM **role** that the `watsonx-s3-connector` user can assume.

## Prerequisites

AWS CLI configured with credentials that have `iam:CreateRole`, `iam:PutRolePolicy` permissions.

## Step 1 — Create the role

```bash
aws iam create-role \
  --role-name polaris-iceberg-role \
  --assume-role-policy-document file://polaris-iceberg-role-trust-policy.json \
  --description "Assumed by Polaris to vend temporary S3 credentials for Iceberg tables"
```

Note the `Arn` in the output — it will be:
`arn:aws:iam::586643076710:role/polaris-iceberg-role`

## Step 2 — Attach the permissions policy (inline)

```bash
aws iam put-role-policy \
  --role-name polaris-iceberg-role \
  --policy-name polaris-iceberg-s3-access \
  --policy-document file://polaris-iceberg-role-permissions-policy.json
```

## Step 3 — Allow the IAM user to assume the role

Add an inline policy to `watsonx-s3-connector` so it can call `sts:AssumeRole`:

```bash
aws iam put-user-policy \
  --user-name watsonx-s3-connector \
  --policy-name allow-assume-polaris-iceberg-role \
  --policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Action": "sts:AssumeRole",
      "Resource": "arn:aws:iam::586643076710:role/polaris-iceberg-role"
    }]
  }'
```

## Step 4 — Run the bootstrap Job

Once the role exists, update the ConfigMap and re-run the bootstrap Job to
patch the `roleArn` in all Polaris catalogs:

```bash
# Update the role ARN in the ConfigMap
kubectl -n prod set data configmap/polaris-bootstrap-config \
  POLARIS_ROLE_ARN="arn:aws:iam::586643076710:role/polaris-iceberg-role"

# Re-run the job
kubectl -n prod delete job polaris-bootstrap --ignore-not-found
kubectl apply -f manifests/tableflow/polaris-bootstrap-job.yaml
kubectl -n prod logs -f job/polaris-bootstrap
```

## Step 5 — Restart tableflow pods

```bash
kubectl -n prod rollout restart \
  deployment/tableflow-postgres \
  deployment/tableflow-oracle \
  deployment/tableflow-mongodb
```

## Verification

```bash
# Check STS assume-role works directly
aws sts assume-role \
  --role-arn arn:aws:iam::586643076710:role/polaris-iceberg-role \
  --role-session-name polaris-test \
  --external-id polaris-iceberg
```
