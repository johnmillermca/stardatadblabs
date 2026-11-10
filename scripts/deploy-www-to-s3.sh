#!/usr/bin/env bash
# =============================================================================
# deploy-www-to-s3.sh
# Deploys the www.stardatadb.com landing page (index.html) to S3 as a static
# website.  Bucket: stardatadb-www   Region: us-east-1
#
# Usage:
#   ./scripts/deploy-www-to-s3.sh
#
# Requirements:
#   aws CLI v2  — https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html
#
# HMAC credentials are read from environment variables (set them in your shell
# or CI secrets — never hard-code in this file):
#   export AWS_ACCESS_KEY_ID=...
#   export AWS_SECRET_ACCESS_KEY=...
#   export AWS_DEFAULT_REGION=us-east-1
# =============================================================================
set -euo pipefail

BUCKET="stardatadb-web"
REGION="us-east-2"
SOURCE_HTML="docker/webconsole/index.html"

# ── Validate ──────────────────────────────────────────────────────────────────
if [[ ! -f "${SOURCE_HTML}" ]]; then
  echo "ERROR: ${SOURCE_HTML} not found. Run this script from the k8s_platform root." >&2
  exit 1
fi

if [[ -z "${AWS_ACCESS_KEY_ID:-}" || -z "${AWS_SECRET_ACCESS_KEY:-}" ]]; then
  echo "ERROR: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set." >&2
  exit 1
fi

export AWS_DEFAULT_REGION="${REGION}"

echo "==> Ensuring bucket ${BUCKET} exists in ${REGION}…"
aws s3api head-bucket --bucket "${BUCKET}" 2>/dev/null || \
  aws s3api create-bucket \
    --bucket "${BUCKET}" \
    --region "${REGION}" \
    --create-bucket-configuration LocationConstraint="${REGION}" 2>/dev/null || true

# Remove the default "block all public access" settings so the bucket can host a website
echo "==> Disabling Block Public Access on ${BUCKET}…"
aws s3api put-public-access-block \
  --bucket "${BUCKET}" \
  --public-access-block-configuration \
    "BlockPublicAcls=false,IgnorePublicAcls=false,BlockPublicPolicy=false,RestrictPublicBuckets=false"

echo "==> Applying public-read bucket policy…"
aws s3api put-bucket-policy --bucket "${BUCKET}" --policy '{
  "Version": "2012-10-17",
  "Statement": [{
    "Sid": "PublicReadGetObject",
    "Effect": "Allow",
    "Principal": "*",
    "Action": "s3:GetObject",
    "Resource": "arn:aws:s3:::'"${BUCKET}"'/*"
  }]
}'

echo "==> Enabling static website hosting on ${BUCKET}…"
aws s3api put-bucket-website \
  --bucket "${BUCKET}" \
  --website-configuration '{
    "IndexDocument": {"Suffix": "index.html"},
    "ErrorDocument": {"Key": "index.html"}
  }'

echo "==> Uploading index.html to s3://${BUCKET}/index.html…"
aws s3 cp "${SOURCE_HTML}" "s3://${BUCKET}/index.html" \
  --content-type "text/html; charset=utf-8" \
  --cache-control "no-cache, no-store, must-revalidate" \
  --metadata-directive REPLACE

WEBSITE_URL="http://${BUCKET}.s3-website-${REGION}.amazonaws.com"
echo "==> Done.  Website URL: ${WEBSITE_URL}"
echo "==> Point Cloudflare www.stardatadb.com CNAME to: ${BUCKET}.s3-website-${REGION}.amazonaws.com"
