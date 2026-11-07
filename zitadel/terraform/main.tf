# =============================================================================
# ZITADEL Terraform — StarData platform configuration as code
# Manages: org, project, all roles, all OIDC apps, machine users, policies, branding
# Run  : terraform plan  (CI on every PR)
#        terraform apply (after review; nightly plan reports drift → /access/policy)
# Provider: zitadel/zitadel (pin to latest v2.x; confirm resource names against it)
# =============================================================================

terraform {
  required_providers {
    zitadel = {
      source  = "zitadel/zitadel"
      version = "~> 2.0"
    }
  }
  backend "s3" {
    # Store state in the on-prem MinIO / Ceph RGW bucket
    bucket                      = "stardata-terraform-state"
    key                         = "access/zitadel/terraform.tfstate"
    region                      = "us-east-1"   # dummy value required by S3 provider
    endpoint                    = "http://minio.prod.svc.cluster.local:9000"
    force_path_style            = true
    skip_credentials_validation = true
    skip_metadata_api_check     = true
  }
}

# ── Provider ─────────────────────────────────────────────────────────────────
provider "zitadel" {
  domain           = "auth.stardatadb.com"
  insecure         = false
  port             = "443"
  jwt_profile_file = var.terraform_admin_key_file
}

# ── Variables ─────────────────────────────────────────────────────────────────
variable "terraform_admin_key_file" {
  description = "Path to the terraform-admin machine user JSON key file"
  type        = string
  default     = "/run/secrets/terraform-admin-key.json"
}
variable "org_id" {
  description = "ZITADEL org ID (created by first-instance setup, read from ZITADEL console)"
  type        = string
}

# ── Organisation ──────────────────────────────────────────────────────────────
data "zitadel_org" "stardatadb" {
  id = var.org_id
}

# ── Project ───────────────────────────────────────────────────────────────────
resource "zitadel_project" "platform" {
  name                   = "stardata-platform"
  org_id                 = data.zitadel_org.stardatadb.id
  project_role_assertion = true   # put roles in tokens
  project_role_check     = false  # no-role users still sign in to reach /waiting
  has_project_check      = false
}

# ── Project Roles ─────────────────────────────────────────────────────────────
locals {
  roles = {
    # Platform
    "platform.owner"       = "Platform"
    "platform.admin"       = "Platform"
    # Doris
    "doris.viewer"         = "Doris"
    "doris.analyst"        = "Doris"
    "doris.engineer"       = "Doris"
    "doris.admin"          = "Doris"
    # Kafka
    "kafka.viewer"         = "Kafka"
    "kafka.producer"       = "Kafka"
    "kafka.engineer"       = "Kafka"
    # Memgraph / StarGraph
    "memgraph.viewer"      = "Memgraph"
    "memgraph.engineer"    = "Memgraph"
    # Flink
    "flink.viewer"         = "Flink"
    "flink.engineer"       = "Flink"
    # SQLMesh
    "sqlmesh.engineer"     = "SQLMesh"
    # ETL Studio
    "etl.viewer"           = "ETL"
    "etl.developer"        = "ETL"
    "etl.operator"         = "ETL"
    "etl.admin"            = "ETL"
    # MDM
    "mdm.viewer"           = "MDM"
    "mdm.steward"          = "MDM"
    "mdm.admin"            = "MDM"
    # Monitoring
    "monitoring.viewer"    = "Monitoring"
    "monitoring.operator"  = "Monitoring"
  }
}

resource "zitadel_project_role" "r" {
  for_each     = local.roles
  org_id       = data.zitadel_org.stardatadb.id
  project_id   = zitadel_project.platform.id
  role_key     = each.key
  display_name = each.key
  group        = each.value
}

# ── OIDC Applications ─────────────────────────────────────────────────────────

# StarData console (BFF) — confidential, PKCE, authorization code + refresh
resource "zitadel_application_oidc" "console" {
  org_id                    = data.zitadel_org.stardatadb.id
  project_id                = zitadel_project.platform.id
  name                      = "stardata-console"
  redirect_uris             = ["https://console.stardatadb.com/auth/callback"]
  post_logout_redirect_uris = ["https://console.stardatadb.com/"]
  response_types            = ["OIDC_RESPONSE_TYPE_CODE"]
  grant_types               = ["OIDC_GRANT_TYPE_AUTHORIZATION_CODE", "OIDC_GRANT_TYPE_REFRESH_TOKEN"]
  app_type                  = "OIDC_APP_TYPE_WEB"
  auth_method_type          = "OIDC_AUTH_METHOD_TYPE_BASIC"
  access_token_type         = "OIDC_TOKEN_TYPE_JWT"    # required for preaccesstoken Action
  access_token_role_assertion = true
  id_token_role_assertion   = true
  dev_mode                  = false
}

# Outputs → stored in Kubernetes Secret console-oidc (namespace access)
output "console_client_id"     { value = zitadel_application_oidc.console.client_id }
output "console_client_secret" { value = zitadel_application_oidc.console.client_secret; sensitive = true }

# API Gateway (tools.stardatadb.com)
resource "zitadel_application_oidc" "gateway" {
  org_id                    = data.zitadel_org.stardatadb.id
  project_id                = zitadel_project.platform.id
  name                      = "stardata-gateway"
  redirect_uris             = ["https://tools.stardatadb.com/oauth2/callback"]
  post_logout_redirect_uris = ["https://tools.stardatadb.com/"]
  response_types            = ["OIDC_RESPONSE_TYPE_CODE"]
  grant_types               = ["OIDC_GRANT_TYPE_AUTHORIZATION_CODE", "OIDC_GRANT_TYPE_REFRESH_TOKEN"]
  app_type                  = "OIDC_APP_TYPE_WEB"
  auth_method_type          = "OIDC_AUTH_METHOD_TYPE_BASIC"
  access_token_type         = "OIDC_TOKEN_TYPE_JWT"
  access_token_role_assertion = true
  id_token_role_assertion   = true
}

# Grafana
resource "zitadel_application_oidc" "grafana" {
  org_id                    = data.zitadel_org.stardatadb.id
  project_id                = zitadel_project.platform.id
  name                      = "stardata-grafana"
  redirect_uris             = ["https://grafana.stardatadb.com/login/generic_oauth"]
  post_logout_redirect_uris = ["https://grafana.stardatadb.com/"]
  response_types            = ["OIDC_RESPONSE_TYPE_CODE"]
  grant_types               = ["OIDC_GRANT_TYPE_AUTHORIZATION_CODE"]
  app_type                  = "OIDC_APP_TYPE_WEB"
  auth_method_type          = "OIDC_AUTH_METHOD_TYPE_BASIC"
  access_token_type         = "OIDC_TOKEN_TYPE_JWT"
  access_token_role_assertion = true
  id_token_role_assertion   = true
}

# Argo CD
resource "zitadel_application_oidc" "argocd" {
  org_id                    = data.zitadel_org.stardatadb.id
  project_id                = zitadel_project.platform.id
  name                      = "stardata-argocd"
  redirect_uris             = ["https://argocd.stardatadb.com/auth/callback"]
  post_logout_redirect_uris = ["https://argocd.stardatadb.com/"]
  response_types            = ["OIDC_RESPONSE_TYPE_CODE"]
  grant_types               = ["OIDC_GRANT_TYPE_AUTHORIZATION_CODE"]
  app_type                  = "OIDC_APP_TYPE_WEB"
  auth_method_type          = "OIDC_AUTH_METHOD_TYPE_BASIC"
  access_token_type         = "OIDC_TOKEN_TYPE_JWT"
  access_token_role_assertion = true
  id_token_role_assertion   = true
}

# Platform CLI — native, PKCE, no secret
resource "zitadel_application_oidc" "platform_cli" {
  org_id                    = data.zitadel_org.stardatadb.id
  project_id                = zitadel_project.platform.id
  name                      = "platform-cli"
  redirect_uris             = ["http://127.0.0.1:8085/callback"]
  post_logout_redirect_uris = ["http://127.0.0.1:8085/"]
  response_types            = ["OIDC_RESPONSE_TYPE_CODE"]
  grant_types               = ["OIDC_GRANT_TYPE_AUTHORIZATION_CODE", "OIDC_GRANT_TYPE_DEVICE_CODE", "OIDC_GRANT_TYPE_REFRESH_TOKEN"]
  app_type                  = "OIDC_APP_TYPE_NATIVE"
  auth_method_type          = "OIDC_AUTH_METHOD_TYPE_NONE"
  access_token_type         = "OIDC_TOKEN_TYPE_JWT"
  access_token_role_assertion = true
  id_token_role_assertion   = true
}

# ── Machine Users ─────────────────────────────────────────────────────────────

# console-backend machine user — manages users and authorizations on behalf of the BFF
resource "zitadel_machine_user" "console_backend" {
  org_id      = data.zitadel_org.stardatadb.id
  user_name   = "console-backend"
  name        = "Console Backend Service"
  description = "BFF machine user: Members, Invite, Applications APIs"
  access_token_type = "ACCESS_TOKEN_TYPE_JWT"
}

# audit-reader — used by audit-forwarder for event backfill
resource "zitadel_machine_user" "audit_reader" {
  org_id      = data.zitadel_org.stardatadb.id
  user_name   = "audit-reader"
  name        = "Audit Reader"
  description = "Read-only access to ZITADEL events for audit-forwarder backfill"
  access_token_type = "ACCESS_TOKEN_TYPE_JWT"
}

# ── Policies ─────────────────────────────────────────────────────────────────

resource "zitadel_login_policy" "default" {
  org_id                  = data.zitadel_org.stardatadb.id
  allow_register          = true
  allow_username_password = true
  allow_external_idps     = false
  force_mfa               = false       # MFA for Owners/Admins enforced by BFF amr check
  passwordless_type       = "PASSWORDLESS_TYPE_ALLOWED"
  hide_password_reset     = false
  # Prevents user enumeration: ZITADEL returns the same error whether the
  # email exists or not — matches the sign-in screenshot behaviour.
  ignore_unknown_usernames = true
  default_redirect_uri    = "https://console.stardatadb.com/"
  password_check_lifetime           = "604800s"   # 7 days
  external_login_check_lifetime     = "604800s"
  mfa_init_skip_lifetime            = "0s"        # never skip MFA init for enrolled users
  second_factor_check_lifetime      = "43200s"    # 12 hours (re-ask after 12 h idle)
  multi_factor_check_lifetime       = "43200s"
  user_login_must_be_domain         = false
  # Second-factor types shown in the Login v2 step-2 screen.
  # TOTP = authenticator app 6-digit code (RFC 6238)
  # U2F  = security key / passkey as second factor
  second_factors = ["SECOND_FACTOR_TYPE_OTP", "SECOND_FACTOR_TYPE_U2F"]
  # Multi-factor type: passkey with user-verification (Touch ID, Windows Hello)
  multi_factors  = ["MULTI_FACTOR_TYPE_U2F_WITH_VERIFICATION"]
}

resource "zitadel_password_complexity_policy" "default" {
  org_id         = data.zitadel_org.stardatadb.id
  min_length     = 12
  has_lowercase  = true
  has_uppercase  = false   # spec: uppercase NOT required (matches the spec table)
  has_number     = true
  has_symbol     = true
}

resource "zitadel_lockout_policy" "default" {
  org_id               = data.zitadel_org.stardatadb.id
  max_password_attempts = 5
  show_lockout_failure  = true
}

# ── Branding ──────────────────────────────────────────────────────────────────
resource "zitadel_label_policy" "default" {
  org_id                = data.zitadel_org.stardatadb.id
  primary_color         = "#3346C4"
  background_color      = "#FFFFFF"
  warn_color            = "#F59E0B"
  font_color            = "#1F2328"
  primary_color_dark    = "#3346C4"
  background_color_dark = "#0D1117"
  warn_color_dark       = "#F59E0B"
  font_color_dark       = "#F0F6FC"
  # hide_login_name_suffix = true: people type john@stardatadb.com, not
  # john@stardatadb.auth.stardatadb.com — matches the email field in the screenshot.
  hide_login_name_suffix = true
  # disable_watermark removes the "Secured by ZITADEL" badge from the login page.
  disable_watermark     = true
  # set_active applies the policy immediately; without this the policy is saved
  # but the Login v2 page keeps the previous branding until it is activated.
  set_active            = true
}

# ── Privacy Policy ────────────────────────────────────────────────────────────
resource "zitadel_privacy_policy" "default" {
  org_id       = data.zitadel_org.stardatadb.id
  tos_link     = "https://www.stardatadb.com/terms"
  privacy_link = "https://www.stardatadb.com/privacy"
  help_link    = "https://www.stardatadb.com/help"
}

# ── SMTP ─────────────────────────────────────────────────────────────────────
# SMTP credentials stored outside Terraform (apply with -var or CI secret)
variable "smtp_password" {
  description = "SMTP relay password for no-reply@stardatadb.com"
  type        = string
  sensitive   = true
}

resource "zitadel_smtp_config" "default" {
  instance_id  = data.zitadel_org.stardatadb.id
  sender_address = "no-reply@stardatadb.com"
  sender_name    = "StarData"
  tls            = true
  host           = "smtp-relay.prod.svc.cluster.local:587"
  user           = "no-reply@stardatadb.com"
  password       = var.smtp_password
}
