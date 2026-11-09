# StarData Access — Authentication Checkpoint

**Date:** 2026-11-08  
**Status:** ✅ Working — `www.stardatadb.com` → `auth.stardatadb.com/ui/v2/login` → `console.stardatadb.com`

---

## What is live and working

### Sign-in flow (end-to-end verified)
| Step | URL | Who owns it |
|------|-----|-------------|
| Landing | `https://www.stardatadb.com` | Cloudflare / static site |
| Login redirect | `https://auth.stardatadb.com/ui/v2/login` | ZITADEL v4 Login v2 (on-prem) |
| MFA (Owners + Admins) | same host, step 2 | ZITADEL Login v2 |
| Callback | `https://console.stardatadb.com/auth/callback` | console-backend BFF (`stardata-access/console-backend`) |
| Console | `https://console.stardatadb.com` | same BFF + `stardata-console.html` |

### ZITADEL deployment
- **Chart:** `charts.zitadel.com` chart `zitadel` v9.x  
- **Namespace:** `access`  
- **Database:** CloudNativePG cluster `zitadel-db`, PostgreSQL 16, 3 instances  
- **Replicas:** 3 ZITADEL pods, 2 Login v2 pods (PDB: minAvailable 2)  
- **TLS:** cert-manager certificate `zitadel-tls` (company CA issuer); TLS terminates at ingress  
- **Masterkey:** Kubernetes Secret `zitadel-masterkey` — **never change after install**  
- **Argo CD:** Application defined in `deploy/argocd/`; values file `zitadel/helm/values-prod.yaml`

### Terraform configuration (all applied, plan clean)
- **File:** `zitadel/terraform/main.tf`  
- **State:** MinIO bucket `stardata-terraform-state`, key `access/zitadel/terraform.tfstate`  
- **Provider:** `zitadel/zitadel ~> 2.0`  
- **What it manages:** org `stardatadb`, project `stardata-platform`, all 24 project roles,  
  5 OIDC apps (`stardata-console`, `stardata-gateway`, `stardata-grafana`, `stardata-argocd`, `platform-cli`),  
  2 machine users (`console-backend`, `audit-reader`),  
  login / password-complexity / lockout / label / privacy / SMTP policies

### Policies in effect
| Policy | Setting |
|--------|---------|
| Self sign-up | On |
| Email verification | Required before first sign-in |
| MFA | Required for Owners + Admins (enforced by BFF `amr` claim check) |
| Passkeys | Allowed for everyone |
| Password | 12+ chars, number, symbol |
| Lockout | After 5 wrong passwords |
| Session | Idle 1 day / max 7 days |
| Access token | JWT, 10 minutes |
| Branding | StarData logo, `#3346C4`, Inter font, no "Secured by ZITADEL" |

### Console backend (BFF)
- **Location:** `stardata-access/console-backend/`  
- **Auth routes:** `internal/auth/login.go`, `callback.go`, `middleware.go`  
- **Session store:** Valkey (Redis-compatible), key pattern `sess:<id>`, TTL 24 h idle / 7 d absolute  
- **Cookie:** `__Host-stardata_session`, HttpOnly, Secure, SameSite=Lax  
- **OIDC app:** `stardata-console` — client ID + secret in Kubernetes Secret `console-oidc` (namespace `access`)  
- **Claims action target:** `stardata-claims` service (`stardata-access/claims/main.go`) — called on `preaccesstoken` and `preuserinfo`; adds `doris_role`, `kafka_role`, `memgraph_role`, `flink_role`, `sqlmesh_role`, `etl_role`, `mdm_role`, `monitoring_role`, `platform_roles` to every JWT

### stardata-claims service
- **Language:** Go, standard-library HTTP  
- **Namespace:** `access`, 2 replicas  
- **NetworkPolicy:** only ZITADEL pods may call it  
- **Signing key:** Kubernetes Secret `claims-target-key` (HMAC; verify `ZITADEL-Signature` header)

### Role catalogue (ZITADEL project `stardata-platform`)
| Component | Role keys | Claim |
|-----------|-----------|-------|
| Platform | `platform.owner`, `platform.admin` | `platform_roles` (array) |
| StarData (Doris) | `doris.viewer/analyst/engineer/admin` | `doris_role` |
| Kafka | `kafka.viewer/producer/engineer` | `kafka_role` |
| StarGraph (Memgraph) | `memgraph.viewer/engineer` | `memgraph_role` |
| Flink | `flink.viewer/engineer` | `flink_role` |
| SQLMesh | `sqlmesh.engineer` | `sqlmesh_role` |
| ETL Studio | `etl.viewer/developer/operator/admin` | `etl_role` |
| MDM | `mdm.viewer/steward/admin` | `mdm_role` |
| Monitoring | `monitoring.viewer/operator` | `monitoring_role` |

> **Role keys in ZITADEL use `doris.*` / `memgraph.*` prefixes — do not rename them.**  
> Display labels in the console show "StarData" instead of "Doris" and "StarGraph" instead of "Memgraph".

---

## What is NOT yet deployed (next steps)

| Item | Location | Status |
|------|----------|--------|
| `access` routes in console backend | `console-backend/internal/access/` | Not yet wired — screens shown from `stardata-console.html` |
| React access pages | `web/src/pages/access/` | Built, not bundled into the deployed console yet |
| `audit-forwarder` | `stardata-access/audit-forwarder/` | Not created yet |
| Console database migrations | `stardata-access/migrations/` | Not created yet |
| `platform_audit` Doris table | `stardata-access/setup/platform_audit.sql` | Created, not yet seeded |
| Terraform `stardata-access` Helm chart Argo CD app | `deploy/argocd/` | Not created yet |

---

## Breaking this setup

**Do not do any of the following without a plan + second Owner approval:**
- Change the ZITADEL masterkey (`zitadel-masterkey` secret) — will corrupt the database  
- Delete or recreate the `stardata-platform` ZITADEL project — will invalidate all tokens  
- Change `redirect_uris` on `stardata-console` app without updating the BFF env var  
- Apply Terraform changes to login/lockout policies without testing on staging first  
- Remove `project_role_assertion = true` from the project — roles will stop appearing in JWTs  
- Delete the `console-backend` machine user — BFF will lose its ZITADEL management credentials  
