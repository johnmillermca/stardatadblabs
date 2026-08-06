# Runbook — RBAC End-to-End Test

> **Removed** — This runbook covered Ranger-based RBAC testing which has been
> decommissioned from this platform (commit `f77006a`).
>
> **Current RBAC verification:**
>
> ```bash
> # Verify Kerberos principals and keytab secrets
> sudo bash scripts/master/17-verify-rbac.sh
> ```
>
> The script checks:
> 1. All KDC principals exist (`svc/*`, user personas)
> 2. All keytab secrets are present in the `prod` namespace
> 3. `caching_dev_user` can connect to Doris
>
> See [Runbook 11](runbook-11-kerberos-integration.md) for adding/removing users.
