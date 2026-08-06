# Runbook 10 — Ranger User Management

> **Removed** — Apache Ranger has been decommissioned from this platform (commit `f77006a`).
>
> This runbook is preserved as a historical reference only. Do not follow these steps.
>
> **Current access control model:**
> - **Authentication** — Kerberos (all services). See [Runbook 11](runbook-11-kerberos-integration.md).
> - **Doris** — native SQL `CREATE USER` / `GRANT` / `REVOKE`
> - **Kafka** — GSSAPI Kerberos on port 9093; ACL enforcement removed (`allow.everyone.if.no.acl.found=true`)
> - **OpenSearch** — built-in security plugin (internal user database + SPNEGO)
>
> **Adding a new user today:**
> 1. Create KDC principal — see [Runbook 11 §4](runbook-11-kerberos-integration.md#4-adding-a-new-user)
> 2. Create Doris SQL user — `CREATE USER 'alice'@'%' IDENTIFIED BY '...';` then `GRANT ...`
> 3. Store credentials in OpenBao — `bao kv put secret/data/doris/users/alice ...`
