# Apache Ranger

> **Removed** — Apache Ranger has been decommissioned from this platform.
>
> Authorization is now handled natively:
> - **Doris** — built-in Doris SQL `GRANT`/`REVOKE` privilege system
> - **Kafka** — Kerberos GSSAPI authentication on port 9093; `allow.everyone.if.no.acl.found=true`
> - **OpenSearch** — built-in security plugin with internal user database and SPNEGO authentication
>
> **What was removed (commit `f77006a`):**
> - `docker/ranger/` — Ranger Admin custom image
> - `docker/doris-ranger/` — Doris FE + Ranger Hive plugin image
> - `docker/strimzi-kafka-ranger/` — Kafka + `RangerKafkaAuthorizer` image
> - `manifests/ranger/ranger-deployment.yaml`
> - `argocd-apps/app-ranger.yaml`
> - `helm/ranger/values.yaml`
> - `scripts/master/16-seed-ranger-rbac.sh`
> - PostgreSQL `ranger` database and `ranger` user
> - `apache-ranger:2.7.0` image from private registry
>
> See [docs/kerberos.md](kerberos.md) for authentication and
> [docs/runbooks/runbook-11-kerberos-integration.md](runbooks/runbook-11-kerberos-integration.md)
> for adding users to the platform.
