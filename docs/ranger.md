# Apache Ranger

## Overview
Apache Ranger 2.4.0 — centralized security framework for fine-grained RBAC, data masking, row-level filtering, and audit logging across the data platform.

| Property | Value |
|---|---|
| Namespace | `security` |
| Admin UI | `http://192.168.1.50:30680` |
| Default credentials | admin / (from `ranger-db-credentials` secret, key `admin-password`) |
| Image | `192.168.1.50:30500/apache-ranger:2.4.0` (must be built) |
| Depends on | PostgreSQL (`databases` namespace, `ranger` database) |
| Secret | `ranger-db-credentials` |
| Manifest | `manifests/ranger/ranger-deployment.yaml` |

## Prerequisites
1. PostgreSQL deployed and `ranger` database created
2. Ranger image built and pushed to registry:
```bash
# Pull from Docker Hub and retag
docker pull apache/ranger:2.4.0
docker tag apache/ranger:2.4.0 192.168.1.50:30500/apache-ranger:2.4.0
docker push 192.168.1.50:30500/apache-ranger:2.4.0
```
3. Seed secrets:
```bash
sudo bash scripts/master/12-seed-openbao-secrets.sh
```

## Deployment (ArgoCD)
ArgoCD application: `argocd-apps/app-ranger.yaml`  
Syncs `manifests/ranger/` to the `security` namespace.

## Manual Deploy
```bash
kubectl apply -f manifests/ranger/ranger-deployment.yaml
kubectl rollout status deployment/ranger-admin -n security
```

## Verify
```bash
kubectl get pods -n security
curl -u admin:<password> http://192.168.1.50:30680/service/public/v2/api/service
```

## Secret Keys
| Key | Description |
|---|---|
| `db-user` | PostgreSQL user (`ranger`) |
| `db-password` | PostgreSQL password for ranger user |
| `db-root-password` | PostgreSQL root password (for install script) |
| `admin-password` | Ranger admin UI password |
| `tagsync-password` | Tag-sync service password |
| `usersync-password` | User-sync service password |
| `keyadmin-password` | Key admin password |

OpenBao path: `secret/data/ranger/credentials`

## Configuring Policies
1. Open `http://192.168.1.50:30680`
2. Login as `admin`
3. Add a service under **Access Manager → Service Manager**
4. Create policies under the service

## Ranger + Kerberos Integration
To protect a Kerberized service, configure the service plugin with:
- `policy.download.auth.users` = service principal
- `ranger.plugin.audit.destination.hdfs.config.conf.dir` = `/etc/krb5.conf`
- Enable SPNEGO for Ranger Admin UI

## Production Hardening
- Enable HTTPS on Ranger Admin (configure `ranger-admin-site.xml`)
- Integrate with Kerberos for admin authentication
- Enable Solr-based audit log storage
- Set up UserSync with LDAP/AD
