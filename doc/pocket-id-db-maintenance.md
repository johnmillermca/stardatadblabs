# Pocket ID — Database Maintenance Guide

> **Scope:** Day-to-day and periodic administration of the Pocket ID SQLite database running
> on `worker1.local` inside the `access` namespace.  
> For lockout recovery steps see [pocket-id-recovery.md](./pocket-id-recovery.md).

---

## Database Location

| Item | Value |
|------|-------|
| Pod | `deployment/pocket-id` in namespace `access` |
| Node | `worker1.local` (`192.168.1.51`) |
| PVC | `pocket-id-data` |
| PV path on worker1 | `/home/local-path-provisioner/pvc-e3a35cc0-6219-4ba5-ade3-099311bef67e_access_pocket-id-data/` |
| DB file inside pod | `/app/data/pocket-id.db` |
| WAL files | `pocket-id.db-shm`, `pocket-id.db-wal` (present while pod is running) |
| Engine | SQLite 3, WAL journal mode |

---

## Table Reference

| Table | Contents |
|-------|----------|
| `users` | All user accounts (username, email, is_admin, disabled) |
| `webauthn_credentials` | Registered passkeys per user |
| `webauthn_sessions` | In-progress WebAuthn handshakes (short-lived) |
| `oidc_clients` | OAuth2/OIDC application registrations |
| `oauth2_sessions` | Active access/refresh token sessions |
| `interaction_sessions` | In-progress OIDC authorization flows |
| `audit_logs` | Sign-in, passkey, and client events |
| `api_keys` | API keys (for SCIM / CI access) |
| `user_groups` / `user_groups_users` | Group memberships |
| `reauthentication_tokens` | Short-lived re-auth tokens |
| `kv` | Internal key-value store (instance ID, JWT key, session key) |
| `storage` | Binary blobs (uploaded images) |
| `francis_actor_state` | Actor framework state (device-login, one-time-access, cron jobs, AppConfig) |
| `francis_alarms` / `francis_*` | Internal actor scheduler tables |
| `schema_migrations` | Applied migration versions |

---

## Rule #1 — Never Write to a Running Pod's DB File Directly

Pocket ID uses SQLite **WAL mode**. The running process owns the WAL.
Writing the base `.db` file while the pod is up creates an inconsistent state and causes:

- `database disk image is malformed` errors
- `corrupt database (11)` errors in actor subsystem
- CrashLoopBackOff on next restart

**Always follow the safe procedure:**

```
scale down → copy out → modify → copy back → scale up
```

---

## Safe DB Edit Procedure

Use this wrapper for **any** manual DB change.

```bash
NS=access
DEPLOY=pocket-id
PVC=pocket-id-data
NODE=worker1.local
REGISTRY=192.168.1.50:30500

# 1. Scale down
kubectl -n $NS scale deployment $DEPLOY --replicas=0
sleep 5

# 2. Spin up a helper pod on the same node (so PVC is accessible)
kubectl -n $NS run pid-maint --restart=Never \
  --image=$REGISTRY/pocket-id:latest \
  --overrides='{
    "spec":{
      "nodeSelector":{"kubernetes.io/hostname":"'$NODE'"},
      "containers":[{
        "name":"pid-maint",
        "image":"'$REGISTRY'/pocket-id:latest",
        "command":["sleep","300"],
        "resources":{"requests":{"cpu":"50m","memory":"64Mi"},
                     "limits":{"cpu":"200m","memory":"128Mi"}},
        "volumeMounts":[{"name":"data","mountPath":"/data"}]
      }],
      "volumes":[{"name":"data","persistentVolumeClaim":{"claimName":"'$PVC'"}}]
    }
  }'

kubectl -n $NS wait pod/pid-maint --for=condition=Ready --timeout=60s

# 3. Copy DB out (with WAL files)
kubectl -n $NS cp pid-maint:/data/pocket-id.db     /tmp/pid-maint.db
kubectl -n $NS cp pid-maint:/data/pocket-id.db-shm /tmp/pid-maint.db-shm 2>/dev/null || true
kubectl -n $NS cp pid-maint:/data/pocket-id.db-wal /tmp/pid-maint.db-wal 2>/dev/null || true

# 4. Merge WAL into base file and verify
sqlite3 /tmp/pid-maint.db "PRAGMA wal_checkpoint(TRUNCATE);"
sqlite3 /tmp/pid-maint.db "PRAGMA integrity_check;"   # must print "ok"

# --- MAKE YOUR CHANGES HERE ---
# sqlite3 /tmp/pid-maint.db "..."

# 5. Push modified DB back
kubectl -n $NS cp /tmp/pid-maint.db pid-maint:/data/pocket-id.db
kubectl -n $NS exec pid-maint -- sh -c \
  "rm -f /data/pocket-id.db-shm /data/pocket-id.db-wal && echo OK"

# 6. Tear down helper pod and scale up
kubectl -n $NS delete pod pid-maint --force
kubectl -n $NS scale deployment $DEPLOY --replicas=1
kubectl -n $NS rollout status deployment/$DEPLOY --timeout=90s
```

---

## Backup

### Manual snapshot (run any time)

```bash
# Option A — copy via kubectl (pod must be running; read-only safe because SQLite allows concurrent readers)
NEW_POD=$(kubectl -n access get pods -l app=pocket-id -o jsonpath='{.items[0].metadata.name}')
kubectl -n access cp $NEW_POD:/app/data/pocket-id.db /tmp/pocket-id-$(date +%Y%m%d-%H%M).db

# Option B — SQLite online backup (consistent snapshot, no downtime)
# Run from the node that has the PV mounted
PV_PATH=/home/local-path-provisioner/pvc-e3a35cc0-6219-4ba5-ade3-099311bef67e_access_pocket-id-data
sqlite3 $PV_PATH/pocket-id.db ".backup /opt/backups/pocket-id-$(date +%Y%m%d-%H%M).db"
```

### Automated daily backup (cron on worker1)

SSH into worker1 and add a cron entry:

```bash
ssh 192.168.1.51

# Create backup directory
mkdir -p /opt/backups/pocket-id

# Add cron job (runs at 02:00 daily, keeps 30 days)
crontab -e
```

Add this line:

```cron
0 2 * * * PV=/home/local-path-provisioner/pvc-e3a35cc0-6219-4ba5-ade3-099311bef67e_access_pocket-id-data && sqlite3 $PV/pocket-id.db ".backup /opt/backups/pocket-id/pocket-id-$(date +\%Y\%m\%d).db" && find /opt/backups/pocket-id/ -name "*.db" -mtime +30 -delete
```

### Restore from backup

```bash
# Follow the Safe DB Edit Procedure above, but at step 4 replace:
cp /opt/backups/pocket-id/pocket-id-YYYYMMDD.db /tmp/pid-maint.db
sqlite3 /tmp/pid-maint.db "PRAGMA integrity_check;"
# Then continue from step 5 (copy back, scale up)
```

---

## Integrity Check

Run after any manual change or after a crash:

```bash
sqlite3 /tmp/pid-maint.db "PRAGMA integrity_check;"
# Expected output: ok
# If output is anything else, restore from backup.

sqlite3 /tmp/pid-maint.db "PRAGMA foreign_key_check;"
# Expected output: (empty — no violations)
```

---

## WAL Checkpoint

The WAL file grows over time. Pocket ID checkpoints it automatically, but you can force it:

```bash
# While pod is RUNNING (safe, read-only)
NEW_POD=$(kubectl -n access get pods -l app=pocket-id -o jsonpath='{.items[0].metadata.name}')
# Checkpoint is handled internally by the pod; just verify the WAL size
kubectl -n access exec $NEW_POD -- sh -c "ls -lh /app/data/"

# While pod is STOPPED (full truncation)
sqlite3 /tmp/pid-maint.db "PRAGMA wal_checkpoint(TRUNCATE);"
```

WAL files larger than ~50 MB indicate heavy write load or a checkpoint failure — investigate.

---

## Vacuuming (Reclaim Disk Space)

SQLite does not release disk space automatically after deletes. Run VACUUM periodically
(requires pod to be stopped):

```bash
# While pod is stopped, after copying DB out:
sqlite3 /tmp/pid-maint.db "VACUUM;"
sqlite3 /tmp/pid-maint.db "PRAGMA integrity_check;"
# Then push back using the Safe DB Edit Procedure
```

Run after bulk deletes (e.g. wiping audit logs or expired sessions).

---

## Common Maintenance Tasks

### View all users

```bash
sqlite3 /tmp/pid-maint.db \
  "SELECT username, email, is_admin, disabled, created_at FROM users ORDER BY created_at;"
```

### Disable a user (without deleting)

```bash
sqlite3 /tmp/pid-maint.db \
  "UPDATE users SET disabled=1 WHERE username='johndoe';"
```

### Re-enable a user

```bash
sqlite3 /tmp/pid-maint.db \
  "UPDATE users SET disabled=0 WHERE username='johndoe';"
```

### Promote a user to admin

```bash
sqlite3 /tmp/pid-maint.db \
  "UPDATE users SET is_admin=1 WHERE username='johndoe';"
```

### List registered passkeys

```bash
sqlite3 /tmp/pid-maint.db \
  "SELECT u.username, w.name, w.created_at, w.backup_eligible
   FROM webauthn_credentials w JOIN users u ON w.user_id=u.id
   ORDER BY u.username;"
```

### Delete passkeys for a user (force re-registration)

```bash
USER_ID=$(sqlite3 /tmp/pid-maint.db "SELECT id FROM users WHERE username='johndoe';")
sqlite3 /tmp/pid-maint.db \
  "DELETE FROM webauthn_credentials WHERE user_id='$USER_ID';"
```

### View OIDC clients

```bash
sqlite3 /tmp/pid-maint.db \
  "SELECT id, name, callback_urls, launch_url FROM oidc_clients;"
```

### View active OAuth2 sessions

```bash
sqlite3 /tmp/pid-maint.db \
  "SELECT id, client_id, kind, created_at, expires_at
   FROM oauth2_sessions
   WHERE expires_at > strftime('%s','now')
   ORDER BY created_at DESC LIMIT 20;"
```

### Purge expired OAuth2 sessions manually

```bash
sqlite3 /tmp/pid-maint.db \
  "DELETE FROM oauth2_sessions WHERE expires_at < strftime('%s','now');"
```

### Trim audit logs older than 90 days

```bash
sqlite3 /tmp/pid-maint.db \
  "DELETE FROM audit_logs WHERE created_at < strftime('%s','now','-90 days');"
sqlite3 /tmp/pid-maint.db "VACUUM;"
```

### View AppConfig (SMTP, feature flags, etc.)

```bash
sqlite3 /tmp/pid-maint.db \
  "SELECT hex(actor_state_data) FROM francis_actor_state WHERE actor_type='AppConfig';" \
  | python3 -c "
import sys, binascii, msgpack
h = sys.stdin.read().strip()
d = msgpack.unpackb(binascii.unhexlify(h), raw=False)
for k, v in sorted(d.items()):
    print(f'{k:45s} = {v}')
"
```

### Check schema migration version

```bash
sqlite3 /tmp/pid-maint.db \
  "SELECT version, dirty FROM schema_migrations ORDER BY version DESC LIMIT 1;"
# dirty=1 means a migration was interrupted — dangerous, may need restore
```

---

## Database Size Monitoring

```bash
# Current DB file size
kubectl -n access exec $NEW_POD -- sh -c "ls -lh /app/data/"

# Table row counts
sqlite3 /tmp/pid-maint.db "
SELECT 'users'            , COUNT(*) FROM users            UNION ALL
SELECT 'webauthn_credentials', COUNT(*) FROM webauthn_credentials UNION ALL
SELECT 'oidc_clients'    , COUNT(*) FROM oidc_clients     UNION ALL
SELECT 'oauth2_sessions' , COUNT(*) FROM oauth2_sessions  UNION ALL
SELECT 'audit_logs'      , COUNT(*) FROM audit_logs       UNION ALL
SELECT 'interaction_sessions', COUNT(*) FROM interaction_sessions;
"
```

Expected healthy sizes for a small deployment:

| Table | Normal row count |
|-------|-----------------|
| `users` | < 100 |
| `webauthn_credentials` | < 500 |
| `oauth2_sessions` | < 1 000 (Pocket ID auto-purges) |
| `audit_logs` | < 100 000 (trim if > 1 M) |
| `interaction_sessions` | < 100 (Pocket ID auto-purges) |

---

## Encryption Key Management

The `encryption-key` in `secret/pocket-id-secrets` is used to encrypt stored secrets
(session key, JWT private key). **Do not rotate it** without a full re-encryption of all
stored values — this is not supported by a simple key swap.

```bash
# View the current key (hex string, 32 chars = 128-bit AES)
kubectl -n access get secret pocket-id-secrets \
  -o jsonpath='{.data.encryption-key}' | base64 -d && echo

# The key is also visible inside the running pod:
kubectl -n access exec $NEW_POD -- env | grep ENCRYPTION_KEY
```

> Store the encryption key in a password manager or secrets vault. If lost and the pod is
> restarted with a different key, all encrypted stored values (session key, JWT key) become
> unreadable and Pocket ID will fail to start until you restore the original key or re-init.

---

## Upgrade / Migration Safety

Before upgrading the Pocket ID image:

1. **Take a backup** (see Backup section above).
2. Check the [Pocket ID release notes](https://github.com/stonith404/pocket-id/releases) for
   breaking schema changes.
3. Apply the new image:
   ```bash
   # Update image in registry, then:
   kubectl -n access rollout restart deployment/pocket-id
   kubectl -n access rollout status deployment/pocket-id --timeout=120s
   ```
4. Watch logs for migration output:
   ```bash
   kubectl -n access logs -l app=pocket-id -f | grep -iE 'migrat|error|err'
   ```
5. If the pod crashes post-upgrade, check `schema_migrations.dirty`:
   ```bash
   # Copy DB out and check
   sqlite3 /tmp/pid-maint.db \
     "SELECT version, dirty FROM schema_migrations ORDER BY version DESC LIMIT 3;"
   ```
   If `dirty=1` — restore the pre-upgrade backup.

---

## Quick Reference

```bash
# All commands assume pod is named $NEW_POD
NEW_POD=$(kubectl -n access get pods -l app=pocket-id -o jsonpath='{.items[0].metadata.name}')

# Live pod log tail
kubectl -n access logs $NEW_POD -f

# Copy DB out (pod running — safe for reads)
kubectl -n access cp $NEW_POD:/app/data/pocket-id.db /tmp/snapshot.db

# Integrity check
sqlite3 /tmp/snapshot.db "PRAGMA integrity_check; PRAGMA foreign_key_check;"

# DB size
kubectl -n access exec $NEW_POD -- sh -c "du -sh /app/data/pocket-id.db*"

# Restart pod cleanly
kubectl -n access rollout restart deployment/pocket-id
```
