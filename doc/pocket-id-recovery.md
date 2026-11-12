# Pocket ID — Admin Recovery Runbook

> **When to use this doc**  
> You (or all admins) are locked out of `https://auth.stardatadb.com` — passkey lost,
> device changed, DB corruption, or OIDC client wiped. This runbook covers every
> recovery scenario in order of least-to-most invasive.

---

## Prevention Checklist (Do This Now)

| Action | Why |
|--------|-----|
| Register **2+ passkeys** per admin (e.g. MacBook + iPhone) | One device lost = still have another |
| Keep a passkey on a **hardware key** (YubiKey) stored safely | Survives device loss/wipe |
| After any Pocket ID version upgrade, **verify login** immediately | Schema migrations can reset sessions |
| Never run `kubectl cp` into the live pod's DB file while pod is up | Causes WAL corruption |
| Pocket ID must run on **worker1** (not master2/control-plane) | control-plane taints cause CrashLoopBackOff under memory pressure |

---

## Architecture Reference

```
auth.stardatadb.com ──► pocket-id pod (worker1.local)
                              └─ /app/data/pocket-id.db   (SQLite, WAL mode)
                                   PVC: pocket-id-data
                                   PV path (worker1): /home/local-path-provisioner/
                                           pvc-e3a35cc0-6219-4ba5-ade3-099311bef67e_access_pocket-id-data/
```

**Key Kubernetes objects:**

| Object | Namespace | Purpose |
|--------|-----------|---------|
| `deployment/pocket-id` | `access` | Pocket ID server |
| `pvc/pocket-id-data` | `access` | SQLite DB volume (worker1) |
| `secret/pocket-id-secrets` | `access` | `encryption-key` (AES key for stored secrets) |
| `secret/console-oidc` | `access` | `client_id` + `client_secret` for oauth2-proxy |
| `secret/gmail-smtp-credentials` | `access` | SMTP credentials for login-code emails |

---

## Scenario 1 — Lost Passkey on One Device (Simplest)

> You still have admin access on another device/browser.

1. Log in as admin from the working device.
2. Go to **Administration → Users → (your account)**.
3. Under **Passkeys**, click **Delete** on the lost passkey.
4. Click **Add Passkey** and register the new device.

---

## Scenario 2 — All Admin Passkeys Lost, SMTP Works

> You can receive email but can't log in with any passkey.

1. Go to `https://auth.stardatadb.com` → click **Use passkey** → **Alternative Sign In** → **Login Code**.
2. Enter your admin email address.
3. Pocket ID emails a one-time login code — use it to sign in.
4. Immediately register a new passkey from **My Account → Passkeys → Add Passkey**.

> **SMTP must be reachable from inside the cluster.** If SMTP is blocked (e.g. port 465/587
> firewalled outbound on the cluster nodes), use Scenario 3 instead.

---

## Scenario 3 — All Passkeys Lost, SMTP Blocked (Full Admin Recovery via kubectl)

This is the cluster-operator recovery path. Requires `kubectl` access to the cluster.

### Step 1 — Find the running pod

```bash
kubectl -n access get pods -l app=pocket-id
# Note the pod name, e.g. pocket-id-7f85669cf7-rb4pm
NEW_POD=$(kubectl -n access get pods -l app=pocket-id -o jsonpath='{.items[0].metadata.name}')
```

### Step 2 — Copy the live database out

```bash
kubectl -n access cp $NEW_POD:/app/data/pocket-id.db /tmp/pid-recovery.db
kubectl -n access cp $NEW_POD:/app/data/pocket-id.db-shm /tmp/pid-recovery.db-shm
kubectl -n access cp $NEW_POD:/app/data/pocket-id.db-wal /tmp/pid-recovery.db-wal
```

### Step 3 — Merge WAL and inspect

```bash
sqlite3 /tmp/pid-recovery.db "PRAGMA wal_checkpoint(TRUNCATE);"
sqlite3 /tmp/pid-recovery.db "PRAGMA integrity_check;"   # must return "ok"
sqlite3 /tmp/pid-recovery.db "SELECT id, username, email, is_admin FROM users;"
```

### Step 4 — Delete passkeys for the locked-out user

```bash
# Replace USER_ID with the id from Step 3
USER_ID="000d09fa-4a51-48fd-8316-f5afd8e467d4"
sqlite3 /tmp/pid-recovery.db \
  "DELETE FROM webauthn_credentials WHERE user_id='$USER_ID';"
```

### Step 5 — Scale down, push DB, scale up

```bash
kubectl -n access scale deployment pocket-id --replicas=0
sleep 5

# Spin up a restore helper pod on the same node as the PVC
cat <<'EOF' | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: pid-restore
  namespace: access
spec:
  nodeSelector:
    kubernetes.io/hostname: worker1.local
  restartPolicy: Never
  containers:
  - name: r
    image: 192.168.1.50:30500/pocket-id:latest
    command: ["sleep", "120"]
    resources:
      requests: { cpu: "50m", memory: "64Mi" }
      limits:   { cpu: "200m", memory: "128Mi" }
    volumeMounts:
    - name: data
      mountPath: /data
  volumes:
  - name: data
    persistentVolumeClaim:
      claimName: pocket-id-data
EOF

kubectl -n access wait pod/pid-restore --for=condition=Ready --timeout=60s
kubectl -n access cp /tmp/pid-recovery.db pid-restore:/data/pocket-id.db
kubectl -n access exec pid-restore -- sh -c \
  "rm -f /data/pocket-id.db-shm /data/pocket-id.db-wal && echo OK"
kubectl -n access delete pod pid-restore --force
kubectl -n access scale deployment pocket-id --replicas=1
kubectl -n access rollout status deployment/pocket-id --timeout=90s
```

### Step 6 — Register a new passkey

1. Go to `https://auth.stardatadb.com`.
2. Enter your email — since **no passkeys exist**, Pocket ID will walk you through
   registering a new one directly.
3. Complete passkey registration on your current device.

---

## Scenario 4 — Complete DB Corruption (Nuclear Reset)

> Use only when `PRAGMA integrity_check` fails and the DB cannot be repaired.

### Option A — Wipe all users (full first-run reset)

```bash
# 1. Get a clean copy out
kubectl -n access cp $NEW_POD:/app/data/pocket-id.db /tmp/pid-reset.db
# ... (cp shm + wal, then checkpoint as above)

# 2. Wipe user data only — preserve OIDC clients and app config
sqlite3 /tmp/pid-reset.db "DELETE FROM webauthn_credentials;"
sqlite3 /tmp/pid-reset.db "DELETE FROM webauthn_sessions;"
sqlite3 /tmp/pid-reset.db "DELETE FROM reauthentication_tokens;"
sqlite3 /tmp/pid-reset.db "DELETE FROM interaction_sessions;"
sqlite3 /tmp/pid-reset.db "DELETE FROM user_authorized_oidc_clients;"
sqlite3 /tmp/pid-reset.db "DELETE FROM oauth2_sessions;"
sqlite3 /tmp/pid-reset.db "DELETE FROM users;"
sqlite3 /tmp/pid-reset.db "PRAGMA integrity_check;"

# 3. Push back (same scale-down / restore-pod / scale-up steps as Scenario 3)
```

4. Go to `https://auth.stardatadb.com/setup` — the first-run wizard appears.
5. Create your admin account and register a passkey.

### Option B — Restore from the PV backup on worker1

The PV directory on worker1 is:
```
/home/local-path-provisioner/pvc-e3a35cc0-6219-4ba5-ade3-099311bef67e_access_pocket-id-data/
```

If you kept a manual backup (`pocket-id.db.bak`), restore it:

```bash
ssh 192.168.1.51
# On worker1:
PV=/home/local-path-provisioner/pvc-e3a35cc0-6219-4ba5-ade3-099311bef67e_access_pocket-id-data
cp $PV/pocket-id.db $PV/pocket-id.db.bak.$(date +%Y%m%d-%H%M)   # take snapshot first
```

> **Tip:** Add a daily cron on master or worker1 that does:
> ```bash
> sqlite3 $PV/pocket-id.db ".backup /opt/backups/pocket-id-$(date +%Y%m%d).db"
> ```

---

## Scenario 5 — OIDC Client for console.stardatadb.com Wiped

> "No apps available" on Pocket ID, or oauth2-proxy returns 500/redirect loop.

### Check the existing K8s secret for credentials

```bash
kubectl -n access get secret console-oidc \
  -o jsonpath='{.data.client_id}'     | base64 -d && echo
kubectl -n access get secret console-oidc \
  -o jsonpath='{.data.client_secret}' | base64 -d && echo
```

### Re-create the OIDC client via Pocket ID UI

1. Log in to `https://auth.stardatadb.com` as admin.
2. **Administration → OIDC Clients → Create OIDC Client**.
3. Fill in:

   | Field | Value |
   |-------|-------|
   | **Name** | `stardata-console` |
   | **Client type** | Confidential Client |
   | **Callback URLs** | `https://console.stardatadb.com/oauth2/callback` |
   | **Logout callback URLs** | `https://console.stardatadb.com/oauth2/sign_out` |
   | **Launch URL** | `https://console.stardatadb.com` |
   | **Custom client ID** | `cd32c999-0811-4af3-b549-98fe9b859252` *(must match secret)* |

4. Click **Create**. Pocket ID shows the generated client secret.
5. If the secret shown **differs** from what is in the `console-oidc` K8s secret, update the secret:

   ```bash
   kubectl -n access create secret generic console-oidc \
     --from-literal=client_id=<CLIENT_ID> \
     --from-literal=client_secret=<NEW_SECRET> \
     --dry-run=client -o yaml | kubectl apply -f -

   # Restart oauth2-proxy to pick up the new secret
   kubectl -n access rollout restart deployment/console-oauth2-proxy
   ```

---

## Quick-Reference Commands

```bash
# Pod status
kubectl -n access get pods -l app=pocket-id

# Live logs (watch for ERR/WRN)
kubectl -n access logs -l app=pocket-id -f

# Scale down / up
kubectl -n access scale deployment pocket-id --replicas=0
kubectl -n access scale deployment pocket-id --replicas=1

# Check DB integrity (while pod is stopped)
PV=/home/local-path-provisioner/pvc-e3a35cc0-6219-4ba5-ade3-099311bef67e_access_pocket-id-data
sqlite3 $PV/pocket-id.db "PRAGMA integrity_check;"
sqlite3 $PV/pocket-id.db "SELECT username, email, is_admin FROM users;"
sqlite3 $PV/pocket-id.db "SELECT id, name FROM oidc_clients;"
sqlite3 $PV/pocket-id.db "SELECT name, credential_id FROM webauthn_credentials;"

# Encryption key (needed for any crypto operations)
kubectl -n access get secret pocket-id-secrets \
  -o jsonpath='{.data.encryption-key}' | base64 -d && echo
```

---

## Common Errors & Fixes

| Error | Cause | Fix |
|-------|-------|-----|
| `it appears that there's already one instance of Pocket ID running` | Stale lock in DB from previous crash or dual pods | `kubectl -n access delete pod --all --force` and let the deployment recreate |
| `database disk image is malformed` | DB file was written while pod was running (WAL conflict) | Restore from backup, or checkpoint + integrity check |
| `One-time access is disabled` | SMTP port blocked from cluster nodes (outbound 465/587 firewalled) | Use Scenario 3 (kubectl recovery) |
| `Token is invalid or expired` | Device login code expired (5 min TTL) | Generate a new code immediately before patching |
| Passkey prompt doesn't appear | No passkeys registered for this user | Delete all passkeys in DB → user gets registration wizard on next login |
| `No apps available` on My Apps | OIDC client `stardata-console` missing | Re-create via UI (Scenario 5) |
| oauth2-proxy `500` or infinite redirect | Client secret mismatch between Pocket ID and K8s secret | Re-create secret with matching value (Scenario 5) |

---

## Node & PVC Reference

| Item | Value |
|------|-------|
| Pod node | `worker1.local` (`192.168.1.51`) |
| PVC name | `pocket-id-data` |
| PV path on worker1 | `/home/local-path-provisioner/pvc-e3a35cc0-6219-4ba5-ade3-099311bef67e_access_pocket-id-data/` |
| DB file | `pocket-id.db` (+ `-shm`, `-wal` while pod is running) |
| Encryption key secret | `pocket-id-secrets` → key `encryption-key` |
| OIDC client secret | `console-oidc` → keys `client_id`, `client_secret` |
| SMTP secret | `gmail-smtp-credentials` → keys `username`, `password` |
