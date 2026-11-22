# StarData Console — OIDC Client Setup

The StarData Console OIDC client **must be created via the Pocket ID admin UI**
(`https://auth.stardatadb.com` → Administration → OIDC Clients → Create).
Pocket ID stores client secrets using HMAC keyed on `ENCRYPTION_KEY`; raw DB
inserts will always fail authentication.

## Client settings (created 2026-11-22)

| Field | Value |
|---|---|
| **Client ID** | `500dcebb-3568-4f2d-97ab-a2c0e6486bba` |
| **Name** | StarData Console |
| **Type** | Confidential |
| **Callback URL** | `https://console.stardatadb.com/oauth2/callback` |
| **Client Launch URL** | `https://console.stardatadb.com` |
| **Skip Consent** | true |
| **Is Group Restricted** | false |

## Post-creation DB fixes (apply after UI creation)

```sql
UPDATE oidc_clients
SET launch_url        = 'https://console.stardatadb.com',
    skip_consent      = true,
    is_group_restricted = false
WHERE name = 'StarData Console';
```

## K8s secret

```bash
kubectl -n access create secret generic console-oidc \
  --from-literal=client_id=<CLIENT_ID_FROM_UI> \
  --from-literal=client_secret=<SECRET_FROM_UI> \
  --from-literal=cookie_secret=$(openssl rand -base64 24 | tr -d '\n=' | cut -c1-32) \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n access rollout restart deployment/console-oauth2-proxy
```
