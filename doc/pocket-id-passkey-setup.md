# Pocket ID — Passkey Setup Guide

> **Platform context**
> Pocket ID runs at `https://auth.stardatadb.com` (deployed as a Kubernetes Deployment in the
> `access` namespace on **`worker1.local`**, exposed via `webconsole-proxy`).
> The console at `https://console.stardatadb.com` uses `console-oauth2-proxy` as the OIDC gate,
> which redirects unauthenticated users to Pocket ID for login.
>
> **Locked out?** See [pocket-id-recovery.md](./pocket-id-recovery.md) for the full recovery runbook.

---

## What is a Passkey?

A **passkey** is a FIDO2/WebAuthn credential tied to your device (Face ID, Touch ID, Windows Hello,
or a hardware key like a YubiKey). Instead of typing a password, you authenticate with biometrics
or a PIN stored securely on your device. Pocket ID supports passkeys natively — no password is
required once one is registered.

---

## Prerequisites

| Requirement | Details |
|-------------|---------|
| Account on Pocket ID | An admin must create your user first (see Admin: Create a User below) |
| Supported browser | Chrome 108+, Firefox 119+, Safari 16+, Edge 108+ |
| Authenticator device | Phone with Face/Touch ID, laptop with Windows Hello, or a FIDO2 hardware key |
| Network access | `https://auth.stardatadb.com` reachable (via Cloudflare or LAN with `/etc/hosts` entry) |

---

## Admin: Create a User Account

Before a user can register a passkey, an admin must create their account in Pocket ID.

1. Go to **`https://auth.stardatadb.com`** and sign in as admin.
2. Navigate to **Users → New User**.
3. Fill in the user details:
   - **First name / Last name**
   - **Email address** (this is the login identifier)
4. Click **Save**.
5. Pocket ID will generate a **one-time setup link** — share it with the user via a secure channel
   (Slack DM, email, etc.).  
   The link expires after 24 hours.

---

## User: Register Your First Passkey

### Step 1 — Open your setup link

Click the one-time setup link the admin sent you. It will open `https://auth.stardatadb.com` and
log you in temporarily so you can register a credential.

> If the link has expired, ask an admin to regenerate it from **Users → (your account) →
> Send One-Time Link**.

### Step 2 — Go to Passkey settings

Once inside the Pocket ID user interface:

1. Click your avatar / name in the top-right corner.
2. Select **Settings** (or **My Account**).
3. Find the **Passkeys** section.

### Step 3 — Add a new passkey

1. Click **Add Passkey** (or **Register new passkey**).
2. Give it a friendly name, for example:
   - `MacBook Pro Touch ID`
   - `iPhone 15 Face ID`
   - `YubiKey 5`
3. Click **Save / Continue**.
4. Your browser will prompt you to authenticate with your device:
   - **macOS / iOS** → Face ID or Touch ID dialog.
   - **Windows** → Windows Hello PIN or fingerprint.
   - **Android** → Fingerprint or screen lock.
   - **Hardware key** → Insert the key and touch the button.
5. After successful device authentication the passkey is saved and listed under your account.

### Step 4 — Test the passkey login

1. Open a new browser tab (or a private/incognito window).
2. Navigate to **`https://console.stardatadb.com`**.
3. The oauth2-proxy will redirect you to **`https://auth.stardatadb.com/authorize`**.
4. On the Pocket ID login page, click **Sign in with Passkey** (or just enter your email — Pocket
   ID will offer the passkey automatically if one is registered for your device).
5. Authenticate with your device biometric / PIN.
6. You will be redirected back to the console automatically.

---

## Register an Additional Passkey (e.g., a YubiKey as backup)

Repeat **Step 2 → Step 4** above from an already-authenticated session, giving the new key a
different friendly name. There is no limit on the number of passkeys per account.

---

## Admin: Remove or Revoke a Passkey

1. Go to **`https://auth.stardatadb.com`** and sign in as admin.
2. Navigate to **Users → (select user)**.
3. In the **Passkeys** section, find the key to remove.
4. Click the **Delete / Revoke** icon next to it.
5. Confirm the deletion.

> Revoking a passkey is useful when a user loses a device. Ask the user to register a new passkey
> from a trusted device afterwards.

---

## Admin: Regenerate a One-Time Setup / Recovery Link

If a user is locked out (all passkeys lost, no password set):

1. Go to **`https://auth.stardatadb.com`** → **Users → (select user)**.
2. Click **Send One-Time Link** (or **Generate Recovery Link**).
3. Copy the link and share it securely with the user.
4. The user follows the link and registers a new passkey (Steps 2–4 above).

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| "No passkey found for this device" on login | Passkey was registered on a different device or browser | Use a one-time recovery link to register a new one |
| Browser prompt doesn't appear after clicking Add Passkey | Browser or OS doesn't support WebAuthn | Use Chrome 108+, Firefox 119+, or Safari 16+ on a supported OS |
| Redirect loop on `console.stardatadb.com` | Session cookie domain mismatch | Ensure the browser accepts cookies for `.stardatadb.com`; clear cookies and retry |
| One-time link says "expired or already used" | Link is older than 24 h or was already clicked | Ask an admin to generate a new one |
| YubiKey not recognised | NFC/USB mode not supported by the browser | Use USB-A/C in Chrome with the FIDO2 flag enabled |

---

## Quick Reference

| URL | Purpose |
|-----|---------|
| `https://auth.stardatadb.com` | Pocket ID admin + login UI |
| `https://console.stardatadb.com` | StarData platform console (OIDC-protected) |
| `https://auth.stardatadb.com/authorize` | OIDC authorization endpoint (redirect target from oauth2-proxy) |

---

## Platform Architecture (for reference)

```
Browser
  └─► Cloudflare (TLS)
        └─► master2 :80  [webconsole-proxy nginx, hostNetwork]
              ├─► auth.stardatadb.com  → pocket-id.access.svc.cluster.local:1411
              └─► console.stardatadb.com
                    └─► console-oauth2-proxy.access.svc.cluster.local:4180
                          └─► console-spa.access.svc.cluster.local:80
```

Pocket ID runs on **`worker1.local`** (`192.168.1.51`).
Data is stored on a 2 Gi `local-path` PersistentVolumeClaim (`pocket-id-data`) mounted
at `/app/data` inside the container.
PV path on worker1: `/home/local-path-provisioner/pvc-e3a35cc0-6219-4ba5-ade3-099311bef67e_access_pocket-id-data/`
