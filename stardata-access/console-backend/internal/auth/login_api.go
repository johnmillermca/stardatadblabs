// =============================================================================
// console-backend/internal/auth/login_api.go
// GET /auth/login-complete — finalises the cross-domain session hand-off
//                            after the standard OIDC callback flow.
//
// NOTE: POST /api/login (direct credential sign-in) has been removed.
// Sign-in now follows the standard OIDC redirect flow:
//   www.stardatadb.com  →  click "Sign In"
//   → redirect to https://console.stardatadb.com/auth/login  (BFF)
//   → BFF redirects to auth.stardatadb.com  (ZITADEL Login UI)
//   → user enters email + password on auth.stardatadb.com
//   → on success, ZITADEL redirects to https://console.stardatadb.com/auth/callback
//   → callback.go exchanges the code, creates Valkey session, sets cookie
//   → browser lands on /console.html
// =============================================================================
package auth
