// =============================================================================
// console-backend/internal/auth/middleware.go
// Session gate and /mfa-required handler.
//
// RequireSession reads the __Host-stardata_session cookie, looks up the
// session in Valkey and stores the session data in the request context.
// Unauthenticated requests are redirected to /auth/login with ?next= set to
// the original URL.
//
// RequireMFA is a stricter gate used on admin-only routes: it additionally
// checks the "mfa" flag stored in the session and rejects sessions that
// were established without a second factor.
// =============================================================================
package auth

import (
	"context"
	"encoding/json"
	"net/http"
)

type contextKey string

const SessionKey contextKey = "session"

// Session holds the fields persisted in Valkey for a signed-in user.
type Session struct {
	Sub             string `json:"sub"`
	Email           string `json:"email"`
	Name            string `json:"name"`
	AccessToken     string `json:"access_token"`
	RefreshToken    string `json:"refresh_token"`
	IDToken         string `json:"id_token"`
	AccessExpiresAt int64  `json:"access_expires_at"`
	MFA             bool   `json:"mfa"`
	CreatedAt       int64  `json:"created_at"`
}

// RequireSession is middleware that gates any route behind a valid session.
func (h *Handler) RequireSession(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sess, sid, ok := h.loadSession(w, r)
		if !ok {
			// Redirect to login; preserve the requested URL in ?next=.
			next := safeNext(r.URL.RequestURI())
			http.Redirect(w, r, "/auth/login?next="+next, http.StatusFound)
			return
		}

		// Slide the idle TTL on every authenticated request.
		h.Valkey.Expire(r.Context(), "sess:"+sid, h.SessionIdleTTL)

		ctx := context.WithValue(r.Context(), SessionKey, sess)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// RequireMFA is middleware that additionally enforces that the session was
// established with a second factor.  Only Owner / Admin routes need this.
func (h *Handler) RequireMFA(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sess, ok := r.Context().Value(SessionKey).(*Session)
		if !ok || !sess.MFA {
			http.Redirect(w, r, "/mfa-required", http.StatusFound)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// MFARequired handles GET /mfa-required.
// It explains why MFA is required and links to ZITADEL's self-service page
// where the person can enrol their authenticator app or passkey.
func (h *Handler) MFARequired(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusForbidden)
	w.Write([]byte(`<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1.0"/>
  <title>MFA required – StarData</title>
  <style>
    body{font-family:-apple-system,"Segoe UI",system-ui,sans-serif;background:#0f1117;
         color:#e8eaf0;display:flex;align-items:center;justify-content:center;
         min-height:100vh;margin:0}
    .card{background:#16181f;border:1px solid #2a2d38;border-radius:14px;
          padding:40px;max-width:420px;width:100%;text-align:center}
    .icon{font-size:42px;margin-bottom:16px}
    h1{font-size:22px;font-weight:700;margin:0 0 10px}
    p{color:#9ca3af;font-size:14px;line-height:1.6;margin:0 0 24px}
    a.btn{display:inline-block;background:#3346C4;color:#fff;text-decoration:none;
          padding:11px 24px;border-radius:8px;font-weight:600;font-size:14px}
    a.btn:hover{background:#4458e0}
    .back{display:block;margin-top:16px;color:#6b7280;font-size:13px;text-decoration:none}
    .back:hover{color:#e8eaf0}
  </style>
</head>
<body>
  <div class="card">
    <div class="icon">🔐</div>
    <h1>Two-step verification required</h1>
    <p>
      Owners and Admins must sign in with two-step verification (an
      authenticator app or passkey). Set one up in your account settings,
      then sign in again.
    </p>
    <a class="btn" href="https://auth.stardatadb.com/ui/v2/user/mfa">
      Set up two-step verification
    </a>
    <a class="back" href="/auth/logout">Sign out and try again</a>
  </div>
</body>
</html>`))
}

// Logout handles GET /auth/logout.
// It deletes the Valkey session, clears the cookie and redirects to the
// ZITADEL end-session endpoint so the ZITADEL session is also terminated.
func (h *Handler) Logout(w http.ResponseWriter, r *http.Request) {
	cookie, err := r.Cookie("__Host-stardata_session")
	if err == nil && cookie.Value != "" {
		h.Valkey.Del(r.Context(), "sess:"+cookie.Value)
	}

	// Clear the cookie.
	http.SetCookie(w, &http.Cookie{
		Name:     "__Host-stardata_session",
		Value:    "",
		Path:     "/",
		Secure:   true,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   -1,
	})

	// Redirect to ZITADEL's end-session endpoint.
	http.Redirect(w, r,
		h.ZitadelDomain+"/oidc/v1/end_session?post_logout_redirect_uri=https://console.stardatadb.com/",
		http.StatusFound,
	)
}

// loadSession reads the session cookie and fetches the session from Valkey.
// Returns the session, the session ID, and whether the load succeeded.
func (h *Handler) loadSession(w http.ResponseWriter, r *http.Request) (*Session, string, bool) {
	cookie, err := r.Cookie("__Host-stardata_session")
	if err != nil || cookie.Value == "" {
		return nil, "", false
	}
	sid := cookie.Value

	raw, err := h.Valkey.HGetAll(r.Context(), "sess:"+sid).Result()
	if err != nil || len(raw) == 0 {
		return nil, "", false
	}

	// Marshal the flat string map back into a Session struct via JSON.
	b, _ := json.Marshal(raw)
	var sess Session
	if err := json.Unmarshal(b, &sess); err != nil {
		return nil, "", false
	}
	return &sess, sid, true
}
