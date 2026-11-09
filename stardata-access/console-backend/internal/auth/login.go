// =============================================================================
// console-backend/internal/auth/login.go
// Step 1: redirect the browser to ZITADEL with PKCE, state and nonce.
// State and verifier are stored in Valkey under "pre:<state>" for 5 minutes
// (single-use; consumed by Callback).
// =============================================================================
package auth

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"
)

// Login redirects an unauthenticated browser to ZITADEL's Login v2 page.
// It creates a short-lived pre-session in Valkey that ties the callback
// to this exact browser request (PKCE + state + nonce).
func (h *Handler) Login(w http.ResponseWriter, r *http.Request) {
	state := rand32()
	nonce := rand32()
	verifier := oauth2.GenerateVerifier()

	pre := PreSession{
		State:    state,
		Nonce:    nonce,
		Verifier: verifier,
		Next:     safeNext(r.URL.Query().Get("next")),
	}

	if err := h.Valkey.Set(
		r.Context(),
		"pre:"+state,
		mustJSON(pre),
		15*time.Minute,
	).Err(); err != nil {
		http.Error(w, "session store unavailable", http.StatusServiceUnavailable)
		return
	}

	// Build the authorization URL. Pass login_hint if the caller provided one
	// (e.g. www.stardatadb.com pre-fills the email so ZITADEL shows it).
	authOpts := []oauth2.AuthCodeOption{
		oidc.Nonce(nonce),
		oauth2.S256ChallengeOption(verifier),
	}
	if hint := r.URL.Query().Get("login_hint"); hint != "" {
		authOpts = append(authOpts, oauth2.SetAuthURLParam("login_hint", hint))
	}
	url := h.OAuth.AuthCodeURL(state, authOpts...)
	http.Redirect(w, r, url, http.StatusFound)
}

// safeNext returns next only when it is a safe relative path (starts with /).
// Any other value (empty, absolute URL, data: etc.) falls back to "/console.html".
// This prevents open-redirect attacks via the ?next= parameter.
func safeNext(next string) string {
	if strings.HasPrefix(next, "/") && !strings.HasPrefix(next, "//") {
		return next
	}
	return "/console.html"
}

func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(b)
}
