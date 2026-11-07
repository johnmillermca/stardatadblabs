// =============================================================================
// console-backend/cmd/server/main.go
// Wires the OIDC provider, Valkey client, OAuth2 config and HTTP routes.
//
// Routes handled by this binary:
//   GET  /auth/login       — redirect to ZITADEL (starts PKCE flow)
//   GET  /auth/callback    — receive code from ZITADEL, create session
//   GET  /auth/logout      — delete session, redirect to ZITADEL end_session
//   GET  /mfa-required     — shown to Owners/Admins who signed in without MFA
//   GET  /healthz          — liveness / readiness probe
//   *    /api/*            — gated behind RequireSession; proxied to access APIs
//
// All configuration is read from environment variables (injected by Kubernetes
// Secrets and ConfigMaps via the console-backend Deployment manifest).
// =============================================================================
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/redis/go-redis/v9"
	"golang.org/x/oauth2"

	"github.com/stardatadb/console-backend/internal/auth"
)

func main() {
	ctx := context.Background()

	// ── Configuration from environment ───────────────────────────────────────
	zitadelDomain := mustEnv("ZITADEL_DOMAIN")      // "https://auth.stardatadb.com"
	clientID := mustEnv("OIDC_CLIENT_ID")
	clientSecret := mustEnv("OIDC_CLIENT_SECRET")
	redirectURI := mustEnv("OIDC_REDIRECT_URI")     // "https://console.stardatadb.com/auth/callback"
	projectID := mustEnv("ZITADEL_PROJECT_ID")

	valkeyAddr := mustEnv("VALKEY_ADDR")             // "valkey.access.svc.cluster.local:6379"
	valkeyPass := os.Getenv("VALKEY_PASSWORD")       // optional when running without auth
	sessionIdleSec := envInt("SESSION_IDLE_TTL", 86400)  // default 24 h

	// ── OIDC provider (discovers endpoints from /.well-known) ────────────────
	provider, err := oidc.NewProvider(ctx, zitadelDomain)
	if err != nil {
		slog.Error("oidc provider discovery failed", "err", err)
		os.Exit(1)
	}

	// ── OAuth2 config ─────────────────────────────────────────────────────────
	// The project audience scope makes ZITADEL include project roles in tokens.
	oauthCfg := &oauth2.Config{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		RedirectURL:  redirectURI,
		Endpoint:     provider.Endpoint(),
		Scopes: []string{
			oidc.ScopeOpenID,
			"profile",
			"email",
			"offline_access", // refresh token
			"urn:zitadel:iam:org:project:id:" + projectID + ":aud",
		},
	}

	// ── ID token verifier ─────────────────────────────────────────────────────
	verifier := provider.Verifier(&oidc.Config{ClientID: clientID})

	// ── Valkey (Redis-compatible) client ──────────────────────────────────────
	valkeyClient := redis.NewClient(&redis.Options{
		Addr:     valkeyAddr,
		Password: valkeyPass,
	})
	if err := valkeyClient.Ping(ctx).Err(); err != nil {
		slog.Error("valkey connection failed", "addr", valkeyAddr, "err", err)
		os.Exit(1)
	}

	// ── Auth handler ──────────────────────────────────────────────────────────
	h := &auth.Handler{
		OAuth:          oauthCfg,
		Verifier:       verifier,
		Valkey:         valkeyClient,
		ZitadelDomain:  zitadelDomain,
		SessionIdleTTL: time.Duration(sessionIdleSec) * time.Second,
	}

	// ── HTTP routes ───────────────────────────────────────────────────────────
	mux := http.NewServeMux()

	// Public auth routes (no session required).
	mux.HandleFunc("GET /auth/login", h.Login)
	mux.HandleFunc("GET /auth/callback", h.Callback)
	mux.HandleFunc("GET /auth/logout", h.Logout)
	mux.HandleFunc("GET /mfa-required", h.MFARequired)

	// Health check (no session required).
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	// /api/me — returns the current session's user info as JSON.
	mux.Handle("GET /api/me", h.RequireSession(http.HandlerFunc(handleMe)))

	// All other /api/* routes are gated by session; individual handlers add
	// RequireMFA where needed (Owner/Admin-only operations).
	mux.Handle("/api/", h.RequireSession(http.NotFoundHandler()))

	// ── Start server ──────────────────────────────────────────────────────────
	addr := ":8090"
	slog.Info("console-backend listening", "addr", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		slog.Error("server error", "err", err)
		os.Exit(1)
	}
}

// handleMe returns session identity as JSON.
func handleMe(w http.ResponseWriter, r *http.Request) {
	sess, _ := r.Context().Value(auth.SessionKey).(*auth.Session)
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"sub":"` + sess.Sub + `","email":"` + sess.Email +
		`","name":"` + sess.Name + `","mfa":` + boolStr(sess.MFA) + `}`))
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		slog.Error("required env var not set", "key", key)
		os.Exit(1)
	}
	return v
}

func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}
