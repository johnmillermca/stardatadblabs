// =============================================================================
// console-backend/cmd/server/main.go
// Wires the OIDC provider, Valkey client, OAuth2 config and HTTP routes.
//
// Routes handled by this binary:
//   GET  /auth/login       — redirect to ZITADEL (starts PKCE flow)
//   GET  /auth/callback    — receive code from ZITADEL, create session
//   GET  /auth/logout      — delete session, redirect to ZITADEL end_session
//   GET  /mfa-required     — shown to Owners/Admins who signed in without MFA
//   GET  /healthz          — liveness / readiness probe (responds immediately)
//   *    /api/*            — gated behind RequireSession; proxied to access APIs
//
// All configuration is read from environment variables (injected by Kubernetes
// Secrets and ConfigMaps via the console-backend Deployment manifest).
// =============================================================================
package main

import (
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	"golang.org/x/oauth2"

	"github.com/stardatadb/console-backend/internal/access"
	"github.com/stardatadb/console-backend/internal/auth"
)

func main() {
	ctx := context.Background()

	// ── Configuration from environment ───────────────────────────────────────
	zitadelDomain := mustEnv("ZITADEL_DOMAIN") // "https://auth.stardatadb.com"
	// ZITADEL_INTERNAL_URL: plain in-cluster address for discovery fetch.
	// ZITADEL serves HTTPS with a self-signed cert; Host header must equal the
	// ExternalDomain so ZITADEL routes to the correct instance.
	// A custom transport handles both: skip TLS verify + rewrite Host header.
	zitadelInternalURL := os.Getenv("ZITADEL_INTERNAL_URL") // "https://zitadel.access.svc.cluster.local:8080"
	clientID := mustEnv("OIDC_CLIENT_ID")
	clientSecret := mustEnv("OIDC_CLIENT_SECRET")
	redirectURI := mustEnv("OIDC_REDIRECT_URI") // "https://console.stardatadb.com/auth/callback"
	projectID := mustEnv("ZITADEL_PROJECT_ID")

	valkeyAddr := mustEnv("VALKEY_ADDR")        // "valkey.access.svc.cluster.local:6379"
	valkeyPass := os.Getenv("VALKEY_PASSWORD")  // optional when running without auth
	sessionIdleSec := envInt("SESSION_IDLE_TTL", 86400) // default 24 h

	// ── Valkey (Redis-compatible) client ──────────────────────────────────────
	valkeyClient := redis.NewClient(&redis.Options{
		Addr:     valkeyAddr,
		Password: valkeyPass,
	})
	if err := valkeyClient.Ping(ctx).Err(); err != nil {
		slog.Error("valkey connection failed", "addr", valkeyAddr, "err", err)
		os.Exit(1)
	}

	// ── Handler placeholder — swapped in once OIDC discovery succeeds ─────────
	// authReady is atomically swapped from nil → *auth.Handler once discovery
	// completes. Auth routes return 503 until then; /healthz always returns 200
	// so the readiness probe passes immediately and the pod is marked Ready.
	var authReady atomic.Pointer[auth.Handler]

	// ── HTTP routes ───────────────────────────────────────────────────────────
	mux := http.NewServeMux()

	// Health check — always available, no dependency on OIDC.
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	// Auth routes — served once OIDC discovery has completed.
	authGate := func(next func(*auth.Handler, http.ResponseWriter, *http.Request)) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			h := authReady.Load()
			if h == nil {
				http.Error(w, "auth service starting, please retry in a moment", http.StatusServiceUnavailable)
				return
			}
			next(h, w, r)
		}
	}
	mux.HandleFunc("GET /auth/login", authGate((*auth.Handler).Login))
	mux.HandleFunc("GET /auth/callback", authGate((*auth.Handler).Callback))
	mux.HandleFunc("GET /auth/logout", authGate((*auth.Handler).Logout))
	mux.HandleFunc("GET /mfa-required", authGate((*auth.Handler).MFARequired))
	// /api/me — requires session.
	mux.HandleFunc("GET /api/me", func(w http.ResponseWriter, r *http.Request) {
		h := authReady.Load()
		if h == nil {
			http.Error(w, "auth service starting, please retry in a moment", http.StatusServiceUnavailable)
			return
		}
		h.RequireSession(http.HandlerFunc(handleMe)).ServeHTTP(w, r)
	})

	// /api/access/* — gated by session; access.Handler registers sub-routes.
	// We register access routes onto a sub-mux wrapped in RequireSession.
	accessMux := http.NewServeMux()
	mux.HandleFunc("/api/access/", func(w http.ResponseWriter, r *http.Request) {
		h := authReady.Load()
		if h == nil {
			http.Error(w, "auth service starting, please retry in a moment", http.StatusServiceUnavailable)
			return
		}
		h.RequireSession(accessMux).ServeHTTP(w, r)
	})

	// All other /api/* routes are gated by session.
	mux.HandleFunc("/api/", func(w http.ResponseWriter, r *http.Request) {
		h := authReady.Load()
		if h == nil {
			http.Error(w, "auth service starting, please retry in a moment", http.StatusServiceUnavailable)
			return
		}
		h.RequireSession(http.NotFoundHandler()).ServeHTTP(w, r)
	})

	// ── Start HTTP server immediately so /healthz responds ────────────────────
	addr := ":8090"
	slog.Info("console-backend listening", "addr", addr)
	go func() {
		if err := http.ListenAndServe(addr, mux); err != nil {
			slog.Error("server error", "err", err)
			os.Exit(1)
		}
	}()

	// ── OIDC provider discovery (retries with backoff) ────────────────────────
	// Runs after the server is already accepting connections so /healthz passes
	// while we wait for ZITADEL to become available.
	//
	// ZITADEL_INTERNAL_URL (http://zitadel.access.svc.cluster.local:8080) bypasses
	// the CoreDNS override (auth.stardatadb.com → ClusterIP:443 → pod:8080).
	// Port 8080 is plain HTTP. The custom transport injects Host: auth.stardatadb.com
	// so ZITADEL's instance router matches ExternalDomain.
	// InsecureIssuerURLContext lets go-oidc accept the external issuer URL in the
	// discovery doc even though we fetched it from the internal address.
	// When ZITADEL_INTERNAL_URL is unset the public URL is used as-is.
	discoveryURL := zitadelDomain
	var oidcRoundTripper http.RoundTripper
	if zitadelInternalURL != "" {
		discoveryURL = zitadelInternalURL
		// ZITADEL serves HTTPS with a self-signed cert on the internal service.
		// Skip TLS verification (traffic never leaves the cluster) and inject
		// Host: auth.stardatadb.com so ZITADEL's instance router matches ExternalDomain.
		tlsSkip := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}} //nolint:gosec
		oidcRoundTripper = newHostOverrideTransport(tlsSkip, zitadelDomain)
	}
	oidcCtx := oidc.InsecureIssuerURLContext(
		oidc.ClientContext(ctx, &http.Client{Transport: oidcRoundTripper}),
		zitadelDomain,
	)

	var provider *oidc.Provider
	for attempt, delay := 1, 2*time.Second; ; attempt++ {
		var err error
		provider, err = oidc.NewProvider(oidcCtx, discoveryURL)
		if err == nil {
			break
		}
		if attempt >= 10 {
			slog.Error("oidc provider discovery failed after retries", "err", err)
			os.Exit(1)
		}
		slog.Warn("oidc provider discovery failed, retrying", "attempt", attempt, "delay", delay, "err", err)
		time.Sleep(delay)
		if delay < 60*time.Second {
			delay *= 2
		}
	}

	// ── OAuth2 config ─────────────────────────────────────────────────────────
	// Override the token endpoint to the internal ZITADEL service so the token
	// exchange never goes through Cloudflare (whose cert the busybox image cannot
	// verify — no CA bundle). The Host header rewrite makes ZITADEL accept the
	// request as if it came from auth.stardatadb.com.
	ep := provider.Endpoint()
	if zitadelInternalURL != "" {
		internalToken, _ := url.JoinPath(zitadelInternalURL, "/oauth/v2/token")
		ep.TokenURL = internalToken
	}
	oauthCfg := &oauth2.Config{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		RedirectURL:  redirectURI,
		Endpoint:     ep,
		Scopes: []string{
			oidc.ScopeOpenID,
			"profile",
			"email",
			"offline_access", // refresh token
			"urn:zitadel:iam:org:project:id:" + projectID + ":aud",
		},
	}

	// ── TLS-skipping HTTP client for token exchange ───────────────────────────
	// Stored on the Handler so callback.go injects it into the oauth2 context.
	var internalClient *http.Client
	if zitadelInternalURL != "" {
		tlsSkip2 := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}} //nolint:gosec
		internalClient = &http.Client{Transport: newHostOverrideTransport(tlsSkip2, zitadelDomain)}
	}

	// ── ID token verifier ─────────────────────────────────────────────────────
	verifier := provider.Verifier(&oidc.Config{ClientID: clientID})

	// ── Console database connection ───────────────────────────────────────────
	consoleDB, err := sql.Open("postgres", mustEnv("CONSOLE_DB_DSN"))
	if err != nil {
		slog.Error("console db connect failed", "err", err)
		os.Exit(1)
	}
	consoleDB.SetMaxOpenConns(10)

	// ── ZITADEL machine-user client (console-backend service account) ─────────
	zKeyData := []byte(mustEnv("ZITADEL_MACHINE_KEY_JSON"))
	zClient, err := access.NewZitadelClient(
		zitadelDomain,
		zitadelInternalURL,
		mustEnv("ZITADEL_ORG_ID"),
		projectID,
		zKeyData,
	)
	if err != nil {
		slog.Error("zitadel client init failed", "err", err)
		os.Exit(1)
	}

	// ── Register access routes onto the sub-mux ───────────────────────────────
	accessHandler := &access.Handler{
		Z:            zClient,
		DB:           consoleDB,
		Valkey:       valkeyClient,
		OrgID:        mustEnv("ZITADEL_ORG_ID"),
		ProjectID:    projectID,
		InviteMailer: os.Getenv("INVITE_MAILER_URL"), // e.g. http://invite-mailer.access.svc.cluster.local:8025
		OwnerEmail:   os.Getenv("OWNER_EMAIL"),        // e.g. johnmillermca@gmail.com
	}
	accessHandler.Register(accessMux)

	// /api/signup — unauthenticated self-registration (no session required).
	// Registered here so the closure captures the fully-initialised accessHandler.
	mux.HandleFunc("POST /api/signup", accessHandler.HandleSignup)

	// Internal cron endpoints — secret-protected, not session-gated.
	accessHandler.RegisterInternal(mux, os.Getenv("CRON_SECRET"))

	// ── Publish the handler — auth routes become live ─────────────────────────
	h := &auth.Handler{
		OAuth:          oauthCfg,
		Verifier:       verifier,
		Valkey:         valkeyClient,
		ZitadelDomain:  zitadelDomain,
		SessionIdleTTL: time.Duration(sessionIdleSec) * time.Second,
		InternalClient: internalClient,
	}
	authReady.Store(h)
	slog.Info("oidc discovery complete, auth routes live", "issuer", zitadelDomain)

	// Block forever (server runs in a goroutine above).
	select {}
}

// handleMe returns session identity as JSON.
// Includes platform_roles decoded from the access token so the SPA can
// determine whether the sidebar admin nav should be shown.
func handleMe(w http.ResponseWriter, r *http.Request) {
	sess, _ := r.Context().Value(auth.SessionKey).(*auth.Session)

	// Decode platform_roles from the JWT access token.
	roles := platformRolesFromToken(sess.AccessToken)
	rolesJSON := "[]"
	if len(roles) > 0 {
		b := `["`
		for i, rr := range roles {
			if i > 0 {
				b += `","`
			}
			b += rr
		}
		b += `"]`
		rolesJSON = b
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"sub":"` + sess.Sub + `","email":"` + sess.Email +
		`","name":"` + sess.Name + `","mfa":` + boolStr(sess.MFA) +
		`,"platformRoles":` + rolesJSON + `}`))
}

// platformRolesFromToken decodes the platform_roles claim from a JWT access
// token without verifying the signature (the token was already verified at
// sign-in; we only need the claim value here).
func platformRolesFromToken(accessToken string) []string {
	parts := strings.Split(accessToken, ".")
	if len(parts) != 3 {
		return nil
	}
	payload := parts[1]
	// Pad to a multiple of 4 for standard base64.
	switch len(payload) % 4 {
	case 2:
		payload += "=="
	case 3:
		payload += "="
	}
	b, err := base64.URLEncoding.DecodeString(payload)
	if err != nil {
		return nil
	}
	var claims struct {
		PlatformRoles []string `json:"platform_roles"`
	}
	json.Unmarshal(b, &claims) //nolint:errcheck
	return claims.PlatformRoles
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


// hostOverrideTransport wraps an http.RoundTripper and rewrites the Host
// header on every request to the given value. This is required when fetching
// the OIDC discovery document directly from the in-cluster ZITADEL service
// (bypassing CoreDNS override): ZITADEL routes requests by Host header, so it
// must see "auth.stardatadb.com" even though we dial the internal FQDN.
type hostOverrideTransport struct {
	base http.RoundTripper
	host string // host[:port] — no scheme
}

func newHostOverrideTransport(base http.RoundTripper, issuerURL string) *hostOverrideTransport {
	host := issuerURL
	if u, err := url.Parse(issuerURL); err == nil {
		host = u.Host // e.g. "auth.stardatadb.com"
	}
	return &hostOverrideTransport{base: base, host: host}
}

func (t *hostOverrideTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	r2 := req.Clone(req.Context())
	r2.Host = t.host
	return t.base.RoundTrip(r2)
}
