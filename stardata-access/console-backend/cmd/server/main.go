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
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/redis/go-redis/v9"
	"golang.org/x/oauth2"

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
		// ZITADEL on port 8080 is plain HTTP. Inject Host: auth.stardatadb.com
		// so ZITADEL's instance router matches ExternalDomain instead of the
		// internal service FQDN.
		oidcRoundTripper = newHostOverrideTransport(http.DefaultTransport, zitadelDomain)
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

	// ── Publish the handler — auth routes become live ─────────────────────────
	h := &auth.Handler{
		OAuth:          oauthCfg,
		Verifier:       verifier,
		Valkey:         valkeyClient,
		ZitadelDomain:  zitadelDomain,
		SessionIdleTTL: time.Duration(sessionIdleSec) * time.Second,
	}
	authReady.Store(h)
	slog.Info("oidc discovery complete, auth routes live", "issuer", zitadelDomain)

	// Block forever (server runs in a goroutine above).
	select {}
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
