// =============================================================================
// console-backend — BFF for console.stardatadb.com
//
// Routes:
//   POST /auth/login     — initiate OIDC login (redirect to Pocket ID)
//   GET  /auth/callback  — OIDC callback; sets session cookie; redirects to /
//   POST /auth/logout    — clears session cookie + Valkey entry; redirects to /
//   GET  /api/me         — returns session user info (JSON) or 401
//   GET  /api/users      — lists users (admin only) via Pocket ID API
//   GET  /healthz        — liveness/readiness probe
//
// Session: cookie __Host-stardata_session → Valkey key sess:<id>, TTL 24h.
// Auth:    Pocket ID OIDC (PKCE, no client_secret stored in cookies).
// =============================================================================
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"time"

	"stardata-console-backend/internal/auth"
	"stardata-console-backend/internal/api"
	"stardata-console-backend/internal/middleware"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg := loadConfig()

	// Session store (Valkey/Redis)
	store, err := auth.NewValkey(cfg.ValkeyAddr, cfg.ValkeyPassword)
	if err != nil {
		log.Error("valkey connect", "err", err)
		os.Exit(1)
	}

	oidcClient := auth.NewOIDCClient(auth.OIDCConfig{
		Issuer:      cfg.OIDCIssuer,
		ClientID:    cfg.ClientID,
		ClientSecret: cfg.ClientSecret,
		RedirectURL: cfg.RedirectURL,
		InternalTokenURL: cfg.InternalTokenURL,
		InternalJWKSURL:  cfg.InternalJWKSURL,
	})

	authHandler  := auth.NewHandler(store, oidcClient, log)
	apiHandler   := api.NewHandler(store, oidcClient, log)
	authn        := middleware.RequireSession(store, log)

	mux := http.NewServeMux()

	// Auth routes (no session required)
	mux.HandleFunc("POST /auth/login",    authHandler.Login)
	mux.HandleFunc("GET  /auth/callback", authHandler.Callback)
	mux.HandleFunc("POST /auth/logout",   authHandler.Logout)

	// API routes (session required)
	mux.Handle("GET /api/me",     authn(http.HandlerFunc(apiHandler.Me)))
	mux.Handle("GET /api/users",  authn(http.HandlerFunc(apiHandler.Users)))

	// Health
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok\n"))
	})

	addr := ":" + cfg.Port
	srv := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	log.Info("console-backend starting", "addr", addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Error("server error", "err", err)
		os.Exit(1)
	}
}

type config struct {
	Port             string
	ValkeyAddr       string
	ValkeyPassword   string
	OIDCIssuer       string
	ClientID         string
	ClientSecret     string
	RedirectURL      string
	InternalTokenURL string
	InternalJWKSURL  string
}

func loadConfig() config {
	return config{
		Port:             getenv("PORT", "8080"),
		ValkeyAddr:       getenv("VALKEY_ADDR", "valkey.access.svc.cluster.local:6379"),
		ValkeyPassword:   os.Getenv("VALKEY_PASSWORD"),
		OIDCIssuer:       getenv("OIDC_ISSUER", "https://auth.stardatadb.com"),
		ClientID:         mustEnv("OIDC_CLIENT_ID"),
		ClientSecret:     mustEnv("OIDC_CLIENT_SECRET"),
		RedirectURL:      getenv("OIDC_REDIRECT_URL", "https://console.stardatadb.com/auth/callback"),
		InternalTokenURL: getenv("OIDC_TOKEN_URL", "http://pocket-id.access.svc.cluster.local:80/api/oidc/token"),
		InternalJWKSURL:  getenv("OIDC_JWKS_URL",  "http://pocket-id.access.svc.cluster.local:80/.well-known/jwks.json"),
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		slog.Error("required env var missing", "key", key)
		os.Exit(1)
	}
	return v
}

// Satisfy unused import (context used in sub-packages)
var _ = context.Background
