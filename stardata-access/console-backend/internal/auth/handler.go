// =============================================================================
// console-backend/internal/auth/handler.go
// Shared types and helpers for the auth package.
// =============================================================================
package auth

import (
	"crypto/rand"
	"encoding/base64"
	"net/http"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/redis/go-redis/v9"
	"golang.org/x/oauth2"
)

// Handler holds the dependencies shared across Login, Callback and middleware.
type Handler struct {
	// OAuth is the OAuth2 config for the ZITADEL authorization code flow.
	OAuth *oauth2.Config

	// Verifier verifies ZITADEL-issued ID tokens (checks JWKS, issuer, aud, exp).
	Verifier *oidc.IDTokenVerifier

	// Valkey is the Redis-compatible client used to store pre-sessions and
	// full sessions.  StarData uses Valkey (Redis-compatible) in production.
	Valkey *redis.Client

	// ZitadelDomain is the base URL of the ZITADEL instance, e.g.
	// "https://auth.stardatadb.com". Used to build end-session URLs.
	ZitadelDomain string

	// SessionIdleTTL is how long to extend the session on each authenticated
	// request. Defaults to 24 hours. The Valkey key expires when the browser
	// is idle for this long.
	SessionIdleTTL time.Duration

	// InternalClient is an HTTP client that routes token exchange requests
	// directly to the in-cluster ZITADEL service, bypassing Cloudflare TLS.
	// Nil when ZITADEL_INTERNAL_URL is unset (uses default transport).
	InternalClient *http.Client
}

// PreSession is serialised to JSON and stored in Valkey under "pre:<state>"
// for 15 minutes.  It binds the PKCE verifier, the nonce and the post-login
// redirect target to the state parameter so they survive the round trip to
// ZITADEL and back.
type PreSession struct {
	State    string `json:"state"`
	Nonce    string `json:"nonce"`
	Verifier string `json:"verifier"`
	Next     string `json:"next"`
}

// rand32 returns a 32-byte URL-safe random string (256 bits of entropy).
func rand32() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic("crypto/rand unavailable: " + err.Error())
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

