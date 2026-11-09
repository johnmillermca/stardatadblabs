// =============================================================================
// internal/access/zitadel.go
// Thin ZITADEL v2 API client used by the access handlers.
// The console-backend machine user's JWT profile key is loaded once at startup
// and refreshed automatically before it expires.
// =============================================================================
package access

import (
	"bytes"
	"context"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// ZitadelClient makes authenticated calls to the ZITADEL v2 REST API using a
// machine-user JWT profile.
type ZitadelClient struct {
	domain    string // "https://auth.stardatadb.com" — used as JWT audience and Host header
	apiBase   string // base URL for API calls: internalURL when set, else domain
	orgID     string // ZITADEL org ID
	projectID string // stardata-platform project ID
	keyID     string // JWT key ID from the machine user key file
	issuer    string // service account user ID (sub)
	privKey   *rsa.PrivateKey

	mu             sync.Mutex
	cachedToken    string    // project-scoped token (for /v2/users, /v2/authorizations, etc.)
	tokenExpiry    time.Time
	sysToken       string    // system-scoped token (for /v2/sessions, /v2/oidc/auth_requests)
	sysTokenExpiry time.Time

	httpClient *http.Client
}

// JWTKeyFile is the JSON key file format ZITADEL emits for machine users.
type JWTKeyFile struct {
	Type    string `json:"type"`
	KeyID   string `json:"keyId"`
	Key     string `json:"key"`   // PEM private key
	UserID  string `json:"userId"`
	AppID   string `json:"appId"`
}

// NewZitadelClient creates a client from a machine-user JSON key file.
// internalURL may be "" (falls back to domain) or an in-cluster address.
func NewZitadelClient(domain, internalURL, orgID, projectID string, keyData []byte) (*ZitadelClient, error) {
	var kf JWTKeyFile
	if err := json.Unmarshal(keyData, &kf); err != nil {
		return nil, fmt.Errorf("parse key file: %w", err)
	}

	block, _ := pem.Decode([]byte(kf.Key))
	if block == nil {
		return nil, fmt.Errorf("no PEM block in key")
	}
	priv, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse private key: %w", err)
	}
	rsaKey, ok := priv.(*rsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("expected RSA key, got %T", priv)
	}

	apiBase   := domain
	transport := http.DefaultTransport
	if internalURL != "" {
		// Dial the in-cluster ZITADEL service directly (bypasses Cloudflare).
		// TLS cert is self-signed; skip verification — traffic stays in the cluster.
		// Rewrite Host to the external domain so ZITADEL's instance router matches.
		base := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}} //nolint:gosec
		transport = &hostOverrideTransport{base: base, host: mustHost(domain)}
		apiBase = internalURL
	}

	return &ZitadelClient{
		domain:    domain,
		apiBase:   apiBase,
		orgID:     orgID,
		projectID: projectID,
		keyID:     kf.KeyID,
		issuer:    kf.UserID,
		privKey:   rsaKey,
		httpClient: &http.Client{Transport: transport, Timeout: 15 * time.Second},
	}, nil
}

// token returns a valid project-scoped bearer token, refreshing if necessary.
// Used for project-level API calls (authorizations, users, etc.)
func (z *ZitadelClient) token(ctx context.Context) (string, error) {
	return z.mintToken(ctx,
		"openid urn:zitadel:iam:org:project:id:"+z.projectID+":aud urn:zitadel:iam:user:metadata urn:zitadel:iam:org:id:"+z.orgID,
		&z.cachedToken, &z.tokenExpiry)
}

// systemToken returns a ZITADEL-project-scoped bearer token for ZITADEL
// internal APIs (e.g. POST /v2/sessions, POST /v2/oidc/auth_requests).
// These endpoints require the token audience to be ZITADEL's own internal
// project ("urn:zitadel:iam:org:project:id:zitadel:aud"), not the
// application project audience.
// Cached separately from the project token.
func (z *ZitadelClient) systemToken(ctx context.Context) (string, error) {
	return z.mintToken(ctx, "urn:zitadel:iam:org:project:id:zitadel:aud", &z.sysToken, &z.sysTokenExpiry)
}

// mintToken mints a machine-user access token with the given scope.
// The token and its expiry are stored in *cached / *expiry (caller-provided pointers).
func (z *ZitadelClient) mintToken(ctx context.Context, scope string, cached *string, expiry *time.Time) (string, error) {
	z.mu.Lock()
	defer z.mu.Unlock()
	if *cached != "" && time.Now().Before(expiry.Add(-30*time.Second)) {
		return *cached, nil
	}

	now := time.Now()
	exp := now.Add(55 * time.Minute)
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"iss": z.issuer,
		"sub": z.issuer,
		"aud": []string{z.domain},
		"iat": now.Unix(),
		"exp": exp.Unix(),
	})
	tok.Header["kid"] = z.keyID
	signed, err := tok.SignedString(z.privKey)
	if err != nil {
		return "", fmt.Errorf("sign JWT: %w", err)
	}

	form := url.Values{
		"grant_type": {"urn:ietf:params:oauth:grant-type:jwt-bearer"},
		"scope":      {scope},
		"assertion":  {signed},
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost,
		z.apiBase+"/oauth/v2/token",
		bytes.NewBufferString(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("User-Agent", "console-backend/1.0 (stardatadb)")

	resp, err := z.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("token request: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("token exchange %d: %s", resp.StatusCode, body)
	}
	var result struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return "", fmt.Errorf("parse token response: %w", err)
	}
	*cached = result.AccessToken
	*expiry = time.Now().Add(time.Duration(result.ExpiresIn) * time.Second)
	return *cached, nil
}

// DoAuthed executes an authenticated ZITADEL API request using the project-scoped
// machine-user bearer token. Satisfies the auth.ZClient interface.
func (z *ZitadelClient) DoAuthed(ctx context.Context, method, path string, body any) ([]byte, int, error) {
	return z.do(ctx, method, path, body)
}

// DoSystem executes an authenticated ZITADEL API request using the system-scoped
// (openid-only) machine-user bearer token. Used for /v2/sessions and
// /v2/oidc/auth_requests which reject the project-audience token.
func (z *ZitadelClient) DoSystem(ctx context.Context, method, path string, body any) ([]byte, int, error) {
	tok, err := z.systemToken(ctx)
	if err != nil {
		return nil, 0, fmt.Errorf("get system token: %w", err)
	}
	return z.execRequest(ctx, method, path, body, tok)
}

// do executes an authenticated ZITADEL API request with the project-scoped token.
func (z *ZitadelClient) do(ctx context.Context, method, path string, body any) ([]byte, int, error) {
	tok, err := z.token(ctx)
	if err != nil {
		return nil, 0, fmt.Errorf("get token: %w", err)
	}
	return z.execRequest(ctx, method, path, body, tok)
}

// execRequest sends the HTTP request with the given bearer token.
func (z *ZitadelClient) execRequest(ctx context.Context, method, path string, body any, tok string) ([]byte, int, error) {

	var bodyReader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, 0, err
		}
		bodyReader = bytes.NewReader(b)
	}

	req, err := http.NewRequestWithContext(ctx, method, z.apiBase+path, bodyReader)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "console-backend/1.0 (stardatadb)")

	resp, err := z.httpClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	return respBody, resp.StatusCode, nil
}

// ── User helpers ─────────────────────────────────────────────────────────────

// ZUser is a minimal ZITADEL user shape used by the access handlers.
type ZUser struct {
	UserID          string `json:"userId"`
	Username        string `json:"username"`
	State           string `json:"state"`
	Human           *ZHuman `json:"human,omitempty"`
	Machine         *ZMachine `json:"machine,omitempty"`
}

type ZHuman struct {
	Profile ZProfile `json:"profile"`
	Email   ZEmail   `json:"email"`
}

type ZMachine struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

type ZProfile struct {
	GivenName  string `json:"givenName"`
	FamilyName string `json:"familyName"`
	DisplayName string `json:"displayName"`
}

type ZEmail struct {
	Email     string `json:"email"`
	IsVerified bool  `json:"isVerified"`
}

// GetUser fetches a single user by ID.
func (z *ZitadelClient) GetUser(ctx context.Context, userID string) (*ZUser, error) {
	b, status, err := z.do(ctx, http.MethodGet, "/v2/users/"+userID, nil)
	if err != nil {
		return nil, err
	}
	if status == http.StatusNotFound {
		return nil, nil
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("GetUser %d: %s", status, b)
	}
	var wrap struct{ User ZUser `json:"user"` }
	if err := json.Unmarshal(b, &wrap); err != nil {
		return nil, err
	}
	return &wrap.User, nil
}

// ListHumanUsers returns users matching the query (human accounts only).
// query fields: {"queries":[{"emailQuery":{"emailAddress":"...","method":"..."}},...]}
func (z *ZitadelClient) ListHumanUsers(ctx context.Context, query map[string]any) ([]ZUser, error) {
	b, status, err := z.do(ctx, http.MethodPost, "/v2/users", query)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("ListUsers %d: %s", status, b)
	}
	var wrap struct {
		Result []ZUser `json:"result"`
	}
	if err := json.Unmarshal(b, &wrap); err != nil {
		return nil, err
	}
	return wrap.Result, nil
}

// DeactivateUser disables the user account.
func (z *ZitadelClient) DeactivateUser(ctx context.Context, userID string) error {
	b, status, err := z.do(ctx, http.MethodPost, "/v2/users/"+userID+"/deactivate", map[string]any{})
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("DeactivateUser %d: %s", status, b)
	}
	return nil
}

// PasswordReset sends a password-reset email via ZITADEL.
func (z *ZitadelClient) PasswordReset(ctx context.Context, userID string) error {
	body := map[string]any{"sendLink": map[string]any{}}
	b, status, err := z.do(ctx, http.MethodPost, "/v2/users/"+userID+"/password_reset", body)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("PasswordReset %d: %s", status, b)
	}
	return nil
}

// RemoveMFA removes TOTP, U2F, and passkey auth methods from a user.
func (z *ZitadelClient) RemoveMFA(ctx context.Context, userID string) error {
	// Remove TOTP
	z.do(ctx, http.MethodDelete, "/v2/users/"+userID+"/auth_factors/otp", nil)          //nolint:errcheck
	// Remove U2F hardware keys (list first, then delete each)
	z.do(ctx, http.MethodDelete, "/v2/users/"+userID+"/auth_factors/u2f", nil)           //nolint:errcheck
	// Remove passkeys
	z.do(ctx, http.MethodDelete, "/v2/users/"+userID+"/auth_factors/passkeys", nil)      //nolint:errcheck
	return nil
}

// AddHumanUser creates a new human user and returns the user ID.
func (z *ZitadelClient) AddHumanUser(ctx context.Context, email, firstName, lastName string) (string, error) {
	body := map[string]any{
		"username": email,
		"organization": map[string]any{"orgId": z.orgID},
		"human": map[string]any{
			"profile": map[string]any{
				"givenName":   firstName,
				"familyName":  lastName,
				"displayName": firstName + " " + lastName,
			},
			"email": map[string]any{
				"email":      email,
				"isVerified": false,
			},
		},
	}
	b, status, err := z.do(ctx, http.MethodPost, "/v2/users/human", body)
	if err != nil {
		return "", err
	}
	if status == http.StatusConflict {
		return "", fmt.Errorf("email_exists")
	}
	if status != http.StatusCreated && status != http.StatusOK {
		return "", fmt.Errorf("AddHumanUser %d: %s", status, b)
	}
	var result struct{ UserID string `json:"userId"` }
	if err := json.Unmarshal(b, &result); err != nil {
		return "", err
	}
	return result.UserID, nil
}

// CreateInviteCode sends an invite email via ZITADEL (user gets a link to set their password).
func (z *ZitadelClient) CreateInviteCode(ctx context.Context, userID string) error {
	body := map[string]any{"sendCode": map[string]any{}}
	b, status, err := z.do(ctx, http.MethodPost, "/v2/users/"+userID+"/invite_code", body)
	if err != nil {
		return err
	}
	if status != http.StatusOK && status != http.StatusCreated {
		return fmt.Errorf("CreateInviteCode %d: %s", status, b)
	}
	return nil
}

// ── Authorization helpers ─────────────────────────────────────────────────────

// ZAuthorization represents one ZITADEL project authorization (role grant).
type ZAuthorization struct {
	ID        string   `json:"id"`
	UserID    string   `json:"userId"`
	ProjectID string   `json:"projectId"`
	Roles     []string `json:"roles"`
}

// ListAuthorizations returns all authorizations for a given user on the project.
func (z *ZitadelClient) ListAuthorizations(ctx context.Context, userID string) ([]ZAuthorization, error) {
	path := fmt.Sprintf("/v2/users/%s/authorizations?projectId=%s", userID, z.projectID)
	b, status, err := z.do(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	if status == http.StatusNotFound {
		return nil, nil
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("ListAuthorizations %d: %s", status, b)
	}
	var wrap struct {
		Result []ZAuthorization `json:"result"`
	}
	if err := json.Unmarshal(b, &wrap); err != nil {
		return nil, err
	}
	return wrap.Result, nil
}

// SetAuthorization creates or replaces the user's project authorization with the given roles.
// If an authorization already exists it is replaced; ZITADEL allows at most one per user per project.
func (z *ZitadelClient) SetAuthorization(ctx context.Context, userID string, roleKeys []string) error {
	// Try create first.
	body := map[string]any{
		"userId":    userID,
		"projectId": z.projectID,
		"roleKeys":  roleKeys,
	}
	b, status, err := z.do(ctx, http.MethodPost, "/v2/users/"+userID+"/authorizations", body)
	if err != nil {
		return err
	}
	if status == http.StatusOK || status == http.StatusCreated {
		return nil
	}
	// If conflict (409), update the existing one.
	if status == http.StatusConflict {
		auths, err := z.ListAuthorizations(ctx, userID)
		if err != nil || len(auths) == 0 {
			return fmt.Errorf("SetAuthorization conflict, no existing auth: %s", b)
		}
		authID := auths[0].ID
		ub, ustatus, uerr := z.do(ctx, http.MethodPut,
			fmt.Sprintf("/v2/users/%s/authorizations/%s", userID, authID),
			map[string]any{"roleKeys": roleKeys})
		if uerr != nil {
			return uerr
		}
		if ustatus != http.StatusOK {
			return fmt.Errorf("SetAuthorization update %d: %s", ustatus, ub)
		}
		return nil
	}
	return fmt.Errorf("SetAuthorization %d: %s", status, b)
}

// DeleteAuthorization removes the user's project authorization entirely.
func (z *ZitadelClient) DeleteAuthorization(ctx context.Context, userID string) error {
	auths, err := z.ListAuthorizations(ctx, userID)
	if err != nil {
		return err
	}
	for _, a := range auths {
		if a.ProjectID != z.projectID {
			continue
		}
		b, status, err := z.do(ctx, http.MethodDelete,
			fmt.Sprintf("/v2/users/%s/authorizations/%s", userID, a.ID), nil)
		if err != nil {
			return err
		}
		if status != http.StatusOK && status != http.StatusNoContent {
			return fmt.Errorf("DeleteAuthorization %d: %s", status, b)
		}
	}
	return nil
}

// ── Session helpers ───────────────────────────────────────────────────────────

// ListSessions returns active sessions for a user.
func (z *ZitadelClient) ListSessions(ctx context.Context, userID string) ([]map[string]any, error) {
	path := fmt.Sprintf("/v2/sessions?queries[0][userIdQuery][userId]=%s", url.QueryEscape(userID))
	b, status, err := z.do(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("ListSessions %d: %s", status, b)
	}
	var wrap struct{ Sessions []map[string]any `json:"sessions"` }
	if err := json.Unmarshal(b, &wrap); err != nil {
		return nil, err
	}
	return wrap.Sessions, nil
}

// DeleteSession terminates a single ZITADEL session by ID.
func (z *ZitadelClient) DeleteSession(ctx context.Context, sessionID string) error {
	b, status, err := z.do(ctx, http.MethodDelete, "/v2/sessions/"+sessionID, nil)
	if err != nil {
		return err
	}
	if status != http.StatusOK && status != http.StatusNoContent {
		return fmt.Errorf("DeleteSession %d: %s", status, b)
	}
	return nil
}

// ── Settings helpers ──────────────────────────────────────────────────────────

// GetLoginSettings returns the organisation's login policy settings.
func (z *ZitadelClient) GetLoginSettings(ctx context.Context) (map[string]any, error) {
	b, status, err := z.do(ctx, http.MethodGet,
		fmt.Sprintf("/v2/settings/login?orgId=%s", z.orgID), nil)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("GetLoginSettings %d: %s", status, b)
	}
	var result map[string]any
	json.Unmarshal(b, &result)
	return result, nil
}

// ListProjectRoles returns all roles defined on the stardata-platform project.
func (z *ZitadelClient) ListProjectRoles(ctx context.Context) ([]map[string]any, error) {
	b, status, err := z.do(ctx, http.MethodGet,
		fmt.Sprintf("/v2/projects/%s/roles", z.projectID), nil)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("ListProjectRoles %d: %s", status, b)
	}
	var wrap struct{ Result []map[string]any `json:"result"` }
	json.Unmarshal(b, &wrap)
	return wrap.Result, nil
}

// ── Misc helpers ──────────────────────────────────────────────────────────────

func mustHost(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	return u.Host
}

// hostOverrideTransport rewrites the Host header (reused from main package pattern).
type hostOverrideTransport struct {
	base http.RoundTripper
	host string
}

func (t *hostOverrideTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	r2 := req.Clone(req.Context())
	r2.Host = t.host
	return t.base.RoundTrip(r2)
}

// logAndIgnore logs a ZITADEL call error but does not return it (best-effort cleanup).
func logAndIgnore(op string, err error) {
	if err != nil {
		slog.Warn("best-effort ZITADEL call failed", "op", op, "err", err)
	}
}

// listAllAuthorizations returns all authorizations across all users on the project.
// Used by members list and roles catalogue to compute per-level counts.
func (z *ZitadelClient) listAllAuthorizations(ctx context.Context) ([]ZAuthorization, error) {
	path := fmt.Sprintf("/v2/projects/%s/authorizations", z.projectID)
	b, status, err := z.do(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("listAllAuthorizations %d: %s", status, b)
	}
	var wrap struct {
		Result []ZAuthorization `json:"result"`
	}
	if err := json.Unmarshal(b, &wrap); err != nil {
		return nil, err
	}
	return wrap.Result, nil
}
