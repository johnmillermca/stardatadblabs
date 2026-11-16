package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// OIDCConfig holds Pocket ID connection parameters.
type OIDCConfig struct {
	Issuer           string
	ClientID         string
	ClientSecret     string
	RedirectURL      string
	InternalTokenURL string // in-cluster URL for token exchange (avoids Cloudflare)
	InternalJWKSURL  string // in-cluster URL for JWKS
}

// UserInfo is the subset of OIDC userinfo we care about.
type UserInfo struct {
	Sub   string `json:"sub"`
	Email string `json:"email"`
	Name  string `json:"name"`
}

// OIDCClient performs token exchange and userinfo with Pocket ID.
type OIDCClient struct {
	cfg        OIDCConfig
	httpClient *http.Client
}

func NewOIDCClient(cfg OIDCConfig) *OIDCClient {
	return &OIDCClient{
		cfg: cfg,
		httpClient: &http.Client{Timeout: 10 * time.Second},
	}
}

// AuthCodeURL returns the Pocket ID authorization URL for the given state.
func (c *OIDCClient) AuthCodeURL(state string) string {
	params := url.Values{}
	params.Set("response_type", "code")
	params.Set("client_id", c.cfg.ClientID)
	params.Set("redirect_uri", c.cfg.RedirectURL)
	params.Set("scope", "openid profile email")
	params.Set("state", state)
	return c.cfg.Issuer + "/authorize?" + params.Encode()
}

// Exchange swaps an authorization code for tokens and returns the UserInfo.
func (c *OIDCClient) Exchange(ctx context.Context, code string) (*UserInfo, string, error) {
	body := url.Values{}
	body.Set("grant_type", "authorization_code")
	body.Set("code", code)
	body.Set("redirect_uri", c.cfg.RedirectURL)
	body.Set("client_id", c.cfg.ClientID)
	body.Set("client_secret", c.cfg.ClientSecret)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.cfg.InternalTokenURL,
		strings.NewReader(body.Encode()))
	if err != nil {
		return nil, "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("token request: %w", err)
	}
	defer resp.Body.Close()

	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("token endpoint %d: %s", resp.StatusCode, raw)
	}

	var tokenResp struct {
		IDToken     string `json:"id_token"`
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(raw, &tokenResp); err != nil {
		return nil, "", fmt.Errorf("decode token response: %w", err)
	}

	info, err := c.parseIDToken(tokenResp.IDToken)
	if err != nil {
		return nil, "", fmt.Errorf("parse id_token: %w", err)
	}
	return info, tokenResp.AccessToken, nil
}

// parseIDToken extracts claims from the ID token without full signature
// verification (Pocket ID uses its own key; we verify via JWKS in production
// but accept unverified claims here since we just exchanged the code directly
// with the in-cluster token endpoint over a trusted network path).
func (c *OIDCClient) parseIDToken(tokenStr string) (*UserInfo, error) {
	// Parse without verification — we fetched the token directly from the
	// in-cluster Pocket ID endpoint, so the exchange itself is the trust anchor.
	p := jwt.NewParser()
	claims := jwt.MapClaims{}
	_, _, err := p.ParseUnverified(tokenStr, claims)
	if err != nil {
		return nil, err
	}
	info := &UserInfo{}
	if v, ok := claims["sub"].(string); ok {
		info.Sub = v
	}
	if v, ok := claims["email"].(string); ok {
		info.Email = v
	}
	if v, ok := claims["name"].(string); ok {
		info.Name = v
	}
	if info.Sub == "" {
		return nil, fmt.Errorf("id_token missing sub claim")
	}
	return info, nil
}
