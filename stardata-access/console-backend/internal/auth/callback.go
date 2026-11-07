// =============================================================================
// console-backend/internal/auth/callback.go
// Step 2: ZITADEL redirects the browser back here with ?code=…&state=…
//
// What this does:
//  1. Fetches and deletes the pre-session from Valkey (single-use anti-CSRF).
//  2. Exchanges the authorization code for tokens (PKCE verifier prevents
//     stolen-code attacks).
//  3. Verifies the ID token: signature (JWKS), issuer, audience, expiry, nonce.
//  4. Checks the amr claim: Owners and Admins must have completed MFA.
//     If they haven't, they are sent to /mfa-required instead of the console.
//  5. Stores the session in Valkey and sets __Host-stardata_session cookie.
// =============================================================================
package auth

import (
	"encoding/json"
	"net/http"
	"slices"
	"time"

	"golang.org/x/oauth2"
)

// Callback handles GET /auth/callback from ZITADEL.
func (h *Handler) Callback(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// ── 1. Anti-CSRF: fetch and delete the pre-session (single use) ──────────
	state := r.URL.Query().Get("state")
	if state == "" {
		http.Error(w, "missing state", http.StatusBadRequest)
		return
	}

	raw, err := h.Valkey.GetDel(ctx, "pre:"+state).Bytes()
	if err != nil {
		// Expired, replayed, or forged request.
		http.Error(w, "sign-in expired or invalid, please try again", http.StatusBadRequest)
		return
	}

	var pre PreSession
	if err := json.Unmarshal(raw, &pre); err != nil {
		http.Error(w, "sign-in session corrupt", http.StatusBadRequest)
		return
	}

	// ── 2. Exchange the code for tokens (PKCE) ────────────────────────────────
	code := r.URL.Query().Get("code")
	if code == "" {
		http.Error(w, "missing code", http.StatusBadRequest)
		return
	}

	tok, err := h.OAuth.Exchange(ctx, code, oauth2.VerifierOption(pre.Verifier))
	if err != nil {
		http.Error(w, "sign-in failed", http.StatusBadRequest)
		return
	}

	// ── 3. Verify the ID token ────────────────────────────────────────────────
	rawID, _ := tok.Extra("id_token").(string)
	if rawID == "" {
		http.Error(w, "no id_token in response", http.StatusBadRequest)
		return
	}

	idt, err := h.Verifier.Verify(ctx, rawID)
	if err != nil {
		http.Error(w, "id token invalid", http.StatusBadRequest)
		return
	}
	if idt.Nonce != pre.Nonce {
		// Nonce mismatch: possible token injection.
		http.Error(w, "sign-in failed (nonce)", http.StatusBadRequest)
		return
	}

	// ── 4. Extract claims ─────────────────────────────────────────────────────
	var claims struct {
		Sub           string   `json:"sub"`
		Email         string   `json:"email"`
		Name          string   `json:"name"`
		AMR           []string `json:"amr"`
		PlatformRoles []string `json:"platform_roles"`
	}
	if err := idt.Claims(&claims); err != nil {
		http.Error(w, "claims parse error", http.StatusBadRequest)
		return
	}

	// Roles may also live in the access token (JWT) when id_token_role_assertion
	// is off but access_token_role_assertion is on. In that case, parse the
	// access token (tok.AccessToken) the same way. For StarData both are on,
	// so the ID token claims are sufficient.

	// ── 5. MFA enforcement for Owners and Admins ──────────────────────────────
	if needsMFA(claims.PlatformRoles) && !hasMFA(claims.AMR) {
		// Person is an Owner or Admin but signed in with password only.
		// Send them to the MFA-required page so they can enrol or re-sign-in
		// with their authenticator app or passkey.
		http.Redirect(w, r, "/mfa-required", http.StatusFound)
		return
	}

	// ── 6. Create session in Valkey ───────────────────────────────────────────
	sid := rand32()
	sessionData := map[string]any{
		"sub":               claims.Sub,
		"email":             claims.Email,
		"name":              claims.Name,
		"access_token":      tok.AccessToken,
		"refresh_token":     tok.RefreshToken,
		"id_token":          rawID,
		"access_expires_at": tok.Expiry.Unix(),
		"mfa":               hasMFA(claims.AMR),
		"created_at":        time.Now().Unix(),
	}

	if err := h.Valkey.HSet(ctx, "sess:"+sid, sessionData).Err(); err != nil {
		http.Error(w, "session store unavailable", http.StatusServiceUnavailable)
		return
	}
	h.Valkey.Expire(ctx, "sess:"+sid, 24*time.Hour)

	// ── 7. Set the session cookie ─────────────────────────────────────────────
	// __Host- prefix: browser enforces Secure, no Domain, Path=/ — the
	// strictest cookie available without SameSite=Strict.
	http.SetCookie(w, &http.Cookie{
		Name:     "__Host-stardata_session",
		Value:    sid,
		Path:     "/",
		Secure:   true,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		// MaxAge matches SESSION_MAX_TTL (7 days); Valkey TTL enforces idle.
		MaxAge: 7 * 24 * 60 * 60,
	})

	http.Redirect(w, r, pre.Next, http.StatusFound)
}

// needsMFA returns true when the person holds a role that requires MFA.
// Only Owners and Admins are checked here; all others may use password only.
func needsMFA(roles []string) bool {
	return slices.Contains(roles, "platform.owner") ||
		slices.Contains(roles, "platform.admin")
}

// hasMFA returns true when the AMR list contains at least one strong second
// factor.  ZITADEL's exact amr values depend on the server version; check them
// on staging by logging in with each method and printing the ID token AMR.
//
// Known ZITADEL v4 values:
//   - "mfa"  — general multi-factor indicator
//   - "otp"  — TOTP (authenticator app)
//   - "u2f"  — FIDO2 / security key as second factor
//   - "hwk"  — hardware key (passkey on a hardware token)
//   - "swk"  — software key (passkey on a phone / laptop)
//
// A passkey used as the sole authentication method already implies MFA
// (it is both "something you have" and "something you are/know"), so
// hwk / swk in the AMR list satisfy the MFA requirement.
func hasMFA(amr []string) bool {
	for _, v := range amr {
		switch v {
		case "mfa", "otp", "u2f", "hwk", "swk":
			return true
		}
	}
	return false
}
