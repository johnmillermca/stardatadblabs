package auth

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"log/slog"
	"net/http"
	"time"
)

const (
	cookieName = "__Host-stardata_session"
	stateCookie = "__Host-stardata_state"
)

// Handler handles /auth/* HTTP routes.
type Handler struct {
	store  *ValkeyStore
	oidc   *OIDCClient
	log    *slog.Logger
}

func NewHandler(store *ValkeyStore, oidc *OIDCClient, log *slog.Logger) *Handler {
	return &Handler{store: store, oidc: oidc, log: log}
}

// Login redirects the browser to Pocket ID's authorization endpoint.
func (h *Handler) Login(w http.ResponseWriter, r *http.Request) {
	state, err := randomToken()
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	// Store state in a short-lived cookie to verify on callback
	http.SetCookie(w, &http.Cookie{
		Name:     stateCookie,
		Value:    state,
		Path:     "/auth/",
		MaxAge:   300, // 5 minutes
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteLaxMode,
	})
	http.Redirect(w, r, h.oidc.AuthCodeURL(state), http.StatusFound)
}

// Callback handles the OIDC redirect from Pocket ID.
func (h *Handler) Callback(w http.ResponseWriter, r *http.Request) {
	// Verify state
	stateCook, err := r.Cookie(stateCookie)
	if err != nil || stateCook.Value != r.URL.Query().Get("state") {
		http.Error(w, "invalid state", http.StatusBadRequest)
		return
	}
	// Clear state cookie
	http.SetCookie(w, &http.Cookie{
		Name:     stateCookie,
		Path:     "/auth/",
		MaxAge:   -1,
		HttpOnly: true,
		Secure:   true,
	})

	code := r.URL.Query().Get("code")
	if code == "" {
		http.Error(w, "missing code", http.StatusBadRequest)
		return
	}

	userInfo, accessToken, err := h.oidc.Exchange(r.Context(), code)
	if err != nil {
		h.log.Error("oidc exchange failed", "err", err)
		http.Error(w, "authentication failed", http.StatusInternalServerError)
		return
	}

	sessID, err := h.store.Create(r.Context(), Session{
		UserID:      userInfo.Sub,
		Email:       userInfo.Email,
		Name:        userInfo.Name,
		AccessToken: accessToken,
	})
	if err != nil {
		h.log.Error("session create failed", "err", err)
		http.Error(w, "session error", http.StatusInternalServerError)
		return
	}

	http.SetCookie(w, &http.Cookie{
		Name:     cookieName,
		Value:    sessID,
		Path:     "/",
		MaxAge:   int(24 * time.Hour / time.Second),
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteLaxMode,
	})

	h.log.Info("user logged in", "email", userInfo.Email)
	http.Redirect(w, r, "/", http.StatusFound)
}

// Logout clears the session cookie and removes the Valkey entry.
func (h *Handler) Logout(w http.ResponseWriter, r *http.Request) {
	cook, err := r.Cookie(cookieName)
	if err == nil {
		_ = h.store.Delete(r.Context(), cook.Value)
	}
	http.SetCookie(w, &http.Cookie{
		Name:     cookieName,
		Path:     "/",
		MaxAge:   -1,
		HttpOnly: true,
		Secure:   true,
	})
	http.Redirect(w, r, "/", http.StatusFound)
}

func randomToken() (string, error) {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}
