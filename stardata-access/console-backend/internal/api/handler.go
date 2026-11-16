package api

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"stardata-console-backend/internal/auth"
	"stardata-console-backend/internal/middleware"
)

// Handler serves /api/* routes.
type Handler struct {
	store *auth.ValkeyStore
	oidc  *auth.OIDCClient
	log   *slog.Logger
}

func NewHandler(store *auth.ValkeyStore, oidc *auth.OIDCClient, log *slog.Logger) *Handler {
	return &Handler{store: store, oidc: oidc, log: log}
}

type meResponse struct {
	UserID string   `json:"user_id"`
	Email  string   `json:"email"`
	Name   string   `json:"name"`
	Roles  []string `json:"roles"`
}

// Me returns the current user's session data.
func (h *Handler) Me(w http.ResponseWriter, r *http.Request) {
	sess := middleware.SessionFromContext(r.Context())
	if sess == nil {
		http.Error(w, `{"error":"unauthenticated"}`, http.StatusUnauthorized)
		return
	}
	writeJSON(w, meResponse{
		UserID: sess.UserID,
		Email:  sess.Email,
		Name:   sess.Name,
		Roles:  sess.Roles,
	})
}

// Users proxies to Pocket ID's user list API (admin only).
// Requires the X-Auth-Request-User header set by oauth2-proxy to be present,
// and the session to carry at least the platform.admin role.
func (h *Handler) Users(w http.ResponseWriter, r *http.Request) {
	sess := middleware.SessionFromContext(r.Context())
	if sess == nil {
		http.Error(w, `{"error":"unauthenticated"}`, http.StatusUnauthorized)
		return
	}
	if !hasRole(sess.Roles, "platform.admin") && !hasRole(sess.Roles, "platform.owner") {
		http.Error(w, `{"error":"forbidden"}`, http.StatusForbidden)
		return
	}
	// Pocket ID user list is managed by the IAM admin — return a stub for now.
	// Full implementation fetches from pocket-id.access.svc.cluster.local/api/users
	// using the iam-admin service account key.
	writeJSON(w, map[string]string{"status": "ok"})
}

func hasRole(roles []string, role string) bool {
	for _, r := range roles {
		if r == role {
			return true
		}
	}
	return false
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		http.Error(w, `{"error":"encode error"}`, http.StatusInternalServerError)
	}
}
