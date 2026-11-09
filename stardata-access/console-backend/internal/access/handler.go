// =============================================================================
// internal/access/handler.go
// Router and shared dependencies for the /api/access/* endpoints.
//
// All routes require a valid session (RequireSession middleware applied by
// main.go before these handlers are reached).  Additionally all access routes
// require platform.owner or platform.admin from the session's access token
// claims.  Sign-in policy is owner-only.
// =============================================================================
package access

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stardatadb/console-backend/internal/auth"
)

// Handler holds shared dependencies for all access routes.
type Handler struct {
	Z            *ZitadelClient
	DB           *sql.DB       // console Postgres (approval, app_identity, invite tables)
	Valkey       *redis.Client // for Valkey session invalidation
	OrgID        string
	ProjectID    string
	InviteMailer string // base URL of invite-mailer service, e.g. http://invite-mailer.access.svc.cluster.local:8025
	OwnerEmail   string // platform owner notification address
	cronSecret   string // shared secret for /internal/cron/* endpoints
}

// RegisterInternal mounts unauthenticated internal endpoints onto mux.
// These are NOT behind RequireSession — they are protected by a shared
// CRON_SECRET token verified in each handler.  Mount on the top-level mux
// before the session-gated catch-all.
func (h *Handler) RegisterInternal(mux *http.ServeMux, cronSecret string) {
	if cronSecret == "" {
		return // internal endpoints disabled when no secret is configured
	}
	h.cronSecret = cronSecret
	mux.HandleFunc("POST /internal/cron/approvals", h.handleCronApprovals)
}

// Register mounts all /api/access/* routes onto mux.
// The caller (main.go) has already wrapped the mux subtree with
// RequireSession, so every request here carries a valid *auth.Session.
func (h *Handler) Register(mux *http.ServeMux) {
	// Summary badge counts (sidebar)
	mux.HandleFunc("GET /api/access/summary", h.requireAdmin(h.handleSummary))

	// Members
	mux.HandleFunc("GET /api/access/members",                          h.requireAdmin(h.handleListMembers))
	mux.HandleFunc("GET /api/access/members/{id}",                     h.requireAdmin(h.handleGetMember))
	mux.HandleFunc("POST /api/access/members/invite",                  h.requireAdmin(h.handleInvite))
	mux.HandleFunc("POST /api/access/members/{id}/approve",            h.requireAdmin(h.handleApprove))
	mux.HandleFunc("POST /api/access/members/{id}/decline",            h.requireAdmin(h.handleDecline))
	mux.HandleFunc("PUT /api/access/members/{id}/levels",              h.requireAdmin(h.handleSetLevels))
	mux.HandleFunc("POST /api/access/members/{id}/revoke",             h.requireAdmin(h.handleRevoke))
	mux.HandleFunc("DELETE /api/access/members/{id}/sessions/{sid}",   h.requireAdmin(h.handleEndSession))
	mux.HandleFunc("POST /api/access/members/{id}/reset-mfa",          h.requireAdmin(h.handleResetMFA))
	mux.HandleFunc("POST /api/access/members/{id}/password-reset",     h.requireAdmin(h.handlePasswordReset))

	// Applications
	mux.HandleFunc("GET /api/access/apps",              h.requireAdmin(h.handleListApps))
	mux.HandleFunc("POST /api/access/apps",             h.requireAdmin(h.handleCreateApp))
	mux.HandleFunc("POST /api/access/apps/{id}/rotate", h.requireAdmin(h.handleRotateApp))
	mux.HandleFunc("POST /api/access/apps/{id}/revoke", h.requireAdmin(h.handleRevokeApp))

	// Roles catalogue (read-only)
	mux.HandleFunc("GET /api/access/roles", h.requireAdmin(h.handleListRoles))

	// Approvals
	mux.HandleFunc("GET /api/access/approvals",         h.requireAdmin(h.handleListApprovals))
	mux.HandleFunc("POST /api/access/approvals/{id}",   h.requireAdmin(h.handleDecideApproval))

	// Sign-in policy — Owner only
	mux.HandleFunc("GET /api/access/policy", h.requireOwner(h.handleGetPolicy))

	// Audit log
	mux.HandleFunc("GET /api/access/audit",         h.requireAdmin(h.handleListAudit))
	mux.HandleFunc("GET /api/access/audit/export",  h.requireAdmin(h.handleExportAudit))
}

// ── Role guards ───────────────────────────────────────────────────────────────

func (h *Handler) requireAdmin(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sess := sessFromCtx(r)
		if sess == nil {
			jsonErr(w, "unauthenticated", http.StatusUnauthorized)
			return
		}
		if !hasAny(platformRoles(sess.AccessToken), "platform.owner", "platform.admin") {
			jsonErr(w, "forbidden: Owner or Admin required", http.StatusForbidden)
			return
		}
		next(w, r)
	}
}

func (h *Handler) requireOwner(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sess := sessFromCtx(r)
		if sess == nil {
			jsonErr(w, "unauthenticated", http.StatusUnauthorized)
			return
		}
		if !hasAny(platformRoles(sess.AccessToken), "platform.owner") {
			jsonErr(w, "forbidden: Owner required", http.StatusForbidden)
			return
		}
		next(w, r)
	}
}

// ── Shared utilities ──────────────────────────────────────────────────────────

func sessFromCtx(r *http.Request) *auth.Session {
	s, _ := r.Context().Value(auth.SessionKey).(*auth.Session)
	return s
}

// platformRoles decodes platform_roles from the JWT access token payload.
// The token was already verified at sign-in; we only read claims here.
func platformRoles(accessToken string) []string {
	parts := strings.Split(accessToken, ".")
	if len(parts) != 3 {
		return nil
	}
	payload := padBase64(parts[1])
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

func padBase64(s string) string {
	switch len(s) % 4 {
	case 2:
		return s + "=="
	case 3:
		return s + "="
	}
	return s
}

func hasAny(roles []string, want ...string) bool {
	for _, r := range roles {
		for _, w := range want {
			if r == w {
				return true
			}
		}
	}
	return false
}

func jsonOK(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Warn("json encode", "err", err)
	}
}

func jsonErr(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg}) //nolint:errcheck
}

func decodeBody(r *http.Request, dst any) error {
	return json.NewDecoder(http.MaxBytesReader(nil, r.Body, 1<<20)).Decode(dst)
}

// nextApprovalID returns a sequential "A-NNNN" approval ID using a Postgres sequence.
func (h *Handler) nextApprovalID(ctx context.Context) (string, error) {
	var seq int64
	err := h.DB.QueryRowContext(ctx, `SELECT nextval('approval_id_seq')`).Scan(&seq)
	if err != nil {
		// Fallback to timestamp-based ID if sequence not yet created.
		return fmt.Sprintf("A-%d", time.Now().UnixMilli()%100000), nil
	}
	return fmt.Sprintf("A-%d", seq+1000), nil
}

// auditLog writes a console audit event. Synchronous log for now;
// the audit-forwarder will pick it up once wired to Kafka.
func (h *Handler) auditLog(ctx context.Context, event map[string]any) {
	event["ts"] = time.Now().UTC().Format(time.RFC3339Nano)
	event["source"] = "console"
	b, _ := json.Marshal(event)
	slog.Info("audit_event", "json", string(b))
}
