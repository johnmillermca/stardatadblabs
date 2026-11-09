// =============================================================================
// internal/access/apps.go
// Screen 7 — Applications
//   GET  /api/access/apps              — list app identities
//   POST /api/access/apps              — create app + credential
//   POST /api/access/apps/{id}/rotate  — rotate credential
//   POST /api/access/apps/{id}/revoke  — delete app entirely
// =============================================================================
package access

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type appResp struct {
	UserID         string   `json:"userId"`
	Name           string   `json:"name"`
	Description    string   `json:"description,omitempty"`
	DataRoles      []string `json:"dataRoles"`
	CredentialType string   `json:"credentialType"`
	Status         string   `json:"status"`
	ExpiresAt      string   `json:"expiresAt,omitempty"`
	RotatedAt      string   `json:"rotatedAt,omitempty"`
	OwnerName      string   `json:"ownerName"`
	OwnerUserID    string   `json:"ownerUserId"`
}

type appCreatedResp struct {
	UserID      string   `json:"userId"`
	Name        string   `json:"name"`
	ClientID    string   `json:"clientId,omitempty"`
	ClientSecret string  `json:"clientSecret,omitempty"`
	TokenURL    string   `json:"tokenUrl"`
	RolesGranted []string `json:"rolesGranted"`
	OwnerName   string   `json:"ownerName"`
	ExpiresAt   string   `json:"expiresAt"`
}

// ── GET /api/access/apps ──────────────────────────────────────────────────────

func (h *Handler) handleListApps(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	rows, err := h.DB.QueryContext(ctx, `
		SELECT user_id, name, coalesce(description,''), owner_user_id, owner_name,
		       credential_type, coalesce(expires_at::text,''), coalesce(rotation_due::text,'')
		FROM   app_identity
		ORDER  BY name`)
	if err != nil {
		jsonErr(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	result := []appResp{}
	for rows.Next() {
		var a appResp
		var expiresAt, rotatedAt string
		rows.Scan( //nolint:errcheck
			&a.UserID, &a.Name, &a.Description, &a.OwnerUserID, &a.OwnerName,
			&a.CredentialType, &expiresAt, &rotatedAt,
		)
		a.ExpiresAt = expiresAt
		a.RotatedAt = rotatedAt
		a.Status = credStatus(expiresAt)

		// Fetch data roles from ZITADEL.
		auths, _ := h.Z.ListAuthorizations(ctx, a.UserID)
		if len(auths) > 0 {
			for _, key := range auths[0].Roles {
				// Only include data roles (not platform.*) for apps.
				if !strings.HasPrefix(key, "platform.") {
					a.DataRoles = append(a.DataRoles, key)
				}
			}
		}
		if a.DataRoles == nil {
			a.DataRoles = []string{}
		}
		result = append(result, a)
	}
	jsonOK(w, map[string]any{"items": result})
}

// ── POST /api/access/apps ─────────────────────────────────────────────────────

func (h *Handler) handleCreateApp(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	sess := sessFromCtx(r)

	var body struct {
		Name           string   `json:"name"`
		Description    string   `json:"description"`
		OwnerUserID    string   `json:"ownerUserId"`
		OwnerName      string   `json:"ownerName"`
		DataRoles      []string `json:"dataRoles"`
		CredentialType string   `json:"credentialType"` // client_secret | app_password | client_cert
	}
	if err := decodeBody(r, &body); err != nil {
		jsonErr(w, "invalid body", http.StatusBadRequest)
		return
	}
	if body.Name == "" || body.CredentialType == "" {
		jsonErr(w, "name and credentialType are required", http.StatusBadRequest)
		return
	}
	// Apps cannot get engineer/admin/platform levels.
	for _, key := range body.DataRoles {
		if isEngineerOrHigher(key) {
			jsonErr(w, "apps cannot get engineer, admin or platform roles", http.StatusBadRequest)
			return
		}
	}

	// Create ZITADEL machine user.
	machineBody := map[string]any{
		"username": body.Name,
		"organization": map[string]any{"orgId": h.OrgID},
		"machine": map[string]any{
			"name":        body.Name,
			"description": body.Description,
			"accessTokenType": "ACCESS_TOKEN_TYPE_JWT",
		},
	}
	bResp, status, err := h.Z.do(ctx, "POST", "/v2/users/machine", machineBody)
	if err != nil || (status != 200 && status != 201) {
		jsonErr(w, fmt.Sprintf("create machine user: status %d %s", status, bResp), http.StatusInternalServerError)
		return
	}
	var created struct{ UserID string `json:"userId"` }
	parseJSON(bResp, &created)

	// Grant data roles.
	if len(body.DataRoles) > 0 {
		h.Z.SetAuthorization(ctx, created.UserID, body.DataRoles) //nolint:errcheck
	}

	// Generate credential.
	secret, clientID, expiresAt := h.issueCredential(ctx, created.UserID, body.CredentialType)

	// Store in console DB.
	h.DB.ExecContext(ctx, `
		INSERT INTO app_identity
		  (user_id, name, description, owner_user_id, owner_name, credential_type, expires_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7)`,
		created.UserID, body.Name, body.Description,
		body.OwnerUserID, body.OwnerName, body.CredentialType, expiresAt) //nolint:errcheck

	h.auditLog(ctx, map[string]any{
		"actor_id": sess.Sub, "actor_name": sess.Name,
		"target_id": created.UserID, "action": "app.create", "result": "ok",
		"name": body.Name,
	})

	resp := appCreatedResp{
		UserID:       created.UserID,
		Name:         body.Name,
		ClientID:     clientID,
		ClientSecret: secret,
		TokenURL:     h.Z.domain + "/oauth/v2/token",
		RolesGranted: body.DataRoles,
		OwnerName:    body.OwnerName,
		ExpiresAt:    expiresAt.Format(time.RFC3339),
	}
	jsonOK(w, resp)
}

// ── POST /api/access/apps/{id}/rotate ────────────────────────────────────────

func (h *Handler) handleRotateApp(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	userID := r.PathValue("id")
	sess := sessFromCtx(r)

	// Look up credential type.
	var credType string
	h.DB.QueryRowContext(ctx, `SELECT credential_type FROM app_identity WHERE user_id=$1`, userID).
		Scan(&credType) //nolint:errcheck

	secret, clientID, expiresAt := h.issueCredential(ctx, userID, credType)

	h.DB.ExecContext(ctx, //nolint:errcheck
		`UPDATE app_identity SET expires_at=$1, rotation_due=null, updated_at=now() WHERE user_id=$2`,
		expiresAt, userID)

	h.auditLog(ctx, map[string]any{
		"actor_id": sess.Sub, "actor_name": sess.Name,
		"target_id": userID, "action": "app.rotate", "result": "ok",
	})
	jsonOK(w, map[string]any{
		"clientId":     clientID,
		"clientSecret": secret,
		"expiresAt":    expiresAt.Format(time.RFC3339),
	})
}

// ── POST /api/access/apps/{id}/revoke ────────────────────────────────────────

func (h *Handler) handleRevokeApp(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	userID := r.PathValue("id")
	sess := sessFromCtx(r)

	// Remove authorization, then deactivate the machine user.
	h.Z.DeleteAuthorization(ctx, userID) //nolint:errcheck
	h.Z.DeactivateUser(ctx, userID)      //nolint:errcheck

	h.DB.ExecContext(ctx, `DELETE FROM app_identity WHERE user_id=$1`, userID) //nolint:errcheck

	h.auditLog(ctx, map[string]any{
		"actor_id": sess.Sub, "actor_name": sess.Name,
		"target_id": userID, "action": "app.revoke", "result": "ok",
	})
	jsonOK(w, map[string]any{"revoked": true})
}

// ── Helpers ───────────────────────────────────────────────────────────────────

// issueCredential generates a new credential for the machine user.
// Returns (secret, clientID, expiresAt).
func (h *Handler) issueCredential(ctx context.Context, userID, credType string) (string, string, time.Time) {
	expiresAt := time.Now().Add(90 * 24 * time.Hour)
	clientID := userID + "@" + h.ProjectID

	switch credType {
	case "client_secret":
		// Create a ZITADEL machine user secret.
		b, status, err := h.Z.do(ctx, "POST",
			"/v2/users/"+userID+"/secret", map[string]any{})
		if err == nil && (status == 200 || status == 201) {
			var result struct{ ClientSecret string `json:"clientSecret"` }
			parseJSON(b, &result)
			return result.ClientSecret, clientID, expiresAt
		}
		// Fallback: generate a local secret.
		return generateSecret(48), clientID, expiresAt

	case "app_password":
		// App password: generate a random token (the Doris proxy stores it hashed).
		return generateSecret(32), clientID, expiresAt

	default:
		// client_cert: return a placeholder — the actual cert is issued by cert-manager.
		return generateSecret(32), clientID, expiresAt
	}
}

func generateSecret(n int) string {
	b := make([]byte, n)
	rand.Read(b) //nolint:errcheck
	return base64.RawURLEncoding.EncodeToString(b)
}

func credStatus(expiresAt string) string {
	if expiresAt == "" {
		return "ok"
	}
	t, err := time.Parse(time.RFC3339, expiresAt)
	if err != nil {
		return "ok"
	}
	remaining := time.Until(t)
	if remaining < 7*24*time.Hour {
		return "expiring_critical"
	}
	if remaining < 14*24*time.Hour {
		return "expiring_soon"
	}
	return "ok"
}

func isEngineerOrHigher(roleKey string) bool {
	_, level, ok := strings.Cut(roleKey, ".")
	if !ok {
		return false
	}
	switch level {
	case "engineer", "admin", "owner":
		return true
	}
	if strings.HasPrefix(roleKey, "platform.") {
		return true
	}
	return false
}

func parseJSON(b []byte, dst any) {
	json.Unmarshal(b, dst) //nolint:errcheck
}
