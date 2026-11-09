// =============================================================================
// internal/access/invite.go
// POST /api/access/members/invite
// Creates a ZITADEL user and sends an invite code email, then stores the invite
// in the console DB so levels are applied when the person accepts.
// =============================================================================
package access

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

func (h *Handler) handleInvite(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	sess := sessFromCtx(r)

	var body struct {
		Email     string            `json:"email"`
		FirstName string            `json:"firstName"`
		LastName  string            `json:"lastName"`
		Platform  string            `json:"platform"`
		Levels    map[string]string `json:"levels"`
		Message   string            `json:"message"`
	}
	if err := decodeBody(r, &body); err != nil {
		jsonErr(w, "invalid body", http.StatusBadRequest)
		return
	}
	if body.Email == "" || body.FirstName == "" || body.LastName == "" {
		jsonErr(w, "email, firstName and lastName are required", http.StatusBadRequest)
		return
	}

	// Create the human user in ZITADEL.
	userID, err := h.Z.AddHumanUser(ctx, body.Email, body.FirstName, body.LastName)
	if err != nil {
		if err.Error() == "email_exists" {
			jsonErr(w, "email already exists", http.StatusConflict)
			return
		}
		jsonErr(w, fmt.Sprintf("create user: %s", err), http.StatusInternalServerError)
		return
	}

	// Send the invite code email (ZITADEL sends it).
	if err := h.Z.CreateInviteCode(ctx, userID); err != nil {
		jsonErr(w, fmt.Sprintf("send invite code: %s", err), http.StatusInternalServerError)
		return
	}

	// Apply roles immediately (no approval needed for invited users unless platform.*).
	roleKeys := levelsToRoleKeys(body.Levels, body.Platform)
	if len(roleKeys) > 0 && !hasPlatformAdminOrOwner(roleKeys) {
		h.Z.SetAuthorization(ctx, userID, roleKeys) //nolint:errcheck
	}

	// Store in invite table.
	levelsJSON, _ := json.Marshal(body.Levels)
	h.DB.ExecContext(ctx, `
		INSERT INTO invite (user_id, email, first_name, last_name, invited_by, invited_by_name, levels, platform)
		VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8)`,
		userID, body.Email, body.FirstName, body.LastName,
		sess.Sub, sess.Name, levelsJSON, body.Platform) //nolint:errcheck

	h.auditLog(ctx, map[string]any{
		"actor_id": sess.Sub, "actor_name": sess.Name,
		"target_id": userID, "action": "member.invite", "result": "ok",
		"email": body.Email,
	})

	jsonOK(w, map[string]any{
		"userId":    userID,
		"email":     body.Email,
		"sentAt":    time.Now().UTC().Format(time.RFC3339),
	})
}
