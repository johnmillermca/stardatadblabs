// =============================================================================
// internal/access/approvals.go
// GET  /api/access/approvals       — list pending (and optionally all) approvals
// POST /api/access/approvals/{id}  — decide: approve | reject
// POST /internal/cron/approvals    — cron: expire stale + email owner reminders
//
// Two-person rule: the person who created the approval cannot approve it.
// Only Owners can approve. Admins may view but not approve.
// =============================================================================
package access

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type approvalResp struct {
	ID              string `json:"id"`
	Action          string `json:"action"`
	TargetUserID    string `json:"targetUserId"`
	TargetUserName  string `json:"targetUserName"`
	Payload         string `json:"payload"`
	Reason          string `json:"reason,omitempty"`
	RequestedBy     string `json:"requestedBy"`
	RequestedByName string `json:"requestedByName"`
	RequestedAt     string `json:"requestedAt"`
	ExpiresAt       string `json:"expiresAt"`
	State           string `json:"state"`
	DecidedBy       string `json:"decidedBy,omitempty"`
	DecidedByName   string `json:"decidedByName,omitempty"`
	DecidedAt       string `json:"decidedAt,omitempty"`
}

// GET /api/access/approvals?state=pending|all
func (h *Handler) handleListApprovals(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	stateFilter := r.URL.Query().Get("state")
	if stateFilter == "" {
		stateFilter = "pending"
	}

	// Expire overdue approvals.
	h.DB.ExecContext(ctx, //nolint:errcheck
		`UPDATE approval SET state='expired' WHERE state='pending' AND expires_at < now()`)

	query := `
		SELECT id, action, target_user, target_name, payload::text, coalesce(reason,''),
		       requested_by, requested_name, requested_at::text, expires_at::text, state,
		       coalesce(decided_by,''), coalesce(decided_name,''), coalesce(decided_at::text,'')
		FROM   approval`
	args := []any{}
	if stateFilter != "all" {
		query += ` WHERE state = $1`
		args = append(args, stateFilter)
	}
	query += ` ORDER BY requested_at DESC LIMIT 100`

	rows, err := h.DB.QueryContext(ctx, query, args...)
	if err != nil {
		jsonErr(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var result []approvalResp
	for rows.Next() {
		var a approvalResp
		rows.Scan( //nolint:errcheck
			&a.ID, &a.Action, &a.TargetUserID, &a.TargetUserName,
			&a.Payload, &a.Reason,
			&a.RequestedBy, &a.RequestedByName, &a.RequestedAt, &a.ExpiresAt, &a.State,
			&a.DecidedBy, &a.DecidedByName, &a.DecidedAt,
		)
		result = append(result, a)
	}
	if result == nil {
		result = []approvalResp{}
	}
	jsonOK(w, map[string]any{"items": result})
}

// POST /api/access/approvals/{id}
func (h *Handler) handleDecideApproval(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	approvalID := r.PathValue("id")
	sess := sessFromCtx(r)

	// Only Owners can approve.
	if !hasAny(platformRoles(sess.AccessToken), "platform.owner") {
		jsonErr(w, "forbidden: only Owners can approve", http.StatusForbidden)
		return
	}

	var body struct {
		Decision string `json:"decision"` // "approve" | "reject"
		Comment  string `json:"comment"`
	}
	if err := decodeBody(r, &body); err != nil || (body.Decision != "approve" && body.Decision != "reject") {
		jsonErr(w, "decision must be 'approve' or 'reject'", http.StatusBadRequest)
		return
	}

	// Load the approval.
	var (
		action      string
		targetUser  string
		payload     string
		requestedBy string
		state       string
		expiresAt   time.Time
	)
	err := h.DB.QueryRowContext(ctx,
		`SELECT action, target_user, payload::text, requested_by, state, expires_at
		 FROM approval WHERE id = $1`, approvalID).
		Scan(&action, &targetUser, &payload, &requestedBy, &state, &expiresAt)
	if err != nil {
		jsonErr(w, "approval not found", http.StatusNotFound)
		return
	}

	if state != "pending" {
		jsonErr(w, fmt.Sprintf("approval is %s", state), http.StatusConflict)
		return
	}
	if time.Now().After(expiresAt) {
		jsonErr(w, "approval has expired", http.StatusConflict)
		return
	}
	// Two-person rule.
	if requestedBy == sess.Sub {
		jsonErr(w, "you cannot approve your own request", http.StatusConflict)
		return
	}

	if body.Decision == "reject" {
		h.DB.ExecContext(ctx, //nolint:errcheck
			`UPDATE approval SET state='rejected', decided_by=$1, decided_name=$2, decided_at=now()
			 WHERE id=$3`, sess.Sub, sess.Name, approvalID)
		h.auditLog(ctx, map[string]any{
			"actor_id": sess.Sub, "actor_name": sess.Name,
			"action": "approval.reject", "result": "ok", "approval_id": approvalID,
		})
		jsonOK(w, map[string]any{"state": "rejected"})
		return
	}

	// Parse the "to" role keys from the approval payload.
	var p struct {
		To []string `json:"to"`
	}
	json.Unmarshal([]byte(payload), &p) //nolint:errcheck

	if err := h.Z.SetAuthorization(ctx, targetUser, p.To); err != nil {
		jsonErr(w, err.Error(), http.StatusInternalServerError)
		return
	}

	h.DB.ExecContext(ctx, //nolint:errcheck
		`UPDATE approval SET state='approved', decided_by=$1, decided_name=$2, decided_at=now()
		 WHERE id=$3`, sess.Sub, sess.Name, approvalID)

	h.auditLog(ctx, map[string]any{
		"actor_id": sess.Sub, "actor_name": sess.Name,
		"target_id": targetUser, "action": "approval.approve", "result": "ok",
		"approval_id": approvalID,
	})
	jsonOK(w, map[string]any{"state": "approved"})
}

// POST /internal/cron/approvals
// Called every 5 min by the CronJob (not by a user browser).
// 1. Expires overdue approvals.
// 2. For each approval that has been pending >30 min, sends a reminder email
//    to the configured owner address via invite-mailer.
// 3. Returns a JSON summary of actions taken.
func (h *Handler) handleCronApprovals(w http.ResponseWriter, r *http.Request) {
	// Verify shared secret from Authorization header: "Bearer <secret>".
	auth := r.Header.Get("Authorization")
	secret, _ := strings.CutPrefix(auth, "Bearer ")
	if secret != h.cronSecret {
		jsonErr(w, "forbidden", http.StatusForbidden)
		return
	}

	ctx := r.Context()

	// 1. Expire overdue approvals.
	res, err := h.DB.ExecContext(ctx,
		`UPDATE approval SET state='expired' WHERE state='pending' AND expires_at < now()`)
	expired := int64(0)
	if err == nil {
		expired, _ = res.RowsAffected()
	}

	// 2. Find approvals pending >30 min that have not yet had a reminder sent.
	rows, err := h.DB.QueryContext(ctx, `
		SELECT id, action, target_name, target_user, requested_at::text
		FROM   approval
		WHERE  state = 'pending'
		  AND  requested_at < now() - interval '30 minutes'
		  AND  (reminder_sent_at IS NULL OR reminder_sent_at < now() - interval '4 hours')
		ORDER BY requested_at ASC
		LIMIT  20`)
	if err != nil {
		jsonErr(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	type pending struct {
		ID          string
		Action      string
		TargetName  string
		TargetUser  string
		RequestedAt string
	}
	var items []pending
	for rows.Next() {
		var p pending
		rows.Scan(&p.ID, &p.Action, &p.TargetName, &p.TargetUser, &p.RequestedAt) //nolint:errcheck
		items = append(items, p)
	}
	rows.Close()

	reminded := 0
	if h.InviteMailer != "" && h.OwnerEmail != "" && len(items) > 0 {
		// Build a single summary email listing all pending approvals.
		var sb strings.Builder
		sb.WriteString(fmt.Sprintf("There are %d approval(s) waiting for your action on console.stardatadb.com:\n\n", len(items)))
		for _, p := range items {
			sb.WriteString(fmt.Sprintf("  • [%s] %s — %s (since %s)\n", p.ID, p.Action, p.TargetName, p.RequestedAt))
		}
		sb.WriteString("\nVisit https://console.stardatadb.com to review and approve or reject.\n")

		payload := map[string]any{
			"to":   h.OwnerEmail,
			"name": "StarData Owner",
			// Reuse the invite-mailer /send-invite endpoint with a "system" level list
			// so the owner gets an email about pending items.
			"levels":  []string{},
			"subject": fmt.Sprintf("Action needed: %d pending approval(s)", len(items)),
			"body":    sb.String(),
		}
		b, _ := json.Marshal(payload)
		resp, err := http.Post(h.InviteMailer+"/send-invite", "application/json", bytes.NewReader(b)) //nolint:noctx
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode < 300 {
				reminded = len(items)
				// Mark reminder sent.
				ids := make([]string, len(items))
				for i, p := range items {
					ids[i] = p.ID
				}
				h.DB.ExecContext(ctx, //nolint:errcheck
					`UPDATE approval SET reminder_sent_at = now() WHERE id = ANY($1::text[])`,
					"{"+strings.Join(ids, ",")+"}")
			}
		}
	}

	jsonOK(w, map[string]any{
		"expired":  expired,
		"reminded": reminded,
	})
}
