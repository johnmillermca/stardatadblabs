// =============================================================================
// internal/access/signup.go
// POST /api/signup  — unauthenticated self-registration endpoint.
//
// Called from www.stardatadb.com with {email, firstName, lastName}.
// Creates the ZITADEL human user (no password — ZITADEL sends a verification
// email), then notifies the platform owner that someone is waiting for approval.
// No roles are assigned; the user appears in the Members "Waiting" list.
// =============================================================================
package access

import (
	"fmt"
	"net/http"
	"strings"
	"time"
)

// HandleSignup handles POST /api/signup.
// This route is mounted without RequireSession — anyone can self-register.
func (h *Handler) HandleSignup(w http.ResponseWriter, r *http.Request) {
	// Only accept POST.
	if r.Method != http.MethodPost {
		jsonErr(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx := r.Context()

	var body struct {
		Email     string `json:"email"`
		FirstName string `json:"firstName"`
		LastName  string `json:"lastName"`
	}
	if err := decodeBody(r, &body); err != nil {
		jsonErr(w, "invalid request body", http.StatusBadRequest)
		return
	}

	body.Email = strings.TrimSpace(strings.ToLower(body.Email))
	body.FirstName = strings.TrimSpace(body.FirstName)
	body.LastName = strings.TrimSpace(body.LastName)

	if body.Email == "" || body.FirstName == "" || body.LastName == "" {
		jsonErr(w, "email, firstName and lastName are required", http.StatusBadRequest)
		return
	}
	if !strings.Contains(body.Email, "@") {
		jsonErr(w, "invalid email address", http.StatusBadRequest)
		return
	}

	// Create the human user in ZITADEL without a password.
	// ZITADEL will send an email verification / invite link automatically.
	userID, err := h.Z.AddHumanUser(ctx, body.Email, body.FirstName, body.LastName)
	if err != nil {
		if err.Error() == "email_exists" {
			jsonErr(w, "An account with this email already exists. Please sign in instead.", http.StatusConflict)
			return
		}
		jsonErr(w, fmt.Sprintf("could not create account: %s", err), http.StatusInternalServerError)
		return
	}

	// Send the ZITADEL invite code so the user gets an email to verify + set password.
	if err := h.Z.CreateInviteCode(ctx, userID); err != nil {
		// Non-fatal: user exists in ZITADEL, just log it.
		h.auditLog(ctx, map[string]any{
			"target_id": userID, "action": "signup.invite_code_failed",
			"result": "warn", "err": err.Error(),
		})
	}

	h.auditLog(ctx, map[string]any{
		"target_id": userID, "action": "signup.self_register",
		"result": "ok", "email": body.Email,
	})

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	fmt.Fprintf(w, `{"state":"waiting","userId":%q,"submittedAt":%q}`,
		userID, time.Now().UTC().Format(time.RFC3339))
}
