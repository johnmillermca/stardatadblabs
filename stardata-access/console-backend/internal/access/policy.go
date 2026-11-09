// =============================================================================
// internal/access/policy.go
// GET /api/access/policy — Sign-in policy screen (Owner only, read-only).
// =============================================================================
package access

import (
	"fmt"
	"net/http"
	"os"
)

func (h *Handler) handleGetPolicy(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	loginSettings, _ := h.Z.GetLoginSettings(ctx)

	zitadelPods := parsePodEnv("ZITADEL_PODS")
	loginPods := parsePodEnv("ZITADEL_LOGIN_PODS")

	driftStatus := os.Getenv("TERRAFORM_DRIFT_STATUS")
	if driftStatus == "" {
		driftStatus = "unknown"
	}

	jsonOK(w, map[string]any{
		"zitadelPods":         zitadelPods,
		"loginPods":           loginPods,
		"driftStatus":         driftStatus,
		"signingKeyRotatedAt": os.Getenv("SIGNING_KEY_ROTATED_AT"),
		"settings": map[string]any{
			"selfSignUp":          settingBool(loginSettings, "allowRegister"),
			"emailVerification":   "Required before first sign-in",
			"mfa":                 "Required for Owners and Admins; optional for others",
			"passkeys":            "Allowed for everyone",
			"password":            "12+ characters, number, symbol",
			"lockout":             "After 5 wrong passwords; unlock by Admin",
			"sessionIdle":         "1 day",
			"sessionMax":          "7 days",
			"accessToken":         "JWT, 10 minutes",
			"allowedEmailDomains": []string{"stardatadb.com"},
			"branding":            "StarData logo, #3346C4, Inter",
			"smtp":                "Company SMTP relay · no-reply@stardatadb.com",
		},
	})
}

// parsePodEnv reads an env var of the form "ready/total" (e.g. "3/3").
func parsePodEnv(key string) map[string]int {
	v := os.Getenv(key)
	var ready, total int
	fmt.Sscanf(v, "%d/%d", &ready, &total) //nolint:errcheck
	return map[string]int{"ready": ready, "total": total}
}

func settingBool(m map[string]any, key string) bool {
	if m == nil {
		return false
	}
	b, _ := m[key].(bool)
	return b
}
