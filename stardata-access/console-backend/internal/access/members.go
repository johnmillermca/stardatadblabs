// =============================================================================
// internal/access/members.go
// Handlers for the Members screen (Screen 4 & 5 in the canvas):
//   GET  /api/access/members           — list waiting + active members
//   GET  /api/access/members/{id}      — member detail with sessions + history
//   POST /api/access/members/{id}/approve
//   POST /api/access/members/{id}/decline
//   PUT  /api/access/members/{id}/levels
//   POST /api/access/members/{id}/revoke
//   DELETE /api/access/members/{id}/sessions/{sid}
//   POST /api/access/members/{id}/reset-mfa
//   POST /api/access/members/{id}/password-reset
// =============================================================================
package access

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/stardatadb/console-backend/internal/auth"
)

// ── Role catalogue (static; mirrors Terraform) ────────────────────────────────

// roleCatalogue is the authoritative ordered level list per component.
// The order defines the level hierarchy (low → high).
var roleCatalogue = []struct {
	Component string
	Prefix    string
	Levels    []string
	Claim     string
	Allows    string
}{
	{"Platform",    "platform.*", []string{"member", "admin", "owner"},              "platform_roles", "Owner: everything. Admin: members, apps, audit."},
	{"StarData",    "doris.*",    []string{"viewer", "analyst", "engineer", "admin"},"doris_role",     "SQL access via the StarData proxy; admin = backup, restore, nodes"},
	{"Kafka",       "kafka.*",    []string{"viewer", "producer", "engineer"},         "kafka_role",     "Topics data.*, OAUTHBEARER"},
	{"StarGraph",   "memgraph.*", []string{"viewer", "engineer"},                     "memgraph_role",  "Bolt token; engineer can write"},
	{"Flink",       "flink.*",    []string{"viewer", "engineer"},                     "flink_role",     "Flink UI through the gateway"},
	{"SQLMesh",     "sqlmesh.*",  []string{"engineer"},                               "sqlmesh_role",   "SQLMesh UI"},
	{"ETL Studio",  "etl.*",      []string{"viewer", "developer", "operator", "admin"},"etl_role",     "Pipelines, CDC, document sources"},
	{"MDM",         "mdm.*",      []string{"viewer", "steward", "admin"},             "mdm_role",       "Golden records, steward queue, rules"},
	{"Monitoring",  "monitoring.*",[]string{"viewer", "operator"},                    "monitoring_role","Monitoring area, Grafana role"},
}

// compPrefix maps component ID → role key prefix (e.g. "doris" → "doris").
var compPrefix = map[string]string{
	"platform":   "platform",
	"doris":      "doris",
	"kafka":      "kafka",
	"memgraph":   "memgraph",
	"flink":      "flink",
	"sqlmesh":    "sqlmesh",
	"etl":        "etl",
	"mdm":        "mdm",
	"monitoring": "monitoring",
}

// levelsToRoleKeys converts a levels map + platform role into ZITADEL role key strings.
// e.g. {"doris":"analyst","kafka":"viewer"}, "admin"
// → ["doris.analyst","kafka.viewer","platform.admin"]
func levelsToRoleKeys(levels map[string]string, platform string) []string {
	keys := make([]string, 0, len(levels)+1)
	for comp, level := range levels {
		if level == "" || level == "none" {
			continue
		}
		prefix, ok := compPrefix[comp]
		if !ok {
			continue
		}
		keys = append(keys, prefix+"."+level)
	}
	if platform != "" && platform != "none" && platform != "member" {
		keys = append(keys, "platform."+platform)
	}
	return keys
}

// roleKeysToLevels converts ZITADEL role keys → (levels map, platform role).
func roleKeysToLevels(keys []string) (map[string]string, string) {
	levels := make(map[string]string)
	platform := "member"
	for _, key := range keys {
		comp, level, ok := strings.Cut(key, ".")
		if !ok {
			continue
		}
		if comp == "platform" {
			platform = level
			continue
		}
		levels[comp] = level
	}
	return levels, platform
}

// isPlatformChange returns true when the new role keys differ in platform.* from old.
func isPlatformChange(oldKeys, newKeys []string) bool {
	oldP := platformFromKeys(oldKeys)
	newP := platformFromKeys(newKeys)
	return oldP != newP
}

func platformFromKeys(keys []string) string {
	for _, k := range keys {
		if strings.HasPrefix(k, "platform.") {
			return k
		}
	}
	return ""
}

// ── Member shape (API response) ────────────────────────────────────────────────

type memberResp struct {
	ID             string            `json:"id"`
	Name           string            `json:"name"`
	Email          string            `json:"email"`
	State          string            `json:"state"`   // waiting|active|deactivated
	EmailVerified  bool              `json:"emailVerified"`
	PersonalDomain bool              `json:"personalDomain"`
	CreatedAt      string            `json:"createdAt"`
	Platform       string            `json:"platform"`
	Levels         map[string]string `json:"levels"`
	LastSignIn     string            `json:"lastSignIn,omitempty"`
	MFA            bool              `json:"mfa,omitempty"`
}

type memberDetailResp struct {
	memberResp
	ZitadelUserID string            `json:"zitadelUserId"`
	MemberSince   string            `json:"memberSince"`
	Sessions      []sessionResp     `json:"sessions"`
	History       []auditRowResp    `json:"history"`
	Account       accountResp       `json:"account"`
}

type sessionResp struct {
	ID        string `json:"id"`
	UserAgent string `json:"userAgent"`
	IP        string `json:"ip"`
	CreatedAt string `json:"createdAt"`
	Method    string `json:"method"`
}

type accountResp struct {
	EmailVerified           bool     `json:"emailVerified"`
	MFAMethods              []string `json:"mfaMethods"`
	PasswordChangedAt       string   `json:"passwordChangedAt,omitempty"`
	LastSignIn              string   `json:"lastSignIn,omitempty"`
	FailedSignInsLast30Days int      `json:"failedSignInsLast30Days"`
}

type auditRowResp struct {
	EventID string `json:"eventId"`
	Ts      string `json:"ts"`
	Action  string `json:"action"`
	Result  string `json:"result"`
	IP      string `json:"ip,omitempty"`
}

// ── GET /api/access/members ───────────────────────────────────────────────────

func (h *Handler) handleListMembers(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	state := r.URL.Query().Get("state")   // "waiting" | "active"
	search := r.URL.Query().Get("q")

	// Fetch all human users with an authorization on the project.
	auths, err := h.Z.listAllAuthorizations(ctx)
	if err != nil {
		jsonErr(w, "failed to list authorizations: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Build a set of user IDs that have an authorization.
	authedIDs := make(map[string][]string) // userID → roleKeys
	for _, a := range auths {
		authedIDs[a.UserID] = a.Roles
	}

	// Fetch human users; filter by state.
	query := map[string]any{
		"queries": []map[string]any{
			{"stateQuery": map[string]any{"state": "USER_STATE_ACTIVE"}},
		},
		"asc": false,
	}
	users, err := h.Z.ListHumanUsers(ctx, query)
	if err != nil {
		jsonErr(w, "failed to list users: "+err.Error(), http.StatusInternalServerError)
		return
	}

	var result []memberResp
	for _, u := range users {
		if u.Human == nil {
			continue
		}
		email := u.Human.Email.Email
		name := u.Human.Profile.DisplayName
		if search != "" && !strings.Contains(strings.ToLower(name+email), strings.ToLower(search)) {
			continue
		}

		roleKeys := authedIDs[u.UserID]
		levels, platform := roleKeysToLevels(roleKeys)

		// "waiting" = has no authorization (no roles assigned yet)
		isWaiting := len(roleKeys) == 0

		if state == "waiting" && !isWaiting {
			continue
		}
		if state == "active" && isWaiting {
			continue
		}

		memberState := "active"
		if isWaiting {
			memberState = "waiting"
		}
		if u.State == "USER_STATE_INACTIVE" {
			memberState = "deactivated"
			if state != "" && state != "deactivated" {
				continue
			}
		}

		result = append(result, memberResp{
			ID:             u.UserID,
			Name:           name,
			Email:          email,
			State:          memberState,
			EmailVerified:  u.Human.Email.IsVerified,
			PersonalDomain: !strings.HasSuffix(email, "@stardatadb.com"),
			CreatedAt:      "", // ZITADEL v2 user list doesn't include createdAt in search results
			Platform:       platform,
			Levels:         levels,
		})
	}

	if result == nil {
		result = []memberResp{}
	}
	jsonOK(w, map[string]any{"items": result, "next": nil})
}

// ── GET /api/access/members/{id} ──────────────────────────────────────────────

func (h *Handler) handleGetMember(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	userID := r.PathValue("id")

	u, err := h.Z.GetUser(ctx, userID)
	if err != nil {
		jsonErr(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if u == nil || u.Human == nil {
		jsonErr(w, "member not found", http.StatusNotFound)
		return
	}

	auths, err := h.Z.ListAuthorizations(ctx, userID)
	if err != nil {
		jsonErr(w, err.Error(), http.StatusInternalServerError)
		return
	}
	var roleKeys []string
	if len(auths) > 0 {
		roleKeys = auths[0].Roles
	}
	levels, platform := roleKeysToLevels(roleKeys)

	// Sessions
	zSessions, _ := h.Z.ListSessions(ctx, userID)
	sessions := make([]sessionResp, 0, len(zSessions))
	for _, s := range zSessions {
		sid, _ := s["id"].(string)
		ua := agentFromSession(s)
		ip := ipFromSession(s)
		at := timeFromSession(s)
		method := methodFromSession(s)
		sessions = append(sessions, sessionResp{
			ID: sid, UserAgent: ua, IP: ip, CreatedAt: at, Method: method,
		})
	}

	// History — last 10 audit rows from console DB (Doris audit not yet wired)
	history := h.recentHistory(ctx, userID)

	detail := memberDetailResp{
		memberResp: memberResp{
			ID:             u.UserID,
			Name:           u.Human.Profile.DisplayName,
			Email:          u.Human.Email.Email,
			State:          zStateToAPIState(u.State, len(roleKeys) == 0),
			EmailVerified:  u.Human.Email.IsVerified,
			PersonalDomain: !strings.HasSuffix(u.Human.Email.Email, "@stardatadb.com"),
			Platform:       platform,
			Levels:         levels,
		},
		ZitadelUserID: userID,
		MemberSince:   "",
		Sessions:      sessions,
		History:       history,
		Account: accountResp{
			EmailVerified: u.Human.Email.IsVerified,
			MFAMethods:    []string{},
		},
	}
	jsonOK(w, detail)
}

// ── POST /api/access/members/{id}/approve ─────────────────────────────────────

func (h *Handler) handleApprove(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	userID := r.PathValue("id")
	sess := sessFromCtx(r)

	var body struct {
		Platform string            `json:"platform"`
		Levels   map[string]string `json:"levels"`
	}
	if err := decodeBody(r, &body); err != nil {
		jsonErr(w, "invalid body", http.StatusBadRequest)
		return
	}

	roleKeys := levelsToRoleKeys(body.Levels, body.Platform)
	if len(roleKeys) == 0 {
		jsonErr(w, "at least one level required", http.StatusBadRequest)
		return
	}

	// If the approval includes platform.owner or platform.admin, route through the approval flow.
	if hasPlatformAdminOrOwner(roleKeys) {
		approvalID, err := h.createApproval(ctx, sess, userID, nil, roleKeys, "approve sign-up with Owner/Admin role")
		if err != nil {
			jsonErr(w, err.Error(), http.StatusInternalServerError)
			return
		}
		jsonOK(w, map[string]any{"state": "pending_approval", "approvalId": approvalID})
		return
	}

	if err := h.Z.SetAuthorization(ctx, userID, roleKeys); err != nil {
		jsonErr(w, err.Error(), http.StatusInternalServerError)
		return
	}
	h.auditLog(ctx, map[string]any{
		"actor_id": sess.Sub, "actor_name": sess.Name,
		"target_id": userID, "action": "member.approve", "result": "ok",
		"after": roleKeys,
	})
	jsonOK(w, map[string]any{"state": "active"})
}

// ── POST /api/access/members/{id}/decline ─────────────────────────────────────

func (h *Handler) handleDecline(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	userID := r.PathValue("id")
	sess := sessFromCtx(r)

	var body struct{ Reason string `json:"reason"` }
	decodeBody(r, &body) //nolint:errcheck

	if err := h.Z.DeactivateUser(ctx, userID); err != nil {
		jsonErr(w, err.Error(), http.StatusInternalServerError)
		return
	}
	h.auditLog(ctx, map[string]any{
		"actor_id": sess.Sub, "actor_name": sess.Name,
		"target_id": userID, "action": "member.decline", "result": "ok",
		"reason": body.Reason,
	})
	jsonOK(w, map[string]any{"state": "deactivated"})
}

// ── PUT /api/access/members/{id}/levels ───────────────────────────────────────

func (h *Handler) handleSetLevels(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	userID := r.PathValue("id")
	sess := sessFromCtx(r)

	var body struct {
		Platform string            `json:"platform"`
		Levels   map[string]string `json:"levels"`
	}
	if err := decodeBody(r, &body); err != nil {
		jsonErr(w, "invalid body", http.StatusBadRequest)
		return
	}

	// Fetch existing roles for before/after audit and platform-change detection.
	auths, _ := h.Z.ListAuthorizations(ctx, userID)
	var oldKeys []string
	if len(auths) > 0 {
		oldKeys = auths[0].Roles
	}

	newKeys := levelsToRoleKeys(body.Levels, body.Platform)

	// Admins cannot change platform.* — only Owners can.
	if isPlatformChange(oldKeys, newKeys) {
		roles := platformRoles(sess.AccessToken)
		if !hasAny(roles, "platform.owner") {
			jsonErr(w, "forbidden: only Owners can change platform roles", http.StatusForbidden)
			return
		}
		// Two-person rule: platform changes go through approval.
		approvalID, err := h.createApproval(ctx, sess, userID, oldKeys, newKeys, "")
		if err != nil {
			jsonErr(w, err.Error(), http.StatusInternalServerError)
			return
		}
		jsonOK(w, map[string]any{"applied": false, "approvalId": approvalID})
		return
	}

	if err := h.Z.SetAuthorization(ctx, userID, newKeys); err != nil {
		jsonErr(w, err.Error(), http.StatusInternalServerError)
		return
	}
	h.auditLog(ctx, map[string]any{
		"actor_id": sess.Sub, "actor_name": sess.Name,
		"target_id": userID, "action": "member.set_levels", "result": "ok",
		"before": oldKeys, "after": newKeys,
	})
	jsonOK(w, map[string]any{"applied": true})
}

// ── POST /api/access/members/{id}/revoke ──────────────────────────────────────

func (h *Handler) handleRevoke(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	userID := r.PathValue("id")
	sess := sessFromCtx(r)

	var body struct {
		Reason       string `json:"reason"`
		SessionsOnly bool   `json:"sessionsOnly"`
	}
	decodeBody(r, &body) //nolint:errcheck

	if !body.SessionsOnly {
		// Delete ZITADEL authorization.
		if err := h.Z.DeleteAuthorization(ctx, userID); err != nil {
			jsonErr(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	// End all ZITADEL sessions.
	zSessions, _ := h.Z.ListSessions(ctx, userID)
	for _, s := range zSessions {
		if sid, ok := s["id"].(string); ok {
			logAndIgnore("DeleteSession", h.Z.DeleteSession(ctx, sid))
		}
	}

	// Delete Valkey console sessions for this user.
	h.deleteValkeySessions(ctx, userID)

	h.auditLog(ctx, map[string]any{
		"actor_id": sess.Sub, "actor_name": sess.Name,
		"target_id": userID, "action": "member.revoke", "result": "ok",
		"reason": body.Reason, "sessions_only": body.SessionsOnly,
	})
	jsonOK(w, map[string]any{"revoked": true})
}

// ── DELETE /api/access/members/{id}/sessions/{sid} ────────────────────────────

func (h *Handler) handleEndSession(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	sess := sessFromCtx(r)
	sid := r.PathValue("sid")

	if err := h.Z.DeleteSession(ctx, sid); err != nil {
		jsonErr(w, err.Error(), http.StatusInternalServerError)
		return
	}
	h.auditLog(ctx, map[string]any{
		"actor_id": sess.Sub, "actor_name": sess.Name,
		"action": "session.end", "result": "ok", "session_id": sid,
	})
	jsonOK(w, map[string]any{"ended": true})
}

// ── POST /api/access/members/{id}/reset-mfa ───────────────────────────────────

func (h *Handler) handleResetMFA(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	userID := r.PathValue("id")
	sess := sessFromCtx(r)

	if err := h.Z.RemoveMFA(ctx, userID); err != nil {
		jsonErr(w, err.Error(), http.StatusInternalServerError)
		return
	}
	h.auditLog(ctx, map[string]any{
		"actor_id": sess.Sub, "actor_name": sess.Name,
		"target_id": userID, "action": "member.reset_mfa", "result": "ok",
	})
	jsonOK(w, map[string]any{"reset": true})
}

// ── POST /api/access/members/{id}/password-reset ──────────────────────────────

func (h *Handler) handlePasswordReset(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	userID := r.PathValue("id")
	sess := sessFromCtx(r)

	if err := h.Z.PasswordReset(ctx, userID); err != nil {
		jsonErr(w, err.Error(), http.StatusInternalServerError)
		return
	}
	h.auditLog(ctx, map[string]any{
		"actor_id": sess.Sub, "actor_name": sess.Name,
		"target_id": userID, "action": "member.password_reset", "result": "ok",
	})
	jsonOK(w, map[string]any{"sent": true})
}

// ── GET /api/access/summary ───────────────────────────────────────────────────

func (h *Handler) handleSummary(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Count waiting members (no authorization).
	auths, _ := h.Z.listAllAuthorizations(ctx)
	authedIDs := make(map[string]bool)
	for _, a := range auths {
		authedIDs[a.UserID] = true
	}
	users, _ := h.Z.ListHumanUsers(ctx, map[string]any{
		"queries": []map[string]any{
			{"stateQuery": map[string]any{"state": "USER_STATE_ACTIVE"}},
		},
	})
	waiting := 0
	for _, u := range users {
		if u.Human != nil && !authedIDs[u.UserID] {
			waiting++
		}
	}

	// Count pending approvals.
	var pending int
	h.DB.QueryRowContext(ctx, `SELECT count(*) FROM approval WHERE state='pending'`).Scan(&pending) //nolint:errcheck

	jsonOK(w, map[string]any{
		"waitingCount":          waiting,
		"pendingApprovalsCount": pending,
	})
}

// ── Helper: approval flow ─────────────────────────────────────────────────────

func (h *Handler) createApproval(ctx context.Context, sess *auth.Session,
	targetUserID string, fromKeys, toKeys []string, reason string) (string, error) {

	id, err := h.nextApprovalID(ctx)
	if err != nil {
		return "", err
	}
	expiresAt := time.Now().Add(time.Hour)

	// Fetch target user name.
	u, _ := h.Z.GetUser(ctx, targetUserID)
	targetName := targetUserID
	if u != nil && u.Human != nil {
		targetName = u.Human.Profile.DisplayName
	}

	action := "set_platform_role"
	if len(toKeys) == 0 {
		action = "remove_platform_role"
	}

	payloadJSON := fmt.Sprintf(`{"from":%s,"to":%s}`,
		jsonArr(fromKeys), jsonArr(toKeys))

	_, err = h.DB.ExecContext(ctx, `
		INSERT INTO approval
		  (id, action, target_user, target_name, payload, reason,
		   requested_by, requested_name, expires_at, state)
		VALUES ($1,$2,$3,$4,$5::jsonb,$6,$7,$8,$9,'pending')`,
		id, action, targetUserID, targetName, payloadJSON, reason,
		sess.Sub, sess.Name, expiresAt)
	if err != nil {
		return "", fmt.Errorf("insert approval: %w", err)
	}
	return id, nil
}

func hasPlatformAdminOrOwner(keys []string) bool {
	for _, k := range keys {
		if k == "platform.admin" || k == "platform.owner" {
			return true
		}
	}
	return false
}

// ── Helper: Valkey session cleanup ────────────────────────────────────────────

func (h *Handler) deleteValkeySessions(ctx context.Context, userID string) {
	// Scan all sess:* keys and delete those belonging to this user.
	// This is O(sessions) — acceptable for admin-triggered revoke.
	iter := h.Valkey.Scan(ctx, 0, "sess:*", 100).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()
		sub, err := h.Valkey.HGet(ctx, key, "sub").Result()
		if err != nil {
			continue
		}
		if sub == userID {
			h.Valkey.Del(ctx, key) //nolint:errcheck
		}
	}
}

// ── Helper: ZITADEL session field extraction ──────────────────────────────────

func agentFromSession(s map[string]any) string {
	if ua, ok := s["userAgent"].(map[string]any); ok {
		if desc, ok := ua["description"].(string); ok {
			return desc
		}
	}
	return "unknown"
}

func ipFromSession(s map[string]any) string {
	if ip, ok := s["currentFactor"].(map[string]any); ok {
		if rem, ok := ip["remoteIp"].(string); ok {
			return rem
		}
	}
	return ""
}

func timeFromSession(s map[string]any) string {
	if t, ok := s["creationDate"].(string); ok {
		return t
	}
	return ""
}

func methodFromSession(s map[string]any) string {
	if factors, ok := s["factors"].(map[string]any); ok {
		methods := []string{}
		if _, ok := factors["password"]; ok {
			methods = append(methods, "password")
		}
		if _, ok := factors["passkey"]; ok {
			methods = append(methods, "passkey")
		}
		if _, ok := factors["totp"]; ok {
			methods = append(methods, "totp")
		}
		if len(methods) > 0 {
			return strings.Join(methods, " + ")
		}
	}
	return "unknown"
}

func zStateToAPIState(zState string, noAuth bool) string {
	if zState == "USER_STATE_INACTIVE" {
		return "deactivated"
	}
	if noAuth {
		return "waiting"
	}
	return "active"
}

// recentHistory returns the last 10 audit rows for a user.
// Queries the console DB audit_log view; falls back to empty if not yet set up.
func (h *Handler) recentHistory(ctx context.Context, userID string) []auditRowResp {
	rows, err := h.DB.QueryContext(ctx, `
		SELECT coalesce(event_id,''), coalesce(ts::text,''), coalesce(action,''),
		       coalesce(result,''), coalesce(ip,'')
		FROM   audit_log
		WHERE  actor_id = $1 OR target_id = $1
		ORDER  BY ts DESC
		LIMIT  10`,
		userID)
	if err != nil {
		// audit_log view not yet created — return empty history gracefully.
		return []auditRowResp{}
	}
	defer rows.Close()
	var result []auditRowResp
	for rows.Next() {
		var row auditRowResp
		rows.Scan(&row.EventID, &row.Ts, &row.Action, &row.Result, &row.IP) //nolint:errcheck
		result = append(result, row)
	}
	if result == nil {
		return []auditRowResp{}
	}
	return result
}

// jsonArr encodes a string slice as a compact JSON array.
func jsonArr(ss []string) string {
	if len(ss) == 0 {
		return "[]"
	}
	b := `["`
	for i, s := range ss {
		if i > 0 {
			b += `","`
		}
		b += s
	}
	b += `"]`
	return b
}
