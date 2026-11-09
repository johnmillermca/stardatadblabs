// =============================================================================
// internal/access/audit.go
// GET /api/access/audit        — paginated audit log from console DB
// GET /api/access/audit/export — CSV export (up to 100k rows)
// =============================================================================
package access

import (
	"encoding/csv"
	"fmt"
	"net/http"
)

type auditFilter struct {
	Who       string
	Component string
	Result    string
	From      string
	To        string
	Cursor    string
}

// GET /api/access/audit
func (h *Handler) handleListAudit(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := r.URL.Query()
	f := auditFilter{
		Who:       q.Get("who"),
		Component: q.Get("component"),
		Result:    q.Get("result"),
		From:      q.Get("from"),
		To:        q.Get("to"),
		Cursor:    q.Get("cursor"),
	}

	query, args := buildAuditQuery(f, 50, false)
	rows, err := h.DB.QueryContext(ctx, query, args...)
	if err != nil {
		jsonErr(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var result []map[string]any
	for rows.Next() {
		var (
			eventID, ts, source, actorID, actorName    string
			targetType, targetID, targetName, component string
			action, result_, reason, ip, requestID     string
		)
		rows.Scan( //nolint:errcheck
			&eventID, &ts, &source, &actorID, &actorName,
			&targetType, &targetID, &targetName, &component,
			&action, &result_, &reason, &ip, &requestID,
		)
		result = append(result, map[string]any{
			"eventId": eventID, "ts": ts, "source": source,
			"actorId": actorID, "actorName": actorName,
			"targetType": targetType, "targetId": targetID, "targetName": targetName,
			"component": component, "action": action,
			"result": result_, "reason": reason,
			"ip": ip, "requestId": requestID,
		})
	}
	if result == nil {
		result = []map[string]any{}
	}

	// Determine next cursor (use last ts as simple cursor).
	var next *string
	if len(result) == 50 {
		last := result[len(result)-1]["ts"].(string)
		next = &last
	}
	jsonOK(w, map[string]any{"items": result, "next": next})
}

// GET /api/access/audit/export
func (h *Handler) handleExportAudit(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := r.URL.Query()
	f := auditFilter{
		Who: q.Get("who"), Component: q.Get("component"),
		Result: q.Get("result"), From: q.Get("from"), To: q.Get("to"),
	}

	query, args := buildAuditQuery(f, 100_000, false)
	rows, err := h.DB.QueryContext(ctx, query, args...)
	if err != nil {
		jsonErr(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	w.Header().Set("Content-Type", "text/csv")
	w.Header().Set("Content-Disposition", `attachment; filename="audit.csv"`)
	cw := csv.NewWriter(w)
	cw.Write([]string{"ts", "event_id", "source", "actor_id", "actor_name", //nolint:errcheck
		"target_type", "target_id", "target_name", "component",
		"action", "result", "reason", "ip", "request_id"})

	for rows.Next() {
		var (
			eventID, ts, source, actorID, actorName    string
			targetType, targetID, targetName, component string
			action, result_, reason, ip, requestID     string
		)
		rows.Scan( //nolint:errcheck
			&eventID, &ts, &source, &actorID, &actorName,
			&targetType, &targetID, &targetName, &component,
			&action, &result_, &reason, &ip, &requestID,
		)
		cw.Write([]string{ //nolint:errcheck
			ts, eventID, source, actorID, actorName,
			targetType, targetID, targetName, component,
			action, result_, reason, ip, requestID,
		})
	}
	cw.Flush()
}

// buildAuditQuery constructs a parameterised SELECT against platform_audit.
func buildAuditQuery(f auditFilter, limit int, _ bool) (string, []any) {
	args := []any{}
	where := []string{}

	addArg := func(col, val string) {
		if val == "" {
			return
		}
		args = append(args, "%"+val+"%")
		where = append(where, fmt.Sprintf("(%s ILIKE $%d OR actor_name ILIKE $%d)",
			col, len(args), len(args)))
	}

	if f.Who != "" {
		args = append(args, "%"+f.Who+"%")
		n := len(args)
		where = append(where, fmt.Sprintf(
			"(actor_id ILIKE $%d OR actor_name ILIKE $%d OR target_id ILIKE $%d OR target_name ILIKE $%d)",
			n, n, n, n))
		_ = addArg // suppress unused warning
	}
	if f.Component != "" {
		args = append(args, f.Component)
		where = append(where, fmt.Sprintf("component = $%d", len(args)))
	}
	if f.Result != "" {
		args = append(args, f.Result)
		where = append(where, fmt.Sprintf("result = $%d", len(args)))
	}
	if f.From != "" {
		args = append(args, f.From)
		where = append(where, fmt.Sprintf("ts >= $%d", len(args)))
	}
	if f.To != "" {
		args = append(args, f.To)
		where = append(where, fmt.Sprintf("ts <= $%d", len(args)))
	}
	if f.Cursor != "" {
		args = append(args, f.Cursor)
		where = append(where, fmt.Sprintf("ts < $%d", len(args)))
	}

	q := `SELECT event_id, ts::text, source,
	             coalesce(actor_id,''), coalesce(actor_name,''),
	             coalesce(target_type,''), coalesce(target_id,''), coalesce(target_name,''),
	             coalesce(component,''), action,
	             result, coalesce(reason,''), coalesce(ip,''), coalesce(request_id,'')
	      FROM   platform_audit`
	if len(where) > 0 {
		q += " WHERE "
		for i, w := range where {
			if i > 0 {
				q += " AND "
			}
			q += w
		}
	}
	args = append(args, limit)
	q += fmt.Sprintf(" ORDER BY ts DESC LIMIT $%d", len(args))
	return q, args
}
