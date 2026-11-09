// =============================================================================
// internal/access/roles.go
// GET /api/access/roles — returns the role catalogue with per-level user counts.
// The catalogue is derived from the ZITADEL project roles + authorizations.
// =============================================================================
package access

import (
	"net/http"
	"strings"
)

type roleCatalogueResp struct {
	Component string   `json:"component"`
	Prefix    string   `json:"prefix"`
	Levels    []string `json:"levels"`
	Claim     string   `json:"claim"`
	Allows    string   `json:"allows"`
	Counts    []int    `json:"counts"`
}

func (h *Handler) handleListRoles(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Fetch all authorizations to build per-level counts.
	auths, _ := h.Z.listAllAuthorizations(ctx)

	// Count people per component per level.
	// counts[component][level] = n
	counts := make(map[string]map[string]int)
	for _, a := range auths {
		for _, key := range a.Roles {
			comp, level, ok := strings.Cut(key, ".")
			if !ok {
				continue
			}
			if counts[comp] == nil {
				counts[comp] = make(map[string]int)
			}
			counts[comp][level]++
		}
	}

	result := make([]roleCatalogueResp, 0, len(roleCatalogue))
	for _, c := range roleCatalogue {
		// Map display component to the ZITADEL prefix key.
		prefixKey := strings.TrimSuffix(c.Prefix, ".*")
		levelCounts := make([]int, len(c.Levels))
		for i, lv := range c.Levels {
			levelCounts[i] = counts[prefixKey][lv]
		}
		result = append(result, roleCatalogueResp{
			Component: c.Component,
			Prefix:    c.Prefix,
			Levels:    c.Levels,
			Claim:     c.Claim,
			Allows:    c.Allows,
			Counts:    levelCounts,
		})
	}
	jsonOK(w, result)
}
