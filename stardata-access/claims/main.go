// =============================================================================
// stardata-claims — Actions v2 target
// Called by ZITADEL on preaccesstoken and preuserinfo.
// Maps project roles → flat claims: doris_role, kafka_role, …, platform_roles.
// Pure function: no DB, no network calls. Verifies ZITADEL-Signature HMAC.
// =============================================================================
package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"os"
	"slices"
	"strings"
)

// projectID is the ZITADEL project ID for stardata-platform.
// Set via ZITADEL_PROJECT_ID environment variable at deploy time.
var (
	projectID  string
	signingKey []byte
)

// Ordered levels per component (low → high). Only roles from stardata-platform count.
var order = map[string][]string{
	"doris":      {"viewer", "analyst", "engineer", "admin"},
	"kafka":      {"viewer", "producer", "engineer"},
	"memgraph":   {"viewer", "engineer"},
	"flink":      {"viewer", "engineer"},
	"sqlmesh":    {"engineer"},
	"etl":        {"viewer", "developer", "operator", "admin"},
	"mdm":        {"viewer", "steward", "admin"},
	"monitoring": {"viewer", "operator"},
}

type grant struct {
	ProjectID string   `json:"projectId"`
	Roles     []string `json:"roles"`
}

type claimsReq struct {
	Function   string  `json:"function"`
	UserGrants []grant `json:"user_grants"`
}

type claim struct {
	Key   string `json:"key"`
	Value any    `json:"value"`
}

func verifySignature(header string, body []byte, key []byte) bool {
	// ZITADEL sends: "v1=<hex-hmac-sha256>"
	const prefix = "v1="
	if !strings.HasPrefix(header, prefix) {
		return false
	}
	got, err := hex.DecodeString(strings.TrimPrefix(header, prefix))
	if err != nil {
		return false
	}
	mac := hmac.New(sha256.New, key)
	mac.Write(body)
	return hmac.Equal(mac.Sum(nil), got)
}

func handleClaims(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, "read error", http.StatusInternalServerError)
		return
	}

	if !verifySignature(r.Header.Get("ZITADEL-Signature"), body, signingKey) {
		slog.Warn("bad signature", "remote", r.RemoteAddr)
		http.Error(w, "bad signature", http.StatusUnauthorized)
		return
	}

	var in claimsReq
	if err := json.Unmarshal(body, &in); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}

	best := map[string]int{}
	platform := []string{}

	for _, g := range in.UserGrants {
		if g.ProjectID != projectID {
			continue
		}
		for _, role := range g.Roles {
			comp, level, ok := strings.Cut(role, ".")
			if !ok {
				continue
			}
			if comp == "platform" {
				platform = append(platform, role)
				continue
			}
			levels, known := order[comp]
			if !known {
				continue
			}
			idx := slices.Index(levels, level)
			if idx >= 0 && idx+1 > best[comp] {
				best[comp] = idx + 1
			} else if idx < 0 {
				slog.Warn("unknown level", "role", role)
			}
		}
	}

	// Build output claims
	out := make([]claim, 0, len(best)+1)
	for comp, n := range best {
		out = append(out, claim{Key: comp + "_role", Value: order[comp][n-1]})
	}
	out = append(out, claim{Key: "platform_roles", Value: platform})

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{"append_claims": out}); err != nil {
		slog.Error("encode error", "err", err)
	}
}

func handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

func main() {
	projectID = os.Getenv("ZITADEL_PROJECT_ID")
	if projectID == "" {
		slog.Error("ZITADEL_PROJECT_ID must be set")
		os.Exit(1)
	}

	key := os.Getenv("SIGNING_KEY")
	if key == "" {
		slog.Error("SIGNING_KEY must be set")
		os.Exit(1)
	}
	signingKey = []byte(key)

	mux := http.NewServeMux()
	mux.HandleFunc("/zitadel/claims", handleClaims)
	mux.HandleFunc("/healthz", handleHealth)

	slog.Info("stardata-claims listening", "addr", ":8080")
	if err := http.ListenAndServe(":8080", mux); err != nil {
		slog.Error("server error", "err", err)
		os.Exit(1)
	}
}
