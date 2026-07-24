package cdm

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/policyengine/common"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// HandleAuditLog returns maker-checker audit rows for one CDM variable (newest activity first).
func HandleAuditLog(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		var req struct {
			VariableID string `json:"variable_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "invalid JSON body", "BAD_REQUEST")
			return
		}
		variableID := strings.TrimSpace(req.VariableID)
		if variableID == "" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "variable_id is required", "BAD_REQUEST")
			return
		}

		rows, err := pool.Query(r.Context(), `
			SELECT *
			FROM policyengine_svc.cdm_variable_audit
			WHERE variable_id = $1::uuid
			ORDER BY GREATEST(
				COALESCE(requested_at, '1970-01-01'::timestamptz),
				COALESCE(checker_at, '1970-01-01'::timestamptz)
			) DESC`, variableID)
		if err != nil {
			api.LogErrorForResponse(w, "cdm audit-log: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to load CDM audit", "CDM_AUDIT_FAILED")
			return
		}
		defer rows.Close()

		out, err := rowsToMaps(rows)
		if err != nil {
			api.LogErrorForResponse(w, "cdm audit-log scan: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to load CDM audit", "CDM_AUDIT_FAILED")
			return
		}
		api.RespondEnvelopeSuccess(w, "CDM audit fetched", out)
	}
}

func rowsToMaps(rows pgx.Rows) ([]map[string]interface{}, error) {
	fds := rows.FieldDescriptions()
	out := make([]map[string]interface{}, 0)
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, err
		}
		row := make(map[string]interface{}, len(fds))
		for i, fd := range fds {
			row[string(fd.Name)] = normalizeAuditValue(vals[i])
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func normalizeAuditValue(v interface{}) interface{} {
	switch t := v.(type) {
	case time.Time:
		return t.UTC().Format(time.RFC3339)
	case *time.Time:
		if t == nil {
			return nil
		}
		return t.UTC().Format(time.RFC3339)
	default:
		return v
	}
}
