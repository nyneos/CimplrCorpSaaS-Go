package cdm

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/policyengine/common"

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

		// Explicit columns (uuid::text) — SELECT * left uuids as binary blobs that
		// JSON-encode poorly and the FE audit table looked empty / broken.
		rows, err := pool.Query(r.Context(), `
			SELECT
				audit_id::text,
				variable_id::text,
				action_type,
				processing_status,
				COALESCE(reason, '') AS reason,
				COALESCE(requested_by, '') AS requested_by,
				requested_at,
				COALESCE(requested_ip, '') AS requested_ip,
				COALESCE(checker_by, '') AS checker_by,
				checker_at,
				COALESCE(checker_ip, '') AS checker_ip,
				COALESCE(checker_comment, '') AS checker_comment,
				old_name, new_name,
				old_data_type, new_data_type,
				old_unit, new_unit,
				old_label, new_label,
				old_description, new_description,
				old_domain, new_domain,
				old_source_system, new_source_system,
				old_canonical_ref, new_canonical_ref,
				old_user_alias, new_user_alias,
				old_nullable, new_nullable,
				old_status, new_status,
				old_is_deleted, new_is_deleted
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

		out := make([]map[string]interface{}, 0)
		for rows.Next() {
			var (
				auditID, varID, actionType, procStatus, reason, requestedBy, requestedIP string
				checkerBy, checkerIP, checkerComment                                     string
				requestedAt                                                              time.Time
				checkerAt                                                                *time.Time
				oldName, newName, oldDataType, newDataType, oldUnit, newUnit             *string
				oldLabel, newLabel, oldDesc, newDesc, oldDomain, newDomain               *string
				oldSource, newSource, oldCanon, newCanon, oldAlias, newAlias             *string
				oldStatus, newStatus                                                     *string
				oldNullable, newNullable, oldDeleted, newDeleted                         *bool
			)
			if err := rows.Scan(
				&auditID, &varID, &actionType, &procStatus, &reason, &requestedBy, &requestedAt, &requestedIP,
				&checkerBy, &checkerAt, &checkerIP, &checkerComment,
				&oldName, &newName, &oldDataType, &newDataType, &oldUnit, &newUnit,
				&oldLabel, &newLabel, &oldDesc, &newDesc, &oldDomain, &newDomain,
				&oldSource, &newSource, &oldCanon, &newCanon, &oldAlias, &newAlias,
				&oldNullable, &newNullable, &oldStatus, &newStatus, &oldDeleted, &newDeleted,
			); err != nil {
				api.LogErrorForResponse(w, "cdm audit-log scan: %v", err)
				api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to load CDM audit", "CDM_AUDIT_FAILED")
				return
			}
			row := map[string]interface{}{
				"audit_id":          auditID,
				"variable_id":       varID,
				"action_type":       actionType,
				"processing_status": procStatus,
				"reason":            nullIfBlank(reason),
				"requested_by":      nullIfBlank(requestedBy),
				"requested_at":      requestedAt.UTC().Format(time.RFC3339),
				"requested_ip":      nullIfBlank(requestedIP),
				"checker_by":        nullIfBlank(checkerBy),
				"checker_ip":        nullIfBlank(checkerIP),
				"checker_comment":   nullIfBlank(checkerComment),
				"old_name":          ptrStr(oldName), "new_name": ptrStr(newName),
				"old_data_type": ptrStr(oldDataType), "new_data_type": ptrStr(newDataType),
				"old_unit": ptrStr(oldUnit), "new_unit": ptrStr(newUnit),
				"old_label": ptrStr(oldLabel), "new_label": ptrStr(newLabel),
				"old_description": ptrStr(oldDesc), "new_description": ptrStr(newDesc),
				"old_domain": ptrStr(oldDomain), "new_domain": ptrStr(newDomain),
				"old_source_system": ptrStr(oldSource), "new_source_system": ptrStr(newSource),
				"old_canonical_ref": ptrStr(oldCanon), "new_canonical_ref": ptrStr(newCanon),
				"old_user_alias": ptrStr(oldAlias), "new_user_alias": ptrStr(newAlias),
				"old_nullable": ptrBool(oldNullable), "new_nullable": ptrBool(newNullable),
				"old_status": ptrStr(oldStatus), "new_status": ptrStr(newStatus),
				"old_is_deleted": ptrBool(oldDeleted), "new_is_deleted": ptrBool(newDeleted),
			}
			if checkerAt != nil {
				row["checker_at"] = checkerAt.UTC().Format(time.RFC3339)
			} else {
				row["checker_at"] = nil
			}
			out = append(out, row)
		}
		if err := rows.Err(); err != nil {
			api.LogErrorForResponse(w, "cdm audit-log rows: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to load CDM audit", "CDM_AUDIT_FAILED")
			return
		}
		api.RespondEnvelopeSuccess(w, "CDM audit fetched", out)
	}
}

func nullIfBlank(s string) interface{} {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return s
}

func ptrStr(p *string) interface{} {
	if p == nil {
		return nil
	}
	return *p
}

func ptrBool(p *bool) interface{} {
	if p == nil {
		return nil
	}
	return *p
}
