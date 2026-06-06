package investment

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func collectOnboardAuditRows(rows pgx.Rows) ([]map[string]interface{}, error) {
	defer rows.Close()
	fields := rows.FieldDescriptions()
	out := make([]map[string]interface{}, 0)
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, err
		}
		row := make(map[string]interface{}, len(fields))
		for i, f := range fields {
			if vals[i] == nil {
				row[string(f.Name)] = ""
			} else {
				row[string(f.Name)] = vals[i]
			}
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// ─── GetOnboardingAuditHistory ───────────────────────────────────────────────
// POST /investment/onboard/audit-history
// Body: { user_id, batch_id? }
// Returns all audit records across AMC/Scheme/DP/Demat/Folio for the given batch,
// plus the batch-level record from onboard_batch.

func GetOnboardingAuditHistory(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			UserID  string `json:"user_id"`
			BatchID string `json:"batch_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.UserID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrUserIIsRequired)
			return
		}
		if api.GetSessionFromCtx(r.Context()) == nil {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()

		// Query 1: batch-level info
		batchQ := `
			SELECT
				b.batch_id::text AS batch_id,
				COALESCE(b.user_id,'') AS user_id,
				COALESCE(b.user_email,'') AS user_email,
				COALESCE(b.source,'') AS source,
				COALESCE(b.status,'') AS status,
				COALESCE(b.approval_status,'') AS approval_status,
				TO_CHAR(b.created_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
				TO_CHAR(b.completed_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS completed_at
			FROM investment.onboard_batch b`

		// Query 2: entity-level audit union (AMC + Scheme + DP + Demat + Folio)
		entityQ := `
			SELECT
				'AMC' AS entity_type,
				a.action_id::text AS action_id,
				a.amc_id::text AS entity_id,
				COALESCE(m.amc_name,'') AS entity_name,
				COALESCE(m.internal_amc_code,'') AS entity_code,
				a.actiontype,
				a.processing_status,
				COALESCE(a.reason,'') AS reason,
				COALESCE(a.requested_by,'') AS requested_by,
				TO_CHAR(a.requested_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS requested_at,
				COALESCE(a.checker_by,'') AS checker_by,
				TO_CHAR(a.checker_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS checker_at,
				COALESCE(a.checker_comment,'') AS checker_comment
			FROM investment.auditactionamc a
			LEFT JOIN investment.masteramc m ON m.amc_id = a.amc_id
			JOIN investment.portfolio_onboarding_map pom ON pom.amc_id = a.amc_id

			UNION ALL

			SELECT
				'SCHEME' AS entity_type,
				a.action_id::text AS action_id,
				a.scheme_id::text AS entity_id,
				COALESCE(m.scheme_name,'') AS entity_name,
				COALESCE(m.isin,'') AS entity_code,
				a.actiontype,
				a.processing_status,
				COALESCE(a.reason,'') AS reason,
				COALESCE(a.requested_by,'') AS requested_by,
				TO_CHAR(a.requested_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS requested_at,
				COALESCE(a.checker_by,'') AS checker_by,
				TO_CHAR(a.checker_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS checker_at,
				COALESCE(a.checker_comment,'') AS checker_comment
			FROM investment.auditactionscheme a
			LEFT JOIN investment.masterscheme m ON m.scheme_id = a.scheme_id
			JOIN investment.portfolio_onboarding_map pom ON pom.scheme_id = a.scheme_id

			UNION ALL

			SELECT
				'DP' AS entity_type,
				a.action_id::text AS action_id,
				a.dp_id::text AS entity_id,
				COALESCE(m.dp_name,'') AS entity_name,
				COALESCE(m.dp_code,'') AS entity_code,
				a.actiontype,
				a.processing_status,
				COALESCE(a.reason,'') AS reason,
				COALESCE(a.requested_by,'') AS requested_by,
				TO_CHAR(a.requested_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS requested_at,
				COALESCE(a.checker_by,'') AS checker_by,
				TO_CHAR(a.checker_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS checker_at,
				COALESCE(a.checker_comment,'') AS checker_comment
			FROM investment.auditactiondp a
			LEFT JOIN investment.masterdepositoryparticipant m ON m.dp_id = a.dp_id
			JOIN investment.masterdepositoryparticipant mdp ON mdp.dp_id = a.dp_id AND mdp.batch_id = $BATCH_PLACEHOLDER

			UNION ALL

			SELECT
				'DEMAT' AS entity_type,
				a.action_id::text AS action_id,
				a.demat_id::text AS entity_id,
				COALESCE(m.demat_account_number,'') AS entity_name,
				COALESCE(m.depository,'') AS entity_code,
				a.actiontype,
				a.processing_status,
				COALESCE(a.reason,'') AS reason,
				COALESCE(a.requested_by,'') AS requested_by,
				TO_CHAR(a.requested_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS requested_at,
				COALESCE(a.checker_by,'') AS checker_by,
				TO_CHAR(a.checker_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS checker_at,
				COALESCE(a.checker_comment,'') AS checker_comment
			FROM investment.auditactiondemat a
			LEFT JOIN investment.masterdemataccount m ON m.demat_id = a.demat_id
			JOIN investment.portfolio_onboarding_map pom ON pom.demat_id = a.demat_id

			UNION ALL

			SELECT
				'FOLIO' AS entity_type,
				a.action_id::text AS action_id,
				a.folio_id::text AS entity_id,
				COALESCE(m.folio_number,'') AS entity_name,
				COALESCE(m.amc_name,'') AS entity_code,
				a.actiontype,
				a.processing_status,
				COALESCE(a.reason,'') AS reason,
				COALESCE(a.requested_by,'') AS requested_by,
				TO_CHAR(a.requested_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS requested_at,
				COALESCE(a.checker_by,'') AS checker_by,
				TO_CHAR(a.checker_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS checker_at,
				COALESCE(a.checker_comment,'') AS checker_comment
			FROM investment.auditactionfolio a
			LEFT JOIN investment.masterfolio m ON m.folio_id = a.folio_id
			JOIN investment.portfolio_onboarding_map pom ON pom.folio_id = a.folio_id`

		if strings.TrimSpace(req.BatchID) != "" {
			// Batch-level info
			batchRows, err := pgxPool.Query(ctx, batchQ+` WHERE b.batch_id::text = $1`, req.BatchID)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
				return
			}
			batchRecords, err := collectOnboardAuditRows(batchRows)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "failed to read batch info")
				return
			}

			// Entity-level audit filtered by batch
			filteredEntityQ := `
				SELECT
					'AMC' AS entity_type,
					a.action_id::text AS action_id,
					a.amc_id::text AS entity_id,
					COALESCE(m.amc_name,'') AS entity_name,
					COALESCE(m.internal_amc_code,'') AS entity_code,
					a.actiontype,
					a.processing_status,
					COALESCE(a.reason,'') AS reason,
					COALESCE(a.requested_by,'') AS requested_by,
					TO_CHAR(a.requested_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS requested_at,
					COALESCE(a.checker_by,'') AS checker_by,
					TO_CHAR(a.checker_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS checker_at,
					COALESCE(a.checker_comment,'') AS checker_comment
				FROM investment.auditactionamc a
				LEFT JOIN investment.masteramc m ON m.amc_id = a.amc_id
				JOIN investment.portfolio_onboarding_map pom ON pom.amc_id = a.amc_id AND pom.batch_id::text = $1

				UNION ALL

				SELECT
					'SCHEME' AS entity_type,
					a.action_id::text AS action_id,
					a.scheme_id::text AS entity_id,
					COALESCE(m.scheme_name,'') AS entity_name,
					COALESCE(m.isin,'') AS entity_code,
					a.actiontype,
					a.processing_status,
					COALESCE(a.reason,'') AS reason,
					COALESCE(a.requested_by,'') AS requested_by,
					TO_CHAR(a.requested_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS requested_at,
					COALESCE(a.checker_by,'') AS checker_by,
					TO_CHAR(a.checker_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS checker_at,
					COALESCE(a.checker_comment,'') AS checker_comment
				FROM investment.auditactionscheme a
				LEFT JOIN investment.masterscheme m ON m.scheme_id = a.scheme_id
				JOIN investment.portfolio_onboarding_map pom ON pom.scheme_id = a.scheme_id AND pom.batch_id::text = $1

				UNION ALL

				SELECT
					'DP' AS entity_type,
					a.action_id::text AS action_id,
					a.dp_id::text AS entity_id,
					COALESCE(m.dp_name,'') AS entity_name,
					COALESCE(m.dp_code,'') AS entity_code,
					a.actiontype,
					a.processing_status,
					COALESCE(a.reason,'') AS reason,
					COALESCE(a.requested_by,'') AS requested_by,
					TO_CHAR(a.requested_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS requested_at,
					COALESCE(a.checker_by,'') AS checker_by,
					TO_CHAR(a.checker_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS checker_at,
					COALESCE(a.checker_comment,'') AS checker_comment
				FROM investment.auditactiondp a
				LEFT JOIN investment.masterdepositoryparticipant m ON m.dp_id = a.dp_id
				JOIN investment.masterdepositoryparticipant dp2 ON dp2.dp_id = a.dp_id AND dp2.batch_id::text = $1

				UNION ALL

				SELECT
					'DEMAT' AS entity_type,
					a.action_id::text AS action_id,
					a.demat_id::text AS entity_id,
					COALESCE(m.demat_account_number,'') AS entity_name,
					COALESCE(m.depository,'') AS entity_code,
					a.actiontype,
					a.processing_status,
					COALESCE(a.reason,'') AS reason,
					COALESCE(a.requested_by,'') AS requested_by,
					TO_CHAR(a.requested_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS requested_at,
					COALESCE(a.checker_by,'') AS checker_by,
					TO_CHAR(a.checker_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS checker_at,
					COALESCE(a.checker_comment,'') AS checker_comment
				FROM investment.auditactiondemat a
				LEFT JOIN investment.masterdemataccount m ON m.demat_id = a.demat_id
				JOIN investment.portfolio_onboarding_map pom ON pom.demat_id = a.demat_id AND pom.batch_id::text = $1

				UNION ALL

				SELECT
					'FOLIO' AS entity_type,
					a.action_id::text AS action_id,
					a.folio_id::text AS entity_id,
					COALESCE(m.folio_number,'') AS entity_name,
					COALESCE(m.amc_name,'') AS entity_code,
					a.actiontype,
					a.processing_status,
					COALESCE(a.reason,'') AS reason,
					COALESCE(a.requested_by,'') AS requested_by,
					TO_CHAR(a.requested_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS requested_at,
					COALESCE(a.checker_by,'') AS checker_by,
					TO_CHAR(a.checker_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS checker_at,
					COALESCE(a.checker_comment,'') AS checker_comment
				FROM investment.auditactionfolio a
				LEFT JOIN investment.masterfolio m ON m.folio_id = a.folio_id
				JOIN investment.portfolio_onboarding_map pom ON pom.folio_id = a.folio_id AND pom.batch_id::text = $1

				UNION ALL

				SELECT
					'BATCH_FILE' AS entity_type,
					a.audit_id::text AS action_id,
					a.parent_record_id AS entity_id,
					COALESCE(f.stored_file_name, a.file_id, '') AS entity_name,
					COALESCE(a.file_id, '') AS entity_code,
					CASE WHEN a.action_type = 'CREATE' THEN 'UPLOAD_FILE' ELSE a.action_type END AS actiontype,
					a.processing_status,
					COALESCE(a.reason,'') AS reason,
					COALESCE(a.requested_by,'') AS requested_by,
					TO_CHAR(a.requested_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS requested_at,
					COALESCE(a.checker_by,'') AS checker_by,
					TO_CHAR(a.checker_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS checker_at,
					COALESCE(a.checker_comment,'') AS checker_comment
				FROM investment.additional_file_audit a
				LEFT JOIN investment.onboard_batch_files f
					ON f.file_id = a.file_id
					AND f.batch_id::text = a.parent_record_id
				WHERE a.module_key = 'investment-onboarding-additional'
				  AND a.parent_record_id = $1

				ORDER BY requested_at DESC`

			entityRows, err := pgxPool.Query(ctx, filteredEntityQ, req.BatchID)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
				return
			}
			entityRecords, err := collectOnboardAuditRows(entityRows)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "failed to read entity audit records")
				return
			}

			api.LogInfo("Onboarding AuditHistory batch=%s: batch=%d entity=%d records", req.BatchID, len(batchRecords), len(entityRecords))
			api.RespondWithPayload(w, true, "", map[string]interface{}{
				"batch":    batchRecords,
				"entities": entityRecords,
			})
			return
		}

		// No batch_id: return all batches summary
		_ = entityQ // used only in batch-filtered path above
		batchRows, err := pgxPool.Query(ctx, batchQ+`
			ORDER BY COALESCE(b.created_at,'1970-01-01'::timestamp) DESC
			LIMIT 500`)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		batchRecords, err := collectOnboardAuditRows(batchRows)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to read batch audit history")
			return
		}
		api.LogInfo("Onboarding AuditHistory: returned %d batch records", len(batchRecords))
		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"batch":    batchRecords,
			"entities": []interface{}{},
		})
	}
}
