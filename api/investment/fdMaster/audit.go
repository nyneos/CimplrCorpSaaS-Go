package fdMaster

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"fmt"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ─── GetFDMasterAuditHistory ──────────────────────────────────────────────────
// GET /investment/fd/master/audit?fd_id=
// Response: { success: true, data: [...] }

func GetFDMasterAuditHistory(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		fdID := strings.TrimSpace(r.URL.Query().Get("fd_id"))
		if fdID == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrFDIDRequired)
			return
		}

		auditTable := resolveFDAuditTable(ctx, pgxPool)
		if auditTable == "" {
			respondFDAuditPayload(w, []map[string]interface{}{})
			return
		}

		schemaName, tableName := splitQualifiedTable(auditTable)
		cols, err := loadTableColumns(ctx, pgxPool, schemaName, tableName)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrFailedToQuery+": "+err.Error())
			return
		}
		refCol := pickFirstExistingColumn(cols, "fd_id", "master_id", "confirmation_id")
		if refCol == "" {
			respondFDAuditPayload(w, []map[string]interface{}{})
			return
		}

		rows, err := pgxPool.Query(ctx, fmt.Sprintf("SELECT * FROM %s WHERE %s = $1 ORDER BY requested_at DESC", auditTable, refCol), fdID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrFailedToQuery+": "+err.Error())
			return
		}
		defer rows.Close()

		payload, err := rowsToMaps(rows)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrRowsScanFailed+err.Error())
			return
		}
		uploadRows, err := pgxPool.Query(ctx, `
			SELECT
				('file-' || a.audit_id::text) AS audit_id,
				a.parent_record_id AS fd_id,
				a.file_id,
				CASE WHEN a.action_type = 'CREATE' THEN 'UPLOAD_FILE' ELSE a.action_type END AS action_type,
				COALESCE(a.processing_status, '') AS processing_status,
				COALESCE(a.reason, '') AS reason,
				COALESCE(a.requested_by, '') AS requested_by,
				COALESCE(TO_CHAR((a.requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'), '') AS requested_at,
				COALESCE(a.requested_ip, '') AS requested_ip,
				COALESCE(a.checker_by, '') AS checker_by,
				COALESCE(TO_CHAR((a.checker_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'), '') AS checker_at,
				COALESCE(a.checker_ip, '') AS checker_ip,
				COALESCE(a.checker_comment, '') AS checker_comment
			FROM investment.additional_file_audit a
			JOIN investment.fd_master_files f ON f.file_id = a.file_id AND f.fd_id::text = a.parent_record_id
			WHERE a.module_key = 'fd-master-additional'
			  AND a.parent_record_id = $1
			ORDER BY a.requested_at DESC`, fdID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrFailedToQuery+": "+err.Error())
			return
		}
		defer uploadRows.Close()
		uploadPayload, err := rowsToMaps(uploadRows)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrRowsScanFailed+err.Error())
			return
		}
		payload = append(payload, uploadPayload...)
		respondFDAuditPayload(w, payload)
	}
}

// ─── GetCashflowAuditHistory ──────────────────────────────────────────────────
// GET /investment/fd/master/cashflow/audit?fd_id=&cashflow_id=
// Response: { success: true, data: [...] }

func GetCashflowAuditHistory(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		fdID := strings.TrimSpace(r.URL.Query().Get("fd_id"))
		cashflowID := strings.TrimSpace(r.URL.Query().Get("cashflow_id"))
		if fdID == "" && cashflowID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "fd_id or cashflow_id is required")
			return
		}

		query := `
			SELECT a.*,
				COALESCE(ai.instance_id,'')        AS approval_instance_id,
				COALESCE(ai.status,'')             AS approval_engine_status,
				COALESCE(aie.instance_eye_id,'')   AS current_eye_id,
				COALESCE(aie.position::text,'')    AS current_eye_position,
				COALESCE(aie.approvals_required,0) AS approvals_required,
				COALESCE(aie.approvals_received,0) AS approvals_received,
				aie.sla_deadline                   AS sla_deadline,
				COALESCE(aie.is_escalated,false)   AS is_escalated
			FROM investment.fd_audit_cashflow_schedule a
			LEFT JOIN LATERAL (
				SELECT ai.* FROM uam.approval_instance ai
				WHERE ai.record_id = a.audit_id::text
				  AND ai.module_code = 'FIXED_DEPOSIT'
				  AND ai.status = 'PENDING'
				  AND ai.is_deleted = false
				ORDER BY ai.submitted_at DESC, ai.instance_id DESC
				LIMIT 1
			) ai ON true
			LEFT JOIN LATERAL (
				SELECT aie.* FROM uam.approval_instance_eye aie
				WHERE aie.instance_id = ai.instance_id
				  AND aie.status = 'ACTIVE'
				ORDER BY aie.position ASC, aie.instance_eye_id ASC
				LIMIT 1
			) aie ON true
			WHERE 1=1`
		var args []interface{}
		pos := 1
		if fdID != "" {
			query += fmt.Sprintf(" AND a.fd_id = $%d", pos)
			args = append(args, fdID)
			pos++
		}
		if cashflowID != "" {
			query += fmt.Sprintf(" AND a.cashflow_id = $%d", pos)
			args = append(args, cashflowID)
			pos++
		}
		query += " ORDER BY a.requested_at DESC"

		rows, err := pgxPool.Query(ctx, query, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrFailedToQuery+": "+err.Error())
			return
		}
		defer rows.Close()

		payload, err := rowsToMaps(rows)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrRowsScanFailed+err.Error())
			return
		}
		if payload == nil {
			payload = []map[string]interface{}{}
		}
		if fdID != "" {
			uploadRows, err := pgxPool.Query(ctx, `
				SELECT
					('file-' || a.audit_id::text) AS audit_id,
					a.parent_record_id AS fd_id,
					a.file_id,
					CASE WHEN a.action_type = 'CREATE' THEN 'UPLOAD_FILE' ELSE a.action_type END AS action_type,
					COALESCE(a.processing_status, '') AS audit_status,
					COALESCE(a.reason, '') AS reason,
					COALESCE(a.requested_by, '') AS requested_by,
					COALESCE(TO_CHAR((a.requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'), '') AS requested_at,
					COALESCE(a.requested_ip, '') AS requested_ip,
					COALESCE(a.checker_by, '') AS checker_by,
					COALESCE(TO_CHAR((a.checker_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'), '') AS checker_at,
					COALESCE(a.checker_ip, '') AS checker_ip,
					COALESCE(a.checker_comment, '') AS checker_comment
				FROM investment.additional_file_audit a
				JOIN investment.fd_cashflow_files f ON f.file_id = a.file_id AND f.fd_id::text = a.parent_record_id
				WHERE a.module_key = 'fd-cashflow-additional'
				  AND a.parent_record_id = $1
				ORDER BY a.requested_at DESC`, fdID)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, constants.ErrFailedToQuery+": "+err.Error())
				return
			}
			defer uploadRows.Close()
			uploadPayload, err := rowsToMaps(uploadRows)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, constants.ErrRowsScanFailed+err.Error())
				return
			}
			payload = append(payload, uploadPayload...)
		}
		respondFDAuditPayload(w, payload)
	}
}

func respondFDAuditPayload(w http.ResponseWriter, payload interface{}) {
	api.RespondEnvelopeSuccess(w, "Success", payload)
}
