package fdBooking

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ─── Request types ────────────────────────────────────────────────────────────

type bookingAuditRequest struct {
	UserID    string `json:"user_id"`
	BookingID string `json:"booking_id"`
}

type confirmationAuditRequest struct {
	UserID         string `json:"user_id"`
	ConfirmationID string `json:"confirmation_id"`
}

// ─── GetBookingAuditHandler ───────────────────────────────────────────────────
// POST /investment/fd/booking/audit
// Body: { user_id, booking_id }
// Response: { success: true, data: [...] }

func GetBookingAuditHandler(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}

		var req bookingAuditRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil ||
			strings.TrimSpace(req.BookingID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "booking_id is required")
			return
		}

		ctx := r.Context()
		if code, msg := validateBookingAuditAccess(ctx, pgxPool, req.BookingID); code != 0 {
			api.RespondWithError(w, code, msg)
			return
		}

		rows, err := pgxPool.Query(ctx, `
			SELECT
				a.audit_id,
				a.booking_id,
				a.action_type,
				a.processing_status,
				COALESCE(a.requested_by, '')                                      AS requested_by,
				COALESCE(TO_CHAR(a.requested_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'), '')    AS requested_at,
				COALESCE(a.checker_by, '')                                        AS checker_by,
				COALESCE(TO_CHAR(a.checker_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'), '')      AS checker_at,
				COALESCE(a.checker_comment, '')                                   AS checker_comment,
				COALESCE(a.reason, '')                                            AS reason,
				COALESCE(m.entity_id, '')                                         AS entity_id,
				COALESCE(m.entity_name, '')                                       AS entity_name,
				COALESCE(m.bank_name, '')                                         AS bank_name,
				COALESCE(m.booking_status, '')                                    AS booking_status,
				COALESCE(m.principal_amount, 0)                                   AS principal_amount,
				COALESCE(m.interest_rate, 0)                                      AS interest_rate,
				COALESCE(m.interest_type_code, '')                                AS interest_type,
				COALESCE(m.tenure_days, 0)                                        AS tenor_days,
				COALESCE(TO_CHAR(m.value_date, 'YYYY-MM-DD'), '')                 AS value_date,
				COALESCE(TO_CHAR(m.expected_maturity_date, 'YYYY-MM-DD'), '')     AS maturity_date,
				COALESCE(TO_CHAR(m.expected_start_date, 'YYYY-MM-DD'), '')        AS expected_start_date,
				COALESCE(a.old_principal_amount, 0)                               AS old_principal_amount,
				COALESCE(a.old_interest_rate, 0)                                  AS old_interest_rate,
				COALESCE(a.old_interest_type_code, '')                            AS old_interest_type,
				COALESCE(a.old_interest_type_id, '')                              AS old_interest_type_id,
				COALESCE(a.old_tenure_days, 0)                                    AS old_tenor_days,
				COALESCE(a.old_tenor_type, '')                                    AS old_tenor_type,
				COALESCE(a.old_tenure_years, 0)                                   AS old_tenure_years,
				COALESCE(TO_CHAR(a.old_value_date, 'YYYY-MM-DD'), '')             AS old_value_date,
				COALESCE(TO_CHAR(a.old_expected_maturity_date, 'YYYY-MM-DD'), '') AS old_maturity_date,
				COALESCE(TO_CHAR(a.old_expected_start_date, 'YYYY-MM-DD'), '')    AS old_expected_start_date,
				COALESCE(a.old_booking_status, '')                                AS old_booking_status,
				COALESCE(a.old_source_account_id, '')                             AS old_bank_account_id,
				COALESCE(a.old_frequency_id, '')                                  AS old_frequency_id,
				COALESCE(a.old_day_count_code, '')                                AS old_day_count_code,
				COALESCE(a.old_tds_plan_id, '')                                   AS old_tds_plan_id,
				COALESCE(a.old_auto_renewal, false)                               AS old_auto_renewal,
				COALESCE(a.old_booking_remarks, '')                               AS old_booking_remarks
			FROM investment.fd_audit_booking_request a
			LEFT JOIN investment.fd_booking_request m ON m.booking_id = a.booking_id
			WHERE a.booking_id = $1
			ORDER BY a.requested_at DESC
		`, req.BookingID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}

		payload, err := collectPgxRows(rows)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to read booking audit history")
			return
		}
		uploadRows, err := pgxPool.Query(ctx, `
			SELECT
				('file-' || a.audit_id::text) AS audit_id,
				a.parent_record_id AS booking_id,
				a.file_id,
				'UPLOAD_FILE' AS action_type,
				COALESCE(a.processing_status, '') AS processing_status,
				COALESCE(a.requested_by, '') AS requested_by,
				COALESCE(TO_CHAR(a.requested_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'), '') AS requested_at,
				COALESCE(a.checker_by, '') AS checker_by,
				COALESCE(TO_CHAR(a.checker_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'), '') AS checker_at,
				COALESCE(a.checker_comment, '') AS checker_comment,
				COALESCE(a.reason, '') AS reason
			FROM investment.additional_file_audit a
			JOIN investment.fd_booking_request_files f ON f.file_id = a.file_id AND f.booking_id::text = a.parent_record_id
			WHERE a.module_key = 'fd-booking-additional'
			  AND a.parent_record_id = $1
			  AND a.action_type = 'CREATE'
			ORDER BY a.requested_at DESC`, req.BookingID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		uploadPayload, err := collectPgxRows(uploadRows)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to read booking upload audit history")
			return
		}
		payload = append(payload, uploadPayload...)

		respondFDBookingAuditPayload(w, payload)
	}
}

// ─── GetConfirmationAuditHandler ─────────────────────────────────────────────
// POST /investment/fd/confirmation/audit
// Body: { user_id, confirmation_id }
// Response: { success: true, data: [...] }

func GetConfirmationAuditHandler(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}

		var req confirmationAuditRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil ||
			strings.TrimSpace(req.ConfirmationID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "confirmation_id is required")
			return
		}

		ctx := r.Context()
		if code, msg := validateConfirmationAuditAccess(ctx, pgxPool, req.ConfirmationID); code != 0 {
			api.RespondWithError(w, code, msg)
			return
		}

		rows, err := pgxPool.Query(ctx, `
			SELECT
				a.audit_id::text,
				a.confirmation_id,
				a.action_type,
				a.processing_status,
				COALESCE(a.requested_by, '')                                               AS requested_by,
				COALESCE(TO_CHAR(a.requested_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'), '')             AS requested_at,
				COALESCE(a.checker_by, '')                                                 AS checker_by,
				COALESCE(TO_CHAR(a.checker_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'), '')               AS checker_at,
				COALESCE(a.checker_comment, '')                                            AS checker_comment,
				COALESCE(a.reason, '')                                                     AS reason,
				COALESCE(b.entity_id, '')                                                  AS entity_id,
				COALESCE(c.confirmation_status, '')                                        AS confirmation_status,
				COALESCE(c.actual_principal, 0)                                            AS confirmed_principal_amount,
				COALESCE(c.confirmed_rate, 0)                                              AS confirmed_interest_rate,
				COALESCE(c.variance_flag, false)                                           AS has_variance,
				COALESCE(c.variance_action, '')                                            AS variance_action,
				COALESCE(a.old_confirmation_status, '')                                    AS old_confirmation_status,
				COALESCE(a.old_actual_principal, 0)                                        AS old_confirmed_principal_amount,
				COALESCE(a.old_confirmed_rate, 0)                                          AS old_confirmed_interest_rate,
				COALESCE(TO_CHAR(a.old_actual_start_date, 'YYYY-MM-DD'), '')                AS old_confirmed_value_date,
				COALESCE(TO_CHAR(a.old_actual_maturity_date, 'YYYY-MM-DD'), '')             AS old_confirmed_maturity_date,
				COALESCE(a.old_variance_flag, false)                                       AS old_has_variance,
				COALESCE(a.old_variance_action, '')                                        AS old_variance_action,
				COALESCE(a.old_bank_fd_ref_no, '')                                         AS old_bank_fd_reference
			FROM investment.fd_audit_confirmation a
			LEFT JOIN investment.fd_confirmation c ON c.confirmation_id = a.confirmation_id
			LEFT JOIN investment.fd_booking_request b ON b.booking_id = c.booking_id
			WHERE a.confirmation_id = $1
			ORDER BY a.requested_at DESC
		`, req.ConfirmationID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}

		payload, err := collectPgxRows(rows)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to read confirmation audit history")
			return
		}
		uploadRows, err := pgxPool.Query(ctx, `
			SELECT
				('file-' || a.audit_id::text) AS audit_id,
				a.parent_record_id AS confirmation_id,
				a.file_id,
				'UPLOAD_FILE' AS action_type,
				COALESCE(a.processing_status, '') AS processing_status,
				COALESCE(a.requested_by, '') AS requested_by,
				COALESCE(TO_CHAR(a.requested_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'), '') AS requested_at,
				COALESCE(a.checker_by, '') AS checker_by,
				COALESCE(TO_CHAR(a.checker_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'), '') AS checker_at,
				COALESCE(a.checker_comment, '') AS checker_comment,
				COALESCE(a.reason, '') AS reason
			FROM investment.additional_file_audit a
			JOIN investment.fd_confirmation_files f ON f.file_id = a.file_id AND f.confirmation_id::text = a.parent_record_id
			WHERE a.module_key = 'fd-confirmation-additional'
			  AND a.parent_record_id = $1
			  AND a.action_type = 'CREATE'
			ORDER BY a.requested_at DESC`, req.ConfirmationID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		uploadPayload, err := collectPgxRows(uploadRows)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to read confirmation upload audit history")
			return
		}
		payload = append(payload, uploadPayload...)

		respondFDBookingAuditPayload(w, payload)
	}
}

// ─── Access validators ────────────────────────────────────────────────────────

func validateBookingAuditAccess(ctx context.Context, pool *pgxpool.Pool, bookingID string) (int, string) {
	var exists bool
	if err := pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM investment.fd_booking_request WHERE booking_id = $1)`,
		bookingID,
	).Scan(&exists); err != nil || !exists {
		return http.StatusNotFound, "booking not found"
	}
	return 0, ""
}

func validateConfirmationAuditAccess(ctx context.Context, pool *pgxpool.Pool, confirmationID string) (int, string) {
	var exists bool
	if err := pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM investment.fd_confirmation WHERE confirmation_id = $1)`,
		confirmationID,
	).Scan(&exists); err != nil || !exists {
		return http.StatusNotFound, "confirmation not found"
	}
	return 0, ""
}

// ─── Shared helpers ───────────────────────────────────────────────────────────

// collectPgxRows converts pgx rows into a generic slice of maps, setting nil
// values to empty string to keep JSON responses consistent.
func collectPgxRows(rows pgx.Rows) ([]map[string]interface{}, error) {
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
				row[f.Name] = ""
			} else {
				row[f.Name] = vals[i]
			}
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func respondFDBookingAuditPayload(w http.ResponseWriter, payload interface{}) {
	w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
	json.NewEncoder(w).Encode(map[string]interface{}{ //nolint:errcheck
		"success": true,
		"data":    payload,
	})
}
