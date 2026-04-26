package bankstatement

import (
	api "CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

type bankStatementAuditRequest struct {
	UserID          string `json:"user_id"`
	BankStatementID string `json:"bank_statement_id"`
}

type bankStatementDownloadAuditRequest struct {
	UserID          string `json:"user_id"`
	BankStatementID string `json:"bank_statement_id"`
}

func GetBankStatementAuditHandler(db *sql.DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}

		var body bankStatementAuditRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || strings.TrimSpace(body.UserID) == "" || strings.TrimSpace(body.BankStatementID) == "" {
			http.Error(w, "Missing user_id or bank_statement_id", http.StatusBadRequest)
			return
		}

		ctx := r.Context()
		if code, msg := validateBankStatementAuditAccess(ctx, db, body.BankStatementID); code != 0 {
			api.RespondWithError(w, code, msg)
			return
		}

		rows, err := db.QueryContext(ctx, `
			SELECT
				action_id,
				bankstatementid,
				actiontype,
				processing_status,
				requested_by,
				requested_at,
				checker_by,
				checker_at,
				checker_comment,
				reason
			FROM cimplrcorpsaas.auditactionbankstatement
			WHERE bankstatementid = $1
			ORDER BY requested_at ASC, action_id ASC
		`, body.BankStatementID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, pqUserFriendlyMessage(err))
			return
		}
		defer rows.Close()

		payload := make([]map[string]interface{}, 0)
		for rows.Next() {
			var auditID, entityID string
			var action, status, performedBy, checkerBy, comment, reason sql.NullString
			var performedAt, checkerAt sql.NullTime
			if err := rows.Scan(&auditID, &entityID, &action, &status, &performedBy, &performedAt, &checkerBy, &checkerAt, &comment, &reason); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "failed to read audit history")
				return
			}

			payload = append(payload, map[string]interface{}{
				"audit_id":     auditID,
				"entity_id":    entityID,
				"action":       action.String,
				"status":       status.String,
				"performed_by": performedBy.String,
				"performed_at": nullableTime(performedAt),
				"checker_by":   checkerBy.String,
				"checker_at":   nullableTime(checkerAt),
				"comment":      comment.String,
				"reason":       reason.String,
			})
		}
		if err := rows.Err(); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to read audit history")
			return
		}

		respondAuditPayload(w, payload)
	})
}

func GetBankStatementDownloadAuditHandler(db *sql.DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}

		var body bankStatementDownloadAuditRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || strings.TrimSpace(body.UserID) == "" || strings.TrimSpace(body.BankStatementID) == "" {
			http.Error(w, "Missing user_id or bank_statement_id", http.StatusBadRequest)
			return
		}

		ctx := r.Context()
		if code, msg := validateBankStatementAuditAccess(ctx, db, body.BankStatementID); code != 0 {
			api.RespondWithError(w, code, msg)
			return
		}

		rows, err := db.QueryContext(ctx, `
			SELECT
				file_id,
				bankstatementid,
				user_id,
				NOW() AS performed_at,
				ip,
				entity_name
			FROM cimplrcorpsaas.bank_pdf_download_audits
			WHERE bankstatementid = $1
		`, body.BankStatementID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, pqUserFriendlyMessage(err))
			return
		}
		defer rows.Close()

		payload := make([]map[string]interface{}, 0)
		for rows.Next() {
			var fileID, bankStatementID, performedBy, ip, entityName sql.NullString
			var performedAt time.Time
			if err := rows.Scan(&fileID, &bankStatementID, &performedBy, &performedAt, &ip, &entityName); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "failed to read download audit history")
				return
			}

			payload = append(payload, map[string]interface{}{
				"file_id":           fileID.String,
				"bank_statement_id": bankStatementID.String,
				"performed_by":      performedBy.String,
				"performed_at":      performedAt,
				"ip":                ip.String,
				"entity_name":       entityName.String,
			})
		}
		if err := rows.Err(); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to read download audit history")
			return
		}

		respondAuditPayload(w, payload)
	})
}

func GetBankStatementBalanceImpactAuditHandler(db *sql.DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}

		var body bankStatementAuditRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || strings.TrimSpace(body.UserID) == "" || strings.TrimSpace(body.BankStatementID) == "" {
			http.Error(w, "Missing user_id or bank_statement_id", http.StatusBadRequest)
			return
		}

		ctx := r.Context()
		if code, msg := validateBankStatementAuditAccess(ctx, db, body.BankStatementID); code != 0 {
			api.RespondWithError(w, code, msg)
			return
		}

		rows, err := db.QueryContext(ctx, `
			SELECT
				balance_id,
				actiontype,
				processing_status,
				requested_by,
				requested_at
			FROM public.auditactionbankbalances
			WHERE balance_id = $1
			ORDER BY requested_at ASC, action_id ASC
		`, body.BankStatementID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, pqUserFriendlyMessage(err))
			return
		}
		defer rows.Close()

		payload := make([]map[string]interface{}, 0)
		for rows.Next() {
			var balanceID string
			var action, status, performedBy sql.NullString
			var performedAt sql.NullTime
			if err := rows.Scan(&balanceID, &action, &status, &performedBy, &performedAt); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "failed to read balance impact audit")
				return
			}

			payload = append(payload, map[string]interface{}{
				"balance_id":        balanceID,
				"action":            action.String,
				"status":            status.String,
				"performed_by":      performedBy.String,
				"performed_at":      nullableTime(performedAt),
				"bank_statement_id": body.BankStatementID,
			})
		}
		if err := rows.Err(); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to read balance impact audit")
			return
		}

		respondAuditPayload(w, payload)
	})
}

func GetBankStatementTransactionAuditHandler(db *sql.DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}

		var body bankStatementAuditRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || strings.TrimSpace(body.UserID) == "" || strings.TrimSpace(body.BankStatementID) == "" {
			http.Error(w, "Missing user_id or bank_statement_id", http.StatusBadRequest)
			return
		}

		ctx := r.Context()
		if code, msg := validateBankStatementAuditAccess(ctx, db, body.BankStatementID); code != 0 {
			api.RespondWithError(w, code, msg)
			return
		}

		rows, err := db.QueryContext(ctx, `
			SELECT
				transaction_id,
				field_name,
				old_value,
				new_value,
				performed_by,
				performed_at
			FROM cimplrcorpsaas.auditactionbankstatementtxn
			WHERE bankstatementid = $1
			ORDER BY performed_at DESC, transaction_id ASC
		`, body.BankStatementID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, pqUserFriendlyMessage(err))
			return
		}
		defer rows.Close()

		payload := make([]map[string]interface{}, 0)
		for rows.Next() {
			var transactionID, fieldName, oldValue, newValue, performedBy sql.NullString
			var performedAt sql.NullTime
			if err := rows.Scan(&transactionID, &fieldName, &oldValue, &newValue, &performedBy, &performedAt); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "failed to read transaction audit history")
				return
			}

			payload = append(payload, map[string]interface{}{
				"transaction_id": transactionID.String,
				"field_name":     fieldName.String,
				"old_value":      oldValue.String,
				"new_value":      newValue.String,
				"performed_by":   performedBy.String,
				"performed_at":   nullableTime(performedAt),
			})
		}
		if err := rows.Err(); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to read transaction audit history")
			return
		}

		respondAuditPayload(w, payload)
	})
}

func validateBankStatementAuditAccess(ctx context.Context, db *sql.DB, bankStatementID string) (int, string) {
	var entityID string
	if err := db.QueryRowContext(ctx, `SELECT entity_id FROM cimplrcorpsaas.bank_statements WHERE bank_statement_id = $1`, bankStatementID).Scan(&entityID); err != nil {
		if err == sql.ErrNoRows {
			return http.StatusNotFound, "bank statement not found"
		}
		return http.StatusInternalServerError, pqUserFriendlyMessage(err)
	}
	if ids := api.GetEntityIDsFromCtx(ctx); len(ids) > 0 && !api.IsEntityAllowed(ctx, entityID) {
		return http.StatusForbidden, constants.ErrNoAccessToBankStatement
	}
	return 0, ""
}

func validateEntityAuditAccess(ctx context.Context, db *sql.DB, entityName string) (int, string) {
	entityIDs := api.GetEntityIDsFromCtx(ctx)
	if len(entityIDs) == 0 {
		return http.StatusUnauthorized, "No accessible business units found"
	}

	var entityID string
	if err := db.QueryRowContext(ctx, `
		SELECT entity_id
		FROM public.masterentitycash
		WHERE entity_name = $1
		LIMIT 1
	`, entityName).Scan(&entityID); err != nil {
		if err == sql.ErrNoRows {
			return http.StatusNotFound, "entity not found"
		}
		return http.StatusInternalServerError, pqUserFriendlyMessage(err)
	}
	if !api.IsEntityAllowed(ctx, entityID) {
		return http.StatusForbidden, constants.ErrNoAccessToBankStatement
	}
	return 0, ""
}

func nullableTime(value sql.NullTime) interface{} {
	if value.Valid {
		return value.Time
	}
	return nil
}

func respondAuditPayload(w http.ResponseWriter, payload interface{}) {
	w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"data":    payload,
	})
}
