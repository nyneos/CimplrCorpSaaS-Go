package payablerecievable

import (
	api "CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/constants"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type transactionAuditRequest struct {
	UserID          string `json:"user_id"`
	TransactionType string `json:"transaction_type"`
	TransactionID   string `json:"transaction_id"`
}

func GetTransactionAuditHandler(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}

		var req transactionAuditRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.UserID) == "" || strings.TrimSpace(req.TransactionID) == "" || strings.TrimSpace(req.TransactionType) == "" {
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "message": "user_id, transaction_type and transaction_id are required"})
			return
		}

		valid := false
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				valid = true
				break
			}
		}
		if !valid {
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "message": constants.ErrInvalidSession})
			return
		}

		txType := strings.ToUpper(strings.TrimSpace(req.TransactionType))
		tableName := ""
		idColumn := ""
		switch txType {
		case "PAYABLE":
			tableName = "auditactionpayable"
			idColumn = "payable_id"
		case "RECEIVABLE":
			tableName = "auditactionreceivable"
			idColumn = "receivable_id"
		default:
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "message": "transaction_type must be PAYABLE or RECEIVABLE"})
			return
		}

		ctx := r.Context()
		rows, err := pgxPool.Query(ctx, `
			SELECT
				`+idColumn+`,
				actiontype,
				processing_status,
				requested_by,
				requested_at,
				checker_by,
				checker_at,
				checker_comment,
				reason
			FROM `+tableName+`
			WHERE `+idColumn+` = $1
			ORDER BY requested_at ASC, action_id ASC
		`, req.TransactionID)
		if err != nil {
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "message": err.Error()})
			return
		}
		defer rows.Close()

		payload := make([]map[string]interface{}, 0)
		for rows.Next() {
			var entityID, action, status, performedBy string
			var performedAt time.Time
			var checkerBy, checkerComment, reason *string
			var checkerAt *time.Time
			if err := rows.Scan(&entityID, &action, &status, &performedBy, &performedAt, &checkerBy, &checkerAt, &checkerComment, &reason); err != nil {
				json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "message": "failed to read transaction audit history"})
				return
			}

			payload = append(payload, map[string]interface{}{
				"entity_id":    entityID,
				"action":       action,
				"status":       status,
				"performed_by": performedBy,
				"performed_at": performedAt,
				"checker_by":   stringPointerValue(checkerBy),
				"checker_at":   timePointerValue(checkerAt),
				"comment":      stringPointerValue(checkerComment),
				"reason":       stringPointerValue(reason),
			})
		}
		if err := rows.Err(); err != nil {
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "message": "failed to read transaction audit history"})
			return
		}

		api.RespondWithPayload(w, true, "", payload)
	}
}

func stringPointerValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func timePointerValue(value *time.Time) interface{} {
	if value == nil {
		return nil
	}
	return *value
}

func transactionRequestedBy(userID string) string {
	for _, s := range auth.GetActiveSessions() {
		if s.UserID == userID {
			if strings.TrimSpace(s.Name) != "" {
				return strings.TrimSpace(s.Name)
			}
			break
		}
	}
	return strings.TrimSpace(userID)
}

func insertTransactionDownloadAudit(ctx context.Context, pgxPool *pgxpool.Pool, transactionType, transactionID, requestedBy string) {
	if strings.TrimSpace(transactionID) == "" || strings.TrimSpace(requestedBy) == "" {
		return
	}

	var query string
	switch strings.ToUpper(strings.TrimSpace(transactionType)) {
	case "PAYABLE":
		query = `INSERT INTO auditactionpayable (payable_id, actiontype, processing_status, requested_by, requested_at) VALUES ($1, 'DOWNLOAD', 'COMPLETED', $2, now())`
	case "RECEIVABLE":
		query = `INSERT INTO auditactionreceivable (receivable_id, actiontype, processing_status, requested_by, requested_at) VALUES ($1, 'DOWNLOAD', 'COMPLETED', $2, now())`
	default:
		return
	}

	if _, err := pgxPool.Exec(ctx, query, transactionID, requestedBy); err != nil {
		log.Printf("failed to insert %s download audit for %s: %v", strings.ToLower(transactionType), transactionID, err)
	}
}
