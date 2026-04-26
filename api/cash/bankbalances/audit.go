package bankbalances

import (
	api "CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

type bankBalanceAuditRequest struct {
	UserID    string `json:"user_id"`
	BalanceID string `json:"balance_id"`
}

func GetBankBalanceAuditHandler(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}

		var req bankBalanceAuditRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.UserID) == "" || strings.TrimSpace(req.BalanceID) == "" {
			http.Error(w, "Missing user_id or balance_id", http.StatusBadRequest)
			return
		}

		ctx := r.Context()
		if code, msg := ensureBalanceIDsAccessible(ctx, pgxPool, []string{req.BalanceID}); code != 0 {
			api.RespondWithError(w, code, msg)
			return
		}

		rows, err := pgxPool.Query(ctx, `
			SELECT
				balance_id,
				actiontype,
				processing_status,
				requested_by,
				requested_at,
				checker_by,
				checker_at,
				checker_comment,
				reason
			FROM public.auditactionbankbalances
			WHERE balance_id = $1
			ORDER BY requested_at ASC, action_id ASC
		`, req.BalanceID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, pgUserFriendlyMessage(err))
			return
		}
		defer rows.Close()

		payload := make([]map[string]interface{}, 0)
		for rows.Next() {
			var entityID string
			var action, status, performedBy, checkerBy, comment, reason sql.NullString
			var performedAt, checkerAt sql.NullTime
			if err := rows.Scan(&entityID, &action, &status, &performedBy, &performedAt, &checkerBy, &checkerAt, &comment, &reason); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "failed to read bank balance audit history")
				return
			}

			payload = append(payload, map[string]interface{}{
				"entity_id":    entityID,
				"action":       auditString(action),
				"status":       auditString(status),
				"performed_by": auditString(performedBy),
				"performed_at": auditTime(performedAt),
				"checker_by":   auditString(checkerBy),
				"checker_at":   auditTime(checkerAt),
				"comment":      auditString(comment),
				"reason":       auditString(reason),
			})
		}
		if err := rows.Err(); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to read bank balance audit history")
			return
		}

		api.RespondWithPayload(w, true, "", payload)
	}
}

func auditString(value sql.NullString) string {
	if !value.Valid {
		return ""
	}
	return value.String
}

func auditTime(value sql.NullTime) interface{} {
	if !value.Valid {
		return nil
	}
	return value.Time
}

func insertBankBalanceDownloadAudit(ctx context.Context, pgxPool *pgxpool.Pool, balanceID, requestedBy string) {
	if strings.TrimSpace(balanceID) == "" || strings.TrimSpace(requestedBy) == "" {
		return
	}

	_, err := pgxPool.Exec(ctx, `
		INSERT INTO public.auditactionbankbalances (balance_id, actiontype, processing_status, requested_by, requested_at)
		VALUES ($1, 'DOWNLOAD', 'COMPLETED', $2, now())
	`, balanceID, requestedBy)
	if err != nil {
		log.Printf("failed to insert bank balance download audit for %s: %v", balanceID, err)
	}
}
