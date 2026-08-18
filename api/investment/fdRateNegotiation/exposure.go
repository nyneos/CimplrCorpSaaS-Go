package fdRateNegotiation

import (
	"encoding/json"
	"net/http"
	"strings"

	"CimplrCorpSaas/api"

	"github.com/jackc/pgx/v5/pgxpool"
)

// BankExposure returns the sum of activated FD principal for a bank (optionally entity).
func BankExposure(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			BankID   string `json:"bank_id"`
			BankName string `json:"bank_name"`
			EntityID string `json:"entity_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}
		bankID := strings.TrimSpace(req.BankID)
		bankName := strings.TrimSpace(req.BankName)
		entityID := strings.TrimSpace(req.EntityID)
		if bankID == "" && bankName == "" {
			api.RespondWithError(w, http.StatusBadRequest, "bank_id or bank_name is required")
			return
		}

		var total float64
		err := pgxPool.QueryRow(r.Context(), `
			SELECT COALESCE(SUM(principal_amount), 0)
			FROM investment.fd_master
			WHERE COALESCE(is_deleted, false) = false
			  AND UPPER(COALESCE(fd_status, '')) IN ('ACTIVE','ACTIVATED','LIVE','OPEN')
			  AND (
			        ($1 <> '' AND bank_id::text = $1)
			     OR ($2 <> '' AND LOWER(COALESCE(bank_name,'')) = LOWER($2))
			  )
			  AND ($3 = '' OR entity_id::text = $3)`,
			bankID, bankName, entityID,
		).Scan(&total)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to load bank exposure")
			return
		}
		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"bank_id":             bankID,
			"bank_name":           bankName,
			"entity_id":           entityID,
			"activated_principal": total,
		})
	}
}
