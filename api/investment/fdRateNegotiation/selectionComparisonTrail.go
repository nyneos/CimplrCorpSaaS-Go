package fdRateNegotiation

import (
	"CimplrCorpSaas/api"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

type comparisonTrailRow struct {
	ComparisonID            string   `json:"comparison_id"`
	SelectionRateRequestID  string   `json:"selection_rate_request_id"`
	SelectedOfferID         string   `json:"selected_offer_id"`
	ComparedOfferID         string   `json:"compared_offer_id"`
	ComparedRateRequestID   string   `json:"compared_rate_request_id"`
	ComparedRateRequestRef  string   `json:"compared_rate_request_ref"`
	OfferReferenceID        string   `json:"offer_reference_id"`
	BankID                  *string  `json:"bank_id"`
	BankName                *string  `json:"bank_name"`
	OfferedInterestRate     *float64 `json:"offered_interest_rate"`
	EffectiveYield          *float64 `json:"effective_yield"`
	IsSelected              bool     `json:"is_selected"`
	CreatedBy               string   `json:"created_by"`
	CreatedAt               string   `json:"created_at"`
}

// ListSelectionComparisonTrail returns the offers a selection was compared against
// at SubmitSelection time, for the selected offer's own detail view and for
// FD Booking Integration once the selection is approved.
func ListSelectionComparisonTrail(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			RateRequestID string `json:"rate_request_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil && err != io.EOF {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "Invalid JSON", "")
			return
		}
		rateRequestID := strings.TrimSpace(req.RateRequestID)
		if rateRequestID == "" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "rate_request_id is required", "")
			return
		}

		ctx := r.Context()
		rows, err := pgxPool.Query(ctx, `
			SELECT
				c.comparison_id::text,
				c.selection_rate_request_id::text,
				c.selected_offer_id::text,
				c.compared_offer_id::text,
				c.compared_rate_request_id::text,
				COALESCE(rn.rate_request_ref,''),
				COALESCE(o.offer_reference_id,''),
				c.bank_id,
				c.bank_name,
				c.offered_interest_rate,
				c.effective_yield,
				c.is_selected,
				c.created_by,
				TO_CHAR(c.created_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"')
			FROM investment.fd_rate_selection_comparison c
			LEFT JOIN investment.fd_rate_negotiation rn ON rn.rate_request_id = c.compared_rate_request_id
			LEFT JOIN investment.fd_rate_offer o ON o.offer_id = c.compared_offer_id
			WHERE c.selection_rate_request_id = $1::uuid
			ORDER BY c.is_selected DESC, c.offered_interest_rate DESC NULLS LAST, c.created_at DESC`,
			rateRequestID,
		)
		if err != nil {
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "Failed to fetch comparison trail", "")
			return
		}
		defer rows.Close()

		results := make([]comparisonTrailRow, 0)
		for rows.Next() {
			var item comparisonTrailRow
			if err := rows.Scan(
				&item.ComparisonID, &item.SelectionRateRequestID, &item.SelectedOfferID,
				&item.ComparedOfferID, &item.ComparedRateRequestID, &item.ComparedRateRequestRef,
				&item.OfferReferenceID, &item.BankID, &item.BankName,
				&item.OfferedInterestRate, &item.EffectiveYield, &item.IsSelected,
				&item.CreatedBy, &item.CreatedAt,
			); err != nil {
				continue
			}
			results = append(results, item)
		}

		api.RespondEnvelopeSuccess(w, "", results)
	}
}
