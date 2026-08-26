package fdRateNegotiation

import (
	"CimplrCorpSaas/api"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type approvedOfferLookupPayload struct {
	EntityID string `json:"entity_id"`
	BankID   string `json:"bank_id,omitempty"`
	BankName string `json:"bank_name"`
}

// ListApprovedOffersByEntityBank returns the selected APPROVED offer for rate
// requests past selection (PENDING_RATE_APPROVAL / APPROVED / CONVERTED_TO_FD),
// scoped to the given entity and bank, for prefilling a manual FD booking form.
// Entity match is required; bank match is by bank_id when provided, otherwise a
// case-insensitive bank_name match. Non-selected competing offers are excluded.
func ListApprovedOffersByEntityBank(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req approvedOfferLookupPayload
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil && err != io.EOF {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "Invalid JSON", "")
			return
		}
		entityID := strings.TrimSpace(req.EntityID)
		bankID := strings.TrimSpace(req.BankID)
		bankName := strings.TrimSpace(req.BankName)
		if entityID == "" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "entity_id is required", "")
			return
		}
		if bankID == "" && bankName == "" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "bank_id or bank_name is required", "")
			return
		}

		ctx := r.Context()
		var (
			rows pgx.Rows
			err  error
		)
		// Only the selected offer on requests past selection (pending rate
		// approval, approved-with-selection, or already converted to FD).
		const bookingReadyFilter = `
				WHERE COALESCE(n.is_deleted,false)=false
				  AND COALESCE(o.is_deleted,false)=false
				  AND o.offer_status = 'APPROVED'
				  AND n.selected_offer_id IS NOT NULL
				  AND n.selected_offer_id = o.offer_id
				  AND upper(n.request_status) IN ('PENDING_RATE_APPROVAL','APPROVED','CONVERTED_TO_FD')
				  AND n.entity_id = $1`
		if bankID != "" {
			rows, err = pgxPool.Query(ctx, approvedOfferLookupSelect+bookingReadyFilter+`
				  AND o.bank_id = $2
				ORDER BY n.created_at DESC, o.created_at DESC`, entityID, bankID)
		} else {
			rows, err = pgxPool.Query(ctx, approvedOfferLookupSelect+bookingReadyFilter+`
				  AND lower(o.bank_name) = lower($2)
				ORDER BY n.created_at DESC, o.created_at DESC`, entityID, bankName)
		}
		if err != nil {
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "Failed to list approved offers", "")
			return
		}
		defer rows.Close()

		results := make([]map[string]interface{}, 0)
		for rows.Next() {
			var (
				rateRequestID, rateRequestRef, entityIDVal, entityName string
				proposedFdAmount                                       float64
				currencyCode, tenureType                               string
				tenureValue                                            int
				expectedStartDate, expectedMaturityDate                string
				interestType, interestPayoutMode                       string
				offerID, offerReferenceID, offerBankID, offerBankName  string
				offeredInterestRate                                    float64
				effectiveYield                                         *float64
				applicableTenure, validTillDate                        string
			)
			if err := rows.Scan(
				&rateRequestID, &rateRequestRef, &entityIDVal, &entityName,
				&proposedFdAmount, &currencyCode, &tenureType, &tenureValue,
				&expectedStartDate, &expectedMaturityDate,
				&interestType, &interestPayoutMode,
				&offerID, &offerReferenceID, &offerBankID, &offerBankName,
				&offeredInterestRate, &effectiveYield, &applicableTenure, &validTillDate,
			); err != nil {
				continue
			}
			var yield interface{}
			if effectiveYield != nil {
				yield = *effectiveYield
			}
			results = append(results, map[string]interface{}{
				"rate_request_id":        rateRequestID,
				"rate_request_ref":       rateRequestRef,
				"entity_id":              entityIDVal,
				"entity_name":            entityName,
				"proposed_fd_amount":     proposedFdAmount,
				"currency_code":          currencyCode,
				"tenure_type":            tenureType,
				"tenure_value":           tenureValue,
				"expected_start_date":    expectedStartDate,
				"expected_maturity_date": expectedMaturityDate,
				"interest_type":          interestType,
				"interest_payout_mode":   interestPayoutMode,
				"offer_id":               offerID,
				"offer_reference_id":     offerReferenceID,
				"bank_id":                offerBankID,
				"bank_name":              offerBankName,
				"offered_interest_rate":  offeredInterestRate,
				"effective_yield":        yield,
				"applicable_tenure":      applicableTenure,
				"valid_till_date":        validTillDate,
			})
		}
		if err := rows.Err(); err != nil {
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "Row iteration error: "+err.Error(), "")
			return
		}

		api.RespondEnvelopeSuccess(w, "Success", map[string]interface{}{"rows": results})
	}
}

const approvedOfferLookupSelect = `
	SELECT
		n.rate_request_id::text,
		n.rate_request_ref,
		COALESCE(n.entity_id,''),
		COALESCE(n.entity_name,''),
		n.proposed_fd_amount,
		n.currency_code,
		n.tenure_type,
		n.tenure_value,
		TO_CHAR(n.expected_start_date,'YYYY-MM-DD'),
		COALESCE(TO_CHAR(n.expected_maturity_date,'YYYY-MM-DD'),''),
		n.interest_type,
		COALESCE(n.interest_payout_mode,''),
		o.offer_id::text,
		o.offer_reference_id,
		COALESCE(o.bank_id,''),
		o.bank_name,
		o.offered_interest_rate,
		o.effective_yield,
		COALESCE(o.applicable_tenure,''),
		TO_CHAR(o.valid_till_date,'YYYY-MM-DD')
	FROM investment.fd_rate_negotiation n
	JOIN investment.fd_rate_offer o ON o.rate_request_id = n.rate_request_id
`
