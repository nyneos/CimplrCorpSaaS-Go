package exposures

import (
	"encoding/json"
	"net/http"
	"strings"

	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/internal/ctxutil"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Handler for settlement

// FilterForwardBookingsForSettlement handles the filtering of forward bookings for settlement
func FilterForwardBookingsForSettlement(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID            string   `json:"user_id"`
			ExposureHeaderIDs []string `json:"exposure_header_ids"`
			Entity            string   `json:"entity"`
			Currency          string   `json:"currency"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.ExposureHeaderIDs) == 0 || req.Entity == "" || req.Currency == "" || req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, "user_id, exposure_header_ids (array), entity, and currency are required")
			return
		}
		scope := ctxutil.FromContext(ctx)
		buNames := scope.EntityNames
		if len(buNames) == 0 {
			respondWithError(w, http.StatusNotFound, constants.ErrNoAccessibleBusinessUnit)
			return
		}
		// Same source rules as dash/hedgingProposal/fwdDashHedge.go: approved
		// forwards, outstanding = latest ledger running_open_amount (fallback
		// booking_amount). Status/currency compared case-insensitively and the
		// exposure currency may sit on either leg of the forward.
		query := `
			SELECT DISTINCT
				fb.internal_reference_id AS "Forward Ref",
				COALESCE(fbl.running_open_amount, fb.booking_amount) AS "Outstanding Amount",
				fb.spot_rate AS "Spot",
				fb.total_rate AS "Fwd",
				fb.bank_margin AS "Margin",
				COALESCE(NULLIF(fb.counterparty_dealer,''), fb.counterparty) AS "Bank Name",
				fb.maturity_date AS "Maturity",
				fb.system_transaction_id AS "Booking ID",
				fb.base_currency AS "Base Currency",
				fb.quote_currency AS "Quote Currency",
				fb.order_type AS "Order Type",
				COALESCE(ehl.hedged_amount, 0) AS "Hedged Amount"
			FROM exposure_hedge_links ehl
			JOIN forward_bookings fb ON ehl.booking_id = fb.system_transaction_id
			LEFT JOIN LATERAL (
				SELECT l.running_open_amount
				FROM forward_booking_ledger l
				WHERE l.booking_id = fb.system_transaction_id
				ORDER BY l.ledger_sequence DESC
				LIMIT 1
			) fbl ON TRUE
			WHERE ehl.exposure_header_id = ANY($1)
				AND COALESCE(fb.is_deleted, false) = false
				AND LOWER(COALESCE(fb.processing_status,'')) = 'approved'
				AND UPPER(COALESCE(fb.status,'')) NOT IN ('CANCELLED','CANCELED','MATURED','CLOSED')
				AND (UPPER(fb.base_currency) = UPPER($2) OR UPPER(fb.quote_currency) = UPPER($2))
				AND (
					fb.entity_level_0 = ANY($3)
					OR fb.entity_level_1 = ANY($3)
					OR fb.entity_level_2 = ANY($3)
					OR fb.entity_level_3 = ANY($3)
				)
			ORDER BY fb.maturity_date, fb.internal_reference_id
		`
		rows, err := pool.Query(ctx, query, req.ExposureHeaderIDs, strings.TrimSpace(req.Currency), buNames)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "Failed to fetch forward bookings for settlement")
			return
		}
		defer rows.Close()
		cols := pgxColumnNames(rows)
		data := []map[string]interface{}{}
		for rows.Next() {
			vals := make([]interface{}, len(cols))
			valPtrs := make([]interface{}, len(cols))
			for i := range vals {
				valPtrs[i] = &vals[i]
			}
			if err := rows.Scan(valPtrs...); err != nil {
				continue
			}
			rowMap := map[string]interface{}{}
			for i, col := range cols {
				rowMap[col] = parseDBValue(col, vals[i])
			}
			data = append(data, rowMap)
		}
		respondWithSuccess(w, http.StatusOK, "Forward bookings for settlement fetched successfully", data)
	}
}

// GetApprovedExposuresForSettlement returns exposure header + line item rows
// that are fully approved (approval_status) and accessible to the session BUs.
// Used by Settlement Exposure Selection — not the general headers-line-items list.
func GetApprovedExposuresForSettlement(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrPleaseLogin)
			return
		}

		ctx := r.Context()
		scope := ctxutil.FromContext(ctx)
		buNames := scope.EntityNames
		if len(buNames) == 0 {
			respondWithError(w, http.StatusNotFound, constants.ErrNoAccessibleBusinessUnit)
			return
		}

		query := `
			SELECT
				h.exposure_header_id,
				h.document_id,
				h.exposure_type,
				h.entity,
				h.entity1,
				h.entity2,
				h.entity3,
				h.company_code,
				h.counterparty_type,
				h.counterparty_code,
				h.counterparty_name,
				h.exposure_category,
				h.currency,
				h.document_date,
				h.value_date,
				h.status,
				h.is_active,
				h.approval_status,
				h.total_original_amount,
				h.total_open_amount,
				h.amount_in_local_currency,
				h.gl_account,
				h.upload_s3_key,
				h.created_at,
				h.updated_at,
				l.line_item_id,
				l.line_number,
				l.product_id,
				l.product_description,
				l.quantity,
				l.unit_of_measure,
				l.unit_price,
				l.line_item_amount,
				l.plant_code,
				l.delivery_date,
				l.payment_terms,
				l.inco_terms
			FROM exposure_headers h
			LEFT JOIN exposure_line_items l ON l.exposure_header_id = h.exposure_header_id
			WHERE (
					COALESCE(h.entity, '') = ANY($1)
					OR COALESCE(h.entity1, '') = ANY($1)
					OR COALESCE(h.entity2, '') = ANY($1)
					OR COALESCE(h.entity3, '') = ANY($1)
				)
			  AND COALESCE(h.is_deleted, false) = false
			  AND (
					UPPER(REPLACE(COALESCE(h.approval_status, ''), '-', '_')) = 'APPROVED'
					OR UPPER(TRIM(COALESCE(h.approval_status, ''))) = 'APPROVED'
				)
			  AND (
					COALESCE(h.exposure_creation_status, '') = ''
					OR UPPER(TRIM(COALESCE(h.exposure_creation_status, ''))) = 'APPROVED'
				)
			  AND ABS(COALESCE(h.total_open_amount, 0)) > 0
			  AND UPPER(TRIM(COALESCE(h.status, ''))) NOT IN ('CLOSED', 'CANCELLED', 'CANCELED', 'SETTLED', 'PAID')
			ORDER BY h.document_id, l.line_number NULLS FIRST
		`

		joinRows, err := pool.Query(ctx, query, buNames)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "Failed to fetch approved exposures for settlement")
			return
		}
		defer joinRows.Close()

		joinData, err := scanHeadersLineItemsRows(joinRows)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "Failed to scan approved exposures for settlement")
			return
		}

		respondWithSuccess(w, http.StatusOK, "Approved exposures for settlement fetched successfully", map[string]interface{}{
			"buAccessible": buNames,
			"pageData":     joinData,
		})
	}
}

// Handler: GetForwardBookingsByEntityAndCurrency
func GetForwardBookingsByEntityAndCurrency(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID            string   `json:"user_id"`
			Entity            string   `json:"entity"`
			Currency          string   `json:"currency"`
			ExposureHeaderIDs []string `json:"exposure_header_ids"` // optional: forwards already linked to these are excluded
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Entity == "" || req.Currency == "" || req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, "user_id, entity, and currency are required")
			return
		}
		scope := ctxutil.FromContext(ctx)
		buNames := scope.EntityNames
		if len(buNames) == 0 {
			respondWithError(w, http.StatusNotFound, constants.ErrNoAccessibleBusinessUnit)
			return
		}
		// Additional (unlinked) forwards: same forward_bookings rules as the
		// hedging-proposal dashboard, restricted to the exposure's entity (any
		// level) and currency (either leg), with a non-zero open amount, and
		// excluding forwards already linked to the selected exposures.
		query := `
			SELECT
				fb.internal_reference_id AS "Forward Ref",
				COALESCE(fbl.running_open_amount, fb.booking_amount) AS "Outstanding Amount",
				fb.spot_rate AS "Spot",
				fb.total_rate AS "Fwd",
				fb.bank_margin AS "Margin",
				COALESCE(NULLIF(fb.counterparty_dealer,''), fb.counterparty) AS "Bank Name",
				fb.maturity_date AS "Maturity",
				fb.system_transaction_id AS "Booking ID",
				fb.base_currency AS "Base Currency",
				fb.quote_currency AS "Quote Currency",
				fb.order_type AS "Order Type"
			FROM forward_bookings fb
			LEFT JOIN LATERAL (
				SELECT l.running_open_amount
				FROM forward_booking_ledger l
				WHERE l.booking_id = fb.system_transaction_id
				ORDER BY l.ledger_sequence DESC
				LIMIT 1
			) fbl ON TRUE
			WHERE COALESCE(fb.is_deleted, false) = false
				AND LOWER(COALESCE(fb.processing_status,'')) = 'approved'
				AND UPPER(COALESCE(fb.status,'')) NOT IN ('CANCELLED','CANCELED','MATURED','CLOSED')
				AND (UPPER(fb.base_currency) = UPPER($1) OR UPPER(fb.quote_currency) = UPPER($1))
				AND (
					UPPER(TRIM(COALESCE(fb.entity_level_0,''))) = UPPER($2)
					OR UPPER(TRIM(COALESCE(fb.entity_level_1,''))) = UPPER($2)
					OR UPPER(TRIM(COALESCE(fb.entity_level_2,''))) = UPPER($2)
					OR UPPER(TRIM(COALESCE(fb.entity_level_3,''))) = UPPER($2)
				)
				AND (
					fb.entity_level_0 = ANY($3)
					OR fb.entity_level_1 = ANY($3)
					OR fb.entity_level_2 = ANY($3)
					OR fb.entity_level_3 = ANY($3)
				)
				AND ABS(COALESCE(fbl.running_open_amount, fb.booking_amount, 0)) > 0
				AND NOT EXISTS (
					SELECT 1 FROM exposure_hedge_links x
					WHERE x.booking_id = fb.system_transaction_id
					  AND x.exposure_header_id = ANY($4)
				)
			ORDER BY fb.maturity_date, fb.internal_reference_id
		`
		if req.ExposureHeaderIDs == nil {
			req.ExposureHeaderIDs = []string{}
		}
		rows, err := pool.Query(ctx, query, strings.TrimSpace(req.Currency), strings.TrimSpace(req.Entity), buNames, req.ExposureHeaderIDs)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "Failed to fetch forward bookings")
			return
		}
		defer rows.Close()
		cols := pgxColumnNames(rows)
		data := []map[string]interface{}{}
		for rows.Next() {
			vals := make([]interface{}, len(cols))
			valPtrs := make([]interface{}, len(cols))
			for i := range vals {
				valPtrs[i] = &vals[i]
			}
			if err := rows.Scan(valPtrs...); err != nil {
				continue
			}
			rowMap := map[string]interface{}{}
			for i, col := range cols {
				rowMap[col] = parseDBValue(col, vals[i])
			}
			data = append(data, rowMap)
		}
		respondWithSuccess(w, http.StatusOK, "Forward bookings for settlement fetched successfully", data)
	}
}
