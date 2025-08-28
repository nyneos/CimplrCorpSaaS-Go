package forwards

import (
	"CimplrCorpSaas/api"
	"database/sql"
	"encoding/json"
	"math"
	"net/http"

	"github.com/lib/pq"
)

// Handler for forward cancel/roll

// Handler: GetForwardBookingList
func GetForwardBookingList(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, "user_id is required")
			return
		}
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusNotFound, "No accessible business units found")
			return
		}
		query := `
			SELECT 
				system_transaction_id,
				internal_reference_id,
				currency_pair,
				booking_amount,
				total_rate AS spot_rate,
				maturity_date,
				order_type,
				entity_level_0,
				counterparty
			FROM forward_bookings
			WHERE entity_level_0 = ANY($1)
				AND status NOT IN ('Cancelled', 'Pending Confirmation')
		`
		rows, err := db.Query(query, pq.Array(buNames))
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "Failed to fetch forward booking list")
			return
		}
		defer rows.Close()
		cols, _ := rows.Columns()
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
				rowMap[col] = vals[i]
			}
			data = append(data, rowMap)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "data": data})
	}
}

// Handler: GetExposuresByBookingIds
func GetExposuresByBookingIds(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID              string   `json:"user_id"`
			SystemTransactionIDs []string `json:"system_transaction_ids"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.SystemTransactionIDs) == 0 || req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, "user_id and system_transaction_ids (array) required")
			return
		}
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusNotFound, "No accessible business units found")
			return
		}
		query := `
			SELECT
				ehl.exposure_header_id,
				eh.document_id,
				eh.exposure_type,
				eh.currency,
				eh.total_open_amount,
				eh.total_original_amount,
				eh.document_date
			FROM exposure_hedge_links ehl
			JOIN exposure_headers eh ON ehl.exposure_header_id = eh.exposure_header_id
			WHERE ehl.booking_id = ANY($1)
				AND (ehl.is_active = true OR ehl.is_active IS NULL)
				AND eh.entity = ANY($2)
		`
		rows, err := db.Query(query, pq.Array(req.SystemTransactionIDs), pq.Array(buNames))
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "Failed to fetch exposures by booking ids")
			return
		}
		defer rows.Close()
		cols, _ := rows.Columns()
		data := []map[string]interface{}{}
		for rows.Next() {
			vals := make([]interface{}, len(cols))
			valPtrs := make([]interface{}, len(cols))
			for i := range vals { valPtrs[i] = &vals[i] }
			if err := rows.Scan(valPtrs...); err != nil { continue }
			rowMap := map[string]interface{}{}
			for i, col := range cols { rowMap[col] = vals[i] }
			data = append(data, rowMap)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "data": data})
	}
}

// Helper: DeactivateExposureHedgeLinks
func DeactivateExposureHedgeLinks(db *sql.DB, bookingIDs []string) error {
	if len(bookingIDs) == 0 {
		return nil
	}
	_, err := db.Exec(`UPDATE exposure_hedge_links SET is_active = false WHERE booking_id = ANY($1)`, pq.Array(bookingIDs))
	return err
}

// Handler: CreateForwardCancellations
func CreateForwardCancellations(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID             string             `json:"user_id"`
			BookingAmounts     map[string]float64 `json:"booking_amounts"` // booking_id: amount_cancelled
			CancellationDate   string             `json:"cancellation_date"`
			CancellationRate   float64            `json:"cancellation_rate"`
			RealizedGainLoss   float64            `json:"realized_gain_loss"`
			CancellationReason string             `json:"cancellation_reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.BookingAmounts) == 0 || req.CancellationDate == "" || req.CancellationRate == 0 {
			respondWithError(w, http.StatusBadRequest, "user_id, booking_amounts (map), cancellation_date, and cancellation_rate are required")
			return
		}
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusNotFound, "No accessible business units found")
			return
		}

		insertQuery := `INSERT INTO forward_cancellations (booking_id, amount_cancelled, cancellation_date, cancellation_rate, realized_gain_loss, cancellation_reason) VALUES ($1, $2, $3, $4, $5, $6) RETURNING booking_id, amount_cancelled, cancellation_date, cancellation_rate, realized_gain_loss, cancellation_reason`
		ledgerInsertQuery := `INSERT INTO forward_booking_ledger (booking_id, ledger_sequence, action_type, action_id, action_date, amount_changed, running_open_amount) VALUES ($1, $2, $3, $4, $5, $6, $7)`
		cancellations := []map[string]interface{}{}
		cancelledBookingIDs := []string{}
		for bid, amtCancelled := range req.BookingAmounts {
			// 1. Get open amount from ledger (latest running_open_amount for booking_id)
			var openAmount float64
			var ledgerSeq int
			err := db.QueryRow(`SELECT running_open_amount, ledger_sequence FROM forward_booking_ledger WHERE booking_id = $1 ORDER BY ledger_sequence DESC LIMIT 1`, bid).Scan(&openAmount, &ledgerSeq)
			if err == sql.ErrNoRows {
				// fallback to booking_amount from forward_bookings
				err = db.QueryRow(`SELECT booking_amount FROM forward_bookings WHERE system_transaction_id = $1`, bid).Scan(&openAmount)
				if err != nil {

					continue
				}
				ledgerSeq = 0
			} else if err != nil {

				continue // skip on error
			}
			ledgerSeq++ // next sequence for this booking_id
			newOpenAmount := math.Abs(openAmount) - amtCancelled
			actionType := "Partial Cancellation"
			if newOpenAmount <= 0.0001 {
				newOpenAmount = 0
				actionType = "Cancellation"
			}
			// 2. Insert ledger entry with correct sequence
			_, err = db.Exec(ledgerInsertQuery, bid, ledgerSeq, actionType, bid, req.CancellationDate, -amtCancelled, newOpenAmount)
			if err != nil {

				continue // skip on error
			}
			// 3. Insert cancellation record
			var cancel struct {
				BookingID          string
				AmountCancelled    float64
				CancellationDate   string
				CancellationRate   float64
				RealizedGainLoss   float64
				CancellationReason string
			}
			err = db.QueryRow(insertQuery, bid, amtCancelled, req.CancellationDate, req.CancellationRate, req.RealizedGainLoss, req.CancellationReason).Scan(
				&cancel.BookingID,
				&cancel.AmountCancelled,
				&cancel.CancellationDate,
				&cancel.CancellationRate,
				&cancel.RealizedGainLoss,
				&cancel.CancellationReason,
			)
			if err == nil {
				cancellations = append(cancellations, map[string]interface{}{
					"booking_id":          cancel.BookingID,
					"amount_cancelled":    cancel.AmountCancelled,
					"cancellation_date":   cancel.CancellationDate,
					"cancellation_rate":   cancel.CancellationRate,
					"realized_gain_loss":  cancel.RealizedGainLoss,
					"cancellation_reason": cancel.CancellationReason,
				})
			} else {

			}
			// 4. If fully cancelled, add to list for status update and deactivate links
			if newOpenAmount == 0 {
				cancelledBookingIDs = append(cancelledBookingIDs, bid)
			}
		}
		// Update status in forward_bookings only for fully cancelled
		if len(cancelledBookingIDs) > 0 {
			_, err := db.Exec(`UPDATE forward_bookings SET status = 'Cancelled' WHERE system_transaction_id = ANY($1)`, pq.Array(cancelledBookingIDs))
			if err != nil {
				respondWithError(w, http.StatusInternalServerError, "Failed to update forward bookings status")
				return
			}
			// Set is_active = false in exposure_hedge_links
			err = DeactivateExposureHedgeLinks(db, cancelledBookingIDs)
			if err != nil {
				respondWithError(w, http.StatusInternalServerError, "Failed to deactivate exposure hedge links")
				return
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":       true,
			"message":       "Forward cancellations processed successfully",
			"inserted":      len(cancellations),
			"cancellations": cancellations,
		})
	}
}
