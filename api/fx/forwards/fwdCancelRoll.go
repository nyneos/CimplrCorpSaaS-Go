package forwards

import (
	"CimplrCorpSaas/api"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/lib/pq"
	// "github.com/lib/pq"
)

func GenerateFXRef() string {
	rand.Seed(time.Now().UnixNano())
	randomPart := rand.Intn(900000) + 100000
	return fmt.Sprintf("FX-TACO-%d", randomPart)
}

// Handler: RolloverForwardBooking
func RolloverForwardBooking(db *sql.DB) http.HandlerFunc {

	return func(w http.ResponseWriter, r *http.Request) {

		parseDate := func(dateStr string) (string, error) {
			if strings.TrimSpace(dateStr) == "" {
				return "", fmt.Errorf("empty date string")
			}
			t, err := time.Parse("2006-01-02", dateStr)
			if err != nil {
				// Try parsing as RFC3339 or other common formats
				t2, err2 := time.Parse("2006-01-02T15:04:05Z07:00", dateStr)
				if err2 != nil {
					return "", fmt.Errorf("invalid date format: %s", dateStr)
				}
				t = t2
			}
			return t.Format("2006-01-02"), nil
		}

		var req struct {
			UserID             string             `json:"user_id"`
			BookingAmounts     map[string]float64 `json:"booking_amounts"`
			CancellationDate   string             `json:"cancellation_date"`
			CancellationRate   float64            `json:"cancellation_rate"`
			RealizedGainLoss   float64            `json:"realized_gain_loss"`
			CancellationReason string             `json:"cancellation_reason"`
			NewForward         struct {
				FXPair          string `json:"fxPair"`
				OrderType       string `json:"orderType"`
				MaturityDate    string `json:"maturityDate"`
				Amount          string `json:"amount"`
				SpotRate        string `json:"spotRate"`
				PremiumDiscount string `json:"premiumDiscount"`
				MarginRate      string `json:"marginRate"`
				NetRate         string `json:"netRate"`
			} `json:"new_forward"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.BookingAmounts) == 0 || req.CancellationDate == "" {
			respondWithError(w, http.StatusBadRequest, "Invalid request body")
			return
		}

		addDate, err := parseDate(req.CancellationDate)
		if err != nil {
			respondWithError(w, http.StatusBadRequest, "Invalid cancellation_date: "+err.Error())
			return
		}
		settlementDate, err := parseDate(req.CancellationDate)
		if err != nil {
			respondWithError(w, http.StatusBadRequest, "Invalid settlement_date: "+err.Error())
			return
		}
		maturityDate, err := parseDate(req.NewForward.MaturityDate)
		if err != nil {
			respondWithError(w, http.StatusBadRequest, "Invalid maturity_date: "+err.Error())
			return
		}
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusNotFound, "No accessible business units found")
			return
		}

		cancelledBookingIDs := []string{}
		var origBookingID string
		for bid, amtCancelled := range req.BookingAmounts {
			origBookingID = bid

			if b, ok := any(origBookingID).([]byte); ok {
				origBookingID = string(b)
			}
			origBookingID = strings.TrimSpace(origBookingID)

			var openAmount float64
			var ledgerSeq int
			err := db.QueryRow(`SELECT running_open_amount, ledger_sequence FROM forward_booking_ledger WHERE booking_id = $1 ORDER BY ledger_sequence DESC LIMIT 1`, bid).Scan(&openAmount, &ledgerSeq)
			if err == sql.ErrNoRows {

				err = db.QueryRow(`SELECT booking_amount FROM forward_bookings WHERE system_transaction_id = $1`, bid).Scan(&openAmount)
				if err != nil {
					respondWithError(w, http.StatusNotFound, "Booking not found for cancellation")
					return
				}
				ledgerSeq = 0
			} else if err != nil {
				respondWithError(w, http.StatusInternalServerError, "Failed to fetch ledger for cancellation")
				return
			}
			ledgerSeq++
			newOpenAmount := math.Abs(openAmount) - amtCancelled
			actionType := "Partial Cancellation"
			if newOpenAmount <= 0.0001 {
				newOpenAmount = 0
				actionType = "Cancellation"
			}

			_, err = db.Exec(`INSERT INTO forward_booking_ledger (booking_id, ledger_sequence, action_type, action_id, action_date, amount_changed, running_open_amount) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
				bid, ledgerSeq, actionType, bid, req.CancellationDate, -amtCancelled, newOpenAmount)
			if err != nil {
				respondWithError(w, http.StatusInternalServerError, "Failed to insert cancellation ledger entry")
				return
			}

			_, err = db.Exec(`INSERT INTO forward_cancellations (booking_id, amount_cancelled, cancellation_date, cancellation_rate, realized_gain_loss, cancellation_reason) VALUES ($1, $2, $3, $4, $5, $6)`,
				bid, amtCancelled, req.CancellationDate, req.CancellationRate, req.RealizedGainLoss, req.CancellationReason)
			if err != nil {
				respondWithError(w, http.StatusInternalServerError, "Failed to insert cancellation record")
				return
			}
			if newOpenAmount == 0 {
				cancelledBookingIDs = append(cancelledBookingIDs, bid)
			}
		}

		if len(cancelledBookingIDs) > 0 {
			_, err := db.Exec(`UPDATE forward_bookings SET status = 'Cancelled' WHERE system_transaction_id = ANY($1)`, pq.Array(cancelledBookingIDs))
			if err != nil {
				respondWithError(w, http.StatusInternalServerError, "Failed to update forward bookings status")
				return
			}

			err = DeactivateExposureHedgeLinks(db, cancelledBookingIDs)
			if err != nil {
				respondWithError(w, http.StatusInternalServerError, "Failed to deactivate exposure hedge links")
				return
			}
		}

		randomRef := GenerateFXRef()

		var entityLevel0, localCurrency, counterparty string
		var entityLevel1, entityLevel2, entityLevel3 sql.NullString
		err = db.QueryRow(`SELECT entity_level_0, entity_level_1, entity_level_2, entity_level_3, local_currency, counterparty FROM forward_bookings WHERE system_transaction_id::text = $1`, origBookingID).Scan(&entityLevel0, &entityLevel1, &entityLevel2, &entityLevel3, &localCurrency, &counterparty)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "Failed to fetch original booking details for rollover: "+err.Error())
			return
		}

		var entityLevel1Val, entityLevel2Val, entityLevel3Val *string
		if entityLevel1.Valid {
			entityLevel1Val = &entityLevel1.String
		}
		if entityLevel2.Valid {
			entityLevel2Val = &entityLevel2.String
		}
		if entityLevel3.Valid {
			entityLevel3Val = &entityLevel3.String
		}

		var newBookingID string
		err = db.QueryRow(`INSERT INTO forward_bookings (
		      internal_reference_id, entity_level_0, entity_level_1, entity_level_2, entity_level_3, local_currency, order_type, transaction_type, counterparty, mode_of_delivery, delivery_period, add_date, settlement_date, maturity_date, delivery_date, currency_pair, base_currency, quote_currency, booking_amount, value_type, actual_value_base_currency, spot_rate, forward_points, bank_margin, total_rate, value_quote_currency, intervening_rate_quote_to_local, value_local_currency, internal_dealer, counterparty_dealer, remarks, narration, transaction_timestamp, status, processing_status
	      ) VALUES (
		      $1,$2,$3,$4,$5,$6,$7,NULL,$8,NULL,NULL,$9,$10,$11,NULL,$12,NULL,$13,$14,NULL,NULL,$15,$16,$17,$18,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Pending Confirmation','pending'
	      ) RETURNING system_transaction_id`,
			randomRef, entityLevel0, entityLevel1Val, entityLevel2Val, entityLevel3Val, localCurrency, req.NewForward.OrderType, counterparty, addDate, settlementDate, maturityDate, req.NewForward.FXPair, localCurrency, req.NewForward.Amount, req.NewForward.SpotRate, req.NewForward.PremiumDiscount, req.NewForward.MarginRate, req.NewForward.NetRate,
		).Scan(&newBookingID)
		if err != nil {
			fmt.Printf("Error inserting new forward booking for rollover: %v\n", err)
			respondWithError(w, http.StatusInternalServerError, "Failed to insert new forward booking for rollover: "+err.Error())
			return
		}

		var newLedgerSeq int
		_ = db.QueryRow(`SELECT COALESCE(MAX(ledger_sequence),0) FROM forward_booking_ledger WHERE booking_id = $1`, newBookingID).Scan(&newLedgerSeq)
		newLedgerSeq++
		_, err = db.Exec(`INSERT INTO forward_booking_ledger (booking_id, ledger_sequence, action_type, action_id, action_date, amount_changed, running_open_amount) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			newBookingID, newLedgerSeq, "Rollover", newBookingID, req.CancellationDate, req.NewForward.Amount, req.NewForward.Amount)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "Failed to insert rollover ledger entry")
			return
		}

		_, err = db.Exec(`INSERT INTO forward_rollovers (booking_id, amount_rolled_over, rollover_date, original_maturity_date, new_maturity_date, rollover_cost) VALUES ($1, $2, $3, $4, $5, $6)`,
			origBookingID, req.NewForward.Amount, req.CancellationDate, req.CancellationDate, req.NewForward.MaturityDate, req.RealizedGainLoss)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "Failed to insert forward rollover record")
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":        true,
			"message":        "Rollover completed successfully",
			"new_booking_id": newBookingID,
		})
	}
}

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
				AND processing_status = 'Approved'
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
			var bookingID string
			var origBookingAmount float64
			for i, col := range cols {
				v := vals[i]
				// decode and parse as needed
				switch col {
				case "system_transaction_id":
					switch val := v.(type) {
					case string:
						bookingID = val
						rowMap[col] = val
					case []byte:
						s := string(val)
						bookingID = s
						rowMap[col] = s
					default:
						rowMap[col] = v
					}
				case "booking_amount":
					switch val := v.(type) {
					case float64:
						origBookingAmount = val
						rowMap[col] = val
					case []byte:
						s := string(val)
						if f, err := strconv.ParseFloat(s, 64); err == nil {
							origBookingAmount = f
							rowMap[col] = f
						} else {
							rowMap[col] = s
						}
					case string:
						if f, err := strconv.ParseFloat(val, 64); err == nil {
							origBookingAmount = f
							rowMap[col] = f
						} else {
							rowMap[col] = val
						}
					default:
						rowMap[col] = v
					}
				case "spot_rate":
					switch val := v.(type) {
					case float64:
						rowMap[col] = val
					case []byte:
						s := string(val)
						if f, err := strconv.ParseFloat(s, 64); err == nil {
							rowMap[col] = f
						} else {
							rowMap[col] = s
						}
					case string:
						if f, err := strconv.ParseFloat(val, 64); err == nil {
							rowMap[col] = f
						} else {
							rowMap[col] = val
						}
					default:
						rowMap[col] = v
					}
				default:
					rowMap[col] = v
				}
			}
			// Try to get running_open_amount from ledger
			var openAmount float64
			var found bool
			if bookingID != "" {
				err := db.QueryRow(`SELECT running_open_amount FROM forward_booking_ledger WHERE booking_id = $1 ORDER BY ledger_sequence DESC LIMIT 1`, bookingID).Scan(&openAmount)
				if err == nil {
					found = true
				}
			}
			// Set booking_amount to running_open_amount if present, else original booking_amount
			if found {
				rowMap["booking_amount"] = openAmount
			} else {
				rowMap["booking_amount"] = origBookingAmount
			}
			data = append(data, rowMap)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "data": data})
	}
}

// Handler: GetExposuresByBookingIds
// Handler: GetExposuresByBookingIds
func GetExposuresByBookingIds(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID               string   `json:"user_id"`
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
			for i := range vals {
				valPtrs[i] = &vals[i]
			}
			if err := rows.Scan(valPtrs...); err != nil {
				continue
			}
			rowMap := map[string]interface{}{}
			for i, col := range cols {
				v := vals[i]
				switch col {
				case "total_open_amount", "total_original_amount":
					switch val := v.(type) {
					case float64:
						rowMap[col] = val
					case []byte:
						s := string(val)
						if f, err := strconv.ParseFloat(s, 64); err == nil {
							rowMap[col] = f
						} else {
							rowMap[col] = s
						}
					case string:
						if f, err := strconv.ParseFloat(val, 64); err == nil {
							rowMap[col] = f
						} else {
							rowMap[col] = val
						}
					default:
						rowMap[col] = v
					}
				case "currency":
					switch val := v.(type) {
					case string:
						rowMap[col] = val
					case []byte:
						rowMap[col] = string(val)
					default:
						rowMap[col] = v
					}
				default:
					rowMap[col] = v
				}
			}
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





