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
	"time"

	"github.com/lib/pq"
	// "github.com/lib/pq"
)

// Handler: CancellationStatusRequest
func CancellationStatusRequest(db *sql.DB) http.HandlerFunc {
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
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "user_id, booking_amounts (map), cancellation_date, and cancellation_rate are required"})
			return
		}
		for bid, amtCancelled := range req.BookingAmounts {
			// Only approve if status is currently Pending Cancellation
			var currentStatus string
			err := db.QueryRow(`SELECT status FROM forward_bookings WHERE system_transaction_id = $1`, bid).Scan(&currentStatus)
			if err != nil || currentStatus != "Pending Cancellation" {
				w.WriteHeader(http.StatusBadRequest)
				json.NewEncoder(w).Encode(map[string]interface{}{"error": "Booking not in pending cancellation state"})
				return
			}
			// Perform the actual cancellation logic (ledger, cancellation record, etc.)
			var openAmount float64
			var ledgerSeq int
			err = db.QueryRow(`SELECT running_open_amount, ledger_sequence FROM forward_booking_ledger WHERE booking_id = $1 ORDER BY ledger_sequence DESC LIMIT 1`, bid).Scan(&openAmount, &ledgerSeq)
			if err == sql.ErrNoRows {
				err = db.QueryRow(`SELECT booking_amount FROM forward_bookings WHERE system_transaction_id = $1`, bid).Scan(&openAmount)
				if err != nil {
					continue
				}
				ledgerSeq = 0
			} else if err != nil {
				continue // skip on error
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
				continue // skip on error
			}
			// Update cancellation record to Approved
			_, err = db.Exec(`UPDATE forward_cancellations SET status = 'Approved' WHERE booking_id = $1 AND cancellation_date = $2`, bid, req.CancellationDate)
			// If fully cancelled, update booking status to Cancelled and processing_status to Approved
			if newOpenAmount == 0 {
				_, err = db.Exec(`UPDATE forward_bookings SET status = 'Cancelled' WHERE system_transaction_id = $1`, bid)
				if err == nil {
					_ = DeactivateExposureHedgeLinks(db, []string{bid})
				}
			} else {
				// If partial, just update processing_status
				_, _ = db.Exec(`UPDATE forward_bookings SET status = 'Partiallu Cancelled' WHERE system_transaction_id = $1`, bid)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"message": "Forward Cancellation Request Processed Successfully",
		})
	}
}

// Handler: GetPendingCancellations
func GetPendingCancellations(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "user_id is required"})
			return
		}
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "failed to retrieve business units"})
			return
		}
		// Join forward_cancellations with forward_bookings to get entity_level_0 (bu)
		getQuery := `
			SELECT fc.booking_id, fc.status, fb.entity_level_0 AS business_unit, fc.amount_cancelled, fc.cancellation_date, fc.cancellation_rate, fc.realized_gain_loss, fc.cancellation_reason
			FROM forward_cancellations fc
			LEFT JOIN forward_bookings fb ON fc.booking_id = fb.system_transaction_id
			WHERE fc.status = 'Pending' AND fb.entity_level_0 = ANY($1)
		`
		rows, err := db.Query(getQuery, pq.Array(buNames))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "failed to retrieve pending cancellations"})
			return
		}
		defer rows.Close()

		var bookings []map[string]interface{}
		cols, _ := rows.Columns()
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
				case "booking_id":
					switch val := v.(type) {
					case []byte:
						rowMap[col] = string(val)
					case string:
						rowMap[col] = val
					default:
						rowMap[col] = fmt.Sprintf("%v", v)
					}
				case "amount_cancelled", "cancellation_rate", "realized_gain_loss":
					switch val := v.(type) {
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
					case float64:
						rowMap[col] = val
					default:
						rowMap[col] = v
					}
				default:
					rowMap[col] = v
				}
			}
			bookings = append(bookings, rowMap)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":  true,
			"bookings": bookings,
		})
	}
}

func GenerateFXRef() string {
	rand.Seed(time.Now().UnixNano())
	randomPart := rand.Intn(900000) + 100000
	return fmt.Sprintf("FX-TACO-%d", randomPart)
}

// Handler: RolloverForwardBooking
func RolloverForwardBooking(db *sql.DB) http.HandlerFunc {

	return func(w http.ResponseWriter, r *http.Request) {

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

		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusNotFound, "No accessible business units found")
			return
		}

		for bid, amtCancelled := range req.BookingAmounts {
			// Save rollover request as pending, including new forward details
			_, err := db.Exec(`INSERT INTO forward_rollovers (
				   booking_id, amount_rolled_over, rollover_date, original_maturity_date, new_maturity_date, rollover_cost, status,
				   fx_pair, order_type, new_forward_amount, new_forward_spot_rate, new_forward_premium_discount, new_forward_margin_rate, new_forward_net_rate
			   ) VALUES (
				   $1, $2, $3, $4, $5, $6, $7,
				   $8, $9, $10, $11, $12, $13, $14
			   )`,
				bid, amtCancelled, req.CancellationDate, req.CancellationDate, req.NewForward.MaturityDate, req.RealizedGainLoss, "Pending",
				req.NewForward.FXPair, req.NewForward.OrderType, req.NewForward.Amount, req.NewForward.SpotRate, req.NewForward.PremiumDiscount, req.NewForward.MarginRate, req.NewForward.NetRate,
			)
			if err != nil {
				respondWithError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to insert rollover request: %v", err))
				return
			}
			// Set booking status to Pending Rollover
			_, err = db.Exec(`UPDATE forward_bookings SET status = 'Pending Rollover', processing_status = 'Pending' WHERE system_transaction_id = $1`, bid)
			if err != nil {
				respondWithError(w, http.StatusInternalServerError, "Failed to update booking status for rollover")
				return
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"message": "Forward rollover request submitted for approval",
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
				eh.value_date
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
		// Save cancellation request as pending
		for bid, amtCancelled := range req.BookingAmounts {
			_, err := db.Exec(`INSERT INTO forward_cancellations (booking_id, amount_cancelled, cancellation_date, cancellation_rate, realized_gain_loss, cancellation_reason, status) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
				bid, amtCancelled, req.CancellationDate, req.CancellationRate, req.RealizedGainLoss, req.CancellationReason, "Pending")
			if err != nil {
				respondWithError(w, http.StatusInternalServerError, "Failed to save cancellation request")
				return
			}
			// Set booking status to Pending Cancellation
			_, err = db.Exec(`UPDATE forward_bookings SET status = 'Pending Cancellation', processing_status = 'Pending' WHERE system_transaction_id = $1`, bid)
			if err != nil {
				respondWithError(w, http.StatusInternalServerError, "Failed to update booking status to pending cancellation")
				return
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"message": "Forward cancellation request submitted for approval",
		})
	}
}

func RolloverStatusRequest(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID         string             `json:"user_id"`
			BookingAmounts map[string]float64 `json:"booking_amounts"` // booking_id: amount_cancelled
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.BookingAmounts) == 0 {
			respondWithError(w, http.StatusBadRequest, "Invalid request body")
			return
		}
		for bid, amtCancelled := range req.BookingAmounts {
			// Fetch rollover details (including new forward) from DB
			var cancellationDate, origMaturityDate, newMaturityDate, fxPair, orderType, amount, spotRate, premiumDiscount, marginRate, netRate string
			var realizedGainLoss sql.NullFloat64
			err := db.QueryRow(`SELECT rollover_date, original_maturity_date, new_maturity_date, fx_pair, order_type, new_forward_amount, new_forward_spot_rate, new_forward_premium_discount, new_forward_margin_rate, new_forward_net_rate, rollover_cost FROM forward_rollovers WHERE booking_id = $1 AND status = 'Pending'`, bid).
				Scan(&cancellationDate, &origMaturityDate, &newMaturityDate, &fxPair, &orderType, &amount, &spotRate, &premiumDiscount, &marginRate, &netRate, &realizedGainLoss)
			if err != nil {
				return
			}
			// Only approve if status is currently Pending Rollover
			var currentStatus string
			err = db.QueryRow(`SELECT status FROM forward_bookings WHERE system_transaction_id = $1`, bid).Scan(&currentStatus)
			if err != nil || currentStatus != "Pending Rollover" {
				continue // skip if not pending
			}
			// Ledger logic
			var openAmount float64
			var ledgerSeq int
			err = db.QueryRow(`SELECT running_open_amount, ledger_sequence FROM forward_booking_ledger WHERE booking_id = $1 ORDER BY ledger_sequence DESC LIMIT 1`, bid).Scan(&openAmount, &ledgerSeq)
			if err == sql.ErrNoRows {
				err = db.QueryRow(`SELECT booking_amount FROM forward_bookings WHERE system_transaction_id = $1`, bid).Scan(&openAmount)
				if err != nil {
					continue
				}
				ledgerSeq = 0
			} else if err != nil {
				continue
			}
			ledgerSeq++
			newOpenAmount := math.Abs(openAmount) - amtCancelled
			actionType := "Partial Rollover"
			if newOpenAmount <= 0.0001 {
				newOpenAmount = 0
				actionType = "Rollover"
			}
			_, err = db.Exec(`INSERT INTO forward_booking_ledger (booking_id, ledger_sequence, action_type, action_id, action_date, amount_changed, running_open_amount) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
				bid, ledgerSeq, actionType, bid, cancellationDate, -amtCancelled, newOpenAmount)
			if err != nil {
				continue
			}
			// Update rollover record to Approved
			_, err = db.Exec(`UPDATE forward_rollovers SET status = 'Approved' WHERE booking_id = $1 AND rollover_date = $2`, bid, cancellationDate)
			// If fully rolled over, update booking status to Rolled Over and processing_status to Approved
			if newOpenAmount == 0 {
				_, _ = db.Exec(`UPDATE forward_bookings SET status = 'Rolled Over' WHERE system_transaction_id = $1`, bid)
			} else {
				// If partial, just update processing_status
				_, _ = db.Exec(`UPDATE forward_bookings SET processing_status = 'Partially Rollovered' WHERE system_transaction_id = $1`, bid)
			}
			// Insert new forward booking for the rollover using fetched details
			randomRef := GenerateFXRef()
			var entityLevel0, localCurrency, counterparty string
			var entityLevel1, entityLevel2, entityLevel3 sql.NullString
			err = db.QueryRow(`SELECT entity_level_0, entity_level_1, entity_level_2, entity_level_3, local_currency, counterparty FROM forward_bookings WHERE system_transaction_id::text = $1`, bid).Scan(&entityLevel0, &entityLevel1, &entityLevel2, &entityLevel3, &localCurrency, &counterparty)
			if err != nil {
				continue
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
				randomRef, entityLevel0, entityLevel1Val, entityLevel2Val, entityLevel3Val, localCurrency, orderType, counterparty, cancellationDate, cancellationDate, newMaturityDate, fxPair, localCurrency, amount, spotRate, premiumDiscount, marginRate, netRate,
			).Scan(&newBookingID)
			if err != nil {
				continue
			}
			var newLedgerSeq int
			_ = db.QueryRow(`SELECT COALESCE(MAX(ledger_sequence),0) FROM forward_booking_ledger WHERE booking_id = $1`, newBookingID).Scan(&newLedgerSeq)
			newLedgerSeq++
			_, err = db.Exec(`INSERT INTO forward_booking_ledger (booking_id, ledger_sequence, action_type, action_id, action_date, amount_changed, running_open_amount) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
				newBookingID, newLedgerSeq, "Rollover", newBookingID, cancellationDate, amount, amount)
			if err != nil {
				continue
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"message": "Forward Rollover Request Processed Successfully",
		})
	}
}

func GetPendingRollovers(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "user_id is required"})
			return
		}
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "failed to retrieve business units"})
			return
		}
		// Join forward_rollovers with forward_bookings to get entity_level_0 (bu)
		getQuery := `
		       SELECT fr.booking_id, fr.status, fb.entity_level_0 AS business_unit, fr.amount_rolled_over, fr.rollover_date, fr.original_maturity_date, fr.new_maturity_date, fr.rollover_cost, fr.fx_pair, fr.order_type, fr.new_forward_amount, fr.new_forward_spot_rate, fr.new_forward_premium_discount, fr.new_forward_margin_rate, fr.new_forward_net_rate
		       FROM forward_rollovers fr
		       LEFT JOIN forward_bookings fb ON fr.booking_id = fb.system_transaction_id
		       WHERE fr.status = 'Pending' AND fb.entity_level_0 = ANY($1)
	       `
		rows, err := db.Query(getQuery, pq.Array(buNames))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "failed to retrieve pending rollovers"})
			return
		}
		defer rows.Close()

		var bookings []map[string]interface{}
		cols, _ := rows.Columns()
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
				case "booking_id":
					switch val := v.(type) {
					case []byte:
						rowMap[col] = string(val)
					case string:
						rowMap[col] = val
					default:
						rowMap[col] = fmt.Sprintf("%v", v)
					}
				case "amount_rolled_over", "rollover_cost", "new_forward_amount", "new_forward_spot_rate", "new_forward_premium_discount", "new_forward_margin_rate", "new_forward_net_rate":
					switch val := v.(type) {
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
					case float64:
						rowMap[col] = val
					default:
						rowMap[col] = v
					}
				default:
					rowMap[col] = v
				}
			}
			bookings = append(bookings, rowMap)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":  true,
			"bookings": bookings,
		})
	}
}
