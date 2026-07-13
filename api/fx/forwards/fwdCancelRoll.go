package forwards

import (
	api "CimplrCorpSaas/api"
	"CimplrCorpSaas/api/fx/auditutil"
	fxnotification "CimplrCorpSaas/api/fx/notification"
	"CimplrCorpSaas/internal/ctxutil"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/lib/pq"
	"github.com/jackc/pgx/v5/pgxpool"
	// "github.com/lib/pq"
	"CimplrCorpSaas/api/constants"
)

const (
	cancelRollTypeCancellation = "cancellation"
	cancelRollTypeRollover     = "rollover"
	cancelRollStatusDeleted    = "Deleted"
)

type cancelRollActionItem struct {
	RequestType string `json:"request_type"`
	BookingID   string `json:"booking_id"`
	RequestDate string `json:"request_date"`
}

func normalizeCancelRollType(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case cancelRollTypeCancellation:
		return cancelRollTypeCancellation
	case cancelRollTypeRollover:
		return cancelRollTypeRollover
	default:
		return ""
	}
}

func normalizeCancelRollAction(value string) string {
	return strings.ToUpper(strings.TrimSpace(value))
}

func cancelRollAuditTable(requestType string) string {
	if requestType == cancelRollTypeRollover {
		return auditutil.TableForwardRollover
	}
	return auditutil.TableForwardCancellation
}

func cancelRollMainTable(requestType string) string {
	if requestType == cancelRollTypeRollover {
		return "public.forward_rollovers"
	}
	return "public.forward_cancellations"
}

func cancelRollDateColumn(requestType string) string {
	if requestType == cancelRollTypeRollover {
		return "rollover_date"
	}
	return "cancellation_date"
}

func cancelRollAmountColumn(requestType string) string {
	if requestType == cancelRollTypeRollover {
		return "amount_rolled_over"
	}
	return "amount_cancelled"
}

func isUUIDValue(value string) bool {
	value = strings.TrimSpace(value)
	if len(value) != 36 {
		return false
	}
	for idx, char := range value {
		if idx == 8 || idx == 13 || idx == 18 || idx == 23 {
			if char != '-' {
				return false
			}
			continue
		}
		if !((char >= '0' && char <= '9') || (char >= 'a' && char <= 'f') || (char >= 'A' && char <= 'F')) {
			return false
		}
	}
	return true
}

func resolveForwardBookingID(ctx context.Context, db *sql.DB, bookingOrReferenceID string, buNames []string) (string, error) {
	bookingOrReferenceID = strings.TrimSpace(bookingOrReferenceID)
	if bookingOrReferenceID == "" {
		return "", sql.ErrNoRows
	}
	if isUUIDValue(bookingOrReferenceID) {
		return bookingOrReferenceID, nil
	}

	var systemTransactionID string
	if len(buNames) > 0 {
		err := db.QueryRowContext(ctx, `
			SELECT system_transaction_id::text
			FROM forward_bookings
			WHERE internal_reference_id = $1
			  AND entity_level_0 = ANY($2)
			LIMIT 1
		`, bookingOrReferenceID, pq.Array(buNames)).Scan(&systemTransactionID)
		return systemTransactionID, err
	}

	err := db.QueryRowContext(ctx, `
		SELECT system_transaction_id::text
		FROM forward_bookings
		WHERE internal_reference_id = $1
		LIMIT 1
	`, bookingOrReferenceID).Scan(&systemTransactionID)
	return systemTransactionID, err
}

func cancelRollRequestLabel(requestType string) string {
	if requestType == cancelRollTypeRollover {
		return "Rollover"
	}
	return "Cancellation"
}

func cancelRollRowSnapshot(ctx context.Context, db *sql.DB, requestType, bookingID, requestDate string) map[string]interface{} {
	snapshot := auditutil.FetchRowSnapshot(ctx, db, cancelRollMainTable(requestType), "booking_id", bookingID)
	if snapshot == nil {
		snapshot = map[string]interface{}{}
	}
	if strings.TrimSpace(requestDate) != "" {
		snapshot["request_date"] = requestDate
	}
	return snapshot
}

type cancelRollStatusActionParams struct {
	RequestType string
	BookingID   string
	RequestDate string
	Action      string
	Status      string
	Actor       string
	Comment     string
}

func recordCancelRollStatusAction(ctx context.Context, db *sql.DB, params cancelRollStatusActionParams) {
	auditutil.RecordAction(ctx, db, auditutil.ActionParams{
		TableName:      cancelRollAuditTable(params.RequestType),
		ParentColumn:   "booking_id",
		ParentID:       params.BookingID,
		ActionType:     params.Action,
		Status:         params.Status,
		Reason:         params.Comment,
		RequestedBy:    params.Actor,
		CheckerBy:      params.Actor,
		CheckerComment: params.Comment,
		OldValues:      map[string]interface{}{"status": "Pending", "request_date": params.RequestDate},
		NewValues:      map[string]interface{}{"status": params.Status, "request_date": params.RequestDate},
	})
}

func executeCancellationApproval(ctx context.Context, db *sql.DB, userID, bookingID, requestDate, comment string) error {
	var amountCancelled, cancellationRate, realizedGainLoss float64
	var cancellationReason string
	err := db.QueryRowContext(ctx, `
		SELECT amount_cancelled, cancellation_rate, realized_gain_loss, COALESCE(cancellation_reason, '')
		FROM forward_cancellations
		WHERE booking_id = $1
		  AND cancellation_date = $2
		  AND status = 'Pending'
		  AND COALESCE(is_deleted, false) = false
	`, bookingID, requestDate).Scan(&amountCancelled, &cancellationRate, &realizedGainLoss, &cancellationReason)
	if err != nil {
		return err
	}
	if strings.TrimSpace(comment) != "" {
		cancellationReason = comment
	}

	var openAmount float64
	var ledgerSeq int
	err = db.QueryRowContext(ctx, `SELECT running_open_amount, ledger_sequence FROM forward_booking_ledger WHERE booking_id = $1 ORDER BY ledger_sequence DESC LIMIT 1`, bookingID).Scan(&openAmount, &ledgerSeq)
	if err == sql.ErrNoRows {
		err = db.QueryRowContext(ctx, `SELECT booking_amount FROM forward_bookings WHERE system_transaction_id = $1`, bookingID).Scan(&openAmount)
		if err != nil {
			return err
		}
		ledgerSeq = 0
	} else if err != nil {
		return err
	}

	ledgerSeq++
	newOpenAmount := math.Abs(openAmount) - amountCancelled
	actionType := "Partial Cancellation"
	if newOpenAmount <= 0.0001 {
		newOpenAmount = 0
		actionType = "Cancellation"
	}
	if _, err = db.ExecContext(ctx, `INSERT INTO forward_booking_ledger (booking_id, ledger_sequence, action_type, action_id, action_date, amount_changed, running_open_amount) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		bookingID, ledgerSeq, actionType, bookingID, requestDate, -amountCancelled, newOpenAmount); err != nil {
		return err
	}
	result, err := db.ExecContext(ctx, `UPDATE forward_cancellations SET status = 'Approved' WHERE booking_id = $1 AND cancellation_date = $2 AND status = 'Pending' AND COALESCE(is_deleted, false) = false`, bookingID, requestDate)
	if err != nil {
		return err
	}
	if affected, _ := result.RowsAffected(); affected == 0 {
		return fmt.Errorf(constants.ErrPendingCancellationNotUpdated)
	}
	auditutil.RecordDecision(ctx, db, auditutil.DecisionParams{TableName: auditutil.TableForwardCancellation, ParentColumn: "booking_id", ParentID: bookingID, Status: constants.StatusApproved, CheckerBy: auditutil.Actor(userID), Comment: cancellationReason})
	recordCancelRollStatusAction(ctx, db, cancelRollStatusActionParams{RequestType: cancelRollTypeCancellation, BookingID: bookingID, RequestDate: requestDate, Action: constants.AuditActionApprove, Status: constants.StatusApproved, Actor: auditutil.Actor(userID), Comment: cancellationReason})
	if newOpenAmount == 0 {
		if _, err = db.ExecContext(ctx, `UPDATE forward_bookings SET status = 'Cancelled' WHERE system_transaction_id = $1`, bookingID); err == nil {
			_ = DeactivateExposureHedgeLinks(db, []string{bookingID})
		}
	} else {
		_, _ = db.ExecContext(ctx, `UPDATE forward_bookings SET status = 'Partiallu Cancelled' WHERE system_transaction_id = $1`, bookingID)
	}
	_ = cancellationRate
	_ = realizedGainLoss
	return nil
}

func executeRolloverApproval(ctx context.Context, db *sql.DB, userID, bookingID, requestDate, comment string) error {
	var cancellationDate, origMaturityDate, newMaturityDate, fxPair, orderType, amount, spotRate, premiumDiscount, marginRate, netRate string
	var amountRolledOver float64
	var realizedGainLoss sql.NullFloat64
	err := db.QueryRowContext(ctx, `SELECT rollover_date, original_maturity_date, new_maturity_date, fx_pair, order_type, new_forward_amount, new_forward_spot_rate, new_forward_premium_discount, new_forward_margin_rate, new_forward_net_rate, rollover_cost, amount_rolled_over FROM forward_rollovers WHERE booking_id = $1 AND rollover_date = $2 AND status = 'Pending' AND COALESCE(is_deleted, false) = false`, bookingID, requestDate).
		Scan(&cancellationDate, &origMaturityDate, &newMaturityDate, &fxPair, &orderType, &amount, &spotRate, &premiumDiscount, &marginRate, &netRate, &realizedGainLoss, &amountRolledOver)
	if err != nil {
		return err
	}

	var currentStatus string
	err = db.QueryRowContext(ctx, `SELECT status FROM forward_bookings WHERE system_transaction_id = $1`, bookingID).Scan(&currentStatus)
	if err != nil {
		return fmt.Errorf("forward booking not found for rollover request")
	}

	var openAmount float64
	var ledgerSeq int
	err = db.QueryRowContext(ctx, `SELECT running_open_amount, ledger_sequence FROM forward_booking_ledger WHERE booking_id = $1 ORDER BY ledger_sequence DESC LIMIT 1`, bookingID).Scan(&openAmount, &ledgerSeq)
	if err == sql.ErrNoRows {
		err = db.QueryRowContext(ctx, `SELECT booking_amount FROM forward_bookings WHERE system_transaction_id = $1`, bookingID).Scan(&openAmount)
		if err != nil {
			return err
		}
		ledgerSeq = 0
	} else if err != nil {
		return err
	}
	ledgerSeq++
	newOpenAmount := math.Abs(openAmount) - amountRolledOver
	actionType := "Partial Rollover"
	if newOpenAmount <= 0.0001 {
		newOpenAmount = 0
		actionType = "Rollover"
	}
	if _, err = db.ExecContext(ctx, `INSERT INTO forward_booking_ledger (booking_id, ledger_sequence, action_type, action_id, action_date, amount_changed, running_open_amount) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		bookingID, ledgerSeq, actionType, bookingID, cancellationDate, -amountRolledOver, newOpenAmount); err != nil {
		return err
	}
	result, err := db.ExecContext(ctx, `UPDATE forward_rollovers SET status = 'Approved' WHERE booking_id = $1 AND rollover_date = $2 AND status = 'Pending' AND COALESCE(is_deleted, false) = false`, bookingID, cancellationDate)
	if err != nil {
		return err
	}
	if affected, _ := result.RowsAffected(); affected == 0 {
		return fmt.Errorf(constants.ErrPendingRolloverNotUpdated)
	}
	auditutil.RecordDecision(ctx, db, auditutil.DecisionParams{TableName: auditutil.TableForwardRollover, ParentColumn: "booking_id", ParentID: bookingID, Status: constants.StatusApproved, CheckerBy: auditutil.Actor(userID), Comment: comment})
	recordCancelRollStatusAction(ctx, db, cancelRollStatusActionParams{RequestType: cancelRollTypeRollover, BookingID: bookingID, RequestDate: requestDate, Action: constants.AuditActionApprove, Status: constants.StatusApproved, Actor: auditutil.Actor(userID), Comment: comment})
	if newOpenAmount == 0 {
		_, _ = db.ExecContext(ctx, `UPDATE forward_bookings SET status = 'Rolled Over' WHERE system_transaction_id = $1`, bookingID)
	} else {
		_, _ = db.ExecContext(ctx, `UPDATE forward_bookings SET processing_status = 'Partially Rollovered' WHERE system_transaction_id = $1`, bookingID)
	}

	randomRef := GenerateFXRef()
	var entityLevel0, localCurrency, counterparty string
	var entityLevel1, entityLevel2, entityLevel3 sql.NullString
	err = db.QueryRowContext(ctx, `SELECT entity_level_0, entity_level_1, entity_level_2, entity_level_3, local_currency, counterparty FROM forward_bookings WHERE system_transaction_id::text = $1`, bookingID).Scan(&entityLevel0, &entityLevel1, &entityLevel2, &entityLevel3, &localCurrency, &counterparty)
	if err != nil {
		return err
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
	err = db.QueryRowContext(ctx, `INSERT INTO forward_bookings (
			internal_reference_id, entity_level_0, entity_level_1, entity_level_2, entity_level_3, local_currency, order_type, transaction_type, counterparty, mode_of_delivery, delivery_period, add_date, settlement_date, maturity_date, delivery_date, currency_pair, base_currency, quote_currency, booking_amount, value_type, actual_value_base_currency, spot_rate, forward_points, bank_margin, total_rate, value_quote_currency, intervening_rate_quote_to_local, value_local_currency, internal_dealer, counterparty_dealer, remarks, narration, transaction_timestamp, status, processing_status
		) VALUES (
			$1,$2,$3,$4,$5,$6,$7,NULL,$8,NULL,NULL,$9,$10,$11,NULL,$12,NULL,$13,$14,NULL,NULL,$15,$16,$17,$18,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Pending Confirmation','pending'
		) RETURNING system_transaction_id`,
		randomRef, entityLevel0, entityLevel1Val, entityLevel2Val, entityLevel3Val, localCurrency, orderType, counterparty, cancellationDate, cancellationDate, newMaturityDate, fxPair, localCurrency, amount, spotRate, premiumDiscount, marginRate, netRate,
	).Scan(&newBookingID)
	if err != nil {
		return err
	}
	var newLedgerSeq int
	_ = db.QueryRowContext(ctx, `SELECT COALESCE(MAX(ledger_sequence),0) FROM forward_booking_ledger WHERE booking_id = $1`, newBookingID).Scan(&newLedgerSeq)
	newLedgerSeq++
	_, err = db.ExecContext(ctx, `INSERT INTO forward_booking_ledger (booking_id, ledger_sequence, action_type, action_id, action_date, amount_changed, running_open_amount) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		newBookingID, newLedgerSeq, "Rollover", newBookingID, cancellationDate, amount, amount)
	return err
}

// Handler: CancellationStatusRequest
func CancellationStatusRequest(db *sql.DB, pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID             string             `json:"user_id"`
			BookingAmounts     map[string]float64 `json:"booking_amounts"` // booking_id: amount_cancelled
			CancellationDates  map[string]string  `json:"cancellation_dates"`
			CancellationDate   string             `json:"cancellation_date"`
			CancellationRate   float64            `json:"cancellation_rate"`
			RealizedGainLoss   float64            `json:"realized_gain_loss"`
			CancellationReason string             `json:"cancellation_reason"`
			Status             string             `json:"status"`
			RejectionComment   string             `json:"rejection_comment"`
			Comment            string             `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.BookingAmounts) == 0 || req.CancellationRate == 0 {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueError: "user_id, booking_amounts (map), cancellation date, and cancellation_rate are required"})
			return
		}
		requestedStatus := strings.TrimSpace(req.Status)
		if requestedStatus == "" {
			requestedStatus = "Approved"
		}
		notifItems := make([]fxnotification.CancelRollItem, 0, len(req.BookingAmounts))
		for bid, amtCancelled := range req.BookingAmounts {
			cancellationDate := strings.TrimSpace(req.CancellationDates[bid])
			if cancellationDate == "" {
				cancellationDate = strings.TrimSpace(req.CancellationDate)
			}
			if cancellationDate == "" {
				respondWithError(w, http.StatusBadRequest, "cancellation date is required for selected booking")
				return
			}
			var hasPendingCancellation bool
			err := db.QueryRow(`
				SELECT EXISTS (
					SELECT 1
					FROM forward_cancellations
					WHERE booking_id = $1
					  AND cancellation_date = $2
					  AND status = 'Pending'
					  AND COALESCE(is_deleted, false) = false
				)
			`, bid, cancellationDate).Scan(&hasPendingCancellation)
			if err != nil || !hasPendingCancellation {
				w.WriteHeader(http.StatusBadRequest)
				json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueError: "Booking not in pending cancellation state"})
				return
			}
			if strings.EqualFold(requestedStatus, "Rejected") {
				result, err := db.Exec(`UPDATE forward_cancellations SET status = 'Rejected' WHERE booking_id = $1 AND cancellation_date = $2 AND status = 'Pending' AND COALESCE(is_deleted, false) = false`, bid, cancellationDate)
				if err != nil {
					w.WriteHeader(http.StatusInternalServerError)
					json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueError: "failed to reject pending cancellation"})
					return
				}
				if affected, _ := result.RowsAffected(); affected == 0 {
					w.WriteHeader(http.StatusBadRequest)
					json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueError: constants.ErrPendingCancellationNotUpdated})
					return
				}
				_, _ = db.Exec(`UPDATE forward_bookings SET status = $1, processing_status = $2 WHERE system_transaction_id = $3 AND status = 'Pending Cancellation'`, constants.FwdStatusConfirmed, constants.FwdProcessingStatusApproved, bid)
				decisionComment := strings.TrimSpace(req.Comment)
				if decisionComment == "" {
					decisionComment = strings.TrimSpace(req.CancellationReason)
				}
				auditutil.RecordDecision(r.Context(), db, auditutil.DecisionParams{TableName: auditutil.TableForwardCancellation, ParentColumn: "booking_id", ParentID: bid, Status: constants.StatusRejected, CheckerBy: auditutil.Actor(req.UserID), Comment: decisionComment})
				recordCancelRollStatusAction(r.Context(), db, cancelRollStatusActionParams{RequestType: cancelRollTypeCancellation, BookingID: bid, RequestDate: cancellationDate, Action: constants.AuditActionReject, Status: constants.StatusRejected, Actor: auditutil.Actor(req.UserID), Comment: decisionComment})
				notifItems = append(notifItems, fxnotification.CancelRollItem{RequestType: cancelRollTypeCancellation, BookingID: bid, RequestDate: cancellationDate})
				continue
			}
			// Perform the actual cancellation logic (ledger, cancellation record, etc.)
			var openAmount float64
			var ledgerSeq int
			err = db.QueryRow(`SELECT running_open_amount, ledger_sequence FROM forward_booking_ledger WHERE booking_id = $1 ORDER BY ledger_sequence DESC LIMIT 1`, bid).Scan(&openAmount, &ledgerSeq)
			if err == sql.ErrNoRows {
				err = db.QueryRow(`SELECT booking_amount FROM forward_bookings WHERE system_transaction_id = $1`, bid).Scan(&openAmount)
				if err != nil {
					w.WriteHeader(http.StatusInternalServerError)
					json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueError: "failed to read cancellation booking amount"})
					return
				}
				ledgerSeq = 0
			} else if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueError: "failed to read cancellation ledger"})
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
				bid, ledgerSeq, actionType, bid, cancellationDate, -amtCancelled, newOpenAmount)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueError: "failed to write cancellation ledger"})
				return
			}
			// Update cancellation record to Approved
			result, err := db.Exec(`UPDATE forward_cancellations SET status = 'Approved' WHERE booking_id = $1 AND cancellation_date = $2 AND status = 'Pending' AND COALESCE(is_deleted, false) = false`, bid, cancellationDate)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueError: "failed to approve pending cancellation"})
				return
			}
			if affected, _ := result.RowsAffected(); affected == 0 {
				w.WriteHeader(http.StatusBadRequest)
				json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueError: constants.ErrPendingCancellationNotUpdated})
				return
			}
			auditutil.RecordDecision(r.Context(), db, auditutil.DecisionParams{TableName: auditutil.TableForwardCancellation, ParentColumn: "booking_id", ParentID: bid, Status: constants.StatusApproved, CheckerBy: auditutil.Actor(req.UserID), Comment: req.CancellationReason})
			recordCancelRollStatusAction(r.Context(), db, cancelRollStatusActionParams{RequestType: cancelRollTypeCancellation, BookingID: bid, RequestDate: cancellationDate, Action: constants.AuditActionApprove, Status: constants.StatusApproved, Actor: auditutil.Actor(req.UserID), Comment: req.CancellationReason})
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
			notifItems = append(notifItems, fxnotification.CancelRollItem{RequestType: cancelRollTypeCancellation, BookingID: bid, RequestDate: cancellationDate})
		}
		action := constants.AuditActionApprove
		if strings.EqualFold(requestedStatus, "Rejected") {
			action = constants.AuditActionReject
		}
		triggerCancelRollNotif(r.Context(), pool, routeForwardCancellationStatus, action, auditutil.Actor(req.UserID), requestedStatus, notifItems)
		respondEnvelopeSuccess(w, "Forward cancellation request processed successfully", map[string]interface{}{
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
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueError: constants.ErrUserIIsRequired})
			return
		}
		scope := ctxutil.FromContext(r.Context())
		buNames := scope.EntityNames
		if len(buNames) == 0 {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueError: constants.ErrNoAccessibleBusinessUnit})
			return
		}
		// Join forward_cancellations with forward_bookings to get entity_level_0 (bu)
		getQuery := `
			SELECT fc.booking_id, fc.status, fb.entity_level_0 AS business_unit, fc.amount_cancelled, fc.cancellation_date, fc.cancellation_rate, fc.realized_gain_loss, fc.cancellation_reason
			FROM forward_cancellations fc
			LEFT JOIN forward_bookings fb ON fc.booking_id = fb.system_transaction_id
			WHERE fc.status = 'Pending'
			  AND COALESCE(fc.is_deleted, false) = false
			  AND (fb.entity_level_0 = ANY($1) OR fb.system_transaction_id IS NULL)
		`
		rows, err := db.Query(getQuery, pq.Array(buNames))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueError: "failed to retrieve pending cancellations"})
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
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			constants.ValueSuccess: true,
			"bookings":             bookings,
		})
	}
}

// Handler: GetAllCancellationRollovers
func GetAllCancellationRollovers(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.UserID) == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueError: constants.ErrUserIIsRequired})
			return
		}
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueError: constants.ErrNoAccessibleBusinessUnit})
			return
		}

		query := `
			SELECT *
			FROM (
				SELECT
					'cancellation' AS request_type,
					fc.booking_id,
					fb.entity_level_0 AS business_unit,
					fc.amount_cancelled AS amount,
					fc.cancellation_date AS request_date,
					fc.status,
					fc.cancellation_rate,
					fc.realized_gain_loss,
					fc.cancellation_reason,
					NULL::numeric AS rollover_cost,
					NULL::text AS fx_pair
				FROM forward_cancellations fc
				LEFT JOIN forward_bookings fb ON fc.booking_id = fb.system_transaction_id
				WHERE COALESCE(fc.is_deleted, false) = false
				  AND (fb.entity_level_0 = ANY($1) OR fb.system_transaction_id IS NULL)
				UNION ALL
				SELECT
					'rollover' AS request_type,
					fr.booking_id,
					fb.entity_level_0 AS business_unit,
					fr.amount_rolled_over AS amount,
					fr.rollover_date AS request_date,
					fr.status,
					NULL::numeric AS cancellation_rate,
					NULL::numeric AS realized_gain_loss,
					NULL::text AS cancellation_reason,
					fr.rollover_cost,
					fr.fx_pair
				FROM forward_rollovers fr
				LEFT JOIN forward_bookings fb ON fr.booking_id = fb.system_transaction_id
				WHERE COALESCE(fr.is_deleted, false) = false
				  AND (fb.entity_level_0 = ANY($1) OR fb.system_transaction_id IS NULL)
			) rows
			ORDER BY request_date DESC NULLS LAST, booking_id
		`
		rows, err := db.Query(query, pq.Array(buNames))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueError: "failed to retrieve cancellation and rollover requests"})
			return
		}
		defer rows.Close()

		bookings := []map[string]interface{}{}
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
				case "booking_id", "request_type", "business_unit", "status", "cancellation_reason", "fx_pair":
					switch val := v.(type) {
					case []byte:
						rowMap[col] = string(val)
					default:
						rowMap[col] = val
					}
				case "amount", "cancellation_rate", "realized_gain_loss", "rollover_cost":
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
					default:
						rowMap[col] = val
					}
				default:
					rowMap[col] = v
				}
			}
			bookings = append(bookings, rowMap)
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			constants.ValueSuccess: true,
			"bookings":             bookings,
		})
	}
}

// Handler: CancellationRolloverAction
func CancellationRolloverAction(db *sql.DB, pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID  string                 `json:"user_id"`
			Action  string                 `json:"action"`
			Comment string                 `json:"comment"`
			Items   []cancelRollActionItem `json:"items"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.UserID) == "" || len(req.Items) == 0 {
			respondWithError(w, http.StatusBadRequest, constants.ErrInvalidRequestBody)
			return
		}
		action := normalizeCancelRollAction(req.Action)
		if action != constants.AuditActionApprove && action != constants.AuditActionReject && action != constants.AuditActionDelete {
			respondWithError(w, http.StatusBadRequest, "action must be APPROVE, REJECT, or DELETE")
			return
		}
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusNotFound, constants.ErrNoAccessibleBusinessUnit)
			return
		}

		processed := 0
		notifItems := make([]fxnotification.CancelRollItem, 0, len(req.Items))
		for _, item := range req.Items {
			requestType := normalizeCancelRollType(item.RequestType)
			bookingID := strings.TrimSpace(item.BookingID)
			requestDate := strings.TrimSpace(item.RequestDate)
			if requestType == "" || bookingID == "" || requestDate == "" {
				respondWithError(w, http.StatusBadRequest, "request_type, booking_id, and request_date are required for each item")
				return
			}

			tableName := cancelRollMainTable(requestType)
			dateColumn := cancelRollDateColumn(requestType)
			auditTable := cancelRollAuditTable(requestType)
			var currentStatus string
			statusQuery := fmt.Sprintf(`
				SELECT cr.status
				FROM %s cr
				JOIN forward_bookings fb ON cr.booking_id = fb.system_transaction_id
				WHERE cr.booking_id = $1
				  AND cr.%s = $2
				  AND COALESCE(cr.is_deleted, false) = false
				  AND fb.entity_level_0 = ANY($3)
				LIMIT 1
			`, tableName, dateColumn)
			if err := db.QueryRow(statusQuery, bookingID, requestDate, pq.Array(buNames)).Scan(&currentStatus); err != nil {
				respondWithError(w, http.StatusBadRequest, fmt.Sprintf("%s request not found or not accessible", cancelRollRequestLabel(requestType)))
				return
			}

			switch action {
			case constants.AuditActionDelete:
				if strings.EqualFold(currentStatus, constants.StatusPendingDeleteApproval) || strings.EqualFold(currentStatus, cancelRollStatusDeleted) {
					respondWithError(w, http.StatusBadRequest, "delete is not allowed for selected status")
					return
				}
				oldValues := cancelRollRowSnapshot(r.Context(), db, requestType, bookingID, requestDate)
				tx, err := db.BeginTx(r.Context(), nil)
				if err != nil {
					respondWithError(w, http.StatusInternalServerError, "failed to start delete request")
					return
				}
				updateQuery := fmt.Sprintf(`UPDATE %s SET status = $1 WHERE booking_id = $2 AND %s = $3 AND COALESCE(is_deleted, false) = false`, tableName, dateColumn)
				if _, err := tx.ExecContext(r.Context(), updateQuery, constants.StatusPendingDeleteApproval, bookingID, requestDate); err != nil {
					_ = tx.Rollback()
					respondWithError(w, http.StatusInternalServerError, "failed to request delete")
					return
				}
				newValues := map[string]interface{}{"status": constants.StatusPendingDeleteApproval, "request_date": requestDate}
				if err := auditutil.RecordActionTx(r.Context(), tx, auditutil.ActionParams{TableName: auditTable, ParentColumn: "booking_id", ParentID: bookingID, ActionType: constants.AuditActionDelete, Status: constants.StatusPendingDeleteApproval, Reason: req.Comment, RequestedBy: auditutil.Actor(req.UserID), OldValues: oldValues, NewValues: newValues}); err != nil {
					_ = tx.Rollback()
					respondWithError(w, http.StatusInternalServerError, "failed to record delete audit: "+err.Error())
					return
				}
				if err := tx.Commit(); err != nil {
					respondWithError(w, http.StatusInternalServerError, "failed to complete delete request")
					return
				}
			case constants.AuditActionApprove:
				if strings.EqualFold(currentStatus, constants.StatusPendingDeleteApproval) {
					updateQuery := fmt.Sprintf(`UPDATE %s SET status = $1, is_deleted = true WHERE booking_id = $2 AND %s = $3 AND COALESCE(is_deleted, false) = false`, tableName, dateColumn)
					if _, err := db.Exec(updateQuery, cancelRollStatusDeleted, bookingID, requestDate); err != nil {
						respondWithError(w, http.StatusInternalServerError, "failed to approve delete")
						return
					}
					auditutil.RecordDecision(r.Context(), db, auditutil.DecisionParams{TableName: auditTable, ParentColumn: "booking_id", ParentID: bookingID, Status: constants.StatusApproved, CheckerBy: auditutil.Actor(req.UserID), Comment: req.Comment})
				} else if strings.EqualFold(currentStatus, "Pending") {
					if requestType == cancelRollTypeRollover {
						if err := executeRolloverApproval(r.Context(), db, req.UserID, bookingID, requestDate, req.Comment); err != nil {
							respondWithError(w, http.StatusInternalServerError, "failed to approve rollover request")
							return
						}
					} else {
						if err := executeCancellationApproval(r.Context(), db, req.UserID, bookingID, requestDate, req.Comment); err != nil {
							respondWithError(w, http.StatusInternalServerError, "failed to approve cancellation request")
							return
						}
					}
				} else {
					respondWithError(w, http.StatusBadRequest, "approve is only allowed for Pending or PENDING_DELETE_APPROVAL rows")
					return
				}
			case constants.AuditActionReject:
				if strings.EqualFold(currentStatus, constants.StatusPendingDeleteApproval) {
					updateQuery := fmt.Sprintf(`UPDATE %s SET status = $1 WHERE booking_id = $2 AND %s = $3 AND COALESCE(is_deleted, false) = false`, tableName, dateColumn)
					if _, err := db.Exec(updateQuery, constants.StatusRejected, bookingID, requestDate); err != nil {
						respondWithError(w, http.StatusInternalServerError, "failed to reject delete")
						return
					}
					auditutil.RecordDecision(r.Context(), db, auditutil.DecisionParams{TableName: auditTable, ParentColumn: "booking_id", ParentID: bookingID, Status: constants.StatusRejected, CheckerBy: auditutil.Actor(req.UserID), Comment: req.Comment})
					recordCancelRollStatusAction(r.Context(), db, cancelRollStatusActionParams{RequestType: requestType, BookingID: bookingID, RequestDate: requestDate, Action: constants.AuditActionReject, Status: constants.StatusRejected, Actor: auditutil.Actor(req.UserID), Comment: req.Comment})
				} else if strings.EqualFold(currentStatus, "Pending") {
					updateQuery := fmt.Sprintf(`UPDATE %s SET status = 'Rejected' WHERE booking_id = $1 AND %s = $2 AND COALESCE(is_deleted, false) = false`, tableName, dateColumn)
					if _, err := db.Exec(updateQuery, bookingID, requestDate); err != nil {
						respondWithError(w, http.StatusInternalServerError, "failed to reject request")
						return
					}
					parentPendingStatus := "Pending Cancellation"
					if requestType == cancelRollTypeRollover {
						parentPendingStatus = "Pending Rollover"
					}
					_, _ = db.Exec(`UPDATE forward_bookings SET status = $1, processing_status = $2 WHERE system_transaction_id = $3 AND status = $4`, constants.FwdStatusConfirmed, constants.FwdProcessingStatusApproved, bookingID, parentPendingStatus)
					auditutil.RecordDecision(r.Context(), db, auditutil.DecisionParams{TableName: auditTable, ParentColumn: "booking_id", ParentID: bookingID, Status: constants.StatusRejected, CheckerBy: auditutil.Actor(req.UserID), Comment: req.Comment})
					recordCancelRollStatusAction(r.Context(), db, cancelRollStatusActionParams{RequestType: requestType, BookingID: bookingID, RequestDate: requestDate, Action: constants.AuditActionReject, Status: constants.StatusRejected, Actor: auditutil.Actor(req.UserID), Comment: req.Comment})
				} else {
					respondWithError(w, http.StatusBadRequest, "reject is only allowed for Pending or PENDING_DELETE_APPROVAL rows")
					return
				}
			}
			notifItems = append(notifItems, fxnotification.CancelRollItem{
				RequestType: requestType,
				BookingID:   bookingID,
				RequestDate: requestDate,
			})
			processed++
		}

		triggerCancelRollNotif(r.Context(), pool, routeForwardCancelRollAction, action, auditutil.Actor(req.UserID), action, notifItems)
		respondEnvelopeSuccess(w, "Cancellation/rollover action processed successfully", map[string]interface{}{
			"processed": processed,
			"message":   "Cancellation/Rollover action processed successfully",
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
			respondWithError(w, http.StatusBadRequest, constants.ErrInvalidRequestBody)
			return
		}

		scope := ctxutil.FromContext(r.Context())
		buNames := scope.EntityNames
		if len(buNames) == 0 {
			respondWithError(w, http.StatusNotFound, constants.ErrNoAccessibleBusinessUnit)
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
			auditutil.RecordAction(r.Context(), db, auditutil.ActionParams{TableName: auditutil.TableForwardRollover, ParentColumn: "booking_id", ParentID: bid, ActionType: constants.AuditActionCreate, Status: constants.StatusPendingApproval, Reason: "", RequestedBy: auditutil.Actor(req.UserID), OldValues: nil, NewValues: map[string]interface{}{"amount_rolled_over": amtCancelled, "rollover_date": req.CancellationDate, "new_forward": req.NewForward}})
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			constants.ValueSuccess: true,
			"message":              "Forward rollover request submitted for approval",
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
			respondWithError(w, http.StatusBadRequest, constants.ErrUserIIsRequired)
			return
		}
		scope := ctxutil.FromContext(r.Context())
		buNames := scope.EntityNames
		if len(buNames) == 0 {
			respondWithError(w, http.StatusNotFound, constants.ErrNoAccessibleBusinessUnit)
			return
		}
		// fetch forward_bookings with upload links for /all endpoint
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
				counterparty,
				upload_link,
				confirmation_upload_link
			FROM forward_bookings
			WHERE entity_level_0 = ANY($1)
				AND COALESCE(is_deleted, false) = false
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
				case "upload_link", "confirmation_upload_link":
					switch val := v.(type) {
					case nil:
						rowMap[col] = ""
					case string:
						rowMap[col] = val
					case []byte:
						rowMap[col] = string(val)
					default:
						rowMap[col] = fmt.Sprintf("%v", v)
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
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: true, "data": data})
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
		scope := ctxutil.FromContext(r.Context())
		buNames := scope.EntityNames
		if len(buNames) == 0 {
			respondWithError(w, http.StatusNotFound, constants.ErrNoAccessibleBusinessUnit)
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
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: true, "data": data})
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
		buNames, _ := r.Context().Value(api.BusinessUnitsKey).([]string)
		// Save cancellation request as pending
		for bookingOrReferenceID, amtCancelled := range req.BookingAmounts {
			bookingID, err := resolveForwardBookingID(r.Context(), db, bookingOrReferenceID, buNames)
			if err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					respondWithError(w, http.StatusBadRequest, fmt.Sprintf("Forward booking not found for %s", bookingOrReferenceID))
					return
				}
				respondWithError(w, http.StatusInternalServerError, "Failed to validate forward booking: "+err.Error())
				return
			}
			_, err = db.Exec(`INSERT INTO forward_cancellations (booking_id, amount_cancelled, cancellation_date, cancellation_rate, realized_gain_loss, cancellation_reason, status) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
				bookingID, amtCancelled, req.CancellationDate, req.CancellationRate, req.RealizedGainLoss, req.CancellationReason, "Pending")
			if err != nil {
				respondWithError(w, http.StatusInternalServerError, "Failed to save cancellation request: "+err.Error())
				return
			}
			// Set booking status to Pending Cancellation
			_, err = db.Exec(`UPDATE forward_bookings SET status = 'Pending Cancellation', processing_status = 'Pending' WHERE system_transaction_id = $1`, bookingID)
			if err != nil {
				respondWithError(w, http.StatusInternalServerError, "Failed to update booking status to pending cancellation")
				return
			}
			auditutil.RecordAction(r.Context(), db, auditutil.ActionParams{TableName: auditutil.TableForwardCancellation, ParentColumn: "booking_id", ParentID: bookingID, ActionType: constants.AuditActionCreate, Status: constants.StatusPendingApproval, Reason: req.CancellationReason, RequestedBy: auditutil.Actor(req.UserID), OldValues: nil, NewValues: map[string]interface{}{"amount_cancelled": amtCancelled, "cancellation_date": req.CancellationDate, "cancellation_rate": req.CancellationRate, "realized_gain_loss": req.RealizedGainLoss}})
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			constants.ValueSuccess: true,
			"message":              "Forward cancellation request submitted for approval",
		})
	}
}

func RolloverStatusRequest(db *sql.DB, pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID           string             `json:"user_id"`
			BookingAmounts   map[string]float64 `json:"booking_amounts"` // booking_id: amount_cancelled
			RolloverDates    map[string]string  `json:"rollover_dates"`
			RolloverDate     string             `json:"rollover_date"`
			Status           string             `json:"status"`
			RejectionComment string             `json:"rejection_comment"`
			Comment          string             `json:"comment"`
			ApprovalComment  string             `json:"approval_comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.BookingAmounts) == 0 {
			respondWithError(w, http.StatusBadRequest, constants.ErrInvalidRequestBody)
			return
		}
		requestedStatus := strings.TrimSpace(req.Status)
		if requestedStatus == "" {
			requestedStatus = "Approved"
		}
		notifItems := make([]fxnotification.CancelRollItem, 0, len(req.BookingAmounts))
		for bid, amtCancelled := range req.BookingAmounts {
			// Fetch rollover details (including new forward) from DB
			var cancellationDate, origMaturityDate, newMaturityDate, fxPair, orderType, amount, spotRate, premiumDiscount, marginRate, netRate string
			var realizedGainLoss sql.NullFloat64
			rolloverDate := strings.TrimSpace(req.RolloverDates[bid])
			if rolloverDate == "" {
				rolloverDate = strings.TrimSpace(req.RolloverDate)
			}
			var err error
			if rolloverDate != "" {
				err = db.QueryRow(`SELECT rollover_date, original_maturity_date, new_maturity_date, fx_pair, order_type, new_forward_amount, new_forward_spot_rate, new_forward_premium_discount, new_forward_margin_rate, new_forward_net_rate, rollover_cost FROM forward_rollovers WHERE booking_id = $1 AND rollover_date = $2 AND status = 'Pending' AND COALESCE(is_deleted, false) = false`, bid, rolloverDate).
					Scan(&cancellationDate, &origMaturityDate, &newMaturityDate, &fxPair, &orderType, &amount, &spotRate, &premiumDiscount, &marginRate, &netRate, &realizedGainLoss)
			} else {
				err = db.QueryRow(`SELECT rollover_date, original_maturity_date, new_maturity_date, fx_pair, order_type, new_forward_amount, new_forward_spot_rate, new_forward_premium_discount, new_forward_margin_rate, new_forward_net_rate, rollover_cost FROM forward_rollovers WHERE booking_id = $1 AND status = 'Pending' AND COALESCE(is_deleted, false) = false ORDER BY rollover_date DESC NULLS LAST LIMIT 1`, bid).
					Scan(&cancellationDate, &origMaturityDate, &newMaturityDate, &fxPair, &orderType, &amount, &spotRate, &premiumDiscount, &marginRate, &netRate, &realizedGainLoss)
			}
			if err != nil {
				respondWithError(w, http.StatusBadRequest, "booking not in pending rollover state")
				return
			}
			if strings.EqualFold(requestedStatus, "Rejected") {
				result, err := db.Exec(`UPDATE forward_rollovers SET status = 'Rejected' WHERE booking_id = $1 AND rollover_date = $2 AND status = 'Pending' AND COALESCE(is_deleted, false) = false`, bid, cancellationDate)
				if err != nil {
					respondWithError(w, http.StatusInternalServerError, "failed to reject pending rollover")
					return
				}
				if affected, _ := result.RowsAffected(); affected == 0 {
					respondWithError(w, http.StatusBadRequest, constants.ErrPendingRolloverNotUpdated)
					return
				}
				_, _ = db.Exec(`UPDATE forward_bookings SET status = $1, processing_status = $2 WHERE system_transaction_id = $3 AND status = 'Pending Rollover'`, constants.FwdStatusConfirmed, constants.FwdProcessingStatusApproved, bid)
				decisionComment := strings.TrimSpace(req.Comment)
				if decisionComment == "" {
					decisionComment = strings.TrimSpace(req.RejectionComment)
				}
				auditutil.RecordDecision(r.Context(), db, auditutil.DecisionParams{TableName: auditutil.TableForwardRollover, ParentColumn: "booking_id", ParentID: bid, Status: constants.StatusRejected, CheckerBy: auditutil.Actor(req.UserID), Comment: decisionComment})
				recordCancelRollStatusAction(r.Context(), db, cancelRollStatusActionParams{RequestType: cancelRollTypeRollover, BookingID: bid, RequestDate: cancellationDate, Action: constants.AuditActionReject, Status: constants.StatusRejected, Actor: auditutil.Actor(req.UserID), Comment: decisionComment})
				notifItems = append(notifItems, fxnotification.CancelRollItem{RequestType: cancelRollTypeRollover, BookingID: bid, RequestDate: cancellationDate})
				continue
			}
			// The pending tab is driven by forward_rollovers.status, so do not block
			// valid pending rollover requests because the parent booking status is stale.
			var currentStatus string
			err = db.QueryRow(`SELECT status FROM forward_bookings WHERE system_transaction_id = $1`, bid).Scan(&currentStatus)
			if err != nil {
				respondWithError(w, http.StatusBadRequest, "forward booking not found for rollover request")
				return
			}
			// Ledger logic
			var openAmount float64
			var ledgerSeq int
			err = db.QueryRow(`SELECT running_open_amount, ledger_sequence FROM forward_booking_ledger WHERE booking_id = $1 ORDER BY ledger_sequence DESC LIMIT 1`, bid).Scan(&openAmount, &ledgerSeq)
			if err == sql.ErrNoRows {
				err = db.QueryRow(`SELECT booking_amount FROM forward_bookings WHERE system_transaction_id = $1`, bid).Scan(&openAmount)
				if err != nil {
					respondWithError(w, http.StatusInternalServerError, "failed to read rollover booking amount")
					return
				}
				ledgerSeq = 0
			} else if err != nil {
				respondWithError(w, http.StatusInternalServerError, "failed to read rollover ledger")
				return
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
				respondWithError(w, http.StatusInternalServerError, "failed to write rollover ledger")
				return
			}
			// Update rollover record to Approved
			result, err := db.Exec(`UPDATE forward_rollovers SET status = 'Approved' WHERE booking_id = $1 AND rollover_date = $2 AND status = 'Pending' AND COALESCE(is_deleted, false) = false`, bid, cancellationDate)
			if err != nil {
				respondWithError(w, http.StatusInternalServerError, "failed to approve pending rollover")
				return
			}
			if affected, _ := result.RowsAffected(); affected == 0 {
				respondWithError(w, http.StatusBadRequest, constants.ErrPendingRolloverNotUpdated)
				return
			}
			auditutil.RecordDecision(r.Context(), db, auditutil.DecisionParams{TableName: auditutil.TableForwardRollover, ParentColumn: "booking_id", ParentID: bid, Status: constants.StatusApproved, CheckerBy: auditutil.Actor(req.UserID), Comment: ""})
			recordCancelRollStatusAction(r.Context(), db, cancelRollStatusActionParams{RequestType: cancelRollTypeRollover, BookingID: bid, RequestDate: cancellationDate, Action: constants.AuditActionApprove, Status: constants.StatusApproved, Actor: auditutil.Actor(req.UserID), Comment: strings.TrimSpace(req.Comment)})
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
				respondWithError(w, http.StatusInternalServerError, "failed to read rollover booking details")
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
				randomRef, entityLevel0, entityLevel1Val, entityLevel2Val, entityLevel3Val, localCurrency, orderType, counterparty, cancellationDate, cancellationDate, newMaturityDate, fxPair, localCurrency, amount, spotRate, premiumDiscount, marginRate, netRate,
			).Scan(&newBookingID)
			if err != nil {
				respondWithError(w, http.StatusInternalServerError, "failed to create rollover forward booking")
				return
			}
			var newLedgerSeq int
			_ = db.QueryRow(`SELECT COALESCE(MAX(ledger_sequence),0) FROM forward_booking_ledger WHERE booking_id = $1`, newBookingID).Scan(&newLedgerSeq)
			newLedgerSeq++
			_, err = db.Exec(`INSERT INTO forward_booking_ledger (booking_id, ledger_sequence, action_type, action_id, action_date, amount_changed, running_open_amount) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
				newBookingID, newLedgerSeq, "Rollover", newBookingID, cancellationDate, amount, amount)
			if err != nil {
				respondWithError(w, http.StatusInternalServerError, "failed to write new rollover ledger")
				return
			}
			notifItems = append(notifItems, fxnotification.CancelRollItem{RequestType: cancelRollTypeRollover, BookingID: bid, RequestDate: cancellationDate})
		}
		action := constants.AuditActionApprove
		if strings.EqualFold(requestedStatus, "Rejected") {
			action = constants.AuditActionReject
		}
		triggerCancelRollNotif(r.Context(), pool, routeForwardRolloverStatus, action, auditutil.Actor(req.UserID), requestedStatus, notifItems)
		respondEnvelopeSuccess(w, "Forward rollover request processed successfully", map[string]interface{}{
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
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueError: constants.ErrUserIIsRequired})
			return
		}
		scope := ctxutil.FromContext(r.Context())
		buNames := scope.EntityNames
		if len(buNames) == 0 {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueError: constants.ErrNoAccessibleBusinessUnit})
			return
		}
		// Join forward_rollovers with forward_bookings to get entity_level_0 (bu)
		getQuery := `
		       SELECT fr.booking_id, fr.status, fb.entity_level_0 AS business_unit, fr.amount_rolled_over, fr.rollover_date, fr.original_maturity_date, fr.new_maturity_date, fr.rollover_cost, fr.fx_pair, fr.order_type, fr.new_forward_amount, fr.new_forward_spot_rate, fr.new_forward_premium_discount, fr.new_forward_margin_rate, fr.new_forward_net_rate
		       FROM forward_rollovers fr
		       LEFT JOIN forward_bookings fb ON fr.booking_id = fb.system_transaction_id
		       WHERE fr.status = 'Pending'
		         AND COALESCE(fr.is_deleted, false) = false
		         AND (fb.entity_level_0 = ANY($1) OR fb.system_transaction_id IS NULL)
	       `
		rows, err := db.Query(getQuery, pq.Array(buNames))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueError: "failed to retrieve pending rollovers"})
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
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			constants.ValueSuccess: true,
			"bookings":             bookings,
		})
	}
}
