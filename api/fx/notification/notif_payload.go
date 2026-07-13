package notification

// notif_payload.go — Rich notification payload builders for FX forwards + MTM events.

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ForwardBookingPayload holds forward booking notification data.
type ForwardBookingPayload struct {
	Action          string
	RequestedBy     string
	Count           int
	ActionAt        string
	BookingIDs      []string
	Bookings        []map[string]interface{}
	ByEntityKPIs    []map[string]interface{}
	TotalAmount     float64
	ProcessingStatus string
}

func (p ForwardBookingPayload) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"Action":           p.Action,
		"RequestedBy":      p.RequestedBy,
		"Count":            p.Count,
		"ActionAt":         p.ActionAt,
		"BookingIDs":       p.BookingIDs,
		"Bookings":         p.Bookings,
		"ByEntityKPIs":     p.ByEntityKPIs,
		"TotalAmount":      p.TotalAmount,
		"ProcessingStatus": p.ProcessingStatus,
	}
}

// ForwardConfirmationPayload holds forward confirmation notification data.
type ForwardConfirmationPayload struct {
	Action          string
	RequestedBy     string
	Count           int
	ActionAt        string
	BookingIDs      []string
	Confirmations   []map[string]interface{}
	ByEntityKPIs    []map[string]interface{}
	ProcessingStatus string
}

func (p ForwardConfirmationPayload) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"Action":           p.Action,
		"RequestedBy":      p.RequestedBy,
		"Count":            p.Count,
		"ActionAt":         p.ActionAt,
		"BookingIDs":       p.BookingIDs,
		"Confirmations":    p.Confirmations,
		"ByEntityKPIs":     p.ByEntityKPIs,
		"ProcessingStatus": p.ProcessingStatus,
	}
}

// CancelRollPayload holds cancellation / rollover notification data.
type CancelRollPayload struct {
	Action       string
	RequestedBy  string
	Count        int
	ActionAt     string
	RequestType  string
	BookingIDs   []string
	Requests     []map[string]interface{}
	ByEntityKPIs []map[string]interface{}
	Status       string
}

func (p CancelRollPayload) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"Action":       p.Action,
		"RequestedBy":  p.RequestedBy,
		"Count":        p.Count,
		"ActionAt":     p.ActionAt,
		"RequestType":  p.RequestType,
		"BookingIDs":   p.BookingIDs,
		"Requests":     p.Requests,
		"ByEntityKPIs": p.ByEntityKPIs,
		"Status":       p.Status,
	}
}

// MTMPayload holds MTM rate notification data.
type MTMPayload struct {
	Action           string
	RequestedBy      string
	Count            int
	ActionAt         string
	MTMIDs           []string
	Records          []map[string]interface{}
	ByEntityKPIs     []map[string]interface{}
	TotalNotional    float64
	TotalMTMValue    float64
	ProcessingStatus string
}

func (p MTMPayload) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"Action":           p.Action,
		"RequestedBy":      p.RequestedBy,
		"Count":            p.Count,
		"ActionAt":         p.ActionAt,
		"MTMIDs":           p.MTMIDs,
		"Records":          p.Records,
		"ByEntityKPIs":     p.ByEntityKPIs,
		"TotalNotional":    p.TotalNotional,
		"TotalMTMValue":    p.TotalMTMValue,
		"ProcessingStatus": p.ProcessingStatus,
	}
}

func BuildForwardBookingPayload(ctx context.Context, pool *pgxpool.Pool, bookingIDs []string, action, requestedBy, processingStatus string) ForwardBookingPayload {
	p := ForwardBookingPayload{
		Action:           action,
		RequestedBy:      requestedBy,
		Count:            len(bookingIDs),
		ActionAt:         time.Now().Format(time.RFC3339),
		BookingIDs:       bookingIDs,
		Bookings:         []map[string]interface{}{},
		ByEntityKPIs:     []map[string]interface{}{},
		ProcessingStatus: processingStatus,
	}
	if pool == nil || len(bookingIDs) == 0 {
		return p
	}

	rows, err := pool.Query(ctx, `
		SELECT system_transaction_id, internal_reference_id, entity_level_0, entity_level_1,
		       base_currency, quote_currency, local_currency, currency_pair,
		       booking_amount, value_quote_currency, value_local_currency,
		       processing_status, status, counterparty, maturity_date, order_type
		FROM forward_bookings
		WHERE system_transaction_id = ANY($1)
		  AND COALESCE(is_deleted, false) = false
	`, bookingIDs)
	if err != nil {
		return p
	}
	defer rows.Close()

	entityGroups := map[string]map[string]interface{}{}
	for rows.Next() {
		var bookingID, internalRef, entity, entity1 sqlNullString
		var baseCur, quoteCur, localCur, pair, procStatus, status, counterparty, maturity, orderType sqlNullString
		var bookingAmt, valueQuote, valueLocal sqlNullFloat
		if scanErr := rows.Scan(&bookingID, &internalRef, &entity, &entity1, &baseCur, &quoteCur, &localCur, &pair,
			&bookingAmt, &valueQuote, &valueLocal, &procStatus, &status, &counterparty, &maturity, &orderType); scanErr != nil {
			continue
		}
		row := map[string]interface{}{
			"booking_id":         bookingID.String,
			"system_transaction_id": bookingID.String,
			"internal_reference_id": internalRef.String,
			"entity":             entity.String,
			"entity_level_0":     entity.String,
			"entity_level_1":     entity1.String,
			"base_currency":      baseCur.String,
			"quote_currency":     quoteCur.String,
			"local_currency":     localCur.String,
			"currency_pair":      pair.String,
			"currencies":         strings.TrimSpace(fmt.Sprintf("%s/%s", baseCur.String, quoteCur.String)),
			"booking_amount":     bookingAmt.Float64,
			"amounts": map[string]interface{}{
				"booking_amount":     bookingAmt.Float64,
				"value_quote_currency": valueQuote.Float64,
				"value_local_currency": valueLocal.Float64,
			},
			"processing_status": procStatus.String,
			"status":            status.String,
			"counterparty":      counterparty.String,
			"maturity_date":     maturity.String,
			"order_type":        orderType.String,
		}
		p.Bookings = append(p.Bookings, row)
		p.TotalAmount += bookingAmt.Float64

		entityName := entity.String
		if entityName == "" {
			entityName = "Unknown"
		}
		kpi, ok := entityGroups[entityName]
		if !ok {
			kpi = map[string]interface{}{"group_name": entityName, "count": 0, "total_amount": float64(0)}
			entityGroups[entityName] = kpi
		}
		kpi["count"] = kpi["count"].(int) + 1
		kpi["total_amount"] = kpi["total_amount"].(float64) + bookingAmt.Float64
	}
	for _, kpi := range entityGroups {
		p.ByEntityKPIs = append(p.ByEntityKPIs, kpi)
	}
	return p
}

func BuildForwardConfirmationPayload(ctx context.Context, pool *pgxpool.Pool, bookingIDs []string, action, requestedBy, processingStatus string) ForwardConfirmationPayload {
	p := ForwardConfirmationPayload{
		Action:           action,
		RequestedBy:      requestedBy,
		Count:            len(bookingIDs),
		ActionAt:         time.Now().Format(time.RFC3339),
		BookingIDs:       bookingIDs,
		Confirmations:    []map[string]interface{}{},
		ByEntityKPIs:     []map[string]interface{}{},
		ProcessingStatus: processingStatus,
	}
	if pool == nil || len(bookingIDs) == 0 {
		return p
	}

	rows, err := pool.Query(ctx, `
		SELECT system_transaction_id, internal_reference_id, entity_level_0,
		       base_currency, quote_currency, currency_pair, booking_amount,
		       bank_transaction_id, swift_unique_id, bank_confirmation_date,
		       processing_status, status
		FROM forward_bookings
		WHERE system_transaction_id = ANY($1)
		  AND COALESCE(is_deleted, false) = false
	`, bookingIDs)
	if err != nil {
		return p
	}
	defer rows.Close()

	entityGroups := map[string]map[string]interface{}{}
	for rows.Next() {
		var bookingID, internalRef, entity sqlNullString
		var baseCur, quoteCur, pair, bankTxn, swift, bankConfDate, procStatus, status sqlNullString
		var bookingAmt sqlNullFloat
		if scanErr := rows.Scan(&bookingID, &internalRef, &entity, &baseCur, &quoteCur, &pair, &bookingAmt,
			&bankTxn, &swift, &bankConfDate, &procStatus, &status); scanErr != nil {
			continue
		}
		row := map[string]interface{}{
			"booking_id":              bookingID.String,
			"internal_reference_id":   internalRef.String,
			"entity":                  entity.String,
			"base_currency":           baseCur.String,
			"quote_currency":          quoteCur.String,
			"currency_pair":           pair.String,
			"currencies":              strings.TrimSpace(fmt.Sprintf("%s/%s", baseCur.String, quoteCur.String)),
			"booking_amount":          bookingAmt.Float64,
			"bank_transaction_id":     bankTxn.String,
			"swift_unique_id":         swift.String,
			"bank_confirmation_date":  bankConfDate.String,
			"processing_status":       procStatus.String,
			"status":                  status.String,
		}
		p.Confirmations = append(p.Confirmations, row)

		entityName := entity.String
		if entityName == "" {
			entityName = "Unknown"
		}
		kpi, ok := entityGroups[entityName]
		if !ok {
			kpi = map[string]interface{}{"group_name": entityName, "count": 0}
			entityGroups[entityName] = kpi
		}
		kpi["count"] = kpi["count"].(int) + 1
	}
	for _, kpi := range entityGroups {
		p.ByEntityKPIs = append(p.ByEntityKPIs, kpi)
	}
	return p
}

type cancelRollKey struct {
	requestType string
	bookingID   string
	requestDate string
}

func BuildCancelRollPayload(ctx context.Context, pool *pgxpool.Pool, items []cancelRollKey, action, requestedBy, status string) CancelRollPayload {
	p := CancelRollPayload{
		Action:       action,
		RequestedBy:  requestedBy,
		Count:        len(items),
		ActionAt:     time.Now().Format(time.RFC3339),
		BookingIDs:   []string{},
		Requests:     []map[string]interface{}{},
		ByEntityKPIs: []map[string]interface{}{},
		Status:       status,
	}
	if pool == nil || len(items) == 0 {
		return p
	}

	entityGroups := map[string]map[string]interface{}{}
	seenBooking := map[string]bool{}
	for _, item := range items {
		if item.requestType != "" && p.RequestType == "" {
			p.RequestType = item.requestType
		}
		if !seenBooking[item.bookingID] {
			p.BookingIDs = append(p.BookingIDs, item.bookingID)
			seenBooking[item.bookingID] = true
		}

		var row map[string]interface{}
		switch item.requestType {
		case "rollover":
			row = fetchRolloverRequest(ctx, pool, item.bookingID, item.requestDate)
		default:
			row = fetchCancellationRequest(ctx, pool, item.bookingID, item.requestDate)
		}
		if row == nil {
			row = map[string]interface{}{
				"booking_id":   item.bookingID,
				"request_type": item.requestType,
				"request_date": item.requestDate,
				"status":       status,
			}
		} else {
			row["request_type"] = item.requestType
			row["action"] = action
			row["status"] = status
		}
		p.Requests = append(p.Requests, row)

		entityName := fmt.Sprint(row["entity"])
		if entityName == "" || entityName == "<nil>" {
			entityName = "Unknown"
		}
		kpi, ok := entityGroups[entityName]
		if !ok {
			kpi = map[string]interface{}{"group_name": entityName, "count": 0}
			entityGroups[entityName] = kpi
		}
		kpi["count"] = kpi["count"].(int) + 1
	}
	for _, kpi := range entityGroups {
		p.ByEntityKPIs = append(p.ByEntityKPIs, kpi)
	}
	return p
}

func BuildCancelRollPayloadFromBookings(ctx context.Context, pool *pgxpool.Pool, bookingIDs []string, requestType, action, requestedBy, status string) CancelRollPayload {
	items := make([]cancelRollKey, 0, len(bookingIDs))
	for _, id := range bookingIDs {
		items = append(items, cancelRollKey{requestType: requestType, bookingID: id, requestDate: ""})
	}
	return BuildCancelRollPayload(ctx, pool, items, action, requestedBy, status)
}

func BuildMTMPayload(ctx context.Context, pool *pgxpool.Pool, mtmIDs []string, action, requestedBy, processingStatus string) MTMPayload {
	p := MTMPayload{
		Action:           action,
		RequestedBy:      requestedBy,
		Count:            len(mtmIDs),
		ActionAt:         time.Now().Format(time.RFC3339),
		MTMIDs:           mtmIDs,
		Records:          []map[string]interface{}{},
		ByEntityKPIs:     []map[string]interface{}{},
		ProcessingStatus: processingStatus,
	}
	if pool == nil || len(mtmIDs) == 0 {
		return p
	}

	rows, err := pool.Query(ctx, `
		SELECT mtm_id, booking_id, entity, currency_pair, buy_sell,
		       notional_amount, contract_rate, mtm_rate, mtm_value,
		       COALESCE(processing_status, status) AS processing_status, status,
		       deal_date, maturity_date, internal_reference_id
		FROM forward_mtm
		WHERE mtm_id = ANY($1)
		  AND COALESCE(is_deleted, false) = false
	`, mtmIDs)
	if err != nil {
		return p
	}
	defer rows.Close()

	entityGroups := map[string]map[string]interface{}{}
	for rows.Next() {
		var mtmID, bookingID, entity, pair, buySell, procStatus, status, dealDate, maturity, internalRef sqlNullString
		var notional, contractRate, mtmRate, mtmValue sqlNullFloat
		if scanErr := rows.Scan(&mtmID, &bookingID, &entity, &pair, &buySell, &notional, &contractRate, &mtmRate, &mtmValue,
			&procStatus, &status, &dealDate, &maturity, &internalRef); scanErr != nil {
			continue
		}
		row := map[string]interface{}{
			"mtm_id":              mtmID.String,
			"booking_id":          bookingID.String,
			"entity":              entity.String,
			"currency_pair":       pair.String,
			"buy_sell":            buySell.String,
			"notional_amount":     notional.Float64,
			"contract_rate":       contractRate.Float64,
			"mtm_rate":            mtmRate.Float64,
			"mtm_value":           mtmValue.Float64,
			"amounts": map[string]interface{}{
				"notional_amount": notional.Float64,
				"mtm_value":       mtmValue.Float64,
			},
			"processing_status":   procStatus.String,
			"status":              status.String,
			"deal_date":           dealDate.String,
			"maturity_date":       maturity.String,
			"internal_reference_id": internalRef.String,
		}
		p.Records = append(p.Records, row)
		p.TotalNotional += notional.Float64
		p.TotalMTMValue += mtmValue.Float64

		entityName := entity.String
		if entityName == "" {
			entityName = "Unknown"
		}
		kpi, ok := entityGroups[entityName]
		if !ok {
			kpi = map[string]interface{}{"group_name": entityName, "count": 0, "total_notional": float64(0), "total_mtm_value": float64(0)}
			entityGroups[entityName] = kpi
		}
		kpi["count"] = kpi["count"].(int) + 1
		kpi["total_notional"] = kpi["total_notional"].(float64) + notional.Float64
		kpi["total_mtm_value"] = kpi["total_mtm_value"].(float64) + mtmValue.Float64
	}
	for _, kpi := range entityGroups {
		p.ByEntityKPIs = append(p.ByEntityKPIs, kpi)
	}
	return p
}

// CancelRollItem is exported for handler use when building cancel/roll payloads.
type CancelRollItem struct {
	RequestType string
	BookingID   string
	RequestDate string
}

func BuildCancelRollPayloadFromItems(ctx context.Context, pool *pgxpool.Pool, items []CancelRollItem, action, requestedBy, status string) CancelRollPayload {
	keys := make([]cancelRollKey, len(items))
	for i, item := range items {
		keys[i] = cancelRollKey{
			requestType: item.RequestType,
			bookingID:   item.BookingID,
			requestDate: item.RequestDate,
		}
	}
	return BuildCancelRollPayload(ctx, pool, keys, action, requestedBy, status)
}

func fetchCancellationRequest(ctx context.Context, pool *pgxpool.Pool, bookingID, requestDate string) map[string]interface{} {
	query := `
		SELECT fc.booking_id, fc.cancellation_date, fc.amount_cancelled, fc.cancellation_rate,
		       fc.realized_gain_loss, fc.cancellation_reason, fc.status,
		       fb.entity_level_0, fb.currency_pair, fb.base_currency, fb.quote_currency, fb.booking_amount
		FROM forward_cancellations fc
		LEFT JOIN forward_bookings fb ON fc.booking_id = fb.system_transaction_id
		WHERE fc.booking_id = $1 AND COALESCE(fc.is_deleted, false) = false`
	args := []interface{}{bookingID}
	if requestDate != "" {
		query += ` AND fc.cancellation_date = $2`
		args = append(args, requestDate)
	}
	query += ` ORDER BY fc.cancellation_date DESC LIMIT 1`

	var bid, cancelDate, reason, fcStatus, entity, pair, baseCur, quoteCur sqlNullString
	var amount, rate, gainLoss, bookingAmt sqlNullFloat
	if err := pool.QueryRow(ctx, query, args...).Scan(&bid, &cancelDate, &amount, &rate, &gainLoss, &reason, &fcStatus,
		&entity, &pair, &baseCur, &quoteCur, &bookingAmt); err != nil {
		return nil
	}
	return map[string]interface{}{
		"booking_id":          bid.String,
		"request_date":        cancelDate.String,
		"amount_cancelled":    amount.Float64,
		"cancellation_rate":   rate.Float64,
		"realized_gain_loss":  gainLoss.Float64,
		"cancellation_reason": reason.String,
		"status":              fcStatus.String,
		"entity":              entity.String,
		"currency_pair":       pair.String,
		"currencies":          strings.TrimSpace(fmt.Sprintf("%s/%s", baseCur.String, quoteCur.String)),
		"booking_amount":      bookingAmt.Float64,
	}
}

func fetchRolloverRequest(ctx context.Context, pool *pgxpool.Pool, bookingID, requestDate string) map[string]interface{} {
	query := `
		SELECT fr.booking_id, fr.rollover_date, fr.amount_rolled_over, fr.rollover_cost,
		       fr.fx_pair, fr.new_forward_amount, fr.status,
		       fb.entity_level_0, fb.currency_pair, fb.base_currency, fb.quote_currency
		FROM forward_rollovers fr
		LEFT JOIN forward_bookings fb ON fr.booking_id = fb.system_transaction_id
		WHERE fr.booking_id = $1 AND COALESCE(fr.is_deleted, false) = false`
	args := []interface{}{bookingID}
	if requestDate != "" {
		query += ` AND fr.rollover_date = $2`
		args = append(args, requestDate)
	}
	query += ` ORDER BY fr.rollover_date DESC LIMIT 1`

	var bid, rollDate, pair, fcStatus, entity, curPair, baseCur, quoteCur, newAmt sqlNullString
	var amount, cost sqlNullFloat
	if err := pool.QueryRow(ctx, query, args...).Scan(&bid, &rollDate, &amount, &cost, &pair, &newAmt, &fcStatus,
		&entity, &curPair, &baseCur, &quoteCur); err != nil {
		return nil
	}
	return map[string]interface{}{
		"booking_id":         bid.String,
		"request_date":       rollDate.String,
		"amount_rolled_over": amount.Float64,
		"rollover_cost":      cost.Float64,
		"fx_pair":            pair.String,
		"new_forward_amount": newAmt.String,
		"status":             fcStatus.String,
		"entity":             entity.String,
		"currency_pair":      curPair.String,
		"currencies":         strings.TrimSpace(fmt.Sprintf("%s/%s", baseCur.String, quoteCur.String)),
	}
}

type sqlNullString struct {
	String string
	Valid  bool
}

type sqlNullFloat struct {
	Float64 float64
	Valid   bool
}

func (n *sqlNullString) Scan(src interface{}) error {
	if src == nil {
		n.Valid = false
		n.String = ""
		return nil
	}
	switch v := src.(type) {
	case string:
		n.String = v
		n.Valid = true
	case []byte:
		n.String = string(v)
		n.Valid = true
	default:
		n.String = fmt.Sprint(v)
		n.Valid = n.String != "" && n.String != "<nil>"
	}
	return nil
}

func (n *sqlNullFloat) Scan(src interface{}) error {
	if src == nil {
		n.Valid = false
		n.Float64 = 0
		return nil
	}
	switch v := src.(type) {
	case float64:
		n.Float64 = v
		n.Valid = true
	case float32:
		n.Float64 = float64(v)
		n.Valid = true
	case int64:
		n.Float64 = float64(v)
		n.Valid = true
	case int32:
		n.Float64 = float64(v)
		n.Valid = true
	case []byte:
		f, err := strconv.ParseFloat(string(v), 64)
		if err != nil {
			n.Valid = false
			return nil
		}
		n.Float64 = f
		n.Valid = true
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		if err != nil {
			n.Valid = false
			return nil
		}
		n.Float64 = f
		n.Valid = true
	default:
		f, err := strconv.ParseFloat(fmt.Sprint(v), 64)
		if err != nil {
			n.Valid = false
			return nil
		}
		n.Float64 = f
		n.Valid = true
	}
	return nil
}
