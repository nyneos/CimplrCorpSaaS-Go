package forwards

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5/pgxpool"
)

// forwardBookingPolicyFields.go — canonical FORWARD_BOOKING policy field
// builder. Before this file, the four real call sites in fwdBookings.go /
// fwdConfirmations.go each hand-rolled their own Fields{} map:
//   - AddForwardBookingManualEntry (create): 5 of ~39 real columns.
//   - UpdateForwardBookingFields (edit): already correct — merges a full
//     row snapshot (auditutil.FetchRowSnapshotPGX, keyed by real column
//     name) with the patch before calling Enforce, so it already sees the
//     full post-edit row under the same key names as field_code below
//     (field_code == column name for every field on this table). Left
//     untouched deliberately — rebuilding it through forwardBookingRow would
//     be a lateral move, not a fix, and risks losing a column
//     FetchRowSnapshotPGX exposes that the typed struct doesn't.
//   - BulkApproveForwardBookings / BulkRejectForwardBookings (ID only):
//     2 fields (system_transaction_id, processing_status).
//   - UploadForwardBookingsMulti (pre-parse batch upload gate): file_count
//     only — deliberately left thin, same precedent as EXPOSURE_UPLOAD.
//
// Two real handlers in this same sub-module have ZERO Enforce call anywhere
// in their chain — flagged, not silently fixed:
//   - BulkDeleteForwardBookings (fwdConfirmations.go) — soft-deletes
//     (sets processing_status to pending-delete-approval) with no policy
//     check at all.
//   - AddForwardConfirmationManualEntry (fwdConfirmations.go) — mutates
//     forward_bookings (status -> Confirmed) with no policy check at all.
//
// field_code keys below match what's already seeded in domain_catalog for
// FORWARD_BOOKING (see cmd/seedDomainCatalog/forwardBookingCanonical.go) —
// field_code equals the real forward_bookings column name for every field.
type forwardBookingRow struct {
	SystemTransactionID         string
	InternalReferenceID         string
	EntityLevel0                string
	EntityLevel1                string
	EntityLevel2                string
	EntityLevel3                string
	LocalCurrency               string
	OrderType                   string
	TransactionType             string
	Counterparty                string
	ModeOfDelivery              string
	DeliveryPeriod              string
	AddDate                     string
	SettlementDate              string
	MaturityDate                string
	DeliveryDate                string
	CurrencyPair                string
	BaseCurrency                string
	QuoteCurrency               string
	BookingAmount               *float64
	ValueType                   string
	ActualValueBaseCurrency     *float64
	SpotRate                    *float64
	ForwardPoints               *float64
	BankMargin                  *float64
	TotalRate                   *float64
	ValueQuoteCurrency          *float64
	InterveningRateQuoteToLocal *float64
	ValueLocalCurrency          *float64
	InternalDealer              string
	CounterpartyDealer          string
	Remarks                     string
	Narration                   string
	TransactionTimestamp        string
	Status                      string
	BankTransactionID           string
	SwiftUniqueID               string
	BankConfirmationDate        string
	ProcessingStatus            string
}

// buildForwardBookingPolicyFields maps the canonical row onto the exact
// field_code keys seeded in domain_catalog for FORWARD_BOOKING.
func buildForwardBookingPolicyFields(row forwardBookingRow) map[string]interface{} {
	return map[string]interface{}{
		"system_transaction_id":           row.SystemTransactionID,
		"internal_reference_id":           row.InternalReferenceID,
		"entity_level_0":                  row.EntityLevel0,
		"entity_level_1":                  row.EntityLevel1,
		"entity_level_2":                  row.EntityLevel2,
		"entity_level_3":                  row.EntityLevel3,
		"local_currency":                  row.LocalCurrency,
		"order_type":                      row.OrderType,
		"transaction_type":                row.TransactionType,
		"counterparty":                    row.Counterparty,
		"mode_of_delivery":                row.ModeOfDelivery,
		"delivery_period":                 row.DeliveryPeriod,
		"add_date":                        row.AddDate,
		"settlement_date":                 row.SettlementDate,
		"maturity_date":                   row.MaturityDate,
		"delivery_date":                   row.DeliveryDate,
		"currency_pair":                   row.CurrencyPair,
		"base_currency":                   row.BaseCurrency,
		"quote_currency":                  row.QuoteCurrency,
		"booking_amount":                  row.BookingAmount,
		"value_type":                      row.ValueType,
		"actual_value_base_currency":      row.ActualValueBaseCurrency,
		"spot_rate":                       row.SpotRate,
		"forward_points":                  row.ForwardPoints,
		"bank_margin":                     row.BankMargin,
		"total_rate":                      row.TotalRate,
		"value_quote_currency":            row.ValueQuoteCurrency,
		"intervening_rate_quote_to_local": row.InterveningRateQuoteToLocal,
		"value_local_currency":            row.ValueLocalCurrency,
		"internal_dealer":                 row.InternalDealer,
		"counterparty_dealer":             row.CounterpartyDealer,
		"remarks":                         row.Remarks,
		"narration":                       row.Narration,
		"transaction_timestamp":           row.TransactionTimestamp,
		"status":                          row.Status,
		"bank_transaction_id":             row.BankTransactionID,
		"swift_unique_id":                 row.SwiftUniqueID,
		"bank_confirmation_date":          row.BankConfirmationDate,
		"processing_status":               row.ProcessingStatus,
	}
}

// loadForwardBookingRow fetches the full canonical row by
// system_transaction_id — used by BulkApproveForwardBookings / BulkRejectForwardBookings,
// which only ever receives an id per item, never the business data itself.
func loadForwardBookingRow(ctx context.Context, pool *pgxpool.Pool, systemTransactionID string) (forwardBookingRow, error) {
	var row forwardBookingRow
	err := pool.QueryRow(ctx, `
		SELECT system_transaction_id::text, COALESCE(internal_reference_id,''),
		       entity_level_0, COALESCE(entity_level_1,''), COALESCE(entity_level_2,''), COALESCE(entity_level_3,''),
		       COALESCE(local_currency,''), order_type, COALESCE(transaction_type,''), COALESCE(counterparty,''),
		       COALESCE(mode_of_delivery,''), COALESCE(delivery_period,''),
		       COALESCE(TO_CHAR(add_date,'YYYY-MM-DD'),''), COALESCE(TO_CHAR(settlement_date,'YYYY-MM-DD'),''),
		       COALESCE(TO_CHAR(maturity_date,'YYYY-MM-DD'),''), COALESCE(TO_CHAR(delivery_date,'YYYY-MM-DD'),''),
		       currency_pair, COALESCE(base_currency,''), COALESCE(quote_currency,''),
		       booking_amount, COALESCE(value_type,''), actual_value_base_currency,
		       spot_rate, forward_points, bank_margin, total_rate,
		       value_quote_currency, intervening_rate_quote_to_local, value_local_currency,
		       COALESCE(internal_dealer,''), COALESCE(counterparty_dealer,''),
		       COALESCE(remarks,''), COALESCE(narration,''), COALESCE(transaction_timestamp::text,''),
		       COALESCE(status,''), COALESCE(bank_transaction_id,''), COALESCE(swift_unique_id,''),
		       COALESCE(bank_confirmation_date::text,''), COALESCE(processing_status,'')
		FROM forward_bookings
		WHERE system_transaction_id = $1 AND COALESCE(is_deleted,false) = false`, systemTransactionID,
	).Scan(
		&row.SystemTransactionID, &row.InternalReferenceID,
		&row.EntityLevel0, &row.EntityLevel1, &row.EntityLevel2, &row.EntityLevel3,
		&row.LocalCurrency, &row.OrderType, &row.TransactionType, &row.Counterparty,
		&row.ModeOfDelivery, &row.DeliveryPeriod,
		&row.AddDate, &row.SettlementDate,
		&row.MaturityDate, &row.DeliveryDate,
		&row.CurrencyPair, &row.BaseCurrency, &row.QuoteCurrency,
		&row.BookingAmount, &row.ValueType, &row.ActualValueBaseCurrency,
		&row.SpotRate, &row.ForwardPoints, &row.BankMargin, &row.TotalRate,
		&row.ValueQuoteCurrency, &row.InterveningRateQuoteToLocal, &row.ValueLocalCurrency,
		&row.InternalDealer, &row.CounterpartyDealer,
		&row.Remarks, &row.Narration, &row.TransactionTimestamp,
		&row.Status, &row.BankTransactionID, &row.SwiftUniqueID,
		&row.BankConfirmationDate, &row.ProcessingStatus,
	)
	if err != nil {
		return forwardBookingRow{}, err
	}
	return row, nil
}

// jsonNumberToFloatPtr converts an optional JSON number (as decoded via
// json.Decoder.UseNumber, which AddForwardBookingManualEntry's request uses
// to preserve numeric precision) into a *float64 for the canonical row.
func jsonNumberToFloatPtr(n json.Number) *float64 {
	if n == "" {
		return nil
	}
	f, err := n.Float64()
	if err != nil {
		return nil
	}
	return &f
}
