package fdBooking

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/api/varianceengine"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v5"
)

// ─── Variance tolerances ──────────────────────────────────────────────────────

const (
	rateTolerance   = 0.05 // 5 basis points
	amountTolerance = 1.0  // ₹1
)

// ─── coerceDateValue ──────────────────────────────────────────────────────────

// coerceDateValue converts various date input representations to either
// a non-empty string (passed to Postgres) or nil (so DB receives NULL).
func coerceDateValue(iv interface{}) interface{} {
	if iv == nil {
		return nil
	}
	switch v := iv.(type) {
	case string:
		if strings.TrimSpace(v) == "" {
			return nil
		}
		return v
	case *string:
		if v == nil {
			return nil
		}
		if strings.TrimSpace(*v) == "" {
			return nil
		}
		return *v
	default:
		return v
	}
}

// ─── logDBError ───────────────────────────────────────────────────────────────

// logDBError provides richer logging for database errors (commit failures etc.).
func logDBError(err error, context string) {
	if err == nil {
		return
	}
	api.LogError("%s: %v", context, err)
	api.LogError("Error string: %s", err.Error())
	api.LogError("Verbose error: %#v", err)
	for u := errors.Unwrap(err); u != nil; u = errors.Unwrap(u) {
		api.LogError("Unwrapped: %T -> %v", u, u)
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		api.LogError("Postgres error detail: Code=%s Message=%s Detail=%s Where=%s Constraint=%s Table=%s Column=%s",
			pgErr.Code, pgErr.Message, pgErr.Detail, pgErr.Where,
			pgErr.ConstraintName, pgErr.TableName, pgErr.ColumnName)
	} else {
		api.LogError("Non-pg error type: %T", err)
	}
}

type fdSchemaQueryExecutor interface {
	Query(ctx context.Context, sql string, arguments ...interface{}) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, arguments ...interface{}) pgx.Row
}

func loadFDTableColumns(ctx context.Context, exec fdSchemaQueryExecutor, schemaName string, tableName string) (map[string]bool, error) {
	rows, err := exec.Query(ctx, `
		SELECT column_name
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2
	`, schemaName, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns := make(map[string]bool)
	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return nil, err
		}
		columns[columnName] = true
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return columns, nil
}

func pickFirstExistingFDColumn(columns map[string]bool, candidates ...string) string {
	for _, candidate := range candidates {
		if columns[candidate] {
			return candidate
		}
	}
	return ""
}

func buildFDPlaceholders(count int) string {
	placeholders := make([]string, count)
	for index := 0; index < count; index++ {
		placeholders[index] = fmt.Sprintf("$%d", index+1)
	}
	return strings.Join(placeholders, ",")
}

func buildFDDynamicInsert(tableName string, columns map[string]bool, preferredColumns []string, values map[string]interface{}, returningCandidates []string) (string, []interface{}, string, bool) {
	insertColumns := make([]string, 0, len(preferredColumns))
	args := make([]interface{}, 0, len(preferredColumns))
	for _, columnName := range preferredColumns {
		if columnName == "" || !columns[columnName] {
			continue
		}
		value, ok := values[columnName]
		if !ok {
			continue
		}
		insertColumns = append(insertColumns, columnName)
		args = append(args, value)
	}
	if len(insertColumns) == 0 {
		return "", nil, "", false
	}

	returningColumn := ""
	for _, candidate := range returningCandidates {
		if columns[candidate] {
			returningColumn = candidate
			break
		}
	}

	insertSQL := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		tableName,
		strings.Join(insertColumns, ","),
		buildFDPlaceholders(len(insertColumns)),
	)
	if returningColumn != "" {
		insertSQL += " RETURNING " + returningColumn
	}
	return insertSQL, args, returningColumn, true
}

func resolveFDBookingAccountColumn(ctx context.Context, exec fdSchemaQueryExecutor) (string, error) {
	columns, err := loadFDTableColumns(ctx, exec, "investment", "fd_booking_request")
	if err != nil {
		return "", err
	}
	return pickFirstExistingFDColumn(columns, "source_account_id", "bank_account_id", "account_id", "bank_account"), nil
}

func resolveFDBookingAccountExpression(ctx context.Context, exec fdSchemaQueryExecutor, tableAlias string) (string, error) {
	columnName, err := resolveFDBookingAccountColumn(ctx, exec)
	if err != nil {
		return "", err
	}
	if columnName == "" {
		return constants.ErrEmptyString, nil
	}
	return fmt.Sprintf("COALESCE(%s.%s,'')", tableAlias, columnName), nil
}

// getEntityName attempts to resolve an entity's display name using master tables.
// Returns empty string if not found or on error.
func getEntityName(ctx context.Context, exec fdSchemaQueryExecutor, entityID string) string {
	var name string
	// Try masterentitycash first
	if err := exec.QueryRow(ctx, `SELECT entity_name FROM masterentitycash WHERE entity_id=$1`, entityID).Scan(&name); err == nil {
		return name
	}
	// Fallback to masterentity
	if err := exec.QueryRow(ctx, `SELECT entity_name FROM masterentity WHERE entity_id=$1`, entityID).Scan(&name); err == nil {
		return name
	}
	return ""
}

// ─── getUserFriendlyFDError ───────────────────────────────────────────────────

// getUserFriendlyFDError converts database errors to user-friendly messages
// for FD Booking / Confirmation operations.
func getUserFriendlyFDError(err error, context string) (string, int) {
	if err == nil {
		return "", http.StatusOK
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "42703":
			return "FD booking schema mismatch detected. The service is using a column name that does not exist in the live database.", http.StatusInternalServerError
		case "23503":
			constraint := strings.ToLower(pgErr.ConstraintName)
			switch {
			case strings.Contains(constraint, "entity"):
				return "Referenced entity was not found or is not valid.", http.StatusOK
			case strings.Contains(constraint, "bank_account") || strings.Contains(constraint, "account"):
				return "Referenced bank account was not found or is not valid.", http.StatusOK
			case strings.Contains(constraint, "bank_config") || strings.Contains(constraint, "config"):
				return "Referenced bank config was not found or is not valid.", http.StatusOK
			case strings.Contains(constraint, "tds"):
				return "Referenced TDS plan was not found or is not valid.", http.StatusOK
			case strings.Contains(constraint, "bank"):
				return "Referenced bank was not found or is not valid.", http.StatusOK
			default:
				return "Referenced record not found or is invalid.", http.StatusOK
			}
		case "23514":
			constraint := strings.ToLower(pgErr.ConstraintName)
			switch {
			case strings.Contains(constraint, "interest_type"):
				return "Invalid interest type. Must be SIMPLE or COMPOUND.", http.StatusOK
			case strings.Contains(constraint, "currency"):
				return "Invalid currency code.", http.StatusOK
			case strings.Contains(constraint, "day_count"):
				return "Invalid day count convention.", http.StatusOK
			case strings.Contains(constraint, "compounding"):
				return "Invalid compounding frequency.", http.StatusOK
			case strings.Contains(constraint, "interest_payout"):
				return "Invalid interest payout frequency.", http.StatusOK
			}
		}
	}
	errStr := strings.ToLower(err.Error())
	// Known infra errors — treat as business logic (200 OK)
	if strings.Contains(errStr, "tx begin failed") ||
		strings.Contains(errStr, "commit failed") ||
		strings.Contains(errStr, "audit insert failed") {
		return err.Error(), http.StatusOK
	}

	// ── Booking constraints ───────────────────────────────────────────────────
	if strings.Contains(errStr, "fd_booking_entity_not_empty") {
		return "Entity ID is required for a booking.", http.StatusOK
	}
	if strings.Contains(errStr, "fd_booking_bank_not_empty") {
		return "Bank ID is required for a booking.", http.StatusOK
	}
	if strings.Contains(errStr, "fd_booking_account_not_empty") {
		return "Account ID is required for a booking.", http.StatusOK
	}
	if strings.Contains(errStr, "fd_booking_principal_positive") {
		return "Principal amount must be greater than zero.", http.StatusOK
	}
	if strings.Contains(errStr, "fd_booking_rate_positive") {
		return "Interest rate must be greater than zero.", http.StatusOK
	}
	if strings.Contains(errStr, "fd_booking_tenor_positive") {
		return "Tenor days must be greater than zero.", http.StatusOK
	}
	if strings.Contains(errStr, "fd_booking_maturity_after_value") {
		return "Maturity date must be after the value date.", http.StatusOK
	}
	if strings.Contains(errStr, "fd_booking_status_chk") {
		return "Invalid booking status. Must be DRAFT, PENDING_APPROVAL, APPROVED, REJECTED, or SENT_TO_BANK.", http.StatusOK
	}
	if strings.Contains(errStr, "fd_booking_interest_payout_chk") {
		return "Invalid interest payout frequency.", http.StatusOK
	}
	if strings.Contains(errStr, "fd_booking_currency_chk") {
		return "Invalid currency code.", http.StatusOK
	}
	if strings.Contains(errStr, "fd_booking_interest_type_chk") {
		return "Invalid interest type. Must be SIMPLE or COMPOUND.", http.StatusOK
	}
	if strings.Contains(errStr, "fd_booking_day_count_chk") {
		return "Invalid day count convention.", http.StatusOK
	}
	if strings.Contains(errStr, "fd_booking_compounding_chk") {
		return "Invalid compounding frequency.", http.StatusOK
	}

	// ── Confirmation constraints ──────────────────────────────────────────────
	if strings.Contains(errStr, "fd_confirmation_booking_fk") {
		return "Booking record not found. Cannot create confirmation.", http.StatusOK
	}
	if strings.Contains(errStr, "fd_confirmation_principal_positive") {
		return "Confirmed principal amount must be greater than zero.", http.StatusOK
	}
	if strings.Contains(errStr, "fd_confirmation_rate_positive") {
		return "Confirmed interest rate must be greater than zero.", http.StatusOK
	}
	if strings.Contains(errStr, "fd_confirmation_tenor_positive") {
		return "Confirmed tenor days must be greater than zero.", http.StatusOK
	}
	if strings.Contains(errStr, "fd_confirmation_status_chk") {
		return "Invalid confirmation status.", http.StatusOK
	}
	if strings.Contains(errStr, "fd_confirmation_variance_action_chk") {
		return "Invalid variance action. Must be ACCEPTED or REJECTED.", http.StatusOK
	}

	// ── Master constraints ────────────────────────────────────────────────────
	if strings.Contains(errStr, "fd_master_confirmation_fk") {
		return "Confirmation record not found. Cannot activate FD.", http.StatusOK
	}
	if strings.Contains(errStr, "fd_master_receipt_date_after_value") {
		return "Receipt date must be on or after the value date.", http.StatusOK
	}
	if strings.Contains(errStr, "fd_master_status_chk") {
		return "Invalid FD master status.", http.StatusOK
	}
	if strings.Contains(errStr, "fd_master_fdno_not_empty") {
		return "FD number / certificate number is required.", http.StatusOK
	}

	// ── Generic DB constraint patterns ───────────────────────────────────────
	if strings.Contains(errStr, "unique constraint") || strings.Contains(errStr, "duplicate key") {
		return "Duplicate entry detected. Please check for existing records.", http.StatusOK
	}
	if strings.Contains(errStr, "foreign key constraint") || strings.Contains(errStr, "violates foreign key") {
		return "Referenced record not found or is invalid.", http.StatusOK
	}
	if strings.Contains(errStr, "check constraint") {
		return "Invalid data format or value.", http.StatusOK
	}
	if strings.Contains(errStr, "not-null constraint") || strings.Contains(errStr, "null value") {
		return "Required field is missing.", http.StatusOK
	}
	if strings.Contains(errStr, "no rows") || strings.Contains(errStr, "not found") {
		return "Record not found.", http.StatusOK
	}
	if strings.Contains(errStr, "connection") || strings.Contains(errStr, "timeout") {
		return "Database connection issue.", http.StatusServiceUnavailable
	}
	return fmt.Sprintf("Internal server error: %s", context), http.StatusInternalServerError
}

// ─── validateBookingFields ───────────────────────────────────────────────────

// validateBookingFields performs pre-DB validation on booking request fields.
// Returns a non-empty string describing the error, or "" if valid.
func validateBookingFields(req map[string]interface{}) string {
	// interest_type
	if v, ok := req["interest_type"].(string); ok && v != "" {
		valid := map[string]bool{"SIMPLE": true, "COMPOUND": true}
		if !valid[v] {
			return fmt.Sprintf("Invalid interest_type '%s'. Must be SIMPLE or COMPOUND.", v)
		}
	}
	// booking_status
	if v, ok := req["booking_status"].(string); ok && v != "" {
		valid := map[string]bool{
			"DRAFT": true, "PENDING_APPROVAL": true,
			"APPROVED": true, "REJECTED": true, "SENT_TO_BANK": true,
		}
		if !valid[v] {
			return fmt.Sprintf("Invalid booking_status '%s'.", v)
		}
	}
	// principal_amount
	if v, ok := req["principal_amount"].(float64); ok && v <= 0 {
		return "principal_amount must be greater than zero."
	}
	// interest_rate
	if v, ok := req["interest_rate"].(float64); ok && v <= 0 {
		return "interest_rate must be greater than zero."
	}
	// tenor: accept tenor_days OR tenure_months OR tenure_years (validate when provided)
	if v, ok := req["tenor_days"].(float64); ok && v <= 0 {
		return "tenor_days must be greater than zero."
	}
	if v, ok := req["tenure_months"].(float64); ok && v <= 0 {
		return "tenure_months must be greater than zero."
	}
	if v, ok := req["tenure_years"].(float64); ok && v <= 0 {
		return "tenure_years must be greater than zero."
	}
	return ""
}

// ─── VarianceResult ──────────────────────────────────────────────────────────

// VarianceResult holds the computed variance between booking and confirmation.
type VarianceResult struct {
	RateVariance         float64
	AmountVariance       float64
	MaturityDateVariance int // days
	HasVariance          bool
	IsThresholdBreached  bool
}

// ─── calculateVariance ───────────────────────────────────────────────────────

// calculateVariance computes the differences between booked values and confirmed
// values. A variance exists when any field differs beyond tolerance. The
// threshold is breached when rate or amount exceeds the defined tolerances.
func calculateVariance(
	bookedRate, confirmedRate float64,
	bookedAmount, confirmedAmount float64,
	bookedMaturity, confirmedMaturity string, // YYYY-MM-DD
) VarianceResult {
	rateVar := math.Abs(confirmedRate - bookedRate)
	amtVar := math.Abs(confirmedAmount - bookedAmount)

	// date variance in days (0 if either is unparseable)
	dateVar := 0
	layout := constants.DateFormat
	bd, errB := time.Parse(layout, bookedMaturity)
	cd, errC := time.Parse(layout, confirmedMaturity)
	if errB == nil && errC == nil {
		diff := cd.Sub(bd)
		if diff < 0 {
			diff = -diff
		}
		dateVar = int(diff.Hours() / 24)
	}

	hasVariance := rateVar > 0 || amtVar > 0 || dateVar > 0
	thresholdBreached := rateVar > rateTolerance || amtVar > amountTolerance || dateVar > 0

	return VarianceResult{
		RateVariance:         rateVar,
		AmountVariance:       amtVar,
		MaturityDateVariance: dateVar,
		HasVariance:          hasVariance,
		IsThresholdBreached:  thresholdBreached,
	}
}

// ─── payoutJSON ───────────────────────────────────────────────────────────────
// payoutJSON passes through a *json.RawMessage for JSONB columns.
// Returns nil when the pointer is nil so Postgres stores NULL.
func payoutJSON(raw *json.RawMessage) interface{} {
	if raw == nil {
		return nil
	}
	return *raw
}

// ─── buildVarianceDetailsJSON ─────────────────────────────────────────────────
// Builds a compact JSON summary of flagged variance items for storage in
// fd_confirmation.variance_details (text / jsonb column).
func buildVarianceDetailsJSON(items []varianceengine.VarianceItem) *string {
	type entry struct {
		Field    string  `json:"field"`
		Expected string  `json:"expected"`
		Actual   string  `json:"actual"`
		Delta    float64 `json:"delta"`
		Priority string  `json:"priority"`
	}
	var entries []entry
	for _, v := range items {
		if v.HasVariance {
			entries = append(entries, entry{
				Field:    v.FieldName,
				Expected: v.ExpectedValue,
				Actual:   v.ActualValue,
				Delta:    v.VarianceDelta,
				Priority: v.Priority,
			})
		}
	}
	if len(entries) == 0 {
		return nil // → NULL in jsonb column
	}
	b, err := json.Marshal(entries)
	if err != nil {
		return nil
	}
	s := string(b)
	return &s
}

// ─── formatNumber ─────────────────────────────────────────────────────────────
// formatNumber formats a float64 in fixed-point notation (no scientific notation).
// Trailing zeroes are stripped; e.g. 456688888.0 → "456688888".
func formatNumber(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}
