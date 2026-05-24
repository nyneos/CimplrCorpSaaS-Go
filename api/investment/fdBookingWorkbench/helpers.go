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
	"github.com/jackc/pgx/v5/pgxpool"
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

// coerceDateString returns a YYYY-MM-DD string for variance comparisons, or "" when unset.
func coerceDateString(iv interface{}) string {
	v := coerceDateValue(iv)
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return strings.TrimSpace(s)
	}
	return strings.TrimSpace(fmt.Sprint(v))
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

func fdTextExpr(columns map[string]bool, tableAlias string, fallback string, candidates ...string) string {
	columnName := pickFirstExistingFDColumn(columns, candidates...)
	if columnName == "" {
		return fallback
	}
	return fmt.Sprintf("COALESCE(%s.%s,'')", tableAlias, columnName)
}

func fdNumericExpr(columns map[string]bool, tableAlias string, fallback string, candidates ...string) string {
	columnName := pickFirstExistingFDColumn(columns, candidates...)
	if columnName == "" {
		return fallback
	}
	return fmt.Sprintf("COALESCE(%s.%s,0)", tableAlias, columnName)
}

func fdBoolExpr(columns map[string]bool, tableAlias string, fallback string, candidates ...string) string {
	columnName := pickFirstExistingFDColumn(columns, candidates...)
	if columnName == "" {
		return fallback
	}
	return fmt.Sprintf("COALESCE(%s.%s,false)", tableAlias, columnName)
}

func fdDateTextExpr(columns map[string]bool, tableAlias string, fallback string, candidates ...string) string {
	columnName := pickFirstExistingFDColumn(columns, candidates...)
	if columnName == "" {
		return fallback
	}
	return fmt.Sprintf("COALESCE(TO_CHAR(%s.%s,'YYYY-MM-DD'),'')", tableAlias, columnName)
}

func fdTimestampTextExpr(columns map[string]bool, tableAlias string, fallback string, candidates ...string) string {
	columnName := pickFirstExistingFDColumn(columns, candidates...)
	if columnName == "" {
		return fallback
	}
	return fmt.Sprintf("COALESCE(TO_CHAR(%s.%s,'YYYY-MM-DD HH24:MI:SS'),'')", tableAlias, columnName)
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

func resolveFDBookingAccountNumberExpression(ctx context.Context, exec fdSchemaQueryExecutor, tableAlias string) (string, error) {
	columns, err := loadFDTableColumns(ctx, exec, "investment", "fd_booking_request")
	if err != nil {
		return "", err
	}
	columnName := pickFirstExistingFDColumn(columns, "source_account_number", "bank_account_number", "account_number")
	if columnName == "" {
		return constants.ErrEmptyString, nil
	}
	return fmt.Sprintf("COALESCE(%s.%s,'')", tableAlias, columnName), nil
}

// fdConfirmationHasUploadS3Key returns true when investment.fd_confirmation
// currently has the upload_s3_key column. The deployment target sometimes ships
// without that column (the file-upload feature is optional / behind a later
// migration), so every query that touches it must guard against its absence.
func fdConfirmationHasUploadS3Key(ctx context.Context, exec fdSchemaQueryExecutor) bool {
	columns, err := loadFDTableColumns(ctx, exec, "investment", "fd_confirmation")
	if err != nil {
		return false
	}
	return columns["upload_s3_key"]
}

// resolveFDConfirmationUploadKeyExpression returns a SELECT-list expression
// that yields the upload_s3_key value when the column exists in the live
// database, or an empty string literal otherwise. The returned snippet is
// safe to embed inline (it already includes the `AS upload_s3_key` alias).
func resolveFDConfirmationUploadKeyExpression(ctx context.Context, exec fdSchemaQueryExecutor, tableAlias string) string {
	if fdConfirmationHasUploadS3Key(ctx, exec) {
		return fmt.Sprintf("COALESCE(%s.upload_s3_key,'') AS upload_s3_key", tableAlias)
	}
	return "''::text AS upload_s3_key"
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

// ─── FD confirmation reference uniqueness ────────────────────────────────────

type fdConfirmationReferenceCheck struct {
	EntityID              string
	BankFDRefNo           string
	BankReferenceNumber   string
	ExcludeConfirmationID string
}

func fdReferenceValueInUseOnConfirmation(
	ctx context.Context,
	exec fdSchemaQueryExecutor,
	entityID, refValue, excludeConfirmationID string,
	hasBankReferenceNumberCol bool,
) (bool, error) {
	refValue = strings.TrimSpace(refValue)
	if refValue == "" {
		return false, nil
	}
	excludeConfirmationID = strings.TrimSpace(excludeConfirmationID)

	matchSQL := `BTRIM(COALESCE(c.bank_fd_ref_no, '')) = $3`
	if hasBankReferenceNumberCol {
		matchSQL = `(
			BTRIM(COALESCE(c.bank_fd_ref_no, '')) = $3
			OR BTRIM(COALESCE(c.bank_reference_number::text, '')) = $3
		)`
	}

	var exists bool
	err := exec.QueryRow(ctx, fmt.Sprintf(`
		SELECT EXISTS (
			SELECT 1
			FROM investment.fd_confirmation c
			INNER JOIN investment.fd_booking_request b ON b.booking_id = c.booking_id
			WHERE COALESCE(c.is_deleted, false) = false
			  AND COALESCE(b.is_deleted, false) = false
			  AND b.entity_id = $1
			  AND ($2 = '' OR c.confirmation_id <> $2)
			  AND %s
		)`, matchSQL), entityID, excludeConfirmationID, refValue).Scan(&exists)
	return exists, err
}

func fdReferenceValueInUseOnMaster(
	ctx context.Context,
	exec fdSchemaQueryExecutor,
	entityID, refValue string,
	masterCols map[string]bool,
) (bool, error) {
	refValue = strings.TrimSpace(refValue)
	if refValue == "" || strings.TrimSpace(entityID) == "" {
		return false, nil
	}

	matchSQL := `BTRIM(COALESCE(m.bank_fd_ref_no, '')) = $2`
	if masterCols["bank_reference_number"] {
		matchSQL = `(
			BTRIM(COALESCE(m.bank_fd_ref_no, '')) = $2
			OR BTRIM(COALESCE(m.bank_reference_number::text, '')) = $2
		)`
	}

	var exists bool
	err := exec.QueryRow(ctx, fmt.Sprintf(`
		SELECT EXISTS (
			SELECT 1
			FROM investment.fd_master m
			WHERE COALESCE(m.is_deleted, false) = false
			  AND m.entity_id = $1
			  AND %s
		)`, matchSQL), entityID, refValue).Scan(&exists)
	return exists, err
}

// validateFDConfirmationReferenceUniqueness ensures bank FD ref and bank reference
// number are unique per entity across fd_confirmation and fd_master.
func validateFDConfirmationReferenceUniqueness(
	ctx context.Context,
	exec fdSchemaQueryExecutor,
	check fdConfirmationReferenceCheck,
) (string, int) {
	entityID := strings.TrimSpace(check.EntityID)
	if entityID == "" {
		return "", http.StatusOK
	}

	confCols, err := loadFDTableColumns(ctx, exec, "investment", "fd_confirmation")
	if err != nil {
		return "Failed to load confirmation schema: " + err.Error(), http.StatusInternalServerError
	}
	masterCols, err := loadFDTableColumns(ctx, exec, "investment", "fd_master")
	if err != nil {
		return "Failed to load FD master schema: " + err.Error(), http.StatusInternalServerError
	}
	hasBankReferenceNumberCol := confCols["bank_reference_number"]

	refs := []struct {
		value   string
		fdLabel string
	}{
		{strings.TrimSpace(check.BankFDRefNo), "Bank FD reference number"},
		{strings.TrimSpace(check.BankReferenceNumber), "Bank reference number"},
	}
	seen := make(map[string]bool, len(refs))
	for _, ref := range refs {
		if ref.value == "" || seen[ref.value] {
			continue
		}
		seen[ref.value] = true

		inUse, err := fdReferenceValueInUseOnConfirmation(
			ctx, exec, entityID, ref.value, check.ExcludeConfirmationID, hasBankReferenceNumberCol,
		)
		if err != nil {
			return "Failed to validate confirmation reference uniqueness: " + err.Error(), http.StatusInternalServerError
		}
		if inUse {
			return fmt.Sprintf(
				`%s "%s" already exists for this entity. Please use a unique reference.`,
				ref.fdLabel, ref.value,
			), http.StatusConflict
		}

		inUse, err = fdReferenceValueInUseOnMaster(ctx, exec, entityID, ref.value, masterCols)
		if err != nil {
			return "Failed to validate FD master reference uniqueness: " + err.Error(), http.StatusInternalServerError
		}
		if inUse {
			return fmt.Sprintf(
				`%s "%s" already exists for this entity. Please use a unique reference.`,
				ref.fdLabel, ref.value,
			), http.StatusConflict
		}
	}

	return "", http.StatusOK
}

func resolveEditConfirmationReferenceFields(
	fields map[string]interface{},
	oldBankFDRefNo, oldBankReferenceNumber string,
) (bankFDRefNo, bankReferenceNumber string) {
	bankFDRefNo = strings.TrimSpace(oldBankFDRefNo)
	bankReferenceNumber = strings.TrimSpace(oldBankReferenceNumber)

	if v, ok := fields["bank_fd_ref_no"]; ok {
		bankFDRefNo = strings.TrimSpace(fmt.Sprint(v))
	} else if v, ok := fields["bank_fd_reference"]; ok {
		bankFDRefNo = strings.TrimSpace(fmt.Sprint(v))
	}
	if v, ok := fields["bank_reference_number"]; ok {
		bankReferenceNumber = strings.TrimSpace(fmt.Sprint(v))
	}

	return bankFDRefNo, bankReferenceNumber
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
		case "23505":
			constraint := strings.ToLower(pgErr.ConstraintName)
			if strings.Contains(constraint, "bank_ref") {
				return "Bank reference number already exists for this entity. Please use a unique reference.", http.StatusConflict
			}
			return "Duplicate entry detected. Please check for existing records.", http.StatusConflict
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

// ─── FD confirmation variance (shared) ───────────────────────────────────────
//
// Pending variance is represented by confirmation_status = VARIANCE_PENDING and
// variance_flag = true. variance_action stays NULL until ACCEPTED (exception) or REJECTED.

// fdConfirmationVarianceInput holds booked vs actual values for varianceengine.Compare.
type fdConfirmationVarianceInput struct {
	BookingID, EntityID, RunID  string
	ResolvedBy, ResolvedByEmail string

	BookedPrincipal, ActualPrincipal             float64
	BookedRate, ActualRate                       float64
	BookedTenorDays, ActualTenorDays             int
	BookedTenorMonths, ActualTenorMonths         int
	BookedTenorYears, ActualTenorYears           int
	BookedValueDate, ActualValueDate             string
	BookedMaturityDate, ActualMaturityDate       string
	BookedInterestType, ActualInterestType       string
	BookedFrequencyID, ActualFrequencyID         string
	BookedPayoutFrequencyID, ActualPayoutFrequencyID string
	BookedResetType, ActualResetType             string
	BookedAccrualFrequencyCode, ActualAccrualFrequencyCode string
	BookedTenorType, ActualTenorType             string
	BookedFirstCapDate, ActualFirstCapDate       string
	BookedFirstPayoutDate, ActualFirstPayoutDate string
}

func normalizeResetType(v string) string {
	v = strings.ToUpper(strings.TrimSpace(v))
	if v == "" {
		return "AT_MATURITY"
	}
	return v
}

// normalizeAccrualFrequencyCode treats blank accrual as "same as payout", then compounding.
func normalizeAccrualFrequencyCode(code, payoutFreqID, compoundingFreqID string) string {
	c := strings.TrimSpace(strings.ToUpper(code))
	if c != "" {
		return c
	}
	if p := strings.TrimSpace(payoutFreqID); p != "" {
		return p
	}
	return strings.TrimSpace(compoundingFreqID)
}

func normalizedPayoutFrequency(bookedPayout, bookedCompound, actualPayout, actualCompound string) (booked, actual string) {
	b := strings.TrimSpace(bookedPayout)
	if b == "" {
		b = strings.TrimSpace(bookedCompound)
	}
	a := strings.TrimSpace(actualPayout)
	if a == "" {
		a = strings.TrimSpace(actualCompound)
	}
	return b, a
}

func normalizedAccrualFrequency(bookedCode, actualCode, bookedPayout, actualPayout, bookedCompound, actualCompound string) (booked, actual string) {
	return normalizeAccrualFrequencyCode(bookedCode, bookedPayout, bookedCompound),
		normalizeAccrualFrequencyCode(actualCode, actualPayout, actualCompound)
}

func resetTypeDisplayLabel(v string) string {
	switch normalizeResetType(v) {
	case "AT_EACH_PAYOUT":
		return "At Each Payout"
	default:
		return "At Maturity (default)"
	}
}

// lookupFrequencyLabel resolves a compounding-frequency id to a human readable
// label of the form "FREQ-MONTHLY (Monthly)" pulled from
// investment.fd_compounding_frequency_master. Lookup is best-effort: a blank
// input or a missing row returns "" so callers can decide whether to fall back
// to the raw id.
func lookupFrequencyLabel(ctx context.Context, pool *pgxpool.Pool, id string) string {
	id = strings.TrimSpace(id)
	if id == "" || pool == nil {
		return ""
	}
	var code, name string
	err := pool.QueryRow(ctx, `
		SELECT COALESCE(frequency_code,''), COALESCE(frequency_name,'')
		FROM investment.fd_compounding_frequency_master
		WHERE (frequency_id::text = $1 OR frequency_code = $1 OR frequency_name = $1)
		  AND COALESCE(is_deleted, false) = false
		LIMIT 1`, id).Scan(&code, &name)
	if err != nil {
		return ""
	}
	code = strings.TrimSpace(code)
	name = strings.TrimSpace(name)
	switch {
	case code != "" && name != "":
		return fmt.Sprintf("%s (%s)", code, name)
	case name != "":
		return name
	case code != "":
		return code
	default:
		return ""
	}
}

// identityLabelFor enriches identity variance fields with human-readable labels.
func identityLabelFor(ctx context.Context, pool *pgxpool.Pool, fieldName, value string) string {
	switch fieldName {
	case "frequency_id", "payout_frequency_id", "accrual_frequency_code":
		if lbl := lookupFrequencyLabel(ctx, pool, value); lbl != "" {
			return lbl
		}
		return strings.TrimSpace(value)
	case "reset_type":
		return resetTypeDisplayLabel(value)
	default:
		return ""
	}
}

// serializeVarianceItem turns a varianceengine.VarianceItem into the JSON map
// the UI consumes. expected_value / actual_value remain the raw stable values
// (ids, codes, numbers) used everywhere else; sibling *_label fields carry the
// human readable rendering when one is available (currently only for
// frequency_id, which looks up code + name from the frequency master). includeAll
// mirrors the legacy behaviour for resolve-variance which returns clean rows
// too — pass false for endpoints that only return flagged rows.
func serializeVarianceItem(ctx context.Context, pool *pgxpool.Pool, v varianceengine.VarianceItem) map[string]interface{} {
	out := map[string]interface{}{
		"field_name":     v.FieldName,
		"variance_type":  v.VarianceType,
		"expected_value": v.ExpectedValue,
		"actual_value":   v.ActualValue,
		"variance_delta": v.VarianceDelta,
		"priority":       v.Priority,
		"has_variance":   v.HasVariance,
		"system_comment": v.SystemComment,
	}
	if expLabel := identityLabelFor(ctx, pool, v.FieldName, v.ExpectedValue); expLabel != "" {
		out["expected_value_label"] = expLabel
	}
	if actLabel := identityLabelFor(ctx, pool, v.FieldName, v.ActualValue); actLabel != "" {
		out["actual_value_label"] = actLabel
	}
	return out
}

func buildFDConfirmationVarianceRules(in fdConfirmationVarianceInput) []varianceengine.Rule {
	return []varianceengine.Rule{
		{FieldName: "interest_rate", VarianceType: varianceengine.TypeRate, ExpectedValue: formatNumber(in.BookedRate), ActualValue: formatNumber(in.ActualRate), Tolerance: rateTolerance, Priority: varianceengine.PriorityHigh},
		{FieldName: "principal_amount", VarianceType: varianceengine.TypeAmount, ExpectedValue: formatNumber(in.BookedPrincipal), ActualValue: formatNumber(in.ActualPrincipal), Tolerance: amountTolerance, Priority: varianceengine.PriorityHigh},
		{FieldName: "tenor_days", VarianceType: varianceengine.TypeDays, ExpectedValue: fmt.Sprintf("%d", in.BookedTenorDays), ActualValue: fmt.Sprintf("%d", in.ActualTenorDays), Priority: varianceengine.PriorityMedium},
		{FieldName: "tenor_months", VarianceType: varianceengine.TypeDays, ExpectedValue: fmt.Sprintf("%d", in.BookedTenorMonths), ActualValue: fmt.Sprintf("%d", in.ActualTenorMonths), Priority: varianceengine.PriorityMedium},
		{FieldName: "tenor_years", VarianceType: varianceengine.TypeDays, ExpectedValue: fmt.Sprintf("%d", in.BookedTenorYears), ActualValue: fmt.Sprintf("%d", in.ActualTenorYears), Priority: varianceengine.PriorityMedium},
		{FieldName: "value_date", VarianceType: varianceengine.TypeDate, ExpectedValue: in.BookedValueDate, ActualValue: in.ActualValueDate, Priority: varianceengine.PriorityMedium},
		{FieldName: "maturity_date", VarianceType: varianceengine.TypeDate, ExpectedValue: in.BookedMaturityDate, ActualValue: in.ActualMaturityDate, Priority: varianceengine.PriorityHigh},
		{FieldName: "interest_type_code", VarianceType: varianceengine.TypeIdentity, ExpectedValue: in.BookedInterestType, ActualValue: in.ActualInterestType, Priority: varianceengine.PriorityHigh},
		{FieldName: "frequency_id", VarianceType: varianceengine.TypeIdentity, ExpectedValue: in.BookedFrequencyID, ActualValue: in.ActualFrequencyID, Priority: varianceengine.PriorityMedium},
		{FieldName: "payout_frequency_id", VarianceType: varianceengine.TypeIdentity, ExpectedValue: in.BookedPayoutFrequencyID, ActualValue: in.ActualPayoutFrequencyID, Priority: varianceengine.PriorityMedium},
		{FieldName: "reset_type", VarianceType: varianceengine.TypeIdentity, ExpectedValue: normalizeResetType(in.BookedResetType), ActualValue: normalizeResetType(in.ActualResetType), Priority: varianceengine.PriorityMedium},
		{FieldName: "accrual_frequency_code", VarianceType: varianceengine.TypeIdentity, ExpectedValue: in.BookedAccrualFrequencyCode, ActualValue: in.ActualAccrualFrequencyCode, Priority: varianceengine.PriorityMedium},
		{FieldName: "tenor_type", VarianceType: varianceengine.TypeIdentity, ExpectedValue: in.BookedTenorType, ActualValue: in.ActualTenorType, Priority: varianceengine.PriorityLow},
		{FieldName: "first_capitalization_date", VarianceType: varianceengine.TypeDate, ExpectedValue: in.BookedFirstCapDate, ActualValue: in.ActualFirstCapDate, Priority: varianceengine.PriorityMedium},
		{FieldName: "first_payout_date", VarianceType: varianceengine.TypeDate, ExpectedValue: in.BookedFirstPayoutDate, ActualValue: in.ActualFirstPayoutDate, Priority: varianceengine.PriorityMedium},
	}
}

// runFDConfirmationVarianceCompare evaluates rules, persists OPEN rows, and auto-resolves cleared fields.
func runFDConfirmationVarianceCompare(ctx context.Context, pool *pgxpool.Pool, in fdConfirmationVarianceInput) ([]varianceengine.VarianceItem, bool) {
	if in.BookingID == "" || (in.BookedPrincipal <= 0 && in.BookedRate <= 0) {
		return nil, false
	}
	if in.RunID == "" {
		in.RunID = varianceengine.NewRunID()
	}
	items := varianceengine.Compare("FD_CONFIRMATION", in.BookingID, in.EntityID, in.RunID, buildFDConfirmationVarianceRules(in))
	hasVariance := false
	for _, v := range items {
		if v.HasVariance {
			hasVariance = true
			break
		}
	}
	if err := varianceengine.PersistVariances(ctx, pool, items); err != nil {
		api.LogError("[FDBooking] PersistVariances booking=%s: %v", in.BookingID, err)
	}
	if err := varianceengine.AutoResolveCleared(ctx, pool, in.BookingID, items, in.ResolvedBy, in.ResolvedByEmail); err != nil {
		api.LogError("[FDBooking] AutoResolveCleared booking=%s: %v", in.BookingID, err)
	}
	return items, hasVariance
}

func fetchOpenVarianceIDs(ctx context.Context, pool *pgxpool.Pool, bookingID, confirmationID string) ([]string, error) {
	rows, err := pool.Query(ctx, `
		SELECT variance_id FROM public.variance_log
		WHERE status = 'OPEN'
		  AND (
		    ($1 <> '' AND record_id = $1)
		    OR ($2 <> '' AND record_id = $2)
		  )
		ORDER BY field_name`,
		bookingID, confirmationID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if scanErr := rows.Scan(&id); scanErr == nil && id != "" {
			ids = append(ids, id)
		}
	}
	return ids, rows.Err()
}

func varianceTypeForField(field string) string {
	switch field {
	case "interest_rate":
		return varianceengine.TypeRate
	case "principal_amount":
		return varianceengine.TypeAmount
	case "tenor_days", "tenor_months", "tenor_years":
		return varianceengine.TypeDays
	case "value_date", "maturity_date", "first_capitalization_date", "first_payout_date":
		return varianceengine.TypeDate
	case "interest_type_code", "frequency_id", "payout_frequency_id", "accrual_frequency_code", "reset_type", "tenor_type":
		return varianceengine.TypeIdentity
	default:
		return varianceengine.TypeOther
	}
}

func backfillOpenVarianceLogFromDetails(ctx context.Context, pool *pgxpool.Pool, bookingID, entityID, confirmationID, detailsJSON string) ([]string, error) {
	detailsJSON = strings.TrimSpace(detailsJSON)
	if detailsJSON == "" || detailsJSON == "null" {
		return nil, nil
	}
	var entries []struct {
		Field    string  `json:"field"`
		Expected string  `json:"expected"`
		Actual   string  `json:"actual"`
		Delta    float64 `json:"delta"`
		Priority string  `json:"priority"`
	}
	if err := json.Unmarshal([]byte(detailsJSON), &entries); err != nil {
		return nil, err
	}
	recordID := bookingID
	if recordID == "" {
		recordID = confirmationID
	}
	if recordID == "" {
		return nil, nil
	}
	runID := varianceengine.NewRunID()
	items := make([]varianceengine.VarianceItem, 0, len(entries))
	for _, e := range entries {
		if strings.TrimSpace(e.Field) == "" {
			continue
		}
		pri := e.Priority
		if pri == "" {
			pri = varianceengine.PriorityMedium
		}
		items = append(items, varianceengine.VarianceItem{
			ModuleCode:    "FD_CONFIRMATION",
			RecordID:      recordID,
			EntityID:      entityID,
			RunID:         runID,
			FieldName:     e.Field,
			VarianceType:  varianceTypeForField(e.Field),
			ExpectedValue: e.Expected,
			ActualValue:   e.Actual,
			VarianceDelta: e.Delta,
			Priority:      pri,
			Status:        varianceengine.StatusOpen,
			HasVariance:   true,
		})
	}
	if len(items) == 0 {
		return nil, nil
	}
	if err := varianceengine.PersistVariances(ctx, pool, items); err != nil {
		return nil, err
	}
	return fetchOpenVarianceIDs(ctx, pool, bookingID, confirmationID)
}

// buildEditConfirmationFieldMap maps JSON field keys from the UI to fd_confirmation column names.
// Only columns present in the live schema are included (except core columns always assumed present).
func buildEditConfirmationFieldMap(confCols map[string]bool) map[string]string {
	// clientKey → dbColumn
	candidates := map[string]string{
		"actual_principal":             "actual_principal",
		"confirmed_principal_amount":   "actual_principal",
		"confirmed_rate":               "confirmed_rate",
		"confirmed_interest_rate":      "confirmed_rate",
		"actual_start_date":            "actual_start_date",
		"confirmed_value_date":         "actual_start_date",
		"actual_maturity_date":         "actual_maturity_date",
		"confirmed_maturity_date":      "actual_maturity_date",
		"bank_fd_ref_no":               "bank_fd_ref_no",
		"bank_fd_reference":            "bank_fd_ref_no",
		"confirmation_received_date":   "confirmation_received_date",
		"receipt_date":                 "confirmation_received_date",
		"confirmed_interest_type_code": "confirmed_interest_type_code",
		"confirmed_interest_type":      "confirmed_interest_type_code",
		"confirmed_frequency_id":       "confirmed_frequency_id",
		"tenor_type":                   "tenor_type",
		"confirmed_tenor_type":         "tenor_type",
		"tenor_days":                   "tenor_days",
		"confirmed_tenor_days":         "tenor_days",
		"tenor_months":                 "tenor_months",
		"confirmed_tenor_months":       "tenor_months",
		"tenor_years":                  "tenor_years",
		"confirmed_tenor_years":        "tenor_years",
		"payout_dates":                 "payout_dates",
		"compounding_dates":            "compounding_dates",
		"penalty_id":                   "penalty_id",
		"first_payout_date":            "first_payout_date",
		"first_capitalization_date":    "first_capitalization_date",
		"accrual_frequency_code":       "accrual_frequency_code",
		"reset_type":                   "reset_type",
		"payout_frequency_id":          "payout_frequency_id",
		"bank_reference_number":        "bank_reference_number",
		"premature_closure_terms":      "premature_closure_terms",
		"confirmation_notes":           "confirmation_notes",
		"notes":                        "confirmation_notes",
		"confirmation_mode":            "confirmation_mode",
	}
	out := make(map[string]string, len(candidates))
	for clientKey, dbCol := range candidates {
		if confCols[dbCol] {
			out[clientKey] = dbCol
			continue
		}
		// notes alias when only generic notes column exists
		if clientKey == "notes" && confCols["notes"] {
			out[clientKey] = "notes"
		}
	}
	return out
}

// fdConfirmationDetailExtraSelect returns optional confirmation columns for GET detail/list
// (empty string literals when the column is absent in the live schema).
func fdConfirmationDetailExtraSelect(confCols map[string]bool) string {
	textCol := func(col, alias string) string {
		if confCols[col] {
			return fmt.Sprintf("COALESCE(c.%s,'') AS %s", col, alias)
		}
		return fmt.Sprintf("'' AS %s", alias)
	}
	jsonCol := func(col, alias string) string {
		if confCols[col] {
			return fmt.Sprintf("COALESCE(c.%s::text,'') AS %s", col, alias)
		}
		return fmt.Sprintf("'' AS %s", alias)
	}
	parts := []string{
		textCol("penalty_id", "penalty_id"),
		textCol("premature_closure_terms", "premature_closure_terms"),
		textCol("confirmation_notes", "confirmation_notes"),
		textCol("accrual_frequency_code", "accrual_frequency_code"),
		textCol("accrual_frequency_code", "confirmed_accrual_frequency_code"),
		textCol("reset_type", "reset_type"),
		textCol("reset_type", "confirmed_reset_type"),
		textCol("payout_frequency_id", "payout_frequency_id"),
		jsonCol("payout_dates", "payout_dates"),
		jsonCol("compounding_dates", "compounding_dates"),
	}
	return strings.Join(parts, ",\n\t\t\t\t")
}

func editConfirmationCoerceFieldValue(clientKey, dbCol string, v interface{}) interface{} {
	switch dbCol {
	case "actual_start_date", "actual_maturity_date", "confirmation_received_date",
		"first_payout_date", "first_capitalization_date":
		return coerceDateValue(v)
	case "accrual_frequency_code", "reset_type":
		if sv, ok := v.(string); ok {
			return strings.ToUpper(strings.TrimSpace(sv))
		}
	case "bank_fd_ref_no", "bank_reference_number", "premature_closure_terms", "confirmation_notes", "notes":
		if sv, ok := v.(string); ok {
			return strings.TrimSpace(sv)
		}
	case "payout_dates", "compounding_dates":
		b, err := json.Marshal(v)
		if err != nil {
			return nil
		}
		if string(b) == "null" {
			return nil
		}
		return string(b)
	}
	if clientKey == "receipt_date" || clientKey == "confirmation_received_date" {
		return coerceDateValue(v)
	}
	return v
}

// ensureOpenVarianceLogForException returns OPEN variance_log IDs, backfilling from variance_details when needed.
func ensureOpenVarianceLogForException(ctx context.Context, pool *pgxpool.Pool, confirmationID, bookingID, entityID string) ([]string, bool, error) {
	ids, err := fetchOpenVarianceIDs(ctx, pool, bookingID, confirmationID)
	if err != nil {
		return nil, false, err
	}
	if len(ids) > 0 {
		return ids, true, nil
	}
	var varianceFlag bool
	var details *string
	err = pool.QueryRow(ctx, `
		SELECT COALESCE(variance_flag, false), variance_details
		FROM investment.fd_confirmation
		WHERE confirmation_id = $1 AND COALESCE(is_deleted, false) = false`,
		confirmationID).Scan(&varianceFlag, &details)
	if err != nil {
		return nil, false, err
	}
	if !varianceFlag {
		return nil, false, nil
	}
	detailsStr := ""
	if details != nil {
		detailsStr = *details
	}
	ids, err = backfillOpenVarianceLogFromDetails(ctx, pool, bookingID, entityID, confirmationID, detailsStr)
	if err != nil {
		return nil, true, err
	}
	return ids, true, nil
}
