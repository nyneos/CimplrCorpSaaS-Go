package fdMaster

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/approvalengine"
	"CimplrCorpSaas/api/constants"
	notifcatalog "CimplrCorpSaas/api/notification/catalog"
	"CimplrCorpSaas/api/policyengine/common"
	"CimplrCorpSaas/internal/ctxutil"
	"CimplrCorpSaas/internal/validation"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var tenureTypeBackfillOnce sync.Once

// dateLayoutISO is the Go reference layout for ISO-8601 (YYYY-MM-DD) dates.
const dateLayoutISO = "2006-01-02"

// deriveTenureType resolves DAYS | MONTHS | YEARS when tenure_type is blank.
func deriveTenureType(tenureType string, tenureYears, tenureMonths, tenureDays int) string {
	if v := strings.ToUpper(strings.TrimSpace(tenureType)); v != "" {
		return v
	}
	if tenureYears > 0 {
		return "YEARS"
	}
	if tenureMonths > 0 {
		return "MONTHS"
	}
	if tenureDays > 0 {
		return "DAYS"
	}
	return ""
}

func mapRowInt(row map[string]interface{}, keys ...string) int {
	for _, key := range keys {
		raw, ok := row[key]
		if !ok || raw == nil {
			continue
		}
		switch v := raw.(type) {
		case int:
			return v
		case int32:
			return int(v)
		case int64:
			return int(v)
		case float64:
			return int(v)
		case float32:
			return int(v)
		case string:
			if i, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
				return i
			}
		}
	}
	return 0
}

func enrichTenureFields(row map[string]interface{}) {
	if mapRowInt(row, "tenure_months") == 0 {
		if v := mapRowInt(row, "tenor_months"); v > 0 {
			row["tenure_months"] = v
		}
	}
	if mapRowInt(row, "tenure_days") == 0 {
		if v := mapRowInt(row, "tenor_days"); v > 0 {
			row["tenure_days"] = v
		}
	}
	if mapRowInt(row, "tenure_years") == 0 {
		if v := mapRowInt(row, "tenor_years"); v > 0 {
			row["tenure_years"] = v
		}
	}

	current := ""
	if v, ok := row["tenure_type"]; ok && v != nil {
		current = strings.TrimSpace(fmt.Sprintf("%v", v))
	}
	if current == "" {
		if v, ok := row["tenor_type"]; ok && v != nil {
			current = strings.TrimSpace(fmt.Sprintf("%v", v))
		}
	}
	derived := deriveTenureType(
		current,
		mapRowInt(row, "tenure_years", "tenor_years"),
		mapRowInt(row, "tenure_months", "tenor_months"),
		mapRowInt(row, "tenure_days", "tenor_days"),
	)
	if derived != "" {
		row["tenure_type"] = derived
	}
}

func mapRowString(row map[string]interface{}, keys ...string) string {
	for _, key := range keys {
		raw, ok := row[key]
		if !ok || raw == nil {
			continue
		}
		if s := strings.TrimSpace(fmt.Sprintf("%v", raw)); s != "" && s != "<nil>" {
			return s
		}
	}
	return ""
}

// enrichBankReferenceFields fills bank_reference_number from fd_confirmation when
// the master row does not already carry a value (older activations).
func enrichBankReferenceFields(ctx context.Context, pool *pgxpool.Pool, rows []map[string]interface{}) {
	if len(rows) == 0 {
		return
	}

	confCols, err := loadTableColumns(ctx, pool, "investment", "fd_confirmation")
	if err != nil || !confCols["bank_reference_number"] {
		return
	}

	confirmationIDs := make([]string, 0)
	seen := make(map[string]struct{})
	for _, row := range rows {
		if mapRowString(row, "bank_reference_number") != "" {
			continue
		}
		cid := mapRowString(row, "confirmation_id")
		if cid == "" {
			continue
		}
		if _, ok := seen[cid]; ok {
			continue
		}
		seen[cid] = struct{}{}
		confirmationIDs = append(confirmationIDs, cid)
	}
	if len(confirmationIDs) == 0 {
		return
	}

	lookup := make(map[string]string, len(confirmationIDs))
	qRows, qErr := pool.Query(ctx, `
		SELECT
			confirmation_id::text,
			COALESCE(NULLIF(BTRIM(bank_reference_number::text), ''), bank_fd_ref_no, '') AS bank_reference_number
		FROM investment.fd_confirmation
		WHERE confirmation_id = ANY($1::text[])
		  AND COALESCE(is_deleted, false) = false`, confirmationIDs)
	if qErr != nil {
		return
	}
	defer qRows.Close()
	for qRows.Next() {
		var cid, ref string
		if scanErr := qRows.Scan(&cid, &ref); scanErr != nil {
			continue
		}
		lookup[strings.TrimSpace(cid)] = strings.TrimSpace(ref)
	}

	for _, row := range rows {
		if mapRowString(row, "bank_reference_number") != "" {
			continue
		}
		cid := mapRowString(row, "confirmation_id")
		if ref, ok := lookup[cid]; ok && ref != "" {
			row["bank_reference_number"] = ref
		}
	}
}

func backfillEmptyTenureTypes(ctx context.Context, pool *pgxpool.Pool) {
	tenureTypeBackfillOnce.Do(func() {
		_, _ = pool.Exec(ctx, `
			UPDATE investment.fd_master
			SET tenure_type = CASE
				WHEN COALESCE(tenure_years, 0) > 0 THEN 'YEARS'
				WHEN COALESCE(tenure_months, 0) > 0 THEN 'MONTHS'
				WHEN COALESCE(tenure_days, 0) > 0 THEN 'DAYS'
				ELSE tenure_type
			END
			WHERE COALESCE(tenure_type, '') = ''
			  AND (
				COALESCE(tenure_years, 0) > 0
				OR COALESCE(tenure_months, 0) > 0
				OR COALESCE(tenure_days, 0) > 0
			  )`)
	})
}

type queryExecutor interface {
	QueryRow(ctx context.Context, sql string, arguments ...interface{}) pgx.Row
	Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, arguments ...interface{}) (pgx.Rows, error)
}

// InsertFDAuditParams groups parameters for insertFDAuditRecord to avoid long parameter lists.
type InsertFDAuditParams struct {
	AuditTable       string
	RefID            string
	UserEmail        string
	ActionType       string
	ProcessingStatus string
	Reason           string
}

// cashflowAuditStatusPending must match investment.fd_audit_cashflow_schedule
// check constraint fd_audit_cashflow_status_chk (not fd_master PENDING_ACTIVATION).
const cashflowAuditStatusPending = constants.StatusPendingApproval

type activateFDRequest struct {
	UserID         string `json:"user_id"`
	ConfirmationID string `json:"confirmation_id"`
	// Deprecated: legacy clients sent bank ref here; use confirmation bank_fd_ref_no instead.
	FDNumber    string `json:"fd_number"`
	ReceiptDate string `json:"receipt_date"`
	// Ops activation comment (preferred over fd_number in UI).
	Reason string `json:"reason"`
	Notes  string `json:"notes"`
}

func requireFDMasterScope(ctx context.Context, pgxPool *pgxpool.Pool, fdID string) (string, bool, error) {
	var entityID string
	err := pgxPool.QueryRow(ctx, `
		SELECT COALESCE(entity_id,'')
		FROM investment.fd_master
		WHERE fd_id=$1 AND COALESCE(is_deleted,false)=false`, fdID).Scan(&entityID)
	if err != nil {
		if err == pgx.ErrNoRows {
			return "", false, nil
		}
		return "", false, err
	}
	if !ctxutil.FromContext(ctx).HasEntityAccess(entityID) {
		return entityID, false, nil
	}
	return entityID, true, nil
}

type bulkFDActionRequest struct {
	UserID  string   `json:"user_id"`
	FDIDs   []string `json:"fd_ids"`
	FDID    string   `json:"fd_id"` // single-ID convenience alias
	Comment string   `json:"comment"`
}

// normalize ensures a single fd_id is merged into FDIDs so callers can pass either.
func (r *bulkFDActionRequest) normalize() {
	if r.FDID != "" {
		for _, id := range r.FDIDs {
			if id == r.FDID {
				return
			}
		}
		r.FDIDs = append(r.FDIDs, r.FDID)
	}
}

func getUserEmail(ctx context.Context) string {
	if s := api.GetSessionFromCtx(ctx); s != nil {
		return s.Email
	}
	return ""
}

func parseDateOrDefault(dateString string, fallback time.Time) time.Time {
	trimmed := strings.TrimSpace(dateString)
	if trimmed == "" {
		return fallback
	}
	for _, layout := range []string{constants.DateFormat, constants.DateTimeFormat, time.RFC3339} {
		if parsed, err := time.Parse(layout, trimmed); err == nil {
			return parsed
		}
	}
	return fallback
}

func splitQualifiedTable(name string) (string, string) {
	parts := strings.SplitN(name, ".", 2)
	if len(parts) != 2 {
		return "public", name
	}
	return parts[0], parts[1]
}

func loadTableColumns(ctx context.Context, exec queryExecutor, schemaName string, tableName string) (map[string]bool, error) {
	rows, err := exec.Query(ctx, `
		SELECT column_name
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2
	`, schemaName, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols := map[string]bool{}
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, err
		}
		cols[col] = true
	}
	return cols, rows.Err()
}

func resolveFirstExistingTable(ctx context.Context, exec queryExecutor, candidates []string) string {
	for _, candidate := range candidates {
		schemaName, tableName := splitQualifiedTable(candidate)
		var exists bool
		err := exec.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM information_schema.tables
				WHERE table_schema = $1 AND table_name = $2
			)
		`, schemaName, tableName).Scan(&exists)
		if err == nil && exists {
			return candidate
		}
	}
	return ""
}

func pickFirstExistingColumn(columns map[string]bool, candidates ...string) string {
	for _, candidate := range candidates {
		if columns[candidate] {
			return candidate
		}
	}
	return ""
}

func buildPlaceholders(count int, start int) string {
	parts := make([]string, count)
	for index := 0; index < count; index++ {
		parts[index] = fmt.Sprintf("$%d", start+index)
	}
	return strings.Join(parts, ",")
}

func buildDynamicInsert(table string, columns map[string]bool, preferred []string, valueMap map[string]interface{}, returningCandidates []string) (string, []interface{}, string, bool) {
	insertCols := make([]string, 0, len(preferred))
	args := make([]interface{}, 0, len(preferred))
	for _, col := range preferred {
		if !columns[col] {
			continue
		}
		value, ok := valueMap[col]
		if !ok {
			continue
		}
		insertCols = append(insertCols, col)
		args = append(args, value)
	}
	if len(insertCols) == 0 {
		return "", nil, "", false
	}
	returningCol := ""
	for _, col := range returningCandidates {
		if columns[col] {
			returningCol = col
			break
		}
	}

	insertSQL := fmt.Sprintf(
		constants.ErrInsertFailed,
		table,
		strings.Join(insertCols, ","),
		buildPlaceholders(len(insertCols), 1),
	)
	if returningCol != "" {
		insertSQL += " RETURNING " + returningCol
	}
	return insertSQL, args, returningCol, true
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func rowsToMaps(rows pgx.Rows) ([]map[string]interface{}, error) {
	fields := rows.FieldDescriptions()
	out := make([]map[string]interface{}, 0)
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, err
		}
		rowMap := make(map[string]interface{}, len(fields))
		for index, field := range fields {
			if values[index] == nil {
				rowMap[string(field.Name)] = ""
			} else {
				rowMap[string(field.Name)] = values[index]
			}
		}
		out = append(out, rowMap)
	}
	return out, rows.Err()
}

func getFDMasterError(err error, contextMessage string) (string, int) {
	if err == nil {
		return "", http.StatusOK
	}
	errStr := strings.ToLower(err.Error())
	if strings.Contains(errStr, "fd_master_confirmation_fk") {
		return "Confirmation record not found. Cannot activate FD.", http.StatusOK
	}
	if strings.Contains(errStr, "fd_master_receipt_date_after_value") {
		return "Receipt date must be on or after the value date.", http.StatusOK
	}
	if strings.Contains(errStr, "fd_master_status_chk") {
		return "Invalid FD master status.", http.StatusOK
	}
	if strings.Contains(errStr, "fd_audit_cashflow_status_chk") {
		return "Invalid cashflow audit status. Contact support if activation was just deployed.", http.StatusInternalServerError
	}
	if strings.Contains(errStr, "fd_master_fdno_not_empty") {
		return "FD number / certificate number is required.", http.StatusOK
	}
	if strings.Contains(errStr, "duplicate key") || strings.Contains(errStr, "unique constraint") {
		return "Duplicate entry detected. Please check for existing records.", http.StatusConflict
	}
	if strings.Contains(errStr, "25p02") || strings.Contains(errStr, "current transaction is aborted") {
		return contextMessage + ": a previous database step in this activation failed. Please retry; if it persists, check server logs for the first SQL error.", http.StatusInternalServerError
	}
	if strings.Contains(errStr, "no unique or exclusion constraint") && strings.Contains(errStr, "on conflict") {
		return "Cashflow audit setup failed (database constraint configuration). Please contact support.", http.StatusInternalServerError
	}
	if strings.Contains(errStr, "invalid") || strings.Contains(errStr, "required") {
		return err.Error(), http.StatusBadRequest
	}
	return contextMessage + ": " + err.Error(), http.StatusInternalServerError
}

func resolveFDAuditTable(ctx context.Context, exec queryExecutor) string {
	return resolveFirstExistingTable(ctx, exec, []string{
		constants.QuerryAuditMaster,
		"investment.fd_audit_fd_master",
		"investment.fd_master_audit",
	})
}

func insertFDAuditRecord(ctx context.Context, exec queryExecutor, p InsertFDAuditParams) error {
	if p.AuditTable == "" || p.RefID == "" {
		return nil
	}

	schemaName, tableName := splitQualifiedTable(p.AuditTable)
	cols, err := loadTableColumns(ctx, exec, schemaName, tableName)
	if err != nil {
		return err
	}
	refCol := pickFirstExistingColumn(cols, "fd_id", "master_id", "confirmation_id")
	if refCol == "" {
		return nil
	}

	valueMap := map[string]interface{}{
		refCol:              p.RefID,
		"action_type":       p.ActionType,
		"processing_status": p.ProcessingStatus,
		"requested_by":      api.SystemIfBlank(p.UserEmail),
		"requested_at":      time.Now(),
		"requested_ip":      api.SystemIfBlank(api.ClientIPFromContext(ctx)),
		"reason":            p.Reason,
	}
	preferred := []string{refCol, "action_type", "processing_status", "reason", "requested_by", "requested_at", "requested_ip"}
	insertSQL, args, _, ok := buildDynamicInsert(p.AuditTable, cols, preferred, valueMap, nil)
	if !ok {
		return nil
	}
	_, err = exec.Exec(ctx, insertSQL, args...)
	return err
}

func updateFDAuditStatus(ctx context.Context, exec queryExecutor, auditTable string, fdIDs []string, userEmail string, status string, comment string) error {
	if auditTable == "" || len(fdIDs) == 0 {
		return nil
	}

	schemaName, tableName := splitQualifiedTable(auditTable)
	cols, err := loadTableColumns(ctx, exec, schemaName, tableName)
	if err != nil {
		return err
	}
	refCol := pickFirstExistingColumn(cols, "fd_id", "master_id", "confirmation_id")
	if refCol == "" {
		return nil
	}

	setParts := []string{"processing_status = $1", "checker_by = $2", "checker_at = now()", "checker_comment = $3"}
	args := []interface{}{status, api.SystemIfBlank(userEmail), comment}
	if cols["checker_ip"] {
		args = append(args, api.SystemIfBlank(api.ClientIPFromContext(ctx)))
		setParts = append(setParts, fmt.Sprintf("checker_ip = $%d", len(args)))
	}
	args = append(args, fdIDs)
	query := fmt.Sprintf(
		"UPDATE %s SET %s WHERE %s = ANY($%d::text[]) AND processing_status LIKE '%%PENDING%%'",
		auditTable, strings.Join(setParts, ", "), refCol, len(args),
	)
	_, err = exec.Exec(ctx, query, args...)
	return err
}

func lookupEntityName(ctx context.Context, exec queryExecutor, entityID string) string {
	if entityID == "" {
		return ""
	}
	candidates := []string{
		"SELECT entity_name FROM masterentitycash WHERE entity_id = $1 LIMIT 1",
		"SELECT entity_name FROM masterentity WHERE entity_id = $1 LIMIT 1",
		"SELECT entity_short_name FROM masterentitycash WHERE entity_id = $1 LIMIT 1",
	}
	for _, q := range candidates {
		var name string
		if err := exec.QueryRow(ctx, q, entityID).Scan(&name); err == nil && name != "" {
			return name
		}
	}
	return entityID // fallback: use entityID as display name rather than send NULL
}

func lookupBankName(ctx context.Context, exec queryExecutor, bankID string) string {
	if bankID == "" {
		return ""
	}
	candidates := []string{
		"SELECT bank_name FROM masterbank WHERE bank_id = $1 LIMIT 1",
		"SELECT bank_name FROM master_bank WHERE bank_id = $1 LIMIT 1",
		"SELECT bank_name FROM investment.fd_bank_config_master WHERE bank_id = $1 LIMIT 1",
	}
	for _, q := range candidates {
		var name string
		if err := exec.QueryRow(ctx, q, bankID).Scan(&name); err == nil && name != "" {
			return name
		}
	}
	return bankID // fallback: use bankID rather than send NULL
}

func insertFDMaster(ctx context.Context, exec queryExecutor, rec *FDRecord, fdNumber string, receiptDate time.Time, notes string, createdBy string) (string, string, error) {
	masterCols, err := loadTableColumns(ctx, exec, "investment", "fd_master")
	if err != nil {
		return "", "", err
	}

	refCol := pickFirstExistingColumn(masterCols, "fd_id", "master_id", "confirmation_id")

	// fd_status CHECK constraint: PENDING_ACTIVATION | ACTIVE | MATURED | PREMATURELY_CLOSED | ROLLED_OVER | CANCELLED
	statusValue := "PENDING_ACTIVATION"

	fdRef := firstNonEmpty(fdNumber, rec.BankFDReference)
	if fdRef == "" {
		fdRef = firstNonEmpty(rec.ConfirmationID, "REF-PENDING")
	}

	// Resolve NOT NULL name fields that must be populated.
	entityName := rec.EntityName
	if entityName == "" {
		entityName = lookupEntityName(ctx, exec, rec.EntityID)
	}
	bankName := rec.BankName
	if bankName == "" {
		bankName = lookupBankName(ctx, exec, rec.BankID)
	}
	if createdBy == "" {
		createdBy = "system"
	}

	interestTypeCode := firstNonEmpty(rec.InterestTypeCode, "SIMPLE")
	effectiveReceiptDate := receiptDate
	if effectiveReceiptDate.IsZero() {
		effectiveReceiptDate = rec.ReceiptDate
	}
	termsSnapshot := map[string]interface{}{
		"confirmation_id":             rec.ConfirmationID,
		"booking_id":                  rec.BookingID,
		"interest_type_code":          interestTypeCode,
		"frequency_id":                rec.FrequencyID,
		"interest_payout_frequency":   rec.InterestPayoutFrequency,
		"payout_frequency_id":         rec.InterestPayoutFrequency,
		"accrual_frequency_code":      rec.AccrualFrequencyCode,
		"reset_type":                  rec.ResetType,
		"day_count_code":              rec.DayCountConvention,
		"tds_plan_id":                 rec.TDSPlanID,
		"bank_config_id":              rec.BankConfigID,
		"first_payout_date":           nilIfZero(rec.FirstPayoutDate),
		"first_capitalization_date":   nilIfZero(rec.FirstCapitalizationDate),
		"bank_fd_ref_no":              fdRef,
		"bank_reference_number":       rec.BankReferenceNumber,
		"premature_closure_terms_set": rec.PrematureClosureTerms != "",
	}
	termsSnapshotJSON, _ := json.Marshal(termsSnapshot)

	valueMap := map[string]interface{}{
		// identity
		"confirmation_id": rec.ConfirmationID,
		"booking_id":      rec.BookingID,
		// entity
		"entity_id":   rec.EntityID,
		"entity_name": entityName,
		// bank
		"bank_id":   rec.BankID,
		"bank_name": bankName,
		// account — actual column is source_account_id
		"source_account_id": rec.BankAccountID,
		"bank_account_id":   rec.BankAccountID, // fallback if schema uses old name
		// financials
		"principal_amount":   rec.PrincipalAmount,
		"interest_rate":      rec.InterestRate,
		"interest_type_code": interestTypeCode,
		// dates — actual column is start_date
		"start_date":    rec.ValueDate,
		"value_date":    rec.ValueDate, // fallback
		"maturity_date": rec.MaturityDate,
		// tenor — actual columns: tenure_days, tenure_months, tenure_type, tenure_years
		"tenure_days":   rec.TenorDays,
		"tenor_days":    rec.TenorDays, // fallback old name
		"tenure_months": rec.TenorMonths,
		"tenor_months":  rec.TenorMonths, // fallback old name
		"tenure_type":   deriveTenureType(rec.TenureType, rec.TenureYears, rec.TenorMonths, rec.TenorDays),
		"tenor_type":    deriveTenureType(rec.TenureType, rec.TenureYears, rec.TenorMonths, rec.TenorDays), // fallback old name
		"tenure_years":  rec.TenureYears,
		"tenor_years":   rec.TenureYears, // fallback old name
		// optional
		"frequency_id":              rec.FrequencyID,
		"interest_payout_frequency": rec.InterestPayoutFrequency,
		"payout_frequency_id":       rec.InterestPayoutFrequency,
		"accrual_frequency_code":    strings.ToUpper(strings.TrimSpace(rec.AccrualFrequencyCode)),
		"reset_type":                firstNonEmpty(strings.ToUpper(strings.TrimSpace(rec.ResetType)), "AT_MATURITY"),
		"first_payout_date":         nilIfZero(rec.FirstPayoutDate),
		"first_capitalization_date": nilIfZero(rec.FirstCapitalizationDate),
		"bank_config_id":            rec.BankConfigID,
		"tds_plan_id":               rec.TDSPlanID,
		"day_count_code":            rec.DayCountConvention,
		"receipt_date":              nilIfZero(effectiveReceiptDate),
		"bank_reference_number":     nilIfEmpty(rec.BankReferenceNumber),
		"premature_closure_terms":   nilIfEmpty(rec.PrematureClosureTerms),
		"cashflow_engine_version":   "2.0",
		"cashflow_terms_snapshot":   string(termsSnapshotJSON),
		// fd reference — NOT NULL in schema
		"bank_fd_ref_no":     fdRef,
		"bank_fd_reference":  fdRef,
		"fd_number":          fdRef,
		"fd_no":              fdRef,
		"certificate_number": fdRef,
		// status
		"status":    statusValue,
		"fd_status": statusValue,
		// audit
		"created_by":   createdBy,
		"notes":        notes,
		"is_deleted":   false,
		"auto_renewal": rec.AutoRenewal,
	}
	preferred := []string{
		"confirmation_id", "booking_id",
		"entity_id", "entity_name",
		"bank_id", "bank_name",
		"source_account_id", "bank_account_id",
		"principal_amount", "interest_rate", "interest_type_code",
		"start_date", "value_date",
		"maturity_date",
		"tenure_days", "tenor_days",
		"tenure_months", "tenor_months",
		"tenure_type", "tenor_type",
		"tenure_years", "tenor_years",
		"frequency_id", "interest_payout_frequency", "payout_frequency_id",
		"accrual_frequency_code", "reset_type", "first_payout_date", "first_capitalization_date",
		"bank_config_id", "tds_plan_id", "day_count_code", "receipt_date",
		"bank_reference_number", "premature_closure_terms", "cashflow_engine_version", "cashflow_terms_snapshot",
		"bank_fd_ref_no", "fd_number", "fd_no", "certificate_number", "bank_fd_reference",
		"status", "fd_status",
		"created_by", "notes", "is_deleted", "auto_renewal",
	}
	insertSQL, args, returningCol, ok := buildDynamicInsert(constants.QuerryMaster, masterCols, preferred, valueMap, []string{"fd_id", "master_id", "confirmation_id"})
	if !ok {
		return "", "", fmt.Errorf("unable to build fd_master insert")
	}
	if returningCol == "" {
		return "", refCol, fmt.Errorf("fd_master return column not found")
	}

	var fdID string
	if err := exec.QueryRow(ctx, insertSQL, args...).Scan(&fdID); err != nil {
		return "", refCol, err
	}
	return fdID, firstNonEmpty(refCol, returningCol), nil
}

// updateFDMasterStatus sets fd_status for the given fd_ids.
// When targetStatus is constants.StatusActive, it also stamps activated_by and activated_at
// if those columns exist on the table.
func updateFDMasterStatus(ctx context.Context, exec queryExecutor, fdIDs []string, targetStatus string) error {
	return updateFDMasterStatusBy(ctx, exec, fdIDs, targetStatus, "")
}

// updateFDMasterStatusBy is the full variant that accepts the acting user email
// so that activated_by / activated_at can be stamped when status → ACTIVE.
func updateFDMasterStatusBy(ctx context.Context, exec queryExecutor, fdIDs []string, targetStatus string, actorEmail string) error {
	if len(fdIDs) == 0 {
		return nil
	}

	masterCols, err := loadTableColumns(ctx, exec, "investment", "fd_master")
	if err != nil {
		return err
	}
	keyCol := pickFirstExistingColumn(masterCols, "fd_id", "master_id", "confirmation_id")
	statusCol := pickFirstExistingColumn(masterCols, "status", "fd_status")
	if keyCol == "" || statusCol == "" {
		return nil
	}

	// When activating, also stamp activated_by / activated_at if the columns exist.
	if targetStatus == constants.StatusActive && actorEmail != "" &&
		masterCols["activated_by"] && masterCols["activated_at"] {
		query := fmt.Sprintf(
			`UPDATE investment.fd_master
			 SET %s = $1, activated_by = $2, activated_at = now()
			 WHERE %s = ANY($3::text[]) AND COALESCE(is_deleted,false)=false`,
			statusCol, keyCol)
		_, err = exec.Exec(ctx, query, targetStatus, actorEmail, fdIDs)
		return err
	}

	query := fmt.Sprintf("UPDATE investment.fd_master SET %s = $1 WHERE %s = ANY($2::text[]) AND COALESCE(is_deleted,false)=false", statusCol, keyCol)
	_, err = exec.Exec(ctx, query, targetStatus, fdIDs)
	return err
}

func cashflowAuditScheduleTableExists(ctx context.Context, exec queryExecutor) bool {
	var exists bool
	_ = exec.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema = 'investment' AND table_name = 'fd_audit_cashflow_schedule'
		)`).Scan(&exists)
	return exists
}

func updateFDMasterCashflowGenerated(ctx context.Context, exec queryExecutor, fdID string) error {
	masterCols, err := loadTableColumns(ctx, exec, "investment", "fd_master")
	if err != nil {
		return err
	}
	if !masterCols["cashflow_generated"] {
		return nil
	}
	setParts := []string{"cashflow_generated = true"}
	if masterCols["cashflow_generated_at"] {
		setParts = append(setParts, "cashflow_generated_at = now()")
	}
	keyCol := pickFirstExistingColumn(masterCols, "fd_id", "master_id")
	if keyCol == "" {
		return fmt.Errorf("fd_master key column not found")
	}
	q := fmt.Sprintf("UPDATE investment.fd_master SET %s WHERE %s = $1", strings.Join(setParts, ", "), keyCol)
	_, err = exec.Exec(ctx, q, fdID)
	return err
}

// insertCashflowAuditRowsPending writes one pending-approval audit entry per cashflow row
// into fd_audit_cashflow_schedule. Called at FD activation to seed the audit trail.
// Status is promoted to APPROVED later via markCashflowAuditApproved when the checker
// approves the FD activation request (see BulkApproveActivation).
func insertCashflowAuditRowsPending(ctx context.Context, exec queryExecutor, fdID string, rows []CashflowRow, createdBy string) error {
	if !cashflowAuditScheduleTableExists(ctx, exec) {
		return nil
	}
	table := resolveFirstExistingTable(ctx, exec, []string{
		constants.QuerryCashflowSchedule,
		constants.QuerryCashflow,
		constants.QuerryMasterCashflowSchedule,
	})
	if table == "" {
		return nil
	}
	schemaName, tableName := splitQualifiedTable(table)
	cols, err := loadTableColumns(ctx, exec, schemaName, tableName)
	if err != nil {
		return err
	}
	fdCol := pickFirstExistingColumn(cols, "fd_id", "master_id")
	cfIDCol := pickFirstExistingColumn(cols, "cashflow_id", "id")
	seqCol := pickFirstExistingColumn(cols, "sequence_number", "period_number")
	evtCol := pickFirstExistingColumn(cols, "event_type")
	if fdCol == "" || cfIDCol == "" || evtCol == "" {
		return nil
	}

	// Fetch all cashflow_ids for this FD ordered by sequence so we can match rows.
	orderCol := seqCol
	if orderCol == "" {
		orderCol = cfIDCol
	}
	cfRows, qErr := exec.Query(ctx,
		fmt.Sprintf(`SELECT %s, %s FROM %s WHERE %s = $1 ORDER BY %s`, cfIDCol, evtCol, table, fdCol, orderCol),
		fdID)
	if qErr != nil {
		return qErr
	}
	type cfEntry struct {
		id        string
		eventType string
	}
	var cfList []cfEntry
	for cfRows.Next() {
		var e cfEntry
		if sErr := cfRows.Scan(&e.id, &e.eventType); sErr != nil {
			cfRows.Close()
			return sErr
		}
		cfList = append(cfList, e)
	}
	cfRows.Close()
	if cfRows.Err() != nil {
		return cfRows.Err()
	}

	reason := "System generated at FD activation"
	for _, cf := range cfList {
		var already bool
		if err := exec.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM investment.fd_audit_cashflow_schedule
				WHERE cashflow_id = $1::text
				  AND fd_id = $2::text
				  AND action_type = 'CREATE'
				  AND processing_status = $3::text
			)`,
			cf.id, fdID, cashflowAuditStatusPending,
		).Scan(&already); err != nil {
			return fmt.Errorf("cashflow audit lookup for %s: %w", cf.id, err)
		}
		if already {
			continue
		}
		_, insErr := exec.Exec(ctx, `
			INSERT INTO investment.fd_audit_cashflow_schedule (
				cashflow_id, fd_id,
				action_type, processing_status,
				reason,
				requested_by, requested_at, requested_ip
			) VALUES (
				$1::text, $2::text,
				'CREATE', $5::text,
				$3::text,
				$4::text, now(), $6::text
			)`,
			cf.id, fdID, reason, api.SystemIfBlank(createdBy), cashflowAuditStatusPending, api.SystemIfBlank(api.ClientIPFromContext(ctx)),
		)
		if insErr != nil {
			return fmt.Errorf("cashflow audit insert for %s: %w", cf.id, insErr)
		}
	}
	return nil
}

// markCashflowAuditApproved promotes activation-seeded cashflow audit rows for an FD
// to APPROVED, recording the checker email. Called from BulkApproveActivation inside the
// same transaction as fd_status → ACTIVE so the promotion is atomic.
func markCashflowAuditApproved(ctx context.Context, exec queryExecutor, fdID, checkerEmail string) {
	_, _ = exec.Exec(ctx, `
		UPDATE investment.fd_audit_cashflow_schedule
		SET processing_status = 'APPROVED',
		    checker_by = $2,
		    checker_at = now(),
		    checker_comment = 'Auto-approved on FD activation approval',
		    checker_ip = $4
		WHERE fd_id = $1
		  AND action_type = 'CREATE'
		  AND processing_status = $3`,
		fdID, api.SystemIfBlank(checkerEmail), cashflowAuditStatusPending, api.SystemIfBlank(api.ClientIPFromContext(ctx)),
	)
}

// softDeleteFDCashflowForRejection performs the three-step cashflow cleanup when an FD
// activation is rejected, inside the caller's transaction:
//  1. Soft-delete cashflow rows (is_deleted=true, is_active=false) so the accrual engine
//     cannot pick them up.
//  2. Mark cashflow audit rows REJECTED so they no longer appear as PENDING_ACTIVATION.
//  3. Clear cashflow_generated=false on fd_master so the generation flag stays consistent.
func softDeleteFDCashflowForRejection(ctx context.Context, exec queryExecutor, fdID, userEmail, comment string) {
	cfTable := resolveFirstExistingTable(ctx, exec, []string{
		constants.QuerryCashflowSchedule,
		constants.QuerryCashflow,
		constants.QuerryMasterCashflowSchedule,
	})
	if cfTable != "" {
		schemaName, tableName := splitQualifiedTable(cfTable)
		cfCols, err := loadTableColumns(ctx, exec, schemaName, tableName)
		if err == nil {
			fdCol := pickFirstExistingColumn(cfCols, "fd_id", "master_id")
			if fdCol != "" {
				setParts := []string{"is_deleted = true"}
				if cfCols["is_active"] {
					setParts = append(setParts, "is_active = false")
				}
				if cfCols["updated_at"] {
					setParts = append(setParts, "updated_at = now()")
				}
				args := []interface{}{fdID}
				if cfCols["updated_by"] {
					args = append(args, userEmail)
					setParts = append(setParts, fmt.Sprintf("updated_by = $%d", len(args)))
				}
				q := fmt.Sprintf("UPDATE %s SET %s WHERE %s = $1", cfTable, strings.Join(setParts, ", "), fdCol)
				_, _ = exec.Exec(ctx, q, args...)
			}
		}
	}
	// Mark activation-seeded cashflow audit rows as REJECTED.
	_, _ = exec.Exec(ctx, `
		UPDATE investment.fd_audit_cashflow_schedule
		SET processing_status = 'REJECTED', checker_by = $2, checker_at = now(), checker_comment = $3, checker_ip = $5
		WHERE fd_id = $1
		  AND action_type = 'CREATE'
		  AND processing_status = $4`,
		fdID, api.SystemIfBlank(userEmail), comment, cashflowAuditStatusPending, api.SystemIfBlank(api.ClientIPFromContext(ctx)),
	)
	// Clear cashflow_generated flag on fd_master.
	_, _ = exec.Exec(ctx, `
		UPDATE investment.fd_master
		SET cashflow_generated = false, cashflow_generated_at = NULL
		WHERE fd_id = $1`,
		fdID,
	)
}

func ActivateFD(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req activateFDRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if strings.TrimSpace(req.ConfirmationID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrConfirmationIDsRequired)
			return
		}

		userEmail := getUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			msg, status := getFDMasterError(err, constants.ErrTransactionFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx) //nolint:errcheck

		rec, err := loadFDRecord(ctx, tx, req.ConfirmationID)
		if err != nil {
			msg, status := getFDMasterError(err, "Load confirmation failed")
			api.RespondWithError(w, status, msg)
			return
		}

		scope := ctxutil.FromContext(r.Context())
		if !scope.HasEntityAccess(rec.EntityID) {
			api.RespondWithError(w, http.StatusForbidden,
				fmt.Sprintf(constants.ErrEntityIDNotAuthorized, rec.EntityID))
			return
		}
		if errMsg := validation.ValidateFDMasterReferences(r.Context(), map[string]interface{}{
			"bank_id":         rec.BankID,
			"bank_account_id": rec.BankAccountID,
			"interest_type":   rec.InterestTypeCode,
			"frequency_id":    rec.FrequencyID,
			"day_count_code":  rec.DayCountConvention,
			"tds_plan_id":     rec.TDSPlanID,
			"bank_config_id":  rec.BankConfigID,
		}); errMsg != "" {
			api.RespondWithError(w, http.StatusBadRequest, errMsg)
			return
		}

		receiptDate := parseDateOrDefault(req.ReceiptDate, rec.ReceiptDate)

		// Guard 1: Double-submit — reject if this confirmation was already activated.
		// Also reject if the EXISTS query itself fails (pooler error) — we cannot
		// safely determine uniqueness, so block rather than risk a duplicate.
		var alreadyExists bool
		guardErr := tx.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM investment.fd_master WHERE confirmation_id = $1)`,
			req.ConfirmationID,
		).Scan(&alreadyExists)
		if alreadyExists {
			api.RespondWithError(w, http.StatusConflict,
				fmt.Sprintf("FD already activated for confirmation: %s", req.ConfirmationID))
			return
		}
		if guardErr != nil {
			api.RespondWithError(w, http.StatusInternalServerError,
				fmt.Sprintf("Could not verify activation status for %s: %v", req.ConfirmationID, guardErr))
			return
		}

		// Guard 2: Bank-reference uniqueness — check (entity_id, bank_fd_ref_no) before INSERT.
		// The unique constraint uniq_fd_bank_ref_entity fires when two confirmations share
		// the same bank reference number for the same entity. Catch it here with a clear message.
		bankRef := firstNonEmpty(rec.BankFDReference, req.FDNumber)
		if bankRef != "" && rec.EntityID != "" {
			var refExists bool
			if refErr := tx.QueryRow(ctx,
				`SELECT EXISTS(
					SELECT 1 FROM investment.fd_master
					WHERE entity_id = $1
					  AND bank_fd_ref_no = $2
					  AND COALESCE(is_deleted,false) = false
				)`,
				rec.EntityID, bankRef,
			).Scan(&refExists); refErr == nil && refExists {
				api.RespondWithError(w, http.StatusConflict,
					fmt.Sprintf("Bank reference number %q already exists for this entity. Please use a unique reference.", bankRef))
				return
			}
		}

		if !fdEnforce(ctx, w, r, pgxPool, enforceCtx{EventCode: common.TriggerPreCreate, HandlerName: "ActivateFD", APIPath: "/investment/fd/master/activate",
			SubModule: fdSubMaster, EntityCode: rec.EntityID, Actor: userEmail}, buildFDMasterPolicyFields(fdMasterRow{
			BookingID:               rec.BookingID,
			ConfirmationID:          rec.ConfirmationID,
			EntityID:                rec.EntityID,
			EntityName:              rec.EntityName,
			BankID:                  rec.BankID,
			BankName:                rec.BankName,
			BankFDRefNo:             bankRef,
			BankReferenceNumber:     rec.BankReferenceNumber,
			PrincipalAmount:         rec.PrincipalAmount,
			InterestTypeCode:        rec.InterestTypeCode,
			InterestRate:            rec.InterestRate,
			TenureDays:              rec.TenorDays,
			TenureMonths:            rec.TenorMonths,
			TenureYears:             rec.TenureYears,
			TenureType:              rec.TenureType,
			StartDate:               rec.ValueDate.Format(dateLayoutISO),
			MaturityDate:            rec.MaturityDate.Format(dateLayoutISO),
			FrequencyID:             rec.FrequencyID,
			InterestPayoutFrequency: rec.InterestPayoutFrequency,
			PayoutFrequencyID:       rec.InterestPayoutFrequency,
			TDSPlanID:               rec.TDSPlanID,
			BankConfigID:            rec.BankConfigID,
			DayCountCode:            rec.DayCountConvention,
			SourceAccountID:         rec.BankAccountID,
			AutoRenewal:             rec.AutoRenewal,
			FDStatus:                "PENDING_ACTIVATION",
			AccrualFrequencyCode:    rec.AccrualFrequencyCode,
			ResetType:               rec.ResetType,
			PenaltyID:               rec.PenaltyID,
			FirstPayoutDate:         rec.FirstPayoutDate.Format(dateLayoutISO),
			FirstCapitalizationDate: rec.FirstCapitalizationDate.Format(dateLayoutISO),
			ReceiptDate:             receiptDate.Format(dateLayoutISO),
			PrematureClosureTerms:   rec.PrematureClosureTerms,
		})) {
			return
		}

		fdID, _, err := insertFDMaster(ctx, tx, rec, bankRef, receiptDate, req.Notes, userEmail)
		if err != nil {
			msg, status := getFDMasterError(err, "FD activation failed")
			api.RespondWithError(w, status, msg)
			return
		}

		auditTable := resolveFDAuditTable(ctx, tx)
		if err := insertFDAuditRecord(ctx, tx, InsertFDAuditParams{
			AuditTable:       auditTable,
			RefID:            fdID,
			UserEmail:        userEmail,
			ActionType:       constants.AuditActionCreate,
			ProcessingStatus: constants.StatusPendingApproval,
			Reason:           firstNonEmpty(req.Reason, req.Notes),
		}); err != nil {
			msg, status := getFDMasterError(err, constants.ErrAuditInsertFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		// Use the already-loaded rec to avoid a redundant DB round-trip inside the transaction.
		cashflows, _, err := GenerateCashflowFromRecord(ctx, tx, rec)
		if err != nil {
			msg, status := getFDMasterError(err, "Cashflow generation failed")
			api.RespondWithError(w, status, msg)
			return
		}
		if err := SaveCashflowScheduleWithRecord(ctx, tx, fdID, cashflows, userEmail, rec); err != nil {
			msg, status := getFDMasterError(err, "Cashflow save failed")
			api.RespondWithError(w, status, msg)
			return
		}

		// Write one pending-approval audit row per cashflow row to seed the audit trail.
		// These will be promoted to APPROVED by BulkApproveActivation when the checker approves.
		if auditErr := insertCashflowAuditRowsPending(ctx, tx, fdID, cashflows, userEmail); auditErr != nil {
			msg, status := getFDMasterError(auditErr, "Cashflow audit setup failed")
			api.RespondWithError(w, status, msg)
			return
		}

		// Mark cashflow_generated=true so the accrual engine picks this FD up in scope.
		if err := updateFDMasterCashflowGenerated(ctx, tx, fdID); err != nil {
			msg, status := getFDMasterError(err, "Cashflow flag update failed")
			api.RespondWithError(w, status, msg)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getFDMasterError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}

		go func(fdRecordID string, confirmationID string, email string, entityID string, amount float64, req activateFDRequest) {
			defer func() {
				if rec := recover(); rec != nil {
					api.LogError("[FDMaster] ActivateFD engine goroutine panic for fd %s: %v", fdRecordID, rec)
				}
			}()
			bgCtx, bgCancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer bgCancel()
			auditTableName := resolveFDAuditTable(bgCtx, pgxPool)
			auditIDColumn := "fd_id"
			if auditTableName != "" {
				schemaName, tableName := splitQualifiedTable(auditTableName)
				if cols, err := loadTableColumns(bgCtx, pgxPool, schemaName, tableName); err == nil {
					auditIDColumn = firstNonEmpty(pickFirstExistingColumn(cols, "fd_id", "master_id", "confirmation_id"), auditIDColumn)
				}
			}
			instID, err := approvalengine.CreateInstance(bgCtx, pgxPool, approvalengine.InstanceRequest{
				ModuleCode:       "FIXED_DEPOSIT",
				EntityCode:       entityID,
				TransactionType:  "FD_MASTER_CREATE",
				RecordID:         fdRecordID,
				RecordTable:      constants.QuerryMaster,
				AuditTable:       auditTableName,
				AuditIDColumn:    auditIDColumn,
				ActionType:       constants.AuditActionCreate,
				Amount:           amount,
				SubmittedBy:      req.UserID,
				SubmittedByEmail: email,
			})
			if err != nil {
				api.LogError("[FDMaster] CreateInstance failed for fd %s: %v", fdRecordID, err)
				return
			}
			if instID != "" {
				// Flip fd_master status to APPROVAL_PENDING
				masterCols, colErr := loadTableColumns(bgCtx, pgxPool, "investment", "fd_master")
				if colErr == nil {
					statusCol := pickFirstExistingColumn(masterCols, "status", "fd_status")
					keyCol := pickFirstExistingColumn(masterCols, "fd_id", "master_id")
					if statusCol != "" && keyCol != "" {
						q := fmt.Sprintf(`UPDATE investment.fd_master SET %s = 'APPROVAL_PENDING' WHERE %s = $1`, statusCol, keyCol)
						if _, uerr := pgxPool.Exec(bgCtx, q, fdRecordID); uerr != nil {
							api.LogError("[FDMaster] Status→APPROVAL_PENDING failed for fd %s: %v", fdRecordID, uerr)
						} else {
							api.LogInfo("[FDMaster] CreateInstance %s → fd %s APPROVAL_PENDING", instID, fdRecordID)
						}
					}
				}
			} else {
				api.LogInfo("[FDMaster] No matrix for fd %s — stays PENDING_APPROVAL", fdRecordID)
			}
		}(fdID, req.ConfirmationID, userEmail, rec.EntityID, rec.PrincipalAmount, req)

		go func(fdRecordID, eID, uEmail string, amount float64) {
			defer func() {
				if rec := recover(); rec != nil {
					api.LogError("[FDMaster] ActivateFD notification goroutine panic for fd %s: %v", fdRecordID, rec)
				}
			}()
			notifcatalog.TriggerNotification(context.Background(), pgxPool, "/investment/fd/master/activate", fdRecordID, map[string]interface{}{
				"entity_id":   eID,
				"record_id":   fdRecordID,
				"event":       "FD_ACTIVATION_SUBMITTED",
				"actor_email": uEmail,
				"amount":      amount,
			})
		}(fdID, rec.EntityID, userEmail, rec.PrincipalAmount)

		api.RespondWithPayload(w, true, "", map[string]interface{}{"fd_id": fdID, "confirmation_id": req.ConfirmationID, "cashflow_count": len(cashflows), "requested_by": userEmail})
	}
}

func BulkApproveActivation(pgxPool *pgxpool.Pool) http.HandlerFunc {
	// Cashflow audit lifecycle on approval:
	//   PENDING_APPROVAL on cashflow audit (written at FD activation by insertCashflowAuditRowsPending)
	//   → APPROVED (written here by markCashflowAuditApproved)
	// Both the engine-driven path (approval-matrix) and the direct-stamp path (no matrix)
	// call markCashflowAuditApproved inside their respective transactions so the promotion
	// is atomic with the fd_status → ACTIVE update.
	return func(w http.ResponseWriter, r *http.Request) {
		var req bulkFDActionRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		req.normalize()
		if len(req.FDIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "fd_ids are required")
			return
		}

		userEmail := getUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		engineActed := 0
		directActed := 0
		var errors []string

		for _, rawID := range req.FDIDs {
			// Resolve the real fd_id — caller may pass either an fd_id or a confirmation_id.
			fdID := rawID
			var resolvedFDID string
			if lookupErr := pgxPool.QueryRow(ctx,
				`SELECT fd_id FROM investment.fd_master WHERE fd_id = $1 AND COALESCE(is_deleted,false)=false`,
				rawID,
			).Scan(&resolvedFDID); lookupErr != nil {
				// Try as confirmation_id.
				if lookupErr2 := pgxPool.QueryRow(ctx,
					`SELECT fd_id FROM investment.fd_master WHERE confirmation_id = $1 AND COALESCE(is_deleted,false)=false`,
					rawID,
				).Scan(&resolvedFDID); lookupErr2 == nil {
					fdID = resolvedFDID
				}
			}

			approveRow, loadErr := loadFDMasterRow(ctx, pgxPool, fdID)
			if loadErr != nil {
				errors = append(errors, fdID+": fd master not found")
				continue
			}
			if ok, pmsg := fdEnforceInline(ctx, r, pgxPool, enforceCtx{EventCode: common.TriggerPreApprove, HandlerName: "BulkApproveActivation",
				APIPath: "/investment/fd/master/bulk-approve", SubModule: fdSubMaster, EntityCode: approveRow.EntityID, Actor: userEmail},
				buildFDMasterPolicyFields(approveRow)); !ok {
				errors = append(errors, fdID+": "+pmsg)
				continue
			}

			actionRes, actionErr := approvalengine.ActOnPendingOrDiagnose(ctx, pgxPool, approvalengine.ActOnPendingRequest{ModuleCode: "FIXED_DEPOSIT", RecordID: fdID, UserID: req.UserID, UserEmail: userEmail, RoleID: "", Action: approvalengine.ActionApproved, Comment: req.Comment})
			if actionErr != nil {
				api.LogError("[FDMaster] RecordAction approve failed for fd %s: %v", fdID, actionErr)
				errors = append(errors, fdID+": "+actionErr.Error())
				continue
			}
			if actionRes.Acted {
				// Check if fully approved (last eye) — if so, set ACTIVE and create journals.
				if actionRes.InstanceStatus == constants.StatusApproved {
					tx, txErr := pgxPool.Begin(ctx)
					if txErr != nil {
						errors = append(errors, fdID+": post-approval tx begin failed")
						engineActed++
						continue
					}
					auditTable := resolveFDAuditTable(ctx, tx)
					_ = updateFDMasterStatusBy(ctx, tx, []string{fdID}, constants.StatusActive, userEmail)
					_ = updateFDAuditStatus(ctx, tx, auditTable, []string{fdID}, userEmail, constants.StatusApproved, req.Comment)
					markCashflowAuditApproved(ctx, tx, fdID, userEmail) // promote cashflow audit CREATE rows → APPROVED
					rec, err := loadFDRecordByFDID(ctx, tx, fdID)
					if err == nil {
						activityID, err := CreateFDAccountingActivity(ctx, tx, fdID, rec.ValueDate, userEmail)
						if err == nil {
							bankInfo, _ := loadBankAccountInfo(ctx, tx, rec.BankAccountID)
							journalEntries := buildJournalEntries(rec, bankInfo, activityID)
							_ = SaveFDJournalEntries(ctx, tx, fdID, userEmail, journalEntries)
						}
					}
					if cerr := tx.Commit(ctx); cerr != nil {
						_ = tx.Rollback(ctx)
						errors = append(errors, fdID+": post-approval commit failed")
					}
				}
				engineActed++
			} else {
				if actionRes.CancelledStale {
					api.LogInfo("[FDMaster] Cancelled stale activation approval instance for fd=%s: %s", fdID, actionRes.Reason)
				} else if actionRes.Reason != "" {
					errors = append(errors, fdID+": "+actionRes.Reason)
					continue
				}
				// No matrix — direct stamp + ACTIVE + journals.
				tx, err := pgxPool.Begin(ctx)
				if err != nil {
					errors = append(errors, fdID+": tx begin failed")
					continue
				}
				auditTable := resolveFDAuditTable(ctx, tx)
				if err := updateFDAuditStatus(ctx, tx, auditTable, []string{fdID}, userEmail, constants.StatusApproved, req.Comment); err != nil {
					_ = tx.Rollback(ctx)
					errors = append(errors, fdID+": audit update failed")
					continue
				}
				if err := updateFDMasterStatusBy(ctx, tx, []string{fdID}, constants.StatusActive, userEmail); err != nil {
					_ = tx.Rollback(ctx)
					errors = append(errors, fdID+": status update failed")
					continue
				}
				rec, err := loadFDRecordByFDID(ctx, tx, fdID)
				if err != nil {
					_ = tx.Rollback(ctx)
					errors = append(errors, fdID+": FD load failed")
					continue
				}
				activityID, err := CreateFDAccountingActivity(ctx, tx, fdID, rec.ValueDate, userEmail)
				if err != nil {
					_ = tx.Rollback(ctx)
					errors = append(errors, fdID+": accounting activity failed")
					continue
				}
				bankInfo, _ := loadBankAccountInfo(ctx, tx, rec.BankAccountID)
				journalEntries := buildJournalEntries(rec, bankInfo, activityID)
				if err := SaveFDJournalEntries(ctx, tx, fdID, userEmail, journalEntries); err != nil {
					_ = tx.Rollback(ctx)
					errors = append(errors, fdID+": journal save failed")
					continue
				}
				markCashflowAuditApproved(ctx, tx, fdID, userEmail) // promote PENDING_ACTIVATION → APPROVED
				if cerr := tx.Commit(ctx); cerr != nil {
					errors = append(errors, fdID+constants.ErrCommitFailed)
					continue
				}
				directActed++
			}
		}
		totalActed := engineActed + directActed
		success := totalActed > 0 || len(errors) == 0
		msg := ""
		if !success {
			msg = "No FDs were activated"
			if len(errors) > 0 {
				msg += ": " + strings.Join(errors, "; ")
			}
		}
		api.RespondWithPayload(w, success, msg, map[string]interface{}{
			"engine_acted": engineActed, "direct_acted": directActed,
			"errors": errors, "checker": userEmail,
		})
		for _, fdID := range req.FDIDs {
			go func(id, uEmail string) {
				defer func() {
					if rec := recover(); rec != nil {
						api.LogError("[FDMaster] BulkApproveActivation notification goroutine panic for fd %s: %v", id, rec)
					}
				}()
				notifcatalog.TriggerNotification(context.Background(), pgxPool, "/investment/fd/master/bulk-approve", id, map[string]interface{}{
					"record_id":   id,
					"event":       "FD_ACTIVATION_APPROVED",
					"actor_email": uEmail,
				})
			}(fdID, userEmail)
		}
		api.LogInfo("[FDMaster] BulkApproveActivation: engine=%d direct=%d errors=%d by=%s",
			engineActed, directActed, len(errors), userEmail)
	}
}

func BulkRejectActivation(pgxPool *pgxpool.Pool) http.HandlerFunc {
	// On rejection, three cleanup steps run inside the same transaction as fd_status → REJECTED:
	//  1. Soft-delete all cashflow rows (is_deleted=true, is_active=false) so the accrual engine
	//     cannot pick up cashflow for a rejected FD.
	//  2. Mark activation cashflow audit rows REJECTED.
	//  3. Clear cashflow_generated=false on fd_master so the generation flag stays consistent.
	// Implemented via softDeleteFDCashflowForRejection, called in both engine and direct paths.
	return func(w http.ResponseWriter, r *http.Request) {
		var req bulkFDActionRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		req.normalize()
		if len(req.FDIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "fd_ids are required")
			return
		}

		userEmail := getUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		engineActed := 0
		directActed := 0
		var errors []string

		for _, rawID := range req.FDIDs {
			// Resolve the real fd_id — caller may pass either an fd_id or a confirmation_id.
			fdID := rawID
			var resolvedFDID string
			if lookupErr := pgxPool.QueryRow(ctx,
				`SELECT fd_id FROM investment.fd_master WHERE fd_id = $1 AND COALESCE(is_deleted,false)=false`,
				rawID,
			).Scan(&resolvedFDID); lookupErr != nil {
				if lookupErr2 := pgxPool.QueryRow(ctx,
					`SELECT fd_id FROM investment.fd_master WHERE confirmation_id = $1 AND COALESCE(is_deleted,false)=false`,
					rawID,
				).Scan(&resolvedFDID); lookupErr2 == nil {
					fdID = resolvedFDID
				}
			}

			rejectRow, loadErr := loadFDMasterRow(ctx, pgxPool, fdID)
			if loadErr != nil {
				errors = append(errors, fdID+": fd master not found")
				continue
			}
			if ok, pmsg := fdEnforceInline(ctx, r, pgxPool, enforceCtx{EventCode: common.TriggerPreReject, HandlerName: "BulkRejectActivation",
				APIPath: "/investment/fd/master/bulk-reject", SubModule: fdSubMaster, EntityCode: rejectRow.EntityID, Actor: userEmail},
				buildFDMasterPolicyFields(rejectRow)); !ok {
				errors = append(errors, fdID+": "+pmsg)
				continue
			}

			actionRes, actionErr := approvalengine.ActOnPendingOrDiagnose(ctx, pgxPool, approvalengine.ActOnPendingRequest{ModuleCode: "FIXED_DEPOSIT", RecordID: fdID, UserID: req.UserID, UserEmail: userEmail, RoleID: "", Action: approvalengine.ActionRejected, Comment: req.Comment})
			if actionErr != nil {
				api.LogError("[FDMaster] RecordAction reject failed for fd %s: %v", fdID, actionErr)
				errors = append(errors, fdID+": "+actionErr.Error())
				continue
			}
			if actionRes.Acted {
				// Wrap in a transaction so the three-step cleanup is atomic with the status update.
				rejectTx, rejectTxErr := pgxPool.Begin(ctx)
				if rejectTxErr != nil {
					api.LogError("[FDMaster] fd_status→REJECTED tx begin failed for %s: %v", fdID, rejectTxErr)
				} else {
					softDeleteFDCashflowForRejection(ctx, rejectTx, fdID, userEmail, req.Comment)
					if _, execErr := rejectTx.Exec(ctx,
						`UPDATE investment.fd_master SET fd_status='REJECTED'
						 WHERE fd_id=$1 AND fd_status NOT IN ('ACTIVE','REJECTED')`, fdID,
					); execErr != nil {
						api.LogError("[FDMaster] fd_status→REJECTED failed for %s: %v", fdID, execErr)
					}
					if cerr := rejectTx.Commit(ctx); cerr != nil {
						_ = rejectTx.Rollback(ctx)
						api.LogError("[FDMaster] fd_status→REJECTED commit failed for %s: %v", fdID, cerr)
					}
				}
				engineActed++
			} else {
				if actionRes.CancelledStale {
					api.LogInfo("[FDMaster] Cancelled stale activation rejection instance for fd=%s: %s", fdID, actionRes.Reason)
				} else if actionRes.Reason != "" {
					errors = append(errors, fdID+": "+actionRes.Reason)
					continue
				}
				tx, err := pgxPool.Begin(ctx)
				if err != nil {
					errors = append(errors, fdID+": tx begin failed")
					continue
				}
				auditTable := resolveFDAuditTable(ctx, tx)
				if err := updateFDAuditStatus(ctx, tx, auditTable, []string{fdID}, userEmail, constants.StatusRejected, req.Comment); err != nil {
					_ = tx.Rollback(ctx)
					errors = append(errors, fdID+": audit update failed")
					continue
				}
				if err := updateFDMasterStatus(ctx, tx, []string{fdID}, constants.StatusRejected); err != nil {
					_ = tx.Rollback(ctx)
					errors = append(errors, fdID+": status update failed")
					continue
				}
				softDeleteFDCashflowForRejection(ctx, tx, fdID, userEmail, req.Comment)
				if cerr := tx.Commit(ctx); cerr != nil {
					errors = append(errors, fdID+constants.ErrCommitFailed)
					continue
				}
				directActed++
			}
		}
		totalActed := engineActed + directActed
		success := totalActed > 0 || len(errors) == 0
		msg := ""
		if !success {
			msg = "No FDs were rejected"
			if len(errors) > 0 {
				msg += ": " + strings.Join(errors, "; ")
			}
		}
		api.RespondWithPayload(w, success, msg, map[string]interface{}{
			"engine_acted": engineActed, "direct_acted": directActed,
			"errors": errors, "checker": userEmail,
		})
		for _, fdID := range req.FDIDs {
			go func(id, uEmail string) {
				defer func() {
					if rec := recover(); rec != nil {
						api.LogError("[FDMaster] BulkRejectActivation notification goroutine panic for fd %s: %v", id, rec)
					}
				}()
				notifcatalog.TriggerNotification(context.Background(), pgxPool, "/investment/fd/master/bulk-reject", id, map[string]interface{}{
					"record_id":   id,
					"event":       "FD_ACTIVATION_REJECTED",
					"actor_email": uEmail,
				})
			}(fdID, userEmail)
		}
		api.LogInfo("[FDMaster] BulkRejectActivation: engine=%d direct=%d errors=%d by=%s",
			engineActed, directActed, len(errors), userEmail)
	}
}

// ─── GetFDMasterDetail ────────────────────────────────────────────────────────
// Returns one fd_master row + its full audit history + inline approval_workflow.
// Query: GET /investment/fd/master/detail?fd_id=FDMST-xxx&user_id=1
func GetFDMasterDetail(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fdID := strings.TrimSpace(r.URL.Query().Get("fd_id"))
		if fdID == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrFDIDRequired)
			return
		}
		viewerUserID := r.URL.Query().Get("user_id")
		ctx := r.Context()

		// ── FD master row ───────────────────────────────────────────────────
		masterCols, err := loadTableColumns(ctx, pgxPool, "investment", "fd_master")
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Load schema failed: "+err.Error())
			return
		}
		keyCol := pickFirstExistingColumn(masterCols, "fd_id", "master_id")
		if keyCol == "" {
			api.RespondWithError(w, http.StatusInternalServerError, "fd_master has no recognisable PK column")
			return
		}
		fdRows, err := pgxPool.Query(ctx,
			fmt.Sprintf(`SELECT * FROM investment.fd_master WHERE %s = $1 AND COALESCE(is_deleted,false)=false`, keyCol),
			fdID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		fdMaps, err := rowsToMaps(fdRows)
		fdRows.Close()
		if err != nil || len(fdMaps) == 0 {
			api.RespondWithError(w, http.StatusNotFound, constants.ErrFDNotFound1)
			return
		}
		fdMaster := fdMaps[0]
		enrichTenureFields(fdMaster)
		if entityID := strings.TrimSpace(fmt.Sprint(fdMaster["entity_id"])); entityID != "" && !ctxutil.FromContext(ctx).HasEntityAccess(entityID) {
			api.RespondWithError(w, http.StatusForbidden,
				fmt.Sprintf(constants.ErrEntityIDNotAuthorized, entityID))
			return
		}

		// ── Audit history ────────────────────────────────────────────────────
		auditTable := resolveFDAuditTable(ctx, pgxPool)
		var auditHistory []map[string]interface{}
		if auditTable != "" {
			schemaName, tableName := splitQualifiedTable(auditTable)
			auditCols, _ := loadTableColumns(ctx, pgxPool, schemaName, tableName)
			refCol := pickFirstExistingColumn(auditCols, "fd_id", "master_id", "confirmation_id")
			if refCol != "" {
				auditRows, aErr := pgxPool.Query(ctx,
					fmt.Sprintf(`SELECT * FROM %s WHERE %s = $1 ORDER BY requested_at DESC`, auditTable, refCol),
					fdID)
				if aErr == nil {
					auditHistory, _ = rowsToMaps(auditRows)
					auditRows.Close()
				}
			}
		}
		if auditHistory == nil {
			auditHistory = []map[string]interface{}{}
		}

		// ── Approval workflow ────────────────────────────────────────────────
		var approvalWorkflow interface{}
		{
			var instanceID string
			_ = pgxPool.QueryRow(ctx, `
				SELECT instance_id
				FROM uam.approval_instance
				WHERE record_id = $1 AND module_code = 'FIXED_DEPOSIT' AND is_deleted = false
				ORDER BY submitted_at DESC LIMIT 1`, fdID,
			).Scan(&instanceID)

			// Self-heal: if no instance but audit has PENDING row, create now
			if instanceID == "" {
				var pendingStatus, submittedBy, entityID string
				var amount float64
				scanErr := pgxPool.QueryRow(ctx, fmt.Sprintf(`
					SELECT a.processing_status,
					       COALESCE(a.requested_by,''),
					       COALESCE(m.entity_id,''),
					       COALESCE(m.principal_amount,0)
					FROM %s a
					JOIN investment.fd_master m ON m.%s = a.%s
					WHERE a.%s = $1
					  AND a.processing_status LIKE '%%PENDING%%'
					ORDER BY a.requested_at DESC LIMIT 1`, auditTable, keyCol, refColForSelfHeal(masterCols), refColForSelfHeal(masterCols)),
					fdID,
				).Scan(&pendingStatus, &submittedBy, &entityID, &amount)
				if scanErr == nil && pendingStatus != "" {
					newInstID, instErr := approvalengine.CreateInstance(ctx, pgxPool, approvalengine.InstanceRequest{
						ModuleCode:       "FIXED_DEPOSIT",
						EntityCode:       entityID,
						TransactionType:  "FD_MASTER_CREATE",
						RecordID:         fdID,
						RecordTable:      constants.QuerryMaster,
						AuditTable:       auditTable,
						AuditIDColumn:    keyCol,
						ActionType:       constants.AuditActionCreate,
						Amount:           amount,
						SubmittedBy:      submittedBy,
						SubmittedByEmail: submittedBy,
					})
					if instErr != nil {
						api.LogError("[FDMaster] Self-heal CreateInstance for %s: %v", fdID, instErr)
					} else if newInstID != "" {
						instanceID = newInstID
						api.LogInfo("[FDMaster] Self-heal: created instance %s for fd %s", newInstID, fdID)
					}
				}
			}

			if instanceID != "" {
				richDetail, richErr := approvalengine.GetRichInstanceDetail(ctx, pgxPool, instanceID, viewerUserID)
				if richErr != nil {
					api.LogError("[FDMaster] GetRichInstanceDetail failed instance=%s fd=%s: %v", instanceID, fdID, richErr)
				} else {
					approvalWorkflow = richDetail
				}
			}
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"fd_master":         fdMaster,
			"audit_history":     auditHistory,
			"approval_workflow": approvalWorkflow,
		})
		api.LogInfo("[FDMaster] GetFDMasterDetail: fd_id=%s", fdID)
	}
}

// refColForSelfHeal returns the column name used to join audit→master in the self-heal query.
func refColForSelfHeal(masterCols map[string]bool) string {
	for _, c := range []string{"fd_id", "master_id"} {
		if masterCols[c] {
			return c
		}
	}
	return "fd_id"
}

func GetFDMasterWithAudit(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		backfillEmptyTenureTypes(ctx, pgxPool)
		masterCols, err := loadTableColumns(ctx, pgxPool, "investment", "fd_master")
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrFailedToQuery+": "+err.Error())
			return
		}

		// ── Resolve audit table and its reference column ─────────────────────
		auditTable := resolveFDAuditTable(ctx, pgxPool)
		var auditRefCol string
		if auditTable != "" {
			schemaName, tableName := splitQualifiedTable(auditTable)
			auditCols, _ := loadTableColumns(ctx, pgxPool, schemaName, tableName)
			auditRefCol = pickFirstExistingColumn(auditCols, "fd_id", "master_id", "confirmation_id")
		}

		// ── Resolve master key column once (used in query building and filters) ──
		masterKeyCol := pickFirstExistingColumn(masterCols, "fd_id", "master_id", "confirmation_id")

		// ── Build main query with optional audit join for ordering ───────────
		var query string
		if auditTable != "" && auditRefCol != "" {
			// Join with a lateral subquery that picks the latest audit timestamp
			query = fmt.Sprintf(`
				SELECT m.*,
					COALESCE(ai.instance_id,'')        AS approval_instance_id,
					COALESCE(ai.status,'')             AS approval_engine_status,
					COALESCE(aie.instance_eye_id,'')   AS current_eye_id,
					COALESCE(aie.position::text,'')    AS current_eye_position,
					COALESCE(aie.approvals_required,0) AS approvals_required,
					COALESCE(aie.approvals_received,0) AS approvals_received,
					aie.sla_deadline                   AS sla_deadline,
					COALESCE(aie.is_escalated,false)   AS is_escalated
				FROM investment.fd_master m
				LEFT JOIN LATERAL (
					SELECT GREATEST(
						COALESCE(a.requested_at, '1970-01-01'::timestamp),
						COALESCE(a.checker_at, '1970-01-01'::timestamp)
					) AS latest_audit_ts
					FROM %s a
					WHERE a.%s = m.%s
					ORDER BY GREATEST(
						COALESCE(a.requested_at, '1970-01-01'::timestamp),
						COALESCE(a.checker_at, '1970-01-01'::timestamp)
					) DESC
					LIMIT 1
				) aud ON true
				LEFT JOIN LATERAL (
					SELECT ai.* FROM uam.approval_instance ai
					WHERE ai.record_id = m.%s
					  AND ai.module_code = 'FIXED_DEPOSIT'
					  AND ai.status = 'PENDING'
					  AND ai.is_deleted = false
					ORDER BY ai.submitted_at DESC, ai.instance_id DESC
					LIMIT 1
				) ai ON true
				LEFT JOIN LATERAL (
					SELECT aie.* FROM uam.approval_instance_eye aie
					WHERE aie.instance_id = ai.instance_id
					  AND aie.status = 'ACTIVE'
					ORDER BY aie.position ASC, aie.instance_eye_id ASC
					LIMIT 1
				) aie ON true
				WHERE COALESCE(m.is_deleted, false) = false`,
				auditTable, auditRefCol,
				masterKeyCol,
				masterKeyCol)
		} else {
			query = fmt.Sprintf(`
				SELECT m.*,
					COALESCE(ai.instance_id,'')        AS approval_instance_id,
					COALESCE(ai.status,'')             AS approval_engine_status,
					COALESCE(aie.instance_eye_id,'')   AS current_eye_id,
					COALESCE(aie.position::text,'')    AS current_eye_position,
					COALESCE(aie.approvals_required,0) AS approvals_required,
					COALESCE(aie.approvals_received,0) AS approvals_received,
					aie.sla_deadline                   AS sla_deadline,
					COALESCE(aie.is_escalated,false)   AS is_escalated
				FROM investment.fd_master m
				LEFT JOIN LATERAL (
					SELECT ai.* FROM uam.approval_instance ai
					WHERE ai.record_id = m.%s
					  AND ai.module_code = 'FIXED_DEPOSIT'
					  AND ai.status = 'PENDING'
					  AND ai.is_deleted = false
					ORDER BY ai.submitted_at DESC, ai.instance_id DESC
					LIMIT 1
				) ai ON true
				LEFT JOIN LATERAL (
					SELECT aie.* FROM uam.approval_instance_eye aie
					WHERE aie.instance_id = ai.instance_id
					  AND aie.status = 'ACTIVE'
					ORDER BY aie.position ASC, aie.instance_eye_id ASC
					LIMIT 1
				) aie ON true
				WHERE COALESCE(m.is_deleted,false)=false`, masterKeyCol)
		}

		args := make([]interface{}, 0)
		position := 1
		scope := ctxutil.FromContext(ctx)

		if fdID := strings.TrimSpace(r.URL.Query().Get("fd_id")); fdID != "" {
			if keyCol := pickFirstExistingColumn(masterCols, "fd_id", "master_id", "confirmation_id"); keyCol != "" {
				query += fmt.Sprintf(" AND m.%s = $%d", keyCol, position)
				args = append(args, fdID)
				position++
			}
		}
		if entityID := strings.TrimSpace(r.URL.Query().Get("entity_id")); entityID != "" {
			if !scope.HasEntityAccess(entityID) {
				api.RespondWithError(w, http.StatusForbidden,
					fmt.Sprintf(constants.ErrEntityIDNotAuthorized, entityID))
				return
			}
			if masterCols["entity_id"] {
				query += fmt.Sprintf(" AND m.entity_id = $%d", position)
				args = append(args, entityID)
				position++
			}
		} else if masterCols["entity_id"] && len(scope.EntityIDs) > 0 {
			query += fmt.Sprintf(" AND m.entity_id = ANY($%d::text[])", position)
			args = append(args, scope.EntityIDs)
			position++
		}
		if confirmationID := strings.TrimSpace(r.URL.Query().Get("confirmation_id")); confirmationID != "" && masterCols["confirmation_id"] {
			query += fmt.Sprintf(" AND m.confirmation_id = $%d", position)
			args = append(args, confirmationID)
			position++
		}

		// Order by audit timestamp (newest action first), fall back to updated_at
		if auditTable != "" && auditRefCol != "" {
			query += " ORDER BY aud.latest_audit_ts DESC NULLS LAST"
		} else {
			query += " ORDER BY m.updated_at DESC NULLS LAST"
		}

		rows, err := pgxPool.Query(ctx, query, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrFailedToQuery+": "+err.Error())
			return
		}
		defer rows.Close()

		payload, err := rowsToMaps(rows)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrRowsScanFailed+err.Error())
			return
		}

		for i := range payload {
			enrichTenureFields(payload[i])
		}
		enrichBankReferenceFields(ctx, pgxPool, payload)

		// ── Enrich each FD row with its latest activation audit processing_status ──
		if auditTable != "" && auditRefCol != "" {
			for i, row := range payload {
				fdKey := ""
				for _, k := range []string{"fd_id", "master_id", "confirmation_id"} {
					if v, ok := row[k]; ok && v != nil {
						fdKey = fmt.Sprintf("%v", v)
						break
					}
				}
				if fdKey == "" {
					continue
				}
				var activationStatus string
				_ = pgxPool.QueryRow(ctx, fmt.Sprintf(
					`SELECT COALESCE(processing_status,'') FROM %s
					 WHERE %s = $1
					 ORDER BY GREATEST(
						COALESCE(requested_at, '1970-01-01'::timestamp),
						COALESCE(checker_at, '1970-01-01'::timestamp)
					 ) DESC LIMIT 1`,
					auditTable, auditRefCol), fdKey).Scan(&activationStatus)
				payload[i]["processing_status"] = activationStatus
			}
		}

		api.RespondWithPayload(w, true, "", payload)
	}
}

// ─── GetActiveFDsInRange ──────────────────────────────────────────────────────
// POST /investment/fd/master/active-range
// Returns all ACTIVE FDs (fd_status = ACTIVE, approved) whose lifespan
// overlaps the caller-supplied date window:
//
//	FD start_date <= range_end  AND  FD maturity_date >= range_start
//
// Excludes MATURED, PREMATURELY_CLOSED, ROLLED_OVER, CANCELLED, REJECTED.
// Returns the full fd_master rows plus a convenience `fd_ids` string array.
//
// Body: { "user_id":"", "entity_id":"", "start_date":"YYYY-MM-DD", "end_date":"YYYY-MM-DD" }
// All body fields are optional filters except that start_date and end_date
// must both be present to enable the date-range filter.
func GetActiveFDsInRange(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID    string `json:"user_id"`
			EntityID  string `json:"entity_id"`
			BankID    string `json:"bank_id"`
			StartDate string `json:"start_date"` // range window start (inclusive)
			EndDate   string `json:"end_date"`   // range window end   (inclusive)
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}

		ctx := r.Context()
		scope := ctxutil.FromContext(ctx)

		masterCols, err := loadTableColumns(ctx, pgxPool, "investment", "fd_master")
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrFailedToQuery+": "+err.Error())
			return
		}

		// Resolve dynamic column names
		startCol := pickFirstExistingColumn(masterCols, "value_date", "start_date", "booking_date")
		if startCol == "" {
			startCol = "value_date"
		}
		maturityCol := pickFirstExistingColumn(masterCols, "maturity_date", "mature_date", "maturity")
		if maturityCol == "" {
			maturityCol = "maturity_date"
		}
		statusCol := pickFirstExistingColumn(masterCols, "fd_status", "status")
		if statusCol == "" {
			statusCol = "fd_status"
		}

		// Closed/terminated statuses to exclude
		excludedStatuses := []string{"MATURED", "PREMATURELY_CLOSED", "ROLLED_OVER", "CANCELLED", constants.StatusRejected}

		query := fmt.Sprintf(`
			SELECT *
			FROM investment.fd_master
			WHERE COALESCE(is_deleted, false) = false
			  AND %s = 'ACTIVE'
			  AND %s <> ALL($1::text[])
		`, statusCol, statusCol)
		args := []interface{}{excludedStatuses}
		pos := 2

		if req.EntityID != "" {
			if !scope.HasEntityAccess(req.EntityID) {
				api.RespondWithError(w, http.StatusForbidden,
					fmt.Sprintf(constants.ErrEntityIDNotAuthorized, req.EntityID))
				return
			}
			if masterCols["entity_id"] {
				query += fmt.Sprintf(" AND entity_id = $%d", pos)
				args = append(args, req.EntityID)
				pos++
			}
		} else if masterCols["entity_id"] && len(scope.EntityIDs) > 0 {
			query += fmt.Sprintf(" AND entity_id = ANY($%d::text[])", pos)
			args = append(args, scope.EntityIDs)
			pos++
		}
		if req.BankID != "" && masterCols["bank_id"] {
			if !scope.HasApprovedBank(req.BankID) {
				api.RespondWithError(w, http.StatusForbidden,
					fmt.Sprintf(constants.ErrBankNotApproved1, req.BankID))
				return
			}
			query += fmt.Sprintf(" AND bank_id = $%d", pos)
			args = append(args, req.BankID)
			pos++
		}

		// Date-range overlap: FD lifespan overlaps [start_date, end_date]
		// Condition: fd.start_date <= range_end AND fd.maturity_date >= range_start
		if req.StartDate != "" && req.EndDate != "" {
			if masterCols[startCol] && masterCols[maturityCol] {
				query += fmt.Sprintf(" AND %s <= $%d::date AND %s >= $%d::date",
					maturityCol, pos, startCol, pos+1)
				args = append(args, req.EndDate, req.StartDate)
				pos += 2
			}
		} else if req.StartDate != "" {
			if masterCols[maturityCol] {
				query += fmt.Sprintf(" AND %s >= $%d::date", maturityCol, pos)
				args = append(args, req.StartDate)
				pos++
			}
		} else if req.EndDate != "" {
			if masterCols[startCol] {
				query += fmt.Sprintf(" AND %s <= $%d::date", startCol, pos)
				args = append(args, req.EndDate)
				pos++
			}
		}

		query += fmt.Sprintf(" ORDER BY %s ASC", maturityCol)

		rows, err := pgxPool.Query(ctx, query, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrFailedToQuery+": "+err.Error())
			return
		}
		defer rows.Close()

		payload, err := rowsToMaps(rows)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrRowsScanFailed+err.Error())
			return
		}
		if payload == nil {
			payload = []map[string]interface{}{}
		}

		// Build convenience fd_ids array
		fdIDs := make([]string, 0, len(payload))
		for _, row := range payload {
			for _, k := range []string{"fd_id", "master_id", "confirmation_id"} {
				if v, ok := row[k]; ok && v != nil {
					fdIDs = append(fdIDs, fmt.Sprintf("%v", v))
					break
				}
			}
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"fd_ids": fdIDs,
			"data":   payload,
			"count":  len(payload),
		})
	}
}

func GetCashflowSchedule(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		fdID := strings.TrimSpace(r.URL.Query().Get("fd_id"))
		if fdID == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrFDIDRequired)
			return
		}
		if entityID, ok, scopeErr := requireFDMasterScope(ctx, pgxPool, fdID); scopeErr != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "FD scope lookup failed: "+scopeErr.Error())
			return
		} else if !ok {
			api.RespondWithError(w, http.StatusNotFound, constants.ErrFDNotFound1)
			if entityID != "" {
				api.LogError("[FDMaster] GetCashflowSchedule denied fd_id=%s entity_id=%s", fdID, entityID)
			}
			return
		}

		table := resolveFirstExistingTable(ctx, pgxPool, []string{
			constants.QuerryCashflowSchedule,
			constants.QuerryCashflow,
			constants.QuerryMasterCashflowSchedule,
		})
		if table == "" {
			api.RespondWithPayload(w, true, "", []map[string]interface{}{})
			return
		}

		schemaName, tableName := splitQualifiedTable(table)
		cols, err := loadTableColumns(ctx, pgxPool, schemaName, tableName)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrFailedToQuery+": "+err.Error())
			return
		}
		fdCol := pickFirstExistingColumn(cols, "fd_id", "master_id")
		cfIDCol := pickFirstExistingColumn(cols, "cashflow_id", "id")
		if fdCol == "" {
			api.RespondWithPayload(w, true, "", []map[string]interface{}{})
			return
		}

		orderCol := firstNonEmpty(
			pickFirstExistingColumn(cols, "sequence_number"),
			pickFirstExistingColumn(cols, "period_number"),
			pickFirstExistingColumn(cols, "event_date"),
			fdCol,
		)

		// Fetch cashflow rows
		cfRows, err := pgxPool.Query(ctx,
			fmt.Sprintf("SELECT * FROM %s WHERE %s = $1 AND COALESCE(is_deleted,false)=false ORDER BY %s", table, fdCol, orderCol),
			fdID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrFailedToQuery+": "+err.Error())
			return
		}
		cashflowMaps, err := rowsToMaps(cfRows)
		cfRows.Close()
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrRowsScanFailed+err.Error())
			return
		}
		if cashflowMaps == nil {
			cashflowMaps = []map[string]interface{}{}
		}

		// Fetch ALL audit rows for all cashflow_ids of this FD, grouped per cashflow_id.
		// Returns full history array (not just latest) so the UI can show complete audit trail.
		auditByCFID := map[string][]map[string]interface{}{}
		if cfIDCol != "" {
			auditRows, aErr := pgxPool.Query(ctx, `
				SELECT *
				FROM investment.fd_audit_cashflow_schedule
				WHERE fd_id = $1
				ORDER BY cashflow_id, requested_at DESC`, fdID)
			if aErr == nil {
				auditMaps, _ := rowsToMaps(auditRows)
				auditRows.Close()
				for _, am := range auditMaps {
					cfID := ""
					if v, ok := am["cashflow_id"]; ok && v != nil {
						cfID = fmt.Sprintf("%v", v)
					}
					if cfID == "" {
						continue
					}
					// Strip the pending_fields suffix from reason for clean display
					if reasonRaw, ok := am["reason"]; ok && reasonRaw != nil {
						rs := fmt.Sprintf("%v", reasonRaw)
						if idx := strings.Index(rs, constants.QuerryPendingFields); idx >= 0 {
							am["reason"] = rs[:idx]
						}
					}
					// Rename processing_status → audit_status so it doesn't clash
					// with any posting_status column on the cashflow row itself.
					am["audit_status"] = am["processing_status"]
					delete(am, "processing_status")
					auditByCFID[cfID] = append(auditByCFID[cfID], am)
				}
			}
		}

		// Merge full audit history array into each cashflow row under "audit_trail".
		// Also expose the latest audit entry's status as "pending_action" for quick UI checks.
		for i, row := range cashflowMaps {
			cfID := ""
			if v, ok := row[cfIDCol]; ok && v != nil {
				cfID = fmt.Sprintf("%v", v)
			}
			history := auditByCFID[cfID]
			if history == nil {
				history = []map[string]interface{}{}
			}
			cashflowMaps[i]["audit_trail"] = history
			// Expose the latest entry's status for easy client-side checks
			if len(history) > 0 {
				cashflowMaps[i]["latest_audit_status"] = history[0]["audit_status"]
				cashflowMaps[i]["latest_audit_action"] = history[0]["action_type"]
				cashflowMaps[i]["latest_audit_id"] = history[0]["audit_id"]
				cashflowMaps[i]["latest_audit_requested_by"] = history[0]["requested_by"]
			} else {
				cashflowMaps[i]["latest_audit_status"] = nil
				cashflowMaps[i]["latest_audit_action"] = nil
				cashflowMaps[i]["latest_audit_id"] = nil
				cashflowMaps[i]["latest_audit_requested_by"] = nil
			}
		}

		api.RespondWithPayload(w, true, "", cashflowMaps)
	}
}

func GetFDJournalEntries(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		fdID := strings.TrimSpace(r.URL.Query().Get("fd_id"))
		if fdID == "" {
			var body struct {
				FDID string `json:"fd_id"`
			}
			json.NewDecoder(r.Body).Decode(&body)
			fdID = strings.TrimSpace(body.FDID)
		}
		if fdID == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrFDIDRequired)
			return
		}
		if entityID, ok, scopeErr := requireFDMasterScope(ctx, pgxPool, fdID); scopeErr != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "FD scope lookup failed: "+scopeErr.Error())
			return
		} else if !ok {
			api.RespondWithError(w, http.StatusNotFound, constants.ErrFDNotFound1)
			if entityID != "" {
				api.LogError("[FDMaster] GetFDJournalEntries denied fd_id=%s entity_id=%s", fdID, entityID)
			}
			return
		}

		rows, err := pgxPool.Query(ctx, `
			SELECT
				je.entry_id, je.activity_id, je.entity_id, je.entity_name,
				je.fd_id, je.receipt_id, je.accrual_run_id, je.accrual_ledger_id,
				TO_CHAR(je.entry_date, 'YYYY-MM-DD') AS entry_date,
				je.accounting_period, je.entry_type, je.description,
				je.total_debit, je.total_credit, je.status,
				TO_CHAR(je.created_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
				je.created_by,
				jl.line_id, jl.line_number, jl.account_number, jl.account_name,
				jl.account_type, jl.debit_amount, jl.credit_amount, jl.narration,
				jl.folio_id, jl.demat_id
			FROM investment.accounting_journal_entry je
			LEFT JOIN investment.accounting_journal_entry_line jl ON jl.entry_id = je.entry_id
			WHERE je.fd_id = $1 AND je.is_deleted = false
			ORDER BY je.entry_date DESC, jl.line_number ASC`, fdID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrFailedToQuery+": "+err.Error())
			return
		}
		defer rows.Close()

		payload, err := rowsToMaps(rows)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrRowsScanFailed+err.Error())
			return
		}
		if payload == nil {
			payload = []map[string]interface{}{}
		}
		api.RespondWithPayload(w, true, "", payload)
	}
}

// ─── GetCashflowGroupView ─────────────────────────────────────────────────────
// GET /investment/fd/master/cashflow/group
// Query params: entity_id, fd_status, fd_id (all optional)
//
// Returns one summary row per fd_id with aggregated cashflow statistics and
// per-event-type event counts. Use as the "list" view before drilling into
// the per-row cashflow schedule.
func GetCashflowGroupView(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		scope := ctxutil.FromContext(ctx)
		entityID := strings.TrimSpace(r.URL.Query().Get("entity_id"))
		fdStatus := strings.TrimSpace(r.URL.Query().Get("fd_status"))
		fdIDFilter := strings.TrimSpace(r.URL.Query().Get("fd_id"))

		// Resolve cashflow table
		table := resolveFirstExistingTable(ctx, pgxPool, []string{
			constants.QuerryCashflowSchedule,
			constants.QuerryCashflow,
			constants.QuerryMasterCashflowSchedule,
		})
		if table == "" {
			api.RespondWithError(w, http.StatusInternalServerError, "cashflow table not found")
			return
		}
		masterTable := resolveFirstExistingTable(ctx, pgxPool, []string{"investment.fd_master"})
		if masterTable == "" {
			masterTable = "investment.fd_master"
		}

		// Inspect master table columns and pick sensible column names for
		// start/maturity dates. This avoids runtime SQL errors when deployed
		// against a schema where the column may have a different name.
		mschema, mtable := splitQualifiedTable(masterTable)
		masterCols, _ := loadTableColumns(ctx, pgxPool, mschema, mtable)
		startCol := pickFirstExistingColumn(masterCols, "value_date", "start_date", "booking_date", "receipt_date", "created_at")
		if startCol == "" {
			startCol = "value_date"
		}
		maturityCol := pickFirstExistingColumn(masterCols, "maturity_date", "mature_date", "maturity")
		if maturityCol == "" {
			maturityCol = "maturity_date"
		}

		var args []interface{}
		pos := 1

		groupSQL := fmt.Sprintf(`
			SELECT
				cf.fd_id,
				MAX(m.booking_id)                                                    AS booking_id,
				MAX(m.confirmation_id)                                               AS confirmation_id,
				MAX(m.entity_name)                                                   AS entity_name,
				MAX(m.bank_name)                                                     AS bank_name,
				MAX(m.principal_amount)                                              AS principal_amount,
				MAX(m.interest_rate)                                                 AS interest_rate,
				MAX(m.%s)                                                            AS start_date,
				MAX(m.%s)                                                            AS maturity_date,
				MAX(m.fd_status)                                                     AS fd_status,
				COUNT(cf.cashflow_id)                                                AS total_cashflow_events,
				COALESCE(SUM(cf.interest_accrued), 0)                               AS total_interest_accrued,
				COALESCE(SUM(cf.tds_amount), 0)                                     AS total_tds,
				COALESCE(SUM(cf.net_cash_flow), 0)                                  AS total_net_cash_flow,
				MAX(cf.event_date)                                                   AS last_event_date,
				COUNT(CASE WHEN aud.processing_status LIKE 'PENDING%%' THEN 1 END)  AS pending_edit_count,
				CASE
					WHEN COUNT(CASE WHEN aud.processing_status LIKE 'PENDING%%' THEN 1 END) > 0
						THEN 'PENDING_EDIT_APPROVAL'
					ELSE 'APPROVED'
				END                                                                  AS cashflow_processing_status
			FROM %s cf
			JOIN %s m ON m.fd_id = cf.fd_id
			LEFT JOIN LATERAL (
				SELECT processing_status
				FROM investment.fd_audit_cashflow_schedule a
				WHERE a.cashflow_id = cf.cashflow_id
				  AND a.processing_status LIKE 'PENDING%%'
				LIMIT 1
			) aud ON true
			WHERE (cf.is_deleted IS NULL OR cf.is_deleted = false)
		`, startCol, maturityCol, table, masterTable)

		if entityID != "" {
			if !scope.HasEntityAccess(entityID) {
				api.RespondWithError(w, http.StatusForbidden,
					fmt.Sprintf(constants.ErrEntityIDNotAuthorized, entityID))
				return
			}
			groupSQL += fmt.Sprintf(" AND m.entity_id = $%d", pos)
			args = append(args, entityID)
			pos++
		} else if len(scope.EntityIDs) > 0 {
			groupSQL += fmt.Sprintf(" AND m.entity_id = ANY($%d::text[])", pos)
			args = append(args, scope.EntityIDs)
			pos++
		}
		if fdStatus != "" {
			groupSQL += fmt.Sprintf(" AND m.fd_status = $%d", pos)
			args = append(args, fdStatus)
			pos++
		}
		if fdIDFilter != "" {
			groupSQL += fmt.Sprintf(" AND cf.fd_id = $%d", pos)
			args = append(args, fdIDFilter)
			pos++
		}
		groupSQL += fmt.Sprintf(" GROUP BY cf.fd_id ORDER BY MAX(m.%s) DESC", startCol)

		rows, err := pgxPool.Query(ctx, groupSQL, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "group query failed: "+err.Error())
			return
		}
		defer rows.Close()

		type GroupRow struct {
			FDID                     string           `json:"fd_id"`
			BookingID                *string          `json:"booking_id"`
			ConfirmationID           *string          `json:"confirmation_id"`
			EntityName               *string          `json:"entity_name"`
			BankName                 *string          `json:"bank_name"`
			PrincipalAmount          *float64         `json:"principal_amount"`
			InterestRate             *float64         `json:"interest_rate"`
			StartDate                *string          `json:"start_date"`
			MaturityDate             *string          `json:"maturity_date"`
			FDStatus                 *string          `json:"fd_status"`
			TotalCashflowEvents      int64            `json:"total_cashflow_events"`
			TotalInterestAccrued     float64          `json:"total_interest_accrued"`
			TotalTDS                 float64          `json:"total_tds"`
			TotalNetCashFlow         float64          `json:"total_net_cash_flow"`
			LastEventDate            *string          `json:"last_event_date"`
			HasPendingEdits          bool             `json:"has_pending_edits"`
			CashflowProcessingStatus string           `json:"cashflow_processing_status"` // PENDING | CONSOLIDATED
			EventTypeCounts          map[string]int64 `json:"event_type_counts"`
		}

		var groups []GroupRow
		for rows.Next() {
			var g GroupRow
			var startRaw, maturityRaw, lastRaw interface{}
			var pendingCount int64
			if scanErr := rows.Scan(
				&g.FDID, &g.BookingID, &g.ConfirmationID,
				&g.EntityName, &g.BankName,
				&g.PrincipalAmount, &g.InterestRate,
				&startRaw, &maturityRaw,
				&g.FDStatus,
				&g.TotalCashflowEvents,
				&g.TotalInterestAccrued, &g.TotalTDS, &g.TotalNetCashFlow,
				&lastRaw,
				&pendingCount,
				&g.CashflowProcessingStatus,
			); scanErr != nil {
				api.LogError("[GetCashflowGroupView] scan error: %v", scanErr)
				continue
			}
			g.HasPendingEdits = pendingCount > 0
			// Format time.Time dates → "YYYY-MM-DD"
			formatIF := func(v interface{}) *string {
				if v == nil {
					return nil
				}
				type fmtable interface{ Format(string) string }
				if t, ok := v.(fmtable); ok {
					s := t.Format(constants.DateFormat)
					return &s
				}
				return nil
			}
			g.StartDate = formatIF(startRaw)
			g.MaturityDate = formatIF(maturityRaw)
			g.LastEventDate = formatIF(lastRaw)

			// Per-event-type counts
			g.EventTypeCounts = map[string]int64{}
			etRows, etErr := pgxPool.Query(ctx,
				fmt.Sprintf(`SELECT event_type, COUNT(*) FROM %s
				             WHERE fd_id = $1 AND (is_deleted IS NULL OR is_deleted = false)
				             GROUP BY event_type`, table),
				g.FDID)
			if etErr == nil {
				for etRows.Next() {
					var et string
					var cnt int64
					if etErr2 := etRows.Scan(&et, &cnt); etErr2 == nil {
						g.EventTypeCounts[et] = cnt
					}
				}
				etRows.Close()
			}
			groups = append(groups, g)
		}
		if rows.Err() != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "scan error: "+rows.Err().Error())
			return
		}
		if groups == nil {
			groups = []GroupRow{}
		}
		api.RespondWithPayload(w, true, "", map[string]interface{}{"data": groups})
	}
}

// ─── BulkApproveCashflowEdit ──────────────────────────────────────────────────
// POST /investment/fd/master/cashflow/bulk-approve
// Body: { "user_id":"", "fd_id":"FD-xxx", "fd_ids":["FD-xxx","FD-yyy"], "cashflow_ids":["",""], "comment":"" }
func BulkApproveCashflowEdit(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID      string   `json:"user_id"`
			FDID        string   `json:"fd_id"`        // single FD — approve all pending edits
			FDIDs       []string `json:"fd_ids"`       // multiple FDs — approve all pending edits for each
			CashflowIDs []string `json:"cashflow_ids"` // OR explicit list
			Comment     string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		userEmail := getUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		ctx := r.Context()

		// Collect all FD IDs (single fd_id + fd_ids array)
		allFDIDs := append([]string{}, req.FDIDs...)
		if strings.TrimSpace(req.FDID) != "" {
			allFDIDs = append(allFDIDs, req.FDID)
		}
		for _, fdID := range allFDIDs {
			rows, qErr := pgxPool.Query(ctx,
				`SELECT DISTINCT cashflow_id
				 FROM investment.fd_audit_cashflow_schedule
				 WHERE fd_id = $1 AND processing_status LIKE 'PENDING%'`, fdID)
			if qErr != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "db error resolving cashflow_ids for "+fdID+": "+qErr.Error())
				return
			}
			for rows.Next() {
				var cid string
				if scanErr := rows.Scan(&cid); scanErr == nil {
					req.CashflowIDs = append(req.CashflowIDs, cid)
				}
			}
			rows.Close()
		}

		if len(req.CashflowIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrCashflowIDsRequired)
			return
		}
		approved, skipped, errs := 0, 0, []string{}

		for _, cashflowID := range req.CashflowIDs {
			var auditID, requestedBy, status, cfFDID string
			if err := pgxPool.QueryRow(ctx,
				`SELECT audit_id, COALESCE(requested_by,''), COALESCE(processing_status,''), COALESCE(fd_id,'')
				 FROM investment.fd_audit_cashflow_schedule
				 WHERE cashflow_id = $1 AND processing_status LIKE 'PENDING%'
				 ORDER BY requested_at DESC LIMIT 1`,
				cashflowID,
			).Scan(&auditID, &requestedBy, &status, &cfFDID); err != nil {
				errs = append(errs, cashflowID+constants.QuerryNoPendingAuditFound)
				continue
			}
			if auditID == "" {
				errs = append(errs, cashflowID+constants.QuerryNoPendingAuditFound)
				continue
			}
			if requestedBy == userEmail {
				errs = append(errs, cashflowID+constants.QuerryMakerCheckerViolation)
				continue
			}
			if !strings.HasPrefix(status, "PENDING") {
				skipped++
				continue
			}
			bulkApproveCFRow, _ := loadFDCashflowRow(ctx, pgxPool, cashflowID)
			bulkApproveCFFields := buildFDCashflowPolicyFields(bulkApproveCFRow)
			bulkApproveCFFields["audit_id"] = auditID
			if ok, pmsg := fdEnforceInline(ctx, r, pgxPool, enforceCtx{EventCode: common.TriggerPreApprove, HandlerName: "BulkApproveCashflowEdit",
				APIPath: "/investment/fd/master/cashflow/bulk-approve", SubModule: fdSubCashflow, EntityCode: bulkApproveCFRow.EntityID, Actor: userEmail},
				bulkApproveCFFields); !ok {
				errs = append(errs, cashflowID+": "+pmsg)
				continue
			}
			actionRes, actionErr := approvalengine.ActOnPendingOrDiagnose(ctx, pgxPool, approvalengine.ActOnPendingRequest{ModuleCode: "FIXED_DEPOSIT", RecordID: auditID, UserID: req.UserID, UserEmail: userEmail, RoleID: "", Action: approvalengine.ActionApproved, Comment: req.Comment})
			if actionErr != nil {
				errs = append(errs, cashflowID+": "+actionErr.Error())
				continue
			}
			if actionRes.Acted {
				if actionRes.InstanceStatus == constants.StatusApproved {
					if applyErr := applyApprovedCashflowEdit(ctx, pgxPool, auditID, userEmail); applyErr != nil {
						errs = append(errs, cashflowID+": apply failed: "+applyErr.Error())
						continue
					}
				}
			} else {
				if actionRes.CancelledStale {
					api.LogInfo("[BulkApproveCashflow] Cancelled stale approval instance for audit=%s: %s", auditID, actionRes.Reason)
				} else if actionRes.Reason != "" {
					errs = append(errs, cashflowID+": "+actionRes.Reason)
					continue
				}
				if applyErr := applyApprovedCashflowEdit(ctx, pgxPool, auditID, userEmail); applyErr != nil {
					errs = append(errs, cashflowID+": apply failed: "+applyErr.Error())
					continue
				}
			}
			approved++
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"approved": approved, "skipped": skipped,
			"errors": errs, "checker": userEmail,
		})
		for _, cfID := range req.CashflowIDs {
			go func(id, uEmail string) {
				defer func() {
					if rec := recover(); rec != nil {
						api.LogError("[FDMaster] BulkApproveCashflowEdit notification panic for %s: %v", id, rec)
					}
				}()
				notifcatalog.TriggerNotification(context.Background(), pgxPool,
					"/investment/fd/master/cashflow/bulk-approve", id, map[string]interface{}{
						"record_id":   id,
						"event":       "FD_CASHFLOW_EDIT_APPROVED",
						"actor_email": uEmail,
					})
			}(cfID, userEmail)
		}
		api.LogInfo("[BulkApproveCashflow] approved=%d skipped=%d errors=%d by=%s", approved, skipped, len(errs), userEmail)
	}
}

// ─── BulkRejectCashflowEdit ───────────────────────────────────────────────────
// POST /investment/fd/master/cashflow/bulk-reject
// Body: { "user_id":"", "fd_id":"FD-xxx", "fd_ids":["FD-xxx","FD-yyy"], "cashflow_ids":["",""], "comment":"" }
// On rejection the cashflow row is reverted to the old_* snapshot stored in the
// audit table, and downstream rows are re-propagated.
func BulkRejectCashflowEdit(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID      string   `json:"user_id"`
			FDID        string   `json:"fd_id"`        // single FD — reject all pending edits
			FDIDs       []string `json:"fd_ids"`       // multiple FDs
			CashflowIDs []string `json:"cashflow_ids"` // OR explicit list
			Comment     string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		userEmail := getUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		ctx := r.Context()

		// Collect all FD IDs (single fd_id + fd_ids array)
		allFDIDs := append([]string{}, req.FDIDs...)
		if strings.TrimSpace(req.FDID) != "" {
			allFDIDs = append(allFDIDs, req.FDID)
		}
		for _, fdID := range allFDIDs {
			rows, qErr := pgxPool.Query(ctx,
				`SELECT DISTINCT cashflow_id
				 FROM investment.fd_audit_cashflow_schedule
				 WHERE fd_id = $1 AND processing_status LIKE 'PENDING%'`, fdID)
			if qErr != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "db error resolving cashflow_ids for "+fdID+": "+qErr.Error())
				return
			}
			for rows.Next() {
				var cid string
				if scanErr := rows.Scan(&cid); scanErr == nil {
					req.CashflowIDs = append(req.CashflowIDs, cid)
				}
			}
			rows.Close()
		}

		if len(req.CashflowIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrCashflowIDsRequired)
			return
		}

		// Resolve cashflow table once.
		table := resolveFirstExistingTable(ctx, pgxPool, []string{
			constants.QuerryCashflowSchedule,
			constants.QuerryCashflow,
			constants.QuerryMasterCashflowSchedule,
		})

		rejected, skipped, errs := 0, 0, []string{}

		for _, cashflowID := range req.CashflowIDs {
			// Read audit row — get both identity and old_* snapshot columns.
			var auditID, requestedBy, status, fdID string
			var oldIntAcc, oldTDSAmt, oldNetCF, oldCapAmt, oldCloseP, oldOpenP *float64
			if err := pgxPool.QueryRow(ctx,
				`SELECT audit_id,
				        COALESCE(requested_by,''),
				        COALESCE(processing_status,''),
				        COALESCE(fd_id,''),
				        old_interest_accrued,
				        old_tds_amount,
				        old_net_cash_flow,
				        old_capitalized_amount,
				        old_closing_principal,
				        old_opening_principal
				 FROM investment.fd_audit_cashflow_schedule
				 WHERE cashflow_id = $1 AND processing_status LIKE 'PENDING%'
				 ORDER BY requested_at DESC LIMIT 1`,
				cashflowID,
			).Scan(&auditID, &requestedBy, &status, &fdID,
				&oldIntAcc, &oldTDSAmt, &oldNetCF, &oldCapAmt, &oldCloseP, &oldOpenP,
			); err != nil {
				errs = append(errs, cashflowID+constants.QuerryNoPendingAuditFound)
				continue
			}
			if auditID == "" {
				errs = append(errs, cashflowID+constants.QuerryNoPendingAuditFound)
				continue
			}
			if requestedBy == userEmail {
				errs = append(errs, cashflowID+constants.QuerryMakerCheckerViolation)
				continue
			}
			if !strings.HasPrefix(status, "PENDING") {
				skipped++
				continue
			}
			bulkRejectCFRow, _ := loadFDCashflowRow(ctx, pgxPool, cashflowID)
			bulkRejectCFFields := buildFDCashflowPolicyFields(bulkRejectCFRow)
			bulkRejectCFFields["audit_id"] = auditID
			if ok, pmsg := fdEnforceInline(ctx, r, pgxPool, enforceCtx{EventCode: common.TriggerPreReject, HandlerName: "BulkRejectCashflowEdit",
				APIPath: "/investment/fd/master/cashflow/bulk-reject", SubModule: fdSubCashflow, EntityCode: bulkRejectCFRow.EntityID, Actor: userEmail},
				bulkRejectCFFields); !ok {
				errs = append(errs, cashflowID+": "+pmsg)
				continue
			}

			actionRes, actionErr := approvalengine.ActOnPendingOrDiagnose(ctx, pgxPool, approvalengine.ActOnPendingRequest{ModuleCode: "FIXED_DEPOSIT", RecordID: auditID, UserID: req.UserID, UserEmail: userEmail, RoleID: "", Action: approvalengine.ActionRejected, Comment: req.Comment})
			if actionErr != nil {
				errs = append(errs, cashflowID+": "+actionErr.Error())
				continue
			}
			if actionRes.CancelledStale {
				api.LogInfo("[BulkRejectCashflow] Cancelled stale approval instance for audit=%s: %s", auditID, actionRes.Reason)
			} else if !actionRes.Acted && actionRes.Reason != "" {
				errs = append(errs, cashflowID+": "+actionRes.Reason)
				continue
			}

			// Stamp audit row as REJECTED
			_, _ = pgxPool.Exec(ctx,
				`UPDATE investment.fd_audit_cashflow_schedule
				 SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2, checker_ip=$3
				 WHERE audit_id=$4`, api.SystemIfBlank(userEmail), req.Comment, api.SystemIfBlank(api.ClientIPFromContext(ctx)), auditID)

			// Revert cashflow row to old_* snapshot (if snapshots exist)
			if table != "" && fdID != "" &&
				(oldIntAcc != nil || oldTDSAmt != nil || oldNetCF != nil || oldCapAmt != nil) {
				revertedFields := map[string]interface{}{}
				if oldIntAcc != nil {
					revertedFields["interest_accrued"] = *oldIntAcc
				}
				if oldTDSAmt != nil {
					revertedFields["tds_amount"] = *oldTDSAmt
				}
				if oldNetCF != nil {
					revertedFields["net_cash_flow"] = *oldNetCF
				}
				if oldCapAmt != nil {
					revertedFields["capitalized_amount"] = *oldCapAmt
				}
				if oldCloseP != nil {
					revertedFields["closing_principal"] = *oldCloseP
				}
				if oldOpenP != nil {
					revertedFields["opening_principal"] = *oldOpenP
				}

				// Build SET clause for revert
				setParts := []string{"is_edited=false", "edited_by=NULL", "edited_at=NULL"}
				revertArgs := []interface{}{}
				argIdx := 1
				colAliases := map[string][]string{
					"interest_accrued":   {"interest_accrued", "interest_amount"},
					"tds_amount":         {"tds_amount"},
					"net_cash_flow":      {"net_cash_flow", "net_cashflow", "net_amount"},
					"capitalized_amount": {"capitalized_amount"},
					"closing_principal":  {"closing_principal"},
					"opening_principal":  {"opening_principal"},
				}
				// Resolve actual column names from table schema
				tSchema, tName := splitQualifiedTable(table)
				tableCols, _ := loadTableColumns(ctx, pgxPool, tSchema, tName)
				for logicCol, val := range revertedFields {
					for _, alias := range colAliases[logicCol] {
						if tableCols[alias] {
							setParts = append(setParts, fmt.Sprintf("%s=$%d", alias, argIdx))
							revertArgs = append(revertArgs, val)
							argIdx++
							break
						}
					}
				}
				revertArgs = append(revertArgs, cashflowID, fdID)
				revertSQL := fmt.Sprintf(`UPDATE %s SET %s WHERE cashflow_id=$%d AND fd_id=$%d`,
					table, strings.Join(setParts, ", "), argIdx, argIdx+1)
				if _, execErr := pgxPool.Exec(ctx, revertSQL, revertArgs...); execErr != nil {
					api.LogError("[BulkRejectCashflow] revert failed cf=%s: %v", cashflowID, execErr)
				} else {
					// Re-propagate downstream from the reverted row
					pSchema, pName := splitQualifiedTable(table)
					cols, _ := loadTableColumns(ctx, pgxPool, pSchema, pName)
					propagateCashflowDownstream(ctx, pgxPool, PropagateCashflowParams{
						Table:            table,
						FDCol:            "fd_id",
						CashflowIDCol:    "cashflow_id",
						Cols:             cols,
						FDID:             fdID,
						EditedCashflowID: cashflowID,
						AppliedFields:    revertedFields,
					})
				}
			}
			rejected++
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"rejected": rejected, "skipped": skipped,
			"errors": errs, "checker": userEmail,
		})
		for _, cfID := range req.CashflowIDs {
			go func(id, uEmail string) {
				defer func() {
					if rec := recover(); rec != nil {
						api.LogError("[FDMaster] BulkRejectCashflowEdit notification panic for %s: %v", id, rec)
					}
				}()
				notifcatalog.TriggerNotification(context.Background(), pgxPool,
					"/investment/fd/master/cashflow/bulk-reject", id, map[string]interface{}{
						"record_id":   id,
						"event":       "FD_CASHFLOW_EDIT_REJECTED",
						"actor_email": uEmail,
					})
			}(cfID, userEmail)
		}
		api.LogInfo("[BulkRejectCashflow] rejected=%d skipped=%d errors=%d by=%s", rejected, skipped, len(errs), userEmail)
	}
}

// ─── BulkDeleteCashflow ───────────────────────────────────────────────────────
// POST /investment/fd/master/cashflow/bulk-delete
// Body: { "user_id":"", "fd_id":"FD-xxx", "fd_ids":["FD-xxx","FD-yyy"], "cashflow_ids":["",""], "comment":"" }
// Approves pending DELETE audit rows (actually soft-deletes the cashflow rows).
func BulkDeleteCashflow(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID      string   `json:"user_id"`
			FDID        string   `json:"fd_id"`  // single FD
			FDIDs       []string `json:"fd_ids"` // multiple FDs
			CashflowIDs []string `json:"cashflow_ids"`
			Comment     string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		userEmail := getUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		ctx := r.Context()

		// Collect all FD IDs (single fd_id + fd_ids array)
		allFDIDs := append([]string{}, req.FDIDs...)
		if strings.TrimSpace(req.FDID) != "" {
			allFDIDs = append(allFDIDs, req.FDID)
		}
		for _, fdID := range allFDIDs {
			rows, qErr := pgxPool.Query(ctx,
				`SELECT DISTINCT cashflow_id
				 FROM investment.fd_audit_cashflow_schedule
				 WHERE fd_id = $1 AND processing_status LIKE 'PENDING%'`, fdID)
			if qErr == nil {
				for rows.Next() {
					var cid string
					if scanErr := rows.Scan(&cid); scanErr == nil {
						req.CashflowIDs = append(req.CashflowIDs, cid)
					}
				}
				rows.Close()
			}
		}

		table := resolveFirstExistingTable(ctx, pgxPool, []string{
			constants.QuerryCashflowSchedule,
			constants.QuerryCashflow,
			constants.QuerryMasterCashflowSchedule,
		})

		deleted, skipped, errs := 0, 0, []string{}

		for _, cashflowID := range req.CashflowIDs {
			var auditID, requestedBy, status, fdID string
			if err := pgxPool.QueryRow(ctx,
				`SELECT audit_id, COALESCE(requested_by,''), COALESCE(processing_status,''), COALESCE(fd_id,'')
				 FROM investment.fd_audit_cashflow_schedule
				 WHERE cashflow_id = $1 AND processing_status LIKE 'PENDING%'
				 ORDER BY requested_at DESC LIMIT 1`,
				cashflowID,
			).Scan(&auditID, &requestedBy, &status, &fdID); err != nil {
				errs = append(errs, cashflowID+constants.QuerryNoPendingAuditFound)
				continue
			}
			if auditID == "" {
				errs = append(errs, cashflowID+constants.QuerryNoPendingAuditFound)
				continue
			}
			if requestedBy == userEmail {
				errs = append(errs, cashflowID+constants.QuerryMakerCheckerViolation)
				continue
			}
			if !strings.HasPrefix(status, "PENDING") {
				skipped++
				continue
			}
			bulkDeleteCFRow, _ := loadFDCashflowRow(ctx, pgxPool, cashflowID)
			if ok, pmsg := fdEnforceInline(ctx, r, pgxPool, enforceCtx{EventCode: common.TriggerPreDelete, HandlerName: "BulkDeleteCashflow",
				APIPath: "/investment/fd/master/cashflow/bulk-delete", SubModule: fdSubCashflow, EntityCode: bulkDeleteCFRow.EntityID, Actor: userEmail},
				buildFDCashflowPolicyFields(bulkDeleteCFRow)); !ok {
				errs = append(errs, cashflowID+": "+pmsg)
				continue
			}
			if table != "" {
				if _, execErr := pgxPool.Exec(ctx,
					fmt.Sprintf(`UPDATE %s SET is_deleted=true, is_active=false, updated_at=now(), updated_by=$1
					             WHERE cashflow_id=$2 AND fd_id=$3`, table),
					userEmail, cashflowID, fdID,
				); execErr != nil {
					api.LogError("[BulkDeleteCashflow] soft-delete failed cashflow=%s fd=%s: %v", cashflowID, fdID, execErr)
				}
			}
			if _, execErr := pgxPool.Exec(ctx,
				`UPDATE investment.fd_audit_cashflow_schedule
				 SET processing_status='DELETED', checker_by=$1, checker_at=now(), checker_comment=$2, checker_ip=$3
				 WHERE audit_id=$4`, api.SystemIfBlank(userEmail), req.Comment, api.SystemIfBlank(api.ClientIPFromContext(ctx)), auditID,
			); execErr != nil {
				api.LogError("[BulkDeleteCashflow] audit stamp failed audit=%s: %v", auditID, execErr)
			}
			deleted++
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"deleted": deleted, "skipped": skipped,
			"errors": errs, "checker": userEmail,
		})
		for _, cfID := range req.CashflowIDs {
			go func(id, uEmail string) {
				defer func() {
					if rec := recover(); rec != nil {
						api.LogError("[FDMaster] BulkDeleteCashflow notification panic for %s: %v", id, rec)
					}
				}()
				notifcatalog.TriggerNotification(context.Background(), pgxPool,
					"/investment/fd/master/cashflow/bulk-delete", id, map[string]interface{}{
						"record_id":   id,
						"event":       "FD_CASHFLOW_DELETE_APPROVED",
						"actor_email": uEmail,
					})
			}(cfID, userEmail)
		}
		api.LogInfo("[BulkDeleteCashflow] deleted=%d skipped=%d errors=%d by=%s", deleted, skipped, len(errs), userEmail)
	}
}

// ─── EditCashflowLineItem ─────────────────────────────────────────────────────
// POST /investment/fd/master/cashflow/edit
// Maker-checker edit of individual cashflow schedule line items.
// Editable: interest_accrued, tds_amount, net_cash_flow, period_days,
//
//	event_date, formula_used, accrual_rate_per_day, notes.
func EditCashflowLineItem(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID     string                 `json:"user_id"`
			FDID       string                 `json:"fd_id"`
			CashflowID string                 `json:"cashflow_id"`
			Fields     map[string]interface{} `json:"fields"`
			Reason     string                 `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.FDID == "" || req.CashflowID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "fd_id and cashflow_id are required")
			return
		}
		if len(req.Fields) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "fields map is required")
			return
		}

		userEmail := getUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()

		table := resolveFirstExistingTable(ctx, pgxPool, []string{
			constants.QuerryCashflowSchedule,
			constants.QuerryCashflow,
			constants.QuerryMasterCashflowSchedule,
		})
		if table == "" {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCashflowTableNotFound)
			return
		}

		schemaName, tableName := splitQualifiedTable(table)
		cols, err := loadTableColumns(ctx, pgxPool, schemaName, tableName)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to load table columns: "+err.Error())
			return
		}

		fdCol := pickFirstExistingColumn(cols, "fd_id", "master_id")
		cashflowIDCol := pickFirstExistingColumn(cols, "cashflow_id", "id")
		if fdCol == "" || cashflowIDCol == "" {
			api.RespondWithError(w, http.StatusInternalServerError, "required columns not found in cashflow table")
			return
		}

		// Map incoming field names to actual column names.
		allowedFields := map[string]string{
			"interest_accrued":     "interest_accrued",
			"interest_amount":      "interest_accrued",
			"tds_amount":           "tds_amount",
			"net_cash_flow":        "net_cash_flow",
			"net_cashflow":         "net_cash_flow",
			"period_days":          "period_days",
			"event_date":           "event_date",
			"cashflow_date":        "event_date",
			"formula_used":         "formula_used",
			"accrual_rate_per_day": "accrual_rate_per_day",
			"notes":                "notes",
			"override_reason":      "notes",
		}

		// Validate that at least one valid editable field was provided
		validFieldCount := 0
		for inputKey := range req.Fields {
			colName, ok := allowedFields[strings.ToLower(inputKey)]
			if ok && cols[colName] {
				validFieldCount++
			}
		}
		if validFieldCount == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "no valid editable fields found")
			return
		}

		// Verify the cashflow row exists before creating the audit entry
		var existsCheck int
		_ = pgxPool.QueryRow(ctx,
			fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE %s = $1 AND %s = $2`, table, cashflowIDCol, fdCol),
			req.CashflowID, req.FDID,
		).Scan(&existsCheck)
		if existsCheck == 0 {
			api.RespondWithError(w, http.StatusNotFound, "cashflow line item not found")
			return
		}

		// ── Block duplicate pending edits for the same cashflow_id ─────────────
		var existingPending int
		_ = pgxPool.QueryRow(ctx,
			`SELECT COUNT(*) FROM investment.fd_audit_cashflow_schedule
			 WHERE cashflow_id = $1 AND processing_status LIKE 'PENDING%'`,
			req.CashflowID,
		).Scan(&existingPending)
		if existingPending > 0 {
			api.RespondWithError(w, http.StatusConflict, "a pending edit already exists for this cashflow line — approve or reject it first")
			return
		}

		// ── Snapshot old values from the current cashflow row ───────────────
		type oldSnapshot struct {
			seqNumber    *int
			eventType    *string
			eventDate    *time.Time
			periodStart  *time.Time
			periodEnd    *time.Time
			periodDays   *int
			openingP     *float64
			interestAcc  *float64
			capAmt       *float64
			closingP     *float64
			tdsAmt       *float64
			netCF        *float64
			dayCountCode *string
			divisor      *int
			formulaUsed  *string
			accrualRate  *float64
			drAccCode    *string
			drAccName    *string
			crAccCode    *string
			crAccName    *string
			systemCalc   *bool
			isEdited     *bool
			bankConfirm  *bool
			bankConfDate *time.Time
			bankRef      *string
			voucherGen   *bool
			voucherNum   *string
			postStatus   *string
			remarks      *string
			isActive     *bool
			isDeleted    *bool
			receiptID    *string
			receiptClr   *bool
		}
		var snap oldSnapshot
		_ = pgxPool.QueryRow(ctx,
			fmt.Sprintf(`SELECT
				sequence_number, event_type, event_date, period_start_date, period_end_date, period_days,
				opening_principal, interest_accrued, capitalized_amount, closing_principal, tds_amount, net_cash_flow,
				day_count_code, divisor, formula_used, accrual_rate_per_day,
				dr_account_code, dr_account_name, cr_account_code, cr_account_name,
				system_calculated, is_edited,
				bank_confirmed, bank_confirmed_date, bank_reference,
				voucher_generated, voucher_number, posting_status,
				remarks, is_active, is_deleted,
				receipt_id, receipt_cleared
			FROM %s WHERE %s = $1 AND %s = $2`, table, cashflowIDCol, fdCol),
			req.CashflowID, req.FDID,
		).Scan(
			&snap.seqNumber, &snap.eventType, &snap.eventDate, &snap.periodStart, &snap.periodEnd, &snap.periodDays,
			&snap.openingP, &snap.interestAcc, &snap.capAmt, &snap.closingP, &snap.tdsAmt, &snap.netCF,
			&snap.dayCountCode, &snap.divisor, &snap.formulaUsed, &snap.accrualRate,
			&snap.drAccCode, &snap.drAccName, &snap.crAccCode, &snap.crAccName,
			&snap.systemCalc, &snap.isEdited,
			&snap.bankConfirm, &snap.bankConfDate, &snap.bankRef,
			&snap.voucherGen, &snap.voucherNum, &snap.postStatus,
			&snap.remarks, &snap.isActive, &snap.isDeleted,
			&snap.receiptID, &snap.receiptClr,
		)

		// ── Encode pending fields as JSON suffix in reason ───────────────────
		reasonWithFields := req.Reason
		if len(req.Fields) > 0 {
			pfJSON, _ := json.Marshal(req.Fields)
			reasonWithFields = req.Reason + constants.QuerryPendingFields + string(pfJSON)
		}

		// ── Read entity_id + principal for the approval engine ───────────────
		var entityID string
		var principalAmount float64
		_ = pgxPool.QueryRow(ctx,
			`SELECT COALESCE(entity_id,''), COALESCE(principal_amount,0)
			 FROM investment.fd_master WHERE fd_id = $1`, req.FDID,
		).Scan(&entityID, &principalAmount)

		editCashflowRow := fdCashflowRow{
			CashflowID: req.CashflowID, FDID: req.FDID, EntityID: entityID,
		}
		if snap.seqNumber != nil {
			editCashflowRow.SequenceNumber = *snap.seqNumber
		}
		if snap.eventType != nil {
			editCashflowRow.EventType = *snap.eventType
		}
		if snap.eventDate != nil {
			editCashflowRow.EventDate = snap.eventDate.Format(dateLayoutISO)
		}
		if snap.periodStart != nil {
			editCashflowRow.PeriodStartDate = snap.periodStart.Format(dateLayoutISO)
		}
		if snap.periodEnd != nil {
			editCashflowRow.PeriodEndDate = snap.periodEnd.Format(dateLayoutISO)
		}
		if snap.periodDays != nil {
			editCashflowRow.PeriodDays = *snap.periodDays
		}
		if snap.openingP != nil {
			editCashflowRow.OpeningPrincipal = *snap.openingP
		}
		if snap.interestAcc != nil {
			editCashflowRow.InterestAccrued = *snap.interestAcc
		}
		if snap.capAmt != nil {
			editCashflowRow.CapitalizedAmount = *snap.capAmt
		}
		if snap.closingP != nil {
			editCashflowRow.ClosingPrincipal = *snap.closingP
		}
		if snap.tdsAmt != nil {
			editCashflowRow.TDSAmount = *snap.tdsAmt
		}
		if snap.netCF != nil {
			editCashflowRow.NetCashFlow = *snap.netCF
		}
		if snap.dayCountCode != nil {
			editCashflowRow.DayCountCode = *snap.dayCountCode
		}
		if snap.divisor != nil {
			editCashflowRow.Divisor = *snap.divisor
		}
		if snap.formulaUsed != nil {
			editCashflowRow.FormulaUsed = *snap.formulaUsed
		}
		if snap.accrualRate != nil {
			editCashflowRow.AccrualRatePerDay = *snap.accrualRate
		}
		if snap.drAccCode != nil {
			editCashflowRow.DrAccountCode = *snap.drAccCode
		}
		if snap.drAccName != nil {
			editCashflowRow.DrAccountName = *snap.drAccName
		}
		if snap.crAccCode != nil {
			editCashflowRow.CrAccountCode = *snap.crAccCode
		}
		if snap.crAccName != nil {
			editCashflowRow.CrAccountName = *snap.crAccName
		}
		if snap.systemCalc != nil {
			editCashflowRow.SystemCalculated = *snap.systemCalc
		}
		if snap.isEdited != nil {
			editCashflowRow.IsEdited = *snap.isEdited
		}
		if snap.bankConfirm != nil {
			editCashflowRow.BankConfirmed = *snap.bankConfirm
		}
		if snap.bankConfDate != nil {
			editCashflowRow.BankConfirmedDate = snap.bankConfDate.Format(dateLayoutISO)
		}
		if snap.bankRef != nil {
			editCashflowRow.BankReference = *snap.bankRef
		}
		if snap.voucherGen != nil {
			editCashflowRow.VoucherGenerated = *snap.voucherGen
		}
		if snap.voucherNum != nil {
			editCashflowRow.VoucherNumber = *snap.voucherNum
		}
		if snap.postStatus != nil {
			editCashflowRow.PostingStatus = *snap.postStatus
		}
		if snap.remarks != nil {
			editCashflowRow.Remarks = *snap.remarks
		}
		if snap.receiptID != nil {
			editCashflowRow.ReceiptID = *snap.receiptID
		}
		if snap.receiptClr != nil {
			editCashflowRow.ReceiptCleared = *snap.receiptClr
		}
		if !fdEnforce(ctx, w, r, pgxPool, enforceCtx{EventCode: common.TriggerPreEdit, HandlerName: "EditCashflowLineItem",
			APIPath: "/investment/fd/master/cashflow/edit", SubModule: fdSubCashflow, EntityCode: entityID, Actor: userEmail},
			buildFDCashflowPolicyFields(editCashflowRow)) {
			return
		}

		// ── Insert audit row into fd_audit_cashflow_schedule ─────────────────
		var auditID string
		auditInsertErr := pgxPool.QueryRow(ctx, `
			INSERT INTO investment.fd_audit_cashflow_schedule (
				cashflow_id, fd_id,
				action_type, processing_status,
				reason,
				requested_by, requested_at, requested_ip,
				old_sequence_number, old_event_type, old_event_date,
				old_period_start_date, old_period_end_date, old_period_days,
				old_opening_principal, old_interest_accrued, old_capitalized_amount,
				old_closing_principal, old_tds_amount, old_net_cash_flow,
				old_day_count_code, old_divisor, old_formula_used, old_accrual_rate_per_day,
				old_dr_account_code, old_dr_account_name, old_cr_account_code, old_cr_account_name,
				old_system_calculated, old_is_edited,
				old_bank_confirmed, old_bank_confirmed_date, old_bank_reference,
				old_voucher_generated, old_voucher_number, old_posting_status,
				old_remarks, old_is_active, old_is_deleted,
				old_receipt_id, old_receipt_cleared
			) VALUES (
				$1,$2,'EDIT','PENDING_EDIT_APPROVAL',$3,
				$4,now(),$5,
				$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,
				$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,
				$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38
			) RETURNING audit_id`,
			req.CashflowID, req.FDID,
			reasonWithFields,
			api.SystemIfBlank(userEmail),
			api.SystemIfBlank(api.ClientIPFromContext(ctx)),
			snap.seqNumber, snap.eventType, snap.eventDate,
			snap.periodStart, snap.periodEnd, snap.periodDays,
			snap.openingP, snap.interestAcc, snap.capAmt,
			snap.closingP, snap.tdsAmt, snap.netCF,
			snap.dayCountCode, snap.divisor, snap.formulaUsed, snap.accrualRate,
			snap.drAccCode, snap.drAccName, snap.crAccCode, snap.crAccName,
			snap.systemCalc, snap.isEdited,
			snap.bankConfirm, snap.bankConfDate, snap.bankRef,
			snap.voucherGen, snap.voucherNum, snap.postStatus,
			snap.remarks, snap.isActive, snap.isDeleted,
			snap.receiptID, snap.receiptClr,
		).Scan(&auditID)

		if auditInsertErr != nil {
			api.LogError("[CashflowEdit] audit insert failed fd=%s cf=%s: %v", req.FDID, req.CashflowID, auditInsertErr)
			// Non-fatal: continue without audit trail
			auditID = ""
		}

		// ── Spawn approval engine goroutine ──────────────────────────────────
		if auditID != "" {
			go func(auditID, cashflowID, fdID, entityID, userID, email string, amount float64) {
				defer func() {
					if rec := recover(); rec != nil {
						api.LogError("[CashflowEdit] engine goroutine panic for audit %s: %v", auditID, rec)
					}
				}()
				bgCtx, bgCancel := context.WithTimeout(context.Background(), 2*time.Minute)
				defer bgCancel()
				instID, err := approvalengine.CreateInstance(bgCtx, pgxPool, approvalengine.InstanceRequest{
					ModuleCode:       "FIXED_DEPOSIT",
					EntityCode:       entityID,
					TransactionType:  "FD_CASHFLOW_EDIT",
					RecordID:         auditID,
					RecordTable:      constants.QuerryAuditCashflowSchedule,
					AuditTable:       constants.QuerryAuditCashflowSchedule,
					AuditIDColumn:    "audit_id",
					ActionType:       constants.AuditActionEdit,
					Amount:           amount,
					SubmittedBy:      userID,
					SubmittedByEmail: email,
				})
				if err != nil {
					api.LogError("[CashflowEdit] CreateInstance failed audit=%s: %v", auditID, err)
					return
				}
				if instID != "" {
					_, _ = pgxPool.Exec(bgCtx,
						`UPDATE investment.fd_audit_cashflow_schedule
						 SET processing_status='PENDING_EDIT_APPROVAL'
						 WHERE audit_id=$1`, auditID)
					api.LogInfo("[CashflowEdit] Engine instance %s created for audit %s", instID, auditID)
				} else {
					// No matrix configured — auto-approve immediately
					api.LogInfo("[CashflowEdit] No matrix for FD_CASHFLOW_EDIT — auto-approving audit %s", auditID)
					if applyErr := applyApprovedCashflowEdit(bgCtx, pgxPool, auditID, "system"); applyErr != nil {
						api.LogError("[CashflowEdit] Auto-approve apply failed audit=%s: %v", auditID, applyErr)
					}
				}
			}(auditID, req.CashflowID, req.FDID, entityID, req.UserID, userEmail, principalAmount)
		}

		go func(cfID, fdID, eID, uEmail string) {
			defer func() {
				if rec := recover(); rec != nil {
					api.LogError("[FDMaster] EditCashflowLineItem notification panic for %s: %v", cfID, rec)
				}
			}()
			notifcatalog.TriggerNotification(context.Background(), pgxPool,
				"/investment/fd/master/cashflow/edit", cfID, map[string]interface{}{
					"entity_id":   eID,
					"record_id":   cfID,
					"fd_id":       fdID,
					"event":       "FD_CASHFLOW_EDIT_SUBMITTED",
					"actor_email": uEmail,
				})
		}(req.CashflowID, req.FDID, entityID, userEmail)
		api.LogInfo("[FDMaster] EditCashflowLineItem submitted: fd=%s cashflow=%s audit=%s by=%s", req.FDID, req.CashflowID, auditID, userEmail)
		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"fd_id":        req.FDID,
			"cashflow_id":  req.CashflowID,
			"audit_id":     auditID,
			"status":       constants.StatusPendingEditApproval,
			"submitted_by": userEmail,
			"message":      "Edit submitted for approval. It will be applied once approved.",
		})
	}
}

// ─── applyApprovedCashflowEdit ────────────────────────────────────────────────
// Shared logic used by both ApproveCashflowEdit (checker path) and the
// auto-approve path (no matrix configured).
// It reads the audit row, applies the pending_fields to fd_cashflow_schedule,
// marks the audit as APPROVED, and propagates downstream principal changes.
func applyApprovedCashflowEdit(ctx context.Context, pool *pgxpool.Pool, auditID string, checkerEmail string) error {
	// 1. Read audit row
	var cashflowID, fdID, processingStatus, actionType, reasonRaw string
	err := pool.QueryRow(ctx,
		`SELECT cashflow_id, fd_id, processing_status, action_type, COALESCE(reason,'')
		 FROM investment.fd_audit_cashflow_schedule WHERE audit_id = $1`, auditID,
	).Scan(&cashflowID, &fdID, &processingStatus, &actionType, &reasonRaw)
	if err != nil {
		return fmt.Errorf("applyApprovedCashflowEdit: read audit row: %w", err)
	}

	// 2. Parse pending_fields JSON from reason suffix " | pending_fields:{json}"
	var pendingFields map[string]interface{}
	humanReason := reasonRaw
	const pfMarker = constants.QuerryPendingFields
	if idx := strings.Index(reasonRaw, pfMarker); idx >= 0 {
		humanReason = reasonRaw[:idx]
		pfJSON := reasonRaw[idx+len(pfMarker):]
		if jErr := json.Unmarshal([]byte(pfJSON), &pendingFields); jErr != nil {
			return fmt.Errorf("applyApprovedCashflowEdit: parse pending_fields: %w", jErr)
		}
	}
	if len(pendingFields) == 0 {
		// Nothing to apply — still mark approved
		_, _ = pool.Exec(ctx,
			`UPDATE investment.fd_audit_cashflow_schedule
			 SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_ip=$2
			 WHERE audit_id=$3`, api.SystemIfBlank(checkerEmail), api.SystemIfBlank(api.ClientIPFromContext(ctx)), auditID)
		return nil
	}

	// 3. Resolve cashflow table and columns
	table := resolveFirstExistingTable(ctx, pool, []string{
		constants.QuerryCashflowSchedule,
		constants.QuerryCashflow,
		constants.QuerryMasterCashflowSchedule,
	})
	if table == "" {
		return fmt.Errorf("applyApprovedCashflowEdit: cashflow table not found")
	}
	schemaName, tableName := splitQualifiedTable(table)
	cols, err := loadTableColumns(ctx, pool, schemaName, tableName)
	if err != nil {
		return fmt.Errorf("applyApprovedCashflowEdit: load cols: %w", err)
	}
	fdCol := pickFirstExistingColumn(cols, "fd_id", "master_id")
	cashflowIDCol := pickFirstExistingColumn(cols, "cashflow_id", "id")
	if fdCol == "" || cashflowIDCol == "" {
		return fmt.Errorf("applyApprovedCashflowEdit: required cols missing")
	}

	// 4. Build UPDATE SET from pending_fields
	allowedFields := map[string]string{
		"interest_accrued":     "interest_accrued",
		"interest_amount":      "interest_accrued",
		"tds_amount":           "tds_amount",
		"net_cash_flow":        "net_cash_flow",
		"net_cashflow":         "net_cash_flow",
		"period_days":          "period_days",
		"event_date":           "event_date",
		"cashflow_date":        "event_date",
		"formula_used":         "formula_used",
		"accrual_rate_per_day": "accrual_rate_per_day",
		"notes":                "notes",
		"override_reason":      "notes",
	}
	var sets []string
	var args []interface{}
	pos := 1
	for inputKey, inputVal := range pendingFields {
		colName, ok := allowedFields[strings.ToLower(inputKey)]
		if !ok || !cols[colName] {
			continue
		}
		sets = append(sets, fmt.Sprintf("%s = $%d", colName, pos))
		args = append(args, inputVal)
		pos++
	}
	if len(sets) == 0 {
		_, _ = pool.Exec(ctx,
			`UPDATE investment.fd_audit_cashflow_schedule
			 SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_ip=$2
			 WHERE audit_id=$3`, api.SystemIfBlank(checkerEmail), api.SystemIfBlank(api.ClientIPFromContext(ctx)), auditID)
		return nil
	}
	// Always mark is_edited = true + set edit audit columns on approval
	if cols["is_edited"] {
		sets = append(sets, fmt.Sprintf("is_edited = $%d", pos))
		args = append(args, true)
		pos++
	}
	if cols["edited_by"] {
		sets = append(sets, fmt.Sprintf("edited_by = $%d", pos))
		args = append(args, checkerEmail)
		pos++
	}
	if cols["edited_at"] {
		sets = append(sets, fmt.Sprintf("edited_at = $%d", pos))
		args = append(args, time.Now())
		pos++
	}
	if cols["edit_reason"] {
		sets = append(sets, fmt.Sprintf("edit_reason = $%d", pos))
		args = append(args, humanReason)
		pos++
	}
	if cols["last_modified_at"] {
		sets = append(sets, fmt.Sprintf("last_modified_at = $%d", pos))
		args = append(args, time.Now())
		pos++
	}
	if cols["last_modified_by"] {
		sets = append(sets, fmt.Sprintf("last_modified_by = $%d", pos))
		args = append(args, checkerEmail)
		pos++
	}

	updateSQL := fmt.Sprintf(
		"UPDATE %s SET %s WHERE %s = $%d AND %s = $%d",
		table, strings.Join(sets, ", "),
		cashflowIDCol, pos,
		fdCol, pos+1,
	)
	args = append(args, cashflowID, fdID)

	if _, upErr := pool.Exec(ctx, updateSQL, args...); upErr != nil {
		return fmt.Errorf("applyApprovedCashflowEdit: apply update: %w", upErr)
	}

	// 5. Mark audit row as APPROVED
	_, _ = pool.Exec(ctx,
		`UPDATE investment.fd_audit_cashflow_schedule
		 SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_ip=$2
		 WHERE audit_id=$3`, api.SystemIfBlank(checkerEmail), api.SystemIfBlank(api.ClientIPFromContext(ctx)), auditID)

	// 6. Propagate downstream principal changes
	propagateCashflowDownstream(ctx, pool, PropagateCashflowParams{
		Table:            table,
		FDCol:            fdCol,
		CashflowIDCol:    cashflowIDCol,
		Cols:             cols,
		FDID:             fdID,
		EditedCashflowID: cashflowID,
		AppliedFields:    pendingFields,
	})

	return nil
}

// propagateCashflowDownstream re-reads all cashflow rows for an FD after an
// edit was applied, then cascades opening/closing principal changes forward.
func propagateCashflowDownstream(
	ctx context.Context, pool *pgxpool.Pool,
	p PropagateCashflowParams,
) {
	// map struct fields to local variables to keep existing logic unchanged
	table := p.Table
	fdCol := p.FDCol
	cashflowIDCol := p.CashflowIDCol
	cols := p.Cols
	fdID := p.FDID
	editedCashflowID := p.EditedCashflowID
	appliedFields := p.AppliedFields
	seqCol := pickFirstExistingColumn(cols, "sequence_number", "period_number")
	openCol := pickFirstExistingColumn(cols, "opening_principal")
	closeCol := pickFirstExistingColumn(cols, "closing_principal")
	capCol := pickFirstExistingColumn(cols, "capitalized_amount")
	intCol := pickFirstExistingColumn(cols, "interest_accrued", "interest_amount")
	tdsCol := pickFirstExistingColumn(cols, "tds_amount")
	ncfCol := pickFirstExistingColumn(cols, "net_cash_flow", "net_cashflow", "net_amount")
	evtCol := pickFirstExistingColumn(cols, "event_type")
	if seqCol == "" || openCol == "" || closeCol == "" || evtCol == "" {
		return
	}

	cfRows, err := pool.Query(ctx,
		fmt.Sprintf(`SELECT %s, %s, COALESCE(%s,0), COALESCE(%s,0), COALESCE(%s,0), COALESCE(%s,0), COALESCE(%s,0), COALESCE(%s,0), %s
		             FROM %s WHERE %s = $1 ORDER BY %s`,
			cashflowIDCol, evtCol, openCol, closeCol, capCol, intCol, tdsCol, ncfCol, seqCol,
			table, fdCol, seqCol),
		fdID)
	if err != nil {
		return
	}
	type cfRow struct {
		id                                         string
		eventType                                  string
		openP, closeP, capAmt, intAmt, tdsAmt, ncf float64
		seq                                        int
	}
	var allRows []cfRow
	for cfRows.Next() {
		var r cfRow
		if scanErr := cfRows.Scan(&r.id, &r.eventType, &r.openP, &r.closeP, &r.capAmt, &r.intAmt, &r.tdsAmt, &r.ncf, &r.seq); scanErr != nil {
			cfRows.Close()
			return
		}
		allRows = append(allRows, r)
	}
	cfRows.Close()
	if cfRows.Err() != nil || len(allRows) == 0 {
		return
	}

	// Find the edited row and apply numeric overrides in-memory
	editedIdx := -1
	for i, r := range allRows {
		if r.id == editedCashflowID {
			editedIdx = i
			for k, v := range appliedFields {
				switch strings.ToLower(k) {
				case "interest_accrued", "interest_amount":
					if fv, ok2 := toFloat64(v); ok2 {
						allRows[i].intAmt = fv
					}
				case "tds_amount":
					if fv, ok2 := toFloat64(v); ok2 {
						allRows[i].tdsAmt = fv
					}
				case "capitalized_amount":
					if fv, ok2 := toFloat64(v); ok2 {
						allRows[i].capAmt = fv
					}
				}
			}
			switch strings.ToUpper(r.eventType) {
			case "CAPITALIZATION":
				allRows[i].capAmt = allRows[i].intAmt - allRows[i].tdsAmt
				allRows[i].closeP = allRows[i].openP + allRows[i].capAmt
				allRows[i].ncf = 0
			case "MATURITY":
				allRows[i].closeP = 0
				allRows[i].ncf = allRows[i].openP + allRows[i].intAmt - allRows[i].tdsAmt
			case "ACCRUAL":
				allRows[i].closeP = allRows[i].openP
				allRows[i].ncf = 0
			}
			break
		}
	}
	if editedIdx < 0 {
		return
	}

	// Propagate forward
	for i := editedIdx + 1; i < len(allRows); i++ {
		prevClose := allRows[i-1].closeP
		allRows[i].openP = prevClose
		switch strings.ToUpper(allRows[i].eventType) {
		case "CAPITALIZATION":
			allRows[i].capAmt = allRows[i].intAmt - allRows[i].tdsAmt
			allRows[i].closeP = prevClose + allRows[i].capAmt
			allRows[i].ncf = 0
		case "MATURITY":
			allRows[i].closeP = 0
			allRows[i].ncf = allRows[i].openP + allRows[i].intAmt - allRows[i].tdsAmt
		case "ACCRUAL", "TDS_DEDUCTION", "INTEREST_RECEIPT":
			allRows[i].closeP = prevClose
		}
	}

	// Persist from editedIdx onward
	for _, r := range allRows[editedIdx:] {
		if _, execErr := pool.Exec(ctx,
			fmt.Sprintf(`UPDATE %s SET %s=$1, %s=$2, %s=$3, %s=$4, %s=$5 WHERE %s=$6 AND %s=$7`,
				table, openCol, closeCol, capCol, intCol, ncfCol, cashflowIDCol, fdCol),
			r.openP, r.closeP, r.capAmt, r.intAmt, r.ncf, r.id, fdID,
		); execErr != nil {
			api.LogError("[CashflowPropagate] update failed cf=%s fd=%s: %v", r.id, fdID, execErr)
		}
	}
}

// PropagateCashflowParams groups parameters for propagateCashflowDownstream.
type PropagateCashflowParams struct {
	Table            string
	FDCol            string
	CashflowIDCol    string
	Cols             map[string]bool
	FDID             string
	EditedCashflowID string
	AppliedFields    map[string]interface{}
}

// ─── ApproveCashflowEdit ──────────────────────────────────────────────────────
// POST /investment/fd/master/cashflow/approve
// Checker approves a pending cashflow edit audit row.
func ApproveCashflowEdit(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID  string `json:"user_id"`
			AuditID string `json:"audit_id"`
			Comment string `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.AuditID == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrAuditIDRequired)
			return
		}
		userEmail := getUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		ctx := r.Context()

		// Read audit row
		var requestedBy, status, cashflowID, fdID string
		err := pgxPool.QueryRow(ctx,
			`SELECT requested_by, processing_status, cashflow_id, fd_id
			 FROM investment.fd_audit_cashflow_schedule WHERE audit_id = $1`, req.AuditID,
		).Scan(&requestedBy, &status, &cashflowID, &fdID)
		if err != nil {
			api.RespondWithError(w, http.StatusNotFound, constants.ErrAuditRecordNotFound)
			return
		}
		// Same-person check
		if requestedBy == userEmail {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrMakerCheckerSamePerson)
			return
		}
		if !strings.HasPrefix(status, "PENDING") {
			api.RespondWithError(w, http.StatusConflict, "audit record is not in a pending state (current: "+status+")")
			return
		}

		approveCFRow, _ := loadFDCashflowRow(ctx, pgxPool, cashflowID)
		approveCFFields := buildFDCashflowPolicyFields(approveCFRow)
		approveCFFields["audit_id"] = req.AuditID
		if !fdEnforce(ctx, w, r, pgxPool, enforceCtx{EventCode: common.TriggerPreApprove, HandlerName: "ApproveCashflowEdit",
			APIPath: "/investment/fd/master/cashflow/approve", SubModule: fdSubCashflow, EntityCode: approveCFRow.EntityID, Actor: userEmail},
			approveCFFields) {
			return
		}

		actionRes, actionErr := approvalengine.ActOnPendingOrDiagnose(ctx, pgxPool, approvalengine.ActOnPendingRequest{ModuleCode: "FIXED_DEPOSIT", RecordID: req.AuditID, UserID: req.UserID, UserEmail: userEmail, RoleID: "", Action: approvalengine.ActionApproved, Comment: req.Comment})
		if actionErr != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "approval engine error: "+actionErr.Error())
			return
		}
		if actionRes.Acted {
			// Engine path: record action
			// Check if instance is now fully APPROVED
			if actionRes.InstanceStatus == constants.StatusApproved {
				if applyErr := applyApprovedCashflowEdit(ctx, pgxPool, req.AuditID, userEmail); applyErr != nil {
					api.LogError("[CashflowApprove] apply failed audit=%s: %v", req.AuditID, applyErr)
					api.RespondWithError(w, http.StatusInternalServerError, "apply edit failed: "+applyErr.Error())
					return
				}
			}
			api.RespondWithPayload(w, true, "", map[string]interface{}{
				"audit_id": req.AuditID, "status": actionRes.InstanceStatus, "approved_by": userEmail,
			})
		} else {
			if actionRes.CancelledStale {
				api.LogInfo("[CashflowApprove] Cancelled stale approval instance for audit=%s: %s", req.AuditID, actionRes.Reason)
			} else if actionRes.Reason != "" {
				api.RespondWithError(w, http.StatusForbidden, actionRes.Reason)
				return
			}
			// No engine instance — direct approval
			if applyErr := applyApprovedCashflowEdit(ctx, pgxPool, req.AuditID, userEmail); applyErr != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "apply edit failed: "+applyErr.Error())
				return
			}
			api.RespondWithPayload(w, true, "", map[string]interface{}{
				"audit_id": req.AuditID, "status": constants.StatusApproved, "approved_by": userEmail,
			})
		}
		go func(cfID, fdID, uEmail string) {
			defer func() {
				if rec := recover(); rec != nil {
					api.LogError("[FDMaster] ApproveCashflowEdit notification panic for %s: %v", cfID, rec)
				}
			}()
			notifcatalog.TriggerNotification(context.Background(), pgxPool,
				"/investment/fd/master/cashflow/approve", cfID, map[string]interface{}{
					"record_id":   cfID,
					"fd_id":       fdID,
					"event":       "FD_CASHFLOW_EDIT_APPROVED",
					"actor_email": uEmail,
				})
		}(cashflowID, fdID, userEmail)
		api.LogInfo("[CashflowApprove] audit=%s cf=%s fd=%s by=%s", req.AuditID, cashflowID, fdID, userEmail)
	}
}

// ─── RejectCashflowEdit ───────────────────────────────────────────────────────
// POST /investment/fd/master/cashflow/reject
// Checker rejects a pending cashflow edit audit row.
func RejectCashflowEdit(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID  string `json:"user_id"`
			AuditID string `json:"audit_id"`
			Comment string `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.AuditID == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrAuditIDRequired)
			return
		}
		userEmail := getUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		ctx := r.Context()

		// Read audit row
		var requestedBy, status, cashflowID, fdID string
		err := pgxPool.QueryRow(ctx,
			`SELECT requested_by, processing_status, cashflow_id, fd_id
			 FROM investment.fd_audit_cashflow_schedule WHERE audit_id = $1`, req.AuditID,
		).Scan(&requestedBy, &status, &cashflowID, &fdID)
		if err != nil {
			api.RespondWithError(w, http.StatusNotFound, constants.ErrAuditRecordNotFound)
			return
		}
		// Same-person check
		if requestedBy == userEmail {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrMakerCheckerSamePerson)
			return
		}
		if !strings.HasPrefix(status, "PENDING") {
			api.RespondWithError(w, http.StatusConflict, "audit record is not in a pending state (current: "+status+")")
			return
		}

		rejectCFRow, _ := loadFDCashflowRow(ctx, pgxPool, cashflowID)
		rejectCFFields := buildFDCashflowPolicyFields(rejectCFRow)
		rejectCFFields["audit_id"] = req.AuditID
		if !fdEnforce(ctx, w, r, pgxPool, enforceCtx{EventCode: common.TriggerPreReject, HandlerName: "RejectCashflowEdit",
			APIPath: "/investment/fd/master/cashflow/reject", SubModule: fdSubCashflow, EntityCode: rejectCFRow.EntityID, Actor: userEmail},
			rejectCFFields) {
			return
		}

		actionRes, actionErr := approvalengine.ActOnPendingOrDiagnose(ctx, pgxPool, approvalengine.ActOnPendingRequest{ModuleCode: "FIXED_DEPOSIT", RecordID: req.AuditID, UserID: req.UserID, UserEmail: userEmail, RoleID: "", Action: approvalengine.ActionRejected, Comment: req.Comment})
		if actionErr != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "approval engine error: "+actionErr.Error())
			return
		}
		if actionRes.Acted {
			// Engine finalizer stamps audit row; also ensure our status is REJECTED
			if _, execErr := pgxPool.Exec(ctx,
				`UPDATE investment.fd_audit_cashflow_schedule
				 SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2, checker_ip=$3
				 WHERE audit_id=$4`, api.SystemIfBlank(userEmail), req.Comment, api.SystemIfBlank(api.ClientIPFromContext(ctx)), req.AuditID,
			); execErr != nil {
				api.LogError("[CashflowReject] audit stamp failed audit=%s: %v", req.AuditID, execErr)
			}
		} else {
			if actionRes.CancelledStale {
				api.LogInfo("[CashflowReject] Cancelled stale approval instance for audit=%s: %s", req.AuditID, actionRes.Reason)
			} else if actionRes.Reason != "" {
				api.RespondWithError(w, http.StatusForbidden, actionRes.Reason)
				return
			}
			// No engine — direct stamp
			if _, execErr := pgxPool.Exec(ctx,
				`UPDATE investment.fd_audit_cashflow_schedule
				 SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2, checker_ip=$3
				 WHERE audit_id=$4`, api.SystemIfBlank(userEmail), req.Comment, api.SystemIfBlank(api.ClientIPFromContext(ctx)), req.AuditID,
			); execErr != nil {
				api.LogError("[CashflowReject] audit stamp failed audit=%s: %v", req.AuditID, execErr)
			}
		}
		go func(cfID, fdID, uEmail string) {
			defer func() {
				if rec := recover(); rec != nil {
					api.LogError("[FDMaster] RejectCashflowEdit notification panic for %s: %v", cfID, rec)
				}
			}()
			notifcatalog.TriggerNotification(context.Background(), pgxPool,
				"/investment/fd/master/cashflow/reject", cfID, map[string]interface{}{
					"record_id":   cfID,
					"fd_id":       fdID,
					"event":       "FD_CASHFLOW_EDIT_REJECTED",
					"actor_email": uEmail,
				})
		}(cashflowID, fdID, userEmail)
		api.LogInfo("[CashflowReject] audit=%s cf=%s fd=%s by=%s", req.AuditID, cashflowID, fdID, userEmail)
		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"audit_id": req.AuditID, "status": constants.StatusRejected, "rejected_by": userEmail,
		})
	}
}

// ─── DeleteCashflowLineItem ───────────────────────────────────────────────────
// POST /investment/fd/master/cashflow/delete
// Soft-deletes a cashflow row (sets is_deleted=true) with maker-checker audit.
func DeleteCashflowLineItem(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID     string `json:"user_id"`
			FDID       string `json:"fd_id"`
			CashflowID string `json:"cashflow_id"`
			Reason     string `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.FDID == "" || req.CashflowID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "fd_id and cashflow_id are required")
			return
		}
		userEmail := getUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		ctx := r.Context()

		// Block duplicate pending deletes
		var existingPending int
		_ = pgxPool.QueryRow(ctx,
			`SELECT COUNT(*) FROM investment.fd_audit_cashflow_schedule
			 WHERE cashflow_id = $1 AND processing_status LIKE 'PENDING%'`,
			req.CashflowID,
		).Scan(&existingPending)
		if existingPending > 0 {
			api.RespondWithError(w, http.StatusConflict, "a pending action already exists for this cashflow line")
			return
		}

		table := resolveFirstExistingTable(ctx, pgxPool, []string{
			constants.QuerryCashflowSchedule,
			constants.QuerryCashflow,
			constants.QuerryMasterCashflowSchedule,
		})
		if table == "" {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCashflowTableNotFound)
			return
		}
		schemaName, tableName := splitQualifiedTable(table)
		cols, err := loadTableColumns(ctx, pgxPool, schemaName, tableName)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to load table columns")
			return
		}
		fdCol := pickFirstExistingColumn(cols, "fd_id", "master_id")
		cashflowIDCol := pickFirstExistingColumn(cols, "cashflow_id", "id")
		if fdCol == "" || cashflowIDCol == "" {
			api.RespondWithError(w, http.StatusInternalServerError, "required columns not found")
			return
		}

		// Snapshot old values for the audit row
		type oldSnap struct {
			seqNumber    *int
			eventType    *string
			eventDate    *time.Time
			periodStart  *time.Time
			periodEnd    *time.Time
			periodDays   *int
			openingP     *float64
			interestAcc  *float64
			capAmt       *float64
			closingP     *float64
			tdsAmt       *float64
			netCF        *float64
			dayCountCode *string
			divisor      *int
			formulaUsed  *string
			accrualRate  *float64
			drAccCode    *string
			drAccName    *string
			crAccCode    *string
			crAccName    *string
			systemCalc   *bool
			isEdited     *bool
			bankConfirm  *bool
			bankConfDate *time.Time
			bankRef      *string
			voucherGen   *bool
			voucherNum   *string
			postStatus   *string
			remarks      *string
			isActive     *bool
			isDeleted    *bool
			receiptID    *string
			receiptClr   *bool
		}
		var snap oldSnap
		_ = pgxPool.QueryRow(ctx,
			fmt.Sprintf(`SELECT
				sequence_number, event_type, event_date, period_start_date, period_end_date, period_days,
				opening_principal, interest_accrued, capitalized_amount, closing_principal, tds_amount, net_cash_flow,
				day_count_code, divisor, formula_used, accrual_rate_per_day,
				dr_account_code, dr_account_name, cr_account_code, cr_account_name,
				system_calculated, is_edited,
				bank_confirmed, bank_confirmed_date, bank_reference,
				voucher_generated, voucher_number, posting_status,
				remarks, is_active, is_deleted,
				receipt_id, receipt_cleared
			FROM %s WHERE %s = $1 AND %s = $2`, table, cashflowIDCol, fdCol),
			req.CashflowID, req.FDID,
		).Scan(
			&snap.seqNumber, &snap.eventType, &snap.eventDate, &snap.periodStart, &snap.periodEnd, &snap.periodDays,
			&snap.openingP, &snap.interestAcc, &snap.capAmt, &snap.closingP, &snap.tdsAmt, &snap.netCF,
			&snap.dayCountCode, &snap.divisor, &snap.formulaUsed, &snap.accrualRate,
			&snap.drAccCode, &snap.drAccName, &snap.crAccCode, &snap.crAccName,
			&snap.systemCalc, &snap.isEdited,
			&snap.bankConfirm, &snap.bankConfDate, &snap.bankRef,
			&snap.voucherGen, &snap.voucherNum, &snap.postStatus,
			&snap.remarks, &snap.isActive, &snap.isDeleted,
			&snap.receiptID, &snap.receiptClr,
		)

		deleteCFRow, _ := loadFDCashflowRow(ctx, pgxPool, req.CashflowID)
		if !fdEnforce(ctx, w, r, pgxPool, enforceCtx{EventCode: common.TriggerPreDelete, HandlerName: "DeleteCashflowLineItem",
			APIPath: "/investment/fd/master/cashflow/delete", SubModule: fdSubCashflow, EntityCode: deleteCFRow.EntityID, Actor: userEmail},
			buildFDCashflowPolicyFields(deleteCFRow)) {
			return
		}

		// Insert audit row for delete request
		var auditID string
		_ = pgxPool.QueryRow(ctx, `
			INSERT INTO investment.fd_audit_cashflow_schedule (
				cashflow_id, fd_id,
				action_type, processing_status,
				reason,
				requested_by, requested_at, requested_ip,
				old_sequence_number, old_event_type, old_event_date,
				old_period_start_date, old_period_end_date, old_period_days,
				old_opening_principal, old_interest_accrued, old_capitalized_amount,
				old_closing_principal, old_tds_amount, old_net_cash_flow,
				old_day_count_code, old_divisor, old_formula_used, old_accrual_rate_per_day,
				old_dr_account_code, old_dr_account_name, old_cr_account_code, old_cr_account_name,
				old_system_calculated, old_is_edited,
				old_bank_confirmed, old_bank_confirmed_date, old_bank_reference,
				old_voucher_generated, old_voucher_number, old_posting_status,
				old_remarks, old_is_active, old_is_deleted,
				old_receipt_id, old_receipt_cleared
			) VALUES (
				$1,$2,'DELETE','PENDING_DELETE_APPROVAL',$3,
				$4,now(),$5,
				$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,
				$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,
				$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38
			) RETURNING audit_id`,
			req.CashflowID, req.FDID,
			req.Reason, api.SystemIfBlank(userEmail), api.SystemIfBlank(api.ClientIPFromContext(ctx)),
			snap.seqNumber, snap.eventType, snap.eventDate,
			snap.periodStart, snap.periodEnd, snap.periodDays,
			snap.openingP, snap.interestAcc, snap.capAmt,
			snap.closingP, snap.tdsAmt, snap.netCF,
			snap.dayCountCode, snap.divisor, snap.formulaUsed, snap.accrualRate,
			snap.drAccCode, snap.drAccName, snap.crAccCode, snap.crAccName,
			snap.systemCalc, snap.isEdited,
			snap.bankConfirm, snap.bankConfDate, snap.bankRef,
			snap.voucherGen, snap.voucherNum, snap.postStatus,
			snap.remarks, snap.isActive, snap.isDeleted,
			snap.receiptID, snap.receiptClr,
		).Scan(&auditID)

		var cfPrincipal float64
		_ = pgxPool.QueryRow(ctx,
			`SELECT COALESCE(principal_amount,0) FROM investment.fd_master WHERE fd_id = $1`,
			req.FDID,
		).Scan(&cfPrincipal)

		if auditID != "" {
			go func(aID, cfID, fdID, eID, uID, uEmail string, amount float64) {
				defer func() {
					if rec := recover(); rec != nil {
						api.LogError("[FDMaster] DeleteCashflowLineItem engine panic for %s: %v", cfID, rec)
					}
				}()
				bgCtx, bgCancel := context.WithTimeout(context.Background(), 2*time.Minute)
				defer bgCancel()
				if err := approvalengine.CancelPendingInstances(bgCtx, pgxPool, "FIXED_DEPOSIT", aID, uEmail); err != nil {
					api.LogError("[FDMaster] DeleteCashflowLineItem CancelPendingInstances failed audit=%s: %v", aID, err)
					return
				}
				instID, err := approvalengine.CreateInstance(bgCtx, pgxPool, approvalengine.InstanceRequest{
					ModuleCode:       "FIXED_DEPOSIT",
					EntityCode:       eID,
					TransactionType:  "FD_CASHFLOW_DELETE",
					RecordID:         aID,
					RecordTable:      constants.QuerryAuditCashflowSchedule,
					AuditTable:       constants.QuerryAuditCashflowSchedule,
					AuditIDColumn:    "audit_id",
					ActionType:       "DELETE",
					Amount:           amount,
					SubmittedBy:      uID,
					SubmittedByEmail: uEmail,
				})
				if err != nil {
					api.LogError("[FDMaster] DeleteCashflowLineItem CreateInstance failed audit=%s: %v", aID, err)
					return
				}
				if instID != "" {
					api.LogInfo("[FDMaster] DeleteCashflowLineItem engine instance %s for audit %s", instID, aID)
				}
			}(auditID, req.CashflowID, req.FDID, deleteCFRow.EntityID, req.UserID, userEmail, cfPrincipal)
		}

		go func(cfID, fdID, eID, uEmail string) {
			defer func() {
				if rec := recover(); rec != nil {
					api.LogError("[FDMaster] DeleteCashflowLineItem notification panic for %s: %v", cfID, rec)
				}
			}()
			notifcatalog.TriggerNotification(context.Background(), pgxPool,
				"/investment/fd/master/cashflow/delete", cfID, map[string]interface{}{
					"entity_id":   eID,
					"record_id":   cfID,
					"fd_id":       fdID,
					"event":       "FD_CASHFLOW_DELETE_SUBMITTED",
					"actor_email": uEmail,
				})
		}(req.CashflowID, req.FDID, deleteCFRow.EntityID, userEmail)

		api.LogInfo("[CashflowDelete] submitted fd=%s cf=%s audit=%s by=%s", req.FDID, req.CashflowID, auditID, userEmail)
		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"fd_id":        req.FDID,
			"cashflow_id":  req.CashflowID,
			"audit_id":     auditID,
			"status":       constants.StatusPendingDeleteApproval,
			"submitted_by": userEmail,
			"message":      "Delete request submitted for approval.",
		})
	}
}

// ─── ApproveDeleteCashflow ────────────────────────────────────────────────────
// POST /investment/fd/master/cashflow/approve-delete
// Checker approves a pending delete request — actually soft-deletes the row.
func ApproveDeleteCashflow(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID  string `json:"user_id"`
			AuditID string `json:"audit_id"`
			Comment string `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.AuditID == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrAuditIDRequired)
			return
		}
		userEmail := getUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		ctx := r.Context()

		var requestedBy, status, cashflowID, fdID string
		err := pgxPool.QueryRow(ctx,
			`SELECT requested_by, processing_status, cashflow_id, fd_id
			 FROM investment.fd_audit_cashflow_schedule WHERE audit_id = $1`, req.AuditID,
		).Scan(&requestedBy, &status, &cashflowID, &fdID)
		if err != nil {
			api.RespondWithError(w, http.StatusNotFound, constants.ErrAuditRecordNotFound)
			return
		}
		if requestedBy == userEmail {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrMakerCheckerSamePerson)
			return
		}
		if !strings.HasPrefix(status, "PENDING") {
			api.RespondWithError(w, http.StatusConflict, "audit record is not pending (current: "+status+")")
			return
		}

		table := resolveFirstExistingTable(ctx, pgxPool, []string{
			constants.QuerryCashflowSchedule,
			constants.QuerryCashflow,
			constants.QuerryMasterCashflowSchedule,
		})
		if table == "" {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCashflowTableNotFound)
			return
		}

		approveDelCFRow, _ := loadFDCashflowRow(ctx, pgxPool, cashflowID)
		approveDelCFFields := buildFDCashflowPolicyFields(approveDelCFRow)
		approveDelCFFields["audit_id"] = req.AuditID
		if !fdEnforce(ctx, w, r, pgxPool, enforceCtx{EventCode: common.TriggerPreApprove, HandlerName: "ApproveDeleteCashflow",
			APIPath: "/investment/fd/master/cashflow/approve-delete", SubModule: fdSubCashflow, EntityCode: approveDelCFRow.EntityID, Actor: userEmail},
			approveDelCFFields) {
			return
		}

		actionRes, actionErr := approvalengine.ActOnPendingOrDiagnose(ctx, pgxPool, approvalengine.ActOnPendingRequest{
			ModuleCode: "FIXED_DEPOSIT", RecordID: req.AuditID,
			UserID: req.UserID, UserEmail: userEmail, RoleID: "",
			Action: approvalengine.ActionApproved, Comment: firstNonEmpty(req.Comment, "Cashflow delete approved"),
		})
		if actionErr != nil {
			api.LogError("[CashflowApproveDelete] ActOnPendingOrDiagnose failed audit=%s: %v", req.AuditID, actionErr)
		}
		if actionRes.Acted {
			if actionRes.InstanceStatus == constants.StatusApproved {
				if _, execErr := pgxPool.Exec(ctx,
					fmt.Sprintf(`UPDATE %s SET is_deleted=true, is_active=false, updated_at=now(), updated_by=$1
					             WHERE cashflow_id=$2 AND fd_id=$3`, table),
					userEmail, cashflowID, fdID,
				); execErr != nil {
					api.LogError("[CashflowApproveDelete] soft-delete (engine) failed audit=%s cf=%s: %v", req.AuditID, cashflowID, execErr)
				}
				if _, execErr := pgxPool.Exec(ctx,
					`UPDATE investment.fd_audit_cashflow_schedule
					 SET processing_status='DELETED', checker_by=$1, checker_at=now(), checker_comment=$2, checker_ip=$3
					 WHERE audit_id=$4`, api.SystemIfBlank(userEmail), req.Comment, api.SystemIfBlank(api.ClientIPFromContext(ctx)), req.AuditID,
				); execErr != nil {
					api.LogError("[CashflowApproveDelete] audit stamp (engine) failed audit=%s: %v", req.AuditID, execErr)
				}
			}
		} else {
			if actionRes.CancelledStale {
				api.LogInfo("[CashflowApproveDelete] Cancelled stale approval instance for audit=%s: %s", req.AuditID, actionRes.Reason)
			} else if actionRes.Reason != "" {
				api.RespondWithError(w, http.StatusForbidden, actionRes.Reason)
				return
			}
			// No matrix — direct soft-delete + stamp
			if _, execErr := pgxPool.Exec(ctx,
				fmt.Sprintf(`UPDATE %s SET is_deleted=true, is_active=false, updated_at=now(), updated_by=$1
				             WHERE cashflow_id=$2 AND fd_id=$3`, table),
				userEmail, cashflowID, fdID,
			); execErr != nil {
				api.LogError("[CashflowApproveDelete] soft-delete failed audit=%s cf=%s: %v", req.AuditID, cashflowID, execErr)
			}
			if _, execErr := pgxPool.Exec(ctx,
				`UPDATE investment.fd_audit_cashflow_schedule
				 SET processing_status='DELETED', checker_by=$1, checker_at=now(), checker_comment=$2, checker_ip=$3
				 WHERE audit_id=$4`, api.SystemIfBlank(userEmail), req.Comment, api.SystemIfBlank(api.ClientIPFromContext(ctx)), req.AuditID,
			); execErr != nil {
				api.LogError("[CashflowApproveDelete] audit stamp failed audit=%s: %v", req.AuditID, execErr)
			}
		}

		go func(cfID, fdID, uEmail string) {
			defer func() {
				if rec := recover(); rec != nil {
					api.LogError("[FDMaster] ApproveDeleteCashflow notification panic for %s: %v", cfID, rec)
				}
			}()
			notifcatalog.TriggerNotification(context.Background(), pgxPool,
				"/investment/fd/master/cashflow/approve-delete", cfID, map[string]interface{}{
					"record_id":   cfID,
					"fd_id":       fdID,
					"event":       "FD_CASHFLOW_DELETE_APPROVED",
					"actor_email": uEmail,
				})
		}(cashflowID, fdID, userEmail)

		api.LogInfo("[CashflowApproveDelete] audit=%s cf=%s fd=%s by=%s", req.AuditID, cashflowID, fdID, userEmail)
		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"audit_id": req.AuditID, "cashflow_id": cashflowID, "status": "DELETED", "approved_by": userEmail,
		})
	}
}
