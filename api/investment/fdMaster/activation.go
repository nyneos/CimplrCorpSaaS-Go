package fdMaster

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/approvalengine"
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/constants"
	notifcatalog "CimplrCorpSaas/api/notification/catalog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type queryExecutor interface {
	QueryRow(ctx context.Context, sql string, arguments ...interface{}) pgx.Row
	Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, arguments ...interface{}) (pgx.Rows, error)
}

type activateFDRequest struct {
	UserID         string `json:"user_id"`
	ConfirmationID string `json:"confirmation_id"`
	FDNumber       string `json:"fd_number"`
	ReceiptDate    string `json:"receipt_date"`
	Notes          string `json:"notes"`
}

type bulkFDActionRequest struct {
	UserID  string   `json:"user_id"`
	FDIDs   []string `json:"fd_ids"`
	Comment string   `json:"comment"`
}

func getUserEmail(userID string) string {
	for _, s := range auth.GetActiveSessions() {
		if s.UserID == userID {
			return s.Email
		}
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

// ─────────────────────────────────────────────────────────────────────────────
// Schema-discovery cache
// ─────────────────────────────────────────────────────────────────────────────
// information_schema queries are expensive (each one is a separate network
// round-trip to Supabase/Postgres).  ActivateFD previously made 10-13 of these
// per request, adding 0.5–2.5 s of latency.
//
// Tables and columns never change at runtime, so we cache every result in a
// sync.Map keyed by "schema.table" (column map) and "schema.table:exists"
// (bool).  First call populates; all subsequent calls hit RAM — zero DB cost.
// ─────────────────────────────────────────────────────────────────────────────

var (
	schemaColCache   sync.Map // "schema.table" → map[string]bool
	schemaExistCache sync.Map // "schema.table" → bool
)

func loadTableColumns(ctx context.Context, exec queryExecutor, schemaName string, tableName string) (map[string]bool, error) {
	cacheKey := schemaName + "." + tableName

	// Fast path — already cached
	if cached, ok := schemaColCache.Load(cacheKey); ok {
		return cached.(map[string]bool), nil
	}

	// Slow path — query information_schema once
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
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Only cache non-empty results so a transient error doesn't poison the cache
	if len(cols) > 0 {
		schemaColCache.Store(cacheKey, cols)
	}
	return cols, nil
}

func resolveFirstExistingTable(ctx context.Context, exec queryExecutor, candidates []string) string {
	for _, candidate := range candidates {
		schemaName, tableName := splitQualifiedTable(candidate)
		cacheKey := schemaName + "." + tableName + ":exists"

		// Fast path — already cached
		if cached, ok := schemaExistCache.Load(cacheKey); ok {
			if cached.(bool) {
				return candidate
			}
			continue
		}

		// Slow path — query information_schema once
		var exists bool
		err := exec.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM information_schema.tables
				WHERE table_schema = $1 AND table_name = $2
			)
		`, schemaName, tableName).Scan(&exists)
		if err == nil {
			schemaExistCache.Store(cacheKey, exists)
		}
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
		"INSERT INTO %s (%s) VALUES (%s)",
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
	if strings.Contains(errStr, "fd_master_fdno_not_empty") {
		return "FD number / certificate number is required.", http.StatusOK
	}
	if strings.Contains(errStr, "duplicate key") || strings.Contains(errStr, "unique constraint") {
		return "Duplicate entry detected. Please check for existing records.", http.StatusOK
	}
	if strings.Contains(errStr, "invalid") || strings.Contains(errStr, "required") {
		return err.Error(), http.StatusBadRequest
	}
	return contextMessage + ": " + err.Error(), http.StatusInternalServerError
}

func resolveFDAuditTable(ctx context.Context, exec queryExecutor) string {
	return resolveFirstExistingTable(ctx, exec, []string{
		"investment.fd_audit_master",
		"investment.fd_audit_fd_master",
		"investment.fd_master_audit",
	})
}

func insertFDAuditRecord(ctx context.Context, exec queryExecutor, auditTable string, refID string, userEmail string, actionType string, processingStatus string, reason string) error {
	if auditTable == "" || refID == "" {
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

	valueMap := map[string]interface{}{
		refCol:              refID,
		"action_type":       actionType,
		"processing_status": processingStatus,
		"requested_by":      userEmail,
		"requested_at":      time.Now(),
		"reason":            reason,
	}
	preferred := []string{refCol, "action_type", "processing_status", "reason", "requested_by", "requested_at"}
	insertSQL, args, _, ok := buildDynamicInsert(auditTable, cols, preferred, valueMap, nil)
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

	query := fmt.Sprintf(
		"UPDATE %s SET processing_status = $1, checker_by = $2, checker_at = now(), checker_comment = $3 WHERE %s = ANY($4::text[]) AND processing_status LIKE '%%PENDING%%'",
		auditTable, refCol,
	)
	_, err = exec.Exec(ctx, query, status, userEmail, comment, fdIDs)
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

	valueMap := map[string]interface{}{
		// identity
		"confirmation_id":    rec.ConfirmationID,
		"booking_id":         rec.BookingID,
		// entity
		"entity_id":          rec.EntityID,
		"entity_name":        entityName,
		// bank
		"bank_id":            rec.BankID,
		"bank_name":          bankName,
		// account — actual column is source_account_id
		"source_account_id":  rec.BankAccountID,
		"bank_account_id":    rec.BankAccountID, // fallback if schema uses old name
		// financials
		"principal_amount":   rec.PrincipalAmount,
		"interest_rate":      rec.InterestRate,
		"interest_type_code": interestTypeCode,
		// dates — actual column is start_date
		"start_date":         rec.ValueDate,
		"value_date":         rec.ValueDate, // fallback
		"maturity_date":      rec.MaturityDate,
		// tenor — actual column is tenure_days
		"tenure_days":        rec.TenorDays,
		"tenor_days":         rec.TenorDays, // fallback
		// optional
		"frequency_id":       rec.FrequencyID,
		"bank_config_id":     rec.BankConfigID,
		"tds_plan_id":        rec.TDSPlanID,
		"day_count_code":     rec.DayCountConvention,
		// fd reference — NOT NULL in schema
		"bank_fd_ref_no":     fdRef,
		"bank_fd_reference":  fdRef,
		"fd_number":          fdRef,
		"fd_no":              fdRef,
		"certificate_number": fdRef,
		// status
		"status":             statusValue,
		"fd_status":          statusValue,
		// audit
		"created_by":         createdBy,
		"notes":              notes,
		"is_deleted":         false,
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
		"frequency_id", "bank_config_id", "tds_plan_id", "day_count_code",
		"bank_fd_ref_no", "fd_number", "fd_no", "certificate_number", "bank_fd_reference",
		"status", "fd_status",
		"created_by", "notes", "is_deleted",
	}
	insertSQL, args, returningCol, ok := buildDynamicInsert("investment.fd_master", masterCols, preferred, valueMap, []string{"fd_id", "master_id", "confirmation_id"})
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

func updateFDMasterStatus(ctx context.Context, exec queryExecutor, fdIDs []string, targetStatus string) error {
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

	query := fmt.Sprintf("UPDATE investment.fd_master SET %s = $1 WHERE %s = ANY($2::text[]) AND COALESCE(is_deleted,false)=false", statusCol, keyCol)
	_, err = exec.Exec(ctx, query, targetStatus, fdIDs)
	return err
}

// insertCashflowAuditRowsApproved writes one APPROVED audit entry per cashflow row
// into fd_audit_cashflow_schedule. Called at FD activation to seed the audit trail.
// The cashflow_id is read back from the just-inserted rows using the cashflow table.
func insertCashflowAuditRowsApproved(ctx context.Context, exec queryExecutor, fdID string, rows []CashflowRow, createdBy string) error {
	table := resolveFirstExistingTable(ctx, exec, []string{
		"investment.fd_cashflow_schedule",
		"investment.fd_cashflow",
		"investment.fd_master_cashflow_schedule",
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

	if len(cfList) == 0 {
		return nil
	}

	// Batch INSERT all audit rows in one round-trip.
	reason := "System generated at FD activation"
	var valueTuples []string
	var args []interface{}
	paramIdx := 1
	for _, cf := range cfList {
		valueTuples = append(valueTuples, fmt.Sprintf(
			"($%d,$%d,'CREATE','APPROVED',$%d,$%d,now(),$%d,now(),'Auto-approved at FD activation')",
			paramIdx, paramIdx+1, paramIdx+2, paramIdx+3, paramIdx+3,
		))
		args = append(args, cf.id, fdID, reason, createdBy)
		paramIdx += 4
	}
	_, insErr := exec.Exec(ctx, fmt.Sprintf(`
		INSERT INTO investment.fd_audit_cashflow_schedule (
			cashflow_id, fd_id,
			action_type, processing_status,
			reason,
			requested_by, requested_at,
			checker_by, checker_at, checker_comment
		) VALUES %s
		ON CONFLICT DO NOTHING`, strings.Join(valueTuples, ",")), args...)
	return insErr
}

func ActivateFD(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		t0 := time.Now()
		var req activateFDRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if strings.TrimSpace(req.ConfirmationID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "confirmation_id is required")
			return
		}

		api.LogInfo("[FDActivate][%s] ► request received user=%s", req.ConfirmationID, req.UserID)

		userEmail := getUserEmail(req.UserID)
		if userEmail == "" {
			api.LogError("[FDActivate][%s] ✗ getUserEmail returned empty for user_id=%q — session missing?", req.ConfirmationID, req.UserID)
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		api.LogInfo("[FDActivate][%s] ✓ session resolved email=%s (+%s)", req.ConfirmationID, userEmail, time.Since(t0).Round(time.Millisecond))

		ctx := r.Context()

		// ── PHASE 1: ALL READS — done outside any transaction ─────────────────
		// We use pgxPool directly so none of these queries hold a tx connection
		// open. This keeps the transaction that follows as short as possible and
		// avoids Supabase statement-timeout errors caused by long-lived txns.

		t1 := time.Now()
		rec, err := loadFDRecord(ctx, pgxPool, req.ConfirmationID)
		if err != nil {
			api.LogError("[FDActivate][%s] ✗ loadFDRecord failed after %s: %v", req.ConfirmationID, time.Since(t1).Round(time.Millisecond), err)
			msg, status := getFDMasterError(err, "Load confirmation failed")
			api.RespondWithError(w, status, msg)
			return
		}
		api.LogInfo("[FDActivate][%s] ✓ loadFDRecord (+%s) entity=%s bank=%s principal=%.2f tenor=%v",
			req.ConfirmationID, time.Since(t1).Round(time.Millisecond),
			rec.EntityID, rec.BankID, rec.PrincipalAmount, rec.TenorDays)

		receiptDate := parseDateOrDefault(req.ReceiptDate, rec.ReceiptDate)

		// Duplicate-check outside tx — optimistic, re-checked inside tx below.
		t2 := time.Now()
		var existingFDIDCheck string
		_ = pgxPool.QueryRow(ctx,
			`SELECT COALESCE(fd_id,'') FROM investment.fd_master
			 WHERE confirmation_id = $1 AND COALESCE(is_deleted,false)=false LIMIT 1`,
			req.ConfirmationID,
		).Scan(&existingFDIDCheck)
		if existingFDIDCheck != "" {
			api.LogInfo("[FDActivate][%s] ✗ already activated (pre-tx check) as %s", req.ConfirmationID, existingFDIDCheck)
			api.RespondWithError(w, http.StatusConflict,
				fmt.Sprintf("FD already activated for this confirmation: %s", existingFDIDCheck))
			return
		}
		api.LogInfo("[FDActivate][%s] ✓ duplicate-check clear (+%s)", req.ConfirmationID, time.Since(t2).Round(time.Millisecond))

		// Resolve audit table name outside tx.
		t3 := time.Now()
		auditTable := resolveFDAuditTable(ctx, pgxPool)
		api.LogInfo("[FDActivate][%s] ✓ resolveFDAuditTable=%s (+%s)", req.ConfirmationID, auditTable, time.Since(t3).Round(time.Millisecond))

		// Generate cashflow schedule outside tx — pure computation + config reads.
		t4 := time.Now()
		cashflows, _, err := GenerateCashflowFromRecord(ctx, pgxPool, rec)
		if err != nil {
			api.LogError("[FDActivate][%s] ✗ GenerateCashflowFromRecord failed after %s: %v", req.ConfirmationID, time.Since(t4).Round(time.Millisecond), err)
			msg, status := getFDMasterError(err, "Cashflow generation failed")
			api.RespondWithError(w, status, msg)
			return
		}
		api.LogInfo("[FDActivate][%s] ✓ GenerateCashflowFromRecord → %d rows (+%s)", req.ConfirmationID, len(cashflows), time.Since(t4).Round(time.Millisecond))

		// ── PHASE 2: SHORT TRANSACTION — writes only ──────────────────────────
		// At this point all data is ready in memory. The transaction only does:
		//   1. Re-check duplicate (serialisation safety)
		//   2. INSERT fd_master
		//   3. INSERT fd_audit_master
		//   4. Batch INSERT cashflow rows
		//   5. Batch INSERT cashflow audit rows
		//   6. UPDATE cashflow_generated flag
		//   7. COMMIT
		// Target wall-clock: < 2 seconds.

		tTx := time.Now()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.LogError("[FDActivate][%s] ✗ tx.Begin failed: %v", req.ConfirmationID, err)
			msg, status := getFDMasterError(err, "Transaction begin failed")
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx) //nolint:errcheck
		api.LogInfo("[FDActivate][%s] ✓ tx.Begin (+%s)", req.ConfirmationID, time.Since(tTx).Round(time.Millisecond))

		// Serialisation safety re-check inside tx.
		var existingFDID string
		_ = tx.QueryRow(ctx,
			`SELECT COALESCE(fd_id,'') FROM investment.fd_master
			 WHERE confirmation_id = $1 AND COALESCE(is_deleted,false)=false LIMIT 1`,
			req.ConfirmationID,
		).Scan(&existingFDID)
		if existingFDID != "" {
			api.LogInfo("[FDActivate][%s] ✗ already activated (in-tx check) as %s", req.ConfirmationID, existingFDID)
			api.RespondWithError(w, http.StatusConflict,
				fmt.Sprintf("FD already activated for this confirmation: %s", existingFDID))
			return
		}

		t5 := time.Now()
		fdID, _, err := insertFDMaster(ctx, tx, rec, req.FDNumber, receiptDate, req.Notes, userEmail)
		if err != nil {
			api.LogError("[FDActivate][%s] ✗ insertFDMaster failed after %s: %v", req.ConfirmationID, time.Since(t5).Round(time.Millisecond), err)
			msg, status := getFDMasterError(err, "FD activation failed")
			api.RespondWithError(w, status, msg)
			return
		}
		api.LogInfo("[FDActivate][%s] ✓ insertFDMaster → fd_id=%s (+%s)", req.ConfirmationID, fdID, time.Since(t5).Round(time.Millisecond))

		t6 := time.Now()
		if err := insertFDAuditRecord(ctx, tx, auditTable, fdID, userEmail, "CREATE", "PENDING_APPROVAL", ""); err != nil {
			api.LogError("[FDActivate][%s] ✗ insertFDAuditRecord failed after %s: %v", req.ConfirmationID, time.Since(t6).Round(time.Millisecond), err)
			msg, status := getFDMasterError(err, constants.ErrAuditInsertFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		api.LogInfo("[FDActivate][%s] ✓ insertFDAuditRecord (+%s)", req.ConfirmationID, time.Since(t6).Round(time.Millisecond))

		t7 := time.Now()
		if err := SaveCashflowScheduleWithRecord(ctx, tx, fdID, cashflows, userEmail, rec); err != nil {
			api.LogError("[FDActivate][%s] ✗ SaveCashflowSchedule failed after %s: %v", req.ConfirmationID, time.Since(t7).Round(time.Millisecond), err)
			msg, status := getFDMasterError(err, "Cashflow save failed")
			api.RespondWithError(w, status, msg)
			return
		}
		api.LogInfo("[FDActivate][%s] ✓ SaveCashflowSchedule (+%s)", req.ConfirmationID, time.Since(t7).Round(time.Millisecond))

		t8 := time.Now()
		if auditErr := insertCashflowAuditRowsApproved(ctx, tx, fdID, cashflows, userEmail); auditErr != nil {
			api.LogError("[FDActivate][%s] ✗ cashflow audit insert failed after %s: %v", req.ConfirmationID, time.Since(t8).Round(time.Millisecond), auditErr)
		} else {
			api.LogInfo("[FDActivate][%s] ✓ insertCashflowAuditRows (+%s)", req.ConfirmationID, time.Since(t8).Round(time.Millisecond))
		}

		// Mark cashflow_generated=true so the accrual engine picks this FD up in scope.
		t9 := time.Now()
		if _, err := tx.Exec(ctx,
			`UPDATE investment.fd_master SET cashflow_generated=true, cashflow_generated_at=now() WHERE fd_id=$1`,
			fdID,
		); err != nil {
			api.LogError("[FDActivate][%s] ✗ cashflow flag update failed after %s: %v", req.ConfirmationID, time.Since(t9).Round(time.Millisecond), err)
			msg, status := getFDMasterError(err, "Cashflow flag update failed")
			api.RespondWithError(w, status, msg)
			return
		}
		api.LogInfo("[FDActivate][%s] ✓ cashflow_generated flag (+%s)", req.ConfirmationID, time.Since(t9).Round(time.Millisecond))

		t10 := time.Now()
		if err := tx.Commit(ctx); err != nil {
			api.LogError("[FDActivate][%s] ✗ tx.Commit failed after %s: %v", req.ConfirmationID, time.Since(t10).Round(time.Millisecond), err)
			msg, status := getFDMasterError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}
		api.LogInfo("[FDActivate][%s] ✓ tx.Commit (+%s) | TOTAL SYNC=%s", req.ConfirmationID, time.Since(t10).Round(time.Millisecond), time.Since(t0).Round(time.Millisecond))

		go func(fdRecordID string, confirmationID string, email string, entityID string, amount float64, req activateFDRequest) {
			defer func() {
				if rec := recover(); rec != nil {
					api.LogError("[FDMaster] ActivateFD engine goroutine panic for fd %s: %v", fdRecordID, rec)
				}
			}()
			bgCtx := context.Background()
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
				RecordTable:      "investment.fd_master",
				AuditTable:       auditTableName,
				AuditIDColumn:    auditIDColumn,
				ActionType:       "CREATE",
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
	return func(w http.ResponseWriter, r *http.Request) {
		var req bulkFDActionRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.FDIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "fd_ids are required")
			return
		}

		userEmail := getUserEmail(req.UserID)
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

			// Try engine path first.
			var instanceEyeID string
			engineErr := pgxPool.QueryRow(ctx, `
				SELECT ie.instance_eye_id
				FROM uam.approval_instance i
				JOIN uam.approval_instance_eye ie ON ie.instance_id = i.instance_id AND ie.status = 'ACTIVE'
				JOIN uam.approval_matrix_eye_member m ON m.eye_id = ie.matrix_eye_id
					AND m.member_type = 'APPROVER' AND m.is_active = true AND m.is_deleted = false
					AND m.assignment_type IN ('USER_ONLY','ROLE_USER') AND m.user_id = $2
				WHERE i.record_id = $1 AND i.module_code = 'FIXED_DEPOSIT' AND i.status = 'PENDING'
				ORDER BY ie.position ASC LIMIT 1`, fdID, req.UserID,
			).Scan(&instanceEyeID)

			if engineErr == nil && instanceEyeID != "" {
				if err := approvalengine.RecordAction(ctx, pgxPool, approvalengine.ActionRequest{
					InstanceEyeID: instanceEyeID,
					ActorUserID:   req.UserID,
					ActorEmail:    userEmail,
					ActionType:    approvalengine.ActionApproved,
					Comment:       firstNonEmpty(req.Comment, "Bulk approved FD activation"),
				}); err != nil {
					api.LogError("[FDMaster] RecordAction approve failed for fd %s: %v", fdID, err)
					errors = append(errors, fdID+": "+err.Error())
					continue
				}
				// Check if fully approved (last eye) — if so, set ACTIVE and create journals.
				var instStatus string
				_ = pgxPool.QueryRow(ctx, `SELECT i.status FROM uam.approval_instance i
					JOIN uam.approval_instance_eye ie ON ie.instance_id = i.instance_id
					WHERE ie.instance_eye_id = $1`, instanceEyeID).Scan(&instStatus)
				if instStatus == "APPROVED" {
					tx, txErr := pgxPool.Begin(ctx)
					if txErr != nil {
						errors = append(errors, fdID+": post-approval tx begin failed")
						engineActed++
						continue
					}
					auditTable := resolveFDAuditTable(ctx, tx)
					_ = updateFDMasterStatus(ctx, tx, []string{fdID}, "ACTIVE")
					_ = updateFDAuditStatus(ctx, tx, auditTable, []string{fdID}, userEmail, "APPROVED", req.Comment)
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
				var anyInstance int
				_ = pgxPool.QueryRow(ctx, `SELECT COUNT(*) FROM uam.approval_instance
					WHERE record_id=$1 AND module_code='FIXED_DEPOSIT' AND status='PENDING'`, fdID).Scan(&anyInstance)
				if anyInstance > 0 {
					errors = append(errors, fdID+": not your turn in approval sequence")
					continue
				}
				// No matrix — direct stamp + ACTIVE + journals.
				tx, err := pgxPool.Begin(ctx)
				if err != nil {
					errors = append(errors, fdID+": tx begin failed")
					continue
				}
				auditTable := resolveFDAuditTable(ctx, tx)
				if err := updateFDAuditStatus(ctx, tx, auditTable, []string{fdID}, userEmail, "APPROVED", req.Comment); err != nil {
					_ = tx.Rollback(ctx)
					errors = append(errors, fdID+": audit update failed")
					continue
				}
				if err := updateFDMasterStatus(ctx, tx, []string{fdID}, "ACTIVE"); err != nil {
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
				if cerr := tx.Commit(ctx); cerr != nil {
					errors = append(errors, fdID+": commit failed")
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
	return func(w http.ResponseWriter, r *http.Request) {
		var req bulkFDActionRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.FDIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "fd_ids are required")
			return
		}

		userEmail := getUserEmail(req.UserID)
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

			var instanceEyeID string
			engineErr := pgxPool.QueryRow(ctx, `
				SELECT ie.instance_eye_id
				FROM uam.approval_instance i
				JOIN uam.approval_instance_eye ie ON ie.instance_id = i.instance_id AND ie.status = 'ACTIVE'
				JOIN uam.approval_matrix_eye_member m ON m.eye_id = ie.matrix_eye_id
					AND m.member_type = 'APPROVER' AND m.is_active = true AND m.is_deleted = false
					AND m.assignment_type IN ('USER_ONLY','ROLE_USER') AND m.user_id = $2
				WHERE i.record_id = $1 AND i.module_code = 'FIXED_DEPOSIT' AND i.status = 'PENDING'
				ORDER BY ie.position ASC LIMIT 1`, fdID, req.UserID,
			).Scan(&instanceEyeID)

			if engineErr == nil && instanceEyeID != "" {
				if err := approvalengine.RecordAction(ctx, pgxPool, approvalengine.ActionRequest{
					InstanceEyeID: instanceEyeID,
					ActorUserID:   req.UserID,
					ActorEmail:    userEmail,
					ActionType:    approvalengine.ActionRejected,
					Comment:       req.Comment,
				}); err != nil {
					api.LogError("[FDMaster] RecordAction reject failed for fd %s: %v", fdID, err)
					errors = append(errors, fdID+": "+err.Error())
					continue
				}
				// finalizeRecord stamped audit; flip fd_master status.
				if _, execErr := pgxPool.Exec(ctx,
					`UPDATE investment.fd_master SET fd_status='REJECTED'
					 WHERE fd_id=$1 AND fd_status NOT IN ('ACTIVE','REJECTED')`, fdID,
				); execErr != nil {
					api.LogError("[FDMaster] fd_status→REJECTED failed for %s: %v", fdID, execErr)
				}
				engineActed++
			} else {
				var anyInstance int
				_ = pgxPool.QueryRow(ctx, `SELECT COUNT(*) FROM uam.approval_instance
					WHERE record_id=$1 AND module_code='FIXED_DEPOSIT' AND status='PENDING'`, fdID).Scan(&anyInstance)
				if anyInstance > 0 {
					errors = append(errors, fdID+": not your turn in approval sequence")
					continue
				}
				tx, err := pgxPool.Begin(ctx)
				if err != nil {
					errors = append(errors, fdID+": tx begin failed")
					continue
				}
				auditTable := resolveFDAuditTable(ctx, tx)
				if err := updateFDAuditStatus(ctx, tx, auditTable, []string{fdID}, userEmail, "REJECTED", req.Comment); err != nil {
					_ = tx.Rollback(ctx)
					errors = append(errors, fdID+": audit update failed")
					continue
				}
				if err := updateFDMasterStatus(ctx, tx, []string{fdID}, "REJECTED"); err != nil {
					_ = tx.Rollback(ctx)
					errors = append(errors, fdID+": status update failed")
					continue
				}
				if cerr := tx.Commit(ctx); cerr != nil {
					errors = append(errors, fdID+": commit failed")
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
			api.RespondWithError(w, http.StatusBadRequest, "fd_id is required")
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
			api.RespondWithError(w, http.StatusInternalServerError, "Query failed: "+err.Error())
			return
		}
		fdMaps, err := rowsToMaps(fdRows)
		fdRows.Close()
		if err != nil || len(fdMaps) == 0 {
			api.RespondWithError(w, http.StatusNotFound, "FD master record not found")
			return
		}
		fdMaster := fdMaps[0]

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
						RecordTable:      "investment.fd_master",
						AuditTable:       auditTable,
						AuditIDColumn:    keyCol,
						ActionType:       "CREATE",
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
		masterCols, err := loadTableColumns(ctx, pgxPool, "investment", "fd_master")
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrFailedToQuery+": "+err.Error())
			return
		}

		query := "SELECT * FROM investment.fd_master WHERE COALESCE(is_deleted,false)=false"
		args := make([]interface{}, 0)
		position := 1

		if fdID := strings.TrimSpace(r.URL.Query().Get("fd_id")); fdID != "" {
			if keyCol := pickFirstExistingColumn(masterCols, "fd_id", "master_id", "confirmation_id"); keyCol != "" {
				query += fmt.Sprintf(" AND %s = $%d", keyCol, position)
				args = append(args, fdID)
				position++
			}
		}
		if entityID := strings.TrimSpace(r.URL.Query().Get("entity_id")); entityID != "" && masterCols["entity_id"] {
			query += fmt.Sprintf(" AND entity_id = $%d", position)
			args = append(args, entityID)
			position++
		}
		if confirmationID := strings.TrimSpace(r.URL.Query().Get("confirmation_id")); confirmationID != "" && masterCols["confirmation_id"] {
			query += fmt.Sprintf(" AND confirmation_id = $%d", position)
			args = append(args, confirmationID)
			position++
		}
		query += " ORDER BY updated_at DESC NULLS LAST"

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
		api.RespondWithPayload(w, true, "", payload)
	}
}

func GetFDMasterAuditHistory(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		fdID := strings.TrimSpace(r.URL.Query().Get("fd_id"))
		if fdID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "fd_id is required")
			return
		}

		auditTable := resolveFDAuditTable(ctx, pgxPool)
		if auditTable == "" {
			api.RespondWithPayload(w, true, "", []map[string]interface{}{})
			return
		}

		schemaName, tableName := splitQualifiedTable(auditTable)
		cols, err := loadTableColumns(ctx, pgxPool, schemaName, tableName)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrFailedToQuery+": "+err.Error())
			return
		}
		refCol := pickFirstExistingColumn(cols, "fd_id", "master_id", "confirmation_id")
		if refCol == "" {
			api.RespondWithPayload(w, true, "", []map[string]interface{}{})
			return
		}

		rows, err := pgxPool.Query(ctx, fmt.Sprintf("SELECT * FROM %s WHERE %s = $1 ORDER BY requested_at DESC", auditTable, refCol), fdID)
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
		api.RespondWithPayload(w, true, "", payload)
	}
}

func GetCashflowSchedule(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		fdID := strings.TrimSpace(r.URL.Query().Get("fd_id"))
		if fdID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "fd_id is required")
			return
		}

		table := resolveFirstExistingTable(ctx, pgxPool, []string{
			"investment.fd_cashflow_schedule",
			"investment.fd_cashflow",
			"investment.fd_master_cashflow_schedule",
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
						if idx := strings.Index(rs, " | pending_fields:"); idx >= 0 {
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
			api.RespondWithError(w, http.StatusBadRequest, "fd_id is required")
			return
		}

		rows, err := pgxPool.Query(ctx, `
			SELECT
				je.entry_id, je.activity_id, je.entity_id, je.entity_name,
				je.fd_id, je.receipt_id, je.accrual_run_id, je.accrual_ledger_id,
				TO_CHAR(je.entry_date, 'YYYY-MM-DD') AS entry_date,
				je.accounting_period, je.entry_type, je.description,
				je.total_debit, je.total_credit, je.status,
				TO_CHAR(je.created_at, 'YYYY-MM-DD HH24:MI:SS') AS created_at,
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

// ─── BulkApproveCashflowEdit ──────────────────────────────────────────────────
// POST /investment/fd/master/cashflow/bulk-approve
// Body: { "user_id":"", "cashflow_ids":["",""], "comment":"" }
func BulkApproveCashflowEdit(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID      string   `json:"user_id"`
			CashflowIDs []string `json:"cashflow_ids"`
			Comment     string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.CashflowIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "cashflow_ids are required")
			return
		}
		userEmail := getUserEmail(req.UserID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		ctx := r.Context()
		approved, skipped, errs := 0, 0, []string{}

		for _, cashflowID := range req.CashflowIDs {
			var auditID, requestedBy, status string
			if err := pgxPool.QueryRow(ctx,
				`SELECT audit_id, COALESCE(requested_by,''), COALESCE(processing_status,'')
				 FROM investment.fd_audit_cashflow_schedule
				 WHERE cashflow_id = $1 AND processing_status LIKE 'PENDING%'
				 ORDER BY requested_at DESC LIMIT 1`,
				cashflowID,
			).Scan(&auditID, &requestedBy, &status); err != nil {
				errs = append(errs, cashflowID+": no pending audit found")
				continue
			}
			if auditID == "" {
				errs = append(errs, cashflowID+": no pending audit found")
				continue
			}
			if requestedBy == userEmail {
				errs = append(errs, cashflowID+": maker-checker violation")
				continue
			}
			if !strings.HasPrefix(status, "PENDING") {
				skipped++
				continue
			}
			// Try engine path
			var instanceEyeID string
			engineErr := pgxPool.QueryRow(ctx, `
				SELECT ie.instance_eye_id
				FROM uam.approval_instance i
				JOIN uam.approval_instance_eye ie ON ie.instance_id = i.instance_id AND ie.status = 'ACTIVE'
				JOIN uam.approval_matrix_eye_member m ON m.eye_id = ie.matrix_eye_id
					AND m.member_type = 'APPROVER' AND m.is_active = true AND m.is_deleted = false
					AND m.assignment_type IN ('USER_ONLY','ROLE_USER') AND m.user_id = $2
				WHERE i.record_id = $1 AND i.module_code = 'FIXED_DEPOSIT' AND i.status = 'PENDING'
				ORDER BY ie.position ASC LIMIT 1`, auditID, req.UserID,
			).Scan(&instanceEyeID)

			if engineErr == nil && instanceEyeID != "" {
				if err := approvalengine.RecordAction(ctx, pgxPool, approvalengine.ActionRequest{
					InstanceEyeID: instanceEyeID,
					ActorUserID:   req.UserID,
					ActorEmail:    userEmail,
					ActionType:    approvalengine.ActionApproved,
					Comment:       firstNonEmpty(req.Comment, "Bulk cashflow edit approved"),
				}); err != nil {
					errs = append(errs, cashflowID+": "+err.Error())
					continue
				}
				var instStatus string
				_ = pgxPool.QueryRow(ctx, `SELECT i.status FROM uam.approval_instance i
					JOIN uam.approval_instance_eye ie ON ie.instance_id = i.instance_id
					WHERE ie.instance_eye_id = $1`, instanceEyeID).Scan(&instStatus)
				if instStatus == "APPROVED" {
					if applyErr := applyApprovedCashflowEdit(ctx, pgxPool, auditID, userEmail); applyErr != nil {
						errs = append(errs, cashflowID+": apply failed: "+applyErr.Error())
						continue
					}
				}
			} else {
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
		api.LogInfo("[BulkApproveCashflow] approved=%d skipped=%d errors=%d by=%s", approved, skipped, len(errs), userEmail)
	}
}

// ─── BulkRejectCashflowEdit ───────────────────────────────────────────────────
// POST /investment/fd/master/cashflow/bulk-reject
// Body: { "user_id":"", "cashflow_ids":["",""], "comment":"" }
func BulkRejectCashflowEdit(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID      string   `json:"user_id"`
			CashflowIDs []string `json:"cashflow_ids"`
			Comment     string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.CashflowIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "cashflow_ids are required")
			return
		}
		userEmail := getUserEmail(req.UserID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		ctx := r.Context()
		rejected, skipped, errs := 0, 0, []string{}

		for _, cashflowID := range req.CashflowIDs {
			var auditID, requestedBy, status string
			if err := pgxPool.QueryRow(ctx,
				`SELECT audit_id, COALESCE(requested_by,''), COALESCE(processing_status,'')
				 FROM investment.fd_audit_cashflow_schedule
				 WHERE cashflow_id = $1 AND processing_status LIKE 'PENDING%'
				 ORDER BY requested_at DESC LIMIT 1`,
				cashflowID,
			).Scan(&auditID, &requestedBy, &status); err != nil {
				errs = append(errs, cashflowID+": no pending audit found")
				continue
			}
			if auditID == "" {
				errs = append(errs, cashflowID+": no pending audit found")
				continue
			}
			if requestedBy == userEmail {
				errs = append(errs, cashflowID+": maker-checker violation")
				continue
			}
			if !strings.HasPrefix(status, "PENDING") {
				skipped++
				continue
			}
			// Try engine path
			var instanceEyeID string
			engineErr := pgxPool.QueryRow(ctx, `
				SELECT ie.instance_eye_id
				FROM uam.approval_instance i
				JOIN uam.approval_instance_eye ie ON ie.instance_id = i.instance_id AND ie.status = 'ACTIVE'
				JOIN uam.approval_matrix_eye_member m ON m.eye_id = ie.matrix_eye_id
					AND m.member_type = 'APPROVER' AND m.is_active = true AND m.is_deleted = false
					AND m.assignment_type IN ('USER_ONLY','ROLE_USER') AND m.user_id = $2
				WHERE i.record_id = $1 AND i.module_code = 'FIXED_DEPOSIT' AND i.status = 'PENDING'
				ORDER BY ie.position ASC LIMIT 1`, auditID, req.UserID,
			).Scan(&instanceEyeID)

			if engineErr == nil && instanceEyeID != "" {
				_ = approvalengine.RecordAction(ctx, pgxPool, approvalengine.ActionRequest{
					InstanceEyeID: instanceEyeID,
					ActorUserID:   req.UserID,
					ActorEmail:    userEmail,
					ActionType:    approvalengine.ActionRejected,
					Comment:       req.Comment,
				})
			}
			_, _ = pgxPool.Exec(ctx,
				`UPDATE investment.fd_audit_cashflow_schedule
				 SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2
				 WHERE audit_id=$3`, userEmail, req.Comment, auditID)
			rejected++
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"rejected": rejected, "skipped": skipped,
			"errors": errs, "checker": userEmail,
		})
		api.LogInfo("[BulkRejectCashflow] rejected=%d skipped=%d errors=%d by=%s", rejected, skipped, len(errs), userEmail)
	}
}

// ─── BulkDeleteCashflow ───────────────────────────────────────────────────────
// POST /investment/fd/master/cashflow/bulk-delete
// Body: { "user_id":"", "cashflow_ids":["",""], "comment":"" }
// Approves pending DELETE audit rows (actually soft-deletes the cashflow rows).
func BulkDeleteCashflow(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID      string   `json:"user_id"`
			CashflowIDs []string `json:"cashflow_ids"`
			Comment     string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.CashflowIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "cashflow_ids are required")
			return
		}
		userEmail := getUserEmail(req.UserID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		ctx := r.Context()

		table := resolveFirstExistingTable(ctx, pgxPool, []string{
			"investment.fd_cashflow_schedule",
			"investment.fd_cashflow",
			"investment.fd_master_cashflow_schedule",
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
				errs = append(errs, cashflowID+": no pending audit found")
				continue
			}
			if auditID == "" {
				errs = append(errs, cashflowID+": no pending audit found")
				continue
			}
			if requestedBy == userEmail {
				errs = append(errs, cashflowID+": maker-checker violation")
				continue
			}
			if !strings.HasPrefix(status, "PENDING") {
				skipped++
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
				 SET processing_status='DELETED', checker_by=$1, checker_at=now(), checker_comment=$2
				 WHERE audit_id=$3`, userEmail, req.Comment, auditID,
			); execErr != nil {
				api.LogError("[BulkDeleteCashflow] audit stamp failed audit=%s: %v", auditID, execErr)
			}
			deleted++
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"deleted": deleted, "skipped": skipped,
			"errors": errs, "checker": userEmail,
		})
		api.LogInfo("[BulkDeleteCashflow] deleted=%d skipped=%d errors=%d by=%s", deleted, skipped, len(errs), userEmail)
	}
}
// ─── EditCashflowLineItem ─────────────────────────────────────────────────────
// POST /investment/fd/master/cashflow/edit
// Maker-checker edit of individual cashflow schedule line items.
// Editable: interest_accrued, tds_amount, net_cash_flow, period_days,
//           event_date, formula_used, accrual_rate_per_day, notes.
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

		userEmail := getUserEmail(req.UserID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()

		table := resolveFirstExistingTable(ctx, pgxPool, []string{
			"investment.fd_cashflow_schedule",
			"investment.fd_cashflow",
			"investment.fd_master_cashflow_schedule",
		})
		if table == "" {
			api.RespondWithError(w, http.StatusInternalServerError, "cashflow table not found")
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
			reasonWithFields = req.Reason + " | pending_fields:" + string(pfJSON)
		}

		// ── Read entity_id + principal for the approval engine ───────────────
		var entityID string
		var principalAmount float64
		_ = pgxPool.QueryRow(ctx,
			`SELECT COALESCE(entity_id,''), COALESCE(principal_amount,0)
			 FROM investment.fd_master WHERE fd_id = $1`, req.FDID,
		).Scan(&entityID, &principalAmount)

		// ── Insert audit row into fd_audit_cashflow_schedule ─────────────────
		var auditID string
		auditInsertErr := pgxPool.QueryRow(ctx, `
			INSERT INTO investment.fd_audit_cashflow_schedule (
				cashflow_id, fd_id,
				action_type, processing_status,
				reason,
				requested_by, requested_at,
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
				$4,now(),
				$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,
				$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,
				$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37
			) RETURNING audit_id`,
			req.CashflowID, req.FDID,
			reasonWithFields,
			userEmail,
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
				bgCtx := context.Background()
				instID, err := approvalengine.CreateInstance(bgCtx, pgxPool, approvalengine.InstanceRequest{
					ModuleCode:       "FIXED_DEPOSIT",
					EntityCode:       entityID,
					TransactionType:  "FD_CASHFLOW_EDIT",
					RecordID:         auditID,
					RecordTable:      "investment.fd_audit_cashflow_schedule",
					AuditTable:       "investment.fd_audit_cashflow_schedule",
					AuditIDColumn:    "audit_id",
					ActionType:       "EDIT",
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

		api.LogInfo("[FDMaster] EditCashflowLineItem submitted: fd=%s cashflow=%s audit=%s by=%s", req.FDID, req.CashflowID, auditID, userEmail)
		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"fd_id":       req.FDID,
			"cashflow_id": req.CashflowID,
			"audit_id":    auditID,
			"status":      "PENDING_EDIT_APPROVAL",
			"submitted_by": userEmail,
			"message":     "Edit submitted for approval. It will be applied once approved.",
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
	const pfMarker = " | pending_fields:"
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
			 SET processing_status='APPROVED', checker_by=$1, checker_at=now()
			 WHERE audit_id=$2`, checkerEmail, auditID)
		return nil
	}

	// 3. Resolve cashflow table and columns
	table := resolveFirstExistingTable(ctx, pool, []string{
		"investment.fd_cashflow_schedule",
		"investment.fd_cashflow",
		"investment.fd_master_cashflow_schedule",
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
			 SET processing_status='APPROVED', checker_by=$1, checker_at=now()
			 WHERE audit_id=$2`, checkerEmail, auditID)
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
		 SET processing_status='APPROVED', checker_by=$1, checker_at=now()
		 WHERE audit_id=$2`, checkerEmail, auditID)

	// 6. Propagate downstream principal changes
	propagateCashflowDownstream(ctx, pool, table, fdCol, cashflowIDCol, cols, fdID, cashflowID, pendingFields)

	return nil
}

// propagateCashflowDownstream re-reads all cashflow rows for an FD after an
// edit was applied, then cascades opening/closing principal changes forward.
func propagateCashflowDownstream(
	ctx context.Context, pool *pgxpool.Pool,
	table, fdCol, cashflowIDCol string, cols map[string]bool,
	fdID, editedCashflowID string,
	appliedFields map[string]interface{},
) {
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
		id                                          string
		eventType                                   string
		openP, closeP, capAmt, intAmt, tdsAmt, ncf float64
		seq                                         int
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

// ─── ApproveCashflowEdit ──────────────────────────────────────────────────────
// POST /investment/fd/master/cashflow/approve
// Checker approves a pending cashflow edit audit row.
func ApproveCashflowEdit(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string `json:"user_id"`
			AuditID  string `json:"audit_id"`
			Comment  string `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.AuditID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "audit_id is required")
			return
		}
		userEmail := getUserEmail(req.UserID)
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
			api.RespondWithError(w, http.StatusNotFound, "audit record not found")
			return
		}
		// Same-person check
		if requestedBy == userEmail {
			api.RespondWithError(w, http.StatusBadRequest, "maker and checker cannot be the same person")
			return
		}
		if !strings.HasPrefix(status, "PENDING") {
			api.RespondWithError(w, http.StatusConflict, "audit record is not in a pending state (current: "+status+")")
			return
		}

		// Try engine path
		var instanceEyeID string
		engineErr := pgxPool.QueryRow(ctx, `
			SELECT ie.instance_eye_id
			FROM uam.approval_instance i
			JOIN uam.approval_instance_eye ie ON ie.instance_id = i.instance_id AND ie.status = 'ACTIVE'
			JOIN uam.approval_matrix_eye_member m ON m.eye_id = ie.matrix_eye_id
				AND m.member_type = 'APPROVER' AND m.is_active = true AND m.is_deleted = false
				AND m.assignment_type IN ('USER_ONLY','ROLE_USER') AND m.user_id = $2
			WHERE i.record_id = $1 AND i.module_code = 'FIXED_DEPOSIT' AND i.status = 'PENDING'
			ORDER BY ie.position ASC LIMIT 1`, req.AuditID, req.UserID,
		).Scan(&instanceEyeID)

		if engineErr == nil && instanceEyeID != "" {
			// Engine path: record action
			if err := approvalengine.RecordAction(ctx, pgxPool, approvalengine.ActionRequest{
				InstanceEyeID: instanceEyeID,
				ActorUserID:   req.UserID,
				ActorEmail:    userEmail,
				ActionType:    approvalengine.ActionApproved,
				Comment:       firstNonEmpty(req.Comment, "Cashflow edit approved"),
			}); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "approval engine error: "+err.Error())
				return
			}
			// Check if instance is now fully APPROVED
			var instStatus string
			_ = pgxPool.QueryRow(ctx, `
				SELECT i.status FROM uam.approval_instance i
				JOIN uam.approval_instance_eye ie ON ie.instance_id = i.instance_id
				WHERE ie.instance_eye_id = $1`, instanceEyeID,
			).Scan(&instStatus)
			if instStatus == "APPROVED" {
				if applyErr := applyApprovedCashflowEdit(ctx, pgxPool, req.AuditID, userEmail); applyErr != nil {
					api.LogError("[CashflowApprove] apply failed audit=%s: %v", req.AuditID, applyErr)
					api.RespondWithError(w, http.StatusInternalServerError, "apply edit failed: "+applyErr.Error())
					return
				}
			}
			api.RespondWithPayload(w, true, "", map[string]interface{}{
				"audit_id": req.AuditID, "status": instStatus, "approved_by": userEmail,
			})
		} else {
			// No engine instance — direct approval
			if applyErr := applyApprovedCashflowEdit(ctx, pgxPool, req.AuditID, userEmail); applyErr != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "apply edit failed: "+applyErr.Error())
				return
			}
			api.RespondWithPayload(w, true, "", map[string]interface{}{
				"audit_id": req.AuditID, "status": "APPROVED", "approved_by": userEmail,
			})
		}
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
			api.RespondWithError(w, http.StatusBadRequest, "audit_id is required")
			return
		}
		userEmail := getUserEmail(req.UserID)
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
			api.RespondWithError(w, http.StatusNotFound, "audit record not found")
			return
		}
		// Same-person check
		if requestedBy == userEmail {
			api.RespondWithError(w, http.StatusBadRequest, "maker and checker cannot be the same person")
			return
		}
		if !strings.HasPrefix(status, "PENDING") {
			api.RespondWithError(w, http.StatusConflict, "audit record is not in a pending state (current: "+status+")")
			return
		}

		// Try engine path
		var instanceEyeID string
		engineErr := pgxPool.QueryRow(ctx, `
			SELECT ie.instance_eye_id
			FROM uam.approval_instance i
			JOIN uam.approval_instance_eye ie ON ie.instance_id = i.instance_id AND ie.status = 'ACTIVE'
			JOIN uam.approval_matrix_eye_member m ON m.eye_id = ie.matrix_eye_id
				AND m.member_type = 'APPROVER' AND m.is_active = true AND m.is_deleted = false
				AND m.assignment_type IN ('USER_ONLY','ROLE_USER') AND m.user_id = $2
			WHERE i.record_id = $1 AND i.module_code = 'FIXED_DEPOSIT' AND i.status = 'PENDING'
			ORDER BY ie.position ASC LIMIT 1`, req.AuditID, req.UserID,
		).Scan(&instanceEyeID)

		if engineErr == nil && instanceEyeID != "" {
			if err := approvalengine.RecordAction(ctx, pgxPool, approvalengine.ActionRequest{
				InstanceEyeID: instanceEyeID,
				ActorUserID:   req.UserID,
				ActorEmail:    userEmail,
				ActionType:    approvalengine.ActionRejected,
				Comment:       req.Comment,
			}); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "approval engine error: "+err.Error())
				return
			}
			// Engine finalizer stamps audit row; also ensure our status is REJECTED
			if _, execErr := pgxPool.Exec(ctx,
				`UPDATE investment.fd_audit_cashflow_schedule
				 SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2
				 WHERE audit_id=$3`, userEmail, req.Comment, req.AuditID,
			); execErr != nil {
				api.LogError("[CashflowReject] audit stamp failed audit=%s: %v", req.AuditID, execErr)
			}
		} else {
			// No engine — direct stamp
			if _, execErr := pgxPool.Exec(ctx,
				`UPDATE investment.fd_audit_cashflow_schedule
				 SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2
				 WHERE audit_id=$3`, userEmail, req.Comment, req.AuditID,
			); execErr != nil {
				api.LogError("[CashflowReject] audit stamp failed audit=%s: %v", req.AuditID, execErr)
			}
		}
		api.LogInfo("[CashflowReject] audit=%s cf=%s fd=%s by=%s", req.AuditID, cashflowID, fdID, userEmail)
		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"audit_id": req.AuditID, "status": "REJECTED", "rejected_by": userEmail,
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
		userEmail := getUserEmail(req.UserID)
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
			"investment.fd_cashflow_schedule",
			"investment.fd_cashflow",
			"investment.fd_master_cashflow_schedule",
		})
		if table == "" {
			api.RespondWithError(w, http.StatusInternalServerError, "cashflow table not found")
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

		// Insert audit row for delete request
		var auditID string
		_ = pgxPool.QueryRow(ctx, `
			INSERT INTO investment.fd_audit_cashflow_schedule (
				cashflow_id, fd_id,
				action_type, processing_status,
				reason,
				requested_by, requested_at,
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
				$4,now(),
				$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,
				$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,
				$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37
			) RETURNING audit_id`,
			req.CashflowID, req.FDID,
			req.Reason, userEmail,
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

		api.LogInfo("[CashflowDelete] submitted fd=%s cf=%s audit=%s by=%s", req.FDID, req.CashflowID, auditID, userEmail)
		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"fd_id":       req.FDID,
			"cashflow_id": req.CashflowID,
			"audit_id":    auditID,
			"status":      "PENDING_DELETE_APPROVAL",
			"submitted_by": userEmail,
			"message":     "Delete request submitted for approval.",
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
			api.RespondWithError(w, http.StatusBadRequest, "audit_id is required")
			return
		}
		userEmail := getUserEmail(req.UserID)
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
			api.RespondWithError(w, http.StatusNotFound, "audit record not found")
			return
		}
		if requestedBy == userEmail {
			api.RespondWithError(w, http.StatusBadRequest, "maker and checker cannot be the same person")
			return
		}
		if !strings.HasPrefix(status, "PENDING") {
			api.RespondWithError(w, http.StatusConflict, "audit record is not pending (current: "+status+")")
			return
		}

		table := resolveFirstExistingTable(ctx, pgxPool, []string{
			"investment.fd_cashflow_schedule",
			"investment.fd_cashflow",
			"investment.fd_master_cashflow_schedule",
		})
		if table == "" {
			api.RespondWithError(w, http.StatusInternalServerError, "cashflow table not found")
			return
		}

		// Soft-delete the cashflow row
		if _, execErr := pgxPool.Exec(ctx,
			fmt.Sprintf(`UPDATE %s SET is_deleted=true, is_active=false, updated_at=now(), updated_by=$1
			             WHERE cashflow_id=$2 AND fd_id=$3`, table),
			userEmail, cashflowID, fdID,
		); execErr != nil {
			api.LogError("[CashflowApproveDelete] soft-delete failed audit=%s cf=%s: %v", req.AuditID, cashflowID, execErr)
		}

		// Stamp audit row
		if _, execErr := pgxPool.Exec(ctx,
			`UPDATE investment.fd_audit_cashflow_schedule
			 SET processing_status='DELETED', checker_by=$1, checker_at=now(), checker_comment=$2
			 WHERE audit_id=$3`, userEmail, req.Comment, req.AuditID,
		); execErr != nil {
			api.LogError("[CashflowApproveDelete] audit stamp failed audit=%s: %v", req.AuditID, execErr)
		}

		api.LogInfo("[CashflowApproveDelete] audit=%s cf=%s fd=%s by=%s", req.AuditID, cashflowID, fdID, userEmail)
		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"audit_id": req.AuditID, "cashflow_id": cashflowID, "status": "DELETED", "approved_by": userEmail,
		})
	}
}

// ─── GetCashflowAuditHistory ──────────────────────────────────────────────────
// GET /investment/fd/master/cashflow/audit?fd_id=&cashflow_id=
func GetCashflowAuditHistory(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		fdID := strings.TrimSpace(r.URL.Query().Get("fd_id"))
		cashflowID := strings.TrimSpace(r.URL.Query().Get("cashflow_id"))
		if fdID == "" && cashflowID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "fd_id or cashflow_id is required")
			return
		}

		query := `SELECT * FROM investment.fd_audit_cashflow_schedule WHERE 1=1`
		var args []interface{}
		pos := 1
		if fdID != "" {
			query += fmt.Sprintf(" AND fd_id = $%d", pos)
			args = append(args, fdID)
			pos++
		}
		if cashflowID != "" {
			query += fmt.Sprintf(" AND cashflow_id = $%d", pos)
			args = append(args, cashflowID)
			pos++
		}
		query += " ORDER BY requested_at DESC"

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
		api.RespondWithPayload(w, true, "", payload)
	}
}
