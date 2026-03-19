package fdMaster

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
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

func ActivateFD(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req activateFDRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if strings.TrimSpace(req.ConfirmationID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "confirmation_id is required")
			return
		}

		userEmail := getUserEmail(req.UserID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			msg, status := getFDMasterError(err, "Transaction begin failed")
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

		receiptDate := parseDateOrDefault(req.ReceiptDate, rec.ReceiptDate)
		fdID, _, err := insertFDMaster(ctx, tx, rec, req.FDNumber, receiptDate, req.Notes, userEmail)
		if err != nil {
			msg, status := getFDMasterError(err, "FD activation failed")
			api.RespondWithError(w, status, msg)
			return
		}

		auditTable := resolveFDAuditTable(ctx, tx)
		if err := insertFDAuditRecord(ctx, tx, auditTable, fdID, userEmail, "CREATE", "PENDING_APPROVAL", ""); err != nil {
			msg, status := getFDMasterError(err, constants.ErrAuditInsertFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		cashflows, _, err := GenerateCashflowForFD(ctx, tx, "", req.ConfirmationID)
		if err != nil {
			msg, status := getFDMasterError(err, "Cashflow generation failed")
			api.RespondWithError(w, status, msg)
			return
		}
		if err := SaveCashflowScheduleWithCreator(ctx, tx, fdID, cashflows, userEmail); err != nil {
			msg, status := getFDMasterError(err, "Cashflow save failed")
			api.RespondWithError(w, status, msg)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getFDMasterError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}

		go func(fdRecordID string, confirmationID string, email string, entityID string, amount float64, req activateFDRequest) {
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

		for _, fdID := range req.FDIDs {
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

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"engine_acted": engineActed, "direct_acted": directActed,
			"errors": errors, "checker": userEmail,
		})
		for _, fdID := range req.FDIDs {
			go func(id, uEmail string) {
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

		for _, fdID := range req.FDIDs {
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
				_, _ = pgxPool.Exec(ctx, `UPDATE investment.fd_master SET fd_status='REJECTED'
					WHERE fd_id=$1 AND fd_status NOT IN ('ACTIVE','REJECTED')`, fdID)
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

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"engine_acted": engineActed, "direct_acted": directActed,
			"errors": errors, "checker": userEmail,
		})
		for _, fdID := range req.FDIDs {
			go func(id, uEmail string) {
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

		table := resolveFirstExistingTable(ctx, pgxPool, []string{"investment.fd_cashflow_schedule", "investment.fd_cashflow", "investment.fd_master_cashflow_schedule"})
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
		if fdCol == "" {
			api.RespondWithPayload(w, true, "", []map[string]interface{}{})
			return
		}

		orderCol := firstNonEmpty(pickFirstExistingColumn(cols, "period_number"), pickFirstExistingColumn(cols, "cashflow_date"), fdCol)
		rows, err := pgxPool.Query(ctx, fmt.Sprintf("SELECT * FROM %s WHERE %s = $1 ORDER BY %s", table, fdCol, orderCol), fdID)
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

func GetFDJournalEntries(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		// Accept fd_id from query param (GET-style) or from JSON body (POST-style)
		fdID := strings.TrimSpace(r.URL.Query().Get("fd_id"))
		if fdID == "" {
			var body struct {
				FDID string `json:"fd_id"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err == nil {
				fdID = strings.TrimSpace(body.FDID)
			}
		}
		if fdID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "fd_id is required")
			return
		}

		rows, err := pgxPool.Query(ctx, `
			SELECT
				je.entry_id,
				je.activity_id,
				je.entity_id,
				je.entity_name,
				je.entry_date,
				je.accounting_period,
				je.entry_type,
				je.description,
				je.total_debit,
				je.total_credit,
				je.status,
				jl.line_id,
				jl.line_number,
				jl.account_number,
				jl.account_name,
				jl.account_type,
				jl.debit_amount,
				jl.credit_amount,
				jl.narration
			FROM investment.accounting_journal_entry je
			LEFT JOIN investment.accounting_journal_entry_line jl ON jl.entry_id = je.entry_id
			WHERE je.description ILIKE $1 OR jl.narration ILIKE $1
			ORDER BY je.entry_date DESC, jl.line_number ASC
		`, "%fd_id="+fdID+"%")
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
