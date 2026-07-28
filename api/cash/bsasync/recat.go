package bsasync

import (
	"context"
	"fmt"

	"CimplrCorpSaas/api/policyengine/common"
	"CimplrCorpSaas/api/policyengine/runtime"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	JobPathSmartCat     = "job://cash/smart-cat"
	JobPathBSPostUpload = "job://cash/bs-post-upload"
)

// TxnCategoryChange is one transaction field change recorded under a RECAT audit.
type TxnCategoryChange struct {
	TransactionID int64
	FieldName     string
	OldValue      string
	NewValue      string
}

func loadBSFields(ctx context.Context, pool *pgxpool.Pool, bankStatementID, actionType string, extra map[string]interface{}) (entityID string, fields map[string]interface{}, err error) {
	var accountNumber, periodStart, periodEnd, reqDate, fileHash, uploadedAt, uploadLink, uploadS3Key, status string
	var openBal, closeBal *float64
	var totalTxns, uncat int
	err = pool.QueryRow(ctx, `
		SELECT COALESCE(s.entity_id,''), COALESCE(s.account_number,''),
		       COALESCE(TO_CHAR(s.statement_period_start,'YYYY-MM-DD'),''),
		       COALESCE(TO_CHAR(s.statement_period_end,'YYYY-MM-DD'),''),
		       COALESCE(TO_CHAR(s.statement_request_date,'YYYY-MM-DD'),''),
		       COALESCE(s.file_hash,''),
		       COALESCE(TO_CHAR(s.uploaded_at,'YYYY-MM-DD"T"HH24:MI:SS'),''),
		       s.opening_balance, s.closing_balance,
		       COALESCE(s.upload_link,''), COALESCE(s.upload_s3_key,''),
		       COALESCE(s.current_status,''),
		       (SELECT COUNT(*)::int FROM cimplrcorpsaas.bank_statement_transactions t WHERE t.bank_statement_id = s.bank_statement_id),
		       (SELECT COUNT(*)::int FROM cimplrcorpsaas.bank_statement_transactions t WHERE t.bank_statement_id = s.bank_statement_id AND t.category_id IS NULL)
		FROM cimplrcorpsaas.bank_statements s WHERE s.bank_statement_id = $1`, bankStatementID,
	).Scan(&entityID, &accountNumber, &periodStart, &periodEnd, &reqDate, &fileHash, &uploadedAt,
		&openBal, &closeBal, &uploadLink, &uploadS3Key, &status, &totalTxns, &uncat)
	if err != nil {
		return "", nil, err
	}
	fields = map[string]interface{}{
		"bank_statement_id":      bankStatementID,
		"entity_id":              entityID,
		"account_number":         accountNumber,
		"statement_period_start": periodStart,
		"statement_period_end":   periodEnd,
		"statement_request_date": reqDate,
		"file_hash":              fileHash,
		"uploaded_at":            uploadedAt,
		"opening_balance":        openBal,
		"closing_balance":        closeBal,
		"upload_link":            uploadLink,
		"upload_s3_key":          uploadS3Key,
		"processing_status":      status,
		"total_transactions":     totalTxns,
		"uncategorized_count":    uncat,
		"action_type":            actionType,
	}
	for k, v := range extra {
		fields[k] = v
	}
	return entityID, fields, nil
}

// EnforceSmartCatJob gates SmartCat RECAT writes for one bank statement.
func EnforceSmartCatJob(ctx context.Context, pool *pgxpool.Pool, bankStatementID, eventCode string, recatTxnCount int) (bool, string) {
	entityID, fields, err := loadBSFields(ctx, pool, bankStatementID, "RECAT", map[string]interface{}{
		"recat_txn_count": recatTxnCount,
		"recat_source":    "smart-cat",
	})
	if err != nil {
		return false, "policy check failed — could not load bank statement"
	}
	return runtime.EnforceJobInline(ctx, pool, runtime.JobEnforceInput{
		EventCode: eventCode, ModuleCode: common.ModuleCash, SubModule: "BANK_STATEMENT",
		EntityCode: entityID, ActorUserID: "SYSTEM:smart-cat",
		HandlerName: "ProcessUncategorizedTransactions", APIPath: JobPathSmartCat,
		CorrelationID: fmt.Sprintf("SMARTCAT-%s", bankStatementID),
		BusinessRecordID: bankStatementID, BusinessRecordType: "BANK_STATEMENT",
		Fields: fields, DefaultBlockMessage: "Smart categorization blocked by policy",
	})
}

// EnforcePostUploadJob gates post-upload SmartCat kickoff.
func EnforcePostUploadJob(ctx context.Context, pool *pgxpool.Pool, bankStatementID string) (bool, string) {
	entityID, fields, err := loadBSFields(ctx, pool, bankStatementID, "CREATE", nil)
	if err != nil {
		return false, "policy check failed — could not load bank statement"
	}
	return runtime.EnforceJobInline(ctx, pool, runtime.JobEnforceInput{
		EventCode: common.TriggerOnDataImport, ModuleCode: common.ModuleCash, SubModule: "BANK_STATEMENT",
		EntityCode: entityID, ActorUserID: "SYSTEM:bs-upload",
		HandlerName: "PostUploadSmartCat", APIPath: JobPathBSPostUpload,
		CorrelationID: fmt.Sprintf("BS-UPLOAD-%s", bankStatementID),
		BusinessRecordID: bankStatementID, BusinessRecordType: "BANK_STATEMENT",
		Fields: fields, DefaultBlockMessage: "Post-upload categorization blocked by policy",
	})
}

// ApplySmartCatRecat runs policy, then inserts RECAT pending + per-txn audit rows.
func ApplySmartCatRecat(ctx context.Context, pool *pgxpool.Pool, bankStatementID string, changes []TxnCategoryChange, eventCode string) error {
	if len(changes) == 0 {
		return nil
	}
	if eventCode == "" {
		eventCode = common.TriggerScheduledDaily
	}
	ok, msg := EnforceSmartCatJob(ctx, pool, bankStatementID, eventCode, len(changes))
	if !ok {
		return fmt.Errorf("%s", msg)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if err := insertRecatAuditInTx(ctx, tx, bankStatementID, changes, "system", "system", ""); err != nil {
		return err
	}
	for _, ch := range changes {
		if ch.NewValue == "" {
			if _, err := tx.Exec(ctx, `UPDATE cimplrcorpsaas.bank_statement_transactions SET category_id = NULL WHERE transaction_id = $1`, ch.TransactionID); err != nil {
				return fmt.Errorf("apply recat category clear txn=%d: %w", ch.TransactionID, err)
			}
		} else {
			if _, err := tx.Exec(ctx, `UPDATE cimplrcorpsaas.bank_statement_transactions SET category_id = $1 WHERE transaction_id = $2`, ch.NewValue, ch.TransactionID); err != nil {
				return fmt.Errorf("apply recat category txn=%d: %w", ch.TransactionID, err)
			}
		}
	}
	return tx.Commit(ctx)
}

// InsertCategoryDeleteRecatInTx stages RECAT headers + per-txn audit when master
// categories are deleted and transactions will be unassigned.
func InsertCategoryDeleteRecatInTx(ctx context.Context, tx pgx.Tx, deleteCategoryIDs []string, requestedBy, checkerComment, requestedIP string) error {
	if len(deleteCategoryIDs) == 0 {
		return nil
	}
	rows, err := tx.Query(ctx, `
		SELECT transaction_id, bank_statement_id::text, COALESCE(category_id::text, '')
		FROM cimplrcorpsaas.bank_statement_transactions
		WHERE category_id = ANY($1::text[])`, deleteCategoryIDs)
	if err != nil {
		return fmt.Errorf("load affected txns: %w", err)
	}
	defer rows.Close()

	byBS := map[string][]TxnCategoryChange{}
	for rows.Next() {
		var txnID int64
		var bsID, oldCat string
		if err := rows.Scan(&txnID, &bsID, &oldCat); err != nil {
			return err
		}
		if bsID == "" || oldCat == "" {
			continue
		}
		byBS[bsID] = append(byBS[bsID], TxnCategoryChange{
			TransactionID: txnID,
			FieldName:     "category_id",
			OldValue:      oldCat,
			NewValue:      "",
		})
	}
	if len(byBS) == 0 {
		return nil
	}
	if requestedBy == "" {
		requestedBy = "system"
	}
	if requestedIP == "" {
		requestedIP = "system"
	}
	for bsID, changes := range byBS {
		if err := insertRecatAuditInTx(ctx, tx, bsID, changes, requestedBy, requestedIP, checkerComment); err != nil {
			return err
		}
	}
	return nil
}

func insertRecatAuditInTx(ctx context.Context, tx pgx.Tx, bankStatementID string, changes []TxnCategoryChange, requestedBy, requestedIP, checkerComment string) error {
	var actionID string
	err := tx.QueryRow(ctx, `
		INSERT INTO cimplrcorpsaas.auditactionbankstatement
		    (bankstatementid, actiontype, processing_status, requested_by, requested_at, requested_ip, checker_comment)
		VALUES ($1, 'RECAT', 'PENDING_EDIT_APPROVAL', $2, now(), $3, $4)
		RETURNING action_id::text`, bankStatementID, requestedBy, requestedIP, checkerComment,
	).Scan(&actionID)
	if err != nil {
		return fmt.Errorf("insert RECAT audit bs=%s: %w", bankStatementID, err)
	}
	batch := &pgx.Batch{}
	for _, ch := range changes {
		field := ch.FieldName
		if field == "" {
			field = "category_id"
		}
		batch.Queue(`
			INSERT INTO cimplrcorpsaas.auditactionbankstatementtxn
			    (action_id, bankstatementid, transaction_id, field_name, old_value, new_value,
			     performed_by, performed_at, requested_ip)
			VALUES ($1::int, $2, $3, $4, $5, $6, $7, now(), $8)`,
			actionID, bankStatementID, fmt.Sprintf("%d", ch.TransactionID), field, ch.OldValue, ch.NewValue,
			requestedBy, requestedIP,
		)
	}
	br := tx.SendBatch(ctx, batch)
	for i := 0; i < batch.Len(); i++ {
		if _, err := br.Exec(); err != nil {
			br.Close()
			return fmt.Errorf("insert txn audit bs=%s: %w", bankStatementID, err)
		}
	}
	return br.Close()
}

// ApplyManualRecatInTx writes RECAT header + per-txn old/new audit and applies
// category updates inside the caller's transaction (HTTP manual categorize paths).
func ApplyManualRecatInTx(ctx context.Context, tx pgx.Tx, bankStatementID string, changes []TxnCategoryChange, requestedBy, requestedIP string) error {
	if len(changes) == 0 {
		return nil
	}
	if requestedBy == "" {
		requestedBy = "system"
	}
	if requestedIP == "" {
		requestedIP = "system"
	}
	if err := insertRecatAuditInTx(ctx, tx, bankStatementID, changes, requestedBy, requestedIP, ""); err != nil {
		return err
	}
	for _, ch := range changes {
		if ch.NewValue == "" {
			if _, err := tx.Exec(ctx, `UPDATE cimplrcorpsaas.bank_statement_transactions SET category_id = NULL WHERE transaction_id = $1`, ch.TransactionID); err != nil {
				return fmt.Errorf("manual recat clear txn=%d: %w", ch.TransactionID, err)
			}
		} else if _, err := tx.Exec(ctx, `UPDATE cimplrcorpsaas.bank_statement_transactions SET category_id = $1 WHERE transaction_id = $2`, ch.NewValue, ch.TransactionID); err != nil {
			return fmt.Errorf("manual recat apply txn=%d: %w", ch.TransactionID, err)
		}
	}
	return nil
}
