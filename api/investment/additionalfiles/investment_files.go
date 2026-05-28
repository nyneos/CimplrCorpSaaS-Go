package additionalfiles

import (
	cashfiles "CimplrCorpSaas/api/cash/additionalfiles"
	"CimplrCorpSaas/api/constants"
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type investmentFileDefinition struct {
	Module        string
	ParentIDField string
	TableName     string
	ParentColumn  string
	ParentTable   string
	ParentFilter  string
}

type investmentAuditColumn struct {
	Nullable   bool
	HasDefault bool
}

var (
	onboardingFilesDefinition = investmentFileDefinition{
		Module:        "investment-onboarding-additional",
		ParentIDField: "batch_id",
		TableName:     "investment.onboard_batch_files",
		ParentColumn:  "batch_id",
		ParentTable:   "investment.onboard_batch",
	}
	proposalFilesDefinition = investmentFileDefinition{
		Module:        "investment-proposal",
		ParentIDField: "proposal_id",
		TableName:     "investment.investment_proposal_files",
		ParentColumn:  "proposal_id",
		ParentTable:   "investment.investment_proposal",
		ParentFilter:  constants.FormatDeletedFilter,
	}
	initiationFilesDefinition = investmentFileDefinition{
		Module:        "investment-initiation",
		ParentIDField: "initiation_id",
		TableName:     "investment.investment_initiation_files",
		ParentColumn:  "initiation_id",
		ParentTable:   "investment.investment_initiation",
		ParentFilter:  constants.FormatDeletedFilter,
	}
	confirmationFilesDefinition = investmentFileDefinition{
		Module:        "investment-confirmation-additional",
		ParentIDField: "confirmation_id",
		TableName:     "investment.investment_confirmation_files",
		ParentColumn:  "confirmation_id",
		ParentTable:   "investment.investment_confirmation",
		ParentFilter:  constants.FormatDeletedFilter,
	}
	redemptionInitiationFilesDefinition = investmentFileDefinition{
		Module:        "investment-redemption-initiation",
		ParentIDField: "redemption_id",
		TableName:     "investment.redemption_initiation_files",
		ParentColumn:  "redemption_id",
		ParentTable:   "investment.redemption_initiation",
		ParentFilter:  constants.FormatDeletedFilter,
	}
	redemptionConfirmationFilesDefinition = investmentFileDefinition{
		Module:        "investment-redemption-confirmation-additional",
		ParentIDField: "redemption_confirm_id",
		TableName:     "investment.redemption_confirmation_files",
		ParentColumn:  "redemption_confirm_id",
		ParentTable:   "investment.redemption_confirmation",
		ParentFilter:  constants.FormatDeletedFilter,
	}
	accountingActivityFilesDefinition = investmentFileDefinition{
		Module:        "investment-accounting-activity",
		ParentIDField: "activity_id",
		TableName:     "investment.accounting_activity_files",
		ParentColumn:  "activity_id",
		ParentTable:   "investment.accounting_activity",
		ParentFilter:  constants.FormatDeletedFilter,
	}
	fdBookingFilesDefinition = investmentFileDefinition{
		Module:        "fd-booking-additional",
		ParentIDField: "booking_id",
		TableName:     "investment.fd_booking_request_files",
		ParentColumn:  "booking_id",
		ParentTable:   "investment.fd_booking_request",
		ParentFilter:  constants.FormatDeletedFilter,
	}
	fdConfirmationFilesDefinition = investmentFileDefinition{
		Module:        "fd-confirmation-additional",
		ParentIDField: "confirmation_id",
		TableName:     "investment.fd_confirmation_files",
		ParentColumn:  "confirmation_id",
		ParentTable:   "investment.fd_confirmation",
		ParentFilter:  constants.FormatDeletedFilter,
	}
	fdMasterFilesDefinition = investmentFileDefinition{
		Module:        "fd-master-additional",
		ParentIDField: "fd_id",
		TableName:     "investment.fd_master_files",
		ParentColumn:  "fd_id",
		ParentTable:   "investment.fd_master",
		ParentFilter:  constants.FormatDeletedFilter,
	}
	fdClosureFilesDefinition = investmentFileDefinition{
		Module:        "fd-closure-additional",
		ParentIDField: "closure_request_id",
		TableName:     "investment.fd_closure_request_files",
		ParentColumn:  "closure_request_id",
		ParentTable:   constants.QuerryClosureRequest,
		ParentFilter: `AND COALESCE(p.is_deleted, FALSE) = FALSE
		  AND COALESCE(p.closure_type, '') <> 'ROLLOVER'
		  AND EXISTS (
		    SELECT 1 FROM investment.fd_master m
		    WHERE m.fd_id = p.fd_id
		      AND COALESCE(m.is_deleted, FALSE) = FALSE
		  )`,
	}
	fdRolloverFilesDefinition = investmentFileDefinition{
		Module:        "fd-rollover-additional",
		ParentIDField: "closure_request_id",
		TableName:     "investment.fd_rollover_request_files",
		ParentColumn:  "closure_request_id",
		ParentTable:   constants.QuerryClosureRequest,
		ParentFilter: `AND COALESCE(p.is_deleted, FALSE) = FALSE
		  AND COALESCE(p.closure_type, '') = 'ROLLOVER'
		  AND EXISTS (
		    SELECT 1 FROM investment.fd_master m
		    WHERE m.fd_id = p.fd_id
		      AND COALESCE(m.is_deleted, FALSE) = FALSE
		  )`,
	}
	fdCashflowFilesDefinition = investmentFileDefinition{
		Module:        "fd-cashflow-additional",
		ParentIDField: "fd_id",
		TableName:     "investment.fd_cashflow_files",
		ParentColumn:  "fd_id",
		ParentTable:   "investment.fd_master",
		ParentFilter: `AND COALESCE(p.is_deleted, FALSE) = FALSE
		  AND EXISTS (
		    SELECT 1 FROM investment.fd_cashflow_schedule c WHERE c.fd_id = p.fd_id
		  )`,
	}
	fdInterestReceiptFilesDefinition = investmentFileDefinition{
		Module:        "fd-interest-receipt-additional",
		ParentIDField: "receipt_id",
		TableName:     "investment.fd_interest_receipt_files",
		ParentColumn:  "receipt_id",
		ParentTable:   "investment.fd_interest_receipt",
		ParentFilter:  constants.FormatDeletedFilter,
	}
	fdTDSReceiptFilesDefinition = investmentFileDefinition{
		Module:        "fd-tds-receipt-additional",
		ParentIDField: "tds_id",
		TableName:     "investment.fd_tds_receipt_files",
		ParentColumn:  "tds_id",
		ParentTable:   "investment.fd_tds_receipt",
		ParentFilter:  constants.FormatDeletedFilter,
	}
	fdReconcileResultFilesDefinition = investmentFileDefinition{
		Module:        "fd-reconcile-result-additional",
		ParentIDField: "result_id",
		TableName:     "investment.fd_receipt_reconcile_result_files",
		ParentColumn:  "result_id",
		ParentTable:   "investment.fd_receipt_reconcile_result",
	}
	fdReceiptExceptionFilesDefinition = investmentFileDefinition{
		Module:        "fd-receipt-exception-additional",
		ParentIDField: "exception_id",
		TableName:     "investment.fd_receipt_exception_files",
		ParentColumn:  "exception_id",
		ParentTable:   "investment.fd_receipt_exception",
		ParentFilter:  constants.FormatDeletedFilter,
	}
	varianceExceptionFilesDefinition = investmentFileDefinition{
		Module:        "investment-variance-exception-additional",
		ParentIDField: "exception_id",
		TableName:     "investment.fd_receipt_exception_files",
		ParentColumn:  "exception_id",
		ParentTable:   "investment.fd_receipt_exception",
		ParentFilter:  constants.FormatDeletedFilter,
	}
	fdAccrualRunFilesDefinition = investmentFileDefinition{
		Module:        "fd-accrual-run-additional",
		ParentIDField: "run_id",
		TableName:     "investment.fd_accrual_run_files",
		ParentColumn:  "run_id",
		ParentTable:   "investment.fd_accrual_run",
	}
	fdAccrualScheduleConfigFilesDefinition = investmentFileDefinition{
		Module:        "fd-accrual-schedule-config-additional",
		ParentIDField: "config_id",
		TableName:     "investment.fd_accrual_schedule_config_files",
		ParentColumn:  "config_id",
		ParentTable:   "investment.fd_accrual_schedule_config",
		ParentFilter:  constants.FormatDeletedFilter,
	}
	fdAccrualLedgerFilesDefinition = investmentFileDefinition{
		Module:        "fd-accrual-ledger-additional",
		ParentIDField: "ledger_id",
		TableName:     "investment.fd_accrual_ledger_files",
		ParentColumn:  "ledger_id",
		ParentTable:   "investment.fd_accrual_ledger",
		ParentFilter:  constants.FormatDeletedFilter,
	}
	fdAccountingJournalFilesDefinition = investmentFileDefinition{
		Module:        "fd-accounting-journal-additional",
		ParentIDField: "entry_id",
		TableName:     "investment.fd_accounting_journal_entry_files",
		ParentColumn:  "entry_id",
		ParentTable:   "investment.accounting_journal_entry",
	}
)

const investmentAdditionalFilesAuditTable = "investment.additional_file_audit"

func splitInvestmentAuditTableName(tableName string) (string, string) {
	parts := strings.SplitN(strings.TrimSpace(tableName), ".", 2)
	if len(parts) != 2 {
		return "public", strings.TrimSpace(tableName)
	}
	return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
}

func loadInvestmentAuditColumns(ctx context.Context, tx pgx.Tx, tableName string) (map[string]investmentAuditColumn, error) {
	schemaName, relationName := splitInvestmentAuditTableName(tableName)
	rows, err := tx.Query(ctx, `
		SELECT column_name,
		       is_nullable = 'YES' AS nullable,
		       column_default IS NOT NULL OR is_identity = 'YES' AS has_default
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2
	`, schemaName, relationName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns := make(map[string]investmentAuditColumn)
	for rows.Next() {
		var name string
		var column investmentAuditColumn
		if err := rows.Scan(&name, &column.Nullable, &column.HasDefault); err != nil {
			return nil, err
		}
		columns[name] = column
	}
	return columns, rows.Err()
}

func firstInvestmentAuditColumn(columns map[string]investmentAuditColumn, candidates ...string) string {
	for _, candidate := range candidates {
		if _, ok := columns[candidate]; ok {
			return candidate
		}
	}
	return ""
}

func recordDynamicInvestmentMainUploadAudit(ctx context.Context, tx pgx.Tx, tableName string, parentColumns []string, parentID string, payload cashfiles.MainUploadAuditPayload, extraValues map[string]interface{}) error {
	reason, err := cashfiles.MainUploadAuditReasonJSON(payload)
	if err != nil {
		return err
	}

	columns, err := loadInvestmentAuditColumns(ctx, tx, tableName)
	if err != nil {
		return err
	}
	if len(columns) == 0 {
		return nil
	}

	values := map[string]interface{}{}
	parentColumn := firstInvestmentAuditColumn(columns, parentColumns...)
	if parentColumn != "" {
		values[parentColumn] = parentID
	}
	if actionColumn := firstInvestmentAuditColumn(columns, "action_type", "actiontype"); actionColumn != "" {
		values[actionColumn] = "UPLOAD_FILE"
	}
	if _, ok := columns["processing_status"]; ok {
		values["processing_status"] = constants.StatusApproved
	}
	if reasonColumn := firstInvestmentAuditColumn(columns, "reason", "action_reason"); reasonColumn != "" {
		values[reasonColumn] = reason
	}
	if requestedByColumn := firstInvestmentAuditColumn(columns, "requested_by", "performed_by", "uploaded_by"); requestedByColumn != "" {
		values[requestedByColumn] = payload.UploadedBy
	}
	if _, ok := columns["performed_by_email"]; ok {
		values["performed_by_email"] = payload.UploadedBy
	}
	if requestedAtColumn := firstInvestmentAuditColumn(columns, "requested_at", "created_at", "uploaded_at"); requestedAtColumn != "" {
		values[requestedAtColumn] = payload.UploadedAt
	}
	for key, value := range extraValues {
		if _, ok := columns[key]; ok {
			values[key] = value
		}
	}

	for columnName, column := range columns {
		if column.Nullable || column.HasDefault {
			continue
		}
		if _, ok := values[columnName]; !ok {
			return nil
		}
	}

	preferred := []string{
		parentColumn,
		"run_id",
		"fd_id",
		"action_type",
		"actiontype",
		"processing_status",
		"reason",
		"action_reason",
		"requested_by",
		"performed_by",
		"performed_by_email",
		"requested_at",
		"created_at",
	}
	insertColumns := make([]string, 0, len(values))
	args := make([]interface{}, 0, len(values))
	seen := map[string]bool{}
	for _, column := range preferred {
		if column == "" || seen[column] {
			continue
		}
		value, ok := values[column]
		if !ok {
			continue
		}
		insertColumns = append(insertColumns, column)
		args = append(args, value)
		seen[column] = true
	}
	for column, value := range values {
		if seen[column] {
			continue
		}
		insertColumns = append(insertColumns, column)
		args = append(args, value)
	}
	if len(insertColumns) == 0 {
		return nil
	}

	placeholders := make([]string, len(insertColumns))
	for i := range placeholders {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}
	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		tableName,
		strings.Join(insertColumns, ", "),
		strings.Join(placeholders, ", "),
	)
	_, err = tx.Exec(ctx, query, args...)
	return err
}

func recordInvestmentProposalMainUploadAudit(ctx context.Context, tx pgx.Tx, proposalID string, payload cashfiles.MainUploadAuditPayload) error {
	return cashfiles.InsertMainUploadAudit(
		ctx,
		tx,
		"investment.auditactionproposal",
		"proposal_id",
		"actiontype",
		proposalID,
		payload,
	)
}

func recordInvestmentConfirmationMainUploadAudit(ctx context.Context, tx pgx.Tx, confirmationID string, payload cashfiles.MainUploadAuditPayload) error {
	return cashfiles.InsertMainUploadAudit(
		ctx,
		tx,
		"investment.auditactioninvestmentconfirmation",
		"confirmation_id",
		"actiontype",
		confirmationID,
		payload,
	)
}

func recordInvestmentInitiationMainUploadAudit(ctx context.Context, tx pgx.Tx, initiationID string, payload cashfiles.MainUploadAuditPayload) error {
	return cashfiles.InsertMainUploadAudit(
		ctx,
		tx,
		"investment.auditactioninitiation",
		"initiation_id",
		"actiontype",
		initiationID,
		payload,
	)
}

func recordRedemptionInitiationMainUploadAudit(ctx context.Context, tx pgx.Tx, redemptionID string, payload cashfiles.MainUploadAuditPayload) error {
	return cashfiles.InsertMainUploadAudit(
		ctx,
		tx,
		"investment.auditactionredemption",
		"redemption_id",
		"actiontype",
		redemptionID,
		payload,
	)
}

func recordRedemptionConfirmationMainUploadAudit(ctx context.Context, tx pgx.Tx, redemptionConfirmID string, payload cashfiles.MainUploadAuditPayload) error {
	return cashfiles.InsertMainUploadAudit(
		ctx,
		tx,
		"investment.auditactionredemptionconfirmation",
		"redemption_confirm_id",
		"actiontype",
		redemptionConfirmID,
		payload,
	)
}

func recordAccountingActivityMainUploadAudit(ctx context.Context, tx pgx.Tx, activityID string, payload cashfiles.MainUploadAuditPayload) error {
	return cashfiles.InsertMainUploadAudit(
		ctx,
		tx,
		"investment.auditactionaccountingactivity",
		"activity_id",
		"actiontype",
		activityID,
		payload,
	)
}

func recordFDBookingMainUploadAudit(ctx context.Context, tx pgx.Tx, bookingID string, payload cashfiles.MainUploadAuditPayload) error {
	return recordDynamicInvestmentMainUploadAudit(ctx, tx, "investment.fd_audit_booking_request", []string{"booking_id"}, bookingID, payload, nil)
}

func recordFDConfirmationMainUploadAudit(ctx context.Context, tx pgx.Tx, confirmationID string, payload cashfiles.MainUploadAuditPayload) error {
	return recordDynamicInvestmentMainUploadAudit(ctx, tx, "investment.fd_audit_confirmation", []string{"confirmation_id"}, confirmationID, payload, nil)
}

func recordFDMasterMainUploadAudit(ctx context.Context, tx pgx.Tx, fdID string, payload cashfiles.MainUploadAuditPayload) error {
	return recordDynamicInvestmentMainUploadAudit(ctx, tx, "investment.fd_audit_master", []string{"fd_id", "master_id", "confirmation_id"}, fdID, payload, nil)
}

func recordFDClosureMainUploadAudit(ctx context.Context, tx pgx.Tx, closureRequestID string, payload cashfiles.MainUploadAuditPayload) error {
	reason, err := cashfiles.MainUploadAuditReasonJSON(payload)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO investment.fd_audit_closure_request (
			closure_request_id,
			action_type,
			processing_status,
			action_reason,
			performed_by,
			performed_by_email,
			created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		closureRequestID,
		"UPLOAD_FILE",
		"COMPLETED",
		reason,
		payload.UploadedBy,
		payload.UploadedBy,
		payload.UploadedAt,
	)
	return err
}

func recordFDCashflowMainUploadAudit(ctx context.Context, tx pgx.Tx, fdID string, payload cashfiles.MainUploadAuditPayload) error {
	return recordDynamicInvestmentMainUploadAudit(ctx, tx, "investment.fd_audit_cashflow_schedule", []string{"fd_id", "master_id"}, fdID, payload, nil)
}

func recordFDInterestReceiptMainUploadAudit(ctx context.Context, tx pgx.Tx, receiptID string, payload cashfiles.MainUploadAuditPayload) error {
	reason, err := cashfiles.MainUploadAuditReasonJSON(payload)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO investment.fd_interest_receipt_audit (
			receipt_id,
			action_type,
			processing_status,
			reason,
			requested_by,
			requested_at
		) VALUES ($1, $2, $3, $4, $5, $6)`,
		receiptID,
		"EDIT",
		constants.StatusApproved,
		reason,
		payload.UploadedBy,
		payload.UploadedAt,
	)
	return err
}

func recordFDTDSReceiptMainUploadAudit(ctx context.Context, tx pgx.Tx, tdsID string, payload cashfiles.MainUploadAuditPayload) error {
	reason, err := cashfiles.MainUploadAuditReasonJSON(payload)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO investment.fd_tds_receipt_audit (
			tds_id,
			action_type,
			processing_status,
			reason,
			requested_by,
			requested_at
		) VALUES ($1, $2, $3, $4, $5, $6)`,
		tdsID,
		"EDIT",
		constants.StatusApproved,
		reason,
		payload.UploadedBy,
		payload.UploadedAt,
	)
	return err
}

func recordFDReconcileResultMainUploadAudit(ctx context.Context, tx pgx.Tx, resultID string, payload cashfiles.MainUploadAuditPayload) error {
	return recordDynamicInvestmentMainUploadAudit(ctx, tx, "investment.fd_receipt_reconcile_result_audit", []string{"result_id"}, resultID, payload, nil)
}

func recordFDReceiptExceptionMainUploadAudit(ctx context.Context, tx pgx.Tx, exceptionID string, payload cashfiles.MainUploadAuditPayload) error {
	reason, err := cashfiles.MainUploadAuditReasonJSON(payload)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO investment.fd_receipt_exception_audit (
			exception_id,
			action_type,
			processing_status,
			reason,
			requested_by,
			requested_at
		) VALUES ($1, $2, $3, $4, $5, $6)`,
		exceptionID,
		"UPLOAD_FILE",
		"APPROVED",
		reason,
		payload.UploadedBy,
		payload.UploadedAt,
	)
	return err
}

func recordFDAccrualRunMainUploadAudit(ctx context.Context, tx pgx.Tx, runID string, payload cashfiles.MainUploadAuditPayload) error {
	return recordDynamicInvestmentMainUploadAudit(ctx, tx, "investment.fd_accrual_run_audit", []string{"run_id"}, runID, payload, nil)
}

func recordFDAccrualScheduleConfigMainUploadAudit(ctx context.Context, tx pgx.Tx, configID string, payload cashfiles.MainUploadAuditPayload) error {
	return recordDynamicInvestmentMainUploadAudit(ctx, tx, "investment.fd_accrual_schedule_config_audit", []string{"config_id"}, configID, payload, nil)
}

func recordFDAccrualLedgerMainUploadAudit(ctx context.Context, tx pgx.Tx, ledgerID string, payload cashfiles.MainUploadAuditPayload) error {
	var runID, fdID string
	if err := tx.QueryRow(ctx, `
		SELECT COALESCE(run_id, ''), COALESCE(fd_id, '')
		FROM investment.fd_accrual_ledger
		WHERE ledger_id = $1
	`, ledgerID).Scan(&runID, &fdID); err != nil {
		return err
	}

	return recordDynamicInvestmentMainUploadAudit(ctx, tx, "investment.fd_accrual_ledger_audit", []string{"ledger_id"}, ledgerID, payload, map[string]interface{}{
		"run_id": runID,
		"fd_id":  fdID,
	})
}

func recordFDAccountingJournalMainUploadAudit(ctx context.Context, tx pgx.Tx, entryID string, payload cashfiles.MainUploadAuditPayload) error {
	return recordDynamicInvestmentMainUploadAudit(ctx, tx, "investment.accounting_journal_entry_audit", []string{"entry_id"}, entryID, payload, nil)
}

func ListOnboardAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewListHandler(pool, investmentAdditionalFilesConfig(onboardingFilesDefinition))
}

func UploadOnboardAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewUploadHandler(pool, investmentAdditionalFilesConfig(onboardingFilesDefinition))
}

func DownloadOnboardAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDownloadHandler(pool, investmentAdditionalFilesConfig(onboardingFilesDefinition))
}

func DownloadSelectedOnboardAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDownloadSelectedHandler(pool, investmentAdditionalFilesConfig(onboardingFilesDefinition))
}

func DeleteOnboardAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDeleteHandler(pool, investmentAdditionalFilesConfig(onboardingFilesDefinition))
}

func AuditOnboardAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewAuditHandler(pool, investmentAdditionalFilesConfig(onboardingFilesDefinition))
}

func ApproveDeleteOnboardAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewApproveDeleteHandler(pool, investmentAdditionalFilesConfig(onboardingFilesDefinition))
}

func RejectDeleteOnboardAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewRejectDeleteHandler(pool, investmentAdditionalFilesConfig(onboardingFilesDefinition))
}

func ListProposalAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewListHandler(pool, investmentAdditionalFilesConfig(proposalFilesDefinition))
}

func UploadProposalAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewUploadHandler(pool, investmentAdditionalFilesConfig(proposalFilesDefinition))
}

func DownloadProposalAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDownloadHandler(pool, investmentAdditionalFilesConfig(proposalFilesDefinition))
}

func DownloadSelectedProposalAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDownloadSelectedHandler(pool, investmentAdditionalFilesConfig(proposalFilesDefinition))
}

func DeleteProposalAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDeleteHandler(pool, investmentAdditionalFilesConfig(proposalFilesDefinition))
}

func AuditProposalAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewAuditHandler(pool, investmentAdditionalFilesConfig(proposalFilesDefinition))
}

func ApproveDeleteProposalAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewApproveDeleteHandler(pool, investmentAdditionalFilesConfig(proposalFilesDefinition))
}

func RejectDeleteProposalAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewRejectDeleteHandler(pool, investmentAdditionalFilesConfig(proposalFilesDefinition))
}

func ListInitiationAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewListHandler(pool, investmentAdditionalFilesConfig(initiationFilesDefinition))
}

func UploadInitiationAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewUploadHandler(pool, investmentAdditionalFilesConfig(initiationFilesDefinition))
}

func DownloadInitiationAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDownloadHandler(pool, investmentAdditionalFilesConfig(initiationFilesDefinition))
}

func DownloadSelectedInitiationAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDownloadSelectedHandler(pool, investmentAdditionalFilesConfig(initiationFilesDefinition))
}

func DeleteInitiationAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDeleteHandler(pool, investmentAdditionalFilesConfig(initiationFilesDefinition))
}

func AuditInitiationAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewAuditHandler(pool, investmentAdditionalFilesConfig(initiationFilesDefinition))
}

func ApproveDeleteInitiationAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewApproveDeleteHandler(pool, investmentAdditionalFilesConfig(initiationFilesDefinition))
}

func RejectDeleteInitiationAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewRejectDeleteHandler(pool, investmentAdditionalFilesConfig(initiationFilesDefinition))
}

func ListConfirmationAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewListHandler(pool, investmentAdditionalFilesConfig(confirmationFilesDefinition))
}

func UploadConfirmationAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewUploadHandler(pool, investmentAdditionalFilesConfig(confirmationFilesDefinition))
}

func DownloadConfirmationAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDownloadHandler(pool, investmentAdditionalFilesConfig(confirmationFilesDefinition))
}

func DownloadSelectedConfirmationAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDownloadSelectedHandler(pool, investmentAdditionalFilesConfig(confirmationFilesDefinition))
}

func DeleteConfirmationAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDeleteHandler(pool, investmentAdditionalFilesConfig(confirmationFilesDefinition))
}

func AuditConfirmationAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewAuditHandler(pool, investmentAdditionalFilesConfig(confirmationFilesDefinition))
}

func ApproveDeleteConfirmationAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewApproveDeleteHandler(pool, investmentAdditionalFilesConfig(confirmationFilesDefinition))
}

func RejectDeleteConfirmationAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewRejectDeleteHandler(pool, investmentAdditionalFilesConfig(confirmationFilesDefinition))
}

func ListRedemptionInitiationAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewListHandler(pool, investmentAdditionalFilesConfig(redemptionInitiationFilesDefinition))
}

func UploadRedemptionInitiationAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewUploadHandler(pool, investmentAdditionalFilesConfig(redemptionInitiationFilesDefinition))
}

func DownloadRedemptionInitiationAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDownloadHandler(pool, investmentAdditionalFilesConfig(redemptionInitiationFilesDefinition))
}

func DownloadSelectedRedemptionInitiationAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDownloadSelectedHandler(pool, investmentAdditionalFilesConfig(redemptionInitiationFilesDefinition))
}

func DeleteRedemptionInitiationAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDeleteHandler(pool, investmentAdditionalFilesConfig(redemptionInitiationFilesDefinition))
}

func AuditRedemptionInitiationAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewAuditHandler(pool, investmentAdditionalFilesConfig(redemptionInitiationFilesDefinition))
}

func ApproveDeleteRedemptionInitiationAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewApproveDeleteHandler(pool, investmentAdditionalFilesConfig(redemptionInitiationFilesDefinition))
}

func RejectDeleteRedemptionInitiationAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewRejectDeleteHandler(pool, investmentAdditionalFilesConfig(redemptionInitiationFilesDefinition))
}

func ListRedemptionConfirmationAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewListHandler(pool, investmentAdditionalFilesConfig(redemptionConfirmationFilesDefinition))
}

func UploadRedemptionConfirmationAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewUploadHandler(pool, investmentAdditionalFilesConfig(redemptionConfirmationFilesDefinition))
}

func DownloadRedemptionConfirmationAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDownloadHandler(pool, investmentAdditionalFilesConfig(redemptionConfirmationFilesDefinition))
}

func DownloadSelectedRedemptionConfirmationAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDownloadSelectedHandler(pool, investmentAdditionalFilesConfig(redemptionConfirmationFilesDefinition))
}

func DeleteRedemptionConfirmationAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDeleteHandler(pool, investmentAdditionalFilesConfig(redemptionConfirmationFilesDefinition))
}

func AuditRedemptionConfirmationAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewAuditHandler(pool, investmentAdditionalFilesConfig(redemptionConfirmationFilesDefinition))
}

func ApproveDeleteRedemptionConfirmationAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewApproveDeleteHandler(pool, investmentAdditionalFilesConfig(redemptionConfirmationFilesDefinition))
}

func RejectDeleteRedemptionConfirmationAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewRejectDeleteHandler(pool, investmentAdditionalFilesConfig(redemptionConfirmationFilesDefinition))
}

func ListAccountingActivityAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewListHandler(pool, investmentAdditionalFilesConfig(accountingActivityFilesDefinition))
}

func UploadAccountingActivityAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewUploadHandler(pool, investmentAdditionalFilesConfig(accountingActivityFilesDefinition))
}

func DownloadAccountingActivityAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDownloadHandler(pool, investmentAdditionalFilesConfig(accountingActivityFilesDefinition))
}

func DownloadSelectedAccountingActivityAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDownloadSelectedHandler(pool, investmentAdditionalFilesConfig(accountingActivityFilesDefinition))
}

func DeleteAccountingActivityAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDeleteHandler(pool, investmentAdditionalFilesConfig(accountingActivityFilesDefinition))
}

func AuditAccountingActivityAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewAuditHandler(pool, investmentAdditionalFilesConfig(accountingActivityFilesDefinition))
}

func ApproveDeleteAccountingActivityAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewApproveDeleteHandler(pool, investmentAdditionalFilesConfig(accountingActivityFilesDefinition))
}

func RejectDeleteAccountingActivityAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewRejectDeleteHandler(pool, investmentAdditionalFilesConfig(accountingActivityFilesDefinition))
}

func ListFDBookingAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewListHandler(pool, investmentAdditionalFilesConfig(fdBookingFilesDefinition))
}

func UploadFDBookingAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewUploadHandler(pool, investmentAdditionalFilesConfig(fdBookingFilesDefinition))
}

func DownloadFDBookingAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDownloadHandler(pool, investmentAdditionalFilesConfig(fdBookingFilesDefinition))
}

func DownloadSelectedFDBookingAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDownloadSelectedHandler(pool, investmentAdditionalFilesConfig(fdBookingFilesDefinition))
}

func DeleteFDBookingAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDeleteHandler(pool, investmentAdditionalFilesConfig(fdBookingFilesDefinition))
}

func AuditFDBookingAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewAuditHandler(pool, investmentAdditionalFilesConfig(fdBookingFilesDefinition))
}

func ApproveDeleteFDBookingAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewApproveDeleteHandler(pool, investmentAdditionalFilesConfig(fdBookingFilesDefinition))
}

func RejectDeleteFDBookingAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewRejectDeleteHandler(pool, investmentAdditionalFilesConfig(fdBookingFilesDefinition))
}

func ListFDConfirmationAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewListHandler(pool, investmentAdditionalFilesConfig(fdConfirmationFilesDefinition))
}

func UploadFDConfirmationAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewUploadHandler(pool, investmentAdditionalFilesConfig(fdConfirmationFilesDefinition))
}

func DownloadFDConfirmationAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDownloadHandler(pool, investmentAdditionalFilesConfig(fdConfirmationFilesDefinition))
}

func DownloadSelectedFDConfirmationAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDownloadSelectedHandler(pool, investmentAdditionalFilesConfig(fdConfirmationFilesDefinition))
}

func DeleteFDConfirmationAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDeleteHandler(pool, investmentAdditionalFilesConfig(fdConfirmationFilesDefinition))
}

func AuditFDConfirmationAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewAuditHandler(pool, investmentAdditionalFilesConfig(fdConfirmationFilesDefinition))
}

func ApproveDeleteFDConfirmationAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewApproveDeleteHandler(pool, investmentAdditionalFilesConfig(fdConfirmationFilesDefinition))
}

func RejectDeleteFDConfirmationAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewRejectDeleteHandler(pool, investmentAdditionalFilesConfig(fdConfirmationFilesDefinition))
}

func ListFDMasterAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewListHandler(pool, investmentAdditionalFilesConfig(fdMasterFilesDefinition))
}

func UploadFDMasterAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewUploadHandler(pool, investmentAdditionalFilesConfig(fdMasterFilesDefinition))
}

func DownloadFDMasterAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDownloadHandler(pool, investmentAdditionalFilesConfig(fdMasterFilesDefinition))
}

func DownloadSelectedFDMasterAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDownloadSelectedHandler(pool, investmentAdditionalFilesConfig(fdMasterFilesDefinition))
}

func DeleteFDMasterAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDeleteHandler(pool, investmentAdditionalFilesConfig(fdMasterFilesDefinition))
}

func AuditFDMasterAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewAuditHandler(pool, investmentAdditionalFilesConfig(fdMasterFilesDefinition))
}

func ApproveDeleteFDMasterAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewApproveDeleteHandler(pool, investmentAdditionalFilesConfig(fdMasterFilesDefinition))
}

func RejectDeleteFDMasterAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewRejectDeleteHandler(pool, investmentAdditionalFilesConfig(fdMasterFilesDefinition))
}

func ListFDClosureAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewListHandler(pool, investmentAdditionalFilesConfig(fdClosureFilesDefinition))
}

func UploadFDClosureAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewUploadHandler(pool, investmentAdditionalFilesConfig(fdClosureFilesDefinition))
}

func DownloadFDClosureAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDownloadHandler(pool, investmentAdditionalFilesConfig(fdClosureFilesDefinition))
}

func DownloadSelectedFDClosureAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDownloadSelectedHandler(pool, investmentAdditionalFilesConfig(fdClosureFilesDefinition))
}

func DeleteFDClosureAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDeleteHandler(pool, investmentAdditionalFilesConfig(fdClosureFilesDefinition))
}

func AuditFDClosureAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewAuditHandler(pool, investmentAdditionalFilesConfig(fdClosureFilesDefinition))
}

func ApproveDeleteFDClosureAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewApproveDeleteHandler(pool, investmentAdditionalFilesConfig(fdClosureFilesDefinition))
}

func RejectDeleteFDClosureAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewRejectDeleteHandler(pool, investmentAdditionalFilesConfig(fdClosureFilesDefinition))
}

func ListFDRolloverAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewListHandler(pool, investmentAdditionalFilesConfig(fdRolloverFilesDefinition))
}

func UploadFDRolloverAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewUploadHandler(pool, investmentAdditionalFilesConfig(fdRolloverFilesDefinition))
}

func DownloadFDRolloverAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDownloadHandler(pool, investmentAdditionalFilesConfig(fdRolloverFilesDefinition))
}

func DownloadSelectedFDRolloverAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDownloadSelectedHandler(pool, investmentAdditionalFilesConfig(fdRolloverFilesDefinition))
}

func DeleteFDRolloverAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDeleteHandler(pool, investmentAdditionalFilesConfig(fdRolloverFilesDefinition))
}

func AuditFDRolloverAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewAuditHandler(pool, investmentAdditionalFilesConfig(fdRolloverFilesDefinition))
}

func ApproveDeleteFDRolloverAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewApproveDeleteHandler(pool, investmentAdditionalFilesConfig(fdRolloverFilesDefinition))
}

func RejectDeleteFDRolloverAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewRejectDeleteHandler(pool, investmentAdditionalFilesConfig(fdRolloverFilesDefinition))
}

func ListFDCashflowAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewListHandler(pool, investmentAdditionalFilesConfig(fdCashflowFilesDefinition))
}

func UploadFDCashflowAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewUploadHandler(pool, investmentAdditionalFilesConfig(fdCashflowFilesDefinition))
}

func DownloadFDCashflowAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDownloadHandler(pool, investmentAdditionalFilesConfig(fdCashflowFilesDefinition))
}

func DownloadSelectedFDCashflowAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDownloadSelectedHandler(pool, investmentAdditionalFilesConfig(fdCashflowFilesDefinition))
}

func DeleteFDCashflowAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDeleteHandler(pool, investmentAdditionalFilesConfig(fdCashflowFilesDefinition))
}

func AuditFDCashflowAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewAuditHandler(pool, investmentAdditionalFilesConfig(fdCashflowFilesDefinition))
}

func ApproveDeleteFDCashflowAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewApproveDeleteHandler(pool, investmentAdditionalFilesConfig(fdCashflowFilesDefinition))
}

func RejectDeleteFDCashflowAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewRejectDeleteHandler(pool, investmentAdditionalFilesConfig(fdCashflowFilesDefinition))
}

func ListFDInterestReceiptAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewListHandler(pool, investmentAdditionalFilesConfig(fdInterestReceiptFilesDefinition))
}

func UploadFDInterestReceiptAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewUploadHandler(pool, investmentAdditionalFilesConfig(fdInterestReceiptFilesDefinition))
}

func DownloadFDInterestReceiptAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDownloadHandler(pool, investmentAdditionalFilesConfig(fdInterestReceiptFilesDefinition))
}

func DownloadSelectedFDInterestReceiptAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDownloadSelectedHandler(pool, investmentAdditionalFilesConfig(fdInterestReceiptFilesDefinition))
}

func DeleteFDInterestReceiptAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDeleteHandler(pool, investmentAdditionalFilesConfig(fdInterestReceiptFilesDefinition))
}

func AuditFDInterestReceiptAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewAuditHandler(pool, investmentAdditionalFilesConfig(fdInterestReceiptFilesDefinition))
}

func ApproveDeleteFDInterestReceiptAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewApproveDeleteHandler(pool, investmentAdditionalFilesConfig(fdInterestReceiptFilesDefinition))
}

func RejectDeleteFDInterestReceiptAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewRejectDeleteHandler(pool, investmentAdditionalFilesConfig(fdInterestReceiptFilesDefinition))
}

func ListFDTDSReceiptAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewListHandler(pool, investmentAdditionalFilesConfig(fdTDSReceiptFilesDefinition))
}

func UploadFDTDSReceiptAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewUploadHandler(pool, investmentAdditionalFilesConfig(fdTDSReceiptFilesDefinition))
}

func DownloadFDTDSReceiptAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDownloadHandler(pool, investmentAdditionalFilesConfig(fdTDSReceiptFilesDefinition))
}

func DownloadSelectedFDTDSReceiptAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDownloadSelectedHandler(pool, investmentAdditionalFilesConfig(fdTDSReceiptFilesDefinition))
}

func DeleteFDTDSReceiptAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDeleteHandler(pool, investmentAdditionalFilesConfig(fdTDSReceiptFilesDefinition))
}

func AuditFDTDSReceiptAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewAuditHandler(pool, investmentAdditionalFilesConfig(fdTDSReceiptFilesDefinition))
}

func ApproveDeleteFDTDSReceiptAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewApproveDeleteHandler(pool, investmentAdditionalFilesConfig(fdTDSReceiptFilesDefinition))
}

func RejectDeleteFDTDSReceiptAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewRejectDeleteHandler(pool, investmentAdditionalFilesConfig(fdTDSReceiptFilesDefinition))
}

func ListFDReconcileResultAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewListHandler(pool, investmentAdditionalFilesConfig(fdReconcileResultFilesDefinition))
}

func UploadFDReconcileResultAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewUploadHandler(pool, investmentAdditionalFilesConfig(fdReconcileResultFilesDefinition))
}

func DownloadFDReconcileResultAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDownloadHandler(pool, investmentAdditionalFilesConfig(fdReconcileResultFilesDefinition))
}

func DownloadSelectedFDReconcileResultAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDownloadSelectedHandler(pool, investmentAdditionalFilesConfig(fdReconcileResultFilesDefinition))
}

func DeleteFDReconcileResultAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDeleteHandler(pool, investmentAdditionalFilesConfig(fdReconcileResultFilesDefinition))
}

func AuditFDReconcileResultAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewAuditHandler(pool, investmentAdditionalFilesConfig(fdReconcileResultFilesDefinition))
}

func ApproveDeleteFDReconcileResultAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewApproveDeleteHandler(pool, investmentAdditionalFilesConfig(fdReconcileResultFilesDefinition))
}

func RejectDeleteFDReconcileResultAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewRejectDeleteHandler(pool, investmentAdditionalFilesConfig(fdReconcileResultFilesDefinition))
}

func ListFDReceiptExceptionAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewListHandler(pool, investmentAdditionalFilesConfig(fdReceiptExceptionFilesDefinition))
}

func UploadFDReceiptExceptionAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewUploadHandler(pool, investmentAdditionalFilesConfig(fdReceiptExceptionFilesDefinition))
}

func DownloadFDReceiptExceptionAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDownloadHandler(pool, investmentAdditionalFilesConfig(fdReceiptExceptionFilesDefinition))
}

func DownloadSelectedFDReceiptExceptionAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDownloadSelectedHandler(pool, investmentAdditionalFilesConfig(fdReceiptExceptionFilesDefinition))
}

func DeleteFDReceiptExceptionAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDeleteHandler(pool, investmentAdditionalFilesConfig(fdReceiptExceptionFilesDefinition))
}

func AuditFDReceiptExceptionAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewAuditHandler(pool, investmentAdditionalFilesConfig(fdReceiptExceptionFilesDefinition))
}

func ApproveDeleteFDReceiptExceptionAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewApproveDeleteHandler(pool, investmentAdditionalFilesConfig(fdReceiptExceptionFilesDefinition))
}

func RejectDeleteFDReceiptExceptionAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewRejectDeleteHandler(pool, investmentAdditionalFilesConfig(fdReceiptExceptionFilesDefinition))
}

func ListVarianceExceptionAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewListHandler(pool, investmentAdditionalFilesConfig(varianceExceptionFilesDefinition))
}

func UploadVarianceExceptionAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewUploadHandler(pool, investmentAdditionalFilesConfig(varianceExceptionFilesDefinition))
}

func DownloadVarianceExceptionAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDownloadHandler(pool, investmentAdditionalFilesConfig(varianceExceptionFilesDefinition))
}

func DownloadSelectedVarianceExceptionAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDownloadSelectedHandler(pool, investmentAdditionalFilesConfig(varianceExceptionFilesDefinition))
}

func DeleteVarianceExceptionAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDeleteHandler(pool, investmentAdditionalFilesConfig(varianceExceptionFilesDefinition))
}

func AuditVarianceExceptionAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewAuditHandler(pool, investmentAdditionalFilesConfig(varianceExceptionFilesDefinition))
}

func ApproveDeleteVarianceExceptionAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewApproveDeleteHandler(pool, investmentAdditionalFilesConfig(varianceExceptionFilesDefinition))
}

func RejectDeleteVarianceExceptionAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewRejectDeleteHandler(pool, investmentAdditionalFilesConfig(varianceExceptionFilesDefinition))
}

func ListFDAccrualRunAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewListHandler(pool, investmentAdditionalFilesConfig(fdAccrualRunFilesDefinition))
}

func UploadFDAccrualRunAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewUploadHandler(pool, investmentAdditionalFilesConfig(fdAccrualRunFilesDefinition))
}

func DownloadFDAccrualRunAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDownloadHandler(pool, investmentAdditionalFilesConfig(fdAccrualRunFilesDefinition))
}

func DownloadSelectedFDAccrualRunAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDownloadSelectedHandler(pool, investmentAdditionalFilesConfig(fdAccrualRunFilesDefinition))
}

func DeleteFDAccrualRunAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDeleteHandler(pool, investmentAdditionalFilesConfig(fdAccrualRunFilesDefinition))
}

func AuditFDAccrualRunAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewAuditHandler(pool, investmentAdditionalFilesConfig(fdAccrualRunFilesDefinition))
}

func ApproveDeleteFDAccrualRunAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewApproveDeleteHandler(pool, investmentAdditionalFilesConfig(fdAccrualRunFilesDefinition))
}

func RejectDeleteFDAccrualRunAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewRejectDeleteHandler(pool, investmentAdditionalFilesConfig(fdAccrualRunFilesDefinition))
}

func ListFDAccrualScheduleConfigAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewListHandler(pool, investmentAdditionalFilesConfig(fdAccrualScheduleConfigFilesDefinition))
}

func UploadFDAccrualScheduleConfigAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewUploadHandler(pool, investmentAdditionalFilesConfig(fdAccrualScheduleConfigFilesDefinition))
}

func DownloadFDAccrualScheduleConfigAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDownloadHandler(pool, investmentAdditionalFilesConfig(fdAccrualScheduleConfigFilesDefinition))
}

func DownloadSelectedFDAccrualScheduleConfigAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDownloadSelectedHandler(pool, investmentAdditionalFilesConfig(fdAccrualScheduleConfigFilesDefinition))
}

func DeleteFDAccrualScheduleConfigAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDeleteHandler(pool, investmentAdditionalFilesConfig(fdAccrualScheduleConfigFilesDefinition))
}

func AuditFDAccrualScheduleConfigAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewAuditHandler(pool, investmentAdditionalFilesConfig(fdAccrualScheduleConfigFilesDefinition))
}

func ApproveDeleteFDAccrualScheduleConfigAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewApproveDeleteHandler(pool, investmentAdditionalFilesConfig(fdAccrualScheduleConfigFilesDefinition))
}

func RejectDeleteFDAccrualScheduleConfigAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewRejectDeleteHandler(pool, investmentAdditionalFilesConfig(fdAccrualScheduleConfigFilesDefinition))
}

func ListFDAccrualLedgerAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewListHandler(pool, investmentAdditionalFilesConfig(fdAccrualLedgerFilesDefinition))
}

func UploadFDAccrualLedgerAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewUploadHandler(pool, investmentAdditionalFilesConfig(fdAccrualLedgerFilesDefinition))
}

func DownloadFDAccrualLedgerAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDownloadHandler(pool, investmentAdditionalFilesConfig(fdAccrualLedgerFilesDefinition))
}

func DownloadSelectedFDAccrualLedgerAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDownloadSelectedHandler(pool, investmentAdditionalFilesConfig(fdAccrualLedgerFilesDefinition))
}

func DeleteFDAccrualLedgerAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDeleteHandler(pool, investmentAdditionalFilesConfig(fdAccrualLedgerFilesDefinition))
}

func AuditFDAccrualLedgerAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewAuditHandler(pool, investmentAdditionalFilesConfig(fdAccrualLedgerFilesDefinition))
}

func ApproveDeleteFDAccrualLedgerAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewApproveDeleteHandler(pool, investmentAdditionalFilesConfig(fdAccrualLedgerFilesDefinition))
}

func RejectDeleteFDAccrualLedgerAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewRejectDeleteHandler(pool, investmentAdditionalFilesConfig(fdAccrualLedgerFilesDefinition))
}

func ListFDAccountingJournalAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewListHandler(pool, investmentAdditionalFilesConfig(fdAccountingJournalFilesDefinition))
}

func UploadFDAccountingJournalAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewUploadHandler(pool, investmentAdditionalFilesConfig(fdAccountingJournalFilesDefinition))
}

func DownloadFDAccountingJournalAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDownloadHandler(pool, investmentAdditionalFilesConfig(fdAccountingJournalFilesDefinition))
}

func DownloadSelectedFDAccountingJournalAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDownloadSelectedHandler(pool, investmentAdditionalFilesConfig(fdAccountingJournalFilesDefinition))
}

func DeleteFDAccountingJournalAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDeleteHandler(pool, investmentAdditionalFilesConfig(fdAccountingJournalFilesDefinition))
}

func AuditFDAccountingJournalAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewAuditHandler(pool, investmentAdditionalFilesConfig(fdAccountingJournalFilesDefinition))
}

func ApproveDeleteFDAccountingJournalAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewApproveDeleteHandler(pool, investmentAdditionalFilesConfig(fdAccountingJournalFilesDefinition))
}

func RejectDeleteFDAccountingJournalAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewRejectDeleteHandler(pool, investmentAdditionalFilesConfig(fdAccountingJournalFilesDefinition))
}

func investmentAdditionalFilesConfig(def investmentFileDefinition) cashfiles.Config {
	cfg := cashfiles.Config{
		Module:         def.Module,
		AuditSource:    strings.ToUpper(strings.ReplaceAll(def.Module, "-", "_")),
		AuditTableName: investmentAdditionalFilesAuditTable,
		ParentIDField:  def.ParentIDField,
		List: func(ctx context.Context, pool *pgxpool.Pool, parentID string) ([]cashfiles.FileRecord, error) {
			return listInvestmentAdditionalFiles(ctx, pool, def, parentID)
		},
		Create: func(ctx context.Context, tx pgx.Tx, input cashfiles.CreateInput) error {
			return createInvestmentAdditionalFile(ctx, tx, def, input)
		},
		CreateReturning: func(ctx context.Context, tx pgx.Tx, input cashfiles.CreateInput) (string, error) {
			return createInvestmentAdditionalFileReturningID(ctx, tx, def, input)
		},
		GetOne: func(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*cashfiles.FileRecord, error) {
			return getInvestmentAdditionalFile(ctx, pool, def, parentID, fileID, false)
		},
		GetAnyFile: func(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*cashfiles.FileRecord, error) {
			return getInvestmentAdditionalFile(ctx, pool, def, parentID, fileID, true)
		},
		GetMany: func(ctx context.Context, pool *pgxpool.Pool, parentID string, fileIDs []string) ([]cashfiles.FileRecord, []string, error) {
			return getInvestmentAdditionalFiles(ctx, pool, def, parentID, fileIDs)
		},
		DuplicateExists: func(ctx context.Context, pool *pgxpool.Pool, parentID, fileHash string) (bool, error) {
			return investmentAdditionalFileDuplicateExists(ctx, pool, def, parentID, fileHash)
		},
		SoftDelete: func(ctx context.Context, pool *pgxpool.Pool, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
			return deleteInvestmentAdditionalFile(ctx, pool, def, parentID, fileID, deletedBy, deletedAt)
		},
		SoftDeleteTx: func(ctx context.Context, tx pgx.Tx, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
			return deleteInvestmentAdditionalFile(ctx, tx, def, parentID, fileID, deletedBy, deletedAt)
		},
	}

	switch def.Module {
	case proposalFilesDefinition.Module:
		cfg.RecordMainUploadAudit = recordInvestmentProposalMainUploadAudit
	case initiationFilesDefinition.Module:
		cfg.RecordMainUploadAudit = recordInvestmentInitiationMainUploadAudit
	case confirmationFilesDefinition.Module:
		cfg.RecordMainUploadAudit = recordInvestmentConfirmationMainUploadAudit
	case redemptionInitiationFilesDefinition.Module:
		cfg.RecordMainUploadAudit = recordRedemptionInitiationMainUploadAudit
	case redemptionConfirmationFilesDefinition.Module:
		cfg.RecordMainUploadAudit = recordRedemptionConfirmationMainUploadAudit
	case accountingActivityFilesDefinition.Module:
		cfg.RecordMainUploadAudit = recordAccountingActivityMainUploadAudit
	case fdBookingFilesDefinition.Module:
		cfg.RecordMainUploadAudit = recordFDBookingMainUploadAudit
	case fdConfirmationFilesDefinition.Module:
		cfg.RecordMainUploadAudit = recordFDConfirmationMainUploadAudit
	case fdMasterFilesDefinition.Module:
		cfg.RecordMainUploadAudit = recordFDMasterMainUploadAudit
	case fdClosureFilesDefinition.Module, fdRolloverFilesDefinition.Module:
		cfg.RecordMainUploadAudit = recordFDClosureMainUploadAudit
	case fdCashflowFilesDefinition.Module:
		cfg.RecordMainUploadAudit = recordFDCashflowMainUploadAudit
	case fdInterestReceiptFilesDefinition.Module:
		cfg.RecordMainUploadAudit = recordFDInterestReceiptMainUploadAudit
	case fdTDSReceiptFilesDefinition.Module:
		cfg.RecordMainUploadAudit = recordFDTDSReceiptMainUploadAudit
	case fdReconcileResultFilesDefinition.Module:
		cfg.RecordMainUploadAudit = recordFDReconcileResultMainUploadAudit
	case fdReceiptExceptionFilesDefinition.Module:
		cfg.RecordMainUploadAudit = recordFDReceiptExceptionMainUploadAudit
	case varianceExceptionFilesDefinition.Module:
		cfg.RecordMainUploadAudit = recordFDReceiptExceptionMainUploadAudit
		cfg.RequireMainUploadAudit = true
	case fdAccrualRunFilesDefinition.Module:
		cfg.RecordMainUploadAudit = recordFDAccrualRunMainUploadAudit
	case fdAccrualScheduleConfigFilesDefinition.Module:
		cfg.RecordMainUploadAudit = recordFDAccrualScheduleConfigMainUploadAudit
	case fdAccrualLedgerFilesDefinition.Module:
		cfg.RecordMainUploadAudit = recordFDAccrualLedgerMainUploadAudit
	case fdAccountingJournalFilesDefinition.Module:
		cfg.RecordMainUploadAudit = recordFDAccountingJournalMainUploadAudit
	}

	return cfg
}

func listInvestmentAdditionalFiles(ctx context.Context, pool *pgxpool.Pool, def investmentFileDefinition, parentID string) ([]cashfiles.FileRecord, error) {
	query := fmt.Sprintf(`
		SELECT f.file_id, f.stored_file_name, f.content_type, f.file_size, f.upload_s3_key, f.uploaded_by, f.uploaded_at
		FROM %s f
		JOIN %s p ON p.%s = f.%s
		WHERE f.%s::text = $1
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		  %s
		ORDER BY f.uploaded_at DESC
	`, def.TableName, def.ParentTable, def.ParentColumn, def.ParentColumn, def.ParentColumn, def.ParentFilter)
	return cashfiles.QueryFiles(ctx, pool, query, strings.TrimSpace(parentID))
}

func createInvestmentAdditionalFile(ctx context.Context, tx pgx.Tx, def investmentFileDefinition, input cashfiles.CreateInput) error {
	_, err := createInvestmentAdditionalFileReturningID(ctx, tx, def, input)
	return err
}

func createInvestmentAdditionalFileReturningID(ctx context.Context, tx pgx.Tx, def investmentFileDefinition, input cashfiles.CreateInput) (string, error) {
	parentScope := fmt.Sprintf(`
		SELECT p.%s AS parent_id
		FROM %s p
		WHERE p.%s::text = $8
		  %s
	`, def.ParentColumn, def.ParentTable, def.ParentColumn, def.ParentFilter)
	return cashfiles.InsertAdditionalFileRowReturningID(ctx, tx, def.TableName, def.ParentColumn, input, parentScope, strings.TrimSpace(input.ParentID))
}

func investmentAdditionalFileDuplicateExists(ctx context.Context, pool *pgxpool.Pool, def investmentFileDefinition, parentID, fileHash string) (bool, error) {
	parentID = strings.TrimSpace(parentID)
	fileHash = strings.TrimSpace(fileHash)
	if parentID == "" || fileHash == "" {
		return false, nil
	}

	query := fmt.Sprintf(`
		SELECT EXISTS (
			SELECT 1
			FROM %s f
			JOIN %s p ON p.%s = f.%s
			WHERE f.%s::text = $1
			  AND f.file_hash = $2
			  AND COALESCE(f.is_deleted, FALSE) = FALSE
			  %s
		)
	`, def.TableName, def.ParentTable, def.ParentColumn, def.ParentColumn, def.ParentColumn, def.ParentFilter)

	var exists bool
	if err := pool.QueryRow(ctx, query, parentID, fileHash).Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
}

func getInvestmentAdditionalFile(ctx context.Context, pool *pgxpool.Pool, def investmentFileDefinition, parentID, fileID string, includeDeleted bool) (*cashfiles.FileRecord, error) {
	deletedClause := "AND COALESCE(f.is_deleted, FALSE) = FALSE"
	if includeDeleted {
		deletedClause = ""
	}

	query := fmt.Sprintf(`
		SELECT f.file_id, f.stored_file_name, f.content_type, f.file_size, f.upload_s3_key, f.uploaded_by, f.uploaded_at
		FROM %s f
		JOIN %s p ON p.%s = f.%s
		WHERE f.%s::text = $1
		  AND f.file_id = $2
		  %s
		  %s
	`, def.TableName, def.ParentTable, def.ParentColumn, def.ParentColumn, def.ParentColumn, deletedClause, def.ParentFilter)
	return cashfiles.FirstFile(ctx, pool, query, strings.TrimSpace(parentID), strings.TrimSpace(fileID))
}

func getInvestmentAdditionalFiles(ctx context.Context, pool *pgxpool.Pool, def investmentFileDefinition, parentID string, fileIDs []string) ([]cashfiles.FileRecord, []string, error) {
	trimmedIDs := trimInvestmentAdditionalFileIDs(fileIDs)
	query := fmt.Sprintf(`
		SELECT f.file_id, f.stored_file_name, f.content_type, f.file_size, f.upload_s3_key, f.uploaded_by, f.uploaded_at
		FROM %s f
		JOIN %s p ON p.%s = f.%s
		WHERE f.%s::text = $1
		  AND f.file_id = ANY($2)
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		  %s
		ORDER BY f.uploaded_at DESC
	`, def.TableName, def.ParentTable, def.ParentColumn, def.ParentColumn, def.ParentColumn, def.ParentFilter)

	files, err := cashfiles.QueryFiles(ctx, pool, query, strings.TrimSpace(parentID), trimmedIDs)
	if err != nil {
		return nil, nil, err
	}
	return files, missingInvestmentAdditionalFileIDs(trimmedIDs, files), nil
}

type investmentFileExec interface {
	Exec(context.Context, string, ...interface{}) (pgconn.CommandTag, error)
}

func deleteInvestmentAdditionalFile(ctx context.Context, exec investmentFileExec, def investmentFileDefinition, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	query := fmt.Sprintf(`
		UPDATE %s f
		SET is_deleted = TRUE,
		    deleted_by = $3,
		    deleted_at = $4
		FROM %s p
		WHERE f.%s::text = $1
		  AND f.file_id = $2
		  AND p.%s = f.%s
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		  %s
	`, def.TableName, def.ParentTable, def.ParentColumn, def.ParentColumn, def.ParentColumn, def.ParentFilter)
	result, err := exec.Exec(ctx, query, strings.TrimSpace(parentID), strings.TrimSpace(fileID), deletedBy, deletedAt)
	if err != nil {
		return false, err
	}
	return result.RowsAffected() > 0, nil
}

func trimInvestmentAdditionalFileIDs(fileIDs []string) []string {
	trimmed := make([]string, 0, len(fileIDs))
	seen := make(map[string]struct{}, len(fileIDs))
	for _, fileID := range fileIDs {
		candidate := strings.TrimSpace(fileID)
		if candidate == "" {
			continue
		}
		if _, exists := seen[candidate]; exists {
			continue
		}
		seen[candidate] = struct{}{}
		trimmed = append(trimmed, candidate)
	}
	return trimmed
}

func missingInvestmentAdditionalFileIDs(expected []string, files []cashfiles.FileRecord) []string {
	found := make(map[string]struct{}, len(files))
	for _, file := range files {
		found[file.FileID] = struct{}{}
	}

	missing := make([]string, 0)
	for _, fileID := range expected {
		if _, ok := found[fileID]; !ok {
			missing = append(missing, fileID)
		}
	}
	return missing
}
