package additionalfiles

import (
	"CimplrCorpSaas/api"
	cashfiles "CimplrCorpSaas/api/cash/additionalfiles"
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/api/policyengine/common"
	s3storage "CimplrCorpSaas/api/utils/s3storage"
	"bytes"
	"context"
	"encoding/json"
	"database/sql"
	"fmt"
	"io"
	"mime/multipart"
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

type dynamicAuditEntry struct {
	Action      string
	Reason      string
	Payload     cashfiles.MainUploadAuditPayload
	ExtraValues map[string]interface{}
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
	fdRateNegotiationFilesDefinition = investmentFileDefinition{
		Module:        "fd-rate-negotiation-additional",
		ParentIDField: "rate_request_id",
		TableName:     "investment.fd_rate_negotiation_files",
		ParentColumn:  "rate_request_id",
		ParentTable:   "investment.fd_rate_negotiation",
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
		ParentTable:   constants.QuerryTDSReceipt,
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
		ParentTable:   constants.QuerryReceiptException,
		ParentFilter:  constants.FormatDeletedFilter,
	}
	varianceExceptionFilesDefinition = investmentFileDefinition{
		Module:        "investment-variance-exception-additional",
		ParentIDField: "exception_id",
		TableName:     "investment.fd_receipt_exception_files",
		ParentColumn:  "exception_id",
		ParentTable:   constants.QuerryReceiptException,
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

// cashfiles.AuditExecutor only exposes Exec.
type investmentAuditQuerier interface {
	Query(context.Context, string, ...interface{}) (pgx.Rows, error)
	QueryRow(context.Context, string, ...interface{}) pgx.Row
}

func loadInvestmentAuditColumns(ctx context.Context, q investmentAuditQuerier, tableName string) (map[string]investmentAuditColumn, error) {
	schemaName, relationName := splitInvestmentAuditTableName(tableName)
	rows, err := q.Query(ctx, `
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
	return recordDynamicInvestmentMainAudit(ctx, tx, tableName, parentColumns, parentID, dynamicAuditEntry{
		Action:      "UPLOAD_FILE",
		Reason:      reason,
		Payload:     payload,
		ExtraValues: extraValues,
	})
}

func recordDynamicInvestmentMainDownloadAudit(ctx context.Context, exec cashfiles.AuditExecutor, tableName string, parentColumns []string, parentID string, payload cashfiles.MainUploadAuditPayload, extraValues map[string]interface{}) error {
	reason, err := cashfiles.MainDownloadAuditReasonJSON(payload)
	if err != nil {
		return err
	}
	actionType := "DOWNLOAD"
	if payload.IsPreview {
		actionType = "PREVIEW"
	}
	return recordDynamicInvestmentMainAudit(ctx, exec, tableName, parentColumns, parentID, dynamicAuditEntry{
		Action:      actionType,
		Reason:      reason,
		Payload:     payload,
		ExtraValues: extraValues,
	})
}

func recordDynamicInvestmentMainAudit(ctx context.Context, exec cashfiles.AuditExecutor, tableName string, parentColumns []string, parentID string, entry dynamicAuditEntry) error {
	// The shared executor only exposes Exec; the schema introspection below needs
	// Query. Both pgx.Tx (upload) and *pgxpool.Pool (download) provide it.
	querier, ok := exec.(investmentAuditQuerier)
	if !ok {
		return nil
	}

	columns, err := loadInvestmentAuditColumns(ctx, querier, tableName)
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
		values[actionColumn] = entry.Action
	}
	if _, ok := columns["processing_status"]; ok {
		values["processing_status"] = constants.StatusApproved
	}
	if reasonColumn := firstInvestmentAuditColumn(columns, "reason", "action_reason"); reasonColumn != "" {
		values[reasonColumn] = entry.Reason
	}
	if requestedByColumn := firstInvestmentAuditColumn(columns, "requested_by", "performed_by", "uploaded_by"); requestedByColumn != "" {
		values[requestedByColumn] = entry.Payload.UploadedBy
	}
	if _, ok := columns["requested_ip"]; ok {
		values["requested_ip"] = api.SystemIfBlank(entry.Payload.RequestedIP)
	}
	if _, ok := columns["performed_by_email"]; ok {
		values["performed_by_email"] = entry.Payload.UploadedBy
	}
	if requestedAtColumn := firstInvestmentAuditColumn(columns, "requested_at", "created_at", "uploaded_at"); requestedAtColumn != "" {
		values[requestedAtColumn] = entry.Payload.UploadedAt
	}
	for key, value := range entry.ExtraValues {
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
		"requested_ip",
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
		constants.ErrInsertFailed,
		tableName,
		strings.Join(insertColumns, ", "),
		strings.Join(placeholders, ", "),
	)
	_, err = exec.Exec(ctx, query, args...)
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

func recordFDRateNegotiationMainUploadAudit(ctx context.Context, tx pgx.Tx, rateRequestID string, payload cashfiles.MainUploadAuditPayload) error {
	return recordDynamicInvestmentMainUploadAudit(ctx, tx, "investment.fd_audit_rate_negotiation", []string{"rate_request_id"}, rateRequestID, payload, nil)
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
			requested_ip,
			created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		closureRequestID,
		"UPLOAD_FILE",
		"COMPLETED",
		reason,
		payload.UploadedBy,
		payload.UploadedBy,
		api.SystemIfBlank(payload.RequestedIP),
		payload.UploadedAt,
	)
	return err
}

func recordFDCashflowMainUploadAudit(ctx context.Context, tx pgx.Tx, fdID string, payload cashfiles.MainUploadAuditPayload) error {
	return recordDynamicInvestmentMainUploadAudit(ctx, tx, constants.QuerryAuditCashflowSchedule, []string{"fd_id", "master_id"}, fdID, payload, nil)
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
			requested_at,
			requested_ip
		) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		receiptID,
		"EDIT",
		constants.StatusApproved,
		reason,
		payload.UploadedBy,
		payload.UploadedAt,
		api.SystemIfBlank(payload.RequestedIP),
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
			requested_at,
			requested_ip
		) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		tdsID,
		"UPLOAD_FILE",
		constants.StatusApproved,
		reason,
		payload.UploadedBy,
		payload.UploadedAt,
		api.SystemIfBlank(payload.RequestedIP),
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
			requested_at,
			requested_ip
		) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		exceptionID,
		"UPLOAD_FILE",
		"APPROVED",
		reason,
		payload.UploadedBy,
		payload.UploadedAt,
		api.SystemIfBlank(payload.RequestedIP),
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

// Download-audit counterparts: same tables and parent columns as the upload
// helpers above, but record a DOWNLOAD action. They run on a cashfiles.AuditExecutor
// (the pool) so they can be invoked from the package ZIP / individual download paths.

func recordInvestmentProposalMainDownloadAudit(ctx context.Context, exec cashfiles.AuditExecutor, proposalID string, payload cashfiles.MainUploadAuditPayload) error {
	return cashfiles.InsertMainDownloadAudit(ctx, exec, "investment.auditactionproposal", "proposal_id", "actiontype", proposalID, payload)
}

func recordInvestmentConfirmationMainDownloadAudit(ctx context.Context, exec cashfiles.AuditExecutor, confirmationID string, payload cashfiles.MainUploadAuditPayload) error {
	return cashfiles.InsertMainDownloadAudit(ctx, exec, "investment.auditactioninvestmentconfirmation", "confirmation_id", "actiontype", confirmationID, payload)
}

func recordInvestmentInitiationMainDownloadAudit(ctx context.Context, exec cashfiles.AuditExecutor, initiationID string, payload cashfiles.MainUploadAuditPayload) error {
	return cashfiles.InsertMainDownloadAudit(ctx, exec, "investment.auditactioninitiation", "initiation_id", "actiontype", initiationID, payload)
}

func recordRedemptionInitiationMainDownloadAudit(ctx context.Context, exec cashfiles.AuditExecutor, redemptionID string, payload cashfiles.MainUploadAuditPayload) error {
	return cashfiles.InsertMainDownloadAudit(ctx, exec, "investment.auditactionredemption", "redemption_id", "actiontype", redemptionID, payload)
}

func recordRedemptionConfirmationMainDownloadAudit(ctx context.Context, exec cashfiles.AuditExecutor, redemptionConfirmID string, payload cashfiles.MainUploadAuditPayload) error {
	return cashfiles.InsertMainDownloadAudit(ctx, exec, "investment.auditactionredemptionconfirmation", "redemption_confirm_id", "actiontype", redemptionConfirmID, payload)
}

func recordAccountingActivityMainDownloadAudit(ctx context.Context, exec cashfiles.AuditExecutor, activityID string, payload cashfiles.MainUploadAuditPayload) error {
	return cashfiles.InsertMainDownloadAudit(ctx, exec, "investment.auditactionaccountingactivity", "activity_id", "actiontype", activityID, payload)
}

func recordFDBookingMainDownloadAudit(ctx context.Context, exec cashfiles.AuditExecutor, bookingID string, payload cashfiles.MainUploadAuditPayload) error {
	return recordDynamicInvestmentMainDownloadAudit(ctx, exec, "investment.fd_audit_booking_request", []string{"booking_id"}, bookingID, payload, nil)
}

func recordFDRateNegotiationMainDownloadAudit(ctx context.Context, exec cashfiles.AuditExecutor, rateRequestID string, payload cashfiles.MainUploadAuditPayload) error {
	return recordDynamicInvestmentMainDownloadAudit(ctx, exec, "investment.fd_audit_rate_negotiation", []string{"rate_request_id"}, rateRequestID, payload, nil)
}

func recordFDConfirmationMainDownloadAudit(ctx context.Context, exec cashfiles.AuditExecutor, confirmationID string, payload cashfiles.MainUploadAuditPayload) error {
	return recordDynamicInvestmentMainDownloadAudit(ctx, exec, "investment.fd_audit_confirmation", []string{"confirmation_id"}, confirmationID, payload, nil)
}

func recordFDMasterMainDownloadAudit(ctx context.Context, exec cashfiles.AuditExecutor, fdID string, payload cashfiles.MainUploadAuditPayload) error {
	return recordDynamicInvestmentMainDownloadAudit(ctx, exec, "investment.fd_audit_master", []string{"fd_id", "master_id", "confirmation_id"}, fdID, payload, nil)
}

func recordFDClosureMainDownloadAudit(ctx context.Context, exec cashfiles.AuditExecutor, closureRequestID string, payload cashfiles.MainUploadAuditPayload) error {
	reason, err := cashfiles.MainDownloadAuditReasonJSON(payload)
	if err != nil {
		return err
	}

	_, err = exec.Exec(ctx, `
		INSERT INTO investment.fd_audit_closure_request (
			closure_request_id,
			action_type,
			processing_status,
			action_reason,
			performed_by,
			performed_by_email,
			requested_ip,
			created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		closureRequestID,
		"DOWNLOAD",
		"COMPLETED",
		reason,
		payload.UploadedBy,
		payload.UploadedBy,
		api.SystemIfBlank(payload.RequestedIP),
		payload.UploadedAt,
	)
	return err
}

func recordFDCashflowMainDownloadAudit(ctx context.Context, exec cashfiles.AuditExecutor, fdID string, payload cashfiles.MainUploadAuditPayload) error {
	return recordDynamicInvestmentMainDownloadAudit(ctx, exec, constants.QuerryAuditCashflowSchedule, []string{"fd_id", "master_id"}, fdID, payload, nil)
}

func recordFDInterestReceiptMainDownloadAudit(ctx context.Context, exec cashfiles.AuditExecutor, receiptID string, payload cashfiles.MainUploadAuditPayload) error {
	reason, err := cashfiles.MainDownloadAuditReasonJSON(payload)
	if err != nil {
		return err
	}

	_, err = exec.Exec(ctx, `
		INSERT INTO investment.fd_interest_receipt_audit (
			receipt_id,
			action_type,
			processing_status,
			reason,
			requested_by,
			requested_at,
			requested_ip
		) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		receiptID,
		"DOWNLOAD",
		constants.StatusApproved,
		reason,
		payload.UploadedBy,
		payload.UploadedAt,
		api.SystemIfBlank(payload.RequestedIP),
	)
	return err
}

func recordFDTDSReceiptMainDownloadAudit(ctx context.Context, exec cashfiles.AuditExecutor, tdsID string, payload cashfiles.MainUploadAuditPayload) error {
	reason, err := cashfiles.MainDownloadAuditReasonJSON(payload)
	if err != nil {
		return err
	}

	_, err = exec.Exec(ctx, `
		INSERT INTO investment.fd_tds_receipt_audit (
			tds_id,
			action_type,
			processing_status,
			reason,
			requested_by,
			requested_at,
			requested_ip
		) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		tdsID,
		"DOWNLOAD",
		constants.StatusApproved,
		reason,
		payload.UploadedBy,
		payload.UploadedAt,
		api.SystemIfBlank(payload.RequestedIP),
	)
	return err
}

func recordFDReceiptExceptionMainDownloadAudit(ctx context.Context, exec cashfiles.AuditExecutor, exceptionID string, payload cashfiles.MainUploadAuditPayload) error {
	reason, err := cashfiles.MainDownloadAuditReasonJSON(payload)
	if err != nil {
		return err
	}

	_, err = exec.Exec(ctx, `
		INSERT INTO investment.fd_receipt_exception_audit (
			exception_id,
			action_type,
			processing_status,
			reason,
			requested_by,
			requested_at,
			requested_ip
		) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		exceptionID,
		"DOWNLOAD",
		"APPROVED",
		reason,
		payload.UploadedBy,
		payload.UploadedAt,
		api.SystemIfBlank(payload.RequestedIP),
	)
	return err
}

func recordFDAccrualRunMainDownloadAudit(ctx context.Context, exec cashfiles.AuditExecutor, runID string, payload cashfiles.MainUploadAuditPayload) error {
	return recordDynamicInvestmentMainDownloadAudit(ctx, exec, "investment.fd_accrual_run_audit", []string{"run_id"}, runID, payload, nil)
}

func recordFDAccrualScheduleConfigMainDownloadAudit(ctx context.Context, exec cashfiles.AuditExecutor, configID string, payload cashfiles.MainUploadAuditPayload) error {
	return recordDynamicInvestmentMainDownloadAudit(ctx, exec, "investment.fd_accrual_schedule_config_audit", []string{"config_id"}, configID, payload, nil)
}

func recordFDAccrualLedgerMainDownloadAudit(ctx context.Context, exec cashfiles.AuditExecutor, ledgerID string, payload cashfiles.MainUploadAuditPayload) error {
	extra := map[string]interface{}{}
	if querier, ok := exec.(investmentAuditQuerier); ok {
		var runID, fdID string
		if err := querier.QueryRow(ctx, `
			SELECT COALESCE(run_id, ''), COALESCE(fd_id, '')
			FROM investment.fd_accrual_ledger
			WHERE ledger_id = $1
		`, ledgerID).Scan(&runID, &fdID); err != nil {
			return err
		}
		extra["run_id"] = runID
		extra["fd_id"] = fdID
	}

	return recordDynamicInvestmentMainDownloadAudit(ctx, exec, "investment.fd_accrual_ledger_audit", []string{"ledger_id"}, ledgerID, payload, extra)
}

func recordFDAccountingJournalMainDownloadAudit(ctx context.Context, exec cashfiles.AuditExecutor, entryID string, payload cashfiles.MainUploadAuditPayload) error {
	return recordDynamicInvestmentMainDownloadAudit(ctx, exec, "investment.accounting_journal_entry_audit", []string{"entry_id"}, entryID, payload, nil)
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

func ListFDRateNegotiationAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewListHandler(pool, investmentAdditionalFilesConfig(fdRateNegotiationFilesDefinition))
}

func UploadFDRateNegotiationAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewUploadHandler(pool, investmentAdditionalFilesConfig(fdRateNegotiationFilesDefinition))
}

func DownloadFDRateNegotiationAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDownloadHandler(pool, investmentAdditionalFilesConfig(fdRateNegotiationFilesDefinition))
}

func DownloadSelectedFDRateNegotiationAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDownloadSelectedHandler(pool, investmentAdditionalFilesConfig(fdRateNegotiationFilesDefinition))
}

func DeleteFDRateNegotiationAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDeleteHandler(pool, investmentAdditionalFilesConfig(fdRateNegotiationFilesDefinition))
}

func AuditFDRateNegotiationAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewAuditHandler(pool, investmentAdditionalFilesConfig(fdRateNegotiationFilesDefinition))
}

func ApproveDeleteFDRateNegotiationAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewApproveDeleteHandler(pool, investmentAdditionalFilesConfig(fdRateNegotiationFilesDefinition))
}

func RejectDeleteFDRateNegotiationAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewRejectDeleteHandler(pool, investmentAdditionalFilesConfig(fdRateNegotiationFilesDefinition))
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
	legacyHandler := cashfiles.NewListHandler(pool, investmentAdditionalFilesConfig(fdClosureFilesDefinition))
	return func(w http.ResponseWriter, r *http.Request) {
		body, parentID, err := fdClosureAdditionalJSONParent(r, fdClosureFilesDefinition.ParentIDField)
		if err != nil {
			api.RespondWithError(w, http.StatusBadRequest, err.Error())
			return
		}
		idKind, err := cimplrClosureAdditionalIDKind(r.Context(), pool, parentID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrParentLookupFailed+err.Error())
			return
		}
		if idKind == "" {
			r.Body = io.NopCloser(bytes.NewReader(body))
			legacyHandler(w, r)
			return
		}
		files, err := listCimplrClosureAdditionalFiles(r.Context(), pool, parentID, idKind)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeInvestmentAdditionalSuccess(w, map[string]interface{}{"files": files})
	}
}

func UploadFDClosureAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	legacyHandler := cashfiles.NewUploadHandler(pool, investmentAdditionalFilesConfig(fdClosureFilesDefinition))
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}
		if err := r.ParseMultipartForm(64 << 20); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}

		parentID := strings.TrimSpace(r.FormValue(fdClosureFilesDefinition.ParentIDField))
		if parentID == "" {
			api.RespondWithError(w, http.StatusBadRequest, fmt.Sprintf("%s required", fdClosureFilesDefinition.ParentIDField))
			return
		}

		idKind, err := cimplrClosureAdditionalIDKind(r.Context(), pool, parentID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrParentLookupFailed+err.Error())
			return
		}
		if idKind == "" {
			legacyHandler(w, r)
			return
		}

		uploaded, err := uploadCimplrClosureAdditionalFiles(r.Context(), pool, r, parentID, idKind)
		if err != nil {
			api.RespondWithError(w, http.StatusBadRequest, err.Error())
			return
		}
		api.RespondWithPayload(w, true, "", map[string]interface{}{"files": uploaded})
	}
}

func cimplrClosureAdditionalIDKind(ctx context.Context, pool *pgxpool.Pool, parentID string) (string, error) {
	var idKind string
	err := pool.QueryRow(ctx, `
		SELECT kind
		FROM (
			SELECT 'initiate' AS kind
			FROM cimplr.fd_closure_initiate
			WHERE closure_initiate_id=$1
			  AND COALESCE(is_deleted,false)=false
			UNION ALL
			SELECT 'confirm' AS kind
			FROM cimplr.fd_closure_confirm
			WHERE closure_confirm_id=$1
			  AND COALESCE(is_deleted,false)=false
		) s
		LIMIT 1`, parentID).Scan(&idKind)
	if err == pgx.ErrNoRows {
		return "", nil
	}
	return strings.TrimSpace(idKind), err
}

func uploadCimplrClosureAdditionalFiles(ctx context.Context, pool *pgxpool.Pool, r *http.Request, parentID, idKind string) ([]cashfiles.FileRecord, error) {
	fileHeaders := append([]*multipart.FileHeader{}, r.MultipartForm.File["file"]...)
	fileHeaders = append(fileHeaders, r.MultipartForm.File["files"]...)
	if len(fileHeaders) == 0 {
		return nil, fmt.Errorf("no files provided")
	}

	userID := strings.TrimSpace(r.FormValue("user_id"))
	uploadedBy := strings.TrimSpace(api.RequestedByFromCtx(r.Context(), userID))
	if uploadedBy == "" {
		uploadedBy = userID
	}

	uploaded := make([]cashfiles.FileRecord, 0, len(fileHeaders))
	for _, header := range fileHeaders {
		file, err := header.Open()
		if err != nil {
			return nil, fmt.Errorf("open file %s: %w", header.Filename, err)
		}
		body, readErr := io.ReadAll(file)
		_ = file.Close()
		if readErr != nil {
			return nil, fmt.Errorf("read file %s: %w", header.Filename, readErr)
		}
		if len(body) == 0 {
			return nil, fmt.Errorf("uploaded file is empty")
		}

		uploadedAt := time.Now().UTC()
		storedFileName := s3storage.BuildUploadedFilename(header.Filename, uploadedBy, uploadedAt)
		s3Key := s3storage.BuildNamedS3Key(s3storage.GetStoragePrefix(fdClosureFilesDefinition.Module), parentID, storedFileName)
		contentType := header.Header.Get(constants.ContentTypeText)
		if contentType == "" {
			contentType = s3storage.DetectContentType(body)
		}
		fileHash := s3storage.ContentHashHex(body)

		if err := s3storage.PutObjectToS3(ctx, s3Key, body, contentType); err != nil {
			return nil, fmt.Errorf("failed to upload file to S3: %w", err)
		}

		tx, err := pool.Begin(ctx)
		if err != nil {
			_ = s3storage.DeleteFromS3(ctx, s3Key)
			return nil, fmt.Errorf(constants.ErrFailedToUploadFileMetadata, err)
		}

		var fileID string
		var insertErr error
		if idKind == "initiate" {
			insertErr = tx.QueryRow(ctx, `
				INSERT INTO cimplr.fd_closure_files (
					closure_initiate_id, closure_confirm_id, file_type, stored_file_name, original_file_name,
					content_type, file_size, file_hash, upload_s3_key, uploaded_by
				) VALUES ($1, NULL, 'SUPPORTING', $2, $3, $4, $5, $6, $7, $8)
				RETURNING file_id::text`,
				parentID, storedFileName, header.Filename, contentType, int64(len(body)), fileHash, s3Key, uploadedBy,
			).Scan(&fileID)
		} else {
			insertErr = tx.QueryRow(ctx, `
				INSERT INTO cimplr.fd_closure_files (
					closure_initiate_id, closure_confirm_id, file_type, stored_file_name, original_file_name,
					content_type, file_size, file_hash, upload_s3_key, uploaded_by
				) VALUES (NULL, $1, 'SUPPORTING', $2, $3, $4, $5, $6, $7, $8)
				RETURNING file_id::text`,
				parentID, storedFileName, header.Filename, contentType, int64(len(body)), fileHash, s3Key, uploadedBy,
			).Scan(&fileID)
		}
		if insertErr != nil {
			_ = tx.Rollback(ctx)
			_ = s3storage.DeleteFromS3(ctx, s3Key)
			return nil, fmt.Errorf(constants.ErrFailedToUploadFileMetadata, insertErr)
		}

		initiateID, confirmID := "", ""
		if idKind == "initiate" {
			initiateID = parentID
		} else {
			confirmID = parentID
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO cimplr.fd_closure_files_audit (
				file_id, closure_initiate_id, closure_confirm_id, action_type, processing_status,
				reason, requested_by, requested_at, requested_ip
			) VALUES ($1::uuid, NULLIF($2,''), NULLIF($3,''), 'CREATE', 'APPROVED', $4, $5, $6, $7)`,
			fileID,
			initiateID,
			confirmID,
			fmt.Sprintf("Uploaded file: %s", storedFileName),
			api.SystemIfBlank(uploadedBy),
			uploadedAt,
			api.SystemIfBlank(api.ClientIPFromRequest(r)),
		); err != nil {
			_ = tx.Rollback(ctx)
			_ = s3storage.DeleteFromS3(ctx, s3Key)
			return nil, fmt.Errorf("file upload audit failed: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			_ = s3storage.DeleteFromS3(ctx, s3Key)
			return nil, fmt.Errorf(constants.ErrFailedToUploadFileMetadata, err)
		}

		uploaded = append(uploaded, cashfiles.FileRecord{
			FileID:           strings.TrimSpace(fileID),
			StoredFileName:   storedFileName,
			ContentType:      contentType,
			FileSize:         int64(len(body)),
			UploadS3Key:      s3Key,
			UploadedBy:       uploadedBy,
			UploadedAt:       uploadedAt,
			ProcessingStatus: "ACTIVE",
		})
	}
	return uploaded, nil
}

func fdClosureAdditionalJSONParent(r *http.Request, parentField string) ([]byte, string, error) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, "", fmt.Errorf("failed to read request body: %w", err)
	}
	var payload map[string]interface{}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, "", fmt.Errorf("%s: %w", constants.ErrInvalidJSONShort, err)
	}
	parentID := strings.TrimSpace(fmt.Sprint(payload[parentField]))
	if parentID == "" || parentID == "<nil>" {
		return nil, "", fmt.Errorf("%s required", parentField)
	}
	return body, parentID, nil
}

func listCimplrClosureAdditionalFiles(ctx context.Context, pool *pgxpool.Pool, parentID, idKind string) ([]cashfiles.FileRecord, error) {
	parentColumn := "closure_confirm_id"
	if idKind == "initiate" {
		parentColumn = "closure_initiate_id"
	}
	rows, err := pool.Query(ctx, fmt.Sprintf(`
		SELECT file_id::text, stored_file_name, COALESCE(content_type,''), COALESCE(file_size,0),
		       upload_s3_key, COALESCE(uploaded_by,''), uploaded_at
		FROM cimplr.fd_closure_files
		WHERE %s=$1
		  AND COALESCE(is_deleted,false)=false
		ORDER BY uploaded_at DESC`, parentColumn), parentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	files := []cashfiles.FileRecord{}
	for rows.Next() {
		var file cashfiles.FileRecord
		if err := rows.Scan(
			&file.FileID,
			&file.StoredFileName,
			&file.ContentType,
			&file.FileSize,
			&file.UploadS3Key,
			&file.UploadedBy,
			&file.UploadedAt,
		); err != nil {
			return nil, err
		}
		file.ProcessingStatus = "ACTIVE"
		files = append(files, file)
	}
	return files, rows.Err()
}

func getCimplrClosureAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, idKind, fileID string) (*cashfiles.FileRecord, error) {
	parentColumn := "closure_confirm_id"
	if idKind == "initiate" {
		parentColumn = "closure_initiate_id"
	}
	where := fmt.Sprintf("%s=$1", parentColumn)
	args := []interface{}{parentID}
	if strings.TrimSpace(fileID) != "" {
		where += " AND file_id=$2::uuid"
		args = append(args, strings.TrimSpace(fileID))
	}
	rows, err := pool.Query(ctx, fmt.Sprintf(`
		SELECT file_id::text, stored_file_name, COALESCE(content_type,''), COALESCE(file_size,0),
		       upload_s3_key, COALESCE(uploaded_by,''), uploaded_at
		FROM cimplr.fd_closure_files
		WHERE %s
		  AND COALESCE(is_deleted,false)=false
		ORDER BY uploaded_at DESC
		LIMIT 1`, where), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, rows.Err()
	}
	var file cashfiles.FileRecord
	if err := rows.Scan(
		&file.FileID,
		&file.StoredFileName,
		&file.ContentType,
		&file.FileSize,
		&file.UploadS3Key,
		&file.UploadedBy,
		&file.UploadedAt,
	); err != nil {
		return nil, err
	}
	file.ProcessingStatus = "ACTIVE"
	return &file, rows.Err()
}

func writeInvestmentAdditionalSuccess(w http.ResponseWriter, data interface{}) {
	api.RespondEnvelopeSuccess(w, "Success", data)
}

func DownloadFDClosureAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	legacyHandler := cashfiles.NewDownloadHandler(pool, investmentAdditionalFilesConfig(fdClosureFilesDefinition))
	return func(w http.ResponseWriter, r *http.Request) {
		body, parentID, err := fdClosureAdditionalJSONParent(r, fdClosureFilesDefinition.ParentIDField)
		if err != nil {
			api.RespondWithError(w, http.StatusBadRequest, err.Error())
			return
		}
		idKind, err := cimplrClosureAdditionalIDKind(r.Context(), pool, parentID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrParentLookupFailed+err.Error())
			return
		}
		if idKind == "" {
			r.Body = io.NopCloser(bytes.NewReader(body))
			legacyHandler(w, r)
			return
		}

		var req struct {
			FileID  string `json:"file_id"`
			UserID  string `json:"user_id"`
			Preview bool   `json:"preview"`
		}
		_ = json.Unmarshal(body, &req)
		record, err := getCimplrClosureAdditionalFile(r.Context(), pool, parentID, idKind, req.FileID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if record == nil || strings.TrimSpace(record.UploadS3Key) == "" {
			api.RespondWithError(w, http.StatusNotFound, constants.ErrFileNotFound)
			return
		}
		downloadURL, err := s3storage.GetDownloadPresignedURL(r.Context(), record.UploadS3Key, 15*time.Minute)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to generate download url: "+err.Error())
			return
		}

		actionType := "DOWNLOAD"
		reasonStr := fmt.Sprintf("Downloaded file: %s", record.StoredFileName)
		if req.Preview {
			actionType = "PREVIEW"
			reasonStr = fmt.Sprintf("Previewed file: %s", record.StoredFileName)
		}

		parentColumn := "closure_confirm_id"
		if idKind == "initiate" {
			parentColumn = "closure_initiate_id"
		}

		requestedBy := api.SystemIfBlank(req.UserID)
		_, _ = pool.Exec(r.Context(), fmt.Sprintf(`
			INSERT INTO cimplr.fd_closure_files_audit (file_id, %s, action_type, processing_status, reason, requested_by, requested_at, requested_ip)
			VALUES ($1, $2, $3, 'COMPLETED', $4, $5, NOW(), $6)
		`, parentColumn), record.FileID, parentID, actionType, reasonStr, requestedBy, api.ClientIPFromRequest(r))
		writeInvestmentAdditionalSuccess(w, map[string]interface{}{
			"download_url": downloadURL,
			"file_id":      record.FileID,
		})
	}
}

func DownloadSelectedFDClosureAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDownloadSelectedHandler(pool, investmentAdditionalFilesConfig(fdClosureFilesDefinition))
}

func DeleteFDClosureAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	legacyHandler := cashfiles.NewDeleteHandler(pool, investmentAdditionalFilesConfig(fdClosureFilesDefinition))
	return func(w http.ResponseWriter, r *http.Request) {
		body, parentID, err := fdClosureAdditionalJSONParent(r, fdClosureFilesDefinition.ParentIDField)
		if err != nil {
			api.RespondWithError(w, http.StatusBadRequest, err.Error())
			return
		}
		idKind, err := cimplrClosureAdditionalIDKind(r.Context(), pool, parentID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrParentLookupFailed+err.Error())
			return
		}
		if idKind == "" {
			r.Body = io.NopCloser(bytes.NewReader(body))
			legacyHandler(w, r)
			return
		}

		var req struct {
			FileID string `json:"file_id"`
			UserID string `json:"user_id"`
			Reason string `json:"reason"`
		}
		if err := json.Unmarshal(body, &req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}
		if strings.TrimSpace(req.UserID) == "" || strings.TrimSpace(req.FileID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "user_id and file_id required")
			return
		}

		record, err := getCimplrClosureAdditionalFile(r.Context(), pool, parentID, idKind, req.FileID)
		if err != nil || record == nil {
			api.RespondWithError(w, http.StatusNotFound, constants.ErrFileNotFound)
			return
		}

		tx, err := pool.Begin(r.Context())
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}

		_, err = tx.Exec(r.Context(), "UPDATE cimplr.fd_closure_files SET is_deleted = true, deleted_by = $1, deleted_at = NOW() WHERE file_id = $2::uuid", req.UserID, req.FileID)
		if err != nil {
			tx.Rollback(r.Context())
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}

		parentColumn := "closure_confirm_id"
		if idKind == "initiate" {
			parentColumn = "closure_initiate_id"
		}
		
		_, err = tx.Exec(r.Context(), fmt.Sprintf(`
			INSERT INTO cimplr.fd_closure_files_audit (file_id, %s, action_type, processing_status, reason, requested_by, requested_at, requested_ip)
			VALUES ($1, $2, 'DELETE', 'PENDING_DELETE_APPROVAL', $3, $4, NOW(), $5)
		`, parentColumn), req.FileID, parentID, fmt.Sprintf("Deleted file: %s", record.StoredFileName), req.UserID, api.ClientIPFromRequest(r))
		if err != nil {
			tx.Rollback(r.Context())
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}

		tx.Commit(r.Context())
		writeInvestmentAdditionalSuccess(w, map[string]interface{}{"message": "File deleted successfully."})
	}
}

type investmentFileAuditEvent struct {
	AuditID          string     `json:"audit_id"`
	ModuleKey        string     `json:"module_key"`
	ParentRecordID   string     `json:"parent_record_id"`
	FileID           string     `json:"file_id"`
	EntityID         string     `json:"entity_id"`
	ActionType       string     `json:"action_type"`
	RequestedBy      string     `json:"requested_by"`
	RequestedAt      *time.Time `json:"requested_at"`
	RequestedIP      string     `json:"requested_ip"`
	CheckerBy        string     `json:"checker_by"`
	CheckerAt        *time.Time `json:"checker_at"`
	CheckerIP        string     `json:"checker_ip"`
	CheckerComment   string     `json:"checker_comment"`
	ProcessingStatus string     `json:"processing_status"`
	Reason           string     `json:"reason"`
}

func listCimplrClosureFileAuditEvents(ctx context.Context, pool *pgxpool.Pool, parentID, idKind, fileID string) ([]investmentFileAuditEvent, error) {
	parentColumn := "closure_confirm_id"
	if idKind == "initiate" {
		parentColumn = "closure_initiate_id"
	}
	query := fmt.Sprintf(`
		SELECT audit_id, %s, file_id::text, action_type, processing_status, COALESCE(reason,''), COALESCE(requested_by,''), requested_at, COALESCE(requested_ip,''), COALESCE(checker_by,''), checker_at, COALESCE(checker_ip,''), COALESCE(checker_comment,'')
		FROM cimplr.fd_closure_files_audit
		WHERE %s = $1 AND file_id = $2::uuid
		ORDER BY requested_at ASC, audit_id ASC
	`, parentColumn, parentColumn)

	rows, err := pool.Query(ctx, query, parentID, fileID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []investmentFileAuditEvent
	for rows.Next() {
		var event investmentFileAuditEvent
		var actionID string
		var parentRecordID sql.NullString
		var requestedBy sql.NullString
		var requestedAt sql.NullTime
		var requestedIP sql.NullString
		var checkerBy sql.NullString
		var checkerAt sql.NullTime
		var checkerIP sql.NullString
		var checkerComment sql.NullString
		var reason sql.NullString
		if err := rows.Scan(
			&actionID,
			&parentRecordID,
			&event.FileID,
			&event.ActionType,
			&event.ProcessingStatus,
			&reason,
			&requestedBy,
			&requestedAt,
			&requestedIP,
			&checkerBy,
			&checkerAt,
			&checkerIP,
			&checkerComment,
		); err != nil {
			return nil, err
		}

		event.EntityID = event.FileID
		event.ModuleKey = fdClosureFilesDefinition.Module
		event.ParentRecordID = strings.TrimSpace(parentRecordID.String)
		event.RequestedBy = strings.TrimSpace(requestedBy.String)
		event.RequestedIP = strings.TrimSpace(requestedIP.String)
		if requestedAt.Valid {
			t := requestedAt.Time
			event.RequestedAt = &t
		}
		event.CheckerBy = strings.TrimSpace(checkerBy.String)
		event.CheckerIP = strings.TrimSpace(checkerIP.String)
		if checkerAt.Valid {
			t := checkerAt.Time
			event.CheckerAt = &t
		}
		event.CheckerComment = strings.TrimSpace(checkerComment.String)
		event.Reason = strings.TrimSpace(reason.String)
		event.AuditID = actionID

		events = append(events, event)
	}
	return events, rows.Err()
}

func AuditFDClosureAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	legacyHandler := cashfiles.NewAuditHandler(pool, investmentAdditionalFilesConfig(fdClosureFilesDefinition))
	return func(w http.ResponseWriter, r *http.Request) {
		body, parentID, err := fdClosureAdditionalJSONParent(r, fdClosureFilesDefinition.ParentIDField)
		if err != nil {
			api.RespondWithError(w, http.StatusBadRequest, err.Error())
			return
		}
		idKind, err := cimplrClosureAdditionalIDKind(r.Context(), pool, parentID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrParentLookupFailed+err.Error())
			return
		}
		if idKind == "" {
			r.Body = io.NopCloser(bytes.NewReader(body))
			legacyHandler(w, r)
			return
		}

		var req struct {
			FileID string `json:"file_id"`
			UserID string `json:"user_id"`
		}
		if err := json.Unmarshal(body, &req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}
		if strings.TrimSpace(req.FileID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrFileIDRequired)
			return
		}

		record, err := getCimplrClosureAdditionalFile(r.Context(), pool, parentID, idKind, req.FileID)
		if err != nil || record == nil {
			api.RespondWithError(w, http.StatusNotFound, constants.ErrFileNotFound)
			return
		}

		events, err := listCimplrClosureFileAuditEvents(r.Context(), pool, parentID, idKind, req.FileID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}

		if len(events) == 0 {
			uploadedAt := record.UploadedAt
			events = append(events, investmentFileAuditEvent{
				EntityID:         record.FileID,
				ModuleKey:        fdClosureFilesDefinition.Module,
				ParentRecordID:   parentID,
				FileID:           record.FileID,
				ActionType:       "CREATE",
				ProcessingStatus: "COMPLETED",
				RequestedBy:      record.UploadedBy,
				RequestedAt:      &uploadedAt,
			})
		}
		writeInvestmentAdditionalSuccess(w, events)
	}
}

func ApproveDeleteFDClosureAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	legacyHandler := cashfiles.NewApproveDeleteHandler(pool, investmentAdditionalFilesConfig(fdClosureFilesDefinition))
	return func(w http.ResponseWriter, r *http.Request) {
		body, parentID, err := fdClosureAdditionalJSONParent(r, fdClosureFilesDefinition.ParentIDField)
		if err == nil {
			idKind, _ := cimplrClosureAdditionalIDKind(r.Context(), pool, parentID)
			if idKind != "" {
				api.RespondWithError(w, http.StatusBadRequest, "Approval workflow not applicable for this file type.")
				return
			}
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		legacyHandler(w, r)
	}
}

func RejectDeleteFDClosureAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	legacyHandler := cashfiles.NewRejectDeleteHandler(pool, investmentAdditionalFilesConfig(fdClosureFilesDefinition))
	return func(w http.ResponseWriter, r *http.Request) {
		body, parentID, err := fdClosureAdditionalJSONParent(r, fdClosureFilesDefinition.ParentIDField)
		if err == nil {
			idKind, _ := cimplrClosureAdditionalIDKind(r.Context(), pool, parentID)
			if idKind != "" {
				api.RespondWithError(w, http.StatusBadRequest, "Approval workflow not applicable for this file type.")
				return
			}
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		legacyHandler(w, r)
	}
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

func investmentFilesPolicyScope(module string) (moduleCode, subModule, apiPath string) {
	switch strings.TrimSpace(module) {
	case onboardingFilesDefinition.Module:
		return common.ModuleInvestmentMF, "MF_ONBOARD", "/investment/onboard/additional-files/upload"
	case proposalFilesDefinition.Module:
		return common.ModuleInvestmentMF, "MF_PROPOSAL", "/investment/proposal/additional-files/upload"
	case initiationFilesDefinition.Module:
		return common.ModuleInvestmentMF, "MF_INITIATION", "/investment/initiation/additional-files/upload"
	case confirmationFilesDefinition.Module:
		return common.ModuleInvestmentMF, "MF_CONFIRMATION", "/investment/confirmation/additional-files/upload"
	case redemptionInitiationFilesDefinition.Module:
		return common.ModuleInvestmentMF, "MF_REDEMPTION", "/investment/redemption/additional-files/upload"
	case redemptionConfirmationFilesDefinition.Module:
		return common.ModuleInvestmentMF, "MF_REDEMPTION_CONF", "/investment/redemption-confirmation/additional-files/upload"
	case accountingActivityFilesDefinition.Module:
		return common.ModuleInvestmentMF, "MF_ACCOUNTING", "/investment/accounting/additional-files/upload"
	case fdBookingFilesDefinition.Module:
		return common.ModuleInvestmentFD, "FD_BOOKING", "/investment/fd/booking/additional-files/upload"
	case fdRateNegotiationFilesDefinition.Module:
		return common.ModuleInvestmentFD, "FD_RATE_NEGOTIATION", "/investment/fd/rate-negotiation/additional-files/upload"
	case fdConfirmationFilesDefinition.Module:
		return common.ModuleInvestmentFD, "FD_CONFIRMATION", "/investment/fd/confirmation/additional-files/upload"
	case fdMasterFilesDefinition.Module:
		return common.ModuleInvestmentFD, "FD_MASTER", "/investment/fd/master/additional-files/upload"
	case fdClosureFilesDefinition.Module:
		return common.ModuleInvestmentFD, "FD_CLOSURE", "/investment/fd/closure/additional-files/upload"
	case fdRolloverFilesDefinition.Module:
		return common.ModuleInvestmentFD, "FD_CLOSURE", "/investment/fd/rollover/additional-files/upload"
	case fdCashflowFilesDefinition.Module:
		return common.ModuleInvestmentFD, "FD_MASTER", "/investment/fd/cashflow/additional-files/upload"
	case fdInterestReceiptFilesDefinition.Module:
		return common.ModuleInvestmentFD, "FD_RECEIPT", "/investment/fd/receipt/additional-files/upload"
	case fdTDSReceiptFilesDefinition.Module:
		return common.ModuleInvestmentFD, "FD_TDS_REGISTER", "/investment/fd/tds/additional-files/upload"
	case fdReconcileResultFilesDefinition.Module, fdReceiptExceptionFilesDefinition.Module, varianceExceptionFilesDefinition.Module:
		return common.ModuleInvestmentFD, "FD_RECEIPT", "/investment/fd/receipt-exception/additional-files/upload"
	case fdAccrualRunFilesDefinition.Module, fdAccrualLedgerFilesDefinition.Module, fdAccountingJournalFilesDefinition.Module:
		return common.ModuleInvestmentFD, "FD_ACCRUAL", "/investment/fd/accrual/additional-files/upload"
	case fdAccrualScheduleConfigFilesDefinition.Module:
		return common.ModuleInvestmentFD, "FD_ACCRUAL_SCHED", "/investment/fd/accrual-schedule/additional-files/upload"
	default:
		return "", "", ""
	}
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
		SoftDelete: func(ctx context.Context, pool *pgxpool.Pool, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
			return deleteInvestmentAdditionalFile(ctx, pool, def, parentID, fileID, deletedBy, deletedAt)
		},
		SoftDeleteTx: func(ctx context.Context, tx pgx.Tx, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
			return deleteInvestmentAdditionalFile(ctx, tx, def, parentID, fileID, deletedBy, deletedAt)
		},
	}
	cfg.PolicyModuleCode, cfg.PolicySubModule, cfg.PolicyAPIPath = investmentFilesPolicyScope(def.Module)

	switch def.Module {
	case proposalFilesDefinition.Module:
		cfg.RecordMainUploadAudit = recordInvestmentProposalMainUploadAudit
		cfg.RecordMainDownloadAudit = recordInvestmentProposalMainDownloadAudit
	case initiationFilesDefinition.Module:
		cfg.RecordMainUploadAudit = recordInvestmentInitiationMainUploadAudit
		cfg.RecordMainDownloadAudit = recordInvestmentInitiationMainDownloadAudit
	case confirmationFilesDefinition.Module:
		cfg.RecordMainUploadAudit = recordInvestmentConfirmationMainUploadAudit
		cfg.RecordMainDownloadAudit = recordInvestmentConfirmationMainDownloadAudit
	case redemptionInitiationFilesDefinition.Module:
		cfg.RecordMainUploadAudit = recordRedemptionInitiationMainUploadAudit
		cfg.RecordMainDownloadAudit = recordRedemptionInitiationMainDownloadAudit
	case redemptionConfirmationFilesDefinition.Module:
		cfg.RecordMainUploadAudit = recordRedemptionConfirmationMainUploadAudit
		cfg.RecordMainDownloadAudit = recordRedemptionConfirmationMainDownloadAudit
	case accountingActivityFilesDefinition.Module:
		cfg.RecordMainUploadAudit = recordAccountingActivityMainUploadAudit
		cfg.RecordMainDownloadAudit = recordAccountingActivityMainDownloadAudit
	case fdBookingFilesDefinition.Module:
		cfg.RecordMainUploadAudit = recordFDBookingMainUploadAudit
		cfg.RecordMainDownloadAudit = recordFDBookingMainDownloadAudit
	case fdRateNegotiationFilesDefinition.Module:
		cfg.RecordMainUploadAudit = recordFDRateNegotiationMainUploadAudit
		cfg.RecordMainDownloadAudit = recordFDRateNegotiationMainDownloadAudit
	case fdConfirmationFilesDefinition.Module:
		cfg.RecordMainUploadAudit = recordFDConfirmationMainUploadAudit
		cfg.RecordMainDownloadAudit = recordFDConfirmationMainDownloadAudit
	case fdMasterFilesDefinition.Module:
		cfg.RecordMainUploadAudit = recordFDMasterMainUploadAudit
		cfg.RecordMainDownloadAudit = recordFDMasterMainDownloadAudit
	case fdClosureFilesDefinition.Module, fdRolloverFilesDefinition.Module:
		cfg.RecordMainUploadAudit = recordFDClosureMainUploadAudit
		cfg.RecordMainDownloadAudit = recordFDClosureMainDownloadAudit
	case fdCashflowFilesDefinition.Module:
		cfg.RecordMainUploadAudit = recordFDCashflowMainUploadAudit
		cfg.RecordMainDownloadAudit = recordFDCashflowMainDownloadAudit
	case fdInterestReceiptFilesDefinition.Module:
		cfg.RecordMainUploadAudit = recordFDInterestReceiptMainUploadAudit
		cfg.RecordMainDownloadAudit = recordFDInterestReceiptMainDownloadAudit
	case fdTDSReceiptFilesDefinition.Module:
		cfg.RecordMainUploadAudit = recordFDTDSReceiptMainUploadAudit
		cfg.RecordMainDownloadAudit = recordFDTDSReceiptMainDownloadAudit
	case fdReceiptExceptionFilesDefinition.Module:
		cfg.RecordMainUploadAudit = recordFDReceiptExceptionMainUploadAudit
		cfg.RecordMainDownloadAudit = recordFDReceiptExceptionMainDownloadAudit
	case varianceExceptionFilesDefinition.Module:
		cfg.RecordMainUploadAudit = recordFDReceiptExceptionMainUploadAudit
		cfg.RecordMainDownloadAudit = recordFDReceiptExceptionMainDownloadAudit
		cfg.RequireMainUploadAudit = true
	case fdAccrualRunFilesDefinition.Module:
		cfg.RecordMainUploadAudit = recordFDAccrualRunMainUploadAudit
		cfg.RecordMainDownloadAudit = recordFDAccrualRunMainDownloadAudit
	case fdAccrualScheduleConfigFilesDefinition.Module:
		cfg.RecordMainUploadAudit = recordFDAccrualScheduleConfigMainUploadAudit
		cfg.RecordMainDownloadAudit = recordFDAccrualScheduleConfigMainDownloadAudit
	case fdAccrualLedgerFilesDefinition.Module:
		cfg.RecordMainUploadAudit = recordFDAccrualLedgerMainUploadAudit
		cfg.RecordMainDownloadAudit = recordFDAccrualLedgerMainDownloadAudit
	case fdAccountingJournalFilesDefinition.Module:
		cfg.RecordMainUploadAudit = recordFDAccountingJournalMainUploadAudit
		cfg.RecordMainDownloadAudit = recordFDAccountingJournalMainDownloadAudit
	}

	if isFDCrossStageModule(def.Module) {
		module := def.Module
		cfg.AuditByFileIDOnly = true
		cfg.List = func(ctx context.Context, pool *pgxpool.Pool, parentID string) ([]cashfiles.FileRecord, error) {
			return listFDCrossStageFiles(ctx, pool, module, parentID)
		}
		cfg.GetOne = func(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*cashfiles.FileRecord, error) {
			return getFDCrossStageFile(ctx, pool, module, parentID, fileID, false)
		}
		cfg.GetAnyFile = func(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*cashfiles.FileRecord, error) {
			return getFDCrossStageFile(ctx, pool, module, parentID, fileID, true)
		}
		cfg.GetMany = func(ctx context.Context, pool *pgxpool.Pool, parentID string, fileIDs []string) ([]cashfiles.FileRecord, []string, error) {
			return getFDCrossStageFiles(ctx, pool, module, parentID, fileIDs)
		}
		cfg.SoftDelete = func(ctx context.Context, pool *pgxpool.Pool, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
			return softDeleteFDCrossStageFile(ctx, pool, fileID, deletedBy, deletedAt)
		}
		cfg.SoftDeleteTx = func(ctx context.Context, tx pgx.Tx, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
			return softDeleteFDCrossStageFile(ctx, tx, fileID, deletedBy, deletedAt)
		}
	}

	if isMFCrossStageModule(def.Module) {
		module := def.Module
		cfg.AuditByFileIDOnly = true
		cfg.List = func(ctx context.Context, pool *pgxpool.Pool, parentID string) ([]cashfiles.FileRecord, error) {
			return listMFCrossStageFiles(ctx, pool, module, parentID)
		}
		cfg.GetOne = func(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*cashfiles.FileRecord, error) {
			return getMFCrossStageFile(ctx, pool, module, parentID, fileID, false)
		}
		cfg.GetAnyFile = func(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*cashfiles.FileRecord, error) {
			return getMFCrossStageFile(ctx, pool, module, parentID, fileID, true)
		}
		cfg.GetMany = func(ctx context.Context, pool *pgxpool.Pool, parentID string, fileIDs []string) ([]cashfiles.FileRecord, []string, error) {
			return getMFCrossStageFiles(ctx, pool, module, parentID, fileIDs)
		}
		cfg.SoftDelete = func(ctx context.Context, pool *pgxpool.Pool, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
			return softDeleteMFCrossStageFile(ctx, pool, fileID, deletedBy, deletedAt)
		}
		cfg.SoftDeleteTx = func(ctx context.Context, tx pgx.Tx, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
			return softDeleteMFCrossStageFile(ctx, tx, fileID, deletedBy, deletedAt)
		}
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

func getInvestmentAdditionalFile(ctx context.Context, pool *pgxpool.Pool, def investmentFileDefinition, parentID, fileID string, includeDeleted bool) (*cashfiles.FileRecord, error) {
	deletedClause := constants.ErrFDReceiptDeletedFilter
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
