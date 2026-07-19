package additionalfiles

import (
	"context"
	"net/http"
	"strings"

	cashfiles "CimplrCorpSaas/api/cash/additionalfiles"

	"github.com/jackc/pgx/v5/pgxpool"
)

func DownloadOnboardPackageZipHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewPackageZipHandler(pool, investmentAdditionalFilesConfig(onboardingFilesDefinition), cashfiles.PackageZipOptions{
		ModuleLabel: "Onboarding Center",
		IDField:     "batch_id",
		LoadMain:    loadOnboardPackageMainFile,
	})
}

func DownloadProposalPackageZipHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return investmentAdditionalOnlyPackageHandler(pool, proposalFilesDefinition, "Investment Proposal", "proposal_id")
}

func DownloadInitiationPackageZipHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return investmentAdditionalOnlyPackageHandler(pool, initiationFilesDefinition, "Investment Initiation", "initiation_id")
}

func DownloadConfirmationPackageZipHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewPackageZipHandler(pool, investmentAdditionalFilesConfig(confirmationFilesDefinition), cashfiles.PackageZipOptions{
		ModuleLabel: "Investment Confirmation",
		IDField:     "confirmation_id",
		LoadMain:    loadConfirmationPackageMainFile,
	})
}

func DownloadRedemptionInitiationPackageZipHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return investmentAdditionalOnlyPackageHandler(pool, redemptionInitiationFilesDefinition, "Redemption Initiation", "redemption_id")
}

func DownloadRedemptionConfirmationPackageZipHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewPackageZipHandler(pool, investmentAdditionalFilesConfig(redemptionConfirmationFilesDefinition), cashfiles.PackageZipOptions{
		ModuleLabel: "Redemption Confirmation",
		IDField:     "redemption_confirm_id",
		LoadMain:    loadRedemptionConfirmationPackageMainFile,
	})
}

func DownloadAccountingActivityPackageZipHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return investmentAdditionalOnlyPackageHandler(pool, accountingActivityFilesDefinition, "Financial Closing Activity", "activity_id")
}

func DownloadFVOPackageZipHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewPackageZipHandler(pool, investmentAdditionalFilesConfig(accountingActivityFilesDefinition), cashfiles.PackageZipOptions{
		ModuleLabel:               "Fair Value Override",
		IDField:                   "fvo_id",
		LoadMain:                  loadFVOPackageMainFile,
		ResolveAdditionalParentID: resolveFVOActivityID,
	})
}

func DownloadFDBookingPackageZipHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return investmentAdditionalOnlyPackageHandler(pool, fdBookingFilesDefinition, "FD Booking", "booking_id")
}

func DownloadFDConfirmationPackageZipHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewPackageZipHandler(pool, investmentAdditionalFilesConfig(fdConfirmationFilesDefinition), cashfiles.PackageZipOptions{
		ModuleLabel: "FD Confirmation",
		IDField:     "confirmation_id",
		LoadMain:    loadFDConfirmationPackageMainFile,
	})
}

func DownloadFDMasterPackageZipHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return investmentAdditionalOnlyPackageHandler(pool, fdMasterFilesDefinition, "FD Master", "fd_id")
}

func DownloadFDClosurePackageZipHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewPackageZipHandler(pool, investmentAdditionalFilesConfig(fdClosureFilesDefinition), cashfiles.PackageZipOptions{
		ModuleLabel: "FD Closure",
		IDField:     "closure_request_id",
		LoadMain:    loadFDClosurePackageMainFile,
	})
}

func DownloadFDRolloverPackageZipHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return investmentAdditionalOnlyPackageHandler(pool, fdRolloverFilesDefinition, "FD Rollover", "closure_request_id")
}

func DownloadFDCashflowPackageZipHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return investmentAdditionalOnlyPackageHandler(pool, fdCashflowFilesDefinition, "FD Cashflow", "fd_id")
}

func DownloadFDInterestReceiptPackageZipHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewPackageZipHandler(pool, investmentAdditionalFilesConfig(fdInterestReceiptFilesDefinition), cashfiles.PackageZipOptions{
		ModuleLabel: "FD Interest Receipt",
		IDField:     "receipt_id",
		LoadMain:    loadFDInterestReceiptPackageMainFile,
	})
}

func DownloadFDTDSReceiptPackageZipHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return investmentAdditionalOnlyPackageHandler(pool, fdTDSReceiptFilesDefinition, "FD TDS Register", "tds_id")
}

func DownloadFDReconcileResultPackageZipHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return investmentAdditionalOnlyPackageHandler(pool, fdReconcileResultFilesDefinition, "FD Reconcile Result", "result_id")
}

func DownloadFDReceiptExceptionPackageZipHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return investmentAdditionalOnlyPackageHandler(pool, fdReceiptExceptionFilesDefinition, "FD Receipt Exception", "exception_id")
}

func DownloadVarianceExceptionPackageZipHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return investmentAdditionalOnlyPackageHandler(pool, varianceExceptionFilesDefinition, "Variance Exception", "exception_id")
}

func DownloadFDAccrualScheduleConfigPackageZipHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return investmentAdditionalOnlyPackageHandler(pool, fdAccrualScheduleConfigFilesDefinition, "FD Accrual Schedule", "config_id")
}

func DownloadFDAccrualRunPackageZipHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return investmentAdditionalOnlyPackageHandler(pool, fdAccrualRunFilesDefinition, "FD Accrual Run", "run_id")
}

func DownloadFDAccrualLedgerPackageZipHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return investmentAdditionalOnlyPackageHandler(pool, fdAccrualLedgerFilesDefinition, "FD Accrual Ledger", "ledger_id")
}

func DownloadFDAccountingJournalPackageZipHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return investmentAdditionalOnlyPackageHandler(pool, fdAccountingJournalFilesDefinition, "FD Accounting Journal", "entry_id")
}

func investmentAdditionalOnlyPackageHandler(pool *pgxpool.Pool, def investmentFileDefinition, moduleLabel, idField string) http.HandlerFunc {
	return cashfiles.NewPackageZipHandler(pool, investmentAdditionalFilesConfig(def), cashfiles.PackageZipOptions{
		ModuleLabel: moduleLabel,
		IDField:     idField,
	})
}

func loadOnboardPackageMainFile(ctx context.Context, pool *pgxpool.Pool, rowID string) (*cashfiles.MainPackageFile, error) {
	return loadInvestmentMainFile(ctx, pool, `
		SELECT COALESCE(upload_s3_key, '')
		FROM investment.onboard_batch
		WHERE batch_id::text = $1
	`, rowID)
}

func loadConfirmationPackageMainFile(ctx context.Context, pool *pgxpool.Pool, rowID string) (*cashfiles.MainPackageFile, error) {
	return loadInvestmentMainFile(ctx, pool, `
		SELECT COALESCE(upload_s3_key, '')
		FROM investment.investment_confirmation
		WHERE confirmation_id::text = $1
	`, rowID)
}

func loadRedemptionConfirmationPackageMainFile(ctx context.Context, pool *pgxpool.Pool, rowID string) (*cashfiles.MainPackageFile, error) {
	return loadInvestmentMainFile(ctx, pool, `
		SELECT COALESCE(upload_s3_key, '')
		FROM investment.redemption_confirmation
		WHERE redemption_confirm_id::text = $1
	`, rowID)
}

func loadFVOPackageMainFile(ctx context.Context, pool *pgxpool.Pool, rowID string) (*cashfiles.MainPackageFile, error) {
	return loadInvestmentMainFile(ctx, pool, `
		SELECT COALESCE(upload_s3_key, '')
		FROM investment.accounting_fvo
		WHERE fvo_id::text = $1
	`, rowID)
}

func loadFDConfirmationPackageMainFile(ctx context.Context, pool *pgxpool.Pool, rowID string) (*cashfiles.MainPackageFile, error) {
	return loadInvestmentMainFile(ctx, pool, `
		SELECT COALESCE(upload_s3_key, '')
		FROM investment.fd_confirmation
		WHERE confirmation_id::text = $1
		  AND COALESCE(is_deleted, false) = false
	`, rowID)
}

func loadFDInterestReceiptPackageMainFile(ctx context.Context, pool *pgxpool.Pool, rowID string) (*cashfiles.MainPackageFile, error) {
	return loadInvestmentMainFile(ctx, pool, `
		SELECT COALESCE(upload_s3_key, '')
		FROM investment.fd_interest_receipt
		WHERE receipt_id::text = $1
		  AND COALESCE(is_deleted, false) = false
	`, rowID)
}

func loadFDClosurePackageMainFile(ctx context.Context, pool *pgxpool.Pool, rowID string) (*cashfiles.MainPackageFile, error) {
	return loadInvestmentMainFile(ctx, pool, `
		SELECT COALESCE(upload_s3_key, '')
		FROM investment.fd_closure_request
		WHERE closure_request_id::text = $1
		  AND COALESCE(is_deleted, false) = false
	`, rowID)
}

func loadInvestmentMainFile(ctx context.Context, pool *pgxpool.Pool, query, rowID string) (*cashfiles.MainPackageFile, error) {
	var uploadS3Key string
	if err := pool.QueryRow(ctx, query, strings.TrimSpace(rowID)).Scan(&uploadS3Key); err != nil {
		return nil, err
	}
	uploadS3Key = strings.TrimSpace(uploadS3Key)
	if uploadS3Key == "" {
		return nil, nil
	}
	return &cashfiles.MainPackageFile{UploadS3Key: uploadS3Key}, nil
}

func resolveFVOActivityID(ctx context.Context, pool *pgxpool.Pool, rowID string) (string, error) {
	var activityID string
	err := pool.QueryRow(ctx, `
		SELECT COALESCE(activity_id, '')
		FROM investment.accounting_fvo
		WHERE fvo_id::text = $1
	`, strings.TrimSpace(rowID)).Scan(&activityID)
	return strings.TrimSpace(activityID), err
}
