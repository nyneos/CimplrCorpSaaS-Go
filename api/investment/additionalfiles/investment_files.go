package additionalfiles

import (
	cashfiles "CimplrCorpSaas/api/cash/additionalfiles"
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
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
		ParentFilter:  "AND COALESCE(p.is_deleted, FALSE) = FALSE",
	}
	initiationFilesDefinition = investmentFileDefinition{
		Module:        "investment-initiation",
		ParentIDField: "initiation_id",
		TableName:     "investment.investment_initiation_files",
		ParentColumn:  "initiation_id",
		ParentTable:   "investment.investment_initiation",
		ParentFilter:  "AND COALESCE(p.is_deleted, FALSE) = FALSE",
	}
	confirmationFilesDefinition = investmentFileDefinition{
		Module:        "investment-confirmation-additional",
		ParentIDField: "confirmation_id",
		TableName:     "investment.investment_confirmation_files",
		ParentColumn:  "confirmation_id",
		ParentTable:   "investment.investment_confirmation",
		ParentFilter:  "AND COALESCE(p.is_deleted, FALSE) = FALSE",
	}
	redemptionInitiationFilesDefinition = investmentFileDefinition{
		Module:        "investment-redemption-initiation",
		ParentIDField: "redemption_id",
		TableName:     "investment.redemption_initiation_files",
		ParentColumn:  "redemption_id",
		ParentTable:   "investment.redemption_initiation",
		ParentFilter:  "AND COALESCE(p.is_deleted, FALSE) = FALSE",
	}
	redemptionConfirmationFilesDefinition = investmentFileDefinition{
		Module:        "investment-redemption-confirmation-additional",
		ParentIDField: "redemption_confirm_id",
		TableName:     "investment.redemption_confirmation_files",
		ParentColumn:  "redemption_confirm_id",
		ParentTable:   "investment.redemption_confirmation",
		ParentFilter:  "AND COALESCE(p.is_deleted, FALSE) = FALSE",
	}
	accountingActivityFilesDefinition = investmentFileDefinition{
		Module:        "investment-accounting-activity",
		ParentIDField: "activity_id",
		TableName:     "investment.accounting_activity_files",
		ParentColumn:  "activity_id",
		ParentTable:   "investment.accounting_activity",
		ParentFilter:  "AND COALESCE(p.is_deleted, FALSE) = FALSE",
	}
)

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

func investmentAdditionalFilesConfig(def investmentFileDefinition) cashfiles.Config {
	return cashfiles.Config{
		Module:        def.Module,
		ParentIDField: def.ParentIDField,
		List: func(ctx context.Context, pool *pgxpool.Pool, parentID string) ([]cashfiles.FileRecord, error) {
			return listInvestmentAdditionalFiles(ctx, pool, def, parentID)
		},
		Create: func(ctx context.Context, tx pgx.Tx, input cashfiles.CreateInput) error {
			return createInvestmentAdditionalFile(ctx, tx, def, input)
		},
		GetOne: func(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*cashfiles.FileRecord, error) {
			return getInvestmentAdditionalFile(ctx, pool, def, parentID, fileID)
		},
		GetMany: func(ctx context.Context, pool *pgxpool.Pool, parentID string, fileIDs []string) ([]cashfiles.FileRecord, []string, error) {
			return getInvestmentAdditionalFiles(ctx, pool, def, parentID, fileIDs)
		},
		SoftDelete: func(ctx context.Context, pool *pgxpool.Pool, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
			return deleteInvestmentAdditionalFile(ctx, pool, def, parentID, fileID, deletedBy, deletedAt)
		},
	}
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
	parentScope := fmt.Sprintf(`
		SELECT p.%s AS parent_id
		FROM %s p
		WHERE p.%s::text = $8
		  %s
	`, def.ParentColumn, def.ParentTable, def.ParentColumn, def.ParentFilter)
	return cashfiles.InsertAdditionalFileRow(ctx, tx, def.TableName, def.ParentColumn, input, parentScope, strings.TrimSpace(input.ParentID))
}

func getInvestmentAdditionalFile(ctx context.Context, pool *pgxpool.Pool, def investmentFileDefinition, parentID, fileID string) (*cashfiles.FileRecord, error) {
	query := fmt.Sprintf(`
		SELECT f.file_id, f.stored_file_name, f.content_type, f.file_size, f.upload_s3_key, f.uploaded_by, f.uploaded_at
		FROM %s f
		JOIN %s p ON p.%s = f.%s
		WHERE f.%s::text = $1
		  AND f.file_id = $2
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		  %s
	`, def.TableName, def.ParentTable, def.ParentColumn, def.ParentColumn, def.ParentColumn, def.ParentFilter)
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

func deleteInvestmentAdditionalFile(ctx context.Context, pool *pgxpool.Pool, def investmentFileDefinition, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
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
	result, err := pool.Exec(ctx, query, strings.TrimSpace(parentID), strings.TrimSpace(fileID), deletedBy, deletedAt)
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