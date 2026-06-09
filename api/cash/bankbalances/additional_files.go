package bankbalances

import (
	api "CimplrCorpSaas/api"
	"CimplrCorpSaas/api/cash/additionalfiles"
	"CimplrCorpSaas/api/constants"
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func ListAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewListHandler(pool, bankBalanceAdditionalFilesConfig())
}

func UploadAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewUploadHandler(pool, bankBalanceAdditionalFilesConfig())
}

func DownloadAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDownloadHandler(pool, bankBalanceAdditionalFilesConfig())
}

func DownloadSelectedAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDownloadSelectedHandler(pool, bankBalanceAdditionalFilesConfig())
}

func DownloadBankBalancePackageZipHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewPackageZipHandler(pool, bankBalanceAdditionalFilesConfig(), additionalfiles.PackageZipOptions{
		ModuleLabel: "Bank Balance",
		IDField:     "balance_id",
		LoadMain:    loadBankBalancePackageMainFile,
	})
}

func DeleteAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDeleteHandler(pool, bankBalanceAdditionalFilesConfig())
}

func AuditAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewAuditHandler(pool, bankBalanceAdditionalFilesConfig())
}

func ApproveAdditionalFileDeleteHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewApproveDeleteHandler(pool, bankBalanceAdditionalFilesConfig())
}

func RejectAdditionalFileDeleteHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewRejectDeleteHandler(pool, bankBalanceAdditionalFilesConfig())
}

func bankBalanceAdditionalFilesConfig() additionalfiles.Config {
	return additionalfiles.WithCashCrossStageVisibility(additionalfiles.Config{
		Module:                "bankbalance",
		AuditSource:           "BANK_BALANCE",
		ParentIDField:         "balance_id",
		List:                  listBankBalanceAdditionalFiles,
		CreateReturning:       createBankBalanceAdditionalFile,
		GetOne:                getBankBalanceAdditionalFile,
		GetAnyFile:            getAnyBankBalanceAdditionalFile,
		GetMany:               getBankBalanceAdditionalFiles,
		SoftDelete:            deleteBankBalanceAdditionalFile,
		SoftDeleteTx:          deleteBankBalanceAdditionalFileTx,
		RecordMainUploadAudit: recordBankBalanceMainUploadAudit,
	})
}

func recordBankBalanceMainUploadAudit(ctx context.Context, tx pgx.Tx, parentID string, payload additionalfiles.MainUploadAuditPayload) error {
	reason, err := additionalfiles.MainUploadAuditReasonJSON(payload)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO public.auditactionbankbalances (
			balance_id,
			actiontype,
			processing_status,
			reason,
			requested_by,
			requested_at
		) VALUES ($1, 'UPLOAD_FILE', 'APPROVED', $2, $3, $4)
	`, parentID, reason, payload.UploadedBy, payload.UploadedAt)
	return err
}

func listBankBalanceAdditionalFiles(ctx context.Context, pool *pgxpool.Pool, parentID string) ([]additionalfiles.FileRecord, error) {
	scopeClause, scopeArgs, err := bankBalanceAccessScope(ctx, 2)
	if err != nil {
		return nil, err
	}

	query := `
		SELECT f.file_id, f.stored_file_name, f.content_type, f.file_size, f.upload_s3_key, f.uploaded_by, f.uploaded_at
		FROM cimplrcorpsaas.bank_balance_files f
		JOIN public.bank_balances_manual b ON b.balance_id = f.balance_id
		JOIN (
			SELECT account_number, MIN(entity_id) AS entity_id
			FROM public.masterbankaccount
			GROUP BY account_number
		) mba ON mba.account_number = b.account_no
		LEFT JOIN public.masterentitycash ec ON ec.entity_id = mba.entity_id
		LEFT JOIN public.masterentity me ON me.entity_id::text = mba.entity_id
		WHERE f.balance_id = $1
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		  AND ` + scopeClause + `
		ORDER BY f.uploaded_at DESC
	`

	args := append([]interface{}{parentID}, scopeArgs...)
	return additionalfiles.QueryFiles(ctx, pool, query, args...)
}

func loadBankBalancePackageMainFile(ctx context.Context, pool *pgxpool.Pool, rowID string) (*additionalfiles.MainPackageFile, error) {
	scopeClause, scopeArgs, err := bankBalanceAccessScope(ctx, 2)
	if err != nil {
		return nil, err
	}
	query := `
		SELECT COALESCE(b.upload_s3_key, '')
		FROM public.bank_balances_manual b
		JOIN (
			SELECT account_number, MIN(entity_id) AS entity_id
			FROM public.masterbankaccount
			GROUP BY account_number
		) mba ON mba.account_number = b.account_no
		LEFT JOIN public.masterentitycash ec ON ec.entity_id = mba.entity_id
		LEFT JOIN public.masterentity me ON me.entity_id::text = mba.entity_id
		WHERE b.balance_id = $1
		  AND ` + scopeClause + `
	`
	args := append([]interface{}{rowID}, scopeArgs...)
	var uploadS3Key string
	if err := pool.QueryRow(ctx, query, args...).Scan(&uploadS3Key); err != nil || strings.TrimSpace(uploadS3Key) == "" {
		return nil, err
	}
	return &additionalfiles.MainPackageFile{UploadS3Key: uploadS3Key}, nil
}

func createBankBalanceAdditionalFile(ctx context.Context, tx pgx.Tx, input additionalfiles.CreateInput) (string, error) {
	scopeClause, scopeArgs, err := bankBalanceAccessScope(ctx, 9)
	if err != nil {
		return "", err
	}

	parentScope := `
		SELECT b.balance_id AS parent_id
		FROM public.bank_balances_manual b
		JOIN (
			SELECT account_number, MIN(entity_id) AS entity_id
			FROM public.masterbankaccount
			GROUP BY account_number
		) mba ON mba.account_number = b.account_no
		LEFT JOIN public.masterentitycash ec ON ec.entity_id = mba.entity_id
		LEFT JOIN public.masterentity me ON me.entity_id::text = mba.entity_id
		WHERE b.balance_id = $8
		  AND ` + scopeClause + `
	`

	args := append([]interface{}{input.ParentID}, scopeArgs...)
	return additionalfiles.InsertAdditionalFileRowReturningID(ctx, tx, "cimplrcorpsaas.bank_balance_files", "balance_id", input, parentScope, args...)
}

func getBankBalanceAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*additionalfiles.FileRecord, error) {
	return getBankBalanceAdditionalFileWithDeleted(ctx, pool, parentID, fileID, false)
}

func getAnyBankBalanceAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*additionalfiles.FileRecord, error) {
	return getBankBalanceAdditionalFileWithDeleted(ctx, pool, parentID, fileID, true)
}

func getBankBalanceAdditionalFileWithDeleted(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string, includeDeleted bool) (*additionalfiles.FileRecord, error) {
	scopeClause, scopeArgs, err := bankBalanceAccessScope(ctx, 3)
	if err != nil {
		return nil, err
	}

	deletedClause := "AND COALESCE(f.is_deleted, FALSE) = FALSE"
	if includeDeleted {
		deletedClause = ""
	}

	query := `
		SELECT f.file_id, f.stored_file_name, f.content_type, f.file_size, f.upload_s3_key, f.uploaded_by, f.uploaded_at
		FROM cimplrcorpsaas.bank_balance_files f
		JOIN public.bank_balances_manual b ON b.balance_id = f.balance_id
		JOIN (
			SELECT account_number, MIN(entity_id) AS entity_id
			FROM public.masterbankaccount
			GROUP BY account_number
		) mba ON mba.account_number = b.account_no
		LEFT JOIN public.masterentitycash ec ON ec.entity_id = mba.entity_id
		LEFT JOIN public.masterentity me ON me.entity_id::text = mba.entity_id
		WHERE f.balance_id = $1
		  AND f.file_id = $2
		  ` + deletedClause + `
		  AND ` + scopeClause + `
	`

	args := append([]interface{}{parentID, fileID}, scopeArgs...)
	return additionalfiles.FirstFile(ctx, pool, query, args...)
}

func getBankBalanceAdditionalFiles(ctx context.Context, pool *pgxpool.Pool, parentID string, fileIDs []string) ([]additionalfiles.FileRecord, []string, error) {
	scopeClause, scopeArgs, err := bankBalanceAccessScope(ctx, 3)
	if err != nil {
		return nil, nil, err
	}

	trimmedIDs := trimBankBalanceAdditionalFileIDs(fileIDs)
	query := `
		SELECT f.file_id, f.stored_file_name, f.content_type, f.file_size, f.upload_s3_key, f.uploaded_by, f.uploaded_at
		FROM cimplrcorpsaas.bank_balance_files f
		JOIN public.bank_balances_manual b ON b.balance_id = f.balance_id
		JOIN (
			SELECT account_number, MIN(entity_id) AS entity_id
			FROM public.masterbankaccount
			GROUP BY account_number
		) mba ON mba.account_number = b.account_no
		LEFT JOIN public.masterentitycash ec ON ec.entity_id = mba.entity_id
		LEFT JOIN public.masterentity me ON me.entity_id::text = mba.entity_id
		WHERE f.balance_id = $1
		  AND f.file_id = ANY($2)
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		  AND ` + scopeClause + `
		ORDER BY f.uploaded_at DESC
	`

	args := append([]interface{}{parentID, trimmedIDs}, scopeArgs...)
	files, err := additionalfiles.QueryFiles(ctx, pool, query, args...)
	if err != nil {
		return nil, nil, err
	}
	return files, missingBankBalanceAdditionalFileIDs(trimmedIDs, files), nil
}

func deleteBankBalanceAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	return deleteBankBalanceAdditionalFileExec(ctx, pool, parentID, fileID, deletedBy, deletedAt)
}

func deleteBankBalanceAdditionalFileTx(ctx context.Context, tx pgx.Tx, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	return deleteBankBalanceAdditionalFileExec(ctx, tx, parentID, fileID, deletedBy, deletedAt)
}

type bankBalanceFileExec interface {
	Exec(context.Context, string, ...interface{}) (pgconn.CommandTag, error)
}

func deleteBankBalanceAdditionalFileExec(ctx context.Context, exec bankBalanceFileExec, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	scopeClause, scopeArgs, err := bankBalanceAccessScope(ctx, 5)
	if err != nil {
		return false, err
	}

	query := `
		UPDATE cimplrcorpsaas.bank_balance_files f
		SET is_deleted = TRUE,
		    deleted_by = $3,
		    deleted_at = $4
		FROM public.bank_balances_manual b,
		     (
			 SELECT account_number, MIN(entity_id) AS entity_id
			 FROM public.masterbankaccount
			 GROUP BY account_number
		     ) mba
		LEFT JOIN public.masterentitycash ec ON ec.entity_id = mba.entity_id
		LEFT JOIN public.masterentity me ON me.entity_id::text = mba.entity_id
		WHERE f.balance_id = $1
		  AND f.file_id = $2
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		  AND b.balance_id = f.balance_id
		  AND mba.account_number = b.account_no
		  AND ` + scopeClause + `
	`

	args := append([]interface{}{parentID, fileID, deletedBy, deletedAt}, scopeArgs...)
	result, execErr := exec.Exec(ctx, query, args...)
	if execErr != nil {
		return false, execErr
	}
	return result.RowsAffected() > 0, nil
}

func bankBalanceAccessScope(ctx context.Context, startPosition int) (string, []interface{}, error) {
	entityIDs := api.GetEntityIDsFromCtx(ctx)
	entityNames := api.GetEntityNamesFromCtx(ctx)
	accountNos := api.CtxApprovedAccountNumbers(ctx)
	bankNames := loweredBankBalanceValues(api.CtxApprovedBankNames(ctx))
	currencyCodes := loweredBankBalanceValues(api.CtxApprovedCurrencies(ctx))

	clauses := make([]string, 0, 5)
	args := make([]interface{}, 0, 5)
	position := startPosition

	if len(entityIDs) > 0 {
		clauses = append(clauses, fmt.Sprintf("mba.entity_id = ANY($%d)", position))
		args = append(args, entityIDs)
		position++
	} else if len(entityNames) > 0 {
		clauses = append(clauses, fmt.Sprintf("LOWER(COALESCE(ec.entity_name, me.entity_name, '')) = ANY($%d)", position))
		args = append(args, loweredBankBalanceValues(entityNames))
		position++
	}

	if len(bankNames) > 0 {
		clauses = append(clauses, fmt.Sprintf("LOWER(b.bank_name) = ANY($%d)", position))
		args = append(args, bankNames)
		position++
	}
	if len(accountNos) > 0 {
		clauses = append(clauses, fmt.Sprintf("b.account_no = ANY($%d)", position))
		args = append(args, accountNos)
		position++
	}
	if len(currencyCodes) > 0 {
		clauses = append(clauses, fmt.Sprintf("LOWER(b.currency_code) = ANY($%d)", position))
		args = append(args, currencyCodes)
		position++
	}

	if len(clauses) == 0 {
		return "", nil, errors.New(constants.ErrNoAccessibleBusinessUnit)
	}

	return strings.Join(clauses, " AND "), args, nil
}

func loweredBankBalanceValues(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if trimmed := strings.ToLower(strings.TrimSpace(value)); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func trimBankBalanceAdditionalFileIDs(fileIDs []string) []string {
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

func missingBankBalanceAdditionalFileIDs(expected []string, files []additionalfiles.FileRecord) []string {
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
