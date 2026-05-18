package payablerecievable

import (
	"CimplrCorpSaas/api/cash/additionalfiles"
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func ListPayableAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewListHandler(pool, transactionAdditionalFilesConfig("payable"))
}

func UploadPayableAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewUploadHandler(pool, transactionAdditionalFilesConfig("payable"))
}

func DownloadPayableAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDownloadHandler(pool, transactionAdditionalFilesConfig("payable"))
}

func DownloadSelectedPayableAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDownloadSelectedHandler(pool, transactionAdditionalFilesConfig("payable"))
}

func DeletePayableAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDeleteHandler(pool, transactionAdditionalFilesConfig("payable"))
}

func ListReceivableAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewListHandler(pool, transactionAdditionalFilesConfig("receivable"))
}

func UploadReceivableAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewUploadHandler(pool, transactionAdditionalFilesConfig("receivable"))
}

func DownloadReceivableAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDownloadHandler(pool, transactionAdditionalFilesConfig("receivable"))
}

func DownloadSelectedReceivableAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDownloadSelectedHandler(pool, transactionAdditionalFilesConfig("receivable"))
}

func DeleteReceivableAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDeleteHandler(pool, transactionAdditionalFilesConfig("receivable"))
}

func transactionAdditionalFilesConfig(kind string) additionalfiles.Config {
	if strings.EqualFold(kind, "receivable") {
		return additionalfiles.Config{
			Module:        "receivables",
			ParentIDField: "receivable_id",
			List:          listReceivableAdditionalFiles,
			Create:        createReceivableAdditionalFile,
			GetOne:        getReceivableAdditionalFile,
			GetMany:       getReceivableAdditionalFiles,
			SoftDelete:    deleteReceivableAdditionalFile,
		}
	}

	return additionalfiles.Config{
		Module:        "payables",
		ParentIDField: "payable_id",
		List:          listPayableAdditionalFiles,
		Create:        createPayableAdditionalFile,
		GetOne:        getPayableAdditionalFile,
		GetMany:       getPayableAdditionalFiles,
		SoftDelete:    deletePayableAdditionalFile,
	}
}

func listPayableAdditionalFiles(ctx context.Context, pool *pgxpool.Pool, parentID string) ([]additionalfiles.FileRecord, error) {
	return additionalfiles.QueryFiles(ctx, pool, payableQuery(`
		WHERE f.payable_id = $1
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		  AND COALESCE(p.is_deleted, FALSE) = FALSE
		ORDER BY f.uploaded_at DESC
	`), parentID)
}

func createPayableAdditionalFile(ctx context.Context, tx pgx.Tx, input additionalfiles.CreateInput) error {
	return additionalfiles.InsertAdditionalFileRow(ctx, tx, "public.payable_files", "payable_id", input, `
		SELECT p.payable_id AS parent_id
		FROM public.tr_payables p
		WHERE p.payable_id = $8
		  AND COALESCE(p.is_deleted, FALSE) = FALSE
	`, input.ParentID)
}

func getPayableAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*additionalfiles.FileRecord, error) {
	return additionalfiles.FirstFile(ctx, pool, payableQuery(`
		WHERE f.payable_id = $1
		  AND f.file_id = $2
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		  AND COALESCE(p.is_deleted, FALSE) = FALSE
	`), parentID, fileID)
}

func getPayableAdditionalFiles(ctx context.Context, pool *pgxpool.Pool, parentID string, fileIDs []string) ([]additionalfiles.FileRecord, []string, error) {
	trimmedIDs := trimTransactionAdditionalFileIDs(fileIDs)
	files, queryErr := additionalfiles.QueryFiles(ctx, pool, payableQuery(`
		WHERE f.payable_id = $1
		  AND f.file_id = ANY($2)
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		  AND COALESCE(p.is_deleted, FALSE) = FALSE
		ORDER BY f.uploaded_at DESC
	`), parentID, trimmedIDs)
	if queryErr != nil {
		return nil, nil, queryErr
	}
	return files, missingTransactionAdditionalFileIDs(trimmedIDs, files), nil
}

func deletePayableAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	result, execErr := pool.Exec(ctx, `
		UPDATE public.payable_files f
		SET is_deleted = TRUE,
		    deleted_by = $3,
		    deleted_at = $4
		FROM public.tr_payables p
		WHERE f.payable_id = $1
		  AND f.file_id = $2
		  AND p.payable_id = f.payable_id
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		  AND COALESCE(p.is_deleted, FALSE) = FALSE
	`, parentID, fileID, deletedBy, deletedAt)
	if execErr != nil {
		return false, execErr
	}
	return result.RowsAffected() > 0, nil
}

func listReceivableAdditionalFiles(ctx context.Context, pool *pgxpool.Pool, parentID string) ([]additionalfiles.FileRecord, error) {
	return additionalfiles.QueryFiles(ctx, pool, receivableQuery(`
		WHERE f.receivable_id = $1
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		  AND COALESCE(r.is_deleted, FALSE) = FALSE
		ORDER BY f.uploaded_at DESC
	`), parentID)
}

func createReceivableAdditionalFile(ctx context.Context, tx pgx.Tx, input additionalfiles.CreateInput) error {
	return additionalfiles.InsertAdditionalFileRow(ctx, tx, "public.receivable_files", "receivable_id", input, `
		SELECT r.receivable_id AS parent_id
		FROM public.tr_receivables r
		WHERE r.receivable_id = $8
		  AND COALESCE(r.is_deleted, FALSE) = FALSE
	`, input.ParentID)
}

func getReceivableAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*additionalfiles.FileRecord, error) {
	return additionalfiles.FirstFile(ctx, pool, receivableQuery(`
		WHERE f.receivable_id = $1
		  AND f.file_id = $2
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		  AND COALESCE(r.is_deleted, FALSE) = FALSE
	`), parentID, fileID)
}

func getReceivableAdditionalFiles(ctx context.Context, pool *pgxpool.Pool, parentID string, fileIDs []string) ([]additionalfiles.FileRecord, []string, error) {
	trimmedIDs := trimTransactionAdditionalFileIDs(fileIDs)
	files, queryErr := additionalfiles.QueryFiles(ctx, pool, receivableQuery(`
		WHERE f.receivable_id = $1
		  AND f.file_id = ANY($2)
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		  AND COALESCE(r.is_deleted, FALSE) = FALSE
		ORDER BY f.uploaded_at DESC
	`), parentID, trimmedIDs)
	if queryErr != nil {
		return nil, nil, queryErr
	}
	return files, missingTransactionAdditionalFileIDs(trimmedIDs, files), nil
}

func deleteReceivableAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	result, execErr := pool.Exec(ctx, `
		UPDATE public.receivable_files f
		SET is_deleted = TRUE,
		    deleted_by = $3,
		    deleted_at = $4
		FROM public.tr_receivables r
		WHERE f.receivable_id = $1
		  AND f.file_id = $2
		  AND r.receivable_id = f.receivable_id
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		  AND COALESCE(r.is_deleted, FALSE) = FALSE
	`, parentID, fileID, deletedBy, deletedAt)
	if execErr != nil {
		return false, execErr
	}
	return result.RowsAffected() > 0, nil
}

func payableQuery(whereClause string) string {
	return `
		SELECT f.file_id, f.stored_file_name, f.content_type, f.file_size, f.upload_s3_key, f.uploaded_by, f.uploaded_at
		FROM public.payable_files f
		JOIN public.tr_payables p ON p.payable_id = f.payable_id
	` + whereClause
}

func receivableQuery(whereClause string) string {
	return `
		SELECT f.file_id, f.stored_file_name, f.content_type, f.file_size, f.upload_s3_key, f.uploaded_by, f.uploaded_at
		FROM public.receivable_files f
		JOIN public.tr_receivables r ON r.receivable_id = f.receivable_id
	` + whereClause
}

func trimTransactionAdditionalFileIDs(fileIDs []string) []string {
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

func missingTransactionAdditionalFileIDs(expected []string, files []additionalfiles.FileRecord) []string {
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
