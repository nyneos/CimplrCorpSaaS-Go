package limit

import (
	api "CimplrCorpSaas/api"
	"CimplrCorpSaas/api/cash/additionalfiles"
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

func ListLimitAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewListHandler(pool, limitAdditionalFilesConfig())
}

func UploadLimitAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewUploadHandler(pool, limitAdditionalFilesConfig())
}

func DownloadLimitAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDownloadHandler(pool, limitAdditionalFilesConfig())
}

func DownloadSelectedLimitAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDownloadSelectedHandler(pool, limitAdditionalFilesConfig())
}

func DeleteLimitAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDeleteHandler(pool, limitAdditionalFilesConfig())
}

func AuditLimitAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewAuditHandler(pool, limitAdditionalFilesConfig())
}

func ApproveLimitAdditionalFileDeleteHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewApproveDeleteHandler(pool, limitAdditionalFilesConfig())
}

func RejectLimitAdditionalFileDeleteHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewRejectDeleteHandler(pool, limitAdditionalFilesConfig())
}

func ListUtilizationAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewListHandler(pool, utilizationAdditionalFilesConfig())
}

func UploadUtilizationAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewUploadHandler(pool, utilizationAdditionalFilesConfig())
}

func DownloadUtilizationAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDownloadHandler(pool, utilizationAdditionalFilesConfig())
}

func DownloadSelectedUtilizationAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDownloadSelectedHandler(pool, utilizationAdditionalFilesConfig())
}

func DeleteUtilizationAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDeleteHandler(pool, utilizationAdditionalFilesConfig())
}

func AuditUtilizationAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewAuditHandler(pool, utilizationAdditionalFilesConfig())
}

func ApproveUtilizationAdditionalFileDeleteHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewApproveDeleteHandler(pool, utilizationAdditionalFilesConfig())
}

func RejectUtilizationAdditionalFileDeleteHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewRejectDeleteHandler(pool, utilizationAdditionalFilesConfig())
}

func limitAdditionalFilesConfig() additionalfiles.Config {
	return additionalfiles.WithCashCrossStageVisibility(additionalfiles.Config{
		Module:           "limit-position",
		AuditSource:      "LIMIT_POSITION",
		ParentIDField:    "limit_id",
		FolderName:       additionalfiles.AdditionalFilesFolder(),
		PolicyModuleCode: "CASH",
		PolicySubModule:  "BANK_LIMIT",
		List:             listLimitAdditionalFiles,
		CreateReturning:  createLimitAdditionalFile,
		GetOne:           getLimitAdditionalFile,
		GetAnyFile:       getAnyLimitAdditionalFile,
		GetMany:          getLimitAdditionalFiles,
		SoftDelete:       deleteLimitAdditionalFile,
		SoftDeleteTx:     deleteLimitAdditionalFileTx,
	})
}

func utilizationAdditionalFilesConfig() additionalfiles.Config {
	return additionalfiles.WithCashCrossStageVisibility(additionalfiles.Config{
		Module:           "limit-utilization",
		AuditSource:      "LIMIT_UTILIZATION",
		ParentIDField:    "utilization_id",
		FolderName:       additionalfiles.AdditionalFilesFolder(),
		PolicyModuleCode: "CASH",
		PolicySubModule:  "LIMIT_UTILIZATION",
		List:             listUtilizationAdditionalFiles,
		CreateReturning:  createUtilizationAdditionalFile,
		GetOne:           getUtilizationAdditionalFile,
		GetAnyFile:       getAnyUtilizationAdditionalFile,
		GetMany:          getUtilizationAdditionalFiles,
		SoftDelete:       deleteUtilizationAdditionalFile,
		SoftDeleteTx:     deleteUtilizationAdditionalFileTx,
	})
}

func listLimitAdditionalFiles(ctx context.Context, pool *pgxpool.Pool, parentID string) ([]additionalfiles.FileRecord, error) {
	query, args := limitFilesQuery(ctx, `
		WHERE f.limit_id = $1
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		  AND COALESCE(l.is_deleted, FALSE) = FALSE
	`, parentID)
	query += ` ORDER BY f.uploaded_at DESC`
	return additionalfiles.QueryFiles(ctx, pool, query, args...)
}

func createLimitAdditionalFile(ctx context.Context, tx pgx.Tx, input additionalfiles.CreateInput) (string, error) {
	entityClause, entityArgs := limitEntityScope(ctx, "l", 9)
	args := append([]interface{}{input.ParentID}, entityArgs...)
	return additionalfiles.InsertAdditionalFileRowReturningID(ctx, tx, "cimplrcorpsaas.bank_limit_files", "limit_id", input, `
		SELECT l.limit_id AS parent_id
		FROM cimplrcorpsaas.bank_limit l
		WHERE l.limit_id = $8
		  AND COALESCE(l.is_deleted, FALSE) = FALSE
	`+entityClause, args...)
}

func getLimitAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*additionalfiles.FileRecord, error) {
	return getLimitAdditionalFileWithDeleted(ctx, pool, parentID, fileID, false)
}

func getAnyLimitAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*additionalfiles.FileRecord, error) {
	return getLimitAdditionalFileWithDeleted(ctx, pool, parentID, fileID, true)
}

func getLimitAdditionalFileWithDeleted(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string, includeDeleted bool) (*additionalfiles.FileRecord, error) {
	deletedClause := constants.ErrFDReceiptDeletedFilter
	if includeDeleted {
		deletedClause = ""
	}
	query, args := limitFilesQuery(ctx, `
		WHERE f.limit_id = $1
		  AND f.file_id = $2
		  `+deletedClause+`
		  AND COALESCE(l.is_deleted, FALSE) = FALSE
	`, parentID, fileID)
	return additionalfiles.FirstFile(ctx, pool, query, args...)
}

func getLimitAdditionalFiles(ctx context.Context, pool *pgxpool.Pool, parentID string, fileIDs []string) ([]additionalfiles.FileRecord, []string, error) {
	trimmedIDs := trimLimitAdditionalFileIDs(fileIDs)
	query, args := limitFilesQuery(ctx, `
		WHERE f.limit_id = $1
		  AND f.file_id = ANY($2)
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		  AND COALESCE(l.is_deleted, FALSE) = FALSE
	`, parentID, trimmedIDs)
	query += ` ORDER BY f.uploaded_at DESC`

	files, err := additionalfiles.QueryFiles(ctx, pool, query, args...)
	if err != nil {
		return nil, nil, err
	}
	return files, missingLimitAdditionalFileIDs(trimmedIDs, files), nil
}

func deleteLimitAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	return deleteLimitAdditionalFileExec(ctx, pool, parentID, fileID, deletedBy, deletedAt)
}

func deleteLimitAdditionalFileTx(ctx context.Context, tx pgx.Tx, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	return deleteLimitAdditionalFileExec(ctx, tx, parentID, fileID, deletedBy, deletedAt)
}

type limitFileExec interface {
	Exec(context.Context, string, ...interface{}) (pgconn.CommandTag, error)
}

func deleteLimitAdditionalFileExec(ctx context.Context, exec limitFileExec, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	entityClause, entityArgs := limitEntityScope(ctx, "l", 5)
	query := `
		UPDATE cimplrcorpsaas.bank_limit_files f
		SET is_deleted = TRUE,
		    deleted_by = $3,
		    deleted_at = $4
		FROM cimplrcorpsaas.bank_limit l
		WHERE f.limit_id = $1
		  AND f.file_id = $2
		  AND l.limit_id = f.limit_id
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		  AND COALESCE(l.is_deleted, FALSE) = FALSE
	` + entityClause

	args := []interface{}{parentID, fileID, deletedBy, deletedAt}
	args = append(args, entityArgs...)
	result, err := exec.Exec(ctx, query, args...)
	if err != nil {
		return false, err
	}
	return result.RowsAffected() > 0, nil
}

func listUtilizationAdditionalFiles(ctx context.Context, pool *pgxpool.Pool, parentID string) ([]additionalfiles.FileRecord, error) {
	query, args := utilizationFilesQuery(ctx, `
		WHERE f.utilization_id = $1
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		  AND COALESCE(u.is_deleted, FALSE) = FALSE
		  AND COALESCE(l.is_deleted, FALSE) = FALSE
	`, parentID)
	query += ` ORDER BY f.uploaded_at DESC`
	return additionalfiles.QueryFiles(ctx, pool, query, args...)
}

func createUtilizationAdditionalFile(ctx context.Context, tx pgx.Tx, input additionalfiles.CreateInput) (string, error) {
	entityClause, entityArgs := limitEntityScope(ctx, "l", 9)
	args := append([]interface{}{input.ParentID}, entityArgs...)
	return additionalfiles.InsertAdditionalFileRowReturningID(ctx, tx, "cimplrcorpsaas.bank_limit_utilization_files", "utilization_id", input, `
		SELECT u.utilization_id AS parent_id
		FROM cimplrcorpsaas.bank_limit_utilization u
		JOIN cimplrcorpsaas.bank_limit l ON l.limit_id = u.limit_id
		WHERE u.utilization_id = $8
		  AND COALESCE(u.is_deleted, FALSE) = FALSE
		  AND COALESCE(l.is_deleted, FALSE) = FALSE
	`+entityClause, args...)
}

func getUtilizationAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*additionalfiles.FileRecord, error) {
	return getUtilizationAdditionalFileWithDeleted(ctx, pool, parentID, fileID, false)
}

func getAnyUtilizationAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*additionalfiles.FileRecord, error) {
	return getUtilizationAdditionalFileWithDeleted(ctx, pool, parentID, fileID, true)
}

func getUtilizationAdditionalFileWithDeleted(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string, includeDeleted bool) (*additionalfiles.FileRecord, error) {
	deletedClause := constants.ErrFDReceiptDeletedFilter
	if includeDeleted {
		deletedClause = ""
	}
	query, args := utilizationFilesQuery(ctx, `
		WHERE f.utilization_id = $1
		  AND f.file_id = $2
		  `+deletedClause+`
		  AND COALESCE(u.is_deleted, FALSE) = FALSE
		  AND COALESCE(l.is_deleted, FALSE) = FALSE
	`, parentID, fileID)
	return additionalfiles.FirstFile(ctx, pool, query, args...)
}

func getUtilizationAdditionalFiles(ctx context.Context, pool *pgxpool.Pool, parentID string, fileIDs []string) ([]additionalfiles.FileRecord, []string, error) {
	trimmedIDs := trimLimitAdditionalFileIDs(fileIDs)
	query, args := utilizationFilesQuery(ctx, `
		WHERE f.utilization_id = $1
		  AND f.file_id = ANY($2)
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		  AND COALESCE(u.is_deleted, FALSE) = FALSE
		  AND COALESCE(l.is_deleted, FALSE) = FALSE
	`, parentID, trimmedIDs)
	query += ` ORDER BY f.uploaded_at DESC`

	files, err := additionalfiles.QueryFiles(ctx, pool, query, args...)
	if err != nil {
		return nil, nil, err
	}
	return files, missingLimitAdditionalFileIDs(trimmedIDs, files), nil
}

func deleteUtilizationAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	return deleteUtilizationAdditionalFileExec(ctx, pool, parentID, fileID, deletedBy, deletedAt)
}

func deleteUtilizationAdditionalFileTx(ctx context.Context, tx pgx.Tx, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	return deleteUtilizationAdditionalFileExec(ctx, tx, parentID, fileID, deletedBy, deletedAt)
}

func deleteUtilizationAdditionalFileExec(ctx context.Context, exec limitFileExec, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	entityClause, entityArgs := limitEntityScope(ctx, "l", 5)
	query := `
		UPDATE cimplrcorpsaas.bank_limit_utilization_files f
		SET is_deleted = TRUE,
		    deleted_by = $3,
		    deleted_at = $4
		FROM cimplrcorpsaas.bank_limit_utilization u
		JOIN cimplrcorpsaas.bank_limit l ON l.limit_id = u.limit_id
		WHERE f.utilization_id = $1
		  AND f.file_id = $2
		  AND u.utilization_id = f.utilization_id
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		  AND COALESCE(u.is_deleted, FALSE) = FALSE
		  AND COALESCE(l.is_deleted, FALSE) = FALSE
	` + entityClause

	args := []interface{}{parentID, fileID, deletedBy, deletedAt}
	args = append(args, entityArgs...)
	result, err := exec.Exec(ctx, query, args...)
	if err != nil {
		return false, err
	}
	return result.RowsAffected() > 0, nil
}

func limitFilesQuery(ctx context.Context, whereClause string, baseArgs ...interface{}) (string, []interface{}) {
	entityClause, entityArgs := limitEntityScope(ctx, "l", len(baseArgs)+1)
	query := `
		SELECT f.file_id, f.stored_file_name, f.content_type, f.file_size, f.upload_s3_key, f.uploaded_by, f.uploaded_at
		FROM cimplrcorpsaas.bank_limit_files f
		JOIN cimplrcorpsaas.bank_limit l ON l.limit_id = f.limit_id
	` + whereClause + entityClause

	args := append([]interface{}{}, baseArgs...)
	args = append(args, entityArgs...)
	return query, args
}

func utilizationFilesQuery(ctx context.Context, whereClause string, baseArgs ...interface{}) (string, []interface{}) {
	entityClause, entityArgs := limitEntityScope(ctx, "l", len(baseArgs)+1)
	query := `
		SELECT f.file_id, f.stored_file_name, f.content_type, f.file_size, f.upload_s3_key, f.uploaded_by, f.uploaded_at
		FROM cimplrcorpsaas.bank_limit_utilization_files f
		JOIN cimplrcorpsaas.bank_limit_utilization u ON u.utilization_id = f.utilization_id
		JOIN cimplrcorpsaas.bank_limit l ON l.limit_id = u.limit_id
	` + whereClause + entityClause

	args := append([]interface{}{}, baseArgs...)
	args = append(args, entityArgs...)
	return query, args
}

func limitEntityScope(ctx context.Context, alias string, position int) (string, []interface{}) {
	names := trimLimitAdditionalFileIDs(api.GetEntityNamesFromCtx(ctx))
	if len(names) == 0 {
		return " AND FALSE", nil
	}
	return fmt.Sprintf(" AND %s.entity_name = ANY($%d)", alias, position), []interface{}{names}
}

func trimLimitAdditionalFileIDs(fileIDs []string) []string {
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

func missingLimitAdditionalFileIDs(expected []string, files []additionalfiles.FileRecord) []string {
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
