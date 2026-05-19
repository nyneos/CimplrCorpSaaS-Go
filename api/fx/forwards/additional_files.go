package forwards

import (
	api "CimplrCorpSaas/api"
	"CimplrCorpSaas/api/cash/additionalfiles"
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const fxAdditionalFileAuditTable = "cimplrcorpsaas.fx_additional_file_audit"

type forwardAdditionalFileExec interface {
	Exec(context.Context, string, ...interface{}) (pgconn.CommandTag, error)
}

func ListAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewListHandler(pool, forwardAdditionalFilesConfig())
}

func UploadAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewUploadHandler(pool, forwardAdditionalFilesConfig())
}

func DownloadAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDownloadHandler(pool, forwardAdditionalFilesConfig())
}

func DownloadSelectedAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDownloadSelectedHandler(pool, forwardAdditionalFilesConfig())
}

func DeleteAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDeleteHandler(pool, forwardAdditionalFilesConfig())
}

func AuditAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewAuditHandler(pool, forwardAdditionalFilesConfig())
}

func ApproveAdditionalFileDeleteHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewApproveDeleteHandler(pool, forwardAdditionalFilesConfig())
}

func RejectAdditionalFileDeleteHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewRejectDeleteHandler(pool, forwardAdditionalFilesConfig())
}

func forwardAdditionalFilesConfig() additionalfiles.Config {
	return additionalfiles.Config{
		Module:                "fx-forward",
		AuditSource:           "FX_FORWARD",
		AuditTableName:        fxAdditionalFileAuditTable,
		ParentIDField:         "system_transaction_id",
		List:                  listForwardAdditionalFiles,
		CreateReturning:       createForwardAdditionalFile,
		GetOne:                getForwardAdditionalFile,
		GetAnyFile:            getAnyForwardAdditionalFile,
		GetMany:               getForwardAdditionalFiles,
		SoftDelete:            deleteForwardAdditionalFile,
		SoftDeleteTx:          deleteForwardAdditionalFileTx,
		RecordMainUploadAudit: recordForwardMainUploadAudit,
	}
}

func recordForwardMainUploadAudit(ctx context.Context, tx pgx.Tx, parentID string, payload additionalfiles.MainUploadAuditPayload) error {
	return additionalfiles.InsertMainUploadAudit(ctx, tx, "public.auditactionforwardbooking", "system_transaction_id", "actiontype", parentID, payload)
}

func listForwardAdditionalFiles(ctx context.Context, pool *pgxpool.Pool, parentID string) ([]additionalfiles.FileRecord, error) {
	names, err := forwardEntityNames(ctx)
	if err != nil {
		return nil, err
	}
	return additionalfiles.QueryFiles(ctx, pool, forwardFileQuery(`
		WHERE f.system_transaction_id = $1
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		  AND LOWER(TRIM(b.entity_level_0)) = ANY($2)
		ORDER BY f.uploaded_at DESC
	`), parentID, names)
}

func createForwardAdditionalFile(ctx context.Context, tx pgx.Tx, input additionalfiles.CreateInput) (string, error) {
	names, err := forwardEntityNames(ctx)
	if err != nil {
		return "", err
	}
	return additionalfiles.InsertAdditionalFileRowReturningID(ctx, tx, "public.forward_booking_files", "system_transaction_id", input, `
		SELECT b.system_transaction_id AS parent_id
		FROM public.forward_bookings b
		WHERE b.system_transaction_id = $8
		  AND LOWER(TRIM(b.entity_level_0)) = ANY($9)
	`, input.ParentID, names)
}

func getForwardAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*additionalfiles.FileRecord, error) {
	return getForwardAdditionalFileWithDeleted(ctx, pool, parentID, fileID, false)
}

func getAnyForwardAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*additionalfiles.FileRecord, error) {
	return getForwardAdditionalFileWithDeleted(ctx, pool, parentID, fileID, true)
}

func getForwardAdditionalFileWithDeleted(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string, includeDeleted bool) (*additionalfiles.FileRecord, error) {
	names, err := forwardEntityNames(ctx)
	if err != nil {
		return nil, err
	}
	deletedClause := "AND COALESCE(f.is_deleted, FALSE) = FALSE"
	if includeDeleted {
		deletedClause = ""
	}
	return additionalfiles.FirstFile(ctx, pool, forwardFileQuery(`
		WHERE f.system_transaction_id = $1
		  AND f.file_id::text = $2
		  `+deletedClause+`
		  AND LOWER(TRIM(b.entity_level_0)) = ANY($3)
	`), parentID, fileID, names)
}

func getForwardAdditionalFiles(ctx context.Context, pool *pgxpool.Pool, parentID string, fileIDs []string) ([]additionalfiles.FileRecord, []string, error) {
	names, err := forwardEntityNames(ctx)
	if err != nil {
		return nil, nil, err
	}
	trimmedIDs := trimForwardAdditionalFileIDs(fileIDs)
	files, queryErr := additionalfiles.QueryFiles(ctx, pool, forwardFileQuery(`
		WHERE f.system_transaction_id = $1
		  AND f.file_id::text = ANY($2)
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		  AND LOWER(TRIM(b.entity_level_0)) = ANY($3)
		ORDER BY f.uploaded_at DESC
	`), parentID, trimmedIDs, names)
	if queryErr != nil {
		return nil, nil, queryErr
	}
	return files, missingForwardAdditionalFileIDs(trimmedIDs, files), nil
}

func deleteForwardAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	return deleteForwardAdditionalFileExec(ctx, pool, parentID, fileID, deletedBy, deletedAt)
}

func deleteForwardAdditionalFileTx(ctx context.Context, tx pgx.Tx, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	return deleteForwardAdditionalFileExec(ctx, tx, parentID, fileID, deletedBy, deletedAt)
}

func deleteForwardAdditionalFileExec(ctx context.Context, exec forwardAdditionalFileExec, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	names, err := forwardEntityNames(ctx)
	if err != nil {
		return false, err
	}
	result, execErr := exec.Exec(ctx, `
		UPDATE public.forward_booking_files f
		SET is_deleted = TRUE,
		    deleted_by = $3,
		    deleted_at = $4
		FROM public.forward_bookings b
		WHERE f.system_transaction_id = $1
		  AND f.file_id::text = $2
		  AND b.system_transaction_id = f.system_transaction_id
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		  AND LOWER(TRIM(b.entity_level_0)) = ANY($5)
	`, parentID, fileID, deletedBy, deletedAt, names)
	if execErr != nil {
		return false, execErr
	}
	return result.RowsAffected() > 0, nil
}

func forwardFileQuery(whereClause string) string {
	return `
		SELECT f.file_id, f.stored_file_name, f.content_type, f.file_size, f.upload_s3_key, f.uploaded_by, f.uploaded_at
		FROM public.forward_booking_files f
		JOIN public.forward_bookings b ON b.system_transaction_id = f.system_transaction_id
	` + whereClause
}

func forwardEntityNames(ctx context.Context) ([]string, error) {
	names := api.GetEntityNamesFromCtx(ctx)
	lowered := make([]string, 0, len(names))
	for _, name := range names {
		if trimmed := strings.ToLower(strings.TrimSpace(name)); trimmed != "" {
			lowered = append(lowered, trimmed)
		}
	}
	if len(lowered) == 0 {
		return nil, errors.New("no accessible business units found")
	}
	return lowered, nil
}

func trimForwardAdditionalFileIDs(fileIDs []string) []string {
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

func missingForwardAdditionalFileIDs(expected []string, files []additionalfiles.FileRecord) []string {
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
