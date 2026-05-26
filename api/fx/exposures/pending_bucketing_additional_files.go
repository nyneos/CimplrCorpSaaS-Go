package exposures

import (
	"CimplrCorpSaas/api/cash/additionalfiles"
	"context"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func ListPendingExposureBucketingAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewListHandler(pool, pendingExposureBucketingAdditionalFilesConfig())
}

func UploadPendingExposureBucketingAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewUploadHandler(pool, pendingExposureBucketingAdditionalFilesConfig())
}

func DownloadPendingExposureBucketingAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDownloadHandler(pool, pendingExposureBucketingAdditionalFilesConfig())
}

func DownloadSelectedPendingExposureBucketingAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDownloadSelectedHandler(pool, pendingExposureBucketingAdditionalFilesConfig())
}

func DeletePendingExposureBucketingAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDeleteHandler(pool, pendingExposureBucketingAdditionalFilesConfig())
}

func AuditPendingExposureBucketingAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewAuditHandler(pool, pendingExposureBucketingAdditionalFilesConfig())
}

func ApprovePendingExposureBucketingAdditionalFileDeleteHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewApproveDeleteHandler(pool, pendingExposureBucketingAdditionalFilesConfig())
}

func RejectPendingExposureBucketingAdditionalFileDeleteHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewRejectDeleteHandler(pool, pendingExposureBucketingAdditionalFilesConfig())
}

func pendingExposureBucketingAdditionalFilesConfig() additionalfiles.Config {
	return additionalfiles.Config{
		Module:                "fx-pending-exposure-bucketing",
		AuditSource:           "FX_EXPOSURE_BUCKETING",
		AuditTableName:        fxAdditionalFileAuditTable,
		ParentIDField:         "exposure_header_id",
		List:                  listPendingExposureBucketingAdditionalFiles,
		CreateReturning:       createPendingExposureBucketingAdditionalFile,
		GetOne:                getPendingExposureBucketingAdditionalFile,
		GetAnyFile:            getAnyPendingExposureBucketingAdditionalFile,
		GetMany:               getPendingExposureBucketingAdditionalFiles,
		SoftDelete:            deletePendingExposureBucketingAdditionalFile,
		SoftDeleteTx:          deletePendingExposureBucketingAdditionalFileTx,
		RecordMainUploadAudit: recordExposureBucketingMainUploadAudit,
	}
}

func listPendingExposureBucketingAdditionalFiles(ctx context.Context, pool *pgxpool.Pool, parentID string) ([]additionalfiles.FileRecord, error) {
	names, err := fxEntityNames(ctx)
	if err != nil {
		return nil, err
	}
	return additionalfiles.QueryFiles(ctx, pool, pendingExposureBucketingFileQuery(`
		WHERE f.exposure_header_id::text = $1
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		  AND LOWER(TRIM(h.entity)) = ANY($2)
		  AND EXISTS (
		      SELECT 1
		      FROM public.exposure_bucketing b
		      WHERE b.exposure_header_id = f.exposure_header_id
		  )
		ORDER BY f.uploaded_at DESC
	`), parentID, names)
}

func createPendingExposureBucketingAdditionalFile(ctx context.Context, tx pgx.Tx, input additionalfiles.CreateInput) (string, error) {
	names, err := fxEntityNames(ctx)
	if err != nil {
		return "", err
	}
	return additionalfiles.InsertAdditionalFileRowReturningID(ctx, tx, "public.pending_exposure_bucketing_files", "exposure_header_id", input, `
		SELECT h.exposure_header_id AS parent_id
		FROM public.exposure_headers h
		WHERE h.exposure_header_id::text = $8
		  AND LOWER(TRIM(h.entity)) = ANY($9)
		  AND EXISTS (
		      SELECT 1
		      FROM public.exposure_bucketing b
		      WHERE b.exposure_header_id = h.exposure_header_id
		  )
	`, input.ParentID, names)
}

func getPendingExposureBucketingAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*additionalfiles.FileRecord, error) {
	return getPendingExposureBucketingAdditionalFileWithDeleted(ctx, pool, parentID, fileID, false)
}

func getAnyPendingExposureBucketingAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*additionalfiles.FileRecord, error) {
	return getPendingExposureBucketingAdditionalFileWithDeleted(ctx, pool, parentID, fileID, true)
}

func getPendingExposureBucketingAdditionalFileWithDeleted(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string, includeDeleted bool) (*additionalfiles.FileRecord, error) {
	names, err := fxEntityNames(ctx)
	if err != nil {
		return nil, err
	}
	deletedClause := "AND COALESCE(f.is_deleted, FALSE) = FALSE"
	if includeDeleted {
		deletedClause = ""
	}
	return additionalfiles.FirstFile(ctx, pool, pendingExposureBucketingFileQuery(`
		WHERE f.exposure_header_id::text = $1
		  AND f.file_id::text = $2
		  `+deletedClause+`
		  AND LOWER(TRIM(h.entity)) = ANY($3)
		  AND EXISTS (
		      SELECT 1
		      FROM public.exposure_bucketing b
		      WHERE b.exposure_header_id = f.exposure_header_id
		  )
	`), parentID, fileID, names)
}

func getPendingExposureBucketingAdditionalFiles(ctx context.Context, pool *pgxpool.Pool, parentID string, fileIDs []string) ([]additionalfiles.FileRecord, []string, error) {
	names, err := fxEntityNames(ctx)
	if err != nil {
		return nil, nil, err
	}
	trimmedIDs := trimFXAdditionalFileIDs(fileIDs)
	files, queryErr := additionalfiles.QueryFiles(ctx, pool, pendingExposureBucketingFileQuery(`
		WHERE f.exposure_header_id::text = $1
		  AND f.file_id::text = ANY($2)
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		  AND LOWER(TRIM(h.entity)) = ANY($3)
		  AND EXISTS (
		      SELECT 1
		      FROM public.exposure_bucketing b
		      WHERE b.exposure_header_id = f.exposure_header_id
		  )
		ORDER BY f.uploaded_at DESC
	`), parentID, trimmedIDs, names)
	if queryErr != nil {
		return nil, nil, queryErr
	}
	return files, missingFXAdditionalFileIDs(trimmedIDs, files), nil
}

func deletePendingExposureBucketingAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	return deletePendingExposureBucketingAdditionalFileExec(ctx, pool, parentID, fileID, deletedBy, deletedAt)
}

func deletePendingExposureBucketingAdditionalFileTx(ctx context.Context, tx pgx.Tx, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	return deletePendingExposureBucketingAdditionalFileExec(ctx, tx, parentID, fileID, deletedBy, deletedAt)
}

func deletePendingExposureBucketingAdditionalFileExec(ctx context.Context, exec fxAdditionalFileExec, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	names, err := fxEntityNames(ctx)
	if err != nil {
		return false, err
	}
	result, execErr := exec.Exec(ctx, `
		UPDATE public.pending_exposure_bucketing_files f
		SET is_deleted = TRUE,
		    deleted_by = $3,
		    deleted_at = $4
		FROM public.exposure_headers h
		WHERE f.exposure_header_id::text = $1
		  AND f.file_id::text = $2
		  AND h.exposure_header_id = f.exposure_header_id
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		  AND LOWER(TRIM(h.entity)) = ANY($5)
		  AND EXISTS (
		      SELECT 1
		      FROM public.exposure_bucketing b
		      WHERE b.exposure_header_id = f.exposure_header_id
		  )
	`, parentID, fileID, deletedBy, deletedAt, names)
	if execErr != nil {
		return false, execErr
	}
	return result.RowsAffected() > 0, nil
}

func pendingExposureBucketingFileQuery(whereClause string) string {
	return `
		SELECT f.file_id, f.stored_file_name, f.content_type, f.file_size, f.upload_s3_key, f.uploaded_by, f.uploaded_at
		FROM public.pending_exposure_bucketing_files f
		JOIN public.exposure_headers h ON h.exposure_header_id = f.exposure_header_id
	` + whereClause
}
