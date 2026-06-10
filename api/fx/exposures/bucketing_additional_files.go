package exposures

import (
	"CimplrCorpSaas/api/cash/additionalfiles"
	"CimplrCorpSaas/api/constants"
	"context"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func ListExposureBucketingAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewListHandler(pool, exposureBucketingAdditionalFilesConfig())
}

func UploadExposureBucketingAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewUploadHandler(pool, exposureBucketingAdditionalFilesConfig())
}

func DownloadExposureBucketingAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDownloadHandler(pool, exposureBucketingAdditionalFilesConfig())
}

func DownloadSelectedExposureBucketingAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDownloadSelectedHandler(pool, exposureBucketingAdditionalFilesConfig())
}

func DeleteExposureBucketingAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDeleteHandler(pool, exposureBucketingAdditionalFilesConfig())
}

func AuditExposureBucketingAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewAuditHandler(pool, exposureBucketingAdditionalFilesConfig())
}

func ApproveExposureBucketingAdditionalFileDeleteHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewApproveDeleteHandler(pool, exposureBucketingAdditionalFilesConfig())
}

func RejectExposureBucketingAdditionalFileDeleteHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewRejectDeleteHandler(pool, exposureBucketingAdditionalFilesConfig())
}

func exposureBucketingAdditionalFilesConfig() additionalfiles.Config {
	return additionalfiles.Config{
		Module:                "fx-exposure-bucketing",
		AuditSource:           "FX_EXPOSURE_BUCKETING",
		AuditTableName:        fxAdditionalFileAuditTable,
		ParentIDField:         "exposure_header_id",
		List:                  listExposureBucketingAdditionalFiles,
		CreateReturning:       createExposureBucketingAdditionalFile,
		GetOne:                getExposureBucketingAdditionalFile,
		GetAnyFile:            getAnyExposureBucketingAdditionalFile,
		GetMany:               getExposureBucketingAdditionalFiles,
		SoftDelete:            deleteExposureBucketingAdditionalFile,
		SoftDeleteTx:          deleteExposureBucketingAdditionalFileTx,
		RecordMainUploadAudit: recordExposureBucketingMainUploadAudit,
	}
}

func recordExposureBucketingMainUploadAudit(ctx context.Context, tx pgx.Tx, parentID string, payload additionalfiles.MainUploadAuditPayload) error {
	return additionalfiles.InsertMainUploadAudit(ctx, tx, "public.auditactionexposurebucketing", "exposure_header_id", "actiontype", parentID, payload)
}

func listExposureBucketingAdditionalFiles(ctx context.Context, pool *pgxpool.Pool, parentID string) ([]additionalfiles.FileRecord, error) {
	names, err := fxEntityNames(ctx)
	if err != nil {
		return nil, err
	}
	return additionalfiles.QueryFiles(ctx, pool, exposureBucketingFileQuery(`
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

func createExposureBucketingAdditionalFile(ctx context.Context, tx pgx.Tx, input additionalfiles.CreateInput) (string, error) {
	names, err := fxEntityNames(ctx)
	if err != nil {
		return "", err
	}
	return additionalfiles.InsertAdditionalFileRowReturningID(ctx, tx, "public.exposure_bucketing_files", "exposure_header_id", input, `
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

func getExposureBucketingAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*additionalfiles.FileRecord, error) {
	return getExposureBucketingAdditionalFileWithDeleted(ctx, pool, parentID, fileID, false)
}

func getAnyExposureBucketingAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*additionalfiles.FileRecord, error) {
	return getExposureBucketingAdditionalFileWithDeleted(ctx, pool, parentID, fileID, true)
}

func getExposureBucketingAdditionalFileWithDeleted(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string, includeDeleted bool) (*additionalfiles.FileRecord, error) {
	names, err := fxEntityNames(ctx)
	if err != nil {
		return nil, err
	}
	deletedClause := constants.ErrFDReceiptDeletedFilter
	if includeDeleted {
		deletedClause = ""
	}
	return additionalfiles.FirstFile(ctx, pool, exposureBucketingFileQuery(`
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

func getExposureBucketingAdditionalFiles(ctx context.Context, pool *pgxpool.Pool, parentID string, fileIDs []string) ([]additionalfiles.FileRecord, []string, error) {
	names, err := fxEntityNames(ctx)
	if err != nil {
		return nil, nil, err
	}
	trimmedIDs := trimFXAdditionalFileIDs(fileIDs)
	files, queryErr := additionalfiles.QueryFiles(ctx, pool, exposureBucketingFileQuery(`
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

func deleteExposureBucketingAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	return deleteExposureBucketingAdditionalFileExec(ctx, pool, parentID, fileID, deletedBy, deletedAt)
}

func deleteExposureBucketingAdditionalFileTx(ctx context.Context, tx pgx.Tx, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	return deleteExposureBucketingAdditionalFileExec(ctx, tx, parentID, fileID, deletedBy, deletedAt)
}

func deleteExposureBucketingAdditionalFileExec(ctx context.Context, exec fxAdditionalFileExec, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	names, err := fxEntityNames(ctx)
	if err != nil {
		return false, err
	}
	result, execErr := exec.Exec(ctx, `
		UPDATE public.exposure_bucketing_files f
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

func exposureBucketingFileQuery(whereClause string) string {
	return `
		SELECT f.file_id, f.stored_file_name, f.content_type, f.file_size, f.upload_s3_key, f.uploaded_by, f.uploaded_at
		FROM public.exposure_bucketing_files f
		JOIN public.exposure_headers h ON h.exposure_header_id = f.exposure_header_id
	` + whereClause
}
