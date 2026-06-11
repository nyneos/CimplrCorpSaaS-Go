package exposures

import (
	api "CimplrCorpSaas/api"
	"CimplrCorpSaas/api/cash/additionalfiles"
	"CimplrCorpSaas/api/constants"
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

type fxAdditionalFileExec interface {
	Exec(context.Context, string, ...interface{}) (pgconn.CommandTag, error)
}

func ListAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewListHandler(pool, exposureAdditionalFilesConfig())
}

func UploadAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewUploadHandler(pool, exposureAdditionalFilesConfig())
}

func DownloadAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDownloadHandler(pool, exposureAdditionalFilesConfig())
}

func DownloadSelectedAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDownloadSelectedHandler(pool, exposureAdditionalFilesConfig())
}

func DownloadExposurePackageZipHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewPackageZipHandler(pool, exposureAdditionalFilesConfig(), additionalfiles.PackageZipOptions{
		ModuleLabel: "FX Exposure",
		IDField:     "exposure_header_id",
		LoadMain:    loadExposureMainPackageFile,
	})
}

func DeleteAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDeleteHandler(pool, exposureAdditionalFilesConfig())
}

func AuditAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewAuditHandler(pool, exposureAdditionalFilesConfig())
}

func ApproveAdditionalFileDeleteHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewApproveDeleteHandler(pool, exposureAdditionalFilesConfig())
}

func RejectAdditionalFileDeleteHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewRejectDeleteHandler(pool, exposureAdditionalFilesConfig())
}

func exposureAdditionalFilesConfig() additionalfiles.Config {
	return withExposureCrossStage(additionalfiles.Config{
		Module:                "fx-exposure",
		AuditSource:           "FX_EXPOSURE",
		AuditTableName:        fxAdditionalFileAuditTable,
		ParentIDField:         "exposure_header_id",
		List:                  listExposureAdditionalFiles,
		CreateReturning:       createExposureAdditionalFile,
		GetOne:                getExposureAdditionalFile,
		GetAnyFile:            getAnyExposureAdditionalFile,
		GetMany:               getExposureAdditionalFiles,
		SoftDelete:            deleteExposureAdditionalFile,
		SoftDeleteTx:          deleteExposureAdditionalFileTx,
		RecordMainUploadAudit: recordExposureMainUploadAudit,
	})
}

func recordExposureMainUploadAudit(ctx context.Context, tx pgx.Tx, parentID string, payload additionalfiles.MainUploadAuditPayload) error {
	return additionalfiles.InsertMainUploadAudit(ctx, tx, "public.auditactionexposure", "exposure_header_id", "actiontype", parentID, payload)
}

func loadExposureMainPackageFile(ctx context.Context, pool *pgxpool.Pool, rowID string) (*additionalfiles.MainPackageFile, error) {
	names, err := fxEntityNames(ctx)
	if err != nil {
		return nil, err
	}
	var uploadS3Key string
	err = pool.QueryRow(ctx, `
		SELECT COALESCE(upload_s3_key, '')
		FROM public.exposure_headers
		WHERE exposure_header_id::text = $1
		  AND LOWER(TRIM(entity)) = ANY($2)
	`, rowID, names).Scan(&uploadS3Key)
	if err != nil || strings.TrimSpace(uploadS3Key) == "" {
		return nil, err
	}
	return &additionalfiles.MainPackageFile{UploadS3Key: uploadS3Key}, nil
}

func listExposureAdditionalFiles(ctx context.Context, pool *pgxpool.Pool, parentID string) ([]additionalfiles.FileRecord, error) {
	names, err := fxEntityNames(ctx)
	if err != nil {
		return nil, err
	}
	return additionalfiles.QueryFiles(ctx, pool, exposureFileQuery(`
		WHERE f.exposure_header_id::text = $1
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		  AND LOWER(TRIM(h.entity)) = ANY($2)
		ORDER BY f.uploaded_at DESC
	`), parentID, names)
}

func createExposureAdditionalFile(ctx context.Context, tx pgx.Tx, input additionalfiles.CreateInput) (string, error) {
	names, err := fxEntityNames(ctx)
	if err != nil {
		return "", err
	}
	return additionalfiles.InsertAdditionalFileRowReturningID(ctx, tx, "public.exposure_header_files", "exposure_header_id", input, `
		SELECT h.exposure_header_id AS parent_id
		FROM public.exposure_headers h
		WHERE h.exposure_header_id::text = $8
		  AND LOWER(TRIM(h.entity)) = ANY($9)
	`, input.ParentID, names)
}

func getExposureAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*additionalfiles.FileRecord, error) {
	return getExposureAdditionalFileWithDeleted(ctx, pool, parentID, fileID, false)
}

func getAnyExposureAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*additionalfiles.FileRecord, error) {
	return getExposureAdditionalFileWithDeleted(ctx, pool, parentID, fileID, true)
}

func getExposureAdditionalFileWithDeleted(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string, includeDeleted bool) (*additionalfiles.FileRecord, error) {
	names, err := fxEntityNames(ctx)
	if err != nil {
		return nil, err
	}
	deletedClause := constants.ErrFDReceiptDeletedFilter
	if includeDeleted {
		deletedClause = ""
	}
	return additionalfiles.FirstFile(ctx, pool, exposureFileQuery(`
		WHERE f.exposure_header_id::text = $1
		  AND f.file_id::text = $2
		  `+deletedClause+`
		  AND LOWER(TRIM(h.entity)) = ANY($3)
	`), parentID, fileID, names)
}

func getExposureAdditionalFiles(ctx context.Context, pool *pgxpool.Pool, parentID string, fileIDs []string) ([]additionalfiles.FileRecord, []string, error) {
	names, err := fxEntityNames(ctx)
	if err != nil {
		return nil, nil, err
	}
	trimmedIDs := trimFXAdditionalFileIDs(fileIDs)
	files, queryErr := additionalfiles.QueryFiles(ctx, pool, exposureFileQuery(`
		WHERE f.exposure_header_id::text = $1
		  AND f.file_id::text = ANY($2)
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		  AND LOWER(TRIM(h.entity)) = ANY($3)
		ORDER BY f.uploaded_at DESC
	`), parentID, trimmedIDs, names)
	if queryErr != nil {
		return nil, nil, queryErr
	}
	return files, missingFXAdditionalFileIDs(trimmedIDs, files), nil
}

func deleteExposureAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	return deleteExposureAdditionalFileExec(ctx, pool, parentID, fileID, deletedBy, deletedAt)
}

func deleteExposureAdditionalFileTx(ctx context.Context, tx pgx.Tx, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	return deleteExposureAdditionalFileExec(ctx, tx, parentID, fileID, deletedBy, deletedAt)
}

func deleteExposureAdditionalFileExec(ctx context.Context, exec fxAdditionalFileExec, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	names, err := fxEntityNames(ctx)
	if err != nil {
		return false, err
	}
	result, execErr := exec.Exec(ctx, `
		UPDATE public.exposure_header_files f
		SET is_deleted = TRUE,
		    deleted_by = $3,
		    deleted_at = $4
		FROM public.exposure_headers h
		WHERE f.exposure_header_id::text = $1
		  AND f.file_id::text = $2
		  AND h.exposure_header_id = f.exposure_header_id
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		  AND LOWER(TRIM(h.entity)) = ANY($5)
	`, parentID, fileID, deletedBy, deletedAt, names)
	if execErr != nil {
		return false, execErr
	}
	return result.RowsAffected() > 0, nil
}

func exposureFileQuery(whereClause string) string {
	return `
		SELECT f.file_id, f.stored_file_name, f.content_type, f.file_size, f.upload_s3_key, f.uploaded_by, f.uploaded_at
		FROM public.exposure_header_files f
		JOIN public.exposure_headers h ON h.exposure_header_id = f.exposure_header_id
	` + whereClause
}

func fxEntityNames(ctx context.Context) ([]string, error) {
	names := api.GetEntityNamesFromCtx(ctx)
	lowered := make([]string, 0, len(names))
	for _, name := range names {
		if trimmed := strings.ToLower(strings.TrimSpace(name)); trimmed != "" {
			lowered = append(lowered, trimmed)
		}
	}
	if len(lowered) == 0 {
		return nil, errors.New(constants.ErrNoAccessibleBusinessUnit)
	}
	return lowered, nil
}

func trimFXAdditionalFileIDs(fileIDs []string) []string {
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

func missingFXAdditionalFileIDs(expected []string, files []additionalfiles.FileRecord) []string {
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
