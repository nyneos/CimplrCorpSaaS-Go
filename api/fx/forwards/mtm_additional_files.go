package forwards

import (
	"CimplrCorpSaas/api/cash/additionalfiles"
	"CimplrCorpSaas/api/constants"
	"context"
	"encoding/base64"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func ListMTMAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewListHandler(pool, mtmAdditionalFilesConfig())
}

func UploadMTMAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewUploadHandler(pool, mtmAdditionalFilesConfig())
}

func DownloadMTMAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDownloadHandler(pool, mtmAdditionalFilesConfig())
}

func DownloadSelectedMTMAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDownloadSelectedHandler(pool, mtmAdditionalFilesConfig())
}

func DownloadMTMPackageZipHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewPackageZipHandler(pool, mtmAdditionalFilesConfig(), additionalfiles.PackageZipOptions{
		ModuleLabel: "FX MTM",
		IDField:     "mtm_id",
		LoadMain:    loadMTMMainPackageFile,
	})
}

func DeleteMTMAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDeleteHandler(pool, mtmAdditionalFilesConfig())
}

func AuditMTMAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewAuditHandler(pool, mtmAdditionalFilesConfig())
}

func ApproveMTMAdditionalFileDeleteHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewApproveDeleteHandler(pool, mtmAdditionalFilesConfig())
}

func RejectMTMAdditionalFileDeleteHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewRejectDeleteHandler(pool, mtmAdditionalFilesConfig())
}

func mtmAdditionalFilesConfig() additionalfiles.Config {
	return withForwardCrossStage(additionalfiles.Config{
		Module:                "fx-mtm",
		AuditSource:           "FX_FORWARD_MTM",
		AuditTableName:        fxAdditionalFileAuditTable,
		ParentIDField:         "mtm_id",
		List:                  listMTMAdditionalFiles,
		CreateReturning:       createMTMAdditionalFile,
		GetOne:                getMTMAdditionalFile,
		GetAnyFile:            getAnyMTMAdditionalFile,
		GetMany:               getMTMAdditionalFiles,
		SoftDelete:            deleteMTMAdditionalFile,
		SoftDeleteTx:          deleteMTMAdditionalFileTx,
		RecordMainUploadAudit: recordMTMMainUploadAudit,
	})
}

func recordMTMMainUploadAudit(ctx context.Context, tx pgx.Tx, parentID string, payload additionalfiles.MainUploadAuditPayload) error {
	return additionalfiles.InsertMainUploadAudit(ctx, tx, "public.auditactionforwardmtm", "mtm_id", "actiontype", parentID, payload)
}

func loadMTMMainPackageFile(ctx context.Context, pool *pgxpool.Pool, rowID string) (*additionalfiles.MainPackageFile, error) {
	names, err := forwardEntityNames(ctx)
	if err != nil {
		return nil, err
	}
	mtmID := normalizeMTMID(rowID)
	var uploadS3Key string
	err = pool.QueryRow(ctx, `
		SELECT COALESCE(upload_s3_key, '')
		FROM public.forward_mtm
		WHERE mtm_id = $1
		  AND LOWER(TRIM(entity)) = ANY($2)
	`, mtmID, names).Scan(&uploadS3Key)
	if err != nil || strings.TrimSpace(uploadS3Key) == "" {
		return nil, err
	}
	return &additionalfiles.MainPackageFile{UploadS3Key: uploadS3Key}, nil
}

func listMTMAdditionalFiles(ctx context.Context, pool *pgxpool.Pool, parentID string) ([]additionalfiles.FileRecord, error) {
	names, err := forwardEntityNames(ctx)
	if err != nil {
		return nil, err
	}
	parentID = normalizeMTMID(parentID)
	return additionalfiles.QueryFiles(ctx, pool, mtmFileQuery(`
		WHERE f.mtm_id = $1
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		  AND LOWER(TRIM(m.entity)) = ANY($2)
		ORDER BY f.uploaded_at DESC
	`), parentID, names)
}

func createMTMAdditionalFile(ctx context.Context, tx pgx.Tx, input additionalfiles.CreateInput) (string, error) {
	names, err := forwardEntityNames(ctx)
	if err != nil {
		return "", err
	}
	input.ParentID = normalizeMTMID(input.ParentID)
	return additionalfiles.InsertAdditionalFileRowReturningID(ctx, tx, "public.forward_mtm_files", "mtm_id", input, `
		SELECT m.mtm_id AS parent_id
		FROM public.forward_mtm m
		WHERE m.mtm_id = $8
		  AND LOWER(TRIM(m.entity)) = ANY($9)
	`, input.ParentID, names)
}

func getMTMAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*additionalfiles.FileRecord, error) {
	return getMTMAdditionalFileWithDeleted(ctx, pool, parentID, fileID, false)
}

func getAnyMTMAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*additionalfiles.FileRecord, error) {
	return getMTMAdditionalFileWithDeleted(ctx, pool, parentID, fileID, true)
}

func getMTMAdditionalFileWithDeleted(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string, includeDeleted bool) (*additionalfiles.FileRecord, error) {
	names, err := forwardEntityNames(ctx)
	if err != nil {
		return nil, err
	}
	parentID = normalizeMTMID(parentID)
	deletedClause := constants.ErrFDReceiptDeletedFilter
	if includeDeleted {
		deletedClause = ""
	}
	return additionalfiles.FirstFile(ctx, pool, mtmFileQuery(`
		WHERE f.mtm_id = $1
		  AND f.file_id::text = $2
		  `+deletedClause+`
		  AND LOWER(TRIM(m.entity)) = ANY($3)
	`), parentID, fileID, names)
}

func getMTMAdditionalFiles(ctx context.Context, pool *pgxpool.Pool, parentID string, fileIDs []string) ([]additionalfiles.FileRecord, []string, error) {
	names, err := forwardEntityNames(ctx)
	if err != nil {
		return nil, nil, err
	}
	parentID = normalizeMTMID(parentID)
	trimmedIDs := trimForwardAdditionalFileIDs(fileIDs)
	files, queryErr := additionalfiles.QueryFiles(ctx, pool, mtmFileQuery(`
		WHERE f.mtm_id = $1
		  AND f.file_id::text = ANY($2)
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		  AND LOWER(TRIM(m.entity)) = ANY($3)
		ORDER BY f.uploaded_at DESC
	`), parentID, trimmedIDs, names)
	if queryErr != nil {
		return nil, nil, queryErr
	}
	return files, missingForwardAdditionalFileIDs(trimmedIDs, files), nil
}

func deleteMTMAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	return deleteMTMAdditionalFileExec(ctx, pool, parentID, fileID, deletedBy, deletedAt)
}

func deleteMTMAdditionalFileTx(ctx context.Context, tx pgx.Tx, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	return deleteMTMAdditionalFileExec(ctx, tx, parentID, fileID, deletedBy, deletedAt)
}

func deleteMTMAdditionalFileExec(ctx context.Context, exec forwardAdditionalFileExec, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	names, err := forwardEntityNames(ctx)
	if err != nil {
		return false, err
	}
	parentID = normalizeMTMID(parentID)
	result, execErr := exec.Exec(ctx, `
		UPDATE public.forward_mtm_files f
		SET is_deleted = TRUE,
		    deleted_by = $3,
		    deleted_at = $4
		FROM public.forward_mtm m
		WHERE f.mtm_id = $1
		  AND f.file_id::text = $2
		  AND m.mtm_id = f.mtm_id
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		  AND LOWER(TRIM(m.entity)) = ANY($5)
	`, parentID, fileID, deletedBy, deletedAt, names)
	if execErr != nil {
		return false, execErr
	}
	return result.RowsAffected() > 0, nil
}

func mtmFileQuery(whereClause string) string {
	return `
		SELECT f.file_id, f.stored_file_name, f.content_type, f.file_size, f.upload_s3_key, f.uploaded_by, f.uploaded_at
		FROM public.forward_mtm_files f
		JOIN public.forward_mtm m ON m.mtm_id = f.mtm_id
	` + whereClause
}

func normalizeMTMID(mtmID string) string {
	mtmID = strings.TrimSpace(mtmID)
	if mtmID == "" {
		return ""
	}
	if decoded, err := base64.StdEncoding.DecodeString(mtmID); err == nil {
		if decodedID := strings.TrimSpace(string(decoded)); decodedID != "" {
			return decodedID
		}
	}
	return mtmID
}
