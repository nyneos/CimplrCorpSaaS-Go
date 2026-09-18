package exposures

import (
	"CimplrCorpSaas/api/cash/additionalfiles"
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/api/fx/auditutil"
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const exposureSettlementFilesTable = "public.exposure_settlement_files"

func ListExposureSettlementAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewListHandler(pool, exposureSettlementAdditionalFilesConfig(pool))
}

func UploadExposureSettlementAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewUploadHandler(pool, exposureSettlementAdditionalFilesConfig(pool))
}

func DownloadExposureSettlementAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDownloadHandler(pool, exposureSettlementAdditionalFilesConfig(pool))
}

func DownloadSelectedExposureSettlementAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDownloadSelectedHandler(pool, exposureSettlementAdditionalFilesConfig(pool))
}

func DownloadExposureSettlementPackageZipHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewPackageZipHandler(pool, exposureSettlementAdditionalFilesConfig(pool), additionalfiles.PackageZipOptions{
		ModuleLabel: "Settlement",
		IDField:     "settlement_id",
	})
}

func DeleteExposureSettlementAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDeleteHandler(pool, exposureSettlementAdditionalFilesConfig(pool))
}

func AuditExposureSettlementAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewAuditHandler(pool, exposureSettlementAdditionalFilesConfig(pool))
}

func ApproveExposureSettlementAdditionalFileDeleteHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewApproveDeleteHandler(pool, exposureSettlementAdditionalFilesConfig(pool))
}

func RejectExposureSettlementAdditionalFileDeleteHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewRejectDeleteHandler(pool, exposureSettlementAdditionalFilesConfig(pool))
}

func exposureSettlementAdditionalFilesConfig(pool *pgxpool.Pool) additionalfiles.Config {
	return additionalfiles.Config{
		Module:                "fx-exposure-settlement",
		AuditSource:           "FX_EXPOSURE_SETTLEMENT",
		AuditTableName:        fxAdditionalFileAuditTable,
		ParentIDField:         "settlement_id",
		List:                  listExposureSettlementAdditionalFiles,
		CreateReturning:       createExposureSettlementAdditionalFile,
		GetOne:                getExposureSettlementAdditionalFile,
		GetAnyFile:            getAnyExposureSettlementAdditionalFile,
		GetMany:               getExposureSettlementAdditionalFiles,
		SoftDelete:            deleteExposureSettlementAdditionalFile,
		SoftDeleteTx:          deleteExposureSettlementAdditionalFileTx,
		RecordMainUploadAudit: recordExposureSettlementMainUploadAudit(pool),
	}
}

func recordExposureSettlementMainUploadAudit(pool *pgxpool.Pool) func(context.Context, pgx.Tx, string, additionalfiles.MainUploadAuditPayload) error {
	return func(ctx context.Context, _ pgx.Tx, parentID string, payload additionalfiles.MainUploadAuditPayload) error {
		reason, err := additionalfiles.MainUploadAuditReasonJSON(payload)
		if err != nil {
			return err
		}
		auditutil.RecordActionPGX(ctx, pool, auditutil.ActionParams{
			TableName:    auditutil.TableExposureSettlement,
			ParentColumn: "settlement_id",
			ParentID:     parentID,
			ActionType:   "UPLOAD_FILE",
			Status:       constants.StatusApproved,
			Reason:       reason,
			RequestedBy:  payload.UploadedBy,
			RequestedIP:  payload.RequestedIP,
		})
		return nil
	}
}

func exposureSettlementFileQuery(whereClause string) string {
	return `
		SELECT f.file_id, f.stored_file_name, f.content_type, f.file_size, f.upload_s3_key, f.uploaded_by, f.uploaded_at
		FROM ` + exposureSettlementFilesTable + ` f
	` + whereClause
}

func listExposureSettlementAdditionalFiles(ctx context.Context, pool *pgxpool.Pool, parentID string) ([]additionalfiles.FileRecord, error) {
	return additionalfiles.QueryFiles(ctx, pool, exposureSettlementFileQuery(`
		WHERE f.settlement_id::text = $1
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		ORDER BY f.uploaded_at DESC
	`), strings.TrimSpace(parentID))
}

func createExposureSettlementAdditionalFile(ctx context.Context, tx pgx.Tx, input additionalfiles.CreateInput) (string, error) {
	return additionalfiles.InsertAdditionalFileRowReturningID(ctx, tx, exposureSettlementFilesTable, "settlement_id", input, `
		SELECT d.settlement_id AS parent_id
		FROM public.exposure_settlement_document d
		WHERE d.settlement_id::text = $8
		  AND COALESCE(d.is_deleted, FALSE) = FALSE
	`, strings.TrimSpace(input.ParentID))
}

func getExposureSettlementAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*additionalfiles.FileRecord, error) {
	return getExposureSettlementAdditionalFileWithDeleted(ctx, pool, parentID, fileID, false)
}

func getAnyExposureSettlementAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*additionalfiles.FileRecord, error) {
	return getExposureSettlementAdditionalFileWithDeleted(ctx, pool, parentID, fileID, true)
}

func getExposureSettlementAdditionalFileWithDeleted(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string, includeDeleted bool) (*additionalfiles.FileRecord, error) {
	deletedClause := constants.ErrFDReceiptDeletedFilter
	if includeDeleted {
		deletedClause = ""
	}
	return additionalfiles.FirstFile(ctx, pool, exposureSettlementFileQuery(`
		WHERE f.settlement_id::text = $1
		  AND f.file_id::text = $2
		  `+deletedClause+`
	`), strings.TrimSpace(parentID), strings.TrimSpace(fileID))
}

func getExposureSettlementAdditionalFiles(ctx context.Context, pool *pgxpool.Pool, parentID string, fileIDs []string) ([]additionalfiles.FileRecord, []string, error) {
	trimmedIDs := trimFXAdditionalFileIDs(fileIDs)
	files, err := additionalfiles.QueryFiles(ctx, pool, exposureSettlementFileQuery(`
		WHERE f.settlement_id::text = $1
		  AND f.file_id::text = ANY($2)
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		ORDER BY f.uploaded_at DESC
	`), strings.TrimSpace(parentID), trimmedIDs)
	if err != nil {
		return nil, nil, err
	}
	return files, missingFXAdditionalFileIDs(trimmedIDs, files), nil
}

func deleteExposureSettlementAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	return deleteExposureSettlementAdditionalFileExec(ctx, pool, parentID, fileID, deletedBy, deletedAt)
}

func deleteExposureSettlementAdditionalFileTx(ctx context.Context, tx pgx.Tx, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	return deleteExposureSettlementAdditionalFileExec(ctx, tx, parentID, fileID, deletedBy, deletedAt)
}

func deleteExposureSettlementAdditionalFileExec(ctx context.Context, exec fxAdditionalFileExec, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	result, err := exec.Exec(ctx, `
		UPDATE `+exposureSettlementFilesTable+`
		SET is_deleted = TRUE,
		    deleted_by = $3,
		    deleted_at = $4
		WHERE settlement_id::text = $1
		  AND file_id::text = $2
		  AND COALESCE(is_deleted, FALSE) = FALSE
	`, strings.TrimSpace(parentID), strings.TrimSpace(fileID), deletedBy, deletedAt)
	if err != nil {
		return false, err
	}
	return result.RowsAffected() > 0, nil
}
