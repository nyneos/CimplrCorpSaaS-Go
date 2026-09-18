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

const hedgingProposalFilesTable = "public.hedging_proposal_files"

func ListHedgingProposalAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewListHandler(pool, hedgingProposalAdditionalFilesConfig(pool))
}

func UploadHedgingProposalAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewUploadHandler(pool, hedgingProposalAdditionalFilesConfig(pool))
}

func DownloadHedgingProposalAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDownloadHandler(pool, hedgingProposalAdditionalFilesConfig(pool))
}

func DownloadSelectedHedgingProposalAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDownloadSelectedHandler(pool, hedgingProposalAdditionalFilesConfig(pool))
}

func DownloadHedgingProposalPackageZipHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewPackageZipHandler(pool, hedgingProposalAdditionalFilesConfig(pool), additionalfiles.PackageZipOptions{
		ModuleLabel: "Hedging Proposal",
		IDField:     "proposal_id",
	})
}

func DeleteHedgingProposalAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDeleteHandler(pool, hedgingProposalAdditionalFilesConfig(pool))
}

func AuditHedgingProposalAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewAuditHandler(pool, hedgingProposalAdditionalFilesConfig(pool))
}

func ApproveHedgingProposalAdditionalFileDeleteHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewApproveDeleteHandler(pool, hedgingProposalAdditionalFilesConfig(pool))
}

func RejectHedgingProposalAdditionalFileDeleteHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewRejectDeleteHandler(pool, hedgingProposalAdditionalFilesConfig(pool))
}

func hedgingProposalAdditionalFilesConfig(pool *pgxpool.Pool) additionalfiles.Config {
	return additionalfiles.Config{
		Module:                "fx-hedging-proposal",
		AuditSource:           "FX_HEDGING_PROPOSAL",
		AuditTableName:        fxAdditionalFileAuditTable,
		ParentIDField:         "proposal_id",
		List:                  listHedgingProposalAdditionalFiles,
		CreateReturning:       createHedgingProposalAdditionalFile,
		GetOne:                getHedgingProposalAdditionalFile,
		GetAnyFile:            getAnyHedgingProposalAdditionalFile,
		GetMany:               getHedgingProposalAdditionalFiles,
		SoftDelete:            deleteHedgingProposalAdditionalFile,
		SoftDeleteTx:          deleteHedgingProposalAdditionalFileTx,
		RecordMainUploadAudit: recordHedgingProposalMainUploadAudit(pool),
	}
}

func recordHedgingProposalMainUploadAudit(pool *pgxpool.Pool) func(context.Context, pgx.Tx, string, additionalfiles.MainUploadAuditPayload) error {
	return func(ctx context.Context, _ pgx.Tx, parentID string, payload additionalfiles.MainUploadAuditPayload) error {
		reason, err := additionalfiles.MainUploadAuditReasonJSON(payload)
		if err != nil {
			return err
		}
		auditutil.RecordActionPGX(ctx, pool, auditutil.ActionParams{
			TableName:    auditutil.TableHedgeProposalDocument,
			ParentColumn: "proposal_id",
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

func hedgingProposalFileQuery(whereClause string) string {
	return `
		SELECT f.file_id, f.stored_file_name, f.content_type, f.file_size, f.upload_s3_key, f.uploaded_by, f.uploaded_at
		FROM ` + hedgingProposalFilesTable + ` f
	` + whereClause
}

func listHedgingProposalAdditionalFiles(ctx context.Context, pool *pgxpool.Pool, parentID string) ([]additionalfiles.FileRecord, error) {
	return additionalfiles.QueryFiles(ctx, pool, hedgingProposalFileQuery(`
		WHERE f.proposal_id::text = $1
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		ORDER BY f.uploaded_at DESC
	`), strings.TrimSpace(parentID))
}

func createHedgingProposalAdditionalFile(ctx context.Context, tx pgx.Tx, input additionalfiles.CreateInput) (string, error) {
	return additionalfiles.InsertAdditionalFileRowReturningID(ctx, tx, hedgingProposalFilesTable, "proposal_id", input, `
		SELECT d.proposal_id AS parent_id
		FROM public.hedging_proposal_document d
		WHERE d.proposal_id::text = $8
		  AND COALESCE(d.is_deleted, FALSE) = FALSE
	`, strings.TrimSpace(input.ParentID))
}

func getHedgingProposalAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*additionalfiles.FileRecord, error) {
	return getHedgingProposalAdditionalFileWithDeleted(ctx, pool, parentID, fileID, false)
}

func getAnyHedgingProposalAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*additionalfiles.FileRecord, error) {
	return getHedgingProposalAdditionalFileWithDeleted(ctx, pool, parentID, fileID, true)
}

func getHedgingProposalAdditionalFileWithDeleted(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string, includeDeleted bool) (*additionalfiles.FileRecord, error) {
	deletedClause := constants.ErrFDReceiptDeletedFilter
	if includeDeleted {
		deletedClause = ""
	}
	return additionalfiles.FirstFile(ctx, pool, hedgingProposalFileQuery(`
		WHERE f.proposal_id::text = $1
		  AND f.file_id::text = $2
		  `+deletedClause+`
	`), strings.TrimSpace(parentID), strings.TrimSpace(fileID))
}

func getHedgingProposalAdditionalFiles(ctx context.Context, pool *pgxpool.Pool, parentID string, fileIDs []string) ([]additionalfiles.FileRecord, []string, error) {
	trimmedIDs := trimFXAdditionalFileIDs(fileIDs)
	files, err := additionalfiles.QueryFiles(ctx, pool, hedgingProposalFileQuery(`
		WHERE f.proposal_id::text = $1
		  AND f.file_id::text = ANY($2)
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		ORDER BY f.uploaded_at DESC
	`), strings.TrimSpace(parentID), trimmedIDs)
	if err != nil {
		return nil, nil, err
	}
	return files, missingFXAdditionalFileIDs(trimmedIDs, files), nil
}

func deleteHedgingProposalAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	return deleteHedgingProposalAdditionalFileExec(ctx, pool, parentID, fileID, deletedBy, deletedAt)
}

func deleteHedgingProposalAdditionalFileTx(ctx context.Context, tx pgx.Tx, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	return deleteHedgingProposalAdditionalFileExec(ctx, tx, parentID, fileID, deletedBy, deletedAt)
}

func deleteHedgingProposalAdditionalFileExec(ctx context.Context, exec fxAdditionalFileExec, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	result, err := exec.Exec(ctx, `
		UPDATE `+hedgingProposalFilesTable+`
		SET is_deleted = TRUE,
		    deleted_by = $3,
		    deleted_at = $4
		WHERE proposal_id::text = $1
		  AND file_id::text = $2
		  AND COALESCE(is_deleted, FALSE) = FALSE
	`, strings.TrimSpace(parentID), strings.TrimSpace(fileID), deletedBy, deletedAt)
	if err != nil {
		return false, err
	}
	return result.RowsAffected() > 0, nil
}
