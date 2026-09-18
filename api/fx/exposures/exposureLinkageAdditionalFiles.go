package exposures

import (
	"CimplrCorpSaas/api/cash/additionalfiles"
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/internal/ctxutil"
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const exposureLinkageFilesTable = "public.exposure_hedge_link_files"

func ListExposureLinkageAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewListHandler(pool, exposureLinkageAdditionalFilesConfig(pool))
}

func UploadExposureLinkageAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewUploadHandler(pool, exposureLinkageAdditionalFilesConfig(pool))
}

func DownloadExposureLinkageAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDownloadHandler(pool, exposureLinkageAdditionalFilesConfig(pool))
}

func DownloadSelectedExposureLinkageAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDownloadSelectedHandler(pool, exposureLinkageAdditionalFilesConfig(pool))
}

func DownloadExposureLinkagePackageZipHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewPackageZipHandler(pool, exposureLinkageAdditionalFilesConfig(pool), additionalfiles.PackageZipOptions{
		ModuleLabel: "Exposure Linkage",
		IDField:     "link_id",
	})
}

func DeleteExposureLinkageAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDeleteHandler(pool, exposureLinkageAdditionalFilesConfig(pool))
}

func AuditExposureLinkageAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewAuditHandler(pool, exposureLinkageAdditionalFilesConfig(pool))
}

func ApproveExposureLinkageAdditionalFileDeleteHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewApproveDeleteHandler(pool, exposureLinkageAdditionalFilesConfig(pool))
}

func RejectExposureLinkageAdditionalFileDeleteHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewRejectDeleteHandler(pool, exposureLinkageAdditionalFilesConfig(pool))
}

func exposureLinkageAdditionalFilesConfig(pool *pgxpool.Pool) additionalfiles.Config {
	return additionalfiles.Config{
		Module:                "fx-exposure-linkage",
		AuditSource:           "FX_HEDGE_LINK",
		AuditTableName:        fxAdditionalFileAuditTable,
		ParentIDField:         "link_id",
		List:                  listExposureLinkageAdditionalFiles,
		CreateReturning:       createExposureLinkageAdditionalFile,
		GetOne:                getExposureLinkageAdditionalFile,
		GetAnyFile:            getAnyExposureLinkageAdditionalFile,
		GetMany:               getExposureLinkageAdditionalFiles,
		SoftDelete:            deleteExposureLinkageAdditionalFile,
		SoftDeleteTx:          deleteExposureLinkageAdditionalFileTx,
		RecordMainUploadAudit: recordExposureLinkageMainUploadAudit(pool),
	}
}

func recordExposureLinkageMainUploadAudit(pool *pgxpool.Pool) func(context.Context, pgx.Tx, string, additionalfiles.MainUploadAuditPayload) error {
	return func(ctx context.Context, _ pgx.Tx, parentID string, payload additionalfiles.MainUploadAuditPayload) error {
		reason, err := additionalfiles.MainUploadAuditReasonJSON(payload)
		if err != nil {
			return err
		}
		_, _ = pool.Exec(ctx, `
			INSERT INTO public.auditactionhedgelink
				(exposure_header_id, booking_id, actiontype, processing_status, reason, requested_by, requested_at, requested_ip)
			SELECT l.exposure_header_id::text, l.booking_id::text, 'UPLOAD_FILE', $2, $3, $4, now(), $5
			FROM public.exposure_hedge_links l
			WHERE l.link_id::text = $1
		`, strings.TrimSpace(parentID), constants.StatusApproved, reason, strings.TrimSpace(payload.UploadedBy), nullIfBlankLinkageIP(payload.RequestedIP))
		return nil
	}
}

func nullIfBlankLinkageIP(value string) interface{} {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return strings.TrimSpace(value)
}

func exposureLinkageEntityNames(ctx context.Context) ([]string, error) {
	names := ctxutil.FromContext(ctx).EntityNames
	if len(names) == 0 {
		return nil, errors.New(constants.ErrNoAccessibleBusinessUnit)
	}
	return names, nil
}

func exposureLinkageEntityFilter(position int) string {
	return fmt.Sprintf(`
		  AND (
			COALESCE(h.entity, '') = ANY($%d)
			OR COALESCE(h.entity1, '') = ANY($%d)
			OR COALESCE(h.entity2, '') = ANY($%d)
			OR COALESCE(h.entity3, '') = ANY($%d)
			OR COALESCE(fb.entity_level_0, '') = ANY($%d)
			OR COALESCE(fb.entity_level_1, '') = ANY($%d)
			OR COALESCE(fb.entity_level_2, '') = ANY($%d)
			OR COALESCE(fb.entity_level_3, '') = ANY($%d)
		  )`, position, position, position, position, position, position, position, position)
}

func exposureLinkageFileQuery(whereClause string) string {
	return `
		SELECT f.file_id, f.stored_file_name, f.content_type, f.file_size, f.upload_s3_key, f.uploaded_by, f.uploaded_at
		FROM ` + exposureLinkageFilesTable + ` f
		JOIN public.exposure_hedge_links l ON l.link_id = f.link_id
		LEFT JOIN public.exposure_headers h ON h.exposure_header_id = l.exposure_header_id
		LEFT JOIN public.forward_bookings fb ON fb.system_transaction_id = l.booking_id
	` + whereClause
}

func listExposureLinkageAdditionalFiles(ctx context.Context, pool *pgxpool.Pool, parentID string) ([]additionalfiles.FileRecord, error) {
	names, err := exposureLinkageEntityNames(ctx)
	if err != nil {
		return nil, err
	}
	return additionalfiles.QueryFiles(ctx, pool, exposureLinkageFileQuery(`
		WHERE f.link_id::text = $1
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		  `+exposureLinkageEntityFilter(2)+`
		ORDER BY f.uploaded_at DESC
	`), strings.TrimSpace(parentID), names)
}

func createExposureLinkageAdditionalFile(ctx context.Context, tx pgx.Tx, input additionalfiles.CreateInput) (string, error) {
	names, err := exposureLinkageEntityNames(ctx)
	if err != nil {
		return "", err
	}
	return additionalfiles.InsertAdditionalFileRowReturningID(ctx, tx, exposureLinkageFilesTable, "link_id", input, `
		SELECT l.link_id AS parent_id
		FROM public.exposure_hedge_links l
		LEFT JOIN public.exposure_headers h ON h.exposure_header_id = l.exposure_header_id
		LEFT JOIN public.forward_bookings fb ON fb.system_transaction_id = l.booking_id
		WHERE l.link_id::text = $8
		  `+exposureLinkageEntityFilter(9)+`
	`, strings.TrimSpace(input.ParentID), names)
}

func getExposureLinkageAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*additionalfiles.FileRecord, error) {
	return getExposureLinkageAdditionalFileWithDeleted(ctx, pool, parentID, fileID, false)
}

func getAnyExposureLinkageAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*additionalfiles.FileRecord, error) {
	return getExposureLinkageAdditionalFileWithDeleted(ctx, pool, parentID, fileID, true)
}

func getExposureLinkageAdditionalFileWithDeleted(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string, includeDeleted bool) (*additionalfiles.FileRecord, error) {
	names, err := exposureLinkageEntityNames(ctx)
	if err != nil {
		return nil, err
	}
	deletedClause := constants.ErrFDReceiptDeletedFilter
	if includeDeleted {
		deletedClause = ""
	}
	return additionalfiles.FirstFile(ctx, pool, exposureLinkageFileQuery(`
		WHERE f.link_id::text = $1
		  AND f.file_id::text = $2
		  `+deletedClause+`
		  `+exposureLinkageEntityFilter(3)+`
	`), strings.TrimSpace(parentID), strings.TrimSpace(fileID), names)
}

func getExposureLinkageAdditionalFiles(ctx context.Context, pool *pgxpool.Pool, parentID string, fileIDs []string) ([]additionalfiles.FileRecord, []string, error) {
	names, err := exposureLinkageEntityNames(ctx)
	if err != nil {
		return nil, nil, err
	}
	trimmedIDs := trimFXAdditionalFileIDs(fileIDs)
	files, queryErr := additionalfiles.QueryFiles(ctx, pool, exposureLinkageFileQuery(`
		WHERE f.link_id::text = $1
		  AND f.file_id::text = ANY($2)
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		  `+exposureLinkageEntityFilter(3)+`
		ORDER BY f.uploaded_at DESC
	`), strings.TrimSpace(parentID), trimmedIDs, names)
	if queryErr != nil {
		return nil, nil, queryErr
	}
	return files, missingFXAdditionalFileIDs(trimmedIDs, files), nil
}

func deleteExposureLinkageAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	return deleteExposureLinkageAdditionalFileExec(ctx, pool, parentID, fileID, deletedBy, deletedAt)
}

func deleteExposureLinkageAdditionalFileTx(ctx context.Context, tx pgx.Tx, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	return deleteExposureLinkageAdditionalFileExec(ctx, tx, parentID, fileID, deletedBy, deletedAt)
}

func deleteExposureLinkageAdditionalFileExec(ctx context.Context, exec fxAdditionalFileExec, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	names, err := exposureLinkageEntityNames(ctx)
	if err != nil {
		return false, err
	}
	result, execErr := exec.Exec(ctx, `
		UPDATE `+exposureLinkageFilesTable+` f
		SET is_deleted = TRUE,
		    deleted_by = $3,
		    deleted_at = $4
		FROM public.exposure_hedge_links l
		LEFT JOIN public.exposure_headers h ON h.exposure_header_id = l.exposure_header_id
		LEFT JOIN public.forward_bookings fb ON fb.system_transaction_id = l.booking_id
		WHERE f.link_id::text = $1
		  AND f.file_id::text = $2
		  AND l.link_id = f.link_id
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		  `+exposureLinkageEntityFilter(5)+`
	`, strings.TrimSpace(parentID), strings.TrimSpace(fileID), deletedBy, deletedAt, names)
	if execErr != nil {
		return false, execErr
	}
	return result.RowsAffected() > 0, nil
}
