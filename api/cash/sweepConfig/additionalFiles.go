package sweepconfig

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

func ListSweepPlanningAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewListHandler(pool, sweepPlanningAdditionalFilesConfig())
}

func UploadSweepPlanningAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewUploadHandler(pool, sweepPlanningAdditionalFilesConfig())
}

func DownloadSweepPlanningAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDownloadHandler(pool, sweepPlanningAdditionalFilesConfig())
}

func DownloadSelectedSweepPlanningAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDownloadSelectedHandler(pool, sweepPlanningAdditionalFilesConfig())
}

func DownloadSweepPlanningPackageZipHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewPackageZipHandler(pool, sweepPlanningAdditionalFilesConfig(), additionalfiles.PackageZipOptions{
		ModuleLabel: "Sweep Planning",
		IDField:     "sweep_id",
	})
}

func DeleteSweepPlanningAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDeleteHandler(pool, sweepPlanningAdditionalFilesConfig())
}

func AuditSweepPlanningAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewAuditHandler(pool, sweepPlanningAdditionalFilesConfig())
}

func ApproveSweepPlanningAdditionalFileDeleteHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewApproveDeleteHandler(pool, sweepPlanningAdditionalFilesConfig())
}

func RejectSweepPlanningAdditionalFileDeleteHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewRejectDeleteHandler(pool, sweepPlanningAdditionalFilesConfig())
}

func ListSweepInitiationAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewListHandler(pool, sweepInitiationAdditionalFilesConfig())
}

func UploadSweepInitiationAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewUploadHandler(pool, sweepInitiationAdditionalFilesConfig())
}

func DownloadSweepInitiationAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDownloadHandler(pool, sweepInitiationAdditionalFilesConfig())
}

func DownloadSelectedSweepInitiationAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDownloadSelectedHandler(pool, sweepInitiationAdditionalFilesConfig())
}

func DownloadSweepInitiationPackageZipHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewPackageZipHandler(pool, sweepInitiationAdditionalFilesConfig(), additionalfiles.PackageZipOptions{
		ModuleLabel: "Sweep Initiation",
		IDField:     "initiation_id",
	})
}

func DeleteSweepInitiationAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDeleteHandler(pool, sweepInitiationAdditionalFilesConfig())
}

func AuditSweepInitiationAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewAuditHandler(pool, sweepInitiationAdditionalFilesConfig())
}

func ApproveSweepInitiationAdditionalFileDeleteHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewApproveDeleteHandler(pool, sweepInitiationAdditionalFilesConfig())
}

func RejectSweepInitiationAdditionalFileDeleteHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewRejectDeleteHandler(pool, sweepInitiationAdditionalFilesConfig())
}

func sweepPlanningAdditionalFilesConfig() additionalfiles.Config {
	return additionalfiles.Config{
		Module:                  "sweep-planning",
		AuditSource:             "SWEEP_PLANNING",
		ParentIDField:           "sweep_id",
		FolderName:              additionalfiles.AdditionalFilesFolder(),
		PolicyModuleCode:        "CASH",
		PolicySubModule:         "SWEEP_CONFIG",
		List:                    listSweepPlanningAdditionalFiles,
		CreateReturning:         createSweepPlanningAdditionalFile,
		GetOne:                  getSweepPlanningAdditionalFile,
		GetAnyFile:              getAnySweepPlanningAdditionalFile,
		GetMany:                 getSweepPlanningAdditionalFiles,
		SoftDelete:              deleteSweepPlanningAdditionalFile,
		SoftDeleteTx:            deleteSweepPlanningAdditionalFileTx,
		RecordMainUploadAudit:   recordSweepPlanningMainUploadAudit,
		RecordMainDownloadAudit: recordSweepPlanningMainDownloadAudit,
	}
}

func sweepInitiationAdditionalFilesConfig() additionalfiles.Config {
	return additionalfiles.Config{
		Module:                  "sweep-initiation",
		AuditSource:             "SWEEP_INITIATION",
		ParentIDField:           "initiation_id",
		FolderName:              additionalfiles.AdditionalFilesFolder(),
		PolicyModuleCode:        "CASH",
		PolicySubModule:         "SWEEP_INITIATION",
		List:                    listSweepInitiationAdditionalFiles,
		CreateReturning:         createSweepInitiationAdditionalFile,
		GetOne:                  getSweepInitiationAdditionalFile,
		GetAnyFile:              getAnySweepInitiationAdditionalFile,
		GetMany:                 getSweepInitiationAdditionalFiles,
		SoftDelete:              deleteSweepInitiationAdditionalFile,
		SoftDeleteTx:            deleteSweepInitiationAdditionalFileTx,
		RecordMainDownloadAudit: recordSweepInitiationMainDownloadAudit,
	}
}

func recordSweepPlanningMainUploadAudit(ctx context.Context, tx pgx.Tx, parentID string, payload additionalfiles.MainUploadAuditPayload) error {
	return additionalfiles.InsertMainUploadAudit(ctx, tx, "cimplrcorpsaas.auditactionsweepconfiguration", "sweep_id", "actiontype", parentID, payload)
}

func recordSweepPlanningMainDownloadAudit(ctx context.Context, exec additionalfiles.AuditExecutor, parentID string, payload additionalfiles.MainUploadAuditPayload) error {
	return additionalfiles.InsertMainDownloadAudit(ctx, exec, "cimplrcorpsaas.auditactionsweepconfiguration", "sweep_id", "actiontype", parentID, payload)
}

func recordSweepInitiationMainUploadAudit(ctx context.Context, tx pgx.Tx, parentID string, payload additionalfiles.MainUploadAuditPayload) error {
	reason, err := additionalfiles.MainUploadAuditReasonJSON(payload)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO cimplrcorpsaas.auditactionsweepinitiation (
			initiation_id,
			sweep_id,
			actiontype,
			processing_status,
			reason,
			requested_by,
			requested_at,
			requested_ip
		)
		SELECT
			si.initiation_id,
			si.sweep_id,
			'UPLOAD_FILE',
			'APPROVED',
			$2,
			$3,
			$4,
			$5
		FROM cimplrcorpsaas.sweep_initiation si
		WHERE si.initiation_id = $1
	`, parentID, reason, payload.UploadedBy, payload.UploadedAt, nullifyEmpty(payload.RequestedIP))
	return err
}

func recordSweepInitiationMainDownloadAudit(ctx context.Context, exec additionalfiles.AuditExecutor, parentID string, payload additionalfiles.MainUploadAuditPayload) error {
	reason, err := additionalfiles.MainDownloadAuditReasonJSON(payload)
	if err != nil {
		return err
	}

	_, err = exec.Exec(ctx, `
		INSERT INTO cimplrcorpsaas.auditactionsweepinitiation (
			initiation_id,
			sweep_id,
			actiontype,
			processing_status,
			reason,
			requested_by,
			requested_at,
			requested_ip
		)
		SELECT
			si.initiation_id,
			si.sweep_id,
			'DOWNLOAD',
			'APPROVED',
			$2,
			$3,
			$4,
			$5
		FROM cimplrcorpsaas.sweep_initiation si
		WHERE si.initiation_id = $1
	`, parentID, reason, payload.UploadedBy, payload.UploadedAt, nullifyEmpty(payload.RequestedIP))
	return err
}

func listSweepPlanningAdditionalFiles(ctx context.Context, pool *pgxpool.Pool, parentID string) ([]additionalfiles.FileRecord, error) {
	query, args := sweepPlanningFilesQuery(ctx, `
		WHERE f.sweep_id = $1
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		  AND COALESCE(sc.is_deleted, FALSE) = FALSE
	`, parentID)
	query += ` ORDER BY f.uploaded_at DESC`
	return additionalfiles.QueryFiles(ctx, pool, query, args...)
}

func createSweepPlanningAdditionalFile(ctx context.Context, tx pgx.Tx, input additionalfiles.CreateInput) (string, error) {
	entityClause, entityArgs := sweepEntityScope(ctx, "sc", 9)
	args := append([]interface{}{input.ParentID}, entityArgs...)
	return additionalfiles.InsertAdditionalFileRowReturningID(ctx, tx, "cimplrcorpsaas.sweep_configuration_files", "sweep_id", input, `
		SELECT sc.sweep_id AS parent_id
		FROM cimplrcorpsaas.sweepconfiguration sc
		WHERE sc.sweep_id = $8
		  AND COALESCE(sc.is_deleted, FALSE) = FALSE
	`+entityClause, args...)
}

func getSweepPlanningAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*additionalfiles.FileRecord, error) {
	return getSweepPlanningAdditionalFileWithDeleted(ctx, pool, parentID, fileID, false)
}

func getAnySweepPlanningAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*additionalfiles.FileRecord, error) {
	return getSweepPlanningAdditionalFileWithDeleted(ctx, pool, parentID, fileID, true)
}

func getSweepPlanningAdditionalFileWithDeleted(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string, includeDeleted bool) (*additionalfiles.FileRecord, error) {
	deletedClause := constants.ErrFDReceiptDeletedFilter
	if includeDeleted {
		deletedClause = ""
	}
	query, args := sweepPlanningFilesQuery(ctx, `
		WHERE f.sweep_id = $1
		  AND f.file_id = $2
		  `+deletedClause+`
		  AND COALESCE(sc.is_deleted, FALSE) = FALSE
	`, parentID, fileID)
	return additionalfiles.FirstFile(ctx, pool, query, args...)
}

func getSweepPlanningAdditionalFiles(ctx context.Context, pool *pgxpool.Pool, parentID string, fileIDs []string) ([]additionalfiles.FileRecord, []string, error) {
	trimmedIDs := trimSweepAdditionalFileIDs(fileIDs)
	query, args := sweepPlanningFilesQuery(ctx, `
		WHERE f.sweep_id = $1
		  AND f.file_id = ANY($2)
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		  AND COALESCE(sc.is_deleted, FALSE) = FALSE
	`, parentID, trimmedIDs)
	query += ` ORDER BY f.uploaded_at DESC`

	files, err := additionalfiles.QueryFiles(ctx, pool, query, args...)
	if err != nil {
		return nil, nil, err
	}
	return files, missingSweepAdditionalFileIDs(trimmedIDs, files), nil
}

func deleteSweepPlanningAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	return deleteSweepPlanningAdditionalFileExec(ctx, pool, parentID, fileID, deletedBy, deletedAt)
}

func deleteSweepPlanningAdditionalFileTx(ctx context.Context, tx pgx.Tx, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	return deleteSweepPlanningAdditionalFileExec(ctx, tx, parentID, fileID, deletedBy, deletedAt)
}

type sweepFileExec interface {
	Exec(context.Context, string, ...interface{}) (pgconn.CommandTag, error)
}

func deleteSweepPlanningAdditionalFileExec(ctx context.Context, exec sweepFileExec, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	entityClause, entityArgs := sweepEntityScope(ctx, "sc", 5)
	query := `
		UPDATE cimplrcorpsaas.sweep_configuration_files f
		SET is_deleted = TRUE,
		    deleted_by = $3,
		    deleted_at = $4
		FROM cimplrcorpsaas.sweepconfiguration sc
		WHERE f.sweep_id = $1
		  AND f.file_id = $2
		  AND sc.sweep_id = f.sweep_id
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		  AND COALESCE(sc.is_deleted, FALSE) = FALSE
	` + entityClause

	args := []interface{}{parentID, fileID, deletedBy, deletedAt}
	args = append(args, entityArgs...)
	result, err := exec.Exec(ctx, query, args...)
	if err != nil {
		return false, err
	}
	return result.RowsAffected() > 0, nil
}

func listSweepInitiationAdditionalFiles(ctx context.Context, pool *pgxpool.Pool, parentID string) ([]additionalfiles.FileRecord, error) {
	query, args := sweepInitiationFilesQuery(ctx, `
		WHERE f.initiation_id = $1
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		  AND COALESCE(sc.is_deleted, FALSE) = FALSE
	`, parentID)
	query += ` ORDER BY f.uploaded_at DESC`
	return additionalfiles.QueryFiles(ctx, pool, query, args...)
}

func createSweepInitiationAdditionalFile(ctx context.Context, tx pgx.Tx, input additionalfiles.CreateInput) (string, error) {
	entityClause, entityArgs := sweepEntityScope(ctx, "sc", 9)
	args := append([]interface{}{input.ParentID}, entityArgs...)
	return additionalfiles.InsertAdditionalFileRowReturningID(ctx, tx, "cimplrcorpsaas.sweep_initiation_files", "initiation_id", input, `
		SELECT si.initiation_id AS parent_id
		FROM cimplrcorpsaas.sweep_initiation si
		JOIN cimplrcorpsaas.sweepconfiguration sc ON sc.sweep_id = si.sweep_id
		WHERE si.initiation_id = $8
		  AND COALESCE(sc.is_deleted, FALSE) = FALSE
	`+entityClause, args...)
}

func getSweepInitiationAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*additionalfiles.FileRecord, error) {
	return getSweepInitiationAdditionalFileWithDeleted(ctx, pool, parentID, fileID, false)
}

func getAnySweepInitiationAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*additionalfiles.FileRecord, error) {
	return getSweepInitiationAdditionalFileWithDeleted(ctx, pool, parentID, fileID, true)
}

func getSweepInitiationAdditionalFileWithDeleted(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string, includeDeleted bool) (*additionalfiles.FileRecord, error) {
	deletedClause := constants.ErrFDReceiptDeletedFilter
	if includeDeleted {
		deletedClause = ""
	}
	query, args := sweepInitiationFilesQuery(ctx, `
		WHERE f.initiation_id = $1
		  AND f.file_id = $2
		  `+deletedClause+`
		  AND COALESCE(sc.is_deleted, FALSE) = FALSE
	`, parentID, fileID)
	return additionalfiles.FirstFile(ctx, pool, query, args...)
}

func getSweepInitiationAdditionalFiles(ctx context.Context, pool *pgxpool.Pool, parentID string, fileIDs []string) ([]additionalfiles.FileRecord, []string, error) {
	trimmedIDs := trimSweepAdditionalFileIDs(fileIDs)
	query, args := sweepInitiationFilesQuery(ctx, `
		WHERE f.initiation_id = $1
		  AND f.file_id = ANY($2)
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		  AND COALESCE(sc.is_deleted, FALSE) = FALSE
	`, parentID, trimmedIDs)
	query += ` ORDER BY f.uploaded_at DESC`

	files, err := additionalfiles.QueryFiles(ctx, pool, query, args...)
	if err != nil {
		return nil, nil, err
	}
	return files, missingSweepAdditionalFileIDs(trimmedIDs, files), nil
}

func deleteSweepInitiationAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	return deleteSweepInitiationAdditionalFileExec(ctx, pool, parentID, fileID, deletedBy, deletedAt)
}

func deleteSweepInitiationAdditionalFileTx(ctx context.Context, tx pgx.Tx, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	return deleteSweepInitiationAdditionalFileExec(ctx, tx, parentID, fileID, deletedBy, deletedAt)
}

func deleteSweepInitiationAdditionalFileExec(ctx context.Context, exec sweepFileExec, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	entityClause, entityArgs := sweepEntityScope(ctx, "sc", 5)
	query := `
		UPDATE cimplrcorpsaas.sweep_initiation_files f
		SET is_deleted = TRUE,
		    deleted_by = $3,
		    deleted_at = $4
		FROM cimplrcorpsaas.sweep_initiation si
		JOIN cimplrcorpsaas.sweepconfiguration sc ON sc.sweep_id = si.sweep_id
		WHERE f.initiation_id = $1
		  AND f.file_id = $2
		  AND si.initiation_id = f.initiation_id
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		  AND COALESCE(sc.is_deleted, FALSE) = FALSE
	` + entityClause

	args := []interface{}{parentID, fileID, deletedBy, deletedAt}
	args = append(args, entityArgs...)
	result, err := exec.Exec(ctx, query, args...)
	if err != nil {
		return false, err
	}
	return result.RowsAffected() > 0, nil
}

func sweepPlanningFilesQuery(ctx context.Context, whereClause string, baseArgs ...interface{}) (string, []interface{}) {
	entityClause, entityArgs := sweepEntityScope(ctx, "sc", len(baseArgs)+1)
	query := `
		SELECT f.file_id, f.stored_file_name, f.content_type, f.file_size, f.upload_s3_key, f.uploaded_by, f.uploaded_at
		FROM cimplrcorpsaas.sweep_configuration_files f
		JOIN cimplrcorpsaas.sweepconfiguration sc ON sc.sweep_id = f.sweep_id
	` + whereClause + entityClause

	args := append([]interface{}{}, baseArgs...)
	args = append(args, entityArgs...)
	return query, args
}

func sweepInitiationFilesQuery(ctx context.Context, whereClause string, baseArgs ...interface{}) (string, []interface{}) {
	entityClause, entityArgs := sweepEntityScope(ctx, "sc", len(baseArgs)+1)
	query := `
		SELECT f.file_id, f.stored_file_name, f.content_type, f.file_size, f.upload_s3_key, f.uploaded_by, f.uploaded_at
		FROM cimplrcorpsaas.sweep_initiation_files f
		JOIN cimplrcorpsaas.sweep_initiation si ON si.initiation_id = f.initiation_id
		JOIN cimplrcorpsaas.sweepconfiguration sc ON sc.sweep_id = si.sweep_id
	` + whereClause + entityClause

	args := append([]interface{}{}, baseArgs...)
	args = append(args, entityArgs...)
	return query, args
}

func sweepEntityScope(ctx context.Context, alias string, position int) (string, []interface{}) {
	names := api.GetEntityNamesFromCtx(ctx)
	lowered := make([]string, 0, len(names))
	for _, name := range names {
		if trimmed := strings.ToLower(strings.TrimSpace(name)); trimmed != "" {
			lowered = append(lowered, trimmed)
		}
	}
	if len(lowered) == 0 {
		return "", nil
	}
	return fmt.Sprintf(" AND LOWER(TRIM(COALESCE(%s.entity_name, ''))) = ANY($%d)", alias, position), []interface{}{lowered}
}

func trimSweepAdditionalFileIDs(fileIDs []string) []string {
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

func missingSweepAdditionalFileIDs(expected []string, files []additionalfiles.FileRecord) []string {
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
