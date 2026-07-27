package projection

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

func ListAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewListHandler(pool, projectionAdditionalFilesConfig())
}

func UploadAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewUploadHandler(pool, projectionAdditionalFilesConfig())
}

func DownloadAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDownloadHandler(pool, projectionAdditionalFilesConfig())
}

func DownloadSelectedAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDownloadSelectedHandler(pool, projectionAdditionalFilesConfig())
}

func DownloadProjectionPackageZipHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewPackageZipHandler(pool, projectionAdditionalFilesConfig(), additionalfiles.PackageZipOptions{
		ModuleLabel: "Projection",
		IDField:     "proposal_id",
		LoadMain:    loadProjectionPackageMainFile,
	})
}

func DeleteAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDeleteHandler(pool, projectionAdditionalFilesConfig())
}

func AuditAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewAuditHandler(pool, projectionAdditionalFilesConfig())
}

func ApproveAdditionalFileDeleteHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewApproveDeleteHandler(pool, projectionAdditionalFilesConfig())
}

func RejectAdditionalFileDeleteHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewRejectDeleteHandler(pool, projectionAdditionalFilesConfig())
}

func projectionAdditionalFilesConfig() additionalfiles.Config {
	return additionalfiles.Config{
		Module:                  "projection",
		AuditSource:             "PROJECTION",
		ParentIDField:           "proposal_id",
		PolicyModuleCode:        "CASH",
		PolicySubModule:         "CASHFLOW_PROJECTION",
		List:                    listProjectionAdditionalFiles,
		CreateReturning:         createProjectionAdditionalFile,
		GetOne:                  getProjectionAdditionalFile,
		GetAnyFile:              getAnyProjectionAdditionalFile,
		GetMany:                 getProjectionAdditionalFiles,
		SoftDelete:              deleteProjectionAdditionalFile,
		SoftDeleteTx:            deleteProjectionAdditionalFileTx,
		RecordMainUploadAudit:   recordProjectionMainUploadAudit,
		RecordMainDownloadAudit: recordProjectionMainDownloadAudit,
	}
}

func recordProjectionMainUploadAudit(ctx context.Context, tx pgx.Tx, parentID string, payload additionalfiles.MainUploadAuditPayload) error {
	return additionalfiles.InsertMainUploadAudit(ctx, tx, "cimplrcorpsaas.audit_action_cashflow_proposal", "proposal_id", "action_type", parentID, payload)
}

func recordProjectionMainDownloadAudit(ctx context.Context, exec additionalfiles.AuditExecutor, parentID string, payload additionalfiles.MainUploadAuditPayload) error {
	return additionalfiles.InsertMainDownloadRecord(ctx, exec, "cimplrcorpsaas.audit_cashflow_proposal_downloads", "proposal_id", parentID, payload, nil)
}

func listProjectionAdditionalFiles(ctx context.Context, pool *pgxpool.Pool, parentID string) ([]additionalfiles.FileRecord, error) {
	names, err := projectionEntityNames(ctx)
	if err != nil {
		return nil, err
	}
	return additionalfiles.QueryFiles(ctx, pool, projectionFileQuery(`
		WHERE f.proposal_id = $1
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		  AND `+projectionParentEntityAllowed(constants.ProposalID, "$2")+`
		ORDER BY f.uploaded_at DESC
	`), parentID, names)
}

func loadProjectionPackageMainFile(ctx context.Context, pool *pgxpool.Pool, rowID string) (*additionalfiles.MainPackageFile, error) {
	names, err := projectionEntityNames(ctx)
	if err != nil {
		return nil, err
	}
	var uploadS3Key string
	err = pool.QueryRow(ctx, `
		SELECT COALESCE(p.upload_s3_key, '')
		FROM cimplrcorpsaas.cashflow_proposal p
		WHERE p.proposal_id = $1
		  AND COALESCE(p.is_deleted, FALSE) = FALSE
		  AND `+projectionParentEntityAllowed(constants.ProposalID, "$2")+`
	`, rowID, names).Scan(&uploadS3Key)
	if err != nil || strings.TrimSpace(uploadS3Key) == "" {
		return nil, err
	}
	return &additionalfiles.MainPackageFile{UploadS3Key: uploadS3Key}, nil
}

func createProjectionAdditionalFile(ctx context.Context, tx pgx.Tx, input additionalfiles.CreateInput) (string, error) {
	names, err := projectionEntityNames(ctx)
	if err != nil {
		return "", err
	}
	return additionalfiles.InsertAdditionalFileRowReturningID(ctx, tx, "cimplrcorpsaas.cashflow_proposal_files", "proposal_id", input, `
		SELECT DISTINCT p.proposal_id AS parent_id
		FROM cimplrcorpsaas.cashflow_proposal p
		WHERE p.proposal_id = $8
		  AND `+projectionParentEntityAllowed(constants.ProposalID, "$9")+`
	`, input.ParentID, names)
}

func getProjectionAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*additionalfiles.FileRecord, error) {
	return getProjectionAdditionalFileWithDeleted(ctx, pool, parentID, fileID, false)
}

func getAnyProjectionAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*additionalfiles.FileRecord, error) {
	return getProjectionAdditionalFileWithDeleted(ctx, pool, parentID, fileID, true)
}

func getProjectionAdditionalFileWithDeleted(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string, includeDeleted bool) (*additionalfiles.FileRecord, error) {
	names, err := projectionEntityNames(ctx)
	if err != nil {
		return nil, err
	}

	deletedClause := constants.ErrFDReceiptDeletedFilter
	if includeDeleted {
		deletedClause = ""
	}
	return additionalfiles.FirstFile(ctx, pool, projectionFileQuery(`
		WHERE f.proposal_id = $1
		  AND f.file_id = $2
		  `+deletedClause+`
		  AND `+projectionParentEntityAllowed(constants.ProposalID, "$3")+`
	`), parentID, fileID, names)
}

func getProjectionAdditionalFiles(ctx context.Context, pool *pgxpool.Pool, parentID string, fileIDs []string) ([]additionalfiles.FileRecord, []string, error) {
	names, err := projectionEntityNames(ctx)
	if err != nil {
		return nil, nil, err
	}
	trimmedIDs := trimProjectionAdditionalFileIDs(fileIDs)
	files, queryErr := additionalfiles.QueryFiles(ctx, pool, projectionFileQuery(`
		WHERE f.proposal_id = $1
		  AND f.file_id = ANY($2)
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		  AND `+projectionParentEntityAllowed(constants.ProposalID, "$3")+`
		ORDER BY f.uploaded_at DESC
	`), parentID, trimmedIDs, names)
	if queryErr != nil {
		return nil, nil, queryErr
	}
	return files, missingProjectionAdditionalFileIDs(trimmedIDs, files), nil
}

func deleteProjectionAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	return deleteProjectionAdditionalFileExec(ctx, pool, parentID, fileID, deletedBy, deletedAt)
}

func deleteProjectionAdditionalFileTx(ctx context.Context, tx pgx.Tx, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	return deleteProjectionAdditionalFileExec(ctx, tx, parentID, fileID, deletedBy, deletedAt)
}

type projectionFileExec interface {
	Exec(context.Context, string, ...interface{}) (pgconn.CommandTag, error)
}

func deleteProjectionAdditionalFileExec(ctx context.Context, exec projectionFileExec, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	names, err := projectionEntityNames(ctx)
	if err != nil {
		return false, err
	}
	result, execErr := exec.Exec(ctx, `
		UPDATE cimplrcorpsaas.cashflow_proposal_files f
		SET is_deleted = TRUE,
		    deleted_by = $3,
		    deleted_at = $4
		FROM cimplrcorpsaas.cashflow_proposal p
		WHERE f.proposal_id = $1
		  AND f.file_id = $2
		  AND p.proposal_id = f.proposal_id
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		  AND `+projectionParentEntityAllowed(constants.ProposalID, "$5")+`
	`, parentID, fileID, deletedBy, deletedAt, names)
	if execErr != nil {
		return false, execErr
	}
	return result.RowsAffected() > 0, nil
}

func projectionFileQuery(whereClause string) string {
	return `
		SELECT DISTINCT f.file_id, f.stored_file_name, f.content_type, f.file_size, f.upload_s3_key, f.uploaded_by, f.uploaded_at
		FROM cimplrcorpsaas.cashflow_proposal_files f
		JOIN cimplrcorpsaas.cashflow_proposal p ON p.proposal_id = f.proposal_id
	` + whereClause
}

func projectionParentEntityAllowed(proposalExpr, namesExpr string) string {
	return `
		(
			NOT EXISTS (
				SELECT 1
				FROM cimplrcorpsaas.cashflow_proposal_item scope_i
				WHERE scope_i.proposal_id = ` + proposalExpr + `
				  AND COALESCE(scope_i.is_deleted, FALSE) = FALSE
			)
			OR EXISTS (
				SELECT 1
				FROM cimplrcorpsaas.cashflow_proposal_item scope_i
				WHERE scope_i.proposal_id = ` + proposalExpr + `
				  AND COALESCE(scope_i.is_deleted, FALSE) = FALSE
				  AND (
					NULLIF(TRIM(scope_i.entity_name), '') IS NULL
					OR LOWER(TRIM(scope_i.entity_name)) = ANY(` + namesExpr + `)
				  )
			)
		)`
}

func projectionEntityNames(ctx context.Context) ([]string, error) {
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

func trimProjectionAdditionalFileIDs(fileIDs []string) []string {
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

func missingProjectionAdditionalFileIDs(expected []string, files []additionalfiles.FileRecord) []string {
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
