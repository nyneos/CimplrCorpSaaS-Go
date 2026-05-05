package projection

import (
	api "CimplrCorpSaas/api"
	"CimplrCorpSaas/api/cash/additionalfiles"
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
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

func DeleteAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDeleteHandler(pool, projectionAdditionalFilesConfig())
}

func projectionAdditionalFilesConfig() additionalfiles.Config {
	return additionalfiles.Config{
		Module:        "projection",
		ParentIDField: "proposal_id",
		List:          listProjectionAdditionalFiles,
		Create:        createProjectionAdditionalFile,
		GetOne:        getProjectionAdditionalFile,
		GetMany:       getProjectionAdditionalFiles,
		SoftDelete:    deleteProjectionAdditionalFile,
	}
}

func listProjectionAdditionalFiles(ctx context.Context, pool *pgxpool.Pool, parentID string) ([]additionalfiles.FileRecord, error) {
	names, err := projectionEntityNames(ctx)
	if err != nil {
		return nil, err
	}
	return additionalfiles.QueryFiles(ctx, pool, projectionFileQuery(`
		WHERE f.proposal_id = $1
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		  AND LOWER(TRIM(i.entity_name)) = ANY($2)
		ORDER BY f.uploaded_at DESC
	`), parentID, names)
}

func createProjectionAdditionalFile(ctx context.Context, tx pgx.Tx, input additionalfiles.CreateInput) error {
	names, err := projectionEntityNames(ctx)
	if err != nil {
		return err
	}
	return additionalfiles.InsertAdditionalFileRow(ctx, tx, "cimplrcorpsaas.cashflow_proposal_files", "proposal_id", input, `
		SELECT DISTINCT p.proposal_id AS parent_id
		FROM cimplrcorpsaas.cashflow_proposal p
		JOIN cimplrcorpsaas.cashflow_proposal_item i ON i.proposal_id = p.proposal_id
		WHERE p.proposal_id = $8
		  AND LOWER(TRIM(i.entity_name)) = ANY($9)
	`, input.ParentID, names)
}

func getProjectionAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*additionalfiles.FileRecord, error) {
	names, err := projectionEntityNames(ctx)
	if err != nil {
		return nil, err
	}
	return additionalfiles.FirstFile(ctx, pool, projectionFileQuery(`
		WHERE f.proposal_id = $1
		  AND f.file_id = $2
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		  AND LOWER(TRIM(i.entity_name)) = ANY($3)
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
		  AND LOWER(TRIM(i.entity_name)) = ANY($3)
		ORDER BY f.uploaded_at DESC
	`), parentID, trimmedIDs, names)
	if queryErr != nil {
		return nil, nil, queryErr
	}
	return files, missingProjectionAdditionalFileIDs(trimmedIDs, files), nil
}

func deleteProjectionAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	names, err := projectionEntityNames(ctx)
	if err != nil {
		return false, err
	}
	result, execErr := pool.Exec(ctx, `
		UPDATE cimplrcorpsaas.cashflow_proposal_files f
		SET is_deleted = TRUE,
		    deleted_by = $3,
		    deleted_at = $4
		FROM cimplrcorpsaas.cashflow_proposal_item i
		WHERE f.proposal_id = $1
		  AND f.file_id = $2
		  AND i.proposal_id = f.proposal_id
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		  AND LOWER(TRIM(i.entity_name)) = ANY($5)
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
		JOIN cimplrcorpsaas.cashflow_proposal_item i ON i.proposal_id = p.proposal_id
	` + whereClause
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
		return nil, errors.New("no accessible business units found")
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
