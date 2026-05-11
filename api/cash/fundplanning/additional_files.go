package fundplanning

import (
	api "CimplrCorpSaas/api"
	"CimplrCorpSaas/api/cash/additionalfiles"
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func ListAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewListHandler(pool, fundPlanAdditionalFilesConfig())
}

func UploadAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewUploadHandler(pool, fundPlanAdditionalFilesConfig())
}

func DownloadAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDownloadHandler(pool, fundPlanAdditionalFilesConfig())
}

func DownloadSelectedAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDownloadSelectedHandler(pool, fundPlanAdditionalFilesConfig())
}

func DeleteAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDeleteHandler(pool, fundPlanAdditionalFilesConfig())
}

func fundPlanAdditionalFilesConfig() additionalfiles.Config {
	return additionalfiles.Config{
		Module:        "fund-planning",
		ParentIDField: "plan_id",
		FolderName:    additionalfiles.AdditionalFilesFolder(),
		List:          listFundPlanAdditionalFiles,
		Create:        createFundPlanAdditionalFile,
		GetOne:        getFundPlanAdditionalFile,
		GetMany:       getFundPlanAdditionalFiles,
		SoftDelete:    deleteFundPlanAdditionalFile,
	}
}

func listFundPlanAdditionalFiles(ctx context.Context, pool *pgxpool.Pool, parentID string) ([]additionalfiles.FileRecord, error) {
	query, args := fundPlanFilesQuery(ctx, `
		WHERE f.plan_id = $1
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
	`, parentID)
	query += `
		ORDER BY f.uploaded_at DESC
	`
	return additionalfiles.QueryFiles(ctx, pool, query, args...)
}

func createFundPlanAdditionalFile(ctx context.Context, tx pgx.Tx, input additionalfiles.CreateInput) error {
	entityClause, entityArgs := fundPlanEntityScope(ctx, 9)
	query := `
		INSERT INTO public.fund_plan_files (
			plan_id,
			group_id,
			stored_file_name,
			content_type,
			file_size,
			file_hash,
			upload_s3_key,
			uploaded_by,
			uploaded_at,
			is_deleted
		)
		SELECT
			fpg.plan_id,
			fpg.group_id,
			$1,
			$2,
			$3,
			$4,
			$5,
			$6,
			$7,
			FALSE
		FROM public.fund_plan_groups fpg
		WHERE fpg.plan_id = $8
	` + entityClause + `
		ORDER BY fpg.group_id
		LIMIT 1
	`

	args := []interface{}{
		input.StoredFileName,
		input.ContentType,
		input.FileSize,
		input.FileHash,
		input.UploadS3Key,
		input.UploadedBy,
		input.UploadedAt,
		input.ParentID,
	}
	args = append(args, entityArgs...)

	result, err := tx.Exec(ctx, query, args...)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

func getFundPlanAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*additionalfiles.FileRecord, error) {
	query, args := fundPlanFilesQuery(ctx, `
		WHERE f.plan_id = $1
		  AND f.file_id = $2
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
	`, parentID, fileID)
	return additionalfiles.FirstFile(ctx, pool, query, args...)
}

func getFundPlanAdditionalFiles(ctx context.Context, pool *pgxpool.Pool, parentID string, fileIDs []string) ([]additionalfiles.FileRecord, []string, error) {
	trimmedIDs := trimFundPlanAdditionalFileIDs(fileIDs)
	query, args := fundPlanFilesQuery(ctx, `
		WHERE f.plan_id = $1
		  AND f.file_id = ANY($2)
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
	`, parentID, trimmedIDs)
	query += `
		ORDER BY f.uploaded_at DESC
	`

	files, err := additionalfiles.QueryFiles(ctx, pool, query, args...)
	if err != nil {
		return nil, nil, err
	}
	return files, missingFundPlanAdditionalFileIDs(trimmedIDs, files), nil
}

func deleteFundPlanAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	entityClause, entityArgs := fundPlanEntityScope(ctx, 5)
	query := `
		UPDATE public.fund_plan_files f
		SET is_deleted = TRUE,
		    deleted_by = $3,
		    deleted_at = $4
		FROM public.fund_plan_groups fpg
		WHERE f.plan_id = $1
		  AND f.file_id = $2
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		  AND fpg.group_id = f.group_id
	` + entityClause

	args := []interface{}{parentID, fileID, deletedBy, deletedAt}
	args = append(args, entityArgs...)
	result, err := pool.Exec(ctx, query, args...)
	if err != nil {
		return false, err
	}
	return result.RowsAffected() > 0, nil
}

func fundPlanFilesQuery(ctx context.Context, whereClause string, baseArgs ...interface{}) (string, []interface{}) {
	entityClause, entityArgs := fundPlanEntityScope(ctx, len(baseArgs)+1)
	query := `
		SELECT DISTINCT f.file_id, f.stored_file_name, f.content_type, f.file_size, f.upload_s3_key, f.uploaded_by, f.uploaded_at
		FROM public.fund_plan_files f
		JOIN public.fund_plan_groups fpg ON fpg.group_id = f.group_id
	` + whereClause + entityClause

	args := append([]interface{}{}, baseArgs...)
	args = append(args, entityArgs...)
	return query, args
}

func fundPlanEntityScope(ctx context.Context, position int) (string, []interface{}) {
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
	return fmt.Sprintf(" AND LOWER(TRIM(COALESCE(fpg.entity_name, ''))) = ANY($%d)", position), []interface{}{lowered}
}

func trimFundPlanAdditionalFileIDs(fileIDs []string) []string {
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

func missingFundPlanAdditionalFileIDs(expected []string, files []additionalfiles.FileRecord) []string {
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
