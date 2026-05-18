package exposures

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

func DeleteAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDeleteHandler(pool, exposureAdditionalFilesConfig())
}

func exposureAdditionalFilesConfig() additionalfiles.Config {
	return additionalfiles.Config{
		Module:        "fx-exposure",
		ParentIDField: "exposure_header_id",
		List:          listExposureAdditionalFiles,
		Create:        createExposureAdditionalFile,
		GetOne:        getExposureAdditionalFile,
		GetMany:       getExposureAdditionalFiles,
		SoftDelete:    deleteExposureAdditionalFile,
	}
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

func createExposureAdditionalFile(ctx context.Context, tx pgx.Tx, input additionalfiles.CreateInput) error {
	names, err := fxEntityNames(ctx)
	if err != nil {
		return err
	}
	return additionalfiles.InsertAdditionalFileRow(ctx, tx, "public.exposure_header_files", "exposure_header_id", input, `
		SELECT h.exposure_header_id AS parent_id
		FROM public.exposure_headers h
		WHERE h.exposure_header_id::text = $8
		  AND LOWER(TRIM(h.entity)) = ANY($9)
	`, input.ParentID, names)
}

func getExposureAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*additionalfiles.FileRecord, error) {
	names, err := fxEntityNames(ctx)
	if err != nil {
		return nil, err
	}
	return additionalfiles.FirstFile(ctx, pool, exposureFileQuery(`
		WHERE f.exposure_header_id::text = $1
		  AND f.file_id::text = $2
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
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
	names, err := fxEntityNames(ctx)
	if err != nil {
		return false, err
	}
	result, execErr := pool.Exec(ctx, `
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
		return nil, errors.New("no accessible business units found")
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
