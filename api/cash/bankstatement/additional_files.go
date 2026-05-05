package bankstatement

import (
	api "CimplrCorpSaas/api"
	"CimplrCorpSaas/api/cash/additionalfiles"
	"context"
	"errors"
	"strings"
	"time"

	"net/http"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func ListAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewListHandler(pool, bankStatementAdditionalFilesConfig())
}

func UploadAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewUploadHandler(pool, bankStatementAdditionalFilesConfig())
}

func DownloadAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDownloadHandler(pool, bankStatementAdditionalFilesConfig())
}

func DownloadSelectedAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDownloadSelectedHandler(pool, bankStatementAdditionalFilesConfig())
}

func DeleteAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDeleteHandler(pool, bankStatementAdditionalFilesConfig())
}

func bankStatementAdditionalFilesConfig() additionalfiles.Config {
	return additionalfiles.Config{
		Module:        "bankstatement",
		ParentIDField: "bank_statement_id",
		List:          listBankStatementAdditionalFiles,
		Create:        createBankStatementAdditionalFile,
		GetOne:        getBankStatementAdditionalFile,
		GetMany:       getBankStatementAdditionalFiles,
		SoftDelete:    deleteBankStatementAdditionalFile,
	}
}

func listBankStatementAdditionalFiles(ctx context.Context, pool *pgxpool.Pool, parentID string) ([]additionalfiles.FileRecord, error) {
	entityIDs := api.GetEntityIDsFromCtx(ctx)
	if len(entityIDs) == 0 {
		return nil, errors.New("no accessible business units found")
	}

	return additionalfiles.QueryFiles(ctx, pool, `
		SELECT f.file_id, f.stored_file_name, f.content_type, f.file_size, f.upload_s3_key, f.uploaded_by, f.uploaded_at
		FROM cimplrcorpsaas.bank_statement_files f
		JOIN cimplrcorpsaas.bank_statements s ON s.bank_statement_id = f.bank_statement_id
		WHERE f.bank_statement_id = $1
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		  AND s.entity_id = ANY($2)
		ORDER BY f.uploaded_at DESC
	`, parentID, entityIDs)
}

func createBankStatementAdditionalFile(ctx context.Context, tx pgx.Tx, input additionalfiles.CreateInput) error {
	entityIDs := api.GetEntityIDsFromCtx(ctx)
	if len(entityIDs) == 0 {
		return errors.New("no accessible business units found")
	}

	return additionalfiles.InsertAdditionalFileRow(
		ctx,
		tx,
		"cimplrcorpsaas.bank_statement_files",
		"bank_statement_id",
		input,
		`SELECT s.bank_statement_id AS parent_id
		 FROM cimplrcorpsaas.bank_statements s
		 WHERE s.bank_statement_id = $8
		   AND s.entity_id = ANY($9)`,
		input.ParentID,
		entityIDs,
	)
}

func getBankStatementAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*additionalfiles.FileRecord, error) {
	entityIDs := api.GetEntityIDsFromCtx(ctx)
	if len(entityIDs) == 0 {
		return nil, errors.New("no accessible business units found")
	}

	return additionalfiles.FirstFile(ctx, pool, `
		SELECT f.file_id, f.stored_file_name, f.content_type, f.file_size, f.upload_s3_key, f.uploaded_by, f.uploaded_at
		FROM cimplrcorpsaas.bank_statement_files f
		JOIN cimplrcorpsaas.bank_statements s ON s.bank_statement_id = f.bank_statement_id
		WHERE f.bank_statement_id = $1
		  AND f.file_id = $2
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		  AND s.entity_id = ANY($3)
	`, parentID, fileID, entityIDs)
}

func getBankStatementAdditionalFiles(ctx context.Context, pool *pgxpool.Pool, parentID string, fileIDs []string) ([]additionalfiles.FileRecord, []string, error) {
	entityIDs := api.GetEntityIDsFromCtx(ctx)
	if len(entityIDs) == 0 {
		return nil, nil, errors.New("no accessible business units found")
	}

	trimmedIDs := trimAdditionalFileIDs(fileIDs)
	files, err := additionalfiles.QueryFiles(ctx, pool, `
		SELECT f.file_id, f.stored_file_name, f.content_type, f.file_size, f.upload_s3_key, f.uploaded_by, f.uploaded_at
		FROM cimplrcorpsaas.bank_statement_files f
		JOIN cimplrcorpsaas.bank_statements s ON s.bank_statement_id = f.bank_statement_id
		WHERE f.bank_statement_id = $1
		  AND f.file_id = ANY($2)
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		  AND s.entity_id = ANY($3)
		ORDER BY f.uploaded_at DESC
	`, parentID, trimmedIDs, entityIDs)
	if err != nil {
		return nil, nil, err
	}

	return files, missingAdditionalFileIDs(trimmedIDs, files), nil
}

func deleteBankStatementAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	entityIDs := api.GetEntityIDsFromCtx(ctx)
	if len(entityIDs) == 0 {
		return false, errors.New("no accessible business units found")
	}

	result, err := pool.Exec(ctx, `
		UPDATE cimplrcorpsaas.bank_statement_files f
		SET is_deleted = TRUE,
		    deleted_by = $3,
		    deleted_at = $4
		FROM cimplrcorpsaas.bank_statements s
		WHERE f.bank_statement_id = $1
		  AND f.file_id = $2
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		  AND s.bank_statement_id = f.bank_statement_id
		  AND s.entity_id = ANY($5)
	`, parentID, fileID, deletedBy, deletedAt, entityIDs)
	if err != nil {
		return false, err
	}
	return result.RowsAffected() > 0, nil
}

func trimAdditionalFileIDs(fileIDs []string) []string {
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

func missingAdditionalFileIDs(expected []string, files []additionalfiles.FileRecord) []string {
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
