package counterpartyHub

import (
	"CimplrCorpSaas/api/cash/additionalfiles"
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// counterpartyHubFilesHandlers groups the 5 HTTP handlers for counterparty hub DMS.
type counterpartyHubFilesHandlers struct {
	List         http.HandlerFunc
	Upload       http.HandlerFunc
	Download     http.HandlerFunc
	DownloadBulk http.HandlerFunc
	Delete       http.HandlerFunc
}

// CounterpartyHubFilesHandlers builds all five additional-files handlers for
// the counterparty hub, keyed by counterparty_id in apibox_svc.counterparty.
func CounterpartyHubFilesHandlers(pool *pgxpool.Pool) counterpartyHubFilesHandlers {
	const (
		moduleKey   = "master-counterparty-hub"
		parentField = "counterparty_id"
		parentTable = "apibox_svc.counterparty"
		parentCol   = "counterparty_id"
		filesTable  = "cimplrcorpsaas.counterparty_hub_files"
	)

	cfg := additionalfiles.Config{
		Module:        moduleKey,
		ParentIDField: parentField,
		List: func(ctx context.Context, pool *pgxpool.Pool, parentID string) ([]additionalfiles.FileRecord, error) {
			q := `SELECT file_id, stored_file_name, content_type, file_size, upload_s3_key, uploaded_by, uploaded_at
			      FROM ` + filesTable + `
			      WHERE ` + parentCol + ` = $1 AND COALESCE(is_deleted, FALSE) = FALSE
			      ORDER BY uploaded_at DESC`
			return additionalfiles.QueryFiles(ctx, pool, q, parentID)
		},
		Create: func(ctx context.Context, tx pgx.Tx, input additionalfiles.CreateInput) error {
			parentScope := fmt.Sprintf(
				`SELECT %s AS parent_id FROM %s WHERE %s = $8`,
				parentCol, parentTable, parentCol,
			)
			return additionalfiles.InsertAdditionalFileRow(ctx, tx, filesTable, parentCol, input, parentScope, input.ParentID)
		},
		GetOne: func(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*additionalfiles.FileRecord, error) {
			q := `SELECT file_id, stored_file_name, content_type, file_size, upload_s3_key, uploaded_by, uploaded_at
			      FROM ` + filesTable + `
			      WHERE ` + parentCol + ` = $1 AND file_id = $2 AND COALESCE(is_deleted, FALSE) = FALSE`
			return additionalfiles.FirstFile(ctx, pool, q, parentID, fileID)
		},
		GetMany: func(ctx context.Context, pool *pgxpool.Pool, parentID string, fileIDs []string) ([]additionalfiles.FileRecord, []string, error) {
			trimmed := trimCounterpartyHubFileIDs(fileIDs)
			q := `SELECT file_id, stored_file_name, content_type, file_size, upload_s3_key, uploaded_by, uploaded_at
			      FROM ` + filesTable + `
			      WHERE ` + parentCol + ` = $1 AND file_id = ANY($2) AND COALESCE(is_deleted, FALSE) = FALSE
			      ORDER BY uploaded_at DESC`
			files, err := additionalfiles.QueryFiles(ctx, pool, q, parentID, trimmed)
			if err != nil {
				return nil, nil, err
			}
			return files, missingCounterpartyHubFileIDs(trimmed, files), nil
		},
		SoftDelete: func(ctx context.Context, pool *pgxpool.Pool, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
			q := `UPDATE ` + filesTable + `
			      SET is_deleted = TRUE, deleted_by = $3, deleted_at = $4
			      WHERE ` + parentCol + ` = $1 AND file_id = $2 AND COALESCE(is_deleted, FALSE) = FALSE`
			result, err := pool.Exec(ctx, q, parentID, fileID, deletedBy, deletedAt)
			if err != nil {
				return false, err
			}
			return result.RowsAffected() > 0, nil
		},
	}

	return counterpartyHubFilesHandlers{
		List:         additionalfiles.NewListHandler(pool, cfg),
		Upload:       additionalfiles.NewUploadHandler(pool, cfg),
		Download:     additionalfiles.NewDownloadHandler(pool, cfg),
		DownloadBulk: additionalfiles.NewDownloadSelectedHandler(pool, cfg),
		Delete:       additionalfiles.NewDeleteHandler(pool, cfg),
	}
}

func trimCounterpartyHubFileIDs(fileIDs []string) []string {
	trimmed := make([]string, 0, len(fileIDs))
	seen := make(map[string]struct{}, len(fileIDs))
	for _, id := range fileIDs {
		candidate := strings.TrimSpace(id)
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

func missingCounterpartyHubFileIDs(expected []string, files []additionalfiles.FileRecord) []string {
	found := make(map[string]struct{}, len(files))
	for _, f := range files {
		found[f.FileID] = struct{}{}
	}
	missing := make([]string, 0)
	for _, id := range expected {
		if _, ok := found[id]; !ok {
			missing = append(missing, id)
		}
	}
	return missing
}
