package counterpartyHub

import (
	"CimplrCorpSaas/api/cash/additionalfiles"
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// counterpartyHubFilesHandlers groups the 5 HTTP handlers for counterparty hub DMS.
type counterpartyHubFilesHandlers struct {
	List          http.HandlerFunc
	Upload        http.HandlerFunc
	Download      http.HandlerFunc
	DownloadBulk  http.HandlerFunc
	Delete        http.HandlerFunc
	Audit         http.HandlerFunc
	ApproveDelete http.HandlerFunc
	RejectDelete  http.HandlerFunc
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
		AuditSource:   "MASTER_COUNTERPARTY_HUB",
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
			_, err := createCounterpartyHubAdditionalFile(ctx, tx, input, parentTable, parentCol, filesTable)
			return err
		},
		CreateReturning: func(ctx context.Context, tx pgx.Tx, input additionalfiles.CreateInput) (string, error) {
			return createCounterpartyHubAdditionalFile(ctx, tx, input, parentTable, parentCol, filesTable)
		},
		GetOne: func(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*additionalfiles.FileRecord, error) {
			return getCounterpartyHubAdditionalFile(ctx, pool, parentID, fileID, parentCol, filesTable, false)
		},
		GetAnyFile: func(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*additionalfiles.FileRecord, error) {
			return getCounterpartyHubAdditionalFile(ctx, pool, parentID, fileID, parentCol, filesTable, true)
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
			return deleteCounterpartyHubAdditionalFile(ctx, pool, counterpartyDeleteFileParams{ParentID: parentID, FileID: fileID, DeletedBy: deletedBy, DeletedAt: deletedAt, ParentCol: parentCol, FilesTable: filesTable})
		},
		SoftDeleteTx: func(ctx context.Context, tx pgx.Tx, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
			return deleteCounterpartyHubAdditionalFile(ctx, tx, counterpartyDeleteFileParams{ParentID: parentID, FileID: fileID, DeletedBy: deletedBy, DeletedAt: deletedAt, ParentCol: parentCol, FilesTable: filesTable})
		},
	}

	return counterpartyHubFilesHandlers{
		List:          additionalfiles.NewListHandler(pool, cfg),
		Upload:        additionalfiles.NewUploadHandler(pool, cfg),
		Download:      additionalfiles.NewDownloadHandler(pool, cfg),
		DownloadBulk:  additionalfiles.NewDownloadSelectedHandler(pool, cfg),
		Delete:        additionalfiles.NewDeleteHandler(pool, cfg),
		Audit:         additionalfiles.NewAuditHandler(pool, cfg),
		ApproveDelete: additionalfiles.NewApproveDeleteHandler(pool, cfg),
		RejectDelete:  additionalfiles.NewRejectDeleteHandler(pool, cfg),
	}
}

func createCounterpartyHubAdditionalFile(ctx context.Context, tx pgx.Tx, input additionalfiles.CreateInput, parentTable, parentCol, filesTable string) (string, error) {
	parentScope := fmt.Sprintf(
		`SELECT %s AS parent_id FROM %s WHERE %s = $8`,
		parentCol, parentTable, parentCol,
	)
	return additionalfiles.InsertAdditionalFileRowReturningID(ctx, tx, filesTable, parentCol, input, parentScope, input.ParentID)
}

func getCounterpartyHubAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID, parentCol, filesTable string, includeDeleted bool) (*additionalfiles.FileRecord, error) {
	deletedClause := "AND COALESCE(is_deleted, FALSE) = FALSE"
	if includeDeleted {
		deletedClause = ""
	}
	q := `SELECT file_id, stored_file_name, content_type, file_size, upload_s3_key, uploaded_by, uploaded_at
	      FROM ` + filesTable + `
	      WHERE ` + parentCol + ` = $1 AND file_id = $2 ` + deletedClause
	return additionalfiles.FirstFile(ctx, pool, q, parentID, fileID)
}

type counterpartyHubFileExec interface {
	Exec(context.Context, string, ...interface{}) (pgconn.CommandTag, error)
}

type counterpartyDeleteFileParams struct {
	ParentID   string
	FileID     string
	DeletedBy  string
	DeletedAt  time.Time
	ParentCol  string
	FilesTable string
}

func deleteCounterpartyHubAdditionalFile(ctx context.Context, exec counterpartyHubFileExec, p counterpartyDeleteFileParams) (bool, error) {
	q := `UPDATE ` + p.FilesTable + `
	      SET is_deleted = TRUE, deleted_by = $3, deleted_at = $4
	      WHERE ` + p.ParentCol + ` = $1 AND file_id = $2 AND COALESCE(is_deleted, FALSE) = FALSE`
	result, err := exec.Exec(ctx, q, p.ParentID, p.FileID, p.DeletedBy, p.DeletedAt)
	if err != nil {
		return false, err
	}
	return result.RowsAffected() > 0, nil
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
