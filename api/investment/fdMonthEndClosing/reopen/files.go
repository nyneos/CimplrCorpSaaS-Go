package reopen

import (
	"context"
	"strings"
	"time"

	cashfiles "CimplrCorpSaas/api/cash/additionalfiles"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// reopenFilesModule is the module_key stamped onto every audit row this
// config produces via the shared investment.additional_file_audit table
// (module_key = 'fd-closing-reopen-additional', per the handler spec's
// Section 5) — same shared audit table 19 other FD/investment modules
// already use (fd-accrual-run-additional, fd-interest-receipt-additional,
// etc., see api/investment/additionalfiles/investmentFiles.go).
const reopenFilesModule = "fd-closing-reopen-additional"

const reopenFilesTable = "investment.fd_closing_reopen_request_files"

// reopenFilesConfig builds the cashfiles.Config for reopen-request evidence
// attachments. This is a thin, self-contained delegation to the shared
// generic upload engine (api/cash/additionalfiles) — mirroring the exact
// shape api/investment/additionalfiles/investmentFiles.go's own
// investmentAdditionalFilesConfig/listInvestmentAdditionalFiles/etc. use for
// every other FD sub-module's "_files" table, reimplemented locally here
// instead of editing that shared file (which sibling agents are touching
// concurrently) or its unexported helpers (which are not reachable from this
// package).
func reopenFilesConfig() cashfiles.Config {
	return cashfiles.Config{
		Module:         reopenFilesModule,
		AuditSource:    "FD_CLOSING_REOPEN_ADDITIONAL",
		AuditTableName: "investment.additional_file_audit",
		ParentIDField:  "request_id",
		List: func(ctx context.Context, pool *pgxpool.Pool, parentID string) ([]cashfiles.FileRecord, error) {
			return listReopenFiles(ctx, pool, parentID)
		},
		Create: func(ctx context.Context, tx pgx.Tx, input cashfiles.CreateInput) error {
			_, err := createReopenFileReturningID(ctx, tx, input)
			return err
		},
		CreateReturning: func(ctx context.Context, tx pgx.Tx, input cashfiles.CreateInput) (string, error) {
			return createReopenFileReturningID(ctx, tx, input)
		},
		GetOne: func(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*cashfiles.FileRecord, error) {
			return getReopenFile(ctx, pool, parentID, fileID, false)
		},
		GetAnyFile: func(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*cashfiles.FileRecord, error) {
			return getReopenFile(ctx, pool, parentID, fileID, true)
		},
		GetMany: func(ctx context.Context, pool *pgxpool.Pool, parentID string, fileIDs []string) ([]cashfiles.FileRecord, []string, error) {
			return getReopenFiles(ctx, pool, parentID, fileIDs)
		},
		SoftDelete: func(ctx context.Context, pool *pgxpool.Pool, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
			return deleteReopenFile(ctx, pool, parentID, fileID, deletedBy, deletedAt)
		},
		SoftDeleteTx: func(ctx context.Context, tx pgx.Tx, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
			return deleteReopenFile(ctx, tx, parentID, fileID, deletedBy, deletedAt)
		},
	}
}

func listReopenFiles(ctx context.Context, pool *pgxpool.Pool, parentID string) ([]cashfiles.FileRecord, error) {
	const q = `
		SELECT f.file_id, f.stored_file_name, f.content_type, f.file_size, f.upload_s3_key, f.uploaded_by, f.uploaded_at
		FROM investment.fd_closing_reopen_request_files f
		JOIN investment.fd_closing_reopen_request p ON p.request_id = f.request_id
		WHERE f.request_id::text = $1
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		ORDER BY f.uploaded_at DESC`
	return cashfiles.QueryFiles(ctx, pool, q, strings.TrimSpace(parentID))
}

func createReopenFileReturningID(ctx context.Context, tx pgx.Tx, input cashfiles.CreateInput) (string, error) {
	const parentScope = `
		SELECT p.request_id AS parent_id
		FROM investment.fd_closing_reopen_request p
		WHERE p.request_id::text = $8`
	return cashfiles.InsertAdditionalFileRowReturningID(ctx, tx, reopenFilesTable, "request_id", input, parentScope, strings.TrimSpace(input.ParentID))
}

func getReopenFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string, includeDeleted bool) (*cashfiles.FileRecord, error) {
	deletedClause := "AND COALESCE(f.is_deleted, FALSE) = FALSE"
	if includeDeleted {
		deletedClause = ""
	}
	q := `
		SELECT f.file_id, f.stored_file_name, f.content_type, f.file_size, f.upload_s3_key, f.uploaded_by, f.uploaded_at
		FROM investment.fd_closing_reopen_request_files f
		JOIN investment.fd_closing_reopen_request p ON p.request_id = f.request_id
		WHERE f.request_id::text = $1
		  AND f.file_id = $2
		  ` + deletedClause
	return cashfiles.FirstFile(ctx, pool, q, strings.TrimSpace(parentID), strings.TrimSpace(fileID))
}

func getReopenFiles(ctx context.Context, pool *pgxpool.Pool, parentID string, fileIDs []string) ([]cashfiles.FileRecord, []string, error) {
	trimmed := make([]string, 0, len(fileIDs))
	seen := make(map[string]struct{}, len(fileIDs))
	for _, id := range fileIDs {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		trimmed = append(trimmed, id)
	}

	const q = `
		SELECT f.file_id, f.stored_file_name, f.content_type, f.file_size, f.upload_s3_key, f.uploaded_by, f.uploaded_at
		FROM investment.fd_closing_reopen_request_files f
		JOIN investment.fd_closing_reopen_request p ON p.request_id = f.request_id
		WHERE f.request_id::text = $1
		  AND f.file_id = ANY($2)
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		ORDER BY f.uploaded_at DESC`
	files, err := cashfiles.QueryFiles(ctx, pool, q, strings.TrimSpace(parentID), trimmed)
	if err != nil {
		return nil, nil, err
	}

	found := make(map[string]struct{}, len(files))
	for _, f := range files {
		found[f.FileID] = struct{}{}
	}
	missing := make([]string, 0)
	for _, id := range trimmed {
		if _, ok := found[id]; !ok {
			missing = append(missing, id)
		}
	}
	return files, missing, nil
}

// reopenFileExec is satisfied by both *pgxpool.Pool (SoftDelete) and pgx.Tx
// (SoftDeleteTx) — mirrors investmentFiles.go's investmentFileExec.
type reopenFileExec interface {
	Exec(ctx context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error)
}

func deleteReopenFile(ctx context.Context, exec reopenFileExec, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	const q = `
		UPDATE investment.fd_closing_reopen_request_files f
		SET is_deleted = TRUE,
		    deleted_by = $3,
		    deleted_at = $4
		FROM investment.fd_closing_reopen_request p
		WHERE f.request_id::text = $1
		  AND f.file_id = $2
		  AND p.request_id = f.request_id
		  AND COALESCE(f.is_deleted, FALSE) = FALSE`
	result, err := exec.Exec(ctx, q, strings.TrimSpace(parentID), strings.TrimSpace(fileID), deletedBy, deletedAt)
	if err != nil {
		return false, err
	}
	return result.RowsAffected() > 0, nil
}
