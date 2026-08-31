package checklist

import (
	"context"
	"net/http"
	"strings"
	"time"

	cashfiles "CimplrCorpSaas/api/cash/additionalfiles"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Checklist-item document attachments delegate entirely to the shared,
// already-live generic upload/delete-approval engine in
// api/cash/additionalfiles (the same engine api/investment/additionalfiles's
// FD Booking/Confirmation/etc. definitions configure against) — this file is
// ONLY a thin config + a handful of query functions the engine's Config
// struct needs, per the handler spec's Section 3 file rows. It deliberately
// does NOT touch api/investment/additionalfiles/investmentFiles.go (the
// shared registry file other sibling agents are editing concurrently);
// module_key='fd-closing-checklist-additional' is registered here, entirely
// package-local.
const (
	checklistFilesModule       = "fd-closing-checklist-additional"
	checklistFilesTable        = "investment.fd_closing_checklist_item_files"
	checklistFilesParentColumn = "item_id"
	// checklistFilesAuditTable is the shared cross-module audit table every
	// other investment/*-additional module already writes to (keyed by
	// module_key) — no new audit table needed for this one, per the migration's
	// own comment on fd_closing_checklist_item_files.
	checklistFilesAuditTable = "investment.additional_file_audit"
)

// checklistFileExec is satisfied by both *pgxpool.Pool and pgx.Tx — the same
// minimal Exec-only interface investmentFiles.go's investmentFileExec uses,
// so deleteChecklistFile can back both Config.SoftDelete (pool) and
// Config.SoftDeleteTx (tx) with one implementation.
type checklistFileExec interface {
	Exec(context.Context, string, ...interface{}) (pgconn.CommandTag, error)
}

// checklistFilesConfig builds the cashfiles.Config for this module. No
// RecordMainUploadAudit/RecordMainDownloadAudit is set — checklist items have
// no per-module "main" audit table of their own for file events (unlike e.g.
// FD Booking's fd_audit_booking_request); the shared additional_file_audit
// table (via AuditSource/AuditTableName below) is the complete trail, same as
// fdAccrualRunFilesDefinition's configuration in investmentFiles.go.
func checklistFilesConfig() cashfiles.Config {
	return cashfiles.Config{
		Module:         checklistFilesModule,
		AuditSource:    strings.ToUpper(strings.ReplaceAll(checklistFilesModule, "-", "_")),
		AuditTableName: checklistFilesAuditTable,
		ParentIDField:  checklistFilesParentColumn,
		List: func(ctx context.Context, pool *pgxpool.Pool, parentID string) ([]cashfiles.FileRecord, error) {
			return listChecklistFiles(ctx, pool, parentID)
		},
		CreateReturning: func(ctx context.Context, tx pgx.Tx, input cashfiles.CreateInput) (string, error) {
			return createChecklistFile(ctx, tx, input)
		},
		GetOne: func(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*cashfiles.FileRecord, error) {
			return getChecklistFile(ctx, pool, parentID, fileID, false)
		},
		GetAnyFile: func(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*cashfiles.FileRecord, error) {
			return getChecklistFile(ctx, pool, parentID, fileID, true)
		},
		GetMany: func(ctx context.Context, pool *pgxpool.Pool, parentID string, fileIDs []string) ([]cashfiles.FileRecord, []string, error) {
			return getChecklistFiles(ctx, pool, parentID, fileIDs)
		},
		SoftDelete: func(ctx context.Context, pool *pgxpool.Pool, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
			return deleteChecklistFile(ctx, pool, parentID, fileID, deletedBy, deletedAt)
		},
		SoftDeleteTx: func(ctx context.Context, tx pgx.Tx, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
			return deleteChecklistFile(ctx, tx, parentID, fileID, deletedBy, deletedAt)
		},
	}
}

func listChecklistFiles(ctx context.Context, pool *pgxpool.Pool, parentID string) ([]cashfiles.FileRecord, error) {
	return cashfiles.QueryFiles(ctx, pool, `
		SELECT f.file_id, f.stored_file_name, f.content_type, f.file_size, f.upload_s3_key, f.uploaded_by, f.uploaded_at
		FROM investment.fd_closing_checklist_item_files f
		JOIN investment.fd_closing_checklist_item p ON p.item_id = f.item_id
		WHERE f.item_id::text = $1
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		ORDER BY f.uploaded_at DESC`,
		strings.TrimSpace(parentID),
	)
}

func createChecklistFile(ctx context.Context, tx pgx.Tx, input cashfiles.CreateInput) (string, error) {
	parentScope := `
		SELECT p.item_id AS parent_id
		FROM investment.fd_closing_checklist_item p
		WHERE p.item_id::text = $8`
	return cashfiles.InsertAdditionalFileRowReturningID(
		ctx, tx, checklistFilesTable, checklistFilesParentColumn, input, parentScope,
		strings.TrimSpace(input.ParentID),
	)
}

func getChecklistFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string, includeDeleted bool) (*cashfiles.FileRecord, error) {
	deletedClause := "AND COALESCE(f.is_deleted, FALSE) = FALSE"
	if includeDeleted {
		deletedClause = ""
	}
	return cashfiles.FirstFile(ctx, pool, `
		SELECT f.file_id, f.stored_file_name, f.content_type, f.file_size, f.upload_s3_key, f.uploaded_by, f.uploaded_at
		FROM investment.fd_closing_checklist_item_files f
		JOIN investment.fd_closing_checklist_item p ON p.item_id = f.item_id
		WHERE f.item_id::text = $1
		  AND f.file_id = $2
		  `+deletedClause,
		strings.TrimSpace(parentID), strings.TrimSpace(fileID),
	)
}

func getChecklistFiles(ctx context.Context, pool *pgxpool.Pool, parentID string, fileIDs []string) ([]cashfiles.FileRecord, []string, error) {
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

	files, err := cashfiles.QueryFiles(ctx, pool, `
		SELECT f.file_id, f.stored_file_name, f.content_type, f.file_size, f.upload_s3_key, f.uploaded_by, f.uploaded_at
		FROM investment.fd_closing_checklist_item_files f
		JOIN investment.fd_closing_checklist_item p ON p.item_id = f.item_id
		WHERE f.item_id::text = $1
		  AND f.file_id = ANY($2)
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		ORDER BY f.uploaded_at DESC`,
		strings.TrimSpace(parentID), trimmed,
	)
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

func deleteChecklistFile(ctx context.Context, exec checklistFileExec, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	result, err := exec.Exec(ctx, `
		UPDATE investment.fd_closing_checklist_item_files
		SET is_deleted = TRUE, deleted_by = $3, deleted_at = $4
		WHERE item_id = $1 AND file_id = $2 AND COALESCE(is_deleted, FALSE) = FALSE`,
		strings.TrimSpace(parentID), strings.TrimSpace(fileID), deletedBy, deletedAt,
	)
	if err != nil {
		return false, err
	}
	return result.RowsAffected() > 0, nil
}

// UploadChecklistFilesHandler handles POST /investment/fd-closing/checklist/files/upload.
func UploadChecklistFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewUploadHandler(pool, checklistFilesConfig())
}

// DeleteChecklistFileHandler handles POST /investment/fd-closing/checklist/files/delete.
// Immediately inserts a PENDING_DELETE_APPROVAL row into the shared
// investment.additional_file_audit table — the file row's is_deleted only
// flips once ApproveDeleteChecklistFileHandler runs.
func DeleteChecklistFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDeleteHandler(pool, checklistFilesConfig())
}

// ApproveDeleteChecklistFileHandler handles POST /investment/fd-closing/checklist/files/approve-delete.
func ApproveDeleteChecklistFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewApproveDeleteHandler(pool, checklistFilesConfig())
}

// RejectDeleteChecklistFileHandler handles POST /investment/fd-closing/checklist/files/reject-delete.
func RejectDeleteChecklistFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewRejectDeleteHandler(pool, checklistFilesConfig())
}

// DownloadChecklistFileHandler handles POST /investment/fd-closing/checklist/files/download.
// Records a synchronous DOWNLOAD/PREVIEW audit row, COMPLETED immediately.
func DownloadChecklistFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDownloadHandler(pool, checklistFilesConfig())
}

// ListChecklistFilesHandler handles POST /investment/fd-closing/checklist/files/list.
func ListChecklistFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewListHandler(pool, checklistFilesConfig())
}

// DownloadBulkChecklistFilesHandler handles POST /investment/fd-closing/checklist/files/download-bulk.
func DownloadBulkChecklistFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDownloadSelectedHandler(pool, checklistFilesConfig())
}

// AuditChecklistFilesHandler handles POST /investment/fd-closing/checklist/files/audit.
func AuditChecklistFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewAuditHandler(pool, checklistFilesConfig())
}
