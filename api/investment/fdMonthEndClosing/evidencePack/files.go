package evidencePack

import (
	"context"
	"net/http"
	"strings"
	"time"

	cashfiles "CimplrCorpSaas/api/cash/additionalfiles"
	"CimplrCorpSaas/api/constants"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Manual supporting-document uploads for an evidence pack — a thin config
// wrapper into the SAME generic engine every other FD/investment additional-
// files module uses (api/cash/additionalfiles.Config + its New*Handler
// constructors), same pattern api/investment/additionalfiles/investmentFiles.go
// uses for the other 19 modules. That file is explicitly off-limits for this
// change (see fdMonthEndClosing.go's bootstrap comment — other agents are
// editing shared wiring files concurrently), so the Config + its backing
// List/Create/GetOne/GetMany/SoftDelete queries are self-contained here
// instead of being added as one more entry to investmentFiles.go's switch.
//
// module_key = "fd-closing-evidence-additional", table =
// investment.fd_closing_evidence_pack_files, parent =
// investment.fd_closing_evidence_pack via pack_id — exactly as specified in
// the handler spec's Section 6.

const (
	evidencePackFilesModuleKey = "fd-closing-evidence-additional"
	evidencePackFilesTable     = "investment.fd_closing_evidence_pack_files"
	evidencePackFilesParentCol = "pack_id"

	// evidencePackFilesAuditTable is the SAME shared, already-live audit table
	// every other investment/FD additional-files module writes to (keyed by
	// module_key) — see api/investment/additionalfiles/investmentFiles.go's
	// investmentAdditionalFilesAuditTable const (same literal value,
	// "investment.additional_file_audit"). Duplicated here as a literal
	// rather than imported/reused because that file is off-limits for this
	// change and is being edited concurrently by sibling agents.
	evidencePackFilesAuditTable = "investment.additional_file_audit"
)

// evidencePackFilesConfig builds the cashfiles.Config for the evidence pack's
// supporting-document uploads. RecordMainUploadAudit/RecordMainDownloadAudit
// are deliberately left nil: fd_closing_evidence_pack is append-only with no
// *_audit sibling table of its own (per the migration's design comment), so
// there is no parent-specific audit table to also write into — the shared
// evidencePackFilesAuditTable write (done unconditionally by the engine via
// AuditTableName) is the only audit trail these uploads need.
func evidencePackFilesConfig() cashfiles.Config {
	return cashfiles.Config{
		Module:          evidencePackFilesModuleKey,
		AuditSource:     "FD_CLOSING_EVIDENCE_ADDITIONAL",
		AuditTableName:  evidencePackFilesAuditTable,
		ParentIDField:   evidencePackFilesParentCol,
		List:            listEvidencePackFiles,
		Create:          createEvidencePackFile,
		CreateReturning: createEvidencePackFileReturningID,
		GetOne: func(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*cashfiles.FileRecord, error) {
			return getEvidencePackFile(ctx, pool, parentID, fileID, false)
		},
		GetAnyFile: func(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*cashfiles.FileRecord, error) {
			return getEvidencePackFile(ctx, pool, parentID, fileID, true)
		},
		GetMany: getEvidencePackFiles,
		SoftDelete: func(ctx context.Context, pool *pgxpool.Pool, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
			return deleteEvidencePackFile(ctx, pool, parentID, fileID, deletedBy, deletedAt)
		},
		SoftDeleteTx: func(ctx context.Context, tx pgx.Tx, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
			return deleteEvidencePackFile(ctx, tx, parentID, fileID, deletedBy, deletedAt)
		},
	}
}

func listEvidencePackFiles(ctx context.Context, pool *pgxpool.Pool, parentID string) ([]cashfiles.FileRecord, error) {
	query := `
		SELECT f.file_id, f.stored_file_name, f.content_type, f.file_size, f.upload_s3_key, f.uploaded_by, f.uploaded_at
		FROM ` + evidencePackFilesTable + ` f
		JOIN ` + evidencePackTable + ` p ON p.pack_id = f.pack_id
		WHERE f.pack_id::text = $1
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		  ` + constants.FormatDeletedFilter + `
		ORDER BY f.uploaded_at DESC`
	return cashfiles.QueryFiles(ctx, pool, query, strings.TrimSpace(parentID))
}

func createEvidencePackFile(ctx context.Context, tx pgx.Tx, input cashfiles.CreateInput) error {
	_, err := createEvidencePackFileReturningID(ctx, tx, input)
	return err
}

func createEvidencePackFileReturningID(ctx context.Context, tx pgx.Tx, input cashfiles.CreateInput) (string, error) {
	parentScope := `
		SELECT p.pack_id AS parent_id
		FROM ` + evidencePackTable + ` p
		WHERE p.pack_id::text = $8
		  ` + constants.FormatDeletedFilter + `
	`
	return cashfiles.InsertAdditionalFileRowReturningID(ctx, tx, evidencePackFilesTable, evidencePackFilesParentCol, input, parentScope, strings.TrimSpace(input.ParentID))
}

func getEvidencePackFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string, includeDeleted bool) (*cashfiles.FileRecord, error) {
	deletedClause := constants.ErrFDReceiptDeletedFilter
	if includeDeleted {
		deletedClause = ""
	}
	query := `
		SELECT f.file_id, f.stored_file_name, f.content_type, f.file_size, f.upload_s3_key, f.uploaded_by, f.uploaded_at
		FROM ` + evidencePackFilesTable + ` f
		JOIN ` + evidencePackTable + ` p ON p.pack_id = f.pack_id
		WHERE f.pack_id::text = $1
		  AND f.file_id = $2
		  ` + deletedClause + `
		  ` + constants.FormatDeletedFilter + `
	`
	return cashfiles.FirstFile(ctx, pool, query, strings.TrimSpace(parentID), strings.TrimSpace(fileID))
}

func getEvidencePackFiles(ctx context.Context, pool *pgxpool.Pool, parentID string, fileIDs []string) ([]cashfiles.FileRecord, []string, error) {
	trimmedIDs := trimEvidencePackFileIDs(fileIDs)
	query := `
		SELECT f.file_id, f.stored_file_name, f.content_type, f.file_size, f.upload_s3_key, f.uploaded_by, f.uploaded_at
		FROM ` + evidencePackFilesTable + ` f
		JOIN ` + evidencePackTable + ` p ON p.pack_id = f.pack_id
		WHERE f.pack_id::text = $1
		  AND f.file_id = ANY($2)
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		  ` + constants.FormatDeletedFilter + `
		ORDER BY f.uploaded_at DESC`

	files, err := cashfiles.QueryFiles(ctx, pool, query, strings.TrimSpace(parentID), trimmedIDs)
	if err != nil {
		return nil, nil, err
	}
	return files, missingEvidencePackFileIDs(trimmedIDs, files), nil
}

// evidencePackFileExec is satisfied by both pgx.Tx and *pgxpool.Pool so
// deleteEvidencePackFile can be shared between SoftDelete (pool) and
// SoftDeleteTx (tx) config hooks, same shape as investmentFiles.go's
// investmentFileExec.
type evidencePackFileExec interface {
	Exec(context.Context, string, ...interface{}) (pgconn.CommandTag, error)
}

func deleteEvidencePackFile(ctx context.Context, exec evidencePackFileExec, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	query := `
		UPDATE ` + evidencePackFilesTable + ` f
		SET is_deleted = TRUE,
		    deleted_by = $3,
		    deleted_at = $4
		FROM ` + evidencePackTable + ` p
		WHERE f.pack_id::text = $1
		  AND f.file_id = $2
		  AND p.pack_id = f.pack_id
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		  ` + constants.FormatDeletedFilter + `
	`
	result, err := exec.Exec(ctx, query, strings.TrimSpace(parentID), strings.TrimSpace(fileID), deletedBy, deletedAt)
	if err != nil {
		return false, err
	}
	return result.RowsAffected() > 0, nil
}

func trimEvidencePackFileIDs(fileIDs []string) []string {
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

func missingEvidencePackFileIDs(expected []string, files []cashfiles.FileRecord) []string {
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

// ─── thin wrapper handlers into the shared generic engine ──────────────────

func UploadEvidencePackFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewUploadHandler(pool, evidencePackFilesConfig())
}

func DeleteEvidencePackFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDeleteHandler(pool, evidencePackFilesConfig())
}

func ApproveDeleteEvidencePackFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewApproveDeleteHandler(pool, evidencePackFilesConfig())
}

func RejectDeleteEvidencePackFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewRejectDeleteHandler(pool, evidencePackFilesConfig())
}

func DownloadEvidencePackFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDownloadHandler(pool, evidencePackFilesConfig())
}

// ListEvidencePackFilesHandler handles POST /investment/fd-closing/evidence/files/list.
func ListEvidencePackFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewListHandler(pool, evidencePackFilesConfig())
}

// DownloadBulkEvidencePackFilesHandler handles POST /investment/fd-closing/evidence/files/download-bulk.
func DownloadBulkEvidencePackFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewDownloadSelectedHandler(pool, evidencePackFilesConfig())
}

// AuditEvidencePackFilesHandler handles POST /investment/fd-closing/evidence/files/audit.
func AuditEvidencePackFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return cashfiles.NewAuditHandler(pool, evidencePackFilesConfig())
}
