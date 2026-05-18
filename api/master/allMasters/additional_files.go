package allMaster

import (
	"CimplrCorpSaas/api/cash/additionalfiles"
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// bytesMultipartFile wraps a *bytes.Reader to satisfy the mime/multipart.File
// interface (Read + ReadAt + Seek + Close). Used so parseCashFlowCategoryFile
// can be called with in-memory bytes after the original file has been closed.
type bytesMultipartFile struct{ *bytes.Reader }

func (bytesMultipartFile) Close() error { return nil }

// newBytesMultipartFile returns a bytesMultipartFile backed by b.
func newBytesMultipartFile(b []byte) bytesMultipartFile {
	return bytesMultipartFile{bytes.NewReader(b)}
}

// masterFilesHandlers groups the 5 HTTP handlers for one master module.
type masterFilesHandlers struct {
	List          http.HandlerFunc
	Upload        http.HandlerFunc
	Download      http.HandlerFunc
	DownloadBulk  http.HandlerFunc
	Delete        http.HandlerFunc
	Audit         http.HandlerFunc
	ApproveDelete http.HandlerFunc
	RejectDelete  http.HandlerFunc
}

// newMasterFilesHandlers builds all five handlers for a master module that
// has no entity-level access scoping. All queries filter only by parent ID.
//
//   - moduleKey    – S3 prefix key (e.g. "master-bank")
//   - parentField  – JSON/form field name for the parent ID (e.g. "bank_id")
//   - parentTable  – fully-qualified parent table (e.g. "public.masterbank")
//   - parentCol    – primary-key column on the parent table (e.g. "bank_id")
//   - filesTable   – fully-qualified files table (e.g. "cimplrcorpsaas.master_bank_files")
func newMasterFilesHandlers(pool *pgxpool.Pool, moduleKey, parentField, parentTable, parentCol, filesTable string) masterFilesHandlers {
	cfg := buildMasterFilesConfig(moduleKey, parentField, parentTable, parentCol, filesTable)
	return masterFilesHandlers{
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

func buildMasterFilesConfig(moduleKey, parentField, parentTable, parentCol, filesTable string) additionalfiles.Config {
	return additionalfiles.Config{
		AuditSource:   strings.ToUpper(strings.ReplaceAll(moduleKey, "-", "_")),
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
			_, err := createMasterAdditionalFile(ctx, tx, input, parentTable, parentCol, filesTable)
			return err
		},
		CreateReturning: func(ctx context.Context, tx pgx.Tx, input additionalfiles.CreateInput) (string, error) {
			return createMasterAdditionalFile(ctx, tx, input, parentTable, parentCol, filesTable)
		},
		GetOne: func(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*additionalfiles.FileRecord, error) {
			return getMasterAdditionalFile(ctx, pool, parentID, fileID, parentCol, filesTable, false)
		},
		GetAnyFile: func(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*additionalfiles.FileRecord, error) {
			return getMasterAdditionalFile(ctx, pool, parentID, fileID, parentCol, filesTable, true)
		},
		GetMany: func(ctx context.Context, pool *pgxpool.Pool, parentID string, fileIDs []string) ([]additionalfiles.FileRecord, []string, error) {
			trimmed := trimMasterFileIDs(fileIDs)
			q := `SELECT file_id, stored_file_name, content_type, file_size, upload_s3_key, uploaded_by, uploaded_at
			      FROM ` + filesTable + `
			      WHERE ` + parentCol + ` = $1 AND file_id = ANY($2) AND COALESCE(is_deleted, FALSE) = FALSE
			      ORDER BY uploaded_at DESC`
			files, err := additionalfiles.QueryFiles(ctx, pool, q, parentID, trimmed)
			if err != nil {
				return nil, nil, err
			}
			return files, missingMasterFileIDs(trimmed, files), nil
		},
		SoftDelete: func(ctx context.Context, pool *pgxpool.Pool, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
			return deleteMasterAdditionalFile(ctx, pool, parentID, fileID, deletedBy, deletedAt, parentCol, filesTable)
		},
		SoftDeleteTx: func(ctx context.Context, tx pgx.Tx, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
			return deleteMasterAdditionalFile(ctx, tx, parentID, fileID, deletedBy, deletedAt, parentCol, filesTable)
		},
	}
}

func createMasterAdditionalFile(ctx context.Context, tx pgx.Tx, input additionalfiles.CreateInput, parentTable, parentCol, filesTable string) (string, error) {
	parentScope := fmt.Sprintf(
		`SELECT %s AS parent_id FROM %s WHERE %s = $8`,
		parentCol, parentTable, parentCol,
	)
	return additionalfiles.InsertAdditionalFileRowReturningID(ctx, tx, filesTable, parentCol, input, parentScope, input.ParentID)
}

func getMasterAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID, parentCol, filesTable string, includeDeleted bool) (*additionalfiles.FileRecord, error) {
	deletedClause := "AND COALESCE(is_deleted, FALSE) = FALSE"
	if includeDeleted {
		deletedClause = ""
	}
	q := `SELECT file_id, stored_file_name, content_type, file_size, upload_s3_key, uploaded_by, uploaded_at
			      FROM ` + filesTable + `
			      WHERE ` + parentCol + ` = $1 AND file_id = $2 ` + deletedClause
	return additionalfiles.FirstFile(ctx, pool, q, parentID, fileID)
}

type masterFileExec interface {
	Exec(context.Context, string, ...interface{}) (pgconn.CommandTag, error)
}

func deleteMasterAdditionalFile(ctx context.Context, exec masterFileExec, parentID, fileID, deletedBy string, deletedAt time.Time, parentCol, filesTable string) (bool, error) {
	q := `UPDATE ` + filesTable + `
			      SET is_deleted = TRUE, deleted_by = $3, deleted_at = $4
			      WHERE ` + parentCol + ` = $1 AND file_id = $2 AND COALESCE(is_deleted, FALSE) = FALSE`
	result, err := exec.Exec(ctx, q, parentID, fileID, deletedBy, deletedAt)
	if err != nil {
		return false, err
	}
	return result.RowsAffected() > 0, nil
}

// ── allMasters handler sets ─────────────────────────────────────────────────

func BankMasterFilesHandlers(pool *pgxpool.Pool) masterFilesHandlers {
	return newMasterFilesHandlers(pool, "master-bank", "bank_id", "public.masterbank", "bank_id", "cimplrcorpsaas.master_bank_files")
}

func CurrencyMasterFilesHandlers(pool *pgxpool.Pool) masterFilesHandlers {
	return newMasterFilesHandlers(pool, "master-currency", "currency_id", "public.mastercurrency", "currency_id", "cimplrcorpsaas.master_currency_files")
}

func BankAccountMasterFilesHandlers(pool *pgxpool.Pool) masterFilesHandlers {
	return newMasterFilesHandlers(pool, "master-bank-account", "account_id", "public.masterbankaccount", "account_id", "cimplrcorpsaas.master_bank_account_files")
}

func CounterpartyMasterFilesHandlers(pool *pgxpool.Pool) masterFilesHandlers {
	return newMasterFilesHandlers(pool, "master-counterparty", "counterparty_id", "public.mastercounterparty", "counterparty_id", "cimplrcorpsaas.master_counterparty_files")
}

func GLAccountMasterFilesHandlers(pool *pgxpool.Pool) masterFilesHandlers {
	return newMasterFilesHandlers(pool, "master-gl-account", "gl_account_id", "public.masterglaccount", "gl_account_id", "cimplrcorpsaas.master_gl_account_files")
}

func CashFlowCategoryMasterFilesHandlers(pool *pgxpool.Pool) masterFilesHandlers {
	return newMasterFilesHandlers(pool, "master-cashflow-category", "category_id", "public.mastercashflowcategory", "category_id", "cimplrcorpsaas.master_cashflow_category_files")
}

func CostProfitCenterMasterFilesHandlers(pool *pgxpool.Pool) masterFilesHandlers {
	return newMasterFilesHandlers(pool, "master-costprofit-center", "centre_id", "public.mastercostprofitcenter", "centre_id", "cimplrcorpsaas.master_cost_profit_center_files")
}

func PayableReceivableMasterFilesHandlers(pool *pgxpool.Pool) masterFilesHandlers {
	return newMasterFilesHandlers(pool, "master-payable-receivable", "type_id", "public.masterpayablereceivabletype", "type_id", "cimplrcorpsaas.master_payable_receivable_files")
}

func EntityCashMasterFilesHandlers(pool *pgxpool.Pool) masterFilesHandlers {
	return newMasterFilesHandlers(pool, "master-entity-cash", "entity_id", "public.masterentitycash", "entity_id", "cimplrcorpsaas.master_entity_cash_files")
}

func EntityMasterFilesHandlers(pool *pgxpool.Pool) masterFilesHandlers {
	return newMasterFilesHandlers(pool, "master-entity", "entity_id", "public.masterentity", "entity_id", "cimplrcorpsaas.master_entity_files")
}

// ── helpers ─────────────────────────────────────────────────────────────────

func trimMasterFileIDs(fileIDs []string) []string {
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

func missingMasterFileIDs(expected []string, files []additionalfiles.FileRecord) []string {
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
