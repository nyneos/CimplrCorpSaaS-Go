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
type MasterFilesConfigArgs struct {
	ModuleKey   string
	ParentField string
	ParentTable string
	ParentCol   string
	FilesTable  string
	AuditTable  string
	ActionCol   string
}

func newMasterFilesHandlers(pool *pgxpool.Pool, args MasterFilesConfigArgs) masterFilesHandlers {
	cfg := buildMasterFilesConfig(args.ModuleKey, args.ParentField, args.ParentTable, args.ParentCol, args.FilesTable, args.AuditTable, args.ActionCol)
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

func buildMasterFilesConfig(moduleKey, parentField, parentTable, parentCol, filesTable, auditTable, actionCol string) additionalfiles.Config {
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
			return deleteMasterAdditionalFile(ctx, pool, masterDeleteFileParams{ParentID: parentID, FileID: fileID, DeletedBy: deletedBy, DeletedAt: deletedAt, ParentCol: parentCol, FilesTable: filesTable})
		},
		SoftDeleteTx: func(ctx context.Context, tx pgx.Tx, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
			return deleteMasterAdditionalFile(ctx, tx, masterDeleteFileParams{ParentID: parentID, FileID: fileID, DeletedBy: deletedBy, DeletedAt: deletedAt, ParentCol: parentCol, FilesTable: filesTable})
		},
		RecordMainUploadAudit: func(ctx context.Context, tx pgx.Tx, parentID string, payload additionalfiles.MainUploadAuditPayload) error {
			return additionalfiles.InsertMainUploadAudit(ctx, tx, auditTable, parentCol, actionCol, parentID, payload)
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

type masterDeleteFileParams struct {
	ParentID   string
	FileID     string
	DeletedBy  string
	DeletedAt  time.Time
	ParentCol  string
	FilesTable string
}

func deleteMasterAdditionalFile(ctx context.Context, exec masterFileExec, p masterDeleteFileParams) (bool, error) {
	q := `UPDATE ` + p.FilesTable + `
			      SET is_deleted = TRUE, deleted_by = $3, deleted_at = $4
			      WHERE ` + p.ParentCol + ` = $1 AND file_id = $2 AND COALESCE(is_deleted, FALSE) = FALSE`
	result, err := exec.Exec(ctx, q, p.ParentID, p.FileID, p.DeletedBy, p.DeletedAt)
	if err != nil {
		return false, err
	}
	return result.RowsAffected() > 0, nil
}

// ── allMasters handler sets ─────────────────────────────────────────────────

func BankMasterFilesHandlers(pool *pgxpool.Pool) masterFilesHandlers {
	return newMasterFilesHandlers(pool, MasterFilesConfigArgs{
		ModuleKey:   "master-bank",
		ParentField: "bank_id",
		ParentTable: "public.masterbank",
		ParentCol:   "bank_id",
		FilesTable:  "cimplrcorpsaas.master_bank_files",
		AuditTable:  "public.auditactionbank",
		ActionCol:   "actiontype",
	})
}

func CurrencyMasterFilesHandlers(pool *pgxpool.Pool) masterFilesHandlers {
	return newMasterFilesHandlers(pool, MasterFilesConfigArgs{
		ModuleKey:   "master-currency",
		ParentField: "currency_id",
		ParentTable: "public.mastercurrency",
		ParentCol:   "currency_id",
		FilesTable:  "cimplrcorpsaas.master_currency_files",
		AuditTable:  "public.auditactioncurrency",
		ActionCol:   "actiontype",
	})
}

func BankAccountMasterFilesHandlers(pool *pgxpool.Pool) masterFilesHandlers {
	return newMasterFilesHandlers(pool, MasterFilesConfigArgs{
		ModuleKey:   "master-bank-account",
		ParentField: "account_id",
		ParentTable: "public.masterbankaccount",
		ParentCol:   "account_id",
		FilesTable:  "cimplrcorpsaas.master_bank_account_files",
		AuditTable:  "public.auditactionbankaccount",
		ActionCol:   "actiontype",
	})
}

func CounterpartyMasterFilesHandlers(pool *pgxpool.Pool) masterFilesHandlers {
	return newMasterFilesHandlers(pool, MasterFilesConfigArgs{
		ModuleKey:   "master-counterparty",
		ParentField: "counterparty_id",
		ParentTable: "public.mastercounterparty",
		ParentCol:   "counterparty_id",
		FilesTable:  "cimplrcorpsaas.master_counterparty_files",
		AuditTable:  "public.auditactioncounterparty",
		ActionCol:   "actiontype",
	})
}

func GLAccountMasterFilesHandlers(pool *pgxpool.Pool) masterFilesHandlers {
	return newMasterFilesHandlers(pool, MasterFilesConfigArgs{
		ModuleKey:   "master-gl-account",
		ParentField: "gl_account_id",
		ParentTable: "public.masterglaccount",
		ParentCol:   "gl_account_id",
		FilesTable:  "cimplrcorpsaas.master_gl_account_files",
		AuditTable:  "public.auditactionglaccount",
		ActionCol:   "actiontype",
	})
}

func CashFlowCategoryMasterFilesHandlers(pool *pgxpool.Pool) masterFilesHandlers {
	return newMasterFilesHandlers(pool, MasterFilesConfigArgs{
		ModuleKey:   "master-cashflow-category",
		ParentField: "category_id",
		ParentTable: "public.mastercashflowcategory",
		ParentCol:   "category_id",
		FilesTable:  "cimplrcorpsaas.master_cashflow_category_files",
		AuditTable:  "public.auditactioncashflowcategory",
		ActionCol:   "actiontype",
	})
}

func CostProfitCenterMasterFilesHandlers(pool *pgxpool.Pool) masterFilesHandlers {
	return newMasterFilesHandlers(pool, MasterFilesConfigArgs{
		ModuleKey:   "master-costprofit-center",
		ParentField: "centre_id",
		ParentTable: "public.mastercostprofitcenter",
		ParentCol:   "centre_id",
		FilesTable:  "cimplrcorpsaas.master_cost_profit_center_files",
		AuditTable:  "public.auditactioncostprofitcenter",
		ActionCol:   "actiontype",
	})
}

func PayableReceivableMasterFilesHandlers(pool *pgxpool.Pool) masterFilesHandlers {
	return newMasterFilesHandlers(pool, MasterFilesConfigArgs{
		ModuleKey:   "master-payable-receivable",
		ParentField: "type_id",
		ParentTable: "public.masterpayablereceivabletype",
		ParentCol:   "type_id",
		FilesTable:  "cimplrcorpsaas.master_payable_receivable_files",
		AuditTable:  "public.auditactionpayablereceivable",
		ActionCol:   "actiontype",
	})
}

func EntityCashMasterFilesHandlers(pool *pgxpool.Pool) masterFilesHandlers {
	return newMasterFilesHandlers(pool, MasterFilesConfigArgs{
		ModuleKey:   "master-entity-cash",
		ParentField: "entity_id",
		ParentTable: "public.masterentitycash",
		ParentCol:   "entity_id",
		FilesTable:  "cimplrcorpsaas.master_entity_cash_files",
		AuditTable:  "public.auditactionentity",
		ActionCol:   "actiontype",
	})
}

func EntityMasterFilesHandlers(pool *pgxpool.Pool) masterFilesHandlers {
	return newMasterFilesHandlers(pool, MasterFilesConfigArgs{
		ModuleKey:   "master-entity",
		ParentField: "entity_id",
		ParentTable: "public.masterentity",
		ParentCol:   "entity_id",
		FilesTable:  "cimplrcorpsaas.master_entity_files",
		AuditTable:  "public.auditactionentity",
		ActionCol:   "actiontype",
	})
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
