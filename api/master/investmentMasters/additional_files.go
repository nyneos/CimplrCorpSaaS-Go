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

// investmentFilesHandlers groups the HTTP handlers for one investment master module.
type investmentFilesHandlers struct {
	List          http.HandlerFunc
	Upload        http.HandlerFunc
	Download      http.HandlerFunc
	DownloadMain  http.HandlerFunc
	DownloadBulk  http.HandlerFunc
	PackageZip    http.HandlerFunc
	Delete        http.HandlerFunc
	Audit         http.HandlerFunc
	ApproveDelete http.HandlerFunc
	RejectDelete  http.HandlerFunc
}

// newInvestmentFilesHandlers builds all handlers for an investment master
// module with no entity-level access scoping.
type InvestmentFilesConfigArgs struct {
	ModuleKey   string
	ModuleLabel string
	ParentField string
	ParentTable string
	ParentCol   string
	FilesTable  string
	AuditTable  string
	ActionCol   string
}

func newInvestmentFilesHandlers(pool *pgxpool.Pool, args InvestmentFilesConfigArgs) investmentFilesHandlers {
	cfg := buildInvestmentFilesConfig(args.ModuleKey, args.ParentField, args.ParentTable, args.ParentCol, args.FilesTable, args.AuditTable, args.ActionCol)
	loadMain := loadInvestmentMainPackageFile(args.ParentTable, args.ParentCol)
	moduleLabel := strings.TrimSpace(args.ModuleLabel)
	if moduleLabel == "" {
		moduleLabel = args.ModuleKey
	}
	return investmentFilesHandlers{
		List:          additionalfiles.NewListHandler(pool, cfg),
		Upload:        additionalfiles.NewUploadHandler(pool, cfg),
		Download:      additionalfiles.NewDownloadHandler(pool, cfg),
		DownloadMain:  additionalfiles.NewMainFileDownloadHandler(pool, cfg, loadMain),
		DownloadBulk:  additionalfiles.NewDownloadSelectedHandler(pool, cfg),
		PackageZip:    additionalfiles.NewPackageZipHandler(pool, cfg, additionalfiles.PackageZipOptions{ModuleLabel: moduleLabel, IDField: args.ParentCol, LoadMain: loadMain}),
		Delete:        additionalfiles.NewDeleteHandler(pool, cfg),
		Audit:         additionalfiles.NewAuditHandler(pool, cfg),
		ApproveDelete: additionalfiles.NewApproveDeleteHandler(pool, cfg),
		RejectDelete:  additionalfiles.NewRejectDeleteHandler(pool, cfg),
	}
}

// loadInvestmentMainPackageFile returns a loader for a master record's own
// bulk-uploaded file (upload_s3_key on the parent table). Returns (nil, nil) when
// the row has no stored file, and surfaces query errors (e.g. a table that has no
// upload_s3_key column yet) so the caller can skip the main file gracefully.
func loadInvestmentMainPackageFile(parentTable, parentCol string) func(ctx context.Context, pool *pgxpool.Pool, rowID string) (*additionalfiles.MainPackageFile, error) {
	q := `SELECT COALESCE(upload_s3_key,'') FROM ` + parentTable + ` WHERE ` + parentCol + ` = $1 AND COALESCE(is_deleted, FALSE) = FALSE`
	return func(ctx context.Context, pool *pgxpool.Pool, rowID string) (*additionalfiles.MainPackageFile, error) {
		var uploadS3Key string
		if err := pool.QueryRow(ctx, q, rowID).Scan(&uploadS3Key); err != nil {
			return nil, err
		}
		if strings.TrimSpace(uploadS3Key) == "" {
			return nil, nil
		}
		return &additionalfiles.MainPackageFile{UploadS3Key: uploadS3Key}, nil
	}
}

func buildInvestmentFilesConfig(moduleKey, parentField, parentTable, parentCol, filesTable, auditTable, actionCol string) additionalfiles.Config {
	return additionalfiles.Config{
		AuditSource:    strings.ToUpper(strings.ReplaceAll(moduleKey, "-", "_")),
		AuditTableName: "cimplrcorpsaas.cash_additional_file_audit",
		Module:         moduleKey,
		ParentIDField:  parentField,
		List: func(ctx context.Context, pool *pgxpool.Pool, parentID string) ([]additionalfiles.FileRecord, error) {
			q := `SELECT file_id, stored_file_name, content_type, file_size, upload_s3_key, uploaded_by, uploaded_at
			      FROM ` + filesTable + `
			      WHERE ` + parentCol + ` = $1 AND COALESCE(is_deleted, FALSE) = FALSE
			      ORDER BY uploaded_at DESC`
			return additionalfiles.QueryFiles(ctx, pool, q, parentID)
		},
		Create: func(ctx context.Context, tx pgx.Tx, input additionalfiles.CreateInput) error {
			_, err := createInvestmentAdditionalFile(ctx, tx, input, parentTable, parentCol, filesTable)
			return err
		},
		CreateReturning: func(ctx context.Context, tx pgx.Tx, input additionalfiles.CreateInput) (string, error) {
			return createInvestmentAdditionalFile(ctx, tx, input, parentTable, parentCol, filesTable)
		},
		GetOne: func(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*additionalfiles.FileRecord, error) {
			return getInvestmentAdditionalFile(ctx, pool, parentID, fileID, parentCol, filesTable, false)
		},
		GetAnyFile: func(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*additionalfiles.FileRecord, error) {
			return getInvestmentAdditionalFile(ctx, pool, parentID, fileID, parentCol, filesTable, true)
		},
		GetMany: func(ctx context.Context, pool *pgxpool.Pool, parentID string, fileIDs []string) ([]additionalfiles.FileRecord, []string, error) {
			trimmed := trimInvestmentFileIDs(fileIDs)
			q := `SELECT file_id, stored_file_name, content_type, file_size, upload_s3_key, uploaded_by, uploaded_at
			      FROM ` + filesTable + `
			      WHERE ` + parentCol + ` = $1 AND file_id = ANY($2) AND COALESCE(is_deleted, FALSE) = FALSE
			      ORDER BY uploaded_at DESC`
			files, err := additionalfiles.QueryFiles(ctx, pool, q, parentID, trimmed)
			if err != nil {
				return nil, nil, err
			}
			return files, missingInvestmentFileIDs(trimmed, files), nil
		},
		SoftDelete: func(ctx context.Context, pool *pgxpool.Pool, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
			return deleteInvestmentAdditionalFile(ctx, pool, investmentDeleteFileParams{ParentID: parentID, FileID: fileID, DeletedBy: deletedBy, DeletedAt: deletedAt, ParentCol: parentCol, FilesTable: filesTable})
		},
		SoftDeleteTx: func(ctx context.Context, tx pgx.Tx, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
			return deleteInvestmentAdditionalFile(ctx, tx, investmentDeleteFileParams{ParentID: parentID, FileID: fileID, DeletedBy: deletedBy, DeletedAt: deletedAt, ParentCol: parentCol, FilesTable: filesTable})
		},
		RecordMainUploadAudit: func(ctx context.Context, tx pgx.Tx, parentID string, payload additionalfiles.MainUploadAuditPayload) error {
			return additionalfiles.InsertMainUploadAudit(ctx, tx, auditTable, parentCol, actionCol, parentID, payload)
		},
		RecordMainDownloadAudit: func(ctx context.Context, exec additionalfiles.AuditExecutor, parentID string, payload additionalfiles.MainUploadAuditPayload) error {
			return additionalfiles.InsertMainDownloadAudit(ctx, exec, auditTable, parentCol, actionCol, parentID, payload)
		},
	}
}

func createInvestmentAdditionalFile(ctx context.Context, tx pgx.Tx, input additionalfiles.CreateInput, parentTable, parentCol, filesTable string) (string, error) {
	parentScope := fmt.Sprintf(
		`SELECT %s AS parent_id FROM %s WHERE %s = $8`,
		parentCol, parentTable, parentCol,
	)
	return additionalfiles.InsertAdditionalFileRowReturningID(ctx, tx, filesTable, parentCol, input, parentScope, input.ParentID)
}

func getInvestmentAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID, parentCol, filesTable string, includeDeleted bool) (*additionalfiles.FileRecord, error) {
	deletedClause := "AND COALESCE(is_deleted, FALSE) = FALSE"
	if includeDeleted {
		deletedClause = ""
	}
	q := `SELECT file_id, stored_file_name, content_type, file_size, upload_s3_key, uploaded_by, uploaded_at
	      FROM ` + filesTable + `
	      WHERE ` + parentCol + ` = $1 AND file_id = $2 ` + deletedClause
	return additionalfiles.FirstFile(ctx, pool, q, parentID, fileID)
}

type investmentFileExec interface {
	Exec(context.Context, string, ...interface{}) (pgconn.CommandTag, error)
}

type investmentDeleteFileParams struct {
	ParentID   string
	FileID     string
	DeletedBy  string
	DeletedAt  time.Time
	ParentCol  string
	FilesTable string
}

func deleteInvestmentAdditionalFile(ctx context.Context, exec investmentFileExec, p investmentDeleteFileParams) (bool, error) {
	q := `UPDATE ` + p.FilesTable + `
	      SET is_deleted = TRUE, deleted_by = $3, deleted_at = $4
	      WHERE ` + p.ParentCol + ` = $1 AND file_id = $2 AND COALESCE(is_deleted, FALSE) = FALSE`
	result, err := exec.Exec(ctx, q, p.ParentID, p.FileID, p.DeletedBy, p.DeletedAt)
	if err != nil {
		return false, err
	}
	return result.RowsAffected() > 0, nil
}

// ── investmentMasters handler sets ──────────────────────────────────────────

func AMCMasterFilesHandlers(pool *pgxpool.Pool) investmentFilesHandlers {
	return newInvestmentFilesHandlers(pool, InvestmentFilesConfigArgs{
		ModuleKey:   "master-amc",
		ParentField: "amc_id",
		ParentTable: "investment.masteramc",
		ParentCol:   "amc_id",
		FilesTable:  "cimplrcorpsaas.master_amc_files",
		AuditTable:  "investment.auditactionamc",
		ActionCol:   "actiontype",
	})
}

func SchemeMasterFilesHandlers(pool *pgxpool.Pool) investmentFilesHandlers {
	return newInvestmentFilesHandlers(pool, InvestmentFilesConfigArgs{
		ModuleKey:   "master-scheme",
		ParentField: "scheme_id",
		ParentTable: "investment.masterscheme",
		ParentCol:   "scheme_id",
		FilesTable:  "cimplrcorpsaas.master_scheme_files",
		AuditTable:  "investment.auditactionscheme",
		ActionCol:   "actiontype",
	})
}

func DPMasterFilesHandlers(pool *pgxpool.Pool) investmentFilesHandlers {
	return newInvestmentFilesHandlers(pool, InvestmentFilesConfigArgs{
		ModuleKey:   "master-dp",
		ParentField: "dp_id",
		ParentTable: "investment.masterdepositoryparticipant",
		ParentCol:   "dp_id",
		FilesTable:  "cimplrcorpsaas.master_dp_files",
		AuditTable:  "investment.auditactiondp",
		ActionCol:   "actiontype",
	})
}

func DematMasterFilesHandlers(pool *pgxpool.Pool) investmentFilesHandlers {
	return newInvestmentFilesHandlers(pool, InvestmentFilesConfigArgs{
		ModuleKey:   "master-demat",
		ParentField: "demat_id",
		ParentTable: "investment.masterdemataccount",
		ParentCol:   "demat_id",
		FilesTable:  "cimplrcorpsaas.master_demat_files",
		AuditTable:  "investment.auditactiondemat",
		ActionCol:   "actiontype",
	})
}

func FolioMasterFilesHandlers(pool *pgxpool.Pool) investmentFilesHandlers {
	return newInvestmentFilesHandlers(pool, InvestmentFilesConfigArgs{
		ModuleKey:   "master-folio",
		ParentField: "folio_id",
		ParentTable: "investment.masterfolio",
		ParentCol:   "folio_id",
		FilesTable:  "cimplrcorpsaas.master_folio_files",
		AuditTable:  "investment.auditactionfolio",
		ActionCol:   "actiontype",
	})
}

func InterestTypeMasterFilesHandlers(pool *pgxpool.Pool) investmentFilesHandlers {
	return newInvestmentFilesHandlers(pool, InvestmentFilesConfigArgs{
		ModuleKey:   "master-interest-type",
		ModuleLabel: "Interest Type Master",
		ParentField: "interest_id",
		ParentTable: "investment.fd_interest_type_master",
		ParentCol:   "interest_id",
		FilesTable:  "cimplrcorpsaas.master_interest_type_files",
		AuditTable:  "investment.fd_audit_interest_type",
		ActionCol:   "action_type",
	})
}

func PenaltyStructureMasterFilesHandlers(pool *pgxpool.Pool) investmentFilesHandlers {
	return newInvestmentFilesHandlers(pool, InvestmentFilesConfigArgs{
		ModuleKey:   "master-penalty-structure",
		ModuleLabel: "Penalty Structure Master",
		ParentField: "penalty_id",
		ParentTable: "investment.fd_penalty_structure_master",
		ParentCol:   "penalty_id",
		FilesTable:  "cimplrcorpsaas.master_penalty_structure_files",
		AuditTable:  "investment.fd_audit_penalty_structure",
		ActionCol:   "action_type",
	})
}

func CompoundingFrequencyMasterFilesHandlers(pool *pgxpool.Pool) investmentFilesHandlers {
	return newInvestmentFilesHandlers(pool, InvestmentFilesConfigArgs{
		ModuleKey:   "master-compounding-frequency",
		ModuleLabel: "Compounding Frequency Master",
		ParentField: "frequency_id",
		ParentTable: "investment.fd_compounding_frequency_master",
		ParentCol:   "frequency_id",
		FilesTable:  "cimplrcorpsaas.master_compounding_frequency_files",
		AuditTable:  "investment.fd_audit_compounding_frequency",
		ActionCol:   "action_type",
	})
}

func TDSPlanMasterFilesHandlers(pool *pgxpool.Pool) investmentFilesHandlers {
	return newInvestmentFilesHandlers(pool, InvestmentFilesConfigArgs{
		ModuleKey:   "master-tds-plan",
		ModuleLabel: "TDS Master",
		ParentField: "tds_plan_id",
		ParentTable: "investment.fd_tds_plan_master",
		ParentCol:   "tds_plan_id",
		FilesTable:  "cimplrcorpsaas.master_tds_plan_files",
		AuditTable:  "investment.fd_audit_tds_plan",
		ActionCol:   "action_type",
	})
}

func CalendarMasterFilesHandlers(pool *pgxpool.Pool) investmentFilesHandlers {
	return newInvestmentFilesHandlers(pool, InvestmentFilesConfigArgs{
		ModuleKey:   "master-calendar",
		ParentField: "calendar_id",
		ParentTable: "investment.mastercalendar",
		ParentCol:   "calendar_id",
		FilesTable:  "cimplrcorpsaas.master_calendar_files",
		AuditTable:  "investment.auditactioncalendar",
		ActionCol:   "actiontype",
	})
}

func DayCountConventionMasterFilesHandlers(pool *pgxpool.Pool) investmentFilesHandlers {
	return newInvestmentFilesHandlers(pool, InvestmentFilesConfigArgs{
		ModuleKey:   "master-day-count-convention",
		ModuleLabel: "Day Count Convention Master",
		ParentField: "day_count_code",
		ParentTable: "investment.fd_day_count_convention_master",
		ParentCol:   "day_count_code",
		FilesTable:  "cimplrcorpsaas.master_day_count_convention_files",
		AuditTable:  "investment.fd_audit_day_count_convention",
		ActionCol:   "action_type",
	})
}

func BankConfigMasterFilesHandlers(pool *pgxpool.Pool) investmentFilesHandlers {
	return newInvestmentFilesHandlers(pool, InvestmentFilesConfigArgs{
		ModuleKey:   "master-bank-config",
		ModuleLabel: "Bank Config Master",
		ParentField: "config_id",
		ParentTable: "investment.fd_bank_config_master",
		ParentCol:   "config_id",
		FilesTable:  "cimplrcorpsaas.master_bank_config_files",
		AuditTable:  "investment.fd_audit_bank_config",
		ActionCol:   "action_type",
	})
}

func BankRateCardMasterFilesHandlers(pool *pgxpool.Pool) investmentFilesHandlers {
	return newInvestmentFilesHandlers(pool, InvestmentFilesConfigArgs{
		ModuleKey:   "master-bank-rate-card",
		ModuleLabel: "Bank Rate Card Master",
		ParentField: "rate_card_id",
		ParentTable: "investment.fd_bank_rate_card_master",
		ParentCol:   "rate_card_id",
		FilesTable:  "cimplrcorpsaas.master_bank_rate_card_files",
		AuditTable:  "investment.fd_audit_bank_rate_card",
		ActionCol:   "action_type",
	})
}

// ── helpers ──────────────────────────────────────────────────────────────────

func trimInvestmentFileIDs(fileIDs []string) []string {
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

func missingInvestmentFileIDs(expected []string, files []additionalfiles.FileRecord) []string {
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
