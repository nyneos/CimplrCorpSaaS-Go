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

// investmentFilesHandlers groups the 5 HTTP handlers for one investment master module.
type investmentFilesHandlers struct {
	List          http.HandlerFunc
	Upload        http.HandlerFunc
	Download      http.HandlerFunc
	DownloadBulk  http.HandlerFunc
	Delete        http.HandlerFunc
	Audit         http.HandlerFunc
	ApproveDelete http.HandlerFunc
	RejectDelete  http.HandlerFunc
}

// newInvestmentFilesHandlers builds all five handlers for an investment master
// module with no entity-level access scoping.
func newInvestmentFilesHandlers(pool *pgxpool.Pool, moduleKey, parentField, parentTable, parentCol, filesTable, auditTable, actionCol string) investmentFilesHandlers {
	cfg := buildInvestmentFilesConfig(moduleKey, parentField, parentTable, parentCol, filesTable, auditTable, actionCol)
	return investmentFilesHandlers{
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

func buildInvestmentFilesConfig(moduleKey, parentField, parentTable, parentCol, filesTable, auditTable, actionCol string) additionalfiles.Config {
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
			return deleteInvestmentAdditionalFile(ctx, pool, parentID, fileID, deletedBy, deletedAt, parentCol, filesTable)
		},
		SoftDeleteTx: func(ctx context.Context, tx pgx.Tx, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
			return deleteInvestmentAdditionalFile(ctx, tx, parentID, fileID, deletedBy, deletedAt, parentCol, filesTable)
		},
		RecordMainUploadAudit: func(ctx context.Context, tx pgx.Tx, parentID string, payload additionalfiles.MainUploadAuditPayload) error {
			return additionalfiles.InsertMainUploadAudit(ctx, tx, auditTable, parentCol, actionCol, parentID, payload)
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

func deleteInvestmentAdditionalFile(ctx context.Context, exec investmentFileExec, parentID, fileID, deletedBy string, deletedAt time.Time, parentCol, filesTable string) (bool, error) {
	q := `UPDATE ` + filesTable + `
	      SET is_deleted = TRUE, deleted_by = $3, deleted_at = $4
	      WHERE ` + parentCol + ` = $1 AND file_id = $2 AND COALESCE(is_deleted, FALSE) = FALSE`
	result, err := exec.Exec(ctx, q, parentID, fileID, deletedBy, deletedAt)
	if err != nil {
		return false, err
	}
	return result.RowsAffected() > 0, nil
}

// ── investmentMasters handler sets ──────────────────────────────────────────

func AMCMasterFilesHandlers(pool *pgxpool.Pool) investmentFilesHandlers {
	return newInvestmentFilesHandlers(pool, "master-amc", "amc_id", "investment.masteramc", "amc_id", "cimplrcorpsaas.master_amc_files", "investment.auditactionamc", "actiontype")
}

func SchemeMasterFilesHandlers(pool *pgxpool.Pool) investmentFilesHandlers {
	return newInvestmentFilesHandlers(pool, "master-scheme", "scheme_id", "investment.masterscheme", "scheme_id", "cimplrcorpsaas.master_scheme_files", "investment.auditactionscheme", "actiontype")
}

func DPMasterFilesHandlers(pool *pgxpool.Pool) investmentFilesHandlers {
	return newInvestmentFilesHandlers(pool, "master-dp", "dp_id", "investment.masterdepositoryparticipant", "dp_id", "cimplrcorpsaas.master_dp_files", "investment.auditactiondp", "actiontype")
}

func DematMasterFilesHandlers(pool *pgxpool.Pool) investmentFilesHandlers {
	return newInvestmentFilesHandlers(pool, "master-demat", "demat_id", "investment.masterdemataccount", "demat_id", "cimplrcorpsaas.master_demat_files", "investment.auditactiondemat", "actiontype")
}

func FolioMasterFilesHandlers(pool *pgxpool.Pool) investmentFilesHandlers {
	return newInvestmentFilesHandlers(pool, "master-folio", "folio_id", "investment.masterfolio", "folio_id", "cimplrcorpsaas.master_folio_files", "investment.auditactionfolio", "actiontype")
}

func InterestTypeMasterFilesHandlers(pool *pgxpool.Pool) investmentFilesHandlers {
	return newInvestmentFilesHandlers(pool, "master-interest-type", "interest_id", "investment.fd_interest_type_master", "interest_id", "cimplrcorpsaas.master_interest_type_files", "investment.fd_audit_interest_type", "action_type")
}

func PenaltyStructureMasterFilesHandlers(pool *pgxpool.Pool) investmentFilesHandlers {
	return newInvestmentFilesHandlers(pool, "master-penalty-structure", "penalty_id", "investment.fd_penalty_structure_master", "penalty_id", "cimplrcorpsaas.master_penalty_structure_files", "investment.fd_audit_penalty_structure", "action_type")
}

func CompoundingFrequencyMasterFilesHandlers(pool *pgxpool.Pool) investmentFilesHandlers {
	return newInvestmentFilesHandlers(pool, "master-compounding-frequency", "frequency_id", "investment.fd_compounding_frequency_master", "frequency_id", "cimplrcorpsaas.master_compounding_frequency_files", "investment.fd_audit_compounding_frequency", "action_type")
}

func TDSPlanMasterFilesHandlers(pool *pgxpool.Pool) investmentFilesHandlers {
	return newInvestmentFilesHandlers(pool, "master-tds-plan", "tds_plan_id", "investment.fd_tds_plan_master", "tds_plan_id", "cimplrcorpsaas.master_tds_plan_files", "investment.fd_audit_tds_plan", "action_type")
}

func CalendarMasterFilesHandlers(pool *pgxpool.Pool) investmentFilesHandlers {
	return newInvestmentFilesHandlers(pool, "master-calendar", "calendar_id", "investment.mastercalendar", "calendar_id", "cimplrcorpsaas.master_calendar_files", "investment.auditactioncalendar", "actiontype")
}

func DayCountConventionMasterFilesHandlers(pool *pgxpool.Pool) investmentFilesHandlers {
	return newInvestmentFilesHandlers(pool, "master-day-count-convention", "day_count_code", "investment.fd_day_count_convention_master", "day_count_code", "cimplrcorpsaas.master_day_count_convention_files", "investment.fd_audit_day_count_convention", "action_type")
}

func BankConfigMasterFilesHandlers(pool *pgxpool.Pool) investmentFilesHandlers {
	return newInvestmentFilesHandlers(pool, "master-bank-config", "config_id", "investment.fd_bank_config_master", "config_id", "cimplrcorpsaas.master_bank_config_files", "investment.fd_audit_bank_config", "action_type")
}

func BankRateCardMasterFilesHandlers(pool *pgxpool.Pool) investmentFilesHandlers {
	return newInvestmentFilesHandlers(pool, "master-bank-rate-card", "rate_card_id", "investment.fd_bank_rate_card_master", "rate_card_id", "cimplrcorpsaas.master_bank_rate_card_files", "investment.fd_audit_bank_rate_card", "action_type")
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
