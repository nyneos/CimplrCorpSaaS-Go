package bankstatement

import (
	"CimplrCorpSaas/api/cash/additionalfiles"
	"CimplrCorpSaas/api/constants"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func ListRuleMasterAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewListHandler(pool, ruleMasterAdditionalFilesConfig())
}

func UploadRuleMasterAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewUploadHandler(pool, ruleMasterAdditionalFilesConfig())
}

func DownloadRuleMasterAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDownloadHandler(pool, ruleMasterAdditionalFilesConfig())
}

func DownloadSelectedRuleMasterAdditionalFilesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDownloadSelectedHandler(pool, ruleMasterAdditionalFilesConfig())
}

func DownloadRuleMasterPackageZipHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewPackageZipHandler(pool, ruleMasterAdditionalFilesConfig(), additionalfiles.PackageZipOptions{
		ModuleLabel: "Rule Master",
		IDField:     "rule_id",
	})
}

func DeleteRuleMasterAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewDeleteHandler(pool, ruleMasterAdditionalFilesConfig())
}

func AuditRuleMasterAdditionalFileHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewAuditHandler(pool, ruleMasterAdditionalFilesConfig())
}

func ApproveRuleMasterAdditionalFileDeleteHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewApproveDeleteHandler(pool, ruleMasterAdditionalFilesConfig())
}

func RejectRuleMasterAdditionalFileDeleteHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return additionalfiles.NewRejectDeleteHandler(pool, ruleMasterAdditionalFilesConfig())
}

func ruleMasterAdditionalFilesConfig() additionalfiles.Config {
	return additionalfiles.Config{
		Module:          "rule-master",
		AuditSource:     "RULE_MASTER",
		ParentIDField:   "rule_id",
		List:            listRuleMasterAdditionalFiles,
		CreateReturning: createRuleMasterAdditionalFile,
		GetOne:          getRuleMasterAdditionalFile,
		GetAnyFile:      getAnyRuleMasterAdditionalFile,
		GetMany:         getRuleMasterAdditionalFiles,
		SoftDelete:      deleteRuleMasterAdditionalFile,
		SoftDeleteTx:    deleteRuleMasterAdditionalFileTx,
	}
}

func listRuleMasterAdditionalFiles(ctx context.Context, pool *pgxpool.Pool, parentID string) ([]additionalfiles.FileRecord, error) {
	if err := validateRuleMasterFileAccess(ctx, pool, parentID); err != nil {
		return nil, err
	}

	query := `
		SELECT f.file_id, f.stored_file_name, f.content_type, f.file_size, f.upload_s3_key, f.uploaded_by, f.uploaded_at
		FROM cimplrcorpsaas.category_rule_files f
		WHERE f.rule_id::text = $1
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		ORDER BY f.uploaded_at DESC
	`
	return additionalfiles.QueryFiles(ctx, pool, query, strings.TrimSpace(parentID))
}

func createRuleMasterAdditionalFile(ctx context.Context, tx pgx.Tx, input additionalfiles.CreateInput) (string, error) {
	if err := validateRuleMasterFileAccess(ctx, tx, input.ParentID); err != nil {
		return "", err
	}

	parentScope := `
		SELECT r.rule_id AS parent_id
		FROM cimplrcorpsaas.category_rules r
		WHERE r.rule_id::text = $8
	`
	return additionalfiles.InsertAdditionalFileRowReturningID(ctx, tx, "cimplrcorpsaas.category_rule_files", "rule_id", input, parentScope, strings.TrimSpace(input.ParentID))
}

func getRuleMasterAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*additionalfiles.FileRecord, error) {
	return getRuleMasterAdditionalFileWithDeleted(ctx, pool, parentID, fileID, false)
}

func getAnyRuleMasterAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*additionalfiles.FileRecord, error) {
	return getRuleMasterAdditionalFileWithDeleted(ctx, pool, parentID, fileID, true)
}

func getRuleMasterAdditionalFileWithDeleted(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string, includeDeleted bool) (*additionalfiles.FileRecord, error) {
	if err := validateRuleMasterFileAccess(ctx, pool, parentID); err != nil {
		return nil, err
	}

	deletedClause := constants.ErrFDReceiptDeletedFilter
	if includeDeleted {
		deletedClause = ""
	}

	query := `
		SELECT f.file_id, f.stored_file_name, f.content_type, f.file_size, f.upload_s3_key, f.uploaded_by, f.uploaded_at
		FROM cimplrcorpsaas.category_rule_files f
		WHERE f.rule_id::text = $1
		  AND f.file_id = $2
		  ` + deletedClause + `
	`
	return additionalfiles.FirstFile(ctx, pool, query, strings.TrimSpace(parentID), strings.TrimSpace(fileID))
}

func getRuleMasterAdditionalFiles(ctx context.Context, pool *pgxpool.Pool, parentID string, fileIDs []string) ([]additionalfiles.FileRecord, []string, error) {
	if err := validateRuleMasterFileAccess(ctx, pool, parentID); err != nil {
		return nil, nil, err
	}

	trimmedIDs := trimRuleMasterAdditionalFileIDs(fileIDs)
	query := `
		SELECT f.file_id, f.stored_file_name, f.content_type, f.file_size, f.upload_s3_key, f.uploaded_by, f.uploaded_at
		FROM cimplrcorpsaas.category_rule_files f
		WHERE f.rule_id::text = $1
		  AND f.file_id = ANY($2)
		  AND COALESCE(f.is_deleted, FALSE) = FALSE
		ORDER BY f.uploaded_at DESC
	`

	files, err := additionalfiles.QueryFiles(ctx, pool, query, strings.TrimSpace(parentID), trimmedIDs)
	if err != nil {
		return nil, nil, err
	}
	return files, missingRuleMasterAdditionalFileIDs(trimmedIDs, files), nil
}

func deleteRuleMasterAdditionalFile(ctx context.Context, pool *pgxpool.Pool, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	return deleteRuleMasterAdditionalFileExec(ctx, pool, parentID, fileID, deletedBy, deletedAt)
}

func deleteRuleMasterAdditionalFileTx(ctx context.Context, tx pgx.Tx, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	return deleteRuleMasterAdditionalFileExec(ctx, tx, parentID, fileID, deletedBy, deletedAt)
}

type ruleMasterFileExec interface {
	Exec(context.Context, string, ...interface{}) (pgconn.CommandTag, error)
	QueryRow(context.Context, string, ...interface{}) pgx.Row
}

func deleteRuleMasterAdditionalFileExec(ctx context.Context, exec ruleMasterFileExec, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	if err := validateRuleMasterFileAccess(ctx, exec, parentID); err != nil {
		return false, err
	}

	query := `
		UPDATE cimplrcorpsaas.category_rule_files
		SET is_deleted = TRUE,
		    deleted_by = $3,
		    deleted_at = $4
		WHERE rule_id::text = $1
		  AND file_id = $2
		  AND COALESCE(is_deleted, FALSE) = FALSE
	`
	result, err := exec.Exec(ctx, query, strings.TrimSpace(parentID), strings.TrimSpace(fileID), deletedBy, deletedAt)
	if err != nil {
		return false, err
	}
	return result.RowsAffected() > 0, nil
}

type ruleMasterScopeQueryer interface {
	QueryRow(context.Context, string, ...interface{}) pgx.Row
}

func validateRuleMasterFileAccess(ctx context.Context, queryer ruleMasterScopeQueryer, parentID string) error {
	ruleID := strings.TrimSpace(parentID)
	if ruleID == "" {
		return errors.New("rule_id required")
	}

	var scopeType string
	var entityID sql.NullString
	var bankCode sql.NullString
	var accountNumber sql.NullString
	var currency sql.NullString

	err := queryer.QueryRow(ctx, `
		SELECT s.scope_type, s.entity_id, s.bank_code, s.account_number, s.currency
		FROM cimplrcorpsaas.category_rules r
		JOIN cimplrcorpsaas.rule_scope s ON s.scope_id = r.scope_id
		WHERE r.rule_id::text = $1
	`, ruleID).Scan(&scopeType, &entityID, &bankCode, &accountNumber, &currency)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return errors.New("parent record not found or access denied")
		}
		return err
	}

	code, message := validateScopeAccess(ctx, scopeType, nullStringPtr(entityID), nullStringPtr(bankCode), nullStringPtr(accountNumber), nullStringPtr(currency))
	if code != 0 {
		if strings.TrimSpace(message) != "" {
			return fmt.Errorf("%s", message)
		}
		return fmt.Errorf("rule access denied: %d", code)
	}
	return nil
}

func nullStringPtr(value sql.NullString) *string {
	if !value.Valid {
		return nil
	}
	trimmed := strings.TrimSpace(value.String)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}

func trimRuleMasterAdditionalFileIDs(fileIDs []string) []string {
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

func missingRuleMasterAdditionalFileIDs(expected []string, files []additionalfiles.FileRecord) []string {
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
