package additionalfiles

import (
	api "CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	cashCrossStageBankStatement = "bankstatement"
	cashCrossStageBankBalance   = "bankbalance"
	cashCrossStageLimitPosition = "limit-position"
	cashCrossStageUtilization   = "limit-utilization"

	cashCrossStageCols      = "f.file_id, f.stored_file_name, f.content_type, f.file_size, f.upload_s3_key, f.uploaded_by, f.uploaded_at"
	cashCrossStageOuterCols = "file_id, stored_file_name, content_type, file_size, upload_s3_key, uploaded_by, uploaded_at"
)

var cashCrossStageFileTables = []string{
	"cimplrcorpsaas.bank_statement_files",
	"cimplrcorpsaas.bank_balance_files",
	"cimplrcorpsaas.bank_limit_files",
	"cimplrcorpsaas.bank_limit_utilization_files",
}

// WithCashCrossStageVisibility wires shared DMS visibility for linked cash screens.
// Uploads still use the caller's configured Create/CreateReturning handlers.
func WithCashCrossStageVisibility(cfg Config) Config {
	if !isCashCrossStageModule(cfg.Module) {
		return cfg
	}

	module := cfg.Module
	cfg.AuditByFileIDOnly = true
	cfg.List = func(ctx context.Context, pool *pgxpool.Pool, parentID string) ([]FileRecord, error) {
		return listCashCrossStageFiles(ctx, pool, module, parentID)
	}
	cfg.GetOne = func(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*FileRecord, error) {
		return getCashCrossStageFile(ctx, pool, module, parentID, fileID, false)
	}
	cfg.GetAnyFile = func(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*FileRecord, error) {
		return getCashCrossStageFile(ctx, pool, module, parentID, fileID, true)
	}
	cfg.GetMany = func(ctx context.Context, pool *pgxpool.Pool, parentID string, fileIDs []string) ([]FileRecord, []string, error) {
		return getCashCrossStageFiles(ctx, pool, module, parentID, fileIDs)
	}
	cfg.SoftDelete = func(ctx context.Context, pool *pgxpool.Pool, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
		return softDeleteCashCrossStageFile(ctx, pool, fileID, deletedBy, deletedAt)
	}
	cfg.SoftDeleteTx = func(ctx context.Context, tx pgx.Tx, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
		return softDeleteCashCrossStageFile(ctx, tx, fileID, deletedBy, deletedAt)
	}
	return cfg
}

func isCashCrossStageModule(module string) bool {
	switch module {
	case cashCrossStageBankStatement, cashCrossStageBankBalance, cashCrossStageLimitPosition, cashCrossStageUtilization:
		return true
	default:
		return false
	}
}

func listCashCrossStageFiles(ctx context.Context, pool *pgxpool.Pool, module, parentID string) ([]FileRecord, error) {
	union, args, err := cashCrossStageUnion(ctx, module, strings.TrimSpace(parentID), nil, false)
	if err != nil {
		return nil, err
	}
	query := union + "\nORDER BY uploaded_at DESC"
	return QueryFiles(ctx, pool, query, args...)
}

func getCashCrossStageFile(ctx context.Context, pool *pgxpool.Pool, module, parentID, fileID string, includeDeleted bool) (*FileRecord, error) {
	union, args, err := cashCrossStageUnion(ctx, module, strings.TrimSpace(parentID), nil, includeDeleted)
	if err != nil {
		return nil, err
	}
	args = append(args, strings.TrimSpace(fileID))
	query := fmt.Sprintf("SELECT %s FROM (%s) u WHERE u.file_id::text = $%d LIMIT 1", cashCrossStageOuterCols, union, len(args))
	return FirstFile(ctx, pool, query, args...)
}

func getCashCrossStageFiles(ctx context.Context, pool *pgxpool.Pool, module, parentID string, fileIDs []string) ([]FileRecord, []string, error) {
	trimmedIDs := trimStringList(fileIDs)
	union, args, err := cashCrossStageUnion(ctx, module, strings.TrimSpace(parentID), nil, false)
	if err != nil {
		return nil, nil, err
	}
	args = append(args, trimmedIDs)
	query := fmt.Sprintf("SELECT %s FROM (%s) u WHERE u.file_id::text = ANY($%d)", cashCrossStageOuterCols, union, len(args))
	files, err := QueryFiles(ctx, pool, query, args...)
	if err != nil {
		return nil, nil, err
	}
	return files, missingCashCrossStageFileIDs(trimmedIDs, files), nil
}

func cashCrossStageUnion(ctx context.Context, module, parentID string, extraArgs []interface{}, includeDeleted bool) (string, []interface{}, error) {
	if parentID == "" {
		return "", nil, errors.New("parent id required")
	}

	args := append([]interface{}{parentID}, extraArgs...)
	deletedClause := constants.ErrFDReceiptDeletedFilter
	if includeDeleted {
		deletedClause = ""
	}

	switch module {
	case cashCrossStageBankStatement:
		return cashBankStatementUnion(ctx, deletedClause, args)
	case cashCrossStageBankBalance:
		return cashBankBalanceUnion(ctx, deletedClause, args)
	case cashCrossStageLimitPosition:
		return cashLimitPositionUnion(ctx, deletedClause, args)
	case cashCrossStageUtilization:
		return cashUtilizationUnion(ctx, deletedClause, args)
	default:
		return "", nil, fmt.Errorf("module %q is not a cash cross-stage module", module)
	}
}

func cashBankStatementUnion(ctx context.Context, deletedClause string, args []interface{}) (string, []interface{}, error) {
	entityIDs := api.GetEntityIDsFromCtx(ctx)
	if len(entityIDs) == 0 {
		return "", nil, errors.New(constants.ErrNoAccessibleBusinessUnit)
	}
	entityPos := len(args) + 1
	args = append(args, entityIDs)

	scopeClause, scopeArgs, err := cashBankBalanceAccessScope(ctx, len(args)+1)
	if err != nil {
		return "", nil, err
	}
	args = append(args, scopeArgs...)

	query := fmt.Sprintf(`
		SELECT %s
		FROM cimplrcorpsaas.bank_statement_files f
		JOIN cimplrcorpsaas.bank_statements s ON s.bank_statement_id = f.bank_statement_id
		WHERE f.bank_statement_id::text = $1
		  %s
		  AND s.entity_id = ANY($%d)
		UNION ALL
		SELECT %s
		FROM cimplrcorpsaas.bank_balance_files f
		JOIN public.bank_balances_manual b ON b.balance_id = f.balance_id
		JOIN (
			SELECT account_number, MIN(entity_id) AS entity_id
			FROM public.masterbankaccount
			GROUP BY account_number
		) mba ON mba.account_number = b.account_no
		LEFT JOIN public.masterentitycash ec ON ec.entity_id = mba.entity_id
		LEFT JOIN public.masterentity me ON me.entity_id::text = mba.entity_id
		WHERE f.balance_id::text = $1
		  %s
		  AND %s`, cashCrossStageCols, deletedClause, entityPos, cashCrossStageCols, deletedClause, scopeClause)
	return query, args, nil
}

func cashBankBalanceUnion(ctx context.Context, deletedClause string, args []interface{}) (string, []interface{}, error) {
	scopeClause, scopeArgs, err := cashBankBalanceAccessScope(ctx, len(args)+1)
	if err != nil {
		return "", nil, err
	}
	args = append(args, scopeArgs...)

	entityIDs := api.GetEntityIDsFromCtx(ctx)
	if len(entityIDs) == 0 {
		return "", nil, errors.New(constants.ErrNoAccessibleBusinessUnit)
	}
	entityPos := len(args) + 1
	args = append(args, entityIDs)

	query := fmt.Sprintf(`
		SELECT %s
		FROM cimplrcorpsaas.bank_balance_files f
		JOIN public.bank_balances_manual b ON b.balance_id = f.balance_id
		JOIN (
			SELECT account_number, MIN(entity_id) AS entity_id
			FROM public.masterbankaccount
			GROUP BY account_number
		) mba ON mba.account_number = b.account_no
		LEFT JOIN public.masterentitycash ec ON ec.entity_id = mba.entity_id
		LEFT JOIN public.masterentity me ON me.entity_id::text = mba.entity_id
		WHERE f.balance_id::text = $1
		  %s
		  AND %s
		UNION ALL
		SELECT %s
		FROM cimplrcorpsaas.bank_statement_files f
		JOIN cimplrcorpsaas.bank_statements s ON s.bank_statement_id = f.bank_statement_id
		WHERE f.bank_statement_id::text = $1
		  %s
		  AND s.entity_id = ANY($%d)`, cashCrossStageCols, deletedClause, scopeClause, cashCrossStageCols, deletedClause, entityPos)
	return query, args, nil
}

func cashLimitPositionUnion(ctx context.Context, deletedClause string, args []interface{}) (string, []interface{}, error) {
	entityClause, entityArgs := cashLimitEntityScope(ctx, "l", len(args)+1)
	args = append(args, entityArgs...)

	query := fmt.Sprintf(`
		SELECT %s
		FROM cimplrcorpsaas.bank_limit_files f
		JOIN cimplrcorpsaas.bank_limit l ON l.limit_id = f.limit_id
		WHERE f.limit_id::text = $1
		  %s
		  AND COALESCE(l.is_deleted, FALSE) = FALSE
		  %s
		UNION ALL
		SELECT %s
		FROM cimplrcorpsaas.bank_limit_utilization_files f
		JOIN cimplrcorpsaas.bank_limit_utilization u ON u.utilization_id = f.utilization_id
		JOIN cimplrcorpsaas.bank_limit l ON l.limit_id = u.limit_id
		WHERE u.limit_id::text = $1
		  %s
		  AND COALESCE(u.is_deleted, FALSE) = FALSE
		  AND COALESCE(l.is_deleted, FALSE) = FALSE
		  %s`, cashCrossStageCols, deletedClause, entityClause, cashCrossStageCols, deletedClause, entityClause)
	return query, args, nil
}

func cashUtilizationUnion(ctx context.Context, deletedClause string, args []interface{}) (string, []interface{}, error) {
	entityClause, entityArgs := cashLimitEntityScope(ctx, "l", len(args)+1)
	args = append(args, entityArgs...)

	query := fmt.Sprintf(`
		SELECT %s
		FROM cimplrcorpsaas.bank_limit_utilization_files f
		JOIN cimplrcorpsaas.bank_limit_utilization u ON u.utilization_id = f.utilization_id
		JOIN cimplrcorpsaas.bank_limit l ON l.limit_id = u.limit_id
		WHERE f.utilization_id::text = $1
		  %s
		  AND COALESCE(u.is_deleted, FALSE) = FALSE
		  AND COALESCE(l.is_deleted, FALSE) = FALSE
		  %s
		UNION ALL
		SELECT %s
		FROM cimplrcorpsaas.bank_limit_files f
		JOIN cimplrcorpsaas.bank_limit l ON l.limit_id = f.limit_id
		WHERE f.limit_id IN (
			SELECT u.limit_id
			FROM cimplrcorpsaas.bank_limit_utilization u
			WHERE u.utilization_id::text = $1
			  AND COALESCE(u.is_deleted, FALSE) = FALSE
		)
		  %s
		  AND COALESCE(l.is_deleted, FALSE) = FALSE
		  %s`, cashCrossStageCols, deletedClause, entityClause, cashCrossStageCols, deletedClause, entityClause)
	return query, args, nil
}

func softDeleteCashCrossStageFile(ctx context.Context, exec AuditExecutor, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	fileID = strings.TrimSpace(fileID)
	if fileID == "" {
		return false, nil
	}
	for _, table := range cashCrossStageFileTables {
		query := fmt.Sprintf(`
			UPDATE %s
			SET is_deleted = TRUE, deleted_by = $2, deleted_at = $3
			WHERE file_id::text = $1 AND COALESCE(is_deleted, FALSE) = FALSE`, table)
		result, err := exec.Exec(ctx, query, fileID, deletedBy, deletedAt)
		if err != nil {
			return false, err
		}
		if result.RowsAffected() > 0 {
			return true, nil
		}
	}
	return false, nil
}

func cashBankBalanceAccessScope(ctx context.Context, startPosition int) (string, []interface{}, error) {
	entityIDs := api.GetEntityIDsFromCtx(ctx)
	entityNames := api.GetEntityNamesFromCtx(ctx)
	accountNos := api.CtxApprovedAccountNumbers(ctx)
	bankNames := cashLoweredValues(api.CtxApprovedBankNames(ctx))
	currencyCodes := cashLoweredValues(api.CtxApprovedCurrencies(ctx))

	clauses := make([]string, 0, 5)
	args := make([]interface{}, 0, 5)
	position := startPosition

	if len(entityIDs) > 0 {
		clauses = append(clauses, fmt.Sprintf("mba.entity_id = ANY($%d)", position))
		args = append(args, entityIDs)
		position++
	} else if len(entityNames) > 0 {
		clauses = append(clauses, fmt.Sprintf("LOWER(COALESCE(ec.entity_name, me.entity_name, '')) = ANY($%d)", position))
		args = append(args, cashLoweredValues(entityNames))
		position++
	}
	if len(bankNames) > 0 {
		clauses = append(clauses, fmt.Sprintf("LOWER(b.bank_name) = ANY($%d)", position))
		args = append(args, bankNames)
		position++
	}
	if len(accountNos) > 0 {
		clauses = append(clauses, fmt.Sprintf("b.account_no = ANY($%d)", position))
		args = append(args, accountNos)
		position++
	}
	if len(currencyCodes) > 0 {
		clauses = append(clauses, fmt.Sprintf("LOWER(b.currency_code) = ANY($%d)", position))
		args = append(args, currencyCodes)
	}
	if len(clauses) == 0 {
		return "", nil, errors.New(constants.ErrNoAccessibleBusinessUnit)
	}
	return strings.Join(clauses, " AND "), args, nil
}

func cashLimitEntityScope(ctx context.Context, alias string, position int) (string, []interface{}) {
	names := trimStringList(api.GetEntityNamesFromCtx(ctx))
	if len(names) == 0 {
		return " AND FALSE", nil
	}
	return fmt.Sprintf(" AND %s.entity_name = ANY($%d)", alias, position), []interface{}{names}
}

func cashLoweredValues(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if trimmed := strings.ToLower(strings.TrimSpace(value)); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func missingCashCrossStageFileIDs(expected []string, files []FileRecord) []string {
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
