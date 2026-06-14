package forwards

import (
	"CimplrCorpSaas/api/cash/additionalfiles"
	"CimplrCorpSaas/api/constants"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var forwardCrossStageFileTables = []string{
	"public.forward_mtm_files",
	"public.forward_booking_files",
}

const forwardCrossStageCols = "f.file_id, f.stored_file_name, f.content_type, f.file_size, f.upload_s3_key, f.uploaded_by, f.uploaded_at"

const forwardCrossStageOuterCols = "file_id, stored_file_name, content_type, file_size, upload_s3_key, uploaded_by, uploaded_at"

// $1 = parent id, $2 = entity names. One-way: MTM sees the upstream forward booking.
func forwardCrossStageUnion(module, d string) (string, bool) {
	c := forwardCrossStageCols
	switch module {
	case "fx-forward":
		// FX Forward Booking / Confirmation — system_transaction_id
		return fmt.Sprintf(`SELECT %s FROM public.forward_booking_files f
			JOIN public.forward_bookings b ON b.system_transaction_id = f.system_transaction_id
			WHERE f.system_transaction_id = $1 AND LOWER(TRIM(b.entity_level_0)) = ANY($2) %s`, c, d), true
	case "fx-mtm":
		// FX MTM — mtm_id (own) + upstream FX Forward linked via internal_reference_id
		return fmt.Sprintf(`SELECT %s FROM public.forward_mtm_files f
			JOIN public.forward_mtm m ON m.mtm_id = f.mtm_id
			WHERE f.mtm_id = $1 AND LOWER(TRIM(m.entity)) = ANY($2) %s
			UNION ALL
			SELECT %s FROM public.forward_booking_files f
			JOIN public.forward_bookings b ON b.system_transaction_id = f.system_transaction_id
			WHERE b.internal_reference_id IN (
				SELECT m.internal_reference_id FROM public.forward_mtm m WHERE m.mtm_id = $1
			) AND LOWER(TRIM(b.entity_level_0)) = ANY($2) %s`, c, d, c, d), true
	}
	return "", false
}

func forwardCrossStageParentID(module, parentID string) string {
	if module == "fx-mtm" {
		return normalizeMTMID(parentID)
	}
	return strings.TrimSpace(parentID)
}

func listForwardCrossStageFiles(ctx context.Context, pool *pgxpool.Pool, module, parentID string) ([]additionalfiles.FileRecord, error) {
	names, err := forwardEntityNames(ctx)
	if err != nil {
		return nil, err
	}
	union, ok := forwardCrossStageUnion(module, constants.ErrFDReceiptDeletedFilter)
	if !ok {
		return nil, fmt.Errorf("module %q is not a cross-stage FX forward module", module)
	}
	return additionalfiles.QueryFiles(ctx, pool, union+"\nORDER BY uploaded_at DESC", forwardCrossStageParentID(module, parentID), names)
}

func getForwardCrossStageFile(ctx context.Context, pool *pgxpool.Pool, module, parentID, fileID string, includeDeleted bool) (*additionalfiles.FileRecord, error) {
	names, err := forwardEntityNames(ctx)
	if err != nil {
		return nil, err
	}
	d := constants.ErrFDReceiptDeletedFilter
	if includeDeleted {
		d = ""
	}
	union, ok := forwardCrossStageUnion(module, d)
	if !ok {
		return nil, fmt.Errorf("module %q is not a cross-stage FX forward module", module)
	}
	query := fmt.Sprintf("SELECT %s FROM (%s) u WHERE u.file_id::text = $3 LIMIT 1", forwardCrossStageOuterCols, union)
	return additionalfiles.FirstFile(ctx, pool, query, forwardCrossStageParentID(module, parentID), names, strings.TrimSpace(fileID))
}

func getForwardCrossStageFiles(ctx context.Context, pool *pgxpool.Pool, module, parentID string, fileIDs []string) ([]additionalfiles.FileRecord, []string, error) {
	names, err := forwardEntityNames(ctx)
	if err != nil {
		return nil, nil, err
	}
	trimmedIDs := trimForwardAdditionalFileIDs(fileIDs)
	union, ok := forwardCrossStageUnion(module, constants.ErrFDReceiptDeletedFilter)
	if !ok {
		return nil, nil, fmt.Errorf("module %q is not a cross-stage FX forward module", module)
	}
	query := fmt.Sprintf("SELECT %s FROM (%s) u WHERE u.file_id::text = ANY($3)", forwardCrossStageOuterCols, union)
	files, err := additionalfiles.QueryFiles(ctx, pool, query, forwardCrossStageParentID(module, parentID), names, trimmedIDs)
	if err != nil {
		return nil, nil, err
	}
	return files, missingForwardAdditionalFileIDs(trimmedIDs, files), nil
}

func softDeleteForwardCrossStageFile(ctx context.Context, exec forwardAdditionalFileExec, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	fileID = strings.TrimSpace(fileID)
	if fileID == "" {
		return false, nil
	}
	for _, table := range forwardCrossStageFileTables {
		result, err := exec.Exec(ctx, fmt.Sprintf(`UPDATE %s
			SET is_deleted = TRUE, deleted_by = $2, deleted_at = $3
			WHERE file_id::text = $1 AND COALESCE(is_deleted, FALSE) = FALSE`, table), fileID, deletedBy, deletedAt)
		if err != nil {
			return false, err
		}
		if result.RowsAffected() > 0 {
			return true, nil
		}
	}
	return false, nil
}

func withForwardCrossStage(cfg additionalfiles.Config) additionalfiles.Config {
	module := cfg.Module
	cfg.AuditByFileIDOnly = true
	cfg.List = func(ctx context.Context, pool *pgxpool.Pool, parentID string) ([]additionalfiles.FileRecord, error) {
		return listForwardCrossStageFiles(ctx, pool, module, parentID)
	}
	cfg.GetOne = func(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*additionalfiles.FileRecord, error) {
		return getForwardCrossStageFile(ctx, pool, module, parentID, fileID, false)
	}
	cfg.GetAnyFile = func(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*additionalfiles.FileRecord, error) {
		return getForwardCrossStageFile(ctx, pool, module, parentID, fileID, true)
	}
	cfg.GetMany = func(ctx context.Context, pool *pgxpool.Pool, parentID string, fileIDs []string) ([]additionalfiles.FileRecord, []string, error) {
		return getForwardCrossStageFiles(ctx, pool, module, parentID, fileIDs)
	}
	cfg.SoftDelete = func(ctx context.Context, pool *pgxpool.Pool, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
		return softDeleteForwardCrossStageFile(ctx, pool, fileID, deletedBy, deletedAt)
	}
	cfg.SoftDeleteTx = func(ctx context.Context, tx pgx.Tx, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
		return softDeleteForwardCrossStageFile(ctx, tx, fileID, deletedBy, deletedAt)
	}
	return cfg
}
