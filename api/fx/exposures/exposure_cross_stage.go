package exposures

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

var exposureCrossStageFileTables = []string{
	"public.exposure_header_files",
	"public.pending_exposure_bucketing_files",
	"public.exposure_bucketing_files",
}

const exposureCrossStageCols = "f.file_id, f.stored_file_name, f.content_type, f.file_size, f.upload_s3_key, f.uploaded_by, f.uploaded_at"

const exposureCrossStageOuterCols = "file_id, stored_file_name, content_type, file_size, upload_s3_key, uploaded_by, uploaded_at"

// $1 = exposure_header_id, $2 = entity names. One-way: downstream sees upstream.
func exposureCrossStageUnion(module, d string) (string, bool) {
	c := exposureCrossStageCols
	// Exposure Upload — exposure_header_id
	exposure := fmt.Sprintf(`SELECT %s FROM public.exposure_header_files f
		JOIN public.exposure_headers h ON h.exposure_header_id = f.exposure_header_id
		WHERE f.exposure_header_id::text = $1 AND LOWER(TRIM(h.entity)) = ANY($2) %s`, c, d)
	// Pending Exposure Bucketing — exposure_header_id
	pending := fmt.Sprintf(`SELECT %s FROM public.pending_exposure_bucketing_files f
		JOIN public.exposure_headers h ON h.exposure_header_id = f.exposure_header_id
		WHERE f.exposure_header_id::text = $1 AND LOWER(TRIM(h.entity)) = ANY($2) %s`, c, d)
	// Exposure Bucketing — exposure_header_id
	bucketing := fmt.Sprintf(`SELECT %s FROM public.exposure_bucketing_files f
		JOIN public.exposure_headers h ON h.exposure_header_id = f.exposure_header_id
		WHERE f.exposure_header_id::text = $1 AND LOWER(TRIM(h.entity)) = ANY($2) %s`, c, d)

	switch module {
	case "fx-exposure":
		return exposure, true
	case "fx-pending-exposure-bucketing":
		return exposure + constants.UnionAll + pending, true
	case "fx-exposure-bucketing":
		return exposure + constants.UnionAll + pending + constants.UnionAll + bucketing, true
	}
	return "", false
}

func listExposureCrossStageFiles(ctx context.Context, pool *pgxpool.Pool, module, parentID string) ([]additionalfiles.FileRecord, error) {
	names, err := fxEntityNames(ctx)
	if err != nil {
		return nil, err
	}
	union, ok := exposureCrossStageUnion(module, constants.ErrFDReceiptDeletedFilter)
	if !ok {
		return nil, fmt.Errorf(constants.ErrInvalidCrossStageModule, module)
	}
	return additionalfiles.QueryFiles(ctx, pool, union+"\nORDER BY uploaded_at DESC", strings.TrimSpace(parentID), names)
}

func getExposureCrossStageFile(ctx context.Context, pool *pgxpool.Pool, module, parentID, fileID string, includeDeleted bool) (*additionalfiles.FileRecord, error) {
	names, err := fxEntityNames(ctx)
	if err != nil {
		return nil, err
	}
	d := constants.ErrFDReceiptDeletedFilter
	if includeDeleted {
		d = ""
	}
	union, ok := exposureCrossStageUnion(module, d)
	if !ok {
		return nil, fmt.Errorf(constants.ErrInvalidCrossStageModule, module)
	}
	query := fmt.Sprintf("SELECT %s FROM (%s) u WHERE u.file_id::text = $3 LIMIT 1", exposureCrossStageOuterCols, union)
	return additionalfiles.FirstFile(ctx, pool, query, strings.TrimSpace(parentID), names, strings.TrimSpace(fileID))
}

func getExposureCrossStageFiles(ctx context.Context, pool *pgxpool.Pool, module, parentID string, fileIDs []string) ([]additionalfiles.FileRecord, []string, error) {
	names, err := fxEntityNames(ctx)
	if err != nil {
		return nil, nil, err
	}
	trimmedIDs := trimFXAdditionalFileIDs(fileIDs)
	union, ok := exposureCrossStageUnion(module, constants.ErrFDReceiptDeletedFilter)
	if !ok {
		return nil, nil, fmt.Errorf(constants.ErrInvalidCrossStageModule, module)
	}
	query := fmt.Sprintf("SELECT %s FROM (%s) u WHERE u.file_id::text = ANY($3)", exposureCrossStageOuterCols, union)
	files, err := additionalfiles.QueryFiles(ctx, pool, query, strings.TrimSpace(parentID), names, trimmedIDs)
	if err != nil {
		return nil, nil, err
	}
	return files, missingFXAdditionalFileIDs(trimmedIDs, files), nil
}

func softDeleteExposureCrossStageFile(ctx context.Context, exec fxAdditionalFileExec, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	fileID = strings.TrimSpace(fileID)
	if fileID == "" {
		return false, nil
	}
	for _, table := range exposureCrossStageFileTables {
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

func withExposureCrossStage(cfg additionalfiles.Config) additionalfiles.Config {
	module := cfg.Module
	cfg.AuditByFileIDOnly = true
	cfg.List = func(ctx context.Context, pool *pgxpool.Pool, parentID string) ([]additionalfiles.FileRecord, error) {
		return listExposureCrossStageFiles(ctx, pool, module, parentID)
	}
	cfg.GetOne = func(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*additionalfiles.FileRecord, error) {
		return getExposureCrossStageFile(ctx, pool, module, parentID, fileID, false)
	}
	cfg.GetAnyFile = func(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*additionalfiles.FileRecord, error) {
		return getExposureCrossStageFile(ctx, pool, module, parentID, fileID, true)
	}
	cfg.GetMany = func(ctx context.Context, pool *pgxpool.Pool, parentID string, fileIDs []string) ([]additionalfiles.FileRecord, []string, error) {
		return getExposureCrossStageFiles(ctx, pool, module, parentID, fileIDs)
	}
	cfg.SoftDelete = func(ctx context.Context, pool *pgxpool.Pool, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
		return softDeleteExposureCrossStageFile(ctx, pool, fileID, deletedBy, deletedAt)
	}
	cfg.SoftDeleteTx = func(ctx context.Context, tx pgx.Tx, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
		return softDeleteExposureCrossStageFile(ctx, tx, fileID, deletedBy, deletedAt)
	}
	return cfg
}
