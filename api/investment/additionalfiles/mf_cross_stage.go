package additionalfiles

import (
	cashfiles "CimplrCorpSaas/api/cash/additionalfiles"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

var mfCrossStageFileTables = []string{
	"investment.investment_initiation_files",
	"investment.investment_confirmation_files",
	"investment.redemption_initiation_files",
	"investment.redemption_confirmation_files",
}

const mfCrossStageCols = "f.file_id, f.stored_file_name, f.content_type, f.file_size, f.upload_s3_key, f.uploaded_by, f.uploaded_at"

const mfCrossStageOuterCols = "file_id, stored_file_name, content_type, file_size, upload_s3_key, uploaded_by, uploaded_at"

const mfCrossStageHoldingMatch = `
			SELECT i.initiation_id
			FROM investment.investment_initiation i
			JOIN investment.redemption_initiation r ON r.redemption_id::text = $1
			WHERE COALESCE(i.is_deleted, FALSE) = FALSE
			  AND COALESCE(r.is_deleted, FALSE) = FALSE
			  AND BTRIM(COALESCE(i.entity_name::text, '')) = BTRIM(COALESCE(r.entity_name::text, ''))
			  AND BTRIM(COALESCE(i.scheme_id::text, '')) = BTRIM(COALESCE(r.scheme_id::text, ''))
			  AND (
			    (
			      BTRIM(COALESCE(i.folio_id::text, '')) <> ''
			      AND BTRIM(COALESCE(i.folio_id::text, '')) = BTRIM(COALESCE(r.folio_id::text, ''))
			    )
			    OR (
			      BTRIM(COALESCE(i.demat_id::text, '')) <> ''
			      AND BTRIM(COALESCE(i.demat_id::text, '')) = BTRIM(COALESCE(r.demat_id::text, ''))
			    )
			  )`

func mfCrossStageUnion(module, deletedClause string) (string, bool) {
	c := mfCrossStageCols
	d := deletedClause
	switch module {
	case initiationFilesDefinition.Module:
		return fmt.Sprintf(`
			SELECT %s FROM investment.investment_initiation_files f WHERE f.initiation_id::text = $1 %s`, c, d), true

	case confirmationFilesDefinition.Module:
		return fmt.Sprintf(`
			SELECT %s FROM investment.investment_confirmation_files f WHERE f.confirmation_id::text = $1 %s
			UNION ALL
			SELECT %s FROM investment.investment_initiation_files f
			  WHERE f.initiation_id::text IN (
			    SELECT c.initiation_id::text FROM investment.investment_confirmation c
			    WHERE c.confirmation_id::text = $1 AND COALESCE(c.is_deleted, FALSE) = FALSE
			  ) %s`, c, d, c, d), true

	case redemptionInitiationFilesDefinition.Module:
		return fmt.Sprintf(`
			SELECT %s FROM investment.redemption_initiation_files f WHERE f.redemption_id::text = $1 %s
			UNION ALL
			SELECT %s FROM investment.investment_initiation_files f
			  WHERE f.initiation_id IN (%s) %s
			UNION ALL
			SELECT %s FROM investment.investment_confirmation_files f
			  WHERE f.confirmation_id IN (
			    SELECT c.confirmation_id
			    FROM investment.investment_confirmation c
			    WHERE COALESCE(c.is_deleted, FALSE) = FALSE
			      AND c.initiation_id IN (%s)
			  ) %s`, c, d, c, mfCrossStageHoldingMatch, d, c, mfCrossStageHoldingMatch, d), true

	case redemptionConfirmationFilesDefinition.Module:
		return fmt.Sprintf(`
			SELECT %s FROM investment.redemption_confirmation_files f WHERE f.redemption_confirm_id::text = $1 %s
			UNION ALL
			SELECT %s FROM investment.redemption_initiation_files f
			  WHERE f.redemption_id IN (
			    SELECT c.redemption_id FROM investment.redemption_confirmation c
			    WHERE c.redemption_confirm_id::text = $1 AND COALESCE(c.is_deleted, FALSE) = FALSE
			  ) %s
			UNION ALL
			SELECT %s FROM investment.investment_initiation_files f
			  WHERE f.initiation_id IN (
			    SELECT i.initiation_id
			    FROM investment.investment_initiation i
			    JOIN investment.redemption_initiation r ON r.redemption_id IN (
			      SELECT c.redemption_id FROM investment.redemption_confirmation c
			      WHERE c.redemption_confirm_id::text = $1 AND COALESCE(c.is_deleted, FALSE) = FALSE
			    )
			    WHERE COALESCE(i.is_deleted, FALSE) = FALSE
			      AND COALESCE(r.is_deleted, FALSE) = FALSE
			      AND BTRIM(COALESCE(i.entity_name::text, '')) = BTRIM(COALESCE(r.entity_name::text, ''))
			      AND BTRIM(COALESCE(i.scheme_id::text, '')) = BTRIM(COALESCE(r.scheme_id::text, ''))
			      AND (
			        (
			          BTRIM(COALESCE(i.folio_id::text, '')) <> ''
			          AND BTRIM(COALESCE(i.folio_id::text, '')) = BTRIM(COALESCE(r.folio_id::text, ''))
			        )
			        OR (
			          BTRIM(COALESCE(i.demat_id::text, '')) <> ''
			          AND BTRIM(COALESCE(i.demat_id::text, '')) = BTRIM(COALESCE(r.demat_id::text, ''))
			        )
			      )
			  ) %s
			UNION ALL
			SELECT %s FROM investment.investment_confirmation_files f
			  WHERE f.confirmation_id IN (
			    SELECT c.confirmation_id
			    FROM investment.investment_confirmation c
			    WHERE COALESCE(c.is_deleted, FALSE) = FALSE
			      AND c.initiation_id IN (
			        SELECT i.initiation_id
			        FROM investment.investment_initiation i
			        JOIN investment.redemption_initiation r ON r.redemption_id IN (
			          SELECT rc.redemption_id FROM investment.redemption_confirmation rc
			          WHERE rc.redemption_confirm_id::text = $1 AND COALESCE(rc.is_deleted, FALSE) = FALSE
			        )
			        WHERE COALESCE(i.is_deleted, FALSE) = FALSE
			          AND COALESCE(r.is_deleted, FALSE) = FALSE
			          AND BTRIM(COALESCE(i.entity_name::text, '')) = BTRIM(COALESCE(r.entity_name::text, ''))
			          AND BTRIM(COALESCE(i.scheme_id::text, '')) = BTRIM(COALESCE(r.scheme_id::text, ''))
			          AND (
			            (
			              BTRIM(COALESCE(i.folio_id::text, '')) <> ''
			              AND BTRIM(COALESCE(i.folio_id::text, '')) = BTRIM(COALESCE(r.folio_id::text, ''))
			            )
			            OR (
			              BTRIM(COALESCE(i.demat_id::text, '')) <> ''
			              AND BTRIM(COALESCE(i.demat_id::text, '')) = BTRIM(COALESCE(r.demat_id::text, ''))
			            )
			          )
			      )
			  ) %s`, c, d, c, d, c, d, c, d), true
	}
	return "", false
}

func isMFCrossStageModule(module string) bool {
	_, ok := mfCrossStageUnion(module, "")
	return ok
}

func listMFCrossStageFiles(ctx context.Context, pool *pgxpool.Pool, module, parentID string) ([]cashfiles.FileRecord, error) {
	union, ok := mfCrossStageUnion(module, "AND COALESCE(f.is_deleted, FALSE) = FALSE")
	if !ok {
		return nil, fmt.Errorf("module %q is not a cross-stage MF module", module)
	}
	query := union + "\nORDER BY uploaded_at DESC"
	return cashfiles.QueryFiles(ctx, pool, query, strings.TrimSpace(parentID))
}

func getMFCrossStageFile(ctx context.Context, pool *pgxpool.Pool, module, parentID, fileID string, includeDeleted bool) (*cashfiles.FileRecord, error) {
	deletedClause := "AND COALESCE(f.is_deleted, FALSE) = FALSE"
	if includeDeleted {
		deletedClause = ""
	}
	union, ok := mfCrossStageUnion(module, deletedClause)
	if !ok {
		return nil, fmt.Errorf("module %q is not a cross-stage MF module", module)
	}
	query := fmt.Sprintf("SELECT %s FROM (%s) u WHERE u.file_id::text = $2 LIMIT 1", mfCrossStageOuterCols, union)
	return cashfiles.FirstFile(ctx, pool, query, strings.TrimSpace(parentID), strings.TrimSpace(fileID))
}

func getMFCrossStageFiles(ctx context.Context, pool *pgxpool.Pool, module, parentID string, fileIDs []string) ([]cashfiles.FileRecord, []string, error) {
	trimmedIDs := trimInvestmentAdditionalFileIDs(fileIDs)
	union, ok := mfCrossStageUnion(module, "AND COALESCE(f.is_deleted, FALSE) = FALSE")
	if !ok {
		return nil, nil, fmt.Errorf("module %q is not a cross-stage MF module", module)
	}
	query := fmt.Sprintf("SELECT %s FROM (%s) u WHERE u.file_id::text = ANY($2)", mfCrossStageOuterCols, union)
	files, err := cashfiles.QueryFiles(ctx, pool, query, strings.TrimSpace(parentID), trimmedIDs)
	if err != nil {
		return nil, nil, err
	}

	found := make(map[string]struct{}, len(files))
	for _, file := range files {
		found[file.FileID] = struct{}{}
	}
	missing := make([]string, 0)
	for _, id := range trimmedIDs {
		if _, ok := found[id]; !ok {
			missing = append(missing, id)
		}
	}
	return files, missing, nil
}

func softDeleteMFCrossStageFile(ctx context.Context, exec investmentFileExec, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	fileID = strings.TrimSpace(fileID)
	if fileID == "" {
		return false, nil
	}
	for _, table := range mfCrossStageFileTables {
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
