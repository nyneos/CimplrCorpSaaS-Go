package additionalfiles

import (
	cashfiles "CimplrCorpSaas/api/cash/additionalfiles"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

var fdCrossStageFileTables = []string{
	"investment.fd_booking_request_files",
	"investment.fd_confirmation_files",
	"investment.fd_master_files",
	"investment.fd_closure_request_files",
}

const fdCrossStageCols = "f.file_id, f.stored_file_name, f.content_type, f.file_size, f.upload_s3_key, f.uploaded_by, f.uploaded_at"

const fdCrossStageOuterCols = "file_id, stored_file_name, content_type, file_size, upload_s3_key, uploaded_by, uploaded_at"

func fdCrossStageUnion(module, deletedClause string) (string, bool) {
	c := fdCrossStageCols
	d := deletedClause
	switch module {
	case fdBookingFilesDefinition.Module:
		return fmt.Sprintf(`
			SELECT %s FROM investment.fd_booking_request_files f WHERE f.booking_id::text = $1 %s
			/*
			Disabled for one-way FD DMS visibility.
			UNION ALL
			SELECT %s FROM investment.fd_confirmation_files f
			  WHERE f.confirmation_id IN (
			    SELECT c.confirmation_id FROM investment.fd_confirmation c
			    WHERE c.booking_id::text = $1 AND COALESCE(c.is_deleted, FALSE) = FALSE
			  ) %s
			UNION ALL
			SELECT %s FROM investment.fd_closure_request_files f
			  WHERE f.closure_request_id IN (
			    SELECT r.closure_request_id FROM investment.fd_closure_request r
			    JOIN investment.fd_master m ON m.fd_id = r.fd_id
			    WHERE m.booking_id::text = $1 AND COALESCE(m.is_deleted, FALSE) = FALSE AND COALESCE(r.is_deleted, FALSE) = FALSE
			  ) %s
			*/
			`, c, d, c, d, c, d), true

	case fdConfirmationFilesDefinition.Module:
		return fmt.Sprintf(`
			SELECT %s FROM investment.fd_confirmation_files f WHERE f.confirmation_id::text = $1 %s
			UNION ALL
			SELECT %s FROM investment.fd_booking_request_files f
			  WHERE f.booking_id IN (
			    SELECT c.booking_id FROM investment.fd_confirmation c
			    WHERE c.confirmation_id::text = $1 AND COALESCE(c.is_deleted, FALSE) = FALSE
			  ) %s


			/*
			Disabled for one-way FD DMS visibility.

			UNION ALL
			SELECT %s FROM investment.fd_master_files f
			  WHERE f.fd_id IN (
			    SELECT m.fd_id FROM investment.fd_master m
			    WHERE m.confirmation_id::text = $1 AND COALESCE(m.is_deleted, FALSE) = FALSE
			  ) %s
			UNION ALL
			SELECT %s FROM investment.fd_closure_request_files f
			  WHERE f.closure_request_id IN (
			    SELECT r.closure_request_id FROM investment.fd_closure_request r
			    JOIN investment.fd_master m ON m.fd_id = r.fd_id
			    WHERE m.confirmation_id::text = $1 AND COALESCE(m.is_deleted, FALSE) = FALSE AND COALESCE(r.is_deleted, FALSE) = FALSE
			  ) %s
			*/
			`, c, d, c, d, c, d, c, d), true

	case fdMasterFilesDefinition.Module:
		return fmt.Sprintf(`
			SELECT %s FROM investment.fd_master_files f WHERE f.fd_id::text = $1 %s
			UNION ALL
			SELECT %s FROM investment.fd_booking_request_files f
			  WHERE f.booking_id IN (
			    SELECT m.booking_id FROM investment.fd_master m
			    WHERE m.fd_id::text = $1 AND COALESCE(m.is_deleted, FALSE) = FALSE
			  ) %s
			UNION ALL
			SELECT %s FROM investment.fd_confirmation_files f
			  WHERE f.confirmation_id IN (
			    SELECT m.confirmation_id FROM investment.fd_master m
			    WHERE m.fd_id::text = $1 AND COALESCE(m.is_deleted, FALSE) = FALSE
			  ) %s

			  
			/*
			Disabled for one-way FD DMS visibility.
			UNION ALL
			SELECT %s FROM investment.fd_closure_request_files f
			  WHERE f.closure_request_id IN (
			    SELECT r.closure_request_id FROM investment.fd_closure_request r
			    WHERE r.fd_id::text = $1 AND COALESCE(r.is_deleted, FALSE) = FALSE
			  ) %s
			*/
			`, c, d, c, d, c, d, c, d), true

	case fdClosureFilesDefinition.Module:
		return fmt.Sprintf(`
			SELECT %s FROM investment.fd_closure_request_files f WHERE f.closure_request_id::text = $1 %s
			UNION ALL
			SELECT %s FROM investment.fd_booking_request_files f
			  WHERE f.booking_id IN (
			    SELECT m.booking_id FROM investment.fd_master m
			    JOIN investment.fd_closure_request r ON r.fd_id = m.fd_id
			    WHERE r.closure_request_id::text = $1 AND COALESCE(m.is_deleted, FALSE) = FALSE
			  ) %s
			UNION ALL
			SELECT %s FROM investment.fd_confirmation_files f
			  WHERE f.confirmation_id IN (
			    SELECT m.confirmation_id FROM investment.fd_master m
			    JOIN investment.fd_closure_request r ON r.fd_id = m.fd_id
			    WHERE r.closure_request_id::text = $1 AND COALESCE(m.is_deleted, FALSE) = FALSE
			  ) %s
			UNION ALL
			SELECT %s FROM investment.fd_master_files f
			  WHERE f.fd_id IN (
			    SELECT m.fd_id FROM investment.fd_master m
			    JOIN investment.fd_closure_request r ON r.fd_id = m.fd_id
			    WHERE r.closure_request_id::text = $1 AND COALESCE(m.is_deleted, FALSE) = FALSE
			  ) %s`, c, d, c, d, c, d, c, d), true
	}
	return "", false
}

func isFDCrossStageModule(module string) bool {
	_, ok := fdCrossStageUnion(module, "")
	return ok
}

func listFDCrossStageFiles(ctx context.Context, pool *pgxpool.Pool, module, parentID string) ([]cashfiles.FileRecord, error) {
	union, ok := fdCrossStageUnion(module, "AND COALESCE(f.is_deleted, FALSE) = FALSE")
	if !ok {
		return nil, fmt.Errorf("module %q is not a cross-stage FD module", module)
	}
	query := union + "\nORDER BY uploaded_at DESC"
	return cashfiles.QueryFiles(ctx, pool, query, strings.TrimSpace(parentID))
}

func getFDCrossStageFile(ctx context.Context, pool *pgxpool.Pool, module, parentID, fileID string, includeDeleted bool) (*cashfiles.FileRecord, error) {
	deletedClause := "AND COALESCE(f.is_deleted, FALSE) = FALSE"
	if includeDeleted {
		deletedClause = ""
	}
	union, ok := fdCrossStageUnion(module, deletedClause)
	if !ok {
		return nil, fmt.Errorf("module %q is not a cross-stage FD module", module)
	}
	query := fmt.Sprintf("SELECT %s FROM (%s) u WHERE u.file_id::text = $2 LIMIT 1", fdCrossStageOuterCols, union)
	return cashfiles.FirstFile(ctx, pool, query, strings.TrimSpace(parentID), strings.TrimSpace(fileID))
}

func getFDCrossStageFiles(ctx context.Context, pool *pgxpool.Pool, module, parentID string, fileIDs []string) ([]cashfiles.FileRecord, []string, error) {
	trimmedIDs := trimInvestmentAdditionalFileIDs(fileIDs)
	union, ok := fdCrossStageUnion(module, "AND COALESCE(f.is_deleted, FALSE) = FALSE")
	if !ok {
		return nil, nil, fmt.Errorf("module %q is not a cross-stage FD module", module)
	}
	query := fmt.Sprintf("SELECT %s FROM (%s) u WHERE u.file_id::text = ANY($2)", fdCrossStageOuterCols, union)
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

func softDeleteFDCrossStageFile(ctx context.Context, exec investmentFileExec, fileID, deletedBy string, deletedAt time.Time) (bool, error) {
	fileID = strings.TrimSpace(fileID)
	if fileID == "" {
		return false, nil
	}
	for _, table := range fdCrossStageFileTables {
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
