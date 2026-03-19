package approvalengine

import (
	"CimplrCorpSaas/api"
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// finalizeRecord writes the final approval decision back to the legacy audit
// table and, for approved DELETEs, flips is_deleted on the master record.
//
// IMPORTANT: auditTable, auditIDColumn, and recordTable come from the
// uam.approval_instance row written by CreateInstance — they are internal
// system values stored at submission time, NOT live user input. They are
// validated to be non-empty before being interpolated into queries.
func finalizeRecord(
	ctx context.Context,
	tx pgx.Tx,
	recordID string,
	auditTable string,
	auditIDColumn string,
	recordTable string,
	actionType string,
	finalStatus string,
	checkerEmail string,
	checkerComment string,
) error {
	// Validate that no field is empty before using it in a dynamic query.
	if auditTable == "" || auditIDColumn == "" || recordTable == "" {
		return fmt.Errorf("finalizeRecord: missing table/column info for record %s", recordID)
	}

	// Step 1: Map final status to legacy processing_status string.
	processingStatus := "APPROVED"
	if finalStatus == InstStatusRejected {
		processingStatus = "REJECTED"
	}

	// Step 2: UPDATE the legacy audit table.
	// Table and column names are system-controlled values stored in the
	// approval_instance row — they originate from module code, not request bodies.
	auditQ := fmt.Sprintf(`
		UPDATE %s
		SET processing_status = $1,
		    checker_by        = $2,
		    checker_at        = now(),
		    checker_comment   = $3
		WHERE %s = $4
		  AND processing_status LIKE 'PENDING%%'
	`, auditTable, auditIDColumn)

	tag, err := tx.Exec(ctx, auditQ, processingStatus, checkerEmail, checkerComment, recordID)
	if err != nil {
		return fmt.Errorf("finalizeRecord audit update: %w", err)
	}
	api.LogInfo("[ApprovalEngine] Finalized audit: table=%s record=%s status=%s rows=%d",
		auditTable, recordID, processingStatus, tag.RowsAffected())

	// Step 3: For approved DELETEs, flip is_deleted on the master record.
	if actionType == "DELETE" && finalStatus == InstStatusApproved {
		// Table and column names are system-controlled — see note above.
		delQ := fmt.Sprintf(`
			UPDATE %s SET is_deleted = true WHERE %s = $1
		`, recordTable, auditIDColumn)
		_, err := tx.Exec(ctx, delQ, recordID)
		if err != nil {
			return fmt.Errorf("finalizeRecord delete flip: %w", err)
		}
		api.LogInfo("[ApprovalEngine] is_deleted flipped: table=%s record=%s", recordTable, recordID)
	}

	return nil
}
