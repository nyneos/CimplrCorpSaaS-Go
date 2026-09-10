package fdAccounting

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	fdclosingcommon "CimplrCorpSaas/api/investment/fdMonthEndClosing/common"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostJournal handles POST /investment/fd/accounting/journal/post (AP-05).
// With no ERP, "posting" = validate + flip APPROVED → POSTED in our ledger.
func PostJournal(pool *pgxpool.Pool) http.HandlerFunc {
	return postHandler(pool, "POST", []string{statusApproved})
}

// RetryJournal handles POST /investment/fd/accounting/journal/retry (AP-06).
func RetryJournal(pool *pgxpool.Pool) http.HandlerFunc {
	return postHandler(pool, "RETRY", []string{statusFailed})
}

func postHandler(pool *pgxpool.Pool, actionType string, allowedFrom []string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req actionRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		ids := mergeIDs(req.EntryID, req.EntryIDs)
		if len(ids) == 0 {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, "entry_id or entry_ids is required")
			return
		}
		actor, ok := fdclosingcommon.ActorFromRequest(r)
		if !ok {
			fdclosingcommon.RespondError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		ctx := r.Context()
		posted, failed := 0, 0
		results := make([]map[string]interface{}, 0, len(ids))
		for _, id := range ids {
			outcome := postOne(ctx, pool, id, actionType, allowedFrom, actor.Email)
			results = append(results, outcome)
			if outcome["success"] == true {
				posted++
			} else {
				failed++
			}
		}
		payload := map[string]interface{}{"posted": posted, "failed": failed, "results": results, "posting_mode": postingModeValue}
		if posted == 0 {
			fdclosingcommon.RespondFailureWithData(w, http.StatusConflict, "No journal entries were posted", payload)
			return
		}
		fdclosingcommon.RespondSuccess(w, fmt.Sprintf("%d journal entr%s posted to ledger", posted, plural(posted)), payload)
	}
}

func postOne(ctx context.Context, pool *pgxpool.Pool, entryID, actionType string, allowedFrom []string, actorEmail string) map[string]interface{} {
	fail := func(msg string) map[string]interface{} {
		return map[string]interface{}{"entry_id": entryID, "success": false, "error": msg}
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return fail(constants.ErrTxBeginFailedCapitalized + err.Error())
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	var (
		status, entityID, entityName, reversalOf string
		entryDate                                time.Time
		totalDebit, totalCredit                  float64
		isReversal                               bool
	)
	err = tx.QueryRow(ctx, `
		SELECT COALESCE(status,''), COALESCE(entity_id,''), COALESCE(entity_name,''), COALESCE(reversal_of_entry_id,''),
		       entry_date, COALESCE(total_debit,0), COALESCE(total_credit,0), COALESCE(is_reversal,false)
		FROM `+journalTable+` WHERE entry_id = $1 AND COALESCE(is_deleted,false) = false FOR UPDATE`, entryID).
		Scan(&status, &entityID, &entityName, &reversalOf, &entryDate, &totalDebit, &totalCredit, &isReversal)
	if err == pgx.ErrNoRows {
		return fail("journal entry not found")
	}
	if err != nil {
		return fail(constants.ErrQueryFailed + err.Error())
	}
	allowed := false
	for _, s := range allowedFrom {
		if s == status {
			allowed = true
		}
	}
	if !allowed {
		return fail("entry is " + status + "; " + actionType + " requires " + fmt.Sprint(allowedFrom))
	}
	// Maker-checker gate: the newest audit decision must be APPROVED.
	if latest, lerr := latestAuditStatus(ctx, tx, entryID); lerr != nil {
		return fail(constants.ErrQueryFailed + lerr.Error())
	} else if latest != constants.StatusApproved && latest != "COMPLETED" && latest != "FAILED" {
		return fail("entry has not been approved by a checker (latest action " + latest + ")")
	}

	// Validations = the whole "ERP response".
	reason := ""
	if locked, why, lerr := periodLocked(ctx, tx, entityID, entityName, entryDate); lerr != nil {
		return fail(constants.ErrQueryFailed + lerr.Error())
	} else if locked {
		reason = why
	}
	if reason == "" {
		lines, lerr := loadLines(ctx, tx, entryID)
		if lerr != nil {
			return fail(constants.ErrQueryFailed + lerr.Error())
		}
		reason = validateForPosting(lines, totalDebit, totalCredit)
	}
	if reason == "" && isReversal && reversalOf != "" {
		var origStatus string
		if oerr := tx.QueryRow(ctx, `SELECT COALESCE(status,'') FROM `+journalTable+` WHERE entry_id = $1 FOR UPDATE`, reversalOf).Scan(&origStatus); oerr != nil {
			reason = "original journal " + reversalOf + " not found"
		} else if origStatus != statusPosted {
			reason = "original journal " + reversalOf + " is " + origStatus + ", expected POSTED"
		}
	}

	if reason != "" {
		if _, uerr := tx.Exec(ctx, `UPDATE `+journalTable+` SET status = $2, failure_reason = $3 WHERE entry_id = $1`,
			entryID, statusFailed, reason); uerr != nil {
			return fail(constants.ErrUpdateFailed + uerr.Error())
		}
		if aerr := insertJournalAudit(ctx, tx, entryID, actionType, "FAILED", reason, actorEmail, true); aerr != nil {
			return fail(constants.ErrAuditInsertFailed + aerr.Error())
		}
		if cerr := tx.Commit(ctx); cerr != nil {
			return fail(constants.ErrCommitFailedCapitalized + cerr.Error())
		}
		api.LogInfo("[FDAccounting] %s %s FAILED: %s", actionType, entryID, reason)
		return map[string]interface{}{"entry_id": entryID, "success": false, "status": statusFailed, "error": reason}
	}

	if _, uerr := tx.Exec(ctx, `
		UPDATE `+journalTable+`
		SET status = $2, posted_by = $3, posted_at = now(), posting_reference = entry_id, failure_reason = NULL
		WHERE entry_id = $1`, entryID, statusPosted, api.SystemIfBlank(actorEmail)); uerr != nil {
		return fail(constants.ErrUpdateFailed + uerr.Error())
	}
	_, _ = tx.Exec(ctx, `
		UPDATE investment.accounting_activity SET status = 'POSTED'
		WHERE activity_id = (SELECT activity_id FROM `+journalTable+` WHERE entry_id = $1)`, entryID)
	if isReversal && reversalOf != "" {
		if _, uerr := tx.Exec(ctx, `UPDATE `+journalTable+` SET status = $2 WHERE entry_id = $1`, reversalOf, statusReversed); uerr != nil {
			return fail("flip original to REVERSED: " + uerr.Error())
		}
		_ = insertJournalAudit(ctx, tx, reversalOf, "EDIT", "COMPLETED", "Reversed by "+entryID, actorEmail, true)
	}
	if aerr := insertJournalAudit(ctx, tx, entryID, actionType, "COMPLETED", "Posted to ledger ("+postingModeValue+")", actorEmail, true); aerr != nil {
		return fail(constants.ErrAuditInsertFailed + aerr.Error())
	}
	if cerr := tx.Commit(ctx); cerr != nil {
		return fail(constants.ErrCommitFailedCapitalized + cerr.Error())
	}
	api.LogInfo("[FDAccounting] %s %s POSTED by %s", actionType, entryID, actorEmail)
	return map[string]interface{}{
		"entry_id": entryID, "success": true, "status": statusPosted,
		"posting_reference": entryID, "posting_mode": postingModeValue,
	}
}
