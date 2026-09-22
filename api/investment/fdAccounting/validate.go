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

// ValidateJournal handles POST /investment/fd/accounting/journal/validate.
// Read-only dry run of the posting gate: same balance, GL account and period
// checks postOne applies, without touching status or writing an audit row.
func ValidateJournal(pool *pgxpool.Pool) http.HandlerFunc {
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
		ctx := r.Context()
		passed, failed := 0, 0
		results := make([]map[string]interface{}, 0, len(ids))
		for _, id := range ids {
			out := validateOne(ctx, pool, id)
			results = append(results, out)
			if out["valid"] == true {
				passed++
			} else {
				failed++
			}
		}
		fdclosingcommon.RespondSuccess(w, fmt.Sprintf("%d of %d journal entr%s passed validation", passed, len(ids), plural(len(ids))),
			map[string]interface{}{"passed": passed, "failed": failed, "results": results})
	}
}

func validateOne(ctx context.Context, pool *pgxpool.Pool, entryID string) map[string]interface{} {
	fail := func(msg string) map[string]interface{} {
		return map[string]interface{}{"entry_id": entryID, "valid": false, "checks": []map[string]interface{}{}, "error": msg}
	}

	var (
		status, entityID, entityName string
		entryDate                    time.Time
		totalDebit, totalCredit      float64
	)
	err := pool.QueryRow(ctx, `
		SELECT COALESCE(status,''), COALESCE(entity_id,''), COALESCE(entity_name,''),
		       entry_date, COALESCE(total_debit,0), COALESCE(total_credit,0)
		FROM `+journalTable+` WHERE entry_id = $1 AND COALESCE(is_deleted,false) = false`, entryID).
		Scan(&status, &entityID, &entityName, &entryDate, &totalDebit, &totalCredit)
	if err == pgx.ErrNoRows {
		return fail("journal entry not found")
	}
	if err != nil {
		return fail(constants.ErrQueryFailed + err.Error())
	}

	checks := []map[string]interface{}{}
	addCheck := func(name string, reason string) bool {
		checks = append(checks, map[string]interface{}{
			"check": name, "passed": reason == "", "reason": reason,
		})
		return reason == ""
	}

	lines, lerr := loadLines(ctx, pool, entryID)
	if lerr != nil {
		return fail(constants.ErrQueryFailed + lerr.Error())
	}
	valid := addCheck("Journal lines", validateForPosting(lines, totalDebit, totalCredit))

	periodReason := ""
	if locked, why, perr := periodLocked(ctx, pool, entityID, entityName, entryDate); perr != nil {
		return fail(constants.ErrQueryFailed + perr.Error())
	} else if locked {
		periodReason = why
	}
	valid = addCheck("Accounting period", periodReason) && valid

	approvalReason := ""
	if latest, aerr := latestAuditStatus(ctx, pool, entryID); aerr != nil {
		return fail(constants.ErrQueryFailed + aerr.Error())
	} else if latest != constants.StatusApproved && latest != "COMPLETED" && latest != "FAILED" {
		approvalReason = "latest checker action is " + latest
	}
	valid = addCheck("Maker-checker", approvalReason) && valid

	api.LogInfo("[FDAccounting] VALIDATE %s valid=%t", entryID, valid)
	return map[string]interface{}{
		"entry_id": entryID, "valid": valid, "status": status, "checks": checks,
	}
}
