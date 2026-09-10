package fdAccounting

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/approvalengine"
	"CimplrCorpSaas/api/constants"
	fdclosingcommon "CimplrCorpSaas/api/investment/fdMonthEndClosing/common"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ReverseJournal handles POST /investment/fd/accounting/journal/reverse (AP-07).
// Creates a mirrored entry (Dr/Cr swapped) in PENDING_APPROVAL, linked to the
// original via reversal_of_entry_id. The original is untouched until the
// reversal is approved AND posted (post.go flips it to REVERSED).
func ReverseJournal(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			EntryID      string `json:"entry_id"`
			ReversalType string `json:"reversal_type"` // FULL only (partial reversals are out of scope without ERP lines to reconcile)
			ReversalDate string `json:"reversal_date"` // YYYY-MM-DD, defaults to today
			ReasonCode   string `json:"reason_code"`
			Remarks      string `json:"remarks"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		req.EntryID = strings.TrimSpace(req.EntryID)
		if req.EntryID == "" {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, "entry_id is required")
			return
		}
		if strings.TrimSpace(req.ReasonCode) == "" {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, "reason_code is required")
			return
		}
		if strings.TrimSpace(req.Remarks) == "" {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, "remarks are required")
			return
		}
		if req.ReversalType != "" && !strings.EqualFold(req.ReversalType, "FULL") {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, "only FULL reversals are supported")
			return
		}
		actor, ok := fdclosingcommon.ActorFromRequest(r)
		if !ok {
			fdclosingcommon.RespondError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		reversalDate := time.Now()
		if s := strings.TrimSpace(req.ReversalDate); s != "" {
			d, err := time.Parse("2006-01-02", s)
			if err != nil {
				fdclosingcommon.RespondError(w, http.StatusBadRequest, "reversal_date must be YYYY-MM-DD")
				return
			}
			reversalDate = d
		}

		ctx := r.Context()
		tx, err := pool.Begin(ctx)
		if err != nil {
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrTxBeginFailedCapitalized+err.Error())
			return
		}
		defer tx.Rollback(ctx) //nolint:errcheck

		// Lock + validate the original.
		var (
			activityID, entityID, entityName, fdID, receiptID, accrualRunID, accrualLedgerID, closureReqID string
			entryType, status, description, accountingPeriod                                               string
			totalDebit, totalCredit                                                                        float64
			isReversal                                                                                     bool
		)
		err = tx.QueryRow(ctx, `
			SELECT COALESCE(activity_id::text,''), COALESCE(entity_id,''), COALESCE(entity_name,''),
			       COALESCE(fd_id,''), COALESCE(receipt_id,''), COALESCE(accrual_run_id,''),
			       COALESCE(accrual_ledger_id::text,''), COALESCE(closure_request_id::text,''),
			       COALESCE(entry_type,''), COALESCE(status,''), COALESCE(description,''), COALESCE(accounting_period,''),
			       COALESCE(total_debit,0), COALESCE(total_credit,0), COALESCE(is_reversal,false)
			FROM `+journalTable+` WHERE entry_id = $1 AND COALESCE(is_deleted,false) = false FOR UPDATE`,
			req.EntryID).Scan(&activityID, &entityID, &entityName, &fdID, &receiptID, &accrualRunID, &accrualLedgerID,
			&closureReqID, &entryType, &status, &description, &accountingPeriod, &totalDebit, &totalCredit, &isReversal)
		if err == pgx.ErrNoRows {
			fdclosingcommon.RespondError(w, http.StatusNotFound, "journal entry not found")
			return
		}
		if err != nil {
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		if status != statusPosted {
			fdclosingcommon.RespondError(w, http.StatusConflict, "only POSTED journals can be reversed (current status "+status+")")
			return
		}
		if isReversal {
			fdclosingcommon.RespondError(w, http.StatusConflict, "a reversal entry cannot itself be reversed")
			return
		}
		var existing string
		_ = tx.QueryRow(ctx, `
			SELECT entry_id FROM `+journalTable+`
			WHERE reversal_of_entry_id = $1 AND COALESCE(is_deleted,false) = false
			  AND COALESCE(status,'') <> 'REJECTED' LIMIT 1`, req.EntryID).Scan(&existing)
		if existing != "" {
			fdclosingcommon.RespondError(w, http.StatusConflict, "a reversal already exists for this journal: "+existing)
			return
		}
		if locked, why, lerr := periodLocked(ctx, tx, entityID, entityName, reversalDate); lerr != nil {
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrQueryFailed+lerr.Error())
			return
		} else if locked {
			fdclosingcommon.RespondError(w, http.StatusConflict, "cannot reverse into a locked period: "+why)
			return
		}

		lines, err := loadLines(ctx, tx, req.EntryID)
		if err != nil {
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		if len(lines) == 0 {
			fdclosingcommon.RespondError(w, http.StatusConflict, "original journal has no lines to reverse")
			return
		}

		// Parent activity row (FK) in the same shape producers use.
		var newActivityID string
		if err := tx.QueryRow(ctx, `
			INSERT INTO investment.accounting_activity (activity_type, activity_subtype, effective_date, accounting_period, data_source, status)
			VALUES ('FIXED_DEPOSIT','JOURNAL_REVERSAL',$1,$2,'FD_ACCOUNTING_WORKBENCH','PENDING_APPROVAL')
			RETURNING activity_id`, reversalDate, reversalDate.Format(constants.DateFormatYearMonth)).Scan(&newActivityID); err != nil {
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrActivityInsertFailed+err.Error())
			return
		}

		newDesc := fmt.Sprintf("REVERSAL of %s (%s) — %s: %s", req.EntryID, entryType, strings.TrimSpace(req.ReasonCode), strings.TrimSpace(req.Remarks))
		var newEntryID string
		// accrual_ledger_id / closure_request_id are typed differently across
		// producers; the reversal keeps the text refs (fd/receipt/run) and the
		// original is always reachable through reversal_of_entry_id.
		_ = accrualLedgerID
		_ = closureReqID
		if err := tx.QueryRow(ctx, `
			INSERT INTO `+journalTable+` (
				activity_id, entity_id, entity_name, fd_id, receipt_id, accrual_run_id,
				entry_date, accounting_period, entry_type, description, total_debit, total_credit, status,
				is_reversal, reversal_of_entry_id, reason_code, remarks, requested_by, created_by
			) VALUES (
				$1, NULLIF($2,''), NULLIF($3,''), NULLIF($4,''), NULLIF($5,''), NULLIF($6,''),
				$7, $8, $9, $10, $11, $12, $13,
				true, $14, $15, $16, $17, $17
			) RETURNING entry_id`,
			newActivityID, entityID, entityName, fdID, receiptID, accrualRunID,
			reversalDate, reversalDate.Format(constants.DateFormatYearMonth), entryTypeReversal, newDesc, totalCredit, totalDebit, statusPendingApproval,
			req.EntryID, strings.TrimSpace(req.ReasonCode), strings.TrimSpace(req.Remarks), api.SystemIfBlank(actor.Email),
		).Scan(&newEntryID); err != nil {
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, "insert reversal entry: "+err.Error())
			return
		}

		for _, l := range lines {
			if _, err := tx.Exec(ctx, `
				INSERT INTO `+journalLineTable+` (entry_id, line_number, account_number, account_name, account_type, debit_amount, credit_amount, narration)
				VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
				newEntryID, l.LineNumber, l.AccountNumber, l.AccountName, l.AccountType,
				l.Credit, l.Debit, // swapped
				fmt.Sprintf("Reversal of %s | %s", req.EntryID, l.Narration)); err != nil {
				fdclosingcommon.RespondError(w, http.StatusInternalServerError, "insert reversal line: "+err.Error())
				return
			}
		}

		if err := insertJournalAudit(ctx, tx, newEntryID, "CREATE", statusPendingApproval,
			"Reversal of "+req.EntryID+" ("+strings.TrimSpace(req.ReasonCode)+")", actor.Email, false); err != nil {
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrAuditInsertFailed+err.Error())
			return
		}
		if err := tx.Commit(ctx); err != nil {
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrCommitFailedCapitalized+err.Error())
			return
		}

		fdclosingcommon.RespondSuccess(w, "Reversal submitted for approval", map[string]interface{}{
			"entry_id":             newEntryID,
			"reversal_of_entry_id": req.EntryID,
			"status":               statusPendingApproval,
		})
		api.LogInfo("[FDAccounting] Reversal %s of %s requested by %s", newEntryID, req.EntryID, actor.Email)

		// Approval-engine instance, same fire-and-forget shape as lock/request.go.
		newID, entity, email, uid := newEntryID, entityID, actor.Email, actor.UserID
		runEngineInBackground(func(bgCtx context.Context) {
			instID, err := approvalengine.CreateInstance(bgCtx, pool, approvalengine.InstanceRequest{
				ModuleCode: moduleCode, EntityCode: entity, TransactionType: txJournalReversal,
				RecordID: newID, RecordTable: journalTable, AuditTable: journalAuditTable, AuditIDColumn: "entry_id",
				ActionType: "CREATE", Amount: totalDebit, SubmittedBy: uid, SubmittedByEmail: email,
				RequirePinnedMatrix: true, AutoApplyIfUnpinned: false,
			})
			if err != nil {
				api.LogError("[FDAccounting] CreateInstance failed for reversal %s: %v", newID, err)
				return
			}
			if instID != "" {
				api.LogInfo("[FDAccounting] CreateInstance %s → reversal %s PENDING_APPROVAL", instID, newID)
			}
		})
	}
}
