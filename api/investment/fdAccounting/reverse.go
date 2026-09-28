package fdAccounting

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/approvalengine"
	"CimplrCorpSaas/api/constants"
	fdclosingcommon "CimplrCorpSaas/api/investment/fdMonthEndClosing/common"
	"CimplrCorpSaas/api/utils/s3storage"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type reverseLineIn struct {
	LineNumber   int     `json:"line_number"`
	DebitAmount  float64 `json:"debit_amount"`
	CreditAmount float64 `json:"credit_amount"`
}

// ReverseJournal handles POST /investment/fd/accounting/journal/reverse (AP-07).
// Creates a NEW mirrored entry (Dr/Cr swapped) in PENDING_APPROVAL, linked to the
// original via reversal_of_entry_id. That new entry is the cancellation journal —
// reversal never rewrites the original lines.
// FULL uses every original amount; PARTIAL accepts maker-edited amounts
// (≤ mirrored original, Dr total = Cr total).
// On post: FULL flips original → REVERSED; PARTIAL leaves original POSTED
// (remaining exposure stays). A corrected replacement journal is a separate
// producer write (new accrual/receipt/etc.), not part of reverse.
func ReverseJournal(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			EntryID      string          `json:"entry_id"`
			ReversalType string          `json:"reversal_type"` // FULL (default) | PARTIAL
			ReversalDate string          `json:"reversal_date"` // YYYY-MM-DD, defaults to today
			ReasonCode   string          `json:"reason_code"`
			Remarks      string          `json:"remarks"`
			Lines        []reverseLineIn `json:"lines"` // required when PARTIAL — swapped amounts
		}
		isMultipart := strings.Contains(strings.ToLower(r.Header.Get(constants.ContentTypeText)), "multipart/form-data")
		if isMultipart {
			if err := r.ParseMultipartForm(32 << 20); err != nil {
				fdclosingcommon.RespondError(w, http.StatusBadRequest, "Invalid multipart form: "+err.Error())
				return
			}
			req.EntryID = r.FormValue("entry_id")
			req.ReversalType = r.FormValue("reversal_type")
			req.ReversalDate = r.FormValue("reversal_date")
			req.ReasonCode = r.FormValue("reason_code")
			req.Remarks = r.FormValue("remarks")
			if s := strings.TrimSpace(r.FormValue("lines")); s != "" {
				_ = json.Unmarshal([]byte(s), &req.Lines)
			}
		} else {
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				fdclosingcommon.RespondError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
				return
			}
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
		reversalType := strings.ToUpper(strings.TrimSpace(req.ReversalType))
		if reversalType == "" {
			reversalType = "FULL"
		}
		if reversalType != "FULL" && reversalType != "PARTIAL" {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, "reversal_type must be FULL or PARTIAL")
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
		evidenceS3Key := ""
		if isMultipart && r.MultipartForm != nil {
			if headers := s3storage.CollectMultipartFiles(r.MultipartForm, "evidence", "file", "files"); len(headers) > 0 && headers[0] != nil {
				f, ferr := headers[0].Open()
				if ferr != nil {
					fdclosingcommon.RespondError(w, http.StatusBadRequest, "open evidence file: "+ferr.Error())
					return
				}
				body, rerr := io.ReadAll(f)
				f.Close()
				if rerr != nil {
					fdclosingcommon.RespondError(w, http.StatusBadRequest, "read evidence file: "+rerr.Error())
					return
				}
				key := s3storage.BuildUploadedS3Key("fd/fd-accounting-journal/reversal-evidence", req.EntryID, headers[0].Filename, actor.Email, time.Now().UTC())
				if uerr := s3storage.PutObjectToS3(ctx, key, body, s3storage.DetectContentType(body)); uerr != nil {
					fdclosingcommon.RespondError(w, http.StatusInternalServerError, "S3 upload failed: "+uerr.Error())
					return
				}
				evidenceS3Key = key
			}
		}
		committed := false
		defer func() {
			if !committed && evidenceS3Key != "" {
				_ = s3storage.DeleteFromS3(context.Background(), evidenceS3Key)
			}
		}()
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

		revLines, revDr, revCr, buildErr := buildReversalLines(reversalType, req.EntryID, lines, req.Lines)
		if buildErr != "" {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, buildErr)
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

		typeLabel := "REVERSAL"
		if reversalType == "PARTIAL" {
			typeLabel = "PARTIAL REVERSAL"
		}
		newDesc := fmt.Sprintf("%s of %s (%s) — %s: %s", typeLabel, req.EntryID, entryType, strings.TrimSpace(req.ReasonCode), strings.TrimSpace(req.Remarks))
		var newEntryID string
		// accrual_ledger_id / closure_request_id are typed differently across
		// producers; the reversal keeps the text refs (fd/receipt/run) and the
		// original is always reachable through reversal_of_entry_id.
		_ = accrualLedgerID
		_ = closureReqID
		// requested_by (text) and created_by (varchar) are different column types,
		// so they need their own placeholders — reusing one parameter across two
		// differently-typed columns makes Postgres unable to deduce a single type
		// for it (SQLSTATE 42P08).
		actorEmail := api.SystemIfBlank(actor.Email)
		if err := tx.QueryRow(ctx, `
			INSERT INTO `+journalTable+` (
				activity_id, entity_id, entity_name, fd_id, receipt_id, accrual_run_id,
				entry_date, accounting_period, entry_type, description, total_debit, total_credit, status,
				is_reversal, reversal_of_entry_id, reversal_type, reason_code, remarks, requested_by, created_by, evidence_s3_key
			) VALUES (
				$1, NULLIF($2,''), NULLIF($3,''), NULLIF($4,''), NULLIF($5,''), NULLIF($6,''),
				$7, $8, $9, $10, $11, $12, $13,
				true, $14, $15, $16, $17, $18, $19, NULLIF($20,'')
			) RETURNING entry_id`,
			newActivityID, entityID, entityName, fdID, receiptID, accrualRunID,
			reversalDate, reversalDate.Format(constants.DateFormatYearMonth), entryTypeReversal, newDesc, revDr, revCr, statusPendingApproval,
			req.EntryID, reversalType, strings.TrimSpace(req.ReasonCode), strings.TrimSpace(req.Remarks), actorEmail, actorEmail, evidenceS3Key,
		).Scan(&newEntryID); err != nil {
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, "insert reversal entry: "+err.Error())
			return
		}

		for _, l := range revLines {
			if _, err := tx.Exec(ctx, `
				INSERT INTO `+journalLineTable+` (
					entry_id, line_number, account_number, account_name, account_type,
					debit_amount, credit_amount, narration,
					cost_center, profit_center, project_code, tax_code
				) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,NULLIF($9,''),NULLIF($10,''),NULLIF($11,''),NULLIF($12,''))`,
				newEntryID, l.LineNumber, l.AccountNumber, l.AccountName, l.AccountType,
				l.Debit, l.Credit,
				l.Narration, l.CostCenter, l.ProfitCenter, l.ProjectCode, l.TaxCode); err != nil {
				fdclosingcommon.RespondError(w, http.StatusInternalServerError, "insert reversal line: "+err.Error())
				return
			}
		}

		if err := insertJournalAudit(ctx, tx, newEntryID, "CREATE", statusPendingApproval,
			typeLabel+" of "+req.EntryID+" ("+strings.TrimSpace(req.ReasonCode)+"): "+strings.TrimSpace(req.Remarks), actor.Email, false); err != nil {
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrAuditInsertFailed+err.Error())
			return
		}
		if err := tx.Commit(ctx); err != nil {
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrCommitFailedCapitalized+err.Error())
			return
		}
		committed = true

		fdclosingcommon.RespondSuccess(w, "Reversal submitted for approval", map[string]interface{}{
			"entry_id":             newEntryID,
			"reversal_of_entry_id": req.EntryID,
			"reversal_type":        reversalType,
			"status":               statusPendingApproval,
			"total_debit":          revDr,
			"total_credit":         revCr,
		})
		api.LogInfo("[FDAccounting] %s %s of %s requested by %s", typeLabel, newEntryID, req.EntryID, actor.Email)

		// Approval-engine instance, same fire-and-forget shape as lock/request.go.
		newID, entity, email, uid := newEntryID, entityID, actor.Email, actor.UserID
		runEngineInBackground(func(bgCtx context.Context) {
			instID, err := approvalengine.CreateInstance(bgCtx, pool, approvalengine.InstanceRequest{
				ModuleCode: moduleCode, EntityCode: entity, TransactionType: txJournalReversal,
				RecordID: newID, RecordTable: journalTable, AuditTable: journalAuditTable, AuditIDColumn: "entry_id",
				ActionType: "CREATE", Amount: revDr, SubmittedBy: uid, SubmittedByEmail: email,
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

// buildReversalLines returns the lines to write for a FULL or PARTIAL reversal.
// Mirrored sides: reversal debit ≤ original credit; reversal credit ≤ original debit.
func buildReversalLines(reversalType, originalEntryID string, original []journalLineRec, partial []reverseLineIn) ([]journalLineRec, float64, float64, string) {
	byNum := make(map[int]journalLineRec, len(original))
	for _, l := range original {
		byNum[l.LineNumber] = l
	}

	out := make([]journalLineRec, 0, len(original))
	var revDr, revCr float64

	if reversalType == "FULL" {
		for _, l := range original {
			out = append(out, journalLineRec{
				LineNumber:    l.LineNumber,
				AccountNumber: l.AccountNumber,
				AccountName:   l.AccountName,
				AccountType:   l.AccountType,
				Debit:         l.Credit,
				Credit:        l.Debit,
				Narration:     fmt.Sprintf("Reversal of %s | %s", originalEntryID, l.Narration),
				CostCenter:    l.CostCenter,
				ProfitCenter:  l.ProfitCenter,
				ProjectCode:   l.ProjectCode,
				TaxCode:       l.TaxCode,
			})
			revDr += l.Credit
			revCr += l.Debit
		}
		return out, revDr, revCr, ""
	}

	if len(partial) == 0 {
		return nil, 0, 0, "lines are required for PARTIAL reversal"
	}
	seen := make(map[int]bool, len(partial))
	for _, in := range partial {
		orig, ok := byNum[in.LineNumber]
		if !ok {
			return nil, 0, 0, fmt.Sprintf("line_number %d is not on the original journal", in.LineNumber)
		}
		if seen[in.LineNumber] {
			return nil, 0, 0, fmt.Sprintf("duplicate line_number %d in lines", in.LineNumber)
		}
		seen[in.LineNumber] = true

		dr, cr := in.DebitAmount, in.CreditAmount
		if dr < 0 || cr < 0 {
			return nil, 0, 0, fmt.Sprintf("line %d amounts must be ≥ 0", in.LineNumber)
		}
		if dr > 0 && cr > 0 {
			return nil, 0, 0, fmt.Sprintf("line %d cannot have both debit and credit", in.LineNumber)
		}
		// Mirrored caps: only the swapped side of the original may carry amount.
		maxDr, maxCr := orig.Credit, orig.Debit
		if dr-maxDr > 0.005 {
			return nil, 0, 0, fmt.Sprintf("line %d debit %.2f exceeds original credit %.2f", in.LineNumber, dr, maxDr)
		}
		if cr-maxCr > 0.005 {
			return nil, 0, 0, fmt.Sprintf("line %d credit %.2f exceeds original debit %.2f", in.LineNumber, cr, maxCr)
		}
		if maxDr <= 0.005 && dr > 0.005 {
			return nil, 0, 0, fmt.Sprintf("line %d had no original credit to reverse as debit", in.LineNumber)
		}
		if maxCr <= 0.005 && cr > 0.005 {
			return nil, 0, 0, fmt.Sprintf("line %d had no original debit to reverse as credit", in.LineNumber)
		}
		if dr <= 0.005 && cr <= 0.005 {
			continue // omit fully-zeroed lines from the partial journal
		}
		out = append(out, journalLineRec{
			LineNumber:    orig.LineNumber,
			AccountNumber: orig.AccountNumber,
			AccountName:   orig.AccountName,
			AccountType:   orig.AccountType,
			Debit:         dr,
			Credit:        cr,
			Narration:     fmt.Sprintf("Partial reversal of %s | %s", originalEntryID, orig.Narration),
			CostCenter:    orig.CostCenter,
			ProfitCenter:  orig.ProfitCenter,
			ProjectCode:   orig.ProjectCode,
			TaxCode:       orig.TaxCode,
		})
		revDr += dr
		revCr += cr
	}
	if len(out) < 2 {
		return nil, 0, 0, "partial reversal needs at least one debit and one credit line"
	}
	if math.Abs(revDr-revCr) > 0.005 {
		return nil, 0, 0, fmt.Sprintf("partial reversal is unbalanced: debit %.2f vs credit %.2f", revDr, revCr)
	}
	if revDr <= 0.005 {
		return nil, 0, 0, "partial reversal amounts must be greater than zero"
	}
	return out, revDr, revCr, ""
}
