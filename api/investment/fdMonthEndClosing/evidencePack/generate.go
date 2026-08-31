// Package evidencePack implements the fd_closing_evidence_pack handlers —
// Section 6 of database/2026-08-27/HANDLER_SPEC_fd_month_quarter_end_closing.md.
// Unlike cycle/scope/checklist/lock/reopen, this record is an append-only
// generated-artifact log (CLAUDE.md rule #5), not a mutable master: no
// maker-checker, no processing_status, no *_audit sibling table — the
// migration's own design comment says so explicitly. Handlers still follow
// the same wire conventions as the rest of the module: pgxpool, ctx :=
// r.Context(), RespondEnvelope* via fdclosingcommon, api.LogErrorForResponse
// for raw server-side error logs, actor resolved via
// fdclosingcommon.ActorFromRequest.
package evidencePack

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	fdclosingcommon "CimplrCorpSaas/api/investment/fdMonthEndClosing/common"
	"CimplrCorpSaas/internal/ctxutil"
	dmsjobs "CimplrCorpSaas/internal/jobs/dms"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	evidencePackTable = "investment.fd_closing_evidence_pack"
	cycleTable        = "investment.fd_closing_cycle"
)

// dmsModuleCode/dmsSubModuleCode are the module/sub-module keys passed to
// dmsjobs.FireDmsEvent for evidence pack generation, following the exact
// convention every other FD workbench caller uses (e.g.
// fdMaster/activation.go: dmsjobs.FireDmsEvent(pool, "INVESTMENT_FD",
// "FD_MASTER", "POST_CREATE", []string{fdID}, userEmail)).
//
// IMPORTANT — config dependency: FireDmsEvent is a silent no-op unless an
// Active+APPROVED dms_svc.generation_rule row exists for module_code=
// "INVESTMENT_FD" + sub_module_code="FD_CLOSING_EVIDENCE_PACK" +
// trigger_type="POST_CREATE". That seed row does not exist yet — seeding it
// is a DB/config task out of scope for this handler (see report-back to the
// requester). Until it is seeded, generate.go will insert the pack row
// correctly but no document will actually be produced.
const (
	dmsModuleCode    = "INVESTMENT_FD"
	dmsSubModuleCode = "FD_CLOSING_EVIDENCE_PACK"
	dmsTriggerType   = "POST_CREATE"
)

// GenerateEvidencePack handles POST /investment/fd-closing/evidence/generate.
// Inserts the fd_closing_evidence_pack row directly (generated_by/at = actor/
// now()) — no approval gate, matching FD Booking's own document generation
// which is entirely unrelated to the approval engine. s3_key/file_size/
// checksum are left NULL on insert since dmsjobs.FireDmsEvent is
// fire-and-forget (it schedules a goroutine internally and never blocks the
// HTTP response) — list.go/download.go pick up the result once ready by
// joining dms_svc.generation_run_source_row/generated_document on
// source_id = pack_id.
func GenerateEvidencePack(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			CycleID                      string `json:"cycle_id"`
			Format                       string `json:"format"`
			IncludeAccrualLedger         *bool  `json:"include_accrual_ledger"`
			IncludeReconciliationReport  *bool  `json:"include_reconciliation_report"`
			IncludeExceptionsRegister    *bool  `json:"include_exceptions_register"`
			IncludePostingSummary        *bool  `json:"include_posting_summary"`
			IncludeApprovalLogs          *bool  `json:"include_approval_logs"`
			IncludePeriodLockCertificate *bool  `json:"include_period_lock_certificate"`
			IncludeAuditTrail            *bool  `json:"include_audit_trail"`
			IncludeSupportingDocuments   *bool  `json:"include_supporting_documents"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}

		req.CycleID = strings.TrimSpace(req.CycleID)
		req.Format = strings.ToUpper(strings.TrimSpace(req.Format))
		if req.CycleID == "" {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, "cycle_id is required")
			return
		}
		if req.Format != "PDF" && req.Format != "ZIP" {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, "format must be PDF or ZIP")
			return
		}

		actor, ok := fdclosingcommon.ActorFromRequest(r)
		if !ok {
			fdclosingcommon.RespondError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()

		var entityID string
		var cycleDeleted bool
		err := pool.QueryRow(ctx, `
			SELECT entity_id, is_deleted FROM `+cycleTable+`
			WHERE cycle_id = $1`,
			req.CycleID,
		).Scan(&entityID, &cycleDeleted)
		if err != nil {
			fdclosingcommon.RespondError(w, http.StatusNotFound, "Cycle not found")
			return
		}
		if cycleDeleted {
			fdclosingcommon.RespondError(w, http.StatusNotFound, "Cycle not found")
			return
		}

		scope := ctxutil.FromContext(ctx)
		if !scope.HasEntityAccess(entityID) {
			fdclosingcommon.RespondError(w, http.StatusForbidden,
				"Entity ID '"+entityID+"' is not within your authorized access scope.")
			return
		}

		include := func(v *bool) bool {
			if v == nil {
				return true
			}
			return *v
		}

		var packID string
		err = pool.QueryRow(ctx, `
			INSERT INTO `+evidencePackTable+` (
				cycle_id, format,
				include_accrual_ledger, include_reconciliation_report, include_exceptions_register,
				include_posting_summary, include_approval_logs, include_period_lock_certificate,
				include_audit_trail, include_supporting_documents,
				generated_by
			) VALUES (
				$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11
			) RETURNING pack_id`,
			req.CycleID, req.Format,
			include(req.IncludeAccrualLedger), include(req.IncludeReconciliationReport), include(req.IncludeExceptionsRegister),
			include(req.IncludePostingSummary), include(req.IncludeApprovalLogs), include(req.IncludePeriodLockCertificate),
			include(req.IncludeAuditTrail), include(req.IncludeSupportingDocuments),
			actor.Email,
		).Scan(&packID)
		if err != nil {
			api.LogErrorForResponse(w, "[FDClosingEvidencePack] GenerateEvidencePack insert: %v", err)
			msg, status := friendlyDBError(err)
			fdclosingcommon.RespondError(w, status, msg)
			return
		}

		// Fire-and-forget: FireDmsEvent schedules its own goroutine and never
		// blocks. It is a silent no-op until a dms_svc.generation_rule row is
		// seeded for module=INVESTMENT_FD/sub_module=FD_CLOSING_EVIDENCE_PACK/
		// trigger=POST_CREATE (see the const block above) — seeding that row is
		// out of scope here (DB config, not handler code).
		dmsjobs.FireDmsEvent(pool, dmsModuleCode, dmsSubModuleCode, dmsTriggerType, []string{packID}, actor.Email)

		fdclosingcommon.RespondSuccess(w, "Evidence pack generation started", map[string]interface{}{
			"pack_id":  packID,
			"cycle_id": req.CycleID,
			"format":   req.Format,
			"s3_key":   nil,
			"status":   "GENERATING",
		})
		api.LogInfo("[FDClosingEvidencePack] Pack generation requested: pack_id=%s cycle=%s by=%s", packID, req.CycleID, actor.Email)
	}
}

// friendlyDBError maps common Postgres error codes to a user-facing message
// and HTTP status, mirroring cycle/create.go's helper of the same name (kept
// as its own copy here since evidencePack is a separate package and routes.go
// must contain nothing but mux.Handle calls per CLAUDE.md).
func friendlyDBError(err error) (string, int) {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23503":
			return "Referenced cycle was not found or is not valid.", http.StatusBadRequest
		case "23514":
			return "Invalid value for one of the submitted fields.", http.StatusBadRequest
		case "23505":
			return "Duplicate record.", http.StatusConflict
		}
	}
	return "Database operation failed. Please contact support if this persists.", http.StatusInternalServerError
}
