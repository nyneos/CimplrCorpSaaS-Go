package fdAccrual

import (
	"CimplrCorpSaas/api"
	"database/sql"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
)

// RegisterFDAccrualRoutes registers all FD Accrual Engine routes on mux.
// Every handler is wrapped with BusinessUnitMiddleware so the entity/BU context
// is available on every request.
func RegisterFDAccrualRoutes(mux *http.ServeMux, pool *pgxpool.Pool, db *sql.DB) {
	mid := api.BusinessUnitMiddleware(db)

	// ── Accrual Run lifecycle ────────────────────────────────────────────────
	// Workflow order: create → validate → execute → submit → approve/reject
	mux.Handle(
		"/investment/fd/accrual/run/create",
		mid(http.HandlerFunc(CreateAccrualRun(pool))),
	)
	mux.Handle(
		"/investment/fd/accrual/run/validate",
		mid(http.HandlerFunc(ValidateScope(pool))),
	)
	mux.Handle(
		"/investment/fd/accrual/run/execute",
		mid(http.HandlerFunc(RunAccrual(pool))),
	)
	mux.Handle(
		"/investment/fd/accrual/run/submit",
		mid(http.HandlerFunc(SubmitForApproval(pool))),
	)
	mux.Handle(
		"/investment/fd/accrual/run/approve",
		mid(http.HandlerFunc(BulkApproveAccrualRun(pool))),
	)
	mux.Handle(
		"/investment/fd/accrual/run/reject",
		mid(http.HandlerFunc(BulkRejectAccrualRun(pool))),
	)
	mux.Handle(
		"/investment/fd/accrual/run/recompute",
		mid(http.HandlerFunc(RecomputeAccrualRun(pool))),
	)
	mux.Handle(
		"/investment/fd/accrual/run/bulk-generate",
		mid(http.HandlerFunc(BulkGenerateMonthlyAccruals(pool))),
	)
	// TC-21 Period Lock Check — used by the manual run setup form to disable
	// the "Run" button when a FINAL run for the chosen financial period has
	// already been approved/posted.
	mux.Handle(
		"/investment/fd/accrual/period/lock-status",
		mid(http.HandlerFunc(CheckPeriodLockStatus(pool))),
	)

	// ── Accrual data / detail ────────────────────────────────────────────────
	mux.Handle(
		"/investment/fd/accrual/run/all",
		mid(http.HandlerFunc(GetAccrualRuns(pool))),
	)
	mux.Handle(
		"/investment/fd/accrual/ledger",
		mid(http.HandlerFunc(GetAccrualLedger(pool))),
	)
	mux.Handle(
		"/investment/fd/accrual/detail",
		mid(http.HandlerFunc(GetAccrualCalculationDetail(pool))),
	)
	mux.Handle(
		"/investment/fd/accrual/findings",
		mid(http.HandlerFunc(GetValidationFindings(pool))),
	)
	mux.Handle(
		"/investment/fd/accrual/execution-log",
		mid(http.HandlerFunc(GetExecutionLog(pool))),
	)
	mux.Handle(
		"/investment/fd/accrual/run/audit",
		mid(http.HandlerFunc(GetAccrualRunAuditHandler(pool))),
	)
	mux.Handle(
		"/investment/fd/accrual/ledger/audit",
		mid(http.HandlerFunc(GetAccrualLedgerAuditHandler(pool))),
	)
	mux.Handle(
		"/investment/fd/accrual/exceptions",
		mid(http.HandlerFunc(GetAccrualExceptions(pool))),
	)

	// ── Overrides ────────────────────────────────────────────────────────────
	mux.Handle(
		"/investment/fd/accrual/override/propose",
		mid(http.HandlerFunc(ProposeOverride(pool))),
	)
	mux.Handle(
		"/investment/fd/accrual/override/approve",
		mid(http.HandlerFunc(ApproveOverride(pool))),
	)
	mux.Handle(
		"/investment/fd/accrual/override/reject",
		mid(http.HandlerFunc(RejectOverride(pool))),
	)

	// ── Schedule management ──────────────────────────────────────────────────
	mux.Handle(
		"/investment/fd/accrual/schedule/create",
		mid(http.HandlerFunc(CreateScheduleConfig(pool))),
	)
	mux.Handle(
		"/investment/fd/accrual/schedule/update",
		mid(http.HandlerFunc(UpdateScheduleConfig(pool))),
	)
	mux.Handle(
		"/investment/fd/accrual/schedule/all",
		mid(http.HandlerFunc(GetScheduleConfigsWithAudit(pool))),
	)
	mux.Handle(
		"/investment/fd/accrual/schedule/detail",
		mid(http.HandlerFunc(GetScheduleConfigDetail(pool))),
	)
	mux.Handle(
		"/investment/fd/accrual/schedule/disable",
		mid(http.HandlerFunc(DisableSchedule(pool))),
	)
	mux.Handle(
		"/investment/fd/accrual/schedule/enable",
		mid(http.HandlerFunc(EnableSchedule(pool))),
	)
	mux.Handle(
		"/investment/fd/accrual/schedule/approve",
		mid(http.HandlerFunc(ApproveAccrualSchedule(pool))),
	)
	mux.Handle(
		"/investment/fd/accrual/schedule/reject",
		mid(http.HandlerFunc(RejectAccrualSchedule(pool))),
	)
	mux.Handle(
		"/investment/fd/accrual/schedule/delete",
		mid(http.HandlerFunc(DeleteScheduleConfig(pool))),
	)
}