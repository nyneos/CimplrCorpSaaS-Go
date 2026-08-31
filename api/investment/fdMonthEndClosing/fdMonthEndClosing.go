// Package fdMonthEndClosing implements the FD Month/Quarter End Closing
// module. Structure follows api/email/ (CLAUDE.md's reference module shape):
// this file is bootstrap ONLY (middleware chain + delegation to each
// feature's own RegisterXRoutes) — no inline routes, no handler logic.
// service.go carries the Start/Stop lifecycle; common/ carries cross-cutting
// helpers; cycle/ carries the first feature slice (Sections 0+1 of
// database/2026-08-27/HANDLER_SPEC_fd_month_quarter_end_closing.md). Sibling
// agents adding scope/, checklist/, lock/, reopen/, evidencePack/ should
// register each one's routes here the same way cycle's are registered below.
package fdMonthEndClosing

import (
	"net/http"

	"CimplrCorpSaas/api/investment/fdMonthEndClosing/checklist"
	"CimplrCorpSaas/api/investment/fdMonthEndClosing/cycle"
	"CimplrCorpSaas/api/investment/fdMonthEndClosing/evidencePack"
	"CimplrCorpSaas/api/investment/fdMonthEndClosing/lock"
	"CimplrCorpSaas/api/investment/fdMonthEndClosing/reopen"
	"CimplrCorpSaas/api/investment/fdMonthEndClosing/scope"
	middlewares "CimplrCorpSaas/api/middlewares"
	"CimplrCorpSaas/internal/observability"

	"github.com/jackc/pgx/v5/pgxpool"
)

const serviceName = "fd-month-end-closing"

// NewFdMonthEndClosingServer builds a standalone HTTP server for this module,
// mirroring api/email's NewEmailServer shape exactly (own mux, own
// observability wrapper/metrics). It exists so this module can be promoted
// to its own process/port later (like email :8183 / policy :8185) without a
// restructure.
//
// IMPORTANT — how these routes are actually reached today: the gateway proxies
// the whole "/investment/" prefix to a single port (api/gateway.go →
// http://localhost:7143), and every existing FD sub-feature (fdBookingWorkbench,
// fdMaster, fdInterestAndTdsWorkbench, ...) is mounted onto that ONE shared
// investment mux via its own RegisterXRoutes call from
// api/investment/routes.go — none of them run their own server/port. Adding a
// second port for this module would make /investment/fd-closing/* unreachable
// through the existing gateway route without ALSO editing api/gateway.go's
// proxy table, which is out of scope for this change. So in addition to this
// constructor, RegisterFDMonthEndClosingRoutes below is called directly from
// api/investment/routes.go (the one top-level wiring file this change touches)
// to mount the same routes onto the shared investment mux/port, exactly like
// fdBooking.RegisterFDBookingRoutes(mux, pool) does today.
func NewFdMonthEndClosingServer(pool *pgxpool.Pool, port string) *http.Server {
	mux := http.NewServeMux()
	RegisterFDMonthEndClosingRoutes(mux, pool)
	mux.Handle("/investment/fd-closing/metrics", observability.MetricsHandler(serviceName))

	return &http.Server{
		Addr:    ":" + port,
		Handler: observability.WrapHTTP(serviceName, mux),
	}
}

// RegisterFDMonthEndClosingRoutes wires every /investment/fd-closing/* route
// onto mux. Bootstrap only: builds the middleware chain once (Session →
// GlobalIndependent → GlobalDependent → InvestmentFD — identical chain to
// fdBookingWorkbench/routes.go) and delegates to each feature's own
// RegisterXRoutes. No mux.Handle calls live in this file — those belong in
// cycle/routes.go (and future scope/routes.go, checklist/routes.go, ...).
func RegisterFDMonthEndClosingRoutes(mux *http.ServeMux, pool *pgxpool.Pool) {
	mid := func(h http.Handler) http.Handler {
		return middlewares.SessionMiddleware(pool)(
			middlewares.GlobalIndependentMiddleware(pool)(
				middlewares.GlobalDependentMiddleware(pool)(
					middlewares.InvestmentFDMiddleware(pool)(h),
				),
			),
		)
	}

	cycle.RegisterCycleRoutes(mux, pool, mid)
	scope.RegisterScopeRoutes(mux, pool, mid)
	checklist.RegisterChecklistRoutes(mux, pool, mid)
	lock.RegisterLockRoutes(mux, pool, mid)
	reopen.RegisterReopenRoutes(mux, pool, mid)
	evidencePack.RegisterEvidencePackRoutes(mux, pool, mid)
}
