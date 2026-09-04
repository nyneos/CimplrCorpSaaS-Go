package scope

import (
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Middleware is the wrapper function passed down from fdMonthEndClosing.go's
// bootstrap (Session → GlobalIndependent → GlobalDependent → InvestmentFD).
// RegisterScopeRoutes never builds middleware itself — that responsibility
// lives entirely in the bootstrap file, per CLAUDE.md's routes.go rule
// ("ONLY mux.Handle(...) calls, nothing else"). Mirrors cycle/routes.go's
// Middleware type exactly (kept as its own type per package rather than
// importing cycle's, so scope has zero compile-time dependency on cycle).
type Middleware func(http.Handler) http.Handler

// RegisterScopeRoutes registers every /investment/fd-closing/scope/* route on
// mux — Section 2 of database/2026-08-27/HANDLER_SPEC_fd_month_quarter_end_closing.md
// (fd_closing_cycle_fd_scope). Route paths follow CLAUDE.md's standard action
// vocabulary: /create + /bulk-create, /delete + /bulk-delete, /approve +
// /bulk-approve, /reject + /bulk-reject, /list. All POST — every parameter
// travels in the JSON body, per this repo's convention. Not yet called from
// anywhere — the human wires this into fdMonthEndClosing.go's bootstrap
// alongside cycle.RegisterCycleRoutes.
func RegisterScopeRoutes(mux *http.ServeMux, pool *pgxpool.Pool, mid Middleware) {
	mux.Handle("/investment/fd-closing/scope/create",
		mid(http.HandlerFunc(CreateScope(pool))))
	mux.Handle("/investment/fd-closing/scope/bulk-create",
		mid(http.HandlerFunc(CreateScope(pool))))
	mux.Handle("/investment/fd-closing/scope/delete",
		mid(http.HandlerFunc(DeleteScope(pool))))
	mux.Handle("/investment/fd-closing/scope/bulk-delete",
		mid(http.HandlerFunc(DeleteScope(pool))))
	mux.Handle("/investment/fd-closing/scope/approve",
		mid(http.HandlerFunc(ApproveScope(pool))))
	mux.Handle("/investment/fd-closing/scope/bulk-approve",
		mid(http.HandlerFunc(ApproveScope(pool))))
	mux.Handle("/investment/fd-closing/scope/reject",
		mid(http.HandlerFunc(RejectScope(pool))))
	mux.Handle("/investment/fd-closing/scope/bulk-reject",
		mid(http.HandlerFunc(RejectScope(pool))))
	mux.Handle("/investment/fd-closing/scope/list",
		mid(http.HandlerFunc(ListScope(pool))))
	mux.Handle("/investment/fd-closing/scope/list-eligible",
		mid(http.HandlerFunc(ListEligibleFDs(pool))))
}
