package cycle

import (
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Middleware is the wrapper function passed down from fdMonthEndClosing.go's
// bootstrap (Session → GlobalIndependent → GlobalDependent → InvestmentFD).
// RegisterCycleRoutes never builds middleware itself — that responsibility
// lives entirely in the bootstrap file, per CLAUDE.md's routes.go rule
// ("ONLY mux.Handle(...) calls, nothing else").
type Middleware func(http.Handler) http.Handler

// RegisterCycleRoutes registers every /investment/fd-closing/cycle/* route on
// mux. Route paths follow CLAUDE.md's standard action vocabulary: /create,
// /update (not /edit), /delete, /approve + /bulk-approve, /reject +
// /bulk-reject, /list, /list-approved-active, /detail, /audit. All POST —
// every parameter travels in the JSON body, per this repo's convention.
func RegisterCycleRoutes(mux *http.ServeMux, pool *pgxpool.Pool, mid Middleware) {
	mux.Handle("/investment/fd-closing/cycle/create",
		mid(http.HandlerFunc(CreateCycle(pool))))
	mux.Handle("/investment/fd-closing/cycle/update",
		mid(http.HandlerFunc(UpdateCycle(pool))))
	mux.Handle("/investment/fd-closing/cycle/delete",
		mid(http.HandlerFunc(DeleteCycle(pool))))
	mux.Handle("/investment/fd-closing/cycle/close",
		mid(http.HandlerFunc(CloseCycle(pool))))
	mux.Handle("/investment/fd-closing/cycle/approve",
		mid(http.HandlerFunc(ApproveCycle(pool))))
	mux.Handle("/investment/fd-closing/cycle/bulk-approve",
		mid(http.HandlerFunc(ApproveCycle(pool))))
	mux.Handle("/investment/fd-closing/cycle/reject",
		mid(http.HandlerFunc(RejectCycle(pool))))
	mux.Handle("/investment/fd-closing/cycle/bulk-reject",
		mid(http.HandlerFunc(RejectCycle(pool))))
	mux.Handle("/investment/fd-closing/cycle/list",
		mid(http.HandlerFunc(ListCycles(pool))))
	mux.Handle("/investment/fd-closing/cycle/list-approved-active",
		mid(http.HandlerFunc(ListApprovedActiveCycles(pool))))
	mux.Handle("/investment/fd-closing/cycle/detail",
		mid(http.HandlerFunc(DetailCycle(pool))))
	mux.Handle("/investment/fd-closing/cycle/audit",
		mid(http.HandlerFunc(AuditCycle(pool))))
}
