// Package lock implements the fd_closing_lock_request handlers — Section 4 of
// database/2026-08-27/HANDLER_SPEC_fd_month_quarter_end_closing.md. This is a
// genuine stage-then-apply maker-checker flow where the request row IS its
// own audit trail (no separate fd_closing_lock_request_audit sibling table,
// per the migration's design comment) — Approve only flips processing_status
// (via the approval engine's generic finalizeRecord, since AuditTable =
// RecordTable = investment.fd_closing_lock_request), and the actual
// fd_closing_cycle.status transition happens in a distinct, later Apply call
// — mirroring api/investment/fdMaster/activation.go's "activation is a
// separate final act from approval" shape.
package lock

import (
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Middleware is the wrapper function passed down from fdMonthEndClosing.go's
// bootstrap (Session → GlobalIndependent → GlobalDependent → InvestmentFD).
// RegisterLockRoutes never builds middleware itself — that responsibility
// lives entirely in the bootstrap file, per CLAUDE.md's routes.go rule ("ONLY
// mux.Handle(...) calls, nothing else").
type Middleware func(http.Handler) http.Handler

// RegisterLockRoutes registers every /investment/fd-closing/lock/* route on
// mux. All POST — every parameter travels in the JSON body, per this repo's
// convention.
func RegisterLockRoutes(mux *http.ServeMux, pool *pgxpool.Pool, mid Middleware) {
	mux.Handle("/investment/fd-closing/lock/request",
		mid(http.HandlerFunc(RequestLock(pool))))
	mux.Handle("/investment/fd-closing/lock/approve",
		mid(http.HandlerFunc(ApproveLock(pool))))
	mux.Handle("/investment/fd-closing/lock/bulk-approve",
		mid(http.HandlerFunc(ApproveLock(pool))))
	mux.Handle("/investment/fd-closing/lock/reject",
		mid(http.HandlerFunc(RejectLock(pool))))
	mux.Handle("/investment/fd-closing/lock/bulk-reject",
		mid(http.HandlerFunc(RejectLock(pool))))
	mux.Handle("/investment/fd-closing/lock/apply",
		mid(http.HandlerFunc(ApplyLock(pool))))
	mux.Handle("/investment/fd-closing/lock/list",
		mid(http.HandlerFunc(ListLockRequests(pool))))
	mux.Handle("/investment/fd-closing/lock/detail",
		mid(http.HandlerFunc(DetailLockRequest(pool))))
}
