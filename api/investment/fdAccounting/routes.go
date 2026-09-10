package fdAccounting

import (
	"net/http"

	middlewares "CimplrCorpSaas/api/middlewares"

	"github.com/jackc/pgx/v5/pgxpool"
)

// RegisterFDAccountingRoutes wires every /investment/fd/accounting/* and
// /investment/fd/gl-mapping/* route. Bootstrap builds the same chain as
// fdMonthEndClosing (Session → GlobalIndependent → GlobalDependent →
// InvestmentFD); all handlers are POST with JSON bodies.
func RegisterFDAccountingRoutes(mux *http.ServeMux, pool *pgxpool.Pool) {
	mid := func(h http.Handler) http.Handler {
		return middlewares.SessionMiddleware(pool)(
			middlewares.GlobalIndependentMiddleware(pool)(
				middlewares.GlobalDependentMiddleware(pool)(
					middlewares.InvestmentFDMiddleware(pool)(h),
				),
			),
		)
	}

	// Journal workbench (AP-01 … AP-07)
	mux.Handle("/investment/fd/accounting/journal/list", mid(http.HandlerFunc(ListJournals(pool))))
	mux.Handle("/investment/fd/accounting/journal/kpis", mid(http.HandlerFunc(JournalKpis(pool))))
	mux.Handle("/investment/fd/accounting/journal/detail", mid(http.HandlerFunc(DetailJournal(pool))))
	mux.Handle("/investment/fd/accounting/journal/audit", mid(http.HandlerFunc(JournalAudit(pool))))
	mux.Handle("/investment/fd/accounting/journal/reverse", mid(http.HandlerFunc(ReverseJournal(pool))))
	mux.Handle("/investment/fd/accounting/journal/approve", mid(http.HandlerFunc(ApproveJournal(pool))))
	mux.Handle("/investment/fd/accounting/journal/bulk-approve", mid(http.HandlerFunc(ApproveJournal(pool))))
	mux.Handle("/investment/fd/accounting/journal/reject", mid(http.HandlerFunc(RejectJournal(pool))))
	mux.Handle("/investment/fd/accounting/journal/bulk-reject", mid(http.HandlerFunc(RejectJournal(pool))))
	mux.Handle("/investment/fd/accounting/journal/post", mid(http.HandlerFunc(PostJournal(pool))))
	mux.Handle("/investment/fd/accounting/journal/retry", mid(http.HandlerFunc(RetryJournal(pool))))

	// GL mapping & rules (AP-08)
	mux.Handle("/investment/fd/gl-mapping/list", mid(http.HandlerFunc(ListGlMappings(pool))))
	mux.Handle("/investment/fd/gl-mapping/detail", mid(http.HandlerFunc(DetailGlMapping(pool))))
	mux.Handle("/investment/fd/gl-mapping/create", mid(http.HandlerFunc(CreateGlMapping(pool))))
	mux.Handle("/investment/fd/gl-mapping/approve", mid(http.HandlerFunc(ApproveGlMapping(pool))))
	mux.Handle("/investment/fd/gl-mapping/reject", mid(http.HandlerFunc(RejectGlMapping(pool))))
	mux.Handle("/investment/fd/gl-mapping/activate", mid(http.HandlerFunc(ActivateGlMapping(pool))))
	mux.Handle("/investment/fd/gl-mapping/retire", mid(http.HandlerFunc(RetireGlMapping(pool))))
}
