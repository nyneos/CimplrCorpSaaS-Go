package fdRateNegotiation

import (
	"net/http"

	middlewares "CimplrCorpSaas/api/middlewares"

	"github.com/jackc/pgx/v5/pgxpool"
)

// RegisterFDRateNegotiationRoutes wires /investment/fd/rate-negotiation/* routes.
func RegisterFDRateNegotiationRoutes(mux *http.ServeMux, pool *pgxpool.Pool) {
	mid := func(h http.Handler) http.Handler {
		return middlewares.SessionMiddleware(pool)(
			middlewares.GlobalIndependentMiddleware(pool)(
				middlewares.GlobalDependentMiddleware(pool)(
					middlewares.InvestmentFDMiddleware(pool)(h),
				),
			),
		)
	}

	mux.Handle("/investment/fd/rate-negotiation/create",
		mid(http.HandlerFunc(CreateRateRequest(pool))))
	mux.Handle("/investment/fd/rate-negotiation/update",
		mid(http.HandlerFunc(UpdateRateRequest(pool))))
	mux.Handle("/investment/fd/rate-negotiation/all",
		mid(http.HandlerFunc(ListRateRequests(pool))))
	mux.Handle("/investment/fd/rate-negotiation/detail",
		mid(http.HandlerFunc(GetRateRequestDetail(pool))))
	mux.Handle("/investment/fd/rate-negotiation/audit",
		mid(http.HandlerFunc(GetRateRequestAudit(pool))))
}
