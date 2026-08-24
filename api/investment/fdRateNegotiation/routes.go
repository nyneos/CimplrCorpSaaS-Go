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
	mux.Handle("/investment/fd/rate-negotiation/list-comparable",
		mid(http.HandlerFunc(ListComparableRateRequests(pool))))
	mux.Handle("/investment/fd/rate-negotiation/detail",
		mid(http.HandlerFunc(GetRateRequestDetail(pool))))
	mux.Handle("/investment/fd/rate-negotiation/audit",
		mid(http.HandlerFunc(GetRateRequestAudit(pool))))
	mux.Handle("/investment/fd/rate-negotiation/approve",
		mid(http.HandlerFunc(ApproveRateRequests(pool))))
	mux.Handle("/investment/fd/rate-negotiation/reject",
		mid(http.HandlerFunc(RejectRateRequests(pool))))
	mux.Handle("/investment/fd/rate-negotiation/delete",
		mid(http.HandlerFunc(DeleteRateRequests(pool))))
	mux.Handle("/investment/fd/rate-negotiation/communication/create",
		mid(http.HandlerFunc(CreateCommunication(pool))))
	mux.Handle("/investment/fd/rate-negotiation/communication/list",
		mid(http.HandlerFunc(ListCommunications(pool))))
	mux.Handle("/investment/fd/rate-negotiation/communication/update",
		mid(http.HandlerFunc(UpdateCommunication(pool))))
	mux.Handle("/investment/fd/rate-negotiation/communication/approve",
		mid(http.HandlerFunc(ApproveCommunications(pool))))
	mux.Handle("/investment/fd/rate-negotiation/communication/reject",
		mid(http.HandlerFunc(RejectCommunications(pool))))
	mux.Handle("/investment/fd/rate-negotiation/communication/delete",
		mid(http.HandlerFunc(DeleteCommunications(pool))))
	mux.Handle("/investment/fd/rate-negotiation/documents/list",
		mid(http.HandlerFunc(ListDocuments(pool))))
	mux.Handle("/investment/fd/rate-negotiation/documents/link",
		mid(http.HandlerFunc(LinkDocuments(pool))))
	mux.Handle("/investment/fd/rate-negotiation/offer/create",
		mid(http.HandlerFunc(CreateOffer(pool))))
	mux.Handle("/investment/fd/rate-negotiation/offer/list",
		mid(http.HandlerFunc(ListOffers(pool))))
	mux.Handle("/investment/fd/rate-negotiation/offer/update",
		mid(http.HandlerFunc(UpdateOffer(pool))))
	mux.Handle("/investment/fd/rate-negotiation/selection/submit",
		mid(http.HandlerFunc(SubmitSelection(pool))))
	mux.Handle("/investment/fd/rate-negotiation/booking/link",
		mid(http.HandlerFunc(LinkBooking(pool))))
	mux.Handle("/investment/fd/rate-negotiation/dashboard",
		mid(http.HandlerFunc(DashboardSummary(pool))))
	mux.Handle("/investment/fd/rate-negotiation/communication/templates",
		mid(http.HandlerFunc(ListCommunicationTemplates(pool))))
	mux.Handle("/investment/fd/rate-negotiation/communication/audit",
		mid(http.HandlerFunc(ListCommunicationAudit(pool))))
	mux.Handle("/investment/fd/rate-negotiation/offer/audit",
		mid(http.HandlerFunc(ListOfferAudit(pool))))
	mux.Handle("/investment/fd/rate-negotiation/offer/approve",
		mid(http.HandlerFunc(ApproveOffers(pool))))
	mux.Handle("/investment/fd/rate-negotiation/offer/reject",
		mid(http.HandlerFunc(RejectOffers(pool))))
	mux.Handle("/investment/fd/rate-negotiation/offer/delete",
		mid(http.HandlerFunc(DeleteOffers(pool))))
	mux.Handle("/investment/fd/rate-negotiation/bank-exposure",
		mid(http.HandlerFunc(BankExposure(pool))))
	mux.Handle("/investment/fd/rate-negotiation/offer/list-approved",
		mid(http.HandlerFunc(ListApprovedOffersByEntityBank(pool))))
	mux.Handle("/investment/fd/rate-negotiation/communication/email/detail",
		mid(http.HandlerFunc(GetCommunicationSentEmail(pool))))
	mux.Handle("/investment/fd/rate-negotiation/selection/comparison-trail",
		mid(http.HandlerFunc(ListSelectionComparisonTrail(pool))))
}
