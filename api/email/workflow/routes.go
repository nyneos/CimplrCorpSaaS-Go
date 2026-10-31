package emailworkflow

import (
	"net/http"

	middlewares "CimplrCorpSaas/api/middlewares"

	"github.com/jackc/pgx/v5/pgxpool"
)

func RegisterWorkflowRoutes(mux *http.ServeMux, pool *pgxpool.Pool, chain func(http.Handler) http.Handler) {
	perm := func(page, action string, h http.Handler) http.Handler {
		return chain(middlewares.RequirePermission(pool, page, action)(h))
	}

	mux.Handle("/email/inbox/workflow/list", chain(http.HandlerFunc(HandleWorkflowInboxList(pool))))
	mux.Handle("/email/inbox/workflow/meta", chain(http.HandlerFunc(HandleWorkflowMeta(pool))))
	mux.Handle("/email/inbox/workflow/create", perm("mail-config", middlewares.PermCreate, http.HandlerFunc(HandleWorkflowInboxCreate(pool))))
	mux.Handle("/email/inbox/workflow/update", perm("mail-config", middlewares.PermEdit, http.HandlerFunc(HandleWorkflowInboxUpdate(pool))))
	mux.Handle("/email/inbox/workflow/delete", perm("mail-config", middlewares.PermDelete, http.HandlerFunc(HandleWorkflowInboxDeleteRequest(pool))))
	mux.Handle("/email/inbox/workflow/approve", perm("mail-config", middlewares.PermApprove, http.HandlerFunc(HandleWorkflowInboxApprove(pool))))
	mux.Handle("/email/inbox/workflow/reject", perm("mail-config", middlewares.PermReject, http.HandlerFunc(HandleWorkflowInboxReject(pool))))
	mux.Handle("/email/inbox/workflow/sync", chain(http.HandlerFunc(HandleWorkflowInboxSync(pool))))
	mux.Handle("/email/inbox/workflow/audit-log", chain(http.HandlerFunc(HandleWorkflowInboxAuditLog(pool))))
	mux.Handle("/email/service/health", chain(http.HandlerFunc(HandleEmailServiceHealth(pool))))
	mux.Handle("/email/poll/trigger", chain(http.HandlerFunc(HandlePollTrigger(pool))))
	mux.Handle("/email/poll/graph/trigger", chain(http.HandlerFunc(HandleGraphPollTrigger(pool))))
	mux.Handle("/email/poll/imap/trigger", chain(http.HandlerFunc(HandleIMAPPollTrigger(pool))))
}
