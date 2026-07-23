package emailworkflow

import (
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
)

func RegisterWorkflowRoutes(mux *http.ServeMux, pool *pgxpool.Pool, chain func(http.Handler) http.Handler) {
	mux.Handle("/email/inbox/workflow/list", chain(http.HandlerFunc(HandleWorkflowInboxList(pool))))
	mux.Handle("/email/inbox/workflow/meta", chain(http.HandlerFunc(HandleWorkflowMeta(pool))))
	mux.Handle("/email/inbox/workflow/create", chain(http.HandlerFunc(HandleWorkflowInboxCreate(pool))))
	mux.Handle("/email/inbox/workflow/update", chain(http.HandlerFunc(HandleWorkflowInboxUpdate(pool))))
	mux.Handle("/email/inbox/workflow/delete", chain(http.HandlerFunc(HandleWorkflowInboxDeleteRequest(pool))))
	mux.Handle("/email/inbox/workflow/approve", chain(http.HandlerFunc(HandleWorkflowInboxApprove(pool))))
	mux.Handle("/email/inbox/workflow/reject", chain(http.HandlerFunc(HandleWorkflowInboxReject(pool))))
	mux.Handle("/email/inbox/workflow/sync", chain(http.HandlerFunc(HandleWorkflowInboxSync(pool))))
	mux.Handle("/email/inbox/workflow/audit-log", chain(http.HandlerFunc(HandleWorkflowInboxAuditLog(pool))))
	mux.Handle("/email/service/health", chain(http.HandlerFunc(HandleEmailServiceHealth(pool))))
	mux.Handle("/email/poll/trigger", chain(http.HandlerFunc(HandlePollTrigger(pool))))
	mux.Handle("/email/poll/graph/trigger", chain(http.HandlerFunc(HandleGraphPollTrigger(pool))))
	mux.Handle("/email/poll/imap/trigger", chain(http.HandlerFunc(HandleIMAPPollTrigger(pool))))
}
