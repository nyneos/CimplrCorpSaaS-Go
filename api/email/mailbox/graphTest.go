package emailmailbox

import (
	"CimplrCorpSaas/api/errtext"
	"encoding/json"
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	emailcommon "CimplrCorpSaas/api/email/common"
	middlewares "CimplrCorpSaas/api/middlewares"
	"CimplrCorpSaas/internal/services/mailruntime"

	"github.com/jackc/pgx/v5/pgxpool"
)

func HandleGraphTest(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			emailcommon.RespondMethodNotAllowed(w)
			return
		}
		var req struct {
			InboxID string `json:"inbox_id"`
			MailboxGraphFields
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			emailcommon.RespondBadRequest(w, constants.ErrInvalidBody)
			return
		}
		userID := strings.TrimSpace(middlewares.GetUserIDFromContext(r.Context()))
		if userID == "" {
			emailcommon.RespondUnauthorized(w, "user context missing")
			return
		}
		fields := req.MailboxGraphFields
		if inboxID := strings.TrimSpace(req.InboxID); inboxID != "" {
			if !emailcommon.CanManageInbox(r.Context(), pool, inboxID, userID) {
				emailcommon.RespondForbidden(w, "inbox not accessible")
				return
			}
			stored, err := LoadMailboxGraphFields(r.Context(), pool, inboxID)
			if err != nil {
				emailcommon.RespondBadRequest(w, "mailbox not found")
				return
			}
			storedTenant := strings.TrimSpace(stored.TenantID)
			fields.TenantID = ""
			merged, mergeErr := MergeGraphFields(stored, fields)
			if mergeErr != nil {
				emailcommon.RespondBadRequest(w, errtext.Detail(w, mergeErr))
				return
			}
			if storedTenant != "" || strings.TrimSpace(stored.ClientSecret) != "" {
				merged.TenantID = stored.TenantID
			}
			fields = merged
		}
		payload, err := ResolveGraphForMailbox(r.Context(), pool, fields)
		if err != nil {
			emailcommon.RespondBadRequest(w, errtext.Detail(w, err))
			return
		}
		rt := mailruntime.NewRuntime()
		if !rt.Ready() {
			emailcommon.RespondFailPayload(w, constants.RouteGraphTest, constants.ErrMailProcessingUnavailable, map[string]interface{}{"ok": false})
			return
		}
		if err := rt.VerifyGraph(r.Context(), payload); err != nil {
			api.LogErrorForResponse(w, "email graph test failed: %v", err)
			emailcommon.RespondFailPayload(w, constants.RouteGraphTest, "request failed", map[string]interface{}{"ok": false})
			return
		}
		label := strings.TrimSpace(payload.TenantLabel)
		if label == "" {
			label = strings.TrimSpace(fields.TenantKey)
		}
		if label == "" {
			label = "default"
		}
		emailcommon.RespondPayload(w, constants.RouteGraphTest, map[string]interface{}{
			"ok":        true,
			"tenant_id": payload.TenantID,
			"label":     label,
			"message":   "Microsoft Graph connection successful",
		})
	}
}
