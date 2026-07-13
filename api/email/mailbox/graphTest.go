package emailmailbox

import (
	"encoding/json"
	"net/http"
	"strings"

	"CimplrCorpSaas/api/constants"
	emailcommon "CimplrCorpSaas/api/email/common"
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
		fields := req.MailboxGraphFields
		if inboxID := strings.TrimSpace(req.InboxID); inboxID != "" {
			stored, err := LoadMailboxGraphFields(r.Context(), pool, inboxID)
			if err != nil {
				emailcommon.RespondBadRequest(w, "mailbox not found")
				return
			}
			merged, mergeErr := MergeGraphFields(stored, fields)
			if mergeErr != nil {
				emailcommon.RespondBadRequest(w, mergeErr.Error())
				return
			}
			fields = merged
		}
		payload, err := ResolveGraphForMailbox(r.Context(), pool, fields)
		if err != nil {
			emailcommon.RespondBadRequest(w, err.Error())
			return
		}
		rt := mailruntime.NewRuntime()
		if !rt.Ready() {
			emailcommon.RespondFailPayload(w, constants.RouteGraphTest, constants.ErrMailProcessingUnavailable, map[string]interface{}{"ok": false})
			return
		}
		if err := rt.VerifyGraph(r.Context(), payload); err != nil {
			emailcommon.RespondFailPayload(w, constants.RouteGraphTest, err.Error(), map[string]interface{}{"ok": false})
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
