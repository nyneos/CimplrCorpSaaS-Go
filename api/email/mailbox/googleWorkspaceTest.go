package emailmailbox

import (
	"encoding/json"
	"net/http"
	"strings"

	emailcommon "CimplrCorpSaas/api/email/common"
	"CimplrCorpSaas/internal/services/mailruntime"

	"github.com/jackc/pgx/v5/pgxpool"
)

func HandleGoogleWorkspaceTest(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			emailcommon.RespondMethodNotAllowed(w)
			return
		}
		var req struct {
			InboxID        string `json:"inbox_id"`
			MailboxAddress string `json:"mailbox_address"`
			MailboxGoogleWorkspaceFields
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			emailcommon.RespondBadRequest(w, "invalid body")
			return
		}
		fields := req.MailboxGoogleWorkspaceFields
		mailbox := strings.TrimSpace(strings.ToLower(req.MailboxAddress))
		if inboxID := strings.TrimSpace(req.InboxID); inboxID != "" {
			stored, err := LoadMailboxGoogleWorkspaceFields(r.Context(), pool, inboxID)
			if err != nil {
				emailcommon.RespondBadRequest(w, "mailbox not found")
				return
			}
			merged, mergeErr := MergeGoogleWorkspaceFields(stored, fields)
			if mergeErr != nil {
				emailcommon.RespondBadRequest(w, mergeErr.Error())
				return
			}
			fields = merged
			if mailbox == "" {
				_ = pool.QueryRow(r.Context(), `SELECT mailbox_address FROM email_svc.inbox_config WHERE inbox_id = $1::uuid`, inboxID).Scan(&mailbox)
			}
		}
		if mailbox == "" {
			emailcommon.RespondBadRequest(w, "mailbox_address is required")
			return
		}
		payload, err := ResolveGoogleWorkspaceForMailbox(r.Context(), pool, fields)
		if err != nil {
			emailcommon.RespondBadRequest(w, err.Error())
			return
		}
		rt := mailruntime.NewRuntime()
		if !rt.Ready() {
			emailcommon.RespondFailPayload(w, "google-workspace/test", "mail processing unavailable", map[string]interface{}{"ok": false})
			return
		}
		if err := rt.VerifyGmailDWD(r.Context(), mailbox, payload); err != nil {
			emailcommon.RespondFailPayload(w, "google-workspace/test", err.Error(), map[string]interface{}{"ok": false})
			return
		}
		label := strings.TrimSpace(payload.TenantLabel)
		if label == "" {
			label = strings.TrimSpace(fields.WorkspaceTenantKey)
		}
		if label == "" {
			label = "default"
		}
		emailcommon.RespondPayload(w, "google-workspace/test", map[string]interface{}{
			"ok":                    true,
			"label":                 label,
			"service_account_email": payload.ServiceAccountEmail,
			"mailbox_address":       mailbox,
			"message":               "Gmail API (domain-wide delegation) connection successful",
		})
	}
}
