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

func HandleIMAPTest(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			emailcommon.RespondMethodNotAllowed(w)
			return
		}
		var req struct {
			InboxID        string `json:"inbox_id"`
			MailboxAddress string `json:"mailbox_address"`
			MailboxIMAPFields
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			emailcommon.RespondBadRequest(w, constants.ErrInvalidBody)
			return
		}
		addr := strings.ToLower(strings.TrimSpace(req.MailboxAddress))
		fields := req.MailboxIMAPFields
		if inboxID := strings.TrimSpace(req.InboxID); inboxID != "" {
			mailbox, stored, err := LoadMailboxIMAPFields(r.Context(), pool, inboxID)
			if err != nil {
				emailcommon.RespondBadRequest(w, "mailbox not found")
				return
			}
			if addr == "" {
				addr = strings.ToLower(mailbox)
			}
			merged, mergeErr := MergeIMAPFields(stored, fields, addr)
			if mergeErr != nil {
				emailcommon.RespondBadRequest(w, mergeErr.Error())
				return
			}
			fields = merged
		}
		if !IsValidMailboxAddress(addr) {
			emailcommon.RespondBadRequest(w, "mailbox_address must be a valid email")
			return
		}
		emailClient := mailruntime.NewRuntime()
		if !emailClient.Ready() {
			emailcommon.RespondFailPayload(w, constants.RouteIMAPTest, constants.ErrMailProcessingUnavailable, map[string]interface{}{"ok": false})
			return
		}
		payload := fields.ToRuntimeIMAP()
		if err := emailClient.VerifyIMAP(r.Context(), addr, payload); err != nil {
			emailcommon.RespondFailPayload(w, constants.RouteIMAPTest, err.Error(), map[string]interface{}{"ok": false})
			return
		}
		host := strings.TrimSpace(payload.Host)
		if host == "" {
			host = strings.TrimSpace(fields.Host)
		}
		provider := strings.TrimSpace(payload.Provider)
		if provider == "" {
			provider = strings.TrimSpace(fields.Provider)
		}
		emailcommon.RespondPayload(w, constants.RouteIMAPTest, map[string]interface{}{
			"ok":       true,
			"host":     host,
			"provider": provider,
			"message":  "IMAP connection successful",
		})
	}
}
