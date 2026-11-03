package emailmailbox

import (
	"CimplrCorpSaas/api/errtext"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	emailcommon "CimplrCorpSaas/api/email/common"
	middlewares "CimplrCorpSaas/api/middlewares"
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
		userID := strings.TrimSpace(middlewares.GetUserIDFromContext(r.Context()))
		if userID == "" {
			emailcommon.RespondUnauthorized(w, "user context missing")
			return
		}
		addr := strings.ToLower(strings.TrimSpace(req.MailboxAddress))
		fields := req.MailboxIMAPFields
		if inboxID := strings.TrimSpace(req.InboxID); inboxID != "" {
			if !emailcommon.CanManageInbox(r.Context(), pool, inboxID, userID) {
				emailcommon.RespondForbidden(w, "inbox not accessible")
				return
			}
			mailbox, stored, err := LoadMailboxIMAPFields(r.Context(), pool, inboxID)
			if err != nil {
				emailcommon.RespondBadRequest(w, "mailbox not found")
				return
			}
			if addr == "" {
				addr = strings.ToLower(mailbox)
			}
			storedHost := strings.TrimSpace(stored.Host)
			fields.Host = ""
			if pw := strings.TrimSpace(fields.Password); pw == "" || pw == constants.RedactedPlaceholder {
				fields.Port = 0
				fields.Username = ""
			}
			merged, mergeErr := MergeIMAPFields(stored, fields, addr)
			if mergeErr != nil {
				emailcommon.RespondBadRequest(w, errtext.Detail(w, mergeErr))
				return
			}
			if storedHost != "" {
				merged.Host = storedHost
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
		if strings.TrimSpace(req.InboxID) == "" && !imapTestHostAllowed(r.Context(), payload.Host) {
			emailcommon.RespondBadRequest(w, "imap host is not allowed")
			return
		}
		if err := emailClient.VerifyIMAP(r.Context(), addr, payload); err != nil {
			api.LogErrorForResponse(w, "email imap test failed: %v", err)
			emailcommon.RespondFailPayload(w, constants.RouteIMAPTest, "request failed", map[string]interface{}{"ok": false})
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

func imapTestHostAllowed(ctx context.Context, host string) bool {
	host = strings.TrimSpace(host)
	if host == "" {
		return true
	}
	addrs, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil || len(addrs) == 0 {
		return false
	}
	for _, a := range addrs {
		ip := a.IP
		if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
			ip.IsInterfaceLocalMulticast() || ip.IsMulticast() || ip.IsUnspecified() {
			return false
		}
	}
	return true
}
