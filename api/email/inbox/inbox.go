package emailinbox

import (
	"encoding/json"
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	emailcommon "CimplrCorpSaas/api/email/common"
	middlewares "CimplrCorpSaas/api/middlewares"

	"github.com/jackc/pgx/v5/pgxpool"
)

type inboxConfig struct {
	InboxID          string          `json:"inbox_id"`
	MailboxAddress   string          `json:"mailbox_address"`
	DisplayName      string          `json:"display_name"`
	FiltersJSON      json.RawMessage `json:"filters_json"`
	PollIntervalSecs int             `json:"poll_interval_secs"`
	Module           string          `json:"module"`
	EntityID         string          `json:"entity_id"`
	IsActive         bool            `json:"is_active"`
}

func HandleInboxList(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			emailcommon.RespondMethodNotAllowed(w)
			return
		}
		ctx := r.Context()
		userID := strings.TrimSpace(middlewares.GetUserIDFromContext(ctx))
		if userID == "" {
			emailcommon.RespondUnauthorized(w, "user context missing")
			return
		}
		rows, err := pool.Query(ctx, `
			SELECT i.inbox_id::text, i.mailbox_address, COALESCE(i.display_name,''),
			       i.filters_json, i.poll_interval_secs, COALESCE(i.module,''),
			       COALESCE(i.entity_id,''), i.is_active
			FROM email_svc.inbox_config i
			WHERE i.is_deleted = false
			  AND (
			      $1::boolean
			      OR i.owner_user_id = $2
			      OR i.submitted_by = $2
			      OR EXISTS (SELECT 1 FROM email_svc.inbox_members im WHERE im.inbox_id = i.inbox_id AND im.user_id = $2)
			      OR (COALESCE(i.entity_id,'') <> '' AND i.entity_id = ANY($3::text[]))
			  )
			ORDER BY i.created_at DESC
		`, emailcommon.IsEmailAdmin(ctx, userID), userID, api.GetEntityIDsFromCtx(ctx))
		if err != nil {
			api.LogErrorForResponse(w, "email inbox list query failed: %v", err)
			emailcommon.RespondInternal(w, "database operation failed")
			return
		}
		defer rows.Close()

		var items []inboxConfig
		for rows.Next() {
			var item inboxConfig
			if err := rows.Scan(&item.InboxID, &item.MailboxAddress, &item.DisplayName,
				&item.FiltersJSON, &item.PollIntervalSecs, &item.Module, &item.EntityID, &item.IsActive); err != nil {
				api.LogErrorForResponse(w, "email inbox list scan failed: %v", err)
				emailcommon.RespondInternal(w, "database operation failed")
				return
			}
			items = append(items, item)
		}
		if err := rows.Err(); err != nil {
			api.LogErrorForResponse(w, "email inbox list rows failed: %v", err)
			emailcommon.RespondInternal(w, "database operation failed")
			return
		}
		emailcommon.RespondList(w, "inbox/list", items, len(items))
	}
}
