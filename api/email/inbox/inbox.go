package emailinbox

import (
	"encoding/json"
	"net/http"
	"strings"

	emailcommon "CimplrCorpSaas/api/email/common"

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
		rows, err := pool.Query(r.Context(), `
			SELECT inbox_id::text, mailbox_address, COALESCE(display_name,''),
			       filters_json, poll_interval_secs, COALESCE(module,''),
			       COALESCE(entity_id,''), is_active
			FROM email_svc.inbox_config
			ORDER BY created_at DESC
		`)
		if err != nil {
			emailcommon.RespondInternal(w, err.Error())
			return
		}
		defer rows.Close()

		var items []inboxConfig
		for rows.Next() {
			var item inboxConfig
			if err := rows.Scan(&item.InboxID, &item.MailboxAddress, &item.DisplayName,
				&item.FiltersJSON, &item.PollIntervalSecs, &item.Module, &item.EntityID, &item.IsActive); err != nil {
				emailcommon.RespondInternal(w, err.Error())
				return
			}
			items = append(items, item)
		}
		emailcommon.RespondList(w, "inbox/list", items, len(items))
	}
}

func HandleInboxCreate(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			emailcommon.RespondMethodNotAllowed(w)
			return
		}
		var req inboxConfig
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			emailcommon.RespondBadRequest(w, "invalid body")
			return
		}
		req.MailboxAddress = strings.ToLower(strings.TrimSpace(req.MailboxAddress))
		if req.MailboxAddress == "" {
			emailcommon.RespondBadRequest(w, "mailbox_address is required")
			return
		}
		if len(req.FiltersJSON) == 0 {
			req.FiltersJSON = json.RawMessage(`{}`)
		}
		if req.PollIntervalSecs <= 0 {
			req.PollIntervalSecs = 60
		}

		var inboxID string
		err := pool.QueryRow(r.Context(), `
			INSERT INTO email_svc.inbox_config (
				mailbox_address, display_name, filters_json, poll_interval_secs, module, entity_id, is_active
			) VALUES ($1, $2, $3::jsonb, $4, NULLIF($5,''), NULLIF($6,''), true)
			RETURNING inbox_id::text
		`, req.MailboxAddress, req.DisplayName, string(req.FiltersJSON), req.PollIntervalSecs, req.Module, req.EntityID).Scan(&inboxID)
		if err != nil {
			emailcommon.RespondInternal(w, err.Error())
			return
		}
		emailcommon.RespondPayload(w, "inbox/create", map[string]interface{}{"inbox_id": inboxID})
	}
}

func HandleInboxUpdate(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			emailcommon.RespondMethodNotAllowed(w)
			return
		}
		var req inboxConfig
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			emailcommon.RespondBadRequest(w, "invalid body")
			return
		}
		if strings.TrimSpace(req.InboxID) == "" {
			emailcommon.RespondBadRequest(w, "inbox_id is required")
			return
		}

		_, err := pool.Exec(r.Context(), `
			UPDATE email_svc.inbox_config
			SET display_name = COALESCE(NULLIF($2,''), display_name),
			    filters_json = COALESCE($3::jsonb, filters_json),
			    poll_interval_secs = CASE WHEN $4 > 0 THEN $4 ELSE poll_interval_secs END,
			    module = COALESCE(NULLIF($5,''), module),
			    entity_id = COALESCE(NULLIF($6,''), entity_id),
			    is_active = $7,
			    updated_at = now()
			WHERE inbox_id = $1::uuid
		`, req.InboxID, req.DisplayName, emailcommon.NullableJSON(req.FiltersJSON), req.PollIntervalSecs, req.Module, req.EntityID, req.IsActive)
		if err != nil {
			emailcommon.RespondInternal(w, err.Error())
			return
		}
		emailcommon.RespondPayload(w, "inbox/update", map[string]interface{}{})
	}
}
