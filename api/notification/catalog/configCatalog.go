package catalog

import (
	"CimplrCorpSaas/api"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// defaultChannels are seeded for every new event automatically.
var defaultChannels = []string{"EMAIL", "SMS", "PUSH", "WHATSAPP"}

// seedNotifConfigForEvents inserts one notification_config row per
// (event_id × channel) with safe defaults.
// Uses ON CONFLICT (event_id, channel) DO NOTHING so repeated calls
// (e.g. re-upload) are harmless.
// Called AFTER the event rows are committed so the FK is satisfied.
func seedNotifConfigForEvents(ctx context.Context, pgxPool *pgxpool.Pool, eventIDs []string, updatedBy string) {
	if len(eventIDs) == 0 {
		return
	}

	var valParts []string
	var args []interface{}
	pos := 1
	for _, eid := range eventIDs {
		for _, ch := range defaultChannels {
			valParts = append(valParts, fmt.Sprintf("($%d,$%d,true,3,60,$%d,now())", pos, pos+1, pos+2))
			args = append(args, eid, ch, updatedBy)
			pos += 3
		}
	}

	q := fmt.Sprintf(`
		INSERT INTO notification_svc.notification_config
			(event_id, channel, is_enabled, retry_max, retry_backoff_secs, updated_by, updated_at)
		VALUES %s
		ON CONFLICT (event_id, channel) DO NOTHING
	`, strings.Join(valParts, ","))

	if _, err := pgxPool.Exec(ctx, q, args...); err != nil {
		api.LogError("seedNotifConfigForEvents: events=%v err=%v", eventIDs, err)
	}
}

// UpsertNotifConfig — POST /notification/config/upsert
//
// Enables/disables a channel for an event or changes retry settings.
// If no config row exists yet for (event_id, channel) it is created.
//
//	{
//	  "event_id":           "EVT-XXXXXXX",   // required
//	  "channel":            "EMAIL",          // required: EMAIL|SMS|PUSH|WHATSAPP
//	  "is_enabled":         false,            // optional
//	  "retry_max":          5,                // optional
//	  "retry_backoff_secs": 120               // optional
//	}
func UpsertNotifConfig(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			EventID         string `json:"event_id"`
			Channel         string `json:"channel"`
			IsEnabled       *bool  `json:"is_enabled"`
			RetryMax        *int   `json:"retry_max"`
			RetryBackoffSec *int   `json:"retry_backoff_secs"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithPayload(w, false, "invalid request body", nil)
			return
		}
		req.Channel = strings.ToUpper(strings.TrimSpace(req.Channel))
		if req.EventID == "" || req.Channel == "" {
			api.RespondWithPayload(w, false, "event_id and channel are required", nil)
			return
		}
		validChannels := map[string]bool{"EMAIL": true, "SMS": true, "PUSH": true, "WHATSAPP": true}
		if !validChannels[req.Channel] {
			api.RespondWithPayload(w, false, "channel must be one of EMAIL, SMS, PUSH, WHATSAPP", nil)
			return
		}

		editor := getRequesterEmail()
		if editor == "" {
			editor = getRequesterEmailTemplate()
		}

		ctx := r.Context()

		// Build SET clause for the ON CONFLICT branch — only update what was sent
		var sets []string
		var args []interface{}
		pos := 1

		if req.IsEnabled != nil {
			sets = append(sets, fmt.Sprintf("is_enabled = $%d", pos))
			args = append(args, *req.IsEnabled)
			pos++
		}
		if req.RetryMax != nil {
			sets = append(sets, fmt.Sprintf("retry_max = $%d", pos))
			args = append(args, *req.RetryMax)
			pos++
		}
		if req.RetryBackoffSec != nil {
			sets = append(sets, fmt.Sprintf("retry_backoff_secs = $%d", pos))
			args = append(args, *req.RetryBackoffSec)
			pos++
		}
		sets = append(sets, fmt.Sprintf("updated_by = $%d", pos))
		args = append(args, editor)
		pos++
		sets = append(sets, "updated_at = now()")

		// Defaults for the INSERT path
		isEnabled := true
		if req.IsEnabled != nil {
			isEnabled = *req.IsEnabled
		}
		retryMax := 3
		if req.RetryMax != nil {
			retryMax = *req.RetryMax
		}
		retryBackoff := 60
		if req.RetryBackoffSec != nil {
			retryBackoff = *req.RetryBackoffSec
		}

		// pos now points to the next free param — append INSERT values after SET args
		q := fmt.Sprintf(`
			INSERT INTO notification_svc.notification_config
				(event_id, channel, is_enabled, retry_max, retry_backoff_secs, updated_by, updated_at)
			VALUES ($%d, $%d, $%d, $%d, $%d, $%d, now())
			ON CONFLICT (event_id, channel) DO UPDATE SET %s
			RETURNING config_id, event_id, channel, is_enabled, retry_max, retry_backoff_secs,
			          COALESCE(updated_by,'') AS updated_by,
			          TO_CHAR(updated_at,'YYYY-MM-DD HH24:MI:SS') AS updated_at
		`,
			pos, pos+1, pos+2, pos+3, pos+4, pos+5,
			strings.Join(sets, ", "),
		)
		args = append(args, req.EventID, req.Channel, isEnabled, retryMax, retryBackoff, editor)

		var configID, eventID, channel, updatedBy, updatedAt string
		var isEnabledOut bool
		var retryMaxOut, retryBackoffOut int
		if err := pgxPool.QueryRow(ctx, q, args...).Scan(
			&configID, &eventID, &channel, &isEnabledOut,
			&retryMaxOut, &retryBackoffOut, &updatedBy, &updatedAt,
		); err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"config_id":          configID,
			"event_id":           eventID,
			"channel":            channel,
			"is_enabled":         isEnabledOut,
			"retry_max":          retryMaxOut,
			"retry_backoff_secs": retryBackoffOut,
			"updated_by":         updatedBy,
			"updated_at":         updatedAt,
		})
	}
}

// GetNotifConfig — POST /notification/config/all
//
// Returns all config rows joined with event metadata.
// Optional filters: event_id, channel, is_enabled.
//
//	{ "event_id": "EVT-xxx", "channel": "EMAIL", "is_enabled": true }
func GetNotifConfig(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			EventID   string `json:"event_id"`
			Channel   string `json:"channel"`
			IsEnabled *bool  `json:"is_enabled"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		req.Channel = strings.ToUpper(strings.TrimSpace(req.Channel))

		ctx := r.Context()

		var whereParts []string
		var args []interface{}
		pos := 1
		if req.EventID != "" {
			whereParts = append(whereParts, fmt.Sprintf("nc.event_id = $%d", pos))
			args = append(args, req.EventID)
			pos++
		}
		if req.Channel != "" {
			whereParts = append(whereParts, fmt.Sprintf("nc.channel = $%d", pos))
			args = append(args, req.Channel)
			pos++
		}
		if req.IsEnabled != nil {
			whereParts = append(whereParts, fmt.Sprintf("nc.is_enabled = $%d", pos))
			args = append(args, *req.IsEnabled)
			pos++
		}

		whereClause := ""
		if len(whereParts) > 0 {
			whereClause = "WHERE " + strings.Join(whereParts, " AND ")
		}

		q := `
			SELECT
				nc.config_id,
				nc.event_id,
				COALESCE(e.event_display_name,'') AS event_display_name,
				COALESCE(e.module_code,'')         AS module_code,
				COALESCE(e.sub_module_code,'')     AS sub_module_code,
				nc.channel,
				nc.is_enabled,
				nc.retry_max,
				nc.retry_backoff_secs,
				COALESCE(nc.updated_by,'')         AS updated_by,
				TO_CHAR(nc.updated_at,'YYYY-MM-DD HH24:MI:SS') AS updated_at,
				-- has_template: true if at least one non-deleted template exists
				-- for this (event_id, channel) combination
				EXISTS (
					SELECT 1
					FROM notification_svc.template t
					WHERE t.event_id  = nc.event_id
					  AND t.channel   = nc.channel
					  AND COALESCE(t.is_deleted, false) = false
				) AS has_template
			FROM notification_svc.notification_config nc
			LEFT JOIN notification_svc.event e ON e.event_id = nc.event_id
		` + whereClause + `
			ORDER BY e.event_display_name, nc.channel
		`

		rows, err := pgxPool.Query(ctx, q, args...)
		if err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}
		defer rows.Close()

		fields := rows.FieldDescriptions()
		out := make([]map[string]interface{}, 0)
		for rows.Next() {
			vals, _ := rows.Values()
			row := make(map[string]interface{}, len(fields))
			for i, f := range fields {
				if vals[i] == nil {
					row[string(f.Name)] = ""
				} else {
					row[string(f.Name)] = vals[i]
				}
			}
			out = append(out, row)
		}
		if rows.Err() != nil {
			api.RespondWithPayload(w, false, "row scan error: "+rows.Err().Error(), nil)
			return
		}
		api.RespondWithPayload(w, true, "", out)
	}
}

// ToggleNotifConfig — POST /notification/config/toggle
//
// Update config for a single (event_id, channel) pair.
// is_enabled is required (on/off). retry_max and retry_backoff_secs are optional —
// omit them to leave current values unchanged.
//
//	{ "event_id": "EVT-XXXXXXX", "channel": "EMAIL", "is_enabled": false }
//	{ "event_id": "EVT-XXXXXXX", "channel": "EMAIL", "is_enabled": true, "retry_max": 5, "retry_backoff_secs": 120 }
func ToggleNotifConfig(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			EventID         string `json:"event_id"`
			Channel         string `json:"channel"`
			IsEnabled       *bool  `json:"is_enabled"`
			RetryMax        *int   `json:"retry_max"`
			RetryBackoffSec *int   `json:"retry_backoff_secs"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithPayload(w, false, "invalid request body", nil)
			return
		}
		req.Channel = strings.ToUpper(strings.TrimSpace(req.Channel))
		if req.EventID == "" || req.Channel == "" || req.IsEnabled == nil {
			api.RespondWithPayload(w, false, "event_id, channel and is_enabled are required", nil)
			return
		}

		editor := getRequesterEmail()
		if editor == "" {
			editor = getRequesterEmailTemplate()
		}

		ctx := r.Context()

		// Always set is_enabled; only set retry fields if provided
		var sets []string
		var args []interface{}
		pos := 1

		sets = append(sets, fmt.Sprintf("is_enabled = $%d", pos))
		args = append(args, *req.IsEnabled)
		pos++

		if req.RetryMax != nil {
			sets = append(sets, fmt.Sprintf("retry_max = $%d", pos))
			args = append(args, *req.RetryMax)
			pos++
		}
		if req.RetryBackoffSec != nil {
			sets = append(sets, fmt.Sprintf("retry_backoff_secs = $%d", pos))
			args = append(args, *req.RetryBackoffSec)
			pos++
		}
		sets = append(sets, fmt.Sprintf("updated_by = $%d", pos))
		args = append(args, editor)
		pos++
		sets = append(sets, "updated_at = now()")

		args = append(args, req.EventID, req.Channel)

		q := fmt.Sprintf(`
			UPDATE notification_svc.notification_config
			SET %s
			WHERE event_id = $%d AND channel = $%d
			RETURNING config_id, event_id, channel, is_enabled,
			          retry_max, retry_backoff_secs,
			          COALESCE(updated_by,'') AS updated_by,
			          TO_CHAR(updated_at,'YYYY-MM-DD HH24:MI:SS') AS updated_at
		`, strings.Join(sets, ", "), pos, pos+1)

		var configID, eventID, channel, updatedBy, updatedAt string
		var isEnabledOut bool
		var retryMax, retryBackoff int
		if err := pgxPool.QueryRow(ctx, q, args...).Scan(
			&configID, &eventID, &channel, &isEnabledOut,
			&retryMax, &retryBackoff, &updatedBy, &updatedAt,
		); err != nil {
			api.RespondWithPayload(w, false, "config not found or update failed: "+err.Error(), nil)
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"config_id":          configID,
			"event_id":           eventID,
			"channel":            channel,
			"is_enabled":         isEnabledOut,
			"retry_max":          retryMax,
			"retry_backoff_secs": retryBackoff,
			"updated_by":         updatedBy,
			"updated_at":         updatedAt,
		})
	}
}
