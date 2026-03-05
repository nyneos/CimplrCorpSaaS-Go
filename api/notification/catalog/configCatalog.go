package catalog

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// defaultChannels are seeded for every new event automatically.
var defaultChannels = []string{"EMAIL", "SMS", "PUSH", "WHATSAPP"}

// seedNotifConfigForEvents inserts one notification_config row per
// (event_id × channel) with safe defaults, then seeds a corresponding
// audit_notification_config row (action_type=CREATE, PENDING_APPROVAL).
// ON CONFLICT DO NOTHING keeps repeated calls idempotent.
// Called AFTER the event rows are committed so the FK is satisfied.
func seedNotifConfigForEvents(ctx context.Context, pgxPool *pgxpool.Pool, eventIDs []string, updatedBy string) {
	if len(eventIDs) == 0 {
		return
	}

	// ── Step 1: insert notification_config rows, collect returned config_ids ──
	var valParts []string
	var args []interface{}
	pos := 1
	for _, eid := range eventIDs {
		for _, ch := range defaultChannels {
			valParts = append(valParts, fmt.Sprintf("($%d,$%d,false,3,60,3,$%d,now())", pos, pos+1, pos+2))
			args = append(args, eid, ch, updatedBy)
			pos += 3
		}
	}

	q := fmt.Sprintf(`
		INSERT INTO notification_svc.notification_config
			(event_id, channel, is_enabled, retry_max, retry_backoff_secs, priority_level, updated_by, updated_at)
		VALUES %s
		ON CONFLICT (event_id, channel) DO NOTHING
		RETURNING config_id, event_id, channel
	`, strings.Join(valParts, ","))

	rows, err := pgxPool.Query(ctx, q, args...)
	if err != nil {
		api.LogError("seedNotifConfigForEvents insert: events=%v err=%v", eventIDs, err)
		return
	}

	type configRow struct{ configID, eventID, channel string }
	var inserted []configRow
	for rows.Next() {
		var r configRow
		if err := rows.Scan(&r.configID, &r.eventID, &r.channel); err == nil {
			inserted = append(inserted, r)
		}
	}
	rows.Close()
	if rows.Err() != nil {
		api.LogError("seedNotifConfigForEvents scan: %v", rows.Err())
	}

	if len(inserted) == 0 {
		return // all rows already existed (idempotent re-run), nothing to audit
	}

	// ── Step 2: insert audit rows (CREATE / PENDING_APPROVAL) ──
	var aValParts []string
	var aArgs []interface{}
	aPos := 1
	for _, r := range inserted {
		// ($1,$2,$3,$4,$5)
		aValParts = append(aValParts, fmt.Sprintf("($%d,$%d,$%d,'CREATE','PENDING_APPROVAL',$%d,now())", aPos, aPos+1, aPos+2, aPos+3))
		aArgs = append(aArgs, r.configID, r.eventID, r.channel, updatedBy)
		aPos += 4
	}
	aQ := fmt.Sprintf(`
		INSERT INTO notification_svc.audit_notification_config
			(config_id, event_id, channel, action_type, processing_status, requested_by, requested_at)
		VALUES %s
	`, strings.Join(aValParts, ","))

	if _, err := pgxPool.Exec(ctx, aQ, aArgs...); err != nil {
		api.LogError("seedNotifConfigForEvents audit: %v", err)
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
			PriorityLevel   *int   `json:"priority_level"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithPayload(w, false, constants.ErrInvalidRequestBody, nil)
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
		if req.PriorityLevel != nil {
			sets = append(sets, fmt.Sprintf("priority_level = $%d", pos))
			args = append(args, *req.PriorityLevel)
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
		priorityLevel := 3
		if req.PriorityLevel != nil {
			priorityLevel = *req.PriorityLevel
		}

		// pos now points to the next free param — append INSERT values after SET args
		q := fmt.Sprintf(`
			INSERT INTO notification_svc.notification_config
				(event_id, channel, is_enabled, retry_max, retry_backoff_secs, priority_level, updated_by, updated_at)
			VALUES ($%d, $%d, $%d, $%d, $%d, $%d, $%d, now())
			ON CONFLICT (event_id, channel) DO UPDATE SET %s
			RETURNING config_id, event_id, channel, is_enabled, retry_max, retry_backoff_secs, priority_level,
			          COALESCE(updated_by,'') AS updated_by,
			          TO_CHAR(updated_at,'YYYY-MM-DD HH24:MI:SS') AS updated_at
		`,
			pos, pos+1, pos+2, pos+3, pos+4, pos+5, pos+6,
			strings.Join(sets, ", "),
		)
		args = append(args, req.EventID, req.Channel, isEnabled, retryMax, retryBackoff, priorityLevel, editor)

		var configID, eventID, channel, updatedBy, updatedAt string
		var isEnabledOut bool
		var retryMaxOut, retryBackoffOut, priorityLevelOut int
		if err := pgxPool.QueryRow(ctx, q, args...).Scan(
			&configID, &eventID, &channel, &isEnabledOut,
			&retryMaxOut, &retryBackoffOut, &priorityLevelOut, &updatedBy, &updatedAt,
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
			"priority_level":     priorityLevelOut,
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

		whereClause := "WHERE COALESCE(e.is_deleted, false) = false"
		if len(whereParts) > 0 {
			whereClause += " AND " + strings.Join(whereParts, " AND ")
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
				COALESCE(nc.priority_level, 3)     AS priority_level,
				COALESCE(nc.updated_by,'')         AS updated_by,
				TO_CHAR(nc.updated_at,'YYYY-MM-DD HH24:MI:SS') AS updated_at,
				-- latest audit row for this config
				COALESCE(la_cfg.processing_status,'') AS processing_status,
				COALESCE(la_cfg.action_type,'')       AS audit_action_type,
				COALESCE(la_cfg.requested_by,'')      AS audit_requested_by,
				TO_CHAR(la_cfg.requested_at,'YYYY-MM-DD HH24:MI:SS') AS audit_requested_at,
				COALESCE(la_cfg.checker_by,'')        AS audit_checker_by,
				TO_CHAR(la_cfg.checker_at,'YYYY-MM-DD HH24:MI:SS')   AS audit_checker_at,
				COALESCE(la_cfg.checker_comment,'')   AS audit_checker_comment,
				-- has_template: true if at least one approved, non-archived template exists
				EXISTS (
					SELECT 1
					FROM notification_svc.template t
					JOIN notification_svc.audit_template at ON at.template_id = t.template_id
					WHERE t.event_id  = nc.event_id
					  AND t.channel   = nc.channel
					  AND at.processing_status = 'APPROVED'
					  AND COALESCE(at.is_deleted, false) = false
				) AS has_template
			FROM notification_svc.notification_config nc
			LEFT JOIN notification_svc.event e ON e.event_id = nc.event_id
			-- exclude configs whose parent event has been soft-deleted
			LEFT JOIN LATERAL (
				SELECT GREATEST(
					COALESCE(MAX(a.requested_at), '1970-01-01'::timestamp),
					COALESCE(MAX(a.checker_at),   '1970-01-01'::timestamp)
				) AS latest_ts
				FROM notification_svc.audit_event a
				WHERE a.event_id = nc.event_id
			) la ON true
			-- latest audit row for the config itself
			LEFT JOIN LATERAL (
				SELECT ac.processing_status, ac.action_type, ac.requested_by,
				       ac.requested_at, ac.checker_by, ac.checker_at, ac.checker_comment
				FROM notification_svc.audit_notification_config ac
				WHERE ac.config_id = nc.config_id
				ORDER BY ac.requested_at DESC NULLS LAST
				LIMIT 1
			) la_cfg ON true
		` + whereClause + `
			ORDER BY la.latest_ts DESC, e.event_display_name, nc.channel
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
// Bulk maker-checker update. Accepts multiple rows, each changing
// is_enabled / retry_max / retry_backoff_secs / priority_level for one
// (event_id, channel) pair.
// For every item it:
//  1. Reads current values (FOR UPDATE).
//  2. Updates the live notification_config row immediately.
//  3. Inserts an audit_notification_config row (EDIT / PENDING_EDIT_APPROVAL)
//     carrying the OLD values so an approver can review / rollback.
//
// Request:
//
//	{
//	  "rows": [
//	    {
//	      "event_id":          "EVT-xxx",   // required
//	      "channel":           "EMAIL",     // required
//	      "is_enabled":        true,        // optional — omit to leave unchanged
//	      "retry_max":         5,           // optional
//	      "retry_backoff_secs":120,         // optional
//	      "priority_level":    2,           // optional
//	      "reason":            "..."        // optional
//	    }
//	  ]
//	}
func ToggleNotifConfig(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Rows []struct {
				EventID         string `json:"event_id"`
				Channel         string `json:"channel"`
				IsEnabled       *bool  `json:"is_enabled"`
				RetryMax        *int   `json:"retry_max"`
				RetryBackoffSec *int   `json:"retry_backoff_secs"`
				PriorityLevel   *int   `json:"priority_level"`
				Reason          string `json:"reason"`
			} `json:"rows"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithPayload(w, false, constants.ErrInvalidRequestBody, nil)
			return
		}
		if len(req.Rows) == 0 {
			api.RespondWithPayload(w, false, "rows required", nil)
			return
		}

		editor := getRequesterEmail()
		if editor == "" {
			editor = getRequesterEmailTemplate()
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}
		defer tx.Rollback(ctx)

		type result struct {
			ConfigID string      `json:"config_id"`
			EventID  string      `json:"event_id"`
			Channel  string      `json:"channel"`
			Success  bool        `json:"success"`
			Error    string      `json:"error,omitempty"`
			Data     interface{} `json:"data,omitempty"`
		}
		results := make([]result, 0, len(req.Rows))

		for _, row := range req.Rows {
			row.Channel = strings.ToUpper(strings.TrimSpace(row.Channel))
			if row.EventID == "" || row.Channel == "" {
				results = append(results, result{EventID: row.EventID, Channel: row.Channel, Success: false, Error: "event_id and channel are required"})
				continue
			}

			// ── 1. Read old values ──
			var configID string
			var oldEnabled bool
			var oldRetryMax, oldRetryBackoff, oldPriority int
			selQ := `
				SELECT config_id, is_enabled, retry_max, retry_backoff_secs, priority_level
				FROM notification_svc.notification_config
				WHERE event_id = $1 AND channel = $2
				FOR UPDATE`
			if err := tx.QueryRow(ctx, selQ, row.EventID, row.Channel).Scan(
				&configID, &oldEnabled, &oldRetryMax, &oldRetryBackoff, &oldPriority,
			); err != nil {
				results = append(results, result{EventID: row.EventID, Channel: row.Channel, Success: false, Error: "config not found: " + err.Error()})
				continue
			}

			// ── 2. Build UPDATE ──
			var sets []string
			var uArgs []interface{}
			uPos := 1
			if row.IsEnabled != nil {
				sets = append(sets, fmt.Sprintf("is_enabled = $%d", uPos))
				uArgs = append(uArgs, *row.IsEnabled)
				uPos++
			}
			if row.RetryMax != nil {
				sets = append(sets, fmt.Sprintf("retry_max = $%d", uPos))
				uArgs = append(uArgs, *row.RetryMax)
				uPos++
			}
			if row.RetryBackoffSec != nil {
				sets = append(sets, fmt.Sprintf("retry_backoff_secs = $%d", uPos))
				uArgs = append(uArgs, *row.RetryBackoffSec)
				uPos++
			}
			if row.PriorityLevel != nil {
				sets = append(sets, fmt.Sprintf("priority_level = $%d", uPos))
				uArgs = append(uArgs, *row.PriorityLevel)
				uPos++
			}
			if len(sets) == 0 {
				results = append(results, result{ConfigID: configID, EventID: row.EventID, Channel: row.Channel, Success: false, Error: "no fields to update"})
				continue
			}
			sets = append(sets, fmt.Sprintf("updated_by = $%d", uPos), "updated_at = now()")
			uArgs = append(uArgs, editor)
			uPos++
			uArgs = append(uArgs, configID)

			updQ := fmt.Sprintf(
				"UPDATE notification_svc.notification_config SET %s WHERE config_id = $%d RETURNING config_id, event_id, channel, is_enabled, retry_max, retry_backoff_secs, priority_level",
				strings.Join(sets, ", "), uPos,
			)
			var newConfigID, newEventID, newChannel string
			var newEnabled bool
			var newRetry, newBackoff, newPriority int
			if err := tx.QueryRow(ctx, updQ, uArgs...).Scan(
				&newConfigID, &newEventID, &newChannel, &newEnabled, &newRetry, &newBackoff, &newPriority,
			); err != nil {
				results = append(results, result{ConfigID: configID, EventID: row.EventID, Channel: row.Channel, Success: false, Error: "update failed: " + err.Error()})
				continue
			}

			// ── 3. Insert PENDING_EDIT_APPROVAL audit with old values ──
			auditQ := `
				INSERT INTO notification_svc.audit_notification_config
					(config_id, event_id, channel, action_type, processing_status, reason, requested_by, requested_at,
					 old_is_enabled, old_retry_max, old_retry_backoff_secs, old_priority_level)
				VALUES ($1,$2,$3,'EDIT','PENDING_EDIT_APPROVAL',$4,$5,now(),$6,$7,$8,$9)`
			if _, err := tx.Exec(ctx, auditQ,
				configID, row.EventID, row.Channel,
				row.Reason, editor,
				oldEnabled, oldRetryMax, oldRetryBackoff, oldPriority,
			); err != nil {
				results = append(results, result{ConfigID: configID, EventID: row.EventID, Channel: row.Channel, Success: false, Error: "audit insert failed: " + err.Error()})
				continue
			}

			results = append(results, result{
				ConfigID: newConfigID, EventID: newEventID, Channel: newChannel, Success: true,
				Data: map[string]interface{}{
					"is_enabled":         newEnabled,
					"retry_max":          newRetry,
					"retry_backoff_secs": newBackoff,
					"priority_level":     newPriority,
					"requested_by":       editor,
					"processing_status":  "PENDING_EDIT_APPROVAL",
				},
			})
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithPayload(w, false, "commit failed: "+err.Error(), nil)
			return
		}

		api.RespondWithPayload(w, api.IsBulkSuccess(func() []map[string]interface{} {
			out := make([]map[string]interface{}, len(results))
			for i, r := range results {
				out[i] = map[string]interface{}{"success": r.Success}
			}
			return out
		}()), "", results)
	}
}

// BulkApproveNotifConfig — POST /notification/config/bulk-approve
//
// Approves the latest PENDING audit row for each supplied config_id.
// Works for both CREATE and EDIT action types.
//
//	{ "config_ids": ["CFG-xxx","CFG-yyy"], "comment": "LGTM" }
func BulkApproveNotifConfig(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ConfigIDs []string `json:"config_ids"`
			Comment   string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithPayload(w, false, constants.ErrInvalidRequestBody, nil)
			return
		}
		if len(req.ConfigIDs) == 0 {
			api.RespondWithPayload(w, false, "config_ids required", nil)
			return
		}

		checker := getRequesterEmail()
		if checker == "" {
			checker = getRequesterEmailTemplate()
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}
		defer tx.Rollback(ctx)

		var tag pgconn.CommandTag
		if strings.TrimSpace(req.Comment) == "" {
			tag, err = tx.Exec(ctx, `
				UPDATE notification_svc.audit_notification_config
				SET processing_status = 'APPROVED',
					checker_by        = $1,
					checker_at        = now()
				WHERE config_id = ANY($2::text[])
				  AND processing_status LIKE '%PENDING%'
			`, checker, req.ConfigIDs)
		} else {
			tag, err = tx.Exec(ctx, `
				UPDATE notification_svc.audit_notification_config
				SET processing_status = 'APPROVED',
					checker_by        = $1,
					checker_at        = now(),
					checker_comment   = $2
				WHERE config_id = ANY($3::text[])
				  AND processing_status LIKE '%PENDING%'
			`, checker, req.Comment, req.ConfigIDs)
		}
		if err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithPayload(w, false, "commit failed: "+err.Error(), nil)
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"approved_count": tag.RowsAffected(),
			"checker":        checker,
		})
		api.LogInfo("BulkApproveNotifConfig: %d rows by %s", tag.RowsAffected(), checker)
	}
}

// BulkRejectNotifConfig — POST /notification/config/bulk-reject
//
// Rejects the latest PENDING audit row(s) for each supplied config_id.
// For EDIT audits it also rolls back the live notification_config row
// to the stored old_* values (since toggle applied changes optimistically).
//
//	{ "config_ids": ["CFG-xxx","CFG-yyy"], "comment": "Not approved" }
func BulkRejectNotifConfig(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ConfigIDs []string `json:"config_ids"`
			Comment   string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithPayload(w, false, constants.ErrInvalidRequestBody, nil)
			return
		}
		if len(req.ConfigIDs) == 0 {
			api.RespondWithPayload(w, false, "config_ids required", nil)
			return
		}

		checker := getRequesterEmail()
		if checker == "" {
			checker = getRequesterEmailTemplate()
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}
		defer tx.Rollback(ctx)

		// Fetch old_* values for EDIT audits so we can rollback the live row
		type rollbackRow struct {
			configID        string
			oldEnabled      *bool
			oldRetryMax     *int
			oldRetryBackoff *int
			oldPriority     *int
			actionType      string
		}
		auditRows, err := tx.Query(ctx, `
			SELECT config_id, action_type,
			       old_is_enabled, old_retry_max, old_retry_backoff_secs, old_priority_level
			FROM notification_svc.audit_notification_config
			WHERE config_id = ANY($1::text[])
			  AND processing_status LIKE '%PENDING%'
		`, req.ConfigIDs)
		if err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}
		var rollbacks []rollbackRow
		for auditRows.Next() {
			var rb rollbackRow
			if err := auditRows.Scan(&rb.configID, &rb.actionType,
				&rb.oldEnabled, &rb.oldRetryMax, &rb.oldRetryBackoff, &rb.oldPriority,
			); err == nil {
				rollbacks = append(rollbacks, rb)
			}
		}
		auditRows.Close()

		// Mark audits REJECTED
		var tag pgconn.CommandTag
		if strings.TrimSpace(req.Comment) == "" {
			tag, err = tx.Exec(ctx, `
				UPDATE notification_svc.audit_notification_config
				SET processing_status = 'REJECTED',
					checker_by        = $1,
					checker_at        = now()
				WHERE config_id = ANY($2::text[])
				  AND processing_status LIKE '%PENDING%'
			`, checker, req.ConfigIDs)
		} else {
			tag, err = tx.Exec(ctx, `
				UPDATE notification_svc.audit_notification_config
				SET processing_status = 'REJECTED',
					checker_by        = $1,
					checker_at        = now(),
					checker_comment   = $2
				WHERE config_id = ANY($3::text[])
				  AND processing_status LIKE '%PENDING%'
			`, checker, req.Comment, req.ConfigIDs)
		}
		if err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}

		// Rollback EDIT changes to the live config row using stored old_* values
		for _, rb := range rollbacks {
			if rb.actionType != "EDIT" {
				continue
			}
			var sets []string
			var rArgs []interface{}
			rPos := 1
			if rb.oldEnabled != nil {
				sets = append(sets, fmt.Sprintf("is_enabled = $%d", rPos))
				rArgs = append(rArgs, *rb.oldEnabled)
				rPos++
			}
			if rb.oldRetryMax != nil {
				sets = append(sets, fmt.Sprintf("retry_max = $%d", rPos))
				rArgs = append(rArgs, *rb.oldRetryMax)
				rPos++
			}
			if rb.oldRetryBackoff != nil {
				sets = append(sets, fmt.Sprintf("retry_backoff_secs = $%d", rPos))
				rArgs = append(rArgs, *rb.oldRetryBackoff)
				rPos++
			}
			if rb.oldPriority != nil {
				sets = append(sets, fmt.Sprintf("priority_level = $%d", rPos))
				rArgs = append(rArgs, *rb.oldPriority)
				rPos++
			}
			if len(sets) == 0 {
				continue
			}
			sets = append(sets, fmt.Sprintf("updated_by = $%d", rPos), "updated_at = now()")
			rArgs = append(rArgs, checker, rb.configID)
			rPos++
			rQ := fmt.Sprintf("UPDATE notification_svc.notification_config SET %s WHERE config_id = $%d", strings.Join(sets, ", "), rPos)
			if _, err := tx.Exec(ctx, rQ, rArgs...); err != nil {
				api.LogError("BulkRejectNotifConfig rollback config_id=%s err=%v", rb.configID, err)
			}
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithPayload(w, false, "commit failed: "+err.Error(), nil)
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"rejected_count": tag.RowsAffected(),
			"checker":        checker,
			"rolled_back":    len(rollbacks),
		})
		api.LogInfo("BulkRejectNotifConfig: %d rows, %d rolled back by %s", tag.RowsAffected(), len(rollbacks), checker)
	}
}
