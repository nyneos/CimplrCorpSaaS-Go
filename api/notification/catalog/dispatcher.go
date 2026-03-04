package catalog

// dispatcher.go — Notification Dispatch Pipeline
//
// FLOW (simplified):
//  1. Caller fires TriggerNotification(ctx, pool, sourceRoute, correlationID, payload)
//     after a domain action succeeds (e.g. bank-statement approved).
//  2. We look up the event by source_route  → must be APPROVED + active + not-deleted.
//  3. For each enabled channel in notification_config  → check has a valid template.
//  4. Fetch the latest APPROVED audit_template version for (event_id, channel).
//  5. Resolve recipients from notification_svc.template_recipient (USER / ROLE).
//  6. Evaluate the template body/subject per recipient using the template engine
//     (goroutines + WaitGroup; mutex-protected outbox accumulator).
//  7. Batch-INSERT into notification_svc.outbox with ON CONFLICT DO NOTHING
//     (correlation_id + channel + recipient_user_id unique index = idempotency).
//
// EXAMPLE FLOWS
// ─────────────
// Flow A – Bank Statement Approved
//   sourceRoute  = "/cash/bank-statements/v2/approve"
//   correlationID = "BSID-<bankStatementID>"
//   payload = { "BankStatementID": "BS-xxx", "AccountNumber": "...", "ApproverEmail": "...",
//               "ClosingBalance": 1250000, "StatementPeriodEnd": "2026-01-31" }
//   → event EVT-B575AA7 (APPROVED, active)
//   → config: EMAIL enabled, SMS disabled
//   → template TPL-xxx v2 (latest approved) for EMAIL
//   → recipients: ROLE=TREASURY, resolved → 3 users
//   → 3 outbox rows inserted with rendered subject/body
//
// Flow B – Bank Statement Uploaded
//   sourceRoute  = "/cash/upload-bank-statement"
//   correlationID = "BSID-<bankStatementID>"
//   payload = { "UploadedBy": "user@co.com", "AccountNumber": "...", "FileName": "Q1.csv" }
//   → event EVT-E19DB8E
//   → EMAIL + PUSH enabled
//   → 2 templates rendered → outbox per recipient × channel
//
// Flow C – Fund Plan Created
//   sourceRoute  = "/cash/fund-planning/create"
//   correlationID = "FP-<fundPlanID>"
//   payload = { "FundPlanID": "FP-xxx", "CreatedBy": "maker@co.com", "TotalAmount": 5000000 }
//   → event EVT-62BBE77
//   → EMAIL enabled
//   → template body uses FORMAT_NUMBER(TotalAmount) via template engine
//
// Flow D – User Created (UAM)
//   sourceRoute  = "/uam/users/create-user"
//   correlationID = "USR-<userID>"
//   payload = { "UserID": "...", "EmployeeName": "Hardik", "Email": "h@co.com", "Role": "MAKER" }
//   → event EVT-C74EB43
//   → EMAIL + PUSH
//   → recipients: direct USER from payload fallback
//
// Flow E – Transaction Approved
//   sourceRoute  = "/cash/transactions/approve"
//   correlationID = "TXN-<txnID>"
//   payload = { "TransactionID": "TXN-xxx", "Amount": 250000, "ApprovedBy": "chk@co.com", "Currency": "INR" }
//   → event EVT-DE285CF
//   → template uses CONCAT('Approved: ', FORMAT_NUMBER(Amount), ' ', Currency)

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	notificationFunctions "CimplrCorpSaas/api/notification/functions"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ─────────────────────────────────────────────────────────────────────────────
// Public Entry Point
// ─────────────────────────────────────────────────────────────────────────────

// TriggerNotification is the single call-site for all domain events.
// It is designed to be called in a fire-and-forget goroutine so it never
// blocks the HTTP response to the end user.
//
//	go catalog.TriggerNotification(context.Background(), pool, "/cash/bank-statements/v2/approve",
//	    "BSID-"+bankStatementID, payload)
func TriggerNotification(
	ctx context.Context,
	pool *pgxpool.Pool,
	sourceRoute string,
	correlationID string,
	payload map[string]interface{},
) {
	if pool == nil || sourceRoute == "" || correlationID == "" {
		return
	}
	if err := dispatchNotification(ctx, pool, sourceRoute, correlationID, payload); err != nil {
		api.LogError("TriggerNotification sourceRoute=%s correlation=%s err=%v", sourceRoute, correlationID, err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Internal pipeline
// ─────────────────────────────────────────────────────────────────────────────

type resolvedEvent struct {
	eventID string
}

type enabledChannel struct {
	channel         string
	retryMax        int
	retryBackoffSec int
	priorityLevel   int // from notification_config.priority_level
}

type resolvedTemplate struct {
	auditID      string // uuid (from audit_template)
	channel      string
	subject      string
	bodyText     string
	bodyHTML     string
	isHTML       bool
	versionLabel string
}

type resolvedRecipient struct {
	userID            string
	email             string
	phone             string
	name              string
	role              string
	recipientPriority int // from template_recipient.recipient_priority
}

type outboxRow struct {
	correlationID    string
	eventID          string
	auditID          string // audit_template.audit_id
	channel          string
	recipientUserID  string
	recipientRole    string
	recipientEmail   string
	recipientPhone   string
	recipientName    string
	renderedSubject  string
	renderedBody     string
	variablesJSON    []byte
	// Priority: config.priority_level * 10 + recipient.recipient_priority
	priorityLevel    int
	// Sender fields (populated from sender master when available)
	senderID         string
	senderCode       string
	senderName       string
	senderEmail      string
	senderIdentifier string // SMS sender ID / WhatsApp number
}

func dispatchNotification(
	ctx context.Context,
	pool *pgxpool.Pool,
	sourceRoute string,
	correlationID string,
	payload map[string]interface{},
) error {
	api.LogInfo("[NOTIF] dispatchNotification START correlation=%s route=%s", correlationID, sourceRoute)

	// Step 1 — resolve event by source_route
	event, err := lookupEvent(ctx, pool, sourceRoute)
	if err != nil {
		return fmt.Errorf("lookupEvent: %w", err)
	}
	if event == nil {
		api.LogInfo("[NOTIF] no active approved event for route=%s — skipping", sourceRoute)
		return nil
	}
	api.LogInfo("[NOTIF] resolved event=%s for route=%s", event.eventID, sourceRoute)

	// Step 2 — fetch enabled channels from notification_config
	channels, err := lookupEnabledChannels(ctx, pool, event.eventID)
	if err != nil {
		return fmt.Errorf("lookupEnabledChannels: %w", err)
	}
	if len(channels) == 0 {
		api.LogInfo("[NOTIF] no enabled channels for event=%s", event.eventID)
		return nil
	}
	api.LogInfo("[NOTIF] event=%s has %d enabled channel(s)", event.eventID, len(channels))

	// Step 3 — for each channel, fetch latest APPROVED template + recipients
	type templateWithRecipients struct {
		tpl        resolvedTemplate
		recipients []resolvedRecipient
		chPriority int // notification_config.priority_level for this channel
	}
	var work []templateWithRecipients

	for _, ch := range channels {
		tpl, err := lookupTemplate(ctx, pool, event.eventID, ch.channel)
		if err != nil {
			api.LogError("[NOTIF] lookupTemplate event=%s ch=%s: %v", event.eventID, ch.channel, err)
			continue
		}
		if tpl == nil {
			api.LogInfo("[NOTIF] no approved template for event=%s ch=%s", event.eventID, ch.channel)
			continue
		}
		api.LogInfo("[NOTIF] resolved template auditID=%s ch=%s", tpl.auditID, ch.channel)
		recipients, err := lookupRecipients(ctx, pool, tpl, payload)
		if err != nil {
			api.LogError("[NOTIF] lookupRecipients tpl=%s: %v", tpl.auditID, err)
			continue
		}
		if len(recipients) == 0 {
			api.LogInfo("[NOTIF] no recipients resolved for event=%s ch=%s", event.eventID, ch.channel)
			continue
		}
		api.LogInfo("[NOTIF] resolved %d recipient(s) for event=%s ch=%s priority=%d", len(recipients), event.eventID, ch.channel, ch.priorityLevel)
		work = append(work, templateWithRecipients{tpl: *tpl, recipients: recipients, chPriority: ch.priorityLevel})
	}

	if len(work) == 0 {
		api.LogInfo("[NOTIF] no work items after template+recipient resolution — nothing to dispatch")
		return nil
	}

	// Step 4 — evaluate templates per recipient concurrently

	// Resolve the identity of the person who TRIGGERED this event so that
	// outbox.sender_* and send_history.sender_* are always populated.
	//
	// Strategy (fastest → slowest):
	//   1. Pick the first non-empty actor UUID from the payload using every key
	//      any event can carry (RequestedBy, CheckerBy, ApprovedBy, …).
	//   2. Check the in-memory active session map — already has UserID + Email + Name,
	//      so no DB hit needed for logged-in users.
	//   3. Fall back to a DB query (id OR email match) for non-session actors.
	//   4. If everything fails, store the raw string as sender_name so the field
	//      is never blank.
	var senderID, senderEmail, senderName, senderCode, senderIdentifier string
	actorValue := payloadString(payload,
		"ApprovedBy", "CheckerBy",
		"RejectedBy",
		"RequestedBy",
		"UploadedBy", "CreatedBy", "UpdatedBy",
		"Approver", "ApproverEmail", "ApprovedByEmail",
		"UserID",
	)
	if actorValue != "" {
		// Pass 1 — check active sessions (in-memory, free)
		if uid, uemail, uname, ok := lookupSenderFromSession(actorValue); ok {
			senderID = uid
			senderEmail = uemail
			senderName = uname
			api.LogInfo("dispatchNotification: resolved sender from session userID=%s name=%s email=%s", uid, uname, uemail)
		} else {
			// Pass 2 — hit the DB (id::text = $1 OR email = $1)
			var uid, uemail, uname string
			q := `SELECT id::text, COALESCE(email,''), COALESCE(employee_name,'') FROM users WHERE id::text=$1 OR email=$1 LIMIT 1`
			if err := pool.QueryRow(ctx, q, actorValue).Scan(&uid, &uemail, &uname); err == nil {
				senderID = uid
				senderEmail = uemail
				senderName = uname
				api.LogInfo("dispatchNotification: resolved sender from DB userID=%s name=%s email=%s", uid, uname, uemail)
			} else {
				// Pass 3 — raw fallback so sender_name is never blank
				senderName = actorValue
				api.LogInfo("dispatchNotification: actor '%s' not in session or DB (%v); using raw value as sender_name", actorValue, err)
			}
		}
	}

	var mu sync.Mutex
	var wg sync.WaitGroup
	var outboxRows []outboxRow

	payloadJSON, _ := json.Marshal(payload)

	for _, tw := range work {
		tw := tw // capture
		for _, recip := range tw.recipients {
			recip := recip // capture
			wg.Add(1)
			go func() {
				defer wg.Done()

				// Merge per-recipient fields into payload copy so templates can
				// reference RecipientName, RecipientEmail etc.
				localPayload := shallowCopyPayload(payload)
				localPayload["RecipientName"] = recip.name
				localPayload["RecipientEmail"] = recip.email
				localPayload["RecipientPhone"] = recip.phone

				body := tw.tpl.bodyText
				if tw.tpl.isHTML {
					body = tw.tpl.bodyHTML
				}

				renderedBody, err := notificationFunctions.EvaluateTemplate(body, localPayload)
				if err != nil {
					// EvaluateTemplate is resilient — it logs individual function errors
					// internally and continues rendering. This branch only fires for
					// catastrophic parse failures (extremely rare). Still deliver the email.
					api.LogInfo("[NOTIF] EvaluateTemplate partial error for event=%s ch=%s: %v (body still sent)", event.eventID, tw.tpl.channel, err)
					renderedBody = body // fallback: send raw template
				}

				renderedSubject, err := notificationFunctions.EvaluateTemplate(tw.tpl.subject, localPayload)
				if err != nil {
					renderedSubject = tw.tpl.subject
				}

				// final priority = channel_priority * 10 + recipient_priority
				// lower value = higher urgency; defaults: 3*10+3 = 33
				finalPriority := tw.chPriority*10 + recip.recipientPriority
				if finalPriority <= 0 {
					finalPriority = 33
				}

				row := outboxRow{
					correlationID:   correlationID,
					eventID:         event.eventID,
					auditID:         tw.tpl.auditID,
					channel:         tw.tpl.channel,
					recipientUserID: recip.userID,
					recipientRole:   recip.role,
					recipientEmail:  recip.email,
					recipientPhone:  recip.phone,
					recipientName:   recip.name,
					renderedSubject: renderedSubject,
					renderedBody:    renderedBody,
					variablesJSON:   payloadJSON,
					priorityLevel:   finalPriority,
					// sender fields: populate from approver lookup when available
					senderID:         senderID,
					senderCode:       senderCode,
					senderName:       senderName,
					senderEmail:      senderEmail,
					senderIdentifier: senderIdentifier,
				}

				mu.Lock()
				outboxRows = append(outboxRows, row)
				mu.Unlock()
			}()
		}
	}
	wg.Wait()

	if len(outboxRows) == 0 {
		return nil
	}

	// Step 5 — batch insert into outbox (idempotent via unique index)
	return insertOutbox(ctx, pool, outboxRows)
}

// ─────────────────────────────────────────────────────────────────────────────
// DB helpers
// ─────────────────────────────────────────────────────────────────────────────

// lookupEvent returns the event for the given source_route if it is
// active, not deleted, and has at least one APPROVED audit row.
// lookupEvent returns all events for the given source_route that are active,
// approved, not deleted, have at least one enabled channel config, and have
// at least one approved template ready. Returns the most-recently-approved
// event that is fully configured. Falls back to any approved+active event
// if none has a config yet (so callers can log a useful warning).
func lookupEvent(ctx context.Context, pool *pgxpool.Pool, sourceRoute string) (*resolvedEvent, error) {
	// Prefer events that have both an enabled config AND an approved template.
	// Falls back to any approved+active event so the caller can log the gap.
	q := `
		SELECT e.event_id
		FROM notification_svc.event e
		WHERE e.source_route = $1
		  AND e.is_active = true
		  AND COALESCE(e.is_deleted, false) = false
		  AND EXISTS (
			  SELECT 1 FROM notification_svc.audit_event ae
			  WHERE ae.event_id = e.event_id AND ae.processing_status = 'APPROVED'
		  )
		ORDER BY
			-- prefer events with an enabled notification_config
			(EXISTS (SELECT 1 FROM notification_svc.notification_config nc WHERE nc.event_id = e.event_id AND nc.is_enabled = true)) DESC,
			-- then prefer events with an approved template
			(EXISTS (
				SELECT 1 FROM notification_svc.template t
				JOIN notification_svc.audit_template at ON at.template_id = t.template_id
				WHERE t.event_id = e.event_id AND at.processing_status = 'APPROVED'
			)) DESC,
			-- finally prefer the most recently approved event
			(SELECT MAX(ae2.checker_at) FROM notification_svc.audit_event ae2
			 WHERE ae2.event_id = e.event_id AND ae2.processing_status = 'APPROVED') DESC NULLS LAST
		LIMIT 1
	`
	var eventID string
	err := pool.QueryRow(ctx, q, sourceRoute).Scan(&eventID)
	if err != nil {
		if strings.Contains(err.Error(), "no rows") {
			return nil, nil
		}
		return nil, err
	}
	return &resolvedEvent{eventID: eventID}, nil
}

// lookupEnabledChannels returns notification_config rows where is_enabled = true.
// priority_level defaults to 3 when NULL so that finalPriority = 3*10+3 = 33.
func lookupEnabledChannels(ctx context.Context, pool *pgxpool.Pool, eventID string) ([]enabledChannel, error) {
	q := `
		SELECT channel, COALESCE(retry_max,3), COALESCE(retry_backoff_secs,60),
		       COALESCE(priority_level,3)
		FROM notification_svc.notification_config
		WHERE event_id = $1 AND is_enabled = true
	`
	rows, err := pool.Query(ctx, q, eventID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []enabledChannel
	for rows.Next() {
		var ch enabledChannel
		if err := rows.Scan(&ch.channel, &ch.retryMax, &ch.retryBackoffSec, &ch.priorityLevel); err == nil {
			out = append(out, ch)
		}
	}
	return out, rows.Err()
}

// lookupTemplate fetches the latest APPROVED audit_template for (event_id, channel).
// Falls back to the latest non-deleted template if no APPROVED version exists yet.
func lookupTemplate(ctx context.Context, pool *pgxpool.Pool, eventID, channel string) (*resolvedTemplate, error) {
	q := `
		SELECT
			at.audit_id::text,
			at.template_id,
			COALESCE(at.subject,'')    AS subject,
			COALESCE(at.body_text,'')  AS body_text,
			COALESCE(at.body_html,'')  AS body_html,
			COALESCE(at.is_html_enabled, false) AS is_html,
			COALESCE(at.version_label,'') AS version_label,
			UPPER(t.channel)           AS channel
		FROM notification_svc.audit_template at
		JOIN notification_svc.template t ON t.template_id = at.template_id
		WHERE t.event_id = $1
		  AND UPPER(t.channel) = UPPER($2)
		  AND at.processing_status = 'APPROVED'
		  AND COALESCE(at.is_deleted, false) = false
		ORDER BY at.requested_at DESC NULLS LAST
		LIMIT 1
	`
	var tpl resolvedTemplate
	err := pool.QueryRow(ctx, q, eventID, channel).Scan(
		&tpl.auditID, new(string),
		&tpl.subject, &tpl.bodyText, &tpl.bodyHTML,
		&tpl.isHTML, &tpl.versionLabel, &tpl.channel,
	)
	if err != nil {
		if strings.Contains(err.Error(), "no rows") {
			return nil, nil
		}
		return nil, err
	}
	return &tpl, nil
}

// lookupRecipients resolves template_recipient rows → actual users + their contact info.
// If no template_recipients exist, tries to use payload keys:
//
//	RecipientEmail / RecipientUserID / RecipientName / RecipientPhone as a fallback.
func lookupRecipients(ctx context.Context, pool *pgxpool.Pool, tpl *resolvedTemplate, payload map[string]interface{}) ([]resolvedRecipient, error) {
	// Get the template_id from the audit row
	var templateID string
	err := pool.QueryRow(ctx, `SELECT template_id FROM notification_svc.audit_template WHERE audit_id = $1`, tpl.auditID).Scan(&templateID)
	if err != nil {
		return nil, err
	}

	q := `
		SELECT DISTINCT
			COALESCE(u.id::text, '')                    AS user_id,
			-- If no matching user found but recipient_user_id looks like an email, use it directly
			COALESCE(u.email,
				CASE WHEN tr.recipient_type = 'USER'
				          AND NULLIF(TRIM(COALESCE(tr.recipient_user_id,'')), '') IS NOT NULL
				          AND POSITION('@' IN COALESCE(tr.recipient_user_id,'')) > 0
				     THEN tr.recipient_user_id
				     ELSE ''
				END, '')                                AS email,
			''                                          AS phone,
			COALESCE(u.employee_name, '')               AS name,
			COALESCE(tr.recipient_role, '')             AS role,
			COALESCE(tr.recipient_priority, 3)          AS recipient_priority
		FROM notification_svc.template_recipient tr
		LEFT JOIN users u
			ON (tr.recipient_type = 'USER'
				AND (
					-- Match by UUID
					u.id::text = NULLIF(TRIM(COALESCE(tr.recipient_user_id,'')), '')
					OR
					-- Match by email (when caller stored an email in recipient_user_id)
					(POSITION('@' IN COALESCE(tr.recipient_user_id,'')) > 0
					 AND u.email = NULLIF(TRIM(COALESCE(tr.recipient_user_id,'')), ''))
				)
			)
			OR (tr.recipient_type = 'ROLE' AND u.id IN (
				SELECT ur2.user_id FROM user_roles ur2
				JOIN roles ro2 ON ro2.id = ur2.role_id
				WHERE ro2.name = tr.recipient_role OR ro2.rolecode = tr.recipient_role
			))
		WHERE tr.template_id = $1
		  AND tr.is_active = true
		  AND (
			-- Has a real user with email
			COALESCE(u.email, '') <> ''
			OR
			-- OR: email stored directly in recipient_user_id (external recipient)
			(tr.recipient_type = 'USER'
			 AND POSITION('@' IN COALESCE(tr.recipient_user_id,'')) > 0)
		  )
	`
	rows, err := pool.Query(ctx, q, templateID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []resolvedRecipient
	seen := map[string]bool{}
	for rows.Next() {
		var r resolvedRecipient
		if err := rows.Scan(&r.userID, &r.email, &r.phone, &r.name, &r.role, &r.recipientPriority); err != nil {
			continue
		}
		key := r.email
		if seen[key] {
			continue
		}
		seen[key] = true
		// If employee_name is empty in users table, derive a friendly name from email
		// (e.g. "hardik.mishra@company.com" → "Hardik Mishra")
		if r.name == "" && r.email != "" {
			r.name = nameFromEmail(r.email)
		}
		out = append(out, r)
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}

	// Fallback: if no recipients configured but payload has a direct email, use that.
	if len(out) == 0 {
		email := payloadString(payload, "RecipientEmail", "recipient_email")
		if email != "" {
			out = append(out, resolvedRecipient{
				userID: payloadString(payload, "RecipientUserID", "recipient_user_id", "UserID"),
				email:  email,
				phone:  payloadString(payload, "RecipientPhone", "recipient_phone"),
				name:   payloadString(payload, "RecipientName", "recipient_name", "EmployeeName"),
			})
		}
	}

	return out, nil
}

// insertOutbox batch-inserts outbox rows and immediately writes a 'QUEUED'
// send_history record for each new row (first-time population).
// The unique index uq_outbox_idempotency (correlation_id, channel, recipient_user_id)
// guarantees idempotency — conflicts are silently skipped.
func insertOutbox(ctx context.Context, pool *pgxpool.Pool, rows []outboxRow) error {
	if len(rows) == 0 {
		return nil
	}

	var vals []string
	var args []interface{}
	pos := 1

	for _, r := range rows {
		vals = append(vals, fmt.Sprintf(
			"($%d,$%d,$%d::uuid,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d::jsonb,now(),$%d,$%d,$%d,$%d,$%d,$%d)",
			pos, pos+1, pos+2, pos+3, pos+4, pos+5,
			pos+6, pos+7, pos+8, pos+9, pos+10, pos+11,
			pos+12, pos+13, pos+14, pos+15, pos+16, pos+17,
		))
		args = append(args,
			r.correlationID,
			r.eventID,
			r.auditID,
			r.channel,
			r.recipientUserID,
			r.recipientRole,
			r.recipientEmail,
			r.recipientPhone,
			safeUTF8(r.recipientName),
			safeUTF8(r.renderedSubject),
			safeUTF8(r.renderedBody),
			string(r.variablesJSON),
			r.priorityLevel,
			nullStr(r.senderID),
			nullStr(r.senderCode),
			nullStr(r.senderName),
			nullStr(r.senderEmail),
			nullStr(r.senderIdentifier),
		)
		pos += 18
	}

	q := fmt.Sprintf(`
		INSERT INTO notification_svc.outbox (
			correlation_id, event_id, audit_id, channel,
			recipient_user_id, recipient_role, recipient_email, recipient_phone, recipient_name,
			rendered_subject, rendered_body, variables_payload, scheduled_at,
			priority_level, sender_id, sender_code, sender_name, sender_email, sender_identifier
		) VALUES %s
		ON CONFLICT (correlation_id, channel, recipient_user_id) DO UPDATE SET
			rendered_subject = CASE
				WHEN notification_svc.outbox.processing_status = 'PENDING'
				THEN EXCLUDED.rendered_subject
				ELSE notification_svc.outbox.rendered_subject
			END,
			rendered_body = CASE
				WHEN notification_svc.outbox.processing_status = 'PENDING'
				THEN EXCLUDED.rendered_body
				ELSE notification_svc.outbox.rendered_body
			END,
			variables_payload = CASE
				WHEN notification_svc.outbox.processing_status = 'PENDING'
				THEN EXCLUDED.variables_payload
				ELSE notification_svc.outbox.variables_payload
			END
		RETURNING outbox_id, correlation_id, event_id, audit_id, channel,
		          recipient_user_id, recipient_email, recipient_phone,
		          sender_name, sender_email, sender_identifier,
		          rendered_subject, rendered_body
	`, strings.Join(vals, ","))

	dbRows, err := pool.Query(ctx, q, args...)
	if err != nil {
		return fmt.Errorf("insertOutbox: %w", err)
	}
	defer dbRows.Close()

	// Collect inserted outbox_ids and seed send_history as 'QUEUED'
	type insertedRow struct {
		outboxID         string
		correlationID    string
		eventID          string
		auditID          string
		channel          string
		recipientUserID  *string
		recipientEmail   *string
		recipientPhone   *string
		senderName       *string
		senderEmail      *string
		senderIdentifier *string
		renderedSubject  *string
		renderedBody     *string
	}
	var inserted []insertedRow
	for dbRows.Next() {
		var ir insertedRow
		if err := dbRows.Scan(
			&ir.outboxID, &ir.correlationID, &ir.eventID, &ir.auditID, &ir.channel,
			&ir.recipientUserID, &ir.recipientEmail, &ir.recipientPhone,
			&ir.senderName, &ir.senderEmail, &ir.senderIdentifier,
			&ir.renderedSubject, &ir.renderedBody,
		); err != nil {
			api.LogError("insertOutbox: scan row failed: %v", err)
			continue
		}
		inserted = append(inserted, ir)
	}
	if dbRows.Err() != nil {
		return fmt.Errorf("insertOutbox scan: %w", dbRows.Err())
	}
	dbRows.Close()

	api.LogInfo("Outbox: inserted %d rows (correlation=%s)", len(inserted), rows[0].correlationID)

	// Attempt to seed send_history with 'QUEUED' status.
	// REQUIRES: send_history_status_chk must include 'QUEUED' and outbox_id must have a UNIQUE constraint.
	// Run this SQL once in Supabase if not done yet:
	//   ALTER TABLE notification_svc.send_history DROP CONSTRAINT send_history_status_chk;
	//   ALTER TABLE notification_svc.send_history ADD CONSTRAINT send_history_status_chk
	//     CHECK (processing_status IN ('QUEUED','SENT','FAILED'));
	//   ALTER TABLE notification_svc.send_history ADD CONSTRAINT send_history_outbox_id_key UNIQUE (outbox_id);
	if len(inserted) > 0 {
		var shVals []string
		var shArgs []interface{}
		shPos := 1
		for _, ir := range inserted {
			shVals = append(shVals, fmt.Sprintf(
				"($%d,$%d,$%d,$%d::uuid,$%d,$%d,$%d,$%d,'QUEUED',NULL,NULL,1,now(),$%d,$%d,$%d,$%d,$%d)",
				shPos, shPos+1, shPos+2, shPos+3, shPos+4,
				shPos+5, shPos+6, shPos+7,
				shPos+8, shPos+9, shPos+10, shPos+11, shPos+12,
			))
			shArgs = append(shArgs,
				ir.outboxID, ir.correlationID, ir.eventID, ir.auditID, ir.channel,
				ir.recipientUserID, ir.recipientEmail, ir.recipientPhone,
				ir.senderName, ir.senderEmail, ir.senderIdentifier,
				derefStr(ir.renderedSubject), derefStr(ir.renderedBody),
			)
			shPos += 13
		}
		shQ := fmt.Sprintf(`
			INSERT INTO notification_svc.send_history (
				outbox_id, correlation_id, event_id, audit_id, channel,
				recipient_user_id, recipient_email, recipient_phone,
				processing_status, provider_response, provider_message_id, attempt_number, attempted_at,
				sender_name, sender_email, sender_identifier,
				rendered_subject, rendered_body
			) VALUES %s
			ON CONFLICT DO NOTHING
		`, strings.Join(shVals, ","))
		if _, err := pool.Exec(ctx, shQ, shArgs...); err != nil {
			api.LogError("insertOutbox: send_history seed failed (run DB migration): %v", err)
		} else {
			api.LogInfo("Outbox: seeded %d send_history rows as QUEUED (correlation=%s)", len(inserted), rows[0].correlationID)
		}
	}
	return nil
}

// InsertSendHistory records a delivery attempt result in send_history.
// Called by the worker after it attempts to send via a provider.
// Uses ON CONFLICT (outbox_id) DO UPDATE so the worker can promote
// a 'QUEUED' row to 'SENT' or 'FAILED' in place.
// status must be "SENT", "FAILED", or "QUEUED".
func InsertSendHistory(
	ctx context.Context,
	pool *pgxpool.Pool,
	params SendHistoryParams,
) error {
	q := `
		INSERT INTO notification_svc.send_history (
			outbox_id, correlation_id, event_id, audit_id, channel,
			recipient_user_id, recipient_email, recipient_phone,
			processing_status, provider_response, provider_message_id, attempt_number, attempted_at,
			sender_name, sender_email, sender_identifier,
			rendered_subject, rendered_body
		) VALUES (
			$1, $2, $3, $4::uuid, $5,
			$6, $7, $8,
			$9, $10, $11, $12, now(),
			$13, $14, $15,
			$16, $17
		)
		ON CONFLICT (outbox_id) DO UPDATE SET
			processing_status = EXCLUDED.processing_status,
			provider_response = EXCLUDED.provider_response,
			provider_message_id = EXCLUDED.provider_message_id,
			attempt_number = EXCLUDED.attempt_number,
			attempted_at = now(),
			sender_name = COALESCE(EXCLUDED.sender_name, notification_svc.send_history.sender_name),
			sender_email = COALESCE(EXCLUDED.sender_email, notification_svc.send_history.sender_email),
			sender_identifier = COALESCE(EXCLUDED.sender_identifier, notification_svc.send_history.sender_identifier),
			rendered_subject = COALESCE(EXCLUDED.rendered_subject, notification_svc.send_history.rendered_subject),
			rendered_body = COALESCE(EXCLUDED.rendered_body, notification_svc.send_history.rendered_body)
	`

	if _, err := pool.Exec(ctx, q,
		params.OutboxID,
		params.Row.correlationID,
		params.Row.eventID,
		params.Row.auditID,
		params.Row.channel,
		nullStr(params.Row.recipientUserID),
		nullStr(params.Row.recipientEmail),
		nullStr(params.Row.recipientPhone),
		params.Status,
		nullStr(params.ProviderResp),
		nullStr(params.ProviderMessageID),
		params.AttemptNumber,
		nullStr(params.Row.senderName),
		nullStr(params.Row.senderEmail),
		nullStr(params.Row.senderIdentifier),
		safeUTF8(params.Row.renderedSubject),
		safeUTF8(params.Row.renderedBody),
	); err != nil {
		return fmt.Errorf("InsertSendHistory: %w", err)
	}

	// Also update outbox row so last_error/processing_status/retry_count/sent_at reflect the
	// latest delivery attempt. If provider reported an error, store it in last_error.
	// For FAILED: increment retry_count and set last_error; for SENT: set sent_at.
	upq := `
		UPDATE notification_svc.outbox SET
			processing_status = $1,
			last_error = $2,
			retry_count = CASE WHEN $1 = 'FAILED' THEN retry_count + 1 ELSE retry_count END,
			sent_at = CASE WHEN $1 = 'SENT' THEN now() ELSE sent_at END,
			processed_at = now()
		WHERE outbox_id = $3
	`

	if _, err := pool.Exec(ctx, upq, params.Status, nullStr(params.ProviderResp), params.OutboxID); err != nil {
		// Log and return error — caller should know if outbox update failed
		return fmt.Errorf("InsertSendHistory (update outbox): %w", err)
	}

	return nil
}

// nullStr converts an empty string to nil (SQL NULL) so optional VARCHAR columns
// receive NULL instead of empty string.
func nullStr(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

// nameFromEmail derives a human-readable display name from an email address.

// SendHistoryParams groups parameters for InsertSendHistory to keep signatures small.
type SendHistoryParams struct {
	OutboxID string
	Row outboxRow
	Status string
	ProviderResp string
	ProviderMessageID string
	AttemptNumber int
}
// "hardik.mishra@company.com" → "Hardik Mishra"
// "admin@company.com"        → "Admin"
// Falls back to the local-part as-is if no dots found.
func nameFromEmail(email string) string {
	local := email
	if at := strings.Index(email, "@"); at > 0 {
		local = email[:at]
	}
	// Replace dots/underscores/hyphens with spaces and title-case each word
	local = strings.NewReplacer(".", " ", "_", " ", "-", " ").Replace(local)
	words := strings.Fields(local)
	for i, w := range words {
		if len(w) > 0 {
			words[i] = strings.ToUpper(w[:1]) + strings.ToLower(w[1:])
		}
	}
	return strings.Join(words, " ")
}

// derefStr safely dereferences a *string returned from a nullable DB column.
// Returns empty string if nil, the value (safe UTF-8) otherwise.
func derefStr(s *string) string {
	if s == nil {
		return ""
	}
	return safeUTF8(*s)
}

// ─────────────────────────────────────────────────────────────────────────────
// Tiny helpers
// ─────────────────────────────────────────────────────────────────────────────

// safeUTF8 strips any invalid UTF-8 bytes from s so PostgreSQL never
// rejects the string with "invalid byte sequence for encoding UTF8".
func safeUTF8(s string) string {
	if utf8.ValidString(s) {
		return s
	}
	return strings.ToValidUTF8(s, "")
}

// shallowCopyPayload returns a shallow copy of the payload map with extra capacity.
func shallowCopyPayload(src map[string]interface{}) map[string]interface{} {
	dst := make(map[string]interface{}, len(src)+4)
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

// lookupSenderFromSession checks the in-memory active session list for a user
// whose UserID or Email matches actorValue. Returns (userID, email, name, true)
// when found, or ("", "", "", false) when not found.
// This avoids a DB round-trip for the common case where the triggering user
// is currently logged in.
func lookupSenderFromSession(actorValue string) (string, string, string, bool) {
	for _, s := range auth.GetActiveSessions() {
		if s.UserID == actorValue || s.Email == actorValue {
			return s.UserID, s.Email, s.Name, true
		}
	}
	return "", "", "", false
}

// payloadString returns the first non-empty string value for any of the given keys.
func payloadString(payload map[string]interface{}, keys ...string) string {
	for _, k := range keys {
		if v, ok := payload[k]; ok {
			if s, ok := v.(string); ok && s != "" {
				return s
			}
		}
	}
	return ""
}
