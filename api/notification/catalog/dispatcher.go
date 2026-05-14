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
//  7. Batch-INSERT into notification_svc.outbox (unique index for idempotency).
//  8. NudgeDeliveryAfterEnqueue — one email / browser-push / SSE pass so sends are
//     not delayed by worker poll defaults (often 10–15s).
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
	middlewares "CimplrCorpSaas/api/middlewares"
	notificationFunctions "CimplrCorpSaas/api/notification/functions"
	dinojobs "CimplrCorpSaas/internal/jobs/dino"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"
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
	eventID    string
	entityName string // empty = no restriction; non-empty = only send to matching business_unit_name
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
	correlationID   string
	eventID         string
	auditID         string // audit_template.audit_id
	channel         string
	recipientUserID string
	recipientRole   string
	recipientEmail  string
	recipientPhone  string
	recipientName   string
	renderedSubject string
	renderedBody    string
	variablesJSON   []byte
	// Priority: config.priority_level * 10 + recipient.recipient_priority
	priorityLevel int
	// Sender fields (populated from sender master when available)
	senderID         string
	senderCode       string
	senderName       string
	senderEmail      string
	senderIdentifier string // SMS sender ID / WhatsApp number
	// Entity scope (copied from event.entity_name; empty = no restriction)
	entityName string
}

// ─────────────────────────────────────────────────────────────────────────────
// resolveActorEntity
// ─────────────────────────────────────────────────────────────────────────────

// actorResolution carries the result of resolveActorEntity so callers get full
// context (which field matched, the resolved user ID/email/name) without extra
// DB round-trips later in the pipeline.
type actorResolution struct {
	UserID    string // public.users.id  (e.g. CIMPLR00D93E14F91E0B)
	Email     string // public.users.email
	Name      string // public.users.employee_name
	Entity    string // first entity_name from user_entity_mappings (empty = no restriction)
	MatchedBy string // which identifier tier resolved this actor: "id","email","username","mobile","name"
	Ambiguous bool   // true when name matched >1 row (entity still returned but flagged)
}

// resolveActorEntity maps any caller-supplied actor identifier to the user's
// business_unit_name so the dispatcher can select the right entity-scoped event.
//
// IDENTIFIER PRIORITY (most-unique → least-unique):
//
//	Tier 1  id                    — CIMPLR00… PK, DB-unique, unambiguous
//	Tier 2  email                 — DB UNIQUE constraint, case-insensitive match
//	Tier 3  username_or_employee_id — DB UNIQUE constraint, case-insensitive
//	Tier 4  mobile                — NOT unique-constrained; accepted only when exactly 1 row matches
//	Tier 5  employee_name         — NOT unique; accepted only when exactly 1 row matches
//
// Each tier is tried in order. The first tier that produces exactly one row wins.
// If name/mobile match >1 row the resolution is rejected (returns empty entity) to
// avoid silently sending notifications to the wrong entity — an explicit error is
// logged so the caller knows to fix the payload.
//
// The caller should always prefer passing `user_id` or `email` in the payload.
func resolveActorEntity(ctx context.Context, pool *pgxpool.Pool, actorValue string) actorResolution {
	if actorValue == "" {
		return actorResolution{}
	}

	type row struct {
		userID string
		email  string
		name   string
		entity string
	}

	scan := func(q string, args ...interface{}) ([]row, error) {
		rows, err := pool.Query(ctx, q, args...)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var out []row
		for rows.Next() {
			var r row
			if err := rows.Scan(&r.userID, &r.email, &r.name, &r.entity); err == nil {
				out = append(out, r)
			}
		}
		return out, rows.Err()
	}

	// sel returns first entity name from user_entity_mappings (or empty if none mapped)
	const sel = `SELECT u.id::text, COALESCE(u.email,''), COALESCE(u.employee_name,''),
		COALESCE((SELECT uem.entity_name FROM user_entity_mappings uem WHERE uem.user_id = u.id ORDER BY uem.entity_id LIMIT 1), '')
	FROM public.users u`

	// ── Tier 1: CIMPLR ID prefix or exact PK match ─────────────────────────
	if rows, err := scan(sel+` WHERE u.id::text = $1`, actorValue); err == nil && len(rows) == 1 {
		api.LogInfo("[NOTIF] resolveActorEntity: actor=%q matched by user_id → entity=%q", actorValue, rows[0].entity)
		return actorResolution{UserID: rows[0].userID, Email: rows[0].email, Name: rows[0].name, Entity: rows[0].entity, MatchedBy: "id"}
	}

	// ── Tier 2: email (case-insensitive, DB-unique) ─────────────────────────
	if rows, err := scan(sel+` WHERE lower(u.email) = lower($1)`, actorValue); err == nil && len(rows) == 1 {
		api.LogInfo("[NOTIF] resolveActorEntity: actor=%q matched by email → entity=%q", actorValue, rows[0].entity)
		return actorResolution{UserID: rows[0].userID, Email: rows[0].email, Name: rows[0].name, Entity: rows[0].entity, MatchedBy: "email"}
	}

	// ── Tier 3: username_or_employee_id (case-insensitive, DB-unique) ───────
	if rows, err := scan(sel+` WHERE lower(u.username_or_employee_id) = lower($1)`, actorValue); err == nil && len(rows) == 1 {
		api.LogInfo("[NOTIF] resolveActorEntity: actor=%q matched by username_or_employee_id → entity=%q", actorValue, rows[0].entity)
		return actorResolution{UserID: rows[0].userID, Email: rows[0].email, Name: rows[0].name, Entity: rows[0].entity, MatchedBy: "username"}
	}

	// ── Tier 4: mobile (NOT unique — reject if multiple rows match) ─────────
	if rows, err := scan(sel+` WHERE u.mobile = $1`, actorValue); err == nil {
		switch len(rows) {
		case 1:
			api.LogInfo("[NOTIF] resolveActorEntity: actor=%q matched by mobile → entity=%q", actorValue, rows[0].entity)
			return actorResolution{UserID: rows[0].userID, Email: rows[0].email, Name: rows[0].name, Entity: rows[0].entity, MatchedBy: "mobile"}
		case 0:
			// no match, continue to next tier
		default:
			api.LogError("[NOTIF] resolveActorEntity: actor=%q matched %d users by mobile — ambiguous, skipping tier", actorValue, len(rows))
		}
	}

	// ── Tier 5: employee_name (NOT unique — reject if multiple rows match) ──
	if rows, err := scan(sel+` WHERE lower(u.employee_name) = lower($1)`, actorValue); err == nil {
		switch len(rows) {
		case 1:
			api.LogInfo("[NOTIF] resolveActorEntity: actor=%q matched by employee_name → entity=%q", actorValue, rows[0].entity)
			return actorResolution{UserID: rows[0].userID, Email: rows[0].email, Name: rows[0].name, Entity: rows[0].entity, MatchedBy: "name"}
		case 0:
			// no match
		default:
			api.LogError(
				"[NOTIF] resolveActorEntity: actor=%q matched %d users by employee_name — AMBIGUOUS (multiple users share this name). "+
					"Entity resolution skipped to prevent wrong-entity notification. "+
					"FIX: pass user_id or email in the notification payload instead of a display name.",
				actorValue, len(rows),
			)
			// Return with Ambiguous flag — entity intentionally left empty
			return actorResolution{MatchedBy: "name", Ambiguous: true}
		}
	}

	// ── No tier matched ──────────────────────────────────────────────────────
	api.LogError(
		"[NOTIF] resolveActorEntity: actor=%q did not match any user (tried id / email / username_or_employee_id / mobile / employee_name). "+
			"Entity will be empty — only global events will fire. "+
			"FIX: ensure the payload carries user_id (CIMPLR00…) or email.",
		actorValue,
	)
	return actorResolution{}
}

func dispatchNotification(
	ctx context.Context,
	pool *pgxpool.Pool,
	sourceRoute string,
	correlationID string,
	payload map[string]interface{},
) error {
	api.LogInfo("[NOTIF] dispatchNotification START correlation=%s route=%s", correlationID, sourceRoute)

	// Step 1a — collect actor value from the payload.
	// Callers should pass user_id (CIMPLR00…) or email for best results.
	// Other identifiers (username_or_employee_id, mobile, employee_name) are
	// accepted as fallbacks — see resolveActorEntity for the full priority chain.
	actorValue := payloadString(payload,
		"UserID",                   // ← preferred: stable CIMPLR00… ID
		"user_id", "actor_user_id", // ← lowercase aliases used by FD/booking callers
		"ApprovedByEmail", "ApproverEmail", // ← preferred: email
		"actor_email", "email", // ← lowercase email aliases
		"ApprovedBy", "CheckerBy", // may be email OR name depending on caller
		"RejectedBy",
		"RequestedBy",
		"UploadedBy", "CreatedBy", "UpdatedBy",
		"Approver",
	)

	// Step 1b — resolve the actor to a business_unit_name (entity) using a
	// priority-ordered, unambiguous lookup.  See resolveActorEntity doc-comment
	// for the full tier chain and ambiguity rules.
	resolution := resolveActorEntity(ctx, pool, actorValue)
	actorEntity := resolution.Entity

	// Step 1b-fallback — if actor resolution didn't yield an entity, try reading
	// entity_name or entity_id directly from the payload. Booking/FD callers pass
	// these keys so we can match the entity-scoped event without a user DB round-trip.
	if actorEntity == "" {
		if en := payloadString(payload, "EntityName", "entity_name"); en != "" {
			actorEntity = en
		} else if eid := payloadString(payload, "EntityID", "entity_id"); eid != "" {
			// Resolve entity_id → entity_name via masterentitycash
			var ename string
			if err := pool.QueryRow(ctx,
				`SELECT COALESCE(entity_name,'') FROM masterentitycash WHERE entity_id = $1 AND (is_deleted=false OR is_deleted IS NULL) LIMIT 1`,
				eid,
			).Scan(&ename); err == nil && ename != "" {
				actorEntity = ename
			}
		}
		if actorEntity != "" {
			api.LogInfo("[NOTIF] entity resolved from payload key for correlation=%s: entity=%q", correlationID, actorEntity)
		}
	}

	// If we got an actorValue but could not resolve it at all, warn the user in-app.
	if actorValue != "" && resolution.UserID == "" {
		PushSystemNotification(resolution, SystemNotifParams{
			Level:         LevelWarn,
			Subject:       "Notification may not have been sent",
			Body:          fmt.Sprintf("Your user identity (%q) could not be resolved. Notifications for '%s' may be missing. Contact your admin.", actorValue, sourceRoute),
			Source:        "notification_pipeline",
			Route:         sourceRoute,
			CorrelationID: correlationID,
		})
	}

	// Step 1c — resolve events for this route filtered by actor's entity.
	// Logic:
	//   - Admin actors (IsAdminUser) → use lookupEvents (unrestricted, all entities), notification
	//     is fired for ALL entities (org-wide broadcast).
	//   - Non-admin actor with entity → org-pool match (entity + ancestors + descendants + global).
	//   - Non-admin actor with no entity → global events only.
	//   - External/extra recipients (raw email in template_recipient, no users row) always get notified.
	isAdmin := resolution.UserID != "" && middlewares.IsAdminUser(resolution.UserID)
	var events []resolvedEvent
	var err error
	if isAdmin {
		// Unrestricted lookup fans out one dispatch per entity-scoped event row — same
		// correlation_id then produces duplicate outbox batches (e.g. bank statement upload).
		// When the payload carries the domain entity, narrow admin to that org pool instead.
		if en := payloadString(payload, "EntityName", "entity_name"); en != "" {
			api.LogInfo("[NOTIF] admin actor — narrowing event lookup to payload EntityName=%q for route=%s", en, sourceRoute)
			events, err = lookupEventsForActor(ctx, pool, sourceRoute, en)
		} else if eid := payloadString(payload, "EntityID", "entity_id"); eid != "" {
			var ename string
			if qerr := pool.QueryRow(ctx,
				`SELECT COALESCE(entity_name,'') FROM masterentitycash WHERE entity_id = $1 AND (is_deleted=false OR is_deleted IS NULL) LIMIT 1`,
				eid,
			).Scan(&ename); qerr == nil && ename != "" {
				api.LogInfo("[NOTIF] admin actor — narrowing event lookup via EntityID to entity=%q for route=%s", ename, sourceRoute)
				events, err = lookupEventsForActor(ctx, pool, sourceRoute, ename)
			} else {
				api.LogInfo("[NOTIF] admin actor %q — EntityID not resolved to entity_name; using unrestricted event lookup for route=%s", resolution.UserID, sourceRoute)
				events, err = lookupEvents(ctx, pool, sourceRoute)
			}
		} else {
			api.LogInfo("[NOTIF] admin actor %q — using unrestricted event lookup (all entities) for route=%s", resolution.UserID, sourceRoute)
			events, err = lookupEvents(ctx, pool, sourceRoute)
		}
	} else {
		events, err = lookupEventsForActor(ctx, pool, sourceRoute, actorEntity)
	}
	if err != nil {
		PushSystemNotification(resolution, SystemNotifParams{
			Level:         LevelError,
			Subject:       "Notification system error",
			Body:          fmt.Sprintf("Failed to look up notification events for '%s'. Please contact support. (err: %v)", sourceRoute, err),
			Source:        "notification_pipeline",
			Route:         sourceRoute,
			CorrelationID: correlationID,
		})
		return fmt.Errorf("lookupEventsForActor: %w", err)
	}
	if len(events) == 0 {
		api.LogInfo("[NOTIF] no active approved event for route=%s entity=%q — skipping", sourceRoute, actorEntity)
		PushSystemNotification(resolution, SystemNotifParams{
			Level:         LevelWarn,
			Subject:       "Notification not configured",
			Body:          fmt.Sprintf("No active approved notification event found for '%s' (entity: %q). Ask your admin to configure and approve a notification event for this action.", sourceRoute, actorEntity),
			Source:        "notification_pipeline",
			Route:         sourceRoute,
			CorrelationID: correlationID,
		})
		return nil
	}
	events = dedupeResolvedEventsByEventID(events)
	api.LogInfo("[NOTIF] resolved %d event(s) for route=%s entity=%q", len(events), sourceRoute, actorEntity)

	var firstErr error
	for _, ev := range events {
		ev := ev // capture
		if err := dispatchForEvent(ctx, pool, sourceRoute, correlationID, payload, &ev, resolution); err != nil {
			api.LogError("[NOTIF] dispatchForEvent event=%s entity=%s err=%v", ev.eventID, ev.entityName, err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func dedupeResolvedEventsByEventID(events []resolvedEvent) []resolvedEvent {
	if len(events) <= 1 {
		return events
	}
	seen := make(map[string]struct{}, len(events))
	out := make([]resolvedEvent, 0, len(events))
	for _, ev := range events {
		if ev.eventID == "" {
			out = append(out, ev)
			continue
		}
		if _, ok := seen[ev.eventID]; ok {
			continue
		}
		seen[ev.eventID] = struct{}{}
		out = append(out, ev)
	}
	return out
}

// dispatchForEvent runs the full notification pipeline for one resolved event.
func dispatchForEvent(
	ctx context.Context,
	pool *pgxpool.Pool,
	sourceRoute string,
	correlationID string,
	payload map[string]interface{},
	event *resolvedEvent,
	actor actorResolution, // used to send system notifications back to the triggering user
) error {
	api.LogInfo("[NOTIF] dispatchForEvent event=%s entity=%q correlation=%s", event.eventID, event.entityName, correlationID)
	channels, err := lookupEnabledChannels(ctx, pool, event.eventID)
	if err != nil {
		PushSystemNotification(actor, SystemNotifParams{
			Level:         LevelError,
			Subject:       "Notification system error",
			Body:          fmt.Sprintf("Failed to load notification channels for event %s on '%s'. (err: %v)", event.eventID, sourceRoute, err),
			Source:        "notification_pipeline",
			Route:         sourceRoute,
			CorrelationID: correlationID,
		})
		return fmt.Errorf("lookupEnabledChannels: %w", err)
	}
	if len(channels) == 0 {
		api.LogInfo("[NOTIF] no enabled channels for event=%s", event.eventID)
		PushSystemNotification(actor, SystemNotifParams{
			Level:         LevelWarn,
			Subject:       "Notification not sent — no channels enabled",
			Body:          fmt.Sprintf("The notification event for '%s' exists but all delivery channels (Email/SMS/Push/WhatsApp) are disabled. Ask your admin to enable at least one channel.", sourceRoute),
			Source:        "notification_pipeline",
			Route:         sourceRoute,
			CorrelationID: correlationID,
		})
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
		// lookupTemplates returns ALL approved templates for this (event, channel) pair.
		// Each approved template is dispatched independently — different subject/body/recipients.
		tpls, err := lookupTemplates(ctx, pool, event.eventID, ch.channel)
		if err != nil {
			api.LogError("[NOTIF] lookupTemplates event=%s ch=%s: %v", event.eventID, ch.channel, err)
			PushSystemNotification(actor, SystemNotifParams{
				Level:         LevelError,
				Subject:       "Notification template error",
				Body:          fmt.Sprintf("Failed to load %s template for '%s'. (err: %v)", ch.channel, sourceRoute, err),
				Source:        "notification_pipeline",
				Route:         sourceRoute,
				CorrelationID: correlationID,
			})
			continue
		}
		if len(tpls) == 0 {
			api.LogInfo("[NOTIF] no approved template for event=%s ch=%s", event.eventID, ch.channel)
			PushSystemNotification(actor, SystemNotifParams{
				Level:         LevelWarn,
				Subject:       "Notification not sent — template not approved",
				Body:          fmt.Sprintf("No approved %s template found for '%s'. Ask your admin to approve a template for this notification event.", ch.channel, sourceRoute),
				Source:        "notification_pipeline",
				Route:         sourceRoute,
				CorrelationID: correlationID,
			})
			continue
		}
		api.LogInfo("[NOTIF] resolved %d template(s) for event=%s ch=%s", len(tpls), event.eventID, ch.channel)
		for _, tpl := range tpls {
			tpl := tpl // capture for goroutine safety
			api.LogInfo("[NOTIF] processing template auditID=%s ch=%s version=%s", tpl.auditID, ch.channel, tpl.versionLabel)
			recipients, err := lookupRecipients(ctx, pool, &tpl, payload, event.entityName)
			if err != nil {
				api.LogError("[NOTIF] lookupRecipients tpl=%s: %v", tpl.auditID, err)
				PushSystemNotification(actor, SystemNotifParams{
					Level:         LevelError,
					Subject:       "Notification recipient error",
					Body:          fmt.Sprintf("Failed to resolve recipients for %s template on '%s'. (err: %v)", ch.channel, sourceRoute, err),
					Source:        "notification_pipeline",
					Route:         sourceRoute,
					CorrelationID: correlationID,
				})
				continue
			}
			if len(recipients) == 0 {
				api.LogInfo("[NOTIF] no recipients resolved for event=%s ch=%s auditID=%s", event.eventID, ch.channel, tpl.auditID)
				PushSystemNotification(actor, SystemNotifParams{
					Level:         LevelWarn,
					Subject:       "Notification not sent — no recipients",
					Body:          fmt.Sprintf("No recipients configured for the %s notification on '%s'. Ask your admin to add recipients to the notification template.", ch.channel, sourceRoute),
					Source:        "notification_pipeline",
					Route:         sourceRoute,
					CorrelationID: correlationID,
				})
				continue
			}
			api.LogInfo("[NOTIF] resolved %d recipient(s) for event=%s ch=%s auditID=%s priority=%d",
				len(recipients), event.eventID, ch.channel, tpl.auditID, ch.priorityLevel)
			work = append(work, templateWithRecipients{tpl: tpl, recipients: recipients, chPriority: ch.priorityLevel})
		}
	}

	if len(work) == 0 {
		api.LogInfo("[NOTIF] no work items after template+recipient resolution — nothing to dispatch")
		// Individual channel errors already pushed above; no need to push again here.
		return nil
	}

	// Step 4 — evaluate templates per recipient concurrently

	// Resolve sender identity for outbox/send_history population.
	// We reuse resolveActorEntity (the same 5-tier lookup used for entity resolution)
	// rather than duplicating a separate DB query here.  Session cache is checked first
	// (free, no DB hit); resolveActorEntity is called only as a fallback.
	var senderID, senderEmail, senderName, senderCode, senderIdentifier string
	senderActorValue := payloadString(payload,
		"UserID",
		"ApprovedByEmail", "ApproverEmail",
		"ApprovedBy", "CheckerBy",
		"RejectedBy",
		"RequestedBy",
		"UploadedBy", "CreatedBy", "UpdatedBy",
		"Approver",
	)
	if senderActorValue != "" {
		// Pass 1 — check active sessions (in-memory, free, no DB hit)
		if uid, uemail, uname, ok := lookupSenderFromSession(senderActorValue); ok {
			senderID = uid
			senderEmail = uemail
			senderName = uname
			api.LogInfo("[NOTIF] sender resolved from session userID=%s name=%s email=%s", uid, uname, uemail)
		} else {
			// Pass 2 — use the same 5-tier resolver (avoids duplicating query logic)
			res := resolveActorEntity(ctx, pool, senderActorValue)
			if res.UserID != "" {
				senderID = res.UserID
				senderEmail = res.Email
				senderName = res.Name
				api.LogInfo("[NOTIF] sender resolved via resolveActorEntity matchedBy=%s userID=%s name=%s email=%s", res.MatchedBy, res.UserID, res.Name, res.Email)
			} else {
				// Last resort — store raw value so sender_name is never blank
				senderName = senderActorValue
				api.LogInfo("[NOTIF] sender unresolved for actor=%q; using raw value as sender_name", senderActorValue)
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

				// sender details
				localPayload["SenderName"] = senderName
				localPayload["SenderEmail"] = senderEmail
				localPayload["SenderID"] = senderID
				localPayload["SenderCode"] = senderCode
				localPayload["SenderIdentifier"] = senderIdentifier

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
					entityName:       event.entityName,
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

	// Deduplicate within the batch BEFORE inserting.
	// PostgreSQL raises "ON CONFLICT DO UPDATE command cannot affect row a second time"
	// (SQLSTATE 21000) when two rows in the SAME VALUES list share the same conflict key.
	// This can happen when the same recipient appears across multiple templates for the
	// same (correlation_id, channel, audit_id). We keep the last writer wins (stable order).
	{
		type conflictKey struct{ correlationID, channel, auditID, recipientUserID string }
		seen := make(map[conflictKey]int, len(outboxRows)) // key → index in deduped slice
		deduped := make([]outboxRow, 0, len(outboxRows))
		for _, row := range outboxRows {
			k := conflictKey{row.correlationID, row.channel, row.auditID, row.recipientUserID}
			if idx, exists := seen[k]; exists {
				deduped[idx] = row // overwrite with latest
			} else {
				seen[k] = len(deduped)
				deduped = append(deduped, row)
			}
		}
		if len(deduped) < len(outboxRows) {
			api.LogInfo("[NOTIF] deduped outbox batch from %d → %d rows", len(outboxRows), len(deduped))
		}
		outboxRows = deduped
	}

	// Step 5 — batch insert into outbox (idempotent via unique index)
	outboxIDMap, err := insertOutbox(ctx, pool, outboxRows)
	if err != nil {
		PushSystemNotification(actor, SystemNotifParams{
			Level:         LevelError,
			Subject:       "Notification delivery failed",
			Body:          fmt.Sprintf("Your action on '%s' was saved successfully, but the notification could not be queued (outbox insert error). Recipients may not be notified. Please inform your admin. (err: %v)", sourceRoute, err),
			Source:        "notification_pipeline",
			Route:         sourceRoute,
			CorrelationID: correlationID,
		})
		return err
	}

	// Step 6 — write PUSH rows into the persistent in-app inbox (non-fatal)
	if err := insertInAppNotification(ctx, pool, outboxRows, outboxIDMap); err != nil {
		api.LogError("[NOTIF] insertInAppNotification failed (non-fatal): %v", err)
	}

	// Step 7 — one-shot delivery pass (same logic as outbox / browser-push / inbox worker ticks).
	// Periodic workers remain; this removes the worst-case wait of OUTBOX_WORKER_POLL_SECS /
	// BROWSER_PUSH_POLL_SECS / IN_APP_WORKER_POLL_SECS for freshly enqueued rows.
	p := pool
	go func() {
		nctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		dinojobs.NudgeDeliveryAfterEnqueue(nctx, p)
	}()
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// DB helpers
// ─────────────────────────────────────────────────────────────────────────────

// lookupEvents returns ALL active+approved events for the given source_route.
// Since uq_event_source_route_entity makes source_route unique per (route, entity_name),
// the same route can belong to multiple entity-scoped events. We return all of them
// so the dispatcher fans out notifications to each entity independently.
func lookupEvents(ctx context.Context, pool *pgxpool.Pool, sourceRoute string) ([]resolvedEvent, error) {
	q := `
		SELECT e.event_id, COALESCE(e.entity_name,'')
		FROM notification_svc.event e
		WHERE e.source_route = $1
		  AND e.is_active = true
		  AND COALESCE(e.is_deleted, false) = false
		  AND EXISTS (
			  SELECT 1 FROM notification_svc.audit_event ae
			  WHERE ae.event_id = e.event_id AND ae.processing_status = 'APPROVED'
		  )
		ORDER BY
			-- entity-scoped events first (more specific)
			(COALESCE(e.entity_name,'') <> '') DESC,
			-- prefer events with an enabled notification_config
			(EXISTS (SELECT 1 FROM notification_svc.notification_config nc WHERE nc.event_id = e.event_id AND nc.is_enabled = true)) DESC,
			-- then prefer events with an approved template
			(EXISTS (
				SELECT 1 FROM notification_svc.template t
				JOIN notification_svc.audit_template at ON at.template_id = t.template_id
				WHERE t.event_id = e.event_id AND at.processing_status = 'APPROVED'
				  AND COALESCE(at.is_deleted, false) = false
			)) DESC,
			-- finally most recently approved
			(SELECT MAX(ae2.checker_at) FROM notification_svc.audit_event ae2
			 WHERE ae2.event_id = e.event_id AND ae2.processing_status = 'APPROVED') DESC NULLS LAST
	`
	rows, err := pool.Query(ctx, q, sourceRoute)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []resolvedEvent
	for rows.Next() {
		var ev resolvedEvent
		if err := rows.Scan(&ev.eventID, &ev.entityName); err != nil {
			return nil, err
		}
		out = append(out, ev)
	}
	return out, rows.Err()
}

// lookupEventsForActor selects which event(s) to fire based on the triggering actor's entity.
//
// Uses a BIDIRECTIONAL recursive CTE on cashentityrelationships to build the actor's full
// "org pool": self + all ancestors + all descendants. Any event scoped to any entity in
// this pool — or globally (entity_name=”) — is eligible.
//
// This implements the org-pool model: a notification event created for a parent entity is
// valid for children, and vice versa. The full organisation shares one notification pool.
//
// Rules:
//  1. If actorEntity != "" → build the full org pool (entity + all ancestors + all descendants).
//     Return event(s) whose entity_name is IN that pool (self/closest match first).
//     Also include global events (entity_name = "") as final fallback.
//  2. If actorEntity == "" → return only global events (entity_name = "" or NULL).
//
// Example: actor is in "BU-North" (child of "APAC" which is child of "Global").
//
//	org pool = {"BU-North", "APAC", "Global", <all siblings/cousins reachable via APAC/Global>}
//	→ any event scoped to any entity in the pool (or global) fires.
//	→ self-entity match (depth=0) is ranked first, then nearest relatives, then global.
func lookupEventsForActor(ctx context.Context, pool *pgxpool.Pool, sourceRoute, actorEntity string) ([]resolvedEvent, error) {
	// Single recursive CTE: PostgreSQL rejects UNION (dedup) between anchor and recursive terms
	// and can error (42P19) when RECURSIVE is paired with a non-recursive CTE before the working table.
	// rel_edges is inlined; anchor ∪ recursive uses UNION ALL only.
	q := `
		WITH RECURSIVE org_pool AS (
			SELECT mec.entity_name::text AS entity_name, 0 AS depth
			FROM public.masterentitycash mec
			WHERE mec.entity_name = $2
			  AND (mec.is_deleted = false OR mec.is_deleted IS NULL)
			UNION ALL
			SELECT rel.to_entity::text AS entity_name, op.depth + rel.step AS depth
			FROM org_pool op
			INNER JOIN (
				SELECT r.parent_entity_name AS from_entity, r.child_entity_name AS to_entity, 1 AS step
				FROM public.cashentityrelationships r
				UNION ALL
				SELECT r.child_entity_name AS from_entity, r.parent_entity_name AS to_entity, -1 AS step
				FROM public.cashentityrelationships r
			) rel ON rel.from_entity = op.entity_name
			WHERE ABS(op.depth) < 10
		)
		SELECT e.event_id, COALESCE(e.entity_name,'')
		FROM notification_svc.event e
		WHERE e.source_route = $1
		  AND e.is_active = true
		  AND COALESCE(e.is_deleted, false) = false
		  AND EXISTS (
			  SELECT 1 FROM notification_svc.audit_event ae
			  WHERE ae.event_id = e.event_id AND ae.processing_status = 'APPROVED'
		  )
		  AND (
			CASE
			  -- Actor has a known entity: match any event in the full org pool OR global
			  WHEN $2 <> ''
			  THEN COALESCE(e.entity_name,'') IN (SELECT entity_name FROM org_pool)
			    OR COALESCE(e.entity_name,'') = ''
			  -- Actor has no entity: only global events
			  ELSE COALESCE(e.entity_name,'') = ''
			END
		  )
		ORDER BY
			-- Self-entity (depth=0) first, then nearest relatives by absolute depth distance
			ABS(COALESCE((
				SELECT op.depth FROM org_pool op
				WHERE op.entity_name = COALESCE(e.entity_name,'') LIMIT 1
			), 999)) ASC,
			-- prefer events with an enabled notification_config
			(EXISTS (SELECT 1 FROM notification_svc.notification_config nc WHERE nc.event_id = e.event_id AND nc.is_enabled = true)) DESC,
			-- then prefer events with an approved template
			(EXISTS (
				SELECT 1 FROM notification_svc.template t
				JOIN notification_svc.audit_template at ON at.template_id = t.template_id
				WHERE t.event_id = e.event_id AND at.processing_status = 'APPROVED'
				  AND COALESCE(at.is_deleted, false) = false
			)) DESC,
			(SELECT MAX(ae2.checker_at) FROM notification_svc.audit_event ae2
			 WHERE ae2.event_id = e.event_id AND ae2.processing_status = 'APPROVED') DESC NULLS LAST
	`
	rows, err := pool.Query(ctx, q, sourceRoute, actorEntity)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []resolvedEvent
	for rows.Next() {
		var ev resolvedEvent
		if err := rows.Scan(&ev.eventID, &ev.entityName); err != nil {
			return nil, err
		}
		out = append(out, ev)
	}
	return out, rows.Err()
}

// lookupEnabledChannels returns notification_config rows where is_enabled = true.
// priority_level defaults to 3 when NULL so that finalPriority = 3*10+3 = 33.
func lookupEnabledChannels(ctx context.Context, pool *pgxpool.Pool, eventID string) ([]enabledChannel, error) {
	q := `
		SELECT nc.channel, COALESCE(nc.retry_max,3), COALESCE(nc.retry_backoff_secs,60),
		       COALESCE(nc.priority_level,3)
		FROM notification_svc.notification_config nc
		INNER JOIN notification_svc.event e ON e.event_id = nc.event_id
		WHERE nc.event_id = $1 AND nc.is_enabled = true
		  AND COALESCE(e.is_deleted, false) = false
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

// lookupTemplates fetches ALL APPROVED audit_template versions for (event_id, channel).
// Multiple approved templates for the same event+channel are all returned so that
// each one produces its own set of outbox rows (different subject/body/recipients).
// Ordered oldest→newest so the earliest-approved template is dispatched first.
func lookupTemplates(ctx context.Context, pool *pgxpool.Pool, eventID, channel string) ([]resolvedTemplate, error) {
	q := `
		SELECT
			at.audit_id::text,
			at.template_id,
			COALESCE(at.subject,'')              AS subject,
			COALESCE(at.body_text,'')            AS body_text,
			COALESCE(at.body_html,'')            AS body_html,
			COALESCE(at.is_html_enabled, false)  AS is_html,
			COALESCE(at.version_label,'')        AS version_label,
			UPPER(t.channel)                     AS channel
		FROM notification_svc.audit_template at
		JOIN notification_svc.template t ON t.template_id = at.template_id
		INNER JOIN notification_svc.event ev ON ev.event_id = t.event_id
		WHERE t.event_id = $1
		  AND UPPER(t.channel) = UPPER($2)
		  AND at.processing_status = 'APPROVED'
		  AND COALESCE(at.is_deleted, false) = false
		  AND COALESCE(ev.is_deleted, false) = false
		ORDER BY at.requested_at ASC NULLS LAST
	`
	dbRows, err := pool.Query(ctx, q, eventID, channel)
	if err != nil {
		return nil, err
	}
	defer dbRows.Close()

	var out []resolvedTemplate
	for dbRows.Next() {
		var tpl resolvedTemplate
		if err := dbRows.Scan(
			&tpl.auditID, new(string),
			&tpl.subject, &tpl.bodyText, &tpl.bodyHTML,
			&tpl.isHTML, &tpl.versionLabel, &tpl.channel,
		); err != nil {
			api.LogError("[NOTIF] lookupTemplates scan: %v", err)
			continue
		}
		out = append(out, tpl)
	}
	return out, dbRows.Err()
}

// lookupRecipients resolves template_recipient rows → actual users + their contact info.
// If entityName is non-empty, only users whose business_unit_name matches are included
// (external recipients stored as raw email in recipient_user_id bypass this filter).
// If no template_recipients exist, tries to use payload keys:
//
//	RecipientEmail / RecipientUserID / RecipientName / RecipientPhone as a fallback.
func lookupRecipients(ctx context.Context, pool *pgxpool.Pool, tpl *resolvedTemplate, payload map[string]interface{}, entityName string) ([]resolvedRecipient, error) {
	// Get the template_id from the audit row
	var templateID string
	err := pool.QueryRow(ctx, `SELECT template_id FROM notification_svc.audit_template WHERE audit_id = $1`, tpl.auditID).Scan(&templateID)
	if err != nil {
		return nil, err
	}

	// $2 = entityName (empty string = no restriction).
	// Entity filter expands the event entity into itself + all descendants via
	// cashentityrelationships, then checks each user's user_entity_mappings.
	// Rules:
	//   - ROLE/USER recipients: they must have a user_entity_mappings row whose
	//     entity_name is IN the expanded set (self + descendants)
	//   - External emails (u.id IS NULL, raw @ in recipient_user_id): ALWAYS pass through
	//   - $2 = '' means no restriction — all recipients pass
	q := `
		WITH RECURSIVE entity_tree AS (
			-- Anchor: the event's own entity (by name)
			SELECT entity_id::text AS eid, entity_name
			FROM masterentitycash
			WHERE entity_name = $2
			  AND (is_deleted = false OR is_deleted IS NULL)
			UNION ALL
			-- Recursive: all children
			SELECT mc.entity_id::text, mc.entity_name
			FROM masterentitycash mc
			INNER JOIN cashentityrelationships r ON mc.entity_name = r.child_entity_name
			INNER JOIN entity_tree et ON et.entity_name = r.parent_entity_name
			WHERE (mc.is_deleted = false OR mc.is_deleted IS NULL)
		)
		SELECT DISTINCT
			COALESCE(u.id::text, '')                    AS user_id,
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
			ON (
				-- USER type: match by UUID or by email stored in recipient_user_id
				(tr.recipient_type = 'USER'
				 AND (
					u.id::text = NULLIF(TRIM(COALESCE(tr.recipient_user_id,'')), '')
					OR
					(POSITION('@' IN COALESCE(tr.recipient_user_id,'')) > 0
					 AND u.email = NULLIF(TRIM(COALESCE(tr.recipient_user_id,'')), ''))
				 )
				)
				OR
				-- ROLE type: expand role → all users holding that role
				(tr.recipient_type = 'ROLE'
				 AND u.id IN (
					SELECT ur2.user_id FROM user_roles ur2
					JOIN roles ro2 ON ro2.id = ur2.role_id
					WHERE ro2.name = tr.recipient_role OR ro2.rolecode = tr.recipient_role
				 )
				)
			)
		WHERE tr.template_id = $1
		  AND tr.is_active = true
		  AND (
			COALESCE(u.email, '') <> ''
			OR
			(tr.recipient_type = 'USER'
			 AND POSITION('@' IN COALESCE(tr.recipient_user_id,'')) > 0)
		  )
		  AND (
			-- No entity restriction on this event
			$2 = ''
			OR
			-- External email (no users row matched) — always include
			u.id IS NULL
			OR
			-- User must be mapped to the event entity or any of its descendants
			EXISTS (
				SELECT 1 FROM user_entity_mappings uem
				JOIN entity_tree et ON et.eid = uem.entity_id OR et.entity_name = uem.entity_name
				WHERE uem.user_id = u.id::text
			)
		  )
	`
	rows, err := pool.Query(ctx, q, templateID, entityName)
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
func insertOutbox(ctx context.Context, pool *pgxpool.Pool, rows []outboxRow) (map[string]string, error) {
	if len(rows) == 0 {
		return nil, nil
	}

	var vals []string
	var args []interface{}
	pos := 1

	for _, r := range rows {
		vals = append(vals, fmt.Sprintf(
			"($%d,$%d,$%d::uuid,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d::jsonb,now(),$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
			pos, pos+1, pos+2, pos+3, pos+4, pos+5,
			pos+6, pos+7, pos+8, pos+9, pos+10, pos+11,
			pos+12, pos+13, pos+14, pos+15, pos+16, pos+17, pos+18,
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
			nullStr(r.entityName),
		)
		pos += 19
	}

	q := fmt.Sprintf(`
		INSERT INTO notification_svc.outbox (
			correlation_id, event_id, audit_id, channel,
			recipient_user_id, recipient_role, recipient_email, recipient_phone, recipient_name,
			rendered_subject, rendered_body, variables_payload, scheduled_at,
			priority_level, sender_id, sender_code, sender_name, sender_email, sender_identifier,
			entity_name
		) VALUES %s
		-- ON CONFLICT now includes audit_id so that two different approved templates
		-- for the same (correlation, channel, recipient) each get their own outbox row.
		-- REQUIRED: run the DB migration to create the new unique index (see below).
		ON CONFLICT (correlation_id, channel, audit_id, recipient_user_id) DO UPDATE SET
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
		          rendered_subject, rendered_body, COALESCE(entity_name,'')
	`, strings.Join(vals, ","))

	dbRows, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("insertOutbox: %w", err)
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
		entityName       string
	}
	var inserted []insertedRow
	for dbRows.Next() {
		var ir insertedRow
		if err := dbRows.Scan(
			&ir.outboxID, &ir.correlationID, &ir.eventID, &ir.auditID, &ir.channel,
			&ir.recipientUserID, &ir.recipientEmail, &ir.recipientPhone,
			&ir.senderName, &ir.senderEmail, &ir.senderIdentifier,
			&ir.renderedSubject, &ir.renderedBody, &ir.entityName,
		); err != nil {
			api.LogError("insertOutbox: scan row failed: %v", err)
			continue
		}
		inserted = append(inserted, ir)
	}
	if dbRows.Err() != nil {
		return nil, fmt.Errorf("insertOutbox scan: %w", dbRows.Err())
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
				"($%d,$%d,$%d,$%d::uuid,$%d,$%d,$%d,$%d,'QUEUED',NULL,NULL,1,now(),$%d,$%d,$%d,$%d,$%d,$%d)",
				shPos, shPos+1, shPos+2, shPos+3, shPos+4,
				shPos+5, shPos+6, shPos+7,
				shPos+8, shPos+9, shPos+10, shPos+11, shPos+12, shPos+13,
			))
			shArgs = append(shArgs,
				ir.outboxID, ir.correlationID, ir.eventID, ir.auditID, ir.channel,
				ir.recipientUserID, ir.recipientEmail, ir.recipientPhone,
				ir.senderName, ir.senderEmail, ir.senderIdentifier,
				derefStr(ir.renderedSubject), derefStr(ir.renderedBody),
				nullStr(ir.entityName),
			)
			shPos += 14
		}
		shQ := fmt.Sprintf(`
			INSERT INTO notification_svc.send_history (
				outbox_id, correlation_id, event_id, audit_id, channel,
				recipient_user_id, recipient_email, recipient_phone,
				processing_status, provider_response, provider_message_id, attempt_number, attempted_at,
				sender_name, sender_email, sender_identifier,
				rendered_subject, rendered_body, entity_name
			) VALUES %s
			ON CONFLICT DO NOTHING
		`, strings.Join(shVals, ","))
		if _, err := pool.Exec(ctx, shQ, shArgs...); err != nil {
			api.LogError("insertOutbox: send_history seed failed (run DB migration): %v", err)
		} else {
			api.LogInfo("Outbox: seeded %d send_history rows as QUEUED (correlation=%s)", len(inserted), rows[0].correlationID)
		}
	}

	// Build outboxID lookup map: "correlationID|channel|auditID|recipientUserID" → outboxID
	outboxIDMap := make(map[string]string, len(inserted))
	for _, ir := range inserted {
		recipUID := ""
		if ir.recipientUserID != nil {
			recipUID = *ir.recipientUserID
		}
		key := ir.correlationID + "|" + ir.channel + "|" + ir.auditID + "|" + recipUID
		outboxIDMap[key] = ir.outboxID
	}
	return outboxIDMap, nil
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
	OutboxID          string
	Row               outboxRow
	Status            string
	ProviderResp      string
	ProviderMessageID string
	AttemptNumber     int
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

// ─────────────────────────────────────────────────────────────────────────────
// insertInAppNotification
// ─────────────────────────────────────────────────────────────────────────────

// insertInAppNotification writes one row into notification_svc.in_app_notification
// for every outboxRow whose channel is "PUSH" and whose recipient_user_id is non-empty.
// Failures are non-fatal — callers MUST log the error and continue.
func insertInAppNotification(
	ctx context.Context,
	pool *pgxpool.Pool,
	rows []outboxRow,
	outboxIDs map[string]string,
) error {
	type inAppRow struct {
		outboxID        string
		correlationID   string
		eventID         string
		auditID         string
		recipientUserID string
		recipientName   string
		renderedSubject string
		renderedBody    string
		priorityLevel   int
	}

	var inAppRows []inAppRow
	for _, r := range rows {
		if r.channel != "PUSH" {
			continue
		}
		if r.recipientUserID == "" {
			continue
		}
		key := r.correlationID + "|" + r.channel + "|" + r.auditID + "|" + r.recipientUserID
		outboxID, ok := outboxIDs[key]
		if !ok || outboxID == "" {
			// Outbox row was skipped (ON CONFLICT) and already delivered — skip inbox too.
			continue
		}
		inAppRows = append(inAppRows, inAppRow{
			outboxID:        outboxID,
			correlationID:   r.correlationID,
			eventID:         r.eventID,
			auditID:         r.auditID,
			recipientUserID: r.recipientUserID,
			recipientName:   safeUTF8(r.recipientName),
			renderedSubject: safeUTF8(r.renderedSubject),
			renderedBody:    safeUTF8(r.renderedBody),
			priorityLevel:   r.priorityLevel,
		})
	}

	if len(inAppRows) == 0 {
		return nil
	}

	var vals []string
	var args []interface{}
	pos := 1
	for _, ir := range inAppRows {
		vals = append(vals, fmt.Sprintf(
			"($%d,$%d,$%d,$%d::uuid,$%d,$%d,$%d,$%d,$%d)",
			pos, pos+1, pos+2, pos+3, pos+4,
			pos+5, pos+6, pos+7, pos+8,
		))
		args = append(args,
			ir.outboxID,
			ir.correlationID,
			ir.eventID,
			ir.auditID,
			ir.recipientUserID,
			nullStr(ir.recipientName),
			nullStr(ir.renderedSubject), // → subject
			nullStr(ir.renderedBody),    // → body
			ir.priorityLevel,
		)
		pos += 9
	}

	q := fmt.Sprintf(`
		INSERT INTO notification_svc.in_app_notification (
			outbox_id, correlation_id, event_id, audit_id,
			recipient_user_id, recipient_name,
			subject, body, priority_level
		) VALUES %s
		ON CONFLICT (outbox_id, recipient_user_id) DO NOTHING
	`, strings.Join(vals, ","))

	if _, err := pool.Exec(ctx, q, args...); err != nil {
		return fmt.Errorf("insertInAppNotification: %w", err)
	}

	api.LogInfo("[NOTIF] in_app_notification: inserted %d PUSH inbox rows (correlation=%s)",
		len(inAppRows), inAppRows[0].correlationID)
	return nil
}
