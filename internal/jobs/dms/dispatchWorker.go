// Package dmsjobs — Phase 4 dispatch: after documents are generated, queue
// emails via the existing notification catalog (TriggerNotificationForTemplates
// WithAttachments → outbox → dino worker → Notification-Service SendRawEmail)
// and log every (doc, recipient) attempt in generated_document_dispatch.
package dmsjobs

import (
	"context"
	"fmt"
	"path"
	"strings"
	"time"

	"CimplrCorpSaas/api"
	notifcatalog "CimplrCorpSaas/api/notification/catalog"
	dinojobs "CimplrCorpSaas/internal/jobs/dino"

	"github.com/jackc/pgx/v5/pgxpool"
)

const dmsDocumentGeneratedRoute = "dms/document-generated"

// DispatchRun queues notification emails for every GENERATED document in a
// successful/partial run, attaching the S3 objects, writing
// generated_document_dispatch rows, and flipping docs to DISPATCHED once
// outbox rows exist. Idempotent: docs already DISPATCHED are skipped.
func DispatchRun(ctx context.Context, pool *pgxpool.Pool, runID string) error {
	if pool == nil || strings.TrimSpace(runID) == "" {
		return fmt.Errorf("pool and run_id required")
	}

	var ruleID, versionID, ruleName, moduleCode, subModuleCode, runStatus, triggeredBy string
	var emailSubject, emailBody *string
	err := pool.QueryRow(ctx, `
		SELECT r.rule_id::text, gr.version_id::text, r.name, r.module_code, r.sub_module_code, gr.status,
		       COALESCE(gr.triggered_by, ''),
		       gr.email_subject, gr.email_body_html
		FROM dms_svc.generation_run gr
		JOIN dms_svc.generation_rule r ON r.rule_id = gr.rule_id
		WHERE gr.run_id = $1::uuid`, runID,
	).Scan(&ruleID, &versionID, &ruleName, &moduleCode, &subModuleCode, &runStatus, &triggeredBy,
		&emailSubject, &emailBody)
	if err != nil {
		return fmt.Errorf("load run: %w", err)
	}
	if runStatus != "SUCCESS" && runStatus != "PARTIAL" {
		return fmt.Errorf("run %s status=%s — nothing to dispatch", runID, runStatus)
	}

	docs, err := loadGeneratedDocs(ctx, pool, runID)
	if err != nil {
		return err
	}
	if len(docs) == 0 {
		api.LogInfo("[DMS-DISPATCH] run=%s has no GENERATED docs — skip", runID)
		return nil
	}

	tplIDs, err := loadRuleNotificationTemplates(ctx, pool, versionID)
	if err != nil {
		return err
	}

	wantEmail, err := ruleWantsEmailDispatch(ctx, pool, versionID)
	if err != nil {
		return err
	}
	if !wantEmail {
		api.LogInfo("[DMS-DISPATCH] run=%s destinations exclude EMAIL — archive/Sent Box only", runID)
		docIDs := make([]string, 0, len(docs))
		for _, d := range docs {
			docIDs = append(docIDs, d.DocID)
		}
		if len(docIDs) > 0 {
			if _, err := pool.Exec(ctx, `
				UPDATE dms_svc.generated_document SET status = 'DISPATCHED'
				WHERE doc_id = ANY($1::uuid[]) AND status = 'GENERATED'`, docIDs); err != nil {
				return fmt.Errorf("mark archive-only docs dispatched: %w", err)
			}
		}
		return nil
	}

	if len(tplIDs) == 0 {
		// Fall back to every approved template on the DMS event (seeded default).
		api.LogInfo("[DMS-DISPATCH] run=%s has no linked notification templates — using all event templates", runID)
	}

	atts := make([]notifcatalog.AttachmentRef, 0, len(docs))
	for _, d := range docs {
		atts = append(atts, notifcatalog.AttachmentRef{
			S3Key:       d.S3Key,
			Filename:    filenameForDoc(d, ruleName),
			ContentType: contentTypeForFormat(d.FileFormat),
		})
	}

	corr := "DMS-RUN-" + runID
	payload := map[string]interface{}{
		"RuleID":             ruleID,
		"RuleName":           ruleName,
		"RunID":              runID,
		"ModuleCode":         moduleCode,
		"SubModuleCode":      subModuleCode,
		"DocCount":           len(docs),
		"UserID":             triggeredBy,
		"actor_user_id":      triggeredBy,
		"DeferDeliveryNudge": true, // apply cover + attachments, then nudge below
	}
	if emailSubject != nil && strings.TrimSpace(*emailSubject) != "" {
		payload["EmailSubject"] = strings.TrimSpace(*emailSubject)
	}
	if emailBody != nil && strings.TrimSpace(*emailBody) != "" {
		payload["EmailBodyHTML"] = *emailBody
	}

	toEmails, ccEmails, recipErr := loadRuleEmailRecipients(ctx, pool, versionID)
	if recipErr != nil {
		api.LogError("[DMS-DISPATCH] loadRuleEmailRecipients run=%s: %v", runID, recipErr)
	}
	if len(toEmails) > 0 {
		payload["RecipientEmails"] = toEmails
	} else if email := resolveActorEmail(ctx, pool, triggeredBy); email != "" {
		// Fallback recipient when the linked template has no template_recipient rows
		// and the rule has no explicit To list.
		payload["RecipientEmail"] = email
		payload["RecipientName"] = triggeredBy
	}
	if len(ccEmails) > 0 {
		payload["RecipientCcEmails"] = ccEmails
	}

	outboxes, err := loadOutboxByCorrelation(ctx, pool, corr)
	if err != nil {
		return fmt.Errorf("load outbox for correlation %s: %w", corr, err)
	}
	if len(outboxes) == 0 {
		notifcatalog.TriggerNotificationForTemplatesWithAttachments(
			ctx, pool, dmsDocumentGeneratedRoute, corr, payload, tplIDs, atts,
		)
		outboxes, err = loadOutboxByCorrelation(ctx, pool, corr)
		if err != nil {
			return fmt.Errorf("load outbox for correlation %s: %w", corr, err)
		}
	}
	if len(outboxes) == 0 {
		return fmt.Errorf("no outbox rows created for correlation %s — check DMS event/templates/recipients", corr)
	}

	if len(ccEmails) > 0 {
		ccJoined := strings.Join(ccEmails, ", ")
		if _, err := pool.Exec(ctx, `
			UPDATE notification_svc.outbox
			   SET cc_emails = $2
			 WHERE correlation_id = $1
			   AND processing_status IN ('PENDING', 'QUEUED', 'PROCESSING')`,
			corr, ccJoined); err != nil {
			return fmt.Errorf("apply Cc recipients to outbox: %w", err)
		}
		api.LogInfo("[DMS-DISPATCH] run=%s applied Cc=%q to %d outbox row(s)", runID, ccJoined, len(outboxes))
	}

	// Prefer the real DMS EMAIL cover (merged at generation) over the
	// notification-catalog system body (belt-and-suspenders if payload override missed).
	if emailSubject != nil && strings.TrimSpace(*emailSubject) != "" &&
		emailBody != nil && strings.TrimSpace(*emailBody) != "" {
		if _, err := pool.Exec(ctx, `
			UPDATE notification_svc.outbox
			   SET rendered_subject = $2, rendered_body = $3
			 WHERE correlation_id = $1
			   AND processing_status IN ('PENDING', 'QUEUED', 'PROCESSING')`,
			corr, strings.TrimSpace(*emailSubject), *emailBody); err != nil {
			return fmt.Errorf("apply DMS email cover to outbox: %w", err)
		}
		api.LogInfo("[DMS-DISPATCH] run=%s applied DMS EMAIL cover subject=%q", runID, strings.TrimSpace(*emailSubject))
	}

	if len(atts) > 0 {
		attachmentCount, err := countOutboxAttachments(ctx, pool, corr)
		if err != nil {
			return fmt.Errorf("verify outbox attachments for correlation %s: %w", corr, err)
		}
		if attachmentCount == 0 {
			return fmt.Errorf("no outbox attachments created for correlation %s", corr)
		}
	}

	if err := insertDispatchRows(ctx, pool, docs, outboxes); err != nil {
		return err
	}

	docIDs := make([]string, 0, len(docs))
	for _, d := range docs {
		docIDs = append(docIDs, d.DocID)
	}
	_, err = pool.Exec(ctx, `
		UPDATE dms_svc.generated_document
		   SET status = 'DISPATCHED'
		 WHERE doc_id = ANY($1::uuid[]) AND status = 'GENERATED'`, docIDs)
	if err != nil {
		return fmt.Errorf("mark docs DISPATCHED: %w", err)
	}
	api.LogInfo("[DMS-DISPATCH] run=%s queued %d doc(s) to %d recipient outbox row(s)", runID, len(docs), len(outboxes))

	// Delivery was deferred so cover + attachments are committed first.
	dinojobs.NudgeDeliveryAfterEnqueue(ctx, pool)
	return nil
}

// StartDispatchWorker polls for GENERATED documents whose run finished, and
// dispatches any that weren't handed off inline (crash recovery / missed call).
func StartDispatchWorker(ctx context.Context, pool *pgxpool.Pool) {
	if pool == nil {
		return
	}
	poll := 30 * time.Second
	api.LogInfo("[DMS-DISPATCH] worker started (poll=%s)", poll)
	ticker := time.NewTicker(poll)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			api.LogInfo("[DMS-DISPATCH] worker stopped")
			return
		case <-ticker.C:
			syncDispatchStatuses(ctx, pool)
			dispatchPendingRuns(ctx, pool)
		}
	}
}

func dispatchPendingRuns(ctx context.Context, pool *pgxpool.Pool) {
	rows, err := pool.Query(ctx, `
		SELECT gr.run_id::text
		FROM dms_svc.generation_run gr
		JOIN dms_svc.generated_document gd ON gd.run_id = gr.run_id
		WHERE gd.status = 'GENERATED'
		  AND gr.status IN ('SUCCESS', 'PARTIAL')
		  AND gr.finished_at IS NOT NULL
		GROUP BY gr.run_id
		ORDER BY gr.run_id
		LIMIT 20`)
	if err != nil {
		api.LogError("[DMS-DISPATCH] pending runs query: %v", err)
		return
	}
	defer rows.Close()
	var runIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err == nil {
			runIDs = append(runIDs, id)
		}
	}
	for _, id := range runIDs {
		if err := DispatchRun(ctx, pool, id); err != nil {
			api.LogError("[DMS-DISPATCH] run=%s: %v", id, err)
		}
	}
}

// syncDispatchStatuses promotes PENDING generated_document_dispatch rows when
// the linked outbox row reaches SENT / DEAD / FAILED.
func syncDispatchStatuses(ctx context.Context, pool *pgxpool.Pool) {
	_, err := pool.Exec(ctx, `
		UPDATE dms_svc.generated_document_dispatch d
		   SET dispatch_status = 'SENT',
		       dispatched_at = COALESCE(d.dispatched_at, o.sent_at, now()),
		       error_detail = NULL
		  FROM notification_svc.outbox o
		 WHERE d.outbox_id = o.outbox_id
		   AND d.dispatch_status = 'PENDING'
		   AND o.processing_status = 'SENT'`)
	if err != nil {
		api.LogError("[DMS-DISPATCH] sync SENT: %v", err)
	}
	_, err = pool.Exec(ctx, `
		UPDATE dms_svc.generated_document_dispatch d
		   SET dispatch_status = 'FAILED',
		       error_detail = COALESCE(o.last_error, 'outbox dead/failed')
		  FROM notification_svc.outbox o
		 WHERE d.outbox_id = o.outbox_id
		   AND d.dispatch_status = 'PENDING'
		   AND o.processing_status IN ('DEAD', 'FAILED')`)
	if err != nil {
		api.LogError("[DMS-DISPATCH] sync FAILED: %v", err)
	}
}

type generatedDoc struct {
	DocID          string
	S3Key          string
	FileFormat     string
	Checksum       string
	OutputFilename string
}

type outboxRecipient struct {
	OutboxID  string
	Recipient string
}

func loadGeneratedDocs(ctx context.Context, pool *pgxpool.Pool, runID string) ([]generatedDoc, error) {
	rows, err := pool.Query(ctx, `
		SELECT doc_id::text, s3_key, file_format, COALESCE(checksum, ''), COALESCE(output_filename, '')
		FROM dms_svc.generated_document
		WHERE run_id = $1::uuid AND status = 'GENERATED'
		ORDER BY created_at`, runID)
	if err != nil {
		return nil, fmt.Errorf("load generated docs: %w", err)
	}
	defer rows.Close()
	var out []generatedDoc
	for rows.Next() {
		var d generatedDoc
		if err := rows.Scan(&d.DocID, &d.S3Key, &d.FileFormat, &d.Checksum, &d.OutputFilename); err != nil {
			continue
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func loadRuleEmailRecipients(ctx context.Context, pool *pgxpool.Pool, versionID string) (toEmails, ccEmails []string, err error) {
	rows, err := pool.Query(ctx, `
		SELECT address_role, email
		FROM dms_svc.generation_rule_email_recipient
		WHERE version_id = $1::uuid
		ORDER BY address_role, sort_order, email`, versionID)
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") {
			return nil, nil, nil
		}
		return nil, nil, fmt.Errorf("load email recipients: %w", err)
	}
	defer rows.Close()
	seenTo := map[string]struct{}{}
	seenCc := map[string]struct{}{}
	for rows.Next() {
		var role, email string
		if err := rows.Scan(&role, &email); err != nil {
			continue
		}
		email = strings.ToLower(strings.TrimSpace(email))
		if email == "" || !strings.Contains(email, "@") {
			continue
		}
		switch strings.ToUpper(strings.TrimSpace(role)) {
		case "TO":
			if _, dup := seenTo[email]; dup {
				continue
			}
			seenTo[email] = struct{}{}
			toEmails = append(toEmails, email)
		case "CC":
			if _, dup := seenCc[email]; dup {
				continue
			}
			seenCc[email] = struct{}{}
			ccEmails = append(ccEmails, email)
		}
	}
	return toEmails, ccEmails, rows.Err()
}

func loadRuleNotificationTemplates(ctx context.Context, pool *pgxpool.Pool, versionID string) ([]string, error) {
	rows, err := pool.Query(ctx, `
		SELECT template_id FROM dms_svc.generation_rule_notification_template
		WHERE version_id = $1::uuid AND is_deleted = false`, versionID)
	if err != nil {
		return nil, fmt.Errorf("load notification templates: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err == nil && id != "" {
			out = append(out, id)
		}
	}
	return out, rows.Err()
}

// ruleWantsEmailDispatch: EMAIL destination enabled, or no destination rows (legacy).
func ruleWantsEmailDispatch(ctx context.Context, pool *pgxpool.Pool, versionID string) (bool, error) {
	var emailN, totalN int
	if err := pool.QueryRow(ctx, `
		SELECT
			COUNT(*) FILTER (WHERE is_enabled AND destination_type = 'EMAIL'),
			COUNT(*)
		FROM dms_svc.generation_rule_destination
		WHERE version_id = $1::uuid`, versionID,
	).Scan(&emailN, &totalN); err != nil {
		// Table may not exist yet on older DBs — fall back to email.
		if strings.Contains(err.Error(), "does not exist") {
			return true, nil
		}
		return false, err
	}
	if totalN == 0 {
		return true, nil
	}
	return emailN > 0, nil
}

func loadOutboxByCorrelation(ctx context.Context, pool *pgxpool.Pool, corr string) ([]outboxRecipient, error) {
	rows, err := pool.Query(ctx, `
		SELECT outbox_id, COALESCE(NULLIF(TRIM(recipient_email), ''), recipient_user_id, '')
		FROM notification_svc.outbox
		WHERE correlation_id = $1 AND channel = 'EMAIL'
		ORDER BY created_at`, corr)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []outboxRecipient
	for rows.Next() {
		var o outboxRecipient
		if err := rows.Scan(&o.OutboxID, &o.Recipient); err != nil {
			continue
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

func countOutboxAttachments(ctx context.Context, pool *pgxpool.Pool, corr string) (int, error) {
	var count int
	err := pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM notification_svc.outbox_attachment a
		JOIN notification_svc.outbox o ON o.outbox_id = a.outbox_id
		WHERE o.correlation_id = $1 AND o.channel = 'EMAIL'`, corr).Scan(&count)
	return count, err
}

func insertDispatchRows(ctx context.Context, pool *pgxpool.Pool, docs []generatedDoc, outboxes []outboxRecipient) error {
	for _, d := range docs {
		for _, o := range outboxes {
			_, err := pool.Exec(ctx, `
				INSERT INTO dms_svc.generated_document_dispatch
					(doc_id, outbox_id, recipient, dispatch_status)
				SELECT $1::uuid, $2::text, $3::text, 'PENDING'
				WHERE NOT EXISTS (
					SELECT 1 FROM dms_svc.generated_document_dispatch x
					WHERE x.doc_id = $1::uuid AND x.outbox_id = $2::text
				)`, d.DocID, o.OutboxID, o.Recipient)
			if err != nil {
				return fmt.Errorf("insert dispatch row doc=%s outbox=%s: %w", d.DocID, o.OutboxID, err)
			}
		}
	}
	return nil
}

func filenameForDoc(d generatedDoc, ruleName string) string {
	if name := strings.TrimSpace(d.OutputFilename); name != "" {
		return name
	}
	ext := strings.ToLower(strings.TrimSpace(d.FileFormat))
	if ext == "" {
		ext = "bin"
	}
	base := strings.TrimSpace(ruleName)
	if base == "" {
		base = path.Base(d.S3Key)
		if base == "" || base == "." || base == "/" {
			base = "document"
		}
	}
	// Safe filename: letters, digits, dash, underscore, space → underscore
	var b strings.Builder
	for _, r := range base {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			b.WriteRune(r)
		case r == ' ':
			b.WriteByte('_')
		}
	}
	safe := b.String()
	if safe == "" {
		safe = "document"
	}
	return safe + "." + ext
}

func contentTypeForFormat(format string) string {
	switch strings.ToUpper(strings.TrimSpace(format)) {
	case "HTML":
		return "text/html; charset=utf-8"
	case "PDF":
		return "application/pdf"
	case "DOCX":
		return "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
	case "XLSX":
		return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
	default:
		return "application/octet-stream"
	}
}

// resolveActorEmail treats actor as a users.id or users.email (or raw email).
func resolveActorEmail(ctx context.Context, pool *pgxpool.Pool, actor string) string {
	actor = strings.TrimSpace(actor)
	if actor == "" {
		return ""
	}
	if strings.Contains(actor, "@") {
		return actor
	}
	var email string
	err := pool.QueryRow(ctx, `
		SELECT COALESCE(email, '') FROM users
		WHERE id::text = $1 OR email = $1
		LIMIT 1`, actor).Scan(&email)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(email)
}
