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
	var runZip *generatedPackage
	wantsZip, err := ruleWantsZipPackage(ctx, pool, versionID)
	if err != nil {
		return fmt.Errorf("check ZIP destinations: %w", err)
	}
	if wantsZip {
		pkg, zipErr := ensureRunZip(ctx, pool, runID, ruleName, docs)
		if zipErr != nil {
			return zipErr
		}
		runZip = &pkg
	}

	// Channel-level delivery log (S3 / EMAIL / SFTP / …) — same idea as
	// email_svc.transformation_result_deliveries. EMAIL still also writes
	// generated_document_dispatch for per-recipient outbox tracking.
	if err := recordChannelDeliveries(ctx, pool, runID, versionID, docs, wantEmail, runZip); err != nil {
		api.LogError("[DMS-DISPATCH] recordChannelDeliveries run=%s: %v", runID, err)
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
			S3Key:          d.S3Key,
			Filename:       filenameForDoc(d, ruleName),
			ContentType:    contentTypeForFormat(d.FileFormat),
			StorageBackend: d.StorageBackend,
		})
	}
	var zipAtts []notifcatalog.AttachmentRef
	if runZip != nil {
		zipAtts = []notifcatalog.AttachmentRef{{
			S3Key:          runZip.S3Key,
			Filename:       runZip.OutputFilename,
			ContentType:    "application/zip",
			StorageBackend: runZip.StorageBackend,
		}}
	}

	emailDestinations, err := loadEmailDestinations(ctx, pool, versionID)
	if err != nil {
		return err
	}
	if len(emailDestinations) == 0 {
		emailDestinations = []emailDestinationConfig{{}} // legacy version
	}
	totalOutboxes := 0
	for _, destination := range emailDestinations {
		if err := validateEmailAttachmentPlan(runID, destination.PackageMode, docs, runZip); err != nil {
			return fmt.Errorf("email destination %s: %w", destination.DestinationID, err)
		}
		destinationAtts := atts
		if destination.PackageMode == "ZIP" {
			destinationAtts = zipAtts
		}
		outboxes, dispatchErr := dispatchOneEmailDestination(
			ctx, pool, runID, ruleID, ruleName, moduleCode, subModuleCode,
			versionID, destination.DestinationID, triggeredBy, emailSubject, emailBody,
			tplIDs, docs, destinationAtts,
		)
		if dispatchErr != nil {
			return dispatchErr
		}
		totalOutboxes += len(outboxes)
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
	logDispatchCostEstimate(runID, docs, len(emailDestinations), totalOutboxes, runZip)
	api.LogInfo("[DMS-DISPATCH] run=%s queued %d doc(s) to %d recipient outbox row(s)", runID, len(docs), totalOutboxes)

	// Delivery was deferred so cover + attachments are committed first.
	dinojobs.NudgeDeliveryAfterEnqueue(ctx, pool)
	return nil
}

type emailDestinationConfig struct {
	DestinationID string
	PackageMode   string
}

func loadEmailDestinations(ctx context.Context, pool *pgxpool.Pool, versionID string) ([]emailDestinationConfig, error) {
	rows, err := pool.Query(ctx, `
		SELECT destination_id::text, COALESCE(package_mode, 'FILES')
		FROM dms_svc.generation_rule_destination
		WHERE version_id = $1::uuid
		  AND destination_type = 'EMAIL'
		  AND is_enabled = true
		  AND is_deleted = false
		ORDER BY sort_order`, versionID)
	if err != nil {
		return nil, fmt.Errorf("load EMAIL destinations: %w", err)
	}
	defer rows.Close()
	var out []emailDestinationConfig
	for rows.Next() {
		var destination emailDestinationConfig
		if err := rows.Scan(&destination.DestinationID, &destination.PackageMode); err != nil {
			return nil, err
		}
		out = append(out, destination)
	}
	return out, rows.Err()
}

func dispatchOneEmailDestination(
	ctx context.Context,
	pool *pgxpool.Pool,
	runID, ruleID, ruleName, moduleCode, subModuleCode string,
	versionID, destinationID, triggeredBy string,
	emailSubject, emailBody *string,
	tplIDs []string,
	docs []generatedDoc,
	atts []notifcatalog.AttachmentRef,
) ([]outboxRecipient, error) {
	corr := "DMS-RUN-" + runID
	if destinationID != "" {
		shortID := destinationID
		if len(shortID) > 8 {
			shortID = shortID[:8]
		}
		corr += "-EMAIL-" + shortID
	}
	payload := map[string]interface{}{
		"RuleID":             ruleID,
		"RuleName":           ruleName,
		"RunID":              runID,
		"ModuleCode":         moduleCode,
		"SubModuleCode":      subModuleCode,
		"DestinationID":      destinationID,
		"DocCount":           len(docs),
		"UserID":             triggeredBy,
		"actor_user_id":      triggeredBy,
		"DeferDeliveryNudge": true,
	}
	if emailSubject != nil && strings.TrimSpace(*emailSubject) != "" {
		payload["EmailSubject"] = strings.TrimSpace(*emailSubject)
	}
	if emailBody != nil && strings.TrimSpace(*emailBody) != "" {
		payload["EmailBodyHTML"] = *emailBody
	}

	toEmails, ccEmails, recipErr := loadRuleEmailRecipients(ctx, pool, versionID, destinationID)
	if recipErr != nil {
		return nil, recipErr
	}
	if len(toEmails) > 0 {
		payload["RecipientEmails"] = toEmails
	} else if email := resolveActorEmail(ctx, pool, triggeredBy); email != "" {
		payload["RecipientEmail"] = email
		payload["RecipientName"] = triggeredBy
	}
	if len(ccEmails) > 0 {
		payload["RecipientCcEmails"] = ccEmails
	}
	enrichDispatchPayload(ctx, pool, runID, payload)

	outboxes, err := loadOutboxByCorrelation(ctx, pool, corr)
	if err != nil {
		return nil, fmt.Errorf("load outbox for correlation %s: %w", corr, err)
	}
	if len(outboxes) == 0 {
		notifcatalog.TriggerNotificationForTemplatesWithAttachments(
			ctx, pool, dmsDocumentGeneratedRoute, corr, payload, tplIDs, atts,
		)
		outboxes, err = loadOutboxByCorrelation(ctx, pool, corr)
		if err != nil {
			return nil, fmt.Errorf("load outbox for correlation %s: %w", corr, err)
		}
	}
	if len(outboxes) == 0 {
		return nil, fmt.Errorf("no outbox rows created for EMAIL destination %s", destinationID)
	}

	if len(ccEmails) > 0 {
		if _, err := pool.Exec(ctx, `
			UPDATE notification_svc.outbox
			SET cc_emails = $2
			WHERE correlation_id = $1
			  AND processing_status IN ('PENDING', 'QUEUED', 'PROCESSING')`,
			corr, strings.Join(ccEmails, ", ")); err != nil {
			return nil, fmt.Errorf("apply Cc recipients: %w", err)
		}
	}
	if emailSubject != nil && strings.TrimSpace(*emailSubject) != "" &&
		emailBody != nil && strings.TrimSpace(*emailBody) != "" {
		if _, err := pool.Exec(ctx, `
			UPDATE notification_svc.outbox
			SET rendered_subject = $2, rendered_body = $3
			WHERE correlation_id = $1
			  AND processing_status IN ('PENDING', 'QUEUED', 'PROCESSING')`,
			corr, strings.TrimSpace(*emailSubject), *emailBody); err != nil {
			return nil, fmt.Errorf("apply DMS email cover: %w", err)
		}
	}
	if len(atts) > 0 {
		count, err := countOutboxAttachments(ctx, pool, corr)
		if err != nil {
			return nil, fmt.Errorf("verify outbox attachments: %w", err)
		}
		if count == 0 {
			return nil, fmt.Errorf("no outbox attachments created for %s", corr)
		}
	}
	if err := insertDispatchRows(ctx, pool, docs, outboxes); err != nil {
		return nil, err
	}
	return outboxes, nil
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
	FileSize       int64
	StorageBackend string
}

type outboxRecipient struct {
	OutboxID  string
	Recipient string
}

func loadGeneratedDocs(ctx context.Context, pool *pgxpool.Pool, runID string) ([]generatedDoc, error) {
	rows, err := pool.Query(ctx, `
		SELECT doc_id::text, s3_key, file_format, COALESCE(checksum, ''), COALESCE(output_filename, ''),
		       COALESCE(file_size, 0), storage_backend
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
		if err := rows.Scan(&d.DocID, &d.S3Key, &d.FileFormat, &d.Checksum, &d.OutputFilename, &d.FileSize, &d.StorageBackend); err != nil {
			continue
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func loadRuleEmailRecipients(ctx context.Context, pool *pgxpool.Pool, versionID, destinationID string) (toEmails, ccEmails []string, err error) {
	// Match recipients for this EMAIL destination, plus version-level rows
	// (destination_id IS NULL) from seeds / older writes that never linked the FK.
	rows, err := pool.Query(ctx, `
		SELECT address_role, email
		FROM dms_svc.generation_rule_email_recipient
		WHERE version_id = $1::uuid
		  AND (
		    destination_id IS NULL
		    OR ($2 <> '' AND destination_id = $2::uuid)
		  )
		ORDER BY address_role, sort_order, email`, versionID, destinationID)
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
			COUNT(*) FILTER (WHERE is_enabled AND NOT is_deleted AND destination_type = 'EMAIL'),
			COUNT(*) FILTER (WHERE NOT is_deleted)
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

type destChannel struct {
	DestinationID   string
	DestinationType string
	PackageMode     string
	SftpHost        string
	SftpFolder      string
	APIURL          string
	TargetURI       string
}

func loadEnabledDestChannels(ctx context.Context, pool *pgxpool.Pool, versionID string) ([]destChannel, error) {
	rows, err := pool.Query(ctx, `
		SELECT destination_id::text, destination_type,
		       COALESCE(package_mode,'FILES'),
		       COALESCE(sftp_host,''), COALESCE(sftp_folder,''),
		       COALESCE(api_url,''), COALESCE(target_uri,'')
		FROM dms_svc.generation_rule_destination
		WHERE version_id = $1::uuid AND is_enabled = true AND is_deleted = false
		ORDER BY sort_order`, versionID)
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") {
			return nil, nil
		}
		return nil, err
	}
	defer rows.Close()
	var out []destChannel
	for rows.Next() {
		var d destChannel
		if err := rows.Scan(&d.DestinationID, &d.DestinationType, &d.PackageMode, &d.SftpHost, &d.SftpFolder, &d.APIURL, &d.TargetURI); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// recordChannelDeliveries writes one generated_document_delivery row per
// (doc × enabled destination). SFTP/WEBHOOK/SHAREPOINT stay PENDING until a
// delivery worker ships them; S3/IN_APP/EMAIL are SUCCESS at queue time.
func recordChannelDeliveries(
	ctx context.Context,
	pool *pgxpool.Pool,
	runID, versionID string,
	docs []generatedDoc,
	wantEmail bool,
	runZip *generatedPackage,
) error {
	channels, err := loadEnabledDestChannels(ctx, pool, versionID)
	if err != nil {
		return err
	}
	if len(channels) == 0 {
		// Legacy: treat as S3 + optional EMAIL.
		channels = []destChannel{{DestinationType: "S3_ARCHIVE"}}
		if wantEmail {
			channels = append(channels, destChannel{DestinationType: "EMAIL"})
		}
	}
	for _, doc := range docs {
		for _, ch := range channels {
			status := "SUCCESS"
			loc := doc.S3Key
			detail := ""
			switch strings.ToUpper(ch.DestinationType) {
			case "S3_ARCHIVE", "IN_APP":
				status = "SUCCESS"
				loc = doc.S3Key
			case "LOCAL":
				status = "SUCCESS"
				loc = doc.S3Key
				if strings.HasPrefix(doc.S3Key, "local:") {
					loc = strings.TrimPrefix(doc.S3Key, "local:")
				}
				detail = "stored under DMS local output folder"
			case "EMAIL":
				if !wantEmail {
					continue
				}
				status = "SUCCESS"
				loc = "outbox:" + runID
			case "SFTP":
				status = "PENDING"
				loc = strings.TrimSpace(ch.SftpHost)
				if ch.SftpFolder != "" {
					loc = loc + ":" + ch.SftpFolder
				}
				detail = "SFTP config stored — delivery worker not yet shipping files"
			case "WEBHOOK":
				status = "PENDING"
				loc = ch.APIURL
				detail = "WEBHOOK config stored — delivery worker not yet calling API"
			case "SHAREPOINT":
				status = "PENDING"
				loc = ch.TargetURI
				detail = "SHAREPOINT config stored — delivery worker not yet syncing"
			default:
				status = "SKIPPED"
				detail = "unknown destination type"
			}
			if ch.PackageMode == "ZIP" && runZip != nil {
				loc = runZip.S3Key
			}
			destID := strings.TrimSpace(ch.DestinationID)
			if _, err := pool.Exec(ctx, `
				INSERT INTO dms_svc.generated_document_delivery
					(doc_id, run_id, destination_id, destination_type, output_location, output_filename,
					 status, error_detail, finished_at)
				VALUES ($1::uuid, $2::uuid,
				        CASE WHEN $3 = '' THEN NULL ELSE $3::uuid END,
				        $4, $5, $6, $7::text, NULLIF($8,''),
				        CASE WHEN $7::text IN ('SUCCESS','FAILED','SKIPPED') THEN now() ELSE NULL END)`,
				doc.DocID, runID, destID, ch.DestinationType, loc, filenameForDoc(doc, ""),
				status, detail); err != nil {
				// Table may not exist on older DBs — don't fail the email path.
				if strings.Contains(err.Error(), "does not exist") {
					return nil
				}
				return err
			}
		}
	}
	return nil
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
