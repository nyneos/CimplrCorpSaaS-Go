package dmsjobs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"CimplrCorpSaas/api"
	s3storage "CimplrCorpSaas/api/utils/s3storage"
	notifcatalog "CimplrCorpSaas/api/notification/catalog"

	"github.com/jackc/pgx/v5/pgxpool"
)

// AdhocRequest generates documents from selected source rows + a template,
// without a generation_rule (trigger_type=ADHOC).
type AdhocRequest struct {
	ModuleCode         string
	SubModuleCode      string
	DocumentTemplateID string
	OutputFormat       string // PDF | DOCX | XLSX | HTML
	SourceIDs          []string
	TriggeredBy        string
	MergeOverrides     map[string]string // optional preview/sample values layered on row
	SendEmail          bool
	EmailTemplateID    string // DMS EMAIL-kind template for subject/body
	EmailTo            []string
	EmailCc            []string
	EmailSubject       string
	EmailBodyHTML      string
}

// AdhocResult is returned after preview or generate.
type AdhocResult struct {
	RunID       string
	HTMLPreview string
	DocIDs      []string
	Filenames   []string
}

// PreviewInput drives PreviewMergedOrDraft — either an approved template id
// or raw draft HTML from the template editor.
type PreviewInput struct {
	DocumentTemplateID string
	HTML               string
	MergeFields        map[string]string // field_key → field_code (draft)
	Row                map[string]any
	Overrides          map[string]string
	Format             string
}

// PreviewMergedOrDraft substitutes merge fields (+ optional txn tables) and returns HTML.
// Does not write to DB/S3. Prefer DocumentTemplateID when set; otherwise use HTML draft.
func PreviewMergedOrDraft(ctx context.Context, pool *pgxpool.Pool, in PreviewInput) (string, error) {
	format := strings.ToUpper(strings.TrimSpace(in.Format))
	if format == "" {
		format = "HTML"
	}
	html := strings.TrimSpace(in.HTML)
	mergeFields := in.MergeFields
	if tplID := strings.TrimSpace(in.DocumentTemplateID); tplID != "" && html == "" {
		tplVersionID, content, err := loadApprovedTemplateContent(ctx, pool, tplID)
		if err != nil {
			return "", err
		}
		html = content.HTML
		loaded, err := loadMergeFields(ctx, pool, tplVersionID)
		if err != nil {
			return "", err
		}
		mergeFields = loaded
	}
	if html == "" {
		return "", fmt.Errorf("html or document_template_id required")
	}
	if mergeFields == nil {
		mergeFields = map[string]string{}
	}
	return PreviewMergedHTML(ctx, pool, html, mergeFields, in.Row, in.Overrides, format)
}

// PreviewMergedHTML substitutes merge fields (+ optional txn tables) and returns HTML.
// Does not write to DB/S3. row may be nil (uses overrides only / sample placeholders).
func PreviewMergedHTML(ctx context.Context, pool *pgxpool.Pool, html string, mergeFields map[string]string, row map[string]any, overrides map[string]string, format string) (string, error) {
	values := buildMergeValues(html, mergeFields, row, overrides)
	out := substituteMergeFields(html, values)
	var err error
	out, err = expandTxnTablePlaceholders(ctx, pool, out, row, attachmentGenCtx{
		AllowUnscopedBankAccount: true,
	}, strings.ToUpper(format))
	if err != nil {
		return "", err
	}
	return wrapHTMLDocument(out), nil
}

func buildMergeValues(html string, mergeFields map[string]string, row map[string]any, overrides map[string]string) map[string]string {
	values := make(map[string]string, len(mergeFields)+8)
	for fieldKey, fieldCode := range mergeFields {
		var raw any
		if row != nil {
			raw = row[fieldCode]
		}
		values[fieldKey] = formatFieldValue(raw)
	}
	for _, key := range extractDmsFieldKeys(html) {
		if _, ok := values[key]; ok {
			continue
		}
		if row != nil {
			values[key] = formatFieldValue(row[key])
		}
	}
	for k, v := range overrides {
		if strings.TrimSpace(k) == "" {
			continue
		}
		values[k] = v
	}
	// Unresolved keys → keep readable sample so Preview isn't blank.
	for k, v := range values {
		if strings.TrimSpace(v) == "" {
			values[k] = "[" + k + "]"
		}
	}
	return values
}

// RunAdhocGeneration creates an ADHOC run, renders one doc per source id (or one
// doc if SourceIDs empty using overrides only), uploads to S3, optionally emails.
func RunAdhocGeneration(ctx context.Context, pool *pgxpool.Pool, req AdhocRequest) (AdhocResult, error) {
	var out AdhocResult
	if pool == nil {
		return out, fmt.Errorf("pool required")
	}
	tplID := strings.TrimSpace(req.DocumentTemplateID)
	if tplID == "" {
		return out, fmt.Errorf("document_template_id required")
	}
	format := strings.ToUpper(strings.TrimSpace(req.OutputFormat))
	if format == "" {
		format = "PDF"
	}

	sourceIDs := req.SourceIDs
	if len(sourceIDs) == 0 {
		sourceIDs = []string{""} // single pass with overrides / empty row
	}

	var runID string
	err := pool.QueryRow(ctx, `
		INSERT INTO dms_svc.generation_run (rule_id, version_id, trigger_type, triggered_by, status)
		VALUES (NULL, NULL, 'ADHOC', $1, 'RUNNING')
		RETURNING run_id::text`, strings.TrimSpace(req.TriggeredBy),
	).Scan(&runID)
	if err != nil {
		return out, fmt.Errorf("insert adhoc run: %w", err)
	}
	out.RunID = runID

	for i, sid := range sourceIDs {
		if sid != "" {
			_, _ = pool.Exec(ctx, `
				INSERT INTO dms_svc.generation_run_source_row (run_id, source_id, sort_order)
				VALUES ($1::uuid, $2, $3)
				ON CONFLICT (run_id, source_id) DO NOTHING`, runID, sid, i)
		}
		row, err := fetchAdhocSourceRow(ctx, pool, req.ModuleCode, req.SubModuleCode, sid)
		if err != nil {
			api.LogError("[DMS-ADHOC] fetch source=%s: %v", sid, err)
			row = map[string]any{}
		}
		if err := generateAdhocAttachment(ctx, pool, runID, tplID, format, row, req.MergeOverrides, &out); err != nil {
			_ = finishRun(ctx, pool, runID, "FAILED", err.Error())
			return out, err
		}
	}

	if req.SendEmail && len(out.DocIDs) > 0 {
		// Prefer explicit EMAIL cover template; fall back to subject/body overrides.
		firstRow, _ := fetchAdhocSourceRow(ctx, pool, req.ModuleCode, req.SubModuleCode, sourceIDs[0])
		if firstRow == nil {
			firstRow = map[string]any{}
		}
		for k, v := range req.MergeOverrides {
			firstRow[k] = v
		}
		emailTpl := strings.TrimSpace(req.EmailTemplateID)
		if emailTpl != "" {
			if err := renderEmailCoverFromTemplate(ctx, pool, runID, emailTpl, firstRow); err != nil {
				api.LogError("[DMS-ADHOC] email cover template=%s: %v", emailTpl, err)
				_ = finishRun(ctx, pool, runID, "PARTIAL", "docs saved; email cover: "+err.Error())
				return out, nil
			}
		} else {
			subj := strings.TrimSpace(req.EmailSubject)
			body := strings.TrimSpace(req.EmailBodyHTML)
			if subj != "" || body != "" {
				_, _ = pool.Exec(ctx, `
					UPDATE dms_svc.generation_run
					SET email_subject = NULLIF($2,''), email_body_html = NULLIF($3,'')
					WHERE run_id = $1::uuid`, runID, subj, body)
			}
		}
		// Load rendered subject/body onto req for dispatch payload overrides.
		var storedSubj, storedBody string
		_ = pool.QueryRow(ctx, `
			SELECT COALESCE(email_subject,''), COALESCE(email_body_html,'')
			FROM dms_svc.generation_run WHERE run_id = $1::uuid`, runID,
		).Scan(&storedSubj, &storedBody)
		if storedSubj != "" {
			req.EmailSubject = storedSubj
		}
		if storedBody != "" {
			req.EmailBodyHTML = storedBody
		}
		if err := dispatchAdhocEmail(ctx, pool, runID, req); err != nil {
			api.LogError("[DMS-ADHOC] email dispatch run=%s: %v", runID, err)
			_ = finishRun(ctx, pool, runID, "PARTIAL", "docs saved; email: "+err.Error())
			return out, nil
		}
	}

	_ = finishRun(ctx, pool, runID, "SUCCESS", "")
	return out, nil
}

func generateAdhocAttachment(ctx context.Context, pool *pgxpool.Pool, runID, tplID, format string, row map[string]any, overrides map[string]string, out *AdhocResult) error {
	tplVersionID, content, err := loadApprovedTemplateContent(ctx, pool, tplID)
	if err != nil {
		return err
	}
	mergeFields, err := loadMergeFields(ctx, pool, tplVersionID)
	if err != nil {
		return err
	}
	values := buildMergeValues(content.HTML, mergeFields, row, overrides)
	renderedHTML := substituteMergeFields(content.HTML, values)
	renderedHTML, err = expandTxnTablePlaceholders(ctx, pool, renderedHTML, row, attachmentGenCtx{
		AllowUnscopedBankAccount: true,
	}, format)
	if err != nil {
		return err
	}
	if out.HTMLPreview == "" {
		out.HTMLPreview = wrapHTMLDocument(renderedHTML)
	}

	file, err := renderMergedOutput(format, renderedHTML, values, content.SheetTokens, content.Kind)
	if err != nil {
		return fmt.Errorf("render %s: %w", format, err)
	}
	sum := sha256.Sum256(file.Bytes)
	checksum := hex.EncodeToString(sum[:])
	s3Key := s3storage.BuildModuleS3Key("dms", "generated", checksum, file.Ext)
	if err := s3storage.PutObjectToS3(ctx, s3Key, file.Bytes, file.ContentType); err != nil {
		return err
	}
	tplName, _ := loadTemplateName(ctx, pool, tplID)
	outName := buildDmsOutputFilename(outputNaming{Prefix: "Adhoc_" + sanitizeOutputName(tplName), AppendDatetime: true}, tplName, file.Ext, time.Now(), false)

	var docID string
	err = pool.QueryRow(ctx, `
		INSERT INTO dms_svc.generated_document
			(run_id, document_template_id, template_version_id, s3_key, file_format, file_size, checksum, status, output_filename)
		VALUES ($1::uuid, $2::uuid, $3::uuid, $4, $5, $6, $7, 'GENERATED', $8)
		RETURNING doc_id::text`,
		runID, tplID, tplVersionID, s3Key, file.Format, len(file.Bytes), checksum, outName,
	).Scan(&docID)
	if err != nil {
		return err
	}
	out.DocIDs = append(out.DocIDs, docID)
	out.Filenames = append(out.Filenames, outName)
	return nil
}

// FetchAdhocSourceRow loads one source row for preview/adhoc merge (exported for API).
func FetchAdhocSourceRow(ctx context.Context, pool *pgxpool.Pool, moduleCode, subModuleCode, sourceID string) (map[string]any, error) {
	return fetchAdhocSourceRow(ctx, pool, moduleCode, subModuleCode, sourceID)
}

func fetchAdhocSourceRow(ctx context.Context, pool *pgxpool.Pool, moduleCode, subModuleCode, sourceID string) (map[string]any, error) {
	sourceID = strings.TrimSpace(sourceID)
	if sourceID == "" {
		return map[string]any{}, nil
	}
	if moduleCode == "CASH" && subModuleCode == "BANK_STATEMENT" {
		rows, err := pool.Query(ctx, `
			SELECT
				COALESCE(s.bank_statement_id::text, '') AS bank_statement_id,
				COALESCE(s.bank_statement_id::text, '') AS statement_id,
				COALESCE(s.entity_id, '') AS entity_id,
				COALESCE(e.entity_name, '') AS entity_name,
				COALESCE(s.account_number, '') AS account_number,
				COALESCE(mb.bank_name, '') AS bank_name,
				COALESCE(mba.account_nickname, '') AS account_nickname,
				s.statement_period_start,
				s.statement_period_end,
				s.opening_balance,
				s.closing_balance,
				s.uploaded_at,
				COALESCE((
					SELECT COUNT(*)::int FROM cimplrcorpsaas.bank_statement_transactions t
					WHERE t.bank_statement_id = s.bank_statement_id
				), 0) AS total_transactions
			FROM cimplrcorpsaas.bank_statements s
			LEFT JOIN public.masterentitycash e ON s.entity_id = e.entity_id
			LEFT JOIN public.masterbankaccount mba
				ON mba.account_number = s.account_number AND COALESCE(mba.is_deleted, false) = false
			LEFT JOIN public.masterbank mb
				ON mb.bank_id = mba.bank_id AND COALESCE(mb.is_deleted, false) = false
			WHERE s.bank_statement_id::text = $1
			  AND COALESCE(s.is_deleted, false) = false
			LIMIT 1`, sourceID)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		fds := rows.FieldDescriptions()
		if !rows.Next() {
			return map[string]any{"bank_statement_id": sourceID}, nil
		}
		vals, err := rows.Values()
		if err != nil {
			return nil, err
		}
		row := make(map[string]any, len(fds))
		for i, fd := range fds {
			row[string(fd.Name)] = vals[i]
		}
		return row, nil
	}
	return map[string]any{"source_id": sourceID}, nil
}

func dispatchAdhocEmail(ctx context.Context, pool *pgxpool.Pool, runID string, req AdhocRequest) error {
	return DispatchAdhocWithRecipients(ctx, pool, runID, req)
}

// renderEmailCoverFromTemplate merges an EMAIL-kind DMS template into the run's
// email_subject / email_body_html (same path as rule email covers).
func renderEmailCoverFromTemplate(ctx context.Context, pool *pgxpool.Pool, runID, templateID string, row map[string]any) error {
	verID, content, err := loadApprovedTemplateContent(ctx, pool, templateID)
	if err != nil {
		return err
	}
	if !strings.EqualFold(content.Kind, "EMAIL") {
		return fmt.Errorf("template %s kind=%q is not EMAIL", templateID, content.Kind)
	}
	mergeFields, err := loadMergeFields(ctx, pool, verID)
	if err != nil {
		return err
	}
	values := buildMergeValues(content.HTML, mergeFields, row, nil)
	// Also expose raw row keys for subject tokens.
	if row != nil {
		for k, raw := range row {
			if _, exists := values[k]; !exists {
				values[k] = formatFieldValue(raw)
			}
		}
	}
	subject := strings.TrimSpace(content.EmailMeta.Subject)
	if subject == "" {
		subject = "Document attached"
	}
	subject = substituteMustache(subject, values)
	body := substituteMergeFields(content.HTML, values)
	if strings.TrimSpace(body) == "" {
		return fmt.Errorf("EMAIL template %s produced empty body", templateID)
	}
	_, err = pool.Exec(ctx, `
		UPDATE dms_svc.generation_run
		   SET email_subject = $2, email_body_html = $3
		 WHERE run_id = $1::uuid`, runID, subject, body)
	return err
}

// DispatchAdhocWithRecipients queues the DMS document-generated notification with
// explicit To/Cc and attaches all GENERATED docs from the ADHOC run.
func DispatchAdhocWithRecipients(ctx context.Context, pool *pgxpool.Pool, runID string, req AdhocRequest) error {
	docs, err := loadGeneratedDocs(ctx, pool, runID)
	if err != nil {
		return err
	}
	if len(docs) == 0 {
		return fmt.Errorf("no generated docs for run %s", runID)
	}
	atts := make([]notifcatalog.AttachmentRef, 0, len(docs))
	for _, d := range docs {
		atts = append(atts, notifcatalog.AttachmentRef{
			S3Key:       d.S3Key,
			Filename:    filenameForDoc(d, "Adhoc"),
			ContentType: contentTypeForFormat(d.FileFormat),
		})
	}
	corr := "DMS-ADHOC-" + runID
	payload := map[string]interface{}{
		"RunID":              runID,
		"ModuleCode":         req.ModuleCode,
		"SubModuleCode":      req.SubModuleCode,
		"DocCount":           len(docs),
		"UserID":             req.TriggeredBy,
		"actor_user_id":      req.TriggeredBy,
		"RecipientEmails":    req.EmailTo,
		"DeferDeliveryNudge": true,
	}
	if len(req.EmailCc) > 0 {
		payload["RecipientCcEmails"] = req.EmailCc
	}
	if strings.TrimSpace(req.EmailSubject) != "" {
		payload["EmailSubject"] = strings.TrimSpace(req.EmailSubject)
	}
	if strings.TrimSpace(req.EmailBodyHTML) != "" {
		payload["EmailBodyHTML"] = req.EmailBodyHTML
	}
	notifcatalog.TriggerNotificationForTemplatesWithAttachments(
		ctx, pool, dmsDocumentGeneratedRoute, corr, payload, nil, atts,
	)
	outboxes, err := loadOutboxByCorrelation(ctx, pool, corr)
	if err != nil {
		return err
	}
	if len(outboxes) == 0 {
		return fmt.Errorf("no outbox for adhoc correlation %s", corr)
	}
	if len(req.EmailCc) > 0 {
		_, _ = pool.Exec(ctx, `
			UPDATE notification_svc.outbox SET cc_emails = $2
			WHERE correlation_id = $1 AND processing_status IN ('PENDING','QUEUED','PROCESSING')`,
			corr, strings.Join(req.EmailCc, ", "))
	}
	if strings.TrimSpace(req.EmailSubject) != "" && strings.TrimSpace(req.EmailBodyHTML) != "" {
		_, _ = pool.Exec(ctx, `
			UPDATE notification_svc.outbox
			SET rendered_subject = $2, rendered_body = $3
			WHERE correlation_id = $1 AND processing_status IN ('PENDING','QUEUED','PROCESSING')`,
			corr, strings.TrimSpace(req.EmailSubject), req.EmailBodyHTML)
	}
	if err := insertDispatchRows(ctx, pool, docs, outboxes); err != nil {
		return err
	}
	docIDs := make([]string, 0, len(docs))
	for _, d := range docs {
		docIDs = append(docIDs, d.DocID)
	}
	_, err = pool.Exec(ctx, `
		UPDATE dms_svc.generated_document SET status = 'DISPATCHED'
		WHERE doc_id = ANY($1::uuid[]) AND status = 'GENERATED'`, docIDs)
	return err
}
