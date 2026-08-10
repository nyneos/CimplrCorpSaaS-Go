package dmsjobs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"CimplrCorpSaas/api"
	dashboardbuilder "CimplrCorpSaas/api/dash/dashboardBuilder"
	"CimplrCorpSaas/api/domaincatalog"
	notifcatalog "CimplrCorpSaas/api/notification/catalog"
	s3storage "CimplrCorpSaas/api/utils/s3storage"
	"CimplrCorpSaas/internal/services/docsvc"

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
	StoreS3            bool              // archive destination (default true)
	StoreLocal         bool              // also/or write under dms_local_output
	SendEmail          bool
	PackageMode        string // FILES | ZIP (email attachments)
	EmailTemplateID    string // DMS EMAIL-kind template for subject/body
	EmailTo            []string
	EmailCc            []string
	EmailSubject       string
	EmailBodyHTML      string
	// RequireApproval stages dispatch for maker-checker (default true when any destination set).
	// Docs are generated immediately; approval gates email send / DISPATCHED finalize.
	RequireApproval *bool
}

// AdhocResult is returned after preview or generate.
type AdhocResult struct {
	RunID            string
	RequestID        string // adhoc_dispatch_request when pending approval
	HTMLPreview      string
	DocIDs           []string
	Filenames        []string
	PendingApproval  bool
	ProcessingStatus string
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
	poolRows := []map[string]any{}
	if row != nil {
		poolRows = []map[string]any{row}
	}
	genCtx := attachmentGenCtx{
		AllowUnscopedBankAccount: true,
		PoolRows:                 poolRows,
	}
	out, err = expandDataTablePlaceholders(ctx, pool, out, row, genCtx, strings.ToUpper(format))
	if err != nil {
		return "", err
	}
	out, err = expandTxnTablePlaceholders(ctx, pool, out, row, genCtx, strings.ToUpper(format))
	if err != nil {
		return "", err
	}
	out, err = expandChartPlaceholders(ctx, pool, out, row, genCtx)
	if err != nil {
		return "", err
	}
	out = expandKPIPlaceholders(out, genCtx.PoolRows)
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
	if !req.StoreS3 && !req.StoreLocal && !req.SendEmail {
		req.StoreS3 = true
	}

	if q, qErr := docsvc.NewFromEnv().QuotaCheck(ctx); qErr != nil {
		return out, fmt.Errorf("document-service unavailable (generation blocked): %w", qErr)
	} else if !q.Allowed {
		code := q.ErrorCode
		if code == "" {
			code = "DMS_GENERATION_QUOTA_EXCEEDED"
		}
		return out, fmt.Errorf("%s: %s", code, q.Message)
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
		row, sourceKey, idField, err := fetchAdhocSourceRowDetailed(ctx, pool, req.ModuleCode, req.SubModuleCode, sid)
		if err != nil {
			api.LogError("[DMS-ADHOC] fetch source=%s: %v", sid, err)
			row = map[string]any{"source_id": sid}
		}
		if err := generateAdhocAttachment(ctx, pool, adhocAttachmentParams{
			RunID:      runID,
			TemplateID: tplID,
			Format:     format,
			Row:        row,
			Overrides:  req.MergeOverrides,
			SourceKey:  sourceKey,
			IDField:    idField,
			SourceID:   sid,
			StoreS3:    req.StoreS3,
			StoreLocal: req.StoreLocal,
			Out:        &out,
		}); err != nil {
			_ = finishRun(ctx, pool, runID, "FAILED", err.Error())
			return out, err
		}
	}

	_, _ = pool.Exec(ctx, `
		UPDATE dms_svc.generation_run SET source_row_count = $2 WHERE run_id = $1::uuid`,
		runID, len(sourceIDs))
	incrementDocSvcQuotaForRun(ctx, pool, runID, req.TriggeredBy)

	needApproval := true
	if req.RequireApproval != nil {
		needApproval = *req.RequireApproval
	}

	// Prepare email cover into generation_run even when pending — approve will dispatch.
	if req.SendEmail && len(out.DocIDs) > 0 {
		poolRows := make([]map[string]any, 0, len(sourceIDs))
		for _, sid := range sourceIDs {
			row, _ := FetchAdhocSourceRow(ctx, pool, req.ModuleCode, req.SubModuleCode, sid)
			if row == nil {
				row = map[string]any{}
			}
			for k, v := range req.MergeOverrides {
				row[k] = v
			}
			poolRows = append(poolRows, row)
		}
		firstRow := map[string]any{}
		if len(poolRows) > 0 {
			firstRow = poolRows[0]
		}
		emailTpl := strings.TrimSpace(req.EmailTemplateID)
		genCtx := attachmentGenCtx{
			AllowUnscopedBankAccount: true,
			PoolRows:                 poolRows,
		}
		if emailTpl != "" {
			if err := renderEmailCoverFromTemplate(ctx, pool, runID, emailTpl, firstRow, genCtx); err != nil {
				api.LogError("[DMS-ADHOC] email cover template=%s: %v", emailTpl, err)
				_ = finishRun(ctx, pool, runID, "FAILED", "email cover: "+err.Error())
				return out, fmt.Errorf("email cover: %w", err)
			}
		} else {
			subj := strings.TrimSpace(req.EmailSubject)
			body := strings.TrimSpace(req.EmailBodyHTML)
			if subj != "" || body != "" {
				body = expandKPIPlaceholders(body, poolRows)
				_, _ = pool.Exec(ctx, `
					UPDATE dms_svc.generation_run
					SET email_subject = NULLIF($2,''), email_body_html = NULLIF($3,'')
					WHERE run_id = $1::uuid`, runID, subj, body)
			}
		}
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
	}

	if needApproval {
		reqID, err := insertAdhocDispatchRequest(ctx, pool, runID, req)
		if err != nil {
			_ = finishRun(ctx, pool, runID, "FAILED", err.Error())
			return out, err
		}
		out.RequestID = reqID
		out.PendingApproval = true
		out.ProcessingStatus = "PENDING_APPROVAL"
		_ = finishRun(ctx, pool, runID, "PENDING_APPROVAL", "")
		return out, nil
	}

	if req.SendEmail && len(out.DocIDs) > 0 {
		if err := dispatchAdhocEmail(ctx, pool, runID, req); err != nil {
			api.LogError("[DMS-ADHOC] email dispatch run=%s: %v", runID, err)
			_ = finishRun(ctx, pool, runID, "PARTIAL", "docs saved; email: "+err.Error())
			return out, nil
		}
	}

	out.ProcessingStatus = "SUCCESS"
	_ = finishRun(ctx, pool, runID, "SUCCESS", "")
	return out, nil
}

func insertAdhocDispatchRequest(ctx context.Context, pool *pgxpool.Pool, runID string, req AdhocRequest) (string, error) {
	pkg := strings.ToUpper(strings.TrimSpace(req.PackageMode))
	if pkg != "ZIP" {
		pkg = "FILES"
	}
	// text[] NOT NULL — pgx encodes nil slices as NULL (bypasses DEFAULT '{}').
	emailTo := req.EmailTo
	if emailTo == nil {
		emailTo = []string{}
	}
	emailCc := req.EmailCc
	if emailCc == nil {
		emailCc = []string{}
	}
	var reqID string
	err := pool.QueryRow(ctx, `
		INSERT INTO dms_svc.adhoc_dispatch_request (
			run_id, module_code, sub_module_code, document_template_id, output_format,
			store_s3, store_local, send_email, package_mode, email_template_id,
			email_to, email_cc, email_subject, email_body_html,
			processing_status, requested_by
		) VALUES (
			$1::uuid, $2, $3, NULLIF($4,'')::uuid, $5,
			$6, $7, $8, $9, NULLIF($10,'')::uuid,
			$11, $12, COALESCE($13, ''), COALESCE($14, ''),
			'PENDING_APPROVAL', $15
		) RETURNING request_id::text`,
		runID, req.ModuleCode, req.SubModuleCode, strings.TrimSpace(req.DocumentTemplateID), strings.ToUpper(req.OutputFormat),
		req.StoreS3, req.StoreLocal, req.SendEmail, pkg, strings.TrimSpace(req.EmailTemplateID),
		emailTo, emailCc, req.EmailSubject, req.EmailBodyHTML,
		strings.TrimSpace(req.TriggeredBy),
	).Scan(&reqID)
	if err != nil {
		return "", fmt.Errorf("insert adhoc_dispatch_request: %w", err)
	}
	_, _ = pool.Exec(ctx, `
		INSERT INTO dms_svc.adhoc_dispatch_request_audit (
			request_id, run_id, action_type, processing_status, reason, requested_by,
			new_processing_status, new_send_email, new_store_s3, new_store_local,
			new_output_format, new_package_mode, new_module_code, new_sub_module_code
		) VALUES ($1::uuid, $2::uuid, 'CREATE', 'PENDING_APPROVAL', 'Adhoc generate staged for dispatch approval', $3,
			'PENDING_APPROVAL', $4, $5, $6, $7, $8, $9, $10)`,
		reqID, runID, req.TriggeredBy, req.SendEmail, req.StoreS3, req.StoreLocal,
		strings.ToUpper(req.OutputFormat), pkg, req.ModuleCode, req.SubModuleCode)
	return reqID, nil
}

// adhocAttachmentParams is one adhoc attachment to render and store: the run it
// belongs to, the template/format, the source row it merges (plus the dashboard
// source key / id field used to expand tables and charts), and where to store it.
// Out accumulates the doc ids, filenames and first HTML preview across sources.
type adhocAttachmentParams struct {
	RunID      string
	TemplateID string
	Format     string
	Row        map[string]any
	Overrides  map[string]string
	SourceKey  string
	IDField    string
	SourceID   string
	StoreS3    bool
	StoreLocal bool
	Out        *AdhocResult
}

func generateAdhocAttachment(ctx context.Context, pool *pgxpool.Pool, p adhocAttachmentParams) error {
	runID, tplID, format := p.RunID, p.TemplateID, p.Format
	row, overrides := p.Row, p.Overrides
	sourceKey, idField, sourceID := p.SourceKey, p.IDField, p.SourceID
	storeS3, storeLocal := p.StoreS3, p.StoreLocal
	out := p.Out

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
	poolRows := []map[string]any{}
	if row != nil {
		poolRows = []map[string]any{row}
	}
	var filters []dashboardbuilder.WidgetFilterRule
	if strings.TrimSpace(idField) != "" && strings.TrimSpace(sourceID) != "" {
		filters = append(filters, dashboardbuilder.WidgetFilterRule{
			Field: strings.TrimSpace(idField), Type: "id", Op: "in",
			Value: sourceID, Conjunction: "AND",
		})
	}
	genCtx := attachmentGenCtx{
		AllowUnscopedBankAccount: true,
		PoolRows:                 poolRows,
		SourceKey:                sourceKey,
		Filters:                  filters,
	}
	renderedHTML, err = expandDataTablePlaceholders(ctx, pool, renderedHTML, row, genCtx, format)
	if err != nil {
		return err
	}
	renderedHTML, err = expandTxnTablePlaceholders(ctx, pool, renderedHTML, row, genCtx, format)
	if err != nil {
		return err
	}
	renderedHTML, err = expandChartPlaceholders(ctx, pool, renderedHTML, row, genCtx)
	if err != nil {
		return err
	}
	renderedHTML = expandKPIPlaceholders(renderedHTML, genCtx.PoolRows)
	if out.HTMLPreview == "" {
		out.HTMLPreview = wrapHTMLDocument(renderedHTML)
	}

	file, err := renderAndStoreViaDocSvc(ctx, format, renderedHTML, values, content.SheetTokens, content.Kind, content.PageDesign)
	if err != nil {
		return fmt.Errorf("render %s: %w", format, err)
	}
	sum := sha256.Sum256(file.Bytes)
	checksum := hex.EncodeToString(sum[:])
	tplName, _ := loadTemplateName(ctx, pool, tplID)
	outName := buildDmsOutputFilename(outputNaming{Prefix: "Adhoc_" + sanitizeOutputName(tplName), AppendDatetime: true}, tplName, file.Ext, time.Now(), false, "")

	if !storeS3 && !storeLocal {
		storeS3 = true
	}

	var localPath string
	if storeLocal {
		rel, _, err := WriteLocalDmsFile(runID, outName, file.Bytes)
		if err != nil {
			return fmt.Errorf("local store: %w", err)
		}
		localPath = rel
	}

	s3Key := ""
	storageBackend := "MAIN_S3"
	if storeS3 {
		if file.StoredKey != "" {
			s3Key = file.StoredKey
			storageBackend = "DOCSVC_S3"
		} else {
			s3Key = s3storage.BuildModuleS3Key("dms", "generated", checksum, file.Ext)
			if err := s3storage.PutObjectToS3(ctx, s3Key, file.Bytes, file.ContentType); err != nil {
				return err
			}
		}
	} else {
		s3Key = LocalStorageKey(localPath)
	}

	var docID string
	err = pool.QueryRow(ctx, `
		INSERT INTO dms_svc.generated_document
			(run_id, document_template_id, template_version_id, s3_key, file_format, file_size, checksum, status, output_filename, local_path, storage_backend)
		VALUES ($1::uuid, $2::uuid, $3::uuid, $4, $5, $6, $7, 'GENERATED', $8, $9, $10)
		RETURNING doc_id::text`,
		runID, tplID, tplVersionID, s3Key, file.Format, len(file.Bytes), checksum, outName, localPath, storageBackend,
	).Scan(&docID)
	if err != nil {
		return err
	}
	out.DocIDs = append(out.DocIDs, docID)
	out.Filenames = append(out.Filenames, outName)
	return nil
}

// adhocSourceIDFieldBySub maps sub-module → primary id column used by dashboard
// filters (same vocabulary as seeded generation_rule_trigger.source_id_field).
// Lookup table rather than a long switch — same shape as businessAuditBySub.
var adhocSourceIDFieldBySub = map[string]string{
	// CASH
	"BANK_STATEMENT":      "bank_statement_id",
	"BANK_BALANCE":        "balance_id",
	"BANK_LIMIT":          "limit_id",
	"CASHFLOW_PROJECTION": "proposal_id",
	"FUND_PLANNING":       "plan_id",
	"LIMIT_UTILIZATION":   "utilization_id",
	"PAYABLE_RECEIVABLE":  "transaction_id",
	"SWEEP_CONFIG":        "sweep_id",
	"SWEEP_INITIATION":    "initiation_id",
	"SWEEP_EXECUTION":     "initiation_id",
	// FD
	"FD_BOOKING":       "booking_id",
	"FD_CONFIRMATION":  "confirmation_id",
	"FD_MASTER":        "fd_id",
	"FD_ACCRUAL":       "run_id",
	"FD_ACCRUAL_SCHED": "config_id",
	"FD_CASHFLOW":      "cashflow_id",
	"FD_CLOSURE":       "closure_initiate_id",
	"FD_EXCEPTION":     "exception_id",
	"FD_RECEIPT":       "receipt_id",
	"FD_TDS_REGISTER":  "tds_id",
	// FX
	"EXPOSURE_CREATION":    "exposure_header_id",
	"EXPOSURE_UPLOAD":      "exposure_header_id",
	"EXPOSURE_BUCKETING":   "exposure_header_id",
	"HEDGE_LINK":           "exposure_header_id",
	"FORWARD_BOOKING":      "system_transaction_id",
	"FORWARD_CANCELLATION": "booking_id",
	"FORWARD_ROLLOVER":     "booking_id",
	"FORWARD_CANCEL_ROLL":  "booking_id",
	"FORWARD_MTM":          "mtm_id",
	// MF
	"MF_ACCOUNTING":      "activity_id",
	"MF_CONFIRMATION":    "confirmation_id",
	"MF_REDEMPTION_CONF": "redemption_confirm_id",
	"MF_INITIATION":      "initiation_id",
	"MF_ONBOARD":         "batch_id",
	"MF_PORTFOLIO":       "proposal_id",
	"MF_PROPOSAL":        "proposal_id",
	"MF_REDEMPTION":      "redemption_id",
}

// adhocSourceIDField returns the primary id column for a sub-module, falling
// back to the generic "source_id" for anything not in the table above.
func adhocSourceIDField(subModuleCode string) string {
	if field, ok := adhocSourceIDFieldBySub[strings.TrimSpace(subModuleCode)]; ok {
		return field
	}
	return "source_id"
}

// FetchAdhocSourceRow loads one source row for preview/adhoc merge (exported for API).
func FetchAdhocSourceRow(ctx context.Context, pool *pgxpool.Pool, moduleCode, subModuleCode, sourceID string) (map[string]any, error) {
	row, _, _, err := fetchAdhocSourceRowDetailed(ctx, pool, moduleCode, subModuleCode, sourceID)
	return row, err
}

func fetchAdhocSourceRowDetailed(
	ctx context.Context,
	pool *pgxpool.Pool,
	moduleCode, subModuleCode, sourceID string,
) (row map[string]any, sourceKey, idField string, err error) {
	_ = moduleCode
	sourceID = strings.TrimSpace(sourceID)
	idField = adhocSourceIDField(subModuleCode)
	if sourceID == "" {
		return map[string]any{}, "", idField, nil
	}

	sourceKey, aliasErr := domaincatalog.ResolveSubModuleAlias(ctx, pool, subModuleCode, "DASHBOARD")
	if aliasErr != nil {
		api.LogError("[DMS-ADHOC] dashboard alias sub=%s: %v", subModuleCode, aliasErr)
		// Direct table fallback for FD booking (dashboard alias missing).
		if row, ok := fetchAdhocFDBookingDirect(ctx, pool, sourceID); ok {
			return row, "", idField, nil
		}
		return map[string]any{"source_id": sourceID, idField: sourceID}, "", idField, nil
	}

	filters := []dashboardbuilder.WidgetFilterRule{{
		Field: idField, Type: "id", Op: "in", Value: sourceID, Conjunction: "AND",
	}}
	rows, _, fetchErr := dashboardbuilder.FetchSourceData(ctx, pool, dashboardbuilder.DataRequest{
		Source:                   sourceKey,
		Filters:                  filters,
		Limit:                    1,
		AllowUnscopedBankAccount: true,
	})
	if fetchErr != nil {
		if row, ok := fetchAdhocFDBookingDirect(ctx, pool, sourceID); ok {
			return row, sourceKey, idField, nil
		}
		return nil, sourceKey, idField, fmt.Errorf("fetch %s row %s: %w", sourceKey, sourceID, fetchErr)
	}
	if len(rows) == 0 {
		if row, ok := fetchAdhocFDBookingDirect(ctx, pool, sourceID); ok {
			return row, sourceKey, idField, nil
		}
		api.LogError("[DMS-ADHOC] no row for source=%s field=%s key=%s", sourceID, idField, sourceKey)
		return map[string]any{"source_id": sourceID, idField: sourceID}, sourceKey, idField, nil
	}
	row = rows[0]
	if _, ok := row["source_id"]; !ok {
		row["source_id"] = sourceID
	}
	if _, ok := row[idField]; !ok {
		row[idField] = sourceID
	}
	return row, sourceKey, idField, nil
}

// fetchAdhocFDBookingDirect loads one FD booking when dashboard source is empty/unavailable.
func fetchAdhocFDBookingDirect(ctx context.Context, pool *pgxpool.Pool, bookingID string) (map[string]any, bool) {
	bookingID = strings.TrimSpace(bookingID)
	if bookingID == "" || pool == nil {
		return nil, false
	}
	rows, err := pool.Query(ctx, `
		SELECT
			COALESCE(br.booking_id::text, '') AS booking_id,
			COALESCE(br.entity_id::text, '') AS entity_id,
			COALESCE(br.entity_name, '') AS entity_name,
			COALESCE(br.bank_name, '') AS bank_name,
			COALESCE(br.tenor_type, '') AS tenor_type,
			COALESCE(br.interest_type_code, '') AS interest_type_code,
			COALESCE(br.booking_status, '') AS booking_status,
			COALESCE(br.principal_amount, 0) AS principal_amount,
			COALESCE(br.interest_rate, 0) AS interest_rate,
			COALESCE(br.tenure_days, 0) AS tenure_days,
			COALESCE(br.tenure_months, 0) AS tenure_months,
			COALESCE(br.tenure_years, 0) AS tenure_years,
			br.value_date,
			br.expected_maturity_date,
			br.expected_start_date
		FROM investment.fd_booking_request br
		WHERE br.booking_id = $1 AND COALESCE(br.is_deleted, false) = false
		LIMIT 1`, bookingID)
	if err != nil {
		return nil, false
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, false
	}
	fds := rows.FieldDescriptions()
	vals, err := rows.Values()
	if err != nil {
		return nil, false
	}
	out := make(map[string]any, len(fds)+1)
	for i, fd := range fds {
		out[string(fd.Name)] = vals[i]
	}
	out["source_id"] = bookingID
	return out, true
}

func dispatchAdhocEmail(ctx context.Context, pool *pgxpool.Pool, runID string, req AdhocRequest) error {
	return DispatchAdhocWithRecipients(ctx, pool, runID, req)
}

// renderEmailCoverFromTemplate merges an EMAIL-kind DMS template into the run's
// email_subject / email_body_html (full merge/expand path, same as rule covers).
func renderEmailCoverFromTemplate(ctx context.Context, pool *pgxpool.Pool, runID, templateID string, row map[string]any, genCtx attachmentGenCtx) error {
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
	subject := strings.TrimSpace(content.EmailMeta.Subject)
	if subject == "" {
		subject = "Document attached"
	}
	body, values, err := mergeTemplateHTML(ctx, pool, content.HTML, mergeFields, row, genCtx, "HTML")
	if err != nil {
		return fmt.Errorf("merge EMAIL template %s: %w", templateID, err)
	}
	subject = substituteMustache(subject, values)
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
	enrichDispatchPayload(ctx, pool, runID, payload)
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
