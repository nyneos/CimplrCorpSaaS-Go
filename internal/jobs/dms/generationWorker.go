// Package dmsjobs is the DMS (document generation) execution worker — thin,
// in the same shape as internal/jobs/email: read rule state from the DB, call
// the module's own handlers/functions (dashboardbuilder.FetchSourceData for
// data, the merge-field substitution below for rendering), write results back.
//
// Output formats:
//   - HTML — full HTML document uploaded to S3
//   - PDF  — pure-Go (gofpdf + embedded DejaVu) from HTML paragraphs
//   - DOCX — OOXML zip from HTML paragraphs
//   - XLSX — excelize workbook (merge fields + body, or sheetTokens for SPREADSHEET)
//
// Merge-field substitution is scalar against the FIRST row (FIRST_ROW), each
// row separately (PER_ROW), or each row as a page in one file (PER_PAGE_IN_ONE_DOC).
// Templates may also include:
//
//	`<div data-dms-table data-source="pool" data-columns="…" data-labels="…">`
//	for a simple header+data table of filtered rows (any dashboard source), and
//	legacy `<div data-dms-txn-table="cashBankStatementTransactions">` for bank
//	statement child lines.
package dmsjobs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	htmlpkg "html"
	"regexp"
	"strconv"
	"strings"
	"time"

	"CimplrCorpSaas/api"
	dashboardbuilder "CimplrCorpSaas/api/dash/dashboardBuilder"
	dmscommon "CimplrCorpSaas/api/dms/common"
	"CimplrCorpSaas/api/domaincatalog"
	s3storage "CimplrCorpSaas/api/utils/s3storage"
	"CimplrCorpSaas/internal/services/docsvc"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

type ruleHeader struct {
	RuleID           string
	Name             string
	ModuleCode       string
	SubModuleCode    string
	EntityID         string
	EntityName       string
	Status           string
	ProcessingStatus string
	CurrentVersionID string
}

type ruleVersionConfig struct {
	VersionID       string
	TimeWindowType  string
	TimeWindowValue *int
	TimeWindowUnit  *string
	CustomStart     *string
	CustomEnd       *string
	RowExpandMode   string // FIRST_ROW | PER_ROW | PER_PAGE_IN_ONE_DOC
	DataRowFrom     int    // 1-based inclusive
	DataRowTo       int    // 1-based inclusive
}

type ruleFilter struct {
	Field       string
	FieldType   string
	Op          string
	Value       string
	Value2      string
	Conjunction string
}

type ruleAttachment struct {
	DocumentTemplateID string
	OutputFormat       string
}

// RunGeneration executes one rule: resolve its approved version + filters +
// attachments, compute the time window, fetch data via the same function a
// live dashboard widget uses, render each attached document, upload to S3,
// and log a generation_run row (+ one generated_document row per attachment
// that succeeds). Returns the run_id even on failure, so the caller can point
// the user at the run's error_detail.
func RunGeneration(ctx context.Context, pool *pgxpool.Pool, ruleID, triggerType, triggeredBy string) (runID string, err error) {
	return runGeneration(ctx, pool, ruleID, triggerType, triggeredBy, "", nil)
}

// RunGenerationForSourceIDs executes an approved rule for explicit source rows
// (selected rows / ON_CREATE / ON_APPROVE) instead of the whole time-window set.
func RunGenerationForSourceIDs(
	ctx context.Context,
	pool *pgxpool.Pool,
	ruleID, triggerType, triggeredBy, sourceIDField string,
	sourceIDs []string,
) (runID string, err error) {
	return runGeneration(ctx, pool, ruleID, triggerType, triggeredBy, sourceIDField, sourceIDs)
}

func runGeneration(
	ctx context.Context,
	pool *pgxpool.Pool,
	ruleID, triggerType, triggeredBy, sourceIDField string,
	sourceIDs []string,
) (runID string, err error) {
	// Master switch: DMS_ENABLED off blocks every generation path, not just the UI.
	if !dmscommon.IsDMSEnabled() {
		return "", fmt.Errorf("DMS_DISABLED: DMS is disabled at application level")
	}

	// Document-Service gate: hard-stop + durable quota (client cannot bypass).
	if q, qErr := docsvc.NewFromEnv().QuotaCheck(ctx); qErr != nil {
		return "", fmt.Errorf("document-service unavailable (generation blocked): %w", qErr)
	} else if !q.Allowed {
		code := q.ErrorCode
		if code == "" {
			code = "DMS_GENERATION_QUOTA_EXCEEDED"
		}
		return "", fmt.Errorf("%s: %s", code, q.Message)
	}

	rule, err := loadRuleHeader(ctx, pool, ruleID)
	if err != nil {
		return "", fmt.Errorf("load rule: %w", err)
	}
	if rule.ProcessingStatus != "APPROVED" || rule.Status != "Active" || rule.CurrentVersionID == "" {
		return "", fmt.Errorf("rule %s is not an approved, active rule with a live version", ruleID)
	}

	version, err := loadRuleVersion(ctx, pool, rule.CurrentVersionID)
	if err != nil {
		return "", fmt.Errorf("load rule version: %w", err)
	}
	windowStart, windowEnd, err := computeWindow(version)
	if err != nil {
		return "", fmt.Errorf("compute time window: %w", err)
	}

	runID, err = insertRun(ctx, pool, runInsert{
		RuleID:      rule.RuleID,
		VersionID:   version.VersionID,
		TriggerType: triggerType,
		TriggeredBy: triggeredBy,
		WindowStart: windowStart,
		WindowEnd:   windowEnd,
	})
	if err != nil {
		return "", fmt.Errorf("insert generation_run: %w", err)
	}
	for i, sourceID := range sourceIDs {
		sourceID = strings.TrimSpace(sourceID)
		if sourceID == "" {
			continue
		}
		_, _ = pool.Exec(ctx, `
			INSERT INTO dms_svc.generation_run_source_row (run_id, source_id, sort_order)
			VALUES ($1::uuid, $2, $3)
			ON CONFLICT (run_id, source_id) DO NOTHING`, runID, sourceID, i)
	}

	filters, err := loadFilters(ctx, pool, version.VersionID)
	if err != nil {
		return runID, finishRunFailed(ctx, pool, runID, fmt.Errorf("load filters: %w", err))
	}
	attachments, err := loadAttachments(ctx, pool, version.VersionID)
	if err != nil {
		return runID, finishRunFailed(ctx, pool, runID, fmt.Errorf("load attachments: %w", err))
	}
	if len(attachments) == 0 {
		return runID, finishRunFailed(ctx, pool, runID, fmt.Errorf("rule has no document attachments"))
	}

	sourceKey, err := domaincatalog.ResolveSubModuleAlias(ctx, pool, rule.SubModuleCode, "DASHBOARD")
	if err != nil {
		return runID, finishRunFailed(ctx, pool, runID, fmt.Errorf("resolve dashboard data source: %w", err))
	}

	dashFilters := make([]dashboardbuilder.WidgetFilterRule, 0, len(filters)+1)
	if rule.EntityID != "" {
		dashFilters = append(dashFilters, dashboardbuilder.WidgetFilterRule{
			Field: "entity_id", Type: "text", Op: "=", Value: rule.EntityID, Conjunction: "AND",
		})
	}
	for _, f := range filters {
		dashFilters = append(dashFilters, dashboardbuilder.WidgetFilterRule{
			Field: f.Field, Type: f.FieldType, Op: normalizeDmsFilterOp(f.Op), Value: f.Value, Value2: f.Value2, Conjunction: f.Conjunction,
		})
	}
	if strings.TrimSpace(sourceIDField) != "" && len(sourceIDs) > 0 {
		dashFilters = append(dashFilters, dashboardbuilder.WidgetFilterRule{
			Field:       strings.TrimSpace(sourceIDField),
			Type:        "id",
			Op:          "in",
			Value:       strings.Join(sourceIDs, ","),
			Conjunction: "AND",
		})
	}
	var entityIDs []string
	if rule.EntityID != "" {
		entityIDs = []string{rule.EntityID}
	}
	asOf, asOn := "", ""
	if windowStart != nil {
		asOf = strings.TrimSpace(*windowStart)
	}
	if windowEnd != nil {
		asOn = strings.TrimSpace(*windowEnd)
	}
	bankScope, err := loadBankAccountScope(ctx, pool, version.VersionID)
	if err != nil {
		return runID, finishRunFailed(ctx, pool, runID, fmt.Errorf("load bank account scope: %w", err))
	}
	naming, err := loadOutputNaming(ctx, pool, version.VersionID, rule.Name)
	if err != nil {
		api.LogError("[DMS] loadOutputNaming run rule=%s: %v — falling back to rule name", rule.RuleID, err)
		naming = outputNaming{Prefix: rule.Name, AppendDatetime: true}
	}
	rowFrom, rowTo := normalizeDataRowRange(version.DataRowFrom, version.DataRowTo)
	fetchLimit := rowTo - rowFrom + 1
	fetchOffset := rowFrom - 1
	rows, _, err := dashboardbuilder.FetchSourceData(ctx, pool, dashboardbuilder.DataRequest{
		Source:                   sourceKey,
		EntityIDs:                entityIDs,
		Filters:                  dashFilters,
		Limit:                    fetchLimit,
		Offset:                   fetchOffset,
		AsOfDate:                 asOf,
		AsOnDate:                 asOn,
		BankAccountScope:         bankScope,
		AllowUnscopedBankAccount: len(bankScope) == 0, // scoped when rule has pairs; else allow all
	})
	if err != nil {
		return runID, finishRunFailed(ctx, pool, runID, fmt.Errorf("fetch data source %q: %w", sourceKey, err))
	}
	_, _ = pool.Exec(ctx, `
		UPDATE dms_svc.generation_run SET source_row_count = $2 WHERE run_id = $1::uuid`,
		runID, len(rows))

	var firstRow map[string]any
	if len(rows) > 0 {
		firstRow = rows[0]
	}

	rowsToMerge := rows
	expandMode := strings.ToUpper(strings.TrimSpace(version.RowExpandMode))
	switch expandMode {
	case "PER_ROW", "PER_PAGE_IN_ONE_DOC":
		if len(rowsToMerge) == 0 {
			rowsToMerge = []map[string]any{nil}
		}
	default:
		// FIRST_ROW (default): one doc set from the first dashboard row only.
		// Pool rows are still kept on genCtx so data-dms-table can list all matches.
		if len(rows) > 0 {
			rowsToMerge = []map[string]any{rows[0]}
		} else {
			rowsToMerge = []map[string]any{nil}
		}
	}

	succeeded, failed := 0, 0
	var errDetails []string
	multiDoc := len(attachments) > 1 || (expandMode == "PER_ROW" && len(rowsToMerge) > 1)
	genCtx := attachmentGenCtx{
		EntityIDs:                entityIDs,
		BankAccountScope:         bankScope,
		AllowUnscopedBankAccount: len(bankScope) == 0,
		PoolRows:                 rows,
		SourceKey:                sourceKey,
		Filters:                  dashFilters,
		AsOfDate:                 asOf,
		AsOnDate:                 asOn,
		DataRowFrom:              rowFrom,
		DataRowTo:                rowTo,
		RuleVersionID:            version.VersionID,
	}

	newAttachmentJob := func(a ruleAttachment) attachmentJob {
		return attachmentJob{RunID: runID, Attachment: a, Naming: naming, MultiDoc: multiDoc, GenCtx: genCtx}
	}

	if expandMode == "PER_PAGE_IN_ONE_DOC" {
		for _, a := range attachments {
			if err := generatePagedAttachment(ctx, pool, newAttachmentJob(a), rowsToMerge); err != nil {
				failed++
				errDetails = append(errDetails, fmt.Sprintf("%s[paged]: %v", a.DocumentTemplateID, err))
				continue
			}
			succeeded++
		}
	} else {
		for ri, mergeRow := range rowsToMerge {
			for _, a := range attachments {
				if err := generateOneAttachment(ctx, pool, newAttachmentJob(a), mergeRow); err != nil {
					failed++
					errDetails = append(errDetails, fmt.Sprintf("%s[row%d]: %v", a.DocumentTemplateID, ri, err))
					continue
				}
				succeeded++
			}
		}
	}

	// Real email cover (DMS EMAIL template) — not the system "run completed" notify.
	wantEmail, emailDestErr := ruleWantsEmailDispatch(ctx, pool, version.VersionID)
	if emailDestErr != nil {
		api.LogError("[DMS] ruleWantsEmailDispatch run=%s: %v", runID, emailDestErr)
	} else if wantEmail {
		if err := renderAndStoreEmailCover(ctx, pool, runID, rule.ModuleCode, rule.SubModuleCode, firstRow, genCtx); err != nil {
			api.LogError("[DMS] email cover render run=%s: %v — dispatch will use notification fallback body", runID, err)
		}
	}

	status := "SUCCESS"
	if failed > 0 && succeeded > 0 {
		status = "PARTIAL"
	} else if failed > 0 && succeeded == 0 {
		status = "FAILED"
	}
	if err := finishRun(ctx, pool, runID, status, strings.Join(errDetails, "; ")); err != nil {
		return runID, err
	}
	if status == "FAILED" {
		return runID, fmt.Errorf("all attachments failed: %s", strings.Join(errDetails, "; "))
	}
	// Phase 4 — queue emails for GENERATED docs (notification after successful storage).
	if err := DispatchRun(ctx, pool, runID); err != nil {
		// Leave docs as GENERATED so StartDispatchWorker can retry.
		api.LogError("[DMS] DispatchRun after generation run=%s: %v", runID, err)
		return runID, fmt.Errorf("generation succeeded (run_id=%s) but dispatch failed: %w", runID, err)
	}

	incrementDocSvcQuotaForRun(ctx, pool, runID, triggeredBy)
	return runID, nil
}

// normalizeDmsFilterOp preserves rules created by the original editor (eq/gte)
// while using Dashboard Builder's canonical operator vocabulary.
func normalizeDmsFilterOp(op string) string {
	switch strings.ToLower(strings.TrimSpace(op)) {
	case "eq":
		return "="
	case "neq":
		return "!="
	case "gt":
		return ">"
	case "gte":
		return ">="
	case "lt":
		return "<"
	case "lte":
		return "<="
	default:
		return strings.TrimSpace(op)
	}
}

func mergeTemplateHTML(ctx context.Context, pool *pgxpool.Pool, contentHTML string, mergeFields map[string]string, row map[string]any, genCtx attachmentGenCtx, format string) (string, map[string]string, error) {
	values := make(map[string]string, len(mergeFields)+8)
	for fieldKey, fieldCode := range mergeFields {
		var raw any
		if row != nil {
			raw = row[fieldCode]
		}
		values[fieldKey] = formatFieldValue(raw)
	}
	for _, key := range extractDmsFieldKeys(contentHTML) {
		if _, ok := values[key]; ok {
			continue
		}
		if row != nil {
			values[key] = formatFieldValue(row[key])
		}
	}
	renderedHTML := substituteMergeFields(contentHTML, values)
	renderedHTML, err := expandDataTablePlaceholders(ctx, pool, renderedHTML, row, genCtx, format)
	if err != nil {
		return "", nil, fmt.Errorf("expand data tables: %w", err)
	}
	renderedHTML, err = expandTxnTablePlaceholders(ctx, pool, renderedHTML, row, genCtx, format)
	if err != nil {
		return "", nil, fmt.Errorf("expand txn tables: %w", err)
	}
	renderedHTML, err = expandChartPlaceholders(ctx, pool, renderedHTML, row, genCtx)
	if err != nil {
		return "", nil, fmt.Errorf("expand charts: %w", err)
	}
	renderedHTML = expandKPIPlaceholders(renderedHTML, genCtx.PoolRows)
	return renderedHTML, values, nil
}

// attachmentJob is one attachment of one run to render: the run it belongs to,
// the rule's attachment definition, the output naming, whether this run emits
// multiple documents, and the shared generation context. The source row(s) stay
// a separate argument since one-per-row and paged rendering differ only there.
type attachmentJob struct {
	RunID      string
	Attachment ruleAttachment
	Naming     outputNaming
	MultiDoc   bool
	GenCtx     attachmentGenCtx
}

func generateOneAttachment(ctx context.Context, pool *pgxpool.Pool, job attachmentJob, row map[string]any) error {
	a := job.Attachment
	format := strings.ToUpper(strings.TrimSpace(a.OutputFormat))
	if format == "" {
		format = "HTML"
	}

	tplVersionID, content, err := loadApprovedTemplateContent(ctx, pool, a.DocumentTemplateID)
	if err != nil {
		return fmt.Errorf("load template content: %w", err)
	}
	mergeFields, err := loadMergeFields(ctx, pool, tplVersionID)
	if err != nil {
		return fmt.Errorf("load merge fields: %w", err)
	}

	renderedHTML, values, err := mergeTemplateHTML(ctx, pool, content.HTML, mergeFields, row, job.GenCtx, format)
	if err != nil {
		return err
	}

	sheetRows := resolveSheetRows(ctx, pool, tplVersionID, content, values)
	file, err := renderAndStoreViaDocSvc(ctx, format, renderedHTML, values, content.SheetTokens, sheetRows, content.Kind, content.PageDesign)
	if err != nil {
		return fmt.Errorf("render %s: %w", format, err)
	}
	return storeGeneratedDocument(ctx, pool, storeDocumentParams{
		RunID:             job.RunID,
		Attachment:        a,
		TemplateVersionID: tplVersionID,
		Naming:            job.Naming,
		MultiDoc:          job.MultiDoc,
		Row:               row,
		File:              file,
		RuleVersionID:     job.GenCtx.RuleVersionID,
	})
}

// generatePagedAttachment renders one file where each source row becomes a page.
func generatePagedAttachment(ctx context.Context, pool *pgxpool.Pool, job attachmentJob, rows []map[string]any) error {
	a := job.Attachment
	format := strings.ToUpper(strings.TrimSpace(a.OutputFormat))
	if format == "" {
		format = "HTML"
	}
	tplVersionID, content, err := loadApprovedTemplateContent(ctx, pool, a.DocumentTemplateID)
	if err != nil {
		return fmt.Errorf("load template content: %w", err)
	}
	mergeFields, err := loadMergeFields(ctx, pool, tplVersionID)
	if err != nil {
		return fmt.Errorf("load merge fields: %w", err)
	}
	if len(rows) == 0 {
		rows = []map[string]any{nil}
	}
	var pages []string
	var values map[string]string
	for i, row := range rows {
		pageHTML, pageValues, err := mergeTemplateHTML(ctx, pool, content.HTML, mergeFields, row, job.GenCtx, format)
		if err != nil {
			return err
		}
		pages = append(pages, pageHTML)
		if i == 0 {
			values = pageValues
		}
	}
	combined := strings.Join(pages, `<div data-dms-page-break="true" class="dms-page-break"></div>`)
	sheetRows := resolveSheetRows(ctx, pool, tplVersionID, content, values)
	file, err := renderMergedOutputViaDocSvc(ctx, format, combined, values, content.SheetTokens, sheetRows, content.Kind, content.PageDesign)
	if err != nil {
		return fmt.Errorf("render %s: %w", format, err)
	}
	return storeGeneratedDocument(ctx, pool, storeDocumentParams{
		RunID:             job.RunID,
		Attachment:        a,
		TemplateVersionID: tplVersionID,
		Naming:            job.Naming,
		MultiDoc:          job.MultiDoc,
		Row:               rows[0],
		File:              file,
		RuleVersionID:     job.GenCtx.RuleVersionID,
	})
}

// storeDocumentParams is one rendered file to persist for a run: which run and
// attachment produced it, the template version it came from, how to name it,
// the source row it was merged from, and the rule version whose destinations
// decide local / S3 / in-app storage.
type storeDocumentParams struct {
	RunID             string
	Attachment        ruleAttachment
	TemplateVersionID string
	Naming            outputNaming
	MultiDoc          bool
	Row               map[string]any
	File              renderedFile
	RuleVersionID     string
}

func storeGeneratedDocument(ctx context.Context, pool *pgxpool.Pool, p storeDocumentParams) error {
	runID, a, tplVersionID, file := p.RunID, p.Attachment, p.TemplateVersionID, p.File
	sum := sha256.Sum256(file.Bytes)
	checksum := hex.EncodeToString(sum[:])

	tplName, _ := loadTemplateName(ctx, pool, a.DocumentTemplateID)
	outName := buildDmsOutputFilename(p.Naming, tplName, file.Ext, time.Now(), p.MultiDoc, rowOutputSuffix(p.Row))

	wantLocal, _ := versionHasDestinationType(ctx, pool, p.RuleVersionID, "LOCAL")
	wantS3, _ := versionHasDestinationType(ctx, pool, p.RuleVersionID, "S3_ARCHIVE")
	wantInApp, _ := versionHasDestinationType(ctx, pool, p.RuleVersionID, "IN_APP")
	// Legacy / default: no destinations → S3. Email-only still needs an object for attachments.
	if !wantLocal && !wantS3 && !wantInApp {
		wantS3 = true
	}

	var localPath string
	if wantLocal {
		rel, _, err := WriteLocalDmsFile(runID, outName, file.Bytes)
		if err != nil {
			return fmt.Errorf("local store: %w", err)
		}
		localPath = rel
	}

	s3Key := ""
	storageBackend := "MAIN_S3"
	if wantS3 || wantInApp || !wantLocal {
		if file.StoredKey != "" {
			// Document-Service already uploaded this to its own bucket.
			s3Key = file.StoredKey
			storageBackend = "DOCSVC_S3"
		} else {
			s3Key = s3storage.BuildModuleS3Key("dms", "generated", checksum, file.Ext)
			if err := s3storage.PutObjectToS3(ctx, s3Key, file.Bytes, file.ContentType); err != nil {
				return fmt.Errorf("s3 upload: %w", err)
			}
		}
	} else {
		s3Key = LocalStorageKey(localPath)
	}

	_, err := pool.Exec(ctx, `
		INSERT INTO dms_svc.generated_document
			(run_id, document_template_id, template_version_id, s3_key, file_format, file_size, checksum, status, output_filename, local_path, storage_backend)
		VALUES ($1::uuid, $2::uuid, $3::uuid, $4, $5, $6, $7, 'GENERATED', $8, $9, $10)`,
		runID, a.DocumentTemplateID, tplVersionID, s3Key, file.Format, len(file.Bytes), checksum, outName, localPath, storageBackend)
	return err
}

func versionHasDestinationType(ctx context.Context, pool *pgxpool.Pool, versionID, destType string) (bool, error) {
	versionID = strings.TrimSpace(versionID)
	if versionID == "" || pool == nil {
		return false, nil
	}
	var n int
	err := pool.QueryRow(ctx, `
		SELECT COUNT(*)::int
		FROM dms_svc.generation_rule_destination
		WHERE version_id = $1::uuid
		  AND is_enabled = true
		  AND COALESCE(is_deleted, false) = false
		  AND destination_type = $2`, versionID, destType).Scan(&n)
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") {
			return false, nil
		}
		return false, err
	}
	return n > 0, nil
}

type outputNaming struct {
	Prefix         string
	AppendDatetime bool
}

func loadOutputNaming(ctx context.Context, pool *pgxpool.Pool, versionID, ruleName string) (outputNaming, error) {
	// Prefer EMAIL destination naming, then S3_ARCHIVE, then any enabled row.
	rows, err := pool.Query(ctx, `
		SELECT COALESCE(output_name_prefix,''), append_datetime, destination_type
		FROM dms_svc.generation_rule_destination
		WHERE version_id = $1::uuid AND is_enabled = true AND COALESCE(is_deleted, false) = false
		ORDER BY CASE destination_type
			WHEN 'EMAIL' THEN 0
			WHEN 'S3_ARCHIVE' THEN 1
			ELSE 2
		END, sort_order`, versionID)
	if err != nil {
		return outputNaming{Prefix: ruleName, AppendDatetime: true}, err
	}
	defer rows.Close()
	fallback := outputNaming{Prefix: ruleName, AppendDatetime: true}
	for rows.Next() {
		var prefix, destType string
		var appendDT bool
		if err := rows.Scan(&prefix, &appendDT, &destType); err != nil {
			continue
		}
		n := outputNaming{Prefix: strings.TrimSpace(prefix), AppendDatetime: appendDT}
		if n.Prefix == "" {
			n.Prefix = ruleName
		}
		if fallback.Prefix == ruleName {
			fallback = n
		}
		if destType == "EMAIL" {
			return n, nil
		}
	}
	return fallback, rows.Err()
}

func loadBankAccountScope(ctx context.Context, pool *pgxpool.Pool, versionID string) ([]dashboardbuilder.BankAccountScopePair, error) {
	rows, err := pool.Query(ctx, `
		SELECT COALESCE(bank_id,''), account_number
		FROM dms_svc.generation_rule_bank_account_scope
		WHERE version_id = $1::uuid
		ORDER BY sort_order, account_number`, versionID)
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") {
			return nil, nil
		}
		return nil, err
	}
	defer rows.Close()
	var out []dashboardbuilder.BankAccountScopePair
	for rows.Next() {
		var p dashboardbuilder.BankAccountScopePair
		if err := rows.Scan(&p.BankID, &p.AccountNumber); err != nil {
			continue
		}
		p.AccountNumber = strings.TrimSpace(p.AccountNumber)
		if p.AccountNumber == "" {
			continue
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func loadTemplateName(ctx context.Context, pool *pgxpool.Pool, templateID string) (string, error) {
	var name string
	err := pool.QueryRow(ctx, `
		SELECT COALESCE(name,'') FROM dms_svc.template WHERE template_id = $1::uuid`, templateID,
	).Scan(&name)
	return name, err
}

// buildDmsOutputFilename mirrors email transform naming:
//
//	{prefix}[_YYYYMMDD_HHMMSS].{ext}
//
// When the rule has multiple document templates, a short template slug is
// appended so files stay distinct. Single-template rules keep the clean prefix.
func buildDmsOutputFilename(n outputNaming, templateName, fileExt string, at time.Time, multiDoc bool, rowSuffix string) string {
	ext := strings.ToLower(strings.TrimSpace(fileExt))
	if ext == "" {
		ext = "bin"
	}
	ext = strings.TrimPrefix(ext, ".")

	base := sanitizeOutputName(n.Prefix)
	if base == "" {
		base = sanitizeOutputName(templateName)
	}
	if base == "" {
		base = "document"
	}
	if multiDoc {
		tplSlug := sanitizeOutputName(templateName)
		if tplSlug != "" && !strings.EqualFold(base, tplSlug) {
			// Keep short: first 24 chars of template slug
			if len(tplSlug) > 24 {
				tplSlug = tplSlug[:24]
			}
			base = base + "_" + tplSlug
		}
	}
	if s := sanitizeOutputName(rowSuffix); s != "" {
		if len(s) > 32 {
			s = s[:32]
		}
		base = base + "_" + s
	}
	if n.AppendDatetime {
		base = base + "_" + at.Format("20060102_150405")
	}
	return base + "." + ext
}

// rowOutputSuffix picks a stable id from the merge row so PER_ROW outputs
// don't collide when generated in the same second.
func rowOutputSuffix(row map[string]any) string {
	if row == nil {
		return ""
	}
	for _, k := range []string{
		"account_number", "bank_statement_id", "fd_booking_id", "fd_id",
		"booking_id", "id", "entity_id",
	} {
		v := strings.TrimSpace(formatFieldValue(row[k]))
		if v == "" || v == "-" || strings.EqualFold(v, "null") {
			continue
		}
		return v
	}
	return ""
}

func sanitizeOutputName(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_', r == '.':
			b.WriteRune(r)
		case r == ' ':
			b.WriteByte('_')
		}
	}
	out := strings.Trim(b.String(), "._-")
	if len(out) > 120 {
		out = out[:120]
	}
	return out
}

// ─── merge-field rendering ──────────────────────────────────────────────────

var mergeFieldSpanRe = regexp.MustCompile(`(?s)<span[^>]*data-dms-field="([^"]+)"[^>]*>.*?</span>`)
var dmsFieldKeyRe = regexp.MustCompile(`data-dms-field="([^"]+)"`)
var txnTableDivRe = regexp.MustCompile(`(?is)<div\b([^>]*)\bdata-dms-txn-table(?:=["']([^"']*)["'])?([^>]*)>.*?</div>`)

func extractDmsFieldKeys(html string) []string {
	seen := map[string]struct{}{}
	var keys []string
	for _, m := range dmsFieldKeyRe.FindAllStringSubmatch(html, -1) {
		if len(m) < 2 {
			continue
		}
		k := strings.TrimSpace(m[1])
		if k == "" {
			continue
		}
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		keys = append(keys, k)
	}
	return keys
}

// expandTxnTablePlaceholders replaces every data-dms-txn-table div with a real
// HTML <table> of child rows (default source: cashBankStatementTransactions).
// ParentID comes from the header row (bank_statement_id / statement_id).
// PDF/DOCX get a smaller default limit than XLSX to keep file size sane.
func expandTxnTablePlaceholders(ctx context.Context, pool *pgxpool.Pool, html string, headerRow map[string]any, genCtx attachmentGenCtx, format string) (string, error) {
	if !strings.Contains(strings.ToLower(html), "data-dms-txn-table") {
		return html, nil
	}
	parentID := ""
	if headerRow != nil {
		parentID = strings.TrimSpace(formatFieldValue(headerRow["bank_statement_id"]))
		if parentID == "" {
			parentID = strings.TrimSpace(formatFieldValue(headerRow["statement_id"]))
		}
	}
	if parentID == "" {
		return txnTableDivRe.ReplaceAllString(html, `<p><em>(No statement selected — transactions omitted.)</em></p>`), nil
	}

	var expandErr error
	out := txnTableDivRe.ReplaceAllStringFunc(html, func(match string) string {
		if expandErr != nil {
			return match
		}
		sub := txnTableDivRe.FindStringSubmatch(match)
		attrs := ""
		source := "cashBankStatementTransactions"
		if len(sub) >= 4 {
			attrs = sub[1] + sub[3]
			if strings.TrimSpace(sub[2]) != "" {
				source = strings.TrimSpace(sub[2])
			}
		}
		limit := defaultTxnLimit(format)
		if m := regexp.MustCompile(`(?i)data-dms-limit=["'](\d+)["']`).FindStringSubmatch(attrs); len(m) >= 2 {
			if n, err := strconv.Atoi(m[1]); err == nil && n > 0 {
				limit = n
			}
		}
		rows, _, err := dashboardbuilder.FetchSourceData(ctx, pool, dashboardbuilder.DataRequest{
			Source:                   source,
			EntityIDs:                genCtx.EntityIDs,
			ParentID:                 parentID,
			Filters:                  genCtx.Filters,
			AsOfDate:                 genCtx.AsOfDate,
			AsOnDate:                 genCtx.AsOnDate,
			Limit:                    limit,
			BankAccountScope:         genCtx.BankAccountScope,
			AllowUnscopedBankAccount: genCtx.AllowUnscopedBankAccount,
		})
		if err != nil {
			expandErr = err
			return match
		}
		return buildTxnHTMLTable(rows, limit)
	})
	return out, expandErr
}

func defaultTxnLimit(format string) int {
	switch strings.ToUpper(format) {
	case "XLSX":
		return 5000
	case "HTML":
		return 1000
	default: // PDF, DOCX
		return 200
	}
}

func buildTxnHTMLTable(rows []map[string]any, limit int) string {
	var b strings.Builder
	b.WriteString(`<h3>Transactions</h3>`)
	if len(rows) == 0 {
		b.WriteString(`<p><em>No transactions found for this statement.</em></p>`)
		return b.String()
	}
	if len(rows) >= limit {
		b.WriteString(fmt.Sprintf(`<p><em>Showing first %d transactions.</em></p>`, limit))
	}
	b.WriteString(`<table class="dms-table"><thead><tr>`)
	headers := []string{"Date", "Description", "Withdrawal", "Deposit", "Balance", "Category", "Channel"}
	for _, h := range headers {
		b.WriteString("<th><p>")
		b.WriteString(htmlpkg.EscapeString(h))
		b.WriteString("</p></th>")
	}
	b.WriteString(`</tr></thead><tbody>`)
	for _, r := range rows {
		date := formatFieldValue(r["value_date"])
		if date == "" {
			date = formatFieldValue(r["transaction_date"])
		}
		desc := formatFieldValue(r["description"])
		if clean := formatFieldValue(r["narration_clean"]); clean != "" {
			desc = clean
		}
		cells := []string{
			date,
			desc,
			formatFieldValue(r["withdrawal_amount"]),
			formatFieldValue(r["deposit_amount"]),
			formatFieldValue(r["balance"]),
			formatFieldValue(r["category_name"]),
			formatFieldValue(r["payment_channel"]),
		}
		b.WriteString("<tr>")
		for _, c := range cells {
			b.WriteString("<td><p>")
			b.WriteString(htmlpkg.EscapeString(c))
			b.WriteString("</p></td>")
		}
		b.WriteString("</tr>")
	}
	b.WriteString(`</tbody></table>`)
	return b.String()
}

// substituteMergeFields replaces every `<span data-dms-field="X">...</span>`
// with the resolved, HTML-escaped value for X. A field with no resolved
// value is left as its original placeholder span rather than silently
// blanked — an unresolved merge field should be visibly wrong, not
// invisibly wrong.
func substituteMergeFields(body string, values map[string]string) string {
	out := mergeFieldSpanRe.ReplaceAllStringFunc(body, func(match string) string {
		sub := mergeFieldSpanRe.FindStringSubmatch(match)
		if len(sub) < 2 {
			return match
		}
		v, ok := values[sub[1]]
		if !ok {
			return match
		}
		return htmlpkg.EscapeString(v)
	})
	return substituteMustache(out, values)
}

var mustacheRe = regexp.MustCompile(`\{\{\s*([^}]+?)\s*\}\}`)

// substituteMustache replaces {{field_key}} / {{Field Label}} tokens (email subjects).
func substituteMustache(s string, values map[string]string) string {
	return mustacheRe.ReplaceAllStringFunc(s, func(match string) string {
		sub := mustacheRe.FindStringSubmatch(match)
		if len(sub) < 2 {
			return match
		}
		token := strings.TrimSpace(sub[1])
		if v, ok := values[token]; ok {
			return v
		}
		// Try slug of label: "Account Number" → account_number
		slug := strings.ToLower(strings.ReplaceAll(token, " ", "_"))
		if v, ok := values[slug]; ok {
			return v
		}
		for k, v := range values {
			if strings.EqualFold(k, token) || strings.EqualFold(strings.ReplaceAll(k, "_", " "), token) {
				return v
			}
		}
		return match
	})
}

// renderAndStoreEmailCover finds an Active DMS EMAIL-kind template for the
// rule's module/sub-module, merges + expands data (tables/charts/KPI), and
// stores subject+HTML on generation_run for DispatchRun.
func renderAndStoreEmailCover(ctx context.Context, pool *pgxpool.Pool, runID, moduleCode, subModuleCode string, row map[string]any, genCtx attachmentGenCtx) error {
	tplID, err := findEmailCoverTemplate(ctx, pool, moduleCode, subModuleCode)
	if err != nil {
		return err
	}
	if tplID == "" {
		return fmt.Errorf("no EMAIL-kind DMS template for %s/%s", moduleCode, subModuleCode)
	}
	verID, content, err := loadApprovedTemplateContent(ctx, pool, tplID)
	if err != nil {
		return err
	}
	if !strings.EqualFold(content.Kind, "EMAIL") {
		return fmt.Errorf("template %s kind=%q is not EMAIL", tplID, content.Kind)
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
		return fmt.Errorf("merge EMAIL template %s: %w", tplID, err)
	}
	subject = substituteMustache(subject, values)
	if strings.TrimSpace(body) == "" {
		return fmt.Errorf("EMAIL template %s produced empty body", tplID)
	}
	_, err = pool.Exec(ctx, `
		UPDATE dms_svc.generation_run
		   SET email_subject = $2, email_body_html = $3
		 WHERE run_id = $1::uuid`, runID, subject, body)
	return err
}

func findEmailCoverTemplate(ctx context.Context, pool *pgxpool.Pool, moduleCode, subModuleCode string) (string, error) {
	var id string
	err := pool.QueryRow(ctx, `
		SELECT t.template_id::text
		FROM dms_svc.template t
		JOIN dms_svc.template_version tv ON tv.version_id = t.current_version_id
		WHERE t.is_deleted = false
		  AND t.status = 'Active'
		  AND t.processing_status = 'APPROVED'
		  AND t.module_code = $1
		  AND t.sub_module_code = $2
		  AND upper(COALESCE(tv.content_json->>'kind', '')) = 'EMAIL'
		ORDER BY t.last_modified_at DESC
		LIMIT 1`, moduleCode, subModuleCode,
	).Scan(&id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", nil
		}
		return "", err
	}
	return id, nil
}

func formatFieldValue(v any) string {
	if v == nil {
		return ""
	}
	switch t := v.(type) {
	case time.Time:
		return t.Format(time.DateOnly)
	case *time.Time:
		if t == nil {
			return ""
		}
		return t.Format(time.DateOnly)
	case float64:
		return formatNumericDisplay(t)
	case float32:
		return formatNumericDisplay(float64(t))
	case int:
		return strconv.Itoa(t)
	case int32:
		return strconv.FormatInt(int64(t), 10)
	case int64:
		return strconv.FormatInt(t, 10)
	case bool:
		if t {
			return "Yes"
		}
		return "No"
	case pgtype.Numeric:
		if !t.Valid {
			return ""
		}
		f, err := t.Float64Value()
		if err != nil || !f.Valid {
			// Fall back to Int/Exp string if Float64 fails
			return t.Int.String()
		}
		return formatNumericDisplay(f.Float64)
	default:
		if s, ok := v.(fmt.Stringer); ok {
			return s.String()
		}
		return fmt.Sprintf("%v", t)
	}
}

// formatNumericDisplay trims float noise (e.g. 66798.31999999999 → 66798.32).
func formatNumericDisplay(v float64) string {
	if v == float64(int64(v)) {
		return strconv.FormatInt(int64(v), 10)
	}
	s := strconv.FormatFloat(v, 'f', 6, 64)
	s = strings.TrimRight(s, "0")
	s = strings.TrimRight(s, ".")
	return s
}

func wrapHTMLDocument(body string) string {
	return "<!doctype html><html><head><meta charset=\"utf-8\"></head><body>" + body + "</body></html>"
}

// ─── DB loaders ──────────────────────────────────────────────────────────────

func loadRuleHeader(ctx context.Context, pool *pgxpool.Pool, ruleID string) (ruleHeader, error) {
	var r ruleHeader
	var currentVersionID *string
	var entityID, entityName *string
	err := pool.QueryRow(ctx, `
		SELECT rule_id::text, name, module_code, sub_module_code, entity_id, entity_name,
		       status, processing_status, current_version_id::text
		FROM dms_svc.generation_rule WHERE rule_id = $1::uuid AND is_deleted = false`, ruleID,
	).Scan(&r.RuleID, &r.Name, &r.ModuleCode, &r.SubModuleCode, &entityID, &entityName,
		&r.Status, &r.ProcessingStatus, &currentVersionID)
	if currentVersionID != nil {
		r.CurrentVersionID = *currentVersionID
	}
	if entityID != nil {
		r.EntityID = *entityID
	}
	if entityName != nil {
		r.EntityName = *entityName
	}
	return r, err
}

func loadRuleVersion(ctx context.Context, pool *pgxpool.Pool, versionID string) (ruleVersionConfig, error) {
	var v ruleVersionConfig
	err := pool.QueryRow(ctx, `
		SELECT version_id::text, time_window_type, time_window_value, time_window_unit,
		       custom_start::text, custom_end::text, COALESCE(row_expand_mode, 'FIRST_ROW'),
		       COALESCE(data_row_from, 1), COALESCE(data_row_to, 500)
		FROM dms_svc.generation_rule_version WHERE version_id = $1::uuid`, versionID,
	).Scan(&v.VersionID, &v.TimeWindowType, &v.TimeWindowValue, &v.TimeWindowUnit, &v.CustomStart, &v.CustomEnd, &v.RowExpandMode,
		&v.DataRowFrom, &v.DataRowTo)
	if err == nil {
		v.DataRowFrom, v.DataRowTo = normalizeDataRowRange(v.DataRowFrom, v.DataRowTo)
	}
	return v, err
}

func loadFilters(ctx context.Context, pool *pgxpool.Pool, versionID string) ([]ruleFilter, error) {
	rows, err := pool.Query(ctx, `
		SELECT field, field_type, op, COALESCE(value,''), COALESCE(value2,''), conjunction
		FROM dms_svc.generation_rule_filter WHERE version_id = $1::uuid ORDER BY sort_order`, versionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ruleFilter
	for rows.Next() {
		var f ruleFilter
		if err := rows.Scan(&f.Field, &f.FieldType, &f.Op, &f.Value, &f.Value2, &f.Conjunction); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, nil
}

func loadAttachments(ctx context.Context, pool *pgxpool.Pool, versionID string) ([]ruleAttachment, error) {
	rows, err := pool.Query(ctx, `
		SELECT document_template_id::text, output_format
		FROM dms_svc.generation_rule_attachment WHERE version_id = $1::uuid ORDER BY sort_order`, versionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ruleAttachment
	for rows.Next() {
		var a ruleAttachment
		if err := rows.Scan(&a.DocumentTemplateID, &a.OutputFormat); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, nil
}

// templateContent is the shape stored in dms_svc.template_version.content_json.
type templateContent struct {
	HTML        string         `json:"html"`
	Kind        string         `json:"kind"`
	SheetTokens []string       `json:"sheetTokens"`
	SheetRows   [][]string     `json:"sheetRows"`
	EmailMeta   emailMetaJSON  `json:"emailMeta"`
	PageDesign  pageDesignJSON `json:"pageDesign"`
}

type emailMetaJSON struct {
	Subject   string `json:"subject"`
	Preheader string `json:"preheader"`
	ReplyTo   string `json:"replyTo"`
}

// pageDesignJSON is PDF chrome stored alongside TipTap html in content_json
// (same documented last-resort envelope as emailMeta / sheetTokens).
type pageDesignJSON struct {
	HeaderText        string  `json:"headerText"`
	FooterText        string  `json:"footerText"`
	WatermarkText     string  `json:"watermarkText"`
	WatermarkDataURL  string  `json:"watermarkDataUrl"`
	WatermarkAngle    float64 `json:"watermarkAngle"` // degrees; default -45
	LetterheadDataURL string  `json:"letterheadDataUrl"`
	LogoDataURL       string  `json:"logoDataUrl"`
	FooterLogoDataURL string  `json:"footerLogoDataUrl"`
	BackgroundColor   string  `json:"backgroundColor"`
	LogoAlign         string  `json:"logoAlign"`   // left | center | right
	PageSize          string  `json:"pageSize"`    // A4 | A3 | Letter | Legal
	Orientation       string  `json:"orientation"` // portrait | landscape
	MarginTop         float64 `json:"marginTop"`
	MarginRight       float64 `json:"marginRight"`
	MarginBottom      float64 `json:"marginBottom"`
	MarginLeft        float64 `json:"marginLeft"`
}

// loadApprovedTemplateContent returns the current approved version's id and
// content_json payload for a document template.
func loadApprovedTemplateContent(ctx context.Context, pool *pgxpool.Pool, templateID string) (versionID string, content templateContent, err error) {
	var currentVersionID *string
	var status string
	err = pool.QueryRow(ctx, `
		SELECT current_version_id::text, status FROM dms_svc.template
		WHERE template_id = $1::uuid AND is_deleted = false`, templateID,
	).Scan(&currentVersionID, &status)
	if err != nil {
		return "", content, err
	}
	if currentVersionID == nil || status != "Active" {
		return "", content, fmt.Errorf("template %s has no approved, active version", templateID)
	}
	var contentRaw []byte
	var verStatus string
	var verDeleted bool
	if err := pool.QueryRow(ctx, `
		SELECT content_json, status, is_deleted FROM dms_svc.template_version WHERE version_id = $1::uuid`,
		*currentVersionID,
	).Scan(&contentRaw, &verStatus, &verDeleted); err != nil {
		return "", content, err
	}
	if verDeleted || verStatus != "APPROVED" {
		return "", content, fmt.Errorf("template %s current version is not usable (deleted or not approved)", templateID)
	}
	if err := json.Unmarshal(contentRaw, &content); err != nil {
		return "", content, fmt.Errorf("template content_json: %w", err)
	}
	return *currentVersionID, content, nil
}

// loadMergeFields returns field_key -> domain_catalog field_code for a
// template version, so the worker can look each one up in a fetched data row.
func loadMergeFields(ctx context.Context, pool *pgxpool.Pool, templateVersionID string) (map[string]string, error) {
	rows, err := pool.Query(ctx, `
		SELECT m.field_key, f.field_code
		FROM dms_svc.template_merge_field m
		JOIN domain_catalog.field f ON f.field_id = m.domain_catalog_field_id
		WHERE m.version_id = $1::uuid`, templateVersionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]string)
	for rows.Next() {
		var key, code string
		if err := rows.Scan(&key, &code); err != nil {
			return nil, err
		}
		out[key] = code
	}
	return out, nil
}

func loadMergeFieldLabels(ctx context.Context, pool *pgxpool.Pool, templateVersionID string) (map[string]string, error) {
	rows, err := pool.Query(ctx, `
		SELECT f.label, m.field_key
		FROM dms_svc.template_merge_field m
		JOIN domain_catalog.field f ON f.field_id = m.domain_catalog_field_id
		WHERE m.version_id = $1::uuid`, templateVersionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]string)
	for rows.Next() {
		var label, key string
		if err := rows.Scan(&label, &key); err != nil {
			return nil, err
		}
		out[label] = key
	}
	return out, nil
}

var sheetCellTokenRe = regexp.MustCompile(`\{\{([^}]+)\}\}`)

func resolveSheetRows(ctx context.Context, pool *pgxpool.Pool, templateVersionID string, content templateContent, values map[string]string) [][]string {
	if !strings.EqualFold(content.Kind, "SPREADSHEET") {
		return nil
	}
	rows := content.SheetRows
	if len(rows) == 0 {
		if len(content.SheetTokens) == 0 {
			return nil
		}
		header := make([]string, len(content.SheetTokens))
		body := make([]string, len(content.SheetTokens))
		for i, t := range content.SheetTokens {
			header[i] = t
			body[i] = "{{" + t + "}}"
		}
		rows = [][]string{header, body}
	}
	labels := map[string]string{}
	if pool != nil {
		if m, err := loadMergeFieldLabels(ctx, pool, templateVersionID); err == nil {
			labels = m
		} else {
			api.LogError("[DMS] load merge field labels version=%s: %v", templateVersionID, err)
		}
	}
	out := make([][]string, len(rows))
	for r, row := range rows {
		outRow := make([]string, len(row))
		for c, cell := range row {
			outRow[c] = sheetCellTokenRe.ReplaceAllStringFunc(cell, func(tok string) string {
				key := strings.TrimSpace(sheetCellTokenRe.FindStringSubmatch(tok)[1])
				if v, ok := values[key]; ok {
					return v
				}
				if fk, ok := labels[key]; ok {
					if v, ok := values[fk]; ok {
						return v
					}
				}
				return tok
			})
		}
		out[r] = outRow
	}
	return out
}

// ─── time window ─────────────────────────────────────────────────────────────

// computeWindow turns a rule version's time-window config into a concrete
// [start, end] date range.
//
// ROLLING and FIXED are treated identically here (today minus value*unit to
// today) — a calendar-aware "fixed period" (e.g. exactly this month-to-date)
// would need more logic than this MVP implements; that distinction is a
// stated simplification, not an oversight.
func computeWindow(v ruleVersionConfig) (start, end *string, err error) {
	switch strings.ToUpper(v.TimeWindowType) {
	case "CUSTOM":
		if v.CustomStart == nil || v.CustomEnd == nil {
			return nil, nil, fmt.Errorf("CUSTOM time window requires custom_start and custom_end")
		}
		return v.CustomStart, v.CustomEnd, nil
	case "ROLLING", "FIXED":
		now := time.Now().UTC()
		endStr := now.Format(time.DateOnly)
		if v.TimeWindowValue == nil || v.TimeWindowUnit == nil {
			return nil, &endStr, nil
		}
		var startTime time.Time
		switch strings.ToUpper(*v.TimeWindowUnit) {
		case "DAYS":
			startTime = now.AddDate(0, 0, -*v.TimeWindowValue)
		case "WEEKS":
			startTime = now.AddDate(0, 0, -7**v.TimeWindowValue)
		case "MONTHS":
			startTime = now.AddDate(0, -*v.TimeWindowValue, 0)
		case "YEARS":
			startTime = now.AddDate(-*v.TimeWindowValue, 0, 0)
		default:
			return nil, nil, fmt.Errorf("unknown time_window_unit %q", *v.TimeWindowUnit)
		}
		startStr := startTime.Format(time.DateOnly)
		return &startStr, &endStr, nil
	default:
		return nil, nil, fmt.Errorf("unknown time_window_type %q", v.TimeWindowType)
	}
}

// ─── generation_run lifecycle ────────────────────────────────────────────────

// runInsert is one new dms_svc.generation_run row: the rule version being run,
// who/what triggered it, and the resolved reporting window.
type runInsert struct {
	RuleID      string
	VersionID   string
	TriggerType string
	TriggeredBy string
	WindowStart *string
	WindowEnd   *string
}

func insertRun(ctx context.Context, pool *pgxpool.Pool, r runInsert) (string, error) {
	var runID string
	err := pool.QueryRow(ctx, `
		INSERT INTO dms_svc.generation_run (rule_id, version_id, trigger_type, triggered_by, window_start, window_end)
		VALUES ($1::uuid, $2::uuid, $3, $4, $5::date, $6::date)
		RETURNING run_id::text`,
		r.RuleID, r.VersionID, r.TriggerType, r.TriggeredBy, r.WindowStart, r.WindowEnd,
	).Scan(&runID)
	return runID, err
}

func finishRun(ctx context.Context, pool *pgxpool.Pool, runID, status, errorDetail string) error {
	_, err := pool.Exec(ctx, `
		UPDATE dms_svc.generation_run SET status = $1, finished_at = now(), error_detail = NULLIF($2,'')
		WHERE run_id = $3::uuid`, status, errorDetail, runID)
	return err
}

func finishRunFailed(ctx context.Context, pool *pgxpool.Pool, runID string, cause error) error {
	_ = finishRun(ctx, pool, runID, "FAILED", cause.Error())
	return cause
}
