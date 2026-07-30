package emailjobs

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"CimplrCorpSaas/api/utils/s3storage"
	"CimplrCorpSaas/internal/transformtool"
)

// EnsureTransformationSchema creates/migrates the results table for converted files.
// Output lands at the rule's destination (S3 / LOCAL / SFTP / API); DB stores location.
func EnsureTransformationSchema(ctx context.Context, pool *pgxpool.Pool) {
	stmts := []string{
		`
		CREATE TABLE IF NOT EXISTS email_svc.transformation_results (
			result_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			attachment_id UUID NOT NULL,
			rule_id UUID NOT NULL,
			transformed_s3_key TEXT NOT NULL DEFAULT '',
			transformed_json JSONB,
			status TEXT NOT NULL DEFAULT 'SUCCESS',
			error_message TEXT,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`ALTER TABLE email_svc.transformation_results ADD COLUMN IF NOT EXISTS transformed_s3_key TEXT`,
		`ALTER TABLE email_svc.transformation_results ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'SUCCESS'`,
		`ALTER TABLE email_svc.transformation_results ADD COLUMN IF NOT EXISTS error_message TEXT`,
		`ALTER TABLE email_svc.transformation_results ADD COLUMN IF NOT EXISTS destination_type TEXT NOT NULL DEFAULT 'S3'`,
		`ALTER TABLE email_svc.transformation_results ADD COLUMN IF NOT EXISTS output_location TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE email_svc.transformation_results ADD COLUMN IF NOT EXISTS output_filename TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE email_svc.transformation_results ALTER COLUMN transformed_json DROP NOT NULL`,
		`ALTER TABLE email_svc.transformation_rules ADD COLUMN IF NOT EXISTS destination_type TEXT NOT NULL DEFAULT 'S3'`,
		`ALTER TABLE email_svc.transformation_rules ADD COLUMN IF NOT EXISTS output_name_prefix TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE email_svc.transformation_rules ADD COLUMN IF NOT EXISTS append_datetime BOOLEAN NOT NULL DEFAULT true`,
		`ALTER TABLE email_svc.transformation_rules ADD COLUMN IF NOT EXISTS s3_prefix TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE email_svc.transformation_rules ADD COLUMN IF NOT EXISTS local_folder TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE email_svc.transformation_rules ADD COLUMN IF NOT EXISTS sftp_host TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE email_svc.transformation_rules ADD COLUMN IF NOT EXISTS sftp_port INT NOT NULL DEFAULT 22`,
		`ALTER TABLE email_svc.transformation_rules ADD COLUMN IF NOT EXISTS sftp_user TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE email_svc.transformation_rules ADD COLUMN IF NOT EXISTS sftp_password TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE email_svc.transformation_rules ADD COLUMN IF NOT EXISTS sftp_folder TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE email_svc.transformation_rules ADD COLUMN IF NOT EXISTS api_url TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE email_svc.transformation_rules ADD COLUMN IF NOT EXISTS api_auth_token TEXT NOT NULL DEFAULT ''`,
		`CREATE UNIQUE INDEX IF NOT EXISTS uq_transformation_results_attachment_rule ON email_svc.transformation_results (attachment_id, rule_id)`,
		`CREATE INDEX IF NOT EXISTS idx_transformation_results_rule_id ON email_svc.transformation_results (rule_id)`,
		`CREATE INDEX IF NOT EXISTS idx_transformation_results_created_at ON email_svc.transformation_results (created_at DESC)`,
		`
		CREATE TABLE IF NOT EXISTS email_svc.transformation_rule_destinations (
			destination_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			rule_id UUID NOT NULL,
			sort_order INT NOT NULL DEFAULT 0,
			destination_type TEXT NOT NULL DEFAULT 'S3',
			output_name_prefix TEXT NOT NULL DEFAULT '',
			append_datetime BOOLEAN NOT NULL DEFAULT true,
			s3_prefix TEXT NOT NULL DEFAULT '',
			local_folder TEXT NOT NULL DEFAULT '',
			sftp_host TEXT NOT NULL DEFAULT '',
			sftp_port INT NOT NULL DEFAULT 22,
			sftp_user TEXT NOT NULL DEFAULT '',
			sftp_password TEXT NOT NULL DEFAULT '',
			sftp_folder TEXT NOT NULL DEFAULT '',
			api_url TEXT NOT NULL DEFAULT '',
			api_auth_token TEXT NOT NULL DEFAULT '',
			is_active BOOLEAN NOT NULL DEFAULT true,
			is_deleted BOOLEAN NOT NULL DEFAULT false,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_tr_dest_rule_id ON email_svc.transformation_rule_destinations (rule_id) WHERE is_deleted = false`,
		`
		CREATE TABLE IF NOT EXISTS email_svc.transformation_result_deliveries (
			delivery_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			result_id UUID NOT NULL,
			destination_id UUID,
			destination_type TEXT NOT NULL DEFAULT 'S3',
			output_location TEXT NOT NULL DEFAULT '',
			output_filename TEXT NOT NULL DEFAULT '',
			transformed_s3_key TEXT NOT NULL DEFAULT '',
			status TEXT NOT NULL DEFAULT 'SUCCESS',
			error_message TEXT,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_tr_result_deliveries_result_id ON email_svc.transformation_result_deliveries (result_id)`,
	}
	for _, q := range stmts {
		if _, err := pool.Exec(ctx, q); err != nil {
			log.Printf("[TransformWorker] Warning: schema ensure failed: %v", err)
		}
	}
}

func normalizeExt(v string) string {
	v = strings.ToLower(strings.TrimSpace(v))
	if v == "" {
		return ""
	}
	if !strings.HasPrefix(v, ".") {
		v = "." + v
	}
	return v
}

// splitConditionValues supports multi-select rules stored as comma-separated
// condition_value (e.g. "pdf,xlsx"). Empty segments are dropped.
func splitConditionValues(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, p := range parts {
		v := strings.TrimSpace(p)
		if v == "" {
			continue
		}
		key := strings.ToLower(v)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, v)
	}
	return out
}

// matchByMode applies PREFIX / SUFFIX / EXACT / CONTAINS / GLOB against value.
// Case-insensitive. Empty pattern never matches.
func matchByMode(value, pattern, mode string) bool {
	v := strings.ToLower(strings.TrimSpace(value))
	p := strings.ToLower(strings.TrimSpace(pattern))
	if p == "" {
		return false
	}
	switch strings.ToUpper(strings.TrimSpace(mode)) {
	case "PREFIX":
		return strings.HasPrefix(v, p)
	case "SUFFIX":
		return strings.HasSuffix(v, p)
	case "EXACT":
		return v == p
	case "GLOB":
		if p == "*" {
			return true
		}
		ok, _ := path.Match(p, v)
		return ok
	case "CONTAINS", "":
		return strings.Contains(v, p)
	default:
		return strings.Contains(v, p)
	}
}

// matchByModeAny is true if value matches any comma-separated pattern.
func matchByModeAny(value, patterns, mode string) bool {
	for _, p := range splitConditionValues(patterns) {
		if matchByMode(value, p, mode) {
			return true
		}
	}
	return false
}

// directionMatches maps UI values (INBOUND/OUTBOUND) to DB mail_direction (RECEIVED/SENT).
func directionMatches(mailDirection, conditionValue string) bool {
	want := strings.ToUpper(strings.TrimSpace(conditionValue))
	got := strings.ToUpper(strings.TrimSpace(mailDirection))
	switch want {
	case "INBOUND", "RECEIVED":
		return got == "RECEIVED"
	case "OUTBOUND", "SENT":
		return got == "SENT"
	default:
		return strings.EqualFold(got, want)
	}
}

func logTransformStep(ctx context.Context, pool *pgxpool.Pool, messageID, step, status string, detail map[string]interface{}) {
	if pool == nil || strings.TrimSpace(messageID) == "" {
		return
	}
	detailJSON := []byte("{}")
	if detail != nil {
		if b, err := json.Marshal(detail); err == nil {
			detailJSON = b
		}
	}
	_, _ = pool.Exec(ctx, `
		INSERT INTO email_svc.processing_log (message_id, step, status, detail)
		VALUES ($1::uuid, $2, $3, $4::jsonb)
	`, messageID, step, status, detailJSON)
}

// Bounded concurrency for external Transform Tool calls so mail bursts
// (e.g. 1000 emails × N rules) do not stampede our process or the remote API.
var (
	transformSemOnce sync.Once
	transformSem     chan struct{}
)

func transformConcurrency() int {
	n := 3
	if v := strings.TrimSpace(os.Getenv("TRANSFORM_MAX_CONCURRENT")); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 {
			n = parsed
			if n > 20 {
				n = 20
			}
		}
	}
	return n
}

func getTransformSem() chan struct{} {
	transformSemOnce.Do(func() {
		n := transformConcurrency()
		transformSem = make(chan struct{}, n)
		log.Printf("[TransformWorker] External transform concurrency capped at %d (TRANSFORM_MAX_CONCURRENT)", n)
	})
	return transformSem
}

func enqueueTransformation(pool *pgxpool.Pool, messageID, attachmentID, ruleID, mappingID, s3Key string) {
	go func() {
		sem := getTransformSem()
		sem <- struct{}{}
		defer func() { <-sem }()
		runTransformation(pool, messageID, attachmentID, ruleID, mappingID, s3Key)
	}()
}

func markTransformPending(ctx context.Context, pool *pgxpool.Pool, attachmentID, ruleID string) {
	if pool == nil || attachmentID == "" || ruleID == "" {
		return
	}
	_, _ = pool.Exec(ctx, `
		INSERT INTO email_svc.transformation_results (attachment_id, rule_id, transformed_s3_key, status, error_message)
		VALUES ($1::uuid, $2::uuid, '', 'PENDING', NULL)
		ON CONFLICT (attachment_id, rule_id) DO UPDATE SET
			status = CASE
				WHEN email_svc.transformation_results.status = 'SUCCESS'
				     AND COALESCE(email_svc.transformation_results.transformed_s3_key, '') <> ''
				THEN email_svc.transformation_results.status
				ELSE 'PENDING'
			END,
			error_message = CASE
				WHEN email_svc.transformation_results.status = 'SUCCESS'
				     AND COALESCE(email_svc.transformation_results.transformed_s3_key, '') <> ''
				THEN email_svc.transformation_results.error_message
				ELSE NULL
			END
	`, attachmentID, ruleID)
}

// ProcessAttachmentRules is called after an attachment is fully ingested and saved to S3.
// Checks ALL approved active rules for the inbox; each match queues a transform job
// (bounded concurrency) and stores a row in transformation_results keyed by (attachment_id, rule_id).
func ProcessAttachmentRules(ctx context.Context, pool *pgxpool.Pool, inboxID, messageID, attachmentID, filename, s3Key string) {
	if pool == nil || inboxID == "" || attachmentID == "" || s3Key == "" {
		return
	}
	EnsureTransformationSchema(ctx, pool)

	log.Printf("[TransformWorker] Checking rules for attachment %s (%s) in inbox %s", attachmentID, filename, inboxID)

	query := `
		SELECT rule_id::text, condition_type, condition_value,
		       COALESCE(NULLIF(match_mode, ''), 'CONTAINS'),
		       mapping_id, COALESCE(rule_name, '')
		FROM email_svc.transformation_rules
		WHERE inbox_id = $1::uuid
		  AND is_active = true
		  AND processing_status = 'APPROVED'
		  AND is_deleted = false
	`
	rows, err := pool.Query(ctx, query, inboxID)
	if err != nil {
		log.Printf("[TransformWorker] Failed to query rules: %v", err)
		return
	}
	defer rows.Close()

	ext := normalizeExt(filepath.Ext(filename))
	baseName := filepath.Base(filename)
	checked := 0
	matchedCount := 0

	for rows.Next() {
		var ruleID, condType, condVal, matchMode, mappingID, ruleName string
		if err := rows.Scan(&ruleID, &condType, &condVal, &matchMode, &mappingID, &ruleName); err != nil {
			continue
		}
		checked++

		matched := false
		switch strings.ToUpper(strings.TrimSpace(condType)) {
		case "FILE_EXTENSION":
			for _, want := range splitConditionValues(condVal) {
				if normalizeExt(want) == ext && ext != "" {
					matched = true
					break
				}
			}
		case "ATTACHMENT_NAME":
			matched = matchByModeAny(baseName, condVal, matchMode)
		default:
			var sender, subject, direction string
			var receivers []string
			msgQuery := `SELECT COALESCE(envelope_from,''), COALESCE(envelope_to, ARRAY[]::text[]), COALESCE(subject,''), COALESCE(mail_direction,'') FROM email_svc.message WHERE message_id = $1::uuid`
			if err := pool.QueryRow(ctx, msgQuery, messageID).Scan(&sender, &receivers, &subject, &direction); err != nil {
				log.Printf("[TransformWorker] Failed to load message %s for rule match: %v", messageID, err)
				continue
			}
			switch strings.ToUpper(strings.TrimSpace(condType)) {
			case "SENDER_EMAIL":
				for _, want := range splitConditionValues(condVal) {
					if strings.EqualFold(sender, want) {
						matched = true
						break
					}
				}
			case "RECEIVER_EMAIL":
				wants := splitConditionValues(condVal)
			outerRecv:
				for _, r := range receivers {
					for _, want := range wants {
						if strings.EqualFold(r, want) {
							matched = true
							break outerRecv
						}
					}
				}
			case "SUBJECT_CONTAINS", "SUBJECT":
				matched = matchByModeAny(subject, condVal, matchMode)
			case "EMAIL_DIRECTION":
				matched = directionMatches(direction, condVal)
			}
		}

		if matched {
			matchedCount++
			log.Printf("[TransformWorker] Attachment %s matches Rule %s (%s, mapping %s). Queuing transformation.", attachmentID, ruleID, ruleName, mappingID)
			logTransformStep(ctx, pool, messageID, "TRANSFORM_MATCH", "OK", map[string]interface{}{
				"attachment_id": attachmentID,
				"rule_id":       ruleID,
				"rule_name":     ruleName,
				"mapping_id":    mappingID,
				"filename":      filename,
				"s3_key":        s3Key,
				"condition":     condType + "=" + condVal,
				"match_mode":    matchMode,
			})
			markTransformPending(ctx, pool, attachmentID, ruleID)
			enqueueTransformation(pool, messageID, attachmentID, ruleID, mappingID, s3Key)
		}
	}

	log.Printf("[TransformWorker] Done checking attachment %s: rules_checked=%d matched=%d", attachmentID, checked, matchedCount)
	if matchedCount == 0 {
		logTransformStep(ctx, pool, messageID, "TRANSFORM_CHECK", "OK", map[string]interface{}{
			"attachment_id":  attachmentID,
			"filename":       filename,
			"rules_checked":  checked,
			"matched":        0,
			"note":           "no approved rule matched",
		})
	}
}

func runTransformation(pool *pgxpool.Pool, messageID, attachmentID, ruleID, mappingID, s3Key string) {
	ctx := context.Background()
	fileContentBytes, err := s3storage.GetObjectBytes(ctx, s3Key)
	if err != nil {
		log.Printf("[TransformWorker] Failed to download source file %s from S3: %v", s3Key, err)
		saveTransformFailureByRuleID(ctx, pool, messageID, attachmentID, ruleID, "s3_download: "+err.Error())
		return
	}

	baseURL := transformtool.BaseURL()
	apiURL := baseURL + "/tftoolapi/transform"

	ext := strings.ToLower(filepath.Ext(s3Key))
	inputFormat := ""
	switch ext {
	case ".json":
		inputFormat = "json"
	case ".xml":
		inputFormat = "xml"
	case ".csv":
		inputFormat = "csv"
	}

	bodyBuf := &bytes.Buffer{}
	writer := multipart.NewWriter(bodyBuf)
	_ = writer.WriteField("mappingId", mappingID)
	_ = writer.WriteField("asFile", "true")
	if inputFormat != "" {
		_ = writer.WriteField("inputFormat", inputFormat)
	}

	part, err := writer.CreateFormFile("file", filepath.Base(s3Key))
	if err != nil {
		log.Printf("[TransformWorker] Failed to create multipart file part: %v", err)
		saveTransformFailureByRuleID(ctx, pool, messageID, attachmentID, ruleID, "multipart: "+err.Error())
		return
	}
	if _, err := part.Write(fileContentBytes); err != nil {
		log.Printf("[TransformWorker] Failed to write file bytes: %v", err)
		saveTransformFailureByRuleID(ctx, pool, messageID, attachmentID, ruleID, "write_file: "+err.Error())
		return
	}
	_ = writer.Close()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, apiURL, bodyBuf)
	if err != nil {
		log.Printf("[TransformWorker] Failed to create request: %v", err)
		saveTransformFailureByRuleID(ctx, pool, messageID, attachmentID, ruleID, "request: "+err.Error())
		return
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	client := &http.Client{Timeout: 3 * time.Minute}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("[TransformWorker] Failed to call Transformation API: %v", err)
		saveTransformFailureByRuleID(ctx, pool, messageID, attachmentID, ruleID, "api_call: "+err.Error())
		return
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		msg := string(bodyBytes)
		if len(msg) > 500 {
			msg = msg[:500]
		}
		log.Printf("[TransformWorker] Transformation API returned %d: %s", resp.StatusCode, msg)
		saveTransformFailureByRuleID(ctx, pool, messageID, attachmentID, ruleID, fmt.Sprintf("api_status_%d: %s", resp.StatusCode, msg))
		return
	}

	transformedExt := ".json"
	outputFormat := strings.TrimSpace(resp.Header.Get("X-Output-Format"))
	if outputFormat != "" {
		transformedExt = "." + strings.TrimPrefix(strings.ToLower(outputFormat), ".")
	}
	contentType := resp.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/json"
	}

	dests, destErr := loadRuleDestinations(ctx, pool, ruleID)
	if destErr != nil || len(dests) == 0 {
		log.Printf("[TransformWorker] Failed to load destinations for rule %s: %v", ruleID, destErr)
		saveTransformFailure(ctx, pool, messageID, attachmentID, ruleID, ruleDestination{}, "load_destination: "+errString(destErr))
		return
	}

	logTransformStep(ctx, pool, messageID, "TRANSFORM_CONVERT", "OK", map[string]interface{}{
		"attachment_id":       attachmentID,
		"rule_id":             ruleID,
		"mapping_id":          mappingID,
		"output_format":       transformedExt,
		"byte_size":           len(bodyBytes),
		"destination_count":   len(dests),
	})

	type deliveryOutcome struct {
		dest     ruleDestination
		location string
		s3Key    string
		outName  string
		err      error
	}
	outcomes := make([]deliveryOutcome, 0, len(dests))
	successCount := 0
	for _, dest := range dests {
		location, s3KeyOut, outName, delErr := deliverTransformed(ctx, dest, transformedExt, bodyBytes, contentType)
		outcomes = append(outcomes, deliveryOutcome{dest: dest, location: location, s3Key: s3KeyOut, outName: outName, err: delErr})
		if delErr != nil {
			log.Printf("[TransformWorker] Failed to deliver transformed file (%s) dest=%s: %v", dest.DestinationType, dest.DestinationID, delErr)
			continue
		}
		successCount++
	}

	primary := outcomes[0]
	for _, o := range outcomes {
		if o.err == nil {
			primary = o
			break
		}
	}

	overallStatus := "SUCCESS"
	var overallErr *string
	if successCount == 0 {
		overallStatus = "FAILED"
		msg := "all destinations failed"
		if primary.err != nil {
			msg = "deliver_" + strings.ToLower(primary.dest.DestinationType) + ": " + primary.err.Error()
		}
		overallErr = &msg
	} else if successCount < len(outcomes) {
		overallStatus = "PARTIAL"
		msg := fmt.Sprintf("%d of %d destinations succeeded", successCount, len(outcomes))
		overallErr = &msg
	}

	primaryLoc := primary.location
	primaryS3 := primary.s3Key
	primaryName := primary.outName
	primaryType := primary.dest.DestinationType
	if primary.err != nil {
		primaryLoc, primaryS3, primaryName = "", "", ""
	}
	// Always keep an S3 key on the result when any destination landed in S3,
	// so preview/download work for mixes like S3+API even if primary display is API.
	for _, o := range outcomes {
		if o.err != nil {
			continue
		}
		if strings.EqualFold(o.dest.DestinationType, "S3") && strings.TrimSpace(o.s3Key) != "" {
			primaryS3 = o.s3Key
			if strings.TrimSpace(primaryName) == "" {
				primaryName = o.outName
			}
			break
		}
		if strings.TrimSpace(o.s3Key) != "" && strings.TrimSpace(primaryS3) == "" {
			primaryS3 = o.s3Key
		}
	}

	var resultID string
	err = pool.QueryRow(ctx, `
		INSERT INTO email_svc.transformation_results
			(attachment_id, rule_id, transformed_s3_key, status, error_message, destination_type, output_location, output_filename)
		VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (attachment_id, rule_id) DO UPDATE SET
			transformed_s3_key = EXCLUDED.transformed_s3_key,
			status = EXCLUDED.status,
			error_message = EXCLUDED.error_message,
			destination_type = EXCLUDED.destination_type,
			output_location = EXCLUDED.output_location,
			output_filename = EXCLUDED.output_filename,
			created_at = now()
		RETURNING result_id::text
	`, attachmentID, ruleID, primaryS3, overallStatus, overallErr, primaryType, primaryLoc, primaryName).Scan(&resultID)
	if err != nil {
		log.Printf("[TransformWorker] Failed to save result to DB: %v", err)
		logTransformStep(ctx, pool, messageID, "TRANSFORM", "FAIL", map[string]interface{}{
			"attachment_id": attachmentID,
			"rule_id":       ruleID,
			"error":         err.Error(),
		})
		return
	}

	// Replace prior delivery rows for this result (retry-safe).
	_, _ = pool.Exec(ctx, `DELETE FROM email_svc.transformation_result_deliveries WHERE result_id = $1::uuid`, resultID)

	deliverySummaries := make([]map[string]interface{}, 0, len(outcomes))
	for _, o := range outcomes {
		st := "SUCCESS"
		var errMsg *string
		loc, s3k, name := o.location, o.s3Key, o.outName
		if o.err != nil {
			st = "FAILED"
			m := o.err.Error()
			errMsg = &m
			loc, s3k, name = "", "", ""
		}
		var destIDArg interface{}
		if strings.TrimSpace(o.dest.DestinationID) != "" {
			destIDArg = o.dest.DestinationID
		}
		_, derr := pool.Exec(ctx, `
			INSERT INTO email_svc.transformation_result_deliveries
				(result_id, destination_id, destination_type, output_location, output_filename, transformed_s3_key, status, error_message)
			VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8)
		`, resultID, destIDArg, o.dest.DestinationType, loc, name, s3k, st, errMsg)
		if derr != nil {
			log.Printf("[TransformWorker] Failed to save delivery row: %v", derr)
		}
		sum := map[string]interface{}{
			"destination_id":     o.dest.DestinationID,
			"destination_type":   o.dest.DestinationType,
			"output_location":    loc,
			"output_filename":    name,
			"transformed_s3_key": s3k,
			"status":             st,
		}
		if errMsg != nil {
			sum["error_message"] = *errMsg
		}
		for k, v := range deliveryAuditFromOutcome(o.dest.DestinationType, loc, ptrString(errMsg)) {
			sum[k] = v
		}
		deliverySummaries = append(deliverySummaries, sum)
	}

	logStatus := "OK"
	if overallStatus == "FAILED" {
		logStatus = "FAIL"
	} else if overallStatus == "PARTIAL" {
		logStatus = "PARTIAL"
	}
	okDetail := map[string]interface{}{
		"attachment_id":      attachmentID,
		"rule_id":            ruleID,
		"mapping_id":         mappingID,
		"result_id":          resultID,
		"status":             overallStatus,
		"destination_type":   primaryType,
		"output_location":    primaryLoc,
		"output_filename":    primaryName,
		"transformed_s3_key": primaryS3,
		"deliveries":         deliverySummaries,
		"delivery_success":   successCount,
		"delivery_total":     len(outcomes),
	}
	if overallErr != nil {
		okDetail["error"] = *overallErr
	}
	logTransformStep(ctx, pool, messageID, "TRANSFORM", logStatus, okDetail)
	log.Printf("[TransformWorker] Saved convert for attachment %s rule %s status=%s deliveries=%d/%d primary=%s",
		attachmentID, ruleID, overallStatus, successCount, len(outcomes), primaryLoc)
}

func errString(err error) string {
	if err == nil {
		return "no destinations configured"
	}
	return err.Error()
}

func ptrString(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func loadRuleDestinations(ctx context.Context, pool *pgxpool.Pool, ruleID string) ([]ruleDestination, error) {
	rows, err := pool.Query(ctx, `
		SELECT destination_id::text,
		       COALESCE(NULLIF(destination_type, ''), 'S3'),
		       COALESCE(output_name_prefix, ''),
		       COALESCE(append_datetime, true),
		       COALESCE(s3_prefix, ''),
		       COALESCE(local_folder, ''),
		       COALESCE(sftp_host, ''),
		       COALESCE(sftp_port, 22),
		       COALESCE(sftp_user, ''),
		       COALESCE(sftp_password, ''),
		       COALESCE(sftp_folder, ''),
		       COALESCE(api_url, ''),
		       COALESCE(api_auth_token, '')
		FROM email_svc.transformation_rule_destinations
		WHERE rule_id = $1::uuid
		  AND is_deleted = false
		  AND COALESCE(is_active, true) = true
		ORDER BY sort_order ASC, created_at ASC
	`, ruleID)
	if err == nil {
		defer rows.Close()
		var out []ruleDestination
		for rows.Next() {
			var d ruleDestination
			if scanErr := rows.Scan(
				&d.DestinationID, &d.DestinationType, &d.OutputNamePrefix, &d.AppendDatetime,
				&d.S3Prefix, &d.LocalFolder,
				&d.SftpHost, &d.SftpPort, &d.SftpUser, &d.SftpPassword, &d.SftpFolder,
				&d.APIURL, &d.APIAuthToken,
			); scanErr != nil {
				continue
			}
			out = append(out, d)
		}
		if len(out) > 0 {
			// Attach rule owner for LOCAL path layout.
			var createdBy, approvedBy string
			_ = pool.QueryRow(ctx, `
				SELECT COALESCE(created_by, ''), COALESCE(approved_by, '')
				FROM email_svc.transformation_rules WHERE rule_id = $1::uuid
			`, ruleID).Scan(&createdBy, &approvedBy)
			for i := range out {
				out[i].CreatedBy = createdBy
				out[i].ApprovedBy = approvedBy
			}
			return out, nil
		}
	}

	// Fallback: legacy fat columns on the rule (pre-migration / empty child table).
	d, ferr := loadRuleDestinationLegacy(ctx, pool, ruleID)
	if ferr != nil {
		if err != nil {
			return nil, err
		}
		return nil, ferr
	}
	return []ruleDestination{d}, nil
}

func loadRuleDestinationLegacy(ctx context.Context, pool *pgxpool.Pool, ruleID string) (ruleDestination, error) {
	var d ruleDestination
	err := pool.QueryRow(ctx, `
		SELECT COALESCE(NULLIF(destination_type, ''), 'S3'),
		       COALESCE(output_name_prefix, ''),
		       COALESCE(append_datetime, true),
		       COALESCE(s3_prefix, ''),
		       COALESCE(local_folder, ''),
		       COALESCE(sftp_host, ''),
		       COALESCE(sftp_port, 22),
		       COALESCE(sftp_user, ''),
		       COALESCE(sftp_password, ''),
		       COALESCE(sftp_folder, ''),
		       COALESCE(api_url, ''),
		       COALESCE(api_auth_token, ''),
		       COALESCE(created_by, ''),
		       COALESCE(approved_by, '')
		FROM email_svc.transformation_rules
		WHERE rule_id = $1::uuid
	`, ruleID).Scan(
		&d.DestinationType, &d.OutputNamePrefix, &d.AppendDatetime,
		&d.S3Prefix, &d.LocalFolder,
		&d.SftpHost, &d.SftpPort, &d.SftpUser, &d.SftpPassword, &d.SftpFolder,
		&d.APIURL, &d.APIAuthToken,
		&d.CreatedBy, &d.ApprovedBy,
	)
	if err != nil {
		return d, err
	}
	return d, nil
}

func loadRuleDestination(ctx context.Context, pool *pgxpool.Pool, ruleID string) (ruleDestination, error) {
	dests, err := loadRuleDestinations(ctx, pool, ruleID)
	if err != nil {
		return ruleDestination{}, err
	}
	if len(dests) == 0 {
		return ruleDestination{}, fmt.Errorf("no destinations for rule %s", ruleID)
	}
	return dests[0], nil
}

func saveTransformFailureByRuleID(ctx context.Context, pool *pgxpool.Pool, messageID, attachmentID, ruleID, errMsg string) {
	dest, _ := loadRuleDestination(ctx, pool, ruleID)
	saveTransformFailure(ctx, pool, messageID, attachmentID, ruleID, dest, errMsg)
}

func saveTransformFailure(ctx context.Context, pool *pgxpool.Pool, messageID, attachmentID, ruleID string, dest ruleDestination, errMsg string) {
	if pool == nil || attachmentID == "" || ruleID == "" {
		return
	}
	EnsureTransformationSchema(ctx, pool)
	destType := strings.TrimSpace(dest.DestinationType)
	if destType == "" {
		destType = "S3"
	}
	_, err := pool.Exec(ctx, `
		INSERT INTO email_svc.transformation_results
			(attachment_id, rule_id, transformed_s3_key, status, error_message, destination_type)
		VALUES ($1::uuid, $2::uuid, '', 'FAILED', $3, $4)
		ON CONFLICT (attachment_id, rule_id) DO UPDATE SET
			status = 'FAILED',
			error_message = EXCLUDED.error_message,
			destination_type = EXCLUDED.destination_type,
			created_at = now()
	`, attachmentID, ruleID, errMsg, destType)
	if err != nil {
		log.Printf("[TransformWorker] Failed to persist failure row: %v", err)
	}
	failDetail := map[string]interface{}{
		"attachment_id":    attachmentID,
		"rule_id":          ruleID,
		"error":            errMsg,
		"destination_type": destType,
	}
	for k, v := range deliveryAuditFromOutcome(destType, "", errMsg) {
		failDetail[k] = v
	}
	if strings.EqualFold(destType, "API") && strings.TrimSpace(dest.APIURL) != "" {
		failDetail["api_url"] = dest.APIURL
	}
	if strings.EqualFold(destType, "SFTP") && strings.TrimSpace(dest.SftpHost) != "" {
		failDetail["sftp_host"] = dest.SftpHost
		failDetail["sftp_port"] = dest.SftpPort
	}
	logTransformStep(ctx, pool, messageID, "TRANSFORM", "FAIL", failDetail)
}
