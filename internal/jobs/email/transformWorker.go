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
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"CimplrCorpSaas/api/utils/s3storage"
)

// EnsureTransformationSchema creates/migrates the results table for converted files.
// Converted output is stored in S3; DB row holds transformed_s3_key linked to rule_id.
func EnsureTransformationSchema(ctx context.Context, pool *pgxpool.Pool) {
	stmts := []string{
		`
		CREATE TABLE IF NOT EXISTS email_svc.transformation_results (
			result_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			attachment_id UUID NOT NULL,
			rule_id UUID NOT NULL,
			transformed_s3_key TEXT NOT NULL,
			transformed_json JSONB,
			status TEXT NOT NULL DEFAULT 'SUCCESS',
			error_message TEXT,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`ALTER TABLE email_svc.transformation_results ADD COLUMN IF NOT EXISTS transformed_s3_key TEXT`,
		`ALTER TABLE email_svc.transformation_results ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'SUCCESS'`,
		`ALTER TABLE email_svc.transformation_results ADD COLUMN IF NOT EXISTS error_message TEXT`,
		`ALTER TABLE email_svc.transformation_results ALTER COLUMN transformed_json DROP NOT NULL`,
		`CREATE UNIQUE INDEX IF NOT EXISTS uq_transformation_results_attachment_rule ON email_svc.transformation_results (attachment_id, rule_id)`,
		`CREATE INDEX IF NOT EXISTS idx_transformation_results_rule_id ON email_svc.transformation_results (rule_id)`,
		`CREATE INDEX IF NOT EXISTS idx_transformation_results_created_at ON email_svc.transformation_results (created_at DESC)`,
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
		SELECT rule_id::text, condition_type, condition_value, mapping_id, COALESCE(rule_name, '')
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
	checked := 0
	matchedCount := 0

	for rows.Next() {
		var ruleID, condType, condVal, mappingID, ruleName string
		if err := rows.Scan(&ruleID, &condType, &condVal, &mappingID, &ruleName); err != nil {
			continue
		}
		checked++

		matched := false
		switch strings.ToUpper(strings.TrimSpace(condType)) {
		case "FILE_EXTENSION":
			matched = normalizeExt(condVal) == ext && ext != ""
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
				matched = strings.EqualFold(sender, condVal)
			case "RECEIVER_EMAIL":
				for _, r := range receivers {
					if strings.EqualFold(r, condVal) {
						matched = true
						break
					}
				}
			case "SUBJECT_CONTAINS":
				matched = strings.Contains(strings.ToLower(subject), strings.ToLower(condVal))
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
		saveTransformFailure(ctx, pool, messageID, attachmentID, ruleID, "s3_download: "+err.Error())
		return
	}

	baseURL := strings.TrimRight(os.Getenv("TRANSFORM_TOOL_URL"), "/")
	if baseURL == "" {
		baseURL = "https://tranformation-tool-go.onrender.com"
	}
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
		saveTransformFailure(ctx, pool, messageID, attachmentID, ruleID, "multipart: "+err.Error())
		return
	}
	if _, err := part.Write(fileContentBytes); err != nil {
		log.Printf("[TransformWorker] Failed to write file bytes: %v", err)
		saveTransformFailure(ctx, pool, messageID, attachmentID, ruleID, "write_file: "+err.Error())
		return
	}
	_ = writer.Close()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, apiURL, bodyBuf)
	if err != nil {
		log.Printf("[TransformWorker] Failed to create request: %v", err)
		saveTransformFailure(ctx, pool, messageID, attachmentID, ruleID, "request: "+err.Error())
		return
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	client := &http.Client{Timeout: 3 * time.Minute}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("[TransformWorker] Failed to call Transformation API: %v", err)
		saveTransformFailure(ctx, pool, messageID, attachmentID, ruleID, "api_call: "+err.Error())
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
		saveTransformFailure(ctx, pool, messageID, attachmentID, ruleID, fmt.Sprintf("api_status_%d: %s", resp.StatusCode, msg))
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

	transformedS3Key := fmt.Sprintf("email/transformed/%s/%s%s",
		time.Now().Format("2006/01/02"),
		uuid.New().String(),
		transformedExt)

	if err := s3storage.PutObjectToS3(ctx, transformedS3Key, bodyBytes, contentType); err != nil {
		log.Printf("[TransformWorker] Failed to upload transformed file to S3: %v", err)
		saveTransformFailure(ctx, pool, messageID, attachmentID, ruleID, "s3_upload: "+err.Error())
		return
	}

	query := `
		INSERT INTO email_svc.transformation_results (attachment_id, rule_id, transformed_s3_key, status, error_message)
		VALUES ($1::uuid, $2::uuid, $3, 'SUCCESS', NULL)
		ON CONFLICT (attachment_id, rule_id) DO UPDATE SET
			transformed_s3_key = EXCLUDED.transformed_s3_key,
			status = 'SUCCESS',
			error_message = NULL,
			created_at = now()
	`
	_, err = pool.Exec(ctx, query, attachmentID, ruleID, transformedS3Key)
	if err != nil {
		log.Printf("[TransformWorker] Failed to save result to DB: %v", err)
		logTransformStep(ctx, pool, messageID, "TRANSFORM", "FAIL", map[string]interface{}{
			"attachment_id": attachmentID,
			"rule_id":       ruleID,
			"error":         err.Error(),
		})
		return
	}
	log.Printf("[TransformWorker] Saved converted file %s for attachment %s rule %s", transformedS3Key, attachmentID, ruleID)
	logTransformStep(ctx, pool, messageID, "TRANSFORM", "OK", map[string]interface{}{
		"attachment_id":      attachmentID,
		"rule_id":            ruleID,
		"mapping_id":         mappingID,
		"transformed_s3_key": transformedS3Key,
	})
}

func saveTransformFailure(ctx context.Context, pool *pgxpool.Pool, messageID, attachmentID, ruleID, errMsg string) {
	if pool == nil || attachmentID == "" || ruleID == "" {
		return
	}
	EnsureTransformationSchema(ctx, pool)
	_, err := pool.Exec(ctx, `
		INSERT INTO email_svc.transformation_results (attachment_id, rule_id, transformed_s3_key, status, error_message)
		VALUES ($1::uuid, $2::uuid, '', 'FAILED', $3)
		ON CONFLICT (attachment_id, rule_id) DO UPDATE SET
			status = 'FAILED',
			error_message = EXCLUDED.error_message,
			created_at = now()
	`, attachmentID, ruleID, errMsg)
	if err != nil {
		log.Printf("[TransformWorker] Failed to persist failure row: %v", err)
	}
	logTransformStep(ctx, pool, messageID, "TRANSFORM", "FAIL", map[string]interface{}{
		"attachment_id": attachmentID,
		"rule_id":       ruleID,
		"error":         errMsg,
	})
}
