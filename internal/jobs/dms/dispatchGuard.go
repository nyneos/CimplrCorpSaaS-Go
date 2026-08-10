package dmsjobs

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"CimplrCorpSaas/api"
)

// SES SendRawEmail hard limit is 40 MiB including MIME overhead. Keep headroom
// for HTML + base64 expansion so we fail before outbox retry loops burn bandwidth.
const sesRawMessageMaxBytes = 40 * 1024 * 1024

func dmsEmailMaxAttachmentBytes() int64 {
	if v := strings.TrimSpace(os.Getenv("DMS_EMAIL_MAX_ATTACHMENT_BYTES")); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			return n
		}
	}
	return 35 * 1024 * 1024
}

func dmsEmailMaxAttachmentCount() int {
	if v := strings.TrimSpace(os.Getenv("DMS_EMAIL_MAX_ATTACHMENT_COUNT")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 25
}

func sumDocBytes(docs []generatedDoc) int64 {
	var total int64
	for _, d := range docs {
		total += d.FileSize
	}
	return total
}

func validateEmailAttachmentPlan(
	runID string,
	packageMode string,
	docs []generatedDoc,
	zip *generatedPackage,
) error {
	mode := strings.ToUpper(strings.TrimSpace(packageMode))
	if mode == "" {
		mode = "FILES"
	}
	maxBytes := dmsEmailMaxAttachmentBytes()
	maxCount := dmsEmailMaxAttachmentCount()

	if mode == "ZIP" {
		if zip == nil {
			return fmt.Errorf(
				"EMAIL destination uses ZIP packaging but no archive was built for run %s",
				runID,
			)
		}
		if zip.FileSize > maxBytes {
			return fmt.Errorf(
				"ZIP attachment is %d bytes (limit %d). Reduce rows/formats or split the rule",
				zip.FileSize, maxBytes,
			)
		}
		if zip.FileSize > sesRawMessageMaxBytes {
			return fmt.Errorf(
				"ZIP attachment exceeds SES 40MB raw limit (%d bytes). Use S3 archive + link instead",
				zip.FileSize,
			)
		}
		return nil
	}

	count := len(docs)
	total := sumDocBytes(docs)
	if count > maxCount {
		return fmt.Errorf(
			"EMAIL would attach %d files (limit %d). Set destination Package to ZIP or use FIRST_ROW",
			count, maxCount,
		)
	}
	if total > maxBytes {
		return fmt.Errorf(
			"EMAIL attachments total %d bytes (limit %d). Set destination Package to ZIP for PER_ROW runs",
			total, maxBytes,
		)
	}
	return nil
}

func logDispatchCostEstimate(runID string, docs []generatedDoc, emailDestCount, outboxCount int, zip *generatedPackage) {
	docCount := len(docs)
	totalBytes := sumDocBytes(docs)
	zipBytes := int64(0)
	if zip != nil {
		zipBytes = zip.FileSize
	}
	api.LogInfo(
		"[DMS-DISPATCH] run=%s cost_estimate docs=%d doc_bytes=%d zip_bytes=%d email_dests=%d outbox_rows=%d "+
			"(bandwidth scales ~ doc_bytes + outbox_rows×4/3×payload; use ZIP for PER_ROW)",
		runID, docCount, totalBytes, zipBytes, emailDestCount, outboxCount,
	)
}
