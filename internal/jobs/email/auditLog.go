package emailjobs

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5/pgxpool"
)

func logAttachmentIngest(ctx context.Context, pool *pgxpool.Pool, messageID, attachmentID, source, filename, contentType, s3Key string, fileSize int64) {
	if pool == nil || messageID == "" || attachmentID == "" {
		return
	}
	detail, _ := json.Marshal(map[string]interface{}{
		"attachment_id": attachmentID,
		"filename":      filename,
		"s3_key":        s3Key,
		"source":        source,
		"content_type":  contentType,
		"file_size":     fileSize,
		"uploaded_by":   "Email parser",
	})
	_, _ = pool.Exec(ctx, `
		INSERT INTO email_svc.processing_log (message_id, step, status, detail)
		VALUES ($1::uuid, 'ATTACHMENT_INGEST', 'OK', $2::jsonb)
	`, messageID, detail)
}
