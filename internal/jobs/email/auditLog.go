package emailjobs

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5/pgxpool"
)

type attachmentIngestInfo struct {
	MessageID    string
	AttachmentID string
	Source       string
	Filename     string
	ContentType  string
	S3Key        string
	FileSize     int64
}

func logAttachmentIngest(ctx context.Context, pool *pgxpool.Pool, info attachmentIngestInfo) {
	if pool == nil || info.MessageID == "" || info.AttachmentID == "" {
		return
	}
	detail, _ := json.Marshal(map[string]interface{}{
		"attachment_id": info.AttachmentID,
		"filename":      info.Filename,
		"s3_key":        info.S3Key,
		"source":        info.Source,
		"content_type":  info.ContentType,
		"file_size":     info.FileSize,
		"uploaded_by":   "Email parser",
	})
	logProcessingStep(ctx, pool, info.MessageID, "ATTACHMENT_INGEST", detail)
}

func logMessageIngest(ctx context.Context, pool *pgxpool.Pool, messageID, step string, detail []byte) {
	logProcessingStep(ctx, pool, messageID, step, detail)
}

func logProcessingStep(ctx context.Context, pool *pgxpool.Pool, messageID, step string, detail []byte) {
	if pool == nil || messageID == "" || step == "" {
		return
	}
	_, _ = pool.Exec(ctx, `
		INSERT INTO email_svc.processing_log (message_id, step, status, detail)
		VALUES ($1::uuid, $2, 'OK', $3::jsonb)
	`, messageID, step, detail)
}
