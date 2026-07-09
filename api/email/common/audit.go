package emailcommon

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"CimplrCorpSaas/api"

	"github.com/jackc/pgx/v5/pgxpool"
)

func LogProcessing(ctx context.Context, pool *pgxpool.Pool, messageID, step, status string, detail map[string]interface{}) {
	if pool == nil || strings.TrimSpace(messageID) == "" {
		return
	}
	var detailJSON []byte
	if detail != nil {
		detailJSON, _ = json.Marshal(detail)
	} else {
		detailJSON = []byte("{}")
	}
	_, _ = pool.Exec(ctx, `
		INSERT INTO email_svc.processing_log (message_id, step, status, detail)
		VALUES ($1::uuid, $2, $3, $4::jsonb)
	`, messageID, step, status, detailJSON)
}

func LogAttachmentIngest(ctx context.Context, pool *pgxpool.Pool, messageID, attachmentID, source, filename, contentType, s3Key string, fileSize int64) {
	LogProcessing(ctx, pool, messageID, "ATTACHMENT_INGEST", "OK", map[string]interface{}{
		"attachment_id": attachmentID,
		"filename":      filename,
		"s3_key":        s3Key,
		"source":        source,
		"content_type":  contentType,
		"file_size":     fileSize,
		"uploaded_by":   "Email parser",
	})
}

func HandleMessageAuditLog(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			RespondMethodNotAllowed(w)
			return
		}
		var req struct {
			MessageID string `json:"message_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			RespondBadRequest(w, "invalid body")
			return
		}
		if strings.TrimSpace(req.MessageID) == "" {
			RespondBadRequest(w, "message_id is required")
			return
		}

		rows, err := pool.Query(r.Context(), `
			SELECT log_id::text, step, status, detail, created_at
			FROM email_svc.processing_log
			WHERE message_id = $1::uuid
			ORDER BY created_at DESC
		`, req.MessageID)
		if err != nil {
			RespondInternal(w, err.Error())
			return
		}
		defer rows.Close()

		type logRow struct {
			LogID     string                 `json:"log_id"`
			Step      string                 `json:"step"`
			Status    string                 `json:"status"`
			Detail    map[string]interface{} `json:"detail"`
			CreatedAt string                 `json:"created_at"`
		}
		var items []logRow
		for rows.Next() {
			var row logRow
			var rawDetail []byte
			var createdAt time.Time
			if err := rows.Scan(&row.LogID, &row.Step, &row.Status, &rawDetail, &createdAt); err != nil {
				RespondInternal(w, err.Error())
				return
			}
			row.CreatedAt = api.FormatAuditTimestampIST(createdAt)
			row.Detail = map[string]interface{}{}
			if len(rawDetail) > 0 {
				_ = json.Unmarshal(rawDetail, &row.Detail)
			}
			items = append(items, row)
		}
		RespondList(w, "messages/audit-log", items, len(items))
	}
}
