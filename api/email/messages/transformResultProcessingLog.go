package emailmessages

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	emailcommon "CimplrCorpSaas/api/email/common"
	"CimplrCorpSaas/api"

	"github.com/jackc/pgx/v5/pgxpool"
)

// HandleTransformResultProcessingLog returns processing_log rows relevant to a transform result.
func HandleTransformResultProcessingLog(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			emailcommon.RespondMethodNotAllowed(w)
			return
		}

		var req struct {
			ResultID string `json:"result_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			emailcommon.RespondBadRequest(w, "Invalid request body")
			return
		}
		resultID := strings.TrimSpace(req.ResultID)
		if resultID == "" {
			emailcommon.RespondBadRequest(w, "result_id is required")
			return
		}

		var messageID, attachmentID, ruleID string
		err := pool.QueryRow(r.Context(), `
			SELECT m.message_id::text, tr.attachment_id::text, tr.rule_id::text
			FROM email_svc.transformation_results tr
			JOIN email_svc.message_attachment ma ON ma.attachment_id = tr.attachment_id
			JOIN email_svc.message m ON m.message_id = ma.message_id
			WHERE tr.result_id = $1::uuid
			LIMIT 1
		`, resultID).Scan(&messageID, &attachmentID, &ruleID)
		if err != nil {
			emailcommon.RespondNotFound(w, "transformation result not found")
			return
		}

		rows, err := pool.Query(r.Context(), `
			SELECT log_id::text, step, status, detail, created_at
			FROM email_svc.processing_log
			WHERE message_id = $1::uuid
			  AND step LIKE 'TRANSFORM%'
			  AND (
			    -- Prefer exact result when present (multi-destination TRANSFORM rows).
			    detail->>'result_id' = $4
			    OR (
			      -- Same rule + attachment that produced this transform result.
			      detail->>'rule_id' = $3
			      AND (
			        COALESCE(detail->>'attachment_id', '') = ''
			        OR detail->>'attachment_id' = $2
			      )
			    )
			  )
			ORDER BY created_at ASC
		`, messageID, attachmentID, ruleID, resultID)
		if err != nil {
			emailcommon.RespondInternal(w, err.Error())
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
				emailcommon.RespondInternal(w, err.Error())
				return
			}
			row.CreatedAt = api.FormatAuditTimestampIST(createdAt)
			row.Detail = map[string]interface{}{}
			if len(rawDetail) > 0 {
				_ = json.Unmarshal(rawDetail, &row.Detail)
			}
			items = append(items, row)
		}

		emailcommon.RespondList(w, "transform-results/processing-log", items, len(items))
	}
}
