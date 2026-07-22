package emailmessages

import (
	"encoding/json"
	"net/http"
	"regexp"
	"strings"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	emailcommon "CimplrCorpSaas/api/email/common"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	executionLogMaxLimit     = 200
	executionLogDefaultLimit = 50
)

var execLogAPIStatusRe = regexp.MustCompile(`status=(\d{3})`)

func HandleTransformExecutionLogList(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			emailcommon.RespondMethodNotAllowed(w)
			return
		}

		var req struct {
			InboxID  string `json:"inbox_id"`
			DateFrom string `json:"date_from"`
			DateTo   string `json:"date_to"`
			Search   string `json:"search"`
			Status   string `json:"status"`
			Limit    int    `json:"limit"`
			Offset   int    `json:"offset"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			emailcommon.RespondBadRequest(w, constants.ErrInvalidBody)
			return
		}

		userID, userEmail, _, _ := emailcommon.RequestIdentity(r, "", "")
		admin := emailcommon.IsEmailAdmin(r.Context(), userID)

		inboxID := strings.TrimSpace(req.InboxID)
		dateFrom := strings.TrimSpace(req.DateFrom)
		dateTo := strings.TrimSpace(req.DateTo)
		search := strings.TrimSpace(req.Search)
		statusFilter := strings.ToUpper(strings.TrimSpace(req.Status))

		offset := req.Offset
		if offset < 0 {
			offset = 0
		}
		limit := req.Limit
		if limit <= 0 {
			limit = executionLogDefaultLimit
		}
		if limit > executionLogMaxLimit {
			limit = executionLogMaxLimit
		}

		baseFrom := `
			FROM email_svc.processing_log pl
			JOIN email_svc.message m ON m.message_id = pl.message_id
			LEFT JOIN email_svc.message_attachment ma
				ON ma.attachment_id = NULLIF(pl.detail->>'attachment_id', '')::uuid
			LEFT JOIN email_svc.transformation_rules r
				ON r.rule_id = NULLIF(pl.detail->>'rule_id', '')::uuid
			LEFT JOIN email_svc.transformation_results tr
				ON tr.attachment_id = ma.attachment_id
			   AND tr.rule_id = r.rule_id
			LEFT JOIN email_svc.inbox_config msg_inbox ON msg_inbox.inbox_id = m.inbox_id
			WHERE pl.step IN ('ATTACHMENT_INGEST', 'TRANSFORM_MATCH', 'TRANSFORM_CHECK', 'TRANSFORM', 'TRANSFORM_CONVERT')
			  AND ($1 = '' OR m.inbox_id::text = $1)
			  AND ($5 = '' OR pl.created_at >= $5::date)
			  AND ($6 = '' OR pl.created_at < ($6::date + interval '1 day'))
			  AND (
				$7 = '' OR
				LOWER(COALESCE(m.subject, '')) LIKE '%' || LOWER($7) || '%' OR
				LOWER(COALESCE(ma.filename, '')) LIKE '%' || LOWER($7) || '%' OR
				LOWER(COALESCE(r.rule_name, '')) LIKE '%' || LOWER($7) || '%' OR
				LOWER(COALESCE(pl.step, '')) LIKE '%' || LOWER($7) || '%'
			  )
			  AND (
				$8 = '' OR $8 = 'ALL' OR UPPER(pl.status) = $8
			  )
			  AND (
				$2::boolean
				OR EXISTS (
					SELECT 1 FROM email_svc.inbox_config i
					WHERE i.is_deleted = false
					  AND i.processing_status = 'APPROVED'
					  AND i.is_active = true
					  AND (
						  i.owner_user_id = $3
						  OR ($4 <> '' AND LOWER(i.mailbox_address) = LOWER($4))
						  OR EXISTS (
							  SELECT 1 FROM email_svc.inbox_members im
							  WHERE im.inbox_id = i.inbox_id AND im.user_id = $3
						  )
					  )
					  AND (
						  (m.inbox_id IS NOT NULL AND i.inbox_id = m.inbox_id)
						  OR LOWER(COALESCE(m.envelope_from, '')) = LOWER(i.mailbox_address)
					  )
				)
			  )`

		args := []interface{}{inboxID, admin, userID, userEmail, dateFrom, dateTo, search, statusFilter}

		var totalCount int
		if err := pool.QueryRow(r.Context(), `SELECT COUNT(*) `+baseFrom, args...).Scan(&totalCount); err != nil {
			emailcommon.RespondInternal(w, err.Error())
			return
		}

		query := `
			SELECT
				pl.log_id::text,
				pl.step,
				pl.status,
				pl.created_at,
				pl.detail,
				m.message_id::text,
				COALESCE(m.subject, ''),
				COALESCE(m.envelope_from, ''),
				COALESCE(ma.filename, pl.detail->>'filename', ''),
				COALESCE(r.rule_name, pl.detail->>'rule_name', ''),
				COALESCE(NULLIF(tr.destination_type, ''), NULLIF(r.destination_type, ''), pl.detail->>'destination_type', ''),
				COALESCE(tr.status, ''),
				COALESCE(tr.error_message, ''),
				COALESCE(tr.output_location, pl.detail->>'output_location', ''),
				COALESCE(msg_inbox.mailbox_address, '')
			` + baseFrom + `
			ORDER BY pl.created_at DESC
			LIMIT $9 OFFSET $10`

		args = append(args, limit, offset)
		rows, err := pool.Query(r.Context(), query, args...)
		if err != nil {
			emailcommon.RespondInternal(w, err.Error())
			return
		}
		defer rows.Close()

		type row struct {
			LogID           string                 `json:"log_id"`
			Step            string                 `json:"step"`
			Status          string                 `json:"status"`
			CreatedAt       string                 `json:"created_at"`
			MessageID       string                 `json:"message_id"`
			Subject         string                 `json:"subject"`
			EnvelopeFrom    string                 `json:"envelope_from"`
			Filename        string                 `json:"filename"`
			RuleName        string                 `json:"rule_name"`
			DestinationType string                 `json:"destination_type"`
			RunStatus       string                 `json:"run_status"`
			ErrorMessage    string                 `json:"error_message"`
			OutputLocation  string                 `json:"output_location"`
			MailboxAddress  string                 `json:"mailbox_address"`
			DeliveryStatus  string                 `json:"delivery_status"`
			DeliveryDetail  string                 `json:"delivery_detail"`
			Detail          map[string]interface{} `json:"detail"`
		}

		var items []row
		for rows.Next() {
			var item row
			var rawDetail []byte
			var createdAt time.Time
			if err := rows.Scan(
				&item.LogID, &item.Step, &item.Status, &createdAt, &rawDetail,
				&item.MessageID, &item.Subject, &item.EnvelopeFrom, &item.Filename,
				&item.RuleName, &item.DestinationType, &item.RunStatus, &item.ErrorMessage,
				&item.OutputLocation, &item.MailboxAddress,
			); err != nil {
				emailcommon.RespondInternal(w, err.Error())
				return
			}
			item.CreatedAt = api.FormatAuditTimestampIST(createdAt)
			item.Detail = map[string]interface{}{}
			if len(rawDetail) > 0 {
				_ = json.Unmarshal(rawDetail, &item.Detail)
			}
			item.DeliveryStatus, item.DeliveryDetail = resolveExecutionDeliveryStatus(
				item.Step, item.Status, item.DestinationType, item.OutputLocation,
				item.ErrorMessage, item.Detail,
			)
			items = append(items, item)
		}

		emailcommon.RespondListPaged(w, "transform-execution-log/list", items, len(items), totalCount)
	}
}

func resolveExecutionDeliveryStatus(
	step, status, destType, outputLocation, runError string,
	detail map[string]interface{},
) (deliveryStatus, deliveryDetail string) {
	if detail != nil {
		if v, ok := detail["delivery_status"].(string); ok && strings.TrimSpace(v) != "" {
			deliveryStatus = v
		}
		if v, ok := detail["http_status"].(string); ok && strings.TrimSpace(v) != "" {
			deliveryStatus = v
		}
		if v, ok := detail["delivery_detail"].(string); ok {
			deliveryDetail = v
		}
		if v, ok := detail["error"].(string); ok && deliveryDetail == "" {
			deliveryDetail = v
		}
	}
	if deliveryStatus != "" {
		if deliveryDetail == "" && outputLocation != "" {
			deliveryDetail = outputLocation
		}
		return deliveryStatus, deliveryDetail
	}

	step = strings.ToUpper(strings.TrimSpace(step))
	if step != "TRANSFORM" && step != "TRANSFORM_CONVERT" {
		return "", deliveryDetail
	}

	if strings.EqualFold(status, "FAIL") || strings.EqualFold(runError, "FAILED") || runError != "" {
		if runError != "" {
			return "FAIL", runError
		}
		return "FAIL", deliveryDetail
	}

	dt := strings.ToUpper(strings.TrimSpace(destType))
	loc := strings.TrimSpace(outputLocation)
	switch dt {
	case "API":
		if m := execLogAPIStatusRe.FindStringSubmatch(loc); len(m) == 2 {
			return m[1], loc
		}
		if loc != "" {
			return "OK", loc
		}
	case "SFTP", "LOCAL", "S3":
		if loc != "" {
			return "OK", loc
		}
	}
	if strings.EqualFold(status, "OK") {
		return "OK", loc
	}
	return deliveryStatus, deliveryDetail
}
