package emailmessages

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"CimplrCorpSaas/api/constants"
	emailcommon "CimplrCorpSaas/api/email/common"

	"github.com/jackc/pgx/v5/pgxpool"
)

const transformResultsMaxLimit = 100
const transformResultsDefaultLimit = 50

func transformResultsACLWhere() string {
	return `
			AND (
				$2::boolean
				OR EXISTS (
					SELECT 1
					FROM email_svc.inbox_config i
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
						  OR LOWER(i.mailbox_address) = ANY (
							  SELECT LOWER(unnest(COALESCE(m.envelope_to, ARRAY[]::text[])))
						  )
					  )
				)
				OR (
					m.processing_status = 'MANUAL_UPLOAD'
					AND EXISTS (
						SELECT 1 FROM email_svc.processing_log upl_self
						WHERE upl_self.message_id = m.message_id
						  AND upl_self.step = 'UPLOAD_EML'
						  AND (
							  upl_self.detail->>'uploaded_by' = $3
							  OR ($4 <> '' AND upl_self.detail->>'uploaded_by' = $4)
						  )
					)
				)
			)`
}

func HandleTransformResultsList(pool *pgxpool.Pool) http.HandlerFunc {
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
		offset := req.Offset
		if offset < 0 {
			offset = 0
		}
		limit := req.Limit
		if limit <= 0 {
			limit = transformResultsDefaultLimit
		}
		if limit > transformResultsMaxLimit {
			limit = transformResultsMaxLimit
		}

		baseFrom := `
			FROM email_svc.transformation_results tr
			JOIN email_svc.message_attachment ma ON ma.attachment_id = tr.attachment_id
			JOIN email_svc.message m ON m.message_id = ma.message_id
			LEFT JOIN email_svc.transformation_rules r ON r.rule_id = tr.rule_id
			LEFT JOIN email_svc.inbox_config msg_inbox ON msg_inbox.inbox_id = m.inbox_id
			LEFT JOIN email_svc.inbox_config rule_inbox ON rule_inbox.inbox_id = r.inbox_id
			WHERE COALESCE(tr.transformed_s3_key, '') <> ''
			  AND COALESCE(tr.status, 'SUCCESS') = 'SUCCESS'
			  AND ($1 = '' OR m.inbox_id::text = $1)
			` + emailcommon.ActiveInboxMessageSQL + transformResultsACLWhere() + `
			  AND ($5 = '' OR tr.created_at >= $5::date)
			  AND ($6 = '' OR tr.created_at < ($6::date + interval '1 day'))
			  AND (
				$7 = '' OR
				LOWER(COALESCE(m.subject, '')) LIKE '%' || LOWER($7) || '%' OR
				LOWER(COALESCE(m.envelope_from, '')) LIKE '%' || LOWER($7) || '%' OR
				LOWER(COALESCE(array_to_string(m.envelope_to, ', '), '')) LIKE '%' || LOWER($7) || '%' OR
				LOWER(COALESCE(r.rule_name, '')) LIKE '%' || LOWER($7) || '%' OR
				LOWER(COALESCE(r.mapping_name, '')) LIKE '%' || LOWER($7) || '%' OR
				LOWER(COALESCE(ma.filename, '')) LIKE '%' || LOWER($7) || '%' OR
				LOWER(COALESCE(msg_inbox.mailbox_address, '')) LIKE '%' || LOWER($7) || '%' OR
				LOWER(COALESCE(rule_inbox.mailbox_address, '')) LIKE '%' || LOWER($7) || '%'
			  )`

		args := []interface{}{inboxID, admin, userID, userEmail, dateFrom, dateTo, search}

		var totalCount int
		countQuery := `SELECT COUNT(*) ` + baseFrom
		if err := pool.QueryRow(r.Context(), countQuery, args...).Scan(&totalCount); err != nil {
			emailcommon.RespondInternal(w, err.Error())
			return
		}

		query := `
			SELECT 
			    tr.result_id::text, 
			    tr.attachment_id::text, 
			    tr.rule_id::text, 
			    COALESCE(tr.transformed_s3_key, ''), 
			    tr.created_at,
			    m.message_id::text, 
			    COALESCE(m.inbox_id::text,''), 
			    COALESCE(m.envelope_from, ''),
			    COALESCE(array_to_string(m.envelope_to, ', '), ''),
			    COALESCE(m.subject, ''),
			    COALESCE(m.received_at, m.created_at) as message_date,
			    ma.filename,
			    COALESCE(r.rule_name, ''),
			    COALESCE(r.mapping_name, ''),
			    COALESCE(msg_inbox.mailbox_address, ''),
			    COALESCE(rule_inbox.mailbox_address, ''),
			    COALESCE(r.inbox_id::text, '')
			` + baseFrom + `
			ORDER BY tr.created_at DESC
			LIMIT $8 OFFSET $9`

		args = append(args, limit, offset)
		rows, err := pool.Query(r.Context(), query, args...)
		if err != nil {
			emailcommon.RespondInternal(w, err.Error())
			return
		}
		defer rows.Close()

		type row struct {
			ID                 string     `json:"id"`
			AttachmentID       string     `json:"attachment_id"`
			RuleID             string     `json:"rule_id"`
			TransformedS3Key   string     `json:"transformed_s3_key"`
			CreatedAt          time.Time  `json:"created_at"`
			MessageID          string     `json:"message_id"`
			InboxID            string     `json:"inbox_id"`
			EnvelopeFrom       string     `json:"envelope_from"`
			EnvelopeTo         string     `json:"envelope_to"`
			Subject            string     `json:"subject"`
			MessageDate        *time.Time `json:"message_date"`
			Filename           string     `json:"filename"`
			RuleName           string     `json:"rule_name"`
			MappingName        string     `json:"mapping_name"`
			MailboxAddress     string     `json:"mailbox_address"`
			RuleMailboxAddress string     `json:"rule_mailbox_address"`
			RuleInboxID        string     `json:"rule_inbox_id"`
		}

		items := make([]row, 0)
		for rows.Next() {
			var i row
			if err := rows.Scan(
				&i.ID, &i.AttachmentID, &i.RuleID, &i.TransformedS3Key, &i.CreatedAt,
				&i.MessageID, &i.InboxID, &i.EnvelopeFrom, &i.EnvelopeTo, &i.Subject, &i.MessageDate,
				&i.Filename, &i.RuleName, &i.MappingName,
				&i.MailboxAddress, &i.RuleMailboxAddress, &i.RuleInboxID,
			); err != nil {
				continue
			}
			items = append(items, i)
		}

		emailcommon.RespondListPaged(w, "transform-results/list", items, len(items), totalCount)
	}
}

func HandleTransformResultsStats(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			emailcommon.RespondMethodNotAllowed(w)
			return
		}

		var req struct {
			InboxID  string `json:"inbox_id"`
			DateFrom string `json:"date_from"`
			DateTo   string `json:"date_to"`
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

		query := `
			SELECT
				COUNT(*)::int AS total_results,
				COUNT(DISTINCT m.message_id)::int AS unique_messages,
				COUNT(DISTINCT tr.rule_id)::int AS unique_rules,
				COUNT(DISTINCT m.inbox_id)::int AS unique_inboxes
			FROM email_svc.transformation_results tr
			JOIN email_svc.message_attachment ma ON ma.attachment_id = tr.attachment_id
			JOIN email_svc.message m ON m.message_id = ma.message_id
			WHERE COALESCE(tr.transformed_s3_key, '') <> ''
			  AND COALESCE(tr.status, 'SUCCESS') = 'SUCCESS'
			  AND ($1 = '' OR m.inbox_id::text = $1)
			` + emailcommon.ActiveInboxMessageSQL + transformResultsACLWhere() + `
			  AND ($5 = '' OR tr.created_at >= $5::date)
			  AND ($6 = '' OR tr.created_at < ($6::date + interval '1 day'))`

		var totalResults, uniqueMessages, uniqueRules, uniqueInboxes int
		err := pool.QueryRow(r.Context(), query, inboxID, admin, userID, userEmail, dateFrom, dateTo).
			Scan(&totalResults, &uniqueMessages, &uniqueRules, &uniqueInboxes)
		if err != nil {
			emailcommon.RespondInternal(w, err.Error())
			return
		}

		emailcommon.RespondPayload(w, "transform-results/stats", map[string]interface{}{
			"total_results":   totalResults,
			"unique_messages": uniqueMessages,
			"unique_rules":    uniqueRules,
			"unique_inboxes":  uniqueInboxes,
		})
	}
}
