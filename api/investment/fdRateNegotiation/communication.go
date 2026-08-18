package fdRateNegotiation

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type communicationRecipientInput struct {
	RecipientRole string `json:"recipient_role"`
	EmailAddress  string `json:"email_address"`
}

type communicationPayload struct {
	UserID               string                         `json:"user_id"`
	CommunicationID      string                         `json:"communication_id,omitempty"`
	RateRequestID        string                         `json:"rate_request_id"`
	CommunicationMode    string                         `json:"communication_mode"`
	CommunicationStatus  string                         `json:"communication_status,omitempty"`
	EmailTemplateID      string                         `json:"email_template_id,omitempty"`
	EmailTemplate        string                         `json:"email_template,omitempty"` // alias for email_template_id
	EmailTemplateName    string                         `json:"email_template_name,omitempty"`
	EmailTemplateVersion string                         `json:"email_template_version,omitempty"`
	EmailContent         string                         `json:"email_content,omitempty"`
	EmailTo              *[]string                      `json:"email_to"`
	EmailCC              *[]string                      `json:"email_cc"`
	Recipients           *[]communicationRecipientInput `json:"recipients"`
	ResponseSource       string                         `json:"response_source,omitempty"`
	ResponseDate         string                         `json:"response_date,omitempty"`
	EmailMessageID       string                         `json:"email_message_id,omitempty"`
	Action               string                         `json:"action,omitempty"` // CREATE | SEND | EDIT
	SendImmediately      bool                           `json:"send_immediately,omitempty"`
	Intent               string                         `json:"intent,omitempty"` // SEND | RECEIVE
}

func (p *communicationPayload) templateID() string {
	id := strings.TrimSpace(p.EmailTemplateID)
	if id != "" {
		return id
	}
	return strings.TrimSpace(p.EmailTemplate)
}

func (p *communicationPayload) recipientsProvided() bool {
	return p.EmailTo != nil || p.EmailCC != nil || p.Recipients != nil
}

func (p *communicationPayload) wantsSend() bool {
	if normalizeCommunicationIntent(p.Intent) == "RECEIVE" {
		return false
	}
	action := strings.ToUpper(strings.TrimSpace(p.Action))
	return p.SendImmediately || action == "SEND"
}

func normalizeCommunicationIntent(v string) string {
	return strings.ToUpper(strings.TrimSpace(v))
}

func inferCommunicationIntent(p communicationPayload) string {
	if intent := normalizeCommunicationIntent(p.Intent); intent == "SEND" || intent == "RECEIVE" {
		return intent
	}
	status := normalizeCommunicationStatus(p.CommunicationStatus)
	if status == "RESPONSE_RECEIVED" {
		return "RECEIVE"
	}
	if p.SendImmediately || strings.ToUpper(strings.TrimSpace(p.Action)) == "SEND" {
		return "SEND"
	}
	return ""
}

func normalizeCommunicationMode(v string) string {
	return strings.ToUpper(strings.TrimSpace(v))
}

func normalizeCommunicationStatus(v string) string {
	return strings.ToUpper(strings.TrimSpace(v))
}

func normalizeResponseSource(v string) string {
	return strings.ToUpper(strings.TrimSpace(v))
}

func normalizeRecipientRole(v string) string {
	return strings.ToUpper(strings.TrimSpace(v))
}

func validCommunicationMode(v string) bool {
	return v == "API" || v == "SYSTEM_EMAIL" || v == "MANUAL"
}

func validCommunicationStatus(v string) bool {
	return v == "DRAFT" || v == "PENDING_APPROVAL" || v == "SENT" || v == "DELIVERED" || v == "RESPONSE_RECEIVED" || v == "REJECTED"
}

func validResponseSource(v string) bool {
	return v == "" || v == "EMAIL" || v == "PHONE" || v == "DOCUMENT"
}

func parseOptionalTimestamp(s string) (interface{}, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}
	layouts := []string{
		"2006-01-02",
		time.RFC3339,
		"2006-01-02T15:04:05Z07:00",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	return nil, fmt.Errorf("response_date must be YYYY-MM-DD or RFC3339")
}

func keepOrReplace(oldVal, incoming string) string {
	if strings.TrimSpace(incoming) == "" {
		return oldVal
	}
	return incoming
}

func collectRecipients(p communicationPayload) []communicationRecipientInput {
	seen := map[string]struct{}{}
	out := make([]communicationRecipientInput, 0)
	add := func(role, addr string) {
		role = normalizeRecipientRole(role)
		addr = strings.TrimSpace(addr)
		if addr == "" {
			return
		}
		if role != "TO" && role != "CC" {
			return
		}
		key := role + "\x00" + strings.ToLower(addr)
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		out = append(out, communicationRecipientInput{RecipientRole: role, EmailAddress: addr})
	}
	if p.Recipients != nil {
		for _, rec := range *p.Recipients {
			add(rec.RecipientRole, rec.EmailAddress)
		}
	}
	if p.EmailTo != nil {
		for _, addr := range *p.EmailTo {
			add("TO", addr)
		}
	}
	if p.EmailCC != nil {
		for _, addr := range *p.EmailCC {
			add("CC", addr)
		}
	}
	return out
}

func countTO(recs []communicationRecipientInput) int {
	n := 0
	for _, rec := range recs {
		if rec.RecipientRole == "TO" {
			n++
		}
	}
	return n
}

func insertCommunicationRecipients(ctx context.Context, tx pgx.Tx, communicationID string, recs []communicationRecipientInput) error {
	for _, rec := range recs {
		_, err := tx.Exec(ctx, `
			INSERT INTO investment.fd_rate_communication_recipient (
				communication_id, recipient_role, email_address, is_deleted
			) VALUES ($1::uuid, $2, $3, false)`,
			communicationID, rec.RecipientRole, rec.EmailAddress,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func replaceCommunicationRecipients(ctx context.Context, tx pgx.Tx, communicationID string, recs []communicationRecipientInput) error {
	_, err := tx.Exec(ctx, `
		UPDATE investment.fd_rate_communication_recipient
		SET is_deleted = true
		WHERE communication_id = $1::uuid AND is_deleted = false`,
		communicationID,
	)
	if err != nil {
		return err
	}
	return insertCommunicationRecipients(ctx, tx, communicationID, recs)
}

type communicationAuditValues struct {
	CommunicationID   string
	RateRequestID     string
	ActionType        string
	ProcessingStatus  string
	RequestedBy       string
	RequestedIP       string
	OldMode           interface{}
	NewMode           interface{}
	OldStatus         interface{}
	NewStatus         interface{}
	OldTemplateID     interface{}
	NewTemplateID     interface{}
	OldContent        interface{}
	NewContent        interface{}
	OldResponseSource interface{}
	NewResponseSource interface{}
	OldResponseDate   interface{}
	NewResponseDate   interface{}
	OldEmailMessageID interface{}
	NewEmailMessageID interface{}
}

func insertCommunicationAudit(ctx context.Context, tx pgx.Tx, v communicationAuditValues) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO investment.fd_rate_communication_audit (
			communication_id, rate_request_id, action_type, processing_status,
			requested_by, requested_at, requested_ip,
			old_communication_mode, new_communication_mode,
			old_communication_status, new_communication_status,
			old_email_template_id, new_email_template_id,
			old_email_content, new_email_content,
			old_response_source, new_response_source,
			old_response_date, new_response_date,
			old_email_message_id, new_email_message_id
		) VALUES (
			$1::uuid, $2::uuid, $3, $4,
			$5, now(), $6,
			$7, $8,
			$9, $10,
			$11, $12,
			$13, $14,
			$15, $16,
			$17, $18,
			NULLIF($19,'')::uuid, NULLIF($20,'')::uuid
		)`,
		v.CommunicationID, v.RateRequestID, v.ActionType, v.ProcessingStatus,
		api.SystemIfBlank(v.RequestedBy), api.SystemIfBlank(v.RequestedIP),
		v.OldMode, v.NewMode,
		v.OldStatus, v.NewStatus,
		v.OldTemplateID, v.NewTemplateID,
		v.OldContent, v.NewContent,
		v.OldResponseSource, v.NewResponseSource,
		v.OldResponseDate, v.NewResponseDate,
		stringifyAuditUUID(v.OldEmailMessageID), stringifyAuditUUID(v.NewEmailMessageID),
	)
	return err
}

func stringifyAuditUUID(v interface{}) string {
	if v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return strings.TrimSpace(t)
	case *string:
		if t == nil {
			return ""
		}
		return strings.TrimSpace(*t)
	default:
		return strings.TrimSpace(fmt.Sprint(t))
	}
}

type communicationQuerier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func loadRecipientsMap(ctx context.Context, q communicationQuerier, commIDs []string) (map[string][]map[string]interface{}, error) {
	out := make(map[string][]map[string]interface{}, len(commIDs))
	for _, id := range commIDs {
		out[id] = make([]map[string]interface{}, 0)
	}
	if len(commIDs) == 0 {
		return out, nil
	}
	rows, err := q.Query(ctx, `
		SELECT recipient_id::text, communication_id::text, recipient_role, email_address
		FROM investment.fd_rate_communication_recipient
		WHERE communication_id = ANY($1::uuid[]) AND is_deleted = false
		ORDER BY recipient_role, email_address`,
		commIDs,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var recipientID, commID, role, addr string
		if err := rows.Scan(&recipientID, &commID, &role, &addr); err != nil {
			continue
		}
		out[commID] = append(out[commID], map[string]interface{}{
			"recipient_id":   recipientID,
			"recipient_role": role,
			"email_address":  addr,
		})
	}
	return out, rows.Err()
}

func splitRecipientEmails(recs []map[string]interface{}) (to []string, cc []string) {
	to = make([]string, 0)
	cc = make([]string, 0)
	for _, rec := range recs {
		role, _ := rec["recipient_role"].(string)
		addr, _ := rec["email_address"].(string)
		switch role {
		case "TO":
			to = append(to, addr)
		case "CC":
			cc = append(cc, addr)
		}
	}
	return to, cc
}

func derefOrEmpty(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func communicationRowMap(
	id, rateRequestID, mode, status string,
	templateID, templateName, templateVersion, content, responseSource, responseDate, emailMessageID *string,
	sentBy, sentAt, createdBy, createdAt, updatedBy, updatedAt *string,
	recipients []map[string]interface{},
) map[string]interface{} {
	if recipients == nil {
		recipients = []map[string]interface{}{}
	}
	emailTo, emailCC := splitRecipientEmails(recipients)
	return map[string]interface{}{
		"communication_id":       id,
		"rate_request_id":        rateRequestID,
		"communication_mode":     mode,
		"communication_status":   status,
		"email_template_id":      derefOrEmpty(templateID),
		"email_template_name":    derefOrEmpty(templateName),
		"email_template_version": derefOrEmpty(templateVersion),
		"email_content":          derefOrEmpty(content),
		"response_source":        derefOrEmpty(responseSource),
		"response_date":          derefOrEmpty(responseDate),
		"email_message_id":       derefOrEmpty(emailMessageID),
		"sent_by":                derefOrEmpty(sentBy),
		"sent_at":                derefOrEmpty(sentAt),
		"created_by":             derefOrEmpty(createdBy),
		"created_at":             derefOrEmpty(createdAt),
		"updated_by":             derefOrEmpty(updatedBy),
		"updated_at":             derefOrEmpty(updatedAt),
		"recipients":             recipients,
		"email_to":               emailTo,
		"email_cc":               emailCC,
	}
}

const communicationSelect = `
	SELECT
		communication_id::text,
		rate_request_id::text,
		communication_mode,
		communication_status,
		email_template_id,
		email_template_name,
		email_template_version,
		email_content,
		response_source,
		COALESCE(TO_CHAR(response_date,'YYYY-MM-DD"T"HH24:MI:SS"Z"'),''),
		email_message_id::text,
		sent_by,
		COALESCE(TO_CHAR(sent_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"'),''),
		created_by,
		TO_CHAR(created_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
		updated_by,
		COALESCE(TO_CHAR(updated_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"'),'')
	FROM investment.fd_rate_communication
`

type scannedCommunication struct {
	id, rateRequestID, mode, status                            string
	templateID, templateName, templateVersion, content         *string
	responseSource, responseDate, emailMessageID               *string
	sentBy, sentAt, createdBy, createdAt, updatedBy, updatedAt *string
}

func scanCommunication(row pgx.Row) (scannedCommunication, error) {
	var it scannedCommunication
	var createdByVal, createdAtVal string
	var respDate, sentAt, updatedAt string
	err := row.Scan(
		&it.id, &it.rateRequestID, &it.mode, &it.status,
		&it.templateID, &it.templateName, &it.templateVersion, &it.content,
		&it.responseSource, &respDate, &it.emailMessageID,
		&it.sentBy, &sentAt, &createdByVal, &createdAtVal, &it.updatedBy, &updatedAt,
	)
	if err != nil {
		return it, err
	}
	it.createdBy = &createdByVal
	it.createdAt = &createdAtVal
	if respDate != "" {
		it.responseDate = &respDate
	}
	if sentAt != "" {
		it.sentAt = &sentAt
	}
	if updatedAt != "" {
		it.updatedAt = &updatedAt
	}
	return it, nil
}

func (it scannedCommunication) asMap(recipients []map[string]interface{}) map[string]interface{} {
	return communicationRowMap(
		it.id, it.rateRequestID, it.mode, it.status,
		it.templateID, it.templateName, it.templateVersion, it.content,
		it.responseSource, it.responseDate, it.emailMessageID,
		it.sentBy, it.sentAt, it.createdBy, it.createdAt, it.updatedBy, it.updatedAt,
		recipients,
	)
}

func requireSessionEmail(w http.ResponseWriter, r *http.Request) (string, bool) {
	userEmail := api.GetUserEmailFromCtx(r.Context())
	if userEmail == "" {
		api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
		return "", false
	}
	return userEmail, true
}

// CreateCommunication inserts a communication + TO/CC recipients and a CREATE audit row.
// SYSTEM_EMAIL + SEND goes through notification_svc (same dispatcher as FD booking).
func CreateCommunication(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req communicationPayload
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if strings.TrimSpace(req.RateRequestID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "rate_request_id is required")
			return
		}

		intent := inferCommunicationIntent(req)
		mode := normalizeCommunicationMode(req.CommunicationMode)
		status := normalizeCommunicationStatus(req.CommunicationStatus)

		switch intent {
		case "RECEIVE":
			// Each bank reply is a NEW row. Never overwrite the original send.
			req.CommunicationID = ""
			mode = "MANUAL"
			status = "RESPONSE_RECEIVED"
		case "SEND":
			if mode == "" {
				mode = "SYSTEM_EMAIL"
			}
			status = "PENDING_APPROVAL"
		default:
			api.RespondWithError(w, http.StatusBadRequest, "intent must be SEND or RECEIVE")
			return
		}

		if !validCommunicationMode(mode) {
			api.RespondWithError(w, http.StatusBadRequest, "communication_mode must be API, SYSTEM_EMAIL or MANUAL")
			return
		}
		if !validCommunicationStatus(status) {
			api.RespondWithError(w, http.StatusBadRequest, "communication_status is invalid")
			return
		}

		responseSource := normalizeResponseSource(req.ResponseSource)
		if !validResponseSource(responseSource) {
			api.RespondWithError(w, http.StatusBadRequest, "response_source must be EMAIL, PHONE or DOCUMENT")
			return
		}
		if status == "RESPONSE_RECEIVED" && responseSource == "" {
			api.RespondWithError(w, http.StatusBadRequest, "response_source is required when communication_status is RESPONSE_RECEIVED")
			return
		}

		responseDate, err := parseOptionalTimestamp(req.ResponseDate)
		if err != nil {
			api.RespondWithError(w, http.StatusBadRequest, err.Error())
			return
		}

		recs := collectRecipients(req)
		if intent == "SEND" && mode == "SYSTEM_EMAIL" && countTO(recs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "at least one TO recipient is required to send SYSTEM_EMAIL")
			return
		}

		userEmail, ok := requireSessionEmail(w, r)
		if !ok {
			return
		}

		templateID := req.templateID()
		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to start transaction")
			return
		}
		defer tx.Rollback(ctx)

		if intent == "RECEIVE" {
			if err = assertParentStatus(ctx, tx, req.RateRequestID, "APPROVED", "SENT_TO_BANKS", "OFFERS_RECEIVED"); err != nil {
				api.RespondWithError(w, http.StatusBadRequest, err.Error())
				return
			}
		} else if err = assertParentStatus(ctx, tx, req.RateRequestID, "APPROVED", "SENT_TO_BANKS", "OFFERS_RECEIVED"); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, err.Error())
			return
		}

		var sentBy, sentAt interface{}

		var id string
		err = tx.QueryRow(ctx, `
			INSERT INTO investment.fd_rate_communication (
				rate_request_id, communication_mode, communication_status,
				email_template_id, email_template_name, email_template_version, email_content,
				response_source, response_date, email_message_id,
				sent_by, sent_at, is_deleted, created_by, created_at
			) VALUES (
				$1::uuid, $2, $3,
				NULLIF($4,''), NULLIF($5,''), NULLIF($6,''), NULLIF($7,''),
				NULLIF($8,''), $9, NULLIF($10,'')::uuid,
				$11, $12, false, $13, now()
			) RETURNING communication_id::text`,
			req.RateRequestID, mode, status,
			templateID, strings.TrimSpace(req.EmailTemplateName), strings.TrimSpace(req.EmailTemplateVersion),
			req.EmailContent, responseSource, responseDate, strings.TrimSpace(req.EmailMessageID),
			sentBy, sentAt, userEmail,
		).Scan(&id)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, fmt.Sprintf("Create failed: %v", err))
			return
		}

		if err = insertCommunicationRecipients(ctx, tx, id, recs); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Recipient insert failed")
			return
		}

		processingStatus := "PENDING_APPROVAL"
		if intent == "RECEIVE" {
			processingStatus = "APPROVED"
		}

		if err = insertCommunicationAudit(ctx, tx, communicationAuditValues{
			CommunicationID:   id,
			RateRequestID:     req.RateRequestID,
			ActionType:        "CREATE",
			ProcessingStatus:  processingStatus,
			RequestedBy:       userEmail,
			RequestedIP:       api.ClientIPFromContext(ctx),
			NewMode:           mode,
			NewStatus:         status,
			NewTemplateID:     nullIfEmpty(templateID),
			NewContent:        nullIfEmpty(req.EmailContent),
			NewResponseSource: nullIfEmpty(responseSource),
			NewResponseDate:   responseDate,
			NewEmailMessageID: nullIfEmpty(strings.TrimSpace(req.EmailMessageID)),
		}); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Audit insert failed")
			return
		}

		if err = tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Commit failed")
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"communication_id":     id,
			"rate_request_id":      req.RateRequestID,
			"communication_mode":   mode,
			"communication_status": status,
			"processing_status":    processingStatus,
		})
	}
}

// ListCommunications returns non-deleted communications for a rate request, including recipients.
func ListCommunications(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			RateRequestID        string `json:"rate_request_id"`
			CommunicationStatus  string `json:"communication_status"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil && err != io.EOF {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}
		if _, ok := requireSessionEmail(w, r); !ok {
			return
		}

		ctx := r.Context()
		rateRequestID := strings.TrimSpace(req.RateRequestID)
		statusFilter := normalizeCommunicationStatus(req.CommunicationStatus)
		where := `WHERE is_deleted = false`
		args := make([]interface{}, 0, 2)
		if rateRequestID != "" {
			args = append(args, rateRequestID)
			where += ` AND rate_request_id = $1::uuid`
		}
		if statusFilter != "" {
			args = append(args, statusFilter)
			if len(args) == 1 {
				where += ` AND communication_status = $1`
			} else {
				where += ` AND communication_status = $2`
			}
		}
		var (
			rows pgx.Rows
			err  error
		)
		rows, err = pgxPool.Query(ctx, communicationSelect+where+`
			ORDER BY created_at DESC, communication_id DESC`, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to list communications")
			return
		}
		defer rows.Close()

		items := make([]scannedCommunication, 0)
		ids := make([]string, 0)
		for rows.Next() {
			it, err := scanCommunication(rows)
			if err != nil {
				continue
			}
			items = append(items, it)
			ids = append(ids, it.id)
		}
		if err = rows.Err(); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to list communications")
			return
		}

		recMap, err := loadRecipientsMap(ctx, pgxPool, ids)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to list recipients")
			return
		}
		procMap := map[string]string{}
		if len(ids) > 0 {
			arows, aerr := pgxPool.Query(ctx, `
				SELECT DISTINCT ON (communication_id)
					communication_id::text, COALESCE(processing_status,'')
				FROM investment.fd_rate_communication_audit
				WHERE communication_id = ANY($1::uuid[])
				ORDER BY communication_id, requested_at DESC`, ids)
			if aerr == nil {
				defer arows.Close()
				for arows.Next() {
					var cid, ps string
					if arows.Scan(&cid, &ps) == nil {
						procMap[cid] = ps
					}
				}
			}
		}

		results := make([]map[string]interface{}, 0, len(items))
		for _, it := range items {
			row := it.asMap(recMap[it.id])
			if ps := procMap[it.id]; ps != "" {
				row["processing_status"] = ps
			} else {
				row["processing_status"] = it.status
			}
			results = append(results, row)
		}
		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"results": results,
		})
	}
}

// UpdateCommunication applies edits immediately and writes EDIT audit with old_/new_ pairs
// (processing_status PENDING_EDIT_APPROVAL). Recipients are replaced when provided.
func UpdateCommunication(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req communicationPayload
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if strings.TrimSpace(req.CommunicationID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "communication_id is required")
			return
		}

		userEmail, ok := requireSessionEmail(w, r)
		if !ok {
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to start transaction")
			return
		}
		defer tx.Rollback(ctx)

		var (
			oldRateRequestID, oldMode, oldStatus string
			oldTemplateID, oldTemplateName       *string
			oldTemplateVersion, oldContent       *string
			oldResponseSource                    *string
			oldResponseDate                      *time.Time
			oldEmailMessageID                    *string
			oldSentBy                            *string
			oldSentAt                            *time.Time
		)
		err = tx.QueryRow(ctx, `
			SELECT rate_request_id::text, communication_mode, communication_status,
				email_template_id, email_template_name, email_template_version, email_content,
				response_source, response_date, email_message_id::text,
				sent_by, sent_at
			FROM investment.fd_rate_communication
			WHERE communication_id = $1::uuid AND is_deleted = false
			FOR UPDATE`, req.CommunicationID).Scan(
			&oldRateRequestID, &oldMode, &oldStatus,
			&oldTemplateID, &oldTemplateName, &oldTemplateVersion, &oldContent,
			&oldResponseSource, &oldResponseDate, &oldEmailMessageID,
			&oldSentBy, &oldSentAt,
		)
		if err != nil {
			api.RespondWithError(w, http.StatusNotFound, "Communication not found")
			return
		}

		if inferCommunicationIntent(req) == "RECEIVE" ||
			(normalizeCommunicationStatus(req.CommunicationStatus) == "RESPONSE_RECEIVED" && oldStatus != "RESPONSE_RECEIVED") {
			api.RespondWithError(w, http.StatusBadRequest, "capture each bank response as a new RECEIVE communication; the original send is not overwritten")
			return
		}

		newMode := oldMode
		if strings.TrimSpace(req.CommunicationMode) != "" {
			newMode = normalizeCommunicationMode(req.CommunicationMode)
			if !validCommunicationMode(newMode) {
				api.RespondWithError(w, http.StatusBadRequest, "communication_mode must be API, SYSTEM_EMAIL or MANUAL")
				return
			}
		}

		newStatus := oldStatus
		if strings.TrimSpace(req.CommunicationStatus) != "" {
			newStatus = normalizeCommunicationStatus(req.CommunicationStatus)
		}
		if newStatus == "SENT" && oldStatus == "PENDING_APPROVAL" {
			api.RespondWithError(w, http.StatusBadRequest, "send is not dispatched until checker approve")
			return
		}
		if !validCommunicationStatus(newStatus) {
			api.RespondWithError(w, http.StatusBadRequest, "communication_status is invalid")
			return
		}

		newTemplateID := keepOrReplace(derefOrEmpty(oldTemplateID), req.templateID())
		newTemplateName := keepOrReplace(derefOrEmpty(oldTemplateName), req.EmailTemplateName)
		newTemplateVersion := keepOrReplace(derefOrEmpty(oldTemplateVersion), req.EmailTemplateVersion)
		newContent := keepOrReplace(derefOrEmpty(oldContent), req.EmailContent)

		newResponseSource := derefOrEmpty(oldResponseSource)
		if strings.TrimSpace(req.ResponseSource) != "" {
			newResponseSource = normalizeResponseSource(req.ResponseSource)
			if !validResponseSource(newResponseSource) {
				api.RespondWithError(w, http.StatusBadRequest, "response_source must be EMAIL, PHONE or DOCUMENT")
				return
			}
		}
		if newStatus == "RESPONSE_RECEIVED" && newResponseSource == "" {
			api.RespondWithError(w, http.StatusBadRequest, "response_source is required when communication_status is RESPONSE_RECEIVED")
			return
		}

		var newResponseDate interface{}
		if oldResponseDate != nil {
			newResponseDate = *oldResponseDate
		}
		if strings.TrimSpace(req.ResponseDate) != "" {
			parsed, err := parseOptionalTimestamp(req.ResponseDate)
			if err != nil {
				api.RespondWithError(w, http.StatusBadRequest, err.Error())
				return
			}
			newResponseDate = parsed
		}

		newEmailMessageID := keepOrReplace(derefOrEmpty(oldEmailMessageID), req.EmailMessageID)

		if req.recipientsProvided() {
			recs := collectRecipients(req)
			if newMode == "SYSTEM_EMAIL" && countTO(recs) == 0 {
				api.RespondWithError(w, http.StatusBadRequest, "at least one TO recipient is required to send SYSTEM_EMAIL")
				return
			}
			if err = replaceCommunicationRecipients(ctx, tx, req.CommunicationID, recs); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Recipient update failed")
				return
			}
		}

		var sentBy, sentAt interface{}
		if oldSentBy != nil {
			sentBy = *oldSentBy
		}
		if oldSentAt != nil {
			sentAt = *oldSentAt
		}

		_, err = tx.Exec(ctx, `
			UPDATE investment.fd_rate_communication SET
				communication_mode = $2,
				communication_status = $3,
				email_template_id = NULLIF($4,''),
				email_template_name = NULLIF($5,''),
				email_template_version = NULLIF($6,''),
				email_content = NULLIF($7,''),
				response_source = NULLIF($8,''),
				response_date = $9,
				email_message_id = NULLIF($10,'')::uuid,
				sent_by = $11,
				sent_at = $12,
				updated_by = $13,
				updated_at = now()
			WHERE communication_id = $1::uuid`,
			req.CommunicationID,
			newMode, newStatus,
			newTemplateID, newTemplateName, newTemplateVersion, newContent,
			newResponseSource, newResponseDate, newEmailMessageID,
			sentBy, sentAt, userEmail,
		)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, fmt.Sprintf("Update failed: %v", err))
			return
		}

		var oldRespDate interface{}
		if oldResponseDate != nil {
			oldRespDate = *oldResponseDate
		}

		if err = insertCommunicationAudit(ctx, tx, communicationAuditValues{
			CommunicationID:   req.CommunicationID,
			RateRequestID:     oldRateRequestID,
			ActionType:        "EDIT",
			ProcessingStatus:  "PENDING_EDIT_APPROVAL",
			RequestedBy:       userEmail,
			RequestedIP:       api.ClientIPFromContext(ctx),
			OldMode:           oldMode,
			NewMode:           newMode,
			OldStatus:         oldStatus,
			NewStatus:         newStatus,
			OldTemplateID:     nullIfEmpty(derefOrEmpty(oldTemplateID)),
			NewTemplateID:     nullIfEmpty(newTemplateID),
			OldContent:        nullIfEmpty(derefOrEmpty(oldContent)),
			NewContent:        nullIfEmpty(newContent),
			OldResponseSource: nullIfEmpty(derefOrEmpty(oldResponseSource)),
			NewResponseSource: nullIfEmpty(newResponseSource),
			OldResponseDate:   oldRespDate,
			NewResponseDate:   newResponseDate,
			OldEmailMessageID: nullIfEmpty(derefOrEmpty(oldEmailMessageID)),
			NewEmailMessageID: nullIfEmpty(newEmailMessageID),
		}); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Audit insert failed")
			return
		}

		if err = tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Commit failed")
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"communication_id":     req.CommunicationID,
			"rate_request_id":      oldRateRequestID,
			"communication_mode":   newMode,
			"communication_status": newStatus,
			"processing_status":    "PENDING_EDIT_APPROVAL",
		})
	}
}

// ListCommunicationAudit returns old_/new_ audit rows for one communication or request.
func ListCommunicationAudit(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			CommunicationID string `json:"communication_id"`
			RateRequestID   string `json:"rate_request_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil && err != io.EOF {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}
		communicationID := strings.TrimSpace(req.CommunicationID)
		rateRequestID := strings.TrimSpace(req.RateRequestID)
		if communicationID == "" && rateRequestID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "communication_id or rate_request_id is required")
			return
		}

		ctx := r.Context()
		query := `
			SELECT
				audit_id::text,
				communication_id::text,
				rate_request_id::text,
				action_type,
				processing_status,
				requested_by,
				TO_CHAR(requested_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS requested_at,
				COALESCE(requested_ip,''),
				old_communication_mode,
				new_communication_mode,
				old_communication_status,
				new_communication_status,
				old_email_template_id,
				new_email_template_id,
				old_email_content,
				new_email_content,
				old_response_source,
				new_response_source,
				COALESCE(TO_CHAR(old_response_date,'YYYY-MM-DD"T"HH24:MI:SS"Z"'),''),
				COALESCE(TO_CHAR(new_response_date,'YYYY-MM-DD"T"HH24:MI:SS"Z"'),''),
				COALESCE(old_email_message_id::text,''),
				COALESCE(new_email_message_id::text,''),
				COALESCE(checker_by,''),
				COALESCE(TO_CHAR(checker_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"'),''),
				COALESCE(checker_comment,''),
				COALESCE(checker_ip,'')
			FROM investment.fd_rate_communication_audit`
		var (
			rows pgx.Rows
			err  error
		)
		if communicationID != "" {
			rows, err = pgxPool.Query(ctx, query+`
			WHERE communication_id = $1::uuid
			ORDER BY requested_at DESC, audit_id DESC`, communicationID)
		} else {
			rows, err = pgxPool.Query(ctx, query+`
			WHERE rate_request_id = $1::uuid
			ORDER BY requested_at DESC, audit_id DESC`, rateRequestID)
		}
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to fetch communication audit")
			return
		}
		defer rows.Close()

		fds := rows.FieldDescriptions()
		results := make([]map[string]interface{}, 0)
		for rows.Next() {
			vals, err := rows.Values()
			if err != nil {
				continue
			}
			m := make(map[string]interface{}, len(fds))
			for i, fd := range fds {
				if vals[i] == nil {
					m[string(fd.Name)] = nil
				} else {
					m[string(fd.Name)] = vals[i]
				}
			}
			results = append(results, m)
		}
		api.RespondWithPayload(w, true, "", results)
	}
}
