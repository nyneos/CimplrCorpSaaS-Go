package projection

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/constants"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"CimplrCorpSaas/internal/logger"
)

type projectionAuditRequest struct {
	UserID     string `json:"user_id"`
	ProposalID string `json:"proposal_id"`
}

func GetProjectionAuditHandler(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		var req projectionAuditRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSON+": "+err.Error())
			return
		}

		if strings.TrimSpace(req.ProposalID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "proposal_id cannot be empty")
			return
		}

		ctx := r.Context()
		rows, err := pgxPool.Query(ctx, `
			SELECT
				proposal_id,
				action_type,
				processing_status,
				requested_by,
				requested_at,
				checker_by,
				checker_at,
				checker_comment,
				reason
			FROM cimplrcorpsaas.audit_action_cashflow_proposal
			WHERE proposal_id = $1
			ORDER BY requested_at ASC, action_id ASC
		`, req.ProposalID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrDBPrefix+err.Error())
			return
		}
		defer rows.Close()

		payload := make([]map[string]interface{}, 0)
		for rows.Next() {
			var entityID, action, status, performedBy string
			var performedAt time.Time
			var checkerBy, checkerComment, reason interface{}
			var checkerAt interface{}
			if err := rows.Scan(&entityID, &action, &status, &performedBy, &performedAt, &checkerBy, &checkerAt, &checkerComment, &reason); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "failed to read projection audit history")
				return
			}

			entry := map[string]interface{}{
				"entity_id":         entityID,
				"action_type":       action,
				"processing_status": status,
				"requested_by":      performedBy,
				"requested_at":      performedAt,
				"checker_by":        ifaceToString(checkerBy),
				"checker_at":        ifaceToTimeString(checkerAt),
				"checker_comment":   ifaceToString(checkerComment),
				"reason":            ifaceToString(reason),
			}
			if strings.EqualFold(action, "EDIT") {
				if changes := buildProjectionChangeSummary(ctx, pgxPool, req.ProposalID); len(changes) > 0 {
					entry["change_summary"] = changes
				}
			}

			payload = append(payload, entry)

			if decisionAction := projectionDecisionAction(action, status, checkerAt); decisionAction != "" {
				payload = append(payload, map[string]interface{}{
					"entity_id":         entityID,
					"action_type":       decisionAction,
					"processing_status": status,
					"requested_by":      projectionFirstNonEmpty(ifaceToString(checkerBy), performedBy),
					"requested_at":      ifaceToTimeString(checkerAt),
					"checker_by":        ifaceToString(checkerBy),
					"checker_at":        ifaceToTimeString(checkerAt),
					"checker_comment":   ifaceToString(checkerComment),
					"reason":            ifaceToString(reason),
				})
			}
		}
		if err := rows.Err(); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to read projection audit history")
			return
		}

		downloadRows, err := pgxPool.Query(ctx, `
			SELECT proposal_id, requested_by, requested_at, file_name, upload_s3_key
			FROM cimplrcorpsaas.audit_cashflow_proposal_downloads
			WHERE proposal_id = $1
			ORDER BY requested_at ASC, download_audit_id ASC
		`, req.ProposalID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrFailedToReadProjectionAuditHistory)
			return
		}
		defer downloadRows.Close()

		for downloadRows.Next() {
			var entityID, requestedBy string
			var requestedAt sql.NullTime
			var fileName, uploadKey sql.NullString
			if err := downloadRows.Scan(&entityID, &requestedBy, &requestedAt, &fileName, &uploadKey); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, constants.ErrFailedToReadProjectionAuditHistory)
				return
			}

			payload = append(payload, map[string]interface{}{
				"entity_id":         entityID,
				"action_type":       "DOWNLOAD",
				"processing_status": "COMPLETED",
				"requested_by":      strings.TrimSpace(requestedBy),
				"requested_at":      requestedAt.Time,
				"checker_by":        "",
				"checker_at":        nil,
				"checker_comment":   "",
				"reason":            "",
				"file_name":         fileName.String,
				"upload_s3_key":     uploadKey.String,
				"source":            "PROJECTION",
			})
		}
		if err := downloadRows.Err(); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrFailedToReadProjectionAuditHistory)
			return
		}

		// Standardize: always return 'rows' as the array field
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":    true,
			"audit_logs": payload,
		})
	}
}

func projectionDecisionAction(action, status string, checkerAt interface{}) string {
	if ifaceToTimeString(checkerAt) == "" {
		return ""
	}

	switch strings.ToUpper(strings.TrimSpace(action)) {
	case "APPROVE", "REJECT":
		return ""
	}

	switch strings.ToUpper(strings.TrimSpace(status)) {
	case constants.StatusApproved:
		return "APPROVE"
	case constants.StatusRejected:
		return "REJECT"
	default:
		return ""
	}
}

func projectionFirstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func projectionRequestedBy(userID string) string {
	for _, s := range auth.GetActiveSessions() {
		if s.UserID == userID {
			if strings.TrimSpace(s.Email) != "" {
				return strings.TrimSpace(s.Email)
			}
			if strings.TrimSpace(s.Name) != "" {
				return strings.TrimSpace(s.Name)
			}
			break
		}
	}
	return strings.TrimSpace(userID)
}

func insertProjectionDownloadAudit(ctx context.Context, pgxPool *pgxpool.Pool, proposalID, requestedBy, uploadS3Key string) {
	proposalID = strings.TrimSpace(proposalID)
	requestedBy = strings.TrimSpace(requestedBy)
	uploadS3Key = strings.TrimSpace(uploadS3Key)
	if proposalID == "" {
		return
	}
	if requestedBy == "" {
		if userID, ok := ctx.Value("user_id").(string); ok {
			requestedBy = strings.TrimSpace(userID)
		}
	}
	if requestedBy == "" {
		return
	}

	_, err := pgxPool.Exec(ctx, `
		INSERT INTO cimplrcorpsaas.audit_cashflow_proposal_downloads (proposal_id, requested_by, requested_at, file_name, upload_s3_key)
		VALUES ($1, $2, now(), $3, $4)
	`, proposalID, requestedBy, projectionExtractAuditFileName(uploadS3Key), projectionNullIfEmpty(uploadS3Key))
	if err != nil {
		logger.LogError("failed to insert projection download audit for %s: %v", proposalID, err)
	}
}

func projectionNullIfEmpty(value string) interface{} {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return strings.TrimSpace(value)
}

func projectionExtractAuditFileName(uploadS3Key string) interface{} {
	uploadS3Key = strings.TrimSpace(uploadS3Key)
	if uploadS3Key == "" {
		return nil
	}

	parts := strings.Split(uploadS3Key, "/")
	name := strings.TrimSpace(parts[len(parts)-1])
	if name == "" {
		return nil
	}
	return name
}

func buildProjectionChangeSummary(ctx context.Context, pgxPool *pgxpool.Pool, proposalID string) []map[string]interface{} {
	var (
		proposalName, oldProposalName     string
		effectiveDate, oldEffectiveDate   string
		currencyCode, oldCurrencyCode     string
		recurrenceType, oldRecurrenceType string
	)

	// Only changed fields will be included in the summary (not all fields)
	err := pgxPool.QueryRow(ctx, `
		SELECT
			COALESCE(proposal_name, ''),
			COALESCE(old_proposal_name, ''),
			COALESCE(TO_CHAR(effective_date, 'YYYY-MM-DD'), ''),
			COALESCE(TO_CHAR(old_effective_date, 'YYYY-MM-DD'), ''),
			COALESCE(currency_code, ''),
			COALESCE(old_currency_code, ''),
			COALESCE(recurrence_type, ''),
			COALESCE(old_recurrence_type, '')
		FROM cimplrcorpsaas.cashflow_proposal
		WHERE proposal_id = $1
	`, proposalID).Scan(
		&proposalName, &oldProposalName,
		&effectiveDate, &oldEffectiveDate,
		&currencyCode, &oldCurrencyCode,
		&recurrenceType, &oldRecurrenceType,
	)
	if err != nil {
		return nil
	}

	changes := make([]map[string]interface{}, 0)
	// Only append if changed (not all fields)
	appendProjectionChange(&changes, "Proposal Name", oldProposalName, proposalName)
	appendProjectionChange(&changes, "Effective Date", oldEffectiveDate, effectiveDate)
	appendProjectionChange(&changes, "Currency Code", oldCurrencyCode, currencyCode)
	appendProjectionChange(&changes, "Recurrence Type", oldRecurrenceType, recurrenceType)
	return changes
}

func appendProjectionChange(changes *[]map[string]interface{}, fieldName, oldValue, newValue string) {
	// Only append if both old and new are non-empty and different, or if one is empty and the other is not
	if strings.TrimSpace(oldValue) == strings.TrimSpace(newValue) {
		return
	}
	// If both are empty, skip
	if strings.TrimSpace(oldValue) == "" && strings.TrimSpace(newValue) == "" {
		return
	}
	*changes = append(*changes, map[string]interface{}{
		"field":     fieldName,
		"old_value": oldValue,
		"new_value": newValue,
	})
}
