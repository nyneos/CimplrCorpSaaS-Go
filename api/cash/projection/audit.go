package projection

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/constants"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type projectionAuditRequest struct {
	UserID     string `json:"user_id"`
	ProposalID string `json:"proposal_id"`
}

func GetProjectionAuditHandler(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}

		var req projectionAuditRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSON+": "+err.Error())
			return
		}

		valid := false
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				valid = true
				break
			}
		}
		if !valid {
			api.RespondWithError(w, http.StatusForbidden, constants.ErrUnauthorized)
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

			payload = append(payload, map[string]interface{}{
				"entity_id":    entityID,
				"action":       action,
				"status":       status,
				"performed_by": performedBy,
				"performed_at": performedAt,
				"checker_by":   ifaceToString(checkerBy),
				"checker_at":   ifaceToTimeString(checkerAt),
				"comment":      ifaceToString(checkerComment),
				"reason":       ifaceToString(reason),
			})
		}
		if err := rows.Err(); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to read projection audit history")
			return
		}

		api.RespondWithPayload(w, true, "", payload)
	}
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

func insertProjectionDownloadAudit(ctx context.Context, pgxPool *pgxpool.Pool, proposalID, requestedBy string) {
	if strings.TrimSpace(proposalID) == "" || strings.TrimSpace(requestedBy) == "" {
		return
	}

	_, err := pgxPool.Exec(ctx, `
		INSERT INTO cimplrcorpsaas.audit_action_cashflow_proposal (proposal_id, action_type, processing_status, requested_by, requested_at)
		VALUES ($1, 'DOWNLOAD', 'COMPLETED', $2, now())
	`, proposalID, requestedBy)
	if err != nil {
		log.Printf("failed to insert projection download audit for %s: %v", proposalID, err)
	}
}
