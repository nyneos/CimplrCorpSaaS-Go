package fdRateNegotiation

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type communicationDecisionReq struct {
	CommunicationIDs []string `json:"communication_ids"`
	CommunicationID  string   `json:"communication_id"`
	Comment          string   `json:"comment"`
}

func collectCommunicationIDs(req communicationDecisionReq) []string {
	ids := make([]string, 0, len(req.CommunicationIDs)+1)
	seen := map[string]struct{}{}
	for _, id := range req.CommunicationIDs {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	if extra := strings.TrimSpace(req.CommunicationID); extra != "" {
		if _, ok := seen[extra]; !ok {
			ids = append(ids, extra)
		}
	}
	return ids
}

func stampCommunicationAudit(ctx context.Context, tx pgx.Tx, communicationID, processing, checker, comment, ip string) (actionType, oldStatus, newStatus string, err error) {
	err = tx.QueryRow(ctx, `
		UPDATE investment.fd_rate_communication_audit
		SET processing_status = $2,
		    checker_by = $3,
		    checker_at = now(),
		    checker_comment = NULLIF($4,''),
		    checker_ip = NULLIF($5,'')
		WHERE audit_id = (
			SELECT audit_id
			FROM investment.fd_rate_communication_audit
			WHERE communication_id = $1::uuid
			AND processing_status IN ('PENDING_APPROVAL','PENDING_EDIT_APPROVAL','PENDING_DELETE_APPROVAL')
			ORDER BY requested_at DESC
			LIMIT 1
		)
		RETURNING COALESCE(action_type,''), COALESCE(old_communication_status,''), COALESCE(new_communication_status,'')`,
		communicationID, processing, checker, comment, ip,
	).Scan(&actionType, &oldStatus, &newStatus)
	return actionType, oldStatus, newStatus, err
}

func revertCommunicationEdit(ctx context.Context, tx pgx.Tx, communicationID string) error {
	_, err := tx.Exec(ctx, `
		UPDATE investment.fd_rate_communication m SET
			communication_mode = COALESCE(a.old_communication_mode, m.communication_mode),
			communication_status = COALESCE(NULLIF(a.old_communication_status,''), m.communication_status),
			email_template_id = a.old_email_template_id,
			email_content = a.old_email_content,
			response_source = a.old_response_source,
			response_date = a.old_response_date,
			email_message_id = a.old_email_message_id,
			updated_at = now()
		FROM (
			SELECT *
			FROM investment.fd_rate_communication_audit
			WHERE communication_id = $1::uuid
			  AND action_type = 'EDIT'
			ORDER BY requested_at DESC
			LIMIT 1
		) a
		WHERE m.communication_id = $1::uuid`,
		communicationID,
	)
	return err
}

func decideCommunications(pgxPool *pgxpool.Pool, approve bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req communicationDecisionReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		ids := collectCommunicationIDs(req)
		if len(ids) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "communication_id is required")
			return
		}
		userEmail, ok := requireSessionEmail(w, r)
		if !ok {
			return
		}
		processing := "APPROVED"
		if !approve {
			processing = "REJECTED"
		}
		ctx := r.Context()
		acted := 0
		toSend := make([][5]string, 0)
		for _, id := range ids {
			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				continue
			}
			actionType, _, newStatus, stampErr := stampCommunicationAudit(
				ctx, tx, id, processing, userEmail, req.Comment, api.ClientIPFromContext(ctx),
			)
			if stampErr != nil {
				tx.Rollback(ctx)
				continue
			}

			var rateRequestID, mode, status, templateID, content string
			if err = tx.QueryRow(ctx, `
				SELECT rate_request_id::text, communication_mode, communication_status,
					COALESCE(email_template_id,''), COALESCE(email_content,'')
				FROM investment.fd_rate_communication
				WHERE communication_id = $1::uuid AND COALESCE(is_deleted,false)=false
				FOR UPDATE`, id).Scan(&rateRequestID, &mode, &status, &templateID, &content); err != nil {
				tx.Rollback(ctx)
				continue
			}

			switch strings.ToUpper(actionType) {
			case "CREATE", "EDIT":
				isEdit := strings.ToUpper(actionType) == "EDIT"
				if approve {
					if status == "PENDING_APPROVAL" || status == "DRAFT" {
						if _, err = tx.Exec(ctx, `
							UPDATE investment.fd_rate_communication
							SET communication_status = 'SENT', sent_by = $2, sent_at = $3, updated_by = $2, updated_at = now()
							WHERE communication_id = $1::uuid`,
							id, userEmail, time.Now()); err != nil {
							tx.Rollback(ctx)
							continue
						}
						if _, err = tx.Exec(ctx, `
							UPDATE investment.fd_rate_negotiation
							SET request_status = 'SENT_TO_BANKS', updated_by = $2, updated_at = now()
							WHERE rate_request_id = $1::uuid
							  AND request_status IN ('APPROVED','SUBMITTED','SENT_TO_BANKS')`,
							rateRequestID, userEmail); err != nil {
							tx.Rollback(ctx)
							continue
						}
						toSend = append(toSend, [5]string{id, rateRequestID, userEmail, templateID, content})
					}
				} else if isEdit {
					if err = revertCommunicationEdit(ctx, tx, id); err != nil {
						tx.Rollback(ctx)
						continue
					}
				} else {
					if _, err = tx.Exec(ctx, `
						UPDATE investment.fd_rate_communication
						SET communication_status = 'REJECTED', updated_by = $2, updated_at = now()
						WHERE communication_id = $1::uuid`,
						id, userEmail); err != nil {
						tx.Rollback(ctx)
						continue
					}
				}
			case "DELETE":
				if approve {
					if _, err = tx.Exec(ctx, `
						UPDATE investment.fd_rate_communication
						SET is_deleted = true, updated_by = $2, updated_at = now()
						WHERE communication_id = $1::uuid`,
						id, userEmail); err != nil {
						tx.Rollback(ctx)
						continue
					}
				}
			}

			_ = newStatus
			if err = tx.Commit(ctx); err != nil {
				continue
			}
			acted++
		}

		if approve {
			for _, item := range toSend {
				if strings.EqualFold(item[0], "") {
					continue
				}
				// mode check: fire only SYSTEM_EMAIL; fireBankCommunicationEmail no-ops empty recipients
				var mode string
				_ = pgxPool.QueryRow(ctx, `
					SELECT communication_mode FROM investment.fd_rate_communication
					WHERE communication_id = $1::uuid`, item[0]).Scan(&mode)
				if strings.ToUpper(mode) == "SYSTEM_EMAIL" {
					fireBankCommunicationEmail(pgxPool, item[0], item[1], item[2], item[3], item[4])
				}
			}
		}

		msg := ""
		if acted == 0 {
			if approve {
				msg = "No communications were approved"
			} else {
				msg = "No communications were rejected"
			}
		}
		api.RespondWithPayload(w, acted > 0, msg, map[string]interface{}{
			"updated": acted,
			"status":  processing,
		})
	}
}

func ApproveCommunications(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return decideCommunications(pgxPool, true)
}

func RejectCommunications(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return decideCommunications(pgxPool, false)
}

// DeleteCommunications writes DELETE audits in PENDING_DELETE_APPROVAL.
// is_deleted is applied only when the checker approves.
func DeleteCommunications(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req communicationDecisionReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		ids := collectCommunicationIDs(req)
		if len(ids) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "communication_id is required")
			return
		}
		userEmail, ok := requireSessionEmail(w, r)
		if !ok {
			return
		}
		ctx := r.Context()
		acted := 0
		for _, id := range ids {
			var rateRequestID, status string
			err := pgxPool.QueryRow(ctx, `
				SELECT rate_request_id::text, communication_status
				FROM investment.fd_rate_communication
				WHERE communication_id = $1::uuid AND COALESCE(is_deleted,false)=false`,
				id).Scan(&rateRequestID, &status)
			if err != nil {
				continue
			}
			var pending int
			_ = pgxPool.QueryRow(ctx, `
				SELECT COUNT(*) FROM investment.fd_rate_communication_audit
				WHERE communication_id = $1::uuid
				  AND action_type = 'DELETE'
				  AND processing_status LIKE 'PENDING%'`, id).Scan(&pending)
			if pending > 0 {
				continue
			}
			var latestProc string
			_ = pgxPool.QueryRow(ctx, `
				SELECT COALESCE(processing_status,'')
				FROM investment.fd_rate_communication_audit
				WHERE communication_id = $1::uuid
				ORDER BY requested_at DESC, audit_id DESC
				LIMIT 1`, id).Scan(&latestProc)
			if strings.EqualFold(strings.TrimSpace(latestProc), "APPROVED") {
				continue
			}
			_, err = pgxPool.Exec(ctx, `
				INSERT INTO investment.fd_rate_communication_audit (
					communication_id, rate_request_id, action_type, processing_status,
					requested_by, requested_at, requested_ip,
					old_communication_status, new_communication_status
				) VALUES (
					$1::uuid, $2::uuid, 'DELETE', 'PENDING_DELETE_APPROVAL',
					$3, now(), $4, $5, $5
				)`,
				id, rateRequestID, userEmail, api.ClientIPFromContext(ctx), status)
			if err != nil {
				continue
			}
			acted++
		}
		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"updated":           acted,
			"processing_status": "PENDING_DELETE_APPROVAL",
		})
	}
}
