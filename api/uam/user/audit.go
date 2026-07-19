package user

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"database/sql"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// GetUserAuditHistory synthesises audit events from the users table fields.
// POST /uam/users/audit-history   Body: { user_id?: string }
func GetUserAuditHistory(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if api.GetSessionFromCtx(r.Context()) == nil {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		var req struct {
			UserID string `json:"user_id"`
		}
		json.NewDecoder(r.Body).Decode(&req) //nolint:errcheck

		filter := strings.TrimSpace(req.UserID)

		q := `
			SELECT entity_id, actiontype, processing_status,
			       requested_by, requested_at, display_name
			FROM (
				SELECT id::text AS entity_id, 'CREATE' AS actiontype, status AS processing_status,
				       COALESCE(created_by, '') AS requested_by, created_at AS requested_at,
				       COALESCE(employee_name, username_or_employee_id, '') AS display_name
				FROM users
				WHERE COALESCE(is_deleted, false) = false

				UNION ALL

				SELECT id::text, 'EDIT', status,
				       COALESCE(updated_by, ''), updated_at,
				       COALESCE(employee_name, username_or_employee_id, '')
				FROM users
				WHERE COALESCE(is_deleted, false) = false AND updated_by IS NOT NULL

				UNION ALL

				SELECT id::text, 'APPROVE', 'APPROVED',
				       COALESCE(approved_by, ''), approved_at,
				       COALESCE(employee_name, username_or_employee_id, '')
				FROM users
				WHERE COALESCE(is_deleted, false) = false AND approved_by IS NOT NULL
			) t
			WHERE $1 = '' OR entity_id = $1
			ORDER BY requested_at DESC NULLS LAST
			LIMIT 200`

		rows, err := pool.Query(r.Context(), q, filter)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		type auditRow struct {
			EntityID         string  `json:"entity_id"`
			Actiontype       string  `json:"actiontype"`
			ProcessingStatus string  `json:"processing_status"`
			RequestedBy      string  `json:"requested_by"`
			RequestedAt      *string `json:"requested_at"`
			DisplayName      string  `json:"display_name"`
		}

		out := make([]auditRow, 0)
		for rows.Next() {
			var a auditRow
			var reqAt sql.NullTime
			if err := rows.Scan(&a.EntityID, &a.Actiontype, &a.ProcessingStatus,
				&a.RequestedBy, &reqAt, &a.DisplayName); err != nil {
				continue
			}
			if reqAt.Valid {
				s := reqAt.Time.UTC().Format("2006-01-02T15:04:05Z")
				a.RequestedAt = &s
			}
			out = append(out, a)
		}

		api.RespondEnvelopeSuccess(w, "Success", out)
	}
}
