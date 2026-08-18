package fdRateNegotiation

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

type documentRow struct {
	FileID          string `json:"file_id"`
	FileName        string `json:"file_name"`
	StoredFileName  string `json:"stored_file_name"`
	ContentType     string `json:"content_type"`
	FileSize        int64  `json:"file_size"`
	CommunicationID string `json:"communication_id"`
	OfferID         string `json:"offer_id"`
	UploadedBy      string `json:"uploaded_by"`
	UploadedAt      string `json:"uploaded_at"`
}

// ListDocuments returns rate-negotiation files, optionally scoped to a received communication.
func ListDocuments(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			RateRequestID   string `json:"rate_request_id"`
			CommunicationID string `json:"communication_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil && err != io.EOF {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}
		if _, ok := requireSessionEmail(w, r); !ok {
			return
		}
		rateRequestID := strings.TrimSpace(req.RateRequestID)
		communicationID := strings.TrimSpace(req.CommunicationID)
		if rateRequestID == "" && communicationID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "rate_request_id or communication_id is required")
			return
		}

		ctx := r.Context()
		query := `
			SELECT
				file_id::text,
				COALESCE(stored_file_name,''),
				COALESCE(stored_file_name,''),
				COALESCE(content_type,''),
				COALESCE(file_size,0),
				COALESCE(communication_id::text,''),
				COALESCE(offer_id::text,''),
				COALESCE(uploaded_by,''),
				COALESCE(TO_CHAR(uploaded_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"'),'')
			FROM investment.fd_rate_negotiation_files
			WHERE COALESCE(is_deleted,false) = false`
		args := make([]interface{}, 0, 2)
		if rateRequestID != "" {
			args = append(args, rateRequestID)
			query += ` AND rate_request_id = $1::uuid`
			if communicationID != "" {
				args = append(args, communicationID)
				query += ` AND communication_id = $2::uuid`
			}
		} else {
			args = append(args, communicationID)
			query += ` AND communication_id = $1::uuid`
		}
		query += ` ORDER BY uploaded_at DESC, file_id DESC`

		rows, err := pgxPool.Query(ctx, query, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to list documents")
			return
		}
		defer rows.Close()

		results := make([]documentRow, 0)
		for rows.Next() {
			var it documentRow
			if err := rows.Scan(
				&it.FileID, &it.FileName, &it.StoredFileName, &it.ContentType,
				&it.FileSize, &it.CommunicationID, &it.OfferID, &it.UploadedBy, &it.UploadedAt,
			); err != nil {
				continue
			}
			results = append(results, it)
		}
		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"results": results,
			"files":   results,
		})
	}
}

// LinkDocuments attaches already-uploaded files to a received communication (and optional offer).
func LinkDocuments(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			RateRequestID   string   `json:"rate_request_id"`
			CommunicationID string   `json:"communication_id"`
			OfferID         string   `json:"offer_id"`
			FileIDs         []string `json:"file_ids"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if _, ok := requireSessionEmail(w, r); !ok {
			return
		}
		rateRequestID := strings.TrimSpace(req.RateRequestID)
		communicationID := strings.TrimSpace(req.CommunicationID)
		if rateRequestID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "rate_request_id is required")
			return
		}
		if communicationID == "" && strings.TrimSpace(req.OfferID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "communication_id or offer_id is required")
			return
		}

		ids := make([]string, 0, len(req.FileIDs))
		for _, id := range req.FileIDs {
			id = strings.TrimSpace(id)
			if id != "" {
				ids = append(ids, id)
			}
		}

		ctx := r.Context()
		var commStatus string
		if communicationID != "" {
			err := pgxPool.QueryRow(ctx, `
				SELECT communication_status
				FROM investment.fd_rate_communication
				WHERE communication_id = $1::uuid
				  AND rate_request_id = $2::uuid
				  AND COALESCE(is_deleted,false)=false`,
				communicationID, rateRequestID).Scan(&commStatus)
			if err != nil {
				api.RespondWithError(w, http.StatusBadRequest, "communication not found for this rate request")
				return
			}
		}

		var tagged int64
		if len(ids) > 0 {
			tag, err := pgxPool.Exec(ctx, `
				UPDATE investment.fd_rate_negotiation_files
				SET communication_id = COALESCE(NULLIF($2,'')::uuid, communication_id),
				    offer_id = COALESCE(NULLIF($3,'')::uuid, offer_id)
				WHERE rate_request_id = $1::uuid
				  AND file_id = ANY($4::uuid[])
				  AND COALESCE(is_deleted,false)=false`,
				rateRequestID, communicationID, strings.TrimSpace(req.OfferID), ids)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Failed to link documents")
				return
			}
			tagged = tag.RowsAffected()
		} else if communicationID != "" {
			tag, err := pgxPool.Exec(ctx, `
				UPDATE investment.fd_rate_negotiation_files
				SET communication_id = $2::uuid
				WHERE rate_request_id = $1::uuid
				  AND communication_id IS NULL
				  AND COALESCE(is_deleted,false)=false
				  AND uploaded_at >= now() - interval '15 minutes'`,
				rateRequestID, communicationID)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Failed to link documents")
				return
			}
			tagged = tag.RowsAffected()
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"linked":              tagged,
			"communication_id":    communicationID,
			"communication_status": commStatus,
		})
	}
}
