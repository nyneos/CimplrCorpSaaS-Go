package exposures

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/api/fx/auditutil"
	"CimplrCorpSaas/internal/ctxutil"

	"github.com/jackc/pgx/v5/pgxpool"
)

type hedgingProposalLineInput struct {
	BusinessUnit           string      `json:"business_unit"`
	Currency               string      `json:"currency"`
	ExposureType           string      `json:"exposure_type"`
	ContributingHeaderIDs  interface{} `json:"contributing_header_ids"`
	HedgeMonth1            float64     `json:"hedge_month1"`
	HedgeMonth2            float64     `json:"hedge_month2"`
	HedgeMonth3            float64     `json:"hedge_month3"`
	HedgeMonth4            float64     `json:"hedge_month4"`
	HedgeMonth4to6         float64     `json:"hedge_month4to6"`
	HedgeMonth6plus        float64     `json:"hedge_month6plus"`
	OldHedgeMonth1         float64     `json:"old_hedge_month1"`
	OldHedgeMonth2         float64     `json:"old_hedge_month2"`
	OldHedgeMonth3         float64     `json:"old_hedge_month3"`
	OldHedgeMonth4         float64     `json:"old_hedge_month4"`
	OldHedgeMonth4to6      float64     `json:"old_hedge_month4to6"`
	OldHedgeMonth6plus     float64     `json:"old_hedge_month6plus"`
	Status                 string      `json:"status"`
	Comments               *string     `json:"comments"`
}

func normalizeHeaderIDs(raw interface{}) []string {
	out := make([]string, 0)
	switch v := raw.(type) {
	case nil:
		return out
	case string:
		for _, part := range strings.Split(v, ",") {
			part = strings.TrimSpace(part)
			if part != "" {
				out = append(out, part)
			}
		}
	case []string:
		for _, part := range v {
			part = strings.TrimSpace(part)
			if part != "" {
				out = append(out, part)
			}
		}
	case []interface{}:
		for _, item := range v {
			part := strings.TrimSpace(stringifyAny(item))
			if part != "" {
				out = append(out, part)
			}
		}
	default:
		part := strings.TrimSpace(stringifyAny(v))
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func stringifyAny(v interface{}) string {
	switch t := v.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	default:
		b, err := json.Marshal(t)
		if err != nil {
			return ""
		}
		s := strings.Trim(string(b), `"`)
		return s
	}
}

func documentSnapshot(ctx context.Context, pool *pgxpool.Pool, proposalID string) map[string]any {
	header := auditutil.FetchRowSnapshotPGX(ctx, pool, "public.hedging_proposal_document", "proposal_id", proposalID)
	lines := make([]map[string]any, 0)
	rows, err := pool.Query(ctx, `
		SELECT row_to_json(t)
		FROM (
			SELECT * FROM public.hedging_proposal_document_line
			WHERE proposal_id = $1::uuid
			ORDER BY line_id
		) t
	`, proposalID)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var raw []byte
			if err := rows.Scan(&raw); err != nil || len(raw) == 0 {
				continue
			}
			var m map[string]any
			if json.Unmarshal(raw, &m) == nil {
				lines = append(lines, m)
			}
		}
	}
	return map[string]any{
		"header": header,
		"lines":  lines,
	}
}

func replaceDocumentLines(ctx context.Context, pool *pgxpool.Pool, proposalID string, lines []hedgingProposalLineInput) error {
	if _, err := pool.Exec(ctx, `DELETE FROM public.hedging_proposal_document_line WHERE proposal_id = $1::uuid`, proposalID); err != nil {
		return err
	}
	for _, line := range lines {
		status := strings.TrimSpace(line.Status)
		if status == "" {
			status = "pending"
		}
		ids := normalizeHeaderIDs(line.ContributingHeaderIDs)
		var comments any
		if line.Comments != nil {
			comments = *line.Comments
		}
		_, err := pool.Exec(ctx, `
			INSERT INTO public.hedging_proposal_document_line (
				proposal_id, business_unit, currency, exposure_type, contributing_header_ids,
				hedge_month1, hedge_month2, hedge_month3, hedge_month4, hedge_month4to6, hedge_month6plus,
				old_hedge_month1, old_hedge_month2, old_hedge_month3, old_hedge_month4, old_hedge_month4to6, old_hedge_month6plus,
				line_status, comments, updated_at
			) VALUES (
				$1::uuid, $2, $3, $4, $5,
				$6, $7, $8, $9, $10, $11,
				$12, $13, $14, $15, $16, $17,
				$18, $19, NOW()
			)
		`, proposalID, line.BusinessUnit, line.Currency, line.ExposureType, ids,
			line.HedgeMonth1, line.HedgeMonth2, line.HedgeMonth3, line.HedgeMonth4, line.HedgeMonth4to6, line.HedgeMonth6plus,
			line.OldHedgeMonth1, line.OldHedgeMonth2, line.OldHedgeMonth3, line.OldHedgeMonth4, line.OldHedgeMonth4to6, line.OldHedgeMonth6plus,
			status, comments)
		if err != nil {
			return err
		}
	}
	return nil
}

// SaveHedgingProposalDocument creates or updates a named proposal (Save = DRAFT / keep status).
func SaveHedgingProposalDocument(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID         string                     `json:"user_id"`
			ProposalID     string                     `json:"proposal_id"`
			ProposalName   string                     `json:"proposal_name"`
			Comments       string                     `json:"comments"`
			Submit         bool                       `json:"submit"`
			Proposals      []hedgingProposalLineInput `json:"proposals"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.UserID) == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrPleaseLogin)
			return
		}
		name := strings.TrimSpace(req.ProposalName)
		if name == "" {
			respondWithError(w, http.StatusBadRequest, "proposal_name is required")
			return
		}
		if len(req.Proposals) == 0 {
			respondWithError(w, http.StatusBadRequest, "at least one proposal line is required")
			return
		}

		actor := auditutil.Actor(req.UserID)
		status := "DRAFT"
		actionType := "CREATE"
		if req.Submit {
			status = constants.StatusPendingApproval
			if status == "" {
				status = "PENDING_APPROVAL"
			}
		}

		proposalID := strings.TrimSpace(req.ProposalID)
		var oldSnap map[string]any

		if proposalID == "" {
			err := pool.QueryRow(ctx, `
				INSERT INTO public.hedging_proposal_document (
					proposal_name, processing_status, created_by, comments, created_at
				) VALUES ($1, $2, $3, NULLIF($4, ''), NOW())
				RETURNING proposal_id::text
			`, name, status, actor, strings.TrimSpace(req.Comments)).Scan(&proposalID)
			if err != nil {
				respondWithError(w, http.StatusInternalServerError, "failed to create hedging proposal")
				return
			}
			if req.Submit {
				actionType = "SUBMIT"
			}
		} else {
			oldSnap = documentSnapshot(ctx, pool, proposalID)
			actionType = "EDIT"
			if req.Submit {
				actionType = "SUBMIT"
			}
			tag, err := pool.Exec(ctx, `
				UPDATE public.hedging_proposal_document
				SET proposal_name = $1,
				    processing_status = CASE WHEN $2 THEN $3 ELSE processing_status END,
				    comments = NULLIF($4, ''),
				    updated_by = $5,
				    updated_at = NOW()
				WHERE proposal_id = $6::uuid
				  AND COALESCE(is_deleted, false) = false
			`, name, req.Submit, status, strings.TrimSpace(req.Comments), actor, proposalID)
			if err != nil || tag.RowsAffected() == 0 {
				respondWithError(w, http.StatusNotFound, "hedging proposal not found")
				return
			}
		}

		if err := replaceDocumentLines(ctx, pool, proposalID, req.Proposals); err != nil {
			respondWithError(w, http.StatusInternalServerError, "failed to save proposal lines")
			return
		}

		newSnap := documentSnapshot(ctx, pool, proposalID)
		finalStatus := status
		if !req.Submit {
			_ = pool.QueryRow(ctx, `SELECT processing_status FROM public.hedging_proposal_document WHERE proposal_id = $1::uuid`, proposalID).Scan(&finalStatus)
		}
		auditutil.RecordActionPGX(ctx, pool, auditutil.ActionParams{
			TableName:    auditutil.TableHedgeProposalDocument,
			ParentColumn: "proposal_id",
			ParentID:     proposalID,
			ActionType:   actionType,
			Status:       finalStatus,
			Reason:       strings.TrimSpace(req.Comments),
			RequestedBy:  actor,
			OldValues:    oldSnap,
			NewValues:    newSnap,
		})

		respondWithSuccess(w, http.StatusOK, "Hedging proposal saved", map[string]any{
			"proposal_id":       proposalID,
			"proposal_name":     name,
			"processing_status": finalStatus,
		})
	}
}

// ListHedgingProposalDocuments returns All-page rows.
func ListHedgingProposalDocuments(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.UserID) == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrPleaseLogin)
			return
		}
		_ = ctxutil.FromContext(ctx)

		rows, err := pool.Query(ctx, `
			SELECT
				proposal_id::text,
				proposal_name,
				processing_status,
				created_by,
				created_at,
				updated_by,
				updated_at,
				comments
			FROM public.hedging_proposal_document
			WHERE COALESCE(is_deleted, false) = false
			ORDER BY created_at DESC
		`)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "failed to list hedging proposals")
			return
		}
		defer rows.Close()

		pageData := make([]map[string]any, 0)
		for rows.Next() {
			var (
				id, name, status, createdBy string
				updatedBy                   *string
				createdAt                   time.Time
				updatedAt                   *time.Time
				comments                    *string
			)
			if err := rows.Scan(&id, &name, &status, &createdBy, &createdAt, &updatedBy, &updatedAt, &comments); err != nil {
				continue
			}
			row := map[string]any{
				"proposal_id":       id,
				"proposal_name":     name,
				"processing_status": status,
				"created_by":        createdBy,
				"created_at":        createdAt,
			}
			if updatedBy != nil {
				row["updated_by"] = *updatedBy
			}
			if updatedAt != nil {
				row["updated_at"] = *updatedAt
			}
			if comments != nil {
				row["comments"] = *comments
			}
			pageData = append(pageData, row)
		}

		respondWithSuccess(w, http.StatusOK, "Success", map[string]any{
			"pageData": pageData,
		})
	}
}

// GetHedgingProposalDocument returns one document + lines for edit/view.
func GetHedgingProposalDocument(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID     string `json:"user_id"`
			ProposalID string `json:"proposal_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.UserID) == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrPleaseLogin)
			return
		}
		proposalID := strings.TrimSpace(req.ProposalID)
		if proposalID == "" {
			respondWithError(w, http.StatusBadRequest, "proposal_id is required")
			return
		}

		var (
			name, status, createdBy string
			updatedBy               *string
			createdAt               time.Time
			updatedAt               *time.Time
			comments                *string
		)
		err := pool.QueryRow(ctx, `
			SELECT proposal_name, processing_status, created_by, created_at, updated_by, updated_at, comments
			FROM public.hedging_proposal_document
			WHERE proposal_id = $1::uuid AND COALESCE(is_deleted, false) = false
		`, proposalID).Scan(&name, &status, &createdBy, &createdAt, &updatedBy, &updatedAt, &comments)
		if err != nil {
			respondWithError(w, http.StatusNotFound, "hedging proposal not found")
			return
		}

		lineRows, err := pool.Query(ctx, `
			SELECT
				business_unit, currency, exposure_type, contributing_header_ids,
				hedge_month1, hedge_month2, hedge_month3, hedge_month4, hedge_month4to6, hedge_month6plus,
				old_hedge_month1, old_hedge_month2, old_hedge_month3, old_hedge_month4, old_hedge_month4to6, old_hedge_month6plus,
				line_status, comments
			FROM public.hedging_proposal_document_line
			WHERE proposal_id = $1::uuid
			ORDER BY line_id
		`, proposalID)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "failed to load proposal lines")
			return
		}
		defer lineRows.Close()

		proposals := make([]map[string]any, 0)
		for lineRows.Next() {
			var (
				bu, cur, expType, lineStatus string
				ids                          []string
				m1, m2, m3, m4, m46, m6p     float64
				o1, o2, o3, o4, o46, o6p     float64
				lineComments                 *string
			)
			if err := lineRows.Scan(
				&bu, &cur, &expType, &ids,
				&m1, &m2, &m3, &m4, &m46, &m6p,
				&o1, &o2, &o3, &o4, &o46, &o6p,
				&lineStatus, &lineComments,
			); err != nil {
				continue
			}
			row := map[string]any{
				"business_unit":           bu,
				"currency":                cur,
				"exposure_type":           expType,
				"contributing_header_ids": ids,
				"hedge_month1":            m1,
				"hedge_month2":            m2,
				"hedge_month3":            m3,
				"hedge_month4":            m4,
				"hedge_month4to6":         m46,
				"hedge_month6plus":        m6p,
				"old_hedge_month1":        o1,
				"old_hedge_month2":        o2,
				"old_hedge_month3":        o3,
				"old_hedge_month4":        o4,
				"old_hedge_month4to6":     o46,
				"old_hedge_month6plus":    o6p,
				"status":                  lineStatus,
			}
			if lineComments != nil {
				row["comments"] = *lineComments
			}
			proposals = append(proposals, row)
		}

		doc := map[string]any{
			"proposal_id":       proposalID,
			"proposal_name":     name,
			"processing_status": status,
			"created_by":        createdBy,
			"created_at":        createdAt,
			"proposals":         proposals,
		}
		if updatedBy != nil {
			doc["updated_by"] = *updatedBy
		}
		if updatedAt != nil {
			doc["updated_at"] = *updatedAt
		}
		if comments != nil {
			doc["comments"] = *comments
		}

		respondWithSuccess(w, http.StatusOK, "Success", doc)
	}
}

func updateHedgingProposalDocumentStatuses(
	ctx context.Context,
	pool *pgxpool.Pool,
	ids []string,
	status string,
	actor string,
	comments string,
	actionType string,
) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	count := 0
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		oldSnap := documentSnapshot(ctx, pool, id)
		tag, err := pool.Exec(ctx, `
			UPDATE public.hedging_proposal_document
			SET processing_status = $1,
			    comments = COALESCE(NULLIF($2, ''), comments),
			    updated_by = $3,
			    updated_at = NOW()
			WHERE proposal_id = $4::uuid
			  AND COALESCE(is_deleted, false) = false
		`, status, comments, actor, id)
		if err != nil || tag.RowsAffected() == 0 {
			continue
		}
		newSnap := documentSnapshot(ctx, pool, id)
		auditutil.RecordActionPGX(ctx, pool, auditutil.ActionParams{
			TableName:    auditutil.TableHedgeProposalDocument,
			ParentColumn: "proposal_id",
			ParentID:     id,
			ActionType:   actionType,
			Status:       status,
			Reason:       comments,
			RequestedBy:  actor,
			OldValues:    oldSnap,
			NewValues:    newSnap,
		})
		if actionType == "CONFIRM" || actionType == "REJECT" {
			auditutil.RecordDecisionPGX(ctx, pool, auditutil.DecisionParams{
				TableName:    auditutil.TableHedgeProposalDocument,
				ParentColumn: "proposal_id",
				ParentID:     id,
				Status:       status,
				CheckerBy:    actor,
				Comment:      comments,
			})
		}
		count++
	}
	return count, nil
}

// ApproveHedgingProposalDocuments approves selected proposal documents.
func ApproveHedgingProposalDocuments(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID      string   `json:"user_id"`
			ProposalIDs []string `json:"proposal_ids"`
			Comments    string   `json:"comments"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.UserID) == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrPleaseLogin)
			return
		}
		if len(req.ProposalIDs) == 0 {
			respondWithError(w, http.StatusBadRequest, "proposal_ids is required")
			return
		}
		actor := auditutil.Actor(req.UserID)
		n, err := updateHedgingProposalDocumentStatuses(ctx, pool, req.ProposalIDs, constants.StatusApproved, actor, strings.TrimSpace(req.Comments), "CONFIRM")
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "failed to approve hedging proposals")
			return
		}
		respondWithSuccess(w, http.StatusOK, fmt.Sprintf("%d proposal(s) approved", n), map[string]any{"count": n})
	}
}

// RejectHedgingProposalDocuments rejects selected proposal documents.
func RejectHedgingProposalDocuments(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID      string   `json:"user_id"`
			ProposalIDs []string `json:"proposal_ids"`
			Comments    string   `json:"comments"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.UserID) == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrPleaseLogin)
			return
		}
		if len(req.ProposalIDs) == 0 {
			respondWithError(w, http.StatusBadRequest, "proposal_ids is required")
			return
		}
		actor := auditutil.Actor(req.UserID)
		n, err := updateHedgingProposalDocumentStatuses(ctx, pool, req.ProposalIDs, constants.StatusRejected, actor, strings.TrimSpace(req.Comments), "REJECT")
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "failed to reject hedging proposals")
			return
		}
		respondWithSuccess(w, http.StatusOK, fmt.Sprintf("%d proposal(s) rejected", n), map[string]any{"count": n})
	}
}

// DeleteHedgingProposalDocuments marks selected proposals for delete approval / soft-delete pending.
func DeleteHedgingProposalDocuments(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID      string   `json:"user_id"`
			ProposalIDs []string `json:"proposal_ids"`
			Comments    string   `json:"comments"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.UserID) == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrPleaseLogin)
			return
		}
		if len(req.ProposalIDs) == 0 {
			respondWithError(w, http.StatusBadRequest, "proposal_ids is required")
			return
		}
		actor := auditutil.Actor(req.UserID)
		n, err := updateHedgingProposalDocumentStatuses(ctx, pool, req.ProposalIDs, constants.StatusPendingDeleteApproval, actor, strings.TrimSpace(req.Comments), "DELETE")
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "failed to delete hedging proposals")
			return
		}
		respondWithSuccess(w, http.StatusOK, fmt.Sprintf("%d proposal(s) marked for delete", n), map[string]any{"count": n})
	}
}
