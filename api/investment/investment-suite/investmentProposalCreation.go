package investmentsuite

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const amountTolerance = 0.01

// CreateProposalRequest models the payload for manual proposal creation
type CreateProposalRequest struct {
	UserID       string                    `json:"user_id"`
	ProposalName string                    `json:"proposal_name"`
	EntityName   string                    `json:"entity_name"`
	TotalAmount  float64                   `json:"total_amount"`
	HorizonDays  *int                      `json:"horizon_days"`
	Source       string                    `json:"source"`
	BatchID      string                    `json:"batch_id"`
	Reason       string                    `json:"reason"`
	Allocations  []ProposalAllocationInput `json:"allocations"`
}

// UpdateProposalRequest models the payload for editing proposals and allocations
type UpdateProposalRequest struct {
	UserID       string                          `json:"user_id"`
	ProposalID   string                          `json:"proposal_id"`
	ProposalName string                          `json:"proposal_name"`
	EntityName   string                          `json:"entity_name"`
	TotalAmount  float64                         `json:"total_amount"`
	HorizonDays  *int                            `json:"horizon_days"`
	Source       string                          `json:"source"`
	BatchID      string                          `json:"batch_id"`
	Reason       string                          `json:"reason"`
	Allocations  []ProposalAllocationUpsertInput `json:"allocations"`
}

// ProposalAllocationInput represents a single allocation row for CREATE operations
type ProposalAllocationInput struct {
	SchemeID           string   `json:"scheme_id"`
	SchemeInternalCode string   `json:"scheme_internal_code"`
	Amount             float64  `json:"amount"`
	Percent            *float64 `json:"percent"`
	PolicyStatus       *bool    `json:"policy_status"`
	PostTradeHolding   *float64 `json:"post_trade_holding"`
	CurrentHolding     *float64 `json:"current_holding"`
}

// ProposalAllocationUpsertInput represents allocation operations for updates
type ProposalAllocationUpsertInput struct {
	AllocationID        *int64   `json:"allocation_id"`
	SchemeID            string   `json:"scheme_id"`
	SchemeInternalCode  string   `json:"scheme_internal_code"`
	Amount              float64  `json:"amount"`
	Percent             *float64 `json:"percent"`
	PolicyStatus        *bool    `json:"policy_status"`
	PostTradeHolding    *float64 `json:"post_trade_holding"`
	OldPostTradeHolding *float64 `json:"old_post_trade_holding"`
	CurrentHolding      *float64 `json:"current_holding"`
	OldCurrentHolding   *float64 `json:"old_current_holding"`
}

// CreateProposalResponse captures the API response payload
type CreateProposalResponse struct {
	Success         bool    `json:"success"`
	ProposalID      string  `json:"proposal_id"`
	TotalAmount     float64 `json:"total_amount"`
	AllocationCount int     `json:"allocation_count"`
}

// UpdateProposalResponse captures the response payload for updates
type UpdateProposalResponse struct {
	Success         bool    `json:"success"`
	ProposalID      string  `json:"proposal_id"`
	TotalAmount     float64 `json:"total_amount"`
	AllocationCount int     `json:"allocation_count"`
}

// ProposalBulkActionRequest models checker actions on proposals
type ProposalBulkActionRequest struct {
	UserID      string   `json:"user_id"`
	ProposalIDs []string `json:"proposal_ids"`
	Comment     string   `json:"comment"`
}

// ProposalBulkActionResponse summarizes checker outcomes
type ProposalBulkActionResponse struct {
	Success        bool     `json:"success"`
	Action         string   `json:"action"`
	ApprovedIDs    []string `json:"approved_ids"`
	RejectedIDs    []string `json:"rejected_ids"`
	DeletedIDs     []string `json:"deleted_ids"`
	SkippedIDs     []string `json:"skipped_ids"`
	ProcessedCount int      `json:"processed_count"`
}

// ProposalBulkDeleteRequest lets makers request delete approvals in bulk
type ProposalBulkDeleteRequest struct {
	UserID      string   `json:"user_id"`
	ProposalIDs []string `json:"proposal_ids"`
	Reason      string   `json:"reason"`
}

// ProposalBulkDeleteResponse reports delete submission stats
type ProposalBulkDeleteResponse struct {
	Success        bool     `json:"success"`
	RequestedIDs   []string `json:"requested_ids"`
	SkippedIDs     []string `json:"skipped_ids"`
	SubmittedCount int      `json:"submitted_count"`
}

// ProposalDetailRequest models the payload for detail retrieval
type ProposalDetailRequest struct {
	ProposalID string `json:"proposal_id"`
}

// EntityHoldingsRequest models the payload to fetch holdings for an entity
type EntityHoldingsRequest struct {
	EntityName string `json:"entity_name"`
}

// EntitySchemeHolding captures per-scheme holding details for an entity
type EntitySchemeHolding struct {
	SchemeID       string  `json:"scheme_id"`
	SchemeName     string  `json:"scheme_name"`
	CurrentHolding float64 `json:"current_holding"`
}

// CreateInvestmentProposal exposes POST /investment/proposals
func CreateInvestmentProposal(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		ctx := r.Context()
		var req CreateProposalRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "invalid JSON body")
			return
		}

		normalizeProposalRequest(&req)
		if err := validateProposalRequest(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, err.Error())
			return
		}

		userEmail, ok := resolveUserEmail(req.UserID)
		if !ok {
			api.RespondWithError(w, http.StatusUnauthorized, "invalid or inactive user session")
			return
		}

		batchUUID, err := parseBatchID(req.BatchID)
		if err != nil {
			api.RespondWithError(w, http.StatusBadRequest, err.Error())
			return
		}

		tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to begin transaction")
			return
		}
		defer tx.Rollback(ctx)

		proposalID, err := insertProposal(ctx, tx, &req, batchUUID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, fmt.Sprintf("failed to create proposal: %v", err))
			return
		}

		allocIDs, err := insertAllocations(ctx, tx, proposalID, req.Allocations)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, fmt.Sprintf("failed to insert allocations: %v", err))
			return
		}

		if err := insertProposalAudit(ctx, tx, proposalID, userEmail, req.Reason, "CREATE", "PENDING_APPROVAL"); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, fmt.Sprintf("failed to insert audit record: %v", err))
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to commit proposal transaction")
			return
		}

		resp := CreateProposalResponse{Success: true, ProposalID: proposalID, TotalAmount: req.TotalAmount, AllocationCount: len(allocIDs)}
		api.RespondWithPayload(w, true, "", resp)

	}
}

// UpdateInvestmentProposal exposes POST /investment/proposals
func UpdateInvestmentProposal(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		ctx := r.Context()
		var req UpdateProposalRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "invalid JSON body")
			return
		}

		normalizeUpdateProposalRequest(&req)
		if err := validateUpdateProposalRequest(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, err.Error())
			return
		}

		userEmail, ok := resolveUserEmail(req.UserID)
		if !ok {
			api.RespondWithError(w, http.StatusUnauthorized, "invalid or inactive user session")
			return
		}

		batchUUID, err := parseBatchID(req.BatchID)
		if err != nil {
			api.RespondWithError(w, http.StatusBadRequest, err.Error())
			return
		}

		tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to begin transaction")
			return
		}
		defer tx.Rollback(ctx)

		if err := applyProposalUpdate(ctx, tx, &req, batchUUID); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, fmt.Sprintf("failed to update proposal: %v", err))
			return
		}

		if err := syncProposalAllocations(ctx, tx, req.ProposalID, req.Allocations); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, fmt.Sprintf("failed to upsert allocations: %v", err))
			return
		}

		if err := insertProposalAudit(ctx, tx, req.ProposalID, userEmail, req.Reason, "EDIT", "PENDING_EDIT_APPROVAL"); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, fmt.Sprintf("failed to insert audit record: %v", err))
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to commit proposal transaction")
			return
		}

		resp := UpdateProposalResponse{Success: true, ProposalID: req.ProposalID, TotalAmount: req.TotalAmount, AllocationCount: len(req.Allocations)}
		api.RespondWithPayload(w, true, "", resp)
	}
}

// BulkApproveProposals allows checkers to approve multiple proposals
func BulkApproveProposals(pool *pgxpool.Pool) http.HandlerFunc {
	return bulkProposalDecision(pool, "APPROVE")
}

// BulkRejectProposals allows checkers to reject multiple proposals
func BulkRejectProposals(pool *pgxpool.Pool) http.HandlerFunc {
	return bulkProposalDecision(pool, "REJECT")
}

func bulkProposalDecision(pool *pgxpool.Pool, action string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		ctx := r.Context()
		var req ProposalBulkActionRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "invalid JSON body")
			return
		}

		normalizeBulkActionRequest(&req)
		if err := validateBulkActionRequest(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, err.Error())
			return
		}

		userEmail, ok := resolveUserEmail(req.UserID)
		if !ok {
			api.RespondWithError(w, http.StatusUnauthorized, "invalid or inactive user session")
			return
		}

		tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to begin transaction")
			return
		}
		defer tx.Rollback(ctx)

		validIDs, invalidIDs, err := gatherProposalIDs(ctx, tx, req.ProposalIDs)
		if err != nil {
			api.RespondWithError(w, http.StatusBadRequest, err.Error())
			return
		}

		skipped := append([]string{}, invalidIDs...)
		const latestAuditSQL = `
			WITH ranked AS (
				SELECT action_id, proposal_id, actiontype, processing_status,
					ROW_NUMBER() OVER (PARTITION BY proposal_id ORDER BY requested_at DESC) AS rn
				FROM investment.auditactionproposal
				WHERE proposal_id = ANY($1)
			)
			SELECT action_id, proposal_id, actiontype, processing_status
			FROM ranked
			WHERE rn = 1
		`

		rows, err := tx.Query(ctx, latestAuditSQL, validIDs)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to fetch proposal audits")
			return
		}
		defer rows.Close()

		type auditSnapshot struct {
			actionID   string
			proposalID string
			actionType string
			status     string
		}

		latest := make(map[string]auditSnapshot)
		for rows.Next() {
			var snap auditSnapshot
			if err := rows.Scan(&snap.actionID, &snap.proposalID, &snap.actionType, &snap.status); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "failed to read audit rows")
				return
			}
			latest[snap.proposalID] = snap
		}
		if err := rows.Err(); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to iterate audit rows")
			return
		}

		approvedActions := make([]string, 0)
		deletedActions := make([]string, 0)
		rejectedActions := make([]string, 0)
		approvedProposals := make([]string, 0)
		deletedProposals := make([]string, 0)
		rejectedProposals := make([]string, 0)

		for _, proposalID := range validIDs {
			snap, ok := latest[proposalID]
			if !ok {
				skipped = append(skipped, proposalID)
				continue
			}
			if !strings.HasPrefix(snap.status, "PENDING") {
				skipped = append(skipped, proposalID)
				continue
			}

			if action == "APPROVE" {
				if snap.status == "PENDING_DELETE_APPROVAL" || strings.EqualFold(snap.actionType, "DELETE") {
					deletedActions = append(deletedActions, snap.actionID)
					deletedProposals = append(deletedProposals, proposalID)
				} else {
					approvedActions = append(approvedActions, snap.actionID)
					approvedProposals = append(approvedProposals, proposalID)
				}
			} else {
				rejectedActions = append(rejectedActions, snap.actionID)
				rejectedProposals = append(rejectedProposals, proposalID)
			}
		}

		if len(approvedActions) > 0 {
			if err := updateAuditStatuses(ctx, tx, approvedActions, "APPROVED", userEmail, req.Comment); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, err.Error())
				return
			}
		}
		if len(deletedActions) > 0 {
			if err := updateAuditStatuses(ctx, tx, deletedActions, "DELETED", userEmail, req.Comment); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, err.Error())
				return
			}
		}
		if len(rejectedActions) > 0 {
			if err := updateAuditStatuses(ctx, tx, rejectedActions, "REJECTED", userEmail, req.Comment); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, err.Error())
				return
			}
		}

		if len(approvedProposals) > 0 {
			if _, err := tx.Exec(ctx, `
					UPDATE investment.investment_proposal
					SET is_deleted=false, updated_at=now()
					WHERE proposal_id = ANY($1)
				`, approvedProposals); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "failed to update approved proposals")
				return
			}
		}
		if len(deletedProposals) > 0 {
			if _, err := tx.Exec(ctx, `
					UPDATE investment.investment_proposal
					SET is_deleted=true, updated_at=now()
					WHERE proposal_id = ANY($1)
				`, deletedProposals); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "failed to delete proposals")
				return
			}
		}
		if len(rejectedProposals) > 0 {
			if _, err := tx.Exec(ctx, `
					UPDATE investment.investment_proposal
					SET updated_at=now()
					WHERE proposal_id = ANY($1)
				`, rejectedProposals); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "failed to reject proposals")
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to commit bulk action")
			return
		}

		processedCount := len(approvedProposals) + len(deletedProposals) + len(rejectedProposals)
		response := ProposalBulkActionResponse{
			Success:        true,
			Action:         action,
			ApprovedIDs:    approvedProposals,
			RejectedIDs:    rejectedProposals,
			DeletedIDs:     deletedProposals,
			SkippedIDs:     uniqueStrings(skipped),
			ProcessedCount: processedCount,
		}
		api.RespondWithPayload(w, true, "", response)
	}
}

// BulkDeleteProposals lets makers raise delete requests for multiple proposals
func BulkDeleteProposals(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		ctx := r.Context()
		var req ProposalBulkDeleteRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "invalid JSON body")
			return
		}

		normalizeBulkDeleteRequest(&req)
		if err := validateBulkDeleteRequest(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, err.Error())
			return
		}

		userEmail, ok := resolveUserEmail(req.UserID)
		if !ok {
			api.RespondWithError(w, http.StatusUnauthorized, "invalid or inactive user session")
			return
		}

		tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to begin transaction")
			return
		}
		defer tx.Rollback(ctx)

		validIDs, invalidIDs, err := gatherProposalIDs(ctx, tx, req.ProposalIDs)
		if err != nil {
			api.RespondWithError(w, http.StatusBadRequest, err.Error())
			return
		}

		skipped := append([]string{}, invalidIDs...)
		const pendingDeleteSQL = `
			WITH ranked AS (
				SELECT proposal_id,
					ROW_NUMBER() OVER (PARTITION BY proposal_id ORDER BY requested_at DESC) AS rn,
					processing_status
				FROM investment.auditactionproposal
				WHERE proposal_id = ANY($1)
			)
			SELECT proposal_id
			FROM ranked
			WHERE rn = 1 AND processing_status = 'PENDING_DELETE_APPROVAL'
		`

		rows, err := tx.Query(ctx, pendingDeleteSQL, validIDs)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to inspect proposal delete state")
			return
		}
		defer rows.Close()

		pendingDelete := make(map[string]struct{})
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "failed to read delete state")
				return
			}
			pendingDelete[id] = struct{}{}
		}
		if err := rows.Err(); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to iterate delete state")
			return
		}

		readyForDelete := make([]string, 0, len(validIDs))
		for _, id := range validIDs {
			if _, exists := pendingDelete[id]; exists {
				skipped = append(skipped, id)
				continue
			}
			readyForDelete = append(readyForDelete, id)
		}

		if len(readyForDelete) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "no proposals eligible for delete submission")
			return
		}

		if _, err := tx.Exec(ctx, `
			UPDATE investment.investment_proposal
			SET updated_at=now()
			WHERE proposal_id = ANY($1)
		`, readyForDelete); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to tag proposals for delete")
			return
		}

		for _, id := range readyForDelete {
			if err := insertProposalAudit(ctx, tx, id, userEmail, req.Reason, "DELETE", "PENDING_DELETE_APPROVAL"); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, fmt.Sprintf("failed to insert delete audit for %s", id))
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to commit delete submissions")
			return
		}

		response := ProposalBulkDeleteResponse{
			Success:        true,
			RequestedIDs:   readyForDelete,
			SkippedIDs:     uniqueStrings(skipped),
			SubmittedCount: len(readyForDelete),
		}
		api.RespondWithPayload(w, true, "", response)
	}
}

// GetProposalMeta mirrors other master GETs: proposal fields + latest audit + history summary ordered by updated_at.
func GetProposalMeta(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		ctx := r.Context()
		const metaSQL = `
			WITH latest_audit AS (
				SELECT DISTINCT ON (proposal_id)
					proposal_id,
					actiontype,
					processing_status,
					action_id,
					requested_by,
					requested_at,
					checker_by,
					checker_at,
					checker_comment,
					reason
				FROM investment.auditactionproposal
				ORDER BY proposal_id, requested_at DESC
			),
			history AS (
				SELECT
					proposal_id,
					MAX(CASE WHEN actiontype='CREATE' THEN requested_by END) AS created_by,
					MAX(CASE WHEN actiontype='CREATE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS created_at,
					MAX(CASE WHEN actiontype='EDIT' THEN requested_by END) AS edited_by,
					MAX(CASE WHEN actiontype='EDIT' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS edited_at,
					MAX(CASE WHEN actiontype='DELETE' THEN requested_by END) AS deleted_by,
					MAX(CASE WHEN actiontype='DELETE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS deleted_at
				FROM investment.auditactionproposal
				GROUP BY proposal_id
			)
			SELECT
				p.proposal_id,
				p.proposal_name,
				p.old_proposal_name,
				p.entity_name,
				p.old_entity_name,
				p.total_amount,
				p.old_total_amount,
				p.horizon_days,
				p.old_horizon_days,
				p.source,
				p.old_source,
				-- status and old_status removed per schema change
				COALESCE(p.batch_id::text,'') AS batch_id,
				COALESCE(p.is_deleted,false) AS is_deleted,
				COALESCE(l.actiontype,'') AS action_type,
				COALESCE(l.processing_status,'') AS processing_status,
				COALESCE(l.action_id::text,'') AS action_id,
				COALESCE(l.requested_by,'') AS requested_by,
				COALESCE(TO_CHAR(l.requested_at,'YYYY-MM-DD HH24:MI:SS'),'') AS requested_at,
				COALESCE(l.checker_by,'') AS checker_by,
				COALESCE(TO_CHAR(l.checker_at,'YYYY-MM-DD HH24:MI:SS'),'') AS checker_at,
				COALESCE(l.checker_comment,'') AS checker_comment,
				COALESCE(l.reason,'') AS reason,
				COALESCE(h.created_by,'') AS created_by,
				COALESCE(h.created_at,'') AS created_at,
				COALESCE(h.edited_by,'') AS edited_by,
				COALESCE(h.edited_at,'') AS edited_at,
				COALESCE(h.deleted_by,'') AS deleted_by,
				COALESCE(h.deleted_at,'') AS deleted_at
			FROM investment.investment_proposal p
			LEFT JOIN latest_audit l ON l.proposal_id = p.proposal_id
			LEFT JOIN history h ON h.proposal_id = p.proposal_id
			ORDER BY l.requested_at DESC NULLS LAST
		`

		rows, err := pool.Query(ctx, metaSQL)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to fetch proposal meta: "+err.Error())
			return
		}
		defer rows.Close()

		fields := rows.FieldDescriptions()
		result := make([]map[string]interface{}, 0, 64)
		for rows.Next() {
			vals, err := rows.Values()
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "failed to read proposal meta")
				return
			}
			rec := make(map[string]interface{}, len(fields))
			for i, f := range fields {
				key := string(f.Name)
				switch v := vals[i].(type) {
				case nil:
					rec[key] = ""
				case time.Time:
					rec[key] = v.Format("2006-01-02 15:04:05")
				default:
					rec[key] = v
				}
			}
			result = append(result, rec)
		}

		if rows.Err() != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to iterate proposal meta: "+rows.Err().Error())
			return
		}

		api.RespondWithPayload(w, true, "", result)
	}
}

// GetApprovedProposalMeta returns only active proposals whose latest audit is approved.
func GetApprovedProposalMeta(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		ctx := r.Context()
		const metaSQL = `
			WITH latest_audit AS (
				SELECT DISTINCT ON (proposal_id)
					proposal_id,
					actiontype,
					processing_status,
					action_id,
					requested_by,
					requested_at,
					checker_by,
					checker_at,
					checker_comment,
					reason
				FROM investment.auditactionproposal
				ORDER BY proposal_id, requested_at DESC
			),
			history AS (
				SELECT
					proposal_id,
					MAX(CASE WHEN actiontype='CREATE' THEN requested_by END) AS created_by,
					MAX(CASE WHEN actiontype='CREATE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS created_at,
					MAX(CASE WHEN actiontype='EDIT' THEN requested_by END) AS edited_by,
					MAX(CASE WHEN actiontype='EDIT' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS edited_at,
					MAX(CASE WHEN actiontype='DELETE' THEN requested_by END) AS deleted_by,
					MAX(CASE WHEN actiontype='DELETE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS deleted_at
				FROM investment.auditactionproposal
				GROUP BY proposal_id
			)
			SELECT
				p.proposal_id,
				p.proposal_name,
				p.old_proposal_name,
				p.entity_name,
				p.old_entity_name,
				p.total_amount,
				p.old_total_amount,
				p.horizon_days,
				p.old_horizon_days,
				p.source,
				p.old_source,
				-- status and old_status removed per schema change
				COALESCE(p.batch_id::text,'') AS batch_id,
				COALESCE(p.is_deleted,false) AS is_deleted,
				COALESCE(l.actiontype,'') AS action_type,
				COALESCE(l.processing_status,'') AS processing_status,
				COALESCE(l.action_id::text,'') AS action_id,
				COALESCE(l.requested_by,'') AS requested_by,
				COALESCE(TO_CHAR(l.requested_at,'YYYY-MM-DD HH24:MI:SS'),'') AS requested_at,
				COALESCE(l.checker_by,'') AS checker_by,
				COALESCE(TO_CHAR(l.checker_at,'YYYY-MM-DD HH24:MI:SS'),'') AS checker_at,
				COALESCE(l.checker_comment,'') AS checker_comment,
				COALESCE(l.reason,'') AS reason,
				COALESCE(h.created_by,'') AS created_by,
				COALESCE(h.created_at,'') AS created_at,
				COALESCE(h.edited_by,'') AS edited_by,
				COALESCE(h.edited_at,'') AS edited_at,
				COALESCE(h.deleted_by,'') AS deleted_by,
				COALESCE(h.deleted_at,'') AS deleted_at
			FROM investment.investment_proposal p
			JOIN latest_audit l ON l.proposal_id = p.proposal_id
			LEFT JOIN history h ON h.proposal_id = p.proposal_id
			WHERE COALESCE(p.is_deleted,false)=false
				AND UPPER(l.processing_status)='APPROVED'
			ORDER BY l.requested_at DESC NULLS LAST
		`

		rows, err := pool.Query(ctx, metaSQL)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to fetch proposal meta: "+err.Error())
			return
		}
		defer rows.Close()

		fields := rows.FieldDescriptions()
		result := make([]map[string]interface{}, 0, 64)
		for rows.Next() {
			vals, err := rows.Values()
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "failed to read proposal meta")
				return
			}
			rec := make(map[string]interface{}, len(fields))
			for i, f := range fields {
				key := string(f.Name)
				switch v := vals[i].(type) {
				case nil:
					rec[key] = ""
				case time.Time:
					rec[key] = v.Format("2006-01-02 15:04:05")
				default:
					rec[key] = v
				}
			}
			result = append(result, rec)
		}

		if rows.Err() != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to iterate proposal meta: "+rows.Err().Error())
			return
		}

		api.RespondWithPayload(w, true, "", result)
	}
}

// GetProposalDetail returns master-style proposal data plus allocations array.
func GetProposalDetail(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req ProposalDetailRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "invalid JSON body")
			return
		}
		req.ProposalID = strings.TrimSpace(req.ProposalID)
		if req.ProposalID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "proposal_id is required")
			return
		}

		ctx := r.Context()
		const proposalSQL = `
			WITH latest_audit AS (
				SELECT DISTINCT ON (proposal_id)
					proposal_id,
					actiontype,
					processing_status,
					action_id,
					requested_by,
					requested_at,
					checker_by,
					checker_at,
					checker_comment,
					reason
				FROM investment.auditactionproposal
				ORDER BY proposal_id, requested_at DESC
			),
			history AS (
				SELECT
					proposal_id,
					MAX(CASE WHEN actiontype='CREATE' THEN requested_by END) AS created_by,
					MAX(CASE WHEN actiontype='CREATE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS created_at,
					MAX(CASE WHEN actiontype='EDIT' THEN requested_by END) AS edited_by,
					MAX(CASE WHEN actiontype='EDIT' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS edited_at,
					MAX(CASE WHEN actiontype='DELETE' THEN requested_by END) AS deleted_by,
					MAX(CASE WHEN actiontype='DELETE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS deleted_at
				FROM investment.auditactionproposal
				GROUP BY proposal_id
			)
			SELECT
				p.proposal_id,
				p.proposal_name,
				p.old_proposal_name,
				p.entity_name,
				p.old_entity_name,
				p.total_amount,
				p.old_total_amount,
				p.horizon_days,
				p.old_horizon_days,
				p.source,
				p.old_source,
				-- status and old_status removed per schema change
				COALESCE(p.is_deleted,false) AS is_deleted,
				COALESCE(p.batch_id::text,'') AS batch_id,
				COALESCE(l.actiontype,'') AS action_type,
				COALESCE(l.processing_status,'') AS processing_status,
				COALESCE(l.action_id::text,'') AS action_id,
				COALESCE(l.requested_by,'') AS requested_by,
				COALESCE(TO_CHAR(l.requested_at,'YYYY-MM-DD HH24:MI:SS'),'') AS requested_at,
				COALESCE(l.checker_by,'') AS checker_by,
				COALESCE(TO_CHAR(l.checker_at,'YYYY-MM-DD HH24:MI:SS'),'') AS checker_at,
				COALESCE(l.checker_comment,'') AS checker_comment,
				COALESCE(l.reason,'') AS reason,
				COALESCE(h.created_by,'') AS created_by,
				COALESCE(h.created_at,'') AS created_at,
				COALESCE(h.edited_by,'') AS edited_by,
				COALESCE(h.edited_at,'') AS edited_at,
				COALESCE(h.deleted_by,'') AS deleted_by,
				COALESCE(h.deleted_at,'') AS deleted_at
			FROM investment.investment_proposal p
			LEFT JOIN latest_audit l ON l.proposal_id = p.proposal_id
			LEFT JOIN history h ON h.proposal_id = p.proposal_id
			WHERE p.proposal_id = $1
		`

		rows, err := pool.Query(ctx, proposalSQL, req.ProposalID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to fetch proposal detail: "+err.Error())
			return
		}
		fields := rows.FieldDescriptions()
		if !rows.Next() {
			rows.Close()
			api.RespondWithError(w, http.StatusNotFound, "proposal not found")
			return
		}
		vals, err := rows.Values()
		if err != nil {
			rows.Close()
			api.RespondWithError(w, http.StatusInternalServerError, "failed to read proposal detail")
			return
		}
		proposal := make(map[string]interface{}, len(fields))
		for i, f := range fields {
			key := string(f.Name)
			switch v := vals[i].(type) {
			case nil:
				proposal[key] = ""
			case time.Time:
				proposal[key] = v.Format("2006-01-02 15:04:05")
			default:
				proposal[key] = v
			}
		}
		rows.Close()

		const allocSQL = `
			SELECT
				id,
				scheme_id,
				scheme_internal_code,
				amount,
				old_amount,
				percent,
				old_percent,
				policy_status,
				post_trade_holding,
				old_post_trade_holding,
				current_holding,
				old_current_holding
			FROM investment.investment_proposal_allocation
			WHERE proposal_id = $1
			ORDER BY id
		`
		allocRows, err := pool.Query(ctx, allocSQL, req.ProposalID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to fetch allocations: "+err.Error())
			return
		}
		allocFields := allocRows.FieldDescriptions()
		allocations := make([]map[string]interface{}, 0, 8)
		for allocRows.Next() {
			vals, err := allocRows.Values()
			if err != nil {
				allocRows.Close()
				api.RespondWithError(w, http.StatusInternalServerError, "failed to read allocations")
				return
			}
			rec := make(map[string]interface{}, len(allocFields))
			for i, f := range allocFields {
				key := string(f.Name)
				switch v := vals[i].(type) {
				case nil:
					rec[key] = ""
				case time.Time:
					rec[key] = v.Format("2006-01-02 15:04:05")
				default:
					rec[key] = v
				}
			}
			allocations = append(allocations, rec)
		}
		if allocRows.Err() != nil {
			allocRows.Close()
			api.RespondWithError(w, http.StatusInternalServerError, "failed to iterate allocations: "+allocRows.Err().Error())
			return
		}
		allocRows.Close()

		// Enrich allocations with scheme name and amc name
		schemeIDs := make([]string, 0)
		schemeSet := make(map[string]struct{})
		for _, a := range allocations {
			if sid, ok := a["scheme_id"].(string); ok && strings.TrimSpace(sid) != "" {
				if _, seen := schemeSet[sid]; !seen {
					schemeSet[sid] = struct{}{}
					schemeIDs = append(schemeIDs, sid)
				}
			}
		}
		schemeMap := make(map[string]map[string]interface{})
		if len(schemeIDs) > 0 {
			const schemeQ = `SELECT scheme_id, scheme_name, amc_name FROM investment.masterscheme WHERE scheme_id = ANY($1)`
			rows, err := pool.Query(ctx, schemeQ, schemeIDs)
			if err == nil {
				defer rows.Close()
				for rows.Next() {
					var sid, sname, amc string
					if err := rows.Scan(&sid, &sname, &amc); err != nil {
						continue
					}
					schemeMap[sid] = map[string]interface{}{"scheme_name": sname, "amc_name": amc}
				}
			}
		}

		// enrich allocations with schemes and placeholder for entity-specific accounts (will set below)
		for i, a := range allocations {
			if sid, ok := a["scheme_id"].(string); ok && sid != "" {
				if info, ok2 := schemeMap[sid]; ok2 {
					a["scheme_name"] = info["scheme_name"]
					a["amc_name"] = info["amc_name"]
				} else {
					a["scheme_name"] = ""
					a["amc_name"] = ""
				}
			} else {
				a["scheme_name"] = ""
				a["amc_name"] = ""
			}
			allocations[i] = a
		}

		payload := map[string]interface{}{
			"proposal":    proposal,
			"allocations": allocations,
		}

		// enrich with folios/schemes and demats for the same entity
		entityFolios := make([]map[string]interface{}, 0)
		entityDemats := make([]map[string]interface{}, 0)
		entityName := ""
		if v, ok := proposal["entity_name"].(string); ok {
			entityName = v
		}
		if strings.TrimSpace(entityName) != "" {
			// Fetch folios for this entity (no scheme array; include entity_name)
			const folioSQL = `
			SELECT m.folio_id, m.folio_number, m.default_subscription_account, m.default_redemption_account
			FROM investment.masterfolio m
			WHERE COALESCE(m.is_deleted,false)=false AND m.entity_name = $1
			ORDER BY m.folio_number
			`
			rows, err := pool.Query(ctx, folioSQL, entityName)
			if err == nil {
				defer rows.Close()
				for rows.Next() {
					var folioID, folioNumber, subAcct, redAcct string
					if err := rows.Scan(&folioID, &folioNumber, &subAcct, &redAcct); err != nil {
						continue
					}
					entityFolios = append(entityFolios, map[string]interface{}{
						"folio_id":                     folioID,
						"folio_number":                 folioNumber,
						"entity_name":                  entityName,
						"default_subscription_account": subAcct,
						"default_redemption_account":   redAcct,
					})
				}
			}

			// Fetch demat accounts for this entity
			const dematSQL = `
			SELECT demat_id, demat_account_number, default_settlement_account
			FROM investment.masterdemataccount
			WHERE COALESCE(is_deleted,false)=false AND entity_name = $1
			ORDER BY demat_account_number
			`
			dRows, derr := pool.Query(ctx, dematSQL, entityName)
			if derr == nil {
				defer dRows.Close()
				for dRows.Next() {
					var dematID, dematNumber, settlement string
					if err := dRows.Scan(&dematID, &dematNumber, &settlement); err != nil {
						continue
					}
					entityDemats = append(entityDemats, map[string]interface{}{
						"demat_id":                   dematID,
						"demat_account_number":       dematNumber,
						"default_settlement_account": settlement,
					})
				}
			}
		}

		// annotate demats with entity (do not include amc names)
		for i, d := range entityDemats {
			if _, ok := d["entity_name"]; !ok {
				d["entity_name"] = entityName
			}
			entityDemats[i] = d
		}

		payload["entity_folios"] = entityFolios
		payload["entity_demats"] = entityDemats

		// attach entity folios/demats into each allocation
		for i, a := range allocations {
			a["entity_folios"] = entityFolios
			a["entity_demats"] = entityDemats
			allocations[i] = a
		}
		api.RespondWithPayload(w, true, "", payload)
	}
}

// GetEntitySchemeHoldings exposes holdings aggregated per scheme for a given entity
func GetEntitySchemeHoldings(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req EntityHoldingsRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "invalid JSON body")
			return
		}

		entityName := strings.TrimSpace(req.EntityName)
		if entityName == "" {
			api.RespondWithError(w, http.StatusBadRequest, "entity_name is required")
			return
		}

		ctx := r.Context()
		const holdingsSQL = `
WITH snapshot_agg AS (
	SELECT 
		scheme_id,
		scheme_name,
		SUM(current_value) AS current_holding
	FROM investment.portfolio_snapshot
	WHERE entity_name = $1
	GROUP BY scheme_id, scheme_name
),
all_schemes AS (
	SELECT DISTINCT
		COALESCE(s.scheme_id, ms.scheme_id::text) AS scheme_id,
		COALESCE(NULLIF(s.scheme_name,''), ms.scheme_name) AS scheme_name,
		COALESCE(s.current_holding, 0) AS current_holding
	FROM snapshot_agg s
	FULL OUTER JOIN investment.masterscheme ms
		ON ms.scheme_id::text = s.scheme_id OR ms.scheme_name = s.scheme_name
	WHERE ms.status = 'Active'
		AND EXISTS (
			SELECT 1 FROM investment.auditactionscheme a
			WHERE a.scheme_id = ms.scheme_id::text
				AND a.processing_status = 'APPROVED'
			ORDER BY a.requested_at DESC
			LIMIT 1
		)
)
SELECT 
	COALESCE(scheme_id, '') AS scheme_id,
	COALESCE(scheme_name, '') AS scheme_name,
	COALESCE(current_holding, 0) AS current_holding
FROM all_schemes
WHERE scheme_name IS NOT NULL AND scheme_name != ''
ORDER BY scheme_name
`

		rows, err := pool.Query(ctx, holdingsSQL, entityName)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to fetch holdings: "+err.Error())
			return
		}
		defer rows.Close()

		holdings := make([]EntitySchemeHolding, 0, 16)
		for rows.Next() {
			var rec EntitySchemeHolding
			if err := rows.Scan(&rec.SchemeID, &rec.SchemeName, &rec.CurrentHolding); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "failed to read holdings rows")
				return
			}
			holdings = append(holdings, rec)
		}
		if err := rows.Err(); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to iterate holdings rows: "+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", holdings)
	}
}

// GetEntityAccounts returns basic entity info plus folios and demats for an entity
func GetEntityAccounts(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req EntityHoldingsRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "invalid JSON body")
			return
		}

		entityName := strings.TrimSpace(req.EntityName)
		if entityName == "" {
			api.RespondWithError(w, http.StatusBadRequest, "entity_name is required")
			return
		}

		ctx := r.Context()

		// Fetch folios for this entity
		entityFolios := make([]map[string]interface{}, 0)
		const folioSQL = `
			SELECT m.folio_id, m.folio_number, m.default_subscription_account, m.default_redemption_account
			FROM investment.masterfolio m
			WHERE COALESCE(m.is_deleted,false)=false AND m.entity_name = $1
			ORDER BY m.folio_number
		`
		rows, err := pool.Query(ctx, folioSQL, entityName)
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var folioID, folioNumber, subAcct, redAcct string
				if err := rows.Scan(&folioID, &folioNumber, &subAcct, &redAcct); err != nil {
					continue
				}
				entityFolios = append(entityFolios, map[string]interface{}{
					"folio_id":                     folioID,
					"folio_number":                 folioNumber,
					"entity_name":                  entityName,
					"default_subscription_account": subAcct,
					"default_redemption_account":   redAcct,
				})
			}
		}

		// Fetch demats for this entity
		entityDemats := make([]map[string]interface{}, 0)
		const dematSQL = `
			SELECT demat_id, demat_account_number, default_settlement_account
			FROM investment.masterdemataccount
			WHERE COALESCE(is_deleted,false)=false AND entity_name = $1
			ORDER BY demat_account_number
		`
		dRows, derr := pool.Query(ctx, dematSQL, entityName)
		if derr == nil {
			defer dRows.Close()
			for dRows.Next() {
				var dematID, dematNumber, settlement string
				if err := dRows.Scan(&dematID, &dematNumber, &settlement); err != nil {
					continue
				}
				rec := map[string]interface{}{
					"demat_id":                   dematID,
					"demat_account_number":       dematNumber,
					"default_settlement_account": settlement,
					"entity_name":                entityName,
				}
				entityDemats = append(entityDemats, rec)
			}
		}

		payload := map[string]interface{}{
			"entity":        map[string]interface{}{"entity_name": entityName},
			"entity_folios": entityFolios,
			"entity_demats": entityDemats,
		}
		api.RespondWithPayload(w, true, "", payload)
	}
}
func normalizeProposalRequest(req *CreateProposalRequest) {
	req.UserID = strings.TrimSpace(req.UserID)
	req.ProposalName = strings.TrimSpace(req.ProposalName)
	req.EntityName = strings.TrimSpace(req.EntityName)
	req.Source = defaultString(strings.TrimSpace(req.Source), "Manual")
	req.BatchID = strings.TrimSpace(req.BatchID)
	req.Reason = strings.TrimSpace(req.Reason)
	for i := range req.Allocations {
		req.Allocations[i].SchemeID = strings.TrimSpace(req.Allocations[i].SchemeID)
		req.Allocations[i].SchemeInternalCode = strings.TrimSpace(req.Allocations[i].SchemeInternalCode)
	}
}

func normalizeUpdateProposalRequest(req *UpdateProposalRequest) {
	req.UserID = strings.TrimSpace(req.UserID)
	req.ProposalID = strings.TrimSpace(req.ProposalID)
	req.ProposalName = strings.TrimSpace(req.ProposalName)
	req.EntityName = strings.TrimSpace(req.EntityName)
	req.Source = defaultString(strings.TrimSpace(req.Source), "Manual")
	req.BatchID = strings.TrimSpace(req.BatchID)
	req.Reason = strings.TrimSpace(req.Reason)
	for i := range req.Allocations {
		req.Allocations[i].SchemeID = strings.TrimSpace(req.Allocations[i].SchemeID)
		req.Allocations[i].SchemeInternalCode = strings.TrimSpace(req.Allocations[i].SchemeInternalCode)
	}
}

func validateProposalRequest(req *CreateProposalRequest) error {
	if req.UserID == "" {
		return fmt.Errorf("user_id is required")
	}
	if req.ProposalName == "" {
		return fmt.Errorf("proposal_name is required")
	}
	if req.EntityName == "" {
		return fmt.Errorf("entity_name is required")
	}
	if req.TotalAmount <= 0 {
		return fmt.Errorf("total_amount must be greater than zero")
	}
	if len(req.Allocations) == 0 {
		return fmt.Errorf("at least one allocation is required")
	}
	sum := 0.0
	for idx, alloc := range req.Allocations {
		if alloc.Amount <= 0 {
			return fmt.Errorf("allocation at position %d must have amount greater than zero", idx+1)
		}
		if alloc.SchemeID == "" && alloc.SchemeInternalCode == "" {
			return fmt.Errorf("allocation at position %d must provide scheme_id or scheme_internal_code", idx+1)
		}
		sum += alloc.Amount
	}
	if math.Abs(sum-req.TotalAmount) > amountTolerance {
		return fmt.Errorf("allocation total %.2f must equal total_amount %.2f", sum, req.TotalAmount)
	}
	return nil
}

func validateUpdateProposalRequest(req *UpdateProposalRequest) error {
	if req.UserID == "" {
		return fmt.Errorf("user_id is required")
	}
	if req.ProposalID == "" {
		return fmt.Errorf("proposal_id is required")
	}
	if req.ProposalName == "" {
		return fmt.Errorf("proposal_name is required")
	}
	if req.EntityName == "" {
		return fmt.Errorf("entity_name is required")
	}
	if req.TotalAmount <= 0 {
		return fmt.Errorf("total_amount must be greater than zero")
	}
	if len(req.Allocations) == 0 {
		return fmt.Errorf("at least one allocation is required")
	}
	sum := 0.0
	for idx, alloc := range req.Allocations {
		if alloc.Amount <= 0 {
			return fmt.Errorf("allocation at position %d must have amount greater than zero", idx+1)
		}
		if alloc.SchemeID == "" && alloc.SchemeInternalCode == "" {
			return fmt.Errorf("allocation at position %d must provide scheme_id or scheme_internal_code", idx+1)
		}
		if alloc.AllocationID != nil && *alloc.AllocationID <= 0 {
			return fmt.Errorf("allocation at position %d has invalid allocation_id", idx+1)
		}
		sum += alloc.Amount
	}
	if math.Abs(sum-req.TotalAmount) > amountTolerance {
		return fmt.Errorf("allocation total %.2f must equal total_amount %.2f", sum, req.TotalAmount)
	}
	return nil
}

func resolveUserEmail(userID string) (string, bool) {
	for _, s := range auth.GetActiveSessions() {
		if s.UserID == userID {
			return s.Name, true
		}
	}
	return "", false
}

func parseBatchID(batch string) (*uuid.UUID, error) {
	if batch == "" {
		return nil, nil
	}
	u, err := uuid.Parse(batch)
	if err != nil {
		return nil, fmt.Errorf("invalid batch_id format")
	}
	return &u, nil
}

func insertProposal(ctx context.Context, tx pgx.Tx, req *CreateProposalRequest, batchUUID *uuid.UUID) (string, error) {
	const insertSQL = `
		INSERT INTO investment.investment_proposal
			(proposal_name, entity_name, total_amount, horizon_days, source, batch_id)
		VALUES ($1,$2,$3,$4,$5,$6)
		RETURNING proposal_id
	`
	var horizon interface{}
	if req.HorizonDays != nil {
		horizon = *req.HorizonDays
	}
	var batch interface{}
	if batchUUID != nil {
		batch = *batchUUID
	}
	var proposalID string
	err := tx.QueryRow(ctx, insertSQL,
		req.ProposalName,
		req.EntityName,
		req.TotalAmount,
		horizon,
		req.Source,
		batch,
	).Scan(&proposalID)
	return proposalID, err
}

func applyProposalUpdate(ctx context.Context, tx pgx.Tx, req *UpdateProposalRequest, batchUUID *uuid.UUID) error {
	const updateSQL = `
		UPDATE investment.investment_proposal
			SET old_proposal_name=proposal_name,
				proposal_name=$1,
				old_entity_name=entity_name,
				entity_name=$2,
				old_total_amount=total_amount,
				total_amount=$3,
				old_horizon_days=horizon_days,
				horizon_days=$4,
				old_source=source,
				source=$5,
			batch_id=$6,
			updated_at=now()
		WHERE proposal_id=$7
	`
	var horizon interface{}
	if req.HorizonDays != nil {
		horizon = *req.HorizonDays
	}
	var batch interface{}
	if batchUUID != nil {
		batch = *batchUUID
	}
	tag, err := tx.Exec(ctx, updateSQL,
		req.ProposalName,
		req.EntityName,
		req.TotalAmount,
		horizon,
		req.Source,
		batch,
		req.ProposalID,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("proposal %s not found", req.ProposalID)
	}
	return nil
}

func insertAllocations(ctx context.Context, tx pgx.Tx, proposalID string, allocations []ProposalAllocationInput) ([]int64, error) {
	const allocSQL = `
		INSERT INTO investment.investment_proposal_allocation
		(proposal_id, scheme_id, scheme_internal_code, amount, percent, policy_status, post_trade_holding, current_holding)
	VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
		RETURNING id
	`
	ids := make([]int64, 0, len(allocations))
	for _, alloc := range allocations {
		var schemeID interface{}
		if alloc.SchemeID != "" {
			schemeID = alloc.SchemeID
		}
		var schemeCode interface{}
		if alloc.SchemeInternalCode != "" {
			schemeCode = alloc.SchemeInternalCode
		}
		var percent interface{}
		if alloc.Percent != nil {
			percent = *alloc.Percent
		}
		policyStatus := boolOrDefault(alloc.PolicyStatus, true)
		postTradeHolding := floatOrZero(alloc.PostTradeHolding)
		currentHolding := floatOrZero(alloc.CurrentHolding)
		var allocationID int64
		if err := tx.QueryRow(ctx, allocSQL, proposalID, schemeID, schemeCode, alloc.Amount, percent, policyStatus, postTradeHolding, currentHolding).Scan(&allocationID); err != nil {
			return nil, err
		}
		ids = append(ids, allocationID)
	}
	return ids, nil
}

func syncProposalAllocations(ctx context.Context, tx pgx.Tx, proposalID string, allocations []ProposalAllocationUpsertInput) error {
	const (
		fetchSQL  = `SELECT id FROM investment.investment_proposal_allocation WHERE proposal_id=$1`
		updateSQL = `
			UPDATE investment.investment_proposal_allocation
			SET scheme_id=$1,
				scheme_internal_code=$2,
				old_amount=amount,
				amount=$3,
				old_percent=percent,
				percent=$4,
				policy_status=$5,
				old_post_trade_holding=post_trade_holding,
				post_trade_holding=$6,
				old_current_holding=current_holding,
				current_holding=$7,
				updated_at=now()
			WHERE id=$8 AND proposal_id=$9
		`
		insertSQL = `
			INSERT INTO investment.investment_proposal_allocation
				(proposal_id, scheme_id, scheme_internal_code, amount, percent, policy_status, post_trade_holding, old_post_trade_holding, current_holding, old_current_holding)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
			RETURNING id
		`
		deleteSQL = `DELETE FROM investment.investment_proposal_allocation WHERE proposal_id=$1 AND id = ANY($2)`
	)

	rows, err := tx.Query(ctx, fetchSQL, proposalID)
	if err != nil {
		return err
	}
	existing := make(map[int64]struct{})
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		existing[id] = struct{}{}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	for _, alloc := range allocations {
		schemeID := nullIfEmpty(alloc.SchemeID)
		schemeCode := nullIfEmpty(alloc.SchemeInternalCode)
		var percent interface{}
		if alloc.Percent != nil {
			percent = *alloc.Percent
		}
		policyStatus := boolOrDefault(alloc.PolicyStatus, true)
		postTradeHolding := floatOrZero(alloc.PostTradeHolding)
		currentHolding := floatOrZero(alloc.CurrentHolding)

		if alloc.AllocationID != nil {
			if _, ok := existing[*alloc.AllocationID]; !ok {
				return fmt.Errorf("allocation_id %d does not belong to proposal %s", *alloc.AllocationID, proposalID)
			}
			tag, err := tx.Exec(ctx, updateSQL, schemeID, schemeCode, alloc.Amount, percent, policyStatus, postTradeHolding, currentHolding, *alloc.AllocationID, proposalID)
			if err != nil {
				return err
			}
			if tag.RowsAffected() == 0 {
				return fmt.Errorf("failed to update allocation_id %d", *alloc.AllocationID)
			}
			delete(existing, *alloc.AllocationID)
			continue
		}

		// New allocation: do NOT set old_* fields on INSERT
		var newID int64
		if err := tx.QueryRow(ctx, insertSQL, proposalID, schemeID, schemeCode, alloc.Amount, percent, policyStatus, postTradeHolding, 0.0, currentHolding, 0.0).Scan(&newID); err != nil {
			return err
		}
	}

	if len(existing) > 0 {
		ids := make([]int64, 0, len(existing))
		for id := range existing {
			ids = append(ids, id)
		}
		if _, err := tx.Exec(ctx, deleteSQL, proposalID, ids); err != nil {
			return err
		}
	}

	return nil
}

func insertProposalAudit(ctx context.Context, tx pgx.Tx, proposalID, requestedBy, reason, actionType, processingStatus string) error {
	const auditSQL = `
		INSERT INTO investment.auditactionproposal
			(proposal_id, actiontype, processing_status, requested_by, requested_at, reason)
		VALUES ($1,$2,$3,$4,now(),$5)
	`
	_, err := tx.Exec(ctx, auditSQL, proposalID, actionType, processingStatus, requestedBy, nullIfEmpty(reason))
	return err
}

func defaultString(val, fallback string) string {
	if val == "" {
		return fallback
	}
	return val
}

func nullIfEmpty(val string) interface{} {
	if strings.TrimSpace(val) == "" {
		return nil
	}
	return val
}

func boolOrDefault(val *bool, fallback bool) bool {
	if val == nil {
		return fallback
	}
	return *val
}

func floatOrZero(val *float64) float64 {
	if val == nil {
		return 0
	}
	return *val
}

func normalizeBulkActionRequest(req *ProposalBulkActionRequest) {
	req.UserID = strings.TrimSpace(req.UserID)
	req.Comment = strings.TrimSpace(req.Comment)
	req.ProposalIDs = normalizeStringSlice(req.ProposalIDs)
}

func validateBulkActionRequest(req *ProposalBulkActionRequest) error {
	if req.UserID == "" {
		return fmt.Errorf("user_id is required")
	}
	if len(req.ProposalIDs) == 0 {
		return fmt.Errorf("proposal_ids are required")
	}
	return nil
}

func normalizeBulkDeleteRequest(req *ProposalBulkDeleteRequest) {
	req.UserID = strings.TrimSpace(req.UserID)
	req.Reason = strings.TrimSpace(req.Reason)
	req.ProposalIDs = normalizeStringSlice(req.ProposalIDs)
}

func validateBulkDeleteRequest(req *ProposalBulkDeleteRequest) error {
	if req.UserID == "" {
		return fmt.Errorf("user_id is required")
	}
	if len(req.ProposalIDs) == 0 {
		return fmt.Errorf("proposal_ids are required")
	}
	return nil
}

func gatherProposalIDs(ctx context.Context, tx pgx.Tx, proposalIDs []string) ([]string, []string, error) {
	ordered := normalizeStringSlice(proposalIDs)
	if len(ordered) == 0 {
		return nil, nil, fmt.Errorf("proposal_ids are required")
	}

	rows, err := tx.Query(ctx, `
		SELECT proposal_id
		FROM investment.investment_proposal
		WHERE proposal_id = ANY($1)
	`, ordered)
	if err != nil {
		return nil, nil, err
	}
	validSet := make(map[string]struct{})
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, nil, err
		}
		validSet[id] = struct{}{}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}

	valid := make([]string, 0, len(validSet))
	invalid := make([]string, 0)
	for _, id := range ordered {
		if _, ok := validSet[id]; ok {
			valid = append(valid, id)
		} else {
			invalid = append(invalid, id)
		}
	}
	if len(valid) == 0 {
		return nil, invalid, fmt.Errorf("no proposals found for provided identifiers")
	}
	sort.Strings(invalid)
	return valid, invalid, nil
}

func updateAuditStatuses(ctx context.Context, tx pgx.Tx, actionIDs []string, status, checkerBy, comment string) error {
	if len(actionIDs) == 0 {
		return nil
	}
	_, err := tx.Exec(ctx, `
		UPDATE investment.auditactionproposal
		SET processing_status=$1,
			checker_by=$2,
			checker_at=now(),
			checker_comment=$3
		WHERE action_id = ANY($4)
	`, status, checkerBy, nullIfEmpty(comment), actionIDs)
	return err
}

func normalizeStringSlice(values []string) []string {
	result := make([]string, 0, len(values))
	seen := make(map[string]struct{})
	for _, v := range values {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		if _, exists := seen[v]; exists {
			continue
		}
		seen[v] = struct{}{}
		result = append(result, v)
	}
	return result
}

func uniqueStrings(values []string) []string {
	result := make([]string, 0, len(values))
	seen := make(map[string]struct{})
	for _, v := range values {
		if v == "" {
			continue
		}
		if _, exists := seen[v]; exists {
			continue
		}
		seen[v] = struct{}{}
		result = append(result, v)
	}
	return result
}
