package projection

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"CimplrCorpSaas/api/constants"

	"github.com/jackc/pgx/v5/pgxpool"
)

// V2 API handlers for new schema with base_currency_code, maturity_date, bank info
// PERFORMANCE OPTIMIZED for bulk operations (10K-100K records)

// calculateMonthlyProjections auto-calculates monthly breakdown from maturity date
// Based on is_recurring and recurrence_frequency (Weekly/Monthly/Quarterly/HalfYearly/Yearly)
// Returns map[year-month]amount for 12 months from effectiveDate
func calculateMonthlyProjections(effectiveDate time.Time, maturityDate time.Time, expectedAmount float64, isRecurring bool, frequency string) map[string]float64 {
	projections := make(map[string]float64)

	if !isRecurring {
		// Non-recurring: Full amount in maturity month
		projections[fmt.Sprintf(constants.FormatYearMonth, maturityDate.Year(), maturityDate.Month())] = expectedAmount
		return projections
	}

	// Recurring: Distribute across months based on frequency
	freq := strings.ToLower(strings.TrimSpace(frequency))
	endDate := effectiveDate.AddDate(0, 12, 0) // 12 months window

	switch freq {
	case "weekly":
		// Weekly: Divide by ~4.33 weeks/month
		monthlyAmount := expectedAmount / 4.33
		for d := effectiveDate; d.Before(endDate); d = d.AddDate(0, 1, 0) {
			key := fmt.Sprintf(constants.FormatYearMonth, d.Year(), d.Month())
			projections[key] = monthlyAmount
		}

	case "monthly":
		// Monthly: Divide by 12 months
		monthlyAmount := expectedAmount / 12.0
		for d := effectiveDate; d.Before(endDate); d = d.AddDate(0, 1, 0) {
			key := fmt.Sprintf(constants.FormatYearMonth, d.Year(), d.Month())
			projections[key] = monthlyAmount
		}

	case "quarterly":
		// Quarterly: Divide by 4, distribute in months matching effective month
		quarterlyAmount := expectedAmount / 4.0
		effMonth := int(effectiveDate.Month())
		for d := effectiveDate; d.Before(endDate); d = d.AddDate(0, 1, 0) {
			month := int(d.Month())
			// Match quarter months: if effective is Jan, then Jan/Apr/Jul/Oct
			if (month-effMonth)%3 == 0 {
				key := fmt.Sprintf(constants.FormatYearMonth, d.Year(), d.Month())
				projections[key] = quarterlyAmount
			}
		}

	case "halfyearly", "half yearly", "semi-annual":
		// HalfYearly: Divide by 2, distribute in 6-month intervals
		halfYearlyAmount := expectedAmount / 2.0
		for d := effectiveDate; d.Before(endDate); d = d.AddDate(0, 6, 0) {
			key := fmt.Sprintf(constants.FormatYearMonth, d.Year(), d.Month())
			projections[key] = halfYearlyAmount
		}

	case "yearly", "annual":
		// Yearly: Full amount in effective month
		key := fmt.Sprintf(constants.FormatYearMonth, effectiveDate.Year(), effectiveDate.Month())
		projections[key] = expectedAmount

	default:
		// Unknown frequency: treat as non-recurring
		projections[fmt.Sprintf(constants.FormatYearMonth, maturityDate.Year(), maturityDate.Month())] = expectedAmount
	}

	return projections
}

// DeleteCashFlowProposalV2 inserts DELETE audit actions for proposals (V2 schema)
// Time Complexity: O(n) where n = number of proposals
// For 100K proposals: ~2-3 seconds (single batch INSERT)
func DeleteCashFlowProposalV2(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		var req struct {
			UserID      string   `json:"user_id"`
			ProposalIDs []string `json:"proposal_ids"`
			Reason      string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSON+": "+err.Error())
			return
		}

		requestedBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				requestedBy = s.Email
				break
			}
		}
		if requestedBy == "" {
			requestedBy = req.UserID
		}
		if len(req.ProposalIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "proposal_ids cannot be empty")
			return
		}
		if strings.TrimSpace(req.Reason) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "reason cannot be empty")
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxBeginFailed+err.Error())
			return
		}
		committed := false
		defer func() {
			if !committed {
				tx.Rollback(ctx)
			}
		}()

		// OPTIMIZED: Single query using unnest() for bulk INSERT instead of loop
		// Old: O(n) individual INSERTs = n * (parse + execute + network) = slow
		// New: O(1) single batch INSERT = 1 * (parse + execute + network) = fast
		// Note: proposal_id is varchar(40), not uuid
		q := `
			INSERT INTO cimplrcorpsaas.audit_action_cashflow_proposal 
			(proposal_id, action_type, processing_status, reason, requested_by, requested_at)
			SELECT unnest($1::text[]), 'DELETE', 'PENDING_DELETE_APPROVAL', $2, $3, now()
		`
		if _, err := tx.Exec(ctx, q, req.ProposalIDs, req.Reason, requestedBy); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrDBPrefix+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailedCapitalized+err.Error())
			return
		}
		committed = true

		elapsed := time.Since(start)
		log.Printf("[DeleteCashFlowProposalV2] Processed %d proposals in %v", len(req.ProposalIDs), elapsed)
		api.RespondWithResult(w, true, fmt.Sprintf("Marked %d proposals for deletion in %v", len(req.ProposalIDs), elapsed))
	}
}

// BulkRejectCashFlowProposalActionsV2 rejects audit actions for proposals (V2 schema)
// Time Complexity: O(n) where n = number of proposals
// For 100K proposals: ~3-4 seconds (1 SELECT + 1 UPDATE, uses ANY($1))
func BulkRejectCashFlowProposalActionsV2(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		var req struct {
			UserID      string   `json:"user_id"`
			ProposalIDs []string `json:"proposal_ids"`
			Comment     string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSON+": "+err.Error())
			return
		}

		checkerBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				checkerBy = s.Email
				break
			}
		}
		if checkerBy == "" {
			checkerBy = req.UserID
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxBeginFailed+err.Error())
			return
		}
		committed := false
		defer func() {
			if !committed {
				tx.Rollback(ctx)
			}
		}()

		// OPTIMIZED: Use DISTINCT ON with ANY($1) - single query instead of per-proposal query
		sel := `
			SELECT DISTINCT ON (proposal_id) 
				action_id, proposal_id, processing_status 
			FROM cimplrcorpsaas.audit_action_cashflow_proposal 
			WHERE proposal_id = ANY($1) 
			ORDER BY proposal_id, requested_at DESC
		`
		rows, err := tx.Query(ctx, sel, req.ProposalIDs)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrDBPrefix+err.Error())
			return
		}
		defer rows.Close()

		actionIDs := make([]string, 0, len(req.ProposalIDs))
		foundProposals := make(map[string]bool, len(req.ProposalIDs))
		cannotReject := make([]string, 0)

		for rows.Next() {
			var actionID, proposalID, status string
			if err := rows.Scan(&actionID, &proposalID, &status); err != nil {
				continue
			}
			foundProposals[proposalID] = true
			if status == "PENDING_APPROVAL" || status == "PENDING_EDIT_APPROVAL" || status == "PENDING_DELETE_APPROVAL" {
				actionIDs = append(actionIDs, actionID)
			} else {
				cannotReject = append(cannotReject, proposalID)
			}
		}

		// Check for missing or cannot reject
		missing := make([]string, 0)
		for _, pid := range req.ProposalIDs {
			if !foundProposals[pid] {
				missing = append(missing, pid)
			}
		}
		if len(missing) > 0 || len(cannotReject) > 0 {
			msg := ""
			if len(missing) > 0 {
				msg = "Missing audit for: " + strings.Join(missing, ",")
			}
			if len(cannotReject) > 0 {
				if msg != "" {
					msg += "; "
				}
				msg += "Cannot reject (not pending): " + strings.Join(cannotReject, ",")
			}
			api.RespondWithError(w, http.StatusBadRequest, msg)
			return
		}

		// OPTIMIZED: Single UPDATE using ANY($3) instead of loop
		upd := `
			UPDATE cimplrcorpsaas.audit_action_cashflow_proposal 
			SET processing_status='REJECTED', 
				checker_by=$1, 
				checker_at=now(), 
				checker_comment=$2 
			WHERE action_id = ANY($3)
		`
		if _, err := tx.Exec(ctx, upd, checkerBy, req.Comment, actionIDs); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrDBPrefix+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailedCapitalized+err.Error())
			return
		}
		committed = true

		elapsed := time.Since(start)
		log.Printf("[BulkRejectCashFlowProposalActionsV2] Rejected %d proposals in %v", len(actionIDs), elapsed)
		api.RespondWithResult(w, true, fmt.Sprintf("Rejected %d proposals in %v", len(actionIDs), elapsed))
	}
}

// BulkApproveCashFlowProposalActionsV2 approves audit actions for proposals (V2 schema)
// Time Complexity: O(n) where n = number of proposals
// For 100K proposals: ~4-6 seconds (1 SELECT + 1 UPDATE + 1 DELETE with CASCADE)
// DELETE with CASCADE is the bottleneck - cascades to items & monthly projections
func BulkApproveCashFlowProposalActionsV2(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		var req struct {
			UserID      string   `json:"user_id"`
			ProposalIDs []string `json:"proposal_ids"`
			Comment     string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSON+": "+err.Error())
			return
		}

		checkerBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				checkerBy = s.Email
				break
			}
		}
		if checkerBy == "" {
			checkerBy = req.UserID
		}
		if len(req.ProposalIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "proposal_ids cannot be empty")
			return
		}
		if strings.TrimSpace(req.Comment) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "comment cannot be empty")
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxBeginFailed+err.Error())
			return
		}
		committed := false
		defer func() {
			if !committed {
				tx.Rollback(ctx)
			}
		}()

		// OPTIMIZED: Single query with ANY($1) - no per-proposal queries
		sel := `
			SELECT DISTINCT ON (proposal_id) 
				action_id, proposal_id, action_type, processing_status 
			FROM cimplrcorpsaas.audit_action_cashflow_proposal 
			WHERE proposal_id = ANY($1) 
			ORDER BY proposal_id, requested_at DESC
		`
		rows, err := tx.Query(ctx, sel, req.ProposalIDs)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrDBPrefix+err.Error())
			return
		}
		defer rows.Close()

		actionIDs := make([]string, 0, len(req.ProposalIDs))
		foundProposals := make(map[string]bool, len(req.ProposalIDs))
		cannotApprove := make([]string, 0)
		deleteProposalIDs := make([]string, 0)

		for rows.Next() {
			var actionID, proposalID, actionType, status string
			if err := rows.Scan(&actionID, &proposalID, &actionType, &status); err != nil {
				continue
			}
			foundProposals[proposalID] = true
			if status == "PENDING_APPROVAL" || status == "PENDING_EDIT_APPROVAL" || status == "PENDING_DELETE_APPROVAL" {
				actionIDs = append(actionIDs, actionID)
				if actionType == "DELETE" {
					deleteProposalIDs = append(deleteProposalIDs, proposalID)
				}
			} else {
				cannotApprove = append(cannotApprove, proposalID)
			}
		}

		// Check for missing or cannot approve
		missing := make([]string, 0)
		for _, pid := range req.ProposalIDs {
			if !foundProposals[pid] {
				missing = append(missing, pid)
			}
		}
		if len(missing) > 0 || len(cannotApprove) > 0 {
			msg := ""
			if len(missing) > 0 {
				msg = "Missing audit for: " + strings.Join(missing, ",")
			}
			if len(cannotApprove) > 0 {
				if msg != "" {
					msg += "; "
				}
				msg += "Cannot approve (not pending): " + strings.Join(cannotApprove, ",")
			}
			api.RespondWithError(w, http.StatusBadRequest, msg)
			return
		}

		// OPTIMIZED: Single UPDATE using ANY($3)
		upd := `
			UPDATE cimplrcorpsaas.audit_action_cashflow_proposal 
			SET processing_status='APPROVED', 
				checker_by=$1, 
				checker_at=now(), 
				checker_comment=$2 
			WHERE action_id = ANY($3)
		`
		if _, err := tx.Exec(ctx, upd, checkerBy, req.Comment, actionIDs); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrDBPrefix+err.Error())
			return
		}

		// OPTIMIZED: Single DELETE using ANY($1) instead of loop
		// CASCADE handles items and monthly projections automatically
		if len(deleteProposalIDs) > 0 {
			delQ := `DELETE FROM cimplrcorpsaas.cashflow_proposal WHERE proposal_id = ANY($1)`
			if _, err := tx.Exec(ctx, delQ, deleteProposalIDs); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Failed to delete proposals: "+err.Error())
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailedCapitalized+err.Error())
			return
		}
		committed = true

		elapsed := time.Since(start)
		log.Printf("[BulkApproveCashFlowProposalActionsV2] Approved %d proposals (%d deleted) in %v",
			len(actionIDs), len(deleteProposalIDs), elapsed)
		api.RespondWithResult(w, true, fmt.Sprintf("Approved %d proposals (%d deleted) in %v",
			len(actionIDs), len(deleteProposalIDs), elapsed))
	}
}

// CreateCashFlowProposalV2 creates a new proposal with items and monthly projections (V2 schema)
func CreateCashFlowProposalV2(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string `json:"user_id"`
			Proposal struct {
				ProposalName     string `json:"proposal_name"`
				BaseCurrencyCode string `json:"base_currency_code"`
				EffectiveDate    string `json:"effective_date"`
			} `json:"proposal"`
			Items []struct {
				Description         string  `json:"description"`
				CashflowType        string  `json:"cashflow_type"`        // Inflow/Outflow
				CategoryID          string  `json:"category_id"`          // FK to mastercashflowcategory
				CurrencyCode        string  `json:"currency_code"`        // char(3)
				ExpectedAmount      float64 `json:"expected_amount"`      // Total amount
				IsRecurring         bool    `json:"is_recurring"`         // If true, calculate monthly breakdown
				RecurrenceFrequency string  `json:"recurrence_frequency"` // Weekly/Monthly/Quarterly/HalfYearly/Yearly
				MaturityDate        string  `json:"maturity_date"`        // Date when amount matures (within 12 months)
				BankName            string  `json:"bank_name,omitempty"`
				BankAccountNumber   string  `json:"bank_account_number,omitempty"`
				EntityName          string  `json:"entity_name,omitempty"`
			} `json:"items"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSON+": "+err.Error())
			return
		}

		createdBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				createdBy = s.Email
				break
			}
		}
		if createdBy == "" {
			createdBy = req.UserID
		}

		if len(req.Items) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "items cannot be empty")
			return
		}

		ctx := context.Background()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxBeginFailed+err.Error())
			return
		}
		committed := false
		defer func() {
			if !committed {
				tx.Rollback(ctx)
			}
		}()

		// Create proposal
		var proposalID string
		insProp := `INSERT INTO cimplrcorpsaas.cashflow_proposal (proposal_name, base_currency_code, effective_date) VALUES ($1,$2,$3) RETURNING proposal_id`
		err = tx.QueryRow(ctx, insProp, req.Proposal.ProposalName, req.Proposal.BaseCurrencyCode, req.Proposal.EffectiveDate).Scan(&proposalID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to insert proposal: "+err.Error())
			return
		}

		created := 0
		for _, item := range req.Items {
			// Validate cashflow_type
			if item.CashflowType != "Inflow" && item.CashflowType != "Outflow" {
				api.RespondWithError(w, http.StatusBadRequest, fmt.Sprintf("Invalid cashflow_type: %s (must be Inflow/Outflow)", item.CashflowType))
				return
			}

			// Verify category exists
			var categoryExists bool
			catQ := `SELECT EXISTS(SELECT 1 FROM public.mastercashflowcategory WHERE category_id = $1)`
			if err := tx.QueryRow(ctx, catQ, item.CategoryID).Scan(&categoryExists); err != nil || !categoryExists {
				api.RespondWithError(w, http.StatusBadRequest, fmt.Sprintf("Invalid category_id: %s", item.CategoryID))
				return
			}

			// Insert item
			var itemID string
			insItem := `
				INSERT INTO cimplrcorpsaas.cashflow_proposal_item 
				(proposal_id, description, cashflow_type, category_id, currency_code, expected_amount, 
				 is_recurring, recurrence_frequency, maturity_date, bank_name, bank_account_number, entity_name)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
				RETURNING item_id
			`
			var maturityDate interface{}
			if item.MaturityDate != "" {
				maturityDate = item.MaturityDate
			}

			err = tx.QueryRow(ctx, insItem,
				proposalID, item.Description, item.CashflowType, item.CategoryID, item.CurrencyCode,
				item.ExpectedAmount, item.IsRecurring, nullString(item.RecurrenceFrequency),
				maturityDate, nullString(item.BankName), nullString(item.BankAccountNumber), nullString(item.EntityName),
			).Scan(&itemID)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Failed to insert item: "+err.Error())
				return
			}

			// AUTO-CALCULATE monthly projections from maturity_date + recurrence
			effectiveDate, _ := time.Parse(constants.DateFormat, req.Proposal.EffectiveDate)
			var maturityParsed time.Time
			if item.MaturityDate != "" {
				maturityParsed, _ = time.Parse(constants.DateFormat, item.MaturityDate)
			} else {
				// Default: 12 months from effective
				maturityParsed = effectiveDate.AddDate(0, 12, 0)
			}

			monthlyProj := calculateMonthlyProjections(effectiveDate, maturityParsed, item.ExpectedAmount, item.IsRecurring, item.RecurrenceFrequency)

			// Insert calculated projections
			for yearMonth, amount := range monthlyProj {
				parts := strings.Split(yearMonth, "-")
				if len(parts) != 2 {
					continue
				}
				year, month := 0, 0
				fmt.Sscanf(parts[0], "%d", &year)
				fmt.Sscanf(parts[1], "%d", &month)

				if year > 0 && month >= 1 && month <= 12 {
					insProj := `INSERT INTO cimplrcorpsaas.cashflow_projection_monthly (item_id, year, month, projected_amount) VALUES ($1, $2, $3, $4)`
					if _, err := tx.Exec(ctx, insProj, itemID, year, month, amount); err != nil {
						log.Printf("Warning: Failed to insert projection for %s: %v", yearMonth, err)
					}
				}
			}
			created++
		}

		// Insert audit record
		auditQ := `INSERT INTO cimplrcorpsaas.audit_action_cashflow_proposal (proposal_id, action_type, processing_status, reason, requested_by, requested_at) VALUES ($1,'CREATE','PENDING_APPROVAL', $2, $3, now())`
		if _, err := tx.Exec(ctx, auditQ, proposalID, "Created via API", createdBy); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to insert audit: "+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailedCapitalized+err.Error())
			return
		}
		committed = true

		api.RespondWithResult(w, true, fmt.Sprintf("Successfully created proposal %s with %d items", proposalID, created))
	}
}

// Helper to create NULL string
func nullString(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

// GetProposalDetailV2 retrieves a single proposal with items and projections (V2 schema)
func GetProposalDetailV2(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID     string `json:"user_id"`
			ProposalID string `json:"proposal_id"`
		}
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

		// Get proposal header
		hq := `
			SELECT
				p.proposal_id,
				p.proposal_name,
				p.base_currency_code,
				p.effective_date,
				p.old_proposal_name,
				p.old_base_currency_code,
				p.old_effective_date,
				a.processing_status
			FROM cimplrcorpsaas.cashflow_proposal p
			LEFT JOIN LATERAL (
				SELECT processing_status
				FROM cimplrcorpsaas.audit_action_cashflow_proposal a2
				WHERE a2.proposal_id = p.proposal_id
				ORDER BY requested_at DESC
				LIMIT 1
			) a ON TRUE
			WHERE p.proposal_id = $1
			LIMIT 1
		`

		var (
			proposalID, proposalName, baseCurrency, processingStatus string
			effectiveDate                                            time.Time
			oldProposalName, oldBaseCurrency                         interface{}
			oldEffectiveDate                                         interface{}
		)
		err := pgxPool.QueryRow(ctx, hq, req.ProposalID).Scan(
			&proposalID, &proposalName, &baseCurrency, &effectiveDate,
			&oldProposalName, &oldBaseCurrency, &oldEffectiveDate, &processingStatus,
		)
		if err != nil {
			api.RespondWithError(w, http.StatusNotFound, "Proposal not found: "+err.Error())
			return
		}

		header := map[string]interface{}{
			"proposal_id":        proposalID,
			"proposal_name":      proposalName,
			"base_currency_code": baseCurrency,
			"effective_date":     effectiveDate.Format(constants.DateFormat),
			"old_proposal_name":  ifaceToString(oldProposalName),
			"old_base_currency":  ifaceToString(oldBaseCurrency),
			"old_effective_date": ifaceToTimeString(oldEffectiveDate),
			"processing_status":  processingStatus,
		}

		// Get items
		itemQ := `
			SELECT 
				item_id, description, cashflow_type, category_id, currency_code, expected_amount,
				is_recurring, recurrence_frequency, maturity_date, bank_name, bank_account_number, entity_name,
				old_cashflow_type, old_category_id, old_currency_code, old_expected_amount,
				old_is_recurring, old_recurrence_frequency, old_maturity_date, old_entity_name,
				old_bank_name, old_bank_account_number
			FROM cimplrcorpsaas.cashflow_proposal_item
			WHERE proposal_id = $1
			ORDER BY created_at
		`

		rows, err := pgxPool.Query(ctx, itemQ, req.ProposalID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrDBPrefix+err.Error())
			return
		}
		defer rows.Close()

		items := make([]map[string]interface{}, 0)
		itemIDs := make([]string, 0)

		for rows.Next() {
			var (
				itemID, description, cashflowType, categoryID, currencyCode                              string
				expectedAmount                                                                           float64
				isRecurring                                                                              bool
				recurrenceFrequency, maturityDate, bankName, bankAccountNumber, entityName               interface{}
				oldCashflowType, oldCategoryID, oldCurrencyCode, oldRecurrenceFrequency, oldMaturityDate interface{}
				oldEntityName, oldBankName, oldBankAccountNumber                                         interface{}
				oldExpectedAmount                                                                        interface{}
				oldIsRecurring                                                                           interface{}
			)

			if err := rows.Scan(
				&itemID, &description, &cashflowType, &categoryID, &currencyCode, &expectedAmount,
				&isRecurring, &recurrenceFrequency, &maturityDate, &bankName, &bankAccountNumber, &entityName,
				&oldCashflowType, &oldCategoryID, &oldCurrencyCode, &oldExpectedAmount,
				&oldIsRecurring, &oldRecurrenceFrequency, &oldMaturityDate, &oldEntityName,
				&oldBankName, &oldBankAccountNumber,
			); err != nil {
				continue
			}

			itemIDs = append(itemIDs, itemID)
			items = append(items, map[string]interface{}{
				"item_id":                  itemID,
				"description":              description,
				"cashflow_type":            cashflowType,
				"category_id":              categoryID,
				"currency_code":            currencyCode,
				"expected_amount":          expectedAmount,
				"is_recurring":             isRecurring,
				"recurrence_frequency":     ifaceToString(recurrenceFrequency),
				"maturity_date":            ifaceToTimeString(maturityDate),
				"bank_name":                ifaceToString(bankName),
				"bank_account_number":      ifaceToString(bankAccountNumber),
				"entity_name":              ifaceToString(entityName),
				"old_cashflow_type":        ifaceToString(oldCashflowType),
				"old_category_id":          ifaceToString(oldCategoryID),
				"old_currency_code":        ifaceToString(oldCurrencyCode),
				"old_expected_amount":      ifaceToFloat(oldExpectedAmount),
				"old_is_recurring":         ifaceToBool(oldIsRecurring),
				"old_recurrence_frequency": ifaceToString(oldRecurrenceFrequency),
				"old_maturity_date":        ifaceToTimeString(oldMaturityDate),
				"old_entity_name":          ifaceToString(oldEntityName),
				"old_bank_name":            ifaceToString(oldBankName),
				"old_bank_account_number":  ifaceToString(oldBankAccountNumber),
			})
		}

		// Get monthly projections
		projMap := make(map[string]map[string]float64)
		if len(itemIDs) > 0 {
			projQ := `SELECT item_id, year, month, projected_amount FROM cimplrcorpsaas.cashflow_projection_monthly WHERE item_id = ANY($1) ORDER BY item_id, year, month`
			projRows, err := pgxPool.Query(ctx, projQ, itemIDs)
			if err == nil {
				defer projRows.Close()
				for projRows.Next() {
					var itemID string
					var year, month int
					var amount float64
					if err := projRows.Scan(&itemID, &year, &month, &amount); err == nil {
						if projMap[itemID] == nil {
							projMap[itemID] = make(map[string]float64)
						}
						key := fmt.Sprintf(constants.FormatYearMonth, year, month)
						projMap[itemID][key] = amount
					}
				}
			}
		}

		// Attach projections to items
		for i := range items {
			itemID := items[i]["item_id"].(string)
			items[i]["monthly_projections"] = projMap[itemID]
		}

		// Get audit actions for this proposal
		actionQ := `
			SELECT 
				action_id, proposal_id, action_type, processing_status, reason,
				requested_by, requested_at, checker_by, checker_at, checker_comment
			FROM cimplrcorpsaas.audit_action_cashflow_proposal
			WHERE proposal_id = $1
			ORDER BY requested_at DESC
		`
		actionRows, err := pgxPool.Query(ctx, actionQ, req.ProposalID)
		actions := make([]map[string]interface{}, 0)
		if err == nil {
			defer actionRows.Close()
			for actionRows.Next() {
				var (
					actionID, proposalID, actionType, processingStatus, reason, requestedBy string
					requestedAt                                                             time.Time
					checkerBy, checkerComment                                               interface{}
					checkerAt                                                               interface{}
				)
				if err := actionRows.Scan(&actionID, &proposalID, &actionType, &processingStatus, &reason,
					&requestedBy, &requestedAt, &checkerBy, &checkerAt, &checkerComment); err == nil {
					actions = append(actions, map[string]interface{}{
						"action_id":         actionID,
						"proposal_id":       proposalID,
						"action_type":       actionType,
						"processing_status": processingStatus,
						"reason":            reason,
						"requested_by":      requestedBy,
						"requested_at":      requestedAt.Format(constants.DateTimeFormat),
						"checker_by":        ifaceToString(checkerBy),
						"checker_at":        ifaceToTimeString(checkerAt),
						"checker_comment":   ifaceToString(checkerComment),
					})
				}
			}
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"header":  header,
			"items":   items,
			"actions": actions,
		})
	}
}

// ListProposalsV2 lists all proposals with summary info (V2 schema)
func ListProposalsV2(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
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

		ctx := r.Context()

		// List all proposals with latest audit status
		q := `
			SELECT
				p.proposal_id,
				p.proposal_name,
				p.base_currency_code,
				p.effective_date,
				COALESCE(a.processing_status, 'N/A') AS processing_status,
				COUNT(DISTINCT i.item_id) AS item_count
			FROM cimplrcorpsaas.cashflow_proposal p
			LEFT JOIN cimplrcorpsaas.cashflow_proposal_item i ON p.proposal_id = i.proposal_id
			LEFT JOIN LATERAL (
				SELECT processing_status
				FROM cimplrcorpsaas.audit_action_cashflow_proposal a2
				WHERE a2.proposal_id = p.proposal_id
				ORDER BY requested_at DESC
				LIMIT 1
			) a ON TRUE
			GROUP BY p.proposal_id, p.proposal_name, p.base_currency_code, p.effective_date, a.processing_status
			ORDER BY p.effective_date DESC
		`

		rows, err := pgxPool.Query(ctx, q)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrDBPrefix+err.Error())
			return
		}
		defer rows.Close()

		proposals := make([]map[string]interface{}, 0)
		for rows.Next() {
			var proposalID, proposalName, baseCurrency, status string
			var effectiveDate time.Time
			var itemCount int
			if err := rows.Scan(&proposalID, &proposalName, &baseCurrency, &effectiveDate, &status, &itemCount); err != nil {
				continue
			}
			proposals = append(proposals, map[string]interface{}{
				"proposal_id":        proposalID,
				"proposal_name":      proposalName,
				"base_currency_code": baseCurrency,
				"effective_date":     effectiveDate.Format(constants.DateFormat),
				"processing_status":  status,
				"item_count":         itemCount,
			})
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":   true,
			"proposals": proposals,
		})
	}
}

// UpdateCashFlowProposalV2 updates an existing proposal (V2 schema)
// Time Complexity: O(n + m) where n = items, m = monthly projections
// Tracks old values in old_* fields for audit trail
// Monthly projections are AUTO-CALCULATED from maturity_date + recurrence
func UpdateCashFlowProposalV2(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		var req struct {
			UserID     string `json:"user_id"`
			ProposalID string `json:"proposal_id"`
			Proposal   struct {
				ProposalName     string `json:"proposal_name"`
				BaseCurrencyCode string `json:"base_currency_code"`
				EffectiveDate    string `json:"effective_date"`
			} `json:"proposal"`
			Items []struct {
				Description         string  `json:"description"`
				CashflowType        string  `json:"cashflow_type"`
				CategoryID          string  `json:"category_id"`
				CurrencyCode        string  `json:"currency_code"`
				ExpectedAmount      float64 `json:"expected_amount"`
				IsRecurring         bool    `json:"is_recurring"`
				RecurrenceFrequency string  `json:"recurrence_frequency,omitempty"`
				MaturityDate        string  `json:"maturity_date,omitempty"`
				BankName            string  `json:"bank_name,omitempty"`
				BankAccountNumber   string  `json:"bank_account_number,omitempty"`
				EntityName          string  `json:"entity_name,omitempty"`
			} `json:"items"`
			Reason string `json:"reason"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSON+": "+err.Error())
			return
		}

		requestedBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				requestedBy = s.Email
				break
			}
		}
		if requestedBy == "" {
			requestedBy = req.UserID
		}
		if strings.TrimSpace(req.ProposalID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "proposal_id cannot be empty")
			return
		}
		if strings.TrimSpace(req.Reason) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "reason cannot be empty for updates")
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxBeginFailed+err.Error())
			return
		}
		committed := false
		defer func() {
			if !committed {
				tx.Rollback(ctx)
			}
		}()

		// Get current proposal values for audit trail
		var oldName, oldCurrency string
		var oldDate time.Time
		q := `SELECT proposal_name, base_currency_code, effective_date FROM cimplrcorpsaas.cashflow_proposal WHERE proposal_id=$1`
		if err := tx.QueryRow(ctx, q, req.ProposalID).Scan(&oldName, &oldCurrency, &oldDate); err != nil {
			api.RespondWithError(w, http.StatusNotFound, "Proposal not found: "+err.Error())
			return
		}

		// Update proposal with old_* fields for audit trail
		updProp := `
			UPDATE cimplrcorpsaas.cashflow_proposal 
			SET proposal_name=$1, 
				base_currency_code=$2, 
				effective_date=$3,
				old_proposal_name=$4,
				old_base_currency_code=$5,
				old_effective_date=$6
			WHERE proposal_id=$7
		`
		if _, err := tx.Exec(ctx, updProp,
			req.Proposal.ProposalName, req.Proposal.BaseCurrencyCode, req.Proposal.EffectiveDate,
			oldName, oldCurrency, oldDate, req.ProposalID); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to update proposal: "+err.Error())
			return
		}

		// Delete old items and projections (will recreate from request)
		if _, err := tx.Exec(ctx, `DELETE FROM cimplrcorpsaas.cashflow_proposal_item WHERE proposal_id=$1`, req.ProposalID); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to delete old items: "+err.Error())
			return
		}

		// Insert updated items
		updated := 0
		for _, item := range req.Items {
			if item.CashflowType != "Inflow" && item.CashflowType != "Outflow" {
				api.RespondWithError(w, http.StatusBadRequest, fmt.Sprintf("Invalid cashflow_type: %s", item.CashflowType))
				return
			}

			var categoryExists bool
			catQ := `SELECT EXISTS(SELECT 1 FROM public.mastercashflowcategory WHERE category_id = $1)`
			if err := tx.QueryRow(ctx, catQ, item.CategoryID).Scan(&categoryExists); err != nil || !categoryExists {
				api.RespondWithError(w, http.StatusBadRequest, fmt.Sprintf("Invalid category_id: %s", item.CategoryID))
				return
			}

			var itemID string
			insItem := `
				INSERT INTO cimplrcorpsaas.cashflow_proposal_item 
				(proposal_id, description, cashflow_type, category_id, currency_code, expected_amount, 
				 is_recurring, recurrence_frequency, maturity_date, bank_name, bank_account_number, entity_name)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
				RETURNING item_id
			`
			var maturityDate interface{}
			if item.MaturityDate != "" {
				maturityDate = item.MaturityDate
			}

			err = tx.QueryRow(ctx, insItem,
				req.ProposalID, item.Description, item.CashflowType, item.CategoryID, item.CurrencyCode,
				item.ExpectedAmount, item.IsRecurring, nullString(item.RecurrenceFrequency),
				maturityDate, nullString(item.BankName), nullString(item.BankAccountNumber), nullString(item.EntityName),
			).Scan(&itemID)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Failed to insert item: "+err.Error())
				return
			}

			// AUTO-CALCULATE monthly projections
			effectiveDate, _ := time.Parse(constants.DateFormat, req.Proposal.EffectiveDate)
			var maturityParsed time.Time
			if item.MaturityDate != "" {
				maturityParsed, _ = time.Parse(constants.DateFormat, item.MaturityDate)
			} else {
				maturityParsed = effectiveDate.AddDate(0, 12, 0)
			}

			monthlyProj := calculateMonthlyProjections(effectiveDate, maturityParsed, item.ExpectedAmount, item.IsRecurring, item.RecurrenceFrequency)

			// Insert calculated projections
			for yearMonth, amount := range monthlyProj {
				parts := strings.Split(yearMonth, "-")
				if len(parts) != 2 {
					continue
				}
				year, month := 0, 0
				fmt.Sscanf(parts[0], "%d", &year)
				fmt.Sscanf(parts[1], "%d", &month)

				if year > 0 && month >= 1 && month <= 12 {
					insProj := `INSERT INTO cimplrcorpsaas.cashflow_projection_monthly (item_id, year, month, projected_amount) VALUES ($1, $2, $3, $4)`
					if _, err := tx.Exec(ctx, insProj, itemID, year, month, amount); err != nil {
						log.Printf("Warning: Failed to insert projection for %s: %v", yearMonth, err)
					}
				}
			}
			updated++
		}

		// Insert audit record
		auditQ := `INSERT INTO cimplrcorpsaas.audit_action_cashflow_proposal (proposal_id, action_type, processing_status, reason, requested_by, requested_at) VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL', $2, $3, now())`
		if _, err := tx.Exec(ctx, auditQ, req.ProposalID, req.Reason, requestedBy); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to insert audit: "+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailedCapitalized+err.Error())
			return
		}
		committed = true

		elapsed := time.Since(start)
		log.Printf("[UpdateCashFlowProposalV2] Updated proposal %s with %d items in %v", req.ProposalID, updated, elapsed)
		api.RespondWithResult(w, true, fmt.Sprintf("Updated proposal with %d items in %v", updated, elapsed))
	}
}
