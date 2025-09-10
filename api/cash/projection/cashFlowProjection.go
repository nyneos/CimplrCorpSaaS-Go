package projection

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func ifaceToString(v interface{}) string {
	if v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return t
	case *string:
		if t == nil {
			return ""
		}
		return *t
	case []byte:
		return string(t)
	case fmt.Stringer:
		return t.String()
	default:
		return fmt.Sprint(t)
	}
}

func ifaceToTimeString(v interface{}) string {
	if v == nil {
		return ""
	}
	switch t := v.(type) {
	case time.Time:
		return t.Format("2006-01-02 15:04:05")
	case *time.Time:
		if t == nil {
			return ""
		}
		return t.Format("2006-01-02 15:04:05")
	case *string:
		if t == nil {
			return ""
		}
		return *t
	case string:
		return t
	case []byte:
		return string(t)
	default:
		return fmt.Sprint(t)
	}
}

// Capitalize capitalizes first letter and lowercases the rest (ASCII-safe)
func Capitalize(s string) string {
	if s == "" {
		return s
	}
	s = strings.TrimSpace(s)
	if s == "" {
		return s
	}
	b := []rune(s)
	if len(b) == 1 {
		return strings.ToUpper(s)
	}
	first := string(b[0])
	rest := string(b[1:])
	return strings.ToUpper(first) + strings.ToLower(rest)
}

// Request structures
type CashFlowProposalRequest struct {
	ProposalName        string                        `json:"proposal_name"`
	EntityName          string                        `json:"entity_name"`
	DepartmentID        string                        `json:"department_id"`
	CurrencyCode        string                        `json:"currency_code"`
	EffectiveDate       string                        `json:"effective_date"`
	RecurrenceType      string                        `json:"recurrence_type"`
	RecurrenceFrequency string                        `json:"recurrence_frequency"`
	Status              string                        `json:"status"`
	Items               []CashFlowProposalItemRequest `json:"items"`
}

type CashFlowProposalItemRequest struct {
	Description        string                             `json:"description"`
	CashflowType       string                             `json:"cashflow_type"`
	CategoryID         string                             `json:"category_id"`
	ExpectedAmount     float64                            `json:"expected_amount"`
	IsRecurring        bool                               `json:"is_recurring"`
	RecurrencePattern  string                             `json:"recurrence_pattern"`
	StartDate          string                             `json:"start_date"`
	EndDate            string                             `json:"end_date"`
	MonthlyProjections []CashFlowProjectionMonthlyRequest `json:"monthly_projections"`
}

type CashFlowProjectionMonthlyRequest struct {
	Year            int     `json:"year"`
	Month           int     `json:"month"`
	ProjectedAmount float64 `json:"projected_amount"`
}

// CreateAndSyncCashFlowProposals creates proposals, items, monthly projections, and audit actions in a single bulk operation
func CreateAndSyncCashFlowProposals(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID    string                    `json:"user_id"`
			Proposals []CashFlowProposalRequest `json:"rows"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, "Invalid JSON: "+err.Error())
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
			api.RespondWithResult(w, false, "Invalid user_id or session")
			return
		}

		if len(req.Proposals) == 0 {
			api.RespondWithResult(w, false, "No proposals provided")
			return
		}

		ctx := context.Background()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithResult(w, false, "Failed to start transaction: "+err.Error())
			return
		}
		committed := false
		defer func() {
			if !committed {
				tx.Rollback(ctx)
			}
		}()

		createdProposalIDs := make([]string, 0)

		for _, prop := range req.Proposals {
			if strings.TrimSpace(prop.ProposalName) == "" || strings.TrimSpace(prop.EntityName) == "" || strings.TrimSpace(prop.DepartmentID) == "" {
				api.RespondWithResult(w, false, "Missing required fields for proposal: "+prop.ProposalName)
				return
			}

			// Insert proposal
			proposalID := fmt.Sprintf("PROP-%06d", time.Now().UnixNano()%1000000)
			insProp := `INSERT INTO cashflow_proposal (proposal_id, proposal_name, entity_name, department_id, currency_code, effective_date, recurrence_type, recurrence_frequency, status) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`
			if _, err := tx.Exec(ctx, insProp, proposalID, prop.ProposalName, prop.EntityName, prop.DepartmentID, prop.CurrencyCode, prop.EffectiveDate, prop.RecurrenceType, prop.RecurrenceFrequency, prop.Status); err != nil {
				api.RespondWithResult(w, false, "Failed to create proposal "+prop.ProposalName+": "+err.Error())
				return
			}

			for _, item := range prop.Items {
				if strings.TrimSpace(item.Description) == "" || strings.TrimSpace(item.CategoryID) == "" {
					api.RespondWithResult(w, false, "Missing required fields for item: "+item.Description)
					return
				}

				// Insert item
				itemID := fmt.Sprintf("ITEM-%06d", time.Now().UnixNano()%1000000)
				insItem := `INSERT INTO cashflow_proposal_item (item_id, proposal_id, description, cashflow_type, category_id, expected_amount, is_recurring, recurrence_pattern, start_date, end_date) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`
				if _, err := tx.Exec(ctx, insItem, itemID, proposalID, item.Description, item.CashflowType, item.CategoryID, item.ExpectedAmount, item.IsRecurring, item.RecurrencePattern, item.StartDate, item.EndDate); err != nil {
					api.RespondWithResult(w, false, "Failed to create item "+item.Description+": "+err.Error())
					return
				}

				for _, monthly := range item.MonthlyProjections {
					if monthly.Year < 2000 || monthly.Month < 1 || monthly.Month > 12 {
						api.RespondWithResult(w, false, "Invalid year/month for projection: "+fmt.Sprintf("%d-%d", monthly.Year, monthly.Month))
						return
					}

					// Insert monthly projection
					projectionID := fmt.Sprintf("PROJ-%06d", time.Now().UnixNano()%1000000)
					insMonthly := `INSERT INTO cashflow_projection_monthly (projection_id, item_id, year, month, projected_amount) VALUES ($1,$2,$3,$4,$5)`
					if _, err := tx.Exec(ctx, insMonthly, projectionID, itemID, monthly.Year, monthly.Month, monthly.ProjectedAmount); err != nil {
						api.RespondWithResult(w, false, "Failed to create monthly projection for "+fmt.Sprintf("%d-%d", monthly.Year, monthly.Month)+": "+err.Error())
						return
					}
				}
			}

			// Insert audit for proposal
			auditQ := `INSERT INTO audit_action_cashflow_proposal (proposal_id, action_type, processing_status, reason, requested_by, requested_at) VALUES ($1,'CREATE','PENDING_APPROVAL', $2, $3, now())`
			if _, err := tx.Exec(ctx, auditQ, proposalID, nil, createdBy); err != nil {
				api.RespondWithResult(w, false, "Failed to create audit for proposal "+proposalID+": "+err.Error())
				return
			}

			createdProposalIDs = append(createdProposalIDs, proposalID)
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithResult(w, false, "Failed to commit transaction: "+err.Error())
			return
		}
		committed = true

		api.RespondWithResult(w, true, fmt.Sprintf("Successfully created %d cash flow proposals", len(createdProposalIDs)))
	}
}

// GetCashFlowProposals retrieves proposals with their items and monthly projections
func GetCashFlowProposals(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
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
			api.RespondWithError(w, http.StatusBadRequest, "Invalid user_id or session")
			return
		}

		ctx := r.Context()
		// Fetch proposals with latest audit
		query := `
			SELECT p.proposal_id, p.proposal_name, p.entity_name, p.department_id, p.currency_code, p.effective_date, p.recurrence_type, p.recurrence_frequency, p.status,
			       a.processing_status, a.requested_by, a.requested_at, a.action_type, a.action_id, a.checker_by, a.checker_at, a.checker_comment, a.reason
			FROM cashflow_proposal p
			LEFT JOIN LATERAL (
				SELECT processing_status, requested_by, requested_at, action_type, action_id, checker_by, checker_at, checker_comment, reason
				FROM audit_action_cashflow_proposal a
				WHERE a.proposal_id = p.proposal_id
				ORDER BY requested_at DESC
				LIMIT 1
			) a ON TRUE
		`
		rows, err := pgxPool.Query(ctx, query)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		proposals := make([]map[string]interface{}, 0)
		proposalIDs := make([]string, 0)
		for rows.Next() {
			var (
				actionIDI string
				proposalID, proposalName, entityName, departmentID, currencyCode, effectiveDate, recurrenceType, recurrenceFrequency, status,
				procStatus, requestedBy, requestedAt, actionType, checkerBy, checkerAt, checkerComment, reason interface{}
			)
			if err := rows.Scan(&proposalID, &proposalName, &entityName, &departmentID, &currencyCode, &effectiveDate, &recurrenceType, &recurrenceFrequency, &status,
				&procStatus, &requestedBy, &requestedAt, &actionType, &actionIDI, &checkerBy, &checkerAt, &checkerComment, &reason); err != nil {
				continue
			}
			proposal := map[string]interface{}{
				"proposal_id":          ifaceToString(proposalID),
				"proposal_name":        ifaceToString(proposalName),
				"entity_name":          ifaceToString(entityName),
				"department_id":        ifaceToString(departmentID),
				"currency_code":        ifaceToString(currencyCode),
				"effective_date":       ifaceToString(effectiveDate),
				"recurrence_type":      ifaceToString(recurrenceType),
				"recurrence_frequency": ifaceToString(recurrenceFrequency),
				"status":               ifaceToString(status),
				"processing_status":    ifaceToString(procStatus),
				"requested_by":         ifaceToString(requestedBy),
				"requested_at":         ifaceToTimeString(requestedAt),
				"action_type":          ifaceToString(actionType),
				"action_id":            ifaceToString(actionIDI),
				"checker_by":           ifaceToString(checkerBy),
				"checker_at":           ifaceToTimeString(checkerAt),
				"checker_comment":      ifaceToString(checkerComment),
				"reason":               ifaceToString(reason),
				"items":                []interface{}{},
			}
			proposals = append(proposals, proposal)
			proposalIDs = append(proposalIDs, ifaceToString(proposalID))
		}

		// Fetch items for proposals
		if len(proposalIDs) > 0 {
			itemQuery := `SELECT item_id, proposal_id, description, cashflow_type, category_id, expected_amount, is_recurring, recurrence_pattern, start_date, end_date FROM cashflow_proposal_item WHERE proposal_id = ANY($1)`
			itemRows, err := pgxPool.Query(ctx, itemQuery, proposalIDs)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, err.Error())
				return
			}
			defer itemRows.Close()

			itemMap := make(map[string][]map[string]interface{})
			itemIDs := make([]string, 0)
			for itemRows.Next() {
				var itemID, propID, desc, cashType, catID, startDate, endDate, recPattern interface{}
				var expAmount float64
				var isRec bool
				if err := itemRows.Scan(&itemID, &propID, &desc, &cashType, &catID, &expAmount, &isRec, &recPattern, &startDate, &endDate); err != nil {
					continue
				}
				item := map[string]interface{}{
					"item_id":             ifaceToString(itemID),
					"description":         ifaceToString(desc),
					"cashflow_type":       ifaceToString(cashType),
					"category_id":         ifaceToString(catID),
					"expected_amount":     expAmount,
					"is_recurring":        isRec,
					"recurrence_pattern":  ifaceToString(recPattern),
					"start_date":          ifaceToString(startDate),
					"end_date":            ifaceToString(endDate),
					"monthly_projections": []interface{}{},
				}
				pid := ifaceToString(propID)
				itemMap[pid] = append(itemMap[pid], item)
				itemIDs = append(itemIDs, ifaceToString(itemID))
			}

			// Fetch monthly projections for items
			if len(itemIDs) > 0 {
				monthlyQuery := `SELECT projection_id, item_id, year, month, projected_amount FROM cashflow_projection_monthly WHERE item_id = ANY($1)`
				monthlyRows, err := pgxPool.Query(ctx, monthlyQuery, itemIDs)
				if err != nil {
					api.RespondWithError(w, http.StatusInternalServerError, err.Error())
					return
				}
				defer monthlyRows.Close()

				monthlyMap := make(map[string][]map[string]interface{})
				for monthlyRows.Next() {
					var projID, itID interface{}
					var year, month int
					var projAmount float64
					if err := monthlyRows.Scan(&projID, &itID, &year, &month, &projAmount); err != nil {
						continue
					}
					monthly := map[string]interface{}{
						"projection_id":    ifaceToString(projID),
						"year":             year,
						"month":            month,
						"projected_amount": projAmount,
					}
					iid := ifaceToString(itID)
					monthlyMap[iid] = append(monthlyMap[iid], monthly)
				}

				// Attach monthly to items
				for pid, items := range itemMap {
					for _, item := range items {
						iid := item["item_id"].(string)
						item["monthly_projections"] = monthlyMap[iid]
					}
					// Attach items to proposals
					for _, prop := range proposals {
						if prop["proposal_id"].(string) == pid {
							prop["items"] = items
							break
						}
					}
				}
			}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "proposals": proposals})
	}
}

// DeleteCashFlowProposal inserts DELETE audit actions for proposals (bulk)
func DeleteCashFlowProposal(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID      string   `json:"user_id"`
			ProposalIDs []string `json:"proposal_ids"`
			Reason      string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, "Invalid JSON")
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
			api.RespondWithResult(w, false, "Invalid user_id or session")
			return
		}
		if len(req.ProposalIDs) == 0 {
			api.RespondWithResult(w, false, "proposal_ids required")
			return
		}
		if strings.TrimSpace(req.Reason) == "" {
			api.RespondWithResult(w, false, "comment required")
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithResult(w, false, "Failed to start transaction: "+err.Error())
			return
		}
		committed := false
		defer func() {
			if !committed {
				tx.Rollback(ctx)
			}
		}()

		// Insert audit actions for each proposal_id
		q := `INSERT INTO audit_action_cashflow_proposal (proposal_id, action_type, processing_status, reason, requested_by, requested_at) VALUES ($1,'DELETE','PENDING_DELETE_APPROVAL',$2,$3,now())`
		for _, pid := range req.ProposalIDs {
			if strings.TrimSpace(pid) == "" {
				continue
			}
			if _, err := tx.Exec(ctx, q, pid, req.Reason, requestedBy); err != nil {
				api.RespondWithResult(w, false, "Failed to insert audit for proposal "+pid+": "+err.Error())
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithResult(w, false, "Commit failed: "+err.Error())
			return
		}
		committed = true
		api.RespondWithResult(w, true, "")
	}
}

// BulkRejectCashFlowProposalActions rejects audit actions for proposals
func BulkRejectCashFlowProposalActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID      string   `json:"user_id"`
			ProposalIDs []string `json:"proposal_ids"`
			Comment     string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, "Invalid JSON")
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
			api.RespondWithResult(w, false, "Invalid user_id or session")
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithResult(w, false, "Failed to start transaction: "+err.Error())
			return
		}
		committed := false
		defer func() {
			if !committed {
				tx.Rollback(ctx)
			}
		}()

		// Fetch latest audit per proposal_id
		sel := `SELECT DISTINCT ON (proposal_id) action_id, proposal_id, processing_status FROM audit_action_cashflow_proposal WHERE proposal_id = ANY($1) ORDER BY proposal_id, requested_at DESC`
		rows, err := tx.Query(ctx, sel, req.ProposalIDs)
		if err != nil {
			api.RespondWithResult(w, false, "Failed to fetch audit rows: "+err.Error())
			return
		}
		defer rows.Close()

		actionIDs := make([]string, 0)
		foundProposals := map[string]bool{}
		cannotReject := []string{}
		for rows.Next() {
			var aid, pid, pstatus string
			if err := rows.Scan(&aid, &pid, &pstatus); err != nil {
				continue
			}
			foundProposals[pid] = true
			if strings.ToUpper(strings.TrimSpace(pstatus)) == "APPROVED" {
				cannotReject = append(cannotReject, pid)
			} else {
				actionIDs = append(actionIDs, aid)
			}
		}

		// Check for missing or cannot reject
		missing := []string{}
		for _, pid := range req.ProposalIDs {
			if !foundProposals[pid] {
				missing = append(missing, pid)
			}
		}
		if len(missing) > 0 || len(cannotReject) > 0 {
			msg := ""
			if len(missing) > 0 {
				msg += "No audit actions found for proposals: " + strings.Join(missing, ", ")
			}
			if len(cannotReject) > 0 {
				if msg != "" {
					msg += "; "
				}
				msg += "Cannot reject already approved proposals: " + strings.Join(cannotReject, ", ")
			}
			api.RespondWithResult(w, false, msg)
			return
		}

		// Update to REJECTED
		upd := `UPDATE audit_action_cashflow_proposal SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2 WHERE action_id = ANY($3)`
		if _, err := tx.Exec(ctx, upd, checkerBy, req.Comment, actionIDs); err != nil {
			api.RespondWithResult(w, false, "Failed to update audit rows: "+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithResult(w, false, "Commit failed: "+err.Error())
			return
		}
		committed = true
		api.RespondWithResult(w, true, "")
	}
}

// BulkApproveCashFlowProposalActions approves audit actions for proposals
func BulkApproveCashFlowProposalActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID      string   `json:"user_id"`
			ProposalIDs []string `json:"proposal_ids"`
			Comment     string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, "Invalid JSON")
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
			api.RespondWithResult(w, false, "Invalid user_id or session")
			return
		}
		if len(req.ProposalIDs) == 0 {
			api.RespondWithResult(w, false, "proposal_ids required")
			return
		}
		if strings.TrimSpace(req.Comment) == "" {
			api.RespondWithResult(w, false, "comment required")
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithResult(w, false, "Failed to start transaction: "+err.Error())
			return
		}
		committed := false
		defer func() {
			if !committed {
				tx.Rollback(ctx)
			}
		}()

		// Fetch latest audit per proposal_id
		sel := `SELECT DISTINCT ON (proposal_id) action_id, proposal_id, action_type, processing_status FROM audit_action_cashflow_proposal WHERE proposal_id = ANY($1) ORDER BY proposal_id, requested_at DESC`
		rows, err := tx.Query(ctx, sel, req.ProposalIDs)
		if err != nil {
			api.RespondWithResult(w, false, "Failed to fetch audit rows: "+err.Error())
			return
		}
		defer rows.Close()

		actionIDs := make([]string, 0)
		foundProposals := map[string]bool{}
		cannotApprove := []string{}
		actionTypeByProposal := map[string]string{}
		for rows.Next() {
			var aid, pid, atype, pstatus string
			if err := rows.Scan(&aid, &pid, &atype, &pstatus); err != nil {
				continue
			}
			foundProposals[pid] = true
			actionTypeByProposal[pid] = atype
			if strings.ToUpper(strings.TrimSpace(pstatus)) == "APPROVED" {
				cannotApprove = append(cannotApprove, pid)
			} else {
				actionIDs = append(actionIDs, aid)
			}
		}

		// Check for missing or cannot approve
		missing := []string{}
		for _, pid := range req.ProposalIDs {
			if !foundProposals[pid] {
				missing = append(missing, pid)
			}
		}
		if len(missing) > 0 || len(cannotApprove) > 0 {
			msg := ""
			if len(missing) > 0 {
				msg += "No audit actions found for proposals: " + strings.Join(missing, ", ")
			}
			if len(cannotApprove) > 0 {
				if msg != "" {
					msg += "; "
				}
				msg += "Cannot approve already approved proposals: " + strings.Join(cannotApprove, ", ")
			}
			api.RespondWithResult(w, false, msg)
			return
		}

		// Update to APPROVED
		upd := `
    UPDATE audit_action_cashflow_proposal 
    SET processing_status='APPROVED', 
        checker_by=$1, 
        checker_at=now(), 
        checker_comment=$2 
    WHERE action_id = ANY($3)
`
		if _, err := tx.Exec(ctx, upd, checkerBy, req.Comment, actionIDs); err != nil {
			api.RespondWithResult(w, false, "Failed to update audit rows: "+err.Error())
			return
		}

		// If any are DELETE, delete the proposal (cascade handles items and monthly)
		deleteProposalIDs := make([]string, 0)
		for _, pid := range req.ProposalIDs {
			if actionTypeByProposal[pid] == "DELETE" {
				deleteProposalIDs = append(deleteProposalIDs, pid)
			}
		}
		if len(deleteProposalIDs) > 0 {
			delQ := `DELETE FROM cashflow_proposal WHERE proposal_id = ANY($1)`
			var err error
			for retries := 0; retries < 3; retries++ {
				if _, err = tx.Exec(ctx, delQ, deleteProposalIDs); err == nil {
					break
				}
				if retries < 2 {
					time.Sleep(time.Duration(retries+1) * 100 * time.Millisecond) // Wait 100ms, 200ms
				}
			}
			if err != nil {
				api.RespondWithResult(w, false, "Failed to delete proposals after retries: "+err.Error())
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithResult(w, false, "Commit failed: "+err.Error())
			return
		}
		committed = true
		api.RespondWithResult(w, true, "")
	}
}


func CreateFromFlattened(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID      string `json:"user_id"`
			Projections []struct {
				Entry struct {
					Type           string  `json:"type"`
					CategoryName   string  `json:"categoryName"`
					Entity         string  `json:"entity"`
					Department     string  `json:"department"`
					ExpectedAmount float64 `json:"expectedAmount"`
					Recurring      bool    `json:"recurring"`
					Frequency      string  `json:"frequency"`
				} `json:"entry"`
				Projection map[string]interface{} `json:"projection"`
			} `json:"projections"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, "Invalid JSON: "+err.Error())
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
			api.RespondWithResult(w, false, "Invalid user_id or session")
			return
		}
		if len(req.Projections) == 0 {
			api.RespondWithResult(w, false, "No projections provided")
			return
		}

		// month abbrev map
		monthMap := map[string]int{"Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6, "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12}

		ctx := context.Background()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithResult(w, false, "Failed to start transaction: "+err.Error())
			return
		}
		committed := false
		defer func() {
			if !committed {
				tx.Rollback(ctx)
			}
		}()

		createdCount := 0

		for _, p := range req.Projections {
			entry := p.Entry
			// basic validation
			if strings.TrimSpace(entry.Type) == "" || strings.TrimSpace(entry.CategoryName) == "" || strings.TrimSpace(entry.Entity) == "" || strings.TrimSpace(entry.Department) == "" {
				api.RespondWithResult(w, false, "Missing required fields in entry")
				return
			}
			tUpper := Capitalize(strings.ToLower(strings.TrimSpace(entry.Type)))
			if tUpper != "Inflow" && tUpper != "Outflow" {
				api.RespondWithResult(w, false, "Invalid cashflow type: "+entry.Type)
				return
			}

			// create proposal
			proposalID := fmt.Sprintf("PROP-%06d", time.Now().UnixNano()%1000000)
			proposalName := fmt.Sprintf("%s - %s - %d", entry.CategoryName, entry.Frequency, time.Now().Unix())
			currencyCode := "USD" // assumption
			effectiveDate := time.Now().Format("2006-01-02")
			insProp := `INSERT INTO cashflow_proposal (proposal_id, proposal_name, entity_name, department_id, currency_code, effective_date, recurrence_type, recurrence_frequency, status) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`
			if _, err := tx.Exec(ctx, insProp, proposalID, proposalName, entry.Entity, entry.Department, currencyCode, effectiveDate, entry.Frequency, entry.Frequency, "Active"); err != nil {
				api.RespondWithResult(w, false, "Failed to create proposal: "+err.Error())
				return
			}

			// create item
			// verify category exists in master table to avoid FK violation
			var existingCat string
			if err := tx.QueryRow(ctx, `SELECT category_name FROM mastercashflowcategory WHERE category_name=$1`, entry.CategoryName).Scan(&existingCat); err != nil {
				api.RespondWithResult(w, false, "Master category not found: "+entry.CategoryName+". Create the category or use a valid category_id before importing.")
				return
			}

			itemID := fmt.Sprintf("ITEM-%06d", time.Now().UnixNano()%1000000)
			insItem := `INSERT INTO cashflow_proposal_item (item_id, proposal_id, description, cashflow_type, category_id, expected_amount, is_recurring, recurrence_pattern, start_date, end_date) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`
			if _, err := tx.Exec(ctx, insItem, itemID, proposalID, entry.CategoryName, tUpper, entry.CategoryName, entry.ExpectedAmount, entry.Recurring, entry.Frequency, effectiveDate, nil); err != nil {
				api.RespondWithResult(w, false, "Failed to create item: "+err.Error())
				return
			}

			// insert monthly projections parsed from projection map
			for k, v := range p.Projection {
				if strings.ToLower(k) == "total" {
					continue
				}
				// expect keys like Jan-25 or Jan-2025
				parts := strings.Split(k, "-")
				if len(parts) < 2 {
					// skip unknown keys
					continue
				}
				mStr := parts[0]
				yStr := parts[1]
				mInt, ok := monthMap[mStr]
				if !ok {
					// try full month name parse
					if len(mStr) >= 3 {
						mStrShort := Capitalize(strings.ToLower(mStr[:3]))
						if mm, ok2 := monthMap[mStrShort]; ok2 {
							mInt = mm
							ok = true
						}
					}
				}
				if !ok {
					continue
				}
				// year handling: 2-digit -> 2000+, else parse
				year := 0
				if len(yStr) == 2 {
					if yi, err := strconv.Atoi(yStr); err == nil {
						year = 2000 + yi
					}
				} else {
					if yi, err := strconv.Atoi(yStr); err == nil {
						year = yi
					}
				}
				if year == 0 {
					continue
				}

				// convert projected amount to float64 from various possible types
				var amt float64
				switch tv := v.(type) {
				case float64:
					amt = tv
				case float32:
					amt = float64(tv)
				case int:
					amt = float64(tv)
				case int64:
					amt = float64(tv)
				case json.Number:
					if f, err := tv.Float64(); err == nil {
						amt = f
					} else {
						continue
					}
				case string:
					s := strings.TrimSpace(tv)
					if s == "" {
						continue
					}
					if f, err := strconv.ParseFloat(s, 64); err == nil {
						amt = f
					} else {
						continue
					}
				default:
					// unsupported type, skip
					continue
				}

				projID := fmt.Sprintf("PROJ-%06d", time.Now().UnixNano()%1000000)
				insMonthly := `INSERT INTO cashflow_projection_monthly (projection_id, item_id, year, month, projected_amount) VALUES ($1,$2,$3,$4,$5)`
				if _, err := tx.Exec(ctx, insMonthly, projID, itemID, year, mInt, amt); err != nil {
					api.RespondWithResult(w, false, "Failed to create monthly projection: "+err.Error())
					return
				}
			}

			// create audit action
			auditQ := `INSERT INTO audit_action_cashflow_proposal (proposal_id, action_type, processing_status, reason, requested_by, requested_at) VALUES ($1,'CREATE','PENDING_APPROVAL', $2, $3, now())`
			if _, err := tx.Exec(ctx, auditQ, proposalID, nil, createdBy); err != nil {
				api.RespondWithResult(w, false, "Failed to create audit for proposal: "+err.Error())
				return
			}

			createdCount++
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithResult(w, false, "Failed to commit transaction: "+err.Error())
			return
		}
		committed = true

		api.RespondWithResult(w, true, fmt.Sprintf("Successfully created %d projections", createdCount))
	}
}

func AbsorbFlattenedProjections(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			Header struct {
				Currency       string `json:"currency,omitempty"`
				ProposalName   string `json:"proposal_name,omitempty"`
				EffectiveDate  string `json:"effective_date,omitempty"`
				ProjectionType string `json:"projection_type,omitempty"`
			} `json:"header"`
			Projections []struct {
				Entry struct {
					Description    string  `json:"description,omitempty"`
					Type           string  `json:"type"`
					CategoryName   string  `json:"categoryName"`
					Entity         string  `json:"entity"`
					Department     string  `json:"department"`
					ExpectedAmount float64 `json:"expectedAmount"`
					Recurring      bool    `json:"recurring"`
					Frequency      string  `json:"frequency"`
				} `json:"entry"`
				Projection map[string]interface{} `json:"projection"`
			} `json:"projections"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, "Invalid JSON: "+err.Error())
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
			api.RespondWithResult(w, false, "Invalid user_id or session")
			return
		}
		if len(req.Projections) == 0 {
			api.RespondWithResult(w, false, "No projections provided")
			return
		}

		monthMap := map[string]int{"Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6, "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12}

		ctx := context.Background()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithResult(w, false, "Failed to start transaction: "+err.Error())
			return
		}
		committed := false
		defer func() {
			if !committed {
				tx.Rollback(ctx)
			}
		}()

		// prepare proposal from header (single proposal for all items)
		proposalID := fmt.Sprintf("PROP-%06d", time.Now().UnixNano()%1000000)
		propName := strings.TrimSpace(req.Header.ProposalName)
		if propName == "" {
			propName = "Imported Proposal"
		}
		currency := strings.TrimSpace(req.Header.Currency)
		if currency == "" {
			currency = "USD"
		}
		effDate := strings.TrimSpace(req.Header.EffectiveDate)
		if effDate == "" {
			effDate = time.Now().Format("2006-01-02")
		}
		recurrenceType := strings.TrimSpace(req.Header.ProjectionType)

		insProp := `INSERT INTO cashflow_proposal (proposal_id, proposal_name, currency_code, effective_date, recurrence_type, status) VALUES ($1,$2,$3,$4,$5,$6)`
		if _, err := tx.Exec(ctx, insProp, proposalID, propName, currency, effDate, recurrenceType, "Active"); err != nil {
			api.RespondWithResult(w, false, "Failed to create proposal: "+err.Error())
			return
		}

		created := 0

		for idx, p := range req.Projections {
			entry := p.Entry
			// normalize and validate
			if strings.TrimSpace(entry.Type) == "" || strings.TrimSpace(entry.CategoryName) == "" || strings.TrimSpace(entry.Entity) == "" || strings.TrimSpace(entry.Department) == "" {
				api.RespondWithResult(w, false, fmt.Sprintf("Missing required fields in projection index %d", idx))
				return
			}
			tUpper := Capitalize(strings.ToLower(strings.TrimSpace(entry.Type)))
			if tUpper != "Inflow" && tUpper != "Outflow" {
				api.RespondWithResult(w, false, "Invalid cashflow type: "+entry.Type)
				return
			}

			// verify category exists
			var tmp string
			if err := tx.QueryRow(ctx, `SELECT category_name FROM mastercashflowcategory WHERE category_name=$1`, entry.CategoryName).Scan(&tmp); err != nil {
				api.RespondWithResult(w, false, "Master category not found: "+entry.CategoryName)
				return
			}

			// optional entity/department: allow empty -> store NULL
			var entityParam interface{}
			var deptParam interface{}
			if s := strings.TrimSpace(entry.Entity); s != "" {
				entityParam = s
				// verify entity exists if provided
				if err := tx.QueryRow(ctx, `SELECT entity_name FROM masterentitycash WHERE entity_name=$1`, s).Scan(&tmp); err != nil {
					api.RespondWithResult(w, false, "Master entity not found: "+s)
					return
				}
			} else {
				entityParam = nil
			}
			if d := strings.TrimSpace(entry.Department); d != "" {
				deptParam = d
				if err := tx.QueryRow(ctx, `SELECT centre_code FROM mastercostprofitcenter WHERE centre_code=$1`, d).Scan(&tmp); err != nil {
					api.RespondWithResult(w, false, "Master department not found: "+d)
					return
				}
			} else {
				deptParam = nil
			}

			// create item
			itemID := fmt.Sprintf("ITEM-%06d", time.Now().UnixNano()%1000000)
			description := strings.TrimSpace(entry.Description)
			if description == "" {
				description = entry.CategoryName
			}
			insItem := `INSERT INTO cashflow_proposal_item (item_id, proposal_id, description, cashflow_type, category_id, expected_amount, is_recurring, recurrence_pattern, start_date, end_date, entity_name, department_id, recurrence_frequency) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`
			if _, err := tx.Exec(ctx, insItem, itemID, proposalID, description, tUpper, entry.CategoryName, entry.ExpectedAmount, entry.Recurring, entry.Frequency, effDate, nil, entityParam, deptParam, entry.Frequency); err != nil {
				api.RespondWithResult(w, false, "Failed to create item: "+err.Error())
				return
			}

			// monthly projections
			for k, v := range p.Projection {
				if strings.ToLower(k) == "total" || k == "type" || k == "categoryName" {
					continue
				}
				parts := strings.Split(k, "-")
				if len(parts) < 2 {
					continue
				}
				mStr := parts[0]
				yStr := parts[1]
				mInt, ok := monthMap[mStr]
				if !ok {
					if len(mStr) >= 3 {
						mStrShort := Capitalize(strings.ToLower(mStr[:3]))
						if mm, ok2 := monthMap[mStrShort]; ok2 {
							mInt = mm
							ok = true
						}
					}
				}
				if !ok {
					continue
				}
				year := 0
				if len(yStr) == 2 {
					if yi, err := strconv.Atoi(yStr); err == nil {
						year = 2000 + yi
					}
				} else {
					if yi, err := strconv.Atoi(yStr); err == nil {
						year = yi
					}
				}
				if year == 0 {
					continue
				}

				var amt float64
				switch tv := v.(type) {
				case float64:
					amt = tv
				case float32:
					amt = float64(tv)
				case int:
					amt = float64(tv)
				case int64:
					amt = float64(tv)
				case json.Number:
					if f, err := tv.Float64(); err == nil {
						amt = f
					} else {
						continue
					}
				case string:
					s := strings.TrimSpace(tv)
					if s == "" {
						continue
					}
					if f, err := strconv.ParseFloat(s, 64); err == nil {
						amt = f
					} else {
						continue
					}
				default:
					continue
				}

				projID := fmt.Sprintf("PROJ-%06d", time.Now().UnixNano()%1000000)
				insMonthly := `INSERT INTO cashflow_projection_monthly (projection_id, item_id, year, month, projected_amount) VALUES ($1,$2,$3,$4,$5)`
				if _, err := tx.Exec(ctx, insMonthly, projID, itemID, year, mInt, amt); err != nil {
					api.RespondWithResult(w, false, "Failed to create monthly projection: "+err.Error())
					return
				}
			}

			created++
		}

		// audit - one per proposal
		auditQ := `INSERT INTO audit_action_cashflow_proposal (proposal_id, action_type, processing_status, reason, requested_by, requested_at) VALUES ($1,'CREATE','PENDING_APPROVAL', $2, $3, now())`
		if _, err := tx.Exec(ctx, auditQ, proposalID, "Imported", createdBy); err != nil {
			api.RespondWithResult(w, false, "Failed to create audit: "+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithResult(w, false, "Failed to commit transaction: "+err.Error())
			return
		}
		committed = true

		api.RespondWithResult(w, true, fmt.Sprintf("Successfully imported %d projections", created))
	}
}


func GetFlattenedProjections(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Parse request body
		var req struct {
			UserID     string `json:"user_id"`
			ProposalID string `json:"proposal_id,omitempty"`
			Entity     string `json:"entity,omitempty"`
			Department string `json:"department,omitempty"`
			Category   string `json:"category,omitempty"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, "Invalid JSON: "+err.Error())
			return
		}

		// Verify user session
		createdBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				createdBy = s.Email
				break
			}
		}
		if createdBy == "" {
			api.RespondWithResult(w, false, "Invalid user_id or session")
			return
		}

		ctx := context.Background()

		// Build base query
		baseQuery := `
			SELECT 
				cp.proposal_id,
				cp.proposal_name,
				cp.entity_name,
				cp.department_id,
				cp.currency_code,
				cp.effective_date,
				cp.recurrence_type,
				cp.recurrence_frequency,
				cp.status,
				cpi.item_id,
				cpi.description,
				cpi.cashflow_type,
				cpi.category_id,
				cpi.expected_amount,
				cpi.is_recurring,
				cpi.recurrence_pattern,
				cpi.start_date,
				cpi.end_date
			FROM cashflow_proposal cp
			JOIN cashflow_proposal_item cpi ON cp.proposal_id = cpi.proposal_id
			WHERE 1=1
		`

		params := []interface{}{}
		paramCount := 0

		if req.ProposalID != "" {
			paramCount++
			baseQuery += fmt.Sprintf(" AND cp.proposal_id = $%d", paramCount)
			params = append(params, req.ProposalID)
		}
		if req.Entity != "" {
			paramCount++
			baseQuery += fmt.Sprintf(" AND cp.entity_name = $%d", paramCount)
			params = append(params, req.Entity)
		}
		if req.Department != "" {
			paramCount++
			baseQuery += fmt.Sprintf(" AND cp.department_id = $%d", paramCount)
			params = append(params, req.Department)
		}
		if req.Category != "" {
			paramCount++
			baseQuery += fmt.Sprintf(" AND cpi.category_id = $%d", paramCount)
			params = append(params, req.Category)
		}

		// Add order by
		baseQuery += " ORDER BY cp.effective_date DESC, cp.proposal_id"

		rows, err := pgxPool.Query(ctx, baseQuery, params...)
		if err != nil {
			api.RespondWithResult(w, false, "Failed to query proposals: "+err.Error())
			return
		}
		defer rows.Close()

		type ProposalItem struct {
			ProposalID          string
			ProposalName        string
			EntityName          string
			DepartmentID        string
			CurrencyCode        string
			EffectiveDate       time.Time
			RecurrenceType      string
			RecurrenceFrequency string
			Status              string
			ItemID              string
			Description         string
			CashflowType        string
			CategoryID          string
			ExpectedAmount      float64
			IsRecurring         bool
			RecurrencePattern   string
			StartDate           time.Time
			EndDate             *time.Time
		}

		var proposals []ProposalItem
		proposalMap := make(map[string][]ProposalItem)

		for rows.Next() {
			var pi ProposalItem
			err := rows.Scan(
				&pi.ProposalID,
				&pi.ProposalName,
				&pi.EntityName,
				&pi.DepartmentID,
				&pi.CurrencyCode,
				&pi.EffectiveDate,
				&pi.RecurrenceType,
				&pi.RecurrenceFrequency,
				&pi.Status,
				&pi.ItemID,
				&pi.Description,
				&pi.CashflowType,
				&pi.CategoryID,
				&pi.ExpectedAmount,
				&pi.IsRecurring,
				&pi.RecurrencePattern,
				&pi.StartDate,
				&pi.EndDate,
			)
			if err != nil {
				api.RespondWithResult(w, false, "Failed to scan proposal: "+err.Error())
				return
			}
			proposals = append(proposals, pi)
			proposalMap[pi.ProposalID] = append(proposalMap[pi.ProposalID], pi)
		}

		if err := rows.Err(); err != nil {
			api.RespondWithResult(w, false, "Error reading proposals: "+err.Error())
			return
		}

		if len(proposals) == 0 {
			api.RespondWithResult(w, false, "No projections found")
			return
		}

		// Get monthly projections for all items
		itemIDs := make([]string, 0, len(proposals))
		for _, pi := range proposals {
			itemIDs = append(itemIDs, pi.ItemID)
		}

		monthlyQuery := `
			SELECT 
				item_id,
				year,
				month,
				projected_amount
			FROM cashflow_projection_monthly
			WHERE item_id = ANY($1)
			ORDER BY item_id, year, month
		`

		monthlyRows, err := pgxPool.Query(ctx, monthlyQuery, itemIDs)
		if err != nil {
			api.RespondWithResult(w, false, "Failed to query monthly projections: "+err.Error())
			return
		}
		defer monthlyRows.Close()

		monthlyMap := make(map[string]map[string]interface{})
		monthNames := map[int]string{
			1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
			7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec",
		}

		for monthlyRows.Next() {
			var itemID string
			var year, month int
			var amount float64

			err := monthlyRows.Scan(&itemID, &year, &month, &amount)
			if err != nil {
				api.RespondWithResult(w, false, "Failed to scan monthly projection: "+err.Error())
				return
			}

			monthName, exists := monthNames[month]
			if !exists {
				continue
			}

			key := fmt.Sprintf("%s-%d", monthName, year%100) // Use 2-digit year

			if monthlyMap[itemID] == nil {
				monthlyMap[itemID] = make(map[string]interface{})
			}
			monthlyMap[itemID][key] = amount
		}

		if err := monthlyRows.Err(); err != nil {
			api.RespondWithResult(w, false, "Error reading monthly projections: "+err.Error())
			return
		}

		// Build response
		response := struct {
			UserID      string                   `json:"user_id"`
			ProposalID  string                   `json:"proposal_id,omitempty"`
			Projections []map[string]interface{} `json:"projections"`
		}{
			UserID:      req.UserID,
			Projections: make([]map[string]interface{}, 0),
		}

		// If a specific proposal ID was requested, include it in response
		if req.ProposalID != "" {
			response.ProposalID = req.ProposalID
		}

		for proposalID, items := range proposalMap {
			for _, item := range items {
				projection := make(map[string]interface{})

				// Entry section
				entry := map[string]interface{}{
					"currency":       item.CurrencyCode,
					"type":           item.CashflowType,
					"proposal_name":  item.ProposalName,
					"effective_date": item.EffectiveDate.Format("2006-01-02"),
					"categoryName":   item.CategoryID,
					"entity":         item.EntityName,
					"department":     item.DepartmentID,
					"expectedAmount": item.ExpectedAmount,
					"recurring":      item.IsRecurring,
					"frequency":      item.RecurrencePattern,
					"proposal_id":    proposalID, // Add proposal_id to entry
				}
				projection["entry"] = entry

				// Monthly projections section
				monthlyProj := make(map[string]interface{})
				if itemMonthly, exists := monthlyMap[item.ItemID]; exists {
					for k, v := range itemMonthly {
						monthlyProj[k] = v
					}
				}
				// Add type and categoryName to projection for consistency with input format
				monthlyProj["proposal_id"] = proposalID
				monthlyProj["type"] = item.CashflowType
				monthlyProj["categoryName"] = item.CategoryID
				projection["projection"] = monthlyProj

				response.Projections = append(response.Projections, projection)
			}
		}

		// Return JSON response
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(response)
	}
}


func GetAuditActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID           string   `json:"user_id"`
			ProposalIDs      []string `json:"proposal_ids"`
			ActionID         string   `json:"action_id"`
			ActionType       string   `json:"action_type"`
			ProcessingStatus string   `json:"processing_status"`
			RequestedBy      string   `json:"requested_by"`
			Since            string   `json:"since"`
			Until            string   `json:"until"`
			Limit            int      `json:"limit"`
			Offset           int      `json:"offset"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, "Invalid JSON: "+err.Error())
			return
		}

		// validate session
		valid := false
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				valid = true
				break
			}
		}
		if !valid {
			api.RespondWithResult(w, false, "Invalid user_id or session")
			return
		}

		ctx := r.Context()

		base := `SELECT action_id, proposal_id, action_type, processing_status, reason, requested_by, requested_at, checker_by, checker_at, checker_comment FROM audit_action_cashflow_proposal WHERE 1=1`
		params := []interface{}{}
		pc := 0

		if len(req.ProposalIDs) > 0 {
			pc++
			base += fmt.Sprintf(" AND proposal_id = ANY($%d)", pc)
			params = append(params, req.ProposalIDs)
		}
		if strings.TrimSpace(req.ActionID) != "" {
			pc++
			base += fmt.Sprintf(" AND action_id = $%d", pc)
			params = append(params, req.ActionID)
		}
		if strings.TrimSpace(req.ActionType) != "" {
			pc++
			base += fmt.Sprintf(" AND action_type = $%d", pc)
			params = append(params, req.ActionType)
		}
		if strings.TrimSpace(req.ProcessingStatus) != "" {
			pc++
			base += fmt.Sprintf(" AND processing_status = $%d", pc)
			params = append(params, req.ProcessingStatus)
		}
		if strings.TrimSpace(req.RequestedBy) != "" {
			pc++
			base += fmt.Sprintf(" AND requested_by = $%d", pc)
			params = append(params, req.RequestedBy)
		}
		// date filters
		if strings.TrimSpace(req.Since) != "" {
			if t, err := time.Parse("2006-01-02", req.Since); err == nil {
				pc++
				base += fmt.Sprintf(" AND requested_at >= $%d", pc)
				params = append(params, t)
			}
		}
		if strings.TrimSpace(req.Until) != "" {
			if t, err := time.Parse("2006-01-02", req.Until); err == nil {
				// include whole day
				t = t.Add(23*time.Hour + 59*time.Minute + 59*time.Second)
				pc++
				base += fmt.Sprintf(" AND requested_at <= $%d", pc)
				params = append(params, t)
			}
		}

		// ordering and limit
		base += " ORDER BY requested_at DESC"
		if req.Limit <= 0 || req.Limit > 1000 {
			req.Limit = 100
		}
		pc++
		base += fmt.Sprintf(" LIMIT $%d", pc)
		params = append(params, req.Limit)
		if req.Offset > 0 {
			pc++
			base += fmt.Sprintf(" OFFSET $%d", pc)
			params = append(params, req.Offset)
		}

		rows, err := pgxPool.Query(ctx, base, params...)
		if err != nil {
			api.RespondWithResult(w, false, "Failed to query audit actions: "+err.Error())
			return
		}
		defer rows.Close()

		actions := make([]map[string]interface{}, 0)
		for rows.Next() {
			var (
				actionID                                                                                                         string
				proposalID, actionType, processingStatus, reason, requestedBy, requestedAt, checkerBy, checkerAt, checkerComment interface{}
			)
			if err := rows.Scan(&actionID, &proposalID, &actionType, &processingStatus, &reason, &requestedBy, &requestedAt, &checkerBy, &checkerAt, &checkerComment); err != nil {
				continue
			}
			a := map[string]interface{}{
				"action_id":         ifaceToString(actionID),
				"proposal_id":       ifaceToString(proposalID),
				"action_type":       ifaceToString(actionType),
				"processing_status": ifaceToString(processingStatus),
				"reason":            ifaceToString(reason),
				"requested_by":      ifaceToString(requestedBy),
				"requested_at":      ifaceToTimeString(requestedAt),
				"checker_by":        ifaceToString(checkerBy),
				"checker_at":        ifaceToTimeString(checkerAt),
				"checker_comment":   ifaceToString(checkerComment),
			}
			actions = append(actions, a)
		}

		if err := rows.Err(); err != nil {
			api.RespondWithResult(w, false, "Error reading audit rows: "+err.Error())
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "actions": actions})
	}
}



func GetProposalVersion(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID     string `json:"user_id"`
			ProposalID string `json:"proposal_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, "Invalid JSON: "+err.Error())
			return
		}

		// validate session
		valid := false
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				valid = true
				break
			}
		}
		if !valid {
			api.RespondWithResult(w, false, "Invalid user_id or session")
			return
		}
		if strings.TrimSpace(req.ProposalID) == "" {
			api.RespondWithResult(w, false, "proposal_id required")
			return
		}

		ctx := r.Context()

		// 1) Fetch proposal header with latest audit processing_status
		hq := `
			SELECT
				p.proposal_id,
				p.proposal_name,
				p.old_proposal_name,
				p.effective_date,
				p.old_effective_date,
				p.currency_code AS currency,
				p.old_currency_code,
				p.recurrence_type AS proposal_type,
				p.old_recurrence_type,
				a.processing_status
			FROM cashflow_proposal p
			LEFT JOIN LATERAL (
				SELECT processing_status
				FROM audit_action_cashflow_proposal a2
				WHERE a2.proposal_id = p.proposal_id
				ORDER BY requested_at DESC
				LIMIT 1
			) a ON TRUE
			WHERE p.proposal_id = $1
			LIMIT 1;
			`

		var (
			h_proposalID, h_proposalName, h_oldProposalName, h_currency, h_oldCurrency, h_proposalType, h_oldProposalType, h_processingStatus interface{}
			h_effectiveDate, h_oldEffectiveDate                                                                                               interface{}
		)

		if err := pgxPool.QueryRow(ctx, hq, req.ProposalID).Scan(
			&h_proposalID,
			&h_proposalName,
			&h_oldProposalName,
			&h_effectiveDate,
			&h_oldEffectiveDate,
			&h_currency,
			&h_oldCurrency,
			&h_proposalType,
			&h_oldProposalType,
			&h_processingStatus,
		); err != nil {
			api.RespondWithResult(w, false, "Proposal not found or query error: "+err.Error())
			return
		}

		header := map[string]interface{}{
			"proposal_id":         ifaceToString(h_proposalID),
			"proposal_name":       ifaceToString(h_proposalName),
			"old_proposal_name":   ifaceToString(h_oldProposalName),
			"effective_date":      ifaceToTimeString(h_effectiveDate),
			"old_effective_date":  ifaceToTimeString(h_oldEffectiveDate),
			"currency":            ifaceToString(h_currency),
			"old_currency":        ifaceToString(h_oldCurrency),
			"projection_type":     ifaceToString(h_proposalType),
			"old_projection_type": ifaceToString(h_oldProposalType),
			"processing_status":   ifaceToString(h_processingStatus),
		}
		// fetch audit actions for this proposal
		actions := make([]map[string]interface{}, 0)
		aq := `SELECT action_id, proposal_id, action_type, processing_status, reason, requested_by, requested_at, checker_by, checker_at, checker_comment FROM audit_action_cashflow_proposal WHERE proposal_id = $1 ORDER BY requested_at DESC`
		if aRows, aErr := pgxPool.Query(ctx, aq, req.ProposalID); aErr == nil {
			defer aRows.Close()
			for aRows.Next() {
				var (
					actionID                                                                                                         string
					proposalID, actionType, processingStatus, reason, requestedBy, requestedAt, checkerBy, checkerAt, checkerComment interface{}
				)
				if err := aRows.Scan(&actionID, &proposalID, &actionType, &processingStatus, &reason, &requestedBy, &requestedAt, &checkerBy, &checkerAt, &checkerComment); err != nil {
					continue
				}
				a := map[string]interface{}{
					"action_id":         ifaceToString(actionID),
					"proposal_id":       ifaceToString(proposalID),
					"action_type":       ifaceToString(actionType),
					"processing_status": ifaceToString(processingStatus),
					"reason":            ifaceToString(reason),
					"requested_by":      ifaceToString(requestedBy),
					"requested_at":      ifaceToTimeString(requestedAt),
					"checker_by":        ifaceToString(checkerBy),
					"checker_at":        ifaceToTimeString(checkerAt),
					"checker_comment":   ifaceToString(checkerComment),
				}
				actions = append(actions, a)
			}
		}

		// 2) Fetch items for the proposal
		itemQ := `
			SELECT item_id, description, cashflow_type, old_cashflow_type, category_id, old_category_id, expected_amount, old_expected_amount, is_recurring, old_is_recurring, recurrence_pattern, old_recurrence_pattern, start_date, old_start_date, end_date, old_end_date, entity_name, old_entity_name, department_id, old_department_id, recurrence_frequency, old_recurrence_frequency
			FROM cashflow_proposal_item
			WHERE proposal_id = $1
			ORDER BY created_at
			`

		rows, err := pgxPool.Query(ctx, itemQ, req.ProposalID)
		if err != nil {
			api.RespondWithResult(w, false, "Failed to query proposal items: "+err.Error())
			return
		}
		defer rows.Close()

		projections := make([]map[string]interface{}, 0)

		// map for monthly projections per item
		for rows.Next() {
			var (
				itemID, description, cashflowType, oldCashflowType, categoryID, oldCategoryID, entityName, oldEntityName, departmentID, oldDepartmentID, recurrenceFrequency, oldRecurrenceFrequency interface{}
				expectedAmountI, oldExpectedAmountI, isRecurringI, oldIsRecurringI, recurrencePattern, oldRecurrencePattern, startDate, oldStartDate, endDate, oldEndDate                            interface{}
			)

			if err := rows.Scan(&itemID, &description, &cashflowType, &oldCashflowType, &categoryID, &oldCategoryID, &expectedAmountI, &oldExpectedAmountI, &isRecurringI, &oldIsRecurringI, &recurrencePattern, &oldRecurrencePattern, &startDate, &oldStartDate, &endDate, &oldEndDate, &entityName, &oldEntityName, &departmentID, &oldDepartmentID, &recurrenceFrequency, &oldRecurrenceFrequency); err != nil {
				continue
			}

			// convert amounts
			expectedAmount := 0.0
			switch v := expectedAmountI.(type) {
			case float64:
				expectedAmount = v
			case int64:
				expectedAmount = float64(v)
			case nil:
				expectedAmount = 0
			case string:
				if f, err := strconv.ParseFloat(v, 64); err == nil {
					expectedAmount = f
				}
			}
			oldExpectedAmount := 0.0
			switch v := oldExpectedAmountI.(type) {
			case float64:
				oldExpectedAmount = v
			case int64:
				oldExpectedAmount = float64(v)
			case nil:
				oldExpectedAmount = 0
			case string:
				if f, err := strconv.ParseFloat(v, 64); err == nil {
					oldExpectedAmount = f
				}
			}

			// recurring flags
			isRec := false
			switch v := isRecurringI.(type) {
			case bool:
				isRec = v
			case string:
				if b, err := strconv.ParseBool(v); err == nil {
					isRec = b
				}
			}
			oldIsRec := false
			switch v := oldIsRecurringI.(type) {
			case bool:
				oldIsRec = v
			case string:
				if b, err := strconv.ParseBool(v); err == nil {
					oldIsRec = b
				}
			}

			// build entry
			entry := map[string]interface{}{
				"item_id":                ifaceToString(itemID),
				"description":            ifaceToString(description),
				"type":                   ifaceToString(cashflowType),
				"old_type":               ifaceToString(oldCashflowType),
				"categoryName":           ifaceToString(categoryID),
				"old_categoryName":       ifaceToString(oldCategoryID),
				"entity":                 ifaceToString(entityName),
				"old_entity":             ifaceToString(oldEntityName),
				"department":             ifaceToString(departmentID),
				"old_department":         ifaceToString(oldDepartmentID),
				"expectedAmount":         expectedAmount,
				"old_expectedAmount":     oldExpectedAmount,
				"recurring":              isRec,
				"old_recurring":          oldIsRec,
				"frequency":              ifaceToString(recurrenceFrequency),
				"old_frequency":          ifaceToString(oldRecurrenceFrequency),
				"recurrence_pattern":     ifaceToString(recurrencePattern),
				"old_recurrence_pattern": ifaceToString(oldRecurrencePattern),
				"start_date":             ifaceToTimeString(startDate),
				"old_start_date":         ifaceToTimeString(oldStartDate),
				"end_date":               ifaceToTimeString(endDate),
				"old_end_date":           ifaceToTimeString(oldEndDate),
			}

			// fetch monthly projections for this item
			monthlyQ := `SELECT year, month, projected_amount FROM cashflow_projection_monthly WHERE item_id = $1 ORDER BY year, month`
			mrows, err := pgxPool.Query(ctx, monthlyQ, ifaceToString(itemID))
			if err != nil {
				// still append entry without monthly data
				projections = append(projections, map[string]interface{}{"entry": entry, "projection": map[string]interface{}{"type": ifaceToString(cashflowType), "categoryName": ifaceToString(categoryID)}})
				continue
			}

			projMap := map[string]interface{}{
				"type":         ifaceToString(cashflowType),
				"categoryName": ifaceToString(categoryID),
				"item_id":      ifaceToString(itemID),
			}
			monthNames := map[int]string{1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun", 7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"}
			for mrows.Next() {
				var year, month int
				var amt float64
				if err := mrows.Scan(&year, &month, &amt); err != nil {
					continue
				}
				mn := monthNames[month]
				key := fmt.Sprintf("%s-%02d", mn, year%100)
				projMap[key] = amt
			}
			mrows.Close()

			projections = append(projections, map[string]interface{}{"entry": entry, "projection": projMap})
		}

		if err := rows.Err(); err != nil {
			api.RespondWithResult(w, false, "Error reading items: "+err.Error())
			return
		}

		// build response
		resp := map[string]interface{}{
			"success":     true,
			"header":      header,
			"projections": projections,
			"actions":     actions,
			// "user_id":     req.UserID,
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}
}

func GetProjectionsSummary(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, "Invalid JSON: "+err.Error())
			return
		}

		// validate session
		valid := false
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				valid = true
				break
			}
		}
		if !valid {
			api.RespondWithResult(w, false, "Invalid user_id or session")
			return
		}

		ctx := r.Context()

		q := `
			SELECT p.proposal_id,
			       p.recurrence_type,
			       p.proposal_name,
			       p.effective_date,
			       p.currency_code,
			       a.processing_status
			FROM cashflow_proposal p
			LEFT JOIN LATERAL (
				SELECT processing_status
				FROM audit_action_cashflow_proposal a2
				WHERE a2.proposal_id = p.proposal_id
				ORDER BY requested_at DESC
				LIMIT 1
			) a ON TRUE
			ORDER BY p.effective_date DESC, p.proposal_id
		`

		rows, err := pgxPool.Query(ctx, q)
		if err != nil {
			api.RespondWithResult(w, false, "Failed to query proposals: "+err.Error())
			return
		}
		defer rows.Close()

		out := make([]map[string]interface{}, 0)
		for rows.Next() {
			var proposalID, recurrenceType, proposalName, currencyCode, processingStatus string
			var effectiveDate time.Time
			if err := rows.Scan(&proposalID, &recurrenceType, &proposalName, &effectiveDate, &currencyCode, &processingStatus); err != nil {
				continue
			}

			// Build header matching cashflow_proposal table fields
			header := map[string]interface{}{
				"proposal_id":       proposalID,
				"proposal_type":     recurrenceType,
				"proposal_name":     proposalName,
				"effective_date":    effectiveDate.Format("2006-01-02"),
				"currency":          currencyCode,
				"processing_status": processingStatus,
			}

			out = append(out, header)
		}

		if err := rows.Err(); err != nil {
			api.RespondWithResult(w, false, "Error reading rows: "+err.Error())
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "header": out})
	}
}
