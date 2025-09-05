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
			UserID      string `json:"user_id"`
			Projections []struct {
				Entry struct {
					Currency       string  `json:"currency"`
					Type           string  `json:"type"`
					ProposalName   string  `json:"proposal_name"`
					EffectiveDate  string  `json:"effective_date"`
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

			// verify master records: category, department, entity
			var tmp string
			if err := tx.QueryRow(ctx, `SELECT category_name FROM mastercashflowcategory WHERE category_name=$1`, entry.CategoryName).Scan(&tmp); err != nil {
				api.RespondWithResult(w, false, "Master category not found: "+entry.CategoryName)
				return
			}
			if err := tx.QueryRow(ctx, `SELECT centre_code FROM mastercostprofitcenter WHERE centre_code=$1`, entry.Department).Scan(&tmp); err != nil {
				api.RespondWithResult(w, false, "Master department not found: "+entry.Department)
				return
			}
			if err := tx.QueryRow(ctx, `SELECT entity_name FROM masterentitycash WHERE entity_name=$1`, entry.Entity).Scan(&tmp); err != nil {
				api.RespondWithResult(w, false, "Master entity not found: "+entry.Entity)
				return
			}

			// create proposal
			proposalID := fmt.Sprintf("PROP-%06d", time.Now().UnixNano()%1000000)
			propName := strings.TrimSpace(entry.ProposalName)
			if propName == "" {
				propName = fmt.Sprintf("%s - %s", entry.CategoryName, entry.Entity)
			}
			currency := entry.Currency
			if strings.TrimSpace(currency) == "" {
				currency = "USD"
			}
			effDate := entry.EffectiveDate
			if strings.TrimSpace(effDate) == "" {
				effDate = time.Now().Format("2006-01-02")
			}

			insProp := `INSERT INTO cashflow_proposal (proposal_id, proposal_name, entity_name, department_id, currency_code, effective_date, recurrence_type, recurrence_frequency, status) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`
			if _, err := tx.Exec(ctx, insProp, proposalID, propName, entry.Entity, entry.Department, currency, effDate, entry.Frequency, entry.Frequency, "Active"); err != nil {
				api.RespondWithResult(w, false, "Failed to create proposal: "+err.Error())
				return
			}

			// create item
			itemID := fmt.Sprintf("ITEM-%06d", time.Now().UnixNano()%1000000)
			insItem := `INSERT INTO cashflow_proposal_item (item_id, proposal_id, description, cashflow_type, category_id, expected_amount, is_recurring, recurrence_pattern, start_date, end_date) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`
			if _, err := tx.Exec(ctx, insItem, itemID, proposalID, entry.CategoryName, tUpper, entry.CategoryName, entry.ExpectedAmount, entry.Recurring, entry.Frequency, effDate, nil); err != nil {
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

			// audit
			auditQ := `INSERT INTO audit_action_cashflow_proposal (proposal_id, action_type, processing_status, reason, requested_by, requested_at) VALUES ($1,'CREATE','PENDING_APPROVAL', $2, $3, now())`
			if _, err := tx.Exec(ctx, auditQ, proposalID, "Imported", createdBy); err != nil {
				api.RespondWithResult(w, false, "Failed to create audit: "+err.Error())
				return
			}

			created++
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithResult(w, false, "Failed to commit transaction: "+err.Error())
			return
		}
		committed = true

		api.RespondWithResult(w, true, fmt.Sprintf("Successfully imported %d projections", created))
	}
}
