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
	"log"
	
	"github.com/jackc/pgx/v5/pgxpool"
)

func ifaceToBool(v interface{}) bool {
	switch val := v.(type) {
	case bool:
		return val
	case string:
		b, _ := strconv.ParseBool(val)
		return b
	case nil:
		return false
	default:
		return false
	}
}
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
					CounterpartyName string  `json:"counterparty_name,omitempty"`
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
			insItem := `INSERT INTO cashflow_proposal_item (item_id, proposal_id, description, cashflow_type, category_id, expected_amount, is_recurring, recurrence_pattern, start_date, end_date, entity_name, department_id, counterparty_name, old_counterparty_name, recurrence_frequency) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`
			cp := strings.TrimSpace(entry.CounterpartyName)
			if cp == "" {
				cp = "Generic"
			}
			if _, err := tx.Exec(ctx, insItem, itemID, proposalID, description, tUpper, entry.CategoryName, entry.ExpectedAmount, entry.Recurring, entry.Frequency, effDate, nil, entityParam, deptParam, cp, nil, entry.Frequency); err != nil {
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
				cpi.end_date,
				cpi.counterparty_name,
				cpi.old_counterparty_name
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
			CounterpartyName    string
			OldCounterpartyName string
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
				&pi.CounterpartyName,
				&pi.OldCounterpartyName,
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

		itemQ := `
			SELECT item_id, description, cashflow_type, old_cashflow_type, category_id, old_category_id,
			       CAST(expected_amount AS float8), CAST(old_expected_amount AS float8),
				   is_recurring, old_is_recurring, recurrence_pattern, old_recurrence_pattern,
				   start_date, old_start_date, end_date, old_end_date, entity_name, old_entity_name,
				   department_id, old_department_id, recurrence_frequency, old_recurrence_frequency,
				   counterparty_name, old_counterparty_name
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
		type ItemRow struct {
			ItemID                 string
			Description            string
			CashflowType           string
			OldCashflowType        interface{}
			CategoryID             string
			OldCategoryID          interface{}
			ExpectedAmount         float64
			OldExpectedAmount      float64
			IsRecurring            bool
			OldIsRecurring         bool
			RecurrencePattern      string
			OldRecurrencePattern   interface{}
			StartDate              interface{}
			OldStartDate           interface{}
			EndDate                interface{}
			OldEndDate             interface{}
			EntityName             string
			OldEntityName          interface{}
			DepartmentID           string
			OldDepartmentID        interface{}
			RecurrenceFrequency    string
			OldRecurrenceFrequency interface{}
			CounterpartyName       string
			OldCounterpartyName    interface{}
		}

		items := make([]ItemRow, 0)
		itemIDs := make([]string, 0)

		for rows.Next() {
			var it ItemRow
			var expectedI, oldExpectedI interface{}
			var isRecI, oldIsRecI interface{}
			if err := rows.Scan(
				&it.ItemID, &it.Description, &it.CashflowType, &it.OldCashflowType,
				&it.CategoryID, &it.OldCategoryID, &expectedI, &oldExpectedI,
				&isRecI, &oldIsRecI, &it.RecurrencePattern, &it.OldRecurrencePattern,
				&it.StartDate, &it.OldStartDate, &it.EndDate, &it.OldEndDate,
				&it.EntityName, &it.OldEntityName, &it.DepartmentID, &it.OldDepartmentID,
				&it.RecurrenceFrequency, &it.OldRecurrenceFrequency, &it.CounterpartyName, &it.OldCounterpartyName,
			); err != nil {
				log.Printf("row scan skip: %v", err)
				continue
			}

			it.ExpectedAmount = ifaceToFloat(expectedI)
			it.OldExpectedAmount = ifaceToFloat(oldExpectedI)
			it.IsRecurring = ifaceToBool(isRecI)
			it.OldIsRecurring = ifaceToBool(oldIsRecI)
			items = append(items, it)
			itemIDs = append(itemIDs, it.ItemID)
		}

		if err := rows.Err(); err != nil {
			api.RespondWithResult(w, false, "Error reading items: "+err.Error())
			return
		}

		if len(items) == 0 {
			resp := map[string]interface{}{
				"success":     true,
				"header":      header,
				"projections": []interface{}{},
				"actions":     actions,
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
			return
		}

		projQuery := `
			SELECT item_id, year, month, projected_amount
			FROM cashflow_projection_monthly
			WHERE item_id = ANY($1)
			ORDER BY item_id, year, month
		`
		projRows, err := pgxPool.Query(ctx, projQuery, itemIDs)
		if err != nil {
			projRows = nil
		}

		monthlyMap := make(map[string]map[string]float64)
		monthNames := map[int]string{1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun", 7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"}
		if projRows != nil {
			defer projRows.Close()
			for projRows.Next() {
				var itemID string
				var year, month int
				var amt float64
				if err := projRows.Scan(&itemID, &year, &month, &amt); err != nil {
					continue
				}
				if monthlyMap[itemID] == nil {
					monthlyMap[itemID] = make(map[string]float64)
				}
				key := fmt.Sprintf("%s-%02d", monthNames[month], year%100)
				monthlyMap[itemID][key] = amt
			}
			_ = projRows.Err()
		}

		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		w.Write([]byte(`{"success":true,"header":`))
		if err := enc.Encode(header); err != nil {
			return
		}
		w.Write([]byte(`,"actions":`))
		if err := enc.Encode(actions); err != nil {
			return
		}
		w.Write([]byte(`,"projections":[`))

		first := true
		for _, it := range items {
			entry := map[string]interface{}{
				"item_id":                ifaceToString(it.ItemID),
				"description":            ifaceToString(it.Description),
				"type":                   ifaceToString(it.CashflowType),
				"old_type":               ifaceToString(it.OldCashflowType),
				"categoryName":           ifaceToString(it.CategoryID),
				"old_categoryName":       ifaceToString(it.OldCategoryID),
				"entity":                 ifaceToString(it.EntityName),
				"old_entity":             ifaceToString(it.OldEntityName),
				"department":             ifaceToString(it.DepartmentID),
				"old_department":         ifaceToString(it.OldDepartmentID),
				"expectedAmount":         it.ExpectedAmount,
				"old_expectedAmount":     it.OldExpectedAmount,
				"recurring":              it.IsRecurring,
				"old_recurring":          it.OldIsRecurring,
				"frequency":              ifaceToString(it.RecurrenceFrequency),
				"old_frequency":          ifaceToString(it.OldRecurrenceFrequency),
				"recurrence_pattern":     ifaceToString(it.RecurrencePattern),
				"old_recurrence_pattern": ifaceToString(it.OldRecurrencePattern),
				"start_date":             ifaceToTimeString(it.StartDate),
				"old_start_date":         ifaceToTimeString(it.OldStartDate),
				"end_date":               ifaceToTimeString(it.EndDate),
				"old_end_date":           ifaceToTimeString(it.OldEndDate),
				"counterparty_name":      ifaceToString(it.CounterpartyName),
				"old_counterparty_name":  ifaceToString(it.OldCounterpartyName),
			}

			projMap := map[string]interface{}{
				"type":         it.CashflowType,
				"categoryName": it.CategoryID,
				"item_id":      it.ItemID,
			}
			if mm, ok := monthlyMap[it.ItemID]; ok {
				for k, v := range mm {
					projMap[k] = v
				}
			}

			outObj := map[string]interface{}{
				"entry":      entry,
				"projection": projMap,
			}

			if !first {
				w.Write([]byte(","))
			}
			first = false

			if err := enc.Encode(outObj); err != nil {
				break
			}
		}
		w.Write([]byte(`]}`))
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
			SELECT 
				p.proposal_id,
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
			var (
				proposalID, recurrenceType, proposalName, currencyCode, processingStatus interface{}
				effectiveDate                                                            interface{}
			)

			if err := rows.Scan(
				&proposalID,
				&recurrenceType,
				&proposalName,
				&effectiveDate,
				&currencyCode,
				&processingStatus,
			); err != nil {
				continue
			}

			header := map[string]interface{}{
				"proposal_id":       ifaceToString(proposalID),
				"proposal_type":     ifaceToString(recurrenceType),
				"proposal_name":     ifaceToString(proposalName),
				"effective_date":    ifaceToTimeString(effectiveDate),
				"currency":          ifaceToString(currencyCode),
				"processing_status": ifaceToString(processingStatus),
			}

			out = append(out, header)
		}

		if err := rows.Err(); err != nil {
			api.RespondWithResult(w, false, "Error reading rows: "+err.Error())
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"header":  out,
		})
	}
}

func UpdateCashFlowProposal(pgxPool *pgxpool.Pool) http.HandlerFunc {
	// entry is defined here

	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Header      map[string]interface{}   `json:"header"`
			Projections []map[string]interface{} `json:"projections"`
			UserID      string                   `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, "Invalid JSON: "+err.Error())
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithResult(w, false, "Failed to start transaction: "+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		// --- Update proposal header ---
		proposalID := ifaceToString(req.Header["proposal_id"])
		// Fetch current proposal
		var curName, curCurrency, curType string
		var curEffDate time.Time
		err = tx.QueryRow(ctx, `SELECT proposal_name, currency_code, effective_date, recurrence_type FROM cashflow_proposal WHERE proposal_id=$1`, proposalID).Scan(&curName, &curCurrency, &curEffDate, &curType)
		if err != nil {
			api.RespondWithResult(w, false, "Proposal not found: "+err.Error())
			return
		}
		
		var newEffDate, oldEffDate interface{}
		effDateStr := ifaceToString(req.Header["effective_date"])
		if effDateStr == "" {
			newEffDate = nil
		} else {
			newEffDate = effDateStr
		}
		oldEffDateStr := curEffDate.Format("2006-01-02")
		if oldEffDateStr == "" {
			oldEffDate = nil
		} else {
			oldEffDate = oldEffDateStr
		}
		_, err = tx.Exec(ctx, `UPDATE cashflow_proposal SET old_proposal_name=$1, proposal_name=$2, old_currency_code=$3, currency_code=$4, old_effective_date=$5, effective_date=$6, old_recurrence_type=$7, recurrence_type=$8 WHERE proposal_id=$9`,
			curName,
			ifaceToString(req.Header["proposal_name"]),
			curCurrency,
			ifaceToString(req.Header["currency"]),
			oldEffDate,
			newEffDate,
			curType,
			ifaceToString(req.Header["projection_type"]),
			proposalID,
		)
		if err != nil {
			api.RespondWithResult(w, false, "Failed to update proposal: "+err.Error())
			return
		}

		for _, proj := range req.Projections {
			entry, ok := proj["entry"].(map[string]interface{})
			if !ok {
				continue
			}
			itemID := ifaceToString(entry["item_id"])
			var curCat, curDept, curEnt, curType, curRecPat, curRecFreq, curDesc interface{}
			var curCP interface{}
			var curAmt interface{}
			var curRec interface{}
			var curStart interface{}
			var curEnd interface{}
			err = tx.QueryRow(ctx, `SELECT category_id, department_id, entity_name, expected_amount, cashflow_type, recurrence_pattern, recurrence_frequency, description, is_recurring, start_date, end_date, counterparty_name FROM cashflow_proposal_item WHERE item_id=$1`, itemID).Scan(
				&curCat, &curDept, &curEnt, &curAmt, &curType, &curRecPat, &curRecFreq, &curDesc, &curRec, &curStart, &curEnd, &curCP)
			if err != nil {
				api.RespondWithResult(w, false, "Item not found: "+err.Error())
				return
			}
			startStr := ifaceToString(entry["start_date"])
			var oldEnd, newEnd, oldStart, newStart interface{}

			curStartStr := ifaceToTimeString(curStart)
			curEndStr := ifaceToTimeString(curEnd)

			if curEndStr == "" {
				oldEnd = nil
			} else {
				oldEnd = curEndStr
			}

			if startStr == "" {
				newStart = nil
			} else {
				newStart = startStr
			}

			if curStartStr == "" {
				oldStart = nil
			} else {
				oldStart = curStartStr
			}

			endStr := ifaceToString(entry["end_date"])
			if endStr == "" {
				newEnd = nil
			} else {
				newEnd = endStr
			}
			curAmtVal := ifaceToFloat(curAmt)
			curRecVal := ifaceToBool(curRec)
			curCPVal := ifaceToString(curCP)

			_, err = tx.Exec(ctx, `UPDATE cashflow_proposal_item SET old_category_id=$1, category_id=$2, old_department_id=$3, department_id=$4, old_entity_name=$5, entity_name=$6, old_expected_amount=$7, expected_amount=$8, old_cashflow_type=$9, cashflow_type=$10, old_recurrence_pattern=$11, recurrence_pattern=$12, old_recurrence_frequency=$13, recurrence_frequency=$14, old_is_recurring=$15, is_recurring=$16, old_start_date=$17, start_date=$18, old_end_date=$19, end_date=$20, description=$21, old_counterparty_name=$22, counterparty_name=$23 WHERE item_id=$24`,
				curCat,
				ifaceToString(entry["categoryName"]),
				curDept,
				ifaceToString(entry["department"]),
				curEnt,
				ifaceToString(entry["entity"]),
				curAmtVal,
				ifaceToFloat(entry["expectedAmount"]),
				curType,
				ifaceToString(entry["type"]),
				curRecPat,
				ifaceToString(entry["recurrence_pattern"]),
				curRecFreq,
				ifaceToString(entry["frequency"]),
				curRecVal,
				entry["recurring"],
				oldStart,
				newStart,
				oldEnd,
				newEnd,
				ifaceToString(entry["description"]),
				curCPVal,
				ifaceToString(entry["counterparty_name"]),
				itemID,
			)
			if err != nil {
				api.RespondWithResult(w, false, "Failed to update item: "+err.Error())
				return
			}
			if ifaceToFloat(entry["expectedAmount"]) != ifaceToFloat(curAmt) {
				_, err = tx.Exec(ctx, `DELETE FROM cashflow_projection_monthly WHERE item_id=$1`, itemID)
				if err != nil {
					api.RespondWithResult(w, false, "Failed to delete monthly projections: "+err.Error())
					return
				}
				projMap, ok := proj["projection"].(map[string]interface{})
				if ok {
					for k, v := range projMap {
						if k == "type" || k == "categoryName" || k == "item_id" {
							continue
						}
						parts := strings.Split(k, "-")
						if len(parts) != 2 {
							continue
						}
						monthStr, yearStr := parts[0], parts[1]
						monthNum := monthNameToInt(monthStr)
						yearNum, _ := strconv.Atoi("20" + yearStr)
						amt := ifaceToFloat(v)
						projectionID := fmt.Sprintf("PROJ-%d", time.Now().UnixNano())
						_, err = tx.Exec(ctx, `INSERT INTO cashflow_projection_monthly (projection_id, item_id, year, month, projected_amount) VALUES ($1,$2,$3,$4,$5)`,
							projectionID, itemID, yearNum, monthNum, amt)
						if err != nil {
							api.RespondWithResult(w, false, "Failed to insert monthly projection: "+err.Error())
							return
						}
					}
				}
			}
		}
		_, err = tx.Exec(ctx, `INSERT INTO audit_action_cashflow_proposal (proposal_id, action_type, processing_status, requested_by, requested_at) VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL',$2,now())`, proposalID, req.UserID)
		if err != nil {
			api.RespondWithResult(w, false, "Failed to insert audit action: "+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithResult(w, false, "Failed to commit transaction: "+err.Error())
			return
		}

		api.RespondWithResult(w, true, "Proposal updated and submitted for approval")
	}
}

// Helper: convert month name to int
func monthNameToInt(m string) int {
	switch strings.ToLower(m) {
	case "jan":
		return 1
	case "feb":
		return 2
	case "mar":
		return 3
	case "apr":
		return 4
	case "may":
		return 5
	case "jun":
		return 6
	case "jul":
		return 7
	case "aug":
		return 8
	case "sep":
		return 9
	case "oct":
		return 10
	case "nov":
		return 11
	case "dec":
		return 12
	}
	return 0
}

// Helper: convert interface{} to float64
func ifaceToFloat(v interface{}) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case int:
		return float64(val)
	case int64:
		return float64(val)
	case string:
		f, _ := strconv.ParseFloat(val, 64)
		return f
	default:
		return 0
	}
}
