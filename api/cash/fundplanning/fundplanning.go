package fundplanning

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// GetFundPlanning returns rows aggregated from payables, receivables and proposals
func GetFundPlanning(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID              string `json:"user_id"`
			HorizonDays         int    `json:"horizon"`
			EntityID            string `json:"entity,omitempty"`
			Currency            string `json:"curr,omitempty"`
			IncludePayables     bool   `json:"pay"`
			IncludeReceivables  bool   `json:"rec"`
			IncludeProjections  bool   `json:"proj"`
			IncludeCounterparty bool   `json:"counterparty"`
			IncludeType         bool   `json:"type"`
			CostProfitCenter    string `json:"costprofit_center,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, "invalid JSON")
			return
		}

		// Validate required fields
		if req.UserID == "" {
			api.RespondWithResult(w, false, "user_id required")
			return
		}
		if req.HorizonDays == 0 {
			req.HorizonDays = 30
		}
		if req.HorizonDays < 1 {
			api.RespondWithResult(w, false, "invalid horizon")
			return
		}

		// Validate that at least one data source is selected
		if !req.IncludePayables && !req.IncludeReceivables && !req.IncludeProjections {
			api.RespondWithResult(w, false, "at least one data source required (pay/rec/proj)")
			return
		}

		// Validate that only one of IncludeCounterparty or IncludeType is true
		if req.IncludeCounterparty && req.IncludeType {
			api.RespondWithResult(w, false, "only one of counterparty or type can be true")
			return
		}

		// Validate session
		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Email
				break
			}
		}
		if userEmail == "" {
			api.RespondWithResult(w, false, "invalid user_id or session")
			return
		}

		ctx := r.Context()
		now := time.Now()
		endDate := now.Add(time.Duration(req.HorizonDays) * 24 * time.Hour)

		parts := make([]string, 0)
		args := make([]interface{}, 0)
		argI := 1

		// Payables
		if req.IncludePayables {
			// Determine primary field
			primaryField := ""
			if req.IncludeCounterparty {
				primaryField = "m.counterparty_name"
			} else if req.IncludeType {
				primaryField = "'Vendor Payment'"
			} else {
				primaryField = "''"
			}

			q := `SELECT dt, direction, currency, primary_name, amount, costprofit_center FROM (
				SELECT 
					p.due_date as dt, 
					'outflow' as direction, 
					p.currency_code as currency, 
					` + primaryField + ` as primary_name, 
					p.amount as amount,
					'' as costprofit_center
				FROM tr_payables p
				LEFT JOIN mastercounterparty m ON m.counterparty_id = p.counterparty_name
				WHERE EXISTS (
					SELECT 1 FROM auditactionpayable a WHERE a.payable_id::text = p.payable_id::text AND a.processing_status = 'APPROVED'
				)
				AND p.due_date >= $` + fmt.Sprint(argI) + ` 
				AND p.due_date <= $` + fmt.Sprint(argI+1)

			args = append(args, now, endDate)
			argI += 2

			if req.EntityID != "" {
				q += ` AND p.entity_name = $` + fmt.Sprint(argI)
				args = append(args, req.EntityID)
				argI++
			}

			if req.Currency != "" {
				q += ` AND p.currency_code = $` + fmt.Sprint(argI)
				args = append(args, req.Currency)
				argI++
			}

			// payables table does not contain department_id in this schema; skip filtering here

			q += `) t`
			parts = append(parts, q)
		}

		// Receivables
		if req.IncludeReceivables {
			// Determine primary field
			primaryField := ""
			if req.IncludeCounterparty {
				primaryField = "m.counterparty_name"
			} else if req.IncludeType {
				primaryField = "'Collection'"
			} else {
				primaryField = "''"
			}

			q := `SELECT dt, direction, currency, primary_name, amount, costprofit_center FROM (
				SELECT 
					r.due_date as dt, 
					'inflow' as direction, 
					r.currency_code as currency, 
					` + primaryField + ` as primary_name, 
					r.invoice_amount as amount,
					'' as costprofit_center
				FROM tr_receivables r
				LEFT JOIN mastercounterparty m ON m.counterparty_id = r.counterparty_name
				WHERE EXISTS (
					SELECT 1 FROM auditactionreceivable a WHERE a.receivable_id::text = r.receivable_id::text AND a.processing_status = 'APPROVED'
				)
				AND r.due_date >= $` + fmt.Sprint(argI) + ` 
				AND r.due_date <= $` + fmt.Sprint(argI+1)

			args = append(args, now, endDate)
			argI += 2

			if req.EntityID != "" {
				q += ` AND r.entity_name = $` + fmt.Sprint(argI)
				args = append(args, req.EntityID)
				argI++
			}

			if req.Currency != "" {
				q += ` AND r.currency_code = $` + fmt.Sprint(argI)
				args = append(args, req.Currency)
				argI++
			}

			// receivables table does not contain department_id in this schema; skip filtering here

			q += `) t`
			parts = append(parts, q)
		}

		// Projections
		if req.IncludeProjections {
			// Determine primary field
			primaryField := ""
			if req.IncludeCounterparty {
				primaryField = "COALESCE(m.counterparty_name, '')"
			} else if req.IncludeType {
				primaryField = `CASE WHEN cpi.cashflow_type = 'Inflow' THEN 'Collection' ELSE 'Vendor Payment' END`
			} else {
				primaryField = "''"
			}

			q := `SELECT dt, direction, currency, primary_name, amount, costprofit_center FROM (
				SELECT 
					cpi.start_date as dt, 
					CASE WHEN cpi.cashflow_type = 'Inflow' THEN 'inflow' ELSE 'outflow' END as direction, 
					cp.currency_code as currency, 
					` + primaryField + ` as primary_name, 
					cpi.expected_amount as amount,
					COALESCE(NULLIF(cpi.department_id, ''), '') as costprofit_center
				FROM cashflow_proposal cp
				JOIN cashflow_proposal_item cpi ON cpi.proposal_id = cp.proposal_id
				LEFT JOIN mastercounterparty m ON m.counterparty_id = cpi.counterparty_id
				WHERE EXISTS (
					SELECT 1 FROM audit_action_cashflow_proposal a WHERE a.proposal_id::text = cp.proposal_id::text AND a.processing_status = 'APPROVED'
				)
				AND cp.status = 'Active'
				AND cpi.start_date >= $` + fmt.Sprint(argI) + ` 
				AND cpi.start_date <= $` + fmt.Sprint(argI+1)

			args = append(args, now, endDate)
			argI += 2

			if req.EntityID != "" {
				q += ` AND cpi.entity_name = $` + fmt.Sprint(argI)
				args = append(args, req.EntityID)
				argI++
			}

			if req.Currency != "" {
				q += ` AND cp.currency_code = $` + fmt.Sprint(argI)
				args = append(args, req.Currency)
				argI++
			}

			if req.CostProfitCenter != "" {
				q += ` AND cpi.department_id = $` + fmt.Sprint(argI)
				args = append(args, req.CostProfitCenter)
				argI++
			}

			q += `) t`
			parts = append(parts, q)
		}

		finalQ := strings.Join(parts, " UNION ALL ") + " ORDER BY dt, currency"

		// debug: log final query and argument types/values to help debug type mismatch
		// defer func() {
		// 	// no-op defer to keep patch context
		// }()
		// api.LogInfo("fundplanning: final query", map[string]interface{}{"query": finalQ})
		// // log each arg type and value
		// for i, a := range args {
		// 	api.LogInfo(fmt.Sprintf("fundplanning: arg %d", i+1), map[string]interface{}{"type": fmt.Sprintf("%T", a), "value": a})
		// }

		rows, err := pgxPool.Query(ctx, finalQ, args...)
		if err != nil {
			api.RespondWithResult(w, false, "query failed: "+err.Error())
			return
		}
		defer rows.Close()

		type Row struct {
			Date             string  `json:"date"`
			Direction        string  `json:"direction"`
			Currency         string  `json:"curr"`
			Primary          string  `json:"primary"`
			Amount           float64 `json:"amount"`
			CostProfitCenter string  `json:"costprofit_center"`
		}
		res := []Row{}
		for rows.Next() {
			var dt time.Time
			var dir, curr, primary, costProfitCenter string
			var amt float64
			if err := rows.Scan(&dt, &dir, &curr, &primary, &amt, &costProfitCenter); err != nil {
				continue
			}
			res = append(res, Row{
				Date:             dt.Format("2006-01-02"),
				Direction:        dir,
				Currency:         curr,
				Primary:          primary,
				Amount:           amt,
				CostProfitCenter: costProfitCenter,
			})
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{"rows": res})
	}
}
