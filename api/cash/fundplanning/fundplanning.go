
package fundplanning

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
	"database/sql"

	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	schemaCache struct {
		hasCpiCounterparty bool
		hasCpCounterparty  bool
		hasCpiDept         bool
		expires            time.Time
	}
)


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

		if !req.IncludePayables && !req.IncludeReceivables && !req.IncludeProjections {
			api.RespondWithResult(w, false, "at least one data source required (pay/rec/proj)")
			return
		}

		if req.IncludeCounterparty && req.IncludeType {
			api.RespondWithResult(w, false, "only one of counterparty or type can be true")
			return
		}

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

		if req.IncludePayables {
			primaryField := ""
			if req.IncludeCounterparty {
				primaryField = "COALESCE(m.counterparty_name, 'Generic')"
			} else if req.IncludeType {
				primaryField = "'Vendor Payment'"
			} else {
				primaryField = "''"
			}

			q := `SELECT dt, direction, currency, primary_name, amount, costprofit_center, source_ref FROM (
				SELECT 
					p.due_date as dt, 
					'outflow' as direction, 
					p.currency_code as currency, 
					` + primaryField + ` as primary_name, 
					p.amount as amount,
					'Generic' as costprofit_center,
					p.payable_id::text as source_ref
				FROM tr_payables p
				LEFT JOIN mastercounterparty m ON m.counterparty_name = p.counterparty_name
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

			q += `) t`
			parts = append(parts, q)
		}

		if req.IncludeReceivables {
			primaryField := ""
			if req.IncludeCounterparty {
				primaryField = "COALESCE(m.counterparty_name, 'Generic')"
			} else if req.IncludeType {
				primaryField = "'Collection'"
			} else {
				primaryField = "''"
			}

			q := `SELECT dt, direction, currency, primary_name, amount, costprofit_center, source_ref  FROM (
				SELECT 
					r.due_date as dt, 
					'inflow' as direction, 
					r.currency_code as currency, 
					` + primaryField + ` as primary_name, 
					r.invoice_amount as amount,
					'Generic' as costprofit_center,
					r.receivable_id::text as source_ref
				FROM tr_receivables r
				LEFT JOIN mastercounterparty m ON m.counterparty_name = r.counterparty_name
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

			q += `) t`
			parts = append(parts, q)
		}
		if req.IncludeProjections {
			var hasCpiCounterparty, hasCpCounterparty, hasCpiDept bool
			if time.Now().Before(schemaCache.expires) {
				hasCpiCounterparty = schemaCache.hasCpiCounterparty
				hasCpCounterparty = schemaCache.hasCpCounterparty
				hasCpiDept = schemaCache.hasCpiDept
			} else {
				row := pgxPool.QueryRow(ctx, `SELECT
					EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name='cashflow_proposal_item' AND column_name='counterparty_name') as has_cpi_counterparty,
					EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name='cashflow_proposal' AND column_name='counterparty_name') as has_cp_counterparty,
					EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name='cashflow_proposal_item' AND column_name='department_id') as has_cpi_dept
				`)
				_ = row.Scan(&hasCpiCounterparty, &hasCpCounterparty, &hasCpiDept)
				schemaCache.hasCpiCounterparty = hasCpiCounterparty
				schemaCache.hasCpCounterparty = hasCpCounterparty
				schemaCache.hasCpiDept = hasCpiDept
				schemaCache.expires = time.Now().Add(5 * time.Minute)
			}
			primaryField := "'Generic'"
			joinCounterparty := false
			if req.IncludeCounterparty {
				if hasCpiCounterparty || hasCpCounterparty {
					primaryField = "COALESCE(NULLIF(m.counterparty_name, ''), 'Generic')"
					joinCounterparty = true
				} else {
					primaryField = "'Generic'"
				}
			} else if req.IncludeType {
				primaryField = "COALESCE(CASE WHEN cpi.cashflow_type = 'Inflow' THEN 'Collection' WHEN cpi.cashflow_type = 'Outflow' THEN 'Vendor Payment' ELSE NULL END, 'Generic')"
			} else {
				primaryField = "'Generic'"
			}
			q := `SELECT dt, direction, currency, primary_name, amount, costprofit_center, source_ref FROM (
				SELECT 
					cpi.start_date as dt, 
					CASE WHEN cpi.cashflow_type = 'Inflow' THEN 'inflow' ELSE 'outflow' END as direction, 
					cp.currency_code as currency, 
					` + primaryField + ` as primary_name, 
					cpi.expected_amount as amount,
					COALESCE(NULLIF(cpi.department_id, ''), 'Generic') as costprofit_center,
					cp.proposal_id::text as source_ref
				FROM cashflow_proposal cp
				JOIN cashflow_proposal_item cpi ON cpi.proposal_id = cp.proposal_id

`
			if joinCounterparty {
					if hasCpiCounterparty && hasCpCounterparty {
					q += "\t\tLEFT JOIN mastercounterparty m ON m.counterparty_name = COALESCE(NULLIF(cpi.counterparty_name, ''), NULLIF(cp.counterparty_name, ''))\n"
				} else if hasCpiCounterparty {
					q += "\t\tLEFT JOIN mastercounterparty m ON m.counterparty_name = cpi.counterparty_name\n"
				} else {
					q += "\t\tLEFT JOIN mastercounterparty m ON m.counterparty_name = cp.counterparty_name\n"
				}
			}

			q += `			WHERE EXISTS (
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

			if strings.TrimSpace(req.CostProfitCenter) != "" && hasCpiDept {
				q += ` AND cpi.department_id = $` + fmt.Sprint(argI)
				args = append(args, req.CostProfitCenter)
				argI++
			}

			q += `) t`
			parts = append(parts, q)
		}

		finalQ := strings.Join(parts, " UNION ALL ") + " ORDER BY dt, currency"

		// // debug: log final query and argument types/values to help debug type mismatch
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
			SourceRef        string  `json:"source_ref"`
		}
		res := []Row{}
		for rows.Next() {
			var dt time.Time
			var dir, curr, primary, costProfitCenter, sourceRef  string
			var amt float64
			if err := rows.Scan(&dt, &dir, &curr, &primary, &amt, &costProfitCenter, &sourceRef); err != nil {
				continue
			}
			if strings.TrimSpace(costProfitCenter) == "" && strings.TrimSpace(req.CostProfitCenter) != "" {
				costProfitCenter = req.CostProfitCenter
			}
			res = append(res, Row{
				Date:             dt.Format("2006-01-02"),
				Direction:        dir,
				Currency:         curr,
				Primary:          primary,
				Amount:           amt,
				CostProfitCenter: costProfitCenter,
				SourceRef:        sourceRef,
			})
		}

		api.RespondWithPayload(w, true, "", res)
	}
}


func GetApprovedBankAccountsForFundPlanning(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if r.Method == http.MethodPost {
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				api.RespondWithResult(w, false, "invalid JSON")
				return
			}
		}

		ctx := r.Context()

		query := `
			SELECT a.usage,
				   a.account_number,
				   NULLIF(a.account_nickname, '') AS account_name,
				   b.bank_name,
				   a.conn_cutoff,
				   a.conn_tz,
				    a.currency
			FROM masterbankaccount a
			LEFT JOIN masterbank b ON a.bank_id = b.bank_id
			LEFT JOIN LATERAL (
				SELECT processing_status
				FROM auditactionbankaccount aa
				WHERE aa.account_id = a.account_id
				ORDER BY requested_at DESC
				LIMIT 1
			) astatus ON TRUE
			WHERE COALESCE(a.is_deleted, false) = false
			  AND a.status = 'Active'
			  AND astatus.processing_status = 'APPROVED'
		`

		rows, err := pgxPool.Query(ctx, query)
		if err != nil {
			api.RespondWithResult(w, false, "query failed: "+err.Error())
			return
		}
		defer rows.Close()

		results := make([]map[string]interface{}, 0)

		for rows.Next() {
			var accountNo string
			var usage, accountName, bankName, connCutoff, connTz, currency sql.NullString

			if err := rows.Scan(&usage, &accountNo, &accountName, &bankName, &connCutoff, &connTz, &currency); err != nil {
				api.LogError("scan failed", map[string]interface{}{"error": err.Error()})
				continue
			}

			results = append(results, map[string]interface{}{
				"usage":        nullableToString(usage),
				"account_no":   accountNo,
				"account_name": nullableToString(accountName),
				"bank_name":    nullableToString(bankName),
				"conn_cutoff":  nullableToString(connCutoff),
				"conn_tz":      nullableToString(connTz),
				"currency":     nullableToString(currency),
			})
		}

		api.RespondWithPayload(w, true, "", results)
	}
}

func nullableToString(ns sql.NullString) interface{} {
	if ns.Valid {
		return ns.String
	}
	return ""
}
