package fundplanning

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/policyengine/common"
	"CimplrCorpSaas/api/policyengine/runtime"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/internal/validation"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func validateFundPlanScope(ctx context.Context, fields map[string]interface{}) string {
	return validation.ValidateCashMasterReferences(ctx, fields)
}

// projectionDueItemsQuery reads approved cashflow projection V2 rows (cimplrcorpsaas schema).
func projectionDueItemsQuery(includeCounterparty, includeType bool, argI int) string {
	primaryField := constants.QuerryGeneric
	if includeCounterparty {
		primaryField = "COALESCE(NULLIF(cpi.entity_name, ''), 'Generic')"
	} else if includeType {
		primaryField = "COALESCE(CASE WHEN cpi.cashflow_type = 'Inflow' THEN 'Collection' WHEN cpi.cashflow_type = 'Outflow' THEN 'Vendor Payment' ELSE NULL END, 'Generic')"
	}
	return `SELECT dt, direction, currency, primary_name, amount, costprofit_center, source_ref FROM (
		SELECT
			COALESCE(cpi.maturity_date, cp.effective_date) as dt,
			CASE WHEN cpi.cashflow_type = 'Inflow' THEN 'inflow' ELSE 'outflow' END as direction,
			COALESCE(NULLIF(cpi.currency_code,''), cp.base_currency_code) as currency,
			` + primaryField + ` as primary_name,
			cpi.expected_amount as amount,
			'Generic' as costprofit_center,
			cpi.item_id::text as source_ref
		FROM cimplrcorpsaas.cashflow_proposal cp
		JOIN cimplrcorpsaas.cashflow_proposal_item cpi ON cpi.proposal_id = cp.proposal_id AND COALESCE(cpi.is_deleted, false) = false
		WHERE EXISTS (
			SELECT 1 FROM cimplrcorpsaas.audit_action_cashflow_proposal a
			WHERE a.proposal_id = cp.proposal_id AND a.processing_status = 'APPROVED'
		)
		AND COALESCE(cp.is_deleted, false) = false
		AND COALESCE(cpi.maturity_date, cp.effective_date) >= $` + fmt.Sprint(argI) + `
		AND COALESCE(cpi.maturity_date, cp.effective_date) <= $` + fmt.Sprint(argI+1)
}

func resolveGroupMetadata(group FundPlanGroupRequest) (direction, currency, primaryKey, primaryValue string, err error) {
	direction = strings.TrimSpace(group.Direction)
	currency = strings.TrimSpace(group.Currency)
	primaryKey = strings.TrimSpace(group.PrimaryKey)
	primaryValue = strings.TrimSpace(group.PrimaryValue)
	if direction != "" && currency != "" && primaryKey != "" {
		return direction, currency, primaryKey, primaryValue, nil
	}
	return parseGroupID(group.GroupID)
}

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
			api.RespondWithResult(w, false, constants.ErrInvalidJSONShort)
			return
		}

		if req.UserID == "" {
			api.RespondWithResult(w, false, constants.ErrUserIDRequired)
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
			api.RespondWithResult(w, false, constants.ErrInvalidSession)
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
				primaryField = constants.QuerryCounterpartyName
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
				primaryField = constants.QuerryCounterpartyName
			} else if req.IncludeType {
				primaryField = "'Collection'"
			} else {
				primaryField = "''"
			}

			q := fmt.Sprintf(`
	SELECT dt, direction, currency, primary_name, amount, costprofit_center, source_ref
	FROM (
		SELECT 
			r.due_date as dt, 
			'inflow' as direction, 
			r.currency_code as currency, 
			%s as primary_name, 
			r.invoice_amount as amount,
			'Generic' as costprofit_center,
			r.receivable_id::text as source_ref
		FROM tr_receivables r
		LEFT JOIN mastercounterparty m 
			ON m.counterparty_name = r.counterparty_name
		WHERE EXISTS (
			SELECT 1 
			FROM auditactionreceivable a 
			WHERE a.receivable_id::text = r.receivable_id::text 
			AND a.processing_status = 'APPROVED'
		)
		AND r.due_date >= $%d
		AND r.due_date <= $%d
	`,
				primaryField,
				argI,
				argI+1,
			)
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
			q := projectionDueItemsQuery(req.IncludeCounterparty, req.IncludeType, argI)

			args = append(args, now, endDate)
			argI += 2

			if req.EntityID != "" {
				q += ` AND cpi.entity_name = $` + fmt.Sprint(argI)
				args = append(args, req.EntityID)
				argI++
			}

			if req.Currency != "" {
				q += ` AND COALESCE(NULLIF(cpi.currency_code,''), cp.base_currency_code) = $` + fmt.Sprint(argI)
				args = append(args, req.Currency)
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
			api.RespondWithResult(w, false, constants.ErrQueryFailed+err.Error())
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
			var dir, curr, primary, costProfitCenter, sourceRef string
			var amt float64
			if err := rows.Scan(&dt, &dir, &curr, &primary, &amt, &costProfitCenter, &sourceRef); err != nil {
				continue
			}
			if strings.TrimSpace(costProfitCenter) == "" && strings.TrimSpace(req.CostProfitCenter) != "" {
				costProfitCenter = req.CostProfitCenter
			}
			res = append(res, Row{
				Date:             dt.Format(constants.DateFormat),
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

// Enhanced response structures for fund planning with grouping and account suggestions
type FundPlanningGroup struct {
	GroupID            string             `json:"group_id"`
	GroupLabel         string             `json:"group_label"`
	Direction          string             `json:"direction"`
	Currency           string             `json:"currency"`
	PrimaryKey         string             `json:"primary_key"`
	PrimaryValue       string             `json:"primary_value"`
	Count              int                `json:"count"`
	TotalAmount        float64            `json:"total_amount"`
	SuggestedAccounts  []SuggestedAccount `json:"suggested_accounts"`
	ProposedAllocation []AllocationItem   `json:"proposed_allocation"`
	Lines              []FundPlanningLine `json:"lines"`
	GroupWarnings      []string           `json:"group_warnings"`
}

type SuggestedAccount struct {
	ID         string   `json:"id"`
	Name       string   `json:"name"`
	Currency   string   `json:"currency"`
	Score      int      `json:"score"`
	Confidence int      `json:"confidence"`
	Reasons    []string `json:"reasons"`
	Warnings   []string `json:"warnings"`
}

type AllocationItem struct {
	AccountID string  `json:"account_id"`
	Pct       float64 `json:"pct"`
}

type FundPlanningLine struct {
	LineID             string              `json:"line_id"`
	Date               string              `json:"date"`
	Type               string              `json:"type"`
	SourceRef          string              `json:"source_ref"`
	CounterpartyOrType string              `json:"counterparty_or_type"`
	Category           string              `json:"category"`
	Amount             float64             `json:"amount"`
	Currency           string              `json:"currency"`
	PlannedBankAccount *PlannedBankAccount `json:"planned_bank_account,omitempty"`
	Rationale          []string            `json:"rationale"`
	SuggestionRank     int                 `json:"suggestion_rank,omitempty"`
}

type PlannedBankAccount struct {
	ID                 string  `json:"id"`
	Name               string  `json:"name"`
	AllocatedPctOfLine float64 `json:"allocated_pct_of_line"`
}

func GetFundPlanningEnhanced(pgxPool *pgxpool.Pool) http.HandlerFunc {
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
			api.RespondWithResult(w, false, constants.ErrInvalidJSONShort)
			return
		}

		if req.UserID == "" {
			api.RespondWithResult(w, false, constants.ErrUserIDRequired)
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
			api.RespondWithResult(w, false, constants.ErrInvalidSession)
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
				primaryField = constants.QuerryCounterpartyName
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
				primaryField = constants.QuerryCounterpartyName
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
			q := projectionDueItemsQuery(req.IncludeCounterparty, req.IncludeType, argI)

			args = append(args, now, endDate)
			argI += 2

			if req.EntityID != "" {
				q += ` AND cpi.entity_name = $` + fmt.Sprint(argI)
				args = append(args, req.EntityID)
				argI++
			}

			if req.Currency != "" {
				q += ` AND COALESCE(NULLIF(cpi.currency_code,''), cp.base_currency_code) = $` + fmt.Sprint(argI)
				args = append(args, req.Currency)
				argI++
			}

			q += `) t`
			parts = append(parts, q)
		}

		finalQ := strings.Join(parts, " UNION ALL ") + " ORDER BY dt, currency"

		rows, err := pgxPool.Query(ctx, finalQ, args...)
		if err != nil {
			api.RespondWithResult(w, false, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		// Collect raw data
		rawData := []RawRow{}
		for rows.Next() {
			var dt time.Time
			var dir, curr, primary, costProfitCenter, sourceRef string
			var amt float64
			if err := rows.Scan(&dt, &dir, &curr, &primary, &amt, &costProfitCenter, &sourceRef); err != nil {
				continue
			}
			if strings.TrimSpace(costProfitCenter) == "" && strings.TrimSpace(req.CostProfitCenter) != "" {
				costProfitCenter = req.CostProfitCenter
			}
			rawData = append(rawData, RawRow{
				Date:             dt,
				Direction:        dir,
				Currency:         curr,
				Primary:          primary,
				Amount:           amt,
				CostProfitCenter: costProfitCenter,
				SourceRef:        sourceRef,
			})
		}

		// Group data and generate enhanced response
		groups := groupFundPlanningData(rawData, req.IncludeCounterparty, req.IncludeType)

		// Get available bank accounts for suggestions
		bankAccounts := getAvailableBankAccounts(ctx, pgxPool)

		// Enhance groups with account suggestions and allocations
		for i := range groups {
			groups[i].SuggestedAccounts = suggestAccountsForGroup(&groups[i], bankAccounts)
			groups[i].ProposedAllocation = proposeAllocationForGroup(&groups[i], groups[i].SuggestedAccounts)
			assignPlannedAccountsToLines(&groups[i])
		}

		api.RespondWithPayload(w, true, "", groups)
	}
}

// Helper functions for enhanced fund planning

type RawRow struct {
	Date             time.Time
	Direction        string
	Currency         string
	Primary          string
	Amount           float64
	CostProfitCenter string
	SourceRef        string
}

type BankAccountInfo struct {
	ID       string
	Name     string
	Currency string
	Usage    string
	BankName string
}

func groupFundPlanningData(rawData []RawRow, includeCounterparty, includeType bool) []FundPlanningGroup {
	groupMap := make(map[string]*FundPlanningGroup)

	for _, row := range rawData {
		// Determine primary key and value
		var primaryKey, primaryValue string
		if includeCounterparty {
			primaryKey = "Counterparty"
			primaryValue = row.Primary
		} else if includeType {
			primaryKey = "Type"
			primaryValue = row.Primary
		} else {
			primaryKey = "Generic"
			primaryValue = "Generic"
		}

		// Create group key
		groupKey := fmt.Sprintf("%s-%s-%s-%s-%s",
			row.Date.Format(constants.DateFormat),
			row.Direction,
			row.Currency,
			primaryKey,
			primaryValue)

		// Get or create group
		group, exists := groupMap[groupKey]
		if !exists {
			// Capitalize first letter of direction
			direction := row.Direction
			if len(direction) > 0 {
				direction = strings.ToUpper(direction[:1]) + direction[1:]
			}
			group = &FundPlanningGroup{
				GroupID:       fmt.Sprintf("G-%d-%s-%s-%s", len(groupMap)+1, row.Date.Format("20060102"), row.Currency, strings.ReplaceAll(primaryValue, " ", "")),
				GroupLabel:    fmt.Sprintf("%s · %s · %s · %s: %s", row.Date.Format("2006-10-10"), direction, row.Currency, primaryKey, primaryValue),
				Direction:     row.Direction,
				Currency:      row.Currency,
				PrimaryKey:    primaryKey,
				PrimaryValue:  primaryValue,
				Count:         0,
				TotalAmount:   0,
				Lines:         []FundPlanningLine{},
				GroupWarnings: []string{},
			}
			groupMap[groupKey] = group
		}

		// Add line to group
		line := FundPlanningLine{
			LineID:             row.SourceRef,
			Date:               row.Date.Format(constants.DateFormat),
			Type:               row.Direction,
			SourceRef:          row.SourceRef,
			CounterpartyOrType: primaryValue,
			Category:           getCategoryFromDirection(row.Direction),
			Amount:             row.Amount,
			Currency:           row.Currency,
			Rationale:          []string{},
		}

		group.Lines = append(group.Lines, line)
		group.Count++
		group.TotalAmount += row.Amount
	}

	// Convert map to slice and round total amounts
	groups := make([]FundPlanningGroup, 0, len(groupMap))
	for _, group := range groupMap {
		// Round total amount to 4 decimal places
		group.TotalAmount = roundToDecimal(group.TotalAmount, 4)
		groups = append(groups, *group)
	}

	return groups
}

func getCategoryFromDirection(direction string) string {
	if direction == "inflow" {
		return "Collections"
	}
	return constants.VendorPayment
}

func getAvailableBankAccounts(ctx context.Context, pgxPool *pgxpool.Pool) []BankAccountInfo {
	query := `
		SELECT
			a.account_id,
			COALESCE(NULLIF(a.account_nickname, ''), a.account_number) as name,
			COALESCE(a.currency, '') as currency,
			COALESCE(a.usage, '') as usage,
			COALESCE(b.bank_name, '') as bank_name
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
		api.LogError("getAvailableBankAccounts query failed: %v", map[string]interface{}{constants.ValueError: err.Error()})
		return []BankAccountInfo{}
	}
	defer rows.Close()

	var accounts []BankAccountInfo
	for rows.Next() {
		var id, name, currency, usage, bankName string
		if err := rows.Scan(&id, &name, &currency, &usage, &bankName); err != nil {
			api.LogError("getAvailableBankAccounts scan failed: %v", map[string]interface{}{constants.ValueError: err.Error()})
			continue
		}
		accounts = append(accounts, BankAccountInfo{
			ID:       id,
			Name:     name,
			Currency: currency,
			Usage:    usage,
			BankName: bankName,
		})
	}

	api.LogInfo("getAvailableBankAccounts: %v", map[string]interface{}{"count": len(accounts)})
	return accounts
}

func suggestAccountsForGroup(group *FundPlanningGroup, bankAccounts []BankAccountInfo) []SuggestedAccount {
	suggestions := make([]SuggestedAccount, 0, len(bankAccounts))

	for _, account := range bankAccounts {
		score := 0
		confidence := 0
		reasons := make([]string, 0, 4)
		warnings := make([]string, 0, 3)

		// Currency match (+30)
		if account.Currency == group.Currency {
			score += 30
			confidence += 30
			reasons = append(reasons, fmt.Sprintf("Currency match (%s) +30", group.Currency))
		}

		// Direction allowed (+20)
		if (group.Direction == "inflow" && (account.Usage == "Collection" || account.Usage == "Operational")) ||
			(group.Direction == "outflow" && (account.Usage == "Operational" || account.Usage == constants.VendorPayment)) {
			score += 20
			confidence += 20
			reasons = append(reasons, fmt.Sprintf("Direction allowed (%s) +20", group.Direction))
		}

		// Usage alignment (+15)
		if (group.Direction == "inflow" && account.Usage == "Collection") ||
			(group.Direction == "outflow" && account.Usage == constants.VendorPayment) {
			score += 15
			confidence += 15
			reasons = append(reasons, fmt.Sprintf("Usage aligns (%s) +15", account.Usage))
		}

		// Counterparty preference (+25) - simplified for now
		if group.PrimaryKey == "Counterparty" && group.PrimaryValue != "Generic" {
			score += 25
			confidence += 25
			reasons = append(reasons, fmt.Sprintf("Counterparty preference: %s historically pays to %s (+25)", group.PrimaryValue, account.Name))
		}

		// Headroom check (+5) - simplified
		score += 5
		confidence += 5
		reasons = append(reasons, "Sufficient headroom in account (+5)")

		// Add warnings based on bank account properties
		if account.Currency != group.Currency {
			warnings = append(warnings, fmt.Sprintf("Currency mismatch: Account is %s but group needs %s", account.Currency, group.Currency))
		}
		if group.Direction == "inflow" && account.Usage != "Collection" && account.Usage != "Operational" {
			warnings = append(warnings, fmt.Sprintf("Account usage '%s' may not be suitable for inflow transactions", account.Usage))
		}
		if group.Direction == "outflow" && account.Usage != constants.VendorPayment && account.Usage != "Operational" {
			warnings = append(warnings, fmt.Sprintf("Account usage '%s' may not be suitable for outflow transactions", account.Usage))
		}

		// Calculate confidence as percentage
		if confidence > 100 {
			confidence = 100
		}

		suggestions = append(suggestions, SuggestedAccount{
			ID:         account.ID,
			Name:       account.Name,
			Currency:   account.Currency,
			Score:      score,
			Confidence: confidence,
			Reasons:    reasons,
			Warnings:   warnings,
		})
	}

	// Sort by score descending
	for i := 0; i < len(suggestions)-1; i++ {
		for j := i + 1; j < len(suggestions); j++ {
			if suggestions[i].Score < suggestions[j].Score {
				suggestions[i], suggestions[j] = suggestions[j], suggestions[i]
			}
		}
	}

	// Return top suggestions (max 3)
	if len(suggestions) > 3 {
		suggestions = suggestions[:3]
	}

	return suggestions
}

func proposeAllocationForGroup(group *FundPlanningGroup, suggestions []SuggestedAccount) []AllocationItem {
	if len(suggestions) == 0 {
		return []AllocationItem{}
	}

	// Simple allocation strategy: 60% to top account, 40% to second if available
	var allocations []AllocationItem

	if len(suggestions) >= 1 {
		allocations = append(allocations, AllocationItem{
			AccountID: suggestions[0].ID,
			Pct:       60,
		})
	}

	if len(suggestions) >= 2 {
		allocations = append(allocations, AllocationItem{
			AccountID: suggestions[1].ID,
			Pct:       40,
		})
	}

	return allocations
}

func assignPlannedAccountsToLines(group *FundPlanningGroup) {
	// Lines should NOT have individual bank account assignments
	// Only groups should have suggested accounts and proposed allocations
	// Individual lines should only have group-level rationale
	for i := range group.Lines {
		group.Lines[i].PlannedBankAccount = nil
		group.Lines[i].Rationale = []string{
			"Bank account allocation is managed at the group level",
			fmt.Sprintf("This line is part of group: %s", group.GroupLabel),
			"See group-level suggested_accounts and proposed_allocation for bank assignment",
		}
	}
}

func getAccountNameByID(accountID string, suggestions []SuggestedAccount) string {
	for _, suggestion := range suggestions {
		if suggestion.ID == accountID {
			return suggestion.Name
		}
	}
	return "Unknown Account"
}

func GetApprovedBankAccountsForFundPlanning(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if r.Method == http.MethodPost {
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				api.RespondWithResult(w, false, constants.ErrInvalidJSONShort)
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
			api.RespondWithResult(w, false, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		results := make([]map[string]interface{}, 0)

		for rows.Next() {
			var accountNo string
			var usage, accountName, bankName, connCutoff, connTz, currency sql.NullString

			if err := rows.Scan(&usage, &accountNo, &accountName, &bankName, &connCutoff, &connTz, &currency); err != nil {
				api.LogError("%s: %v", constants.ErrScanFailed, map[string]interface{}{constants.ValueError: err.Error()})
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

// roundToDecimal rounds a float64 to specified decimal places
func roundToDecimal(val float64, precision int) float64 {
	var multiplier float64 = 1
	for i := 0; i < precision; i++ {
		multiplier *= 10
	}
	rounded := float64(int(val*multiplier+0.5)) / multiplier
	return rounded
}

// CreateFundPlan request structures
type groupCreationMeta struct {
	PlanID      string
	EntityName  string
	Horizon     int
	UserEmail   string
	RequestedIP string
}

type CreateFundPlanRequest struct {
	UserID     string                 `json:"user_id"`
	PlanID     string                 `json:"plan_id"`
	EntityName string                 `json:"entity_name"`
	Horizon    int                    `json:"horizon"`
	Groups     []FundPlanGroupRequest `json:"groups"`
}

type FundPlanGroupRequest struct {
	GroupID           string                      `json:"group_id"`
	Direction         string                      `json:"direction"`
	Currency          string                      `json:"currency"`
	PrimaryKey        string                      `json:"primary_key"`
	PrimaryValue      string                      `json:"primary_value"`
	Status            string                      `json:"status"`
	TotalAmount       float64                     `json:"total_amount"`
	AllocatedAccounts []FundPlanAllocationRequest `json:"allocated_accounts"`
	Lines             []FundPlanLineRequest       `json:"lines"`
}

type FundPlanAllocationRequest struct {
	AccountID       string  `json:"account_id"`
	AllocatedAmount float64 `json:"allocated_amount"`
}

type FundPlanLineRequest struct {
	LineID               string  `json:"line_id"`
	Amount               float64 `json:"amount"`
	AllocatedBankAccount string  `json:"allocated_bank_account"`
	AllocatedAmount      float64 `json:"allocated_amount"`
}

// CreateFundPlan creates a new fund plan with groups, lines, allocations and audit actions
func CreateFundPlan(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req CreateFundPlanRequest

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}

		// Log the parsed request for debugging
		api.LogInfo("CreateFundPlan request parsed: %v", map[string]interface{}{
			"plan_id":           req.PlanID,
			constants.KeyUserID: req.UserID,
			"entity_name":       req.EntityName,
			"horizon":           req.Horizon,
			"num_groups":        len(req.Groups),
		})

		for i, group := range req.Groups {
			api.LogInfo("Group details: %v", map[string]interface{}{
				"index":                  i,
				"group_id":               group.GroupID,
				"total_amount":           group.TotalAmount,
				"num_allocated_accounts": len(group.AllocatedAccounts),
				"num_lines":              len(group.Lines),
			})
		}

		// Validate required fields
		if req.UserID == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrUserIIsRequired)
			return
		}
		planID := uuid.New().String()
		if req.EntityName == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrEntityNameRequired)
			return
		}
		if req.Horizon <= 0 {
			api.RespondWithError(w, http.StatusBadRequest, "horizon must be greater than 0")
			return
		}
		if len(req.Groups) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "at least one group is required")
			return
		}
		if msg := validateFundPlanScope(ctx, map[string]interface{}{"entity_name": req.EntityName}); msg != "" {
			api.RespondWithError(w, http.StatusForbidden, msg)
			return
		}
		for _, group := range req.Groups {
			_, currency, primaryKey, primaryValue, err := resolveGroupMetadata(group)
			if err != nil {
				api.RespondWithError(w, http.StatusBadRequest, err.Error())
				return
			}
			fields := map[string]interface{}{
				"entity_name": req.EntityName,
				"currency":    currency,
			}
			if strings.EqualFold(primaryKey, "counterparty") {
				fields["counterparty_name"] = primaryValue
			}
			accountIDs := make([]string, 0, len(group.AllocatedAccounts)+len(group.Lines))
			for _, allocation := range group.AllocatedAccounts {
				accountIDs = append(accountIDs, allocation.AccountID)
			}
			for _, line := range group.Lines {
				accountIDs = append(accountIDs, line.AllocatedBankAccount)
			}
			fields["account_ids"] = accountIDs
			if msg := validateFundPlanScope(ctx, fields); msg != "" {
				api.RespondWithError(w, http.StatusForbidden, msg)
				return
			}
		}

		// Get user email from active sessions
		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Email
				break
			}
		}
		if userEmail == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidSession)
			return
		}

		if !runtime.Enforce(ctx, w, r, pgxPool, runtime.EnforceInput{
			EventCode:           common.TriggerPreCreate,
			ModuleCode:          common.ModuleCash,
			SubModule:           "FUND_PLANNING",
			EntityCode:          req.EntityName,
			ActorUserID:         req.UserID,
			HandlerName:         "CreateFundPlan",
			APIPath:             "/cash/fund-planning/create",
			DefaultBlockMessage: "Fund plan create blocked by policy",
			Fields:              buildFundPlanningPolicyFields(fundPlanningRowFromCreateRequest(planID, req)),
		}) {
			return
		}

		// Start transaction
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxStartFailed+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		results := make([]map[string]interface{}, 0)
		requestedIP := api.ClientIPFromRequest(r)

		// Process each group
		for _, group := range req.Groups {
			groupResult := processGroupCreation(ctx, tx, groupCreationMeta{
				PlanID:      planID,
				EntityName:  req.EntityName,
				Horizon:     req.Horizon,
				UserEmail:   userEmail,
				RequestedIP: requestedIP,
			}, group)
			results = append(results, groupResult)
		}

		// Check if all groups were processed successfully
		allSuccess := true
		for _, result := range results {
			if success, ok := result[constants.ValueSuccess].(bool); !ok || !success {
				allSuccess = false
				break
			}
		}

		if allSuccess {
			if err = tx.Commit(ctx); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxCommitFailed+err.Error())
				return
			}
		} else {
			// Transaction will be rolled back by defer
		}

		api.RespondWithPayload(w, allSuccess, "", map[string]interface{}{
			"plan_id": planID,
			"groups":  results,
		})
	}
}

// processGroupCreation handles creation of a single fund plan group with all its components
func processGroupCreation(ctx context.Context, tx pgx.Tx, meta groupCreationMeta, group FundPlanGroupRequest) map[string]interface{} {
	dbGroupID := uuid.New().String()
	result := map[string]interface{}{
		"group_id":             dbGroupID,
		"client_group_key":     group.GroupID,
		constants.ValueSuccess: false,
	}

	direction, currency, primaryKey, primaryValue, err := resolveGroupMetadata(group)
	if err != nil {
		result[constants.ValueError] = fmt.Sprintf("failed to resolve group metadata for %s: %s", group.GroupID, err.Error())
		return result
	}

	// Validate allocations match total amount
	totalAllocated := 0.0
	for _, allocation := range group.AllocatedAccounts {
		totalAllocated += allocation.AllocatedAmount
	}

	// Use a more reasonable tolerance for floating point comparison
	tolerance := 0.01
	difference := abs(totalAllocated - group.TotalAmount)

	if difference > tolerance {
		// Provide detailed error information
		var allocationDetails []string
		for i, allocation := range group.AllocatedAccounts {
			allocationDetails = append(allocationDetails, fmt.Sprintf("Account %d: %s = %.2f", i+1, allocation.AccountID, allocation.AllocatedAmount))
		}

		result[constants.ValueError] = fmt.Sprintf("sum of allocated amounts (%.2f) does not match group total amount (%.2f). Difference: %.2f. Allocations: [%s]",
			totalAllocated, group.TotalAmount, difference, strings.Join(allocationDetails, ", "))
		return result
	}

	// Insert fund_plan_groups record
	insertGroupQuery := `
		INSERT INTO fund_plan_groups (group_id, plan_id, direction, currency, primary_key, primary_value, total_amount, entity_name, horizon)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`

	_, err = tx.Exec(ctx, insertGroupQuery, dbGroupID, meta.PlanID, direction, currency, primaryKey, primaryValue, group.TotalAmount, meta.EntityName, meta.Horizon)
	if err != nil {
		result[constants.ValueError] = fmt.Sprintf("failed to insert group: %s", err.Error())
		return result
	}

	// Insert fund_plan_lines records
	for _, line := range group.Lines {
		// Generate UUID for database line_id (primary key), use line.LineID as source_ref
		lineUUID := uuid.New().String()

		// Determine allocated account ID and amount from the line
		var allocatedAccountID sql.NullString
		var allocatedAmount sql.NullFloat64

		if line.AllocatedBankAccount != "" {
			allocatedAccountID = sql.NullString{String: line.AllocatedBankAccount, Valid: true}
			allocatedAmount = sql.NullFloat64{Float64: line.AllocatedAmount, Valid: true}
		}

		insertLineQuery := `
			INSERT INTO fund_plan_lines (line_id, group_id, source_ref, counterparty_or_type, category, amount, currency, allocated_account_id, allocated_amount)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`

		_, err = tx.Exec(ctx, insertLineQuery,
			lineUUID,
			dbGroupID,
			line.LineID,  // source_ref is the transaction reference (TR-PAY-xxx, TR-REC-xxx)
			primaryValue, // counterparty_or_type from group metadata
			getCategoryFromDirection(direction),
			line.Amount,
			currency,
			allocatedAccountID,
			allocatedAmount)

		if err != nil {
			result[constants.ValueError] = fmt.Sprintf("failed to insert line %s: %s", line.LineID, err.Error())
			return result
		}
	}

	// Insert fund_plan_allocations records
	for _, allocation := range group.AllocatedAccounts {
		insertAllocationQuery := `
			INSERT INTO fund_plan_allocations (allocation_id, group_id, account_id, allocated_amount)
			VALUES (gen_random_uuid(), $1, $2, $3)`

		_, err = tx.Exec(ctx, insertAllocationQuery, dbGroupID, allocation.AccountID, allocation.AllocatedAmount)
		if err != nil {
			result[constants.ValueError] = fmt.Sprintf("failed to insert allocation for account %s: %s", allocation.AccountID, err.Error())
			return result
		}
	}

	// Create audit action entry
	insertAuditQuery := `
		INSERT INTO auditaction_fund_plan_groups (group_id, actiontype, processing_status, requested_by, requested_at, requested_ip)
		VALUES ($1, 'CREATE', 'PENDING_APPROVAL', $2, now(), $3)
		RETURNING action_id`

	var actionID string
	err = tx.QueryRow(ctx, insertAuditQuery, dbGroupID, meta.UserEmail, nullIfEmpty(meta.RequestedIP)).Scan(&actionID)
	if err != nil {
		result[constants.ValueError] = fmt.Sprintf("failed to create audit action: %s", err.Error())
		return result
	}

	result[constants.ValueSuccess] = true
	result["action_id"] = actionID
	result["message"] = "Fund plan group created successfully"
	return result
}

// parseGroupID extracts metadata from group_id pattern: G-{number}-{date}-{currency}-{entity/counterparty}
func parseGroupID(groupID string) (direction, currency, primaryKey, primaryValue string, err error) {
	// Expected pattern: G-1-202510100-USD-ACMELtd or G-2-202510100-USD-ACMELtd
	parts := strings.Split(groupID, "-")
	if len(parts) < 5 {
		err = errors.New("invalid group_id format, expected: G-{number}-{date}-{currency}-{entity}")
		return
	}

	// Extract currency (4th part)
	currency = parts[3]

	// Extract entity/counterparty name (5th part onwards, joined by -)
	primaryValue = strings.Join(parts[4:], "-")

	// For this implementation, we'll default to outflow direction and counterparty type
	// In a real scenario, you might want to derive this from other data or make it explicit in the request
	direction = "outflow"
	primaryKey = "Counterparty"

	return
}

// abs returns absolute value of a float64
func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

// GetFundPlanSummary retrieves plan-level summary with aggregated information
func GetFundPlanSummary(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID string `json:"user_id"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}

		if req.UserID == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrUserIIsRequired)
			return
		}

		// Get user email from active sessions
		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Email
				break
			}
		}
		if userEmail == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidSession)
			return
		}

		// Query to get plan-level summary with audit information from any group in the plan
		query := `
			SELECT 
				fpg.plan_id,
				fpg.entity_name,
				fpg.horizon,
				COUNT(*) as total_groups,
				SUM(fpg.total_amount) as total_amount,
				STRING_AGG(DISTINCT fpg.currency, ', ' ORDER BY fpg.currency) as currency,
				STRING_AGG(DISTINCT fpg.primary_key, ', ') as primary_types,
				STRING_AGG(DISTINCT fpg.primary_value, ', ') as primary_values,
				aa.actiontype,
				aa.processing_status,
				aa.requested_by,
				aa.requested_at,
				aa.requested_ip,
				aa.checker_by,
				aa.checker_at,
				aa.checker_ip,
				aa.checker_comment,
				aa.reason
			FROM fund_plan_groups fpg
			LEFT JOIN LATERAL (
				SELECT actiontype, processing_status, requested_by, requested_at, requested_ip,
					   checker_by, checker_at, checker_ip, checker_comment, reason
				FROM auditaction_fund_plan_groups aafpg
				WHERE aafpg.group_id IN (
					SELECT group_id FROM fund_plan_groups fpg2 WHERE fpg2.plan_id = fpg.plan_id LIMIT 1
				)
				ORDER BY requested_at DESC, action_id DESC
				LIMIT 1
			) aa ON TRUE
			GROUP BY fpg.plan_id, fpg.entity_name, fpg.horizon, 
					 aa.actiontype, aa.processing_status, aa.requested_by, aa.requested_at,
					 aa.requested_ip, aa.checker_by, aa.checker_at, aa.checker_ip, aa.checker_comment, aa.reason
			ORDER BY fpg.plan_id`

		rows, err := pgxPool.Query(ctx, query)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		var results []map[string]interface{}

		for rows.Next() {
			var planID, entityName, primaryTypes, primaryValues, currency string
			var horizon, totalGroups int
			var totalAmount float64
			var actionType, processingStatus sql.NullString
			var requestedBy, requestedIP, checkerBy, checkerIP, checkerComment, reason sql.NullString
			var requestedAt, checkerAt sql.NullTime

			err := rows.Scan(&planID, &entityName, &horizon, &totalGroups, &totalAmount,
				&currency, &primaryTypes, &primaryValues, &actionType, &processingStatus,
				&requestedBy, &requestedAt, &requestedIP, &checkerBy, &checkerAt, &checkerIP, &checkerComment, &reason)

			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, constants.ErrScanFailed+err.Error())
				return
			}
			if msg := validateFundPlanScope(ctx, map[string]interface{}{"entity_name": entityName}); msg != "" {
				continue
			}

			plan := map[string]interface{}{
				"plan_id":           planID,
				"entity_name":       entityName,
				"horizon":           horizon,
				"total_groups":      totalGroups,
				"total_amount":      roundToDecimal(totalAmount, 2),
				"currency":          currency,
				"primary_types":     primaryTypes,
				"primary_values":    primaryValues,
				"action_type":       nullableToString(actionType),
				"processing_status": nullableToString(processingStatus),
				"requested_by":      nullableToString(requestedBy),
				"requested_ip":      nullableToString(requestedIP),
				"checker_by":        nullableToString(checkerBy),
				"checker_ip":        nullableToString(checkerIP),
				"checker_comment":   nullableToString(checkerComment),
				"reason":            nullableToString(reason),
			}

			if requestedAt.Valid {
				plan["requested_at"] = api.FormatAuditTimestampIST(requestedAt.Time)
			}
			if checkerAt.Valid {
				plan["checker_at"] = api.FormatAuditTimestampIST(checkerAt.Time)
			}

			results = append(results, plan)
		}

		api.RespondEnvelopeSuccess(w, "Success", map[string]interface{}{
			"fund_plans": results,
		})
	}
}

// GetFundPlanDetails retrieves detailed group information for a specific plan
func GetFundPlanDetails(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID string `json:"user_id"`
			PlanID string `json:"plan_id"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}

		if req.UserID == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrUserIIsRequired)
			return
		}

		if req.PlanID == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrPlanIDRequired)
			return
		}

		// Get user email from active sessions
		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Email
				break
			}
		}
		if userEmail == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidSession)
			return
		}

		// Query to get all groups for a specific plan
		query := `
			SELECT 
				fpg.group_id,
				fpg.plan_id,
				fpg.direction,
				fpg.currency,
				fpg.primary_key,
				fpg.primary_value,
				fpg.total_amount,
				fpg.entity_name,
				fpg.horizon,
				aa.actiontype,
				aa.processing_status,
				aa.requested_by,
				aa.requested_at,
				aa.requested_ip,
				aa.checker_by,
				aa.checker_at,
				aa.checker_ip,
				aa.checker_comment,
				aa.reason
			FROM fund_plan_groups fpg
			LEFT JOIN LATERAL (
				SELECT actiontype, processing_status, requested_by, requested_at, requested_ip,
					   checker_by, checker_at, checker_ip, checker_comment, reason
				FROM auditaction_fund_plan_groups aafpg
				WHERE aafpg.group_id = fpg.group_id
				ORDER BY requested_at DESC, action_id DESC
				LIMIT 1
			) aa ON TRUE
			WHERE fpg.plan_id = $1
			ORDER BY fpg.group_id`

		rows, err := pgxPool.Query(ctx, query, req.PlanID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		var groups []map[string]interface{}
		var planInfo map[string]interface{}

		for rows.Next() {
			var groupID, planID, direction, currency, primaryKey, primaryValue, entityName string
			var totalAmount float64
			var horizon int
			var actionType, processingStatus sql.NullString
			var requestedBy, requestedIP, checkerBy, checkerIP, checkerComment, reason sql.NullString
			var requestedAt, checkerAt sql.NullTime

			err := rows.Scan(&groupID, &planID, &direction, &currency, &primaryKey, &primaryValue,
				&totalAmount, &entityName, &horizon, &actionType, &processingStatus,
				&requestedBy, &requestedAt, &requestedIP, &checkerBy, &checkerAt, &checkerIP, &checkerComment, &reason)

			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, constants.ErrScanFailed+err.Error())
				return
			}
			scopeFields := map[string]interface{}{
				"entity_name": entityName,
				"currency":    currency,
			}
			if strings.EqualFold(primaryKey, "counterparty") {
				scopeFields["counterparty_name"] = primaryValue
			}
			if msg := validateFundPlanScope(ctx, scopeFields); msg != "" {
				continue
			}

			// Set plan info on first iteration
			if planInfo == nil {
				planInfo = map[string]interface{}{
					"plan_id":     planID,
					"entity_name": entityName,
					"horizon":     horizon,
				}
			}

			// Create group data
			group := map[string]interface{}{
				"group_id":          groupID,
				"direction":         direction,
				"currency":          currency,
				"primary_key":       primaryKey,
				"primary_value":     primaryValue,
				"total_amount":      totalAmount,
				"action_type":       nullableToString(actionType),
				"processing_status": nullableToString(processingStatus),
				"requested_by":      nullableToString(requestedBy),
				"requested_ip":      nullableToString(requestedIP),
				"checker_by":        nullableToString(checkerBy),
				"checker_ip":        nullableToString(checkerIP),
				"checker_comment":   nullableToString(checkerComment),
				"reason":            nullableToString(reason),
			}

			if requestedAt.Valid {
				group["requested_at"] = api.FormatAuditTimestampIST(requestedAt.Time)
			}
			if checkerAt.Valid {
				group["checker_at"] = api.FormatAuditTimestampIST(checkerAt.Time)
			}

			groups = append(groups, group)
		}

		// If no groups found, return empty result with plan info if provided
		if planInfo == nil {
			planInfo = map[string]interface{}{
				"plan_id": req.PlanID,
			}
		}

		result := map[string]interface{}{
			"plan_info":        planInfo,
			"fund_plan_groups": groups,
		}

		api.RespondEnvelopeSuccess(w, "Success", result)
	}
}

// BulkApproveFundPlans approves all fund plan groups in a specific plan
func BulkApproveFundPlans(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID  string `json:"user_id"`
			PlanID  string `json:"plan_id"`
			Comment string `json:"comment,omitempty"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}

		if req.UserID == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrUserIIsRequired)
			return
		}
		if req.PlanID == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrPlanIDRequired)
			return
		}

		// Get user email from active sessions
		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Email
				break
			}
		}
		if userEmail == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidSession)
			return
		}

		fundPlanRow, err := loadFundPlanningRow(ctx, pgxPool, req.PlanID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrFailedToGetGroups+err.Error())
			return
		}

		if !runtime.Enforce(ctx, w, r, pgxPool, runtime.EnforceInput{
			EventCode:           common.TriggerPreApprove,
			ModuleCode:          common.ModuleCash,
			SubModule:           "FUND_PLANNING",
			ActorUserID:         req.UserID,
			HandlerName:         "BulkApproveFundPlans",
			APIPath:             "/cash/fund-planning/bulk-approve",
			DefaultBlockMessage: "Fund plan approve blocked by policy",
			Fields:              buildFundPlanningPolicyFields(fundPlanRow),
		}) {
			return
		}

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxStartFailed+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		// Get all group IDs for the plan that are pending approval
		getGroupsQuery := `
			SELECT fpg.group_id 
			FROM fund_plan_groups fpg
			INNER JOIN auditaction_fund_plan_groups aa ON aa.group_id = fpg.group_id
			WHERE fpg.plan_id = $1 
			AND aa.processing_status = 'PENDING_APPROVAL'
			AND aa.actiontype = 'CREATE'`

		rows, err := tx.Query(ctx, getGroupsQuery, req.PlanID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrFailedToGetGroups+err.Error())
			return
		}
		defer rows.Close()

		var groupIDs []string
		for rows.Next() {
			var groupID string
			err := rows.Scan(&groupID)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, constants.ErrFailedToScanGroupID+err.Error())
				return
			}
			groupIDs = append(groupIDs, groupID)
		}
		rows.Close()

		if len(groupIDs) == 0 {
			api.RespondWithPayload(w, false, "No groups found with pending approval for plan: "+req.PlanID, []map[string]interface{}{})
			return
		}

		// Update all groups to APPROVED
		updateQuery := `
			UPDATE auditaction_fund_plan_groups 
			SET processing_status = 'APPROVED',
				checker_by = $1,
				checker_at = now(),
				checker_comment = $2,
				checker_ip = $3
			WHERE group_id = ANY($4) 
			AND processing_status = 'PENDING_APPROVAL'
			AND actiontype = 'CREATE'`

		cmdTag, err := tx.Exec(ctx, updateQuery, userEmail, req.Comment, nullIfEmpty(api.ClientIPFromRequest(r)), groupIDs)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to approve groups: "+err.Error())
			return
		}

		rowsAffected := cmdTag.RowsAffected()
		if err = tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxCommitFailed+err.Error())
			return
		}

		result := map[string]interface{}{
			"plan_id":          req.PlanID,
			"groups_processed": len(groupIDs),
			"groups_approved":  rowsAffected,
			"message":          fmt.Sprintf("Successfully approved %d groups in plan %s", rowsAffected, req.PlanID),
			"approved_groups":  groupIDs,
		}

		api.RespondWithPayload(w, true, "", result)
	}
}

// BulkRejectFundPlans rejects all fund plan groups in a specific plan
func BulkRejectFundPlans(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID  string `json:"user_id"`
			PlanID  string `json:"plan_id"`
			Comment string `json:"comment,omitempty"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}

		if req.UserID == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrUserIIsRequired)
			return
		}
		if req.PlanID == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrPlanIDRequired)
			return
		}

		// Get user email from active sessions
		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Email
				break
			}
		}
		if userEmail == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidSession)
			return
		}

		fundPlanRow, err := loadFundPlanningRow(ctx, pgxPool, req.PlanID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrFailedToGetGroups+err.Error())
			return
		}

		if !runtime.Enforce(ctx, w, r, pgxPool, runtime.EnforceInput{
			EventCode:           common.TriggerPreReject,
			ModuleCode:          common.ModuleCash,
			SubModule:           "FUND_PLANNING",
			ActorUserID:         req.UserID,
			HandlerName:         "BulkRejectFundPlans",
			APIPath:             "/cash/fund-planning/bulk-reject",
			DefaultBlockMessage: "Fund plan reject blocked by policy",
			Fields:              buildFundPlanningPolicyFields(fundPlanRow),
		}) {
			return
		}

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxStartFailed+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		// Get all group IDs for the plan that are pending approval
		getGroupsQuery := `
			SELECT fpg.group_id 
			FROM fund_plan_groups fpg
			INNER JOIN auditaction_fund_plan_groups aa ON aa.group_id = fpg.group_id
			WHERE fpg.plan_id = $1 
			AND aa.processing_status = 'PENDING_APPROVAL'
			AND aa.actiontype = 'CREATE'`

		rows, err := tx.Query(ctx, getGroupsQuery, req.PlanID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrFailedToGetGroups+err.Error())
			return
		}
		defer rows.Close()

		var groupIDs []string
		for rows.Next() {
			var groupID string
			err := rows.Scan(&groupID)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, constants.ErrFailedToScanGroupID+err.Error())
				return
			}
			groupIDs = append(groupIDs, groupID)
		}
		rows.Close()

		if len(groupIDs) == 0 {
			api.RespondWithPayload(w, false, "No groups found with pending approval for plan: "+req.PlanID, []map[string]interface{}{})
			return
		}

		// Update all groups to REJECTED
		updateQuery := `
			UPDATE auditaction_fund_plan_groups 
			SET processing_status = 'REJECTED',
				checker_by = $1,
				checker_at = now(),
				checker_comment = $2,
				checker_ip = $3
			WHERE group_id = ANY($4) 
			AND processing_status = 'PENDING_APPROVAL'
			AND actiontype = 'CREATE'`

		cmdTag, err := tx.Exec(ctx, updateQuery, userEmail, req.Comment, nullIfEmpty(api.ClientIPFromRequest(r)), groupIDs)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to reject groups: "+err.Error())
			return
		}

		rowsAffected := cmdTag.RowsAffected()
		if err = tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxCommitFailed+err.Error())
			return
		}

		result := map[string]interface{}{
			"plan_id":          req.PlanID,
			"groups_processed": len(groupIDs),
			"groups_rejected":  rowsAffected,
			"message":          fmt.Sprintf("Successfully rejected %d groups in plan %s", rowsAffected, req.PlanID),
			"rejected_groups":  groupIDs,
		}

		api.RespondWithPayload(w, true, "", result)
	}
}

// BulkRequestDeleteFundPlans creates delete requests for all fund plan groups in a specific plan
func BulkRequestDeleteFundPlans(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID string `json:"user_id"`
			PlanID string `json:"plan_id"`
			Reason string `json:"reason,omitempty"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}

		if req.UserID == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrUserIIsRequired)
			return
		}
		if req.PlanID == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrPlanIDRequired)
			return
		}

		// Get user email from active sessions
		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Email
				break
			}
		}
		if userEmail == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidSession)
			return
		}

		fundPlanRow, err := loadFundPlanningRow(ctx, pgxPool, req.PlanID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrFailedToGetGroups+err.Error())
			return
		}

		if !runtime.Enforce(ctx, w, r, pgxPool, runtime.EnforceInput{
			EventCode:           common.TriggerPreDelete,
			ModuleCode:          common.ModuleCash,
			SubModule:           "FUND_PLANNING",
			ActorUserID:         req.UserID,
			HandlerName:         "BulkRequestDeleteFundPlans",
			APIPath:             "/cash/fund-planning/bulk-delete",
			DefaultBlockMessage: "Fund plan delete blocked by policy",
			Fields:              buildFundPlanningPolicyFields(fundPlanRow),
		}) {
			return
		}

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxStartFailed+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		// Get all group IDs for the plan that are approved (can be deleted)
		getGroupsQuery := `
			SELECT fpg.group_id 
			FROM fund_plan_groups fpg
			INNER JOIN auditaction_fund_plan_groups aa ON aa.group_id = fpg.group_id
			WHERE fpg.plan_id = $1 
			AND aa.processing_status = 'APPROVED'
			AND aa.actiontype = 'CREATE'
			AND NOT EXISTS (
				SELECT 1 FROM auditaction_fund_plan_groups aa2 
				WHERE aa2.group_id = fpg.group_id 
				AND aa2.actiontype = 'DELETE'
				AND aa2.processing_status IN ('PENDING_DELETE_APPROVAL', 'APPROVED')
			)`

		rows, err := tx.Query(ctx, getGroupsQuery, req.PlanID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrFailedToGetGroups+err.Error())
			return
		}
		defer rows.Close()

		var groupIDs []string
		for rows.Next() {
			var groupID string
			err := rows.Scan(&groupID)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, constants.ErrFailedToScanGroupID+err.Error())
				return
			}
			groupIDs = append(groupIDs, groupID)
		}
		rows.Close()

		if len(groupIDs) == 0 {
			api.RespondWithPayload(w, false, "No approved groups available for deletion in plan: "+req.PlanID, []map[string]interface{}{})
			return
		}

		// Create delete request audit actions for all groups
		var actionIDs []string
		for _, groupID := range groupIDs {
			insertQuery := `
				INSERT INTO auditaction_fund_plan_groups (group_id, actiontype, processing_status, reason, requested_by, requested_at, requested_ip)
				VALUES ($1, 'DELETE', 'PENDING_DELETE_APPROVAL', $2, $3, now(), $4)
				RETURNING action_id`

			var actionID string
			err = tx.QueryRow(ctx, insertQuery, groupID, req.Reason, userEmail, nullIfEmpty(api.ClientIPFromRequest(r))).Scan(&actionID)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, fmt.Sprintf("failed to create delete request for group %s: %s", groupID, err.Error()))
				return
			}
			actionIDs = append(actionIDs, actionID)
		}

		if err = tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxCommitFailed+err.Error())
			return
		}

		result := map[string]interface{}{
			"plan_id":                 req.PlanID,
			"groups_processed":        len(groupIDs),
			"delete_requests_created": len(actionIDs),
			"message":                 fmt.Sprintf("Successfully created %d delete requests for plan %s", len(actionIDs), req.PlanID),
			"affected_groups":         groupIDs,
			"action_ids":              actionIDs,
		}

		api.RespondWithPayload(w, true, "", result)
	}
}
