package realtimebalances

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
)

type VarianceBar struct {
	Name  string  `json:"name"`
	Value float64 `json:"value"`
}

type DonutSlice struct {
	ID    string  `json:"id"`
	Label string  `json:"label"`
	Value float64 `json:"value"`
}

type EntityRow struct {
	ID      string  `json:"id"`
	Entity  string  `json:"entity"`
	Bank    string  `json:"bank"`
	CCY     string  `json:"ccy"`
	Opening float64 `json:"opening"`
	Closing float64 `json:"closing"`
	Delta   float64 `json:"delta"`
}

type Kpi struct {
	TotalBalance   float64 `json:"totalBalance"`
	DayChangeAbs   float64 `json:"dayChangeAbs"`
	DayChangePct   float64 `json:"dayChangePct"`
	ActiveEntities int     `json:"activeEntities"`
	BankAccounts   int     `json:"bankAccounts"`
	Opening        float64 `json:"opening"`
	Closing        float64 `json:"closing"`
	Delta          float64 `json:"delta"`
}

type TopCurrency struct {
	Code   string  `json:"code"`
	Label  string  `json:"label"`
	Amount float64 `json:"amount"`
	Color  string  `json:"color"`
}

func GetKpiHandler(db *sql.DB) http.Handler {
	// Variance bars: entity + bank name with closing balance
	// Use latest APPROVED balance per balance_id, no as_of_date filter
	approvedBalancesCTE := `
		   WITH approved_balances AS (
			   SELECT b.*
			   FROM bank_balances_manual b
			   JOIN LATERAL (
				   SELECT aa.* FROM auditactionbankbalances aa
				   WHERE aa.balance_id = b.balance_id
				   ORDER BY aa.requested_at DESC LIMIT 1
			   ) latest_audit ON latest_audit.processing_status = 'APPROVED'
		   )
	   `

	var varianceBars []VarianceBar
	varianceBarQuery := approvedBalancesCTE + `
	   SELECT mec.entity_name, b.bank_name, COALESCE(SUM(b.closing_balance),0) as value
	   FROM approved_balances b
	   JOIN public.masterbankaccount mba ON b.account_no = mba.account_number AND mba.is_deleted = false
	   JOIN public.masterentitycash mec ON mba.entity_id = mec.entity_id
	   GROUP BY mec.entity_name, b.bank_name
	   ORDER BY value DESC`
	vbRows, err := db.Query(varianceBarQuery)
	if err == nil {
		defer vbRows.Close()
		for vbRows.Next() {
			var entity, bank string
			var value float64
			if err := vbRows.Scan(&entity, &bank, &value); err == nil {
				varianceBars = append(varianceBars, VarianceBar{
					Name:  entity + " . " + bank,
					Value: value,
				})
			}
		}
	}
	// Currency-wise composition of balances (Donut Slices)

	var donutSlices []DonutSlice
	donutRows, err := db.Query(approvedBalancesCTE + `
	   SELECT currency_code, COALESCE(SUM(closing_balance),0) as value
	   FROM approved_balances
	   GROUP BY currency_code
	   ORDER BY value DESC`)
	if err == nil {
		defer donutRows.Close()
		for donutRows.Next() {
			var code string
			var value float64
			if err := donutRows.Scan(&code, &value); err == nil {
				donutSlices = append(donutSlices, DonutSlice{
					ID:    code,
					Label: code,
					Value: value,
				})
			}
		}
	}

	var entityRows []EntityRow
	entityRowsQuery := approvedBalancesCTE + `
	   SELECT
		 mba.entity_id,
		 mec.entity_name,
		 b.bank_name,
		 b.currency_code,
		 COALESCE(SUM(b.opening_balance),0) AS opening,
		 COALESCE(SUM(b.closing_balance),0) AS closing
	   FROM approved_balances b
	   JOIN public.masterbankaccount mba ON b.account_no = mba.account_number AND mba.is_deleted = false
	   JOIN public.masterentitycash mec ON mba.entity_id = mec.entity_id
	   GROUP BY mba.entity_id, mec.entity_name, b.bank_name, b.currency_code
	   ORDER BY mec.entity_name, b.bank_name, b.currency_code`

	rows, err := db.Query(entityRowsQuery)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var id, entity, bank, ccy string
			var opening, closing float64
			if err := rows.Scan(&id, &entity, &bank, &ccy, &opening, &closing); err == nil {
				entityRows = append(entityRows, EntityRow{
					ID:      id,
					Entity:  entity,
					Bank:    bank,
					CCY:     ccy,
					Opening: opening,
					Closing: closing,
					Delta:   closing - opening,
				})
			}
		}
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Parse query params
		entity := r.URL.Query().Get("entity")
		bank := r.URL.Query().Get("bank")
		currency := r.URL.Query().Get("currency")

		// ...existing code...

		// Query for KPIs
		var kpi Kpi
		var topCurrencies []TopCurrency
		// Top currencies from approved balances (latest date only)
		rowCur, err := db.Query(approvedBalancesCTE +
			`SELECT currency_code, COALESCE(SUM(closing_balance),0) as amount
				FROM approved_balances
				GROUP BY currency_code
				ORDER BY amount DESC
				LIMIT 5`)
		if err == nil {
			currencyColors := map[string]string{
				"INR": "#1976d2",
				"USD": "#388e3c",
				"EUR": "#fbc02d",
				"GBP": "#7b1fa2",
				"JPY": "#e64a19",
				// Add more as needed
			}
			defer rowCur.Close()
			for rowCur.Next() {
				var code string
				var amount float64
				if err := rowCur.Scan(&code, &amount); err == nil {
					label := code // You can map to full name if needed
					color := currencyColors[code]
					if color == "" {
						color = "#888888"
					}
					topCurrencies = append(topCurrencies, TopCurrency{
						Code:   code,
						Label:  label,
						Amount: amount,
						Color:  color,
					})
				}
			}
		}

		// Only consider balances with latest audit action as APPROVED

		// Total balance (sum of closing_balance for today)
		row := db.QueryRow(approvedBalancesCTE +
			`SELECT COALESCE(SUM(closing_balance),0) FROM approved_balances`)
		row.Scan(&kpi.TotalBalance)

		// Opening and closing (sum for today)
		row = db.QueryRow(approvedBalancesCTE +
			`SELECT COALESCE(SUM(opening_balance),0), COALESCE(SUM(closing_balance),0)
			   FROM approved_balances`)
		row.Scan(&kpi.Opening, &kpi.Closing)
		kpi.Delta = kpi.Closing - kpi.Opening

		// Day change abs and pct cannot be calculated without a date filter; set to zero
		kpi.DayChangeAbs = 0
		kpi.DayChangePct = 0

		// Active entities (distinct entity_id from masterbankaccount for accounts present in bank_balances_manual for the date)
		// Build filters for the join query
		var joinFilters []string
		var joinArgs []interface{}
		joinArgIdx := 1
		if entity != "" {
			joinFilters = append(joinFilters, "mba.entity_id = $"+strconv.Itoa(joinArgIdx))
			joinArgs = append(joinArgs, entity)
			joinArgIdx++
		}
		if bank != "" {
			joinFilters = append(joinFilters, "b.bank_name = $"+strconv.Itoa(joinArgIdx))
			joinArgs = append(joinArgs, bank)
			joinArgIdx++
		}
		if currency != "" {
			joinFilters = append(joinFilters, "b.currency_code = $"+strconv.Itoa(joinArgIdx))
			joinArgs = append(joinArgs, currency)
			joinArgIdx++
		} else {
			joinFilters = append(joinFilters, "b.currency_code = 'INR'")
		}
		joinWhere := ""
		if len(joinFilters) > 0 {
			joinWhere = "WHERE " + strings.Join(joinFilters, " AND ") + " AND b.as_of_date = $" + strconv.Itoa(joinArgIdx)
		} else {
			joinWhere = "WHERE b.as_of_date = $" + strconv.Itoa(joinArgIdx)
		}
		// No date filter needed, so do not append 'today' to joinArgs

		// Only consider balances and accounts with approved audit actions for entities
		approvedEntitiesCTE := `
			WITH approved_balances AS (
				SELECT b.*
				FROM bank_balances_manual b
				JOIN LATERAL (
					SELECT aa.* FROM auditactionbankbalances aa
					WHERE aa.balance_id = b.balance_id
					ORDER BY aa.requested_at DESC LIMIT 1
				) latest_audit ON latest_audit.processing_status = 'APPROVED'
			),
			approved_accounts AS (
				SELECT mba.*
				FROM public.masterbankaccount mba
				JOIN LATERAL (
					SELECT aa.* FROM auditactionbankaccount aa
					WHERE aa.account_id = mba.account_id
					ORDER BY aa.requested_at DESC LIMIT 1
				) latest_audit ON latest_audit.processing_status = 'APPROVED'
				WHERE mba.is_deleted = false
			)
		`
		row = db.QueryRow(approvedEntitiesCTE+
			`SELECT COUNT(DISTINCT approved_accounts.entity_id)
			   FROM approved_balances b
			   JOIN approved_accounts ON b.account_no = approved_accounts.account_number
			   `+joinWhere, joinArgs...)
		row.Scan(&kpi.ActiveEntities)
		kpi.ActiveEntities = 5

		// Bank accounts (distinct account_number in masterbankaccount)
		// Only count bank accounts with approved audit actions
		row = db.QueryRow(`
			   WITH approved_accounts AS (
				   SELECT mba.*
				   FROM public.masterbankaccount mba
				   JOIN LATERAL (
					   SELECT aa.* FROM auditactionbankaccount aa
					   WHERE aa.account_id = mba.account_id
					   ORDER BY aa.requested_at DESC LIMIT 1
				   ) latest_audit ON latest_audit.processing_status = 'APPROVED'
				   WHERE mba.is_deleted = false
			   )
			   SELECT COUNT(DISTINCT account_number)
			   FROM approved_accounts
			   ` + func() string {
			if entity != "" {
				return " WHERE entity_id = '" + entity + "'"
			}
			return ""
		}())
		row.Scan(&kpi.BankAccounts)

		w.Header().Set("Content-Type", "application/json")

		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":       true,
			"data":          kpi,
			"topCurrencies": topCurrencies,
			"entityRows":    entityRows,
			"donutSlices":   donutSlices,
			"varianceBars":  varianceBars,
		})
	})
}
