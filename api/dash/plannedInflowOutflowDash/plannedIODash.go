package plannedinflowoutflowdash

import (
	"context"
	"encoding/json"
	"log"
	"math"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Currency conversion rates to USD
var rates = map[string]float64{
	"USD": 1.0,
	"AUD": 0.68,
	"CAD": 0.75,
	"CHF": 1.1,
	"CNY": 0.14,
	"RMB": 0.14,
	"EUR": 1.09,
	"GBP": 1.28,
	"JPY": 0.0067,
	"SEK": 0.095,
	"INR": 0.0117,
}

func getCurrencyRate(currency string) float64 {
	if rate, ok := rates[currency]; ok {
		return rate
	}
	log.Printf("Unknown currency encountered: %s, using default rate 1.0", currency)
	return 1.0
}

type PlannedIODashEntity struct {
	Entity  string  `json:"entity"`
	Inflow  float64 `json:"inflow"`
	Outflow float64 `json:"outflow"`
}

type PlannedIODashCashflow struct {
	CashflowNature string  `json:"cashflow_nature"`
	Inflow         float64 `json:"inflow"`
	Outflow        float64 `json:"outflow"`
}

type PlannedInflowOutflowData struct {
	DateRange string                  `json:"date_range"`
	Entities  []PlannedIODashEntity   `json:"entities"`
	Cashflows []PlannedIODashCashflow `json:"cashflows"`
}

func GetPlannedIODash(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		type reqBody struct {
			UserID string `json:"user_id"`
		}
		var body reqBody
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.UserID == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": "Missing or invalid user_id in body"})
			return
		}
		ctx := context.Background()
		now := time.Now()
		today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
		qStart := quarterStart(today)
		ranges := []struct {
			Label string
			Start time.Time
			End   time.Time
		}{
			{"Next 30 Days", today, today.AddDate(0, 0, 30)},
			{"Next 60 Days", today, today.AddDate(0, 30, 60)},
			{"This Quarter", qStart, today.AddDate(0, 60, 90)},
		}
		out := make([]PlannedInflowOutflowData, 0, len(ranges))
		for _, dr := range ranges {
			// Entities inflow/outflow with currency conversion
			entityQ := `
				SELECT COALESCE(i.entity_name, '') AS entity,
					   p.currency_code,
					   SUM(CASE WHEN i.cashflow_type = 'Inflow' THEN i.expected_amount ELSE 0 END) AS inflow,
					   SUM(CASE WHEN i.cashflow_type = 'Outflow' THEN i.expected_amount ELSE 0 END) AS outflow
				FROM cashflow_proposal_item i
				JOIN cashflow_proposal p ON i.proposal_id = p.proposal_id
				WHERE i.start_date >= $1 AND i.start_date <= $2
				  AND p.status = 'Active'
				  AND (
					SELECT a.processing_status FROM audit_action_cashflow_proposal a
					WHERE a.proposal_id = p.proposal_id
					ORDER BY a.requested_at DESC LIMIT 1
				  ) = 'APPROVED'
				GROUP BY i.entity_name, p.currency_code
			`
			rows, err := pgxPool.Query(ctx, entityQ, dr.Start, dr.End)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": err.Error()})
				return
			}

			// Aggregate by entity and convert to USD
			entityMap := make(map[string]*PlannedIODashEntity)
			for rows.Next() {
				var entity, currency string
				var inflow, outflow float64
				if err := rows.Scan(&entity, &currency, &inflow, &outflow); err != nil {
					continue
				}

				// Get conversion rate with better error handling
				rate := getCurrencyRate(currency)

				// Convert to USD
				inflowUSD := math.Abs(inflow) * rate
				outflowUSD := math.Abs(outflow) * rate

				if entityMap[entity] == nil {
					entityMap[entity] = &PlannedIODashEntity{
						Entity:  entity,
						Inflow:  0,
						Outflow: 0,
					}
				}
				entityMap[entity].Inflow += inflowUSD
				entityMap[entity].Outflow += outflowUSD
			}
			rows.Close()

			// Convert map to slice
			entities := []PlannedIODashEntity{}
			for _, entity := range entityMap {
				entities = append(entities, *entity)
			}
			// Cashflow nature inflow/outflow with currency conversion
			cashflowQ := `
				SELECT COALESCE(i.cashflow_type, '') AS cashflow_nature,
					   p.currency_code,
					   SUM(CASE WHEN i.cashflow_type = 'Inflow' THEN i.expected_amount ELSE 0 END) AS inflow,
					   SUM(CASE WHEN i.cashflow_type = 'Outflow' THEN i.expected_amount ELSE 0 END) AS outflow
				FROM cashflow_proposal_item i
				JOIN cashflow_proposal p ON i.proposal_id = p.proposal_id
				WHERE i.start_date >= $1 AND i.start_date <= $2
				  AND p.status = 'Active'
				  AND (
					SELECT a.processing_status FROM audit_action_cashflow_proposal a
					WHERE a.proposal_id = p.proposal_id
					ORDER BY a.requested_at DESC LIMIT 1
				  ) = 'APPROVED'
				GROUP BY i.cashflow_type, p.currency_code
			`
			rows2, err := pgxPool.Query(ctx, cashflowQ, dr.Start, dr.End)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": err.Error()})
				return
			}

			// Aggregate by cashflow nature and convert to USD
			cashflowMap := make(map[string]*PlannedIODashCashflow)
			for rows2.Next() {
				var nature, currency string
				var inflow, outflow float64
				if err := rows2.Scan(&nature, &currency, &inflow, &outflow); err != nil {
					continue
				}

				// Get conversion rate with better error handling
				rate := getCurrencyRate(currency)

				// Convert to USD
				inflowUSD := math.Abs(inflow) * rate
				outflowUSD := math.Abs(outflow) * rate

				if cashflowMap[nature] == nil {
					cashflowMap[nature] = &PlannedIODashCashflow{
						CashflowNature: nature,
						Inflow:         0,
						Outflow:        0,
					}
				}
				cashflowMap[nature].Inflow += inflowUSD
				cashflowMap[nature].Outflow += outflowUSD
			}
			rows2.Close()

			// Convert map to slice
			cashflows := []PlannedIODashCashflow{}
			for _, cashflow := range cashflowMap {
				cashflows = append(cashflows, *cashflow)
			}
			out = append(out, PlannedInflowOutflowData{
				DateRange: dr.Label,
				Entities:  entities,
				Cashflows: cashflows,
			})
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "data": out})
	}
}

func quarterStart(t time.Time) time.Time {
	y, m, _ := t.Date()
	var startMonth time.Month
	switch {
	case m <= 3:
		startMonth = 1
	case m <= 6:
		startMonth = 4
	case m <= 9:
		startMonth = 7
	default:
		startMonth = 10
	}
	return time.Date(y, startMonth, 1, 0, 0, 0, 0, t.Location())
}
