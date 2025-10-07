package forward

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"strings"

	"time"

	"CimplrCorpSaas/api"

	"github.com/lib/pq"
)

type Summary struct {
	ExposureHeaderID    string  `json:"exposure_header_id"`
	CompanyCode         string  `json:"company_code"`
	Entity              string  `json:"entity"`
	Entity1             string  `json:"entity1"`
	Entity2             string  `json:"entity2"`
	Entity3             string  `json:"entity3"`
	ExposureType        string  `json:"exposure_type"`
	DocumentID          string  `json:"document_id"`
	DocumentDate        string  `json:"document_date"`
	MaturityMonth       string  `json:"maturity_month"`
	CounterpartyName    string  `json:"counterparty_name"`
	Currency            string  `json:"currency"`
	TotalOriginalAmount float64 `json:"total_original_amount"`
	TotalOpenAmount     float64 `json:"total_open_amount"`
	ValueDate           string  `json:"value_date"`
	HedgedValue         float64 `json:"hedged_value"`
	UnhedgedValue       float64 `json:"unhedged_value"`
}
type Exposure struct {
	ExposureHeaderID    string
	CompanyCode         string
	Entity              string
	Entity1             sql.NullString
	Entity2             sql.NullString
	Entity3             sql.NullString
	ExposureType        string
	DocumentID          string
	DocumentDate        string
	CounterpartyName    string
	Currency            string
	TotalOriginalAmount float64
	TotalOpenAmount     float64
	ValueDate           string
}

// Handler: GetExposureSummary
func GetExposureSummary(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "user_id required"})
			return
		}
		// User verification via middleware
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			w.WriteHeader(http.StatusForbidden)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "No accessible business units found"})
			return
		}

		expRows, err := db.Query(`SELECT exposure_header_id, company_code, entity, entity1, entity2, entity3, exposure_type, document_id, document_date, counterparty_name, currency, total_original_amount, total_open_amount, value_date FROM exposure_headers WHERE entity = ANY($1)`, pq.Array(buNames))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "Failed to fetch exposures"})
			return
		}

		var exposures []Exposure
		var exposureIds []string
		for expRows.Next() {
			var e Exposure
			err := expRows.Scan(&e.ExposureHeaderID, &e.CompanyCode, &e.Entity, &e.Entity1, &e.Entity2, &e.Entity3, &e.ExposureType, &e.DocumentID, &e.DocumentDate, &e.CounterpartyName, &e.Currency, &e.TotalOriginalAmount, &e.TotalOpenAmount, &e.ValueDate)
			if err == nil {
				exposures = append(exposures, e)
				exposureIds = append(exposureIds, e.ExposureHeaderID)
			}
		}
		expRows.Close()
		// 4. Fetch all hedged values in one query
		hedgeMap := make(map[string]float64)
		if len(exposureIds) > 0 {
			hedgeRows, err := db.Query(`SELECT exposure_header_id, COALESCE(SUM(hedged_amount), 0) AS hedged_value FROM exposure_hedge_links WHERE exposure_header_id = ANY($1) GROUP BY exposure_header_id`, pq.Array(exposureIds))
			if err == nil {
				for hedgeRows.Next() {
					var eid string
					var hedgedValue float64
					if err := hedgeRows.Scan(&eid, &hedgedValue); err == nil {
						hedgeMap[eid] = hedgedValue
					}
				}
				hedgeRows.Close()
			}
		}
		// 5. Build summary

		var summary []Summary
		for _, exp := range exposures {
			hedgedValue := hedgeMap[exp.ExposureHeaderID]
			unhedgedValue := exp.TotalOpenAmount - hedgedValue
			maturityMonth := ""
			if exp.DocumentDate != "" {
				// Format MM-YY
				// Assume DocumentDate is YYYY-MM-DD
				parts := strings.Split(exp.DocumentDate, "-")
				if len(parts) >= 2 {
					mm := parts[1]
					yy := ""
					if len(parts[0]) == 4 {
						yy = parts[0][2:]
					}
					maturityMonth = mm + "-" + yy
				}
			}
			summary = append(summary, Summary{
				ExposureHeaderID: exp.ExposureHeaderID,
				CompanyCode:      exp.CompanyCode,
				Entity:           exp.Entity,
				Entity1: func() string {
					if exp.Entity1.Valid {
						return exp.Entity1.String
					} else {
						return ""
					}
				}(),
				Entity2: func() string {
					if exp.Entity2.Valid {
						return exp.Entity2.String
					} else {
						return ""
					}
				}(),
				Entity3: func() string {
					if exp.Entity3.Valid {
						return exp.Entity3.String
					} else {
						return ""
					}
				}(),
				ExposureType:        exp.ExposureType,
				DocumentID:          exp.DocumentID,
				DocumentDate:        exp.DocumentDate,
				MaturityMonth:       maturityMonth,
				CounterpartyName:    exp.CounterpartyName,
				Currency:            exp.Currency,
				TotalOriginalAmount: exp.TotalOriginalAmount,
				TotalOpenAmount:     exp.TotalOpenAmount,
				ValueDate:           exp.ValueDate,
				HedgedValue:         hedgedValue,
				UnhedgedValue:       unhedgedValue,
			})
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"summary": summary})
	}
}	
type ForwardBooking struct {
	SystemTransactionID         string     `json:"system_transaction_id"`
	InternalReferenceID         *string    `json:"internal_reference_id,omitempty"`
	EntityLevel0                string     `json:"entity_level_0"`
	EntityLevel1                *string    `json:"entity_level_1,omitempty"`
	EntityLevel2                *string    `json:"entity_level_2,omitempty"`
	EntityLevel3                *string    `json:"entity_level_3,omitempty"`
	LocalCurrency               *string    `json:"local_currency,omitempty"`
	OrderType                   string     `json:"order_type"`
	TransactionType             *string    `json:"transaction_type,omitempty"`
	Counterparty                *string    `json:"counterparty,omitempty"`
	ModeOfDelivery              *string    `json:"mode_of_delivery,omitempty"`
	DeliveryPeriod              *string    `json:"delivery_period,omitempty"`
	AddDate                     *time.Time `json:"add_date,omitempty"`
	SettlementDate              *time.Time `json:"settlement_date,omitempty"`
	MaturityDate                time.Time  `json:"maturity_date"`
	DeliveryDate                *time.Time `json:"delivery_date,omitempty"`
	CurrencyPair                string     `json:"currency_pair"`
	BaseCurrency                *string    `json:"base_currency,omitempty"`
	QuoteCurrency               *string    `json:"quote_currency,omitempty"`
	BookingAmount               float64    `json:"booking_amount"`
	ValueType                   *string    `json:"value_type,omitempty"`
	ActualValueBaseCurrency     *float64   `json:"actual_value_base_currency,omitempty"`
	SpotRate                    *float64   `json:"spot_rate,omitempty"`
	ForwardPoints               *float64   `json:"forward_points,omitempty"`
	BankMargin                  *float64   `json:"bank_margin,omitempty"`
	TotalRate                   float64    `json:"total_rate"`
	ValueQuoteCurrency          *float64   `json:"value_quote_currency,omitempty"`
	InterveningRateQuoteToLocal *float64   `json:"intervening_rate_quote_to_local,omitempty"`
	ValueLocalCurrency          *float64   `json:"value_local_currency,omitempty"`
	TransactionTimestamp        *time.Time `json:"transaction_timestamp,omitempty"`
	Status                      *string    `json:"status,omitempty"`
	ProcessingStatus            *string    `json:"processing_status,omitempty"`
}

type ForwardRollover struct {
	RolloverID           string `json:"rollover_id"`
	BookingID            string `json:"booking_id"`
	AmountRolledOver     float64   `json:"amount_rolled_over"`
	RolloverDate         time.Time `json:"rollover_date"`
	OriginalMaturityDate time.Time `json:"original_maturity_date"`
	NewMaturityDate      time.Time `json:"new_maturity_date"`
	RolloverCost         *float64  `json:"rollover_cost,omitempty"`
	EntityLevel0         string    `json:"entity_level_0"`
	EntityLevel1         *string   `json:"entity_level_1,omitempty"`
	EntityLevel2         *string   `json:"entity_level_2,omitempty"`
	EntityLevel3         *string   `json:"entity_level_3,omitempty"`
}

type ForwardCancellation struct {
	CancellationID     string `json:"cancellation_id"`
	BookingID          string `json:"booking_id"`
	AmountCancelled    float64   `json:"amount_cancelled"`
	CancellationDate   time.Time `json:"cancellation_date"`
	CancellationRate   float64   `json:"cancellation_rate"`
	RealizedGainLoss   *float64  `json:"realized_gain_loss,omitempty"`
	CancellationReason *string   `json:"cancellation_reason,omitempty"`
	EntityLevel0       string    `json:"entity_level_0"`
	EntityLevel1       *string   `json:"entity_level_1,omitempty"`
	EntityLevel2       *string   `json:"entity_level_2,omitempty"`
	EntityLevel3       *string   `json:"entity_level_3,omitempty"`
}

func fetchForwardBookings(db *sql.DB, buNames []string) ([]ForwardBooking, error) {
	rows, err := db.Query(`
		SELECT
			system_transaction_id, internal_reference_id, entity_level_0, entity_level_1, entity_level_2, entity_level_3,
			local_currency, order_type, transaction_type, counterparty, mode_of_delivery, delivery_period,
			add_date, settlement_date, maturity_date, delivery_date, currency_pair, base_currency, quote_currency,
			booking_amount, value_type, actual_value_base_currency, spot_rate, forward_points, bank_margin,
			total_rate, value_quote_currency, intervening_rate_quote_to_local, value_local_currency,
			transaction_timestamp, status, processing_status
		FROM forward_bookings
		WHERE entity_level_0 = ANY($1)
	`, pq.Array(buNames))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var list []ForwardBooking
	for rows.Next() {
		var fb ForwardBooking
		var internalRef sql.NullString
		err := rows.Scan(
			&fb.SystemTransactionID, &internalRef, &fb.EntityLevel0, &fb.EntityLevel1, &fb.EntityLevel2, &fb.EntityLevel3,
			&fb.LocalCurrency, &fb.OrderType, &fb.TransactionType, &fb.Counterparty, &fb.ModeOfDelivery, &fb.DeliveryPeriod,
			&fb.AddDate, &fb.SettlementDate, &fb.MaturityDate, &fb.DeliveryDate, &fb.CurrencyPair, &fb.BaseCurrency, &fb.QuoteCurrency,
			&fb.BookingAmount, &fb.ValueType, &fb.ActualValueBaseCurrency, &fb.SpotRate, &fb.ForwardPoints, &fb.BankMargin,
			&fb.TotalRate, &fb.ValueQuoteCurrency, &fb.InterveningRateQuoteToLocal, &fb.ValueLocalCurrency,
			&fb.TransactionTimestamp, &fb.Status, &fb.ProcessingStatus,
		)
		if err != nil {
			return nil, err
		}
		if internalRef.Valid {
			v := internalRef.String
			fb.InternalReferenceID = &v
		} else {
			fb.InternalReferenceID = nil
		}
		list = append(list, fb)
	}
	return list, nil
}

func fetchForwardRollovers(db *sql.DB, buNames []string) ([]ForwardRollover, error) {
	rows, err := db.Query(`
		SELECT
			r.rollover_id, r.booking_id, r.amount_rolled_over, r.rollover_date,
			r.original_maturity_date, r.new_maturity_date, r.rollover_cost,
			b.entity_level_0, b.entity_level_1, b.entity_level_2, b.entity_level_3
		FROM forward_rollovers r
		LEFT JOIN forward_bookings b ON r.booking_id = b.system_transaction_id
		WHERE b.entity_level_0 = ANY($1)
	`, pq.Array(buNames))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var list []ForwardRollover
	for rows.Next() {
		var fr ForwardRollover
		if err := rows.Scan(
			&fr.RolloverID, &fr.BookingID, &fr.AmountRolledOver, &fr.RolloverDate,
			&fr.OriginalMaturityDate, &fr.NewMaturityDate, &fr.RolloverCost,
			&fr.EntityLevel0, &fr.EntityLevel1, &fr.EntityLevel2, &fr.EntityLevel3,
		); err != nil {
			return nil, err
		}
		list = append(list, fr)
	}
	return list, nil
}

func fetchForwardCancellations(db *sql.DB, buNames []string) ([]ForwardCancellation, error) {
	rows, err := db.Query(`
		SELECT
			c.cancellation_id, c.booking_id, c.amount_cancelled, c.cancellation_date,
			c.cancellation_rate, c.realized_gain_loss, c.cancellation_reason,
			b.entity_level_0, b.entity_level_1, b.entity_level_2, b.entity_level_3
		FROM forward_cancellations c
		LEFT JOIN forward_bookings b ON c.booking_id = b.system_transaction_id
		WHERE b.entity_level_0 = ANY($1)
	`, pq.Array(buNames))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var list []ForwardCancellation
	for rows.Next() {
		var fc ForwardCancellation
		if err := rows.Scan(
			&fc.CancellationID, &fc.BookingID, &fc.AmountCancelled, &fc.CancellationDate,
			&fc.CancellationRate, &fc.RealizedGainLoss, &fc.CancellationReason,
			&fc.EntityLevel0, &fc.EntityLevel1, &fc.EntityLevel2, &fc.EntityLevel3,
		); err != nil {
			return nil, err
		}
		list = append(list, fc)
	}
	return list, nil
}

func GetLinkedSummaryByCategory(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			http.Error(w, `{"error":"user_id required"}`, http.StatusBadRequest)
			return
		}

		// Authorized business units from middleware
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			http.Error(w, `{"error":"No accessible business units found"}`, http.StatusForbidden)
			return
		}

		bookings, err := fetchForwardBookings(db, buNames)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "Failed to fetch forward bookings", "detail": err.Error()})
			return
		}

		rollovers, err := fetchForwardRollovers(db, buNames)
		if err != nil {
			http.Error(w, `{"error":"Failed to fetch forward rollovers"}`, http.StatusInternalServerError)
			return
		}

		cancellations, err := fetchForwardCancellations(db, buNames)
		if err != nil {
			http.Error(w, `{"error":"Failed to fetch forward cancellations"}`, http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"forward_bookings":      bookings,
			"forward_rollovers":     rollovers,
			"forward_cancellations": cancellations,
		})
	}
}
