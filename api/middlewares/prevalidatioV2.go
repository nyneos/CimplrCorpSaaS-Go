package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/internal/logger"
	"CimplrCorpSaas/internal/validation"

	"github.com/jackc/pgx/v5/pgxpool"
)

// GlobalIndependentMiddleware loads Currency, Bank, and Holiday Calendars
func GlobalIndependentMiddleware(db *pgxpool.Pool) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()

			var wg sync.WaitGroup
			var banks, currencies, holidays []map[string]string

			wg.Add(3)
			go func() {
				defer wg.Done()
				var err error
				if banks, err = loadApprovedBanks(ctx, db); err != nil {
					logger.LogError("Failed to load approved banks: %v", err)
				}
			}()
			go func() {
				defer wg.Done()
				var err error
				if currencies, err = loadApprovedCurrencies(ctx, db); err != nil {
					logger.LogError("Failed to load approved currencies: %v", err)
				}
			}()
			go func() {
				defer wg.Done()
				var err error
				if holidays, err = loadApprovedHolidayCalendars(ctx, db); err != nil {
					logger.LogError("Failed to load approved holiday calendars: %v", err)
				}
			}()
			wg.Wait()

			ctx = context.WithValue(ctx, "BankInfo", banks)
			ctx = context.WithValue(ctx, "ActiveCurrencies", currencies)
			ctx = context.WithValue(ctx, "ApprovedHolidayCalendars", holidays)

			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// GlobalDependentMiddleware loads Entity, Bank Accounts, GL Accounts
func GlobalDependentMiddleware(db *pgxpool.Pool) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()

			var wg sync.WaitGroup
			var bankAccounts, glAccounts []map[string]string

			wg.Add(2)
			go func() {
				defer wg.Done()
				var err error
				if bankAccounts, err = loadApprovedBankAccounts(ctx, db); err != nil {
					logger.LogError("Failed to load approved bank accounts: %v", err)
				}
			}()
			go func() {
				defer wg.Done()
				var err error
				if glAccounts, err = loadApprovedGLAccounts(ctx, db); err != nil {
					logger.LogError("Failed to load approved GL accounts: %v", err)
				}
			}()
			wg.Wait()

			ctx = context.WithValue(ctx, approvedBankAccountsKey, bankAccounts)
			ctx = context.WithValue(ctx, "ApprovedGLAccounts", glAccounts)
			// Note: Entity and Cash Entity contexts are injected by the Base PreValidateRequest

			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// CashMiddleware loads Counterparty, CashFlowCategory, PayableReceivable, CostProfitCenter
func CashMiddleware(db *pgxpool.Pool) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()

			var wg sync.WaitGroup
			var counterparties, categories, payReceivables, profitCenters []map[string]string

			wg.Add(4)
			go func() {
				defer wg.Done()
				var err error
				if counterparties, err = loadApprovedCounterparties(ctx, db); err != nil {
					logger.LogError("Failed to load approved counterparties: %v", err)
				}
			}()
			go func() {
				defer wg.Done()
				var err error
				if categories, err = loadApprovedCashFlowCategories(ctx, db); err != nil {
					logger.LogError("Failed to load approved cash flow categories: %v", err)
				}
			}()
			go func() {
				defer wg.Done()
				var err error
				if payReceivables, err = loadApprovedPayableReceivables(ctx, db); err != nil {
					logger.LogError("Failed to load approved payable/receivables: %v", err)
				}
			}()
			go func() {
				defer wg.Done()
				var err error
				if profitCenters, err = loadApprovedCostProfitCenters(ctx, db); err != nil {
					logger.LogError("Failed to load approved cost/profit centers: %v", err)
				}
			}()
			wg.Wait()

			ctx = context.WithValue(ctx, "ApprovedCounterparties", counterparties)
			ctx = context.WithValue(ctx, "CashFlowCategories", categories)
			ctx = context.WithValue(ctx, "ApprovedPayableReceivables", payReceivables)
			ctx = context.WithValue(ctx, "ApprovedCostProfitCenters", profitCenters)

			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// InvestmentMFMiddleware loads AMC, Scheme, DP, Demat, Folio
func InvestmentMFMiddleware(db *pgxpool.Pool) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()

			var wg sync.WaitGroup
			var amcs, schemes, dps, demats, folios []map[string]string

			wg.Add(5)
			go func() {
				defer wg.Done()
				var err error
				if amcs, err = loadApprovedAMCs(ctx, db); err != nil {
					logger.LogError("Failed to load approved AMCs: %v", err)
				}
			}()
			go func() {
				defer wg.Done()
				var err error
				if schemes, err = loadApprovedSchemes(ctx, db); err != nil {
					logger.LogError("Failed to load approved schemes: %v", err)
				}
			}()
			go func() {
				defer wg.Done()
				var err error
				if dps, err = loadApprovedDPs(ctx, db); err != nil {
					logger.LogError("Failed to load approved DPs: %v", err)
				}
			}()
			go func() {
				defer wg.Done()
				var err error
				if demats, err = loadApprovedDemats(ctx, db); err != nil {
					logger.LogError("Failed to load approved demats: %v", err)
				}
			}()
			go func() {
				defer wg.Done()
				var err error
				if folios, err = loadApprovedFolios(ctx, db); err != nil {
					logger.LogError("Failed to load approved folios: %v", err)
				}
			}()
			wg.Wait()

			ctx = context.WithValue(ctx, "ApprovedAMCs", amcs)
			ctx = context.WithValue(ctx, "ApprovedSchemes", schemes)
			ctx = context.WithValue(ctx, "ApprovedDPs", dps)
			ctx = context.WithValue(ctx, "ApprovedDemats", demats)
			ctx = context.WithValue(ctx, "ApprovedFolios", folios)

			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// InvestmentFDMiddleware loads Interest Type, Compounding Freq, Day Count, TDS, Penalty, Rate Cards, Bank Config
func InvestmentFDMiddleware(db *pgxpool.Pool) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()

			var wg sync.WaitGroup
			var (
				interestTypes, compoundingFreqs, dayCounts, tdsPlans, penaltyStructures, bankConfigs, bankRateCards []map[string]string
			)

			wg.Add(7)
			go func() {
				defer wg.Done()
				var err error
				if interestTypes, err = loadApprovedInterestTypes(ctx, db); err != nil {
					logger.LogError("Failed to load approved interest types: %v", err)
				}
			}()
			go func() {
				defer wg.Done()
				var err error
				if compoundingFreqs, err = loadApprovedCompoundingFrequencies(ctx, db); err != nil {
					logger.LogError("Failed to load approved compounding frequencies: %v", err)
				}
			}()
			go func() {
				defer wg.Done()
				var err error
				if dayCounts, err = loadApprovedDayCounts(ctx, db); err != nil {
					logger.LogError("Failed to load approved day counts: %v", err)
				}
			}()
			go func() {
				defer wg.Done()
				var err error
				if tdsPlans, err = loadApprovedTDSPlans(ctx, db); err != nil {
					logger.LogError("Failed to load approved TDS plans: %v", err)
				}
			}()
			go func() {
				defer wg.Done()
				var err error
				if penaltyStructures, err = loadApprovedPenaltyStructures(ctx, db); err != nil {
					logger.LogError("Failed to load approved penalty structures: %v", err)
				}
			}()
			go func() {
				defer wg.Done()
				var err error
				if bankConfigs, err = loadApprovedBankConfigs(ctx, db); err != nil {
					logger.LogError("Failed to load approved bank configs: %v", err)
				}
			}()
			go func() {
				defer wg.Done()
				var err error
				if bankRateCards, err = loadApprovedBankRateCards(ctx, db); err != nil {
					logger.LogError("Failed to load approved bank rate cards: %v", err)
				}
			}()
			wg.Wait()

			ctx = context.WithValue(ctx, "ApprovedInterestTypes", interestTypes)
			ctx = context.WithValue(ctx, "ApprovedCompoundingFrequencies", compoundingFreqs)
			ctx = context.WithValue(ctx, "ApprovedDayCounts", dayCounts)
			ctx = context.WithValue(ctx, "ApprovedTDSPlans", tdsPlans)
			ctx = context.WithValue(ctx, "ApprovedPenaltyStructures", penaltyStructures)
			ctx = context.WithValue(ctx, "ApprovedBankConfigs", bankConfigs)
			ctx = context.WithValue(ctx, "ApprovedBankRateCards", bankRateCards)

			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// ============================================================================
// NEW LOADERS FOR CASH/GL DEPENDENCIES
// ============================================================================

func loadApprovedGLAccounts(ctx context.Context, db *pgxpool.Pool) ([]map[string]string, error) {
	query := `
		WITH latest_approved AS (
			SELECT DISTINCT ON (gl_account_id) gl_account_id, processing_status
			FROM auditactionglaccount
			WHERE processing_status = 'APPROVED' AND actiontype IN ('CREATE','EDIT')
			ORDER BY gl_account_id, requested_at DESC
		)
		SELECT COALESCE(m.gl_account_id::text,''), COALESCE(m.gl_account_code,''), COALESCE(m.gl_account_name,''), COALESCE(m.gl_account_type,'')
		FROM masterglaccount m
		JOIN latest_approved l ON l.gl_account_id = m.gl_account_id
		WHERE UPPER(m.status) = 'ACTIVE' AND COALESCE(m.is_deleted, false) = false
		  AND NOT EXISTS (
		  	SELECT 1 FROM auditactionglaccount d
		  	WHERE d.gl_account_id = m.gl_account_id
		  	  AND d.processing_status = 'APPROVED'
		  	  AND d.actiontype = 'DELETE'
		  )
	`
	rows, err := db.Query(ctx, query)
	if err != nil {
		return []map[string]string{}, err
	}
	defer rows.Close()

	results := make([]map[string]string, 0)
	for rows.Next() {
		var id, code, name, ttype string
		if err := rows.Scan(&id, &code, &name, &ttype); err == nil {
			results = append(results, map[string]string{
				"gl_account_id":   id,
				"gl_account_code": code,
				"gl_account_name": name,
				"gl_account_type": ttype,
			})
		}
	}
	return results, nil
}

func loadApprovedCounterparties(ctx context.Context, db *pgxpool.Pool) ([]map[string]string, error) {
	query := `
		WITH latest_approved AS (
			SELECT DISTINCT ON (counterparty_id) counterparty_id, processing_status
			FROM auditactioncounterparty
			WHERE processing_status = 'APPROVED' AND actiontype IN ('CREATE','EDIT')
			ORDER BY counterparty_id, requested_at DESC
		)
		SELECT COALESCE(m.counterparty_id::text,''), COALESCE(m.counterparty_code,''), COALESCE(m.counterparty_name,''), COALESCE(m.counterparty_type,'')
		FROM mastercounterparty m
		JOIN latest_approved l ON l.counterparty_id = m.counterparty_id
		WHERE UPPER(m.status) = 'ACTIVE' AND COALESCE(m.is_deleted, false) = false
		  AND NOT EXISTS (
		  	SELECT 1 FROM auditactioncounterparty d
		  	WHERE d.counterparty_id = m.counterparty_id
		  	  AND d.processing_status = 'APPROVED'
		  	  AND d.actiontype = 'DELETE'
		  )
	`
	rows, err := db.Query(ctx, query)
	if err != nil {
		return []map[string]string{}, err
	}
	defer rows.Close()

	results := make([]map[string]string, 0)
	for rows.Next() {
		var id, code, name, ttype string
		if err := rows.Scan(&id, &code, &name, &ttype); err == nil {
			results = append(results, map[string]string{
				"counterparty_id":   id,
				"counterparty_code": code,
				"counterparty_name": name,
				"counterparty_type": ttype,
			})
		}
	}
	return results, nil
}

func loadApprovedPayableReceivables(ctx context.Context, db *pgxpool.Pool) ([]map[string]string, error) {
	query := `
		WITH latest_approved AS (
			SELECT DISTINCT ON (type_id) type_id, processing_status
			FROM auditactionpayablereceivable
			WHERE processing_status = 'APPROVED' AND actiontype IN ('CREATE','EDIT')
			ORDER BY type_id, requested_at DESC
		)
		SELECT COALESCE(m.type_id::text,''), COALESCE(m.type_code,''), COALESCE(m.type_name,'')
		FROM masterpayablereceivabletype m
		JOIN latest_approved l ON l.type_id = m.type_id
		WHERE UPPER(m.status) = 'ACTIVE' AND COALESCE(m.is_deleted, false) = false
		  AND NOT EXISTS (
		  	SELECT 1 FROM auditactionpayablereceivable d
		  	WHERE d.type_id = m.type_id
		  	  AND d.processing_status = 'APPROVED'
		  	  AND d.actiontype = 'DELETE'
		  )
	`
	rows, err := db.Query(ctx, query)
	if err != nil {
		return []map[string]string{}, err
	}
	defer rows.Close()

	results := make([]map[string]string, 0)
	for rows.Next() {
		var id, code, name string
		if err := rows.Scan(&id, &code, &name); err == nil {
			results = append(results, map[string]string{
				"type_id":   id,
				"type_code": code,
				"type_name": name,
			})
		}
	}
	return results, nil
}

func loadApprovedCostProfitCenters(ctx context.Context, db *pgxpool.Pool) ([]map[string]string, error) {
	query := `
		WITH latest_approved AS (
			SELECT DISTINCT ON (centre_id) centre_id, processing_status
			FROM auditactioncostprofitcenter
			WHERE processing_status = 'APPROVED' AND actiontype IN ('CREATE','EDIT')
			ORDER BY centre_id, requested_at DESC
		)
		SELECT COALESCE(m.centre_id::text,''), COALESCE(m.centre_code,''), COALESCE(m.centre_name,''), COALESCE(m.centre_type,'')
		FROM mastercostprofitcenter m
		JOIN latest_approved l ON l.centre_id = m.centre_id
		WHERE UPPER(m.status) = 'ACTIVE' AND COALESCE(m.is_deleted, false) = false
		  AND NOT EXISTS (
		  	SELECT 1 FROM auditactioncostprofitcenter d
		  	WHERE d.centre_id = m.centre_id
		  	  AND d.processing_status = 'APPROVED'
		  	  AND d.actiontype = 'DELETE'
		  )
	`
	rows, err := db.Query(ctx, query)
	if err != nil {
		return []map[string]string{}, err
	}
	defer rows.Close()

	results := make([]map[string]string, 0)
	for rows.Next() {
		var id, code, name, ttype string
		if err := rows.Scan(&id, &code, &name, &ttype); err == nil {
			results = append(results, map[string]string{
				"centre_id":   id,
				"centre_code": code,
				"centre_name": name,
				"centre_type": ttype,
			})
		}
	}
	return results, nil
}

func loadApprovedTDSPlans(ctx context.Context, db *pgxpool.Pool) ([]map[string]string, error) {
	query := `
		WITH latest_approved AS (
			SELECT DISTINCT ON (tds_plan_id) 
				tds_plan_id, 
				processing_status
			FROM investment.fd_audit_tds_plan
			WHERE processing_status = 'APPROVED' AND action_type IN ('CREATE','EDIT')
			ORDER BY tds_plan_id, GREATEST(COALESCE(requested_at,'1970-01-01'::timestamp), COALESCE(checker_at,'1970-01-01'::timestamp)) DESC
		)
		SELECT 
			COALESCE(m.tds_plan_id,''), COALESCE(m.tds_plan_code,''), COALESCE(m.tds_plan_name,''), COALESCE(m.tds_rate::text,''), COALESCE(m.threshold_amount::text,'')
		FROM investment.fd_tds_plan_master m
		JOIN latest_approved l ON l.tds_plan_id = m.tds_plan_id
		WHERE m.is_active = true AND COALESCE(m.is_deleted, false) = false
		  AND NOT EXISTS (
		  	SELECT 1 FROM investment.fd_audit_tds_plan d
		  	WHERE d.tds_plan_id = m.tds_plan_id
		  	  AND d.processing_status = 'APPROVED'
		  	  AND d.action_type = 'DELETE'
		  )
	`
	rows, err := db.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []map[string]string
	for rows.Next() {
		var id, code, name, rate, th string
		if err := rows.Scan(&id, &code, &name, &rate, &th); err == nil {
			res = append(res, map[string]string{"tds_plan_id": id, "tds_plan_code": code, "tds_plan_name": name, "tds_rate": rate, "threshold_amount": th})
		}
	}
	return res, nil
}

func loadApprovedBankConfigs(ctx context.Context, db *pgxpool.Pool) ([]map[string]string, error) {
	query := `
		WITH latest_approved_delete AS (
			SELECT DISTINCT ON (config_id) config_id, action_type
			FROM investment.fd_audit_bank_config
			WHERE processing_status = 'APPROVED' AND action_type = 'DELETE'
			ORDER BY config_id, GREATEST(COALESCE(requested_at,'1970-01-01'::timestamp), COALESCE(checker_at,'1970-01-01'::timestamp)) DESC
		)
		SELECT COALESCE(m.config_id,''), COALESCE(m.bank_code,''), COALESCE(m.product_type,'')
		FROM investment.fd_bank_config_master m
		LEFT JOIN latest_approved_delete d ON d.config_id = m.config_id
		WHERE m.is_active = true
		  AND COALESCE(m.is_deleted, false) = false
		  AND d.config_id IS NULL
		  AND EXISTS (
		  	SELECT 1
		  	FROM investment.fd_audit_bank_config a
		  	WHERE a.config_id = m.config_id
		  	  AND a.processing_status = 'APPROVED'
		  	  AND a.action_type IN ('CREATE','EDIT')
		  )
	`
	rows, err := db.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []map[string]string
	for rows.Next() {
		var id, code, pt string
		if err := rows.Scan(&id, &code, &pt); err == nil {
			res = append(res, map[string]string{
				"config_id":      id,
				"bank_config_id": id,
				"bank_code":      code,
				"product_type":   pt,
			})
		}
	}
	return res, nil
}

func loadApprovedCompoundingFrequencies(ctx context.Context, db *pgxpool.Pool) ([]map[string]string, error) {
	query := `
		WITH latest_approved AS (
			SELECT DISTINCT ON (frequency_id) frequency_id, processing_status
			FROM investment.fd_audit_compounding_frequency
			WHERE processing_status = 'APPROVED' AND action_type IN ('CREATE','EDIT')
			ORDER BY frequency_id, GREATEST(COALESCE(requested_at,'1970-01-01'::timestamp), COALESCE(checker_at,'1970-01-01'::timestamp)) DESC
		)
		SELECT COALESCE(m.frequency_id,''), COALESCE(m.frequency_code,''), COALESCE(m.frequency_name,'')
		FROM investment.fd_compounding_frequency_master m
		JOIN latest_approved l ON l.frequency_id = m.frequency_id
		WHERE m.is_active = true AND COALESCE(m.is_deleted, false) = false
		  AND NOT EXISTS (
		  	SELECT 1 FROM investment.fd_audit_compounding_frequency d
		  	WHERE d.frequency_id = m.frequency_id
		  	  AND d.processing_status = 'APPROVED'
		  	  AND d.action_type = 'DELETE'
		  )
	`
	rows, err := db.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []map[string]string
	for rows.Next() {
		var id, code, name string
		if err := rows.Scan(&id, &code, &name); err == nil {
			res = append(res, map[string]string{"frequency_id": id, "frequency_code": code, "frequency_name": name})
		}
	}
	return res, nil
}

func loadApprovedDayCounts(ctx context.Context, db *pgxpool.Pool) ([]map[string]string, error) {
	query := `
		WITH latest_approved AS (
			SELECT DISTINCT ON (day_count_code) day_count_code, processing_status
			FROM investment.fd_audit_day_count_convention
			WHERE processing_status = 'APPROVED' AND action_type IN ('CREATE','EDIT')
			ORDER BY day_count_code, GREATEST(COALESCE(requested_at,'1970-01-01'::timestamp), COALESCE(checker_at,'1970-01-01'::timestamp)) DESC
		)
		SELECT COALESCE(m.day_count_code,''), COALESCE(m.day_count_name,''), COALESCE(m.convention_type,'')
		FROM investment.fd_day_count_convention_master m
		JOIN latest_approved l ON l.day_count_code = m.day_count_code
		WHERE m.is_active = true AND COALESCE(m.is_deleted, false) = false
		  AND NOT EXISTS (
		  	SELECT 1 FROM investment.fd_audit_day_count_convention d
		  	WHERE d.day_count_code = m.day_count_code
		  	  AND d.processing_status = 'APPROVED'
		  	  AND d.action_type = 'DELETE'
		  )
	`
	rows, err := db.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []map[string]string
	for rows.Next() {
		var code, name, ct string
		if err := rows.Scan(&code, &name, &ct); err == nil {
			res = append(res, map[string]string{"day_count_code": code, "day_count_name": name, "convention_type": ct})
		}
	}
	return res, nil
}

func loadApprovedBankRateCards(ctx context.Context, db *pgxpool.Pool) ([]map[string]string, error) {
	query := `
		WITH latest_approved AS (
			SELECT DISTINCT ON (rate_card_id) rate_card_id, processing_status
			FROM investment.fd_audit_bank_rate_card
			WHERE processing_status = 'APPROVED' AND action_type IN ('CREATE','EDIT')
			ORDER BY rate_card_id, GREATEST(COALESCE(requested_at,'1970-01-01'::timestamp), COALESCE(checker_at,'1970-01-01'::timestamp)) DESC
		)
		SELECT COALESCE(m.rate_card_id,''), COALESCE(m.bank_code,''), COALESCE(m.interest_rate::text,''), COALESCE(m.min_tenor_days::text,''), COALESCE(m.max_tenor_days::text,'')
		FROM investment.fd_bank_rate_card_master m
		JOIN latest_approved l ON l.rate_card_id = m.rate_card_id
		WHERE m.is_active = true AND COALESCE(m.is_deleted, false) = false
		  AND NOT EXISTS (
		  	SELECT 1 FROM investment.fd_audit_bank_rate_card d
		  	WHERE d.rate_card_id = m.rate_card_id
		  	  AND d.processing_status = 'APPROVED'
		  	  AND d.action_type = 'DELETE'
		  )
	`
	rows, err := db.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []map[string]string
	for rows.Next() {
		var id, code, rate, min, max string
		if err := rows.Scan(&id, &code, &rate, &min, &max); err == nil {
			res = append(res, map[string]string{"rate_card_id": id, "bank_code": code, "interest_rate": rate, "min_tenor_days": min, "max_tenor_days": max})
		}
	}
	return res, nil
}

func loadApprovedPenaltyStructures(ctx context.Context, db *pgxpool.Pool) ([]map[string]string, error) {
	query := `
		WITH latest_approved AS (
			SELECT DISTINCT ON (penalty_id) penalty_id, processing_status
			FROM investment.fd_audit_penalty_structure
			WHERE processing_status = 'APPROVED' AND action_type IN ('CREATE','EDIT')
			ORDER BY penalty_id, GREATEST(COALESCE(requested_at,'1970-01-01'::timestamp), COALESCE(checker_at,'1970-01-01'::timestamp)) DESC
		)
		SELECT COALESCE(m.penalty_id,''), COALESCE(m.bank_code,''), COALESCE(m.penalty_type,''), COALESCE(m.penalty_value::text,'')
		FROM investment.fd_penalty_structure_master m
		JOIN latest_approved l ON l.penalty_id = m.penalty_id
		WHERE m.is_active = true AND COALESCE(m.is_deleted, false) = false
		  AND NOT EXISTS (
		  	SELECT 1 FROM investment.fd_audit_penalty_structure d
		  	WHERE d.penalty_id = m.penalty_id
		  	  AND d.processing_status = 'APPROVED'
		  	  AND d.action_type = 'DELETE'
		  )
	`
	rows, err := db.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []map[string]string
	for rows.Next() {
		var id, code, ptype, pval string
		if err := rows.Scan(&id, &code, &ptype, &pval); err == nil {
			res = append(res, map[string]string{"penalty_id": id, "bank_code": code, "penalty_type": ptype, "penalty_value": pval})
		}
	}
	return res, nil
}

func loadApprovedInterestTypes(ctx context.Context, db *pgxpool.Pool) ([]map[string]string, error) {
	query := `
		WITH latest_approved AS (
			SELECT DISTINCT ON (interest_id) interest_id, processing_status
			FROM investment.fd_audit_interest_type
			WHERE processing_status = 'APPROVED' AND action_type IN ('CREATE','EDIT')
			ORDER BY interest_id, GREATEST(COALESCE(requested_at,'1970-01-01'::timestamp), COALESCE(checker_at,'1970-01-01'::timestamp)) DESC
		)
		SELECT COALESCE(m.interest_id,''), COALESCE(m.interest_type_code,''), COALESCE(m.interest_type_name,'')
		FROM investment.fd_interest_type_master m
		JOIN latest_approved l ON l.interest_id = m.interest_id
		WHERE m.is_active = true AND COALESCE(m.is_deleted, false) = false
		  AND NOT EXISTS (
		  	SELECT 1 FROM investment.fd_audit_interest_type d
		  	WHERE d.interest_id = m.interest_id
		  	  AND d.processing_status = 'APPROVED'
		  	  AND d.action_type = 'DELETE'
		  )
	`
	rows, err := db.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []map[string]string
	for rows.Next() {
		var id, code, name string
		if err := rows.Scan(&id, &code, &name); err == nil {
			res = append(res, map[string]string{"interest_id": id, "interest_type_code": code, "interest_type_name": name})
		}
	}
	return res, nil
}

func loadApprovedHolidayCalendars(ctx context.Context, db *pgxpool.Pool) ([]map[string]string, error) {
	query := `
		WITH latest_approved AS (
			SELECT DISTINCT ON (calendar_id) calendar_id, processing_status
			FROM investment.auditactioncalendar
			WHERE UPPER(processing_status) = 'APPROVED' AND UPPER(actiontype) IN ('CREATE','EDIT')
			ORDER BY calendar_id, requested_at DESC
		)
		SELECT COALESCE(m.calendar_id::text,''), COALESCE(m.calendar_code,''), COALESCE(m.calendar_name,''), COALESCE(m.weekend_pattern,'')
		FROM investment.mastercalendar m
		JOIN latest_approved l ON l.calendar_id = m.calendar_id
		WHERE UPPER(m.status) = 'ACTIVE'
		  AND COALESCE(m.is_deleted, false) = false
		  AND NOT EXISTS (
		  	SELECT 1 FROM investment.auditactioncalendar d
		  	WHERE d.calendar_id = m.calendar_id
		  	  AND UPPER(d.processing_status) = 'APPROVED'
		  	  AND UPPER(d.actiontype) = 'DELETE'
		  )
	`
	rows, err := db.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []map[string]string
	for rows.Next() {
		var id, code, name, wp string
		if err := rows.Scan(&id, &code, &name, &wp); err == nil {
			res = append(res, map[string]string{"calendar_id": id, "calendar_code": code, "calendar_name": name, "weekend_pattern": wp})
		}
	}
	return res, nil
}

// SessionMiddleware validates the user session and loads allowed entities.
func SessionMiddleware(db *pgxpool.Pool) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()

			r.Body = http.MaxBytesReader(w, r.Body, 10<<20) // 10 MB limit
			body, err := io.ReadAll(r.Body)
			if err != nil {
				if err.Error() == "http: request body too large" {
					api.RespondWithError(w, http.StatusRequestEntityTooLarge, "Request body too large (max 10MB)")
					return
				}
				api.RespondWithError(w, http.StatusBadRequest, "Failed to read request body")
				return
			}
			r.Body.Close()
			r.Body = io.NopCloser(bytes.NewBuffer(body))

			userID, err := validation.ExtractUserID(r)
			if err != nil {
				api.RespondWithError(w, http.StatusBadRequest, constants.ErrUserIDRequired)
				return
			}

			r.Body = io.NopCloser(bytes.NewBuffer(body))

			session := validation.ValidateSession(userID)
			if session == nil {
				api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
				return
			}

			ctx = context.WithValue(ctx, "session_info", session)
			ctx = context.WithValue(ctx, "session", session) // backward compat: api.GetSessionFromCtx reads "session"
			ctx = context.WithValue(ctx, "user_id", userID)

			validationResult, err := validation.PreValidateRequest(ctx, db, userID)
			if err != nil {
				le := strings.ToLower(err.Error())
				if err == http.ErrMissingFile || strings.Contains(le, "no business") || strings.Contains(le, "no entity") || strings.Contains(le, "no accessible") {
					w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
					json.NewEncoder(w).Encode(map[string]interface{}{
						constants.ValueSuccess: false,
						"error":                "No accessible business units found for this user",
						"code":                 "NO_ACCESS_ENTITIES",
						"help":                 "Contact your administrator to grant access to business units or set up entities for your account.",
					})
					return
				}
				api.RespondWithError(w, http.StatusUnauthorized, "Validation failed: "+err.Error())
				return
			}

			entityIDs, entityNames, err := resolveEntityHierarchyMulti(ctx, db, validationResult.RootEntityIDs)
			if err != nil {
				if err == http.ErrMissingFile {
					w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
					json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "error": constants.ErrNoAccessibleBusinessUnit})
					return
				}
				api.RespondWithError(w, http.StatusInternalServerError, "Failed to resolve entity hierarchy: "+err.Error())
				return
			}

			// Write entity lists under both the typed constants (read by api.GetEntityIDsFromCtx
			// and the entity-cash hierarchy handler) and the legacy plain-string key "entity_ids"
			// (read directly by all investment workbench handlers via ctx.Value("entity_ids")).
			ctx = context.WithValue(ctx, api.EntityIDsKey, entityIDs)
			ctx = context.WithValue(ctx, api.BusinessUnitsKey, entityNames)
			ctx = context.WithValue(ctx, "entity_ids", entityIDs)
			ctx = context.WithValue(ctx, "entity_names", entityNames)
			ctx = context.WithValue(ctx, "root_entity_id", validationResult.RootEntityID)
			ctx = context.WithValue(ctx, "root_entity_name", validationResult.RootEntityName)
			ctx = context.WithValue(ctx, "root_entity_ids", validationResult.RootEntityIDs)

			// --- Admin Override Logic ---
			if IsAdminOverrideEnabled() && IsAdminUser(userID) {
				allEntityIDs, allEntityNames, entErr := LoadAllEntities(ctx, db)
				if entErr == nil && len(allEntityIDs) > 0 {
					ctx = context.WithValue(ctx, api.EntityIDsKey, allEntityIDs)
					ctx = context.WithValue(ctx, api.BusinessUnitsKey, allEntityNames)
					ctx = context.WithValue(ctx, "entity_ids", allEntityIDs)
					ctx = context.WithValue(ctx, "entity_names", allEntityNames)
				}
				ctx = context.WithValue(ctx, "is_admin_override", true)
				ctx = context.WithValue(ctx, "admin_override_by", "user")
			} else if IsAdminOverrideEnabled() {
				roleMatched := false
				matchedRoles := []string{}
				if session.Role != "" && IsRoleAdminName(session.Role) {
					roleMatched = true
					matchedRoles = append(matchedRoles, session.Role)
				}
				if !roleMatched && session.RoleCode != "" && IsRoleAdminName(session.RoleCode) {
					roleMatched = true
					matchedRoles = append(matchedRoles, session.RoleCode)
				}
				if !roleMatched {
					isRoleAdmin, dbMatched, roleErr := IsUserInAdminRole(ctx, db, userID)
					if roleErr != nil {
						ctx = context.WithValue(ctx, "admin_override_load_errors", []string{"role_lookup: " + roleErr.Error()})
					}
					if isRoleAdmin {
						roleMatched = true
						matchedRoles = append(matchedRoles, dbMatched...)
					}
				}

				if roleMatched {
					allEntityIDs, allEntityNames, entErr := LoadAllEntities(ctx, db)
					if entErr == nil && len(allEntityIDs) > 0 {
						ctx = context.WithValue(ctx, api.EntityIDsKey, allEntityIDs)
						ctx = context.WithValue(ctx, api.BusinessUnitsKey, allEntityNames)
						ctx = context.WithValue(ctx, "entity_ids", allEntityIDs)
						ctx = context.WithValue(ctx, "entity_names", allEntityNames)
					}
					ctx = context.WithValue(ctx, "is_admin_override", true)
					ctx = context.WithValue(ctx, "admin_override_by", "role")
					ctx = context.WithValue(ctx, "admin_override_role", matchedRoles)
				}
			}

			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
