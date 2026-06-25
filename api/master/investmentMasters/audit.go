package allMaster

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ─── Request types ────────────────────────────────────────────────────────────

type interestTypeAuditReq struct {
	UserID     string `json:"user_id"`
	InterestID string `json:"interest_id"`
}

type bankConfigAuditReq struct {
	UserID   string `json:"user_id"`
	ConfigID string `json:"config_id"`
}

type compoundingFreqAuditReq struct {
	UserID      string `json:"user_id"`
	FrequencyID string `json:"frequency_id"`
}

type dayCountAuditReq struct {
	UserID       string `json:"user_id"`
	DayCountCode string `json:"day_count_code"`
}

type bankRateCardAuditReq struct {
	UserID     string `json:"user_id"`
	RateCardID string `json:"rate_card_id"`
}

type tdsPlanAuditReq struct {
	UserID    string `json:"user_id"`
	TdsPlanID string `json:"tds_plan_id"`
}

type penaltyStructureAuditReq struct {
	UserID    string `json:"user_id"`
	PenaltyID string `json:"penalty_id"`
}

// ─── Session validator ────────────────────────────────────────────────────────

func validateMasterAuditSession(ctx context.Context) bool {
	return api.GetSessionFromCtx(ctx) != nil
}

// ─── Shared helpers ───────────────────────────────────────────────────────────

func collectMasterPgxRows(rows pgx.Rows) ([]map[string]interface{}, error) {
	defer rows.Close()
	fields := rows.FieldDescriptions()
	out := make([]map[string]interface{}, 0)
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, err
		}
		row := make(map[string]interface{}, len(fields))
		for i, f := range fields {
			if vals[i] == nil {
				row[f.Name] = ""
			} else {
				row[f.Name] = vals[i]
			}
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func respondMasterAuditPayload(w http.ResponseWriter, payload interface{}) {
	w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
	json.NewEncoder(w).Encode(map[string]interface{}{ //nolint:errcheck
		"success": true,
		"data":    payload,
	})
}

// ─── GetInterestTypeAuditHistory ─────────────────────────────────────────────
// POST /master/interest-type/audit-history
// Body: { user_id, interest_id? }

func GetInterestTypeAuditHistory(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		var req interestTypeAuditReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.UserID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrUserIIsRequired)
			return
		}
		if !validateMasterAuditSession(r.Context()) {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		baseQ := `
			SELECT
				a.audit_id,
				a.interest_id,
				a.action_type,
				a.processing_status,
				COALESCE(a.reason,'') AS reason,
				COALESCE(a.requested_by,'') AS requested_by,
				COALESCE(a.requested_ip,'') AS requested_ip,
				COALESCE(TO_CHAR((a.requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'), '') AS requested_at,
				COALESCE(a.checker_by,'') AS checker_by,
				COALESCE(a.checker_ip,'') AS checker_ip,
				COALESCE(TO_CHAR((a.checker_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'), '') AS checker_at,
				COALESCE(a.checker_comment,'') AS checker_comment,
				COALESCE(m.interest_type_code,'') AS interest_type_code,
				COALESCE(m.interest_type_name,'') AS interest_type_name,
				COALESCE(m.calculation_method,'') AS calculation_method,
				COALESCE(m.min_tenor_days,0) AS min_tenor_days,
				COALESCE(m.max_tenor_days,0) AS max_tenor_days,
				COALESCE(m.is_default,false) AS is_default,
				COALESCE(m.description,'') AS description,
				COALESCE(m.is_active,false) AS is_active,
				COALESCE(m.is_deleted,false) AS is_deleted,
				COALESCE(a.old_interest_type_code,'') AS old_interest_type_code,
				COALESCE(a.old_interest_type_name,'') AS old_interest_type_name,
				COALESCE(a.old_calculation_method,'') AS old_calculation_method,
				COALESCE(a.old_min_tenor_days,0) AS old_min_tenor_days,
				COALESCE(a.old_max_tenor_days,0) AS old_max_tenor_days,
				COALESCE(a.old_is_default,false) AS old_is_default,
				COALESCE(a.old_description,'') AS old_description,
				COALESCE(a.old_is_active,false) AS old_is_active
			FROM investment.fd_audit_interest_type a
			LEFT JOIN investment.fd_interest_type_master m ON m.interest_id = a.interest_id`

		var (
			q    string
			args []interface{}
		)
		if strings.TrimSpace(req.InterestID) != "" {
			q = baseQ + `
			WHERE a.interest_id = $1
			ORDER BY GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp), COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC`
			args = append(args, req.InterestID)
		} else {
			q = baseQ + `
			ORDER BY GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp), COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC
			LIMIT 1000`
		}

		rows, err := pgxPool.Query(ctx, q, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		payload, err := collectMasterPgxRows(rows)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to read interest type audit history")
			return
		}
		api.LogInfo("InterestType AuditHistory: returned %d records", len(payload))
		respondMasterAuditPayload(w, payload)
	}
}

// ─── GetBankConfigAuditHistory ───────────────────────────────────────────────
// POST /master/bank-config/audit-history
// Body: { user_id, config_id? }

func GetBankConfigAuditHistory(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		var req bankConfigAuditReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.UserID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrUserIIsRequired)
			return
		}
		if !validateMasterAuditSession(r.Context()) {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		baseQ := `
			SELECT
				a.audit_id,
				a.config_id,
				a.action_type,
				a.processing_status,
				COALESCE(a.reason,'')          AS reason,
				COALESCE(a.requested_by,'')    AS requested_by,
				COALESCE(a.requested_ip,'')    AS requested_ip,
				COALESCE(TO_CHAR((a.requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'), '') AS requested_at,
				COALESCE(a.checker_by,'')      AS checker_by,
				COALESCE(a.checker_ip,'')      AS checker_ip,
				COALESCE(TO_CHAR((a.checker_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'), '') AS checker_at,
				COALESCE(a.checker_comment,'') AS checker_comment,
				COALESCE(m.bank_code,'')                             AS bank_code,
				COALESCE(mb.bank_name,'')                            AS bank_name,
				COALESCE(mb.bank_short_name,'')                      AS bank_short_name,
				COALESCE(m.product_type,'')                          AS product_type,
				m.minimum_amount,
				m.maximum_amount,
				COALESCE(m.day_count_code,'')                        AS day_count_code,
				COALESCE(m.capitalization_schedule_type,'')          AS capitalization_schedule_type,
				COALESCE(m.capitalization_date_adjustment,'')        AS capitalization_date_adjustment,
				COALESCE(m.accrual_start_convention,'')              AS accrual_start_convention,
				COALESCE(m.accrual_end_convention,'')                AS accrual_end_convention,
				COALESCE(m.period_boundary_definition,'')            AS period_boundary_definition,
				COALESCE(m.weekend_accrual,false)                    AS weekend_accrual,
				COALESCE(m.holiday_accrual,false)                    AS holiday_accrual,
				COALESCE(m.holiday_calendar_code,'')                 AS holiday_calendar_code,
				COALESCE(m.broken_period_method,'')                  AS broken_period_method,
				COALESCE(m.broken_period_location,'')                AS broken_period_location,
				COALESCE(m.interest_rounding_decimals,2)             AS interest_rounding_decimals,
				COALESCE(m.rounding_method,'')                       AS rounding_method,
				COALESCE(m.rounding_frequency,'')                    AS rounding_frequency,
				m.grace_period_days,
				COALESCE(m.grace_period_rate_type,'')                AS grace_period_rate_type,
				m.minimum_compounding_period_days,
				COALESCE(m.quarter_definition,'')                    AS quarter_definition,
				COALESCE(m.tds_deduction_timing,'')                  AS tds_deduction_timing,
				TO_CHAR(m.effective_from,'YYYY-MM-DD')               AS effective_from,
				TO_CHAR(m.effective_to,'YYYY-MM-DD')                 AS effective_to,
				COALESCE(m.config_notes,'')                          AS config_notes,
				COALESCE(m.is_active,false)                          AS is_active,
				COALESCE(m.is_deleted,false)                         AS is_deleted,
				COALESCE(a.old_bank_code,'')                             AS old_bank_code,
				COALESCE(a.old_product_type,'')                          AS old_product_type,
				a.old_minimum_amount,
				a.old_maximum_amount,
				COALESCE(a.old_day_count_code,'')                        AS old_day_count_code,
				COALESCE(a.old_capitalization_schedule_type,'')          AS old_capitalization_schedule_type,
				COALESCE(a.old_capitalization_date_adjustment,'')        AS old_capitalization_date_adjustment,
				COALESCE(a.old_accrual_start_convention,'')              AS old_accrual_start_convention,
				COALESCE(a.old_accrual_end_convention,'')                AS old_accrual_end_convention,
				COALESCE(a.old_period_boundary_definition,'')            AS old_period_boundary_definition,
				COALESCE(a.old_weekend_accrual,false)                    AS old_weekend_accrual,
				COALESCE(a.old_holiday_accrual,false)                    AS old_holiday_accrual,
				COALESCE(a.old_holiday_calendar_code,'')                 AS old_holiday_calendar_code,
				COALESCE(a.old_broken_period_method,'')                  AS old_broken_period_method,
				COALESCE(a.old_broken_period_location,'')                AS old_broken_period_location,
				COALESCE(a.old_interest_rounding_decimals,2)             AS old_interest_rounding_decimals,
				COALESCE(a.old_rounding_method,'')                       AS old_rounding_method,
				COALESCE(a.old_rounding_frequency,'')                    AS old_rounding_frequency,
				a.old_grace_period_days,
				COALESCE(a.old_grace_period_rate_type,'')                AS old_grace_period_rate_type,
				a.old_minimum_compounding_period_days,
				COALESCE(a.old_quarter_definition,'')                    AS old_quarter_definition,
				COALESCE(a.old_tds_deduction_timing,'')                  AS old_tds_deduction_timing,
				TO_CHAR(a.old_effective_from,'YYYY-MM-DD')               AS old_effective_from,
				TO_CHAR(a.old_effective_to,'YYYY-MM-DD')                 AS old_effective_to,
				COALESCE(a.old_config_notes,'')                          AS old_config_notes,
				COALESCE(a.old_is_active,false)                          AS old_is_active
			FROM investment.fd_audit_bank_config a
			LEFT JOIN investment.fd_bank_config_master m ON m.config_id = a.config_id
			LEFT JOIN masterbank mb ON mb.bank_id::text = m.bank_code`

		var (
			q    string
			args []interface{}
		)
		if strings.TrimSpace(req.ConfigID) != "" {
			q = baseQ + `
			WHERE a.config_id = $1
			ORDER BY GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp), COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC`
			args = append(args, req.ConfigID)
		} else {
			q = baseQ + `
			ORDER BY GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp), COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC
			LIMIT 1000`
		}

		rows, err := pgxPool.Query(ctx, q, args...)
		if err != nil {
			msg, status := getUserFriendlyBankConfigError(err, constants.ErrQueryFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		payload, err := collectMasterPgxRows(rows)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to read bank config audit history")
			return
		}
		api.LogInfo("BankConfig AuditHistory: returned %d records", len(payload))
		respondMasterAuditPayload(w, payload)
	}
}

// ─── GetCompoundingFrequencyAuditHistory ─────────────────────────────────────
// POST /master/compounding-frequency/audit-history
// Body: { user_id, frequency_id? }

func GetCompoundingFrequencyAuditHistory(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		var req compoundingFreqAuditReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.UserID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrUserIIsRequired)
			return
		}
		if !validateMasterAuditSession(r.Context()) {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		baseQ := `
			SELECT
				a.audit_id,
				a.frequency_id,
				a.action_type,
				a.processing_status,
				COALESCE(a.reason,'') AS reason,
				COALESCE(a.requested_by,'') AS requested_by,
				COALESCE(a.requested_ip,'') AS requested_ip,
				COALESCE(TO_CHAR((a.requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'), '') AS requested_at,
				COALESCE(a.checker_by,'') AS checker_by,
				COALESCE(a.checker_ip,'') AS checker_ip,
				COALESCE(TO_CHAR((a.checker_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'), '') AS checker_at,
				COALESCE(a.checker_comment,'') AS checker_comment,
				COALESCE(m.frequency_code,'') AS frequency_code,
				COALESCE(m.frequency_name,'') AS frequency_name,
				COALESCE(m.frequency_type,'') AS frequency_type,
				COALESCE(m.compounding_periods_per_year,0) AS compounding_periods_per_year,
				COALESCE(m.days_per_period,0) AS days_per_period,
				COALESCE(m.description,'') AS description,
				COALESCE(m.is_active,false) AS is_active,
				COALESCE(m.is_deleted,false) AS is_deleted,
				COALESCE(a.old_frequency_code,'') AS old_frequency_code,
				COALESCE(a.old_frequency_name,'') AS old_frequency_name,
				COALESCE(a.old_frequency_type,'') AS old_frequency_type,
				COALESCE(a.old_compounding_periods_per_year,0) AS old_compounding_periods_per_year,
				COALESCE(a.old_days_per_period,0) AS old_days_per_period,
				COALESCE(a.old_description,'') AS old_description,
				COALESCE(a.old_is_active,false) AS old_is_active
			FROM investment.fd_audit_compounding_frequency a
			LEFT JOIN investment.fd_compounding_frequency_master m ON m.frequency_id = a.frequency_id`

		var (
			q    string
			args []interface{}
		)
		if strings.TrimSpace(req.FrequencyID) != "" {
			q = baseQ + `
			WHERE a.frequency_id = $1
			ORDER BY GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp), COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC`
			args = append(args, req.FrequencyID)
		} else {
			q = baseQ + `
			ORDER BY GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp), COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC
			LIMIT 1000`
		}

		rows, err := pgxPool.Query(ctx, q, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		payload, err := collectMasterPgxRows(rows)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to read compounding frequency audit history")
			return
		}
		api.LogInfo("CompoundingFrequency AuditHistory: returned %d records", len(payload))
		respondMasterAuditPayload(w, payload)
	}
}

// ─── GetDayCountConventionAuditHistory ───────────────────────────────────────
// POST /master/day-count-convention/audit-history
// Body: { user_id, day_count_code? }

func GetDayCountConventionAuditHistory(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		var req dayCountAuditReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.UserID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrUserIIsRequired)
			return
		}
		if !validateMasterAuditSession(r.Context()) {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		baseQ := `
			SELECT
				a.audit_id,
				a.day_count_code,
				a.action_type,
				a.processing_status,
				COALESCE(a.reason,'') AS reason,
				COALESCE(a.requested_by,'') AS requested_by,
				COALESCE(a.requested_ip,'') AS requested_ip,
				COALESCE(TO_CHAR((a.requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'), '') AS requested_at,
				COALESCE(a.checker_by,'') AS checker_by,
				COALESCE(a.checker_ip,'') AS checker_ip,
				COALESCE(TO_CHAR((a.checker_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'), '') AS checker_at,
				COALESCE(a.checker_comment,'') AS checker_comment,
				COALESCE(m.day_count_name,'') AS day_count_name,
				COALESCE(m.convention_type,'') AS convention_type,
				COALESCE(m.description,'') AS description,
				COALESCE(m.formula_example,'') AS formula_example,
				COALESCE(m.is_active,false) AS is_active,
				COALESCE(m.is_deleted,false) AS is_deleted,
				COALESCE(a.old_day_count_name,'') AS old_day_count_name,
				COALESCE(a.old_convention_type,'') AS old_convention_type,
				COALESCE(a.old_description,'') AS old_description,
				COALESCE(a.old_formula_example,'') AS old_formula_example,
				COALESCE(a.old_is_active,false) AS old_is_active
			FROM investment.fd_audit_day_count_convention a
			LEFT JOIN investment.fd_day_count_convention_master m ON m.day_count_code = a.day_count_code`

		var (
			q    string
			args []interface{}
		)
		if strings.TrimSpace(req.DayCountCode) != "" {
			q = baseQ + `
			WHERE a.day_count_code = $1
			ORDER BY GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp), COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC`
			args = append(args, req.DayCountCode)
		} else {
			q = baseQ + `
			ORDER BY GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp), COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC
			LIMIT 1000`
		}

		rows, err := pgxPool.Query(ctx, q, args...)
		if err != nil {
			msg, status := getUserFriendlyDayCountError(err, constants.ErrQueryFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		payload, err := collectMasterPgxRows(rows)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to read day count convention audit history")
			return
		}
		api.LogInfo("DayCount AuditHistory: returned %d records", len(payload))
		respondMasterAuditPayload(w, payload)
	}
}

// ─── GetBankRateCardAuditHistory ─────────────────────────────────────────────
// POST /master/bank-rate-card/audit-history
// Body: { user_id, rate_card_id? }

func GetBankRateCardAuditHistory(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		var req bankRateCardAuditReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.UserID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrUserIIsRequired)
			return
		}
		if !validateMasterAuditSession(r.Context()) {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		baseQ := `
			SELECT
				a.audit_id,
				a.rate_card_id,
				a.action_type,
				a.processing_status,
				COALESCE(a.reason,'') AS reason,
				COALESCE(a.requested_by,'') AS requested_by,
				COALESCE(a.requested_ip,'') AS requested_ip,
				COALESCE(TO_CHAR((a.requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'), '') AS requested_at,
				COALESCE(a.checker_by,'') AS checker_by,
				COALESCE(a.checker_ip,'') AS checker_ip,
				COALESCE(TO_CHAR((a.checker_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'), '') AS checker_at,
				COALESCE(a.checker_comment,'') AS checker_comment,
				COALESCE(m.bank_code,'') AS bank_code,
				COALESCE(mb.bank_name,'') AS bank_name,
				COALESCE(mb.bank_short_name,'') AS bank_short_name,
				COALESCE(m.deposit_type,'') AS deposit_type,
				COALESCE(m.min_tenor_days,0) AS min_tenor_days,
				COALESCE(m.max_tenor_days,0) AS max_tenor_days,
				COALESCE(m.interest_rate,0) AS interest_rate,
				m.min_amount,
				m.max_amount,
				m.is_callable,
				COALESCE(m.premature_withdrawal_allowed,true) AS premature_withdrawal_allowed,
				m.penalty_percentage,
				TO_CHAR(m.effective_from,'YYYY-MM-DD') AS effective_from,
				TO_CHAR(m.effective_to,'YYYY-MM-DD') AS effective_to,
				COALESCE(m.rate_source,'') AS rate_source,
				COALESCE(m.special_offer,false) AS special_offer,
				COALESCE(m.offer_details,'') AS offer_details,
				COALESCE(m.is_active,false) AS is_active,
				COALESCE(m.is_deleted,false) AS is_deleted,
				COALESCE(a.old_bank_code,'') AS old_bank_code,
				COALESCE(a.old_deposit_type,'') AS old_deposit_type,
				COALESCE(a.old_min_tenor_days,0) AS old_min_tenor_days,
				COALESCE(a.old_max_tenor_days,0) AS old_max_tenor_days,
				COALESCE(a.old_interest_rate,0) AS old_interest_rate,
				a.old_min_amount,
				a.old_max_amount,
				a.old_is_callable,
				COALESCE(a.old_premature_withdrawal_allowed,true) AS old_premature_withdrawal_allowed,
				a.old_penalty_percentage,
				TO_CHAR(a.old_effective_from,'YYYY-MM-DD') AS old_effective_from,
				TO_CHAR(a.old_effective_to,'YYYY-MM-DD') AS old_effective_to,
				COALESCE(a.old_rate_source,'') AS old_rate_source,
				COALESCE(a.old_special_offer,false) AS old_special_offer,
				COALESCE(a.old_offer_details,'') AS old_offer_details,
				COALESCE(a.old_is_active,false) AS old_is_active
			FROM investment.fd_audit_bank_rate_card a
			LEFT JOIN investment.fd_bank_rate_card_master m ON m.rate_card_id = a.rate_card_id
			LEFT JOIN masterbank mb ON mb.bank_id::text = m.bank_code`

		var (
			q    string
			args []interface{}
		)
		if strings.TrimSpace(req.RateCardID) != "" {
			q = baseQ + `
			WHERE a.rate_card_id = $1
			ORDER BY GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp), COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC`
			args = append(args, req.RateCardID)
		} else {
			q = baseQ + `
			ORDER BY GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp), COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC
			LIMIT 1000`
		}

		rows, err := pgxPool.Query(ctx, q, args...)
		if err != nil {
			msg, status := getUserFriendlyRateCardError(err, constants.ErrQueryFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		payload, err := collectMasterPgxRows(rows)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to read bank rate card audit history")
			return
		}
		api.LogInfo("RateCard AuditHistory: returned %d records", len(payload))
		respondMasterAuditPayload(w, payload)
	}
}

// ─── GetTdsPlanAuditHistory ──────────────────────────────────────────────────
// POST /master/tds-plan/audit-history
// Body: { user_id, tds_plan_id? }

func GetTdsPlanAuditHistory(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		var req tdsPlanAuditReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.UserID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrUserIIsRequired)
			return
		}
		if !validateMasterAuditSession(r.Context()) {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		baseQ := `
			SELECT
				a.audit_id,
				a.tds_plan_id,
				a.action_type,
				a.processing_status,
				COALESCE(a.reason,'') AS reason,
				COALESCE(a.requested_by,'') AS requested_by,
				COALESCE(a.requested_ip,'') AS requested_ip,
				COALESCE(TO_CHAR((a.requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'), '') AS requested_at,
				COALESCE(a.checker_by,'') AS checker_by,
				COALESCE(a.checker_ip,'') AS checker_ip,
				COALESCE(TO_CHAR((a.checker_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'), '') AS checker_at,
				COALESCE(a.checker_comment,'') AS checker_comment,
				COALESCE(m.tds_plan_code,'') AS tds_plan_code,
				COALESCE(m.tds_plan_name,'') AS tds_plan_name,
				COALESCE(m.tds_section,'') AS tds_section,
				COALESCE(m.tds_rate,0) AS tds_rate,
				COALESCE(m.has_pan,false) AS has_pan,
				COALESCE(m.threshold_amount,0) AS threshold_amount,
				COALESCE(m.threshold_type,'') AS threshold_type,
				COALESCE(m.deduction_timing,'') AS deduction_timing,
				COALESCE(TO_CHAR(m.applicable_from,'YYYY-MM-DD'),'') AS applicable_from,
				COALESCE(TO_CHAR(m.applicable_to,'YYYY-MM-DD'),'') AS applicable_to,
				COALESCE(m.description,'') AS description,
				COALESCE(m.is_active,false) AS is_active,
				COALESCE(m.is_deleted,false) AS is_deleted,
				COALESCE(a.old_tds_plan_code,'') AS old_tds_plan_code,
				COALESCE(a.old_tds_rate,0) AS old_tds_rate,
				COALESCE(a.old_threshold_amount,0) AS old_threshold_amount,
				COALESCE(a.old_has_pan,false) AS old_has_pan,
				COALESCE(a.old_is_active,false) AS old_is_active
			FROM investment.fd_audit_tds_plan a
			LEFT JOIN investment.fd_tds_plan_master m ON m.tds_plan_id = a.tds_plan_id`

		var (
			q    string
			args []interface{}
		)
		if strings.TrimSpace(req.TdsPlanID) != "" {
			q = baseQ + `
			WHERE a.tds_plan_id = $1
			ORDER BY GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp), COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC`
			args = append(args, req.TdsPlanID)
		} else {
			q = baseQ + `
			ORDER BY GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp), COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC
			LIMIT 1000`
		}

		rows, err := pgxPool.Query(ctx, q, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		payload, err := collectMasterPgxRows(rows)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to read TDS plan audit history")
			return
		}
		api.LogInfo("TdsPlan AuditHistory: returned %d records", len(payload))
		respondMasterAuditPayload(w, payload)
	}
}

// ─── GetPenaltyStructureAuditHistory ─────────────────────────────────────────
// POST /master/penalty-structure/audit-history
// Body: { user_id, penalty_id? }
// Both single-record and list queries join masterbank for consistent bank_name/bank_short_name.

func GetPenaltyStructureAuditHistory(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		var req penaltyStructureAuditReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.UserID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrUserIIsRequired)
			return
		}
		if !validateMasterAuditSession(r.Context()) {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		baseQ := `
			SELECT
				a.audit_id,
				a.penalty_id,
				a.action_type,
				a.processing_status,
				COALESCE(a.reason,'') AS reason,
				COALESCE(a.requested_by,'') AS requested_by,
				COALESCE(a.requested_ip,'') AS requested_ip,
				COALESCE(TO_CHAR((a.requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'), '') AS requested_at,
				COALESCE(a.checker_by,'') AS checker_by,
				COALESCE(a.checker_ip,'') AS checker_ip,
				COALESCE(TO_CHAR((a.checker_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'), '') AS checker_at,
				COALESCE(a.checker_comment,'') AS checker_comment,
				COALESCE(m.bank_code,'') AS bank_code,
				COALESCE(mb.bank_name,'') AS bank_name,
				COALESCE(mb.bank_short_name,'') AS bank_short_name,
				m.min_amount_range,
				m.max_amount_range,
				COALESCE(m.min_tenor_days,0) AS min_tenor_days,
				COALESCE(m.max_tenor_days,0) AS max_tenor_days,
				m.min_held_days,
				m.max_held_days,
				COALESCE(m.penalty_type,'') AS penalty_type,
				COALESCE(m.penalty_value,0) AS penalty_value,
				COALESCE(m.calculation_method,'') AS calculation_method,
				m.no_interest_if_withdrawn_before,
				COALESCE(m.description,'') AS description,
				TO_CHAR(m.effective_from,'YYYY-MM-DD') AS effective_from,
				TO_CHAR(m.effective_to,'YYYY-MM-DD') AS effective_to,
				COALESCE(m.is_active,false) AS is_active,
				COALESCE(m.is_deleted,false) AS is_deleted,
				COALESCE(a.old_bank_code,'') AS old_bank_code,
				a.old_min_amount_range,
				a.old_max_amount_range,
				COALESCE(a.old_min_tenor_days,0) AS old_min_tenor_days,
				COALESCE(a.old_max_tenor_days,0) AS old_max_tenor_days,
				a.old_min_held_days,
				a.old_max_held_days,
				COALESCE(a.old_penalty_type,'') AS old_penalty_type,
				COALESCE(a.old_penalty_value,0) AS old_penalty_value,
				COALESCE(a.old_calculation_method,'') AS old_calculation_method,
				a.old_no_interest_if_withdrawn_before,
				COALESCE(a.old_description,'') AS old_description,
				TO_CHAR(a.old_effective_from,'YYYY-MM-DD') AS old_effective_from,
				TO_CHAR(a.old_effective_to,'YYYY-MM-DD') AS old_effective_to,
				COALESCE(a.old_is_active,false) AS old_is_active
			FROM investment.fd_audit_penalty_structure a
			LEFT JOIN investment.fd_penalty_structure_master m ON m.penalty_id = a.penalty_id
			LEFT JOIN masterbank mb ON mb.bank_id::text = m.bank_code`

		var (
			q    string
			args []interface{}
		)
		if strings.TrimSpace(req.PenaltyID) != "" {
			q = baseQ + `
			WHERE a.penalty_id = $1
			ORDER BY GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp), COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC`
			args = append(args, req.PenaltyID)
		} else {
			q = baseQ + `
			ORDER BY GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp), COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC
			LIMIT 1000`
		}

		rows, err := pgxPool.Query(ctx, q, args...)
		if err != nil {
			msg, status := getUserFriendlyPenaltyError(err, constants.ErrQueryFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		payload, err := collectMasterPgxRows(rows)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to read penalty structure audit history")
			return
		}
		api.LogInfo("PenaltyStructure AuditHistory: returned %d records", len(payload))
		respondMasterAuditPayload(w, payload)
	}
}

// ─── GetAMCAuditHistory ──────────────────────────────────────────────────────
// POST /master/amc/audit-history
// Body: { user_id, amc_id? }

type amcAuditReq struct {
	UserID string `json:"user_id"`
	AMCID  string `json:"amc_id"`
}

func GetAMCAuditHistory(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		var req amcAuditReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.UserID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrUserIIsRequired)
			return
		}
		if !validateMasterAuditSession(r.Context()) {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		baseQ := `
			SELECT
				a.action_id,
				a.amc_id,
				a.actiontype,
				a.processing_status,
				COALESCE(a.reason,'') AS reason,
				COALESCE(a.requested_by,'') AS requested_by,
				COALESCE(a.requested_ip,'') AS requested_ip,
				COALESCE(TO_CHAR((a.requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'), '') AS requested_at,
				COALESCE(a.checker_by,'') AS checker_by,
				COALESCE(a.checker_ip,'') AS checker_ip,
				COALESCE(TO_CHAR((a.checker_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'), '') AS checker_at,
				COALESCE(a.checker_comment,'') AS checker_comment,
				COALESCE(m.amc_name,'') AS amc_name,
				COALESCE(m.old_amc_name,'') AS old_amc_name,
				COALESCE(m.internal_amc_code,'') AS internal_amc_code,
				COALESCE(m.old_internal_amc_code,'') AS old_internal_amc_code,
				COALESCE(m.status,'') AS status,
				COALESCE(m.old_status,'') AS old_status,
				COALESCE(m.primary_contact_name,'') AS primary_contact_name,
				COALESCE(m.old_primary_contact_name,'') AS old_primary_contact_name,
				COALESCE(m.primary_contact_email,'') AS primary_contact_email,
				COALESCE(m.old_primary_contact_email,'') AS old_primary_contact_email,
				COALESCE(m.sebi_registration_no,'') AS sebi_registration_no,
				COALESCE(m.old_sebi_registration_no,'') AS old_sebi_registration_no,
				COALESCE(m.amc_beneficiary_name,'') AS amc_beneficiary_name,
				COALESCE(m.old_amc_beneficiary_name,'') AS old_amc_beneficiary_name,
				COALESCE(m.amc_bank_account_no,'') AS amc_bank_account_no,
				COALESCE(m.old_amc_bank_account_no,'') AS old_amc_bank_account_no,
				COALESCE(m.amc_bank_name,'') AS amc_bank_name,
				COALESCE(m.old_amc_bank_name,'') AS old_amc_bank_name,
				COALESCE(m.amc_bank_ifsc,'') AS amc_bank_ifsc,
				COALESCE(m.old_amc_bank_ifsc,'') AS old_amc_bank_ifsc,
				COALESCE(m.mfu_amc_code,'') AS mfu_amc_code,
				COALESCE(m.old_mfu_amc_code,'') AS old_mfu_amc_code,
				COALESCE(m.cams_amc_code,'') AS cams_amc_code,
				COALESCE(m.old_cams_amc_code,'') AS old_cams_amc_code,
				COALESCE(m.erp_vendor_code,'') AS erp_vendor_code,
				COALESCE(m.old_erp_vendor_code,'') AS old_erp_vendor_code,
				COALESCE(m.source,'') AS source,
				COALESCE(m.is_deleted,false) AS is_deleted
			FROM investment.auditactionamc a
			LEFT JOIN investment.masteramc m ON m.amc_id = a.amc_id`

		var (
			q    string
			args []interface{}
		)
		if strings.TrimSpace(req.AMCID) != "" {
			q = baseQ + `
			WHERE a.amc_id = $1
			ORDER BY GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp), COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC`
			args = append(args, req.AMCID)
		} else {
			q = baseQ + `
			ORDER BY GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp), COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC
			LIMIT 1000`
		}

		rows, err := pgxPool.Query(ctx, q, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		payload, err := collectMasterPgxRows(rows)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to read AMC audit history")
			return
		}
		api.LogInfo("AMC AuditHistory: returned %d records", len(payload))
		respondMasterAuditPayload(w, payload)
	}
}

// ─── GetSchemeAuditHistory ───────────────────────────────────────────────────
// POST /master/scheme/audit-history
// Body: { user_id, scheme_id? }

type schemeAuditHistReq struct {
	UserID   string `json:"user_id"`
	SchemeID string `json:"scheme_id"`
}

func GetSchemeAuditHistory(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		var req schemeAuditHistReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.UserID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrUserIIsRequired)
			return
		}
		if !validateMasterAuditSession(r.Context()) {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		baseQ := `
			SELECT
				a.action_id,
				a.scheme_id,
				a.actiontype,
				a.processing_status,
				COALESCE(a.reason,'') AS reason,
				COALESCE(a.requested_by,'') AS requested_by,
				COALESCE(a.requested_ip,'') AS requested_ip,
				COALESCE(TO_CHAR((a.requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'), '') AS requested_at,
				COALESCE(a.checker_by,'') AS checker_by,
				COALESCE(a.checker_ip,'') AS checker_ip,
				COALESCE(TO_CHAR((a.checker_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'), '') AS checker_at,
				COALESCE(a.checker_comment,'') AS checker_comment,
				COALESCE(m.scheme_name,'') AS scheme_name,
				COALESCE(m.old_scheme_name,'') AS old_scheme_name,
				COALESCE(m.isin,'') AS isin,
				COALESCE(m.old_isin,'') AS old_isin,
				COALESCE(m.amc_name,'') AS amc_name,
				COALESCE(m.old_amc_name,'') AS old_amc_name,
				COALESCE(m.internal_scheme_code,'') AS internal_scheme_code,
				COALESCE(m.old_internal_scheme_code,'') AS old_internal_scheme_code,
				COALESCE(m.internal_risk_rating,'') AS internal_risk_rating,
				COALESCE(m.old_internal_risk_rating,'') AS old_internal_risk_rating,
				COALESCE(m.erp_gl_account,'') AS erp_gl_account,
				COALESCE(m.old_erp_gl_account,'') AS old_erp_gl_account,
				COALESCE(m.amfi_scheme_code,'') AS amfi_scheme_code,
				COALESCE(m.old_amfi_scheme_code,'') AS old_amfi_scheme_code,
				COALESCE(m.status,'') AS status,
				COALESCE(m.old_status,'') AS old_status,
				COALESCE(m.method,'') AS method,
				COALESCE(m.old_method,'') AS old_method,
				COALESCE(m.source,'') AS source,
				COALESCE(m.is_deleted,false) AS is_deleted
			FROM investment.auditactionscheme a
			LEFT JOIN investment.masterscheme m ON m.scheme_id = a.scheme_id`

		var (
			q    string
			args []interface{}
		)
		if strings.TrimSpace(req.SchemeID) != "" {
			q = baseQ + `
			WHERE a.scheme_id = $1
			ORDER BY GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp), COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC`
			args = append(args, req.SchemeID)
		} else {
			q = baseQ + `
			ORDER BY GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp), COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC
			LIMIT 1000`
		}

		rows, err := pgxPool.Query(ctx, q, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		payload, err := collectMasterPgxRows(rows)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to read scheme audit history")
			return
		}
		api.LogInfo("Scheme AuditHistory: returned %d records", len(payload))
		respondMasterAuditPayload(w, payload)
	}
}

// ─── GetDPAuditHistory ───────────────────────────────────────────────────────
// POST /master/dp/audit-history
// Body: { user_id, dp_id? }

type dpAuditHistReq struct {
	UserID string `json:"user_id"`
	DPID   string `json:"dp_id"`
}

func GetDPAuditHistory(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		var req dpAuditHistReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.UserID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrUserIIsRequired)
			return
		}
		if !validateMasterAuditSession(r.Context()) {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		baseQ := `
			SELECT
				a.action_id,
				a.dp_id,
				a.actiontype,
				a.processing_status,
				COALESCE(a.reason,'') AS reason,
				COALESCE(a.requested_by,'') AS requested_by,
				COALESCE(a.requested_ip,'') AS requested_ip,
				COALESCE(TO_CHAR((a.requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'), '') AS requested_at,
				COALESCE(a.checker_by,'') AS checker_by,
				COALESCE(a.checker_ip,'') AS checker_ip,
				COALESCE(TO_CHAR((a.checker_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'), '') AS checker_at,
				COALESCE(a.checker_comment,'') AS checker_comment,
				COALESCE(m.dp_name,'') AS dp_name,
				COALESCE(m.dp_code,'') AS dp_code,
				COALESCE(m.depository,'') AS depository,
				COALESCE(m.status,'') AS status,
				COALESCE(m.source,'') AS source,
				COALESCE(m.is_deleted,false) AS is_deleted
			FROM investment.auditactiondp a
			LEFT JOIN investment.masterdepositoryparticipant m ON m.dp_id = a.dp_id`

		var (
			q    string
			args []interface{}
		)
		if strings.TrimSpace(req.DPID) != "" {
			q = baseQ + `
			WHERE a.dp_id = $1
			ORDER BY GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp), COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC`
			args = append(args, req.DPID)
		} else {
			q = baseQ + `
			ORDER BY GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp), COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC
			LIMIT 1000`
		}

		rows, err := pgxPool.Query(ctx, q, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		payload, err := collectMasterPgxRows(rows)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to read DP audit history")
			return
		}
		api.LogInfo("DP AuditHistory: returned %d records", len(payload))
		respondMasterAuditPayload(w, payload)
	}
}

// ─── GetFolioAuditHistory ────────────────────────────────────────────────────
// POST /master/folio/audit-history
// Body: { user_id, folio_id? }

type folioAuditHistReq struct {
	UserID  string `json:"user_id"`
	FolioID string `json:"folio_id"`
}

func GetFolioAuditHistory(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		var req folioAuditHistReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.UserID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrUserIIsRequired)
			return
		}
		if !validateMasterAuditSession(r.Context()) {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		baseQ := `
			SELECT
				a.action_id,
				a.folio_id,
				a.actiontype,
				a.processing_status,
				COALESCE(a.reason,'') AS reason,
				COALESCE(a.requested_by,'') AS requested_by,
				COALESCE(a.requested_ip,'') AS requested_ip,
				COALESCE(TO_CHAR((a.requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'), '') AS requested_at,
				COALESCE(a.checker_by,'') AS checker_by,
				COALESCE(a.checker_ip,'') AS checker_ip,
				COALESCE(TO_CHAR((a.checker_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'), '') AS checker_at,
				COALESCE(a.checker_comment,'') AS checker_comment,
				COALESCE(m.entity_name,'') AS entity_name,
				COALESCE(m.old_entity_name,'') AS old_entity_name,
				COALESCE(m.amc_name,'') AS amc_name,
				COALESCE(m.old_amc_name,'') AS old_amc_name,
				COALESCE(m.folio_number,'') AS folio_number,
				COALESCE(m.old_folio_number,'') AS old_folio_number,
				COALESCE(m.first_holder_name,'') AS first_holder_name,
				COALESCE(m.old_first_holder_name,'') AS old_first_holder_name,
				COALESCE(m.default_subscription_account,'') AS default_subscription_account,
				COALESCE(m.old_default_subscription_account,'') AS old_default_subscription_account,
				COALESCE(m.default_redemption_account,'') AS default_redemption_account,
				COALESCE(m.old_default_redemption_account,'') AS old_default_redemption_account,
				COALESCE(m.status,'') AS status,
				COALESCE(m.old_status,'') AS old_status,
				COALESCE(m.source,'') AS source,
				COALESCE(m.is_deleted,false) AS is_deleted
			FROM investment.auditactionfolio a
			LEFT JOIN investment.masterfolio m ON m.folio_id = a.folio_id`

		var (
			q    string
			args []interface{}
		)
		if strings.TrimSpace(req.FolioID) != "" {
			q = baseQ + `
			WHERE a.folio_id = $1
			ORDER BY GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp), COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC`
			args = append(args, req.FolioID)
		} else {
			q = baseQ + `
			ORDER BY GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp), COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC
			LIMIT 1000`
		}

		rows, err := pgxPool.Query(ctx, q, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		payload, err := collectMasterPgxRows(rows)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to read folio audit history")
			return
		}
		api.LogInfo("Folio AuditHistory: returned %d records", len(payload))
		respondMasterAuditPayload(w, payload)
	}
}

// ─── GetDematAuditHistory ────────────────────────────────────────────────────
// POST /master/demat/audit-history
// Body: { user_id, demat_id? }

type dematAuditHistReq struct {
	UserID  string `json:"user_id"`
	DematID string `json:"demat_id"`
}

func GetDematAuditHistory(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		var req dematAuditHistReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.UserID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrUserIIsRequired)
			return
		}
		if !validateMasterAuditSession(r.Context()) {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		baseQ := `
			SELECT
				a.action_id,
				a.demat_id,
				a.actiontype,
				a.processing_status,
				COALESCE(a.reason,'') AS reason,
				COALESCE(a.requested_by,'') AS requested_by,
				COALESCE(a.requested_ip,'') AS requested_ip,
				COALESCE(TO_CHAR((a.requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'), '') AS requested_at,
				COALESCE(a.checker_by,'') AS checker_by,
				COALESCE(a.checker_ip,'') AS checker_ip,
				COALESCE(TO_CHAR((a.checker_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'), '') AS checker_at,
				COALESCE(a.checker_comment,'') AS checker_comment,
				COALESCE(m.entity_name,'') AS entity_name,
				COALESCE(m.old_entity_name,'') AS old_entity_name,
				COALESCE(m.depository,'') AS depository,
				COALESCE(m.old_depository,'') AS old_depository,
				COALESCE(m.demat_account_number,'') AS demat_account_number,
				COALESCE(m.old_demat_account_number,'') AS old_demat_account_number,
				COALESCE(m.depository_participant,'') AS depository_participant,
				COALESCE(m.old_depository_participant,'') AS old_depository_participant,
				COALESCE(m.client_id,'') AS client_id,
				COALESCE(m.old_client_id,'') AS old_client_id,
				COALESCE(m.default_settlement_account,'') AS default_settlement_account,
				COALESCE(m.old_default_settlement_account,'') AS old_default_settlement_account,
				COALESCE(m.status,'') AS status,
				COALESCE(m.old_status,'') AS old_status,
				COALESCE(m.source,'') AS source,
				COALESCE(m.is_deleted,false) AS is_deleted
			FROM investment.auditactiondemat a
			LEFT JOIN investment.masterdemataccount m ON m.demat_id = a.demat_id`

		var (
			q    string
			args []interface{}
		)
		if strings.TrimSpace(req.DematID) != "" {
			q = baseQ + `
			WHERE a.demat_id = $1
			ORDER BY GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp), COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC`
			args = append(args, req.DematID)
		} else {
			q = baseQ + `
			ORDER BY GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp), COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC
			LIMIT 1000`
		}

		rows, err := pgxPool.Query(ctx, q, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		payload, err := collectMasterPgxRows(rows)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to read demat audit history")
			return
		}
		api.LogInfo("Demat AuditHistory: returned %d records", len(payload))
		respondMasterAuditPayload(w, payload)
	}
}

// ─── GetCalendarAuditHistory ─────────────────────────────────────────────────
// POST /master/calendar/audit-history
// Body: { user_id, calendar_id? }

type calendarAuditHistReq struct {
	UserID     string `json:"user_id"`
	CalendarID string `json:"calendar_id"`
}

func GetCalendarAuditHistory(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		var req calendarAuditHistReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.UserID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrUserIIsRequired)
			return
		}
		if !validateMasterAuditSession(r.Context()) {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		baseQ := `
			SELECT
				a.action_id,
				a.calendar_id,
				a.actiontype,
				a.processing_status,
				COALESCE(a.reason,'') AS reason,
				COALESCE(a.requested_by,'') AS requested_by,
				COALESCE(a.requested_ip,'') AS requested_ip,
				COALESCE(TO_CHAR((a.requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'), '') AS requested_at,
				COALESCE(a.checker_by,'') AS checker_by,
				COALESCE(a.checker_ip,'') AS checker_ip,
				COALESCE(TO_CHAR((a.checker_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'), '') AS checker_at,
				COALESCE(a.checker_comment,'') AS checker_comment,
				COALESCE(m.calendar_code,'') AS calendar_code,
				COALESCE(m.old_calendar_code,'') AS old_calendar_code,
				COALESCE(m.calendar_name,'') AS calendar_name,
				COALESCE(m.old_calendar_name,'') AS old_calendar_name,
				COALESCE(m.scope,'') AS scope,
				COALESCE(m.old_scope,'') AS old_scope,
				COALESCE(m.country,'') AS country,
				COALESCE(m.old_country,'') AS old_country,
				COALESCE(m.timezone,'') AS timezone,
				COALESCE(m.old_timezone,'') AS old_timezone,
				COALESCE(m.weekend_pattern,'') AS weekend_pattern,
				COALESCE(m.old_weekend_pattern,'') AS old_weekend_pattern,
				COALESCE(m.status,'') AS status,
				COALESCE(m.old_status,'') AS old_status,
				COALESCE(m.source,'') AS source,
				TO_CHAR(m.eff_from,'YYYY-MM-DD') AS eff_from,
				TO_CHAR(m.eff_to,'YYYY-MM-DD') AS eff_to,
				COALESCE(m.is_deleted,false) AS is_deleted
			FROM investment.auditactioncalendar a
			LEFT JOIN investment.mastercalendar m ON m.calendar_id = a.calendar_id`

		var (
			q    string
			args []interface{}
		)
		if strings.TrimSpace(req.CalendarID) != "" {
			q = baseQ + `
			WHERE a.calendar_id = $1
			ORDER BY GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp), COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC`
			args = append(args, req.CalendarID)
		} else {
			q = baseQ + `
			ORDER BY GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp), COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC
			LIMIT 1000`
		}

		rows, err := pgxPool.Query(ctx, q, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		payload, err := collectMasterPgxRows(rows)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to read calendar audit history")
			return
		}
		api.LogInfo("Calendar AuditHistory: returned %d records", len(payload))
		respondMasterAuditPayload(w, payload)
	}
}
