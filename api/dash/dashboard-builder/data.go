package dashboardbuilder

// Data source handler — serves live rows from investment tables to the
// dashboard builder frontend so each widget can render with real data.
//
// Route (registered in dash.go with PreValidationMiddleware):
//
//   POST /dash/builder/data
//
// Request body:
//
//	{
//	  "source":     "fdActivation",          // dataSourceKey from frontend
//	  "entity_ids": ["ent-001","ent-002"],   // optional — auto-filled from session context
//	  "limit":      500                      // optional, default 500, max 2000
//	}
//
// Response:
//
//	{ "success": true, "data": [ { ...row... }, ... ] }
//
// Supported sources:
//   fdBooking        → investment.fd_booking_request (joined with entity/bank names)
//   fdConfirmation   → investment.fd_booking_confirmation
//   fdActivation     → investment.fd_master
//   fdInterestReceipt→ investment.fd_interest_receipt

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/internal/logger"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ─── request / response types ─────────────────────────────────────────────────

type dataRequest struct {
	Source    string   `json:"source"`
	EntityIDs []string `json:"entity_ids"`
	Limit     int      `json:"limit"`
}

// ─── handler ──────────────────────────────────────────────────────────────────

// GetDataSource returns rows for the requested data source key.
// POST /dash/builder/data
func GetDataSource(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		var req dataRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}

		// If no entity IDs in request body, fall back to session context.
		if len(req.EntityIDs) == 0 {
			req.EntityIDs = api.CtxEntityIDs(r.Context())
		}
		if req.Limit <= 0 || req.Limit > 2000 {
			req.Limit = 500
		}

		ctx := r.Context()
		var (
			rows []map[string]any
			err  error
		)

		switch req.Source {
		case "fdBooking":
			rows, err = queryFDBooking(ctx, pool, req.EntityIDs, req.Limit)
		case "fdConfirmation":
			rows, err = queryFDConfirmation(ctx, pool, req.EntityIDs, req.Limit)
		case "fdActivation":
			rows, err = queryFDActivation(ctx, pool, req.EntityIDs, req.Limit)
		case "fdInterestReceipt":
			rows, err = queryFDInterestReceipt(ctx, pool, req.EntityIDs, req.Limit)
		default:
			api.RespondWithError(w, http.StatusBadRequest,
				fmt.Sprintf("unknown data source: %s", req.Source))
			return
		}

		if err != nil {
			logger.LogError("dashboard-builder GetDataSource [%s]: %v", req.Source, err)
			api.RespondWithError(w, http.StatusInternalServerError, "failed to fetch data")
			return
		}

		if rows == nil {
			rows = []map[string]any{}
		}
		api.RespondWithPayload(w, true, "", rows)
	}
}

// ─── helpers ──────────────────────────────────────────────────────────────────

// scanRows converts pgx.Rows into a slice of string-keyed maps.
// Column names come directly from the SELECT list so they match dataSourceFields.ts.
func scanRows(rows pgx.Rows) ([]map[string]any, error) {
	defer rows.Close()

	fds := rows.FieldDescriptions()
	var result []map[string]any

	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, err
		}
		row := make(map[string]any, len(fds))
		for i, fd := range fds {
			row[string(fd.Name)] = normaliseValue(vals[i])
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

// normaliseValue converts pgx-specific types to plain Go types for JSON encoding.
func normaliseValue(v any) any {
	if v == nil {
		return nil
	}
	switch t := v.(type) {
	case time.Time:
		return t.Format("2006-01-02")
	case [16]byte: // UUID
		return fmt.Sprintf("%x-%x-%x-%x-%x", t[0:4], t[4:6], t[6:8], t[8:10], t[10:16])
	default:
		return v
	}
}

// entityFilter returns a WHERE clause fragment and args for entity_id filtering.
// If entityIDs is empty the filter is omitted (superuser / admin view).
func entityFilter(entityIDs []string, baseAlias string, argOffset int) (string, []any) {
	if len(entityIDs) == 0 {
		return "", nil
	}
	clause := fmt.Sprintf("AND %s.entity_id = ANY($%d)", baseAlias, argOffset)
	return clause, []any{entityIDs}
}

// ─── fdBooking ────────────────────────────────────────────────────────────────

func queryFDBooking(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	ef, efArgs := entityFilter(entityIDs, "br", 2)
	args := append([]any{limit}, efArgs...)

	q := fmt.Sprintf(`
		SELECT
			br.booking_id,
			COALESCE(e.entity_name, '')           AS entity_name,
			COALESCE(b.bank_name,   '')            AS bank_name,
			COALESCE(br.tenor_type, '')            AS tenor_type,
			COALESCE(br.interest_type_code, '')    AS interest_type_code,
			COALESCE(br.frequency_id, '')          AS frequency_id,
			COALESCE(br.day_count_code, '')        AS day_count_code,
			COALESCE(br.accrual_frequency_code,'') AS accrual_frequency_code,
			COALESCE(br.reset_type, '')            AS reset_type,
			COALESCE(br.booking_status, '')        AS booking_status,
			COALESCE(br.tds_plan_id::text, '')     AS tds_plan_id,
			COALESCE(br.payout_frequency_id, '')   AS payout_frequency_id,
			COALESCE(br.bank_config_id::text, '')  AS bank_config_id,
			COALESCE(br.value_type, '')            AS value_type,
			COALESCE(br.created_by, '')            AS created_by,
			COALESCE(br.auto_renewal, FALSE)       AS auto_renewal,
			COALESCE(br.principal_amount, 0)       AS principal_amount,
			COALESCE(br.interest_rate,    0)       AS interest_rate,
			COALESCE(br.tenure_days,      0)       AS tenure_days,
			COALESCE(br.tenure_months,    0)       AS tenure_months,
			COALESCE(br.tenure_years,     0)       AS tenure_years,
			br.value_date,
			br.expected_maturity_date
		FROM investment.fd_booking_request br
		LEFT JOIN master.entities e ON br.entity_id = e.entity_id
		LEFT JOIN master.banks    b ON br.bank_id    = b.bank_id
		WHERE TRUE %s
		ORDER BY br.created_at DESC
		LIMIT $1
	`, ef)

	rows, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(rows)
}

// ─── fdConfirmation ───────────────────────────────────────────────────────────

func queryFDConfirmation(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	ef, efArgs := entityFilter(entityIDs, "fc", 2)
	args := append([]any{limit}, efArgs...)

	q := fmt.Sprintf(`
		SELECT
			fc.confirmation_id,
			fc.booking_id,
			COALESCE(fc.tenor_type, '')                        AS tenor_type,
			COALESCE(fc.confirmed_interest_type_code, '')      AS confirmed_interest_type_code,
			COALESCE(fc.confirmation_mode, '')                 AS confirmation_mode,
			COALESCE(fc.confirmed_frequency_id, '')            AS confirmed_frequency_id,
			COALESCE(fc.payout_frequency_id, '')               AS payout_frequency_id,
			COALESCE(fc.accrual_frequency_code, '')            AS accrual_frequency_code,
			COALESCE(fc.reset_type, '')                        AS reset_type,
			COALESCE(fc.confirmation_status, '')               AS confirmation_status,
			COALESCE(fc.variance_action, '')                   AS variance_action,
			COALESCE(fc.bank_fd_ref_no, '')                    AS bank_fd_ref_no,
			COALESCE(fc.bank_reference_number, '')             AS bank_reference_number,
			COALESCE(fc.value_type, '')                        AS value_type,
			COALESCE(fc.created_by, '')                        AS created_by,
			COALESCE(fc.variance_flag, FALSE)                  AS variance_flag,
			COALESCE(fc.actual_principal, 0)                   AS actual_principal,
			COALESCE(fc.confirmed_rate,   0)                   AS confirmed_rate,
			COALESCE(fc.tenor_days,       0)                   AS tenor_days,
			COALESCE(fc.tenor_months,     0)                   AS tenor_months,
			COALESCE(fc.tenor_years,      0)                   AS tenor_years,
			fc.actual_start_date,
			fc.actual_maturity_date,
			fc.confirmation_received_date,
			fc.first_payout_date,
			fc.first_capitalization_date
		FROM investment.fd_booking_confirmation fc
		WHERE TRUE %s
		ORDER BY fc.created_at DESC
		LIMIT $1
	`, ef)

	rows, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(rows)
}

// ─── fdActivation ─────────────────────────────────────────────────────────────

func queryFDActivation(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	ef, efArgs := entityFilter(entityIDs, "m", 2)
	args := append([]any{limit}, efArgs...)

	q := fmt.Sprintf(`
		SELECT
			m.fd_id,
			m.confirmation_id,
			m.booking_id,
			COALESCE(e.entity_name, '')           AS entity_name,
			COALESCE(b.bank_name,   '')            AS bank_name,
			COALESCE(m.interest_type_code, '')     AS interest_type_code,
			COALESCE(m.tenure_type, '')            AS tenure_type,
			COALESCE(m.frequency_id, '')           AS frequency_id,
			COALESCE(m.payout_frequency_id, '')    AS payout_frequency_id,
			COALESCE(m.accrual_frequency_code,'')  AS accrual_frequency_code,
			COALESCE(m.reset_type, '')             AS reset_type,
			COALESCE(m.fd_status, '')              AS fd_status,
			COALESCE(m.bank_fd_ref_no, '')         AS bank_fd_ref_no,
			COALESCE(m.bank_reference_number, '')  AS bank_reference_number,
			COALESCE(m.tds_plan_id::text, '')      AS tds_plan_id,
			COALESCE(m.day_count_code, '')         AS day_count_code,
			COALESCE(m.bank_config_id::text, '')   AS bank_config_id,
			COALESCE(m.created_by, '')             AS created_by,
			COALESCE(m.auto_renewal, FALSE)        AS auto_renewal,
			COALESCE(m.principal_amount, 0)        AS principal_amount,
			COALESCE(m.interest_rate,    0)        AS interest_rate,
			COALESCE(m.tenure_days,      0)        AS tenure_days,
			COALESCE(m.tenure_months,    0)        AS tenure_months,
			COALESCE(m.tenure_years,     0)        AS tenure_years,
			m.start_date,
			m.maturity_date,
			m.first_payout_date,
			m.first_capitalization_date,
			m.receipt_date
		FROM investment.fd_master m
		LEFT JOIN master.entities e ON m.entity_id = e.entity_id
		LEFT JOIN master.banks    b ON m.bank_id    = b.bank_id
		WHERE TRUE %s
		ORDER BY m.created_at DESC
		LIMIT $1
	`, ef)

	rows, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(rows)
}

// ─── fdInterestReceipt ────────────────────────────────────────────────────────

func queryFDInterestReceipt(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	ef, efArgs := entityFilter(entityIDs, "ir", 2)
	args := append([]any{limit}, efArgs...)

	q := fmt.Sprintf(`
		SELECT
			ir.receipt_id,
			ir.fd_id,
			COALESCE(ir.fd_ref_no, '')              AS fd_ref_no,
			COALESCE(e.entity_name, '')             AS entity_name,
			COALESCE(b.bank_name,   '')             AS bank_name,
			COALESCE(ir.currency, 'INR')            AS currency,
			COALESCE(ir.bank_reference_no, '')      AS bank_reference_no,
			COALESCE(ir.ingestion_mode, '')         AS ingestion_mode,
			COALESCE(ir.receipt_status, '')         AS receipt_status,
			COALESCE(ir.reconcile_status, '')       AS reconcile_status,
			COALESCE(ir.is_active, FALSE)           AS is_active,
			COALESCE(ir.gross_interest_received, 0) AS gross_interest_received,
			COALESCE(ir.tds_amount_deducted,     0) AS tds_amount_deducted,
			COALESCE(ir.other_charges,           0) AS other_charges,
			COALESCE(ir.net_amount_received,     0) AS net_amount_received,
			ir.receipt_date,
			ir.period_start,
			ir.period_end
		FROM investment.fd_interest_receipt ir
		LEFT JOIN master.entities e ON ir.entity_id = e.entity_id
		LEFT JOIN master.banks    b ON ir.bank_id    = b.bank_id
		WHERE TRUE %s
		ORDER BY ir.receipt_date DESC
		LIMIT $1
	`, ef)

	rows, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(rows)
}

