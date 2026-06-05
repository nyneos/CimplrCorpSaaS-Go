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
//	  "source":     "fdActivation",
//	  "entity_ids": ["ent-001"],   // optional — auto-filled from session context
//	  "limit":      500            // optional, default 500, max 2000
//	}

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

type dataRequest struct {
	Source    string   `json:"source"`
	EntityIDs []string `json:"entity_ids"`
	Limit     int      `json:"limit"`
}

// GetDataSource — POST /dash/builder/data
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

// entityFilter appends an entity_id filter when entity IDs are provided.
func entityFilter(entityIDs []string, alias string, argOffset int) (string, []any) {
	if len(entityIDs) == 0 {
		return "", nil
	}
	return fmt.Sprintf("AND %s.entity_id = ANY($%d)", alias, argOffset), []any{entityIDs}
}

// ─── fdBooking ────────────────────────────────────────────────────────────────
// entity_name and bank_name are columns on fd_booking_request itself.

func queryFDBooking(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	ef, efArgs := entityFilter(entityIDs, "br", 2)
	args := append([]any{limit}, efArgs...)

	q := fmt.Sprintf(`
		SELECT
			COALESCE(br.booking_id::text,         '')  AS booking_id,
			COALESCE(br.entity_name,              '')  AS entity_name,
			COALESCE(br.bank_name,                '')  AS bank_name,
			COALESCE(br.tenor_type,               '')  AS tenor_type,
			COALESCE(br.interest_type_code,       '')  AS interest_type_code,
			COALESCE(br.booking_status,           '')  AS booking_status,
			COALESCE(br.auto_renewal,          FALSE)  AS auto_renewal,
			COALESCE(br.principal_amount,           0) AS principal_amount,
			COALESCE(br.interest_rate,              0) AS interest_rate,
			COALESCE(br.tenure_days,                0) AS tenure_days,
			COALESCE(br.tenure_months,              0) AS tenure_months,
			COALESCE(br.tenure_years,               0) AS tenure_years,
			br.value_date,
			br.expected_maturity_date
		FROM investment.fd_booking_request br
		WHERE COALESCE(br.is_deleted, false) = false %s
		ORDER BY br.created_at DESC
		LIMIT $1
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

// ─── fdConfirmation ───────────────────────────────────────────────────────────
// Join with fd_booking_request to get entity_name and bank_name.

func queryFDConfirmation(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	ef, efArgs := entityFilter(entityIDs, "br", 2)
	args := append([]any{limit}, efArgs...)

	q := fmt.Sprintf(`
		SELECT
			COALESCE(fc.confirmation_id::text,             '')  AS confirmation_id,
			COALESCE(fc.booking_id::text,                  '')  AS booking_id,
			COALESCE(br.entity_name,                       '')  AS entity_name,
			COALESCE(br.bank_name,                         '')  AS bank_name,
			COALESCE(fc.tenor_type,                        '')  AS tenor_type,
			COALESCE(fc.confirmed_interest_type_code,      '')  AS confirmed_interest_type_code,
			COALESCE(fc.confirmation_status,               '')  AS confirmation_status,
			COALESCE(fc.variance_action,                   '')  AS variance_action,
			COALESCE(fc.bank_fd_ref_no,                    '')  AS bank_fd_ref_no,
			COALESCE(fc.variance_flag,                  FALSE)  AS variance_flag,
			COALESCE(fc.actual_principal,                   0)  AS actual_principal,
			COALESCE(fc.confirmed_rate,                     0)  AS confirmed_rate,
			COALESCE(fc.tenor_days,                         0)  AS tenor_days,
			COALESCE(fc.tenor_months,                       0)  AS tenor_months,
			COALESCE(fc.tenor_years,                        0)  AS tenor_years,
			fc.actual_start_date,
			fc.actual_maturity_date
		FROM investment.fd_confirmation fc
		LEFT JOIN investment.fd_booking_request br ON br.booking_id = fc.booking_id
		WHERE COALESCE(fc.is_deleted, false) = false %s
		ORDER BY fc.created_at DESC
		LIMIT $1
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

// ─── fdActivation ─────────────────────────────────────────────────────────────
// entity_name and bank_name are columns on fd_master itself.

func queryFDActivation(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	ef, efArgs := entityFilter(entityIDs, "m", 2)
	args := append([]any{limit}, efArgs...)

	q := fmt.Sprintf(`
		SELECT
			COALESCE(m.fd_id::text,           '')  AS fd_id,
			COALESCE(m.confirmation_id::text, '')  AS confirmation_id,
			COALESCE(m.booking_id::text,      '')  AS booking_id,
			COALESCE(m.entity_name,           '')  AS entity_name,
			COALESCE(m.bank_name,             '')  AS bank_name,
			COALESCE(m.interest_type_code,    '')  AS interest_type_code,
			COALESCE(m.tenure_type,           '')  AS tenure_type,
			COALESCE(m.fd_status,             '')  AS fd_status,
			COALESCE(m.bank_fd_ref_no,        '')  AS bank_fd_ref_no,
			COALESCE(m.bank_reference_number, '')  AS bank_reference_number,
			COALESCE(m.auto_renewal,       FALSE)  AS auto_renewal,
			COALESCE(m.principal_amount,        0) AS principal_amount,
			COALESCE(m.interest_rate,           0) AS interest_rate,
			COALESCE(m.tenure_days,             0) AS tenure_days,
			COALESCE(m.tenure_months,           0) AS tenure_months,
			COALESCE(m.tenure_years,            0) AS tenure_years,
			m.start_date,
			m.maturity_date
		FROM investment.fd_master m
		WHERE COALESCE(m.is_deleted, false) = false %s
		ORDER BY m.created_at DESC
		LIMIT $1
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}
