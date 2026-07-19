package dashboardbuilder

// // Data source handler — serves live rows from investment tables to the
// // dashboard builder frontend so each widget can render with real data.
// //
// // Route (registered in dash.go with PreValidationMiddleware):
// //
// //   POST /dash/builder/data
// //
// // Request body:
// //
// //	{
// //	  "source":     "fdActivation",
// //	  "entity_ids": ["ent-001"],   // optional — auto-filled from session context
// //	  "limit":      500            // optional, default 500, max 2000
// //	}

// import (
// 	"bytes"
// 	"context"
// 	"encoding/json"
// 	"fmt"
// 	"io"
// 	"net/http"
// 	"regexp"
// 	"strings"
// 	"time"

// 	"CimplrCorpSaas/api"
// 	"CimplrCorpSaas/api/constants"
// 	"CimplrCorpSaas/internal/logger"

// 	"CimplrCorpSaas/api/cash/bankstatement"
// 	fxv91 "CimplrCorpSaas/api/fx/v91"
// 	"CimplrCorpSaas/api/investment/fdMaturityAndRollover"
// 	"CimplrCorpSaas/api/investment/fdReceipt"

// 	"github.com/jackc/pgx/v5"
// 	"github.com/jackc/pgx/v5/pgxpool"
// )

// type dataRequest struct {
// 	Source    string   `json:"source"`
// 	EntityIDs []string `json:"entity_ids"`
// 	Limit     int      `json:"limit"`
// }

// // GetDataSource — POST /dash/builder/data
// func GetDataSource(pool *pgxpool.Pool) http.HandlerFunc {
// 	return func(w http.ResponseWriter, r *http.Request) {
// 		if r.Method != http.MethodPost {
// 			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
// 			return
// 		}

// 		bodyBytes, err := io.ReadAll(r.Body)
// 		if err != nil {
// 			api.RespondWithError(w, http.StatusBadRequest, "failed to read body")
// 			return
// 		}

// 		var req dataRequest
// 		if err := json.Unmarshal(bodyBytes, &req); err != nil {
// 			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
// 			return
// 		}

// 		if len(req.EntityIDs) == 0 {
// 			req.EntityIDs = api.CtxEntityIDs(r.Context())
// 		}
// 		if req.Limit <= 0 || req.Limit > 2000 {
// 			req.Limit = 500
// 		}

// 		// Restore the body so that forwarded handlers can read it if needed
// 		r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
// 		ctx := r.Context()

// 		switch req.Source {
// 		case "fdBooking":
// 			fallthrough // Keep for backwards compatibility
// 		case "fdBookingAll":
// 			rows, err := queryFdBookingAll(ctx, pool, req.EntityIDs, req.Limit)
// 			if err != nil {
// 				logger.LogError("dashboard-builder GetDataSource [%s]: %v", req.Source, err)
// 				api.RespondWithError(w, http.StatusInternalServerError, "failed to fetch data")
// 				return
// 			}
// 			api.RespondWithPayload(w, true, "", rows)
// 			return
// 		case "fdBookingDetail":
// 			rows, err := queryFdBookingDetail(ctx, pool, req.EntityIDs, req.Limit)
// 			if err != nil {
// 				logger.LogError("dashboard-builder GetDataSource [%s]: %v", req.Source, err)
// 				api.RespondWithError(w, http.StatusInternalServerError, "failed to fetch data")
// 				return
// 			}
// 			api.RespondWithPayload(w, true, "", rows)
// 			return
// 		case "fdBookingApprovedActive":
// 			rows, err := queryFdBookingApprovedActive(ctx, pool, req.EntityIDs, req.Limit)
// 			if err != nil {
// 				logger.LogError("dashboard-builder GetDataSource [%s]: %v", req.Source, err)
// 				api.RespondWithError(w, http.StatusInternalServerError, "failed to fetch data")
// 				return
// 			}
// 			api.RespondWithPayload(w, true, "", rows)
// 			return
// 		case "fdConfirmation":
// 			fallthrough
// 		case "fdConfirmationAll":
// 			rows, err := queryFdConfirmationAll(ctx, pool, req.EntityIDs, req.Limit)
// 			if err != nil {
// 				logger.LogError("dashboard-builder GetDataSource [%s]: %v", req.Source, err)
// 				api.RespondWithError(w, http.StatusInternalServerError, "failed to fetch data")
// 				return
// 			}
// 			api.RespondWithPayload(w, true, "", rows)
// 			return
// 		case "fdConfirmationDetail":
// 			rows, err := queryFdConfirmationDetail(ctx, pool, req.EntityIDs, req.Limit)
// 			if err != nil {
// 				logger.LogError("dashboard-builder GetDataSource [%s]: %v", req.Source, err)
// 				api.RespondWithError(w, http.StatusInternalServerError, "failed to fetch data")
// 				return
// 			}
// 			api.RespondWithPayload(w, true, "", rows)
// 			return
// 		case "fdConfirmationConfirmed":
// 			rows, err := queryFdConfirmationConfirmed(ctx, pool, req.EntityIDs, req.Limit)
// 			if err != nil {
// 				logger.LogError("dashboard-builder GetDataSource [%s]: %v", req.Source, err)
// 				api.RespondWithError(w, http.StatusInternalServerError, "failed to fetch data")
// 				return
// 			}
// 			api.RespondWithPayload(w, true, "", rows)
// 			return
// 		case "fdActivation":
// 			fallthrough
// 		case "fdMasterAll":
// 			rows, err := queryFdMasterAll(ctx, pool, req.EntityIDs, req.Limit)
// 			if err != nil {
// 				logger.LogError("dashboard-builder GetDataSource [%s]: %v", req.Source, err)
// 				api.RespondWithError(w, http.StatusInternalServerError, "failed to fetch data")
// 				return
// 			}
// 			api.RespondWithPayload(w, true, "", rows)
// 			return
// 		case "fdMasterDetail":
// 			rows, err := queryFdMasterDetail(ctx, pool, req.EntityIDs, req.Limit)
// 			if err != nil {
// 				logger.LogError("dashboard-builder GetDataSource [%s]: %v", req.Source, err)
// 				api.RespondWithError(w, http.StatusInternalServerError, "failed to fetch data")
// 				return
// 			}
// 			api.RespondWithPayload(w, true, "", rows)
// 			return
// 		case "fdMasterActiveRange":
// 			rows, err := queryFdMasterActiveRange(ctx, pool, req.EntityIDs, req.Limit)
// 			if err != nil {
// 				logger.LogError("dashboard-builder GetDataSource [%s]: %v", req.Source, err)
// 				api.RespondWithError(w, http.StatusInternalServerError, "failed to fetch data")
// 				return
// 			}
// 			api.RespondWithPayload(w, true, "", rows)
// 			return
// 		case "fdCashflows":
// 			rows, err := queryFdCashflows(ctx, pool, req.EntityIDs, req.Limit)
// 			if err != nil {
// 				logger.LogError("dashboard-builder GetDataSource [%s]: %v", req.Source, err)
// 				api.RespondWithError(w, http.StatusInternalServerError, "failed to fetch data")
// 				return
// 			}
// 			api.RespondWithPayload(w, true, "", rows)
// 			return
// 		case "fdCashflowGroup":
// 			rows, err := queryFdCashflowGroup(ctx, pool, req.EntityIDs, req.Limit)
// 			if err != nil {
// 				logger.LogError("dashboard-builder GetDataSource [%s]: %v", req.Source, err)
// 				api.RespondWithError(w, http.StatusInternalServerError, "failed to fetch data")
// 				return
// 			}
// 			api.RespondWithPayload(w, true, "", rows)
// 			return
// 		case "fdJournals":
// 			rows, err := queryFdJournals(ctx, pool, req.EntityIDs, req.Limit)
// 			if err != nil {
// 				logger.LogError("dashboard-builder GetDataSource [%s]: %v", req.Source, err)
// 				api.RespondWithError(w, http.StatusInternalServerError, "failed to fetch data")
// 				return
// 			}
// 			api.RespondWithPayload(w, true, "", rows)
// 			return

// 		// ─── Forwarded Handlers ───────────────────────────────────────────────
// 		case "fdReceiptAll":
// 			fdReceipt.GetReceiptsWithAudit(pool)(w, r)
// 			return
// 		case "fdAccrualRunAll":
// 			rows, err := queryFdAccrualRunAll(ctx, pool, req.EntityIDs, req.Limit)
// 			if err != nil {
// 				logger.LogError("dashboard-builder GetDataSource [%s]: %v", req.Source, err)
// 				api.RespondWithError(w, http.StatusInternalServerError, "failed to fetch data")
// 				return
// 			}
// 			api.RespondWithPayload(w, true, "", rows)
// 			return
// 		case "fdAccrualLedgerAll":
// 			rows, err := queryFdAccrualLedgerAll(ctx, pool, req.EntityIDs, req.Limit)
// 			if err != nil {
// 				logger.LogError("dashboard-builder GetDataSource [%s]: %v", req.Source, err)
// 				api.RespondWithError(w, http.StatusInternalServerError, "failed to fetch data")
// 				return
// 			}
// 			api.RespondWithPayload(w, true, "", rows)
// 			return
// 		case "fdClosureInitiateAll":
// 			fdMaturityAndRollover.CimplrInitiateAll(pool)(w, r)
// 			return
// 		case "fdClosureConfirmAll":
// 			fdMaturityAndRollover.CimplrConfirmAll(pool)(w, r)
// 			return
// 		case "fdClosurePrematureAll":
// 			fdMaturityAndRollover.CimplrPrematureAll(pool)(w, r)
// 			return
// 		case "cashBankStatementAll":
// 			bankstatement.GetAllBankStatements(pool)(w, r)
// 			return
// 		case "fxExposureAll":
// 			fxv91.GetAllExposures(pool)(w, r)
// 			return

// 		default:
// 			// Fallback to dynamic table query based on source map or raw schema.table
// 			var schemaTable string
// 			if val, ok := sourceToTableMap[req.Source]; ok {
// 				schemaTable = val
// 			} else if matched, _ := regexp.MatchString(`^[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+$`, req.Source); matched {
// 				schemaTable = req.Source
// 			} else {
// 				api.RespondWithError(w, http.StatusBadRequest,
// 					fmt.Sprintf("unknown data source: %s", req.Source))
// 				return
// 			}
// 			rows, err := queryDynamicTable(ctx, pool, schemaTable, req.EntityIDs, req.Limit)
// 			if err != nil {
// 				logger.LogError("dashboard-builder GetDataSource [%s]: %v", req.Source, err)
// 				api.RespondWithError(w, http.StatusInternalServerError, "failed to fetch data")
// 				return
// 			}
// 			if rows == nil {
// 				rows = []map[string]any{}
// 			}
// 			api.RespondWithPayload(w, true, "", rows)
// 			return
// 		}
// 	}
// }

// // ─── helpers ──────────────────────────────────────────────────────────────────

// func scanRows(rows pgx.Rows) ([]map[string]any, error) {
// 	defer rows.Close()

// 	fds := rows.FieldDescriptions()
// 	var result []map[string]any

// 	for rows.Next() {
// 		vals, err := rows.Values()
// 		if err != nil {
// 			return nil, err
// 		}
// 		row := make(map[string]any, len(fds))
// 		for i, fd := range fds {
// 			row[string(fd.Name)] = normaliseValue(vals[i])
// 		}
// 		result = append(result, row)
// 	}
// 	return result, rows.Err()
// }

// func normaliseValue(v any) any {
// 	if v == nil {
// 		return nil
// 	}
// 	switch t := v.(type) {
// 	case time.Time:
// 		return t.Format("2006-01-02")
// 	case [16]byte: // UUID
// 		return fmt.Sprintf("%x-%x-%x-%x-%x", t[0:4], t[4:6], t[6:8], t[8:10], t[10:16])
// 	default:
// 		return v
// 	}
// }

// // entityFilter appends an entity_id filter when entity IDs are provided.
// func entityFilter(entityIDs []string, alias string, argOffset int) (string, []any) {
// 	if len(entityIDs) == 0 {
// 		return "", nil
// 	}
// 	return fmt.Sprintf("AND %s.entity_id = ANY($%d)", alias, argOffset), []any{entityIDs}
// }

// // ─── Dynamic Fallback ─────────────────────────────────────────────────────────

// var sourceToTableMap = map[string]string{
// 	// FD Workbench / Investment
// 	"fdAccrualRun":          "investment.fd_accrual_run",
// 	"fdAccrualLedger":       "investment.fd_accrual_ledger",
// 	"fdAccrualDetail":       "investment.fd_accrual_calculation",
// 	"fdAccrualFindings":     "investment.fd_accrual_exception",
// 	"fdAccrualException":    "investment.fd_accrual_exception",
// 	"fdAccrualExecutionLog": "investment.fd_accrual_log",
// 	"fdAccrualAudit":        "investment.fd_accrual_audit",
// 	"fdReceipt":             "investment.fd_interest_receipt",
// 	"fdTDS":                 "investment.fd_tds_receipt",
// 	"fdClosure":             "investment.fd_closure_request",
// 	"fdReconcile":           "investment.fd_reconcile_result",
// 	"fdException":           "investment.fd_exception",
// 	"invOnboard":            "investment.onboard_transaction",
// 	"invPortfolio":          "investment.portfolio_snapshot",
// 	"invProposal":           "investment.investment_proposal",
// 	"invRedemption":         "investment.redemption_request",

// 	// Cash
// 	"cashBankStatement": "cimplrcorpsaas.bank_statements",
// 	"cashBankTxns":      "cimplrcorpsaas.bank_statement_transactions",
// 	"cashProjection":    "cimplrcorpsaas.cashflow_projection_monthly",
// 	"cashProposal":      "cimplrcorpsaas.cashflow_proposal",
// 	"cashSweepConfig":   "cimplrcorpsaas.sweepconfiguration",
// 	"cashSweepInit":     "cimplrcorpsaas.sweep_initiation",

// 	// FX
// 	"fxExposure":        "public.exposure_headers",
// 	"fxBucketing":       "public.exposure_bucketing",
// 	"fxHedgingProposal": "public.hedging_proposal",
// 	"fxLinkage":         "public.exposure_hedge_links",
// 	"fxForwardBooking":  "public.forward_bookings",
// 	"fxConfirmation":    "public.forward_bookings",
// 	"fxPending":         "public.forward_bookings",
// 	"fxCancellation":    "public.forward_rollovers",
// 	"fxRoll":            "public.forward_rollovers",
// }

// func queryDynamicTable(ctx context.Context, pool *pgxpool.Pool, schemaTable string, entityIDs []string, limit int) ([]map[string]any, error) {
// 	parts := strings.Split(schemaTable, ".")
// 	if len(parts) != 2 {
// 		return nil, fmt.Errorf("invalid table format: %s", schemaTable)
// 	}
// 	schema, table := parts[0], parts[1]

// 	// Determine table metadata
// 	colCheckQ := `
// 		SELECT column_name
// 		FROM information_schema.columns
// 		WHERE table_schema = $1 AND table_name = $2`

// 	rows, err := pool.Query(ctx, colCheckQ, schema, table)
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer rows.Close()

// 	var entityCol string
// 	hasIsDeleted := false
// 	hasCreatedAt := false
// 	colCount := 0

// 	for rows.Next() {
// 		var c string
// 		if err := rows.Scan(&c); err != nil {
// 			return nil, err
// 		}
// 		colCount++
// 		cLow := strings.ToLower(c)
// 		if cLow == "is_deleted" {
// 			hasIsDeleted = true
// 		} else if cLow == "created_at" {
// 			hasCreatedAt = true
// 		} else if cLow == "entity_id" || cLow == "entityid" || cLow == "entity_level_0" || cLow == "entity" {
// 			if entityCol == "" || cLow == "entity_id" {
// 				entityCol = c
// 			}
// 		}
// 	}

// 	if colCount == 0 {
// 		return nil, fmt.Errorf("table %s not found or empty", schemaTable)
// 	}

// 	var whereClauses []string
// 	args := []any{limit}
// 	argIdx := 2

// 	if hasIsDeleted {
// 		whereClauses = append(whereClauses, "COALESCE(is_deleted, false) = false")
// 	}

// 	if entityCol != "" && len(entityIDs) > 0 {
// 		whereClauses = append(whereClauses, fmt.Sprintf("%s = ANY($%d)", entityCol, argIdx))
// 		args = append(args, entityIDs)
// 		argIdx++
// 	}

// 	whereSQL := ""
// 	if len(whereClauses) > 0 {
// 		whereSQL = "WHERE " + strings.Join(whereClauses, " AND ")
// 	}

// 	orderSQL := ""
// 	if hasCreatedAt {
// 		orderSQL = "ORDER BY created_at DESC"
// 	}

// 	q := fmt.Sprintf(`SELECT * FROM %s.%s %s %s LIMIT $1`, schema, table, whereSQL, orderSQL)

// 	r, err := pool.Query(ctx, q, args...)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return scanRows(r)
// }
// package dashboardbuilder

// import (
// 	"context"
// 	"fmt"

// 	"github.com/jackc/pgx/v5/pgxpool"
// )

// // ─── FD Booking ─────────────────────────────────────────────────────────────

// func queryFdBookingAll(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
// 	ef, efArgs := entityFilter(entityIDs, "br", 2)
// 	args := append([]any{limit}, efArgs...)

// 	q := fmt.Sprintf(`
// 		SELECT
// 			COALESCE(br.booking_id::text, '')   AS booking_id,
// 			COALESCE(br.entity_name, '')        AS entity_name,
// 			COALESCE(br.bank_name, '')          AS bank_name,
// 			COALESCE(br.booking_status, '')     AS booking_status,
// 			COALESCE(br.principal_amount, 0)    AS principal_amount,
// 			COALESCE(br.interest_rate, 0)       AS interest_rate,
// 			br.value_date,
// 			br.expected_maturity_date,
// 			COALESCE(br.processing_status, '')  AS processing_status,
// 			br.created_at
// 		FROM investment.fd_booking_request br
// 		WHERE COALESCE(br.is_deleted, false) = false %s
// 		ORDER BY br.created_at DESC
// 		LIMIT $1
// 	`, ef)

// 	r, err := pool.Query(ctx, q, args...)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return scanRows(r)
// }

// func queryFdBookingDetail(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
// 	ef, efArgs := entityFilter(entityIDs, "br", 2)
// 	args := append([]any{limit}, efArgs...)

// 	q := fmt.Sprintf(`
// 		SELECT
// 			COALESCE(br.booking_id::text, '')   AS booking_id,
// 			COALESCE(br.entity_name, '')        AS entity_name,
// 			COALESCE(br.bank_name, '')          AS bank_name,
// 			COALESCE(br.booking_status, '')     AS booking_status,
// 			COALESCE(br.principal_amount, 0)    AS principal_amount,
// 			COALESCE(br.interest_rate, 0)       AS interest_rate,
// 			COALESCE(br.tenor_type, '')         AS tenor_type,
// 			COALESCE(br.interest_type_code, '') AS interest_type_code,
// 			COALESCE(br.tenure_days, 0)         AS tenure_days,
// 			COALESCE(br.tenure_months, 0)       AS tenure_months,
// 			COALESCE(br.tenure_years, 0)        AS tenure_years,
// 			br.value_date,
// 			br.expected_maturity_date,
// 			br.created_at
// 		FROM investment.fd_booking_request br
// 		WHERE COALESCE(br.is_deleted, false) = false %s
// 		ORDER BY br.created_at DESC
// 		LIMIT $1
// 	`, ef)

// 	r, err := pool.Query(ctx, q, args...)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return scanRows(r)
// }

// func queryFdBookingApprovedActive(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
// 	ef, efArgs := entityFilter(entityIDs, "m", 2)
// 	args := append([]any{limit}, efArgs...)

// 	q := fmt.Sprintf(`
// 		SELECT
// 			COALESCE(m.booking_id::text, '')   AS booking_id,
// 			COALESCE(m.entity_name, '')        AS entity_name,
// 			COALESCE(m.bank_name, '')          AS bank_name,
// 			COALESCE(m.booking_status, '')     AS booking_status,
// 			COALESCE(m.principal_amount, 0)    AS principal_amount,
// 			COALESCE(m.interest_rate, 0)       AS interest_rate,
// 			m.value_date,
// 			m.expected_maturity_date,
// 			COALESCE(c.confirmation_status, '') AS confirmation_status,
// 			m.created_at
// 		FROM investment.fd_booking_request m
// 		LEFT JOIN investment.fd_confirmation c ON c.booking_id = m.booking_id
// 		WHERE COALESCE(m.is_deleted, false) = false
// 		  AND m.booking_status IN ('SENT_TO_BANK', 'APPROVED')
// 		  %s
// 		ORDER BY m.created_at DESC
// 		LIMIT $1
// 	`, ef)

// 	r, err := pool.Query(ctx, q, args...)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return scanRows(r)
// }

// // ─── FD Confirmation ────────────────────────────────────────────────────────

// func queryFdConfirmationAll(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
// 	ef, efArgs := entityFilter(entityIDs, "b", 2)
// 	args := append([]any{limit}, efArgs...)

// 	q := fmt.Sprintf(`
// 		SELECT
// 			COALESCE(c.confirmation_id::text, '') AS confirmation_id,
// 			COALESCE(c.booking_id::text, '')      AS booking_id,
// 			COALESCE(b.entity_name, '')           AS entity_name,
// 			COALESCE(b.bank_name, '')             AS bank_name,
// 			COALESCE(c.confirmation_status, '')   AS confirmation_status,
// 			COALESCE(c.actual_principal, 0)       AS confirmed_principal_amount,
// 			COALESCE(c.confirmed_rate, 0)         AS confirmed_interest_rate,
// 			c.actual_start_date                   AS confirmed_value_date,
// 			c.actual_maturity_date                AS confirmed_maturity_date,
// 			c.created_at
// 		FROM investment.fd_confirmation c
// 		LEFT JOIN investment.fd_booking_request b ON b.booking_id = c.booking_id
// 		WHERE COALESCE(c.is_deleted, false) = false %s
// 		ORDER BY c.created_at DESC
// 		LIMIT $1
// 	`, ef)

// 	r, err := pool.Query(ctx, q, args...)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return scanRows(r)
// }

// func queryFdConfirmationDetail(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
// 	ef, efArgs := entityFilter(entityIDs, "b", 2)
// 	args := append([]any{limit}, efArgs...)

// 	q := fmt.Sprintf(`
// 		SELECT
// 			COALESCE(c.confirmation_id::text, '') AS confirmation_id,
// 			COALESCE(c.booking_id::text, '')      AS booking_id,
// 			COALESCE(b.entity_name, '')           AS entity_name,
// 			COALESCE(b.bank_name, '')             AS bank_name,
// 			COALESCE(c.confirmation_status, '')   AS confirmation_status,
// 			COALESCE(c.actual_principal, 0)       AS confirmed_principal_amount,
// 			COALESCE(c.confirmed_rate, 0)         AS confirmed_interest_rate,
// 			COALESCE(c.tenor_type, '')            AS confirmed_tenor_type,
// 			COALESCE(c.tenor_days, 0)             AS confirmed_tenor_days,
// 			COALESCE(c.tenor_months, 0)           AS confirmed_tenor_months,
// 			COALESCE(c.tenor_years, 0)            AS confirmed_tenor_years,
// 			COALESCE(c.variance_flag, false)      AS variance_flag,
// 			c.actual_start_date                   AS confirmed_value_date,
// 			c.actual_maturity_date                AS confirmed_maturity_date,
// 			c.created_at
// 		FROM investment.fd_confirmation c
// 		LEFT JOIN investment.fd_booking_request b ON b.booking_id = c.booking_id
// 		WHERE COALESCE(c.is_deleted, false) = false %s
// 		ORDER BY c.created_at DESC
// 		LIMIT $1
// 	`, ef)

// 	r, err := pool.Query(ctx, q, args...)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return scanRows(r)
// }

// func queryFdConfirmationConfirmed(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
// 	ef, efArgs := entityFilter(entityIDs, "b", 2)
// 	args := append([]any{limit}, efArgs...)

// 	q := fmt.Sprintf(`
// 		SELECT
// 			COALESCE(c.confirmation_id::text, '') AS confirmation_id,
// 			COALESCE(c.booking_id::text, '')      AS booking_id,
// 			COALESCE(b.entity_name, '')           AS entity_name,
// 			COALESCE(b.bank_name, '')             AS bank_name,
// 			COALESCE(c.confirmation_status, '')   AS confirmation_status,
// 			COALESCE(c.actual_principal, 0)       AS confirmed_principal_amount,
// 			COALESCE(c.confirmed_rate, 0)         AS confirmed_interest_rate,
// 			c.actual_start_date                   AS confirmed_value_date,
// 			c.actual_maturity_date                AS confirmed_maturity_date,
// 			EXISTS (
// 				SELECT 1 FROM investment.fd_master fm
// 				WHERE fm.confirmation_id = c.confirmation_id
// 				  AND COALESCE(fm.is_deleted,false) = false
// 			) AS fd_activated,
// 			c.created_at
// 		FROM investment.fd_confirmation c
// 		LEFT JOIN investment.fd_booking_request b ON b.booking_id = c.booking_id
// 		WHERE COALESCE(c.is_deleted, false) = false
// 		  AND c.confirmation_status = 'CONFIRMED' %s
// 		ORDER BY c.created_at DESC
// 		LIMIT $1
// 	`, ef)

// 	r, err := pool.Query(ctx, q, args...)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return scanRows(r)
// }

// // ─── FD Master ──────────────────────────────────────────────────────────────

// func queryFdMasterAll(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
// 	ef, efArgs := entityFilter(entityIDs, "m", 2)
// 	args := append([]any{limit}, efArgs...)

// 	q := fmt.Sprintf(`
// 		SELECT
// 			COALESCE(m.fd_id::text, '')         AS fd_id,
// 			COALESCE(m.booking_id::text, '')    AS booking_id,
// 			COALESCE(m.confirmation_id::text,'') AS confirmation_id,
// 			COALESCE(m.entity_name, '')         AS entity_name,
// 			COALESCE(m.bank_name, '')           AS bank_name,
// 			COALESCE(m.fd_status, '')           AS fd_status,
// 			COALESCE(m.principal_amount, 0)     AS principal_amount,
// 			COALESCE(m.interest_rate, 0)        AS interest_rate,
// 			m.value_date,
// 			m.maturity_date,
// 			m.created_at
// 		FROM investment.fd_master m
// 		WHERE COALESCE(m.is_deleted, false) = false %s
// 		ORDER BY m.created_at DESC
// 		LIMIT $1
// 	`, ef)

// 	r, err := pool.Query(ctx, q, args...)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return scanRows(r)
// }

// func queryFdMasterDetail(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
// 	ef, efArgs := entityFilter(entityIDs, "m", 2)
// 	args := append([]any{limit}, efArgs...)

// 	q := fmt.Sprintf(`
// 		SELECT
// 			COALESCE(m.fd_id::text, '')         AS fd_id,
// 			COALESCE(m.booking_id::text, '')    AS booking_id,
// 			COALESCE(m.confirmation_id::text,'') AS confirmation_id,
// 			COALESCE(m.entity_name, '')         AS entity_name,
// 			COALESCE(m.bank_name, '')           AS bank_name,
// 			COALESCE(m.fd_status, '')           AS fd_status,
// 			COALESCE(m.principal_amount, 0)     AS principal_amount,
// 			COALESCE(m.interest_rate, 0)        AS interest_rate,
// 			COALESCE(m.interest_type_code, '')  AS interest_type_code,
// 			COALESCE(m.tenure_type, '')         AS tenor_type,
// 			m.value_date,
// 			m.maturity_date,
// 			m.created_at
// 		FROM investment.fd_master m
// 		WHERE COALESCE(m.is_deleted, false) = false %s
// 		ORDER BY m.created_at DESC
// 		LIMIT $1
// 	`, ef)

// 	r, err := pool.Query(ctx, q, args...)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return scanRows(r)
// }

// func queryFdMasterActiveRange(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
// 	ef, efArgs := entityFilter(entityIDs, "m", 2)
// 	args := append([]any{limit}, efArgs...)

// 	q := fmt.Sprintf(`
// 		SELECT
// 			COALESCE(m.fd_id::text, '')         AS fd_id,
// 			COALESCE(m.booking_id::text, '')    AS booking_id,
// 			COALESCE(m.confirmation_id::text,'') AS confirmation_id,
// 			COALESCE(m.entity_name, '')         AS entity_name,
// 			COALESCE(m.bank_name, '')           AS bank_name,
// 			COALESCE(m.fd_status, '')           AS fd_status,
// 			COALESCE(m.principal_amount, 0)     AS principal_amount,
// 			COALESCE(m.interest_rate, 0)        AS interest_rate,
// 			m.value_date,
// 			m.maturity_date,
// 			m.created_at
// 		FROM investment.fd_master m
// 		WHERE COALESCE(m.is_deleted, false) = false
// 		  AND m.fd_status = 'ACTIVE' %s
// 		ORDER BY m.created_at DESC
// 		LIMIT $1
// 	`, ef)

// 	r, err := pool.Query(ctx, q, args...)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return scanRows(r)
// }

// func queryFdCashflows(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
// 	ef, efArgs := entityFilter(entityIDs, "m", 2)
// 	args := append([]any{limit}, efArgs...)

// 	q := fmt.Sprintf(`
// 		SELECT
// 			COALESCE(c.cashflow_id::text, '')   AS cashflow_id,
// 			COALESCE(c.fd_id::text, '')         AS fd_id,
// 			COALESCE(m.entity_name, '')         AS entity_name,
// 			COALESCE(m.bank_name, '')           AS bank_name,
// 			COALESCE(c.event_type, '')          AS event_type,
// 			c.event_date,
// 			COALESCE(c.principal_amount, 0)     AS principal_amount,
// 			COALESCE(c.interest_accrued, 0)     AS interest_accrued,
// 			COALESCE(c.tds_amount, 0)           AS tds_amount,
// 			COALESCE(c.net_cash_flow, 0)        AS net_cash_flow,
// 			COALESCE(c.status, '')              AS status,
// 			c.created_at
// 		FROM investment.fd_cashflow c
// 		JOIN investment.fd_master m ON m.fd_id = c.fd_id
// 		WHERE COALESCE(c.is_deleted, false) = false %s
// 		ORDER BY c.created_at DESC
// 		LIMIT $1
// 	`, ef)

// 	r, err := pool.Query(ctx, q, args...)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return scanRows(r)
// }

// func queryFdCashflowGroup(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
// 	ef, efArgs := entityFilter(entityIDs, "m", 2)
// 	args := append([]any{limit}, efArgs...)

// 	q := fmt.Sprintf(`
// 		SELECT
// 			cf.fd_id,
// 			MAX(m.booking_id)                                   AS booking_id,
// 			MAX(m.confirmation_id)                              AS confirmation_id,
// 			MAX(m.entity_name)                                  AS entity_name,
// 			MAX(m.bank_name)                                    AS bank_name,
// 			MAX(m.principal_amount)                             AS principal_amount,
// 			MAX(m.interest_rate)                                AS interest_rate,
// 			MAX(m.value_date)                                   AS start_date,
// 			MAX(m.maturity_date)                                AS maturity_date,
// 			MAX(m.fd_status)                                    AS fd_status,
// 			COUNT(cf.cashflow_id)                               AS total_cashflow_events,
// 			COALESCE(SUM(cf.interest_accrued), 0)               AS total_interest_accrued,
// 			COALESCE(SUM(cf.tds_amount), 0)                     AS total_tds,
// 			COALESCE(SUM(cf.net_cash_flow), 0)                  AS total_net_cash_flow,
// 			MAX(cf.event_date)                                  AS last_event_date
// 		FROM investment.fd_cashflow cf
// 		JOIN investment.fd_master m ON m.fd_id = cf.fd_id
// 		WHERE COALESCE(cf.is_deleted, false) = false %s
// 		GROUP BY cf.fd_id
// 		ORDER BY MAX(cf.created_at) DESC
// 		LIMIT $1
// 	`, ef)

// 	r, err := pool.Query(ctx, q, args...)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return scanRows(r)
// }

// func queryFdJournals(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
// 	ef, efArgs := entityFilter(entityIDs, "je", 2)
// 	args := append([]any{limit}, efArgs...)

// 	q := fmt.Sprintf(`
// 		SELECT
// 			COALESCE(je.entry_id::text, '')     AS entry_id,
// 			COALESCE(je.fd_id::text, '')        AS fd_id,
// 			COALESCE(je.entity_name, '')        AS entity_name,
// 			COALESCE(je.description, '')        AS description,
// 			je.entry_date,
// 			COALESCE(je.entry_type, '')         AS entry_type,
// 			COALESCE(je.status, '')             AS status,
// 			COALESCE(je.total_debit, 0)         AS total_debit,
// 			COALESCE(je.total_credit, 0)        AS total_credit,
// 			je.created_at
// 		FROM investment.accounting_journal_entry je
// 		WHERE COALESCE(je.is_deleted, false) = false %s
// 		ORDER BY je.created_at DESC
// 		LIMIT $1
// 	`, ef)

// 	r, err := pool.Query(ctx, q, args...)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return scanRows(r)
// }

// // ─── FD Accrual ─────────────────────────────────────────────────────────────

// func queryFdAccrualRunAll(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
// 	ef, efArgs := entityFilter(entityIDs, "r", 2)
// 	args := append([]any{limit}, efArgs...)

// 	q := fmt.Sprintf(`
// 		SELECT
// 			COALESCE(r.run_id::text, '')        AS run_id,
// 			COALESCE(r.run_type, '')            AS run_type,
// 			COALESCE(r.run_mode, '')            AS run_mode,
// 			COALESCE(r.run_status, '')          AS run_status,
// 			COALESCE(r.entity_name, '')         AS entity_name,
// 			r.accrual_period_start,
// 			r.accrual_period_end,
// 			COALESCE(r.financial_period, '')    AS financial_period,
// 			COALESCE(r.fds_in_scope, 0)         AS fds_in_scope,
// 			COALESCE(r.fds_calculated, 0)       AS fds_calculated,
// 			COALESCE(r.fds_failed, 0)           AS fds_failed,
// 			COALESCE(r.total_interest_accrued, 0) AS total_interest_accrued,
// 			COALESCE(r.total_tds_deducted, 0)   AS total_tds_deducted,
// 			r.created_at
// 		FROM investment.fd_accrual_run r
// 		WHERE COALESCE(r.is_deleted, false) = false %s
// 		ORDER BY r.created_at DESC
// 		LIMIT $1
// 	`, ef)

// 	r, err := pool.Query(ctx, q, args...)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return scanRows(r)
// }

// func queryFdAccrualLedgerAll(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
// 	ef, efArgs := entityFilter(entityIDs, "al", 2)
// 	args := append([]any{limit}, efArgs...)

// 	q := fmt.Sprintf(`
// 		SELECT
// 			COALESCE(al.ledger_id::text, '')     AS ledger_id,
// 			COALESCE(al.run_id::text, '')        AS run_id,
// 			COALESCE(al.fd_id::text, '')         AS fd_id,
// 			COALESCE(al.entity_name, '')         AS entity_name,
// 			COALESCE(al.bank_name, '')           AS bank_name,
// 			COALESCE(al.ledger_row_status, '')   AS status,
// 			COALESCE(al.period_interest_accrued, 0) AS interest_accrued,
// 			COALESCE(al.tds_deducted_in_period, 0)  AS tds_deducted,
// 			COALESCE(al.net_interest_in_period, 0)  AS net_accrued,
// 			al.accrual_period_start              AS period_start,
// 			al.accrual_period_end                AS period_end,
// 			al.created_at
// 		FROM investment.fd_accrual_ledger al
// 		WHERE COALESCE(al.is_deleted, false) = false %s
// 		ORDER BY al.created_at DESC
// 		LIMIT $1
// 	`, ef)

// 	r, err := pool.Query(ctx, q, args...)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return scanRows(r)
// }
