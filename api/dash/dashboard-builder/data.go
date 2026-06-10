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
	FDID      string   `json:"fd_id"`
	// ParentID filters child sources by their parent row (e.g. bank_statement_id, run_id, proposal_id).
	ParentID string `json:"parent_id"`
}

type dataSourceFn func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error)

// dataSources maps each source name to its query function, replacing the switch.
var dataSources = map[string]dataSourceFn{
	// ── FD Core ────────────────────────────────────────────────────────────────
	"fdBooking":              func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryFDBooking(ctx, pool, req.EntityIDs, req.Limit) },
	"fdConfirmation":         func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryFDConfirmation(ctx, pool, req.EntityIDs, req.Limit) },
	"fdActivation":           func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryFDActivation(ctx, pool, req.EntityIDs, req.Limit) },
	"fdCashflowGroup":        func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryFDCashflowGroup(ctx, pool, req.EntityIDs, req.Limit) },
	"fdCashflows":            func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryFDCashflows(ctx, pool, req.EntityIDs, req.Limit, req.FDID) },
	"fdClosureInitiateAll":   func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryFDClosureInitiateAll(ctx, pool, req.EntityIDs, req.Limit) },
	"fdClosureConfirmAll":    func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryFDClosureConfirmAll(ctx, pool, req.EntityIDs, req.Limit) },
	"fdClosurePrematureAll":  func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryFDClosurePrematureAll(ctx, pool, req.EntityIDs, req.Limit) },
	// ── Maturity & Receipt ─────────────────────────────────────────────────────
	"fdMaturitySummary":   func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryFDMaturitySummary(ctx, pool, req.EntityIDs, req.Limit) },
	"fdTdsRegister":       func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryFDTDSRegister(ctx, pool, req.EntityIDs, req.Limit) },
	"fdReceiptAll":        func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryFDReceiptAll(ctx, pool, req.EntityIDs, req.Limit) },
	"fdReconcileResults":  func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryFDReconcileResults(ctx, pool, req.EntityIDs, req.Limit) },
	"fdExceptions":        func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryFDExceptions(ctx, pool, req.EntityIDs, req.Limit) },
	// ── Accrual ────────────────────────────────────────────────────────────────
	"fdAccrualRunAll":               func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryFDAccrualRunAll(ctx, pool, req.EntityIDs, req.Limit) },
	"fdAccrualLedger":               func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryFDAccrualLedger(ctx, pool, req.EntityIDs, req.Limit, req.ParentID) },
	"fdAccrualDetail":               func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryFDAccrualDetail(ctx, pool, req.EntityIDs, req.Limit) },
	"fdAccrualFindings":             func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryFDAccrualFindings(ctx, pool, req.EntityIDs, req.Limit) },
	"fdAccrualExecutionLog":         func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryFDAccrualExecutionLog(ctx, pool, req.EntityIDs, req.Limit) },
	"fdAccrualRunAudit":             func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryFDAccrualRunAudit(ctx, pool, req.EntityIDs, req.Limit) },
	"fdAccrualLedgerAudit":          func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryFDAccrualLedgerAudit(ctx, pool, req.EntityIDs, req.Limit) },
	"fdAccrualExceptions":           func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryFDAccrualExceptions(ctx, pool, req.EntityIDs, req.Limit) },
	"fdAccrualScheduleAll":          func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryFDAccrualScheduleAll(ctx, pool, req.EntityIDs, req.Limit) },
	"fdAccrualScheduleExecutionLog": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryFDAccrualScheduleExecutionLog(ctx, pool, req.EntityIDs, req.Limit) },
	// ── Portfolio & Proposal ───────────────────────────────────────────────────
	"investmentOnboardBatch":             func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryInvestmentOnboardBatch(ctx, pool, req.EntityIDs, req.Limit) },
	"investmentProposalMeta":             func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryInvestmentProposalMeta(ctx, pool, req.EntityIDs, req.Limit) },
	"investmentInitiationAll":            func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryInvestmentInitiationAll(ctx, pool, req.EntityIDs, req.Limit) },
	"investmentConfirmationAll":          func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryInvestmentConfirmationAll(ctx, pool, req.EntityIDs, req.Limit) },
	"investmentPortfolioGet":             func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryInvestmentPortfolioGet(ctx, pool, req.EntityIDs, req.Limit) },
	"investmentRedemptionInitiateAll":    func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryInvestmentRedemptionInitiateAll(ctx, pool, req.EntityIDs, req.Limit) },
	"investmentRedemptionConfirmAll":     func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryInvestmentRedemptionConfirmAll(ctx, pool, req.EntityIDs, req.Limit) },
	// ── Cash Module ────────────────────────────────────────────────────────────
	"cashBankStatements":            func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryCashBankStatements(ctx, pool, req.EntityIDs, req.Limit) },
	"cashBankStatementTransactions": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryCashBankStatementTransactions(ctx, pool, req.EntityIDs, req.Limit, req.ParentID) },
	"cashPayableReceivable":         func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryCashPayableReceivable(ctx, pool, req.EntityIDs, req.Limit) },
	"cashFundPlanSummary":           func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryCashFundPlanSummary(ctx, pool, req.EntityIDs, req.Limit) },
	"cashFundPlanDetails":           func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryCashFundPlanDetails(ctx, pool, req.EntityIDs, req.Limit, req.ParentID) },
	"cashSweepConfig":               func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryCashSweepConfig(ctx, pool, req.EntityIDs, req.Limit) },
	"cashSweepInitiation":           func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryCashSweepInitiation(ctx, pool, req.EntityIDs, req.Limit) },
	"cashSweepExecutionLogs":        func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryCashSweepExecutionLogs(ctx, pool, req.EntityIDs, req.Limit) },
	"cashSweepAllExecutionLogs":     func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryCashSweepAllExecutionLogs(ctx, pool, req.EntityIDs, req.Limit) },
	"cashSweepStatistics":           func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryCashSweepStatistics(ctx, pool, req.EntityIDs, req.Limit) },
	"cashProjectionList":            func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryCashProjectionList(ctx, pool, req.EntityIDs, req.Limit) },
	"cashProjectionDetail":          func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryCashProjectionDetail(ctx, pool, req.EntityIDs, req.Limit, req.ParentID) },
	"cashBankBalances":              func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryCashBankBalances(ctx, pool, req.EntityIDs, req.Limit) },
	"cashFundAvailability":          func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryCashFundAvailability(ctx, pool, req.EntityIDs, req.Limit) },
	"cashBankLimits":                func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryCashBankLimits(ctx, pool, req.EntityIDs, req.Limit) },
	"cashUtilizations":              func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryCashUtilizations(ctx, pool, req.EntityIDs, req.Limit) },
	// ── Audit sub-sources ──────────────────────────────────────────────────────
	"fdBookingAudit":                    func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryFDBookingAudit(ctx, pool, req.EntityIDs, req.Limit, req.ParentID) },
	"fdConfirmationAudit":               func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryFDConfirmationAudit(ctx, pool, req.EntityIDs, req.Limit, req.ParentID) },
	"fdActivationAudit":                 func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryFDActivationAudit(ctx, pool, req.EntityIDs, req.Limit, req.ParentID) },
	"fdCashflowAudit":                   func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryFDCashflowAudit(ctx, pool, req.EntityIDs, req.Limit, req.ParentID) },
	"fdClosureInitiateAudit":            func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryFDClosureInitiateAudit(ctx, pool, req.EntityIDs, req.Limit, req.ParentID) },
	"fdClosureConfirmAudit":             func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryFDClosureConfirmAudit(ctx, pool, req.EntityIDs, req.Limit, req.ParentID) },
	"fdTdsAudit":                        func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryFDTdsAudit(ctx, pool, req.EntityIDs, req.Limit, req.ParentID) },
	"fdReceiptAudit":                    func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryFDReceiptAudit(ctx, pool, req.EntityIDs, req.Limit, req.ParentID) },
	"fdExceptionAudit":                  func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryFDExceptionAudit(ctx, pool, req.EntityIDs, req.Limit, req.ParentID) },
	"cashBankStatementAudit":            func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryCashBankStatementAudit(ctx, pool, req.EntityIDs, req.Limit, req.ParentID) },
	"cashSweepConfigAudit":              func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryCashSweepConfigAudit(ctx, pool, req.EntityIDs, req.Limit, req.ParentID) },
	"cashSweepInitiationAudit":          func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryCashSweepInitiationAudit(ctx, pool, req.EntityIDs, req.Limit, req.ParentID) },
	"cashProjectionAudit":               func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryCashProjectionAudit(ctx, pool, req.EntityIDs, req.Limit, req.ParentID) },
	"cashFundPlanAudit":                 func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryCashFundPlanAudit(ctx, pool, req.EntityIDs, req.Limit, req.ParentID) },
	"investmentProposalAudit":           func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryInvestmentProposalAudit(ctx, pool, req.EntityIDs, req.Limit, req.ParentID) },
	"investmentInitiationAudit":         func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryInvestmentInitiationAudit(ctx, pool, req.EntityIDs, req.Limit, req.ParentID) },
	"investmentConfirmationAudit":       func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryInvestmentConfirmationAudit(ctx, pool, req.EntityIDs, req.Limit, req.ParentID) },
	"investmentRedemptionAudit":         func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryInvestmentRedemptionAudit(ctx, pool, req.EntityIDs, req.Limit, req.ParentID) },
	"investmentRedemptionConfirmAudit":  func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryInvestmentRedemptionConfirmAudit(ctx, pool, req.EntityIDs, req.Limit, req.ParentID) },
	"fxForwardBookingAudit":             func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryFXForwardBookingAudit(ctx, pool, req.EntityIDs, req.Limit, req.ParentID) },
	"fxExposureAudit":                   func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryFXExposureAudit(ctx, pool, req.EntityIDs, req.Limit, req.ParentID) },
	"investmentOnboardBatchInfo":        func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryInvestmentOnboardBatchInfo(ctx, pool, req.EntityIDs, req.Limit, req.ParentID) },
	// ── FX Module ──────────────────────────────────────────────────────────────
	"fxExposureHeadersLineItems":          func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryFXExposureHeadersLineItems(ctx, pool, req.EntityIDs, req.Limit) },
	"fxExposureBucketing":                 func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryFXExposureBucketing(ctx, pool, req.EntityIDs, req.Limit) },
	"fxHedgingProposals":                  func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryFXHedgingProposals(ctx, pool, req.EntityIDs, req.Limit) },
	"fxHedgeLinksDetails":                 func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryFXHedgeLinksDetails(ctx, pool, req.EntityIDs, req.Limit) },
	"fxForwardMTM":                        func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryFXForwardMTM(ctx, pool, req.EntityIDs, req.Limit) },
	"fxForwardBookingList":                func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryFXForwardBookings(ctx, pool, req.EntityIDs, req.Limit) },
	"fxEntityRelevantForwardBookings":     func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) { return queryFXEntityRelevantForwardBookings(ctx, pool, req.EntityIDs, req.Limit) },
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

		var reqEntityNames []string
		ctxEntityIDs := api.CtxEntityIDs(r.Context())
		var ctxNames []string
		if v := r.Context().Value(api.BusinessUnitsKey); v != nil {
			if names, ok := v.([]string); ok {
				ctxNames = names
			}
		}

		if len(req.EntityIDs) == 0 {
			req.EntityIDs = ctxEntityIDs
			reqEntityNames = ctxNames
		} else {
			for _, reqID := range req.EntityIDs {
				for i, ctxID := range ctxEntityIDs {
					if reqID == ctxID && i < len(ctxNames) {
						reqEntityNames = append(reqEntityNames, ctxNames[i])
						break
					}
				}
			}
		}

		if req.Limit <= 0 || req.Limit > 2000 {
			req.Limit = 500
		}

		ctx := context.WithValue(r.Context(), "reqEntityNames", reqEntityNames)
		fn, ok := dataSources[req.Source]
		if !ok {
			api.RespondWithError(w, http.StatusBadRequest,
				fmt.Sprintf("unknown data source: %s", req.Source))
			return
		}
		var (
			rows []map[string]any
			err  error
		)
		rows, err = fn(ctx, pool, req)

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

// entityNameFilter appends a filter for tables that use entity names instead of IDs.
func entityNameFilter(ctx context.Context, alias string, colName string, argOffset int) (string, []any) {
	names, _ := ctx.Value("reqEntityNames").([]string)
	if len(names) == 0 {
		return "", nil
	}
	return fmt.Sprintf("AND %s.%s = ANY($%d)", alias, colName, argOffset), []any{names}
}

// ─── fdBooking ────────────────────────────────────────────────────────────────
// entity_name and bank_name are columns on fd_booking_request itself.

func queryFDBooking(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	ef, efArgs := entityFilter(entityIDs, "br", 2)
	args := append([]any{limit}, efArgs...)

	q := fmt.Sprintf(`
		WITH latest_audit AS (
			SELECT DISTINCT ON (a.booking_id)
				a.booking_id,
				a.processing_status,
				a.requested_at,
				a.checker_at
			FROM investment.fd_audit_booking_request a
			ORDER BY a.booking_id,
			         GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp),
			                  COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC
		)
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
			br.expected_maturity_date,
			COALESCE(l.processing_status,         '')  AS processing_status
		FROM investment.fd_booking_request br
		LEFT JOIN latest_audit l ON l.booking_id = br.booking_id
		WHERE COALESCE(br.is_deleted, false) = false %s
		ORDER BY GREATEST(
			COALESCE(l.requested_at,'1970-01-01'::timestamp),
			COALESCE(l.checker_at,'1970-01-01'::timestamp),
			COALESCE(br.created_at,'1970-01-01'::timestamp)
		) DESC
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
		WITH latest_audit AS (
			SELECT DISTINCT ON (a.confirmation_id)
				a.confirmation_id,
				a.processing_status,
				a.requested_at,
				a.checker_at
			FROM investment.fd_audit_confirmation a
			ORDER BY a.confirmation_id,
			         GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp),
			                  COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC
		)
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
			fc.actual_maturity_date,
			fc.first_payout_date,
			fc.first_capitalization_date,
			COALESCE(l.processing_status,                  '')  AS processing_status
		FROM investment.fd_confirmation fc
		LEFT JOIN investment.fd_booking_request br ON br.booking_id = fc.booking_id
		LEFT JOIN latest_audit l ON l.confirmation_id = fc.confirmation_id
		WHERE COALESCE(fc.is_deleted, false) = false %s
		ORDER BY GREATEST(
			COALESCE(l.requested_at,'1970-01-01'::timestamp),
			COALESCE(l.checker_at,'1970-01-01'::timestamp),
			COALESCE(fc.created_at,'1970-01-01'::timestamp)
		) DESC
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
		WITH latest_audit AS (
			SELECT DISTINCT ON (a.fd_id)
				a.fd_id,
				a.processing_status,
				a.requested_at,
				a.checker_at
			FROM investment.fd_audit_master a
			ORDER BY a.fd_id,
			         GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp),
			                  COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC
		)
		SELECT
			COALESCE(m.fd_id::text,           '')  AS fd_id,
			COALESCE(m.confirmation_id::text, '')  AS confirmation_id,
			COALESCE(m.booking_id::text,      '')  AS booking_id,
			COALESCE(m.entity_id,             '')  AS entity_id,
			COALESCE(m.entity_name,           '')  AS entity_name,
			COALESCE(m.bank_id,               '')  AS bank_id,
			COALESCE(m.bank_name,             '')  AS bank_name,
			COALESCE(m.interest_type_code,    '')  AS interest_type_code,
			COALESCE(m.tenure_type,           '')  AS tenure_type,
			COALESCE(m.fd_status,             '')  AS fd_status,
			COALESCE(m.bank_fd_ref_no,        '')  AS bank_fd_ref_no,
			COALESCE(m.bank_reference_number, '')  AS bank_reference_number,
			COALESCE(m.tds_plan_id,           '')  AS tds_plan_id,
			COALESCE(m.auto_renewal,       FALSE)  AS auto_renewal,
			COALESCE(m.principal_amount,        0) AS principal_amount,
			COALESCE(m.interest_rate,           0) AS interest_rate,
			COALESCE(m.tenure_days,             0) AS tenure_days,
			COALESCE(m.tenure_months,           0) AS tenure_months,
			COALESCE(m.tenure_years,            0) AS tenure_years,
			m.start_date,
			m.maturity_date,
			COALESCE(l.processing_status,      '') AS processing_status
		FROM investment.fd_master m
		LEFT JOIN latest_audit l ON l.fd_id = m.fd_id
		WHERE COALESCE(m.is_deleted, false) = false %s
		ORDER BY GREATEST(
			COALESCE(l.requested_at,'1970-01-01'::timestamp),
			COALESCE(l.checker_at,'1970-01-01'::timestamp),
			COALESCE(m.created_at,'1970-01-01'::timestamp)
		) DESC
		LIMIT $1
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

// ─── fdCashflowGroup ──────────────────────────────────────────────────────────

func queryFDCashflowGroup(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	ef, efArgs := entityFilter(entityIDs, "m", 2)
	args := append([]any{limit}, efArgs...)

	q := fmt.Sprintf(`
		SELECT
			cf.fd_id,
			MAX(m.booking_id)                                                    AS booking_id,
			MAX(m.confirmation_id)                                               AS confirmation_id,
			MAX(m.entity_id)                                                     AS entity_id,
			MAX(m.entity_name)                                                   AS entity_name,
			MAX(m.bank_id)                                                       AS bank_id,
			MAX(m.bank_name)                                                     AS bank_name,
			COALESCE(MAX(m.principal_amount), 0)                                 AS principal_amount,
			COALESCE(MAX(m.interest_rate), 0)                                    AS interest_rate,
			MAX(m.start_date)                                                    AS start_date,
			MAX(m.maturity_date)                                                 AS maturity_date,
			MAX(m.fd_status)                                                     AS fd_status,
			COUNT(cf.cashflow_id)                                                AS total_cashflow_events,
			COALESCE(SUM(cf.interest_accrued), 0)                                AS total_interest_accrued,
			COALESCE(SUM(cf.tds_amount), 0)                                      AS total_tds,
			COALESCE(SUM(cf.net_cash_flow), 0)                                   AS total_net_cash_flow,
			MAX(cf.event_date)                                                   AS last_event_date,
			CASE
				WHEN COUNT(CASE WHEN aud.processing_status LIKE 'PENDING%%' THEN 1 END) > 0
					THEN 'PENDING_EDIT_APPROVAL'
				ELSE 'APPROVED'
			END                                                                  AS processing_status
		FROM investment.fd_cashflow_schedule cf
		JOIN investment.fd_master m ON m.fd_id = cf.fd_id
		LEFT JOIN LATERAL (
			SELECT processing_status
			FROM investment.fd_audit_cashflow_schedule a
			WHERE a.cashflow_id = cf.cashflow_id
			  AND a.processing_status LIKE 'PENDING%%'
			LIMIT 1
		) aud ON true
		WHERE COALESCE(cf.is_deleted, false) = false %s
		GROUP BY cf.fd_id
		ORDER BY MAX(m.start_date) DESC
		LIMIT $1
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

// ─── fdCashflows ──────────────────────────────────────────────────────────────

func queryFDCashflows(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, fdID string) ([]map[string]any, error) {
	ef, efArgs := entityNameFilter(ctx, "m", "entity_name", 2)

	args := []any{limit}
	args = append(args, efArgs...)

	fdFilter := ""
	if fdID != "" {
		fdFilter = fmt.Sprintf(" AND cf.fd_id = $%d ", len(args)+1)
		args = append(args, fdID)
	}

	q := fmt.Sprintf(`
		WITH latest_audit AS (
			SELECT DISTINCT ON (a.cashflow_id)
				a.cashflow_id,
				a.processing_status,
				a.requested_at,
				a.checker_at
			FROM investment.fd_audit_cashflow_schedule a
			ORDER BY a.cashflow_id,
			         GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp),
			                  COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC
		)
		SELECT
			COALESCE(cf.cashflow_id::text,           '')  AS cashflow_id,
			COALESCE(cf.fd_id::text,                 '')  AS fd_id,
			COALESCE(cf.sequence_number,              0)  AS sequence_number,
			cf.event_date,
			cf.period_start_date,
			cf.period_end_date,
			COALESCE(cf.event_type,                  '')  AS event_type,
			COALESCE(cf.cashflow_type,               '')  AS cashflow_type,
			COALESCE(cf.opening_principal,            0)  AS opening_principal,
			COALESCE(cf.closing_principal,            0)  AS closing_principal,
			COALESCE(cf.interest_accrued,             0)  AS interest_accrued,
			COALESCE(cf.tds_amount,                   0)  AS tds_amount,
			COALESCE(cf.net_cash_flow,                0)  AS net_cash_flow,
			COALESCE(cf.posting_status,              '')  AS posting_status,
			COALESCE(cf.is_active,                FALSE)  AS is_active,
			COALESCE(l.processing_status,            '')  AS processing_status
		FROM investment.fd_cashflow_schedule cf
		LEFT JOIN investment.fd_master m ON m.fd_id::text = cf.fd_id::text
		LEFT JOIN latest_audit l ON l.cashflow_id = cf.cashflow_id
		WHERE COALESCE(cf.is_deleted, false) = false %s %s
		ORDER BY GREATEST(
			COALESCE(l.requested_at,'1970-01-01'::timestamp),
			COALESCE(l.checker_at,'1970-01-01'::timestamp),
			COALESCE(cf.created_at,'1970-01-01'::timestamp)
		) DESC
		LIMIT $1
	`, ef, fdFilter)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

// ─── fdClosureInitiateAll ─────────────────────────────────────────────────────

func queryFDClosureInitiateAll(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	ef, efArgs := entityFilter(entityIDs, "br", 2)
	args := append([]any{limit}, efArgs...)

	q := fmt.Sprintf(`
		WITH latest_audit AS (
			SELECT DISTINCT ON (a.closure_initiate_id)
				a.closure_initiate_id,
				a.processing_status,
				a.requested_at,
				a.checker_at
			FROM cimplr.fd_closure_initiate_audit a
			ORDER BY a.closure_initiate_id,
			         GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp),
			                  COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC
		)
		SELECT
			COALESCE(ci.closure_initiate_id::text, '')  AS closure_initiate_id,
			COALESCE(ci.fd_id::text,               '')  AS fd_id,
			COALESCE(m.entity_id,                  '')  AS entity_id,
			COALESCE(br.entity_name,               '')  AS entity_name,
			COALESCE(br.bank_name,                 '')  AS bank_name,
			COALESCE(ci.closure_type,              '')  AS closure_type,
			COALESCE(ci.closure_status,            '')  AS closure_status,
			COALESCE(ci.principal_amount,           0)  AS principal_amount,
			COALESCE(ci.accrued_interest_till_date, 0)  AS accrued_interest_till_date,
			COALESCE(ci.tds_expected,               0)  AS tds_expected,
			COALESCE(ci.net_expected_amount,        0)  AS net_expected_amount,
			ci.requested_closure_date,
			COALESCE(ci.has_variance,            FALSE) AS has_variance,
			COALESCE(l.processing_status,          '')  AS processing_status
		FROM cimplr.fd_closure_initiate ci
		LEFT JOIN investment.fd_master m ON m.fd_id = ci.fd_id
		LEFT JOIN investment.fd_booking_request br ON br.booking_id = m.booking_id
		LEFT JOIN latest_audit l ON l.closure_initiate_id = ci.closure_initiate_id
		WHERE COALESCE(ci.is_deleted, false) = false %s
		ORDER BY GREATEST(
			COALESCE(l.requested_at,'1970-01-01'::timestamp),
			COALESCE(l.checker_at,'1970-01-01'::timestamp),
			COALESCE(ci.requested_closure_date::timestamp,'1970-01-01'::timestamp)
		) DESC
		LIMIT $1
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

// ─── fdClosureConfirmAll ──────────────────────────────────────────────────────

func queryFDClosureConfirmAll(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	ef, efArgs := entityFilter(entityIDs, "br", 2)
	args := append([]any{limit}, efArgs...)

	q := fmt.Sprintf(`
		WITH latest_audit AS (
			SELECT DISTINCT ON (a.closure_confirm_id)
				a.closure_confirm_id,
				a.processing_status,
				a.requested_at,
				a.checker_at
			FROM cimplr.fd_closure_confirm_audit a
			ORDER BY a.closure_confirm_id,
			         GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp),
			                  COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC
		)
		SELECT
			COALESCE(cc.closure_confirm_id::text,  '')  AS closure_confirm_id,
			COALESCE(cc.closure_initiate_id::text, '')  AS closure_initiate_id,
			COALESCE(cc.fd_id::text,               '')  AS fd_id,
			COALESCE(m.entity_id,                  '')  AS entity_id,
			COALESCE(br.entity_name,               '')  AS entity_name,
			COALESCE(br.bank_name,                 '')  AS bank_name,
			COALESCE(cc.closure_type,              '')  AS closure_type,
			COALESCE(cc.closure_status,            '')  AS closure_status,
			COALESCE(cc.posting_status,            '')  AS posting_status,
			COALESCE(cc.principal_received,         0)  AS principal_received,
			COALESCE(cc.interest_received,          0)  AS interest_received,
			COALESCE(cc.tds_deducted,               0)  AS tds_deducted,
			COALESCE(cc.net_amount_received,        0)  AS net_amount_received,
			cc.actual_payout_date,
			COALESCE(cc.has_variance,            FALSE) AS has_variance,
			COALESCE(cc.accounting_posted,       FALSE) AS accounting_posted,
			COALESCE(l.processing_status,          '')  AS processing_status
		FROM cimplr.fd_closure_confirm cc
		LEFT JOIN investment.fd_master m ON m.fd_id = cc.fd_id
		LEFT JOIN investment.fd_booking_request br ON br.booking_id = m.booking_id
		LEFT JOIN latest_audit l ON l.closure_confirm_id = cc.closure_confirm_id
		WHERE COALESCE(cc.is_deleted, false) = false %s
		ORDER BY GREATEST(
			COALESCE(l.requested_at,'1970-01-01'::timestamp),
			COALESCE(l.checker_at,'1970-01-01'::timestamp),
			COALESCE(cc.actual_payout_date::timestamp,'1970-01-01'::timestamp)
		) DESC
		LIMIT $1
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

// ─── fdClosurePrematureAll ────────────────────────────────────────────────────

func queryFDClosurePrematureAll(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	ef, efArgs := entityFilter(entityIDs, "br", 2)
	args := append([]any{limit}, efArgs...)

	q := fmt.Sprintf(`
		WITH latest_audit AS (
			SELECT DISTINCT ON (a.closure_confirm_id)
				a.closure_confirm_id,
				a.processing_status,
				a.requested_at,
				a.checker_at
			FROM cimplr.fd_closure_confirm_audit a
			ORDER BY a.closure_confirm_id,
			         GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp),
			                  COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC
		)
		SELECT
			COALESCE(cc.closure_confirm_id::text,  '')  AS closure_confirm_id,
			COALESCE(cc.closure_initiate_id::text, '')  AS closure_initiate_id,
			COALESCE(cc.fd_id::text,               '')  AS fd_id,
			COALESCE(m.entity_id,                  '')  AS entity_id,
			COALESCE(br.entity_name,               '')  AS entity_name,
			COALESCE(br.bank_name,                 '')  AS bank_name,
			COALESCE(cc.closure_type,              '')  AS closure_type,
			COALESCE(cc.closure_status,            '')  AS closure_status,
			COALESCE(cc.posting_status,            '')  AS posting_status,
			COALESCE(cc.principal_received,         0)  AS principal_received,
			COALESCE(cc.interest_received,          0)  AS interest_received,
			COALESCE(cc.tds_deducted,               0)  AS tds_deducted,
			COALESCE(cc.net_amount_received,        0)  AS net_amount_received,
			cc.actual_payout_date,
			COALESCE(cc.has_variance,            FALSE) AS has_variance,
			COALESCE(cc.accounting_posted,       FALSE) AS accounting_posted,
			COALESCE(l.processing_status,          '')  AS processing_status
		FROM cimplr.fd_closure_confirm cc
		LEFT JOIN investment.fd_master m ON m.fd_id = cc.fd_id
		LEFT JOIN investment.fd_booking_request br ON br.booking_id = m.booking_id
		LEFT JOIN latest_audit l ON l.closure_confirm_id = cc.closure_confirm_id
		WHERE COALESCE(cc.is_deleted, false) = false AND cc.closure_type = 'PREMATURE' %s
		ORDER BY GREATEST(
			COALESCE(l.requested_at,'1970-01-01'::timestamp),
			COALESCE(l.checker_at,'1970-01-01'::timestamp),
			COALESCE(cc.actual_payout_date::timestamp,'1970-01-01'::timestamp)
		) DESC
		LIMIT $1
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}
