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
	"strings"
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
	// Dashboard-builder filter band — populated by the frontend "Generate" button.
	BankIDs        []string               `json:"bank_ids"`
	AccountNumbers []string               `json:"account_numbers"`
	BankAccountScope []bankAccountScopePair `json:"bank_account_scope"`
	ProposalIDs      []string               `json:"proposal_ids"`
	AsOfDate       string                 `json:"as_of_date"`
	AsOnDate       string                 `json:"as_on_date"`
	ViewType       string                 `json:"view_type"`
}

type bankAccountScopePair struct {
	BankID        string `json:"bank_id"`
	AccountNumber string `json:"account_number"`
}

// Context keys for dashboard-builder filter values stashed by GetDataSource so
// that helper functions can read them without changing every query signature.
const (
	ctxKeyReqBankIDs         = "reqBankIDs"
	ctxKeyReqBankNamesNorm   = "reqBankNamesNorm"
	ctxKeyReqAccountNumbers  = "reqAccountNumbers"
	ctxKeyReqAsOfDate        = "reqAsOfDate"
	ctxKeyReqAsOnDate        = "reqAsOnDate"
	ctxKeyReqViewType        = "reqViewType"
)

type dataSourceFn func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error)

// dataSources maps each source name to its query function, replacing the switch.
var dataSources = map[string]dataSourceFn{
	// ── FD Core ────────────────────────────────────────────────────────────────
	"fdBooking": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryFDBooking(ctx, pool, req.EntityIDs, req.Limit)
	},
	"fdConfirmation": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryFDConfirmation(ctx, pool, req.EntityIDs, req.Limit)
	},
	"fdActivation": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryFDActivation(ctx, pool, req.EntityIDs, req.Limit)
	},
	"fdCashflowGroup": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryFDCashflowGroup(ctx, pool, req.EntityIDs, req.Limit)
	},
	"fdCashflows": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryFDCashflows(ctx, pool, req.EntityIDs, req.Limit, req.FDID)
	},
	"fdClosureInitiateAll": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryFDClosureInitiateAll(ctx, pool, req.EntityIDs, req.Limit)
	},
	"fdClosureConfirmAll": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryFDClosureConfirmAll(ctx, pool, req.EntityIDs, req.Limit)
	},
	"fdClosurePrematureAll": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryFDClosurePrematureAll(ctx, pool, req.EntityIDs, req.Limit)
	},
	// ── Maturity & Receipt ─────────────────────────────────────────────────────
	"fdMaturitySummary": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryFDMaturitySummary(ctx, pool, req.EntityIDs, req.Limit)
	},
	"fdTdsRegister": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryFDTDSRegister(ctx, pool, req.EntityIDs, req.Limit)
	},
	"fdReceiptAll": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryFDReceiptAll(ctx, pool, req.EntityIDs, req.Limit)
	},
	"fdReconcileResults": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryFDReconcileResults(ctx, pool, req.EntityIDs, req.Limit)
	},
	"fdExceptions": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryFDExceptions(ctx, pool, req.EntityIDs, req.Limit)
	},
	// ── Accrual ────────────────────────────────────────────────────────────────
	"fdAccrualRunAll": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryFDAccrualRunAll(ctx, pool, req.EntityIDs, req.Limit)
	},
	"fdAccrualLedger": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryFDAccrualLedger(ctx, pool, req.EntityIDs, req.Limit)
	},
	"fdAccrualExecutionLog": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryFDAccrualExecutionLog(ctx, pool, req.EntityIDs, req.Limit)
	},
	"fdAccrualRunAudit": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryFDAccrualRunAudit(ctx, pool, req.EntityIDs, req.Limit)
	},
	"fdAccrualLedgerAudit": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryFDAccrualLedgerAudit(ctx, pool, req.EntityIDs, req.Limit)
	},

	"fdAccrualScheduleAll": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryFDAccrualScheduleAll(ctx, pool, req.EntityIDs, req.Limit)
	},
	"fdAccrualScheduleExecutionLog": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryFDAccrualScheduleExecutionLog(ctx, pool, req.EntityIDs, req.Limit, req.ParentID)
	},
	// ── Portfolio & Proposal ───────────────────────────────────────────────────
	"investmentOnboardBatch": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryInvestmentOnboardBatch(ctx, pool, req.EntityIDs, req.Limit)
	},
	"investmentProposalMeta": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryInvestmentProposalMeta(ctx, pool, req.EntityIDs, req.Limit)
	},
	"investmentInitiationAll": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryInvestmentInitiationAll(ctx, pool, req.EntityIDs, req.Limit)
	},
	"investmentConfirmationAll": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryInvestmentConfirmationAll(ctx, pool, req.EntityIDs, req.Limit)
	},
	"investmentPortfolioGet": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryInvestmentPortfolioGet(ctx, pool, req.EntityIDs, req.Limit)
	},
	"investmentRedemptionInitiateAll": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryInvestmentRedemptionInitiateAll(ctx, pool, req.EntityIDs, req.Limit)
	},
	"investmentRedemptionConfirmAll": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryInvestmentRedemptionConfirmAll(ctx, pool, req.EntityIDs, req.Limit)
	},
	// ── Cash Module ────────────────────────────────────────────────────────────
	"cashBankStatements": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		pairs := resolveBankStatementScopePairs(req)
		if len(pairs) == 0 {
			return []map[string]any{}, nil
		}
		return queryCashBankStatements(ctx, pool, req.EntityIDs, req.Limit, pairs)
	},
	"cashBankStatementTransactions": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		pairs := resolveBankStatementScopePairs(req)
		if len(pairs) == 0 {
			return []map[string]any{}, nil
		}
		return queryCashBankStatementTransactions(ctx, pool, req.EntityIDs, req.Limit, req.ParentID, pairs)
	},
	"cashPayable": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryCashPayable(ctx, pool, req.EntityIDs, req.Limit)
	},
	"cashReceivable": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryCashReceivable(ctx, pool, req.EntityIDs, req.Limit)
	},
	"cashPayableReceivable": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryCashPayableReceivable(ctx, pool, req.EntityIDs, req.Limit)
	},
	"cashFundPlanSummary": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryCashFundPlanSummary(ctx, pool, req.EntityIDs, req.Limit)
	},
	"cashFundPlanDetails": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryCashFundPlanDetails(ctx, pool, req.EntityIDs, req.Limit, req.ParentID)
	},
	"cashSweepConfig": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryCashSweepConfig(ctx, pool, req.EntityIDs, req.Limit)
	},
	"cashSweepInitiation": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryCashSweepInitiation(ctx, pool, req.EntityIDs, req.Limit)
	},
	"cashProjectionList": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryCashProjectionList(ctx, pool, req.EntityIDs, req.Limit)
	},
	"cashProjectionDetail": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		proposalIDs := resolveProjectionProposalIDs(req)
		if len(proposalIDs) == 0 {
			return []map[string]any{}, nil
		}
		return queryCashProjectionDetail(ctx, pool, req.EntityIDs, req.Limit, proposalIDs)
	},
	"cashBankBalances": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryCashBankBalances(ctx, pool, req.EntityIDs, req.Limit)
	},
	"cashFundAvailability": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryCashFundAvailability(ctx, pool, req.EntityIDs, req.Limit)
	},
	"cashBankLimits": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryCashBankLimits(ctx, pool, req.EntityIDs, req.Limit)
	},
	"cashUtilizations": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryCashUtilizations(ctx, pool, req.EntityIDs, req.Limit)
	},
	// ── Audit sub-sources ──────────────────────────────────────────────────────
	"fdBookingAudit": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryFDBookingAudit(ctx, pool, req.EntityIDs, req.Limit, req.ParentID)
	},
	"fdConfirmationAudit": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryFDConfirmationAudit(ctx, pool, req.EntityIDs, req.Limit, req.ParentID)
	},
	"fdActivationAudit": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryFDActivationAudit(ctx, pool, req.EntityIDs, req.Limit, req.ParentID)
	},
	"fdCashflowAudit": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryFDCashflowAudit(ctx, pool, req.EntityIDs, req.Limit, req.ParentID)
	},
	"fdClosureInitiateAudit": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryFDClosureInitiateAudit(ctx, pool, req.EntityIDs, req.Limit, req.ParentID)
	},
	"fdClosureConfirmAudit": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryFDClosureConfirmAudit(ctx, pool, req.EntityIDs, req.Limit, req.ParentID)
	},
	"fdTdsAudit": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryFDTdsAudit(ctx, pool, req.EntityIDs, req.Limit, req.ParentID)
	},
	"fdReceiptAudit": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryFDReceiptAudit(ctx, pool, req.EntityIDs, req.Limit, req.ParentID)
	},
	"fdExceptionAudit": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryFDExceptionAudit(ctx, pool, req.EntityIDs, req.Limit, req.ParentID)
	},
	"cashBankStatementAudit": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryCashBankStatementAudit(ctx, pool, req.EntityIDs, req.Limit, req.ParentID)
	},
	"cashSweepConfigAudit": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryCashSweepConfigAudit(ctx, pool, req.EntityIDs, req.Limit, req.ParentID)
	},
	"cashSweepInitiationAudit": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryCashSweepInitiationAudit(ctx, pool, req.EntityIDs, req.Limit, req.ParentID)
	},
	"cashProjectionAudit": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		proposalIDs := resolveProjectionProposalIDs(req)
		if len(proposalIDs) == 0 {
			return []map[string]any{}, nil
		}
		return queryCashProjectionAudit(ctx, pool, req.EntityIDs, req.Limit, proposalIDs)
	},
	"cashFundPlanAudit": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryCashFundPlanAudit(ctx, pool, req.EntityIDs, req.Limit, req.ParentID)
	},
	"investmentProposalAudit": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryInvestmentProposalAudit(ctx, pool, req.EntityIDs, req.Limit, req.ParentID)
	},
	"investmentInitiationAudit": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryInvestmentInitiationAudit(ctx, pool, req.EntityIDs, req.Limit, req.ParentID)
	},
	"investmentConfirmationAudit": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryInvestmentConfirmationAudit(ctx, pool, req.EntityIDs, req.Limit, req.ParentID)
	},
	"investmentRedemptionAudit": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryInvestmentRedemptionAudit(ctx, pool, req.EntityIDs, req.Limit, req.ParentID)
	},
	"investmentRedemptionConfirmAudit": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryInvestmentRedemptionConfirmAudit(ctx, pool, req.EntityIDs, req.Limit, req.ParentID)
	},
	"fxForwardBookingAudit": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryFXForwardBookingAudit(ctx, pool, req.EntityIDs, req.Limit, req.ParentID)
	},
	"fxExposureAudit": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryFXExposureAudit(ctx, pool, req.EntityIDs, req.Limit, req.ParentID)
	},
	"investmentOnboardBatchInfo": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryInvestmentOnboardBatchInfo(ctx, pool, req.EntityIDs, req.Limit, req.ParentID)
	},
	// ── FX Module ──────────────────────────────────────────────────────────────
	"fxExposureHeadersLineItems": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryFXExposureHeadersLineItems(ctx, pool, req.EntityIDs, req.Limit)
	},
	"fxExposureBucketing": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryFXExposureBucketing(ctx, pool, req.EntityIDs, req.Limit)
	},
	"fxHedgingProposals": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryFXHedgingProposals(ctx, pool, req.EntityIDs, req.Limit)
	},
	"fxHedgeLinksDetails": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryFXHedgeLinksDetails(ctx, pool, req.EntityIDs, req.Limit)
	},
	"fxForwardMTM": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryFXForwardMTM(ctx, pool, req.EntityIDs, req.Limit)
	},
	"fxForwardBookingList": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryFXForwardBookings(ctx, pool, req.EntityIDs, req.Limit)
	},
	"fxEntityRelevantForwardBookings": func(ctx context.Context, pool *pgxpool.Pool, req dataRequest) ([]map[string]any, error) {
		return queryFXEntityRelevantForwardBookings(ctx, pool, req.EntityIDs, req.Limit)
	},
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

		if req.Limit < 0 {
			req.Limit = 0
		}
		const defaultDataLimit = 500
		const maxDataLimit = 2000
		if req.Limit <= 0 {
			req.Limit = defaultDataLimit
		} else if req.Limit > maxDataLimit {
			req.Limit = maxDataLimit
		}

		ctx := context.WithValue(r.Context(), "reqEntityNames", reqEntityNames)

		// ── Dashboard-builder filter band ─────────────────────────────────
		// Resolve the user-selected bank IDs into normalized lower-case
		// bank_name strings using the prevalidation BankInfo context, so
		// queries can filter by bank_id (where available) or bank_name
		// (case-insensitive) without re-querying the bank master.
		bankIDs := normalizeBankIDs(req.BankIDs)
		bankNamesNorm := make([]string, 0, len(bankIDs))
		seen := make(map[string]struct{}, len(bankIDs))
		for _, id := range bankIDs {
			if norm, ok := api.ResolveBankNameNormForFilter(r.Context(), id); ok {
				if _, dup := seen[norm]; !dup {
					seen[norm] = struct{}{}
					bankNamesNorm = append(bankNamesNorm, norm)
				}
			}
		}
		ctx = context.WithValue(ctx, ctxKeyReqBankIDs, bankIDs)
		ctx = context.WithValue(ctx, ctxKeyReqBankNamesNorm, bankNamesNorm)
		ctx = context.WithValue(ctx, ctxKeyReqAccountNumbers, normalizeAccountNumbers(req.AccountNumbers))
		ctx = context.WithValue(ctx, ctxKeyReqAsOfDate, strings.TrimSpace(req.AsOfDate))
		ctx = context.WithValue(ctx, ctxKeyReqAsOnDate, strings.TrimSpace(req.AsOnDate))
		ctx = context.WithValue(ctx, ctxKeyReqViewType, strings.ToLower(strings.TrimSpace(req.ViewType)))

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
		rows = normalizeRowsToINR(req.Source, rows)
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

// bankIDFilter appends a "AND alias.bank_id = ANY($N)" clause when the request
// included bank IDs in its filter band. Used for tables that store bank_id directly.
func bankIDFilter(ctx context.Context, alias string, argOffset int) (string, []any) {
	ids, _ := ctx.Value(ctxKeyReqBankIDs).([]string)
	if len(ids) == 0 {
		return "", nil
	}
	return fmt.Sprintf("AND %s.bank_id = ANY($%d)", alias, argOffset), []any{ids}
}

// bankNameFilter appends a case-insensitive bank_name filter using the bank
// names resolved from the request bank IDs against BankInfo in the session
// context. Used for tables that only store bank_name.
func bankNameFilter(ctx context.Context, alias string, argOffset int) (string, []any) {
	names, _ := ctx.Value(ctxKeyReqBankNamesNorm).([]string)
	if len(names) == 0 {
		return "", nil
	}
	return fmt.Sprintf("AND LOWER(TRIM(COALESCE(%s.bank_name,''))) = ANY($%d)", alias, argOffset), []any{names}
}

func accountNumberFilter(ctx context.Context, alias string, colName string, argOffset int) (string, []any) {
	nums, _ := ctx.Value(ctxKeyReqAccountNumbers).([]string)
	if len(nums) == 0 {
		return "", nil
	}
	return fmt.Sprintf("AND %s.%s = ANY($%d)", alias, colName, argOffset), []any{nums}
}

func normalizeAccountNumbers(values []string) []string {
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, raw := range values {
		s := strings.TrimSpace(raw)
		if s == "" {
			continue
		}
		if _, dup := seen[s]; dup {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}

func normalizeBankIDs(values []string) []string {
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, raw := range values {
		s := strings.TrimSpace(raw)
		if s == "" {
			continue
		}
		if _, dup := seen[s]; dup {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}

// bankStatementScopeFilter builds bank+account pair filters for bank statement queries.
// Each pair is matched with AND; multiple pairs are combined with OR.
func bankStatementScopeFilter(
	stmtAlias string,
	pairs []bankAccountScopePair,
	argIdx int,
) (clause string, args []any, nextIdx int) {
	pairs = normalizeBankAccountScopePairs(pairs)
	if len(pairs) == 0 {
		return "", nil, argIdx
	}

	parts := make([]string, 0, len(pairs))
	for _, pair := range pairs {
		if pair.BankID != "" {
			parts = append(parts, fmt.Sprintf(`(
				TRIM(%s.account_number) = $%d AND EXISTS (
					SELECT 1 FROM public.masterbankaccount mba_scope
					WHERE TRIM(mba_scope.account_number) = TRIM(%s.account_number)
					  AND COALESCE(mba_scope.is_deleted, false) = false
					  AND mba_scope.bank_id = $%d
				)
			)`, stmtAlias, argIdx, stmtAlias, argIdx+1))
			args = append(args, pair.AccountNumber, pair.BankID)
			argIdx += 2
			continue
		}

		parts = append(parts, fmt.Sprintf("TRIM(%s.account_number) = $%d", stmtAlias, argIdx))
		args = append(args, pair.AccountNumber)
		argIdx++
	}

	return " AND (" + strings.Join(parts, " OR ") + ")", args, argIdx
}

func normalizeBankAccountScopePairs(values []bankAccountScopePair) []bankAccountScopePair {
	out := make([]bankAccountScopePair, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, raw := range values {
		bankID := strings.TrimSpace(raw.BankID)
		accountNumber := strings.TrimSpace(raw.AccountNumber)
		if accountNumber == "" {
			continue
		}
		key := bankID + "\x00" + accountNumber
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, bankAccountScopePair{
			BankID:        bankID,
			AccountNumber: accountNumber,
		})
	}
	return out
}

func resolveBankStatementScopePairs(req dataRequest) []bankAccountScopePair {
	if pairs := normalizeBankAccountScopePairs(req.BankAccountScope); len(pairs) > 0 {
		return pairs
	}

	accountNumbers := normalizeAccountNumbers(req.AccountNumbers)
	if len(accountNumbers) == 0 {
		return nil
	}

	bankIDs := normalizeBankIDs(req.BankIDs)
	if len(bankIDs) == 0 {
		out := make([]bankAccountScopePair, 0, len(accountNumbers))
		for _, accountNumber := range accountNumbers {
			out = append(out, bankAccountScopePair{AccountNumber: accountNumber})
		}
		return out
	}

	if len(bankIDs) == len(accountNumbers) {
		out := make([]bankAccountScopePair, 0, len(accountNumbers))
		for i, accountNumber := range accountNumbers {
			out = append(out, bankAccountScopePair{
				BankID:        bankIDs[i],
				AccountNumber: accountNumber,
			})
		}
		return out
	}

	// Legacy flat arrays: match any listed account at any listed bank.
	out := make([]bankAccountScopePair, 0, len(accountNumbers)*len(bankIDs))
	for _, accountNumber := range accountNumbers {
		for _, bankID := range bankIDs {
			out = append(out, bankAccountScopePair{
				BankID:        bankID,
				AccountNumber: accountNumber,
			})
		}
	}
	return normalizeBankAccountScopePairs(out)
}

func normalizeProposalIDs(values []string) []string {
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, raw := range values {
		s := strings.TrimSpace(raw)
		if s == "" {
			continue
		}
		if _, dup := seen[s]; dup {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}

func resolveProjectionProposalIDs(req dataRequest) []string {
	if ids := normalizeProposalIDs(req.ProposalIDs); len(ids) > 0 {
		return ids
	}
	if id := strings.TrimSpace(req.ParentID); id != "" {
		return []string{id}
	}
	return nil
}

// dateRangeFilter appends a date range filter on the given column using the
// request's as_of_date / as_on_date filter band values. Either bound may be
// omitted; both omitted yields no filter.
func dateRangeFilter(ctx context.Context, alias, col string, argOffset int) (string, []any) {
	asOf, _ := ctx.Value(ctxKeyReqAsOfDate).(string)
	asOn, _ := ctx.Value(ctxKeyReqAsOnDate).(string)
	switch {
	case asOf != "" && asOn != "":
		return fmt.Sprintf("AND %s.%s::date BETWEEN $%d::date AND $%d::date", alias, col, argOffset, argOffset+1), []any{asOf, asOn}
	case asOf != "":
		return fmt.Sprintf("AND %s.%s::date >= $%d::date", alias, col, argOffset), []any{asOf}
	case asOn != "":
		return fmt.Sprintf("AND %s.%s::date <= $%d::date", alias, col, argOffset), []any{asOn}
	}
	return "", nil
}

// ─── fdBooking ────────────────────────────────────────────────────────────────
// entity_name and bank_name are columns on fd_booking_request itself.

func queryFDBooking(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	args := []any{limit}
	ef, efArgs := entityFilter(entityIDs, "br", len(args)+1)
	args = append(args, efArgs...)
	bf, bfArgs := bankNameFilter(ctx, "br", len(args)+1)
	args = append(args, bfArgs...)
	df, dfArgs := dateRangeFilter(ctx, "br", "value_date", len(args)+1)
	args = append(args, dfArgs...)

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
		),
		history AS (
			SELECT
				booking_id,
				MAX(CASE WHEN action_type = 'CREATE' THEN requested_by END) AS created_by
			FROM investment.fd_audit_booking_request
			GROUP BY booking_id
		)
		SELECT
			COALESCE(br.booking_id::text,         '')  AS booking_id,
			COALESCE(br.entity_name,              '')  AS entity_name,
			COALESCE(br.bank_name,                '')  AS bank_name,
			COALESCE(br.tenor_type,               '')  AS tenor_type,
			COALESCE(br.interest_type_code,       '')  AS interest_type_code,
			COALESCE(br.frequency_id::text,       '')  AS frequency_id,
			COALESCE(br.payout_frequency_id::text, '') AS payout_frequency_id,
			COALESCE(br.accrual_frequency_code,   '')  AS accrual_frequency_code,
			COALESCE(br.reset_type,               '')  AS reset_type,
			COALESCE(br.day_count_code,           '')  AS day_count_code,
			COALESCE(br.product_code,             '')  AS product_code,
			COALESCE(br.tds_plan_id::text,        '')  AS tds_plan_id,
			COALESCE(br.source_account_number,    '')  AS source_account_number,
			COALESCE(br.bank_config_id::text,     '')  AS bank_config_id,
			COALESCE(br.value_type,               '')  AS value_type,
			COALESCE(br.created_by,               '')  AS created_by,
			COALESCE(br.booking_remarks,          '')  AS booking_remarks,
			COALESCE(br.booking_status,           '')  AS booking_status,
			COALESCE(br.tds_plan_id,              '')  AS tds_plan_id,
			COALESCE(NULLIF(br.payout_frequency_id, ''), br.frequency_id, '') AS payout_frequency_id,
			COALESCE(br.source_account_number,    '')  AS source_account_number,
			COALESCE(br.bank_config_id,           '')  AS bank_config_id,
			COALESCE(br.value_type,               '')  AS value_type,
			COALESCE(h.created_by, br.created_by, '')  AS created_by,
			COALESCE(br.booking_remarks,          '')  AS booking_remarks,
			COALESCE(br.auto_renewal,          FALSE)  AS auto_renewal,
			COALESCE(br.principal_amount,           0) AS principal_amount,
			COALESCE(br.interest_rate,              0) AS interest_rate,
			COALESCE(br.tenure_days,                0) AS tenure_days,
			COALESCE(br.tenure_months,              0) AS tenure_months,
			COALESCE(br.tenure_years,               0) AS tenure_years,
			br.value_date,
			br.expected_maturity_date,
			br.expected_start_date,
			br.offer_valid_till,
			COALESCE(l.processing_status,         '')  AS processing_status
		FROM investment.fd_booking_request br
		LEFT JOIN latest_audit l ON l.booking_id = br.booking_id
		LEFT JOIN history h ON h.booking_id = br.booking_id
		WHERE COALESCE(br.is_deleted, false) = false %s %s %s
		ORDER BY GREATEST(
			COALESCE(l.requested_at,'1970-01-01'::timestamp),
			COALESCE(l.checker_at,'1970-01-01'::timestamp),
			COALESCE(br.created_at,'1970-01-01'::timestamp)
		) DESC
		LIMIT NULLIF($1, 0)
	`, ef, bf, df)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

// ─── fdConfirmation ───────────────────────────────────────────────────────────
// Join with fd_booking_request to get entity_name and bank_name.

func queryFDConfirmation(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	args := []any{limit}
	ef, efArgs := entityFilter(entityIDs, "br", len(args)+1)
	args = append(args, efArgs...)
	bf, bfArgs := bankNameFilter(ctx, "br", len(args)+1)
	args = append(args, bfArgs...)
	df, dfArgs := dateRangeFilter(ctx, "fc", "actual_start_date", len(args)+1)
	args = append(args, dfArgs...)

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
		),
		history AS (
			SELECT
				confirmation_id,
				MAX(CASE WHEN action_type = 'CREATE' THEN requested_by END) AS created_by
			FROM investment.fd_audit_confirmation
			GROUP BY confirmation_id
		)
		SELECT
			COALESCE(fc.confirmation_id::text,             '')  AS confirmation_id,
			COALESCE(fc.booking_id::text,                  '')  AS booking_id,
			COALESCE(br.entity_name,                       '')  AS entity_name,
			COALESCE(br.bank_name,                         '')  AS bank_name,
			COALESCE(fc.tenor_type,                        '')  AS tenor_type,
			COALESCE(fc.confirmed_interest_type_code,      '')  AS confirmed_interest_type_code,
			COALESCE(fc.confirmation_mode,                 '')  AS confirmation_mode,
			COALESCE(fc.confirmed_frequency_id::text,      '')  AS confirmed_frequency_id,
			COALESCE(fc.payout_frequency_id::text,         '')  AS payout_frequency_id,
			COALESCE(fc.accrual_frequency_code,            '')  AS accrual_frequency_code,
			COALESCE(fc.reset_type,                        '')  AS reset_type,
			COALESCE(fc.confirmation_status,               '')  AS confirmation_status,
			COALESCE(fc.variance_action,                   '')  AS variance_action,
			COALESCE(fc.bank_fd_ref_no,                    '')  AS bank_fd_ref_no,
			COALESCE(fc.bank_reference_number,             '')  AS bank_reference_number,
			COALESCE(fc.premature_closure_terms,           '')  AS premature_closure_terms,
			COALESCE(fc.penalty_id::text,                  '')  AS penalty_id,
			COALESCE(fc.value_type,                        '')  AS value_type,
			COALESCE(fc.created_by,                        '')  AS created_by,
			COALESCE(fc.variance_flag,                  FALSE)  AS variance_flag,
			COALESCE(fc.actual_principal,                   0)  AS actual_principal,
			COALESCE(fc.confirmed_rate,                     0)  AS confirmed_rate,
			COALESCE(fc.tenor_days,                         0)  AS tenor_days,
			COALESCE(fc.tenor_months,                       0)  AS tenor_months,
			COALESCE(fc.tenor_years,                        0)  AS tenor_years,
			fc.actual_start_date,
			fc.actual_maturity_date,
			fc.confirmation_received_date,
			fc.first_payout_date,
			fc.first_capitalization_date,
			COALESCE(l.processing_status,                  '')  AS processing_status
		FROM investment.fd_confirmation fc
		LEFT JOIN investment.fd_booking_request br ON br.booking_id = fc.booking_id
		LEFT JOIN latest_audit l ON l.confirmation_id = fc.confirmation_id
		LEFT JOIN history h ON h.confirmation_id = fc.confirmation_id
		WHERE COALESCE(fc.is_deleted, false) = false %s %s %s
		ORDER BY GREATEST(
			COALESCE(l.requested_at,'1970-01-01'::timestamp),
			COALESCE(l.checker_at,'1970-01-01'::timestamp),
			COALESCE(fc.created_at,'1970-01-01'::timestamp)
		) DESC
		LIMIT NULLIF($1, 0)
	`, ef, bf, df)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

// ─── fdActivation ─────────────────────────────────────────────────────────────
// entity_name and bank_name are columns on fd_master itself.

func queryFDActivation(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	args := []any{limit}
	ef, efArgs := entityFilter(entityIDs, "m", len(args)+1)
	args = append(args, efArgs...)
	bf, bfArgs := bankIDFilter(ctx, "m", len(args)+1)
	args = append(args, bfArgs...)
	df, dfArgs := dateRangeFilter(ctx, "m", "start_date", len(args)+1)
	args = append(args, dfArgs...)

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
		),
		history AS (
			SELECT
				fd_id,
				MAX(CASE WHEN action_type = 'CREATE' THEN requested_by END) AS created_by
			FROM investment.fd_audit_master
			GROUP BY fd_id
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
			COALESCE(NULLIF(m.tenure_type, ''),
				CASE
					WHEN COALESCE(m.tenure_years,  0) > 0 THEN 'YEARS'
					WHEN COALESCE(m.tenure_months, 0) > 0 THEN 'MONTHS'
					WHEN COALESCE(m.tenure_days,   0) > 0 THEN 'DAYS'
					ELSE ''
				END)                               AS tenure_type,
			COALESCE(m.frequency_id::text,    '')  AS frequency_id,
			COALESCE(m.payout_frequency_id::text, '') AS payout_frequency_id,
			COALESCE(m.accrual_frequency_code, '') AS accrual_frequency_code,
			COALESCE(m.reset_type,            '')  AS reset_type,
			COALESCE(m.fd_status,             '')  AS fd_status,
			COALESCE(m.bank_fd_ref_no,        '')  AS bank_fd_ref_no,
			COALESCE(m.bank_reference_number, '')  AS bank_reference_number,
			COALESCE(m.tds_plan_id,           '')  AS tds_plan_id,
			COALESCE(m.day_count_code,        '')  AS day_count_code,
			COALESCE(m.bank_config_id::text,  '')  AS bank_config_id,
			COALESCE(m.created_by,            '')  AS created_by,
			COALESCE(m.auto_renewal,       FALSE)  AS auto_renewal,
			COALESCE(m.principal_amount,        0) AS principal_amount,
			COALESCE(m.interest_rate,           0) AS interest_rate,
			COALESCE(m.tenure_days,             0) AS tenure_days,
			COALESCE(m.tenure_months,           0) AS tenure_months,
			COALESCE(m.tenure_years,            0) AS tenure_years,
			m.start_date,
			m.maturity_date,
			m.first_payout_date,
			m.first_capitalization_date,
			m.receipt_date,
			COALESCE(l.processing_status,      '') AS processing_status
		FROM investment.fd_master m
		LEFT JOIN investment.fd_booking_request br ON br.booking_id = m.booking_id
		LEFT JOIN latest_audit l ON l.fd_id = m.fd_id
		LEFT JOIN history h ON h.fd_id = m.fd_id
		WHERE COALESCE(m.is_deleted, false) = false %s %s %s
		ORDER BY GREATEST(
			COALESCE(l.requested_at,'1970-01-01'::timestamp),
			COALESCE(l.checker_at,'1970-01-01'::timestamp),
			COALESCE(m.created_at,'1970-01-01'::timestamp)
		) DESC
		LIMIT NULLIF($1, 0)
	`, ef, bf, df)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

// ─── fdCashflowGroup ──────────────────────────────────────────────────────────

func queryFDCashflowGroup(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	args := []any{limit}
	ef, efArgs := entityFilter(entityIDs, "m", len(args)+1)
	args = append(args, efArgs...)
	bf, bfArgs := bankIDFilter(ctx, "m", len(args)+1)
	args = append(args, bfArgs...)
	df, dfArgs := dateRangeFilter(ctx, "m", "start_date", len(args)+1)
	args = append(args, dfArgs...)

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
		WHERE COALESCE(cf.is_deleted, false) = false %s %s %s
		GROUP BY cf.fd_id
		ORDER BY MAX(m.start_date) DESC
		LIMIT NULLIF($1, 0)
	`, ef, bf, df)

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
		LIMIT NULLIF($1, 0)
	`, ef, fdFilter)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

// ─── fdClosureInitiateAll ─────────────────────────────────────────────────────

func queryFDClosureInitiateAll(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	args := []any{limit}
	ef, efArgs := entityFilter(entityIDs, "ci", len(args)+1)
	args = append(args, efArgs...)
	bf, bfArgs := bankNameFilter(ctx, "ci", len(args)+1)
	args = append(args, bfArgs...)
	df, dfArgs := dateRangeFilter(ctx, "ci", "requested_closure_date", len(args)+1)
	args = append(args, dfArgs...)

	// latest_processing_status mirrors listCimplrRecords:
	// - PAYOUT/ROLLOVER: fd_closure_initiate_audit
	// - PREMATURE: approval audit lives on linked fd_closure_confirm (initiate audit is empty)
	q := fmt.Sprintf(`
		SELECT
			COALESCE(ci.closure_initiate_id::text, '') AS closure_initiate_id,
			COALESCE(ci.fd_id::text,               '') AS fd_id,
			COALESCE(ci.entity_id,                 '') AS entity_id,
			COALESCE(ci.entity_name,               '') AS entity_name,
			COALESCE(ci.bank_name,                 '') AS bank_name,
			COALESCE(ci.closure_type,              '') AS closure_type,
			COALESCE(ci.closure_status,            '') AS closure_status,
			COALESCE(ci.principal_amount,           0) AS principal_amount,
			COALESCE(ci.accrued_interest_till_date, 0) AS accrued_interest_till_date,
			COALESCE(ci.tds_expected,               0) AS tds_expected,
			COALESCE(ci.net_expected_amount,        0) AS net_expected_amount,
			ci.requested_closure_date,
			COALESCE(ci.has_variance,            FALSE) AS has_variance,
			COALESCE(
				NULLIF(ia.processing_status, ''),
				CASE WHEN UPPER(COALESCE(ci.closure_type, '')) = 'PREMATURE' THEN
					COALESCE(
						CASE WHEN UPPER(COALESCE(prem_cc.closure_status, '')) = 'POSTED' THEN 'POSTED' END,
						ca.processing_status,
						''
					)
				END,
				''
			) AS latest_processing_status
		FROM cimplr.fd_closure_initiate ci
		LEFT JOIN LATERAL (
			SELECT a.processing_status
			FROM cimplr.fd_closure_initiate_audit a
			WHERE a.closure_initiate_id = ci.closure_initiate_id
			ORDER BY a.requested_at DESC NULLS LAST, a.audit_id DESC
			LIMIT 1
		) ia ON true
		LEFT JOIN LATERAL (
			SELECT cc.closure_confirm_id, cc.closure_status
			FROM cimplr.fd_closure_confirm cc
			WHERE cc.closure_initiate_id = ci.closure_initiate_id
			  AND COALESCE(cc.is_deleted, false) = false
			ORDER BY cc.closure_confirm_id DESC
			LIMIT 1
		) prem_cc ON true
		LEFT JOIN LATERAL (
			SELECT a.processing_status
			FROM cimplr.fd_closure_confirm_audit a
			WHERE a.closure_confirm_id = prem_cc.closure_confirm_id
			ORDER BY CASE WHEN a.action_type = 'POST' AND a.processing_status = 'POSTED' THEN 0 ELSE 1 END,
			         a.requested_at DESC NULLS LAST, a.audit_id DESC
			LIMIT 1
		) ca ON true
		WHERE COALESCE(ci.is_deleted, false) = false %s %s %s
		ORDER BY ci.closure_initiate_id DESC
		LIMIT NULLIF($1, 0)
	`, ef, bf, df)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	rows, err := scanRows(r)
	if err != nil {
		return nil, err
	}
	return enrichFDClosureInitiateApprovalFields(rows), nil
}

func enrichFDClosureInitiateApprovalFields(rows []map[string]any) []map[string]any {
	for _, row := range rows {
		latest := dashboardStr(row["latest_processing_status"])
		closureStatus := dashboardStr(row["closure_status"])
		effective := effectiveClosureApprovalStatus(latest, closureStatus)
		row["approval_status"] = latest
		row["processing_status"] = effective
	}
	return rows
}

func effectiveClosureApprovalStatus(latest, closureStatus string) string {
	switch strings.ToUpper(strings.TrimSpace(latest)) {
	case "PENDING_APPROVAL", "PENDING_EDIT_APPROVAL", "PENDING_DELETE_APPROVAL", "APPROVED", "REJECTED", "POSTED":
		return strings.ToUpper(strings.TrimSpace(latest))
	}
	return strings.ToUpper(strings.TrimSpace(closureStatus))
}

func dashboardStr(v any) string {
	if v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return t
	case time.Time:
		return t.Format("2006-01-02")
	default:
		return strings.TrimSpace(fmt.Sprint(v))
	}
}

// ─── fdClosureConfirmAll ──────────────────────────────────────────────────────

func queryFDClosureConfirmAll(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	args := []any{limit}
	ef, efArgs := entityFilter(entityIDs, "br", len(args)+1)
	args = append(args, efArgs...)
	bf, bfArgs := bankNameFilter(ctx, "br", len(args)+1)
	args = append(args, bfArgs...)
	df, dfArgs := dateRangeFilter(ctx, "cc", "actual_payout_date", len(args)+1)
	args = append(args, dfArgs...)

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
		WHERE COALESCE(cc.is_deleted, false) = false %s %s %s
		ORDER BY GREATEST(
			COALESCE(l.requested_at,'1970-01-01'::timestamp),
			COALESCE(l.checker_at,'1970-01-01'::timestamp),
			COALESCE(cc.actual_payout_date::timestamp,'1970-01-01'::timestamp)
		) DESC
		LIMIT NULLIF($1, 0)
	`, ef, bf, df)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

// ─── fdClosurePrematureAll ────────────────────────────────────────────────────

func queryFDClosurePrematureAll(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	args := []any{limit}
	ef, efArgs := entityFilter(entityIDs, "br", len(args)+1)
	args = append(args, efArgs...)
	bf, bfArgs := bankNameFilter(ctx, "br", len(args)+1)
	args = append(args, bfArgs...)
	df, dfArgs := dateRangeFilter(ctx, "cc", "actual_payout_date", len(args)+1)
	args = append(args, dfArgs...)

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
		WHERE COALESCE(cc.is_deleted, false) = false AND cc.closure_type = 'PREMATURE' %s %s %s
		ORDER BY GREATEST(
			COALESCE(l.requested_at,'1970-01-01'::timestamp),
			COALESCE(l.checker_at,'1970-01-01'::timestamp),
			COALESCE(cc.actual_payout_date::timestamp,'1970-01-01'::timestamp)
		) DESC
		LIMIT NULLIF($1, 0)
	`, ef, bf, df)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}
