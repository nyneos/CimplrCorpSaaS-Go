package dashboardbuilder

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Run: go test -v -timeout 120s -run TestDataSources ./api/dash/dashboard-builder/
// Connects to the real DB, fires every source, and reports failures.

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=require",
		getEnv("DB_USER", "postgres.zksyhgkeyisnxwvapxqx"),
		getEnv("DB_PASSWORD", "nyneos123"),
		getEnv("DB_HOST", "aws-1-ap-south-1.pooler.supabase.com"),
		getEnv("DB_PORT", "5432"),
		getEnv("DB_NAME", "postgres"),
	)
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	return pool
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func TestDataSources(t *testing.T) {
	pool := testPool(t)
	defer pool.Close()
	ctx := context.Background()

	entityIDs := []string{}
	limit := 5
	parentID := ""

	type tc struct {
		name string
		fn   func() ([]map[string]any, error)
	}

	cases := []tc{
		// FD
		{"fdBooking", func() ([]map[string]any, error) { return queryFDBooking(ctx, pool, entityIDs, limit) }},
		{"fdConfirmation", func() ([]map[string]any, error) { return queryFDConfirmation(ctx, pool, entityIDs, limit) }},
		{"fdActivation", func() ([]map[string]any, error) { return queryFDActivation(ctx, pool, entityIDs, limit) }},
		{"fdCashflowGroup", func() ([]map[string]any, error) { return queryFDCashflowGroup(ctx, pool, entityIDs, limit) }},
		{"fdCashflows", func() ([]map[string]any, error) { return queryFDCashflows(ctx, pool, entityIDs, limit, parentID) }},
		{"fdClosureInitiateAll", func() ([]map[string]any, error) { return queryFDClosureInitiateAll(ctx, pool, entityIDs, limit) }},
		{"fdClosureConfirmAll", func() ([]map[string]any, error) { return queryFDClosureConfirmAll(ctx, pool, entityIDs, limit) }},
		{"fdClosurePrematureAll", func() ([]map[string]any, error) { return queryFDClosurePrematureAll(ctx, pool, entityIDs, limit) }},
		// Maturity & Receipt
		{"fdMaturitySummary", func() ([]map[string]any, error) { return queryFDMaturitySummary(ctx, pool, entityIDs, limit) }},
		{"fdTdsRegister", func() ([]map[string]any, error) { return queryFDTDSRegister(ctx, pool, entityIDs, limit) }},
		{"fdReceiptAll", func() ([]map[string]any, error) { return queryFDReceiptAll(ctx, pool, entityIDs, limit) }},
		{"fdReconcileResults", func() ([]map[string]any, error) { return queryFDReconcileResults(ctx, pool, entityIDs, limit) }},
		{"fdExceptions", func() ([]map[string]any, error) { return queryFDExceptions(ctx, pool, entityIDs, limit) }},
		// Accrual
		{"fdAccrualRunAll", func() ([]map[string]any, error) { return queryFDAccrualRunAll(ctx, pool, entityIDs, limit) }},
		{"fdAccrualLedger", func() ([]map[string]any, error) { return queryFDAccrualLedger(ctx, pool, entityIDs, limit, parentID) }},
		{"fdAccrualDetail", func() ([]map[string]any, error) { return queryFDAccrualDetail(ctx, pool, entityIDs, limit) }},
		{"fdAccrualFindings", func() ([]map[string]any, error) { return queryFDAccrualFindings(ctx, pool, entityIDs, limit) }},
		{"fdAccrualExecutionLog", func() ([]map[string]any, error) { return queryFDAccrualExecutionLog(ctx, pool, entityIDs, limit) }},
		{"fdAccrualRunAudit", func() ([]map[string]any, error) { return queryFDAccrualRunAudit(ctx, pool, entityIDs, limit) }},
		{"fdAccrualLedgerAudit", func() ([]map[string]any, error) { return queryFDAccrualLedgerAudit(ctx, pool, entityIDs, limit) }},
		{"fdAccrualExceptions", func() ([]map[string]any, error) { return queryFDAccrualExceptions(ctx, pool, entityIDs, limit) }},
		{"fdAccrualScheduleAll", func() ([]map[string]any, error) { return queryFDAccrualScheduleAll(ctx, pool, entityIDs, limit) }},
		{"fdAccrualScheduleExecutionLog", func() ([]map[string]any, error) { return queryFDAccrualScheduleExecutionLog(ctx, pool, entityIDs, limit) }},
		// Portfolio
		{"investmentOnboardBatch", func() ([]map[string]any, error) { return queryInvestmentOnboardBatch(ctx, pool, entityIDs, limit) }},
		{"investmentOnboardBatchInfo", func() ([]map[string]any, error) { return queryInvestmentOnboardBatchInfo(ctx, pool, entityIDs, limit, parentID) }},
		{"investmentProposalMeta", func() ([]map[string]any, error) { return queryInvestmentProposalMeta(ctx, pool, entityIDs, limit) }},
		{"investmentInitiationAll", func() ([]map[string]any, error) { return queryInvestmentInitiationAll(ctx, pool, entityIDs, limit) }},
		{"investmentConfirmationAll", func() ([]map[string]any, error) { return queryInvestmentConfirmationAll(ctx, pool, entityIDs, limit) }},
		{"investmentPortfolioGet", func() ([]map[string]any, error) { return queryInvestmentPortfolioGet(ctx, pool, entityIDs, limit) }},
		{"investmentRedemptionInitiateAll", func() ([]map[string]any, error) { return queryInvestmentRedemptionInitiateAll(ctx, pool, entityIDs, limit) }},
		{"investmentRedemptionConfirmAll", func() ([]map[string]any, error) { return queryInvestmentRedemptionConfirmAll(ctx, pool, entityIDs, limit) }},
		// Cash
		{"cashBankStatements", func() ([]map[string]any, error) { return queryCashBankStatements(ctx, pool, entityIDs, limit) }},
		{"cashBankStatementTransactions", func() ([]map[string]any, error) { return queryCashBankStatementTransactions(ctx, pool, entityIDs, limit, parentID) }},
		{"cashPayableReceivable", func() ([]map[string]any, error) { return queryCashPayableReceivable(ctx, pool, entityIDs, limit) }},
		{"cashFundPlanSummary", func() ([]map[string]any, error) { return queryCashFundPlanSummary(ctx, pool, entityIDs, limit) }},
		{"cashFundPlanDetails", func() ([]map[string]any, error) { return queryCashFundPlanDetails(ctx, pool, entityIDs, limit, parentID) }},
		{"cashSweepConfig", func() ([]map[string]any, error) { return queryCashSweepConfig(ctx, pool, entityIDs, limit) }},
		{"cashSweepInitiation", func() ([]map[string]any, error) { return queryCashSweepInitiation(ctx, pool, entityIDs, limit) }},
		{"cashSweepExecutionLogs", func() ([]map[string]any, error) { return queryCashSweepExecutionLogs(ctx, pool, entityIDs, limit) }},
		{"cashSweepStatistics", func() ([]map[string]any, error) { return queryCashSweepStatistics(ctx, pool, entityIDs, limit) }},
		{"cashProjectionList", func() ([]map[string]any, error) { return queryCashProjectionList(ctx, pool, entityIDs, limit) }},
		{"cashProjectionDetail", func() ([]map[string]any, error) { return queryCashProjectionDetail(ctx, pool, entityIDs, limit, parentID) }},
		{"cashBankBalances", func() ([]map[string]any, error) { return queryCashBankBalances(ctx, pool, entityIDs, limit) }},
		{"cashFundAvailability", func() ([]map[string]any, error) { return queryCashFundAvailability(ctx, pool, entityIDs, limit) }},
		{"cashBankLimits", func() ([]map[string]any, error) { return queryCashBankLimits(ctx, pool, entityIDs, limit) }},
		{"cashUtilizations", func() ([]map[string]any, error) { return queryCashUtilizations(ctx, pool, entityIDs, limit) }},
		// FX
		{"fxExposureHeadersLineItems", func() ([]map[string]any, error) { return queryFXExposureHeadersLineItems(ctx, pool, entityIDs, limit) }},
		{"fxExposureBucketing", func() ([]map[string]any, error) { return queryFXExposureBucketing(ctx, pool, entityIDs, limit) }},
		{"fxHedgingProposals", func() ([]map[string]any, error) { return queryFXHedgingProposals(ctx, pool, entityIDs, limit) }},
		{"fxHedgeLinksDetails", func() ([]map[string]any, error) { return queryFXHedgeLinksDetails(ctx, pool, entityIDs, limit) }},
		{"fxForwardMTM", func() ([]map[string]any, error) { return queryFXForwardMTM(ctx, pool, entityIDs, limit) }},
		{"fxForwardBookingList", func() ([]map[string]any, error) { return queryFXForwardBookings(ctx, pool, entityIDs, limit) }},
		// Audit
		{"fdBookingAudit", func() ([]map[string]any, error) { return queryFDBookingAudit(ctx, pool, entityIDs, limit, parentID) }},
		{"fdConfirmationAudit", func() ([]map[string]any, error) { return queryFDConfirmationAudit(ctx, pool, entityIDs, limit, parentID) }},
		{"fdActivationAudit", func() ([]map[string]any, error) { return queryFDActivationAudit(ctx, pool, entityIDs, limit, parentID) }},
		{"fdCashflowAudit", func() ([]map[string]any, error) { return queryFDCashflowAudit(ctx, pool, entityIDs, limit, parentID) }},
		{"fdClosureInitiateAudit", func() ([]map[string]any, error) { return queryFDClosureInitiateAudit(ctx, pool, entityIDs, limit, parentID) }},
		{"fdClosureConfirmAudit", func() ([]map[string]any, error) { return queryFDClosureConfirmAudit(ctx, pool, entityIDs, limit, parentID) }},
		{"fdTdsAudit", func() ([]map[string]any, error) { return queryFDTdsAudit(ctx, pool, entityIDs, limit, parentID) }},
		{"fdReceiptAudit", func() ([]map[string]any, error) { return queryFDReceiptAudit(ctx, pool, entityIDs, limit, parentID) }},
		{"fdExceptionAudit", func() ([]map[string]any, error) { return queryFDExceptionAudit(ctx, pool, entityIDs, limit, parentID) }},
		{"cashBankStatementAudit", func() ([]map[string]any, error) { return queryCashBankStatementAudit(ctx, pool, entityIDs, limit, parentID) }},
		{"cashSweepConfigAudit", func() ([]map[string]any, error) { return queryCashSweepConfigAudit(ctx, pool, entityIDs, limit, parentID) }},
		{"cashSweepInitiationAudit", func() ([]map[string]any, error) { return queryCashSweepInitiationAudit(ctx, pool, entityIDs, limit, parentID) }},
		{"cashProjectionAudit", func() ([]map[string]any, error) { return queryCashProjectionAudit(ctx, pool, entityIDs, limit, parentID) }},
		{"cashFundPlanAudit", func() ([]map[string]any, error) { return queryCashFundPlanAudit(ctx, pool, entityIDs, limit, parentID) }},
		{"investmentProposalAudit", func() ([]map[string]any, error) { return queryInvestmentProposalAudit(ctx, pool, entityIDs, limit, parentID) }},
		{"investmentInitiationAudit", func() ([]map[string]any, error) { return queryInvestmentInitiationAudit(ctx, pool, entityIDs, limit, parentID) }},
		{"investmentConfirmationAudit", func() ([]map[string]any, error) { return queryInvestmentConfirmationAudit(ctx, pool, entityIDs, limit, parentID) }},
		{"investmentRedemptionAudit", func() ([]map[string]any, error) { return queryInvestmentRedemptionAudit(ctx, pool, entityIDs, limit, parentID) }},
		{"investmentRedemptionConfirmAudit", func() ([]map[string]any, error) { return queryInvestmentRedemptionConfirmAudit(ctx, pool, entityIDs, limit, parentID) }},
		{"fxForwardBookingAudit", func() ([]map[string]any, error) { return queryFXForwardBookingAudit(ctx, pool, entityIDs, limit, parentID) }},
		{"fxExposureAudit", func() ([]map[string]any, error) { return queryFXExposureAudit(ctx, pool, entityIDs, limit, parentID) }},
	}

	pass, fail := 0, 0
	for _, c := range cases {
		_, err := c.fn()
		if err != nil {
			t.Errorf("FAIL %-40s %v", c.name, err)
			fail++
		} else {
			t.Logf("OK   %s", c.name)
			pass++
		}
	}
	t.Logf("\n=== SUMMARY: %d passed, %d failed out of %d ===", pass, fail, pass+fail)
}
