package fdAccrual

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ─── Structs ──────────────────────────────────────────────────────────────────

// AccrualInput holds every field the engine needs for a single FD.
type AccrualInput struct {
	FDID             string
	FdRefNo          string  // bank_fd_ref_no
	BankID           string
	BankName         string
	EntityID         string
	EntityName       string
	InterestTypeCode string  // SIMPLE / COMPOUND / STEPPED
	PrincipalAmount  float64
	InterestRate     float64 // annual percentage e.g. 7.5
	DayCountCode     string  // ACT_365 / ACT_360 / 30_360
	FdStartDate      time.Time
	FdMaturityDate   time.Time
}

// AccrualRunParams controls what the engine calculates.
type AccrualRunParams struct {
	PeriodStart        time.Time
	PeriodEnd          time.Time
	FinancialPeriod    string
	DayCountConvention string   // overrides per-FD code if set
	RoundingRule       string   // ROUND / TRUNCATE
	PrecisionDecimals  int
	EntityID           string   // for scope
	BankIDFilter       string   // optional
	FDStatusFilter     string   // ACTIVE default
	FDInclusionMethod  string   // ALL / SELECT_LIST
	FDInclusionList    []string
}

// AccrualPeriodResult holds the engine output for one FD x one period.
type AccrualPeriodResult struct {
	FDID             string
	FdRefNo          string
	BankID           string
	BankName         string
	EntityID         string
	EntityName       string
	InterestTypeCode string
	PrincipalAmount  float64
	InterestRate     float64
	DayCountCode     string
	FdStartDate      time.Time
	FdMaturityDate   time.Time

	// Period
	AccrualPeriodStart time.Time
	AccrualPeriodEnd   time.Time
	AccrualDays        int

	// Calculation inputs
	OpeningPrincipal float64
	DailyAccrualRate float64
	Divisor          int

	// Outputs
	PeriodInterestAccrued    float64
	OpeningAccruedBalance    float64
	InterestReceivedInPeriod float64
	ClosingAccruedBalance    float64
	TDSApplicableAmount      float64
	TDSDeductedInPeriod      float64
	NetInterestInPeriod      float64

	// References
	CashflowRowIDs   []string // cashflow_ids used in this calc
	FormulaUsed      string
	LedgerRowStatus  string // CALCULATED / ERROR / EXCLUDED
	CalculationError string
}

// ValidationFinding is a single validation issue.
type ValidationFinding struct {
	FDID            string
	FdRefNo         string
	BankName        string
	IssueType       string // MISSING_RATE / ZERO_PRINCIPAL / etc.
	Severity        string // BLOCKER / WARNING / INFO
	Description     string
	SuggestedAction string
}

// CreateAccrualRunInput is used by the internal scheduler to create a run.
type CreateAccrualRunInput struct {
	RunType            string
	RunMode            string
	EntityID           string
	EntityName         string
	BankIDFilter       string
	FDStatusFilter     string
	AccrualPeriodStart time.Time
	AccrualPeriodEnd   time.Time
	FinancialPeriod    string
	DayCountConvention string
	RoundingRule       string
	PrecisionDecimals  int
	CreatedBy          string
}

// ScheduleConfigRow holds a row from fd_accrual_schedule_config.
type ScheduleConfigRow struct {
	ConfigID               string
	EntityID               string
	EntityName             string
	ScheduleFrequency      string
	RunDayOfMonth          *int
	RunTime                *time.Time
	DefaultBankIDFilter    string
	DefaultFDStatusFilter  string
	DefaultRunMode         string
	AutoSubmitForApproval  bool
	NotificationRecipients []byte // JSONB raw
}

// ─── Day-count helpers ────────────────────────────────────────────────────────

// getDivisorForAccrual returns the annual divisor. Never returns 0.
func getDivisorForAccrual(dayCountCode string, refDate time.Time) int {
	switch strings.ToUpper(dayCountCode) {
	case "ACT_365":
		return 365
	case "ACT_360":
		return 360
	case "30_360":
		return 360
	case "ACT_ACT":
		year := refDate.Year()
		if year%400 == 0 || (year%4 == 0 && year%100 != 0) {
			return 366
		}
		return 365
	default:
		return 365
	}
}

// roundAccrual rounds to the given decimals using the specified rule.
func roundAccrual(amount float64, decimals int, rule string) float64 {
	if decimals <= 0 {
		decimals = 2
	}
	pow := math.Pow(10, float64(decimals))
	if strings.EqualFold(rule, "TRUNCATE") {
		return math.Floor(amount*pow) / pow
	}
	return math.Round(amount*pow) / pow
}

// buildAccrualPeriod returns "APR 2025" uppercase month-year string.
func buildAccrualPeriod(t time.Time) string {
	return strings.ToUpper(t.Format("Jan 2006"))
}

// ─── Scope query ──────────────────────────────────────────────────────────────

// getFDsInScope queries investment.fd_master using exact column names.
func getFDsInScope(ctx context.Context, pool *pgxpool.Pool, params AccrualRunParams) ([]AccrualInput, error) {
	fdStatus := params.FDStatusFilter
	if fdStatus == "" {
		fdStatus = "ACTIVE"
	}

	query := `
		SELECT
			fd_id,
			COALESCE(bank_fd_ref_no, '')           AS fd_ref_no,
			COALESCE(bank_id, '')                   AS bank_id,
			COALESCE(bank_name, '')                 AS bank_name,
			COALESCE(entity_id, '')                 AS entity_id,
			COALESCE(entity_name, '')               AS entity_name,
			COALESCE(interest_type_code, 'SIMPLE')  AS interest_type_code,
			COALESCE(principal_amount, 0)           AS principal_amount,
			COALESCE(interest_rate, 0)              AS interest_rate,
			COALESCE(day_count_code, 'ACT_365')     AS day_count_code,
			start_date,
			maturity_date
		FROM investment.fd_master
		WHERE entity_id = $1
		  AND fd_status = $2
		  AND cashflow_generated = true
		  AND is_deleted = false
		  AND is_active = true
		  AND start_date <= $3
		  AND maturity_date >= $4`

	args := []interface{}{params.EntityID, fdStatus, params.PeriodEnd, params.PeriodStart}
	argIdx := 5

	if params.BankIDFilter != "" {
		query += fmt.Sprintf(" AND bank_id = $%d", argIdx)
		args = append(args, params.BankIDFilter)
		argIdx++
	}
	if params.FDInclusionMethod == "SELECT_LIST" && len(params.FDInclusionList) > 0 {
		query += fmt.Sprintf(" AND fd_id = ANY($%d)", argIdx)
		args = append(args, params.FDInclusionList)
		argIdx++
	}
	query += " ORDER BY bank_name, start_date"

	rows, err := pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getFDsInScope: %w", err)
	}
	defer rows.Close()

	var results []AccrualInput
	for rows.Next() {
		var fd AccrualInput
		if err := rows.Scan(
			&fd.FDID, &fd.FdRefNo, &fd.BankID, &fd.BankName,
			&fd.EntityID, &fd.EntityName, &fd.InterestTypeCode,
			&fd.PrincipalAmount, &fd.InterestRate, &fd.DayCountCode,
			&fd.FdStartDate, &fd.FdMaturityDate,
		); err != nil {
			return nil, fmt.Errorf("getFDsInScope scan: %w", err)
		}
		results = append(results, fd)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("getFDsInScope rows.Err: %w", err)
	}
	return results, nil
}

// ─── Validation ───────────────────────────────────────────────────────────────

// validateFDsForAccrual checks each FD for blockers and warnings.
func validateFDsForAccrual(fds []AccrualInput, params AccrualRunParams) []ValidationFinding {
	var findings []ValidationFinding
	for _, fd := range fds {
		if fd.PrincipalAmount <= 0 {
			findings = append(findings, ValidationFinding{
				FDID: fd.FDID, FdRefNo: fd.FdRefNo, BankName: fd.BankName,
				IssueType:       "ZERO_PRINCIPAL",
				Severity:        "BLOCKER",
				Description:     "Principal amount is zero or negative",
				SuggestedAction: "Update FD principal before running accrual",
			})
		}
		if fd.InterestRate <= 0 {
			findings = append(findings, ValidationFinding{
				FDID: fd.FDID, FdRefNo: fd.FdRefNo, BankName: fd.BankName,
				IssueType:       "MISSING_RATE",
				Severity:        "BLOCKER",
				Description:     "Interest rate is zero or negative",
				SuggestedAction: "Set a positive interest rate on the FD",
			})
		}
		if fd.DayCountCode == "" {
			findings = append(findings, ValidationFinding{
				FDID: fd.FDID, FdRefNo: fd.FdRefNo, BankName: fd.BankName,
				IssueType:       "MISSING_DAY_COUNT",
				Severity:        "BLOCKER",
				Description:     "Day count code is missing",
				SuggestedAction: "Set day_count_code on the FD (ACT_365 / ACT_360 / 30_360)",
			})
		}
		if !fd.FdMaturityDate.IsZero() && fd.FdMaturityDate.Before(params.PeriodStart) {
			findings = append(findings, ValidationFinding{
				FDID: fd.FDID, FdRefNo: fd.FdRefNo, BankName: fd.BankName,
				IssueType: "MATURITY_BEFORE_PERIOD",
				Severity:  "BLOCKER",
				Description: fmt.Sprintf("FD matured on %s before period start %s",
					fd.FdMaturityDate.Format("2006-01-02"), params.PeriodStart.Format("2006-01-02")),
				SuggestedAction: "Exclude this FD from the run or verify maturity date",
			})
		}
		if !fd.FdStartDate.IsZero() && fd.FdStartDate.After(params.PeriodEnd) {
			findings = append(findings, ValidationFinding{
				FDID: fd.FDID, FdRefNo: fd.FdRefNo, BankName: fd.BankName,
				IssueType: "FD_NOT_STARTED_IN_PERIOD",
				Severity:  "BLOCKER",
				Description: fmt.Sprintf("FD start date %s is after period end %s",
					fd.FdStartDate.Format("2006-01-02"), params.PeriodEnd.Format("2006-01-02")),
				SuggestedAction: "Exclude this FD or adjust the accrual period",
			})
		}
	}
	return findings
}

// hasBlockers returns true if any finding has severity BLOCKER.
func hasBlockers(findings []ValidationFinding) bool {
	for _, f := range findings {
		if f.Severity == "BLOCKER" {
			return true
		}
	}
	return false
}

// ─── DB-backed calculation helpers ───────────────────────────────────────────

// getCompoundPrincipalAtDate returns MAX(closing_principal) from CAPITALIZATION rows.
func getCompoundPrincipalAtDate(ctx context.Context, pool *pgxpool.Pool,
	fdID string, atDate time.Time) float64 {

	var val float64
	_ = pool.QueryRow(ctx, `
		SELECT COALESCE(MAX(closing_principal), 0)
		FROM investment.fd_cashflow_schedule
		WHERE fd_id = $1
		  AND event_type = 'CAPITALIZATION'
		  AND event_date <= $2
		  AND COALESCE(is_deleted, false) = false`,
		fdID, atDate,
	).Scan(&val)
	return val
}

// getCashflowDataForPeriod sums interest receipts and TDS deductions in the period.
func getCashflowDataForPeriod(ctx context.Context, pool *pgxpool.Pool,
	fdID string, periodStart, periodEnd time.Time) (interestReceived float64, tdsDeducted float64, cashflowIDs []string) {

	rows, err := pool.Query(ctx, `
		SELECT cashflow_id, event_type,
		       COALESCE(net_cash_flow, 0),
		       COALESCE(tds_amount, 0)
		FROM investment.fd_cashflow_schedule
		WHERE fd_id = $1
		  AND event_date >= $2
		  AND event_date <= $3
		  AND event_type IN ('INTEREST_RECEIPT', 'TDS_DEDUCTION')
		  AND COALESCE(is_deleted, false) = false`,
		fdID, periodStart, periodEnd,
	)
	if err != nil {
		return 0, 0, nil
	}
	defer rows.Close()

	for rows.Next() {
		var cashflowID, eventType string
		var netCash, tdsAmt float64
		if err := rows.Scan(&cashflowID, &eventType, &netCash, &tdsAmt); err != nil {
			continue
		}
		cashflowIDs = append(cashflowIDs, cashflowID)
		if eventType == "INTEREST_RECEIPT" {
			interestReceived += netCash
		}
		if eventType == "TDS_DEDUCTION" {
			tdsDeducted += tdsAmt
		}
	}
	return interestReceived, tdsDeducted, cashflowIDs
}

// getPriorRunClosingBalance fetches closing_accrued_balance from the most recent
// prior FINAL+POSTED ledger row for this FD.
func getPriorRunClosingBalance(ctx context.Context, pool *pgxpool.Pool,
	entityID string, fdID string, currentPeriodStart time.Time) float64 {

	var closing float64
	_ = pool.QueryRow(ctx, `
		SELECT COALESCE(l.closing_accrued_balance, 0)
		FROM investment.fd_accrual_ledger l
		JOIN investment.fd_accrual_run r ON r.run_id = l.run_id
		WHERE l.fd_id = $1
		  AND r.entity_id = $2
		  AND r.run_mode = 'FINAL'
		  AND r.run_status = 'POSTED'
		  AND r.accrual_period_end < $3
		  AND COALESCE(l.is_deleted, false) = false
		ORDER BY r.accrual_period_end DESC
		LIMIT 1`,
		fdID, entityID, currentPeriodStart,
	).Scan(&closing)
	return closing
}

// ─── Core calculation ─────────────────────────────────────────────────────────

// calculateAccrualForFD computes the accrual for a single FD over the run period.
func calculateAccrualForFD(ctx context.Context, pool *pgxpool.Pool,
	fd AccrualInput, params AccrualRunParams, openingBalance float64) AccrualPeriodResult {

	excluded := AccrualPeriodResult{
		FDID: fd.FDID, FdRefNo: fd.FdRefNo, BankID: fd.BankID, BankName: fd.BankName,
		EntityID: fd.EntityID, EntityName: fd.EntityName,
		InterestTypeCode: fd.InterestTypeCode,
		PrincipalAmount:  fd.PrincipalAmount, InterestRate: fd.InterestRate,
		FdStartDate: fd.FdStartDate, FdMaturityDate: fd.FdMaturityDate,
		LedgerRowStatus: "EXCLUDED",
	}

	effectiveStart := fd.FdStartDate
	if params.PeriodStart.After(effectiveStart) {
		effectiveStart = params.PeriodStart
	}
	effectiveEnd := fd.FdMaturityDate
	if params.PeriodEnd.Before(effectiveEnd) {
		effectiveEnd = params.PeriodEnd
	}

	if !effectiveStart.Before(effectiveEnd) {
		excluded.CalculationError = "effective period is zero or negative"
		return excluded
	}

	accrualDays := int(effectiveEnd.Sub(effectiveStart).Hours() / 24)
	if accrualDays <= 0 {
		excluded.CalculationError = "accrual days <= 0"
		return excluded
	}

	dayCountCode := fd.DayCountCode
	if params.DayCountConvention != "" {
		dayCountCode = params.DayCountConvention
	}
	divisor := getDivisorForAccrual(dayCountCode, effectiveStart)

	openingPrincipal := fd.PrincipalAmount
	if strings.EqualFold(fd.InterestTypeCode, "COMPOUND") {
		if cp := getCompoundPrincipalAtDate(ctx, pool, fd.FDID, effectiveStart); cp > 0 {
			openingPrincipal = cp
		}
	}

	decimals := params.PrecisionDecimals
	if decimals <= 0 {
		decimals = 2
	}

	dailyRate := (fd.InterestRate / 100.0) / float64(divisor)
	rawInterest := openingPrincipal * dailyRate * float64(accrualDays)
	periodInterest := roundAccrual(rawInterest, decimals, params.RoundingRule)

	interestReceived, tdsDeducted, cashflowIDs := getCashflowDataForPeriod(
		ctx, pool, fd.FDID, effectiveStart, effectiveEnd)

	tdsApplicable := periodInterest
	closingBalance := openingBalance + periodInterest - interestReceived
	netInterest := periodInterest - tdsDeducted

	formula := fmt.Sprintf("P(%.2f) × r(%.4f%%) × d(%d) / D(%d) = %.6f",
		openingPrincipal, fd.InterestRate, accrualDays, divisor, periodInterest)

	// marshal cashflowIDs JSON for DB storage (used in run.go)
	cashflowIDsJSON, _ := json.Marshal(cashflowIDs)
	_ = cashflowIDsJSON

	return AccrualPeriodResult{
		FDID: fd.FDID, FdRefNo: fd.FdRefNo, BankID: fd.BankID, BankName: fd.BankName,
		EntityID: fd.EntityID, EntityName: fd.EntityName,
		InterestTypeCode: fd.InterestTypeCode,
		PrincipalAmount:  fd.PrincipalAmount, InterestRate: fd.InterestRate,
		DayCountCode:   dayCountCode,
		FdStartDate:    fd.FdStartDate,
		FdMaturityDate: fd.FdMaturityDate,

		AccrualPeriodStart: effectiveStart,
		AccrualPeriodEnd:   effectiveEnd,
		AccrualDays:        accrualDays,

		OpeningPrincipal: openingPrincipal,
		DailyAccrualRate: dailyRate,
		Divisor:          divisor,

		PeriodInterestAccrued:    periodInterest,
		OpeningAccruedBalance:    openingBalance,
		InterestReceivedInPeriod: interestReceived,
		ClosingAccruedBalance:    closingBalance,
		TDSApplicableAmount:      tdsApplicable,
		TDSDeductedInPeriod:      tdsDeducted,
		NetInterestInPeriod:      netInterest,

		CashflowRowIDs:  cashflowIDs,
		FormulaUsed:     formula,
		LedgerRowStatus: "CALCULATED",
	}
}
