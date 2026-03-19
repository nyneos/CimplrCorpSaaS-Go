package fdAccrual

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// ─── queryExecutor abstracts pgx.Tx and pgxpool.Pool ─────────────────────────

type queryExecutor interface {
	QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row
	Exec(ctx context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error)
}

// ─── Structs ─────────────────────────────────────────────────────────────────

// AccrualInput holds every field the engine needs for a single FD.
type AccrualInput struct {
	FDID                    string
	ConfirmationID          string
	BookingID               string
	EntityID                string
	BankID                  string
	BankAccountID           string
	BankConfigID            string
	PrincipalAmount         float64
	InterestRate            float64
	TenorDays               int
	ValueDate               time.Time
	MaturityDate            time.Time
	MaturityAmount          float64
	InterestType            string  // SIMPLE / COMPOUND
	InterestPayoutFrequency string  // MONTHLY / QUARTERLY / AT_MATURITY
	CompoundingFrequency    string  // QUARTERLY / MONTHLY
	DayCountConvention      string  // ACT_365 / ACT_360 / 30_360
	Currency                string
	TDSPlanID               string
	TDSRate                 float64 // resolved from plan
	BankFDReference         string
	FDStatus                string
}

// AccrualPeriodResult holds the engine output for one FD x one period.
type AccrualPeriodResult struct {
	FDID              string
	PeriodStart       time.Time
	PeriodEnd         time.Time
	DaysInPeriod      int
	OpeningPrincipal  float64
	GrossInterest     float64
	TDSAmount         float64
	NetInterest       float64
	ClosingPrincipal  float64
	CumulativeAccrual float64
}

// AccrualRunParams controls what the engine calculates.
type AccrualRunParams struct {
	RunMode       string    // FULL / SIMULATION
	PeriodStart   time.Time
	PeriodEnd     time.Time
	EntityIDs     []string  // scope filter (nil = all)
	BankIDs       []string
	FDIDs         []string
	RoundDecimals int       // default 2
}

// AccrualLedgerRow matches the fd_accrual_ledger table structure.
type AccrualLedgerRow struct {
	RunID             string
	FDID              string
	PeriodStart       time.Time
	PeriodEnd         time.Time
	DaysInPeriod      int
	DayCountUsed      string
	OpeningPrincipal  float64
	GrossInterest     float64
	TDSAmount         float64
	NetInterest       float64
	ClosingBalance    float64
	CumulativeAccrual float64
	IsOverridden      bool
	OverrideAmount    *float64
	OverrideReason    string
}

// ValidationFinding is a single validation issue.
type ValidationFinding struct {
	FDID     string
	Severity string // ERROR / WARNING / INFO
	Code     string
	Message  string
}

// ─── Day-count helpers ───────────────────────────────────────────────────────

// getDivisorForAccrual returns the annual divisor for the day count convention.
func getDivisorForAccrual(convention string) float64 {
	switch convention {
	case "ACT_360", "30_360":
		return 360
	case "ACT_365":
		return 365
	default:
		return 365 // default to ACT/365
	}
}

// daysInPeriod returns actual calendar days between two dates.
func daysInPeriod(start, end time.Time) int {
	d := int(end.Sub(start).Hours() / 24)
	if d < 0 {
		return 0
	}
	return d
}

// roundAccrual rounds to the given number of decimal places.
func roundAccrual(val float64, decimals int) float64 {
	if decimals <= 0 {
		decimals = 2
	}
	pow := math.Pow(10, float64(decimals))
	return math.Round(val*pow) / pow
}

// buildAccrualPeriod returns "YYYY-MM" for accounting period identification.
func buildAccrualPeriod(date time.Time) string {
	return date.Format("2006-01")
}

// ─── Core calculation ────────────────────────────────────────────────────────

// calculateAccrualForFD computes the accrual for a single FD for the given period.
func calculateAccrualForFD(fd *AccrualInput, params *AccrualRunParams, priorClosing float64) AccrualPeriodResult {
	decimals := params.RoundDecimals
	if decimals <= 0 {
		decimals = 2
	}

	days := daysInPeriod(params.PeriodStart, params.PeriodEnd)
	if days <= 0 {
		return AccrualPeriodResult{
			FDID:             fd.FDID,
			PeriodStart:      params.PeriodStart,
			PeriodEnd:        params.PeriodEnd,
			DaysInPeriod:     0,
			OpeningPrincipal: priorClosing,
			ClosingPrincipal: priorClosing,
		}
	}

	principal := priorClosing
	if principal == 0 {
		principal = fd.PrincipalAmount
	}

	divisor := getDivisorForAccrual(fd.DayCountConvention)
	rate := fd.InterestRate / 100.0

	var grossInterest float64
	if fd.InterestType == "COMPOUND" {
		grossInterest = getCompoundInterestForPeriod(principal, rate, divisor, days)
	} else {
		// Simple interest: P x R x D / Divisor
		grossInterest = principal * rate * float64(days) / divisor
	}
	grossInterest = roundAccrual(grossInterest, decimals)

	tdsAmount := 0.0
	if fd.TDSRate > 0 {
		tdsAmount = roundAccrual(grossInterest*fd.TDSRate/100.0, decimals)
	}
	netInterest := roundAccrual(grossInterest-tdsAmount, decimals)

	closingPrincipal := principal
	// For compound FDs, interest is added to principal
	if fd.InterestType == "COMPOUND" && fd.InterestPayoutFrequency == "AT_MATURITY" {
		closingPrincipal = roundAccrual(principal+grossInterest, decimals)
	}

	return AccrualPeriodResult{
		FDID:             fd.FDID,
		PeriodStart:      params.PeriodStart,
		PeriodEnd:        params.PeriodEnd,
		DaysInPeriod:     days,
		OpeningPrincipal: principal,
		GrossInterest:    grossInterest,
		TDSAmount:        tdsAmount,
		NetInterest:      netInterest,
		ClosingPrincipal: closingPrincipal,
	}
}

// getCompoundInterestForPeriod calculates compound interest for a period.
// Formula: P x ((1 + r/n)^(n x d/D) - 1) where n = compounding periods/year.
func getCompoundInterestForPeriod(principal, annualRate, divisor float64, days int) float64 {
	if principal <= 0 || annualRate <= 0 || days <= 0 {
		return 0
	}
	// Treat as daily compounding over the period
	dailyRate := annualRate / divisor
	factor := math.Pow(1+dailyRate, float64(days))
	return principal * (factor - 1)
}

// getCompoundPrincipalAtDate returns the compounded principal on a given date
// from value_date for cumulative compound FDs.
func getCompoundPrincipalAtDate(fd *AccrualInput, targetDate time.Time) float64 {
	if fd.InterestType != "COMPOUND" || fd.InterestPayoutFrequency != "AT_MATURITY" {
		return fd.PrincipalAmount
	}
	days := daysInPeriod(fd.ValueDate, targetDate)
	if days <= 0 {
		return fd.PrincipalAmount
	}
	divisor := getDivisorForAccrual(fd.DayCountConvention)
	dailyRate := (fd.InterestRate / 100.0) / divisor
	return fd.PrincipalAmount * math.Pow(1+dailyRate, float64(days))
}

// ─── DB-backed helpers ───────────────────────────────────────────────────────

// getFDsInScope returns all active/confirmed FDs matching the run scope.
func getFDsInScope(ctx context.Context, exec queryExecutor, params *AccrualRunParams) ([]*AccrualInput, error) {
	query := `
		SELECT
			fm.fd_id,
			COALESCE(fm.confirmation_id,''),
			COALESCE(fm.booking_id,''),
			COALESCE(fm.entity_id,''),
			COALESCE(fm.bank_id,''),
			COALESCE(fm.bank_account_id,''),
			COALESCE(fm.bank_config_id,''),
			COALESCE(fm.principal_amount,0),
			COALESCE(fm.interest_rate,0),
			COALESCE(fm.tenor_days,0),
			COALESCE(fm.value_date, fm.created_at::date),
			COALESCE(fm.maturity_date, fm.value_date + fm.tenor_days * interval '1 day'),
			COALESCE(fm.maturity_amount,0),
			COALESCE(fm.interest_type,'SIMPLE'),
			COALESCE(fm.interest_payout_frequency,'AT_MATURITY'),
			COALESCE(fm.compounding_frequency,''),
			COALESCE(fm.day_count_convention,'ACT_365'),
			COALESCE(fm.currency,'INR'),
			COALESCE(fm.tds_plan_id,''),
			COALESCE(fm.bank_fd_reference,''),
			COALESCE(fm.fd_status,'')
		FROM investment.fd_master fm
		WHERE fm.fd_status IN ('ACTIVE','MATURED')
		  AND COALESCE(fm.is_deleted, false) = false
		  AND fm.value_date <= $1
		  AND fm.maturity_date >= $2`

	args := []interface{}{params.PeriodEnd, params.PeriodStart}
	argIdx := 3

	if len(params.EntityIDs) > 0 {
		query += fmt.Sprintf(" AND fm.entity_id = ANY($%d::text[])", argIdx)
		args = append(args, params.EntityIDs)
		argIdx++
	}
	if len(params.BankIDs) > 0 {
		query += fmt.Sprintf(" AND fm.bank_id = ANY($%d::text[])", argIdx)
		args = append(args, params.BankIDs)
		argIdx++
	}
	if len(params.FDIDs) > 0 {
		query += fmt.Sprintf(" AND fm.fd_id = ANY($%d::text[])", argIdx)
		args = append(args, params.FDIDs)
		argIdx++
	}

	query += " ORDER BY fm.entity_id, fm.value_date"

	rows, err := exec.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getFDsInScope: %w", err)
	}
	defer rows.Close()

	var results []*AccrualInput
	for rows.Next() {
		fd := &AccrualInput{}
		if err := rows.Scan(
			&fd.FDID, &fd.ConfirmationID, &fd.BookingID,
			&fd.EntityID, &fd.BankID, &fd.BankAccountID, &fd.BankConfigID,
			&fd.PrincipalAmount, &fd.InterestRate, &fd.TenorDays,
			&fd.ValueDate, &fd.MaturityDate, &fd.MaturityAmount,
			&fd.InterestType, &fd.InterestPayoutFrequency, &fd.CompoundingFrequency,
			&fd.DayCountConvention, &fd.Currency, &fd.TDSPlanID,
			&fd.BankFDReference, &fd.FDStatus,
		); err != nil {
			return nil, fmt.Errorf("getFDsInScope scan: %w", err)
		}
		results = append(results, fd)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("getFDsInScope rows.Err: %w", err)
	}

	// Resolve TDS rates
	for _, fd := range results {
		if fd.TDSPlanID != "" {
			var tdsRate float64
			_ = exec.QueryRow(ctx,
				`SELECT COALESCE(tds_rate,0) FROM investment.fd_tds_plan_master WHERE tds_plan_id=$1`, fd.TDSPlanID,
			).Scan(&tdsRate)
			fd.TDSRate = tdsRate
		}
	}

	return results, nil
}

// getPriorRunClosingBalance fetches the closing_balance from the most recent
// prior accrual ledger entry for this FD (before the current period).
func getPriorRunClosingBalance(ctx context.Context, exec queryExecutor, fdID string, periodStart time.Time) float64 {
	var closing float64
	_ = exec.QueryRow(ctx, `
		SELECT COALESCE(closing_balance, 0)
		FROM investment.fd_accrual_ledger
		WHERE fd_id = $1 AND period_end <= $2
		ORDER BY period_end DESC
		LIMIT 1`,
		fdID, periodStart,
	).Scan(&closing)
	return closing
}

// ─── Validation helpers ──────────────────────────────────────────────────────

// validateFDsForAccrual checks each FD for common issues.
func validateFDsForAccrual(fds []*AccrualInput, params *AccrualRunParams) []ValidationFinding {
	var findings []ValidationFinding

	for _, fd := range fds {
		if fd.PrincipalAmount <= 0 {
			findings = append(findings, ValidationFinding{
				FDID:     fd.FDID,
				Severity: "ERROR",
				Code:     "ZERO_PRINCIPAL",
				Message:  "Principal amount is zero or negative",
			})
		}
		if fd.InterestRate <= 0 {
			findings = append(findings, ValidationFinding{
				FDID:     fd.FDID,
				Severity: "ERROR",
				Code:     "ZERO_RATE",
				Message:  "Interest rate is zero or negative",
			})
		}
		if fd.ValueDate.After(params.PeriodEnd) {
			findings = append(findings, ValidationFinding{
				FDID:     fd.FDID,
				Severity: "WARNING",
				Code:     "FD_NOT_STARTED",
				Message:  fmt.Sprintf("FD value date %s is after period end %s", fd.ValueDate.Format("2006-01-02"), params.PeriodEnd.Format("2006-01-02")),
			})
		}
		if fd.MaturityDate.Before(params.PeriodStart) {
			findings = append(findings, ValidationFinding{
				FDID:     fd.FDID,
				Severity: "WARNING",
				Code:     "FD_MATURED",
				Message:  fmt.Sprintf("FD matured on %s before period start %s", fd.MaturityDate.Format("2006-01-02"), params.PeriodStart.Format("2006-01-02")),
			})
		}
		if fd.DayCountConvention == "" {
			findings = append(findings, ValidationFinding{
				FDID:     fd.FDID,
				Severity: "INFO",
				Code:     "DEFAULT_DAYCOUNT",
				Message:  "No day count convention set -- defaulting to ACT/365",
			})
		}
		if fd.InterestType == "COMPOUND" && fd.CompoundingFrequency == "" {
			findings = append(findings, ValidationFinding{
				FDID:     fd.FDID,
				Severity: "WARNING",
				Code:     "MISSING_COMPOUNDING_FREQ",
				Message:  "Compound FD has no compounding frequency -- using daily compounding",
			})
		}
	}

	return findings
}

// hasErrors returns true if any finding is severity ERROR.
func hasErrors(findings []ValidationFinding) bool {
	for _, f := range findings {
		if f.Severity == "ERROR" {
			return true
		}
	}
	return false
}
