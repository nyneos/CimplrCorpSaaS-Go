package fdMaster

import (
	"context"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PeriodInterestFromSchedule sums period interest from fd_cashflow_schedule rows.
// It follows cashflow summary semantics, adjusted for accrual windows so payout
// rows contribute only the due-not-accrued slice already embedded in the receipt.
// Returns found=true when at least one in-scope cashflow row exists for the FD.
func PeriodInterestFromSchedule(
	ctx context.Context,
	pool *pgxpool.Pool,
	fdID string,
	periodStart, periodEnd time.Time,
	interestTypeCode string,
) (interest float64, cashflowIDs []string, found bool) {
	return PeriodInterestFromScheduleWithEnd(ctx, pool, fdID, periodStart, periodEnd, interestTypeCode, true)
}

// PeriodInterestFromScheduleWithEnd is the accrual-engine variant. Adjacent
// sub-periods should be half-open [start,end), except when end is the FD's
// terminal maturity/closure date.
func PeriodInterestFromScheduleWithEnd(
	ctx context.Context,
	pool *pgxpool.Pool,
	fdID string,
	periodStart, periodEnd time.Time,
	interestTypeCode string,
	includePeriodEnd bool,
) (interest float64, cashflowIDs []string, found bool) {
	rows, err := pool.Query(ctx, `
		SELECT cashflow_id, event_type, event_date,
		       COALESCE(interest_accrued, 0),
		       COALESCE(due_not_accrued, 0)
		FROM investment.fd_cashflow_schedule
		WHERE fd_id = $1
		  AND event_date >= $2
		  AND (event_date < $3 OR ($4 AND event_date = $3))
		  AND COALESCE(is_deleted, false) = false
		ORDER BY event_date, cashflow_id`,
		fdID, periodStart, periodEnd, includePeriodEnd,
	)
	if err != nil {
		return 0, nil, false
	}
	defer rows.Close()

	isCompound := strings.EqualFold(strings.TrimSpace(interestTypeCode), "COMPOUND")
	type row struct {
		id, eventType string
		interest      float64
		dueNotAccrued float64
	}
	var schedule []row
	hasCompoundCap := false
	hasSimpleAccrual := false

	for rows.Next() {
		var r row
		var eventDate time.Time
		if err := rows.Scan(&r.id, &r.eventType, &eventDate, &r.interest, &r.dueNotAccrued); err != nil {
			continue
		}
		switch r.eventType {
		case "ACCRUAL":
			hasSimpleAccrual = true
		case "CAPITALIZATION":
			hasCompoundCap = true
		}
		schedule = append(schedule, r)
	}
	if len(schedule) == 0 {
		return 0, nil, false
	}
	found = true

	for _, r := range schedule {
		cashflowIDs = append(cashflowIDs, r.id)
		switch r.eventType {
		case "ACCRUAL":
			// Compound accrual rows are the source only before a capitalization row
			// lands in the same accrual window. Once cap exists, cap carries the
			// aggregate interest and avoids double counting the accrual sub-rows.
			if !isCompound || !hasCompoundCap {
				interest += r.interest
			}
		case "CAPITALIZATION":
			if isCompound {
				interest += r.interest
			}
		case "INTEREST_RECEIPT", "MATURITY":
			if !isCompound {
				// For simple payout FDs, the receipt/maturity row carries the last
				// due-not-accrued slice. Prior ACCRUAL rows in the same window carry
				// the already accrued slices, so do not add the full receipt amount.
				if r.dueNotAccrued > 0 {
					interest += r.dueNotAccrued
				} else if !hasSimpleAccrual {
					interest += r.interest
				}
			}
		case "GRACE_PERIOD":
			interest += r.interest
		}
	}
	return interest, cashflowIDs, found
}
