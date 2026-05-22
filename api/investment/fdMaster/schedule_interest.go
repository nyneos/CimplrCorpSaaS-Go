package fdMaster

import (
	"context"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PeriodInterestFromSchedule sums interest from fd_cashflow_schedule rows in
// [periodStart, periodEnd] using the same event-type rules as buildSimulateSummary.
// Returns found=true when at least one in-scope cashflow row exists for the FD.
func PeriodInterestFromSchedule(
	ctx context.Context,
	pool *pgxpool.Pool,
	fdID string,
	periodStart, periodEnd time.Time,
	interestTypeCode string,
) (interest float64, cashflowIDs []string, found bool) {
	rows, err := pool.Query(ctx, `
		SELECT cashflow_id, event_type, event_date,
		       COALESCE(interest_accrued, 0)
		FROM investment.fd_cashflow_schedule
		WHERE fd_id = $1
		  AND event_date >= $2
		  AND event_date <= $3
		  AND COALESCE(is_deleted, false) = false
		ORDER BY event_date, cashflow_id`,
		fdID, periodStart, periodEnd,
	)
	if err != nil {
		return 0, nil, false
	}
	defer rows.Close()

	isCompound := strings.EqualFold(strings.TrimSpace(interestTypeCode), "COMPOUND")
	type row struct {
		id, eventType string
		interest      float64
	}
	var schedule []row

	for rows.Next() {
		var r row
		var eventDate time.Time
		if err := rows.Scan(&r.id, &r.eventType, &eventDate, &r.interest); err != nil {
			continue
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
			if !isCompound {
				interest += r.interest
			}
		case "CAPITALIZATION":
			if isCompound {
				interest += r.interest
			}
		case "GRACE_PERIOD":
			interest += r.interest
		}
	}
	return interest, cashflowIDs, found
}
