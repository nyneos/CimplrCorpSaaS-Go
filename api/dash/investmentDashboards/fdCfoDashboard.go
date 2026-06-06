package investmentdashboards

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strings"
	"sync"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ─── request / response types ────────────────────────────────────────────────

type fdCfoDashRequest struct {
	UserID            string `json:"user_id"`
	EntityID          string `json:"entity_id"`
	Currency          string `json:"currency"`
	Period            string `json:"period"`     // MTD | QTD | YTD | CUSTOM
	StartDate         string `json:"start_date"` // YYYY-MM-DD - used when Period=="CUSTOM"
	EndDate           string `json:"end_date"`   // YYYY-MM-DD - used when Period=="CUSTOM"
	AsOnDate          string `json:"as_on_date"` // optional snapshot date (default = today)
	Bank              string `json:"bank"`
	FDStatus          string `json:"fd_status"`          // ACTIVE | NEAR_MATURITY | MATURED | ROLLED_OVER | PREMATURELY_CLOSED
	FDType            string `json:"fd_type"`            // SIMPLE | COMPOUNDING
	InterestFrequency string `json:"interest_frequency"` // PAYOUT | COMPOUNDING
	LadderView        string `json:"ladder_view"`        // WEEK | MONTH | YEAR - Maturity Ladder bucket size (default WEEK)
}

// roundN rounds v to n decimal places.
func fdRound(v float64, n int) float64 {
	p := math.Pow(10, float64(n))
	return math.Round(v*p) / p
}

type interestTrendRow struct {
	Period   string  `json:"period"`
	SortKey  string  `json:"sort_key"`
	Accrued  float64 `json:"accrued"`
	Received float64 `json:"received"`
}

func interestTrendGranularity(granularity string) (dateTrunc, interval string) {
	switch strings.ToUpper(granularity) {
	case "DAY", "DAILY":
		return "day", "30 days"
	case "YEAR", "YEARLY":
		return "year", "5 years"
	case "WEEK", "WEEKLY":
		return "week", "8 weeks"
	default:
		return "month", "12 months"
	}
}

// buildInterestTrendSeries buckets accrued vs received interest for the CFO trend chart.
// snapshotDate is the as_on_date upper bound (YYYY-MM-DD); pass "" for no upper cap.
func buildInterestTrendSeries(ctx context.Context, pool *pgxpool.Pool, entityFilter, granularity, snapshotDate string) ([]interestTrendRow, error) {
	dateTrunc, interval := interestTrendGranularity(granularity)
	return interestTrendFromCashflow(ctx, pool, entityFilter, dateTrunc, interval, snapshotDate)
}

// interestTrendFromCashflow aggregates ACCRUAL vs payout rows from fd_cashflow_schedule.
func interestTrendFromCashflow(ctx context.Context, pool *pgxpool.Pool, entityFilter, dateTrunc, interval, snapshotDate string) ([]interestTrendRow, error) {
	// snapshotDate caps the upper bound; fallback to no upper cap when empty.
	upperBound := ""
	if snapshotDate != "" {
		upperBound = "  AND cf.event_date <= '" + snapshotDate + "'::date"
	}
	lowerBound := "  AND cf.event_date >= '" + snapshotDate + "'::date - INTERVAL '" + interval + "'"
	if snapshotDate == "" {
		lowerBound = "  AND cf.event_date >= CURRENT_DATE - INTERVAL '" + interval + "'"
	}

	entityJoin := `
		FROM investment.fd_cashflow_schedule cf
		INNER JOIN investment.fd_master m ON m.fd_id = cf.fd_id AND COALESCE(m.is_deleted, false) = false
		LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
		WHERE COALESCE(cf.is_deleted, false) = false
		` + lowerBound + `
		` + upperBound + `
		  AND ($1::text = '' OR COALESCE(m.entity_id, b.entity_id) = $1)`

	accrualSQL := `
		SELECT
		  TO_CHAR(DATE_TRUNC('` + dateTrunc + `', cf.event_date), 'YYYY-MM-DD') AS sort_key,
		  COALESCE(SUM(COALESCE(cf.interest_accrued, 0)), 0) AS accrued
		` + entityJoin + `
		  AND cf.event_type = 'ACCRUAL'
		GROUP BY 1
		ORDER BY 1`

	receivedSQL := `
		SELECT
		  TO_CHAR(DATE_TRUNC('` + dateTrunc + `', COALESCE(cf.value_date, cf.event_date)), 'YYYY-MM-DD') AS sort_key,
		  COALESCE(SUM(COALESCE(cf.interest_accrued, 0)), 0) AS received
		` + entityJoin + `
		  AND cf.event_type IN ('INTEREST_RECEIPT', 'CAPITALIZATION', 'MATURITY')
		  AND COALESCE(cf.interest_accrued, 0) <> 0
		GROUP BY 1
		ORDER BY 1`

	return mergeInterestTrendBuckets(ctx, pool, entityFilter, dateTrunc, accrualSQL, receivedSQL)
}

func mergeInterestTrendBuckets(ctx context.Context, pool *pgxpool.Pool, entityFilter, dateTrunc, accrualSQL, receivedSQL string) ([]interestTrendRow, error) {
	monthNames := []string{"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"}
	weekIdx := 0
	accruedMap := map[string]float64{}
	recvMap := map[string]float64{}
	seen := map[string]bool{}
	sortKeys := []string{}

	accrRows, err := pool.Query(ctx, accrualSQL, entityFilter)
	if err != nil {
		return nil, err
	}
	defer accrRows.Close()
	for accrRows.Next() {
		var sortKey string
		var accrued float64
		if err := accrRows.Scan(&sortKey, &accrued); err != nil {
			continue
		}
		accruedMap[sortKey] = fdRound(accrued, 2)
		if !seen[sortKey] {
			seen[sortKey] = true
			sortKeys = append(sortKeys, sortKey)
		}
	}

	recRows, rerr := pool.Query(ctx, receivedSQL, entityFilter)
	if rerr != nil {
		return nil, rerr
	}
	defer recRows.Close()
	for recRows.Next() {
		var sortKey string
		var received float64
		if err := recRows.Scan(&sortKey, &received); err != nil {
			continue
		}
		recvMap[sortKey] = fdRound(received, 2)
		if !seen[sortKey] {
			seen[sortKey] = true
			sortKeys = append(sortKeys, sortKey)
		}
	}

	// sort keys chronologically
	for i := 1; i < len(sortKeys); i++ {
		for j := i; j > 0 && sortKeys[j-1] > sortKeys[j]; j-- {
			sortKeys[j-1], sortKeys[j] = sortKeys[j], sortKeys[j-1]
		}
	}

	out := make([]interestTrendRow, 0, len(sortKeys))
	for _, sortKey := range sortKeys {
		out = append(out, interestTrendRow{
			Period:   interestTrendPeriodLabel(dateTrunc, sortKey, &weekIdx, monthNames),
			SortKey:  sortKey,
			Accrued:  accruedMap[sortKey],
			Received: recvMap[sortKey],
		})
	}
	return out, nil
}

func interestTrendPeriodLabel(dateTrunc, periodStart string, weekIdx *int, monthNames []string) string {
	t, err := time.Parse(constants.DateFormat, periodStart)
	if err != nil {
		return periodStart
	}
	switch dateTrunc {
	case "day":
		return fmt.Sprintf("%02d %s", t.Day(), monthNames[int(t.Month())-1])
	case "month":
		return monthNames[int(t.Month())-1] + " " + t.Format("06")
	case "year":
		return t.Format("2006")
	case "week":
		*weekIdx++
		return fmt.Sprintf("Wk %02d (%02d %s)", *weekIdx, t.Day(), monthNames[int(t.Month())-1])
	default:
		return periodStart
	}
}

// ─── Governance: consolidated approvals + period-closing checklist ───────────

type govFDItem struct {
	FDID             string  `json:"fd_id"`
	BookingID        string  `json:"booking_id"`
	Entity           string  `json:"entity"`
	EntityID         string  `json:"entity_id"`
	Bank             string  `json:"bank"`
	Principal        float64 `json:"principal"`
	Rate             float64 `json:"rate"`
	MaturityDate     string  `json:"maturity_date"`
	Status           string  `json:"status"`            // booking_status (legacy field name)
	BookingStatus    string  `json:"booking_status"`    // fd_booking_request.booking_status
	ProcessingStatus string  `json:"processing_status"` // latest open audit processing_status
	Action           string  `json:"action"`            // same as processing_status when audit pending
	Source           string  `json:"source"`
	SourcePage       string  `json:"source_page"`
	RequestedBy      string  `json:"requested_by"`
	RequestedAt      string  `json:"requested_at"`
	Currency         string  `json:"currency"`
}

type govApprovalRow struct {
	Type       string      `json:"type"`
	Category   string      `json:"category"`
	Status     string      `json:"status"`
	Source     string      `json:"source"`
	SourcePage string      `json:"source_page"`
	Count      int         `json:"count"`
	Value      float64     `json:"value"`
	Priority   string      `json:"priority"`
	Items      []govFDItem `json:"items"`
}

type govChecklistItem struct {
	ID           string `json:"id"`
	Label        string `json:"label"`
	Category     string `json:"category"`
	SourcePage   string `json:"source_page"`
	Done         bool   `json:"done"`
	Blocker      bool   `json:"blocker"`
	PendingCount int    `json:"pending_count"`
	Detail       string `json:"detail,omitempty"`
}

func govFetchItems(ctx context.Context, pool *pgxpool.Pool, entityFilter, bankFilter, sql, action, source, sourcePage string) []govFDItem {
	rows, err := pool.Query(ctx, sql, entityFilter, bankFilter)
	if err != nil {
		api.LogError("[CfoDash] governance %s query error: %v", action, err)
		return []govFDItem{}
	}
	defer rows.Close()
	out := []govFDItem{}
	for rows.Next() {
		var f govFDItem
		if scanErr := rows.Scan(
			&f.BookingID, &f.FDID, &f.Entity, &f.EntityID, &f.Bank,
			&f.Principal, &f.Rate, &f.MaturityDate, &f.Status,
			&f.RequestedBy, &f.RequestedAt,
		); scanErr != nil {
			continue
		}
		f.Principal = fdRound(f.Principal, 2)
		f.Rate = fdRound(f.Rate, 4)
		f.Action = action
		f.Source = source
		f.SourcePage = sourcePage
		f.Currency = "INR"
		out = append(out, f)
	}
	return out
}

// govFetchBookingItems returns one row per booking with booking_status and the
// latest open audit processing_status (create/edit/delete).
func govFetchBookingItems(ctx context.Context, pool *pgxpool.Pool, entityFilter, bankFilter, sql, source, sourcePage string) []govFDItem {
	rows, err := pool.Query(ctx, sql, entityFilter, bankFilter)
	if err != nil {
		api.LogError("[CfoDash] governance booking query error: %v", err)
		return []govFDItem{}
	}
	defer rows.Close()
	out := []govFDItem{}
	for rows.Next() {
		var f govFDItem
		var bookingStatus, processingStatus string
		if scanErr := rows.Scan(
			&f.BookingID, &f.FDID, &f.Entity, &f.EntityID, &f.Bank,
			&f.Principal, &f.Rate, &f.MaturityDate, &bookingStatus, &processingStatus,
			&f.RequestedBy, &f.RequestedAt,
		); scanErr != nil {
			continue
		}
		f.Principal = fdRound(f.Principal, 2)
		f.Rate = fdRound(f.Rate, 4)
		f.BookingStatus = bookingStatus
		f.ProcessingStatus = processingStatus
		f.Status = bookingStatus
		switch {
		case processingStatus != "":
			f.Action = processingStatus
		case strings.EqualFold(bookingStatus, "DRAFT") || strings.EqualFold(bookingStatus, "APPROVAL_PENDING"):
			f.Action = constants.StatusPendingApproval
		default:
			f.Action = bookingStatus
		}
		f.Source = source
		f.SourcePage = sourcePage
		f.Currency = "INR"
		out = append(out, f)
	}
	return out
}

func govSumPrincipal(items []govFDItem) float64 {
	s := 0.0
	for _, x := range items {
		s += x.Principal
	}
	return fdRound(s, 2)
}

// govFetchAccrualRunItems returns accrual runs awaiting CFO approval.
// Mirrors GetAccrualRuns (/run/all): run_status must be PENDING_APPROVAL and the
// latest fd_accrual_run_audit.processing_status must not be APPROVED or REJECTED.
func govFetchAccrualRunItems(ctx context.Context, pool *pgxpool.Pool, entityFilter, snapshotDate string) []govFDItem {
	sql := `
		WITH latest_audit AS (
		  SELECT DISTINCT ON (run_id)
		    run_id,
		    COALESCE(processing_status,'') AS processing_status,
		    COALESCE(requested_by,'')       AS requested_by,
		    requested_at
		  FROM investment.fd_accrual_run_audit
		  ORDER BY run_id, requested_at DESC
		)
		SELECT
		  '', COALESCE(r.run_id,''),
		  COALESCE(r.entity_name, r.entity_id,''), COALESCE(r.entity_id,''),
		  COALESCE(r.bank_id_filter,''),
		  COALESCE(r.total_interest_accrued,0), 0,
		  COALESCE(TO_CHAR(r.accrual_period_end,'YYYY-MM-DD'), ''),
		  COALESCE(r.run_status,''),
		  COALESCE(la.requested_by, r.submitted_by, r.created_by,''),
		  COALESCE(TO_CHAR(COALESCE(la.requested_at, r.submitted_at, r.created_at),'YYYY-MM-DD"T"HH24:MI:SS"Z"'), ''),
		  COALESCE(NULLIF(la.processing_status,''), 'PENDING_APPROVAL') AS processing_status
		FROM investment.fd_accrual_run r
		LEFT JOIN latest_audit la ON la.run_id = r.run_id
		WHERE COALESCE(r.is_deleted,false)=false
		  AND UPPER(COALESCE(r.run_status,'')) = 'PENDING_APPROVAL'
		  AND UPPER(COALESCE(NULLIF(la.processing_status,''), 'PENDING_APPROVAL'))
		      NOT IN ('APPROVED','REJECTED')
		  AND ($1::text='' OR r.entity_id=$1)` +
		func() string {
			if snapshotDate != "" {
				return " AND r.created_at <= '" + snapshotDate + "'::date + INTERVAL '1 day'"
			}
			return ""
		}() + `
		ORDER BY r.created_at DESC
		LIMIT 200`
	rows, err := pool.Query(ctx, sql, entityFilter)
	if err != nil {
		api.LogError("[CfoDash] governance accrual_run query error: %v", err)
		return []govFDItem{}
	}
	defer rows.Close()
	out := []govFDItem{}
	for rows.Next() {
		var f govFDItem
		var procStatus string
		if scanErr := rows.Scan(
			&f.BookingID, &f.FDID, &f.Entity, &f.EntityID, &f.Bank,
			&f.Principal, &f.Rate, &f.MaturityDate, &f.Status,
			&f.RequestedBy, &f.RequestedAt, &procStatus,
		); scanErr != nil {
			continue
		}
		f.Principal = fdRound(f.Principal, 2)
		f.Rate = fdRound(f.Rate, 4)
		f.BookingStatus = f.Status
		f.ProcessingStatus = procStatus
		f.Action = procStatus
		f.Source = "Accrual Engine"
		f.SourcePage = constants.FDAccrualEngine
		f.Currency = "INR"
		out = append(out, f)
	}
	return out
}

func buildGovernanceBundle(ctx context.Context, pool *pgxpool.Pool, entityFilter, bankFilter, snapshotDate string, periodStart time.Time) map[string]interface{} {
	// snapshotDate upper-bound: only show items that existed as of this date.
	// An empty snapshotDate means "no upper bound" (current live view).
	snapUntil := ""
	if snapshotDate != "" {
		snapUntil = " AND b.created_at <= '" + snapshotDate + "'::date + INTERVAL '1 day'"
	}
	snapFilter := snapUntil // booking uses b.created_at
	snapConfirm := ""
	snapActivation := ""
	snapClosure := ""
	if snapshotDate != "" {
		snapConfirm = " AND c.created_at  <= '" + snapshotDate + "'::date + INTERVAL '1 day'"
		snapActivation = " AND m.created_at  <= '" + snapshotDate + "'::date + INTERVAL '1 day'"
		snapClosure = " AND sort_ts       <= '" + snapshotDate + "'::date + INTERVAL '1 day'"
	}
	// Approvals Pending widget shows every row that is *not* finalised.
	// "Finalised" = APPROVED / REJECTED (and CLOSED on booking). Everything
	// in between (PENDING_APPROVAL, APPROVAL_PENDING, SUBMITTED, EDIT/DELETE
	// pending, VARIANCE_PENDING, SENT_TO_BANK, etc.) needs CFO attention.
	pendingBookingSQL := `
		SELECT
		  COALESCE(b.booking_id,''), COALESCE(m.fd_id,''),
		  COALESCE(b.entity_name, m.entity_name,''), COALESCE(b.entity_id, m.entity_id,''),
		  COALESCE(m.bank_name, m.bank_id, b.bank_name, b.bank_id,''),
		  COALESCE(b.principal_amount,0), COALESCE(b.interest_rate, m.interest_rate, 0),
		  COALESCE(TO_CHAR(b.expected_maturity_date,'YYYY-MM-DD'), TO_CHAR(m.maturity_date,'YYYY-MM-DD'),''),
		  COALESCE(b.booking_status,''),
		  COALESCE(la.processing_status,''),
		  COALESCE(la.requested_by, b.created_by,''),
		  COALESCE(TO_CHAR(COALESCE(la.requested_at, b.created_at),'YYYY-MM-DD"T"HH24:MI:SS"Z"'),'')
		FROM investment.fd_booking_request b
		LEFT JOIN investment.fd_master m ON m.booking_id = b.booking_id AND m.is_deleted=false
		LEFT JOIN LATERAL (
		  SELECT a.processing_status, a.requested_by, a.requested_at
		  FROM investment.fd_audit_booking_request a
		  WHERE a.booking_id = b.booking_id
		    AND UPPER(COALESCE(a.processing_status,'')) NOT IN ('APPROVED','REJECTED','DELETED')
		  ORDER BY a.requested_at DESC
		  LIMIT 1
		) la ON true
		WHERE b.is_deleted=false
		  AND ` + sqlExcludeTerminalFdOnBooking + `
		  AND (
		    ` + sqlBookingAwaitingApproval + `
		    OR COALESCE(la.processing_status,'') <> ''
		  )
		  AND ($1::text='' OR COALESCE(m.entity_id,b.entity_id)=$1)
		  AND ($2::text='' OR m.bank_id=$2 OR m.bank_name=$2 OR b.bank_id=$2 OR b.bank_name=$2)` +
		snapFilter + `
		ORDER BY COALESCE(la.requested_at, b.created_at) DESC LIMIT 200`

	pendingConfirmSQL := `
		SELECT
		  COALESCE(b.booking_id,''), COALESCE(m.fd_id,''),
		  COALESCE(b.entity_name, m.entity_name,''), COALESCE(b.entity_id, m.entity_id,''),
		  COALESCE(m.bank_name, m.bank_id, b.bank_name, b.bank_id,''),
		  COALESCE(b.principal_amount,0), COALESCE(b.interest_rate, m.interest_rate, 0),
		  COALESCE(TO_CHAR(b.expected_maturity_date,'YYYY-MM-DD'), TO_CHAR(m.maturity_date,'YYYY-MM-DD'),''),
		  COALESCE(c.confirmation_status,''),
		  COALESCE(c.created_by,''), COALESCE(TO_CHAR(c.created_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"'),'')
		FROM investment.fd_confirmation c
		JOIN investment.fd_booking_request b ON b.booking_id = c.booking_id
		LEFT JOIN investment.fd_master m ON m.booking_id = b.booking_id AND m.is_deleted=false
		WHERE COALESCE(c.is_deleted,false)=false
		  AND ` + sqlConfirmationAwaitingApproval + `
		  AND ` + sqlExcludeTerminalFdOnBooking + `
		  AND ($1::text='' OR COALESCE(m.entity_id,b.entity_id)=$1)
		  AND ($2::text='' OR m.bank_id=$2 OR m.bank_name=$2 OR b.bank_id=$2 OR b.bank_name=$2)` +
		snapConfirm + `
		ORDER BY c.created_at DESC LIMIT 200`

	pendingActivationSQL := `
		SELECT
		  COALESCE(b.booking_id,''), COALESCE(m.fd_id,''),
		  COALESCE(b.entity_name, m.entity_name,''), COALESCE(b.entity_id, m.entity_id,''),
		  COALESCE(m.bank_name, m.bank_id, b.bank_name, b.bank_id,''),
		  COALESCE(m.principal_amount, b.principal_amount,0), COALESCE(m.interest_rate, b.interest_rate, 0),
		  COALESCE(TO_CHAR(m.maturity_date,'YYYY-MM-DD'),''),
		  COALESCE(m.fd_status,''),
		  COALESCE(m.created_by,''), COALESCE(TO_CHAR(m.created_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"'),'')
		FROM investment.fd_master m
		LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
		WHERE COALESCE(m.is_deleted,false)=false
		  AND UPPER(COALESCE(m.fd_status,'')) = 'PENDING_ACTIVATION'
		  AND ` + sqlExcludeTerminalFdOnMaster + `
		  AND ($1::text='' OR COALESCE(m.entity_id,b.entity_id)=$1)
		  AND ($2::text='' OR m.bank_id=$2 OR m.bank_name=$2 OR b.bank_id=$2 OR b.bank_name=$2)` +
		snapActivation + `
		ORDER BY m.created_at DESC LIMIT 200`

	pendingClosureSQL := `
		SELECT booking_id, fd_id, entity, entity_id, bank, principal, rate, maturity_date, status, created_by, created_at
		FROM (
		  -- New cimplr workflow: initiate stage rows
		  SELECT
		    COALESCE(m.booking_id, ci.booking_id, '') AS booking_id,
		    COALESCE(ci.fd_id, '') AS fd_id,
		    COALESCE(ci.entity_name, m.entity_name, b.entity_name, '') AS entity,
		    COALESCE(ci.entity_id, m.entity_id, b.entity_id, '') AS entity_id,
		    COALESCE(ci.bank_name, m.bank_id, b.bank_name, b.bank_id, '') AS bank,
		    COALESCE(ci.principal_amount, m.principal_amount, b.principal_amount, 0) AS principal,
		    COALESCE(ci.interest_rate, m.interest_rate, b.interest_rate, 0) AS rate,
		    COALESCE(TO_CHAR(ci.maturity_date, 'YYYY-MM-DD'), TO_CHAR(m.maturity_date, 'YYYY-MM-DD'), '') AS maturity_date,
		    COALESCE(la.processing_status, ci.closure_status, 'PENDING_APPROVAL') AS status,
		    COALESCE(la.requested_by, '') AS created_by,
		    COALESCE(TO_CHAR(la.requested_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'), '') AS created_at,
		    -- cimplr.fd_closure_initiate has no created_at column; fall back to
		    -- NOW() so brand-new initiate rows without an audit entry still sort
		    -- to the top instead of erroring out the whole query.
		    COALESCE(la.requested_at, NOW()) AS sort_ts
		  FROM cimplr.fd_closure_initiate ci
		  LEFT JOIN investment.fd_master m ON m.fd_id = ci.fd_id AND m.is_deleted = false
		  LEFT JOIN investment.fd_booking_request b ON b.booking_id = COALESCE(ci.booking_id, m.booking_id)
		  LEFT JOIN LATERAL (
		    SELECT processing_status, requested_by, requested_at
		    FROM cimplr.fd_closure_initiate_audit a
		    WHERE a.closure_initiate_id = ci.closure_initiate_id
		    ORDER BY a.requested_at DESC
		    LIMIT 1
		  ) la ON true
		  WHERE COALESCE(ci.is_deleted, false) = false
		    AND UPPER(COALESCE(la.processing_status, '')) NOT IN ('APPROVED','REJECTED','DELETED')
		    AND ($1::text = '' OR COALESCE(ci.entity_id, m.entity_id, b.entity_id) = $1)
		    AND ($2::text = '' OR ci.bank_name=$2 OR m.bank_id=$2 OR m.bank_name=$2 OR b.bank_id=$2 OR b.bank_name=$2)
		  UNION ALL
		  -- New cimplr workflow: confirm stage rows (payout / rollover / premature finalisation)
		  SELECT
		    COALESCE(m.booking_id, cc.booking_id, '') AS booking_id,
		    COALESCE(cc.fd_id, '') AS fd_id,
		    COALESCE(cc.entity_name, m.entity_name, b.entity_name, '') AS entity,
		    COALESCE(cc.entity_id, m.entity_id, b.entity_id, '') AS entity_id,
		    COALESCE(cc.bank_name, m.bank_id, b.bank_name, b.bank_id, '') AS bank,
		    COALESCE(cc.principal_expected, m.principal_amount, b.principal_amount, 0) AS principal,
		    COALESCE(m.interest_rate, b.interest_rate, 0) AS rate,
		    COALESCE(TO_CHAR(cc.requested_closure_date, 'YYYY-MM-DD'),
		             TO_CHAR(m.maturity_date, 'YYYY-MM-DD'), '') AS maturity_date,
		    COALESCE(lca.processing_status, cc.closure_status, 'PENDING_APPROVAL') AS status,
		    COALESCE(lca.requested_by, '') AS created_by,
		    COALESCE(TO_CHAR(lca.requested_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'), '') AS created_at,
		    COALESCE(lca.requested_at, NOW()) AS sort_ts
		  FROM cimplr.fd_closure_confirm cc
		  LEFT JOIN investment.fd_master m ON m.fd_id = cc.fd_id AND m.is_deleted = false
		  LEFT JOIN investment.fd_booking_request b ON b.booking_id = COALESCE(cc.booking_id, m.booking_id)
		  LEFT JOIN LATERAL (
		    SELECT processing_status, requested_by, requested_at
		    FROM cimplr.fd_closure_confirm_audit a
		    WHERE a.closure_confirm_id = cc.closure_confirm_id
		    ORDER BY a.requested_at DESC
		    LIMIT 1
		  ) lca ON true
		  WHERE COALESCE(cc.is_deleted, false) = false
		    AND UPPER(COALESCE(lca.processing_status, '')) NOT IN ('APPROVED','REJECTED','DELETED','POSTED')
		    AND ($1::text = '' OR COALESCE(cc.entity_id, m.entity_id, b.entity_id) = $1)
		    AND ($2::text = '' OR cc.bank_name=$2 OR m.bank_id=$2 OR m.bank_name=$2 OR b.bank_id=$2 OR b.bank_name=$2)
		  UNION ALL
		  -- Legacy fd_closure_request fallback (older environments only)
		  SELECT
		    COALESCE(b.booking_id, ''), COALESCE(m.fd_id, cr.fd_id, ''),
		    COALESCE(m.entity_name, b.entity_name, ''), COALESCE(m.entity_id, b.entity_id, ''),
		    COALESCE(m.bank_name, m.bank_id, b.bank_name, b.bank_id, ''),
		    COALESCE(m.principal_amount, b.principal_amount, 0), COALESCE(m.interest_rate, b.interest_rate, 0),
		    COALESCE(TO_CHAR(m.maturity_date, 'YYYY-MM-DD'), ''),
		    COALESCE(cr.closure_status, ''),
		    COALESCE(cr.created_by, ''), COALESCE(TO_CHAR(cr.created_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'), ''),
		    cr.created_at
		  FROM investment.fd_closure_request cr
		  LEFT JOIN investment.fd_master m ON m.fd_id = cr.fd_id AND m.is_deleted = false
		  LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
		  WHERE COALESCE(cr.is_deleted, false) = false
		    AND UPPER(COALESCE(cr.closure_status,'')) NOT IN ('APPROVED','REJECTED','POSTED','COMPLETED','CLOSED','CANCELLED')
		    AND ($1::text = '' OR COALESCE(m.entity_id, b.entity_id) = $1)
		    AND ($2::text = '' OR m.bank_id=$2 OR m.bank_name=$2 OR b.bank_id=$2 OR b.bank_name=$2)
		) q
		WHERE 1=1` + snapClosure + `
		ORDER BY sort_ts DESC
		LIMIT 200`

	bookingItems := govFetchBookingItems(ctx, pool, entityFilter, bankFilter, pendingBookingSQL, constants.FDBookingLabel, constants.FDBooking)
	confirmItems := govFetchItems(ctx, pool, entityFilter, bankFilter, pendingConfirmSQL, "PENDING_CONFIRMATION_APPROVAL", constants.FDConfirmation, constants.FDConfirmationLabel)
	activationItems := govFetchItems(ctx, pool, entityFilter, bankFilter, pendingActivationSQL, "PENDING_ACTIVATION_APPROVAL", constants.FDActivationLabel, constants.FDActivation)
	maturityItems := govFetchItems(ctx, pool, entityFilter, bankFilter, pendingClosureSQL, "PENDING_CLOSURE_APPROVAL", constants.FDMaturity, constants.FdmaturityLabel)

	accrualItems := govFetchAccrualRunItems(ctx, pool, entityFilter, snapshotDate)
	accrualRunCount := int64(len(accrualItems))

	var latestRunStatus string
	_ = pool.QueryRow(ctx, `
		SELECT COALESCE(run_status,'') FROM investment.fd_accrual_run
		WHERE COALESCE(is_deleted,false)=false AND ($1::text='' OR entity_id=$1)
		ORDER BY created_at DESC LIMIT 1`, entityFilter).Scan(&latestRunStatus)
	accrualPosted := latestRunStatus == "POSTED" || latestRunStatus == constants.StatusApproved

	var unprocessedMaturities int64
	_ = pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM investment.fd_master m
		LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
		WHERE m.is_deleted=false AND m.maturity_date <= CURRENT_DATE
		  AND m.maturity_date >= $2::date
		  AND m.fd_status IN ('ACTIVE','MATURED')
		  AND ($1::text='' OR COALESCE(m.entity_id,b.entity_id)=$1)
		  AND ($3::text='' OR m.bank_id=$3 OR m.bank_name=$3 OR b.bank_id=$3 OR b.bank_name=$3)
		  AND NOT (`+sqlAnyClosureProcessed+`)`,
		entityFilter, periodStart.Format(constants.DateFormat), bankFilter).Scan(&unprocessedMaturities)

	bookingPriority := "Low"
	for _, it := range bookingItems {
		switch it.Action {
		case constants.StatusPendingDeleteApproval, constants.StatusPendingApproval:
			bookingPriority = "High"
		case constants.StatusPendingEditApproval:
			if bookingPriority != "High" {
				bookingPriority = "Medium"
			}
		}
	}

	approvals := []govApprovalRow{
		{Type: constants.FDBookingLabel, Category: constants.FDBookingLabel, Status: constants.StatusPendingApproval,
			Source: constants.FDBookingLabel, SourcePage: constants.FDBooking,
			Count: len(bookingItems), Value: govSumPrincipal(bookingItems),
			Priority: bookingPriority, Items: bookingItems},
		{Type: constants.FDConfirmation, Category: constants.FDConfirmation, Status: constants.StatusPendingApproval,
			Source: constants.FDConfirmation, SourcePage: constants.FDConfirmationLabel,
			Count: len(confirmItems), Value: govSumPrincipal(confirmItems),
			Priority: "Medium", Items: confirmItems},
		{Type: constants.FDActivationLabel, Category: constants.FDActivationLabel, Status: constants.StatusPendingApproval,
			Source: constants.FDActivationLabel, SourcePage: constants.FDActivation,
			Count: len(activationItems), Value: govSumPrincipal(activationItems),
			Priority: "High", Items: activationItems},
		{Type: constants.FDMaturity, Category: constants.FDMaturity, Status: constants.StatusPendingApproval,
			Source: constants.FDMaturity, SourcePage: constants.FdmaturityLabel,
			Count: len(maturityItems), Value: govSumPrincipal(maturityItems),
			Priority: "High", Items: maturityItems},
		{Type: constants.AccrualRun, Category: constants.AccrualRun, Status: constants.StatusPendingApproval,
			Source: "Accrual Engine", SourcePage: constants.FDAccrualEngine,
			Count: len(accrualItems), Value: govSumPrincipal(accrualItems),
			Priority: "Medium", Items: accrualItems},
	}

	maturityPending := len(maturityItems) + int(unprocessedMaturities)
	accrualPending := int(accrualRunCount)

	checklist := []govChecklistItem{
		{ID: "fd_booking", Label: constants.FDBookingLabel, Category: constants.FDBookingLabel, SourcePage: constants.FDBooking,
			PendingCount: len(bookingItems), Done: len(bookingItems) == 0, Blocker: len(bookingItems) > 0,
			Detail: formatInt64(int64(len(bookingItems))) + " pending approval(s)"},
		{ID: "fd_confirmation", Label: constants.FDConfirmation, Category: constants.FDConfirmation, SourcePage: constants.FDConfirmationLabel,
			PendingCount: len(confirmItems), Done: len(confirmItems) == 0, Blocker: len(confirmItems) > 0,
			Detail: formatInt64(int64(len(confirmItems))) + " pending confirmation(s)"},
		{ID: "fd_activation", Label: constants.FDActivationLabel, Category: constants.FDActivationLabel, SourcePage: constants.FDActivation,
			PendingCount: len(activationItems), Done: len(activationItems) == 0, Blocker: len(activationItems) > 0,
			Detail: formatInt64(int64(len(activationItems))) + " pending activation(s)"},
		{ID: "fd_maturity", Label: constants.FDMaturity, Category: constants.FDMaturity, SourcePage: constants.FdmaturityLabel,
			PendingCount: maturityPending, Done: maturityPending == 0, Blocker: maturityPending > 0,
			Detail: formatInt64(int64(len(maturityItems))) + " closure approval(s), " + formatInt64(unprocessedMaturities) + " unprocessed maturity"},
		{ID: "accrual_run", Label: constants.AccrualRun, Category: constants.AccrualRun, SourcePage: constants.FDAccrualEngine,
			PendingCount: accrualPending, Done: accrualPosted && accrualRunCount == 0, Blocker: accrualRunCount > 0,
			Detail: "Latest run: " + latestRunStatus + "; " + formatInt64(accrualRunCount) + " pending approval(s)"},
	}

	completed, blockers := int64(0), int64(0)
	for _, it := range checklist {
		if it.Done {
			completed++
		} else if it.Blocker {
			blockers++
		}
	}
	total := int64(len(checklist))
	pct := 0.0
	if total > 0 {
		pct = fdRound(float64(completed)/float64(total)*100, 2)
	}

	return map[string]interface{}{
		"approvals": approvals,
		"closing_status": map[string]interface{}{
			"completed": completed,
			"total":     total,
			"pct":       pct,
			"blockers":  blockers,
		},
		"closing_checklist": checklist,
	}
}

// ─── handler ─────────────────────────────────────────────────────────────────

// GetFDCfoDashboard returns the full FD CFO dashboard payload in a single call.
func GetFDCfoDashboard(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		var req fdCfoDashRequest
		_ = json.NewDecoder(r.Body).Decode(&req)

		if req.Currency == "" {
			req.Currency = "INR"
		}
		if req.Period == "" {
			req.Period = "MTD"
		}

		now := time.Now().UTC()
		if asOn, ok := parseFDDate(req.AsOnDate); ok {
			now = asOn
		}
		// snapshotDate is passed to SQL queries to replace CURRENT_DATE so all
		// date-sensitive queries are anchored to the requested as_on_date.
		snapshotDate := now.Format(constants.DateFormat)

		periodBounds := resolveFDPeriodBounds(req.Period, req.StartDate, req.EndDate, now)
		periodStart := periodBounds.Start
		ctx := r.Context()

		// Build optional entity filter SQL fragment (used across many queries)
		entityFilter := req.EntityID
		// Surface these filters early so all goroutine closures can access them.
		fdStatusFilter := req.FDStatus
		fdTypeFilter := req.FDType
		bankFilter := req.Bank

		// snapshotFilter limits every fd_master query to FDs that existed on
		// as_on_date. Injected as a SQL fragment (value is already validated).
		snapshotFilter := " AND m.start_date <= '" + snapshotDate + "'::date"

		// ── concurrent sub-computations ──────────────────────────────────────
		type subResult struct {
			data interface{}
			err  error
		}
		results := make(map[string]subResult, 8)
		var mu sync.Mutex
		var wg sync.WaitGroup

		run := func(key string, fn func(context.Context) (interface{}, error)) {
			wg.Add(1)
			go func() {
				defer wg.Done()
				data, err := fn(ctx)
				mu.Lock()
				results[key] = subResult{data, err}
				mu.Unlock()
			}()
		}

		// ── 1. total_exposure ─────────────────────────────────────────────────
		// Sum principal across every non-deleted FD in scope. We deliberately
		// do *not* filter by fd_status here - the FD register tile shows
		// "X instruments in scope" using the same population, and any
		// status-narrowing the user wants is already applied through the
		// dedicated fd_status filter ($2). Keeping these consistent prevents
		// the tile showing ₹0 when the register clearly has live FDs.
		run("total_exposure", func(ctx context.Context) (interface{}, error) {
			// Outstanding exposure only: FDs whose cimplr/legacy closure has
			// already been posted are excluded so the tile shrinks the
			// moment a maturity / rollover / premature is settled.
			sqlStr := `
				SELECT
				  COALESCE(SUM(m.principal_amount), 0) AS value,
				  COUNT(*)                              AS fd_count
				FROM investment.fd_master m
				LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
				WHERE m.is_deleted = false
				  AND ($1::text = '' OR COALESCE(m.entity_id,b.entity_id) = $1)
				  AND m.fd_status = 'ACTIVE'
				  AND ($2::text = '' OR COALESCE(m.interest_type_code,'') = $2)
				  AND ($3::text = '' OR m.bank_id = $3 OR m.bank_name = $3)` +
				snapshotFilter + `
				  AND NOT ` + sqlAnyClosureProcessed
			var value float64
			var count int64
			err := pool.QueryRow(ctx, sqlStr, entityFilter, fdTypeFilter, bankFilter).Scan(&value, &count)
			if err != nil {
				return nil, err
			}
			return map[string]interface{}{
				"value":     fdRound(value, 2),
				"fd_count":  count,
				"trend_pct": 0,
			}, nil
		})

		// ── 2. bank_concentration ─────────────────────────────────────────────
		// Per-bank exposure vs derived bank limit.
		//
		// Bank limit derivation (no dedicated `credit_limit` column exists in
		// fd_bank_config_master, so we derive the cap):
		//   1. Configured cap = SUM(maximum_amount) across that bank's active
		//      product configs (each product's max-per-FD aggregates into a
		//      coarse bank-level cap).
		//   2. Fallback policy cap = 30% of total portfolio exposure
		//      (industry-standard single-counterparty concentration limit).
		// The larger of the two is taken so we never under-state a bank's room
		// but always have a non-zero number to compare against.
		run("bank_concentration", func(ctx context.Context) (interface{}, error) {
			var totalAmt float64
			_ = pool.QueryRow(ctx,
				`SELECT COALESCE(SUM(m.principal_amount),0)
				 FROM investment.fd_master m
				 LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
				 WHERE m.is_deleted=false AND m.fd_status = 'ACTIVE'
				   AND ($1::text='' OR COALESCE(m.entity_id,b.entity_id)=$1)
				   AND ($2::text='' OR COALESCE(m.interest_type_code,'')=$2)
				   AND ($3::text='' OR m.bank_id=$3 OR m.bank_name=$3)`+snapshotFilter,
				entityFilter, fdTypeFilter, bankFilter).Scan(&totalAmt)

			// 30% of total portfolio = default per-counterparty concentration cap
			policyCap := totalAmt * 0.30

			sqlStr := `
				SELECT
				  COALESCE(m.bank_name, m.bank_id, '') AS bank,
				  COALESCE(SUM(m.principal_amount), 0) AS exposure,
				  COALESCE(lim.bank_cap, 0)            AS bank_cap
				FROM investment.fd_master m
				LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
				LEFT JOIN LATERAL (
				  SELECT COALESCE(SUM(COALESCE(bc.maximum_amount, 0)), 0) AS bank_cap
				  FROM investment.fd_bank_config_master bc
				  WHERE (bc.bank_code = m.bank_id OR bc.bank_code = m.bank_name)
				    AND COALESCE(bc.is_deleted, false) = false
				    AND COALESCE(bc.effective_to, '9999-12-31'::date) >= CURRENT_DATE
				) lim ON true
				WHERE m.is_deleted=false AND m.fd_status = 'ACTIVE'
				  AND ($1::text='' OR COALESCE(m.entity_id,b.entity_id)=$1)
				  AND ($2::text='' OR COALESCE(m.interest_type_code,'')=$2)
				  AND ($3::text='' OR m.bank_id=$3 OR m.bank_name=$3)` +
				snapshotFilter + `
				GROUP BY COALESCE(m.bank_name, m.bank_id, ''), lim.bank_cap
				ORDER BY exposure DESC`

			type bcRow struct {
				Bank           string  `json:"bank"`
				Exposure       float64 `json:"exposure"`
				Limit          float64 `json:"limit"`
				Pct            float64 `json:"pct"`
				UtilizationPct float64 `json:"utilization_pct"`
				RemainingLimit float64 `json:"remaining_limit"`
				LimitSource    string  `json:"limit_source"`
				Breach         bool    `json:"breach"`
			}

			rows, err := pool.Query(ctx, sqlStr, entityFilter, fdTypeFilter, bankFilter)
			if err != nil {
				return nil, err
			}
			defer rows.Close()

			var data []bcRow
			for rows.Next() {
				var br bcRow
				var bankCap float64
				if err := rows.Scan(&br.Bank, &br.Exposure, &bankCap); err != nil {
					continue
				}
				if bankCap > 0 {
					br.Limit = bankCap
					br.LimitSource = "bank_config"
				} else {
					br.Limit = policyCap
					br.LimitSource = "policy_30pct"
				}
				if totalAmt > 0 {
					br.Pct = fdRound(br.Exposure/totalAmt*100, 2)
				}
				if br.Limit > 0 {
					br.UtilizationPct = fdRound(br.Exposure/br.Limit*100, 2)
					br.RemainingLimit = math.Max(0, br.Limit-br.Exposure)
				}
				br.Breach = br.Limit > 0 && br.Exposure > br.Limit
				data = append(data, br)
			}
			if len(data) == 0 {
				return map[string]interface{}{
					"top_bank":   "",
					"top_pct":    0,
					"breach":     false,
					"data":       []bcRow{},
					"policy_cap": fdRound(policyCap, 2),
				}, nil
			}
			top := data[0]
			return map[string]interface{}{
				"top_bank":   top.Bank,
				"top_pct":    top.Pct,
				"breach":     top.Breach,
				"data":       data,
				"policy_cap": fdRound(policyCap, 2),
			}, nil
		})

		// ── 3. maturity buckets ───────────────────────────────────────────────
		// Upcoming-maturity counts skip FDs whose closure has already posted
		// via the new cimplr workflow (or the legacy fd_closure_request).
		run("maturity", func(ctx context.Context) (interface{}, error) {
			sqlStr := `
				SELECT
				  COALESCE(SUM(CASE WHEN m.maturity_date BETWEEN CURRENT_DATE AND CURRENT_DATE+7   THEN m.principal_amount ELSE 0 END),0) AS amt7,
				  COUNT(CASE  WHEN m.maturity_date BETWEEN CURRENT_DATE AND CURRENT_DATE+7   THEN 1 END) AS cnt7,
				  COALESCE(SUM(CASE WHEN m.maturity_date BETWEEN CURRENT_DATE AND CURRENT_DATE+15  THEN m.principal_amount ELSE 0 END),0) AS amt15,
				  COUNT(CASE  WHEN m.maturity_date BETWEEN CURRENT_DATE AND CURRENT_DATE+15  THEN 1 END) AS cnt15,
				  COALESCE(SUM(CASE WHEN m.maturity_date BETWEEN CURRENT_DATE AND CURRENT_DATE+30  THEN m.principal_amount ELSE 0 END),0) AS amt30,
				  COUNT(CASE  WHEN m.maturity_date BETWEEN CURRENT_DATE AND CURRENT_DATE+30  THEN 1 END) AS cnt30
				FROM investment.fd_master m
				LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
				WHERE m.is_deleted=false AND m.fd_status = 'ACTIVE'
				  AND NOT ` + sqlAnyClosureProcessed + `
				  AND ($1::text='' OR COALESCE(m.entity_id,b.entity_id)=$1)
				  AND ($2::text='' OR COALESCE(m.interest_type_code,'')=$2)
				  AND ($3::text='' OR m.bank_id=$3 OR m.bank_name=$3)` +
				snapshotFilter
			var a7, a15, a30 float64
			var c7, c15, c30 int64
			err := pool.QueryRow(ctx, sqlStr, entityFilter, fdTypeFilter, bankFilter, snapshotDate).Scan(&a7, &c7, &a15, &c15, &a30, &c30)
			if err != nil {
				return nil, err
			}
			return map[string]interface{}{
				"next_7_days":  map[string]interface{}{"amount": fdRound(a7, 2), "count": c7},
				"next_15_days": map[string]interface{}{"amount": fdRound(a15, 2), "count": c15},
				"next_30_days": map[string]interface{}{"amount": fdRound(a30, 2), "count": c30},
			}, nil
		})

		// ── 4. interest KPIs ──────────────────────────────────────────────────
		run("interest", func(ctx context.Context) (interface{}, error) {
			sql := `
				SELECT
				  -- YTD accrued
				  COALESCE(SUM(CASE WHEN al.accrual_period_end >= $2::date THEN al.period_interest_accrued ELSE 0 END),0) AS ytd_accrued,
				  -- Period (MTD/QTD/YTD) accrued
				  COALESCE(SUM(CASE WHEN al.accrual_period_end >= $3::date THEN al.period_interest_accrued ELSE 0 END),0) AS period_accrued,
				  -- QTD accrued (Apr 1 of current FY quarter start)
				  COALESCE(SUM(CASE WHEN al.accrual_period_end >= $4::date THEN al.period_interest_accrued ELSE 0 END),0) AS qtd_accrued,
				  -- Interest received (receipts: POSTED + MATCHED + not deleted + bank filter)
				  COALESCE((SELECT SUM(ir.gross_interest_received)
				            FROM investment.fd_interest_receipt ir
				            WHERE ir.is_deleted=false
				              AND ir.receipt_status  = 'POSTED'
				              AND ($1::text='' OR ir.entity_id=$1)
				              AND ($5::text='' OR ir.bank_id=$5)
				                ),0) AS received
				FROM (
				  SELECT al_inner.*
				  FROM investment.fd_accrual_ledger al_inner
				  INNER JOIN (
				    SELECT DISTINCT fd_id, (
				      SELECT run_id FROM investment.fd_accrual_ledger al2
				      WHERE al2.fd_id = al1.fd_id AND COALESCE(al2.is_deleted,false)=false
				      ORDER BY created_at DESC LIMIT 1
				    ) as latest_run_id
				    FROM investment.fd_accrual_ledger al1
				    WHERE COALESCE(al1.is_deleted,false)=false
				  ) latest ON al_inner.fd_id = latest.fd_id AND al_inner.run_id = latest.latest_run_id
				  WHERE COALESCE(al_inner.is_deleted,false)=false
				) al
				LEFT JOIN investment.fd_master m ON m.fd_id = al.fd_id
				LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
				WHERE ($1::text='' OR COALESCE(m.entity_id,b.entity_id)=$1)`

			fyStart := periodStartDate("YTD", now)
			qtdStart := periodStartDate("QTD", now)
			var ytd, periodAcc, qtd, received float64
			err := pool.QueryRow(ctx, sql, entityFilter, fyStart.Format(constants.DateFormat),
				periodStart.Format(constants.DateFormat), qtdStart.Format(constants.DateFormat), bankFilter).
				Scan(&ytd, &periodAcc, &qtd, &received)
			if err != nil {
				return nil, err
			}

			key := "mtd_accrued"
			if req.Period == "QTD" {
				key = "qtd_accrued"
			} else if req.Period == "YTD" {
				key = "ytd_accrued"
			}
			return map[string]interface{}{
				"ytd_accrued": fdRound(ytd, 2),
				"mtd_accrued": fdRound(periodAcc, 2),
				"qtd_accrued": fdRound(qtd, 2),
				"received":    fdRound(received, 2),
				"trend_pct":   0,
				"period_key":  key,
			}, nil
		})

		// ── 5. exceptions (Policy Exceptions Summary - TC-74) ─────────────────
		// Aggregates two real exception sources so the CFO sees a true "policy
		// exception" picture (count + value at risk):
		//
		//   a) Operational exceptions raised by the accrual engine
		//      (investment.fd_accrual_exception, status NOT IN RESOLVED/CLOSED)
		//
		//   b) Policy variance exceptions raised by the variance engine
		//      (public.variance_log, module_code LIKE 'FD_%' AND status='OPEN')
		//      - these capture rate / amount / tenor / date breaches the user
		//      hasn't yet resolved.
		//
		// "value" = principal at risk across distinct FDs that have at least
		// one open exception (regardless of source) so a single FD with
		// multiple exceptions is only counted once.
		run("exceptions", func(ctx context.Context) (interface{}, error) {
			// (a) accrual-engine breakdown
			accSQL := `
				SELECT
				  ae.exception_type,
				  COUNT(DISTINCT ae.exception_id) AS cnt
				FROM investment.fd_accrual_exception ae
				LEFT JOIN investment.fd_master m ON m.fd_id = ae.fd_id
				LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
				WHERE COALESCE(ae.is_deleted,false)=false
				  AND ae.exception_status NOT IN ('RESOLVED','CLOSED')
				  AND ($1::text='' OR COALESCE(m.entity_id,b.entity_id)=$1)
				GROUP BY ae.exception_type
				ORDER BY cnt DESC`
			rows, err := pool.Query(ctx, accSQL, entityFilter)
			if err != nil {
				return nil, err
			}
			type bkRow struct {
				Type  string `json:"type"`
				Count int64  `json:"count"`
			}
			breakup := []bkRow{}
			for rows.Next() {
				var br bkRow
				if err := rows.Scan(&br.Type, &br.Count); err != nil {
					continue
				}
				if br.Type == "" {
					br.Type = "Accrual Exception"
				}
				breakup = append(breakup, br)
			}
			rows.Close()

			// (b) variance-engine breakdown (variance_type → count)
			varSQL := `
				SELECT
				  COALESCE(NULLIF(vl.variance_type,''),'OTHER') AS variance_type,
				  COUNT(*) AS cnt
				FROM public.variance_log vl
				WHERE vl.module_code LIKE 'FD_%'
				  AND vl.status='OPEN'
				  AND ($1::text='' OR vl.entity_id=$1)
				GROUP BY 1
				ORDER BY cnt DESC`
			if vrows, verr := pool.Query(ctx, varSQL, entityFilter); verr == nil {
				for vrows.Next() {
					var t string
					var c int64
					if scanErr := vrows.Scan(&t, &c); scanErr != nil {
						continue
					}
					breakup = append(breakup, bkRow{Type: "Variance: " + t, Count: c})
				}
				vrows.Close()
			}

			// (c) total distinct FDs at risk + principal sum (de-duplicated).
			// The variance_log.record_id can be a closure_request_id /
			// booking_id depending on module_code; resolve back to fd_master
			// through those tables before summing the principal-at-risk so
			// the alert tile is not stuck at ₹0.
			var distinctCount int64
			var totalVal float64
			_ = pool.QueryRow(ctx, `
				WITH at_risk AS (
				  -- accrual exceptions hold fd_id directly
				  SELECT DISTINCT ae.fd_id AS fd_id
				  FROM investment.fd_accrual_exception ae
				  WHERE COALESCE(ae.is_deleted,false)=false
				    AND ae.exception_status NOT IN ('RESOLVED','CLOSED')
				  UNION
				  -- legacy variance on closure → fd_id via fd_closure_request
				  SELECT DISTINCT cr.fd_id
				  FROM public.variance_log vl
				  JOIN investment.fd_closure_request cr
				    ON cr.closure_request_id = vl.record_id
				  WHERE vl.module_code='FD_CLOSURE' AND vl.status='OPEN'
				  UNION
				  -- cimplr variance on closure initiate → fd_id directly
				  SELECT DISTINCT ci.fd_id
				  FROM public.variance_log vl
				  JOIN cimplr.fd_closure_initiate ci
				    ON ci.closure_initiate_id = vl.record_id
				  WHERE vl.module_code='FD_CLOSURE' AND vl.status='OPEN'
				    AND COALESCE(ci.is_deleted,false)=false
				  UNION
				  -- cimplr variance on closure confirm → fd_id directly
				  SELECT DISTINCT cc.fd_id
				  FROM public.variance_log vl
				  JOIN cimplr.fd_closure_confirm cc
				    ON cc.closure_confirm_id = vl.record_id
				  WHERE vl.module_code='FD_CLOSURE' AND vl.status='OPEN'
				    AND COALESCE(cc.is_deleted,false)=false
				  UNION
				  -- variance on booking/confirmation → fd_id via fd_master.booking_id
				  SELECT DISTINCT m2.fd_id
				  FROM public.variance_log vl
				  JOIN investment.fd_master m2 ON m2.booking_id = vl.record_id
				  WHERE vl.module_code IN ('FD_BOOKING','FD_CONFIRMATION') AND vl.status='OPEN'
				)
				SELECT
				  COUNT(DISTINCT m.fd_id),
				  COALESCE(SUM(m.principal_amount),0)
				FROM at_risk a
				JOIN investment.fd_master m ON m.fd_id = a.fd_id
				LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
				WHERE m.is_deleted=false
				  AND ($1::text='' OR COALESCE(m.entity_id,b.entity_id)=$1)`,
				entityFilter).Scan(&distinctCount, &totalVal)

			// Sum of breakup counts is a reasonable proxy when the join above
			// returns 0 (e.g. variance_log holds non-fd_id record_ids).
			var sumCounts int64
			for _, b := range breakup {
				sumCounts += b.Count
			}
			finalCount := distinctCount
			if finalCount == 0 {
				finalCount = sumCounts
			}

			return map[string]interface{}{
				"count":   finalCount,
				"value":   fdRound(totalVal, 2),
				"breakup": breakup,
			}, nil
		})

		// ── 5b. variance_impact (TC-83) ───────────────────────────────────────
		// Real numbers from public.variance_log scoped to FD modules.
		run("variance_impact", func(ctx context.Context) (interface{}, error) {
			sql := `
				SELECT
				  COUNT(*) FILTER (WHERE vl.status='OPEN')                                                         AS open_count,
				  COUNT(*) FILTER (WHERE vl.status='OPEN' AND vl.priority='HIGH')                                  AS high_count,
				  COUNT(*) FILTER (WHERE vl.status='OPEN' AND vl.is_exception=true)                                AS exception_count,
				  COALESCE(SUM(ABS(COALESCE(vl.variance_delta,0))) FILTER (WHERE vl.status='OPEN' AND vl.variance_type='AMOUNT'),0) AS amount_impact,
				  COALESCE(AVG(ABS(COALESCE(vl.variance_delta,0))) FILTER (WHERE vl.status='OPEN' AND vl.variance_type='RATE'),0)   AS avg_rate_delta,
				  COALESCE(AVG(ABS(COALESCE(vl.variance_delta,0))) FILTER (WHERE vl.status='OPEN' AND vl.variance_type='DAYS'),0)   AS avg_day_delta
				FROM public.variance_log vl
				WHERE vl.module_code LIKE 'FD_%'
				  AND ($1::text='' OR vl.entity_id=$1)`
			var openCnt, highCnt, excCnt int64
			var amtImpact, avgRateDelta, avgDayDelta float64
			if err := pool.QueryRow(ctx, sql, entityFilter).Scan(
				&openCnt, &highCnt, &excCnt, &amtImpact, &avgRateDelta, &avgDayDelta,
			); err != nil {
				// Empty result if variance_log not available
				return map[string]interface{}{
					"open_count":      0,
					"high_count":      0,
					"exception_count": 0,
					"amount_impact":   0,
					"avg_rate_delta":  0,
					"avg_day_delta":   0,
					"breakup":         []interface{}{},
				}, nil
			}

			// per-type breakup for the tile sub-list
			type vrRow struct {
				Type  string  `json:"type"`
				Count int64   `json:"count"`
				Delta float64 `json:"delta"`
			}
			breakup := []vrRow{}
			brSQL := `
				SELECT COALESCE(NULLIF(variance_type,''),'OTHER') AS type,
				       COUNT(*)                                   AS cnt,
				       COALESCE(SUM(ABS(COALESCE(variance_delta,0))),0) AS delta
				FROM public.variance_log
				WHERE module_code LIKE 'FD_%' AND status='OPEN'
				  AND ($1::text='' OR entity_id=$1)
				GROUP BY 1
				ORDER BY cnt DESC`
			if brRows, berr := pool.Query(ctx, brSQL, entityFilter); berr == nil {
				for brRows.Next() {
					var br vrRow
					if scanErr := brRows.Scan(&br.Type, &br.Count, &br.Delta); scanErr != nil {
						continue
					}
					br.Delta = fdRound(br.Delta, 2)
					breakup = append(breakup, br)
				}
				brRows.Close()
			}

			// per-FD items (HIGH first) - joined through closure/booking so the
			// drill-down drawer can populate real FDs instead of an empty list.
			type vrItem struct {
				VarianceID    string  `json:"variance_id"`
				RecordID      string  `json:"record_id"`
				FDID          string  `json:"fd_id"`
				BookingID     string  `json:"booking_id"`
				Bank          string  `json:"bank"`
				Entity        string  `json:"entity"`
				EntityID      string  `json:"entity_id"`
				Principal     float64 `json:"principal"`
				Rate          float64 `json:"rate"`
				MaturityDate  string  `json:"maturity_date"`
				FieldName     string  `json:"field_name"`
				VarianceType  string  `json:"variance_type"`
				Priority      string  `json:"priority"`
				Delta         float64 `json:"delta"`
				ExpectedValue string  `json:"expected_value"`
				ActualValue   string  `json:"actual_value"`
				SystemComment string  `json:"system_comment"`
				IsException   bool    `json:"is_exception"`
				ModuleCode    string  `json:"module_code"`
				Status        string  `json:"status"`
			}
			items := []vrItem{}
			// Resolve fd_id back to fd_master through every workflow source the
			// variance engine writes against, so the variance drawer can show
			// real principal / rate / maturity for each row instead of zeros:
			//   • legacy investment.fd_closure_request (record_id = closure_request_id)
			//   • new cimplr.fd_closure_initiate       (record_id = closure_initiate_id, FCI-*)
			//   • new cimplr.fd_closure_confirm        (record_id = closure_confirm_id, FCC-*)
			//   • investment.fd_booking_request        (record_id = booking_id)
			itemsSQL := `
				SELECT
				  COALESCE(vl.variance_id,'')                                     AS variance_id,
				  COALESCE(vl.record_id,'')                                       AS record_id,
				  COALESCE(m.fd_id, ci.fd_id, cc.fd_id, '')                       AS fd_id,
				  COALESCE(b.booking_id, m.booking_id, ci.booking_id, cc.booking_id, '') AS booking_id,
				  COALESCE(m.bank_name, m.bank_id, b.bank_name, b.bank_id,
				           ci.bank_name, cc.bank_name, '')                        AS bank,
				  COALESCE(m.entity_name, b.entity_name,
				           ci.entity_name, cc.entity_name, '')                    AS entity,
				  COALESCE(m.entity_id, b.entity_id, ci.entity_id, cc.entity_id,
				           vl.entity_id, '')                                      AS entity_id,
				  COALESCE(m.principal_amount, b.principal_amount,
				           ci.principal_amount, cc.principal_expected, 0)         AS principal,
				  COALESCE(m.interest_rate, b.interest_rate, ci.interest_rate, 0) AS rate,
				  COALESCE(TO_CHAR(m.maturity_date,'YYYY-MM-DD'),
				           TO_CHAR(b.expected_maturity_date,'YYYY-MM-DD'),
				           TO_CHAR(ci.maturity_date,'YYYY-MM-DD'),
				           TO_CHAR(cc.requested_closure_date,'YYYY-MM-DD'),
				           '')                                                    AS maturity_date,
				  COALESCE(vl.field_name,'')                                      AS field_name,
				  COALESCE(vl.variance_type,'')                                   AS variance_type,
				  COALESCE(vl.priority,'')                                        AS priority,
				  COALESCE(ABS(vl.variance_delta),0)                              AS delta,
				  COALESCE(vl.expected_value,'')                                  AS expected_value,
				  COALESCE(vl.actual_value,'')                                    AS actual_value,
				  COALESCE(vl.system_comment,'')                                  AS system_comment,
				  COALESCE(vl.is_exception,false)                                 AS is_exception,
				  COALESCE(vl.module_code,'')                                     AS module_code,
				  COALESCE(vl.status,'')                                          AS status
				FROM public.variance_log vl
				LEFT JOIN investment.fd_closure_request cr
				  ON vl.module_code='FD_CLOSURE' AND cr.closure_request_id = vl.record_id
				LEFT JOIN cimplr.fd_closure_initiate ci
				  ON vl.module_code='FD_CLOSURE' AND ci.closure_initiate_id = vl.record_id
				  AND COALESCE(ci.is_deleted,false)=false
				LEFT JOIN cimplr.fd_closure_confirm cc
				  ON vl.module_code='FD_CLOSURE' AND cc.closure_confirm_id = vl.record_id
				  AND COALESCE(cc.is_deleted,false)=false
				LEFT JOIN investment.fd_booking_request b
				  ON (vl.module_code IN ('FD_CONFIRMATION','FD_BOOKING') AND b.booking_id = vl.record_id)
				LEFT JOIN investment.fd_master m
				  ON  ( cr.fd_id IS NOT NULL AND m.fd_id = cr.fd_id )
				   OR ( ci.fd_id IS NOT NULL AND m.fd_id = ci.fd_id )
				   OR ( cc.fd_id IS NOT NULL AND m.fd_id = cc.fd_id )
				   OR ( b.booking_id IS NOT NULL AND m.booking_id = b.booking_id )
				WHERE vl.module_code LIKE 'FD_%'
				  AND vl.status='OPEN'
				  AND ($1::text='' OR vl.entity_id=$1)
				ORDER BY (vl.priority='HIGH') DESC, ABS(vl.variance_delta) DESC
				LIMIT 500`
			if itRows, ierr := pool.Query(ctx, itemsSQL, entityFilter); ierr == nil {
				defer itRows.Close()
				for itRows.Next() {
					var it vrItem
					if scanErr := itRows.Scan(
						&it.VarianceID, &it.RecordID,
						&it.FDID, &it.BookingID, &it.Bank, &it.Entity, &it.EntityID,
						&it.Principal, &it.Rate, &it.MaturityDate,
						&it.FieldName, &it.VarianceType, &it.Priority,
						&it.Delta, &it.ExpectedValue, &it.ActualValue, &it.SystemComment,
						&it.IsException, &it.ModuleCode, &it.Status,
					); scanErr != nil {
						api.LogError("[CfoDash] variance_impact items scan: %v", scanErr)
						continue
					}
					it.Principal = fdRound(it.Principal, 2)
					it.Rate = fdRound(it.Rate, 4)
					it.Delta = fdRound(it.Delta, 4)
					items = append(items, it)
				}
			} else {
				api.LogError("[CfoDash] variance_impact items query: %v", ierr)
			}

			// Distinct FD count + principal-at-risk derived from the items
			// (so the alert tile shows a real "at risk" rupee number even
			// when variance_log holds non-fd_id record IDs).
			seen := map[string]bool{}
			distinctFDValue := 0.0
			for _, it := range items {
				if it.FDID == "" || seen[it.FDID] {
					continue
				}
				seen[it.FDID] = true
				distinctFDValue += it.Principal
			}

			return map[string]interface{}{
				"open_count":        openCnt,
				"high_count":        highCnt,
				"exception_count":   excCnt,
				"amount_impact":     fdRound(amtImpact, 2),
				"avg_rate_delta":    fdRound(avgRateDelta, 4),
				"avg_day_delta":     fdRound(avgDayDelta, 1),
				"distinct_fd_count": len(seen),
				"distinct_fd_value": fdRound(distinctFDValue, 2),
				"breakup":           breakup,
				"items":             items,
			}, nil
		})

		// ── 6. maturity_ladder (chart) ────────────────────────────────────────
		// Bucket size is controlled by req.LadderView:
		//   WEEK  → 8 weekly buckets covering the next 8 weeks (default)
		//   MONTH → 12 monthly buckets covering the next 12 months
		//   YEAR  → 5 yearly buckets covering the next 5 years
		// The bucket column is built deterministically so labels sort in order.
		ladderView := strings.ToUpper(strings.TrimSpace(req.LadderView))
		if ladderView == "" {
			ladderView = "WEEK"
		}
		run("maturity_ladder", func(ctx context.Context) (interface{}, error) {
			var bucketExpr, sortExpr string
			switch ladderView {
			case "MONTH":
				// Months from today: 0..11, label as 'Mon YY'
				bucketExpr = `to_char(date_trunc('month', m.maturity_date), 'Mon YY')`
				sortExpr = `date_trunc('month', m.maturity_date)`
			case "YEAR":
				bucketExpr = `to_char(date_trunc('year', m.maturity_date), 'YYYY')`
				sortExpr = `date_trunc('year', m.maturity_date)`
			default: // WEEK
				bucketExpr = `'Wk ' || to_char(((m.maturity_date - CURRENT_DATE)::int / 7) + 1, 'FM00') ||
				              ' (' || to_char(date_trunc('week', m.maturity_date), 'DD Mon') || ')'`
				sortExpr = `date_trunc('week', m.maturity_date)`
			}

			horizonClause := "AND m.maturity_date BETWEEN CURRENT_DATE AND (CURRENT_DATE + 56)"
			if ladderView == "MONTH" {
				horizonClause = "AND m.maturity_date BETWEEN CURRENT_DATE AND (CURRENT_DATE + INTERVAL '12 months')"
			} else if ladderView == "YEAR" {
				horizonClause = "AND m.maturity_date BETWEEN CURRENT_DATE AND (CURRENT_DATE + INTERVAL '5 years')"
			}

			// Maturity ladder shows only FDs still outstanding (no posted
			// closure yet) so the bars deflate as the new maturity workflow
			// settles each FD.
			sql := `
				SELECT
				  ` + bucketExpr + ` AS period,
				  COALESCE(SUM(m.principal_amount),0) AS amount,
				  COUNT(*) AS count,
				  MIN(m.maturity_date) AS first_maturity
				FROM investment.fd_master m
				LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
				WHERE m.is_deleted=false AND m.fd_status='ACTIVE'
				  AND NOT ` + sqlAnyClosureProcessed + `
				  AND ($1::text='' OR COALESCE(m.entity_id,b.entity_id)=$1)
				  AND ($2::text='' OR COALESCE(m.interest_type_code,'')=$2)
				  AND ($3::text='' OR m.bank_id=$3 OR m.bank_name=$3)
				  ` + horizonClause + `
				GROUP BY ` + bucketExpr + `, ` + sortExpr + `
				ORDER BY ` + sortExpr
			rows, err := pool.Query(ctx, sql, entityFilter, fdTypeFilter, bankFilter)
			if err != nil {
				return nil, err
			}
			defer rows.Close()
			type ladderRow struct {
				Period        string  `json:"period"`
				Amount        float64 `json:"amount"`
				Count         int64   `json:"count"`
				FirstMaturity string  `json:"first_maturity"`
			}
			var out []ladderRow
			for rows.Next() {
				var lr ladderRow
				var firstMat interface{}
				if err := rows.Scan(&lr.Period, &lr.Amount, &lr.Count, &firstMat); err != nil {
					continue
				}
				lr.Amount = fdRound(lr.Amount, 2)
				if t, ok := firstMat.(time.Time); ok {
					lr.FirstMaturity = t.Format(constants.DateFormat)
				}
				out = append(out, lr)
			}
			if out == nil {
				out = []ladderRow{}
			}
			return map[string]interface{}{
				"view":    ladderView,
				"buckets": out,
			}, nil
		})

		// ── 7. interest_trend (daily / monthly / yearly - cashflow schedule + ledger fallback)
		run("interest_trend", func(ctx context.Context) (interface{}, error) {
			daily, dErr := buildInterestTrendSeries(ctx, pool, entityFilter, "DAY", snapshotDate)
			if dErr != nil {
				daily = []interestTrendRow{}
			}
			monthly, mErr := buildInterestTrendSeries(ctx, pool, entityFilter, "MONTH", snapshotDate)
			if mErr != nil {
				monthly = []interestTrendRow{}
			}
			yearly, yErr := buildInterestTrendSeries(ctx, pool, entityFilter, "YEAR", snapshotDate)
			if yErr != nil {
				yearly = []interestTrendRow{}
			}
			return map[string]interface{}{
				"daily":   daily,
				"monthly": monthly,
				"yearly":  yearly,
				"weekly":  daily, // backward-compatible alias
			}, nil
		})

		// ── 8b. yield_nature (closure profile chart) ────────────────────────────
		// Count posted closures by fd_master.fd_status - matches:
		//   MATURED → Maturity, ROLLED_OVER → Rollover, PREMATURELY_CLOSED → Premature
		// Only rows with approval_status POSTED (posted cimplr/legacy closure).
		run("yield_nature", func(ctx context.Context) (interface{}, error) {
			sql := `
				SELECT
				  CASE m.fd_status
				    WHEN 'MATURED' THEN 'Maturity'
				    WHEN 'ROLLED_OVER' THEN 'Rollover'
				    WHEN 'PREMATURELY_CLOSED' THEN 'Premature'
				  END AS label,
				  COUNT(*)::bigint AS count
				FROM investment.fd_master m
				LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
				WHERE m.is_deleted = false
				  AND m.fd_status IN ('MATURED', 'ROLLED_OVER', 'PREMATURELY_CLOSED')
				  AND ` + sqlAnyClosureProcessed + `
				  AND ($1::text = '' OR COALESCE(m.entity_id, b.entity_id) = $1)
				  AND ($2::text = '' OR m.bank_id = $2 OR m.bank_name = $2)
				  AND ($3::text = '' OR COALESCE(m.interest_type_code,'') = $3)` +
				snapshotFilter + `
				GROUP BY m.fd_status
				ORDER BY m.fd_status`
			rows, err := pool.Query(ctx, sql, entityFilter, bankFilter, fdTypeFilter)
			if err != nil {
				return nil, err
			}
			defer rows.Close()
			type ynRow struct {
				Label string `json:"label"`
				Count int64  `json:"count"`
			}
			var out []ynRow
			for rows.Next() {
				var yr ynRow
				if err := rows.Scan(&yr.Label, &yr.Count); err != nil {
					continue
				}
				out = append(out, yr)
			}
			if out == nil {
				out = []ynRow{}
			}
			return out, nil
		})

		// ── 8. rate_distribution (chart) ──────────────────────────────────────
		run("rate_distribution", func(ctx context.Context) (interface{}, error) {
			// interest_rate on fd_master is annual % (e.g. 8.38). Normalize decimal
			// (0.0838) and mistaken bps (>100) before bucketing.
			sql := `
				WITH base AS (
				  SELECT
				    m.principal_amount,
				    CASE
				      WHEN m.interest_rate > 100 THEN m.interest_rate / 100.0
				      WHEN m.interest_rate > 0 AND m.interest_rate < 1 THEN m.interest_rate * 100.0
				      ELSE m.interest_rate
				    END AS rate_pct
				  FROM investment.fd_master m
				  LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
				  WHERE m.is_deleted = false
				    AND m.fd_status = 'ACTIVE'
				    AND ($1::text = '' OR COALESCE(m.entity_id, b.entity_id) = $1)` +
				snapshotFilter + `
				)
				SELECT
				  CASE
				    WHEN rate_pct < 5  THEN '<5%'
				    WHEN rate_pct < 6  THEN '5–6%'
				    WHEN rate_pct < 7  THEN '6–7%'
				    WHEN rate_pct < 8  THEN '7–8%'
				    WHEN rate_pct < 9  THEN '8–9%'
				    WHEN rate_pct < 10 THEN '9–10%'
				    ELSE '10%+'
				  END AS bucket,
				  COUNT(*)::bigint AS count,
				  COALESCE(SUM(principal_amount), 0) AS value
				FROM base
				WHERE rate_pct > 0
				GROUP BY 1
				ORDER BY MIN(rate_pct)`
			rows, err := pool.Query(ctx, sql, entityFilter)
			if err != nil {
				return nil, err
			}
			defer rows.Close()
			type rateRow struct {
				Bucket string  `json:"bucket"`
				Count  int64   `json:"count"`
				Value  float64 `json:"value"`
			}
			var out []rateRow
			for rows.Next() {
				var rr rateRow
				if err := rows.Scan(&rr.Bucket, &rr.Count, &rr.Value); err != nil {
					continue
				}
				rr.Value = fdRound(rr.Value, 2)
				out = append(out, rr)
			}
			if out == nil {
				out = []rateRow{}
			}
			return out, nil
		})

		// ── 9. governance - approvals + period closing (5 categories) ─────────
		run("governance", func(ctx context.Context) (interface{}, error) {
			return buildGovernanceBundle(ctx, pool, entityFilter, bankFilter, snapshotDate, periodStart), nil
		})

		// ── 11. fd_list ───────────────────────────────────────────────────────
		// Build dynamic WHERE for optional secondary filters (bank, fd_status,
		// fd_type, interest_frequency). Status defaults remain ACTIVE/MATURED
		// when no explicit fdStatusFilter was supplied.
		interestFreqFilter := req.InterestFrequency
		// bankFilter, fdStatusFilter, fdTypeFilter are declared earlier (above the run() loop)

		// Map UI status alias → DB column value
		dbStatusFilter := ""
		switch fdStatusFilter {
		case constants.StatusActive:
			dbStatusFilter = constants.StatusActive
		case "MATURED":
			dbStatusFilter = "MATURED"
		case "ROLLED_OVER":
			dbStatusFilter = "ROLLED_OVER"
		case "PREMATURELY_CLOSED":
			dbStatusFilter = "PREMATURELY_CLOSED"
		case "CLOSED":
			// Legacy UI alias - treat as prematurely closed
			dbStatusFilter = "PREMATURELY_CLOSED"
		}

		// Tenor years between 0 and ~1 = "NEAR_MATURITY" interpreted at SQL
		// time via maturity_date <= CURRENT_DATE + 30. Handle that flag below.
		nearMaturity := fdStatusFilter == "NEAR_MATURITY"

		run("fd_list", func(ctx context.Context) (interface{}, error) {
			// closure_type / closure_status now come from cimplr.fd_closure_* (preferred)
			// with a legacy fd_closure_request fallback so the Yield Nature chart and
			// is_closure_processed flag reflect the new maturity workflow.
			sql := `
				SELECT
				  m.fd_id,
				  COALESCE(b.entity_name, m.entity_name,'') AS entity,
				  COALESCE(m.entity_id, b.entity_id,'') AS entity_id,
				  COALESCE(m.bank_name, m.bank_id,'') AS bank,
				  COALESCE(m.bank_fd_ref_no, m.fd_id,'') AS bank_fd_ref_no,
				  m.principal_amount AS principal,
				  m.interest_rate AS rate,
				  COALESCE(m.interest_type_code,'') AS interest_type_code,
				  COALESCE(m.frequency_id,'') AS frequency_id,
				  COALESCE(TO_CHAR(m.start_date,'YYYY-MM-DD'),'') AS start_date,
				  COALESCE(m.tenure_days, 0) AS tenure_days,
				  COALESCE(m.tenure_months, 0) AS tenure_months,
				  COALESCE(m.tenure_years, 0) AS tenure_years,
				  TO_CHAR(m.maturity_date,'YYYY-MM-DD') AS maturity_date,
				  COALESCE(al.total_interest_accrued, 0) AS interest_accrued,
				  COALESCE(ep.earned_in_period, 0) AS interest_earned_in_period,
				  COALESCE(cfs.total_interest, 0) AS total_interest,
				  m.fd_status AS status,
				  COALESCE(b.booking_id,'') AS booking_id,
				  COALESCE(b.booking_status,'') AS booking_status,
				  CASE
				    WHEN ` + sqlAnyClosureProcessed + ` THEN 'POSTED'
				    ELSE COALESCE(cimplr_cc.closure_status, cimplr_ci.closure_status, cr.closure_status, '')
				  END AS approval_status,
				  ` + sqlEffectiveClosureType + ` AS closure_type,
				  ` + sqlEffectiveClosureStatus + ` AS closure_status,
				  ` + sqlAnyClosureProcessed + ` AS is_closure_processed
				FROM investment.fd_master m
				LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
				LEFT JOIN LATERAL (
				  -- interest_accrued: gross total interest over the full tenor from cashflow schedule.
				  -- CO: sum of CAPITALIZATION rows (gross compound interest before TDS).
				  -- SI: sum of INTEREST_RECEIPT + MATURITY rows (gross simple interest).
				  SELECT COALESCE(SUM(cfs_ia.interest_accrued), 0) AS total_interest_accrued
				  FROM investment.fd_cashflow_schedule cfs_ia
				  WHERE cfs_ia.fd_id = m.fd_id
				    AND COALESCE(cfs_ia.is_deleted, false) = false
				    AND (
				      (UPPER(COALESCE(m.interest_type_code, '')) = 'COMPOUND' AND cfs_ia.event_type = 'CAPITALIZATION')
				      OR
				      (UPPER(COALESCE(m.interest_type_code, '')) != 'COMPOUND' AND cfs_ia.event_type IN ('INTEREST_RECEIPT', 'MATURITY'))
				    )
				) al ON true
				LEFT JOIN LATERAL (
				  -- interest_earned_in_period: pro-rate each ACCRUAL row by how many days
				  -- of its period overlap the dashboard window [start, end].
				  SELECT COALESCE(SUM(
				    cfs_ep.interest_accrued *
				    CASE
				      WHEN ('` + periodBounds.StartStr + `' != '' AND '` + periodBounds.EndStr + `' != '') THEN
				        GREATEST(0.0,
				          LEAST(cfs_ep.period_end_date::date, '` + periodBounds.EndStr + `'::date)
				          - GREATEST(cfs_ep.period_start_date::date, '` + periodBounds.StartStr + `'::date)
				        )::float / GREATEST(1.0, (cfs_ep.period_end_date::date - cfs_ep.period_start_date::date)::float)
				      ELSE 1.0
				    END
				  ), 0) AS earned_in_period
				  FROM investment.fd_cashflow_schedule cfs_ep
				  WHERE cfs_ep.fd_id = m.fd_id
				    AND COALESCE(cfs_ep.is_deleted, false) = false
				    AND cfs_ep.event_type = 'ACCRUAL'
				    AND cfs_ep.period_start_date IS NOT NULL
				    AND cfs_ep.period_end_date IS NOT NULL
				    AND ('` + periodBounds.StartStr + `' = '' OR cfs_ep.period_end_date >= '` + periodBounds.StartStr + `'::date)
				    AND ('` + periodBounds.EndStr + `' = '' OR cfs_ep.period_start_date <= '` + periodBounds.EndStr + `'::date)
				) ep ON true
				LEFT JOIN LATERAL (
				  SELECT COALESCE(SUM(interest_accrued), 0) AS total_interest
				  FROM investment.fd_cashflow_schedule cfs_inner
				  WHERE cfs_inner.fd_id = m.fd_id 
				    AND COALESCE(cfs_inner.is_deleted, false) = false
				    AND cfs_inner.event_type IN ('INTEREST_RECEIPT', 'MATURITY')
				) cfs ON true
				` + sqlLatestCimplrInitiate + `
				` + sqlLatestCimplrConfirm + `
				LEFT JOIN LATERAL (
				  SELECT closure_type, COALESCE(closure_status,'') AS closure_status
				  FROM investment.fd_closure_request
				  WHERE fd_id = m.fd_id AND COALESCE(is_deleted,false)=false
				  ORDER BY created_at DESC LIMIT 1
				) cr ON true
				WHERE m.is_deleted=false
				  AND ($1::text='' OR COALESCE(m.entity_id,b.entity_id)=$1)
				  AND ($2::text='' OR m.bank_id=$2 OR m.bank_name=$2)
				  AND (
				    ($3::text <> '' AND m.fd_status = $3) OR
				    ($3::text = '' AND m.fd_status NOT IN ('DRAFT', 'PENDING_APPROVAL', 'REJECTED', 'CANCELLED', 'CLOSED_IN_SYSTEM'))
				  )
				  AND ($4::boolean=false OR (m.maturity_date BETWEEN $7::date AND $7::date+30))
				  AND ($5::text='' OR COALESCE(m.interest_type_code,'')=$5)
				  AND ($6::text='' OR COALESCE(m.frequency_id,'')=$6)` +
				snapshotFilter + `
				ORDER BY m.maturity_date ASC
				LIMIT 500`
			rows, err := pool.Query(ctx, sql,
				entityFilter,
				bankFilter,
				dbStatusFilter,
				nearMaturity,
				fdTypeFilter,
				interestFreqFilter,
				snapshotDate,
			)
			if err != nil {
				// Log loudly - `run()` swallows the error and a silent failure
				// shows up on the frontend as a mysterious `"fd_list": null`.
				api.LogError("[CfoDash] fd_list query error: %v", err)
				return nil, err
			}
			defer rows.Close()
			type fdRow struct {
				FDID               string  `json:"fd_id"`
				Entity             string  `json:"entity"`
				EntityID           string  `json:"entity_id"`
				Bank               string  `json:"bank"`
				BankFDRefNo        string  `json:"bank_fd_ref_no"`
				Principal          float64 `json:"principal"`
				Rate               float64 `json:"rate"`
				InterestTypeCode   string  `json:"interest_type_code"`
				FrequencyID        string  `json:"frequency_id"`
				StartDate          string  `json:"start_date"`
				TenureDays         int     `json:"tenure_days"`
				TenureMonths       int     `json:"tenure_months"`
				TenureYears        int     `json:"tenure_years"`
				MaturityDate       string  `json:"maturity_date"`
				InterestAccrued    float64 `json:"interest_accrued"`
				InterestEarned     float64 `json:"interest_earned_in_period"`
				TotalInterest      float64 `json:"interest_receivable_at_maturity"`
				Status             string  `json:"status"`
				BookingID          string  `json:"booking_id"`
				BookingStatus      string  `json:"booking_status"`
				ApprovalStatus     string  `json:"approval_status"`
				ClosureType        string  `json:"closure_type"`
				ClosureStatus      string  `json:"closure_status"`
				IsClosureProcessed bool    `json:"is_closure_processed"`
			}
			var out []fdRow
			for rows.Next() {
				var fr fdRow
				if err := rows.Scan(
					&fr.FDID, &fr.Entity, &fr.EntityID, &fr.Bank, &fr.BankFDRefNo,
					&fr.Principal, &fr.Rate, &fr.InterestTypeCode, &fr.FrequencyID,
					&fr.StartDate, &fr.TenureDays, &fr.TenureMonths, &fr.TenureYears,
					&fr.MaturityDate,
					&fr.InterestAccrued, &fr.InterestEarned, &fr.TotalInterest, &fr.Status,
					&fr.BookingID, &fr.BookingStatus,
					&fr.ApprovalStatus,
					&fr.ClosureType, &fr.ClosureStatus,
					&fr.IsClosureProcessed,
				); err != nil {
					continue
				}
				fr.Principal = fdRound(fr.Principal, 2)
				fr.InterestAccrued = fdRound(fr.InterestAccrued, 2)
				fr.InterestEarned = fdRound(fr.InterestEarned, 2)
				fr.Rate = fdRound(fr.Rate, 4)
				out = append(out, fr)
			}
			if out == nil {
				out = []fdRow{}
			}
			return out, nil
		})

		// ── wait for all goroutines ───────────────────────────────────────────
		wg.Wait()

		// ── build bank_concentration chart + limit_utilization KPI ───────────
		// Both derived from the bank_concentration sub-result so the chart and
		// KPI agree on the same per-bank limits.
		type bcChartRow struct {
			Bank           string  `json:"bank"`
			Exposure       float64 `json:"exposure"`
			Limit          float64 `json:"limit"`
			Pct            float64 `json:"pct"`
			UtilizationPct float64 `json:"utilization_pct"`
			RemainingLimit float64 `json:"remaining_limit"`
			Breach         bool    `json:"breach"`
		}
		var bankConcChart interface{} = []bcChartRow{}
		var limitUtilization interface{} = map[string]interface{}{
			"total_exposure":  0.0,
			"total_limit":     0.0,
			"utilization_pct": 0.0,
			"banks_breached":  0,
			"banks_critical":  0,
			"banks_total":     0,
		}
		if bcRes, ok := results["bank_concentration"]; ok && bcRes.err == nil {
			if bcMap, ok2 := bcRes.data.(map[string]interface{}); ok2 {
				if rawData, ok3 := bcMap["data"]; ok3 {
					if jsonB, jerr := json.Marshal(rawData); jerr == nil {
						type wireRow struct {
							Bank           string  `json:"bank"`
							Exposure       float64 `json:"exposure"`
							Limit          float64 `json:"limit"`
							Pct            float64 `json:"pct"`
							UtilizationPct float64 `json:"utilization_pct"`
							RemainingLimit float64 `json:"remaining_limit"`
							Breach         bool    `json:"breach"`
						}
						var wireRows []wireRow
						if json.Unmarshal(jsonB, &wireRows) == nil && len(wireRows) > 0 {
							out := make([]bcChartRow, len(wireRows))
							var totExp, totLim float64
							var breached, critical int
							for i, r := range wireRows {
								out[i] = bcChartRow{
									Bank:           r.Bank,
									Exposure:       r.Exposure,
									Limit:          r.Limit,
									Pct:            r.Pct,
									UtilizationPct: r.UtilizationPct,
									RemainingLimit: r.RemainingLimit,
									Breach:         r.Breach,
								}
								totExp += r.Exposure
								totLim += r.Limit
								if r.Breach {
									breached++
								} else if r.UtilizationPct >= 90 {
									critical++
								}
							}
							bankConcChart = out
							utilPct := 0.0
							if totLim > 0 {
								utilPct = fdRound(totExp/totLim*100, 2)
							}
							limitUtilization = map[string]interface{}{
								"total_exposure":  fdRound(totExp, 2),
								"total_limit":     fdRound(totLim, 2),
								"utilization_pct": utilPct,
								"banks_breached":  breached,
								"banks_critical":  critical,
								"banks_total":     len(wireRows),
							}
						}
					}
				}
			}
		}

		// ── extract helper ─────────────────────────────────────────────────────
		get := func(key string) interface{} {
			if r, ok := results[key]; ok && r.err == nil {
				return r.data
			}
			return nil
		}

		// ── extract governance bundle (approvals + closing status + checklist) ─
		var govApprovals interface{} = []interface{}{}
		var closingStatus interface{} = map[string]interface{}{}
		var closingChecklist interface{} = []interface{}{}
		if govRes, ok := results["governance"]; ok && govRes.err == nil {
			if govMap, ok2 := govRes.data.(map[string]interface{}); ok2 {
				if a, ok3 := govMap["approvals"]; ok3 {
					govApprovals = a
				}
				if cs, ok3 := govMap["closing_status"]; ok3 {
					closingStatus = cs
				}
				if cl, ok3 := govMap["closing_checklist"]; ok3 {
					closingChecklist = cl
				}
			}
		}

		// ── assemble final payload ────────────────────────────────────────────
		payload := map[string]interface{}{
			"generated_at": now.Format(time.RFC3339),
			"filters": map[string]interface{}{
				"entity_id":  entityFilter,
				"currency":   req.Currency,
				"period":     periodBounds.Period,
				"start_date": periodBounds.StartStr,
				"end_date":   periodBounds.EndStr,
			},
			"kpis": map[string]interface{}{
				"total_exposure":     get("total_exposure"),
				"bank_concentration": get("bank_concentration"),
				"maturity":           get("maturity"),
				"interest":           get("interest"),
				"exceptions":         get("exceptions"),
				"variance_impact":    get("variance_impact"),
				"limit_utilization":  limitUtilization,
			},
			"charts": map[string]interface{}{
				"maturity_ladder":    get("maturity_ladder"),
				"interest_trend":     get("interest_trend"),
				"rate_distribution":  get("rate_distribution"),
				"bank_concentration": bankConcChart,
				"yield_nature":       get("yield_nature"),
			},
			"governance": map[string]interface{}{
				"approvals":         govApprovals,
				"closing_status":    closingStatus,
				"closing_checklist": closingChecklist,
			},
			"fd_list": get("fd_list"),
		}

		api.RespondWithPayload(w, true, "", payload)
	}
}
