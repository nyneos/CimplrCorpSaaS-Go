// Package investmentdashboards — BOD/EOD V2 sub-query implementations.
//
// Each function is a thin wrapper that runs a single SQL aggregate or row
// fetch and returns a plain interface{} suitable for direct JSON marshalling.
// V2 calls them in parallel goroutines; the V1 handler can keep its inline
// queries (no rewrite required) — these are only used by V2.
package investmentdashboards

import (
	"context"
	"strings"

	"CimplrCorpSaas/api"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ─── Maturities today (BOD) ──────────────────────────────────────────────────

func queryMaturitiesToday(ctx context.Context, pool *pgxpool.Pool, today, entityFilter string) (interface{}, error) {
	rows, err := pool.Query(ctx, `
		SELECT
		  m.fd_id, m.bank_name, m.entity_name, COALESCE(m.entity_id,'') AS entity_id,
		  COALESCE(m.principal_amount,0), COALESCE(m.interest_rate,0),
		  COALESCE(m.fd_status,''),
		  COALESCE(TO_CHAR(m.maturity_date,'YYYY-MM-DD'),''),
		  COALESCE(cf.interest_accrued,0) AS expected_interest
		FROM investment.fd_master m
		LEFT JOIN investment.fd_cashflow_schedule cf
		  ON cf.fd_id = m.fd_id AND cf.event_type='MATURITY' AND cf.is_deleted=false
		WHERE m.is_deleted=false
		  AND m.maturity_date = $1::date
		  AND ($2::text='' OR m.entity_id=$2)
		ORDER BY m.principal_amount DESC NULLS LAST
		LIMIT 200`, today, entityFilter)
	if err != nil {
		api.LogError("[BodEodDashV2] maturities_today: %v", err)
		return map[string]interface{}{"rows": []interface{}{}, "count": 0, "total_principal": 0, "total_interest": 0}, nil
	}
	defer rows.Close()
	type r struct {
		FDID, Bank, Entity, EntityID, Status, MaturityDate string
		Principal, InterestRate, ExpectedInterest          float64
	}
	out := []map[string]interface{}{}
	tp, ti := 0.0, 0.0
	for rows.Next() {
		var x r
		if e := rows.Scan(&x.FDID, &x.Bank, &x.Entity, &x.EntityID,
			&x.Principal, &x.InterestRate, &x.Status, &x.MaturityDate, &x.ExpectedInterest); e != nil {
			continue
		}
		x.Principal = fdRound(x.Principal, 2)
		x.ExpectedInterest = fdRound(x.ExpectedInterest, 2)
		x.InterestRate = fdRound(x.InterestRate, 4)
		tp += x.Principal
		ti += x.ExpectedInterest
		out = append(out, map[string]interface{}{
			"fd_id": x.FDID, "bank": x.Bank, "entity": x.Entity, "entity_id": x.EntityID,
			"principal": x.Principal, "interest_rate": x.InterestRate,
			"fd_status": x.Status, "maturity_date": x.MaturityDate,
			"expected_interest": x.ExpectedInterest,
		})
	}
	return map[string]interface{}{
		"rows": out, "count": len(out),
		"total_principal": fdRound(tp, 2), "total_interest": fdRound(ti, 2),
	}, nil
}

// ─── Maturities next 1–3 days ────────────────────────────────────────────────

func queryMaturities3Days(ctx context.Context, pool *pgxpool.Pool, today, threeDaysOut, entityFilter string) (interface{}, error) {
	rows, err := pool.Query(ctx, `
		SELECT m.fd_id, m.bank_name, m.entity_name,
		       COALESCE(m.principal_amount,0), COALESCE(m.interest_rate,0),
		       COALESCE(TO_CHAR(m.maturity_date,'YYYY-MM-DD'),''),
		       (m.maturity_date - CURRENT_DATE)::int,
		       COALESCE(m.fd_status,'')
		FROM investment.fd_master m
		WHERE m.is_deleted=false
		  AND m.maturity_date > $1::date AND m.maturity_date <= $2::date
		  AND ($3::text='' OR m.entity_id=$3)
		ORDER BY m.maturity_date ASC
		LIMIT 200`, today, threeDaysOut, entityFilter)
	if err != nil {
		return map[string]interface{}{"rows": []interface{}{}, "count": 0, "total_principal": 0}, nil
	}
	defer rows.Close()
	out := []map[string]interface{}{}
	tp := 0.0
	for rows.Next() {
		var fd, bank, entity, mat, status string
		var pr, rt float64
		var d int
		if e := rows.Scan(&fd, &bank, &entity, &pr, &rt, &mat, &d, &status); e != nil {
			continue
		}
		pr = fdRound(pr, 2)
		tp += pr
		out = append(out, map[string]interface{}{
			"fd_id": fd, "bank": bank, "entity": entity, "principal": pr,
			"interest_rate": fdRound(rt, 4), "maturity_date": mat,
			"days_to_maturity": d, "fd_status": status,
		})
	}
	return map[string]interface{}{
		"rows": out, "count": len(out), "total_principal": fdRound(tp, 2),
	}, nil
}

// ─── Confirmations due ───────────────────────────────────────────────────────

func queryConfirmationsDue(ctx context.Context, pool *pgxpool.Pool, entityFilter string) (interface{}, error) {
	rows, err := pool.Query(ctx, `
		SELECT b.booking_id,
		       COALESCE(b.entity_name,''),
		       COALESCE(m.bank_name, m.bank_id, b.bank_name, b.bank_id,''),
		       COALESCE(b.principal_amount,0),
		       b.booking_status,
		       COALESCE(EXTRACT(DAY FROM NOW()-b.created_at)::int, 0),
		       COALESCE(TO_CHAR(b.created_at,'YYYY-MM-DD'),'')
		FROM investment.fd_booking_request b
		LEFT JOIN investment.fd_master m ON m.booking_id=b.booking_id AND m.is_deleted=false
		WHERE b.is_deleted=false
		  AND b.booking_status IN ('SENT_TO_BANK','APPROVAL_PENDING')
		  AND ($1::text='' OR b.entity_id=$1)
		ORDER BY b.created_at ASC
		LIMIT 100`, entityFilter)
	if err != nil {
		return map[string]interface{}{"rows": []interface{}{}, "count": 0, "overdue_count": 0}, nil
	}
	defer rows.Close()
	out := []map[string]interface{}{}
	overdue := 0
	for rows.Next() {
		var id, entity, bank, status, bookDate string
		var amt float64
		var aging int
		if e := rows.Scan(&id, &entity, &bank, &amt, &status, &aging, &bookDate); e != nil {
			continue
		}
		sla := "OK"
		switch {
		case aging >= 3:
			sla = "Breached"
			overdue++
		case aging >= 2:
			sla = "Warning"
		}
		out = append(out, map[string]interface{}{
			"booking_id": id, "entity": entity, "bank": bank,
			"principal": fdRound(amt, 2), "status": status,
			"aging_days": aging, "booking_date": bookDate, "sla_status": sla,
		})
	}
	return map[string]interface{}{
		"rows": out, "count": len(out), "overdue_count": overdue,
	}, nil
}

// ─── Accrual scheduled today ─────────────────────────────────────────────────

func queryAccrualScheduled(ctx context.Context, pool *pgxpool.Pool, today, entityFilter string) (interface{}, error) {
	rows, err := pool.Query(ctx, `
		SELECT run_id, run_type, run_mode, run_status, entity_name,
		       COALESCE(TO_CHAR(accrual_period_start,'YYYY-MM-DD'),''),
		       COALESCE(TO_CHAR(accrual_period_end,'YYYY-MM-DD'),''),
		       COALESCE(fds_in_scope,0), COALESCE(fds_calculated,0),
		       COALESCE(fds_failed,0), COALESCE(total_interest_accrued,0),
		       posting_status,
		       COALESCE(TO_CHAR(run_date,'YYYY-MM-DD"T"HH24:MI:SS'),'')
		FROM investment.fd_accrual_run
		WHERE is_deleted=false AND DATE(run_date) = $1::date
		  AND ($2::text='' OR entity_id=$2)
		ORDER BY run_date DESC
		LIMIT 20`, today, entityFilter)
	if err != nil {
		return []interface{}{}, nil
	}
	defer rows.Close()
	out := []map[string]interface{}{}
	for rows.Next() {
		var rid, rt, rm, rs, ent, ps, rd, pst, pen string
		var inScope, calc, failed int64
		var total float64
		if e := rows.Scan(&rid, &rt, &rm, &rs, &ent, &pst, &pen,
			&inScope, &calc, &failed, &total, &ps, &rd); e != nil {
			continue
		}
		out = append(out, map[string]interface{}{
			"run_id": rid, "run_type": rt, "run_mode": rm, "run_status": rs,
			"entity": ent, "period_start": pst, "period_end": pen,
			"fds_in_scope": inScope, "fds_calculated": calc, "fds_failed": failed,
			"total_interest_accrued": fdRound(total, 2), "posting_status": ps, "run_date": rd,
		})
	}
	return out, nil
}

// ─── Expected interest today ─────────────────────────────────────────────────

func queryExpectedInterest(ctx context.Context, pool *pgxpool.Pool, today, entityFilter string) (interface{}, error) {
	rows, err := pool.Query(ctx, `
		SELECT cf.cashflow_id, m.fd_id, m.bank_name, m.entity_name,
		       COALESCE(cf.net_cash_flow, cf.interest_accrued, 0),
		       COALESCE(cf.tds_amount,0), cf.event_type, cf.posting_status,
		       cf.bank_confirmed, cf.receipt_cleared
		FROM investment.fd_cashflow_schedule cf
		JOIN investment.fd_master m ON m.fd_id=cf.fd_id AND m.is_deleted=false
		WHERE cf.is_deleted=false
		  AND cf.event_date = $1::date
		  AND cf.event_type IN ('INTEREST_RECEIPT','MATURITY')
		  AND ($2::text='' OR m.entity_id=$2)
		ORDER BY cf.net_cash_flow DESC NULLS LAST
		LIMIT 200`, today, entityFilter)
	if err != nil {
		return map[string]interface{}{"rows": []interface{}{}, "count": 0, "total_expected": 0}, nil
	}
	defer rows.Close()
	out := []map[string]interface{}{}
	total := 0.0
	for rows.Next() {
		var cid, fd, bank, ent, evt, ps string
		var amt, tds float64
		var bc, rc bool
		if e := rows.Scan(&cid, &fd, &bank, &ent, &amt, &tds, &evt, &ps, &bc, &rc); e != nil {
			continue
		}
		amt = fdRound(amt, 2)
		total += amt
		out = append(out, map[string]interface{}{
			"cashflow_id": cid, "fd_id": fd, "bank": bank, "entity": ent,
			"expected_amount": amt, "tds_amount": fdRound(tds, 2),
			"event_type": evt, "posting_status": ps,
			"bank_confirmed": bc, "receipt_cleared": rc,
		})
	}
	return map[string]interface{}{
		"rows": out, "count": len(out), "total_expected": fdRound(total, 2),
	}, nil
}

// ─── Action list (owner-filterable) ──────────────────────────────────────────

func queryActionList(ctx context.Context, pool *pgxpool.Pool, entityFilter, ownerFilter string) (interface{}, error) {
	// $1 = entity, $2 = owner (empty string = no filter).
	rows, err := pool.Query(ctx, `
		SELECT * FROM (
		  SELECT b.booking_id AS ref_id, 'BOOKING' AS task_type,
		         COALESCE(b.entity_name,'') AS entity,
		         COALESCE(m.bank_name, m.bank_id, b.bank_name, b.bank_id,'') AS bank,
		         COALESCE(b.principal_amount,0)::float8 AS amount,
		         b.booking_status AS status,
		         COALESCE(EXTRACT(DAY FROM NOW()-b.created_at)::int, 0) AS aging_days,
		         COALESCE(b.created_by,'') AS owner
		  FROM investment.fd_booking_request b
		  LEFT JOIN investment.fd_master m ON m.booking_id=b.booking_id AND m.is_deleted=false
		  WHERE b.is_deleted=false
		    AND b.booking_status IN ('DRAFT','APPROVAL_PENDING','SENT_TO_BANK')
		    AND ($1::text='' OR b.entity_id=$1)
		  UNION ALL
		  SELECT cr.closure_request_id, 'CLOSURE',
		         COALESCE(cr.entity_name,''),
		         COALESCE(m.bank_name,''),
		         COALESCE(cr.principal_amount,0)::float8,
		         cr.closure_status,
		         COALESCE(EXTRACT(DAY FROM NOW()-cr.created_at)::int, 0),
		         COALESCE(cr.submitted_by,'')
		  FROM investment.fd_closure_request cr
		  JOIN investment.fd_master m ON m.fd_id=cr.fd_id AND m.is_deleted=false
		  WHERE cr.is_deleted=false
		    AND cr.closure_status IN ('PENDING_APPROVAL','APPROVED')
		    AND ($1::text='' OR cr.entity_id=$1)
		) t
		WHERE ($2::text='' OR t.owner = $2)
		ORDER BY aging_days DESC
		LIMIT 100`, entityFilter, ownerFilter)
	if err != nil {
		api.LogError("[BodEodDashV2] action_list: %v", err)
		return []interface{}{}, nil
	}
	defer rows.Close()
	out := []map[string]interface{}{}
	for rows.Next() {
		var ref, tt, ent, bank, status, owner string
		var amt float64
		var aging int
		if e := rows.Scan(&ref, &tt, &ent, &bank, &amt, &status, &aging, &owner); e != nil {
			continue
		}
		amt = fdRound(amt, 2)
		priority := "Low"
		switch {
		case aging >= 3:
			priority = "High"
		case aging >= 1:
			priority = "Medium"
		}
		out = append(out, map[string]interface{}{
			"ref_id": ref, "task_type": tt, "entity": ent, "bank": bank,
			"amount": amt, "status": status, "aging_days": aging,
			"owner": owner, "priority": priority,
		})
	}
	return out, nil
}

// ─── SLA breach yesterday ────────────────────────────────────────────────────

func querySlaBreachYesterday(ctx context.Context, pool *pgxpool.Pool, entityFilter string) (interface{}, error) {
	rows, err := pool.Query(ctx, `
		SELECT b.booking_id, COALESCE(b.entity_name,''),
		       COALESCE(m.bank_name, b.bank_name, b.bank_id,''),
		       b.booking_status,
		       COALESCE(EXTRACT(DAY FROM NOW()-b.created_at)::int, 0)
		FROM investment.fd_booking_request b
		LEFT JOIN investment.fd_master m ON m.booking_id=b.booking_id AND m.is_deleted=false
		WHERE b.is_deleted=false
		  AND b.booking_status NOT IN ('APPROVED','REJECTED')
		  AND EXTRACT(DAY FROM NOW()-b.created_at) >= 2
		  AND ($1::text='' OR b.entity_id=$1)
		ORDER BY aging_days DESC
		LIMIT 50`, entityFilter)
	if err != nil {
		return []interface{}{}, nil
	}
	defer rows.Close()
	out := []map[string]interface{}{}
	for rows.Next() {
		var id, ent, bank, status string
		var aging int
		if e := rows.Scan(&id, &ent, &bank, &status, &aging); e != nil {
			continue
		}
		out = append(out, map[string]interface{}{
			"booking_id": id, "entity": ent, "bank": bank,
			"status": status, "aging_days": aging,
		})
	}
	return out, nil
}

// ─── Bookings today / confirmations today / receipts today / etc. ───────────

func queryBookingsToday(ctx context.Context, pool *pgxpool.Pool, today, entityFilter string) (interface{}, error) {
	rows, err := pool.Query(ctx, `
		SELECT b.booking_id, COALESCE(b.entity_name,''),
		       COALESCE(m.bank_name, b.bank_name, b.bank_id,''),
		       COALESCE(b.principal_amount,0),
		       b.booking_status,
		       COALESCE(TO_CHAR(b.created_at,'YYYY-MM-DD"T"HH24:MI:SS'),''),
		       COALESCE(b.created_by,'')
		FROM investment.fd_booking_request b
		LEFT JOIN investment.fd_master m ON m.booking_id=b.booking_id AND m.is_deleted=false
		WHERE b.is_deleted=false
		  AND DATE(b.created_at) = $1::date
		  AND ($2::text='' OR b.entity_id=$2)
		ORDER BY b.created_at DESC
		LIMIT 100`, today, entityFilter)
	if err != nil {
		return map[string]interface{}{"rows": []interface{}{}, "count": 0, "total_amount": 0}, nil
	}
	defer rows.Close()
	out := []map[string]interface{}{}
	total := 0.0
	for rows.Next() {
		var id, ent, bank, status, at, by string
		var amt float64
		if e := rows.Scan(&id, &ent, &bank, &amt, &status, &at, &by); e != nil {
			continue
		}
		amt = fdRound(amt, 2)
		total += amt
		out = append(out, map[string]interface{}{
			"booking_id": id, "entity": ent, "bank": bank,
			"principal": amt, "status": status, "created_at": at, "created_by": by,
		})
	}
	return map[string]interface{}{
		"rows": out, "count": len(out), "total_amount": fdRound(total, 2),
	}, nil
}

func queryConfirmationsToday(ctx context.Context, pool *pgxpool.Pool, today, entityFilter string) (interface{}, error) {
	rows, err := pool.Query(ctx, `
		SELECT cf.confirmation_id, cf.booking_id,
		       COALESCE(b.entity_name,''),
		       COALESCE(m.bank_name, m.bank_id,''),
		       cf.actual_principal, cf.confirmed_rate, cf.confirmation_status,
		       COALESCE(TO_CHAR(cf.actual_maturity_date,'YYYY-MM-DD'),''),
		       cf.variance_flag, COALESCE(cf.variance_type,'')
		FROM investment.fd_confirmation cf
		JOIN investment.fd_booking_request b ON b.booking_id=cf.booking_id
		LEFT JOIN investment.fd_master m ON m.booking_id=cf.booking_id AND m.is_deleted=false
		WHERE cf.is_deleted=false
		  AND DATE(cf.created_at) = $1::date
		  AND ($2::text='' OR b.entity_id=$2)
		ORDER BY cf.created_at DESC
		LIMIT 100`, today, entityFilter)
	if err != nil {
		return map[string]interface{}{"rows": []interface{}{}, "count": 0, "variance_count": 0}, nil
	}
	defer rows.Close()
	out := []map[string]interface{}{}
	vc := 0
	for rows.Next() {
		var cid, bk, ent, bank, st, md, vt string
		var pr, rt float64
		var vf bool
		if e := rows.Scan(&cid, &bk, &ent, &bank, &pr, &rt, &st, &md, &vf, &vt); e != nil {
			continue
		}
		if vf {
			vc++
		}
		out = append(out, map[string]interface{}{
			"confirmation_id": cid, "booking_id": bk, "entity": ent, "bank": bank,
			"principal": fdRound(pr, 2), "confirmed_rate": fdRound(rt, 4),
			"status": st, "maturity_date": md,
			"variance_flag": vf, "variance_type": vt,
		})
	}
	return map[string]interface{}{
		"rows": out, "count": len(out), "variance_count": vc,
	}, nil
}

func queryReceiptsToday(ctx context.Context, pool *pgxpool.Pool, today, entityFilter string) (interface{}, error) {
	var ingested, matched, unmatched int64
	var total float64
	err := pool.QueryRow(ctx, `
		SELECT COUNT(*),
		       SUM(CASE WHEN COALESCE(reconciliation_status,'') IN ('MATCHED','RECONCILED') THEN 1 ELSE 0 END),
		       SUM(CASE WHEN COALESCE(reconciliation_status,'UNMATCHED') IN ('UNMATCHED','PENDING','') THEN 1 ELSE 0 END),
		       COALESCE(SUM(COALESCE(gross_interest_received,0)),0)
		FROM investment.fd_interest_receipt
		WHERE is_deleted=false
		  AND DATE(created_at) = $1::date
		  AND ($2::text='' OR entity_id=$2)`,
		today, entityFilter,
	).Scan(&ingested, &matched, &unmatched, &total)
	if err != nil {
		return map[string]interface{}{
			"ingested": 0, "matched": 0, "unmatched": 0,
			"total_amount": 0, "match_rate_pct": 0,
		}, nil
	}
	matchPct := 0.0
	if ingested > 0 {
		matchPct = fdRound(float64(matched)/float64(ingested)*100, 1)
	}
	return map[string]interface{}{
		"ingested": ingested, "matched": matched, "unmatched": unmatched,
		"total_amount": fdRound(total, 2), "match_rate_pct": matchPct,
	}, nil
}

func queryExceptionsToday(ctx context.Context, pool *pgxpool.Pool, today, entityFilter string) (interface{}, error) {
	var opened, closed, escalated int64
	err := pool.QueryRow(ctx, `
		SELECT
		  SUM(CASE WHEN exception_status NOT IN ('RESOLVED','CLOSED') THEN 1 ELSE 0 END),
		  SUM(CASE WHEN exception_status IN ('RESOLVED','CLOSED') THEN 1 ELSE 0 END),
		  SUM(CASE WHEN exception_status = 'ESCALATED' THEN 1 ELSE 0 END)
		FROM investment.fd_accrual_exception ae
		WHERE COALESCE(ae.is_deleted,false)=false
		  AND DATE(ae.created_at) = $1::date
		  AND ($2::text='' OR EXISTS (
		      SELECT 1 FROM investment.fd_master m WHERE m.fd_id=ae.fd_id AND m.entity_id=$2
		  ))`,
		today, entityFilter,
	).Scan(&opened, &closed, &escalated)
	if err != nil {
		return map[string]interface{}{"opened": 0, "closed": 0, "escalated": 0}, nil
	}
	return map[string]interface{}{
		"opened": opened, "closed": closed, "escalated": escalated,
	}, nil
}

func queryPostingToday(ctx context.Context, pool *pgxpool.Pool, today, entityFilter string) (interface{}, error) {
	var posted, failed, notPosted int64
	var total float64
	err := pool.QueryRow(ctx, `
		SELECT
		  SUM(CASE WHEN posting_status='POSTED' THEN 1 ELSE 0 END),
		  SUM(CASE WHEN posting_status='FAILED' THEN 1 ELSE 0 END),
		  SUM(CASE WHEN posting_status='NOT_POSTED' THEN 1 ELSE 0 END),
		  COALESCE(SUM(CASE WHEN posting_status='POSTED' THEN COALESCE(net_cash_flow,interest_accrued,0) ELSE 0 END),0)
		FROM investment.fd_cashflow_schedule cf
		JOIN investment.fd_master m ON m.fd_id=cf.fd_id AND m.is_deleted=false
		WHERE cf.is_deleted=false
		  AND DATE(cf.last_modified_at) = $1::date
		  AND ($2::text='' OR m.entity_id=$2)`,
		today, entityFilter,
	).Scan(&posted, &failed, &notPosted, &total)
	if err != nil {
		return map[string]interface{}{
			"posted": 0, "failed": 0, "not_posted": 0, "total_posted_amount": 0,
		}, nil
	}
	return map[string]interface{}{
		"posted": posted, "failed": failed, "not_posted": notPosted,
		"total_posted_amount": fdRound(total, 2),
	}, nil
}

func queryAccrualRunLatest(ctx context.Context, pool *pgxpool.Pool, entityFilter string) (interface{}, error) {
	var rid, rt, rs, ent, ps, rd, ca string
	var inScope, failed int64
	var total float64
	err := pool.QueryRow(ctx, `
		SELECT run_id, run_type, run_status, entity_name,
		       COALESCE(fds_in_scope,0), COALESCE(fds_failed,0),
		       COALESCE(total_interest_accrued,0),
		       posting_status,
		       COALESCE(TO_CHAR(run_date,'YYYY-MM-DD"T"HH24:MI:SS'),''),
		       COALESCE(TO_CHAR(completed_at,'YYYY-MM-DD"T"HH24:MI:SS'),'')
		FROM investment.fd_accrual_run
		WHERE is_deleted=false
		  AND ($1::text='' OR entity_id=$1)
		ORDER BY run_date DESC LIMIT 1`,
		entityFilter,
	).Scan(&rid, &rt, &rs, &ent, &inScope, &failed, &total, &ps, &rd, &ca)
	if err != nil {
		return map[string]interface{}{
			"run_id": "", "run_type": "", "run_status": "Unknown",
			"entity": "", "fds_in_scope": 0, "fds_failed": 0,
			"total_interest_accrued": 0, "posting_status": "",
			"run_date": "", "completed_at": "",
		}, nil
	}
	return map[string]interface{}{
		"run_id": rid, "run_type": rt, "run_status": rs, "entity": ent,
		"fds_in_scope": inScope, "fds_failed": failed,
		"total_interest_accrued": fdRound(total, 2),
		"posting_status":         ps, "run_date": rd, "completed_at": ca,
	}, nil
}

// ─── Bank concentration ──────────────────────────────────────────────────────

func queryBankConcentration(ctx context.Context, pool *pgxpool.Pool, entityFilter string) (interface{}, error) {
	rows, err := pool.Query(ctx, `
		SELECT m.bank_name, m.bank_id, COUNT(*),
		       COALESCE(SUM(m.principal_amount),0),
		       COALESCE(AVG(m.interest_rate),0),
		       COUNT(CASE WHEN m.maturity_date <= (CURRENT_DATE + 30) THEN 1 END),
		       COALESCE(SUM(CASE WHEN m.maturity_date <= (CURRENT_DATE + 30) THEN m.principal_amount ELSE 0 END),0)
		FROM investment.fd_master m
		WHERE m.is_deleted=false
		  AND m.fd_status IN ('ACTIVE','PENDING_ACTIVATION')
		  AND ($1::text='' OR m.entity_id=$1)
		GROUP BY m.bank_name, m.bank_id
		ORDER BY 4 DESC LIMIT 20`, entityFilter)
	if err != nil {
		return map[string]interface{}{"rows": []interface{}{}, "grand_total": 0}, nil
	}
	defer rows.Close()
	type row struct {
		Name, ID string
		Count    int64
		Total    float64
		AvgRate  float64
		Mat30    int64
		Mat30Amt float64
		Pct      float64
	}
	out := []*row{}
	grand := 0.0
	for rows.Next() {
		r := &row{}
		if e := rows.Scan(&r.Name, &r.ID, &r.Count, &r.Total, &r.AvgRate, &r.Mat30, &r.Mat30Amt); e != nil {
			continue
		}
		r.Total = fdRound(r.Total, 2)
		r.AvgRate = fdRound(r.AvgRate, 4)
		r.Mat30Amt = fdRound(r.Mat30Amt, 2)
		grand += r.Total
		out = append(out, r)
	}
	asMap := make([]map[string]interface{}, 0, len(out))
	for _, r := range out {
		if grand > 0 {
			r.Pct = fdRound(r.Total/grand*100, 2)
		}
		asMap = append(asMap, map[string]interface{}{
			"bank_name": r.Name, "bank_id": r.ID, "fd_count": r.Count,
			"total_principal": r.Total, "avg_rate": r.AvgRate,
			"maturing_30d": r.Mat30, "maturing_30d_amount": r.Mat30Amt,
			"concentration_pct": r.Pct,
		})
	}
	return map[string]interface{}{
		"rows": asMap, "grand_total": fdRound(grand, 2),
	}, nil
}

// ─── NEW: Bank contacts (manual + auto-derived) ─────────────────────────────

func queryBankContacts(ctx context.Context, pool *pgxpool.Pool, entityFilter, ownerFilter string) (interface{}, error) {
	// Manual rows from cimplr.fd_bod_eod_bank_contact (graceful fallback if missing).
	manual, _ := pool.Query(ctx, `
		SELECT contact_id, entity_id, COALESCE(bank_id,''), COALESCE(bank_name,''),
		       COALESCE(reason_code,''), COALESCE(reference_id,''),
		       COALESCE(assigned_to,''),
		       COALESCE(TO_CHAR(due_at,'YYYY-MM-DD"T"HH24:MI:SS'),''),
		       status,
		       COALESCE(TO_CHAR(created_at,'YYYY-MM-DD"T"HH24:MI:SS'),'')
		FROM cimplr.fd_bod_eod_bank_contact
		WHERE COALESCE(is_deleted,false)=false
		  AND status <> 'DONE'
		  AND ($1::text='' OR entity_id=$1)
		  AND ($2::text='' OR assigned_to=$2)
		ORDER BY due_at NULLS LAST, created_at DESC
		LIMIT 100`, entityFilter, ownerFilter)
	out := []map[string]interface{}{}
	if manual != nil {
		defer manual.Close()
		for manual.Next() {
			var id int64
			var ent, bid, bn, rc, ref, owner, due, status, created string
			if e := manual.Scan(&id, &ent, &bid, &bn, &rc, &ref, &owner, &due, &status, &created); e != nil {
				continue
			}
			out = append(out, map[string]interface{}{
				"contact_id": id, "entity_id": ent, "bank_id": bid, "bank_name": bn,
				"reason_code": rc, "reference_id": ref, "assigned_to": owner,
				"due_at": due, "status": status, "created_at": created,
				"source": "MANUAL",
			})
		}
	}

	// Auto-derived from variance confirmations (no manual config needed).
	derived, err := pool.Query(ctx, `
		SELECT cf.confirmation_id, COALESCE(b.entity_id,''), '',
		       COALESCE(m.bank_name, m.bank_id,''),
		       'VARIANCE'::text, cf.booking_id::text,
		       '', '', 'OPEN'::text
		FROM investment.fd_confirmation cf
		JOIN investment.fd_booking_request b ON b.booking_id=cf.booking_id
		LEFT JOIN investment.fd_master m ON m.booking_id=cf.booking_id AND m.is_deleted=false
		WHERE cf.is_deleted=false
		  AND cf.variance_flag=true
		  AND COALESCE(cf.variance_action,'PENDING')='PENDING'
		  AND ($1::text='' OR b.entity_id=$1)
		LIMIT 50`, entityFilter)
	if err == nil && derived != nil {
		defer derived.Close()
		for derived.Next() {
			var cid, ent, bid, bn, rc, ref, owner, due, status string
			if e := derived.Scan(&cid, &ent, &bid, &bn, &rc, &ref, &owner, &due, &status); e != nil {
				continue
			}
			out = append(out, map[string]interface{}{
				"contact_id": 0, "entity_id": ent, "bank_id": bid, "bank_name": bn,
				"reason_code": rc, "reference_id": ref, "assigned_to": owner,
				"due_at": due, "status": status, "source": "DERIVED",
			})
		}
	}
	return map[string]interface{}{
		"rows": out, "count": len(out),
	}, nil
}

// ─── NEW: Offers expiring today (rate-negotiation; graceful fallback) ───────

func queryOffersExpiringToday(ctx context.Context, pool *pgxpool.Pool, today, entityFilter string) (interface{}, error) {
	rows, err := pool.Query(ctx, `
		SELECT n.negotiation_id, COALESCE(n.bank_name, n.bank_id,''),
		       COALESCE(n.amount,0), COALESCE(n.offered_rate,0),
		       COALESCE(n.tenor,''),
		       COALESCE(TO_CHAR(n.offer_expiry_at,'YYYY-MM-DD"T"HH24:MI:SS'),''),
		       COALESCE(n.negotiation_status,'')
		FROM investment.fd_bank_rate_negotiation n
		WHERE COALESCE(n.is_deleted,false)=false
		  AND DATE(n.offer_expiry_at) = $1::date
		  AND ($2::text='' OR n.entity_id=$2)
		ORDER BY n.offer_expiry_at ASC
		LIMIT 50`, today, entityFilter)
	if err != nil {
		// Table likely doesn't exist yet (rate-negotiation module pending).
		return map[string]interface{}{"rows": []interface{}{}, "count": 0}, nil
	}
	defer rows.Close()
	out := []map[string]interface{}{}
	for rows.Next() {
		var id, bank, tenor, exp, status string
		var amt, rate float64
		if e := rows.Scan(&id, &bank, &amt, &rate, &tenor, &exp, &status); e != nil {
			continue
		}
		out = append(out, map[string]interface{}{
			"negotiation_id": id, "bank": bank, "amount": fdRound(amt, 2),
			"offered_rate": fdRound(rate, 4), "tenor": tenor,
			"expires_at": exp, "status": status,
		})
	}
	return map[string]interface{}{"rows": out, "count": len(out)}, nil
}

// ─── NEW: Handover notes ─────────────────────────────────────────────────────

func queryHandoverNotes(ctx context.Context, pool *pgxpool.Pool, entityFilter, today, mode, ownerFilter string) (interface{}, error) {
	rows, err := pool.Query(ctx, `
		SELECT note_id, note, tags, created_by,
		       TO_CHAR(created_at, 'YYYY-MM-DD"T"HH24:MI:SS')
		FROM cimplr.fd_bod_eod_handover_note
		WHERE COALESCE(is_deleted,false)=false
		  AND business_date = $1::date
		  AND mode = $2
		  AND ($3::text='' OR entity_id=$3)
		  AND ($4::text='' OR created_by=$4)
		ORDER BY created_at DESC
		LIMIT 25`, today, mode, entityFilter, ownerFilter)
	if err != nil {
		return []interface{}{}, nil
	}
	defer rows.Close()
	out := []map[string]interface{}{}
	for rows.Next() {
		var id int64
		var note, by, at string
		var tags []string
		if e := rows.Scan(&id, &note, &tags, &by, &at); e != nil {
			continue
		}
		out = append(out, map[string]interface{}{
			"note_id": id, "note": note, "tags": tags,
			"created_by": by, "created_at": at,
		})
	}
	return out, nil
}

// ─── NEW: Audit log today (unions key FD audit tables) ──────────────────────

func queryAuditToday(ctx context.Context, pool *pgxpool.Pool, today, entityFilter string) (interface{}, error) {
	// Best-effort union across known audit tables. Each LEFT branch is gated
	// so a missing table simply contributes 0 rows (the surrounding query
	// still succeeds because we run independent queries below).

	out := []map[string]interface{}{}

	add := func(rows []map[string]interface{}) {
		out = append(out, rows...)
	}

	scanAudit := func(q string, args ...interface{}) []map[string]interface{} {
		rs, err := pool.Query(ctx, q, args...)
		if err != nil {
			return nil
		}
		defer rs.Close()
		res := []map[string]interface{}{}
		for rs.Next() {
			var src, ref, action, status, by, at string
			if e := rs.Scan(&src, &ref, &action, &status, &by, &at); e != nil {
				continue
			}
			res = append(res, map[string]interface{}{
				"source": src, "ref_id": ref, "action": action,
				"status": status, "requested_by": by, "requested_at": at,
			})
		}
		return res
	}

	// closure_initiate
	add(scanAudit(`
		SELECT 'CLOSURE_INITIATE', ci.closure_initiate_id::text,
		       COALESCE(a.processing_status,''),
		       COALESCE(ci.closure_status,''),
		       COALESCE(a.requested_by,''),
		       COALESCE(TO_CHAR(a.requested_at,'YYYY-MM-DD"T"HH24:MI:SS'),'')
		FROM cimplr.fd_closure_initiate_audit a
		JOIN cimplr.fd_closure_initiate ci ON ci.closure_initiate_id = a.closure_initiate_id
		WHERE DATE(a.requested_at) = $1::date
		  AND ($2::text='' OR ci.entity_id=$2)
		ORDER BY a.requested_at DESC LIMIT 50`, today, entityFilter))

	// confirmation audit (via fd_master entity)
	add(scanAudit(`
		SELECT 'CONFIRMATION', cc.closure_confirm_id::text,
		       COALESCE(a.processing_status,''),
		       COALESCE(cc.closure_status,''),
		       COALESCE(a.requested_by,''),
		       COALESCE(TO_CHAR(a.requested_at,'YYYY-MM-DD"T"HH24:MI:SS'),'')
		FROM cimplr.fd_closure_confirm_audit a
		JOIN cimplr.fd_closure_confirm cc ON cc.closure_confirm_id = a.closure_confirm_id
		LEFT JOIN investment.fd_master m ON m.fd_id = cc.fd_id AND m.is_deleted=false
		WHERE DATE(a.requested_at) = $1::date
		  AND ($2::text='' OR m.entity_id=$2)
		ORDER BY a.requested_at DESC LIMIT 50`, today, entityFilter))

	return out, nil
}

// ─── NEW: Overrides today ────────────────────────────────────────────────────

func queryOverridesToday(ctx context.Context, pool *pgxpool.Pool, today, entityFilter string) (interface{}, error) {
	rows, err := pool.Query(ctx, `
		SELECT l.ledger_id::text, COALESCE(l.fd_id,''), COALESCE(l.entity_id,''),
		       COALESCE(l.override_amount,0), COALESCE(l.original_amount,0),
		       COALESCE(l.reason_code,''), COALESCE(l.override_status,''),
		       COALESCE(l.proposed_by,''), COALESCE(l.approved_by,''),
		       COALESCE(TO_CHAR(l.proposed_at,'YYYY-MM-DD"T"HH24:MI:SS'),'')
		FROM investment.fd_accrual_ledger l
		WHERE l.override_status IS NOT NULL
		  AND DATE(l.proposed_at) = $1::date
		  AND ($2::text='' OR l.entity_id=$2)
		ORDER BY l.proposed_at DESC
		LIMIT 100`, today, entityFilter)
	if err != nil {
		return []interface{}{}, nil
	}
	defer rows.Close()
	out := []map[string]interface{}{}
	for rows.Next() {
		var id, fd, ent, rc, status, by, app, at string
		var ovr, orig float64
		if e := rows.Scan(&id, &fd, &ent, &ovr, &orig, &rc, &status, &by, &app, &at); e != nil {
			continue
		}
		out = append(out, map[string]interface{}{
			"ledger_id": id, "fd_id": fd, "entity_id": ent,
			"override_amount": fdRound(ovr, 2),
			"original_amount": fdRound(orig, 2),
			"delta":           fdRound(ovr-orig, 2),
			"reason_code":     rc, "override_status": status,
			"proposed_by": by, "approved_by": app, "proposed_at": at,
		})
	}
	return out, nil
}

// ─── NEW: Closing readiness (lightweight) ───────────────────────────────────

func queryClosingReadiness(ctx context.Context, pool *pgxpool.Pool, entityFilter string) (interface{}, error) {
	// Simplified: count of pending approval items across booking/confirmation/closure
	// gives a "closing-ready or not" signal. Real period-close belongs in the
	// MonthQuarterEnd module.
	var bookingPending, confirmPending, closurePending, accrualPending int64

	_ = pool.QueryRow(ctx, `SELECT COUNT(*) FROM investment.fd_booking_request
		WHERE is_deleted=false AND booking_status IN ('DRAFT','APPROVAL_PENDING')
		AND ($1::text='' OR entity_id=$1)`, entityFilter).Scan(&bookingPending)
	_ = pool.QueryRow(ctx, `SELECT COUNT(*) FROM investment.fd_confirmation cf
		JOIN investment.fd_booking_request b ON b.booking_id=cf.booking_id
		WHERE cf.is_deleted=false
		  AND UPPER(COALESCE(cf.confirmation_status,'')) IN ('DRAFT','CAPTURED','PENDING_APPROVAL','PENDING')
		  AND ($1::text='' OR b.entity_id=$1)`, entityFilter).Scan(&confirmPending)
	_ = pool.QueryRow(ctx, `SELECT COUNT(*) FROM investment.fd_closure_request
		WHERE is_deleted=false AND closure_status IN ('PENDING_APPROVAL','APPROVED')
		  AND ($1::text='' OR entity_id=$1)`, entityFilter).Scan(&closurePending)
	_ = pool.QueryRow(ctx, `SELECT COUNT(*) FROM investment.fd_accrual_run
		WHERE is_deleted=false AND run_status IN ('PENDING_APPROVAL','DRAFT')
		  AND ($1::text='' OR entity_id=$1)`, entityFilter).Scan(&accrualPending)

	total := 4.0
	done := 0
	if bookingPending == 0 {
		done++
	}
	if confirmPending == 0 {
		done++
	}
	if closurePending == 0 {
		done++
	}
	if accrualPending == 0 {
		done++
	}
	pct := fdRound(float64(done)/total*100, 1)
	blockers := int(bookingPending + confirmPending + closurePending + accrualPending)

	return map[string]interface{}{
		"pct":             pct,
		"completed":       done,
		"total":           int(total),
		"blockers":        blockers,
		"booking_pending": bookingPending,
		"confirm_pending": confirmPending,
		"closure_pending": closurePending,
		"accrual_pending": accrualPending,
	}, nil
}

// ─── NEW: Sign-off run header ───────────────────────────────────────────────

func queryRunHeader(ctx context.Context, pool *pgxpool.Pool, entityID, today, mode string) (interface{}, error) {
	if strings.TrimSpace(entityID) == "" {
		// Per-entity sign-off makes little sense across "all entities" view —
		// return a stub the UI can render as "Open" so the section still shows.
		return map[string]interface{}{
			"run_id": 0, "status": "OPEN", "signed_off_by": "", "signed_off_at": "",
		}, nil
	}
	var runID int64
	var status, by, at string
	err := pool.QueryRow(ctx, `
		SELECT run_id, status, COALESCE(signed_off_by,''),
		       COALESCE(TO_CHAR(signed_off_at,'YYYY-MM-DD"T"HH24:MI:SS'),'')
		FROM cimplr.fd_bod_eod_run
		WHERE entity_id = $1 AND business_date = $2::date AND mode = $3
		LIMIT 1`, entityID, today, mode).Scan(&runID, &status, &by, &at)
	if err != nil {
		return map[string]interface{}{
			"run_id": 0, "status": "OPEN", "signed_off_by": "", "signed_off_at": "",
		}, nil
	}
	return map[string]interface{}{
		"run_id": runID, "status": status,
		"signed_off_by": by, "signed_off_at": at,
	}, nil
}
