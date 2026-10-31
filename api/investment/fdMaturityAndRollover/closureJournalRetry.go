package fdMaturityAndRollover

import (
	"context"
	"errors"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	closureStatusPosted = "POSTED"
	closureJournalRetryGrace = "10 minutes"
	closureJournalRetryBatch = 50
)

var errClosureAlreadyPosted = errors.New("closure journals already posted")

func isClosureAlreadyPosted(err error) bool {
	return errors.Is(err, errClosureAlreadyPosted)
}

func RetryApprovedClosureJournals(ctx context.Context, pool *pgxpool.Pool) (posted, failed int) {
	rows, err := pool.Query(ctx, `
		SELECT cr.closure_request_id, cr.fd_id, cr.closure_type,
		       COALESCE(cr.entity_id,''), COALESCE(cr.entity_name,''),
		       COALESCE(cr.principal_amount,0), COALESCE(cr.accrued_interest,0), COALESCE(cr.tds_deducted,0),
		       COALESCE(cr.penalty_amount,0), COALESCE(cr.net_payout_amount,0),
		       COALESCE(act.actor_user_id,''), COALESCE(act.actor_email,'')
		FROM investment.fd_closure_request cr
		JOIN LATERAL (
			SELECT ai.instance_id, ai.status, ai.transaction_type
			FROM uam.approval_instance ai
			WHERE ai.record_id = cr.closure_request_id
			  AND ai.module_code = 'FIXED_DEPOSIT'
			  AND ai.is_deleted = false
			ORDER BY ai.submitted_at DESC
			LIMIT 1
		) inst ON true
		LEFT JOIN LATERAL (
			SELECT a.actor_user_id, a.actor_email
			FROM uam.approval_instance_action a
			WHERE a.instance_id = inst.instance_id AND a.action_type = $1
			ORDER BY a.acted_at DESC
			LIMIT 1
		) act ON true
		WHERE cr.is_deleted = false
		  AND cr.closure_status = $2
		  AND inst.status = $1
		  AND inst.transaction_type IN ('FD_CLOSURE_MATURITY','FD_CLOSURE_PREMATURE','FD_CLOSURE_ROLLOVER','FD_CLOSURE_AUTO_RENEWAL')
		  AND cr.updated_at < NOW() - $3::interval
		ORDER BY cr.updated_at
		LIMIT $4`,
		constants.StatusApproved, constants.StatusPendingApproval, closureJournalRetryGrace, closureJournalRetryBatch)
	if err != nil {
		api.LogError("[FDClosure] journal retry query: %v", err)
		return 0, 0
	}
	var pending []PostClosureJournalsParams
	for rows.Next() {
		p := PostClosureJournalsParams{Pool: pool}
		if err := rows.Scan(&p.ClosureRequestID, &p.FDID, &p.ClosureType, &p.EntityID, &p.EntityName,
			&p.PrincipalAmt, &p.AccruedInterest, &p.TDSAmt, &p.PenaltyAmt, &p.NetPayout,
			&p.ApprovedBy, &p.ApprovedByEmail); err != nil {
			rows.Close()
			api.LogError("[FDClosure] journal retry scan: %v", err)
			return 0, 0
		}
		if p.ApprovedByEmail == "" {
			p.ApprovedByEmail = "system@cimplr.auto"
		}
		pending = append(pending, p)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		api.LogError("[FDClosure] journal retry rows: %v", err)
		return 0, 0
	}
	for _, p := range pending {
		postCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		err := postClosureJournals(postCtx, p)
		cancel()
		switch {
		case err == nil:
			posted++
			api.LogInfo("[FDClosure] journal retry posted closure=%s", p.ClosureRequestID)
		case isClosureAlreadyPosted(err):
		default:
			failed++
			api.LogError("[FDClosure] journal retry failed closure=%s: %v", p.ClosureRequestID, err)
		}
	}
	return posted, failed
}
