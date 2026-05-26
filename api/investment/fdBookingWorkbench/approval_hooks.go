package fdBooking

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/approvalengine"
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

func init() {
	approvalengine.RegisterPostFinalizeHook("FD_CONFIRMATION_DELETE", func(ctx context.Context, pool *pgxpool.Pool, confirmationID, transactionType, finalStatus, actorEmail, comment string) {
		if finalStatus != approvalengine.InstStatusApproved {
			return
		}
		_, err := pool.Exec(ctx, `
			UPDATE investment.fd_booking_request b
			SET booking_status='SENT_TO_BANK'
			WHERE b.booking_id IN (
				SELECT c.booking_id
				FROM investment.fd_confirmation c
				WHERE c.confirmation_id=$1
			)
			  AND NOT EXISTS (
				SELECT 1
				FROM investment.fd_master fm
				WHERE (fm.confirmation_id=$1 OR fm.booking_id=b.booking_id)
				  AND COALESCE(fm.is_deleted,false)=false
			  )`,
			confirmationID,
		)
		if err != nil {
			api.LogError("[FDConfirmation] post-finalize delete rollback failed confirmation=%s: %v", confirmationID, err)
		}
	})
}
