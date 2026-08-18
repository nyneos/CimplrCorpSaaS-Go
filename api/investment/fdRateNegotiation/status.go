package fdRateNegotiation

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

func parentRequestStatus(ctx context.Context, tx pgx.Tx, rateRequestID string) (string, error) {
	var status string
	err := tx.QueryRow(ctx, `
		SELECT UPPER(COALESCE(request_status,''))
		FROM investment.fd_rate_negotiation
		WHERE rate_request_id = $1::uuid AND COALESCE(is_deleted,false)=false`,
		rateRequestID).Scan(&status)
	if err != nil {
		return "", err
	}
	return status, nil
}

func assertParentStatus(ctx context.Context, tx pgx.Tx, rateRequestID string, allowed ...string) error {
	status, err := parentRequestStatus(ctx, tx, rateRequestID)
	if err != nil {
		return fmt.Errorf("rate request not found")
	}
	for _, a := range allowed {
		if status == strings.ToUpper(strings.TrimSpace(a)) {
			return nil
		}
	}
	return fmt.Errorf("rate request status %s is not eligible (allowed: %s)", status, strings.Join(allowed, ", "))
}
