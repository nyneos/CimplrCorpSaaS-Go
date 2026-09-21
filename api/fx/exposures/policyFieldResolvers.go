package exposures

import (
	"context"
	"fmt"
	"strings"

	"CimplrCorpSaas/api/policyengine/runtime"

	"github.com/jackc/pgx/v5/pgxpool"
)

func init() {
	runtime.RegisterRecordFieldResolver("EXPOSURE_SETTLEMENT", func(ctx context.Context, pool *pgxpool.Pool, recordID string) (map[string]interface{}, error) {
		row, err := LoadExposureSettlementRow(ctx, pool, recordID)
		if err != nil {
			return nil, err
		}
		return BuildExposureSettlementPolicyFields(row), nil
	})

	runtime.RegisterRecordFieldResolver("FX_HEDGING_PROPOSAL", func(ctx context.Context, pool *pgxpool.Pool, recordID string) (map[string]interface{}, error) {
		row, err := LoadHedgingProposalPolicyRow(ctx, pool, recordID)
		if err != nil {
			return nil, err
		}
		return BuildHedgingProposalPolicyFields(row), nil
	})

	runtime.RegisterRecordFieldResolver("HEDGE_LINK", func(ctx context.Context, pool *pgxpool.Pool, recordID string) (map[string]interface{}, error) {
		exposureHeaderID, bookingID, ok := splitHedgeLinkRecordID(recordID)
		if !ok {
			return nil, fmt.Errorf("hedge link record id must be exposure_header_id:booking_id")
		}
		row, err := loadHedgeLinkRow(ctx, pool, exposureHeaderID, bookingID)
		if err != nil {
			return nil, err
		}
		return buildHedgeLinkPolicyFields(row), nil
	})
}

func splitHedgeLinkRecordID(recordID string) (string, string, bool) {
	parts := strings.SplitN(strings.TrimSpace(recordID), ":", 2)
	if len(parts) != 2 {
		return "", "", false
	}
	exposureHeaderID := strings.TrimSpace(parts[0])
	bookingID := strings.TrimSpace(parts[1])
	if exposureHeaderID == "" || bookingID == "" {
		return "", "", false
	}
	return exposureHeaderID, bookingID, true
}
