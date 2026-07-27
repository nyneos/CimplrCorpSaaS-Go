package exposures

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// hedgeLinkRow is the canonical business-field shape for HEDGE_LINK — every
// real scalar column on public.exposure_hedge_links, plus the
// forward_booking_ledger-derived values (action_type/action_date/
// amount_changed/running_open_amount/user_id) that LinkExposureHedge — the
// only real mutation handler for this sub-module — writes as a side effect
// of the same link action and that domain_catalog already seeds under
// HEDGE_LINK. Those five are transient per-action values, not stored state
// on exposure_hedge_links itself, so loadHedgeLinkRow (for a future
// approve/reject/delete on a link, none of which exist yet) only populates
// the six real columns and leaves them zero-valued — documented, not a bug.
type hedgeLinkRow struct {
	LinkID           string
	ExposureHeaderID string
	BookingID        string
	HedgedAmount     float64
	LinkDate         string
	IsActive         bool
	// Ledger-derived, only meaningfully populated at Create time (see
	// buildHedgeLinkRowForCreate in linkage.go).
	ActionType        string
	ActionDate        string
	AmountChanged     float64
	RunningOpenAmount float64
	UserID            string
}

// buildHedgeLinkPolicyFields maps the canonical row onto the exact field_code
// keys seeded in domain_catalog for HEDGE_LINK (see
// cmd/seedDomainCatalog/hedgeLinkCanonical.go).
func buildHedgeLinkPolicyFields(row hedgeLinkRow) map[string]interface{} {
	return map[string]interface{}{
		"link_id":             row.LinkID,
		"exposure_header_id":  row.ExposureHeaderID,
		"booking_id":          row.BookingID,
		"hedged_amount":       row.HedgedAmount,
		"link_date":           row.LinkDate,
		"is_active":           row.IsActive,
		"action_type":         row.ActionType,
		"action_date":         row.ActionDate,
		"amount_changed":      row.AmountChanged,
		"running_open_amount": row.RunningOpenAmount,
		"user_id":             row.UserID,
	}
}

// loadHedgeLinkRow fetches the full real exposure_hedge_links row by its
// natural (exposure_header_id, booking_id) key — for a future action that
// only receives those two IDs rather than the full link payload. Not called
// today (LinkExposureHedge is the only handler and always has the full
// payload in hand), included for shape parity with every other sub-module in
// this rollout.
func loadHedgeLinkRow(ctx context.Context, pool *pgxpool.Pool, exposureHeaderID, bookingID string) (hedgeLinkRow, error) {
	var row hedgeLinkRow
	row.ExposureHeaderID = exposureHeaderID
	row.BookingID = bookingID
	err := pool.QueryRow(ctx, `
		SELECT link_id::text, COALESCE(hedged_amount,0), COALESCE(link_date::text,''), COALESCE(is_active,false)
		FROM exposure_hedge_links
		WHERE exposure_header_id = $1 AND booking_id = $2`, exposureHeaderID, bookingID,
	).Scan(&row.LinkID, &row.HedgedAmount, &row.LinkDate, &row.IsActive)
	if err != nil {
		return row, fmt.Errorf("load hedge link row for policy: %w", err)
	}
	return row, nil
}
