package forwards

import (
	"context"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// forwardCancelRollPolicyFields.go — canonical policy field builders for the
// three sub-modules that share fwdCancelRoll.go: FORWARD_CANCELLATION
// (public.forward_cancellations), FORWARD_ROLLOVER (public.forward_rollovers),
// and FORWARD_CANCEL_ROLL (no dedicated table — the shared bulk
// approve/reject/delete action over pending rows from either of the two
// tables above).
//
// Findings before this file:
//   - FORWARD_CANCELLATION: CreateForwardCancellations and
//     CancellationStatusRequest both key off a *map* of booking_id -> amount
//     with one common cancellation_date/rate/reason applied to every booking
//     in the batch — there is no single per-request row, so both are kept as
//     one Enforce call per request (unchanged call cadence) with Fields built
//     from the batch-common scalars via the same canonical row/builder,
//     rather than per-booking data that varies across the map.
//   - FORWARD_ROLLOVER: RolloverForwardBooking had a genuine field-naming bug,
//     not just thin coverage — its Enforce Fields map passed keys named
//     "realized_gain_loss" and "cancellation_reason" (copy-pasted from the
//     cancellation handler) even though the value that key held
//     (req.RealizedGainLoss) is actually inserted into forward_rollovers as
//     rollover_cost, and no cancellation_reason column exists on this table
//     at all. domain_catalog had this bogus field_code seeded too — removed
//     in cmd/seedDomainCatalog/forwardRolloverCanonical.go, replaced with the
//     real rollover_cost/fx_pair/order_type/new_forward_* columns.
//     RolloverStatusRequest was thin (status + booking_count only).
//   - FORWARD_CANCEL_ROLL: CancellationRolloverAction processes a list of
//     heterogeneous items (each independently typed cancellation-or-rollover,
//     by booking_id + request_date) but previously called Enforce ONCE before
//     the loop with only action/item_count — no item-specific data at all.
//     Per the task's own suggestion, this now loads whichever of the two
//     tables the target item belongs to and reuses that sub-module's row
//     builder, PER ITEM, moving the Enforce call inside the loop (same
//     per-item-closure pattern BulkUpdateForwardBookingProcessingStatus
//     already uses for FORWARD_BOOKING in this package) so a policy checking
//     e.g. cancellation_rate or rollover_cost actually sees it. This is a
//     judgment call the reviewer should double-check: it changes Enforce from
//     one call per request to one call per item (same as the FORWARD_BOOKING
//     precedent), which is a larger behavioral change than the pure
//     field-consistency fixes elsewhere in this rollout.

// ─────────────────────────── FORWARD_CANCELLATION ──────────────────────────

// fwdCancellationRow is the canonical field set for FORWARD_CANCELLATION
// (public.forward_cancellations). created_at/is_deleted excluded as audit
// plumbing.
type fwdCancellationRow struct {
	CancellationID     string
	BookingID          string
	AmountCancelled    *float64
	CancellationDate   string
	CancellationRate   *float64
	RealizedGainLoss   *float64
	CancellationReason string
	Status             string
}

// buildForwardCancellationPolicyFields maps the canonical row onto the exact
// field_code keys seeded in domain_catalog for FORWARD_CANCELLATION.
func buildForwardCancellationPolicyFields(row fwdCancellationRow) map[string]interface{} {
	return map[string]interface{}{
		"cancellation_id":     row.CancellationID,
		"booking_id":          row.BookingID,
		"amount_cancelled":    row.AmountCancelled,
		"cancellation_date":   row.CancellationDate,
		"cancellation_rate":   row.CancellationRate,
		"realized_gain_loss":  row.RealizedGainLoss,
		"cancellation_reason": row.CancellationReason,
		"status":              row.Status,
	}
}

// loadForwardCancellationRow fetches the most recent forward_cancellations
// row for a given booking_id + cancellation_date — the compound key every
// real call site (executeCancellationApproval, cancelRollRowSnapshot, etc.)
// already keys off, since cancellation_id itself never appears in any
// request payload.
func loadForwardCancellationRow(ctx context.Context, pool *pgxpool.Pool, bookingID, cancellationDate string) (fwdCancellationRow, error) {
	var row fwdCancellationRow
	err := pool.QueryRow(ctx, `
		SELECT cancellation_id::text, booking_id::text, amount_cancelled,
		       COALESCE(TO_CHAR(cancellation_date,'YYYY-MM-DD'),''), cancellation_rate, realized_gain_loss,
		       COALESCE(cancellation_reason,''), COALESCE(status,'')
		FROM forward_cancellations
		WHERE booking_id = $1 AND cancellation_date = $2 AND COALESCE(is_deleted, false) = false
		ORDER BY created_at DESC LIMIT 1`, bookingID, cancellationDate,
	).Scan(&row.CancellationID, &row.BookingID, &row.AmountCancelled,
		&row.CancellationDate, &row.CancellationRate, &row.RealizedGainLoss,
		&row.CancellationReason, &row.Status)
	if err != nil {
		return fwdCancellationRow{}, err
	}
	return row, nil
}

// ───────────────────────────── FORWARD_ROLLOVER ────────────────────────────

// fwdRolloverRow is the canonical field set for FORWARD_ROLLOVER
// (public.forward_rollovers). created_at/is_deleted excluded as audit
// plumbing.
type fwdRolloverRow struct {
	RolloverID                string
	BookingID                 string
	AmountRolledOver          *float64
	RolloverDate              string
	OriginalMaturityDate      string
	NewMaturityDate           string
	RolloverCost              *float64
	Status                    string
	FXPair                    string
	OrderType                 string
	NewForwardAmount          *float64
	NewForwardSpotRate        *float64
	NewForwardPremiumDiscount *float64
	NewForwardMarginRate      *float64
	NewForwardNetRate         *float64
}

// buildForwardRolloverPolicyFields maps the canonical row onto the exact
// field_code keys seeded in domain_catalog for FORWARD_ROLLOVER.
func buildForwardRolloverPolicyFields(row fwdRolloverRow) map[string]interface{} {
	return map[string]interface{}{
		"rollover_id":                  row.RolloverID,
		"booking_id":                   row.BookingID,
		"amount_rolled_over":           row.AmountRolledOver,
		"rollover_date":                row.RolloverDate,
		"original_maturity_date":       row.OriginalMaturityDate,
		"new_maturity_date":            row.NewMaturityDate,
		"rollover_cost":                row.RolloverCost,
		"status":                       row.Status,
		"fx_pair":                      row.FXPair,
		"order_type":                   row.OrderType,
		"new_forward_amount":           row.NewForwardAmount,
		"new_forward_spot_rate":        row.NewForwardSpotRate,
		"new_forward_premium_discount": row.NewForwardPremiumDiscount,
		"new_forward_margin_rate":      row.NewForwardMarginRate,
		"new_forward_net_rate":         row.NewForwardNetRate,
	}
}

// loadForwardRolloverRow fetches the most recent forward_rollovers row for a
// given booking_id + rollover_date — the same compound-key rationale as
// loadForwardCancellationRow above.
func loadForwardRolloverRow(ctx context.Context, pool *pgxpool.Pool, bookingID, rolloverDate string) (fwdRolloverRow, error) {
	var row fwdRolloverRow
	err := pool.QueryRow(ctx, `
		SELECT rollover_id::text, booking_id::text, amount_rolled_over,
		       COALESCE(TO_CHAR(rollover_date,'YYYY-MM-DD'),''),
		       COALESCE(TO_CHAR(original_maturity_date,'YYYY-MM-DD'),''),
		       COALESCE(TO_CHAR(new_maturity_date,'YYYY-MM-DD'),''),
		       rollover_cost, COALESCE(status,''), COALESCE(fx_pair,''), COALESCE(order_type,''),
		       new_forward_amount, new_forward_spot_rate, new_forward_premium_discount,
		       new_forward_margin_rate, new_forward_net_rate
		FROM forward_rollovers
		WHERE booking_id = $1 AND rollover_date = $2 AND COALESCE(is_deleted, false) = false
		ORDER BY created_at DESC LIMIT 1`, bookingID, rolloverDate,
	).Scan(&row.RolloverID, &row.BookingID, &row.AmountRolledOver,
		&row.RolloverDate, &row.OriginalMaturityDate, &row.NewMaturityDate,
		&row.RolloverCost, &row.Status, &row.FXPair, &row.OrderType,
		&row.NewForwardAmount, &row.NewForwardSpotRate, &row.NewForwardPremiumDiscount,
		&row.NewForwardMarginRate, &row.NewForwardNetRate)
	if err != nil {
		return fwdRolloverRow{}, err
	}
	return row, nil
}

// ─────────────────────────── FORWARD_CANCEL_ROLL ───────────────────────────

// forwardCancelRollItemPolicyFields loads whichever of the two tables the
// target item belongs to (by requestType) and reuses that sub-module's row
// builder, then layers the shared CANCEL_ROLL request-level fields
// (action/item_count/request_type) on top. Used per item inside
// CancellationRolloverAction. Falls back to a thin action/item_count/
// request_type map if the row can't be loaded (e.g. row deleted between the
// earlier status lookup and here) so the action still gets *a* policy
// decision instead of erroring out.
func forwardCancelRollItemPolicyFields(ctx context.Context, pool *pgxpool.Pool, requestType, bookingID, requestDate, action string, itemCount int) map[string]interface{} {
	var fields map[string]interface{}
	if requestType == cancelRollTypeRollover {
		if row, err := loadForwardRolloverRow(ctx, pool, bookingID, requestDate); err == nil {
			fields = buildForwardRolloverPolicyFields(row)
		}
	} else {
		if row, err := loadForwardCancellationRow(ctx, pool, bookingID, requestDate); err == nil {
			fields = buildForwardCancellationPolicyFields(row)
		}
	}
	if fields == nil {
		fields = map[string]interface{}{}
	}
	fields["action"] = action
	fields["item_count"] = itemCount
	fields["request_type"] = requestType
	return fields
}

// parseFloatPtr parses an optional numeric string (as carried in the
// NewForward sub-struct of RolloverForwardBooking's request, which arrives as
// JSON strings, not numbers) into a *float64 for the canonical row. Returns
// nil for an empty or unparsable value rather than erroring — Create already
// validates the fields it actually requires before this point.
func parseFloatPtr(s string) *float64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil
	}
	return &f
}
