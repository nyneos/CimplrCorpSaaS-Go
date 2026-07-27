package investmentsuite

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// mfConfirmationRow is the canonical business-field shape for MF_CONFIRMATION
// policy checks — one field per real scalar column on
// investment.investment_confirmation (old_* audit-mirror columns and
// is_deleted/updated_at plumbing excluded; confirmed_by/confirmed_at treated
// as audit plumbing, same as created_by/updated_at elsewhere, and excluded
// too — see report). Every policy-check call site in this file builds its
// Fields{} map from a value of this type instead of hand-picking its own ad
// hoc subset. Before this file, Create passed 5 business fields,
// Update/UpdateBulk passed a raw unvalidated req.Fields edit blob, and
// Delete/BulkApprove/BulkReject/ConfirmInvestment each passed only
// confirmation_id plus (for some) a non-catalogued comment/reason string.
type mfConfirmationRow struct {
	ConfirmationID      string
	InitiationID        string
	NAVDate             string
	NAV                 float64
	AllottedUnits       float64
	StampDuty           float64
	NetAmount           float64
	ActualNAV           *float64
	ActualAllottedUnits *float64
	VarianceNAV         *float64
	VarianceUnits       *float64
	Status              string
	DematID             string
	ResolutionComment   string
	ResolutionVariance  string
	UploadS3Key         string
}

// buildMFConfirmationPolicyFields maps the canonical row onto the exact
// field_code keys seeded in domain_catalog for MF_CONFIRMATION (demat_id,
// resolution_comment, resolution_variance and upload_s3_key are real columns
// that were not catalogued under any field_code at all before this pass —
// see cmd/seedDomainCatalog/mfConfirmationCanonical.go).
func buildMFConfirmationPolicyFields(row mfConfirmationRow) map[string]interface{} {
	return map[string]interface{}{
		"confirmation_id":       row.ConfirmationID,
		"initiation_id":         row.InitiationID,
		"nav_date":              row.NAVDate,
		"nav":                   row.NAV,
		"allotted_units":        row.AllottedUnits,
		"stamp_duty":            row.StampDuty,
		"net_amount":            row.NetAmount,
		"actual_nav":            row.ActualNAV,
		"actual_allotted_units": row.ActualAllottedUnits,
		"variance_nav":          row.VarianceNAV,
		"variance_units":        row.VarianceUnits,
		"status":                row.Status,
		"demat_id":              row.DematID,
		"resolution_comment":    row.ResolutionComment,
		"resolution_variance":   row.ResolutionVariance,
		"upload_s3_key":         row.UploadS3Key,
	}
}

// loadMFConfirmationRow fetches the full canonical row by confirmation_id —
// used by Update/Delete/Approve/Reject/ConfirmInvestment, which only ever
// receive a confirmation_id in the request, never the business data itself.
func loadMFConfirmationRow(ctx context.Context, pool *pgxpool.Pool, confirmationID string) (mfConfirmationRow, error) {
	var row mfConfirmationRow
	row.ConfirmationID = confirmationID
	err := pool.QueryRow(ctx, `
		SELECT initiation_id, COALESCE(nav_date::text,''), nav, allotted_units,
		       COALESCE(stamp_duty,0), net_amount, actual_nav, actual_allotted_units,
		       variance_nav, variance_units, status, COALESCE(demat_id,''),
		       COALESCE(resolution_comment,''), COALESCE(resolution_variance,''), COALESCE(upload_s3_key,'')
		FROM investment.investment_confirmation
		WHERE confirmation_id = $1 AND COALESCE(is_deleted,false) = false`, confirmationID,
	).Scan(&row.InitiationID, &row.NAVDate, &row.NAV, &row.AllottedUnits,
		&row.StampDuty, &row.NetAmount, &row.ActualNAV, &row.ActualAllottedUnits,
		&row.VarianceNAV, &row.VarianceUnits, &row.Status, &row.DematID,
		&row.ResolutionComment, &row.ResolutionVariance, &row.UploadS3Key)
	if err != nil {
		return mfConfirmationRow{}, fmt.Errorf("load mf confirmation for policy: %w", err)
	}
	return row, nil
}

// applyMFConfirmationEdits overlays a partial edit map (UpdateConfirmation/
// UpdateConfirmationBulk's req.Fields — arbitrary subset, whatever the user
// actually changed) onto an already-loaded canonical row, so the policy
// check sees the full row as-it-will-be-after-the-edit, not just the touched
// keys. Key set mirrors the real UPDATE statement's fieldPairs allowlist in
// UpdateConfirmation exactly (demat_id/upload_s3_key are real columns but
// are NOT in that allowlist today, so they are deliberately not
// edit-applicable here either — matching what the real UPDATE actually
// touches, not inventing new editability).
func applyMFConfirmationEdits(row mfConfirmationRow, edits map[string]interface{}) mfConfirmationRow {
	str := func(v interface{}) (string, bool) {
		s, ok := v.(string)
		return s, ok
	}
	num := func(v interface{}) (float64, bool) {
		switch n := v.(type) {
		case float64:
			return n, true
		case int:
			return float64(n), true
		}
		return 0, false
	}
	numPtr := func(v interface{}) (*float64, bool) {
		if n, ok := num(v); ok {
			return &n, true
		}
		return nil, false
	}
	for k, v := range edits {
		switch k {
		case "initiation_id":
			if s, ok := str(v); ok {
				row.InitiationID = s
			}
		case "nav_date":
			if s, ok := str(v); ok {
				row.NAVDate = s
			}
		case "nav":
			if n, ok := num(v); ok {
				row.NAV = n
			}
		case "allotted_units":
			if n, ok := num(v); ok {
				row.AllottedUnits = n
			}
		case "stamp_duty":
			if n, ok := num(v); ok {
				row.StampDuty = n
			}
		case "net_amount":
			if n, ok := num(v); ok {
				row.NetAmount = n
			}
		case "actual_nav":
			if n, ok := numPtr(v); ok {
				row.ActualNAV = n
			}
		case "actual_allotted_units":
			if n, ok := numPtr(v); ok {
				row.ActualAllottedUnits = n
			}
		case "variance_nav":
			if n, ok := numPtr(v); ok {
				row.VarianceNAV = n
			}
		case "variance_units":
			if n, ok := numPtr(v); ok {
				row.VarianceUnits = n
			}
		case "status":
			if s, ok := str(v); ok {
				row.Status = s
			}
		case "resolution_comment":
			if s, ok := str(v); ok {
				row.ResolutionComment = s
			}
		case "resolution_variance":
			if s, ok := str(v); ok {
				row.ResolutionVariance = s
			}
		}
	}
	return row
}
