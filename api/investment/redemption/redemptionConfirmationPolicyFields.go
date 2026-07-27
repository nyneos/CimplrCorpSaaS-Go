package redemption

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// redemptionConfirmationRow is the canonical business-field shape for
// MF_REDEMPTION_CONF — one field per real scalar column on
// investment.redemption_confirmation (old_* audit-pair columns, batch_id,
// is_deleted and updated_at excluded as pure plumbing). Every policy-check
// call site in this package builds its Fields{} map from a value of this
// type instead of hand-picking its own ad hoc subset. Before this file:
// Create/CreateBulk passed 5 of 17 real fields, Update/UpdateBulk passed a
// raw unvalidated {redemption_confirm_id, fields} blob, and
// Delete/Approve/Reject/ConfirmRedemption each passed only an ID (plus
// comment on Approve/Reject).
type redemptionConfirmationRow struct {
	RedemptionConfirmID          string
	RedemptionID                 string
	ActualNAV                    *float64
	ActualUnits                  *float64
	GrossProceeds                *float64
	ExitLoad                     *float64
	TDS                          *float64
	NetCredited                  *float64
	Status                       string
	ConfirmedBy                  string
	ConfirmedAt                  string
	STTCharges                   *float64
	ResolutionVariance           string
	ResolutionComment            string
	VarianceProceeds             *float64
	FinalRealisedCapitalGainLoss *float64
	UploadS3Key                  string
}

// buildRedemptionConfirmationPolicyFields maps the canonical row onto the
// exact field_code keys seeded in domain_catalog for MF_REDEMPTION_CONF (see
// cmd/seedDomainCatalog/mfRedemptionConfCanonical.go).
func buildRedemptionConfirmationPolicyFields(row redemptionConfirmationRow) map[string]interface{} {
	return map[string]interface{}{
		"redemption_confirm_id":            row.RedemptionConfirmID,
		"redemption_id":                    row.RedemptionID,
		"actual_nav":                       row.ActualNAV,
		"actual_units":                     row.ActualUnits,
		"gross_proceeds":                   row.GrossProceeds,
		"exit_load":                        row.ExitLoad,
		"tds":                              row.TDS,
		"net_credited":                     row.NetCredited,
		"status":                           row.Status,
		"confirmed_by":                     row.ConfirmedBy,
		"confirmed_at":                     row.ConfirmedAt,
		"stt_charges":                      row.STTCharges,
		"resolution_variance":              row.ResolutionVariance,
		"resolution_comment":               row.ResolutionComment,
		"variance_proceeds":                row.VarianceProceeds,
		"final_realised_capital_gain_loss": row.FinalRealisedCapitalGainLoss,
		"upload_s3_key":                    row.UploadS3Key,
	}
}

// loadRedemptionConfirmationRow fetches the full canonical row by
// redemption_confirm_id — used by Update/Approve/Reject/Delete/
// ConfirmRedemption, which only ever receive an ID in the request, never the
// business data itself.
func loadRedemptionConfirmationRow(ctx context.Context, pool *pgxpool.Pool, redemptionConfirmID string) (redemptionConfirmationRow, error) {
	var row redemptionConfirmationRow
	row.RedemptionConfirmID = redemptionConfirmID
	err := pool.QueryRow(ctx, `
		SELECT COALESCE(redemption_id,''), actual_nav, actual_units, gross_proceeds,
		       exit_load, tds, net_credited, COALESCE(status,''),
		       COALESCE(confirmed_by,''), COALESCE(TO_CHAR(confirmed_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"'),''),
		       stt_charges, COALESCE(resolution_variance,''), COALESCE(resolution_comment,''),
		       variance_proceeds, final_realised_capital_gain_loss, COALESCE(upload_s3_key,'')
		FROM investment.redemption_confirmation
		WHERE redemption_confirm_id = $1`, redemptionConfirmID,
	).Scan(&row.RedemptionID, &row.ActualNAV, &row.ActualUnits, &row.GrossProceeds,
		&row.ExitLoad, &row.TDS, &row.NetCredited, &row.Status,
		&row.ConfirmedBy, &row.ConfirmedAt,
		&row.STTCharges, &row.ResolutionVariance, &row.ResolutionComment,
		&row.VarianceProceeds, &row.FinalRealisedCapitalGainLoss, &row.UploadS3Key)
	if err != nil {
		return redemptionConfirmationRow{}, err
	}
	return row, nil
}

// applyRedemptionConfirmationEdits overlays a partial edit map (Update's
// req.Fields — arbitrary subset, whatever the user actually changed) onto an
// already-loaded canonical row, so the policy check sees the full row
// as-it-will-be-after-the-edit, not just the touched keys. Mirrors exactly
// the updatable column set in UpdateRedemptionConfirmation's fieldPairs map
// — confirmed_by, confirmed_at and upload_s3_key are not user-editable via
// this action in the real UPDATE statement, so they are left untouched here
// too.
func applyRedemptionConfirmationEdits(row redemptionConfirmationRow, edits map[string]interface{}) redemptionConfirmationRow {
	str := func(v interface{}) (string, bool) {
		s, ok := v.(string)
		return s, ok
	}
	num := func(v interface{}) (*float64, bool) {
		switch n := v.(type) {
		case float64:
			return &n, true
		case int:
			f := float64(n)
			return &f, true
		}
		return nil, false
	}
	for k, v := range edits {
		switch strings.ToLower(k) {
		case "redemption_id":
			if s, ok := str(v); ok {
				row.RedemptionID = s
			}
		case "actual_nav":
			if n, ok := num(v); ok {
				row.ActualNAV = n
			}
		case "actual_units":
			if n, ok := num(v); ok {
				row.ActualUnits = n
			}
		case "gross_proceeds":
			if n, ok := num(v); ok {
				row.GrossProceeds = n
			}
		case "exit_load":
			if n, ok := num(v); ok {
				row.ExitLoad = n
			}
		case "tds":
			if n, ok := num(v); ok {
				row.TDS = n
			}
		case "net_credited":
			if n, ok := num(v); ok {
				row.NetCredited = n
			}
		case "status":
			if s, ok := str(v); ok {
				row.Status = s
			}
		case "stt_charges":
			if n, ok := num(v); ok {
				row.STTCharges = n
			}
		case "resolution_variance":
			if s, ok := str(v); ok {
				row.ResolutionVariance = s
			}
		case "resolution_comment":
			if s, ok := str(v); ok {
				row.ResolutionComment = s
			}
		case "variance_proceeds":
			if n, ok := num(v); ok {
				row.VarianceProceeds = n
			}
		case "final_realised_capital_gain_loss":
			if n, ok := num(v); ok {
				row.FinalRealisedCapitalGainLoss = n
			}
		}
	}
	return row
}
