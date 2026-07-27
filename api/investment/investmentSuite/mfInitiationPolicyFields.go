package investmentsuite

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// mfInitiationRow is the canonical business-field shape for MF_INITIATION
// policy checks — one field per real scalar column on
// investment.investment_initiation (old_* audit-mirror columns and
// is_deleted/updated_at plumbing excluded). Every policy-check call site in
// this file builds its Fields{} map from a value of this type instead of
// hand-picking its own ad hoc subset. Before this file, CreateInitiationSingle/
// Bulk passed 7 business fields, UpdateInitiation/Bulk passed a raw
// unvalidated req.Fields edit blob (not the canonical set), and
// Delete/BulkApprove/BulkReject each passed only initiation_id plus a
// non-catalogued reason/comment string.
type mfInitiationRow struct {
	InitiationID    string
	ProposalID      string
	TransactionDate string
	EntityName      string
	SchemeID        string
	FolioID         string
	DematID         string
	Amount          float64
	Source          string
}

// buildMFInitiationPolicyFields maps the canonical row onto the exact
// field_code keys seeded in domain_catalog for MF_INITIATION.
func buildMFInitiationPolicyFields(row mfInitiationRow) map[string]interface{} {
	return map[string]interface{}{
		"initiation_id":    row.InitiationID,
		"proposal_id":      row.ProposalID,
		"transaction_date": row.TransactionDate,
		"entity_name":      row.EntityName,
		"scheme_id":        row.SchemeID,
		"folio_id":         row.FolioID,
		"demat_id":         row.DematID,
		"amount":           row.Amount,
		"source":           row.Source,
	}
}

// loadMFInitiationRow fetches the full canonical row by initiation_id — used
// by Update/Delete/Approve/Reject, which only ever receive an initiation_id
// in the request, never the business data itself.
func loadMFInitiationRow(ctx context.Context, pool *pgxpool.Pool, initiationID string) (mfInitiationRow, error) {
	var row mfInitiationRow
	row.InitiationID = initiationID
	err := pool.QueryRow(ctx, `
		SELECT COALESCE(proposal_id,''), COALESCE(transaction_date::text,''), entity_name,
		       scheme_id, COALESCE(folio_id,''), COALESCE(demat_id,''), amount, source
		FROM investment.investment_initiation
		WHERE initiation_id = $1 AND COALESCE(is_deleted,false) = false`, initiationID,
	).Scan(&row.ProposalID, &row.TransactionDate, &row.EntityName,
		&row.SchemeID, &row.FolioID, &row.DematID, &row.Amount, &row.Source)
	if err != nil {
		return mfInitiationRow{}, fmt.Errorf("load mf initiation for policy: %w", err)
	}
	return row, nil
}

// applyMFInitiationEdits overlays a partial edit map (UpdateInitiation/
// UpdateInitiationBulk's req.Fields — arbitrary subset, whatever the user
// actually changed) onto an already-loaded canonical row, so the policy
// check sees the full row as-it-will-be-after-the-edit, not just the touched
// keys. Key set mirrors the real UPDATE statement's fieldPairs allowlist in
// UpdateInitiation (the superset of the two update handlers — see report:
// UpdateInitiationBulk's fieldPairs omits demat_id, a pre-existing
// inconsistency between the two handlers, not introduced here).
func applyMFInitiationEdits(row mfInitiationRow, edits map[string]interface{}) mfInitiationRow {
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
	for k, v := range edits {
		switch k {
		case "proposal_id":
			if s, ok := str(v); ok {
				row.ProposalID = s
			}
		case "transaction_date":
			if s, ok := str(v); ok {
				row.TransactionDate = s
			}
		case "entity_name":
			if s, ok := str(v); ok {
				row.EntityName = s
			}
		case "scheme_id":
			if s, ok := str(v); ok {
				row.SchemeID = s
			}
		case "folio_id":
			if s, ok := str(v); ok {
				row.FolioID = s
			}
		case "demat_id":
			if s, ok := str(v); ok {
				row.DematID = s
			}
		case "amount":
			if n, ok := num(v); ok {
				row.Amount = n
			}
		case "source":
			if s, ok := str(v); ok {
				row.Source = s
			}
		}
	}
	return row
}
