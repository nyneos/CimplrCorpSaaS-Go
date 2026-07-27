package redemption

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// redemptionInitiationRow is the canonical business-field shape for
// MF_REDEMPTION — one field per real scalar column on
// investment.redemption_initiation, plus scheme_name/amc_name enrichment
// resolved from investment.masterscheme (already seeded as MF_REDEMPTION
// field_codes even though they aren't columns on this table — the domain
// catalog carried them ahead of any builder actually populating them).
// Every policy-check call site in this package builds its Fields{} map from
// a value of this type instead of hand-picking its own ad hoc subset.
// Before this file: Create/CreateBulk passed 6 of 13 real fields (no
// requested_by/requested_date/method/scheme_name/amc_name),
// Update/UpdateBulk passed a raw unvalidated {redemption_id, fields} blob,
// and Delete/Approve/Reject each passed only {redemption_id, comment}.
type redemptionInitiationRow struct {
	RedemptionID      string
	FolioID           string
	DematID           string
	SchemeID          string
	SchemeName        string
	AMCName           string
	RequestedBy       string
	RequestedDate     string
	TransactionDate   string
	ByAmount          *float64
	ByUnits           *float64
	Method            string
	EntityName        string
	EstimatedProceeds *float64
	GainLoss          *float64
}

// buildRedemptionInitiationPolicyFields maps the canonical row onto the
// exact field_code keys seeded in domain_catalog for MF_REDEMPTION (see
// cmd/seedDomainCatalog/mfRedemptionCanonical.go).
func buildRedemptionInitiationPolicyFields(row redemptionInitiationRow) map[string]interface{} {
	return map[string]interface{}{
		"redemption_id":      row.RedemptionID,
		"folio_id":           row.FolioID,
		"demat_id":           row.DematID,
		"scheme_id":          row.SchemeID,
		"scheme_name":        row.SchemeName,
		"amc_name":           row.AMCName,
		"requested_by":       row.RequestedBy,
		"requested_date":     row.RequestedDate,
		"transaction_date":   row.TransactionDate,
		"by_amount":          row.ByAmount,
		"by_units":           row.ByUnits,
		"method":             row.Method,
		"entity_name":        row.EntityName,
		"estimated_proceeds": row.EstimatedProceeds,
		"gain_loss":          row.GainLoss,
	}
}

// lookupRedemptionSchemeEnrichment resolves scheme_name/amc_name from
// investment.masterscheme for a given scheme_id — used both when building a
// prospective row at Create (no redemption_id exists yet) and when loading
// the full row by ID for Update/Approve/Reject/Delete. Mirrors the join used
// by fetchRedemptionInitiationRows (COALESCE(s.scheme_name, m.scheme_id)).
func lookupRedemptionSchemeEnrichment(ctx context.Context, pool *pgxpool.Pool, schemeID string) (schemeName, amcName string) {
	schemeName = schemeID
	if strings.TrimSpace(schemeID) == "" {
		return "", ""
	}
	_ = pool.QueryRow(ctx, `
		SELECT COALESCE(scheme_name, $1), COALESCE(amc_name, '')
		FROM investment.masterscheme
		WHERE COALESCE(is_deleted, false) = false
		  AND (scheme_id::text = $1 OR internal_scheme_code = $1 OR amfi_scheme_code = $1)
		LIMIT 1`, schemeID,
	).Scan(&schemeName, &amcName)
	return schemeName, amcName
}

// loadRedemptionInitiationRow fetches the full canonical row by
// redemption_id — used by Update/Approve/Reject/Delete, which only ever
// receive an ID in the request, never the business data itself.
func loadRedemptionInitiationRow(ctx context.Context, pool *pgxpool.Pool, redemptionID string) (redemptionInitiationRow, error) {
	var row redemptionInitiationRow
	row.RedemptionID = redemptionID
	err := pool.QueryRow(ctx, `
		SELECT COALESCE(m.folio_id,''), COALESCE(m.demat_id,''), COALESCE(m.scheme_id,''),
		       COALESCE(m.requested_by,''), COALESCE(TO_CHAR(m.requested_date,'YYYY-MM-DD'),''),
		       COALESCE(TO_CHAR(m.transaction_date,'YYYY-MM-DD'),''),
		       m.by_amount, m.by_units, COALESCE(m.method,'FIFO'), COALESCE(m.entity_name,''),
		       m.estimated_proceeds, m.gain_loss
		FROM investment.redemption_initiation m
		WHERE m.redemption_id = $1`, redemptionID,
	).Scan(&row.FolioID, &row.DematID, &row.SchemeID,
		&row.RequestedBy, &row.RequestedDate, &row.TransactionDate,
		&row.ByAmount, &row.ByUnits, &row.Method, &row.EntityName,
		&row.EstimatedProceeds, &row.GainLoss)
	if err != nil {
		return redemptionInitiationRow{}, err
	}
	row.SchemeName, row.AMCName = lookupRedemptionSchemeEnrichment(ctx, pool, row.SchemeID)
	return row, nil
}

// applyRedemptionInitiationEdits overlays a partial edit map (Update's
// req.Fields — arbitrary subset, whatever the user actually changed) onto an
// already-loaded canonical row, so the policy check sees the full row
// as-it-will-be-after-the-edit, not just the touched keys. Mirrors exactly
// the updatable column set in UpdateRedemption's fieldPairs map (folio_id,
// demat_id, scheme_id, requested_by, requested_date, transaction_date,
// by_amount, by_units, estimated_proceeds, gain_loss) — method, entity_name,
// scheme_name and amc_name are not user-editable via this action in the real
// UPDATE statement, so they are left untouched here too.
func applyRedemptionInitiationEdits(row redemptionInitiationRow, edits map[string]interface{}) redemptionInitiationRow {
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
		case "folio_id":
			if s, ok := str(v); ok {
				row.FolioID = s
			}
		case "demat_id":
			if s, ok := str(v); ok {
				row.DematID = s
			}
		case "scheme_id":
			if s, ok := str(v); ok {
				row.SchemeID = s
			}
		case "requested_by":
			if s, ok := str(v); ok {
				row.RequestedBy = s
			}
		case "requested_date":
			if s, ok := str(v); ok {
				row.RequestedDate = s
			}
		case "transaction_date":
			if s, ok := str(v); ok {
				row.TransactionDate = s
			}
		case "by_amount":
			if n, ok := num(v); ok {
				row.ByAmount = n
			}
		case "by_units":
			if n, ok := num(v); ok {
				row.ByUnits = n
			}
		case "estimated_proceeds":
			if n, ok := num(v); ok {
				row.EstimatedProceeds = n
			}
		case "gain_loss":
			if n, ok := num(v); ok {
				row.GainLoss = n
			}
		}
	}
	return row
}
