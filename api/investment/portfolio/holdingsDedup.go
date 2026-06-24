package portfolio

import (
	"CimplrCorpSaas/api/investment/schemejoin"
	"math"
	"strings"
)

// ExcludeOnboardRedemptionDuplicatingSuite hides onboard sells when the same redemption
// was recorded in Redemption Suite (display queries only; holdings math keeps onboard).
const ExcludeOnboardRedemptionDuplicatingSuite = `
      AND NOT (
          LOWER(TRIM(COALESCE(ot.transaction_type, ''))) IN ('sell', 'redemption', 'redeem', 'switch_out')
          AND EXISTS (
              SELECT 1
              FROM investment.redemption_confirmation c
              JOIN investment.redemption_initiation i ON i.redemption_id = c.redemption_id
              LEFT JOIN investment.masterfolio mf_r ON (mf_r.folio_id::text = i.folio_id OR mf_r.folio_number = i.folio_id)
              LEFT JOIN investment.masterscheme s ON (` + schemejoin.JoinInitiationRef + `)
              WHERE COALESCE(c.is_deleted, false) = false
                AND LOWER(TRIM(COALESCE(i.entity_name, ''))) = LOWER(TRIM(COALESCE(ot.entity_name, mf.entity_name, md.entity_name, '')))
                AND (
                  (NULLIF(TRIM(COALESCE(ot.scheme_id, '')), '') IS NOT NULL
                   AND NULLIF(TRIM(COALESCE(i.scheme_id, '')), '') IS NOT NULL
                   AND TRIM(ot.scheme_id) = TRIM(i.scheme_id))
                  OR (NULLIF(TRIM(COALESCE(ot.scheme_internal_code, '')), '') IS NOT NULL
                      AND TRIM(ot.scheme_internal_code) = TRIM(COALESCE(s.internal_scheme_code, i.scheme_id, '')))
                  OR (NULLIF(TRIM(COALESCE(ms.scheme_id::text, '')), '') IS NOT NULL
                      AND NULLIF(TRIM(COALESCE(s.scheme_id::text, '')), '') IS NOT NULL
                      AND ms.scheme_id = s.scheme_id)
                )
                AND (
                  COALESCE(NULLIF(TRIM(mf_r.folio_number), ''), NULLIF(TRIM(i.folio_id), ''), '') = ''
                  OR COALESCE(NULLIF(TRIM(mf.folio_number), ''), NULLIF(TRIM(ot.folio_number), ''), '')
                     = COALESCE(NULLIF(TRIM(mf_r.folio_number), ''), NULLIF(TRIM(i.folio_id), ''), '')
                )
                AND GREATEST(ABS(COALESCE(ot.amount, 0)), ABS(COALESCE(c.gross_proceeds, 0))) > 0
                AND ABS(COALESCE(ot.amount, 0) - COALESCE(c.gross_proceeds, 0))
                    / GREATEST(ABS(COALESCE(ot.amount, 0)), ABS(COALESCE(c.gross_proceeds, 0))) < 0.02
          )
      )`

// ExcludeOnboardPurchaseDuplicatingInvestmentSuite hides onboard buys when the same
// purchase exists in Investment Suite (display queries only).
const ExcludeOnboardPurchaseDuplicatingInvestmentSuite = `
      AND NOT (
          LOWER(TRIM(COALESCE(ot.transaction_type, ''))) IN ('purchase', 'buy', 'subscription', 'bonus')
          AND EXISTS (
              SELECT 1
              FROM investment.investment_confirmation c
              JOIN investment.investment_initiation i ON i.initiation_id = c.initiation_id
              LEFT JOIN investment.masterfolio mf_i ON (mf_i.folio_id::text = i.folio_id OR mf_i.folio_number = i.folio_id)
              LEFT JOIN investment.masterscheme s ON (` + schemejoin.JoinInitiationRef + `)
              WHERE COALESCE(c.is_deleted, false) = false
                AND c.status = 'CONFIRMED'
                AND LOWER(TRIM(COALESCE(i.entity_name, ''))) = LOWER(TRIM(COALESCE(ot.entity_name, mf.entity_name, md.entity_name, '')))
                AND (
                  (NULLIF(TRIM(COALESCE(ot.scheme_id, '')), '') IS NOT NULL
                   AND NULLIF(TRIM(COALESCE(i.scheme_id, '')), '') IS NOT NULL
                   AND TRIM(ot.scheme_id) = TRIM(i.scheme_id))
                  OR (NULLIF(TRIM(COALESCE(ot.scheme_internal_code, '')), '') IS NOT NULL
                      AND TRIM(ot.scheme_internal_code) = TRIM(COALESCE(s.internal_scheme_code, i.scheme_id, '')))
                  OR (NULLIF(TRIM(COALESCE(ms.scheme_id::text, '')), '') IS NOT NULL
                      AND NULLIF(TRIM(COALESCE(s.scheme_id::text, '')), '') IS NOT NULL
                      AND ms.scheme_id = s.scheme_id)
                  OR COALESCE(NULLIF(TRIM(ms.amfi_scheme_code), ''), NULLIF(TRIM(ot.scheme_id), ''), '')
                     = COALESCE(NULLIF(TRIM(s.amfi_scheme_code), ''), NULLIF(TRIM(i.scheme_id), ''), '')
                )
                AND (
                  COALESCE(NULLIF(TRIM(mf_i.folio_number), ''), NULLIF(TRIM(i.folio_id), ''), '') = ''
                  OR COALESCE(NULLIF(TRIM(mf.folio_number), ''), NULLIF(TRIM(ot.folio_number), ''), '')
                     = COALESCE(NULLIF(TRIM(mf_i.folio_number), ''), NULLIF(TRIM(i.folio_id), ''), '')
                )
                AND GREATEST(ABS(COALESCE(ot.amount, 0)), ABS(COALESCE(c.net_amount, 0)), ABS(COALESCE(i.amount, 0))) > 0
                AND (
                  ABS(COALESCE(ot.amount, 0) - COALESCE(c.net_amount, 0))
                      / GREATEST(ABS(COALESCE(ot.amount, 0)), ABS(COALESCE(c.net_amount, 0))) < 0.02
                  OR ABS(COALESCE(ot.amount, 0) - COALESCE(i.amount, 0))
                      / GREATEST(ABS(COALESCE(ot.amount, 0)), ABS(COALESCE(i.amount, 0))) < 0.02
                )
          )
      )`

// ExcludeRedemptionConfirmationDuplicatingOnboard drops suite redemptions already present in onboard.
const ExcludeRedemptionConfirmationDuplicatingOnboard = `
      AND NOT EXISTS (
          SELECT 1
          FROM investment.approved_onboard_transaction ot
          LEFT JOIN investment.masterfolio mf_ot ON mf_ot.folio_id = ot.folio_id
          LEFT JOIN investment.masterscheme ms ON (` + schemejoin.JoinOnboardTx + `)
          WHERE LOWER(TRIM(COALESCE(ot.entity_name, ''))) = LOWER(TRIM(COALESCE(i.entity_name, '')))
            AND LOWER(TRIM(COALESCE(ot.transaction_type, ''))) IN ('sell', 'redemption', 'redeem', 'switch_out')
            AND (
              (NULLIF(TRIM(COALESCE(ot.scheme_id, '')), '') IS NOT NULL
               AND NULLIF(TRIM(COALESCE(i.scheme_id, '')), '') IS NOT NULL
               AND TRIM(ot.scheme_id) = TRIM(i.scheme_id))
              OR (NULLIF(TRIM(COALESCE(ot.scheme_internal_code, '')), '') IS NOT NULL
                  AND TRIM(ot.scheme_internal_code) = TRIM(COALESCE(s.internal_scheme_code, i.scheme_id, '')))
              OR (NULLIF(TRIM(COALESCE(ms.scheme_id::text, '')), '') IS NOT NULL
                  AND NULLIF(TRIM(COALESCE(s.scheme_id::text, '')), '') IS NOT NULL
                  AND ms.scheme_id = s.scheme_id)
            )
            AND (
              COALESCE(NULLIF(TRIM(mf.folio_number), ''), NULLIF(TRIM(i.folio_id), ''), '') = ''
              OR COALESCE(NULLIF(TRIM(mf_ot.folio_number), ''), NULLIF(TRIM(ot.folio_number), ''), '')
                 = COALESCE(NULLIF(TRIM(mf.folio_number), ''), NULLIF(TRIM(i.folio_id), ''), '')
            )
            AND GREATEST(ABS(COALESCE(ot.amount, 0)), ABS(COALESCE(c.gross_proceeds, 0))) > 0
            AND ABS(COALESCE(ot.amount, 0) - COALESCE(c.gross_proceeds, 0))
                / GREATEST(ABS(COALESCE(ot.amount, 0)), ABS(COALESCE(c.gross_proceeds, 0))) < 0.02
      )`

// ExcludeSupersededOnboardPurchase drops older onboard/workbench purchases superseded by a newer edit.
var ExcludeSupersededOnboardPurchase = `
      AND NOT EXISTS (
          SELECT 1
          FROM investment.approved_onboard_transaction newer
          LEFT JOIN investment.masterfolio mf_new ON mf_new.folio_id = newer.folio_id
          LEFT JOIN investment.masterdemataccount md_new ON md_new.demat_id = newer.demat_id
          LEFT JOIN investment.masterscheme ms_new ON (
            COALESCE(ms_new.is_deleted, false) = false
            AND (
              (NULLIF(TRIM(newer.scheme_id), '') IS NOT NULL AND ms_new.scheme_id::text = TRIM(newer.scheme_id))
              OR (NULLIF(TRIM(newer.scheme_internal_code), '') IS NOT NULL AND ms_new.internal_scheme_code = TRIM(newer.scheme_internal_code))
              OR (NULLIF(TRIM(newer.scheme_id), '') IS NOT NULL AND ms_new.amfi_scheme_code = TRIM(newer.scheme_id))
            )
          )
          WHERE newer.id <> ot.id
            AND LOWER(TRIM(COALESCE(newer.entity_name, ''))) = LOWER(TRIM(COALESCE(ot.entity_name, '')))
            AND LOWER(TRIM(COALESCE(newer.transaction_type, ''))) IN ('purchase', 'buy', 'subscription', 'bonus')
            AND LOWER(TRIM(COALESCE(ot.transaction_type, ''))) IN ('purchase', 'buy', 'subscription', 'bonus')
            AND COALESCE(NULLIF(TRIM(newer.folio_number), ''), NULLIF(TRIM(mf_new.folio_number), ''), NULLIF(TRIM(newer.folio_id::text), ''))
                = COALESCE(NULLIF(TRIM(ot.folio_number), ''), NULLIF(TRIM(mf.folio_number), ''), NULLIF(TRIM(ot.folio_id::text), ''))
            AND COALESCE(NULLIF(TRIM(newer.demat_acc_number), ''), NULLIF(TRIM(md_new.demat_account_number), ''), NULLIF(TRIM(newer.demat_id::text), ''))
                = COALESCE(NULLIF(TRIM(ot.demat_acc_number), ''), NULLIF(TRIM(md.demat_account_number), ''), NULLIF(TRIM(ot.demat_id::text), ''))
            AND COALESCE(NULLIF(TRIM(ms_new.amfi_scheme_code), ''), NULLIF(TRIM(newer.scheme_id), ''), '')
                = COALESCE(NULLIF(TRIM(ms.amfi_scheme_code), ''), NULLIF(TRIM(ot.scheme_id), ''), '')
            AND COALESCE(newer.transaction_date, ot.transaction_date) > COALESCE(ot.transaction_date, newer.transaction_date)
            AND GREATEST(ABS(COALESCE(newer.amount, 0)), ABS(COALESCE(ot.amount, 0))) > 0
            AND ABS(COALESCE(newer.amount, 0) - COALESCE(ot.amount, 0))
                / GREATEST(ABS(COALESCE(newer.amount, 0)), ABS(COALESCE(ot.amount, 0))) < 0.02
      )`

type holdingsPositionKey struct {
	entity string
	folio  string
	demat  string
	scheme string
}

func holdingsPositionKeyFromRow(row PortfolioHoldingsRow) holdingsPositionKey {
	scheme := canonicalSchemeKey(row.AmfiSchemeCode, row.SchemeID, row.ISIN, row.SchemeName)
	return holdingsPositionKey{
		entity: strings.ToLower(strings.TrimSpace(row.EntityName)),
		folio:  strings.TrimSpace(row.FolioNumber),
		demat:  strings.TrimSpace(row.DematAccountNumber),
		scheme: scheme,
	}
}

func amountsSimilar(a, b float64) bool {
	g := math.Max(math.Abs(a), math.Abs(b))
	if g == 0 {
		return true
	}
	return math.Abs(a-b)/g < 0.02
}

// dedupeHoldingsRows collapses duplicate position rows that can still leak through snapshot joins.
func dedupeHoldingsRows(rows []PortfolioHoldingsRow) []PortfolioHoldingsRow {
	if len(rows) < 2 {
		return rows
	}
	grouped := make(map[holdingsPositionKey][]PortfolioHoldingsRow)
	order := make([]holdingsPositionKey, 0, len(rows))
	passThrough := make([]PortfolioHoldingsRow, 0)

	for _, row := range rows {
		key := holdingsPositionKeyFromRow(row)
		if key.entity == "" || key.scheme == "" {
			passThrough = append(passThrough, row)
			continue
		}
		if _, ok := grouped[key]; !ok {
			order = append(order, key)
		}
		grouped[key] = append(grouped[key], row)
	}

	out := make([]PortfolioHoldingsRow, 0, len(passThrough)+len(order))
	out = append(out, passThrough...)
	for _, key := range order {
		grp := grouped[key]
		if len(grp) == 1 {
			out = append(out, grp[0])
			continue
		}
		best := grp[0]
		for _, candidate := range grp[1:] {
			if !amountsSimilar(candidate.CurrentValue, best.CurrentValue) {
				if candidate.CurrentValue > best.CurrentValue {
					best = candidate
				}
				continue
			}
			if candidate.UpdatedAt > best.UpdatedAt {
				best = candidate
				continue
			}
			if candidate.UpdatedAt == best.UpdatedAt && candidate.AvgNav > best.AvgNav {
				best = candidate
			}
		}
		out = append(out, best)
	}
	return out
}
