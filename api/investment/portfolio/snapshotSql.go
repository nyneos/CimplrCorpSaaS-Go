package portfolio

import "CimplrCorpSaas/api/investment/schemejoin"

// PortfolioSnapshotInsertSQL rebuilds portfolio_snapshot from approved onboarding,
// confirmed investment purchases, and confirmed redemption sells.
// $1 = optional entity_name[] filter (NULL refreshes all scoped entities).
// $2 = batch_id.
var PortfolioSnapshotInsertSQL = `
WITH ` + portfolioSchemeResolvedCTE + `
INSERT INTO investment.portfolio_snapshot (
  batch_id, entity_name, folio_number, demat_acc_number, folio_id, demat_id,
  scheme_id, scheme_name, isin, total_units, avg_nav, current_nav, current_value,
  total_invested_amount, gain_loss, gain_losss_percent, created_at
)
SELECT
  $2 AS batch_id,
  ts.entity_name,
  ts.folio_number,
  ts.demat_acc_number,
  ts.folio_id,
  ts.demat_id,
  ts.scheme_id,
  ts.scheme_name,
  ts.isin,
  ts.total_units,
  ts.avg_nav,
  COALESCE(ln.nav_value, 0) AS current_nav,
  ts.total_units * COALESCE(ln.nav_value, 0) AS current_value,
  ts.total_invested_amount,
  (ts.total_units * COALESCE(ln.nav_value, 0)) - ts.total_invested_amount AS gain_loss,
  CASE WHEN ts.total_invested_amount = 0 THEN 0 ELSE (((ts.total_units * COALESCE(ln.nav_value, 0)) - ts.total_invested_amount) / ts.total_invested_amount) * 100 END AS gain_losss_percent,
  NOW()
FROM transaction_summary ts
` + schemejoin.NavLateralJoin("ts", "ts.amfi_scheme_code") + `
WHERE ts.total_units > 0;
`
