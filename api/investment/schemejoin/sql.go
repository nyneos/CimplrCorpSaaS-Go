// Package schemejoin provides canonical SQL fragments for resolving investment.masterscheme.
// Source of truth: scheme_id, internal_scheme_code, amfi_scheme_code (active / not deleted only).
// Do not join masterscheme on isin or scheme_name — blank ISINs cause cartesian explosions.
package schemejoin

// LateralWhereInitiationRef matches i.scheme_id when it stores scheme_id, internal code, or AMFI code.
const LateralWhereInitiationRef = `
COALESCE(is_deleted, false) = false
AND (
  (NULLIF(TRIM(i.scheme_id), '') IS NOT NULL AND scheme_id::text = TRIM(i.scheme_id))
  OR (NULLIF(TRIM(i.scheme_id), '') IS NOT NULL AND internal_scheme_code = TRIM(i.scheme_id))
  OR (NULLIF(TRIM(i.scheme_id), '') IS NOT NULL AND amfi_scheme_code = TRIM(i.scheme_id))
)`

// LateralWhereOnboardTx matches onboard_transaction scheme references.
const LateralWhereOnboardTx = `
COALESCE(is_deleted, false) = false
AND (
  (NULLIF(TRIM(ot.scheme_id), '') IS NOT NULL AND scheme_id::text = TRIM(ot.scheme_id))
  OR (NULLIF(TRIM(ot.scheme_internal_code), '') IS NOT NULL AND internal_scheme_code = TRIM(ot.scheme_internal_code))
  OR (NULLIF(TRIM(ot.scheme_id), '') IS NOT NULL AND amfi_scheme_code = TRIM(ot.scheme_id))
)`

// LateralSelectCols are the columns returned by scheme LATERAL subqueries.
const LateralSelectCols = `scheme_id, scheme_name, amfi_scheme_code, amc_name, internal_scheme_code, isin`

// JoinOnboardTx joins masterscheme ms to approved/onboard transaction ot.
const JoinOnboardTx = `
COALESCE(ms.is_deleted, false) = false
AND (
  (NULLIF(TRIM(ot.scheme_id), '') IS NOT NULL AND ms.scheme_id::text = TRIM(ot.scheme_id))
  OR (NULLIF(TRIM(ot.scheme_internal_code), '') IS NOT NULL AND ms.internal_scheme_code = TRIM(ot.scheme_internal_code))
  OR (NULLIF(TRIM(ot.scheme_id), '') IS NOT NULL AND ms.amfi_scheme_code = TRIM(ot.scheme_id))
)`

// JoinInitiationRef joins masterscheme s to initiation i.scheme_id (investment/redemption).
const JoinInitiationRef = `
COALESCE(s.is_deleted, false) = false
AND (
  (NULLIF(TRIM(i.scheme_id), '') IS NOT NULL AND s.scheme_id::text = TRIM(i.scheme_id))
  OR (NULLIF(TRIM(i.scheme_id), '') IS NOT NULL AND s.internal_scheme_code = TRIM(i.scheme_id))
  OR (NULLIF(TRIM(i.scheme_id), '') IS NOT NULL AND s.amfi_scheme_code = TRIM(i.scheme_id))
)`

// JoinOnSchemeID joins when the source row already has a resolved scheme_id (e.g. portfolio_snapshot).
const JoinOnSchemeID = `
COALESCE(s.is_deleted, false) = false
AND NULLIF(TRIM(ps.scheme_id), '') IS NOT NULL
AND s.scheme_id::text = TRIM(ps.scheme_id)`

// JoinOnSchemeIDAlias joins ms to a scheme_id column; alias is the masterscheme table alias, col is e.g. "ts.scheme_id".
func JoinOnSchemeIDAlias(msAlias, schemeIDCol string) string {
	return `
COALESCE(` + msAlias + `.is_deleted, false) = false
AND NULLIF(TRIM(` + schemeIDCol + `), '') IS NOT NULL
AND ` + msAlias + `.scheme_id::text = TRIM(` + schemeIDCol + `)`
}

// JoinOnSchemeRef matches a generic scheme reference column (scheme_id, internal code, or AMFI code).
func JoinOnSchemeRef(msAlias, refCol string) string {
	return `
COALESCE(` + msAlias + `.is_deleted, false) = false
AND (
  (NULLIF(TRIM(` + refCol + `), '') IS NOT NULL AND ` + msAlias + `.scheme_id::text = TRIM(` + refCol + `))
  OR (NULLIF(TRIM(` + refCol + `), '') IS NOT NULL AND ` + msAlias + `.internal_scheme_code = TRIM(` + refCol + `))
  OR (NULLIF(TRIM(` + refCol + `), '') IS NOT NULL AND ` + msAlias + `.amfi_scheme_code = TRIM(` + refCol + `))
)`
}

// LatestNavByAMFICTE resolves current NAV from amfi_nav_staging by AMFI scheme code.
const LatestNavByAMFICTE = `
latest_nav AS (
  SELECT DISTINCT ON (scheme_code)
    scheme_code::text AS scheme_code,
    nav_value,
    nav_date
  FROM investment.amfi_nav_staging
  WHERE NULLIF(TRIM(scheme_code::text), '') IS NOT NULL
  ORDER BY scheme_code, nav_date DESC, file_date DESC NULLS LAST
)`

// NavLateralJoin attaches ln.nav_value / ln.nav_date from investment.amfi_nav_staging
// by AMFI scheme_code only (indexable). Name/ISIN OR-matching forced a seq scan of
// amfi_nav_staging per holding row and blew AUM dashboards to 60s+.
func NavLateralJoin(rowAlias, optionalAmfiCol string) string {
	amfiExpr := "NULLIF(TRIM(ms_nav.amfi_scheme_code), '')"
	if optionalAmfiCol != "" {
		amfiExpr = "NULLIF(TRIM(COALESCE(" + optionalAmfiCol + ", ms_nav.amfi_scheme_code)), '')"
	}
	return `
LEFT JOIN investment.masterscheme ms_nav ON (
  COALESCE(ms_nav.is_deleted, false) = false
  AND NULLIF(TRIM(` + rowAlias + `.scheme_id), '') IS NOT NULL
  AND ms_nav.scheme_id::text = TRIM(` + rowAlias + `.scheme_id)
)
LEFT JOIN LATERAL (
  SELECT ans.nav_value, ans.nav_date, ans.raw_category_header
  FROM investment.amfi_nav_staging ans
  WHERE ` + amfiExpr + ` IS NOT NULL
    AND ans.scheme_code::text = ` + amfiExpr + `
  ORDER BY ans.nav_date DESC, ans.file_date DESC NULLS LAST
  LIMIT 1
) ln ON true`
}

// JoinLatestNav joins latest_nav ln to masterscheme ms via amfi_scheme_code.
const JoinLatestNav = `ln.scheme_code = NULLIF(TRIM(ms.amfi_scheme_code), '')`

// ResolveAMFICodeExpr returns an AMFI scheme code using masterscheme first, then
// amfi_nav_staging matched by scheme name (case-insensitive exact).
func ResolveAMFICodeExpr(msAlias, schemeNameExpr string) string {
	return `COALESCE(
  NULLIF(TRIM(` + msAlias + `.amfi_scheme_code), ''),
  (
    SELECT ans.scheme_code::text
    FROM investment.amfi_nav_staging ans
    WHERE LOWER(TRIM(ans.scheme_name)) = LOWER(TRIM(` + schemeNameExpr + `))
    ORDER BY ans.nav_date DESC, ans.file_date DESC NULLS LAST
    LIMIT 1
  ),
  ''
)`
}
