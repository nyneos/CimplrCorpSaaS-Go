package portfolio

import (
	"CimplrCorpSaas/api/investment/schemejoin"
)

// positionFolioEqual matches normalized folio numbers between two scheme_resolved aliases.
const positionFolioEqual = `
              COALESCE(NULLIF(TRIM(newer.folio_number), ''), '') = COALESCE(NULLIF(TRIM(sr.folio_number), ''), '')`

// positionDematEqual matches normalized demat accounts between two scheme_resolved aliases.
const positionDematEqual = `
              COALESCE(NULLIF(TRIM(newer.demat_acc_number), ''), '') = COALESCE(NULLIF(TRIM(sr.demat_acc_number), ''), '')`

// positionSchemeEqual matches canonical scheme keys between two scheme_resolved aliases.
const positionSchemeEqual = `
              COALESCE(NULLIF(TRIM(newer.amfi_scheme_code), ''), NULLIF(TRIM(newer.scheme_id), ''), '')
                  = COALESCE(NULLIF(TRIM(sr.amfi_scheme_code), ''), NULLIF(TRIM(sr.scheme_id), ''), '')`

// ExcludeInvestmentConfirmationDuplicatingOnboard drops suite confirmations when the
// same position already exists in onboard/workbench with a similar amount (edit/correction).
const ExcludeInvestmentConfirmationDuplicatingOnboard = `
      AND NOT EXISTS (
          SELECT 1
          FROM investment.approved_onboard_transaction ot
          LEFT JOIN investment.masterfolio mf_ot ON mf_ot.folio_id = ot.folio_id
          LEFT JOIN investment.masterscheme ms ON (` + schemejoin.JoinOnboardTx + `)
          WHERE LOWER(TRIM(COALESCE(ot.entity_name, ''))) = LOWER(TRIM(COALESCE(i.entity_name, '')))
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
              COALESCE(NULLIF(TRIM(mf.folio_number), ''), NULLIF(TRIM(i.folio_id), ''), '') = ''
              OR COALESCE(NULLIF(TRIM(mf_ot.folio_number), ''), NULLIF(TRIM(ot.folio_number), ''), '')
                 = COALESCE(NULLIF(TRIM(mf.folio_number), ''), NULLIF(TRIM(i.folio_id), ''), '')
            )
            AND GREATEST(ABS(COALESCE(ot.amount, 0)), ABS(COALESCE(c.net_amount, 0))) > 0
            AND ABS(COALESCE(ot.amount, 0) - COALESCE(c.net_amount, 0))
                / GREATEST(ABS(COALESCE(ot.amount, 0)), ABS(COALESCE(c.net_amount, 0))) < 0.02
      )`

// portfolioSchemeResolvedCTE is the shared transaction union used for live holdings
// and portfolio snapshot rebuilds. $1 = optional entity_name[] filter (NULL = all).
var portfolioSchemeResolvedCTE = `
scheme_resolved_raw AS (
    SELECT
        ot.transaction_date,
        ot.transaction_type,
        ot.amount,
        ot.units,
        ot.nav,
        TRIM(COALESCE(mf.entity_name, md.entity_name, ot.entity_name, '')) AS entity_name,
        COALESCE(NULLIF(TRIM(ot.folio_number), ''), NULLIF(TRIM(mf.folio_number), ''), NULLIF(TRIM(ot.folio_id::text), '')) AS folio_number,
        COALESCE(NULLIF(TRIM(ot.demat_acc_number), ''), NULLIF(TRIM(md.demat_account_number), ''), NULLIF(TRIM(ot.demat_id::text), '')) AS demat_acc_number,
        ot.folio_id,
        ot.demat_id,
        COALESCE(ms.scheme_id::text, ot.scheme_id) AS scheme_id,
        COALESCE(ms.scheme_name, '') AS scheme_name,
        COALESCE(ms.isin, '') AS isin,
        COALESCE(ms.amfi_scheme_code, '') AS amfi_scheme_code
    FROM investment.approved_onboard_transaction ot
    LEFT JOIN investment.masterfolio mf ON mf.folio_id = ot.folio_id
    LEFT JOIN investment.masterdemataccount md ON md.demat_id = ot.demat_id
    LEFT JOIN investment.masterscheme ms ON (` + schemejoin.JoinOnboardTx + `)
    WHERE ($1::text[] IS NULL OR TRIM(COALESCE(mf.entity_name, md.entity_name, ot.entity_name, '')) = ANY($1::text[]))` + ExcludeSupersededOnboardPurchase + `

    UNION ALL

    SELECT
        i.transaction_date,
        'Purchase' AS transaction_type,
        c.net_amount AS amount,
        c.allotted_units AS units,
        c.nav,
        TRIM(i.entity_name) AS entity_name,
        COALESCE(NULLIF(TRIM(mf.folio_number), ''), NULLIF(TRIM(i.folio_id), ''), '') AS folio_number,
        COALESCE(NULLIF(TRIM(md.demat_account_number), ''), NULLIF(TRIM(i.demat_id), ''), '') AS demat_acc_number,
        i.folio_id,
        i.demat_id,
        COALESCE(s.scheme_id::text, i.scheme_id) AS scheme_id,
        COALESCE(s.scheme_name, '') AS scheme_name,
        COALESCE(s.isin, '') AS isin,
        COALESCE(s.amfi_scheme_code, '') AS amfi_scheme_code
    FROM investment.investment_confirmation c
    JOIN investment.investment_initiation i ON i.initiation_id = c.initiation_id
    LEFT JOIN investment.masterfolio mf ON (mf.folio_id::text = i.folio_id OR mf.folio_number = i.folio_id)
    LEFT JOIN investment.masterdemataccount md ON (md.demat_id::text = i.demat_id OR md.demat_account_number = i.demat_id)
    LEFT JOIN investment.masterscheme s ON (` + schemejoin.JoinInitiationRef + `)
    WHERE c.status = 'CONFIRMED' AND COALESCE(c.is_deleted, false) = false
      AND ($1::text[] IS NULL OR TRIM(i.entity_name) = ANY($1::text[]))` + ExcludeInvestmentConfirmationDuplicatingOnboard + `

    UNION ALL

    SELECT
        i.requested_date AS transaction_date,
        'Redemption' AS transaction_type,
        c.gross_proceeds AS amount,
        c.actual_units AS units,
        c.actual_nav AS nav,
        TRIM(i.entity_name) AS entity_name,
        COALESCE(NULLIF(TRIM(mf.folio_number), ''), NULLIF(TRIM(i.folio_id), ''), '') AS folio_number,
        COALESCE(NULLIF(TRIM(md.demat_account_number), ''), NULLIF(TRIM(i.demat_id), ''), '') AS demat_acc_number,
        i.folio_id,
        i.demat_id,
        COALESCE(s.scheme_id::text, i.scheme_id) AS scheme_id,
        COALESCE(s.scheme_name, '') AS scheme_name,
        COALESCE(s.isin, '') AS isin,
        COALESCE(s.amfi_scheme_code, '') AS amfi_scheme_code
    FROM investment.redemption_confirmation c
    JOIN investment.redemption_initiation i ON i.redemption_id = c.redemption_id
    LEFT JOIN investment.masterfolio mf ON (mf.folio_id::text = i.folio_id OR mf.folio_number = i.folio_id)
    LEFT JOIN investment.masterdemataccount md ON (md.demat_id::text = i.demat_id OR md.demat_account_number = i.demat_id)
    LEFT JOIN investment.masterscheme s ON (` + schemejoin.JoinInitiationRef + `)
    WHERE COALESCE(c.is_deleted, false) = false
      AND ($1::text[] IS NULL OR TRIM(i.entity_name) = ANY($1::text[]))` + ExcludeRedemptionConfirmationDuplicatingOnboard + `
),
scheme_resolved AS (
    SELECT sr.*
    FROM scheme_resolved_raw sr
    WHERE NOT (
        LOWER(TRIM(sr.transaction_type)) IN ('purchase','buy','subscription','bonus')
        AND EXISTS (
            SELECT 1
            FROM scheme_resolved_raw newer
            WHERE LOWER(TRIM(newer.entity_name)) = LOWER(TRIM(sr.entity_name))
              AND ` + positionFolioEqual + `
              AND ` + positionDematEqual + `
              AND ` + positionSchemeEqual + `
              AND LOWER(TRIM(newer.transaction_type)) IN ('purchase','buy','subscription','bonus')
              AND newer.transaction_date > sr.transaction_date
              AND GREATEST(ABS(COALESCE(newer.amount, 0)), ABS(COALESCE(sr.amount, 0))) > 0
              AND ABS(COALESCE(newer.amount, 0) - COALESCE(sr.amount, 0))
                  / GREATEST(ABS(COALESCE(newer.amount, 0)), ABS(COALESCE(sr.amount, 0))) < 0.02
        )
    )
),
transaction_summary AS (
    SELECT
        entity_name,
        folio_number,
        demat_acc_number,
        MAX(folio_id) AS folio_id,
        MAX(demat_id) AS demat_id,
        MAX(scheme_id) AS scheme_id,
        MAX(scheme_name) AS scheme_name,
        MAX(isin) AS isin,
        MAX(amfi_scheme_code) AS amfi_scheme_code,
` + TransactionSummaryMetrics + `
    FROM scheme_resolved
    WHERE NULLIF(TRIM(entity_name), '') IS NOT NULL
    GROUP BY entity_name, folio_number, demat_acc_number,
             COALESCE(NULLIF(TRIM(amfi_scheme_code), ''), NULLIF(TRIM(scheme_id), ''), '')
)`
