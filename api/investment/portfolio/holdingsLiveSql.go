package portfolio

import (
	"CimplrCorpSaas/api/investment/schemejoin"
)

// portfolioSchemeResolvedCTE is the shared transaction union used for live holdings
// and portfolio snapshot rebuilds. $1 = optional entity_name[] filter (NULL = all).
const portfolioSchemeResolvedCTE = `
scheme_resolved AS (
    SELECT
        ot.transaction_date,
        ot.transaction_type,
        ot.amount,
        ot.units,
        ot.nav,
        TRIM(COALESCE(mf.entity_name, md.entity_name, ot.entity_name, '')) AS entity_name,
        ot.folio_number,
        ot.demat_acc_number,
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
    WHERE ($1::text[] IS NULL OR TRIM(COALESCE(mf.entity_name, md.entity_name, ot.entity_name, '')) = ANY($1::text[]))

    UNION ALL

    SELECT
        i.transaction_date,
        'Purchase' AS transaction_type,
        c.net_amount AS amount,
        c.allotted_units AS units,
        c.nav,
        TRIM(i.entity_name) AS entity_name,
        mf.folio_number,
        md.demat_account_number AS demat_acc_number,
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
      AND ($1::text[] IS NULL OR TRIM(i.entity_name) = ANY($1::text[]))
),
transaction_summary AS (
    SELECT
        entity_name,
        folio_number,
        demat_acc_number,
        folio_id,
        demat_id,
        scheme_id,
        scheme_name,
        isin,
        MAX(amfi_scheme_code) AS amfi_scheme_code,
` + TransactionSummaryMetrics + `
    FROM scheme_resolved
    WHERE NULLIF(TRIM(entity_name), '') IS NOT NULL
    GROUP BY entity_name, folio_number, demat_acc_number, folio_id, demat_id, scheme_id, scheme_name, isin
)`
