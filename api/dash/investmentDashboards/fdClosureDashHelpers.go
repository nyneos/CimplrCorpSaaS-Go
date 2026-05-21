package investmentdashboards

// Shared SQL helpers for FD dashboard maturity / rollover metrics.
// Primary workflow: cimplr.fd_closure_initiate + cimplr.fd_closure_confirm (+ audit).
// Legacy fallback: investment.fd_closure_request (older environments).

// sqlLatestCimplrInitiate joins the latest non-deleted closure initiate per FD.
const sqlLatestCimplrInitiate = `
LEFT JOIN LATERAL (
  SELECT
    ci.closure_initiate_id,
    ci.closure_type,
    ci.closure_status,
    COALESCE(ci.action_at_maturity, '') AS action_at_maturity,
    COALESCE(ci.net_expected_amount, 0) AS net_expected_amount,
    ci.created_at
  FROM cimplr.fd_closure_initiate ci
  WHERE ci.fd_id = m.fd_id AND COALESCE(ci.is_deleted, false) = false
  ORDER BY ci.created_at DESC
  LIMIT 1
) cimplr_ci ON true`

// sqlLatestCimplrConfirm joins the latest non-deleted closure confirm per FD.
const sqlLatestCimplrConfirm = `
LEFT JOIN LATERAL (
  SELECT
    cc.closure_confirm_id,
    cc.closure_initiate_id,
    cc.closure_type,
    cc.closure_status,
    COALESCE(cc.posting_status, '') AS posting_status,
    COALESCE(cc.accounting_posted, false) AS accounting_posted
  FROM cimplr.fd_closure_confirm cc
  WHERE cc.fd_id = m.fd_id AND COALESCE(cc.is_deleted, false) = false
  ORDER BY cc.created_at DESC
  LIMIT 1
) cimplr_cc ON true`

// sqlCimplrClosureProcessed returns true when the FD has a completed cimplr closure (posted confirm).
const sqlCimplrClosureProcessed = `EXISTS (
  SELECT 1 FROM cimplr.fd_closure_confirm cc
  WHERE cc.fd_id = m.fd_id
    AND COALESCE(cc.is_deleted, false) = false
    AND cc.closure_status = 'POSTED'
    AND COALESCE(cc.accounting_posted, false) = true
)`

// sqlLegacyClosureProcessed returns true when the FD has a completed legacy closure request.
const sqlLegacyClosureProcessed = `EXISTS (
  SELECT 1 FROM investment.fd_closure_request cr
  WHERE cr.fd_id = m.fd_id
    AND COALESCE(cr.is_deleted, false) = false
    AND cr.closure_status IN ('COMPLETED', 'POSTED', 'CLOSED')
)`

// sqlAnyClosureProcessed — FD treated as maturity-processed if either workflow completed.
const sqlAnyClosureProcessed = `(` + sqlCimplrClosureProcessed + ` OR ` + sqlLegacyClosureProcessed + `)`

// sqlCimplrInitiatePendingApproval — initiate row awaiting maker-checker.
const sqlCimplrInitiatePendingApproval = `EXISTS (
  SELECT 1
  FROM cimplr.fd_closure_initiate ci
  JOIN cimplr.fd_closure_initiate_audit cia
    ON cia.closure_initiate_id = ci.closure_initiate_id
  WHERE ci.fd_id = m.fd_id
    AND COALESCE(ci.is_deleted, false) = false
    AND cia.processing_status LIKE 'PENDING%'
    AND cia.requested_at = (
      SELECT MAX(a2.requested_at)
      FROM cimplr.fd_closure_initiate_audit a2
      WHERE a2.closure_initiate_id = ci.closure_initiate_id
    )
)`

// sqlCimplrConfirmPendingApproval — confirm row awaiting maker-checker.
const sqlCimplrConfirmPendingApproval = `EXISTS (
  SELECT 1
  FROM cimplr.fd_closure_confirm cc
  JOIN cimplr.fd_closure_confirm_audit cca
    ON cca.closure_confirm_id = cc.closure_confirm_id
  WHERE cc.fd_id = m.fd_id
    AND COALESCE(cc.is_deleted, false) = false
    AND cca.processing_status LIKE 'PENDING%'
    AND cca.requested_at = (
      SELECT MAX(a2.requested_at)
      FROM cimplr.fd_closure_confirm_audit a2
      WHERE a2.closure_confirm_id = cc.closure_confirm_id
    )
)`

// sqlCimplrRolloverInFlight — rollover initiated but not yet posted.
const sqlCimplrRolloverInFlight = `EXISTS (
  SELECT 1 FROM cimplr.fd_closure_initiate ci
  WHERE ci.fd_id = m.fd_id
    AND COALESCE(ci.is_deleted, false) = false
    AND UPPER(COALESCE(ci.closure_type, '')) = 'ROLLOVER'
    AND COALESCE(ci.closure_status, '') NOT IN ('REJECTED', 'DELETED')
    AND NOT EXISTS (
      SELECT 1 FROM cimplr.fd_closure_confirm cc
      WHERE cc.closure_initiate_id = ci.closure_initiate_id
        AND COALESCE(cc.is_deleted, false) = false
        AND cc.closure_status = 'POSTED'
        AND COALESCE(cc.accounting_posted, false) = true
    )
)`

// Effective closure instruction for dashboard rows (cimplr preferred, then master, then legacy).
const sqlEffectiveClosureType = `COALESCE(NULLIF(cimplr_ci.closure_type, ''), NULLIF(cimplr_cc.closure_type, ''), NULLIF(cr.closure_type, ''), '')`
const sqlEffectiveClosureStatus = `COALESCE(NULLIF(cimplr_cc.closure_status, ''), NULLIF(cimplr_ci.closure_status, ''), NULLIF(cr.closure_status, ''), '')`
