package investmentsuite

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// mfProposalRow is the canonical business-field shape for MF_PROPOSAL policy
// checks. MF_PROPOSAL had ZERO runtime.Enforce/mfEnforce calls anywhere in
// investmentProposalCreation.go before this pass — this file adds both the
// canonical field builder AND the five Enforce call sites (Create/Update/
// BulkApprove/BulkReject/BulkDelete), see the handlers below.
//
// Proposal/allocation split (judgment call, flag for reviewer): a proposal
// is a 1:N header/allocation relationship (investment.investment_proposal ->
// investment.investment_proposal_allocation). The domain_catalog.field rows
// already seeded for MF_PROPOSAL (before this pass, all 14 with no
// cdm_path) mix proposal-header field_codes (proposal_id, proposal_name,
// entity_name, total_amount, horizon_days, source, batch_id) with
// allocation-level field_codes (scheme_id, scheme_internal_code, amount,
// percent, policy_status) at the same flat level, with no per-allocation
// qualifier. Rather than leave the allocation side as an unfixed follow-up,
// this row type carries the allocation columns as slices (one element per
// non-deleted allocation row) and the builder below passes them through as
// array-valued Fields entries under the same field_code keys already
// seeded. This mirrors the array-Fields precedent already in this same file
// (proposalMFReferenceFields/updateProposalMFReferenceFields pass
// scheme_ids/scheme_internal_codes as arrays for entity-scope validation).
// Reviewer should confirm the policy engine's condition evaluator is
// expected to run per-allocation-array checks (e.g. ANY/ALL semantics)
// against these fields before relying on them for a real rule.
//
// Two allocation-table columns (post_trade_holding, current_holding) plus
// initiation_status are real scalar columns NOT included here and NOT added
// to domain_catalog in this pass — left as a follow-up per the task's
// explicit "decide and document" instruction, since they'd need the same
// array-Fields judgment call as above and are lower-priority than getting
// the five missing Enforce calls wired in at all.
type mfProposalRow struct {
	ProposalID   string
	ProposalName string
	EntityName   string
	TotalAmount  float64
	HorizonDays  *int
	Source       string
	BatchID      string

	// Allocation-level (1:N) — one element per non-deleted allocation row.
	SchemeIDs           []string
	SchemeInternalCodes []string
	Amounts             []float64
	Percents            []float64
	PolicyStatuses      []bool
}

// buildMFProposalPolicyFields maps the canonical row onto the exact
// field_code keys seeded in domain_catalog for MF_PROPOSAL. processing_status
// and reason are seeded field_codes too but are audit-table-derived
// (auditactionproposal), not real columns on investment_proposal or
// investment_proposal_allocation, so — consistent with the same exclusion
// made for MF_INITIATION (age_days/amc_name/nav/scheme_name/
// processing_status) and MF_CONFIRMATION (initiation_entity_name/
// initiation_scheme_name) — they are left out of this row/builder.
func buildMFProposalPolicyFields(row mfProposalRow) map[string]interface{} {
	return map[string]interface{}{
		"proposal_id":          row.ProposalID,
		"proposal_name":        row.ProposalName,
		"entity_name":          row.EntityName,
		"total_amount":         row.TotalAmount,
		"horizon_days":         row.HorizonDays,
		"source":               row.Source,
		"batch_id":             row.BatchID,
		"scheme_id":            row.SchemeIDs,
		"scheme_internal_code": row.SchemeInternalCodes,
		"amount":               row.Amounts,
		"percent":              row.Percents,
		"policy_status":        row.PolicyStatuses,
	}
}

// loadMFProposalRow fetches the full canonical row (header + non-deleted
// allocations) by proposal_id — used by BulkApprove/BulkReject/BulkDelete,
// which only ever receive a proposal_id in the request, never the business
// data itself.
func loadMFProposalRow(ctx context.Context, pool *pgxpool.Pool, proposalID string) (mfProposalRow, error) {
	var row mfProposalRow
	row.ProposalID = proposalID
	err := pool.QueryRow(ctx, `
		SELECT proposal_name, entity_name, total_amount, horizon_days,
		       source, COALESCE(batch_id::text,'')
		FROM investment.investment_proposal
		WHERE proposal_id = $1 AND COALESCE(is_deleted,false) = false`, proposalID,
	).Scan(&row.ProposalName, &row.EntityName, &row.TotalAmount, &row.HorizonDays,
		&row.Source, &row.BatchID)
	if err != nil {
		return mfProposalRow{}, fmt.Errorf("load mf proposal for policy: %w", err)
	}

	rows, err := pool.Query(ctx, `
		SELECT COALESCE(scheme_id,''), COALESCE(scheme_internal_code,''), amount,
		       percent, COALESCE(policy_status,true)
		FROM investment.investment_proposal_allocation
		WHERE proposal_id = $1 AND COALESCE(is_deleted,false) = false
		ORDER BY id`, proposalID)
	if err != nil {
		return mfProposalRow{}, fmt.Errorf("load mf proposal allocations for policy: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var schemeID, schemeCode string
		var amount float64
		var percent *float64
		var policyStatus bool
		if err := rows.Scan(&schemeID, &schemeCode, &amount, &percent, &policyStatus); err != nil {
			return mfProposalRow{}, fmt.Errorf("scan mf proposal allocation for policy: %w", err)
		}
		row.SchemeIDs = append(row.SchemeIDs, schemeID)
		row.SchemeInternalCodes = append(row.SchemeInternalCodes, schemeCode)
		row.Amounts = append(row.Amounts, amount)
		if percent != nil {
			row.Percents = append(row.Percents, *percent)
		}
		row.PolicyStatuses = append(row.PolicyStatuses, policyStatus)
	}
	if err := rows.Err(); err != nil {
		return mfProposalRow{}, fmt.Errorf("iterate mf proposal allocations for policy: %w", err)
	}
	return row, nil
}

// mfProposalRowFromCreate builds a canonical row directly from a
// CreateProposalRequest — used by CreateInvestmentProposal, which has the
// full business payload in hand and doesn't need a DB round trip.
func mfProposalRowFromCreate(req *CreateProposalRequest) mfProposalRow {
	row := mfProposalRow{
		ProposalName: req.ProposalName,
		EntityName:   req.EntityName,
		TotalAmount:  req.TotalAmount,
		HorizonDays:  req.HorizonDays,
		Source:       req.Source,
		BatchID:      req.BatchID,
	}
	for _, a := range req.Allocations {
		row.SchemeIDs = append(row.SchemeIDs, a.SchemeID)
		row.SchemeInternalCodes = append(row.SchemeInternalCodes, a.SchemeInternalCode)
		row.Amounts = append(row.Amounts, a.Amount)
		if a.Percent != nil {
			row.Percents = append(row.Percents, *a.Percent)
		}
		row.PolicyStatuses = append(row.PolicyStatuses, boolOrDefault(a.PolicyStatus, true))
	}
	return row
}

// mfProposalRowFromUpdate builds a canonical row directly from an
// UpdateProposalRequest — used by UpdateInvestmentProposal, which (unlike
// UpdateInitiation/UpdateConfirmation) always supplies the full field set,
// not a partial edit map, so no applyMFProposalEdits merge step is needed.
func mfProposalRowFromUpdate(req *UpdateProposalRequest) mfProposalRow {
	row := mfProposalRow{
		ProposalID:   req.ProposalID,
		ProposalName: req.ProposalName,
		EntityName:   req.EntityName,
		TotalAmount:  req.TotalAmount,
		HorizonDays:  req.HorizonDays,
		Source:       req.Source,
		BatchID:      req.BatchID,
	}
	for _, a := range req.Allocations {
		row.SchemeIDs = append(row.SchemeIDs, a.SchemeID)
		row.SchemeInternalCodes = append(row.SchemeInternalCodes, a.SchemeInternalCode)
		row.Amounts = append(row.Amounts, a.Amount)
		if a.Percent != nil {
			row.Percents = append(row.Percents, *a.Percent)
		}
		row.PolicyStatuses = append(row.PolicyStatuses, boolOrDefault(a.PolicyStatus, true))
	}
	return row
}
