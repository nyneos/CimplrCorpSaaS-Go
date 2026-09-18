package notification

import (
	"context"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type HedgingProposalPayload struct {
	Action           string
	RequestedBy      string
	CheckerComment   string
	Count            int
	ActionAt         string
	ProcessingStatus string
	EntityName       string
	ProposalIDs      []string
	Proposals        []map[string]interface{}
	ByEntityKPIs     []map[string]interface{}
	TotalHedgeAmount float64
	TotalLineCount   int
}

func (p HedgingProposalPayload) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"Action":           p.Action,
		"RequestedBy":      p.RequestedBy,
		"CheckerComment":   p.CheckerComment,
		"Count":            p.Count,
		"ActionAt":         p.ActionAt,
		"ProcessingStatus": p.ProcessingStatus,
		"EntityName":       p.EntityName,
		"ProposalIDs":      p.ProposalIDs,
		"Proposals":        p.Proposals,
		"ByEntityKPIs":     p.ByEntityKPIs,
		"TotalHedgeAmount": p.TotalHedgeAmount,
		"TotalLineCount":   p.TotalLineCount,
	}
}

type HedgingProposalPayloadInput struct {
	ProposalIDs      []string
	Action           string
	RequestedBy      string
	ProcessingStatus string
	CheckerComment   string
}

func BuildHedgingProposalPayload(ctx context.Context, pool *pgxpool.Pool, in HedgingProposalPayloadInput) HedgingProposalPayload {
	p := HedgingProposalPayload{
		Action:           in.Action,
		RequestedBy:      in.RequestedBy,
		CheckerComment:   in.CheckerComment,
		Count:            len(in.ProposalIDs),
		ActionAt:         time.Now().Format(time.RFC3339),
		ProcessingStatus: in.ProcessingStatus,
		ProposalIDs:      in.ProposalIDs,
		Proposals:        []map[string]interface{}{},
		ByEntityKPIs:     []map[string]interface{}{},
	}
	if pool == nil || len(in.ProposalIDs) == 0 {
		return p
	}

	rows, err := pool.Query(ctx, `
		SELECT d.proposal_id::text, COALESCE(d.proposal_name,''), COALESCE(d.processing_status,''),
		       COALESCE(d.created_by,''), COALESCE(d.updated_by,''), COALESCE(d.comments,''),
		       COALESCE(l.line_count,0), COALESCE(l.primary_business_unit,''), COALESCE(l.business_units,''),
		       COALESCE(l.currencies,''), COALESCE(l.exposure_types,''),
		       COALESCE(l.total_hedge_amount,0), COALESCE(l.total_old_hedge_amount,0)
		FROM public.hedging_proposal_document d
		LEFT JOIN LATERAL (
			SELECT COUNT(*)                                            AS line_count,
			       MIN(NULLIF(TRIM(hl.business_unit),''))              AS primary_business_unit,
			       STRING_AGG(DISTINCT NULLIF(TRIM(hl.business_unit),''), ', ' ORDER BY NULLIF(TRIM(hl.business_unit),'')) AS business_units,
			       STRING_AGG(DISTINCT NULLIF(TRIM(hl.currency),''),      ', ' ORDER BY NULLIF(TRIM(hl.currency),''))      AS currencies,
			       STRING_AGG(DISTINCT NULLIF(TRIM(hl.exposure_type),''), ', ' ORDER BY NULLIF(TRIM(hl.exposure_type),'')) AS exposure_types,
			       SUM(COALESCE(hl.hedge_month1,0) + COALESCE(hl.hedge_month2,0) + COALESCE(hl.hedge_month3,0)
			         + COALESCE(hl.hedge_month4,0) + COALESCE(hl.hedge_month4to6,0) + COALESCE(hl.hedge_month6plus,0))     AS total_hedge_amount,
			       SUM(COALESCE(hl.old_hedge_month1,0) + COALESCE(hl.old_hedge_month2,0) + COALESCE(hl.old_hedge_month3,0)
			         + COALESCE(hl.old_hedge_month4,0) + COALESCE(hl.old_hedge_month4to6,0) + COALESCE(hl.old_hedge_month6plus,0)) AS total_old_hedge_amount
			FROM public.hedging_proposal_document_line hl
			WHERE hl.proposal_id = d.proposal_id
		) l ON true
		WHERE d.proposal_id::text = ANY($1)
	`, in.ProposalIDs)
	if err != nil {
		return p
	}
	defer rows.Close()

	entityGroups := map[string]map[string]interface{}{}
	for rows.Next() {
		var proposalID, proposalName, processingStatus string
		var createdBy, updatedBy, comments string
		var lineCount int
		var primaryBU, businessUnits, currencies, exposureTypes string
		var hedgeAmount, oldHedgeAmount float64
		if scanErr := rows.Scan(&proposalID, &proposalName, &processingStatus,
			&createdBy, &updatedBy, &comments,
			&lineCount, &primaryBU, &businessUnits,
			&currencies, &exposureTypes,
			&hedgeAmount, &oldHedgeAmount); scanErr != nil {
			continue
		}

		p.Proposals = append(p.Proposals, map[string]interface{}{
			"proposal_id":            proposalID,
			"proposal_name":          proposalName,
			"processing_status":      processingStatus,
			"entity":                 primaryBU,
			"business_unit":          businessUnits,
			"currency":               currencies,
			"exposure_type":          exposureTypes,
			"line_count":             lineCount,
			"total_hedge_amount":     hedgeAmount,
			"total_old_hedge_amount": oldHedgeAmount,
			"comments":               comments,
			"created_by":             createdBy,
			"updated_by":             updatedBy,
		})

		p.TotalHedgeAmount += hedgeAmount
		p.TotalLineCount += lineCount
		if p.EntityName == "" {
			p.EntityName = strings.TrimSpace(primaryBU)
		}

		entityName := strings.TrimSpace(primaryBU)
		if entityName == "" {
			entityName = "Unknown"
		}
		kpi, ok := entityGroups[entityName]
		if !ok {
			kpi = map[string]interface{}{
				"group_name":         entityName,
				"count":              0,
				"line_count":         0,
				"total_hedge_amount": float64(0),
			}
			entityGroups[entityName] = kpi
		}
		kpi["count"] = kpi["count"].(int) + 1
		kpi["line_count"] = kpi["line_count"].(int) + lineCount
		kpi["total_hedge_amount"] = kpi["total_hedge_amount"].(float64) + hedgeAmount
	}
	for _, kpi := range entityGroups {
		p.ByEntityKPIs = append(p.ByEntityKPIs, kpi)
	}
	return p
}
