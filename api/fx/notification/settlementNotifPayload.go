package notification

import (
	"context"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type SettlementPayload struct {
	Action             string
	RequestedBy        string
	CheckerComment     string
	Count              int
	ActionAt           string
	ProcessingStatus   string
	EntityName         string
	SettlementIDs      []string
	Settlements        []map[string]interface{}
	ByEntityKPIs       []map[string]interface{}
	TotalOpenAmount    float64
	TotalSettledAmount float64
	TotalGainLoss      float64
}

func (p SettlementPayload) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"Action":             p.Action,
		"RequestedBy":        p.RequestedBy,
		"CheckerComment":     p.CheckerComment,
		"Count":              p.Count,
		"ActionAt":           p.ActionAt,
		"ProcessingStatus":   p.ProcessingStatus,
		"EntityName":         p.EntityName,
		"SettlementIDs":      p.SettlementIDs,
		"Settlements":        p.Settlements,
		"ByEntityKPIs":       p.ByEntityKPIs,
		"TotalOpenAmount":    p.TotalOpenAmount,
		"TotalSettledAmount": p.TotalSettledAmount,
		"TotalGainLoss":      p.TotalGainLoss,
	}
}

type SettlementPayloadInput struct {
	SettlementIDs    []string
	Action           string
	RequestedBy      string
	ProcessingStatus string
	CheckerComment   string
}

func BuildSettlementPayload(ctx context.Context, pool *pgxpool.Pool, in SettlementPayloadInput) SettlementPayload {
	p := SettlementPayload{
		Action:           in.Action,
		RequestedBy:      in.RequestedBy,
		CheckerComment:   in.CheckerComment,
		Count:            len(in.SettlementIDs),
		ActionAt:         time.Now().Format(time.RFC3339),
		ProcessingStatus: in.ProcessingStatus,
		SettlementIDs:    in.SettlementIDs,
		Settlements:      []map[string]interface{}{},
		ByEntityKPIs:     []map[string]interface{}{},
	}
	if pool == nil || len(in.SettlementIDs) == 0 {
		return p
	}

	rows, err := pool.Query(ctx, `
		SELECT d.settlement_id::text, COALESCE(d.settlement_method,''), COALESCE(d.entity,''), COALESCE(d.currency,''),
		       COALESCE(d.settlement_date::text,''), COALESCE(d.processing_status,''),
		       COALESCE(d.new_exposure_header_id,''), COALESCE(d.comments,''),
		       COALESCE(d.created_by,''), COALESCE(d.updated_by,''),
		       COALESCE(d.total_open_amount,0), COALESCE(d.total_settled_amount,0), COALESCE(d.total_gain_loss,0),
		       COALESCE(l.header_ids,''), COALESCE(l.line_count,0),
		       COALESCE(l.bank_names,''), COALESCE(l.leg_types,'')
		FROM public.exposure_settlement_document d
		LEFT JOIN LATERAL (
			SELECT STRING_AGG(DISTINCT esl.exposure_header_id, ', ' ORDER BY esl.exposure_header_id) AS header_ids,
			       COUNT(*)                                                                          AS line_count,
			       STRING_AGG(DISTINCT NULLIF(TRIM(esl.bank_name),''), ', ' ORDER BY NULLIF(TRIM(esl.bank_name),'')) AS bank_names,
			       STRING_AGG(DISTINCT NULLIF(TRIM(esl.leg_type),''),  ', ' ORDER BY NULLIF(TRIM(esl.leg_type),''))  AS leg_types
			FROM public.exposure_settlement_line esl
			WHERE esl.settlement_id = d.settlement_id
		) l ON true
		WHERE d.settlement_id::text = ANY($1)
	`, in.SettlementIDs)
	if err != nil {
		return p
	}
	defer rows.Close()

	entityGroups := map[string]map[string]interface{}{}
	for rows.Next() {
		var settlementID, method, entity, currency string
		var settlementDate, processingStatus, newHeaderID, comments string
		var createdBy, updatedBy string
		var openAmount, settledAmount, gainLoss float64
		var headerIDs string
		var lineCount int
		var bankNames, legTypes string
		if scanErr := rows.Scan(&settlementID, &method, &entity, &currency,
			&settlementDate, &processingStatus, &newHeaderID, &comments,
			&createdBy, &updatedBy,
			&openAmount, &settledAmount, &gainLoss,
			&headerIDs, &lineCount,
			&bankNames, &legTypes); scanErr != nil {
			continue
		}

		p.Settlements = append(p.Settlements, map[string]interface{}{
			"settlement_id":          settlementID,
			"settlement_method":      method,
			"entity":                 entity,
			"currency":               currency,
			"settlement_date":        settlementDate,
			"processing_status":      processingStatus,
			"exposure_header_id":     headerIDs,
			"new_exposure_header_id": newHeaderID,
			"total_open_amount":      openAmount,
			"total_settled_amount":   settledAmount,
			"total_gain_loss":        gainLoss,
			"line_count":             lineCount,
			"bank_name":              bankNames,
			"leg_type":               legTypes,
			"comments":               comments,
			"created_by":             createdBy,
			"updated_by":             updatedBy,
		})

		p.TotalOpenAmount += openAmount
		p.TotalSettledAmount += settledAmount
		p.TotalGainLoss += gainLoss
		if p.EntityName == "" {
			p.EntityName = strings.TrimSpace(entity)
		}

		entityName := strings.TrimSpace(entity)
		if entityName == "" {
			entityName = "Unknown"
		}
		kpi, ok := entityGroups[entityName]
		if !ok {
			kpi = map[string]interface{}{
				"group_name":           entityName,
				"count":                0,
				"total_open_amount":    float64(0),
				"total_settled_amount": float64(0),
				"total_gain_loss":      float64(0),
			}
			entityGroups[entityName] = kpi
		}
		kpi["count"] = kpi["count"].(int) + 1
		kpi["total_open_amount"] = kpi["total_open_amount"].(float64) + openAmount
		kpi["total_settled_amount"] = kpi["total_settled_amount"].(float64) + settledAmount
		kpi["total_gain_loss"] = kpi["total_gain_loss"].(float64) + gainLoss
	}
	for _, kpi := range entityGroups {
		p.ByEntityKPIs = append(p.ByEntityKPIs, kpi)
	}
	return p
}
