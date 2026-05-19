package investment

import (
	"context"
	"fmt"

	"CimplrCorpSaas/api/notification/catalog"

	"github.com/jackc/pgx/v5/pgxpool"

	"CimplrCorpSaas/internal/logger")

// BuildOnboardUploadNotifPayload constructs and fires a notification after a bulk upload.
// It calls fetchBatchInfo to get the full entity breakdown for the batch.
func BuildOnboardUploadNotifPayload(ctx context.Context, pool *pgxpool.Pool, batchID, userEmail string) {
	info, err := fetchBatchInfo(ctx, pool, batchID)
	if err != nil {
		logger.LogInfo("[notif][onboard-upload] fetchBatchInfo(%s) failed: %v", batchID, err)
		return
	}

	payload := map[string]interface{}{
		"batch_id":      info.BatchID,
		"user_email":    info.UserEmail,
		"source":        info.Source,
		"status":        info.Status,
		"total_records": info.TotalRecords,
		"entity_counts": map[string]int{
			"amc":          len(info.Entities.AMC),
			"scheme":       len(info.Entities.Scheme),
			"dp":           len(info.Entities.DP),
			"demat":        len(info.Entities.Demat),
			"folio":        len(info.Entities.Folio),
			"transactions": len(info.Transactions),
			"snapshots":    len(info.Snapshots),
		},
		"AMCs":             info.Entities.AMC,
		"Schemes":          info.Entities.Scheme,
		"DPs":              info.Entities.DP,
		"Demats":           info.Entities.Demat,
		"Folios":           info.Entities.Folio,
		"transactions":     info.Transactions,
		"snapshots":        info.Snapshots,
		"enriched_summary": buildEnrichedSummary(info),
		"action":           "UPLOAD",
		"triggered_by":     userEmail,
	}

	catalog.TriggerNotification(ctx, pool, "/investment/onboard/upload", batchID, payload)
}

// BuildOnboardApprovalNotifPayload constructs and fires a notification after batch approval/rejection.
func BuildOnboardApprovalNotifPayload(ctx context.Context, pool *pgxpool.Pool, batchID, action, userEmail string) {
	info, err := fetchBatchInfo(ctx, pool, batchID)
	if err != nil {
		logger.LogInfo("[notif][onboard-approval] fetchBatchInfo(%s) failed: %v", batchID, err)
		return
	}

	route := "/investment/onboard/batch/approve"
	if action == "REJECT" {
		route = "/investment/onboard/batch/reject"
	}

	payload := map[string]interface{}{
		"batch_id":        info.BatchID,
		"user_email":      info.UserEmail,
		"source":          info.Source,
		"status":          info.Status,
		"approval_status": info.ApprovalStatus,
		"total_records":   info.TotalRecords,

		"action":       action,
		"triggered_by": userEmail,

		"entity_counts": map[string]int{
			"amc":    len(info.Entities.AMC),
			"scheme": len(info.Entities.Scheme),
			"dp":     len(info.Entities.DP),
			"demat":  len(info.Entities.Demat),
			"folio":  len(info.Entities.Folio),
		},

		"AMCs":    info.Entities.AMC,
		"Schemes": info.Entities.Scheme,
		"DPs":     info.Entities.DP,
		"Demats":  info.Entities.Demat,
		"Folios":  info.Entities.Folio,

		"transactions": info.Transactions,
		"snapshots":    info.Snapshots,

		"summary": fmt.Sprintf("Batch %s %sd by %s — %d AMC, %d Scheme, %d DP, %d Demat, %d Folio",
			batchID, action,
			userEmail,
			len(info.Entities.AMC),
			len(info.Entities.Scheme),
			len(info.Entities.DP),
			len(info.Entities.Demat),
			len(info.Entities.Folio),
		),
	}

	catalog.TriggerNotification(ctx, pool, route, batchID, payload)
}

// buildEnrichedSummary returns counts of enriched vs new entities.
func buildEnrichedSummary(info *BatchInfo) map[string]interface{} {
	newAMC, enrichedAMC := 0, 0
	for _, e := range info.Entities.AMC {
		if e.Enriched {
			enrichedAMC++
		} else {
			newAMC++
		}
	}
	newScheme, enrichedScheme := 0, 0
	for _, e := range info.Entities.Scheme {
		if e.Enriched {
			enrichedScheme++
		} else {
			newScheme++
		}
	}
	newDP, enrichedDP := 0, 0
	for _, e := range info.Entities.DP {
		if e.Enriched {
			enrichedDP++
		} else {
			newDP++
		}
	}
	newDemat, enrichedDemat := 0, 0
	for _, e := range info.Entities.Demat {
		if e.Enriched {
			enrichedDemat++
		} else {
			newDemat++
		}
	}
	newFolio, enrichedFolio := 0, 0
	for _, e := range info.Entities.Folio {
		if e.Enriched {
			enrichedFolio++
		} else {
			newFolio++
		}
	}
	return map[string]interface{}{
		"amc":    map[string]int{"new": newAMC, "enriched": enrichedAMC},
		"scheme": map[string]int{"new": newScheme, "enriched": enrichedScheme},
		"dp":     map[string]int{"new": newDP, "enriched": enrichedDP},
		"demat":  map[string]int{"new": newDemat, "enriched": enrichedDemat},
		"folio":  map[string]int{"new": newFolio, "enriched": enrichedFolio},
	}
}
