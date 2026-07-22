package runtime

import (
	"context"
	"strings"
	"time"

	"CimplrCorpSaas/api"
	notifcatalog "CimplrCorpSaas/api/notification/catalog"
	"CimplrCorpSaas/api/policyengine/common"
	"CimplrCorpSaas/internal/services/policysvc"

	"github.com/jackc/pgx/v5/pgxpool"
)

// dispatchNotifyBreaches fires notification catalog for NotifyOnly / SoftWarning
// breaches. notification_group on the policy is treated as the notification
// source_route (same string used by TriggerNotification). Empty group → skip.
func dispatchNotifyBreaches(
	ctx context.Context,
	pool *pgxpool.Pool,
	req CheckRequest,
	policies []map[string]interface{},
	results []policysvc.PolicyResult,
) {
	if pool == nil || len(results) == 0 {
		return
	}
	groupByPolicy := make(map[string]string, len(policies))
	codeByPolicy := make(map[string]string, len(policies))
	for _, p := range policies {
		id, _ := p["policy_id"].(string)
		if id == "" {
			continue
		}
		if g, ok := p["notification_group"].(string); ok {
			groupByPolicy[id] = strings.TrimSpace(g)
		}
		if c, ok := p["code"].(string); ok {
			codeByPolicy[id] = c
		}
	}

	for _, pr := range results {
		if pr.Result != "BREACH" {
			continue
		}
		if pr.Action != common.BreachNotifyOnly && pr.Action != common.BreachSoftWarning {
			continue
		}
		route := strings.TrimSpace(groupByPolicy[pr.PolicyID])
		if route == "" {
			continue
		}
		corr := strings.TrimSpace(req.CorrelationID)
		if corr == "" {
			corr = "POL-" + pr.PolicyID
		}
		payload := map[string]interface{}{
			"Action":       pr.Action,
			"PolicyCode":   pr.Code,
			"PolicyID":     pr.PolicyID,
			"Message":      pr.Message,
			"EventCode":    req.EventCode,
			"ModuleCode":   req.ModuleCode,
			"SubModule":    req.SubModule,
			"EntityCode":   req.EntityCode,
			"ActorEmail":   req.ActorUserID,
			"HandlerName":  req.HandlerName,
			"APIPath":      req.APIPath,
			"ActionAt":     time.Now().UTC().Format(time.RFC3339),
			"TraceID":      req.TraceID,
			"BusinessID":   req.BusinessRecordID,
			"BusinessType": req.BusinessRecordType,
		}
		if codeByPolicy[pr.PolicyID] != "" && pr.Code == "" {
			payload["PolicyCode"] = codeByPolicy[pr.PolicyID]
		}
		for k, v := range req.Variables {
			if strings.TrimSpace(k) == "" || strings.TrimSpace(v) == "" {
				continue
			}
			payload[k] = v
		}

		bg := context.WithoutCancel(ctx)
		go func(sourceRoute, correlationID string, pl map[string]interface{}) {
			defer func() {
				if rec := recover(); rec != nil {
					api.LogError("policy NotifyOnly dispatch panic route=%s: %v", sourceRoute, rec)
				}
			}()
			notifcatalog.TriggerNotification(bg, pool, sourceRoute, correlationID, pl)
		}(route, corr+"-"+pr.PolicyID, payload)
	}
}
