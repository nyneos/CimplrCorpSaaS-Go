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

// dispatchNotifyBreaches fires notification catalog for NotifyOnly, SoftWarning,
// and TriggerApproval breaches (BRD §6.3 async/scheduled: NotifyOnly |
// TriggerApproval; SoftWarning kept for POST_* monitor triggers).
// source_route (same string used by TriggerNotification) — in practice this
// is always the single, module-agnostic "policy-engine/breach" event
// (module_code=POLICYENGINE, sub_module_code=POLICY_BREACH, seeded by
// cmd/seedDomainCatalog) since a policy breach isn't tied to any one
// handler's route. Empty group → skip (no dispatch at all — the policy form's
// "None — log only" option). When the policy has curated template picks in
// policyengine_svc.policy_notification_template, dispatch is restricted to
// exactly those templates instead of every enabled channel for the event.
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
	templatesByPolicy := loadPolicyNotificationTemplates(ctx, pool, policies)

	for _, pr := range results {
		if pr.Result != "BREACH" {
			continue
		}
		if pr.Action != common.BreachNotifyOnly && pr.Action != common.BreachSoftWarning &&
			pr.Action != common.BreachTriggerApproval {
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
		extras := map[string]interface{}{
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
			extras["PolicyCode"] = codeByPolicy[pr.PolicyID]
		}
		// Prefer field_code / NOTIFICATION alias_key keys (template UI).
		// Fall back to CDM paths only when NotifyFields empty (legacy callers).
		payload := BuildDispatchPayload(ctx, pool, req.SubModule, req.NotifyFields, extras)
		if len(req.NotifyFields) == 0 {
			for k, v := range req.Variables {
				if strings.TrimSpace(k) == "" || strings.TrimSpace(v) == "" {
					continue
				}
				if _, exists := payload[k]; !exists {
					payload[k] = v
				}
			}
		}

		bg := context.WithoutCancel(ctx)
		templateIDs := templatesByPolicy[pr.PolicyID]
		go func(sourceRoute, correlationID string, pl map[string]interface{}, tplIDs []string) {
			defer func() {
				if rec := recover(); rec != nil {
					api.LogError("policy NotifyOnly dispatch panic route=%s: %v", sourceRoute, rec)
				}
			}()
			if len(tplIDs) > 0 {
				notifcatalog.TriggerNotificationForTemplates(bg, pool, sourceRoute, correlationID, pl, tplIDs)
				return
			}
			notifcatalog.TriggerNotification(bg, pool, sourceRoute, correlationID, pl)
		}(route, corr+"-"+pr.PolicyID, payload, templateIDs)
	}
}

// loadPolicyNotificationTemplates batches one query for every policy_id in
// this check's policy set, returning the curated template_id list (if any)
// for each. Policies with no rows here fall back to "every enabled channel
// for the event" (TriggerNotification's default behavior).
func loadPolicyNotificationTemplates(ctx context.Context, pool *pgxpool.Pool, policies []map[string]interface{}) map[string][]string {
	out := map[string][]string{}
	ids := make([]string, 0, len(policies))
	for _, p := range policies {
		if id, _ := p["policy_id"].(string); id != "" {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return out
	}
	rows, err := pool.Query(ctx, `
		SELECT policy_id::text, template_id
		FROM policyengine_svc.policy_notification_template
		WHERE policy_id = ANY($1::uuid[]) AND is_deleted = false`,
		ids,
	)
	if err != nil {
		api.LogError("loadPolicyNotificationTemplates: %v", err)
		return out
	}
	defer rows.Close()
	for rows.Next() {
		var policyID, templateID string
		if err := rows.Scan(&policyID, &templateID); err != nil {
			continue
		}
		out[policyID] = append(out[policyID], templateID)
	}
	return out
}
