package dmsjobs

import (
	"context"
	"fmt"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/internal/jobs/dmsevent"
	"CimplrCorpSaas/internal/services/docsvc"

	"github.com/jackc/pgx/v5/pgxpool"
)

func init() {
	// Leaf facade for handlers that cannot import this package (import cycle via
	// dashboardBuilder → payablerecievable).
	dmsevent.SetFire(FireDmsEvent)
}

// Canonical DMS event triggers (aligned with policy POST_* after-commit hooks).
// ON_CREATE / ON_APPROVE remain as aliases for existing rules.
var dmsEventTriggers = map[string]string{
	"POST_CREATE":  "POST_CREATE",
	"POST_APPROVE": "POST_APPROVE",
	"POST_UPLOAD":  "POST_UPLOAD",
	"POST_EDIT":    "POST_EDIT",
	"POST_DELETE":  "POST_DELETE",
	"POST_REJECT":  "POST_REJECT",
	// Legacy / aliases → canonical
	"ON_CREATE":       "POST_CREATE",
	"ON_APPROVE":      "POST_APPROVE",
	"ON_UPLOAD":       "POST_UPLOAD",
	"ON_EDIT":         "POST_EDIT",
	"ON_FIELD_CHANGE": "POST_EDIT",
	"ON_DELETE":       "POST_DELETE",
	"ON_REJECT":       "POST_REJECT",
}

// NormalizeDmsEventTrigger maps aliases to canonical POST_* codes.
func NormalizeDmsEventTrigger(triggerType string) (string, error) {
	key := strings.ToUpper(strings.TrimSpace(triggerType))
	canon, ok := dmsEventTriggers[key]
	if !ok {
		return "", fmt.Errorf("unsupported DMS event trigger %q", triggerType)
	}
	return canon, nil
}

// FireDmsEvent schedules rule-matched DMS generation after a successful business
// commit. Rules are papa: if no Active+APPROVED rule has this trigger for the
// module/sub-module, this is a silent no-op. Never blocks the HTTP response.
func FireDmsEvent(
	pool *pgxpool.Pool,
	moduleCode, subModuleCode, triggerType string,
	sourceIDs []string,
	triggeredBy string,
) {
	if pool == nil {
		return
	}
	mod := strings.TrimSpace(moduleCode)
	sub := strings.TrimSpace(subModuleCode)
	if mod == "" || sub == "" {
		return
	}
	ids := make([]string, 0, len(sourceIDs))
	for _, id := range sourceIDs {
		if s := strings.TrimSpace(id); s != "" {
			ids = append(ids, s)
		}
	}
	// Never run EVENT generation without explicit source rows — empty IDs would
	// expand to the rule's full time-window set (wrong docs, heavy load).
	if len(ids) == 0 {
		return
	}
	actor := strings.TrimSpace(triggeredBy)
	trig := strings.TrimSpace(triggerType)
	go func() {
		defer func() {
			if rec := recover(); rec != nil {
				api.LogError("[DMS-EVENT] panic module=%s sub=%s trigger=%s: %v", mod, sub, trig, rec)
			}
		}()
		if err := RunEventRules(context.Background(), pool, mod, sub, trig, ids, actor); err != nil {
			api.LogError("[DMS-EVENT] module=%s sub=%s trigger=%s: %v", mod, sub, trig, err)
		}
	}()
}

type eventRule struct {
	RuleID        string
	SourceIDField string
}

// RunEventRules finds approved rules for one module/sub-module event trigger
// and generates only the source rows from the committed business action.
// Prefer FireDmsEvent from handlers (async, recover-safe).
func RunEventRules(
	ctx context.Context,
	pool *pgxpool.Pool,
	moduleCode, subModuleCode, triggerType string,
	sourceIDs []string,
	triggeredBy string,
) error {
	canon, err := NormalizeDmsEventTrigger(triggerType)
	if err != nil {
		return err
	}
	// Match canonical + legacy alias so older seeded rules still fire.
	aliases := []string{canon}
	for alias, target := range dmsEventTriggers {
		if target == canon && alias != canon {
			aliases = append(aliases, alias)
		}
	}

	rows, err := pool.Query(ctx, `
		SELECT r.rule_id::text, t.source_id_field
		FROM dms_svc.generation_rule r
		JOIN dms_svc.generation_rule_version v
		  ON v.version_id = r.current_version_id
		JOIN dms_svc.generation_rule_trigger t
		  ON t.version_id = v.version_id
		WHERE r.module_code = $1
		  AND r.sub_module_code = $2
		  AND r.status = 'Active'
		  AND r.processing_status = 'APPROVED'
		  AND r.is_deleted = false
		  AND v.status = 'APPROVED'
		  AND t.trigger_type = ANY($3::text[])
		  AND t.is_enabled = true
		ORDER BY r.rule_id, t.sort_order`,
		moduleCode, subModuleCode, aliases)
	if err != nil {
		return fmt.Errorf("load DMS event rules: %w", err)
	}
	defer rows.Close()
	var rules []eventRule
	for rows.Next() {
		var rule eventRule
		if err := rows.Scan(&rule.RuleID, &rule.SourceIDField); err != nil {
			return err
		}
		if strings.TrimSpace(rule.SourceIDField) == "" {
			api.LogError("[DMS-EVENT] rule=%s trigger=%s missing source_id_field", rule.RuleID, canon)
			continue
		}
		rules = append(rules, rule)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(rules) == 0 {
		api.LogInfo("[DMS-EVENT] no matching rules module=%s sub=%s trigger=%s — no-op", moduleCode, subModuleCode, canon)
		return nil
	}
	out := businessAuditOutcome{Trigger: canon, RuleCount: len(rules)}
	useQueue := docsvc.QueueEnabled()
	for _, rule := range rules {
		if useQueue {
			jobID, qErr := EnqueueRuleGeneration(
				ctx, pool, rule.RuleID, "EVENT", triggeredBy,
				rule.SourceIDField, sourceIDs,
			)
			if qErr != nil {
				out.FailCount++
				out.LastError = qErr.Error()
				api.LogError("[DMS-EVENT] enqueue rule=%s trigger=%s: %v", rule.RuleID, canon, qErr)
				continue
			}
			out.OKCount++
			api.LogInfo("[DMS-EVENT] queued rule=%s trigger=%s job=%s source_rows=%d", rule.RuleID, canon, jobID, len(sourceIDs))
			continue
		}
		runID, runErr := RunGenerationForSourceIDs(
			ctx, pool, rule.RuleID, "EVENT", triggeredBy,
			rule.SourceIDField, sourceIDs,
		)
		if runErr != nil {
			out.FailCount++
			out.LastError = runErr.Error()
			api.LogError("[DMS-EVENT] rule=%s trigger=%s run=%s: %v", rule.RuleID, canon, runID, runErr)
			continue
		}
		out.OKCount++
		if strings.TrimSpace(runID) != "" {
			out.RunIDs = append(out.RunIDs, runID)
		}
		api.LogInfo("[DMS-EVENT] rule=%s trigger=%s run=%s source_rows=%d", rule.RuleID, canon, runID, len(sourceIDs))
	}
	recordBusinessDmsAudits(ctx, pool, moduleCode, subModuleCode, sourceIDs, triggeredBy, out)
	return nil
}
