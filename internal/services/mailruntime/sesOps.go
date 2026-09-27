package mailruntime

import (
	"context"

	"CimplrCorpSaas/internal/logger"
	"CimplrCorpSaas/internal/mailengine/ses"
)

func (r *Runtime) ApplyInboundRules(ctx context.Context, ruleSetName, bucket, prefix string, rules []InboundRuleSpec) (*InboundRuleSyncResult, error) {
	var out InboundRuleSyncResult
	err := runIsolated(ctx, 0, func(ctx context.Context) error {
		specs := make([]ses.RuleSpec, 0, len(rules))
		for _, rl := range rules {
			specs = append(specs, ses.RuleSpec{RuleName: rl.RuleName, Recipient: rl.Recipient})
		}
		logger.LogInfoCtx(ctx, "[mail] ses/sync: start rule_set=%s rules=%d bucket=%s prefix=%s", ruleSetName, len(rules), bucket, prefix)
		result, err := ses.SyncReceiptRules(ctx, ses.SyncRequest{
			RuleSetName: ruleSetName,
			S3Bucket:    bucket,
			S3Prefix:    prefix,
			Rules:       specs,
		})
		if err != nil {
			logger.LogErrorCtx(ctx, "[mail] ses/sync: failed err=%v", err)
			return err
		}
		out = InboundRuleSyncResult{
			RuleSetName: result.RuleSetName,
			Synced:      result.Synced,
			Removed:     result.Removed,
			Errors:      result.Errors,
		}
		logger.LogInfoCtx(ctx, "[mail] ses/sync: done synced=%d removed=%d errors=%d", out.Synced, out.Removed, len(out.Errors))
		for _, e := range out.Errors {
			logger.LogErrorCtx(ctx, "[mail] ses/sync: %s", e)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &out, nil
}

func (r *Runtime) RemoveInboundRule(ctx context.Context, ruleSetName, ruleName string) error {
	return runIsolated(ctx, 0, func(ctx context.Context) error {
		if err := ses.DeleteReceiptRule(ctx, ruleSetName, ruleName); err != nil {
			logger.LogErrorCtx(ctx, "[mail] ses/delete: rule=%s err=%v", ruleName, err)
			return err
		}
		return nil
	})
}
