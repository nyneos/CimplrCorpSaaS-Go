package policy

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5"
)

// insertPolicyChildren attaches trigger events, modules, and rule-type child
// rows (slabs / composition buckets / list values) to a freshly inserted policy.
func insertPolicyChildren(r *http.Request, tx pgx.Tx, policyID string, triggerEvents, modules []string, ruleType string, rf ruleFields) error {
	ctx := r.Context()
	for _, code := range triggerEvents {
		code = strings.TrimSpace(code)
		if code == "" {
			continue
		}
		// ON CONFLICT DO UPDATE (not DO NOTHING) so a re-edit that keeps the same
		// event_code reactivates the row instead of clashing with the soft-deleted one.
		if _, err := tx.Exec(ctx, `
			INSERT INTO policyengine_svc.policy_trigger (policy_id, event_code)
			VALUES ($1::uuid, $2)
			ON CONFLICT (policy_id, event_code) DO UPDATE SET is_deleted = false`, policyID, code); err != nil {
			return fmt.Errorf("trigger %s: %w", code, err)
		}
	}
	for _, code := range modules {
		code = strings.TrimSpace(code)
		if code == "" {
			continue
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO policyengine_svc.policy_module (policy_id, module_code)
			VALUES ($1::uuid, $2)
			ON CONFLICT (policy_id, module_code) DO UPDATE SET is_deleted = false`, policyID, code); err != nil {
			return fmt.Errorf("module %s: %w", code, err)
		}
	}

	switch ruleType {
	case "slabs":
		for i, row := range rf.SlabRows {
			if _, err := tx.Exec(ctx, `
				INSERT INTO policyengine_svc.policy_slab_row
					(policy_id, seq_order, from_value, to_value, mode, action, approval_ref, label, is_deleted)
				VALUES ($1::uuid, $2, $3, $4, $5, $6, NULLIF($7,''), COALESCE($8,''), false)
				ON CONFLICT (policy_id, seq_order) DO UPDATE SET
					from_value = EXCLUDED.from_value, to_value = EXCLUDED.to_value, mode = EXCLUDED.mode,
					action = EXCLUDED.action, approval_ref = EXCLUDED.approval_ref, label = EXCLUDED.label,
					is_deleted = false`,
				policyID, i+1, row.From, row.To, row.Mode, row.Action, row.ApprovalRef, row.Label); err != nil {
				return fmt.Errorf("slab row %d: %w", i+1, err)
			}
		}
	case "composition":
		for i, bucket := range rf.CompBuckets {
			if _, err := tx.Exec(ctx, `
				INSERT INTO policyengine_svc.policy_composition_bucket
					(policy_id, label, variable, min_value, max_value, seq_order)
				VALUES ($1::uuid, $2, $3, $4, $5, $6)`,
				policyID, bucket.Label, bucket.Variable, bucket.Min, bucket.Max, i+1); err != nil {
				return fmt.Errorf("composition bucket %d: %w", i+1, err)
			}
		}
	case "list":
		for _, value := range rf.ListValues {
			value = strings.TrimSpace(value)
			if value == "" {
				continue
			}
			if _, err := tx.Exec(ctx, `
				INSERT INTO policyengine_svc.policy_list_value (policy_id, value)
				VALUES ($1::uuid, $2)
				ON CONFLICT (policy_id, value) DO UPDATE SET is_deleted = false`, policyID, value); err != nil {
				return fmt.Errorf("list value %q: %w", value, err)
			}
		}
	}
	return nil
}

// replacePolicyChildren soft-deletes existing trigger/module/rule-type children
// and re-inserts the new set — used by HandleUpdate.
func replacePolicyChildren(ctx context.Context, tx pgx.Tx, r *http.Request, policyID string, triggerEvents, modules []string, ruleType string, rf ruleFields) error {
	if _, err := tx.Exec(ctx, `UPDATE policyengine_svc.policy_trigger SET is_deleted = true WHERE policy_id = $1::uuid`, policyID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE policyengine_svc.policy_module SET is_deleted = true WHERE policy_id = $1::uuid`, policyID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE policyengine_svc.policy_slab_row SET is_deleted = true WHERE policy_id = $1::uuid`, policyID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE policyengine_svc.policy_composition_bucket SET is_deleted = true WHERE policy_id = $1::uuid`, policyID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE policyengine_svc.policy_list_value SET is_deleted = true WHERE policy_id = $1::uuid`, policyID); err != nil {
		return err
	}
	return insertPolicyChildren(r, tx, policyID, triggerEvents, modules, ruleType, rf)
}
