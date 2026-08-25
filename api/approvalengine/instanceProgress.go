package approvalengine

import "fmt"

// SQLInstanceApprovalProgressSelect returns aggregated instance-level approval
// counters for list/expandable UIs. Per-eye rows store local required/received;
// display shows total received and remaining required across all eyes.
func SQLInstanceApprovalProgressSelect() string {
	return `COALESCE(aie_prog.total_received, 0) AS approvals_received,
			   COALESCE(aie_prog.remaining_required, 0) AS approvals_required`
}

// SQLInstanceApprovalProgressJoin lateral-joins aggregated counters for instanceAlias
// (the approval_instance table alias in the outer query, e.g. "ai").
func SQLInstanceApprovalProgressJoin(instanceAlias string) string {
	return fmt.Sprintf(`
		   LEFT JOIN LATERAL (
			   SELECT
				   SUM(COALESCE(e.approvals_received, 0))::int AS total_received,
				   GREATEST(
					   SUM(COALESCE(e.approvals_required, 0)) FILTER (WHERE e.status NOT IN ('SKIPPED'))
					   - SUM(COALESCE(e.approvals_received, 0)),
					   0
				   )::int AS remaining_required
			   FROM uam.approval_instance_eye e
			   WHERE e.instance_id = %s.instance_id
		   ) aie_prog ON true`, instanceAlias)
}
