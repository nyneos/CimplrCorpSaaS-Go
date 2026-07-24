package common

// Newest maker/checker activity first (requested_at or checker_at, whichever is later).
// Falls back to master last_modified_at / created_at when no audit row exists yet.
const (
	PolicyListOrderBy = `ORDER BY COALESCE((
		SELECT GREATEST(
			COALESCE(a.requested_at, '1970-01-01'::timestamptz),
			COALESCE(a.checker_at, '1970-01-01'::timestamptz)
		)
		FROM policyengine_svc.policy_master_audit a
		WHERE a.policy_id = p.policy_id
		ORDER BY a.requested_at DESC
		LIMIT 1
	), p.last_modified_at, p.created_at) DESC`

	CdmListOrderBy = `ORDER BY COALESCE((
		SELECT GREATEST(
			COALESCE(a.requested_at, '1970-01-01'::timestamptz),
			COALESCE(a.checker_at, '1970-01-01'::timestamptz)
		)
		FROM policyengine_svc.cdm_variable_audit a
		WHERE a.variable_id = cdm_variable.variable_id
		ORDER BY a.requested_at DESC
		LIMIT 1
	), last_modified_at, created_at) DESC`

	// Alias-friendly variant when the master is selected as `c`.
	CdmListOrderByAliased = `ORDER BY COALESCE((
		SELECT GREATEST(
			COALESCE(a.requested_at, '1970-01-01'::timestamptz),
			COALESCE(a.checker_at, '1970-01-01'::timestamptz)
		)
		FROM policyengine_svc.cdm_variable_audit a
		WHERE a.variable_id = c.variable_id
		ORDER BY a.requested_at DESC
		LIMIT 1
	), c.last_modified_at, c.created_at) DESC`

	TriggerListOrderBy = `ORDER BY COALESCE((
		SELECT GREATEST(
			COALESCE(a.requested_at, '1970-01-01'::timestamptz),
			COALESCE(a.checker_at, '1970-01-01'::timestamptz)
		)
		FROM policyengine_svc.trigger_event_audit a
		WHERE a.event_code = trigger_event.event_code
		ORDER BY a.requested_at DESC
		LIMIT 1
	), last_modified_at, created_at) DESC`
)
