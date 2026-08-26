package approvalengine

// sqlUserOnEyeMember matches the acting user to an approval-matrix eye member
// row aliased as `m`. userParam is the SQL placeholder (e.g. "$1").
//
// Role members match on role id, name, or rolecode so a configured role like
// MASTERMAKER still works when the matrix stored a name instead of roles.id.
// Maker and checker may be the same person — whoever is on the active eye acts.
func sqlUserOnEyeMember(userParam string) string {
	return `(
		(m.assignment_type IN ('USER_ONLY','ROLE_USER') AND m.user_id::text = ` + userParam + `::text)
		OR (
			m.assignment_type IN ('ROLE_ONLY','ROLE_USER')
			AND EXISTS (
				SELECT 1
				FROM public.user_roles ur
				JOIN public.roles r ON r.id = ur.role_id
				WHERE ur.user_id::text = ` + userParam + `::text
				  AND COALESCE(ur.is_deleted, false) = false
				  AND (
					ur.role_id::text = m.role_id::text
					OR r.id::text = m.role_id::text
					OR lower(r.name) = lower(m.role_id)
					OR lower(COALESCE(r.rolecode, '')) = lower(m.role_id)
				  )
			)
		)
	)`
}
