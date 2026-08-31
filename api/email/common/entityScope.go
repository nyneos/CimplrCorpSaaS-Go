package emailcommon

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ResolveEntityTreeIDs returns rootEntityID plus all descendant entity_ids from
// masterentitycash and masterEntity (same org-pool walk as session prevalidation).
func ResolveEntityTreeIDs(ctx context.Context, pool *pgxpool.Pool, rootEntityID string) ([]string, error) {
	if pool == nil || rootEntityID == "" {
		return nil, nil
	}
	seen := make(map[string]bool)
	add := func(id string) {
		id = strings.TrimSpace(id)
		if id != "" && !seen[id] {
			seen[id] = true
		}
	}
	add(rootEntityID)

	const cashQ = `
		WITH RECURSIVE descendants AS (
			SELECT entity_id, entity_name
			FROM masterentitycash
			WHERE entity_id = $1 AND (is_deleted = false OR is_deleted IS NULL)
			UNION ALL
			SELECT me.entity_id, me.entity_name
			FROM masterentitycash me
			INNER JOIN cashentityrelationships er ON me.entity_name = er.child_entity_name
			INNER JOIN descendants d ON er.parent_entity_name = d.entity_name
			WHERE (me.is_deleted = false OR me.is_deleted IS NULL)
			  AND (LOWER(er.status) = 'active' OR er.status IS NULL)
		)
		SELECT DISTINCT entity_id FROM descendants`

	rows1, err1 := pool.Query(ctx, cashQ, rootEntityID)
	if err1 == nil {
		defer rows1.Close()
		for rows1.Next() {
			var id string
			if rows1.Scan(&id) == nil {
				add(id)
			}
		}
	}

	const masterQ = `
		WITH RECURSIVE descendants AS (
			SELECT entity_id, entity_name
			FROM masterEntity
			WHERE entity_id = $1 AND (is_deleted = false OR is_deleted IS NULL)
			UNION ALL
			SELECT me.entity_id, me.entity_name
			FROM masterEntity me
			INNER JOIN entityRelationships er ON me.entity_id = er.child_entity_id
			INNER JOIN descendants d ON er.parent_entity_id = d.entity_id
			WHERE (me.is_deleted = false OR me.is_deleted IS NULL)
		)
		SELECT DISTINCT entity_id FROM descendants`

	rows2, err2 := pool.Query(ctx, masterQ, rootEntityID)
	if err2 == nil {
		defer rows2.Close()
		for rows2.Next() {
			var id string
			if rows2.Scan(&id) == nil {
				add(id)
			}
		}
	}

	if len(seen) == 0 {
		return []string{rootEntityID}, nil
	}
	out := make([]string, 0, len(seen))
	for id := range seen {
		out = append(out, id)
	}
	return out, nil
}

// MessageBusinessEntityFilterSQL limits rows to a business entity tree (self +
// descendants): inbox entity, message.entity_id, or rate requests under any entity
// in the tree.
func MessageBusinessEntityFilterSQL(argN int) string {
	return fmt.Sprintf(` AND (
		m.entity_id = ANY($%[1]d::text[])
		OR EXISTS (
			SELECT 1 FROM email_svc.inbox_config ic
			WHERE ic.inbox_id = m.inbox_id AND ic.entity_id = ANY($%[1]d::text[])
		)
		OR m.entity_id IN (
			SELECT n.rate_request_id::text
			FROM investment.fd_rate_negotiation n
			WHERE n.entity_id = ANY($%[1]d::text[]) AND COALESCE(n.is_deleted, false) = false
		)
	)`, argN)
}

// MessagePrevalidationEntityScopeSQL applies session entity_ids unless admin override.
func MessagePrevalidationEntityScopeSQL(argN int) string {
	return fmt.Sprintf(` AND (
		m.entity_id = ANY($%[1]d::text[])
		OR EXISTS (
			SELECT 1 FROM email_svc.inbox_config ic
			WHERE ic.inbox_id = m.inbox_id AND ic.entity_id = ANY($%[1]d::text[])
		)
		OR m.entity_id IN (
			SELECT n.rate_request_id::text
			FROM investment.fd_rate_negotiation n
			WHERE n.entity_id = ANY($%[1]d::text[]) AND COALESCE(n.is_deleted, false) = false
		)
	)`, argN)
}

// MessageUnlinkedOrRateRequestSQL — when rate_request_id is set, show messages
// already linked to that request or still unlinked (not linked to any request).
func MessageUnlinkedOrRateRequestSQL(argN int) string {
	return fmt.Sprintf(` AND (
		$%[1]d = ''
		OR m.entity_id = $%[1]d
		OR NOT EXISTS (
			SELECT 1 FROM investment.fd_rate_negotiation n
			WHERE n.rate_request_id::text = m.entity_id
		)
	)`, argN)
}

// ListMessageScopedToUserSQLAt is ListMessageScopedToUserSQL with dynamic placeholder indices.
func ListMessageScopedToUserSQLAt(adminN, userIDN, userEmailN int) string {
	return fmt.Sprintf(`
AND (
	$%[1]d::boolean
	OR EXISTS (
		SELECT 1
		FROM email_svc.inbox_config i
		WHERE i.is_deleted = false
		  AND i.processing_status = 'APPROVED'
		  AND i.is_active = true
		  AND (
		      i.owner_user_id = $%[2]d
		      OR ($%[3]d <> '' AND LOWER(i.mailbox_address) = LOWER($%[3]d))
		      OR EXISTS (
		          SELECT 1 FROM email_svc.inbox_members im
		          WHERE im.inbox_id = i.inbox_id AND im.user_id = $%[2]d
		      )
		  )
		  AND (
		      (m.inbox_id IS NOT NULL AND i.inbox_id = m.inbox_id)
		      OR LOWER(COALESCE(m.envelope_from, '')) = LOWER(i.mailbox_address)
		      OR LOWER(i.mailbox_address) = ANY (
		          SELECT LOWER(unnest(COALESCE(m.envelope_to, ARRAY[]::text[])))
		      )
		  )
	)
	OR (
		m.processing_status = 'MANUAL_UPLOAD'
		AND EXISTS (
			SELECT 1 FROM email_svc.processing_log upl_self
			WHERE upl_self.message_id = m.message_id
			  AND upl_self.step = 'UPLOAD_EML'
			  AND (
			      upl_self.detail->>'uploaded_by' = $%[2]d
			      OR ($%[3]d <> '' AND upl_self.detail->>'uploaded_by' = $%[3]d)
			  )
		)
	)
)`, adminN, userIDN, userEmailN, userEmailN)
}
