package api

import (
	"context"
	"database/sql"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"
)

var approvedEntitySQL = `
SELECT me.entity_id
FROM masterentitycash me
JOIN LATERAL (
  SELECT processing_status
  FROM auditactionentity
  WHERE entity_id = me.entity_id
  ORDER BY requested_at DESC
  LIMIT 1
) a ON TRUE
WHERE me.entity_name = $1
  AND COALESCE(me.is_deleted, false) = false
  AND (a.processing_status = 'APPROVED' OR a.processing_status = 'Approved')
LIMIT 1
`

// LookupApprovedEntityID returns the entity_id for a given entity_name when the latest audit shows APPROVED.
func LookupApprovedEntityID(ctx context.Context, db *pgxpool.Pool, entityName string) (string, error) {
	var id string
	log.Printf("[DEBUG entity_helpers] LookupApprovedEntityID input entityName=%q", entityName)
	err := db.QueryRow(ctx, approvedEntitySQL, entityName).Scan(&id)
	if err != nil {
		log.Printf("[DEBUG entity_helpers] LookupApprovedEntityID error: %v", err)
		return "", err
	}
	log.Printf("[DEBUG entity_helpers] LookupApprovedEntityID result id=%s for name=%q", id, entityName)
	return id, nil
}

// LookupApprovedEntityIDSQLDB adapter for codepaths using *sql.DB
func LookupApprovedEntityIDSQLDB(ctx context.Context, db *sql.DB, entityName string) (string, error) {
	var id string
	log.Printf("[DEBUG entity_helpers] LookupApprovedEntityIDSQLDB input entityName=%q", entityName)
	err := db.QueryRowContext(ctx, approvedEntitySQL, entityName).Scan(&id)
	if err != nil {
		log.Printf("[DEBUG entity_helpers] LookupApprovedEntityIDSQLDB error: %v", err)
		return "", err
	}
	log.Printf("[DEBUG entity_helpers] LookupApprovedEntityIDSQLDB result id=%s for name=%q", id, entityName)
	return id, nil
}
