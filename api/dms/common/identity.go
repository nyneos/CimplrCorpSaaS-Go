package common

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5"

	"CimplrCorpSaas/api"
	middlewares "CimplrCorpSaas/api/middlewares"
)

// RequestActor resolves the maker/checker actor for an audit row: session user first,
// then an explicit actor_id/checker_id from the request body, falling back to "system".
func RequestActor(r *http.Request, reqActorID string) string {
	actor := strings.TrimSpace(middlewares.GetUserIDFromContext(r.Context()))
	if actor == "" {
		actor = "system"
	}
	return actor
}

// RequestIP resolves the client IP for audit rows from the request context.
func RequestIP(r *http.Request) string {
	return api.ClientIPFromContext(r.Context())
}

func AuditMaker(ctx context.Context, tx pgx.Tx, auditTable, auditID string) (string, error) {
	var maker string
	err := tx.QueryRow(ctx, fmt.Sprintf(`SELECT COALESCE(requested_by, '') FROM %s WHERE audit_id = $1::uuid`, auditTable), auditID).Scan(&maker)
	return maker, err
}

func RunInSavepoint(ctx context.Context, tx pgx.Tx, fn func(pgx.Tx) error) error {
	sp, err := tx.Begin(ctx)
	if err != nil {
		return err
	}
	if err := fn(sp); err != nil {
		_ = sp.Rollback(ctx)
		return err
	}
	return sp.Commit(ctx)
}
