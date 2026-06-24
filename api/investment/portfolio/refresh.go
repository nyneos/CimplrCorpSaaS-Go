package portfolio

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/internal/ctxutil"

	// "context"
	"encoding/json"
	"net/http"
	"strings"

	// "time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// RefreshRequest optionally scopes refresh to an entity
type RefreshRequest struct {
	EntityName *string `json:"entity_name,omitempty"`
}

// RefreshPortfolioSnapshots rebuilds portfolio snapshots. If `entity_name` is provided,
// it refreshes only for that entity. Otherwise it refreshes all snapshots (may be slow).
func RefreshPortfolioSnapshots(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req RefreshRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil && r.ContentLength > 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}

		ctx := r.Context()
		scope := ctxutil.FromContext(ctx)
		var entityNames []string
		if req.EntityName != nil && strings.TrimSpace(*req.EntityName) != "" {
			entityName := strings.TrimSpace(*req.EntityName)
			if !scope.HasEntityNameAccess(entityName) && !scope.HasEntityAccess(entityName) {
				api.RespondWithError(w, http.StatusForbidden, "entity_name is not within your authorized access scope")
				return
			}
			entityNames = []string{entityName}
		} else if len(scope.EntityNames) > 0 {
			entityNames = scope.EntityNames
		}

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxBeginFailedCapitalized+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		batchID := uuid.New().String()

		// Delete existing snapshots for the scoped entity set. Only admin/no-scope contexts refresh all.
		if len(entityNames) > 0 {
			if _, err := tx.Exec(ctx, `DELETE FROM investment.portfolio_snapshot WHERE entity_name = ANY($1::text[])`, entityNames); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "delete snapshots failed: "+err.Error())
				return
			}
		} else {
			// Careful: this will remove all snapshots. Keep it explicit.
			if _, err := tx.Exec(ctx, `DELETE FROM investment.portfolio_snapshot`); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "delete snapshots failed: "+err.Error())
				return
			}
		}

		// Execute insert with entity filter (nil => NULL)
		var entityArg interface{}
		if len(entityNames) > 0 {
			entityArg = entityNames
		} else {
			entityArg = nil
		}

		if _, err := tx.Exec(ctx, PortfolioSnapshotInsertSQL, entityArg, batchID); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "insert snapshots failed: "+err.Error())
			return
		}

		// Commit
		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailedCapitalized+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "snapshots refreshed", map[string]any{"batch_id": batchID, "entity_names": entityNames})
	}
}
