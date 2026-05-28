// Package investmentdashboards — FD BOD/EOD Checklist persistence APIs
//
// POST /dash/investment/fd/bod-eod-checklist/save
//
//	Persists the user-toggled state for one or many EOD checklist items.
//	Items already auto-derived by the dashboard (e.g. unmatched-receipts count)
//	can be explicitly overridden by the operator with a sign-off + comment.
//
// POST /dash/investment/fd/bod-eod-checklist/signoff
//
//	Marks the entire EOD as signed off for an entity/date — recording who
//	signed it off, when, the role, and any remaining critical items.
//
// Tables expected (created via the SQL block shipped with this feature):
//
//	investment.fd_eod_checklist_item_status — per-item user override
//	investment.fd_eod_checklist_signoff     — header-level sign-off audit
package investmentdashboards

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ─── shared types ─────────────────────────────────────────────────────────────

type checklistItemPayload struct {
	ID       string `json:"id"`
	Label    string `json:"label"`
	Done     bool   `json:"done"`
	Critical bool   `json:"critical"`
	Comment  string `json:"comment"`
	Owner    string `json:"owner_role"`
}

type checklistSavePayload struct {
	UserID      string                 `json:"user_id"`
	EntityID    string                 `json:"entity_id"`
	Currency    string                 `json:"currency"`
	AsOfDate    string                 `json:"as_of_date"` // YYYY-MM-DD; defaults to today
	Mode        string                 `json:"mode"`       // BOD | EOD (defaults EOD)
	Items       []checklistItemPayload `json:"items"`
	SignedOff   bool                   `json:"signed_off"`
	SignOffNote string                 `json:"signoff_note"`
}

// ─── helpers ──────────────────────────────────────────────────────────────────

func resolveAsOfDate(raw string) string {
	if t, err := time.Parse(constants.DateFormat, strings.TrimSpace(raw)); err == nil {
		return t.Format(constants.DateFormat)
	}
	return time.Now().UTC().Format(constants.DateFormat)
}

func resolveActor(ctx context.Context, fallbackUserID string) (userID, name, email, role string) {
	if s := api.GetSessionFromCtx(ctx); s != nil {
		userID = s.UserID
		name = s.Name
		email = s.Email
		role = s.Role
		if role == "" {
			role = s.RoleCode
		}
	}
	if userID == "" {
		userID = fallbackUserID
	}
	return
}

// ─── POST /dash/investment/fd/bod-eod-checklist/save ──────────────────────────
//
// Upserts (entity_id, as_of_date, item_id) → (is_done, comment, owner_role) and
// records an audit row.

func SaveBodEodChecklistItems(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		var req checklistSavePayload
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
			return
		}
		if len(req.Items) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "items[] required")
			return
		}
		if req.Mode == "" {
			req.Mode = "EOD"
		}
		asOf := resolveAsOfDate(req.AsOfDate)
		ctx := r.Context()
		actorID, actorName, actorEmail, actorRole := resolveActor(ctx, req.UserID)
		if actorID == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		tx, err := pool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "begin tx: "+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		saved := make([]map[string]interface{}, 0, len(req.Items))
		for _, it := range req.Items {
			if strings.TrimSpace(it.ID) == "" {
				continue
			}
			var statusID string
			err := tx.QueryRow(ctx, `
				INSERT INTO investment.fd_eod_checklist_item_status (
					entity_id, as_of_date, mode, item_id, item_label, owner_role,
					is_done, is_critical, comment,
					updated_by, updated_by_name, updated_by_email, updated_by_role,
					updated_at, is_deleted
				) VALUES (
					$1,$2,$3,$4,$5,$6,
					$7,$8,$9,
					$10,$11,$12,$13,
					NOW(), false
				)
				ON CONFLICT (entity_id, as_of_date, item_id) WHERE COALESCE(is_deleted,false)=false
				DO UPDATE SET
					is_done          = EXCLUDED.is_done,
					comment          = EXCLUDED.comment,
					owner_role       = EXCLUDED.owner_role,
					updated_by       = EXCLUDED.updated_by,
					updated_by_name  = EXCLUDED.updated_by_name,
					updated_by_email = EXCLUDED.updated_by_email,
					updated_by_role  = EXCLUDED.updated_by_role,
					updated_at       = NOW()
				RETURNING status_id`,
				nullIfBlank(req.EntityID), asOf, req.Mode, it.ID, nullIfBlank(it.Label), nullIfBlank(it.Owner),
				it.Done, it.Critical, nullIfBlank(it.Comment),
				actorID, nullIfBlank(actorName), nullIfBlank(actorEmail), nullIfBlank(actorRole),
			).Scan(&statusID)
			if err != nil {
				api.LogError("[BodEodChecklist] upsert item %s failed: %v", it.ID, err)
				api.RespondWithError(w, http.StatusInternalServerError, "save failed: "+err.Error())
				return
			}
			// audit
			_, _ = tx.Exec(ctx, `
				INSERT INTO investment.fd_eod_checklist_item_audit (
					status_id, entity_id, as_of_date, item_id, item_label,
					is_done, is_critical, comment, action,
					actor_user_id, actor_name, actor_email, actor_role, action_at
				) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,NOW())`,
				statusID, nullIfBlank(req.EntityID), asOf, it.ID, nullIfBlank(it.Label),
				it.Done, it.Critical, nullIfBlank(it.Comment), pickAction(it.Done),
				actorID, nullIfBlank(actorName), nullIfBlank(actorEmail), nullIfBlank(actorRole),
			)
			saved = append(saved, map[string]interface{}{
				"status_id":  statusID,
				"item_id":    it.ID,
				"is_done":    it.Done,
				"owner_role": it.Owner,
			})
		}

		// Optionally finalise the EOD sign-off in the same call.
		var signOffID string
		if req.SignedOff {
			completed, total, criticalPending := summariseItems(req.Items)
			if criticalPending > 0 {
				api.RespondWithError(w, http.StatusBadRequest,
					fmt.Sprintf("cannot sign off: %d critical item(s) still pending", criticalPending))
				return
			}
			err = tx.QueryRow(ctx, `
				INSERT INTO investment.fd_eod_checklist_signoff (
					entity_id, as_of_date, mode, total_items, completed_items, critical_pending,
					note, signed_off_by, signed_off_by_name, signed_off_by_email, signed_off_by_role,
					signed_off_at, is_deleted
				) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,NOW(),false)
				ON CONFLICT (entity_id, as_of_date) WHERE COALESCE(is_deleted,false)=false
				DO UPDATE SET
					total_items         = EXCLUDED.total_items,
					completed_items     = EXCLUDED.completed_items,
					critical_pending    = EXCLUDED.critical_pending,
					note                = EXCLUDED.note,
					signed_off_by       = EXCLUDED.signed_off_by,
					signed_off_by_name  = EXCLUDED.signed_off_by_name,
					signed_off_by_email = EXCLUDED.signed_off_by_email,
					signed_off_by_role  = EXCLUDED.signed_off_by_role,
					signed_off_at       = NOW()
				RETURNING signoff_id`,
				nullIfBlank(req.EntityID), asOf, req.Mode, total, completed, criticalPending,
				nullIfBlank(req.SignOffNote), actorID, nullIfBlank(actorName), nullIfBlank(actorEmail), nullIfBlank(actorRole),
			).Scan(&signOffID)
			if err != nil {
				api.LogError("[BodEodChecklist] signoff insert failed: %v", err)
				api.RespondWithError(w, http.StatusInternalServerError, "signoff failed: "+err.Error())
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "commit failed: "+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"saved":         saved,
			"signoff_id":    signOffID,
			"signed_off":    req.SignedOff,
			"as_of_date":    asOf,
			"entity_id":     req.EntityID,
			"actor_user_id": actorID,
			"actor_role":    actorRole,
		})
	}
}

// ─── helpers (local to this file) ─────────────────────────────────────────────

func nullIfBlank(s string) interface{} {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return s
}

func pickAction(done bool) string {
	if done {
		return "MARKED_DONE"
	}
	return "MARKED_PENDING"
}

func summariseItems(items []checklistItemPayload) (completed, total, criticalPending int) {
	for _, it := range items {
		total++
		if it.Done {
			completed++
		} else if it.Critical {
			criticalPending++
		}
	}
	return
}
