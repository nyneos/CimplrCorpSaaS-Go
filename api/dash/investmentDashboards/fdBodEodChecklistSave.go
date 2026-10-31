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
	"CimplrCorpSaas/api/errtext"
	"CimplrCorpSaas/api/governance"
	middlewares "CimplrCorpSaas/api/middlewares"

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

func resolveActor(ctx context.Context) (userID, name, email, role string) {
	userID = strings.TrimSpace(middlewares.GetUserIDFromContext(ctx))
	if s := api.GetSessionFromCtx(ctx); s != nil {
		if userID == "" {
			userID = strings.TrimSpace(s.UserID)
		}
		name = s.Name
		email = s.Email
		role = s.Role
		if role == "" {
			role = s.RoleCode
		}
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
			api.RespondEnvelopeError(w, http.StatusBadRequest, errtext.Detail(w, err), "VALIDATION_ERROR")
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
		actorID, actorName, actorEmail, actorRole := resolveActor(ctx)
		if actorID == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		entityID, scopeMsg := resolveFDDashboardSingleEntity(ctx, req.EntityID)
		if scopeMsg != "" {
			api.RespondWithError(w, http.StatusForbidden, scopeMsg)
			return
		}
		req.EntityID = entityID

		tx, err := pool.Begin(ctx)
		if err != nil {
			api.RespondEnvelopeError(w, http.StatusInternalServerError, errtext.Detail(w, err), "")
			return
		}
		defer tx.Rollback(ctx)

		itemIDs := make([]string, 0, len(req.Items))
		for _, it := range req.Items {
			if id := strings.TrimSpace(it.ID); id != "" {
				itemIDs = append(itemIDs, id)
			}
		}
		if len(itemIDs) > 0 {
			var itemClash bool
			err = tx.QueryRow(ctx, `
				SELECT EXISTS (
					SELECT 1
					FROM investment.fd_eod_checklist_item_status
					WHERE entity_id = $1
					  AND as_of_date = $2::date
					  AND item_id = ANY($3::text[])
					  AND COALESCE(is_deleted, false) = false
					  AND BTRIM(COALESCE(mode, '')) <> ''
					  AND UPPER(BTRIM(mode)) <> UPPER(BTRIM($4))
				)`,
				req.EntityID, asOf, itemIDs, req.Mode,
			).Scan(&itemClash)
			if err != nil {
				api.LogErrorForResponse(w, "[BodEodChecklist] item mode check failed: %v", err)
				api.RespondWithError(w, http.StatusInternalServerError, "checklist save could not be verified")
				return
			}
			if itemClash {
				api.RespondWithError(w, http.StatusBadRequest, "cannot save BOD and EOD checklist items for the same entity and date")
				return
			}
		}
		if req.SignedOff {
			var signoffClash bool
			err = tx.QueryRow(ctx, `
				SELECT EXISTS (
					SELECT 1
					FROM investment.fd_eod_checklist_signoff
					WHERE entity_id = $1
					  AND as_of_date = $2::date
					  AND COALESCE(is_deleted, false) = false
					  AND BTRIM(COALESCE(mode, '')) <> ''
					  AND UPPER(BTRIM(mode)) <> UPPER(BTRIM($3))
				)`,
				req.EntityID, asOf, req.Mode,
			).Scan(&signoffClash)
			if err != nil {
				api.LogErrorForResponse(w, "[BodEodChecklist] signoff mode check failed: %v", err)
				api.RespondWithError(w, http.StatusInternalServerError, "checklist save could not be verified")
				return
			}
			if signoffClash {
				api.RespondWithError(w, http.StatusBadRequest, "cannot sign off both BOD and EOD for the same entity and date")
				return
			}
		}

		makerByItem := map[string]string{}
		if len(itemIDs) > 0 {
			makerRows, makerErr := tx.Query(ctx, `
				SELECT item_id, COALESCE(updated_by, '')
				FROM investment.fd_eod_checklist_item_status
				WHERE entity_id = $1
				  AND as_of_date = $2::date
				  AND item_id = ANY($3::text[])
				  AND COALESCE(is_deleted, false) = false
			`, req.EntityID, asOf, itemIDs)
			if makerErr != nil {
				api.RespondEnvelopeError(w, http.StatusInternalServerError, errtext.Detail(w, makerErr), "")
				return
			}
			for makerRows.Next() {
				var itemID, maker string
				if scanErr := makerRows.Scan(&itemID, &maker); scanErr != nil {
					makerRows.Close()
					api.RespondEnvelopeError(w, http.StatusInternalServerError, errtext.Detail(w, scanErr), "")
					return
				}
				makerByItem[itemID] = maker
			}
			if rowsErr := makerRows.Err(); rowsErr != nil {
				makerRows.Close()
				api.RespondEnvelopeError(w, http.StatusInternalServerError, errtext.Detail(w, rowsErr), "")
				return
			}
			makerRows.Close()
		}
		for _, it := range req.Items {
			if !it.Done || strings.TrimSpace(it.ID) == "" {
				continue
			}
			maker := strings.TrimSpace(makerByItem[strings.TrimSpace(it.ID)])
			if maker == "" || governance.IsSelfApprovalCtx(ctx, maker) {
				api.RespondEnvelopeError(w, http.StatusForbidden, "you cannot check a checklist row you prepared", "SELF_APPROVAL")
				return
			}
		}

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
				it.Done, isChecklistItemCritical(it.ID), nullIfBlank(it.Comment),
				actorID, nullIfBlank(actorName), nullIfBlank(actorEmail), nullIfBlank(actorRole),
			).Scan(&statusID)
			if err != nil {
				api.RespondEnvelopeError(w, http.StatusInternalServerError, errtext.Detail(w, err), "")
				return
			}
			if _, err = tx.Exec(ctx, `
				INSERT INTO investment.fd_eod_checklist_item_audit (
					status_id, entity_id, as_of_date, item_id, item_label,
					is_done, is_critical, comment, action,
					actor_user_id, actor_name, actor_email, actor_role, action_at
				) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,NOW())`,
				statusID, nullIfBlank(req.EntityID), asOf, it.ID, nullIfBlank(it.Label),
				it.Done, isChecklistItemCritical(it.ID), nullIfBlank(it.Comment), pickAction(it.Done),
				actorID, nullIfBlank(actorName), nullIfBlank(actorEmail), nullIfBlank(actorRole),
			); err != nil {
				api.LogErrorForResponse(w, "[BodEodChecklist] audit insert failed item=%s: %v", it.ID, err)
				api.RespondWithError(w, http.StatusInternalServerError, "checklist audit could not be saved")
				return
			}
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
			overrides := map[string]bool{}
			for _, it := range req.Items {
				if id := strings.TrimSpace(it.ID); id != "" {
					overrides[id] = it.Done
				}
			}
			var criticalPending int
			var critErr error
			if strings.EqualFold(strings.TrimSpace(req.Mode), "EOD") {
				criticalPending, critErr = countCriticalPendingLive(ctx, pool, req.EntityID, asOf, overrides)
			} else {
				_, _, criticalPending = summariseItems(req.Items)
			}
			if critErr != nil {
				api.LogErrorForResponse(w, "[BodEodChecklist] critical recompute failed: %v", critErr)
				api.RespondWithError(w, http.StatusInternalServerError, "checklist criticals could not be verified")
				return
			}
			if criticalPending > 0 {
				api.RespondWithError(w, http.StatusBadRequest,
					fmt.Sprintf("cannot sign off: %d critical item(s) still pending", criticalPending))
				return
			}
			completed, total, _ := summariseItems(req.Items)
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
				api.RespondEnvelopeError(w, http.StatusInternalServerError, errtext.Detail(w, err), "")
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondEnvelopeError(w, http.StatusInternalServerError, errtext.Detail(w, err), "")
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

// Server-authoritative critical checklist IDs for EOD (must match fdBodEodDashboard).
var eodChecklistCriticalIDs = map[string]bool{
	"C1": true,
	"C2": true,
	"C3": true,
	"C6": true,
}

func isChecklistItemCritical(itemID string) bool {
	return eodChecklistCriticalIDs[strings.TrimSpace(itemID)]
}

// countCriticalPendingLive recomputes EOD critical Done from live data + persisted/overrides.
// Client Critical/Done flags are not trusted for sign-off.
func countCriticalPendingLive(ctx context.Context, pool *pgxpool.Pool, entityID, asOf string, overrides map[string]bool) (int, error) {
	entityFilter := strings.TrimSpace(entityID)
	if entityFilter == "" {
		return 0, fmt.Errorf("entity required")
	}

	var unmatchedCount, openVariance, failedPostings, pendingMaturities int64
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM investment.fd_interest_receipt
		WHERE is_deleted=false AND DATE(created_at)=$1::date
		AND COALESCE(reconciliation_status,'UNMATCHED') IN ('UNMATCHED','PENDING','')
		AND (entity_id = ANY(string_to_array($2, ',')))`, asOf, entityFilter).Scan(&unmatchedCount); err != nil {
		return 0, err
	}
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM investment.fd_confirmation
		WHERE is_deleted=false AND variance_flag=true
		AND COALESCE(variance_action,'PENDING') = 'PENDING'
		AND EXISTS(
		    SELECT 1 FROM investment.fd_booking_request b WHERE b.booking_id=fd_confirmation.booking_id AND (b.entity_id = ANY(string_to_array($1, ',')))
		)`, entityFilter).Scan(&openVariance); err != nil {
		return 0, err
	}
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM investment.fd_cashflow_schedule cf
		JOIN investment.fd_master m ON m.fd_id=cf.fd_id AND m.is_deleted=false
		WHERE cf.is_deleted=false AND cf.posting_status='FAILED'
		AND (m.entity_id = ANY(string_to_array($1, ',')))`, entityFilter).Scan(&failedPostings); err != nil {
		return 0, err
	}
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM investment.fd_master m
		WHERE m.is_deleted=false AND m.maturity_date=$1::date
		AND m.fd_status IN ('ACTIVE','MATURED')
		AND NOT (`+sqlAnyClosureProcessed+`)
		AND (m.entity_id = ANY(string_to_array($2, ',')))`, asOf, entityFilter).Scan(&pendingMaturities); err != nil {
		return 0, err
	}

	liveDone := map[string]bool{
		"C1": unmatchedCount == 0,
		"C2": openVariance == 0,
		"C3": failedPostings == 0,
		"C6": pendingMaturities == 0,
	}

	// Overlay persisted operator marks from DB.
	rows, err := pool.Query(ctx, `
		SELECT item_id, is_done
		FROM investment.fd_eod_checklist_item_status
		WHERE entity_id = $1 AND as_of_date = $2::date
		  AND COALESCE(is_deleted, false) = false
		  AND item_id = ANY($3::text[])`,
		entityID, asOf, []string{"C1", "C2", "C3", "C6"})
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var done bool
		if err := rows.Scan(&id, &done); err != nil {
			return 0, err
		}
		if done {
			liveDone[id] = true
		}
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	for id, done := range overrides {
		if isChecklistItemCritical(id) && done {
			liveDone[id] = true
		}
	}

	pending := 0
	for id := range eodChecklistCriticalIDs {
		if !liveDone[id] {
			pending++
		}
	}
	return pending, nil
}


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
		critical := isChecklistItemCritical(it.ID)
		if it.Done {
			completed++
		} else if critical {
			criticalPending++
		}
	}
	return
}
