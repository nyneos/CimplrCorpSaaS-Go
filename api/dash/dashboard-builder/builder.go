package dashboardbuilder

// Dashboard Builder — user-scoped config persistence.
//
// Routes (registered in dash.go with the v2 master middleware chain):
//
//   POST /dash/builder/dashboard/save    — upsert a dashboard for the current user
//   GET  /dash/builder/dashboard/list    — list all dashboards for the current user
//   GET  /dash/builder/dashboard/get     — get one dashboard by ?id=<uuid>
//   POST /dash/builder/dashboard/delete  — delete a dashboard by id
//
// The full frontend DashboardConfig JSON is stored in the JSONB `config` column.
// Each user can have multiple named dashboards.

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/internal/logger"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ─── helpers ──────────────────────────────────────────────────────────────────

func userIDFromCtx(r *http.Request) string {
	ctx := r.Context()
	if v := ctx.Value("user_id"); v != nil {
		if s, ok := v.(string); ok {
			return strings.TrimSpace(s)
		}
	}
	return ""
}

// ─── SaveDashboard ─────────────────────────────────────────────────────────────
//
// POST /dash/builder/dashboard/save
//
// Body:
//
//	{
//	  "id":   "<uuid>",          // present → update; absent / empty → create
//	  "name": "My Dashboard",
//	  "config": { ...DashboardConfig... }
//	}
func SaveDashboard(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			respondBuilderError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		userID := userIDFromCtx(r)
		if userID == "" {
			respondBuilderError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}

		var body struct {
			ID     string          `json:"id"`
			Name   string          `json:"name"`
			Config json.RawMessage `json:"config"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			respondBuilderError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(body.Config) == 0 {
			respondBuilderError(w, http.StatusBadRequest, "config is required")
			return
		}
		name := strings.TrimSpace(body.Name)
		if name == "" {
			name = "My Dashboard"
		}

		ctx := r.Context()
		now := time.Now().UTC()

		// ── update existing ──────────────────────────────────────────────────
		if strings.TrimSpace(body.ID) != "" {
			tag, err := pool.Exec(ctx, `
				UPDATE public.user_dashboards
				   SET name       = $1,
				       config     = $2,
				       updated_at = $3
				 WHERE id = $4 AND user_id = $5
			`, name, body.Config, now, strings.TrimSpace(body.ID), userID)
			if err != nil {
				logger.LogError("dashboard-builder SaveDashboard update: %v", err)
				respondBuilderError(w, http.StatusInternalServerError, "failed to update dashboard")
				return
			}
			if tag.RowsAffected() == 0 {
				// Dashboard not found for this user — fall through to create
			} else {
				respondSuccess(w, http.StatusOK, "Dashboard saved successfully", map[string]any{
					"id":      body.ID,
					"updated": true,
				})
				return
			}
		}

		// ── create new ────────────────────────────────────────────────────────
		var newID string
		err := pool.QueryRow(ctx, `
			INSERT INTO public.user_dashboards (user_id, name, config, is_default, created_at, updated_at)
			VALUES ($1, $2, $3, FALSE, $4, $4)
			RETURNING id
		`, userID, name, body.Config, now).Scan(&newID)
		if err != nil {
			logger.LogError("dashboard-builder SaveDashboard insert: %v", err)
			respondBuilderError(w, http.StatusInternalServerError, "failed to save dashboard")
			return
		}

		respondSuccess(w, http.StatusOK, "Dashboard saved successfully", map[string]any{
			"id":      newID,
			"created": true,
		})
	}
}

// ─── ListDashboards ────────────────────────────────────────────────────────────
//
// POST /dash/builder/dashboard/list
//
// Returns summary rows (no config blob) for the current user, newest first.
func ListDashboards(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			respondBuilderError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		userID := userIDFromCtx(r)
		if userID == "" {
			respondBuilderError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}

		rows, err := pool.Query(r.Context(), `
			SELECT id, name, user_id, is_default, created_at, updated_at, FALSE AS is_shared
			  FROM public.user_dashboards
			 WHERE user_id = $1
			UNION ALL
			SELECT d.id, d.name, d.user_id, d.is_default, d.created_at, d.updated_at, TRUE AS is_shared
			  FROM public.user_dashboard_assignments a
			  JOIN public.user_dashboards d ON d.id = a.dashboard_id
			 WHERE a.assigned_user_id = $1
			 ORDER BY updated_at DESC
		`, userID)
		if err != nil {
			logger.LogError("dashboard-builder ListDashboards: %v", err)
			respondBuilderError(w, http.StatusInternalServerError, "failed to list dashboards")
			return
		}
		defer rows.Close()

		type summary struct {
			ID        string    `json:"id"`
			Name      string    `json:"name"`
			UserID    string    `json:"user_id"`
			IsDefault bool      `json:"is_default"`
			CreatedAt time.Time `json:"created_at"`
			UpdatedAt time.Time `json:"updated_at"`
			IsShared  bool      `json:"is_shared"`
		}
		var list []summary
		for rows.Next() {
			var s summary
			if err := rows.Scan(&s.ID, &s.Name, &s.UserID, &s.IsDefault, &s.CreatedAt, &s.UpdatedAt, &s.IsShared); err != nil {
				continue
			}
			list = append(list, s)
		}
		if list == nil {
			list = []summary{}
		}

		respondSuccess(w, http.StatusOK, "Dashboards fetched successfully", list)
	}
}

// ─── GetDashboardByID ──────────────────────────────────────────────────────────
//
// POST /dash/builder/dashboard/get
//
// Body: { "id": "<uuid>" }
// Returns the full config JSON for one dashboard owned by the current user.
func GetDashboardByID(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			respondBuilderError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		userID := userIDFromCtx(r)
		if userID == "" {
			respondBuilderError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}

		var body struct {
			ID string `json:"id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			respondBuilderError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		id := strings.TrimSpace(body.ID)
		if id == "" {
			respondBuilderError(w, http.StatusBadRequest, constants.ErrIDRequired)
			return
		}

		var (
			name        string
			isDefault   bool
			configRaw   []byte
			createdAt   time.Time
			updatedAt   time.Time
			ownerUserID string
		)
		err := pool.QueryRow(r.Context(), `
			SELECT d.name, d.is_default, d.config, d.created_at, d.updated_at, d.user_id
			  FROM public.user_dashboards d
			 WHERE d.id = $1
			   AND (
			     d.user_id = $2
			     OR EXISTS (
			       SELECT 1
			         FROM public.user_dashboard_assignments a
			        WHERE a.dashboard_id = d.id
			          AND a.assigned_user_id = $2
			     )
			   )
		`, id, userID).Scan(&name, &isDefault, &configRaw, &createdAt, &updatedAt, &ownerUserID)
		if err != nil {
			respondBuilderError(w, http.StatusNotFound, constants.ErrDashboardNotFound)
			return
		}

		respondSuccess(w, http.StatusOK, "Dashboard fetched successfully", map[string]any{
			"id":         id,
			"name":       name,
			"user_id":    ownerUserID,
			"is_default": isDefault,
			"config":     json.RawMessage(configRaw),
			"created_at": createdAt,
			"updated_at": updatedAt,
		})
	}
}

// ─── GetDashboardAssignees ─────────────────────────────────────────────────────
//
// POST /dash/builder/dashboard/assignees
//
// Body: { "id": "<uuid>" }
func GetDashboardAssignees(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			respondBuilderError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		userID := userIDFromCtx(r)
		if userID == "" {
			respondBuilderError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}

		var body struct {
			ID string `json:"id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			respondBuilderError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		id := strings.TrimSpace(body.ID)
		if id == "" {
			respondBuilderError(w, http.StatusBadRequest, constants.ErrIDRequired)
			return
		}

		var ownerID string
		err := pool.QueryRow(r.Context(), `
			SELECT user_id FROM public.user_dashboards WHERE id = $1
		`, id).Scan(&ownerID)
		if err != nil || ownerID != userID {
			respondBuilderError(w, http.StatusNotFound, constants.ErrDashboardNotFound)
			return
		}

		rows, err := pool.Query(r.Context(), `
			SELECT assigned_user_id
			  FROM public.user_dashboard_assignments
			 WHERE dashboard_id = $1
			 ORDER BY created_at ASC
		`, id)
		if err != nil {
			logger.LogError("dashboard-builder GetDashboardAssignees: %v", err)
			respondBuilderError(w, http.StatusInternalServerError, "failed to load assignees")
			return
		}
		defer rows.Close()

		assignees := []string{}
		for rows.Next() {
			var assignedUserID string
			if err := rows.Scan(&assignedUserID); err != nil {
				continue
			}
			assignees = append(assignees, assignedUserID)
		}

		respondSuccess(w, http.StatusOK, "Dashboard assignees fetched successfully", map[string]any{
			"user_ids": assignees,
		})
	}
}

// ─── AssignDashboard ───────────────────────────────────────────────────────────
//
// POST /dash/builder/dashboard/assign
//
// Body: { "id": "<uuid>", "user_ids": ["user-a", "user-b"] }
func AssignDashboard(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			respondBuilderError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		userID := userIDFromCtx(r)
		if userID == "" {
			respondBuilderError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}

		var body struct {
			ID      string   `json:"id"`
			UserIDs []string `json:"user_ids"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			respondBuilderError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		id := strings.TrimSpace(body.ID)
		if id == "" {
			respondBuilderError(w, http.StatusBadRequest, constants.ErrIDRequired)
			return
		}

		var ownerID string
		err := pool.QueryRow(r.Context(), `
			SELECT user_id FROM public.user_dashboards WHERE id = $1
		`, id).Scan(&ownerID)
		if err != nil || ownerID != userID {
			respondBuilderError(w, http.StatusNotFound, constants.ErrDashboardNotFound)
			return
		}

		assignees := make([]string, 0, len(body.UserIDs))
		seen := map[string]struct{}{}
		for _, raw := range body.UserIDs {
			assignedID := strings.TrimSpace(raw)
			if assignedID == "" || assignedID == userID {
				continue
			}
			if _, ok := seen[assignedID]; ok {
				continue
			}
			seen[assignedID] = struct{}{}
			assignees = append(assignees, assignedID)
		}

		ctx := r.Context()
		tx, err := pool.Begin(ctx)
		if err != nil {
			respondBuilderError(w, http.StatusInternalServerError, constants.ErrFailedToAssignDashboard)
			return
		}
		defer tx.Rollback(ctx)

		if _, err := tx.Exec(ctx, `
			DELETE FROM public.user_dashboard_assignments
			 WHERE dashboard_id = $1 AND owner_user_id = $2
		`, id, userID); err != nil {
			logger.LogError("dashboard-builder AssignDashboard delete: %v", err)
			respondBuilderError(w, http.StatusInternalServerError, constants.ErrFailedToAssignDashboard)
			return
		}

		for _, assignedID := range assignees {
			if _, err := tx.Exec(ctx, `
				INSERT INTO public.user_dashboard_assignments
				        (dashboard_id, owner_user_id, assigned_user_id)
				VALUES ($1, $2, $3)
				ON CONFLICT (dashboard_id, assigned_user_id) DO NOTHING
			`, id, userID, assignedID); err != nil {
				logger.LogError("dashboard-builder AssignDashboard insert: %v", err)
				respondBuilderError(w, http.StatusInternalServerError, constants.ErrFailedToAssignDashboard)
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			respondBuilderError(w, http.StatusInternalServerError, constants.ErrFailedToAssignDashboard)
			return
		}

		respondSuccess(w, http.StatusOK, "Dashboard assigned successfully", map[string]any{
			"assigned_count": len(assignees),
			"user_ids":       assignees,
		})
	}
}

// ─── DeleteDashboard ───────────────────────────────────────────────────────────
//
// POST /dash/builder/dashboard/delete
//
// Body: { "id": "<uuid>" }
func DeleteDashboard(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			respondBuilderError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		userID := userIDFromCtx(r)
		if userID == "" {
			respondBuilderError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}

		var body struct {
			ID string `json:"id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			respondBuilderError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if strings.TrimSpace(body.ID) == "" {
			respondBuilderError(w, http.StatusBadRequest, constants.ErrIDRequired)
			return
		}

		tag, err := pool.Exec(r.Context(), `
			DELETE FROM public.user_dashboards
			 WHERE id = $1 AND user_id = $2
		`, strings.TrimSpace(body.ID), userID)
		if err != nil {
			logger.LogError("dashboard-builder DeleteDashboard: %v", err)
			respondBuilderError(w, http.StatusInternalServerError, "failed to delete dashboard")
			return
		}
		if tag.RowsAffected() == 0 {
			respondBuilderError(w, http.StatusNotFound, constants.ErrDashboardNotFound)
			return
		}

		respondSuccess(w, http.StatusOK, "Dashboard deleted successfully", map[string]any{
			"deleted": true,
		})
	}
}
