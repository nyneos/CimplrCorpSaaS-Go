package dashboardbuilder

// Dashboard Builder — user-scoped config persistence.
//
// Routes (registered in dash.go with PreValidationMiddleware):
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

	"CimplrCorpSaas/api"
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
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		userID := userIDFromCtx(r)
		if userID == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		var body struct {
			ID     string          `json:"id"`
			Name   string          `json:"name"`
			Config json.RawMessage `json:"config"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(body.Config) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "config is required")
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
				api.RespondWithError(w, http.StatusInternalServerError, "failed to update dashboard")
				return
			}
			if tag.RowsAffected() == 0 {
				// Dashboard not found for this user — fall through to create
			} else {
				api.RespondWithPayload(w, true, "", map[string]any{"id": body.ID, "updated": true})
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
			api.RespondWithError(w, http.StatusInternalServerError, "failed to save dashboard")
			return
		}

		api.RespondWithPayload(w, true, "", map[string]any{"id": newID, "created": true})
	}
}

// ─── ListDashboards ────────────────────────────────────────────────────────────
//
// GET /dash/builder/dashboard/list
//
// Returns summary rows (no config blob) for the current user, newest first.
func ListDashboards(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		userID := userIDFromCtx(r)
		if userID == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		rows, err := pool.Query(r.Context(), `
			SELECT id, name, is_default, created_at, updated_at
			  FROM public.user_dashboards
			 WHERE user_id = $1
			 ORDER BY updated_at DESC
		`, userID)
		if err != nil {
			logger.LogError("dashboard-builder ListDashboards: %v", err)
			api.RespondWithError(w, http.StatusInternalServerError, "failed to list dashboards")
			return
		}
		defer rows.Close()

		type summary struct {
			ID         string    `json:"id"`
			Name       string    `json:"name"`
			IsDefault  bool      `json:"is_default"`
			CreatedAt  time.Time `json:"created_at"`
			UpdatedAt  time.Time `json:"updated_at"`
		}
		var list []summary
		for rows.Next() {
			var s summary
			if err := rows.Scan(&s.ID, &s.Name, &s.IsDefault, &s.CreatedAt, &s.UpdatedAt); err != nil {
				continue
			}
			list = append(list, s)
		}
		if list == nil {
			list = []summary{}
		}

		api.RespondWithPayload(w, true, "", list)
	}
}

// ─── GetDashboardByID ──────────────────────────────────────────────────────────
//
// GET /dash/builder/dashboard/get?id=<uuid>
//
// Returns the full config JSON for one dashboard owned by the current user.
func GetDashboardByID(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		userID := userIDFromCtx(r)
		if userID == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		id := strings.TrimSpace(r.URL.Query().Get("id"))
		if id == "" {
			api.RespondWithError(w, http.StatusBadRequest, "id query param is required")
			return
		}

		var (
			name      string
			isDefault bool
			configRaw []byte
			createdAt time.Time
			updatedAt time.Time
		)
		err := pool.QueryRow(r.Context(), `
			SELECT name, is_default, config, created_at, updated_at
			  FROM public.user_dashboards
			 WHERE id = $1 AND user_id = $2
		`, id, userID).Scan(&name, &isDefault, &configRaw, &createdAt, &updatedAt)
		if err != nil {
			api.RespondWithError(w, http.StatusNotFound, "dashboard not found")
			return
		}

		api.RespondWithPayload(w, true, "", map[string]any{
			"id":         id,
			"name":       name,
			"is_default": isDefault,
			"config":     json.RawMessage(configRaw),
			"created_at": createdAt,
			"updated_at": updatedAt,
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
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		userID := userIDFromCtx(r)
		if userID == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		var body struct {
			ID string `json:"id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if strings.TrimSpace(body.ID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "id is required")
			return
		}

		tag, err := pool.Exec(r.Context(), `
			DELETE FROM public.user_dashboards
			 WHERE id = $1 AND user_id = $2
		`, strings.TrimSpace(body.ID), userID)
		if err != nil {
			logger.LogError("dashboard-builder DeleteDashboard: %v", err)
			api.RespondWithError(w, http.StatusInternalServerError, "failed to delete dashboard")
			return
		}
		if tag.RowsAffected() == 0 {
			api.RespondWithError(w, http.StatusNotFound, "dashboard not found")
			return
		}

		api.RespondWithPayload(w, true, "", map[string]any{"deleted": true})
	}
}
