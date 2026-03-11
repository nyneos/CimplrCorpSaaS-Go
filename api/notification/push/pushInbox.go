package push

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"
	"strings"

	"CimplrCorpSaas/internal/dashboard"

	"github.com/jackc/pgx/v5/pgxpool"
)

// RegisterPushInboxRoutes wires the inbox REST handlers into mux.
func RegisterPushInboxRoutes(mux *http.ServeMux, pool *pgxpool.Pool) {
	// Primary endpoints
	mux.HandleFunc("/notification/inbox/list", handleGetInbox(pool))
	mux.HandleFunc("/notification/inbox/count", handleGetCount(pool))
	mux.HandleFunc("/notification/inbox/mark-read", handleMarkRead(pool))
	mux.HandleFunc("/notification/inbox/mark-all-read", handleMarkAllRead(pool))
	mux.HandleFunc("/notification/inbox/delete", handleDelete(pool))

	// Convenience alias: POST /notification/inbox → same as /notification/inbox/list
	// Supports frontends that call the bare /notification/inbox path.
	mux.HandleFunc("/notification/inbox", handleGetInbox(pool))
}

// userIDFromCtx resolves the calling user's ID from context or query param.
func userIDFromCtx(r *http.Request) string {
	for _, key := range []interface{}{"userID", "user_id", "UserID"} {
		if v, ok := r.Context().Value(key).(string); ok && v != "" {
			return v
		}
	}
	return r.URL.Query().Get("user_id")
}

// writeJSON is the low-level JSON writer — always sets Content-Type.
func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

// writeOK writes a standard success envelope: {"success":true, "data":{...}}
func writeOK(w http.ResponseWriter, data interface{}) {
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"row":     data,
	})
}

// writeErr writes a standard error envelope: {"success":false, "error":"..."}
func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]interface{}{
		"success": false,
		"error":   msg,
	})
}

// pushCountSSE queries the unread count and pushes it to the user's SSE stream.
func pushCountSSE(ctx context.Context, pool *pgxpool.Pool, userID string) {
	var count int
	err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM notification_svc.in_app_notification
		 WHERE recipient_user_id = $1 AND is_read = false AND is_deleted = false`,
		userID,
	).Scan(&count)
	if err != nil {
		fmt.Printf("[PUSH] pushCountSSE query error for %s: %v\n", userID, err)
		return
	}
	msg, _ := json.Marshal(map[string]interface{}{
		"type":   "notification_count",
		"unread": count,
	})
	dashboard.SendToUser(userID, msg)
}

// inboxItem is the JSON shape returned by GET /notification/inbox/list.
type inboxItem struct {
	ID            string     `json:"id"`
	OutboxID      string     `json:"outbox_id"`
	CorrelationID string     `json:"correlation_id"`
	EventID       string     `json:"event_id"`
	Subject       string     `json:"subject"`
	Body          string     `json:"body"`
	PriorityLevel int        `json:"priority_level"`
	IsRead        bool       `json:"is_read"`
	ReadAt        *time.Time `json:"read_at,omitempty"`
	CreatedAt     time.Time  `json:"created_at"`
	// Added metadata
	ModuleCode    string     `json:"module_code"`
	SubModuleCode string     `json:"sub_module_code"`
	EventCode     string     `json:"event_code"`
	EventName     string     `json:"event_name"`
	SenderID      string     `json:"sender_id"`
	SenderName    string     `json:"sender_name"`
	SenderEmail   string     `json:"sender_email"`
}

// handleGetInbox — POST /notification/inbox/list
func handleGetInbox(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		// Parse optional body fields (user_id, limit, offset)
		var req struct {
			UserID string `json:"user_id"`
			Limit  int    `json:"limit"`
			Offset int    `json:"offset"`
		}
		// Body is optional — decode if present, ignore decode errors
		json.NewDecoder(r.Body).Decode(&req) //nolint:errcheck

		// user_id: body > context > query param
		userID := req.UserID
		if userID == "" {
			userID = userIDFromCtx(r)
		}
		if userID == "" {
			writeErr(w, http.StatusUnauthorized, "user_id required")
			return
		}

		limit := req.Limit
		offset := req.Offset
		// Also accept query-param overrides for limit/offset
		if limit == 0 {
			limit, _ = strconv.Atoi(r.URL.Query().Get("limit"))
		}
		if offset == 0 {
			offset, _ = strconv.Atoi(r.URL.Query().Get("offset"))
		}
		if limit <= 0 || limit > 100 {
			limit = 25
		}
		if offset < 0 {
			offset = 0
		}

				q := `
						SELECT i.id, i.outbox_id, i.correlation_id, i.event_id,
									 COALESCE(i.subject,''), COALESCE(i.body,''),
									 COALESCE(i.priority_level,3), i.is_read, i.read_at, i.created_at,
									 e.module_code, e.sub_module_code, e.event_code, e.event_display_name,
									 o.sender_id, o.sender_name, o.sender_email
						FROM notification_svc.in_app_notification i
						LEFT JOIN notification_svc.event e ON e.event_id = i.event_id
						LEFT JOIN notification_svc.outbox o ON o.outbox_id = i.outbox_id
						WHERE i.recipient_user_id = $1
							AND i.is_deleted = false
						ORDER BY i.created_at DESC
						LIMIT $2 OFFSET $3
				`
		rows, err := pool.Query(r.Context(), q, userID, limit, offset)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, "db error")
			fmt.Printf("[PUSH] handleGetInbox query error: %v\n", err)
			return
		}
		defer rows.Close()

		var items []inboxItem
		for rows.Next() {
			var it inboxItem

			// nullable DB columns -> use sql.NullString and convert to empty string
			var moduleCode, subModuleCode, eventCode, eventName sql.NullString
			var senderID, senderName, senderEmail sql.NullString

			if err := rows.Scan(
				&it.ID, &it.OutboxID, &it.CorrelationID, &it.EventID,
				&it.Subject, &it.Body, &it.PriorityLevel,
				&it.IsRead, &it.ReadAt, &it.CreatedAt,
				&moduleCode, &subModuleCode, &eventCode, &eventName,
				&senderID, &senderName, &senderEmail,
			); err != nil {
				fmt.Printf("[PUSH] handleGetInbox scan error: %v\n", err)
				continue
			}

			// assign with empty-string fallback and trim padding
			it.ModuleCode = strings.TrimSpace(moduleCode.String)
			it.SubModuleCode = strings.TrimSpace(subModuleCode.String)
			it.EventCode = strings.TrimSpace(eventCode.String)
			it.EventName = strings.TrimSpace(eventName.String)
			it.SenderID = strings.TrimSpace(senderID.String)
			it.SenderName = strings.TrimSpace(senderName.String)
			it.SenderEmail = strings.TrimSpace(senderEmail.String)

			// trim padding for fixed-width char columns that may include trailing spaces
			it.ID = strings.TrimSpace(it.ID)
			it.OutboxID = strings.TrimSpace(it.OutboxID)
			it.CorrelationID = strings.TrimSpace(it.CorrelationID)
			it.EventID = strings.TrimSpace(it.EventID)
			it.Subject = strings.TrimSpace(it.Subject)
			it.Body = strings.TrimSpace(it.Body)

			items = append(items, it)
		}
		if rows.Err() != nil {
			writeErr(w, http.StatusInternalServerError, "db error")
			return
		}
		if items == nil {
			items = []inboxItem{}
		}
		writeOK(w, map[string]interface{}{
			"items":  items,
			"limit":  limit,
			"offset": offset,
		})
	}
}

// handleGetCount — POST /notification/inbox/count
func handleGetCount(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		var req struct {
			UserID string `json:"user_id"`
		}
		json.NewDecoder(r.Body).Decode(&req) //nolint:errcheck
		userID := req.UserID
		if userID == "" {
			userID = userIDFromCtx(r)
		}
		if userID == "" {
			writeErr(w, http.StatusUnauthorized, "user_id required")
			return
		}
		var count int
		err := pool.QueryRow(r.Context(),
			`SELECT COUNT(*) FROM notification_svc.in_app_notification
			 WHERE recipient_user_id = $1 AND is_read = false AND is_deleted = false`,
			userID,
		).Scan(&count)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, "db error")
			return
		}
		writeOK(w, map[string]interface{}{
			"unread": count,
		})
	}
}

// handleMarkRead — POST /notification/inbox/mark-read
func handleMarkRead(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		var body struct {
			UserID string `json:"user_id"`
			ID     string `json:"id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.ID == "" {
			writeErr(w, http.StatusBadRequest, "id required")
			return
		}
		userID := body.UserID
		if userID == "" {
			userID = userIDFromCtx(r)
		}
		if userID == "" {
			writeErr(w, http.StatusUnauthorized, "user_id required")
			return
		}
		tag, err := pool.Exec(r.Context(),
			`UPDATE notification_svc.in_app_notification
			 SET is_read = true, read_at = NOW()
			 WHERE id = $1
			   AND recipient_user_id = $2
			   AND is_deleted = false
			   AND is_read = false`,
			body.ID, userID,
		)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, "db error")
			return
		}
		if tag.RowsAffected() == 0 {
			writeErr(w, http.StatusNotFound, "notification not found or already read")
			return
		}
		go pushCountSSE(r.Context(), pool, userID)
		writeOK(w, map[string]interface{}{"message": "notification marked as read"})
	}
}

// handleMarkAllRead — POST /notification/inbox/mark-all-read
func handleMarkAllRead(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		var body struct {
			UserID string `json:"user_id"`
		}
		json.NewDecoder(r.Body).Decode(&body) //nolint:errcheck
		userID := body.UserID
		if userID == "" {
			userID = userIDFromCtx(r)
		}
		if userID == "" {
			writeErr(w, http.StatusUnauthorized, "user_id required")
			return
		}
		tag, err := pool.Exec(r.Context(),
			`UPDATE notification_svc.in_app_notification
			 SET is_read = true, read_at = NOW()
			 WHERE recipient_user_id = $1
			   AND is_deleted = false
			   AND is_read = false`,
			userID,
		)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, "db error")
			return
		}
		go pushCountSSE(r.Context(), pool, userID)
		writeOK(w, map[string]interface{}{
			"message": "all notifications marked as read",
			"updated": tag.RowsAffected(),
		})
	}
}

// handleDelete — POST /notification/inbox/delete (soft-delete)
func handleDelete(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		var body struct {
			UserID string `json:"user_id"`
			ID     string `json:"id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.ID == "" {
			writeErr(w, http.StatusBadRequest, "id required")
			return
		}
		userID := body.UserID
		if userID == "" {
			userID = userIDFromCtx(r)
		}
		if userID == "" {
			writeErr(w, http.StatusUnauthorized, "user_id required")
			return
		}
		tag, err := pool.Exec(r.Context(),
			`UPDATE notification_svc.in_app_notification
			 SET is_deleted = true, deleted_at = NOW()
			 WHERE id = $1
			   AND recipient_user_id = $2
			   AND is_deleted = false`,
			body.ID, userID,
		)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, "db error")
			return
		}
		if tag.RowsAffected() == 0 {
			writeErr(w, http.StatusNotFound, "notification not found or already deleted")
			return
		}
		go pushCountSSE(r.Context(), pool, userID)
		writeOK(w, map[string]interface{}{"message": "notification deleted"})
	}
}
