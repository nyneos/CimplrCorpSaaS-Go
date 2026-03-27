package catalog

// system_notif.go — In-memory System Notification Store + SSE broadcast
//
// PURPOSE
// ───────
// When the normal notification pipeline encounters any error (no event configured,
// no approved template, actor entity unresolved, DB insert error, etc.) every user
// currently in the active session needs to see a diagnostic in-app notification —
// identical in shape to a DB-generated in-app notification, so the frontend needs
// zero changes.
//
// DESIGN
// ──────
// • Same JSON shape as inboxItem (id, outbox_id, correlation_id, event_id,
//   subject, body, priority_level, is_read, created_at, module_code,
//   sub_module_code, event_code, event_name, sender_id, sender_name, sender_email).
//   Extra field: "is_system": true — lets the UI optionally style it differently.
// • Stored in RAM per-user ring buffer (maxPerUser=50, maxAge=8h).
//   Lives until the server restarts or the user logs out (ClearSystemNotifications).
// • On push: ALL active sessions receive a "notification_count" SSE update (the
//   same event the inbox counter already listens to) so no frontend changes needed.
//   Additionally each session user gets the full item as "in_app_item" SSE type
//   so live-loaded inboxes update without a refresh.
// • The actor (who triggered the pipeline) is recorded as sender_id/sender_name/
//   sender_email so the receiving users see who caused the event.
//
// LIFECYCLE
// ─────────
// • Notifications are cleared when the user logs out (call ClearSystemNotifications).
// • Cleanup worker drops entries older than maxAgeDuration every cleanupInterval.
//
// FRONTEND INTEGRATION
// ─────────────────────
// No changes needed — system notifs are blended into POST /notification/inbox
// alongside regular DB notifications. They arrive with is_system=true.
// The existing "notification_count" SSE handler already refreshes the badge.

import (
	// "CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/internal/dashboard"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────────────────────────

const (
	maxPerUser      = 50            // max system notifications stored per user
	maxAgeDuration  = 8 * time.Hour // drop entries older than this on cleanup
	cleanupInterval = 30 * time.Minute
)

// ─────────────────────────────────────────────────────────────────────────────
// Types
// ─────────────────────────────────────────────────────────────────────────────

// SysNotifLevel controls the visual severity displayed in the frontend.
type SysNotifLevel string

const (
	LevelError SysNotifLevel = "error"
	LevelWarn  SysNotifLevel = "warn"
	LevelInfo  SysNotifLevel = "info"
)

// SystemNotification is one system-generated alert stored in memory.
type SystemNotification struct {
	ID            string        `json:"id"`
	Level         SysNotifLevel `json:"level"`
	Subject       string        `json:"subject"`
	Body          string        `json:"body"`
	Source        string        `json:"source"`         // "notification_pipeline" | "auth" | …
	Route         string        `json:"route"`          // originating HTTP route if applicable
	CorrelationID string        `json:"correlation_id"` // domain correlation if applicable
	CreatedAt     time.Time     `json:"created_at"`
	IsRead        bool          `json:"is_read"`
}

// MarshalJSON adds the SSE type discriminator and formats the time.
func (n SystemNotification) ssePayload() map[string]interface{} {
	return map[string]interface{}{
		"type":           "system_notification",
		"id":             n.ID,
		"level":          string(n.Level),
		"subject":        n.Subject,
		"body":           n.Body,
		"source":         n.Source,
		"route":          n.Route,
		"correlation_id": n.CorrelationID,
		"created_at":     n.CreatedAt.Format(time.RFC3339),
		"is_read":        n.IsRead,
	}
}

// SystemNotifParams is the caller-facing input to PushSystemNotification.
type SystemNotifParams struct {
	Level         SysNotifLevel // defaults to LevelError if empty
	Subject       string
	Body          string
	Source        string // e.g. "notification_pipeline"
	Route         string
	CorrelationID string
}

// ─────────────────────────────────────────────────────────────────────────────
// In-memory store
// ─────────────────────────────────────────────────────────────────────────────

type userNotifStore struct {
	mu    sync.RWMutex
	items []*SystemNotification // ring buffer, newest last
}

// systemNotifStore is the global registry: userID → per-user store.
var systemNotifStore = struct {
	mu    sync.RWMutex
	users map[string]*userNotifStore
	once  sync.Once
}{
	users: make(map[string]*userNotifStore),
}

// startCleanupWorker is called once on first push; drops stale entries periodically.
func startCleanupWorker() {
	go func() {
		ticker := time.NewTicker(cleanupInterval)
		defer ticker.Stop()
		for range ticker.C {
			cutoff := time.Now().Add(-maxAgeDuration)
			systemNotifStore.mu.RLock()
			stores := make([]*userNotifStore, 0, len(systemNotifStore.users))
			for _, s := range systemNotifStore.users {
				stores = append(stores, s)
			}
			systemNotifStore.mu.RUnlock()

			for _, s := range stores {
				s.mu.Lock()
				fresh := s.items[:0]
				for _, item := range s.items {
					if item.CreatedAt.After(cutoff) {
						fresh = append(fresh, item)
					}
				}
				s.items = fresh
				s.mu.Unlock()
			}
		}
	}()
}

// getOrCreateUserStore returns (creating if needed) the per-user store.
func getOrCreateUserStore(userID string) *userNotifStore {
	// Fast path: read lock
	systemNotifStore.mu.RLock()
	if s, ok := systemNotifStore.users[userID]; ok {
		systemNotifStore.mu.RUnlock()
		return s
	}
	systemNotifStore.mu.RUnlock()

	// Slow path: write lock
	systemNotifStore.mu.Lock()
	defer systemNotifStore.mu.Unlock()
	if s, ok := systemNotifStore.users[userID]; ok {
		return s
	}
	// First write ever — start the cleanup worker
	systemNotifStore.once.Do(startCleanupWorker)

	s := &userNotifStore{}
	systemNotifStore.users[userID] = s
	return s
}

// store adds one notification to the user's ring buffer.
func (us *userNotifStore) store(n *SystemNotification) {
	us.mu.Lock()
	defer us.mu.Unlock()
	us.items = append(us.items, n)
	// Enforce ring cap: drop oldest
	if len(us.items) > maxPerUser {
		us.items = us.items[len(us.items)-maxPerUser:]
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Public API
// ─────────────────────────────────────────────────────────────────────────────

// PushSystemNotification stores a system notification in memory for the resolved
// actor user AND immediately sends it over their live SSE connection (if open).
//
// Pass the actorResolution from resolveActorEntity so we always have a userID to
// address the notification to.  If userID is empty (completely unresolvable actor)
// the notification is silently dropped — there is no recipient to address it to.
//
// This function is non-blocking (SSE send is best-effort) and never returns an error
// so it is safe to call from any error path without wrapping.
func PushSystemNotification(actor actorResolution, params SystemNotifParams) {
	if actor.UserID == "" {
		// We have no address — nothing we can do; error is already logged by resolver.
		return
	}

	if params.Level == "" {
		params.Level = LevelError
	}
	if params.Source == "" {
		params.Source = "notification_pipeline"
	}

	notif := &SystemNotification{
		ID:            fmt.Sprintf("SN-%s-%d", actor.UserID, time.Now().UnixNano()),
		Level:         params.Level,
		Subject:       params.Subject,
		Body:          params.Body,
		Source:        params.Source,
		Route:         params.Route,
		CorrelationID: params.CorrelationID,
		CreatedAt:     time.Now(),
		IsRead:        false,
	}

	// 1 — Persist to memory store (always, even if SSE is not connected)
	getOrCreateUserStore(actor.UserID).store(notif)

	// 2 — Push over SSE immediately (best-effort; user may not be connected)
	payload, err := json.Marshal(notif.ssePayload())
	if err == nil {
		dashboard.SendToUser(actor.UserID, payload)
	}
}

// GetSystemNotifications returns a copy of all non-expired system notifications
// for the given user, newest first.  Read-flag is NOT mutated here — the frontend
// should call MarkSystemNotificationsRead when the user views them.
func GetSystemNotifications(userID string) []*SystemNotification {
	systemNotifStore.mu.RLock()
	store, ok := systemNotifStore.users[userID]
	systemNotifStore.mu.RUnlock()
	if !ok {
		return nil
	}

	store.mu.RLock()
	defer store.mu.RUnlock()

	// Return newest-first copy
	out := make([]*SystemNotification, len(store.items))
	for i, item := range store.items {
		out[len(store.items)-1-i] = item
	}
	return out
}

// GetUnreadSystemNotificationCount returns the count of unread system notifications.
func GetUnreadSystemNotificationCount(userID string) int {
	systemNotifStore.mu.RLock()
	store, ok := systemNotifStore.users[userID]
	systemNotifStore.mu.RUnlock()
	if !ok {
		return 0
	}

	store.mu.RLock()
	defer store.mu.RUnlock()
	count := 0
	for _, item := range store.items {
		if !item.IsRead {
			count++
		}
	}
	return count
}

// MarkSystemNotificationsRead marks all notifications as read for a user.
// Call this when the user opens the system-alerts panel.
func MarkSystemNotificationsRead(userID string) {
	systemNotifStore.mu.RLock()
	store, ok := systemNotifStore.users[userID]
	systemNotifStore.mu.RUnlock()
	if !ok {
		return
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	for _, item := range store.items {
		item.IsRead = true
	}
}
