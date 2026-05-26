package catalog

// system_notif.go — In-memory System Notification Store + SSE broadcast
//
// DESIGN SUMMARY
// ──────────────
// When the notification pipeline hits any error (no event, no template, bad actor,
// DB failure, etc.) we create a "system notification" and:
//
//  1. Store it in an in-memory ring buffer keyed by the RECIPIENT user ID.
//     Survives until: server restart OR user logs out (ClearSystemNotifications).
//
//  2. Deliver it to EVERY user currently in active sessions via SSE using two
//     complementary messages:
//       a. "in_app_item"         — the full notification item (same inboxItem
//                                  JSON shape as DB-generated notifications,
//                                  plus is_system=true).  Live inboxes append it.
//       b. "notification_count"  — refreshes the badge counter.  No frontend
//                                  changes needed; the existing handler already
//                                  listens for this type.
//
//  3. The actor (who caused the action) is recorded as sender_id / sender_name /
//     sender_email so every recipient can see who triggered the event.
//
// INBOX BLENDING (pushInbox.go)
// ──────────────────────────────
//  GetSystemNotificationsAsInboxItems(userID) returns []*SysInboxItem.
//  handleGetInbox prepends these BEFORE the DB rows so they appear at the top.
//  handleGetCount adds GetUnreadSystemNotifCount(userID) to the DB count.
//
// SHAPE — SysInboxItem matches push.inboxItem exactly, with is_system=true:
//  {
//    "id":             "SN-<userID>-<nano>",
//    "outbox_id":      "",
//    "correlation_id": "LIMIT_BULK_CREATE/...",
//    "event_id":       "",
//    "subject":        "Notification not configured",
//    "body":           "No active approved notification event found ...",
//    "priority_level": 1,
//    "is_read":        false,
//    "created_at":     "2026-03-27T10:00:00Z",
//    "module_code":    "SYS",
//    "sub_module_code":"NOTIFICATION_PIPELINE",
//    "event_code":     "PIPELINE_ERROR",
//    "event_name":     "System Alert",
//    "sender_id":      "CIMPLR00...",
//    "sender_name":    "Hardik Mishra",
//    "sender_email":   "hardik@co.com",
//    "level":          "warn",   ← extra field; UI can use for badge colour
//    "source":         "notification_pipeline",
//    "route":          "/cash/limit/bulk-create",
//    "is_system":      true
//  }

import (
	"CimplrCorpSaas/api/auth"
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
	maxAgeDuration  = 8 * time.Hour // entries expire after this
	cleanupInterval = 30 * time.Minute
)

// ─────────────────────────────────────────────────────────────────────────────
// Types
// ─────────────────────────────────────────────────────────────────────────────

// SysNotifLevel controls visual severity.
type SysNotifLevel string

const (
	LevelError SysNotifLevel = "error"
	LevelWarn  SysNotifLevel = "warn"
	LevelInfo  SysNotifLevel = "info"
)

// SysInboxItem is the exact shape returned by the inbox list API for system
// notifications.  It mirrors push.inboxItem field-for-field so the frontend
// renders it identically, with three extra discriminator fields.
type SysInboxItem struct {
	// ── Standard inboxItem fields ─────────────────────────────────────────
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
	ModuleCode    string     `json:"module_code"`
	SubModuleCode string     `json:"sub_module_code"`
	EventCode     string     `json:"event_code"`
	EventName     string     `json:"event_name"`
	SenderID      string     `json:"sender_id"`
	SenderName    string     `json:"sender_name"`
	SenderEmail   string     `json:"sender_email"`
	// ── System-only extras ────────────────────────────────────────────────
	Level    SysNotifLevel `json:"level"`     // "error" | "warn" | "info"
	Source   string        `json:"source"`    // "notification_pipeline"
	Route    string        `json:"route"`     // originating HTTP route
	IsSystem bool          `json:"is_system"` // always true
}

// SystemNotifParams is the caller input to PushSystemNotification.
type SystemNotifParams struct {
	Level         SysNotifLevel // defaults to LevelError
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
	items []*SysInboxItem // ring buffer, oldest first
}

var sysNotifRegistry = struct {
	mu    sync.RWMutex
	users map[string]*userNotifStore
	once  sync.Once
}{
	users: make(map[string]*userNotifStore),
}

func startCleanupWorker() {
	go func() {
		ticker := time.NewTicker(cleanupInterval)
		defer ticker.Stop()
		for range ticker.C {
			cutoff := time.Now().Add(-maxAgeDuration)
			sysNotifRegistry.mu.RLock()
			stores := make([]*userNotifStore, 0, len(sysNotifRegistry.users))
			for _, s := range sysNotifRegistry.users {
				stores = append(stores, s)
			}
			sysNotifRegistry.mu.RUnlock()
			for _, s := range stores {
				s.mu.Lock()
				kept := s.items[:0]
				for _, it := range s.items {
					if it.CreatedAt.After(cutoff) {
						kept = append(kept, it)
					}
				}
				s.items = kept
				s.mu.Unlock()
			}
		}
	}()
}

func getOrCreateUserStore(userID string) *userNotifStore {
	sysNotifRegistry.mu.RLock()
	if s, ok := sysNotifRegistry.users[userID]; ok {
		sysNotifRegistry.mu.RUnlock()
		return s
	}
	sysNotifRegistry.mu.RUnlock()

	sysNotifRegistry.mu.Lock()
	defer sysNotifRegistry.mu.Unlock()
	if s, ok := sysNotifRegistry.users[userID]; ok {
		return s
	}
	sysNotifRegistry.once.Do(startCleanupWorker)
	s := &userNotifStore{}
	sysNotifRegistry.users[userID] = s
	return s
}

func (us *userNotifStore) push(item *SysInboxItem) {
	us.mu.Lock()
	defer us.mu.Unlock()
	us.items = append(us.items, item)
	if len(us.items) > maxPerUser {
		us.items = us.items[len(us.items)-maxPerUser:]
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Public API
// ─────────────────────────────────────────────────────────────────────────────

// PushSystemNotification creates a system notification and:
//  1. Stores it in the in-memory ring buffer for every active session user
//     (so they see it when they next load the inbox or on reconnect).
//  2. Immediately broadcasts two SSE messages to every active session:
//     • "in_app_item"        — the full item payload (live inbox append)
//     • "notification_count" — badge refresh (uses the same SSE type as
//     DB-driven pushCountSSE; no frontend change needed)
//
// The actor (who caused the pipeline action) is set as sender so recipients
// can see who triggered it.
//
// Non-blocking, best-effort — never returns an error.
func PushSystemNotification(actor actorResolution, params SystemNotifParams) {
	if params.Level == "" {
		params.Level = LevelError
	}
	if params.Source == "" {
		params.Source = "notification_pipeline"
	}

	// Collect all active session users to broadcast to.
	// The actor themselves is always included even if they have no session slot
	// (e.g. API key call) — we fall back to actor.UserID as sole recipient.
	sessions := auth.GetActiveSessions()

	// Build recipient list: all active sessions + actor (if not already covered)
	type recipient struct {
		userID string
		name   string
		email  string
	}
	var recipients []recipient
	actorFound := false
	for _, s := range sessions {
		if s.UserID == "" {
			continue
		}
		if s.UserID == actor.UserID {
			actorFound = true
		}
		recipients = append(recipients, recipient{userID: s.UserID, name: s.Name, email: s.Email})
	}
	// Always include the actor themselves even if they have no active SSE session
	if !actorFound && actor.UserID != "" {
		recipients = append(recipients, recipient{userID: actor.UserID, name: actor.Name, email: actor.Email})
	}
	// If there are no sessions at all and no actor, nothing to do
	if len(recipients) == 0 {
		return
	}

	now := time.Now()
	// Use a single nano-second timestamp as base; suffix with recipient index for uniqueness
	baseNano := now.UnixNano()

	for i, r := range recipients {
		item := &SysInboxItem{
			ID:            fmt.Sprintf("SN-%s-%d-%d", r.userID, baseNano, i),
			OutboxID:      "",
			CorrelationID: params.CorrelationID,
			EventID:       "",
			Subject:       params.Subject,
			Body:          params.Body,
			PriorityLevel: 1, // highest priority — system alerts always at top
			IsRead:        false,
			CreatedAt:     now,
			ModuleCode:    "SYS",
			SubModuleCode: "NOTIFICATION_PIPELINE",
			EventCode:     "PIPELINE_ERROR",
			EventName:     "System Alert",
			SenderID:      actor.UserID,
			SenderName:    actor.Name,
			SenderEmail:   actor.Email,
			Level:         params.Level,
			Source:        params.Source,
			Route:         params.Route,
			IsSystem:      true,
		}

		// 1 — Store in ring buffer for this recipient
		getOrCreateUserStore(r.userID).push(item)

		// 2a — Push the full item over SSE so live-loaded inboxes append it immediately
		if raw, err := json.Marshal(map[string]interface{}{
			"type": "in_app_item",
			"item": item,
		}); err == nil {
			dashboard.SendToUser(r.userID, raw)
		}

		// 2b — Push updated unread count so the badge refreshes
		// We compute the count from memory only (no DB round-trip needed here)
		unread := GetUnreadSystemNotifCount(r.userID)
		if raw, err := json.Marshal(map[string]interface{}{
			"type":   "notification_count",
			"unread": unread,
		}); err == nil {
			dashboard.SendToUser(r.userID, raw)
		}
	}
}

// GetSystemNotificationsAsInboxItems returns the stored system notifications for
// the given user as a slice of *SysInboxItem, newest first.
// Called by pushInbox.go handleGetInbox to prepend system items to the DB results.
func GetSystemNotificationsAsInboxItems(userID string) []*SysInboxItem {
	sysNotifRegistry.mu.RLock()
	store, ok := sysNotifRegistry.users[userID]
	sysNotifRegistry.mu.RUnlock()
	if !ok {
		return nil
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	if len(store.items) == 0 {
		return nil
	}
	// Return newest-first copy
	out := make([]*SysInboxItem, len(store.items))
	for i, item := range store.items {
		out[len(store.items)-1-i] = item
	}
	return out
}

// GetUnreadSystemNotifCount returns the number of unread system notifications for a user.
// Used by handleGetCount to include system alerts in the badge total.
func GetUnreadSystemNotifCount(userID string) int {
	sysNotifRegistry.mu.RLock()
	store, ok := sysNotifRegistry.users[userID]
	sysNotifRegistry.mu.RUnlock()
	if !ok {
		return 0
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	n := 0
	for _, it := range store.items {
		if !it.IsRead {
			n++
		}
	}
	return n
}

// MarkSystemNotifsRead marks all in-memory system notifications as read for a user.
// Called by handleMarkAllRead so the badge goes to zero for system items too.
func MarkSystemNotifsRead(userID string) {
	sysNotifRegistry.mu.RLock()
	store, ok := sysNotifRegistry.users[userID]
	sysNotifRegistry.mu.RUnlock()
	if !ok {
		return
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	for _, it := range store.items {
		it.IsRead = true
	}
}

// MarkSystemNotifRead marks a single system notification as read by its ID.
// Called by handleMarkRead when the user reads one item.
func MarkSystemNotifRead(userID, notifID string) {
	sysNotifRegistry.mu.RLock()
	store, ok := sysNotifRegistry.users[userID]
	sysNotifRegistry.mu.RUnlock()
	if !ok {
		return
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	for _, it := range store.items {
		if it.ID == notifID {
			it.IsRead = true
			return
		}
	}
}

// ClearSystemNotifications removes all stored system notifications for a user.
// Call this from Logout() so memory is freed and no stale items linger.
func ClearSystemNotifications(userID string) {
	sysNotifRegistry.mu.Lock()
	delete(sysNotifRegistry.users, userID)
	sysNotifRegistry.mu.Unlock()
}
