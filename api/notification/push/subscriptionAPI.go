package push

// subscriptionAPI.go — Browser Push Subscription Management
//
// ROUTES
// ──────
// POST /notification/push-subscription/vapid-public-key
//      → { "public_key": "BEl62..." }
//      Frontend calls this to get the key for pushManager.subscribe()
//
// POST /notification/push-subscription/register
//      Body: { "endpoint": "...", "keys": { "p256dh": "...", "auth": "..." }, "user_agent": "..." }
//      → { "id": "uuid", "message": "subscription registered" }
//
// POST /notification/push-subscription/unregister
//      Body: { "endpoint": "..." }
//      → { "message": "unsubscribed" }

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ─────────────────────────────────────────────────────────────────────────────
// Request types
// ─────────────────────────────────────────────────────────────────────────────

type subscribeRequest struct {
	UserID    string `json:"user_id"`
	Endpoint  string `json:"endpoint"`
	Keys      struct {
		P256DH string `json:"p256dh"`
		Auth   string `json:"auth"`
	} `json:"keys"`
	UserAgent string `json:"user_agent"`
}

type unsubscribeRequest struct {
	UserID   string `json:"user_id"`
	Endpoint string `json:"endpoint"`
}

// ─────────────────────────────────────────────────────────────────────────────
// Route registration
// ─────────────────────────────────────────────────────────────────────────────

// RegisterSubscriptionRoutes wires push-subscription routes onto the mux.
// Call from api/notification/dino.go alongside RegisterPushInboxRoutes.
func RegisterSubscriptionRoutes(mux *http.ServeMux, pool *pgxpool.Pool) {
	mux.HandleFunc("/notification/push-subscription/vapid-public-key", handleVAPIDPublicKey)
	mux.HandleFunc("/notification/push-subscription/register", withPool(pool, handleSubscribe))
	mux.HandleFunc("/notification/push-subscription/unregister", withPool(pool, handleUnsubscribe))
}

// withPool adapts a pool-accepting handler func into a standard http.HandlerFunc.
func withPool(pool *pgxpool.Pool, fn func(http.ResponseWriter, *http.Request, *pgxpool.Pool)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fn(w, r, pool)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// POST /notification/push-subscription/vapid-public-key
// ─────────────────────────────────────────────────────────────────────────────

func handleVAPIDPublicKey(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	key := strings.TrimSpace(os.Getenv("VAPID_PUBLIC_KEY"))
	if key == "" {
		writeErr(w, http.StatusServiceUnavailable, "browser push not configured — VAPID_PUBLIC_KEY env var missing")
		return
	}
	writeOK(w, map[string]string{"public_key": key})
}

// ─────────────────────────────────────────────────────────────────────────────
// POST /notification/push-subscription/register
// ─────────────────────────────────────────────────────────────────────────────

func handleSubscribe(w http.ResponseWriter, r *http.Request, pool *pgxpool.Pool) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req subscribeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	// user_id: body > context > query param
	userID := req.UserID
	if userID == "" {
		userID = userIDFromCtx(r)
	}
	if userID == "" {
		writeErr(w, http.StatusUnauthorized, "user_id required")
		return
	}
	if req.Endpoint == "" || req.Keys.P256DH == "" || req.Keys.Auth == "" {
		writeErr(w, http.StatusBadRequest, "endpoint, keys.p256dh, and keys.auth are required")
		return
	}

	var id string
	err := pool.QueryRow(r.Context(), `
		INSERT INTO notification_svc.push_subscription
			(user_id, endpoint, p256dh, auth_key, user_agent, is_active, last_used_at)
		VALUES ($1, $2, $3, $4, $5, TRUE, NOW())
		ON CONFLICT (user_id, endpoint) WHERE is_active = TRUE
		DO UPDATE SET
			p256dh       = EXCLUDED.p256dh,
			auth_key     = EXCLUDED.auth_key,
			user_agent   = EXCLUDED.user_agent,
			last_used_at = NOW(),
			updated_at   = NOW()
		RETURNING id::text
	`, userID, req.Endpoint, req.Keys.P256DH, req.Keys.Auth, req.UserAgent).Scan(&id)
	if err != nil {
		fmt.Printf("[push-sub] register userID=%s: %v\n", userID, err)
		writeErr(w, http.StatusInternalServerError, "failed to register push subscription")
		return
	}

	writeOK(w, map[string]string{"id": id, "message": "subscription registered"})
}

// ─────────────────────────────────────────────────────────────────────────────
// POST /notification/push-subscription/unregister
// ─────────────────────────────────────────────────────────────────────────────

func handleUnsubscribe(w http.ResponseWriter, r *http.Request, pool *pgxpool.Pool) {
	if r.Method != http.MethodPost && r.Method != http.MethodDelete {
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req unsubscribeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Endpoint == "" {
		writeErr(w, http.StatusBadRequest, "endpoint required")
		return
	}
	userID := req.UserID
	if userID == "" {
		userID = userIDFromCtx(r)
	}
	if userID == "" {
		writeErr(w, http.StatusUnauthorized, "user_id required")
		return
	}

	pool.Exec(r.Context(), `
		UPDATE notification_svc.push_subscription
		   SET is_active = FALSE, updated_at = NOW()
		 WHERE user_id = $1 AND endpoint = $2
	`, userID, req.Endpoint)

	writeOK(w, map[string]string{"message": "unsubscribed"})
}
