package auth

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/bcrypt"
)

const (
	resetTokenExpiry    = 30 * time.Minute
	resetTokenBytes     = 32
	rateLimitWindow     = 60 * time.Second // 1 request per email per 60s
	rateLimitCleanup    = 10 * time.Minute
	tokenCleanupPeriod  = 1 * time.Hour
	systemEventID       = "SYS-PWRESET" // placeholder event_id for outbox
	systemCorrelationPx = "PWRST-"
)

// ── Rate limiter (per-email, in-memory) ───────────────────────────────────────

var (
	resetRateMap  = make(map[string]time.Time)
	resetRateMu   sync.Mutex
	resetRateOnce sync.Once
)

func isRateLimited(email string) bool {
	resetRateMu.Lock()
	defer resetRateMu.Unlock()

	// Lazy-start cleanup goroutine
	resetRateOnce.Do(func() {
		go func() {
			for {
				time.Sleep(rateLimitCleanup)
				resetRateMu.Lock()
				now := time.Now()
				for k, t := range resetRateMap {
					if now.Sub(t) > rateLimitWindow {
						delete(resetRateMap, k)
					}
				}
				resetRateMu.Unlock()
			}
		}()
	})

	key := strings.ToLower(email)
	if last, ok := resetRateMap[key]; ok && time.Since(last) < rateLimitWindow {
		return true
	}
	resetRateMap[key] = time.Now()
	return false
}

// ── Expired token cleanup ─────────────────────────────────────────────────────

// StartTokenCleanup starts a background goroutine that periodically deletes
// expired or used tokens older than 24h. Call once at startup.
func StartTokenCleanup(db *sql.DB) {
	go func() {
		for {
			time.Sleep(tokenCleanupPeriod)
			_, _ = db.Exec(`DELETE FROM password_reset_tokens WHERE (used = true AND created_at < NOW() - INTERVAL '24 hours') OR (expires_at < NOW() - INTERVAL '24 hours')`)
		}
	}()
}

// ── Email via notification outbox ─────────────────────────────────────────────

func enqueueResetEmail(db *sql.DB, userID, email, name, resetLink string) {
	subject := "Password Reset Request — Cimplr"
	body := buildResetEmailHTML(name, resetLink)

	_, err := db.Exec(`
		INSERT INTO notification_svc.outbox (
			correlation_id, event_id, audit_id, channel,
			recipient_user_id, recipient_email, recipient_name,
			rendered_subject, rendered_body, variables_payload,
			scheduled_at, priority_level, sender_name, sender_email
		) VALUES (
			$1, $2, gen_random_uuid(), 'EMAIL',
			$3, $4, $5,
			$6, $7, '{}'::jsonb,
			now(), 10, 'Cimplr', 'noreply@cimplr.com'
		)`,
		systemCorrelationPx+generateShortID(), systemEventID,
		userID, email, name,
		subject, body,
	)
	if err != nil {
		// Best-effort — log but don't fail the request
		LogSecurityEvent(db, "", "reset_email_enqueue_failed", err.Error(), "")
	}
}

func buildResetEmailHTML(name, resetLink string) string {
	if name == "" {
		name = "User"
	}
	return fmt.Sprintf(`<!DOCTYPE html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#f4f4f7">
<table width="100%%" cellpadding="0" cellspacing="0" style="padding:40px 0">
<tr><td align="center">
<table width="560" cellpadding="0" cellspacing="0" style="background:#fff;border-radius:8px;box-shadow:0 2px 8px rgba(0,0,0,.08);overflow:hidden">
  <tr><td style="background:#2563eb;padding:24px 32px">
    <h1 style="margin:0;color:#fff;font-size:22px;font-weight:600">Cimplr</h1>
  </td></tr>
  <tr><td style="padding:32px">
    <p style="margin:0 0 16px;font-size:16px;color:#333">Hi %s,</p>
    <p style="margin:0 0 24px;font-size:15px;color:#555;line-height:1.6">
      We received a request to reset your password. Click the button below to choose a new password.
      This link expires in <strong>30 minutes</strong>.
    </p>
    <table cellpadding="0" cellspacing="0" style="margin:0 0 24px"><tr><td style="border-radius:6px;background:#2563eb">
      <a href="%s" target="_blank" style="display:inline-block;padding:12px 32px;color:#fff;text-decoration:none;font-size:15px;font-weight:600">
        Reset Password
      </a>
    </td></tr></table>
    <p style="margin:0 0 8px;font-size:13px;color:#888;line-height:1.5">
      If you didn't request this, you can safely ignore this email. Your password will remain unchanged.
    </p>
    <p style="margin:0;font-size:12px;color:#aaa;word-break:break-all">%s</p>
  </td></tr>
  <tr><td style="padding:16px 32px;background:#f9fafb;border-top:1px solid #eee">
    <p style="margin:0;font-size:12px;color:#999;text-align:center">&copy; Cimplr Corp. This is an automated message.</p>
  </td></tr>
</table>
</td></tr>
</table>
</body></html>`, name, resetLink, resetLink)
}

func generateShortID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// ── Handlers ──────────────────────────────────────────────────────────────────

// ForgotPasswordHandler handles POST /auth/forgot-password
// Accepts { "email": "..." }, generates a reset token, and enqueues a reset email
// via the notification outbox. In DEVEL_MODE the token is also returned inline.
func ForgotPasswordHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.Error(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		var req struct {
			Email string `json:"email"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.Email) == "" {
			api.Error(w, http.StatusBadRequest, "Email is required")
			return
		}
		email := strings.TrimSpace(strings.ToLower(req.Email))
		clientIP := extractClientIPFromRequest(r)

		// Rate limit: 1 request per email per 60 seconds
		if isRateLimited(email) {
			api.Error(w, http.StatusTooManyRequests, "Please wait before requesting another reset link")
			return
		}

		// Constant-time-ish response: always return success even if email unknown
		var userID, employeeName string
		err := db.QueryRow(`SELECT id, COALESCE(employee_name, '') FROM users WHERE LOWER(email) = $1`, email).Scan(&userID, &employeeName)
		if err != nil {
			LogSecurityEvent(db, "", "forgot_password_unknown", email, clientIP)
			api.Success(w, http.StatusOK, map[string]interface{}{}, "If the email is registered, a reset link has been sent.")
			return
		}

		// Generate secure token
		token, err := generateResetToken()
		if err != nil {
			api.Error(w, http.StatusInternalServerError, "Failed to generate reset token")
			return
		}

		expiresAt := time.Now().Add(resetTokenExpiry)

		// Invalidate any existing unused tokens for this user
		_, _ = db.Exec(`UPDATE password_reset_tokens SET used = true WHERE user_id = $1 AND used = false`, userID)

		// Insert new token
		_, err = db.Exec(
			`INSERT INTO password_reset_tokens (user_id, token, expires_at) VALUES ($1, $2, $3)`,
			userID, token, expiresAt,
		)
		if err != nil {
			api.Error(w, http.StatusInternalServerError, "Failed to create reset token")
			return
		}

		LogSecurityEvent(db, userID, "forgot_password", email, clientIP)

		// Build reset link
		frontendURL := os.Getenv("FRONTEND_URL")
		if frontendURL == "" {
			frontendURL = "http://localhost:5173"
		}
		resetLink := frontendURL + "/reset-password?token=" + token

		// Enqueue reset email via notification outbox (picked up by outbox worker)
		go enqueueResetEmail(db, userID, email, employeeName, resetLink)

		// In dev mode, also include token + link in response for testing
		if strings.EqualFold(os.Getenv("DEVEL_MODE"), "true") {
			w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success":    true,
				"message":    "Reset token generated (dev mode — email also enqueued)",
				"token":      token,
				"reset_link": resetLink,
				"expires_at": expiresAt.Format(time.RFC3339),
			})
			return
		}

		api.Success(w, http.StatusOK, map[string]interface{}{}, "If the email is registered, a reset link has been sent.")
	}
}

// ResetPasswordHandler handles POST /auth/reset-password
// Accepts { "token": "...", "new_password": "..." } and resets the user's password.
func ResetPasswordHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.Error(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		var req struct {
			Token       string `json:"token"`
			NewPassword string `json:"new_password"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.Error(w, http.StatusBadRequest, "Invalid request body")
			return
		}

		token := strings.TrimSpace(req.Token)
		newPassword := req.NewPassword

		if token == "" || newPassword == "" {
			api.Error(w, http.StatusBadRequest, "Token and new_password are required")
			return
		}

		clientIP := extractClientIPFromRequest(r)

		if len(newPassword) < 8 {
			api.Error(w, http.StatusBadRequest, "Password must be at least 8 characters")
			return
		}

		// Look up and consume token atomically
		var userID string
		var expiresAt time.Time
		var used bool
		err := db.QueryRow(
			`SELECT user_id, expires_at, used FROM password_reset_tokens WHERE token = $1`,
			token,
		).Scan(&userID, &expiresAt, &used)
		if err != nil {
			LogSecurityEvent(db, "", "reset_password_invalid_token", "", clientIP)
			api.Error(w, http.StatusBadRequest, "Invalid or expired reset token")
			return
		}

		if used {
			LogSecurityEvent(db, userID, "reset_password_reused_token", "", clientIP)
			api.Error(w, http.StatusBadRequest, "This reset token has already been used")
			return
		}

		if time.Now().After(expiresAt) {
			LogSecurityEvent(db, userID, "reset_password_expired_token", "", clientIP)
			api.Error(w, http.StatusBadRequest, "Reset token has expired")
			return
		}

		// Hash new password
		hashed, err := bcrypt.GenerateFromPassword([]byte(newPassword), bcrypt.DefaultCost)
		if err != nil {
			api.Error(w, http.StatusInternalServerError, "Failed to hash password")
			return
		}

		// Update password + mark token used in one transaction
		tx, err := db.Begin()
		if err != nil {
			api.Error(w, http.StatusInternalServerError, "Internal error")
			return
		}
		defer tx.Rollback()

		if _, err := tx.Exec(`UPDATE users SET password = $1 WHERE id = $2`, string(hashed), userID); err != nil {
			api.Error(w, http.StatusInternalServerError, "Failed to update password")
			return
		}
		if _, err := tx.Exec(`UPDATE password_reset_tokens SET used = true WHERE token = $1`, token); err != nil {
			api.Error(w, http.StatusInternalServerError, "Failed to invalidate token")
			return
		}
		if err := tx.Commit(); err != nil {
			api.Error(w, http.StatusInternalServerError, "Internal error")
			return
		}

		LogSecurityEvent(db, userID, "reset_password_success", "", clientIP)

		api.Success(w, http.StatusOK, map[string]interface{}{}, "Password has been reset successfully")
	}
}

// ValidateResetTokenHandler handles POST /auth/validate-reset-token
// Accepts { "token": "..." } and checks if the token is still valid.
func ValidateResetTokenHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.Error(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		var req struct {
			Token string `json:"token"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.Token) == "" {
			api.Error(w, http.StatusBadRequest, "Token is required")
			return
		}

		var userID string
		var expiresAt time.Time
		var used bool
		err := db.QueryRow(
			`SELECT user_id, expires_at, used FROM password_reset_tokens WHERE token = $1`,
			strings.TrimSpace(req.Token),
		).Scan(&userID, &expiresAt, &used)
		if err != nil {
			api.Error(w, http.StatusBadRequest, "Invalid reset token")
			return
		}

		if used {
			api.Error(w, http.StatusBadRequest, "This reset token has already been used")
			return
		}

		if time.Now().After(expiresAt) {
			api.Error(w, http.StatusBadRequest, "Reset token has expired")
			return
		}

		// Get email for display (masked)
		var email string
		_ = db.QueryRow(`SELECT email FROM users WHERE id = $1`, userID).Scan(&email)

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":    true,
			"valid":      true,
			"email":      maskEmail(email),
			"expires_at": expiresAt.Format(time.RFC3339),
		})
	}
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func generateResetToken() (string, error) {
	b := make([]byte, resetTokenBytes)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func writeResetJSON(w http.ResponseWriter, status int, success bool, message string) {
	w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": success,
		"message": message,
	})
}

// maskEmail hides part of the email for privacy: "kanav.arora@nyneos.com" → "ka***@ny***.com"
func maskEmail(email string) string {
	parts := strings.SplitN(email, "@", 2)
	if len(parts) != 2 {
		return "***"
	}
	local := parts[0]
	domain := parts[1]
	if len(local) > 2 {
		local = local[:2] + "***"
	}
	domParts := strings.SplitN(domain, ".", 2)
	if len(domParts) == 2 && len(domParts[0]) > 2 {
		domain = domParts[0][:2] + "***." + domParts[1]
	}
	return local + "@" + domain
}
