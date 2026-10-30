package auth

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha1"
	"database/sql"
	"encoding/base32"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strings"
	"time"

	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/internal/logger"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ── TOTP (RFC 6238) MFA implementation ────────────────────────────────────────

const (
	totpDigits   = 6
	totpPeriod   = 30 // seconds
	totpSkew     = 1  // allow ±1 period
	secretLength = 20 // 160-bit key
)

// ── MFA Handlers ──────────────────────────────────────────────────────────────

// MFASetupHandler generates a new TOTP secret for the logged-in caller.
// When MFA is already active the current code is required.
// POST /auth/mfa/setup  { "user_id": "...", "code": "123456" }
func MFASetupHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			Code   string `json:"code"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		session := callerSession(r, req.UserID)
		if session == nil {
			writeJSON(w, http.StatusUnauthorized, map[string]interface{}{
				"success": false, "error": "Invalid session",
			})
			return
		}
		userID := session.UserID

		var enabled sql.NullBool
		var stored sql.NullString
		if err := pool.QueryRow(r.Context(), `SELECT mfa_enabled, mfa_secret FROM users WHERE id = $1`, userID).Scan(&enabled, &stored); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
				"success": false, "error": "Failed to load MFA state",
			})
			return
		}
		if enabled.Valid && enabled.Bool && stored.Valid && stored.String != "" {
			current, err := openMFASecret(stored.String)
			if err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
					"success": false, "error": "Failed to load MFA state",
				})
				return
			}
			if ok, lockErr := checkTOTP(userID, current, req.Code); !ok {
				msg := "Current MFA code is required to reset MFA"
				if lockErr != nil {
					msg = lockErr.Error()
				}
				LogSecurityEvent(r.Context(), pool, userID, "mfa_reset_denied", "", extractClientIPFromRequest(r))
				writeJSON(w, http.StatusUnauthorized, map[string]interface{}{
					"success": false, "error": msg,
				})
				return
			}
		}

		secret, err := generateTOTPSecret()
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
				"success": false, "error": "Failed to generate MFA secret",
			})
			return
		}
		sealed, err := sealMFASecret(secret)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
				"success": false, "error": "Failed to generate MFA secret",
			})
			return
		}

		var email string
		_ = pool.QueryRow(r.Context(), `SELECT email FROM users WHERE id = $1`, userID).Scan(&email)

		_, err = pool.Exec(r.Context(),
			`UPDATE users SET mfa_secret = $1, mfa_enabled = false WHERE id = $2`,
			sealed, userID,
		)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
				"success": false, "error": "Failed to save MFA secret",
			})
			return
		}

		issuer := "CimplrCorpSaaS"
		otpauthURI := fmt.Sprintf(
			"otpauth://totp/%s:%s?secret=%s&issuer=%s&digits=%d&period=%d",
			issuer, email, secret, issuer, totpDigits, totpPeriod,
		)

		LogSecurityEvent(r.Context(), pool, userID, "mfa_setup_initiated", email, extractClientIPFromRequest(r))

		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success":     true,
			"secret":      secret,
			"otpauth_uri": otpauthURI,
			"message":     "Scan the QR code, then confirm with /auth/mfa/confirm",
		})
	}
}

// MFAConfirmHandler verifies the first TOTP code and activates MFA.
// POST /auth/mfa/confirm  { "user_id": "...", "code": "123456" }
func MFAConfirmHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			Code   string `json:"code"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Code == "" {
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{
				"success": false, "error": constants.ErrUserIDAndCodeRequired,
			})
			return
		}
		session := callerSession(r, req.UserID)
		if session == nil {
			writeJSON(w, http.StatusUnauthorized, map[string]interface{}{
				"success": false, "error": "Invalid session",
			})
			return
		}
		userID := session.UserID

		secret, err := getUserMFASecret(r.Context(), pool, userID)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{
				"success": false, "error": "MFA setup not initiated. Call /auth/mfa/setup first",
			})
			return
		}

		if ok, lockErr := checkTOTP(userID, secret, req.Code); !ok {
			msg := "Invalid code. Please try again"
			if lockErr != nil {
				msg = lockErr.Error()
			}
			LogSecurityEvent(r.Context(), pool, userID, "mfa_confirm_failed", constants.ErrInvalidCode, extractClientIPFromRequest(r))
			writeJSON(w, http.StatusUnauthorized, map[string]interface{}{
				"success": false, "error": msg,
			})
			return
		}

		_, err = pool.Exec(r.Context(), `UPDATE users SET mfa_enabled = true WHERE id = $1`, userID)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
				"success": false, "error": "Failed to activate MFA",
			})
			return
		}

		LogSecurityEvent(r.Context(), pool, userID, "mfa_activated", "", extractClientIPFromRequest(r))
		if logger.GlobalLogger != nil {
			logger.GlobalLogger.LogAudit(fmt.Sprintf("MFA activated for user %s", userID))
		}

		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success": true, "message": "MFA has been activated on your account",
		})
	}
}

// MFAVerifyHandler validates a TOTP code during login. On success the pending
// session is marked fully logged in and returned.
// POST /auth/mfa/verify  { "mfa_token": "...", "user_id": "...", "code": "123456" }
func MFAVerifyHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string `json:"user_id"`
			MFAToken string `json:"mfa_token"`
			MFACode  string `json:"mfa_code"`
			Code     string `json:"code"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Code == "" || (req.UserID == "" && req.MFAToken == "" && req.MFACode == "") {
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{
				"success": false, "error": constants.ErrUserIDAndCodeRequired,
			})
			return
		}
		var pending *UserSession
		if c := strings.TrimSpace(req.MFACode); c != "" {
			pending = ticketSession(c, false)
		} else {
			pending = pendingSession(req.MFAToken, req.UserID)
		}
		if pending == nil || (req.UserID != "" && pending.UserID != req.UserID) {
			writeJSON(w, http.StatusUnauthorized, map[string]interface{}{
				"success": false, "error": "Login session not found. Please sign in again",
			})
			return
		}
		userID := pending.UserID

		secret, err := getUserMFASecret(r.Context(), pool, userID)
		if err != nil {
			LogSecurityEvent(r.Context(), pool, userID, "mfa_verify_failed", "no secret configured", extractClientIPFromRequest(r))
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{
				"success": false, "error": "MFA is not set up for this account. Please set up MFA first via /auth/mfa/setup",
			})
			return
		}

		if ok, lockErr := checkTOTP(userID, secret, req.Code); !ok {
			msg := "Invalid MFA code"
			if lockErr != nil {
				msg = lockErr.Error()
			}
			LogSecurityEvent(r.Context(), pool, userID, "mfa_verify_failed", constants.ErrInvalidCode, extractClientIPFromRequest(r))
			writeJSON(w, http.StatusUnauthorized, map[string]interface{}{
				"success": false, "error": msg,
			})
			return
		}

		var session *UserSession
		if globalAuthService != nil {
			session = globalAuthService.completeMFASession(pending)
		}
		if c := strings.TrimSpace(req.MFACode); c != "" {
			consumeTicket(c)
		}
		if session == nil {
			writeJSON(w, http.StatusUnauthorized, map[string]interface{}{
				"success": false, "error": "Login session not found. Please sign in again",
			})
			return
		}

		LogSecurityEvent(r.Context(), pool, userID, "mfa_verify_success", "", extractClientIPFromRequest(r))
		if logger.GlobalLogger != nil {
			logger.GlobalLogger.LogAudit(fmt.Sprintf("MFA verified for user %s", userID))
		}

		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success": true, "session": session,
		})
	}
}

// MFADisableHandler turns off MFA for the caller (requires a valid code).
// POST /auth/mfa/disable  { "user_id": "...", "code": "123456" }
func MFADisableHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			Code   string `json:"code"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Code == "" {
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{
				"success": false, "error": constants.ErrUserIDAndCodeRequired,
			})
			return
		}
		session := callerSession(r, req.UserID)
		if session == nil {
			writeJSON(w, http.StatusUnauthorized, map[string]interface{}{
				"success": false, "error": "Invalid session",
			})
			return
		}
		userID := session.UserID

		secret, err := getUserMFASecret(r.Context(), pool, userID)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{
				"success": false, "error": "MFA is not set up for this account",
			})
			return
		}

		if ok, lockErr := checkTOTP(userID, secret, req.Code); !ok {
			msg := "Invalid MFA code"
			if lockErr != nil {
				msg = lockErr.Error()
			}
			LogSecurityEvent(r.Context(), pool, userID, "mfa_disable_failed", constants.ErrInvalidCode, extractClientIPFromRequest(r))
			writeJSON(w, http.StatusUnauthorized, map[string]interface{}{
				"success": false, "error": msg,
			})
			return
		}

		_, err = pool.Exec(r.Context(), `UPDATE users SET mfa_enabled = false, mfa_secret = NULL WHERE id = $1`, userID)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
				"success": false, "error": "Failed to disable MFA",
			})
			return
		}

		LogSecurityEvent(r.Context(), pool, userID, "mfa_disabled", "", extractClientIPFromRequest(r))
		if logger.GlobalLogger != nil {
			logger.GlobalLogger.LogAudit(fmt.Sprintf("MFA disabled for user %s", userID))
		}

		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success": true, "message": "MFA has been disabled",
		})
	}
}

// MFAStatusHandler returns whether MFA is enabled for the caller.
// POST /auth/mfa/status  { "user_id": "..." }
func MFAStatusHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		session := callerSession(r, req.UserID)
		if session == nil {
			writeJSON(w, http.StatusUnauthorized, map[string]interface{}{
				"success": false, "error": "Invalid session",
			})
			return
		}

		var mfaEnabled sql.NullBool
		var mfaSecret sql.NullString
		err := pool.QueryRow(r.Context(), `SELECT mfa_enabled, mfa_secret FROM users WHERE id = $1`, session.UserID).Scan(&mfaEnabled, &mfaSecret)
		if err != nil {
			writeJSON(w, http.StatusNotFound, map[string]interface{}{
				"success": false, "error": "User not found",
			})
			return
		}

		enabled := mfaEnabled.Valid && mfaEnabled.Bool
		configured := mfaSecret.Valid && mfaSecret.String != ""

		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success":        true,
			"mfa_enabled":    enabled,
			"mfa_configured": configured,
		})
	}
}

// ── AuthService helpers ───────────────────────────────────────────────────────

// CompleteMFALogin marks a pending-MFA session as fully logged in.
func (a *AuthService) CompleteMFALogin(userID string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if s, ok := a.userPointers[userID]; ok {
		s.IsLoggedIn = true
	}
}

// completeMFASession marks the given pending session logged in when it is still live.
func (a *AuthService) completeMFASession(pending *UserSession) *UserSession {
	a.mu.Lock()
	defer a.mu.Unlock()
	live, ok := a.users[pending.SessionID]
	if !ok || live != pending {
		return nil
	}
	live.IsLoggedIn = true
	live.lastActivityUnix = time.Now().Unix()
	if a.MaxLoginAttempts > 0 {
		delete(a.failedAttempts, live.UserID)
	}
	return live
}

// getUserMFASecret retrieves the MFA secret for a user, returning error if not set.
func getUserMFASecret(ctx context.Context, pool *pgxpool.Pool, userID string) (string, error) {
	var mfaSecret sql.NullString
	err := pool.QueryRow(ctx, `SELECT mfa_secret FROM users WHERE id = $1`, userID).Scan(&mfaSecret)
	if err != nil || !mfaSecret.Valid || mfaSecret.String == "" {
		return "", fmt.Errorf("no MFA secret")
	}
	return openMFASecret(mfaSecret.String)
}

// ── TOTP core ─────────────────────────────────────────────────────────────────

func generateTOTPSecret() (string, error) {
	b := make([]byte, secretLength)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return strings.TrimRight(base32.StdEncoding.EncodeToString(b), "="), nil
}

// ValidateTOTP checks code against the secret allowing ±totpSkew windows.
func ValidateTOTP(secret, code string) bool {
	_, ok := matchTOTP(secret, code)
	return ok
}

func generateTOTPCode(secret string, counter int64) string {
	padded := strings.ToUpper(secret)
	if m := len(padded) % 8; m != 0 {
		padded += strings.Repeat("=", 8-m)
	}
	key, err := base32.StdEncoding.DecodeString(padded)
	if err != nil {
		return ""
	}

	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(counter))
	mac := hmac.New(sha1.New, key)
	mac.Write(buf)
	hash := mac.Sum(nil)

	offset := hash[len(hash)-1] & 0x0f
	code := int64(binary.BigEndian.Uint32(hash[offset:offset+4]) & 0x7fffffff)
	otp := code % int64(math.Pow10(totpDigits))

	return fmt.Sprintf("%0*d", totpDigits, otp)
}
