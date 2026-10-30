package auth

import (
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/internal/dashboard"
	"CimplrCorpSaas/internal/logger"
	"CimplrCorpSaas/internal/serviceiface"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"
)

type SessionEntity struct {
	EntityID         string `json:"entity_id"`
	EntityName       string `json:"entity_name"`
	EntityShortName  string `json:"entity_short_name,omitempty"`
	ParentEntityName string `json:"parent_entity_name,omitempty"`
	EntityLevel      string `json:"entity_level,omitempty"`
	ActiveStatus     string `json:"active_status,omitempty"`
	ApprovalStatus   string `json:"approval_status,omitempty"`
}

type UserSession struct {
	SessionID     string
	UserID        string
	Name          string
	Email         string
	Role          string
	RoleCode      string
	LastLoginTime string
	// lastActivityUnix is refreshed on every authenticated request so the session
	// cleaner enforces an idle timeout rather than a hard cap from login. Accessed
	// via sync/atomic: sessions are handed out as shared pointers, so the cleaner
	// and concurrent requests touch this field without holding the service lock.
	lastActivityUnix int64
	ClientIP         string
	IsLoggedIn       bool
	CreatedEntities  []SessionEntity `json:"created_entities,omitempty"`
}

type failedAttempt struct {
	count    int
	lastTry  time.Time
	isLocked bool
	unlockAt time.Time
}

type AuthService struct {
	pool                 *pgxpool.Pool
	maxUsers             int
	users                map[string]*UserSession
	SessionTimeout       int
	MaxLoginAttempts     int
	AccountLockDuration  int
	SessionCleanerPeriod int
	userPointers         map[string]*UserSession
	mu                   sync.Mutex
	stopCh               chan struct{}
	failedAttempts       map[string]*failedAttempt
}

func NewAuthService(pool *pgxpool.Pool, maxUsers int, SessionTimeout int, MaxLoginAttempts int, AccountLockDuration int, SessionCleanerPeriod int) serviceiface.Service {
	return &AuthService{
		pool:                 pool,
		maxUsers:             maxUsers,
		SessionTimeout:       SessionTimeout,
		MaxLoginAttempts:     MaxLoginAttempts,
		AccountLockDuration:  AccountLockDuration,
		SessionCleanerPeriod: SessionCleanerPeriod,
		users:                make(map[string]*UserSession),
		userPointers:         make(map[string]*UserSession),
		stopCh:               make(chan struct{}),
		failedAttempts:       make(map[string]*failedAttempt),
	}
}

func (a *AuthService) Name() string { return "auth" }

func (a *AuthService) Start() error {
	if a.SessionCleanerPeriod > 0 {
		go a.sessionCleaner()
	}
	StartTokenCleanup(a.pool)
	return nil
}

func (a *AuthService) Stop() error {
	close(a.stopCh)
	return nil
}

// Login authenticates a user by email and password, returning the session and
// an mfaPending flag. Passwords are compared using bcrypt. Database and bcrypt
// work runs outside the service lock; only session and attempt state is locked.
func (a *AuthService) Login(ctx context.Context, username, password string, clientIP string) (*UserSession, bool, error) {
	var dbUserID, dbName, dbEmail string
	var dbPassword sql.NullString
	var dbStatus sql.NullString
	var everApproved bool

	err := a.pool.QueryRow(ctx,
		`SELECT id, employee_name, email, password, status, approved_at IS NOT NULL FROM users WHERE email = $1 AND COALESCE(is_deleted, false) = false LIMIT 1`, username,
	).Scan(&dbUserID, &dbName, &dbEmail, &dbPassword, &dbStatus, &everApproved)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			LogSecurityEvent(ctx, a.pool, "", "login_failed", "unknown user", clientIP)
			return nil, false, errors.New("Invalid Username or Password")
		}
		logger.LogError("[auth] users lookup failed: %v", err)
		return nil, false, errors.New("internal error")
	}

	if !loginStatusAllowed(dbStatus, everApproved) {
		LogSecurityEvent(ctx, a.pool, dbUserID, "login_failed", "account not active: "+dbStatus.String, clientIP)
		return nil, false, errors.New("Account is not active")
	}

	if locked := a.loginLocked(dbUserID); locked {
		LogSecurityEvent(ctx, a.pool, dbUserID, "login_blocked", "account locked", clientIP)
		return nil, false, errors.New("Account has been locked due to multiple failed login attempts")
	}

	passwordValid := dbPassword.Valid && dbPassword.String != "" &&
		bcrypt.CompareHashAndPassword([]byte(dbPassword.String), []byte(password)) == nil

	if !passwordValid {
		LogSecurityEvent(ctx, a.pool, dbUserID, "login_failed", "wrong password", clientIP)
		return nil, false, a.recordLoginFailure(ctx, dbUserID, clientIP)
	}

	var roleName, roleCode sql.NullString
	if err := a.pool.QueryRow(ctx,
		`SELECT r.name, r.rolecode FROM user_roles ur JOIN roles r ON ur.role_id = r.id WHERE ur.user_id = $1 ORDER BY ur.id ASC LIMIT 1`,
		dbUserID,
	).Scan(&roleName, &roleCode); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		logger.LogError("[auth] role lookup failed for user %s: %v", dbUserID, err)
		return nil, false, errors.New("internal error")
	}

	var mfaEnabled sql.NullBool
	var mfaSecret sql.NullString
	if err := a.pool.QueryRow(ctx, `SELECT mfa_enabled, mfa_secret FROM users WHERE id = $1`, dbUserID).Scan(&mfaEnabled, &mfaSecret); err != nil {
		logger.LogError("[auth] mfa lookup failed for user %s: %v", dbUserID, err)
		return nil, false, errors.New("internal error")
	}
	mfaPending := mfaEnabled.Valid && mfaEnabled.Bool && mfaSecret.Valid && mfaSecret.String != ""

	sessionID, err := generateSessionID()
	if err != nil {
		return nil, false, errors.New("internal error")
	}
	session := &UserSession{
		SessionID:        sessionID,
		UserID:           dbUserID,
		Name:             dbName,
		Email:            dbEmail,
		Role:             roleName.String,
		RoleCode:         roleCode.String,
		LastLoginTime:    time.Now().Format(time.RFC3339),
		lastActivityUnix: time.Now().Unix(),
		ClientIP:         clientIP,
		IsLoggedIn:       !mfaPending,
		CreatedEntities:  a.loadCreatedEntities(ctx, dbUserID, roleName.String, roleCode.String),
	}

	a.mu.Lock()
	a.forceLogoutUser(dbUserID)
	if a.maxUsers > 0 && len(a.users) >= a.maxUsers {
		a.mu.Unlock()
		return nil, false, errors.New("maximum concurrent users reached")
	}
	a.users[sessionID] = session
	a.userPointers[dbUserID] = session
	if !mfaPending && a.MaxLoginAttempts > 0 {
		delete(a.failedAttempts, dbUserID)
	}
	a.mu.Unlock()

	if mfaPending {
		LogSecurityEvent(ctx, a.pool, dbUserID, "login_mfa_pending", username, clientIP)
		return session, true, nil
	}
	LogSecurityEvent(ctx, a.pool, dbUserID, "login_success", username, clientIP)
	return session, false, nil
}

// loginStatusAllowed admits approved users, and previously approved users whose
// edit or deletion is still waiting for a checker.
func loginStatusAllowed(status sql.NullString, everApproved bool) bool {
	if !status.Valid || strings.EqualFold(status.String, constants.UserStatusApproved) {
		return true
	}
	if !everApproved {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(status.String)) {
	case strings.ToLower(constants.UserStatusPending), "awaiting-approval", "delete-approval":
		return true
	}
	return false
}

func (a *AuthService) loginLocked(userID string) bool {
	if a.MaxLoginAttempts <= 0 {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	fa, ok := a.failedAttempts[userID]
	if !ok || !fa.isLocked {
		return false
	}
	if time.Now().Before(fa.unlockAt) {
		return true
	}
	fa.isLocked = false
	fa.count = 0
	return false
}

func (a *AuthService) recordLoginFailure(ctx context.Context, userID, clientIP string) error {
	if a.MaxLoginAttempts <= 0 {
		return errors.New("Invalid credentials")
	}
	a.mu.Lock()
	fa, ok := a.failedAttempts[userID]
	if !ok {
		fa = &failedAttempt{}
		a.failedAttempts[userID] = fa
	}
	fa.count++
	fa.lastTry = time.Now()
	count := fa.count
	if count >= a.MaxLoginAttempts {
		fa.isLocked = true
		if a.AccountLockDuration > 0 {
			fa.unlockAt = time.Now().Add(time.Duration(a.AccountLockDuration) * time.Minute)
		} else {
			fa.unlockAt = time.Now().Add(100 * 365 * 24 * time.Hour)
		}
	}
	a.mu.Unlock()
	if count >= a.MaxLoginAttempts {
		LogSecurityEvent(ctx, a.pool, userID, "account_locked", "", clientIP)
		return errors.New("Account has been locked due to multiple failed login attempts")
	}
	left := a.MaxLoginAttempts - count
	return fmt.Errorf("Invalid credentials, %d/%d attempts left", left, a.MaxLoginAttempts)
}

func (a *AuthService) Logout(ctx context.Context, UserID string) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	found := false
	for sessionID, session := range a.users {
		if session.UserID == UserID {
			delete(a.users, sessionID)
			delete(a.userPointers, session.UserID)
			found = true
			LogSecurityEvent(ctx, a.pool, UserID, "logout", session.Email, session.ClientIP)
		}
	}
	if !found {
		return errors.New("no active session found for user")
	}
	if OnLogoutHook != nil {
		OnLogoutHook(UserID)
	}
	return nil
}

// forceLogoutUser removes any existing sessions for the user (must be called under lock).
func (a *AuthService) forceLogoutUser(userID string) {
	for sid, session := range a.users {
		if session.UserID == userID {
			go func(uid string) {
				dashboard.SendToUser(uid, []byte(`{"type":"force_logout","reason":"login_from_other_ip"}`))
			}(session.UserID)
			delete(a.users, sid)
			delete(a.userPointers, session.UserID)
			break
		}
	}
}

func (a *AuthService) loadCreatedEntities(ctx context.Context, userID, roleName, roleCode string) []SessionEntity {
	if a.pool == nil {
		return nil
	}

	// Check if user is admin (via ADMIN_USER_IDS or ADMIN_ROLES env vars)
	isAdmin := a.isAdminForEntities(userID, roleName, roleCode)

	if isAdmin {
		// Admin: return ALL level 0 (top-level) entities
		return a.loadAllLevel0Entities(ctx)
	}

	// Non-admin: find the level 0 root ancestor(s) of the entities assigned to this user
	return a.loadUserRootEntities(ctx, userID)
}

// isAdminForEntities checks ADMIN_USER_IDS and ADMIN_ROLES env vars to determine
// if the user should see all level 0 entities. Mirrors the middleware admin_override logic.
func (a *AuthService) isAdminForEntities(userID, roleName, roleCode string) bool {
	// 1. Check ADMIN_USER_IDS
	if raw := strings.TrimSpace(os.Getenv("ADMIN_USER_IDS")); raw != "" {
		for _, id := range strings.Split(raw, ",") {
			if strings.TrimSpace(id) == userID {
				return true
			}
		}
	}

	// 2. Check ADMIN_ROLES against the user's role name and code
	if raw := strings.TrimSpace(os.Getenv("ADMIN_ROLES")); raw != "" {
		for _, r := range strings.Split(raw, ",") {
			rLower := strings.ToLower(strings.TrimSpace(r))
			if rLower == "" {
				continue
			}
			if strings.ToLower(strings.TrimSpace(roleName)) == rLower || strings.ToLower(strings.TrimSpace(roleCode)) == rLower {
				return true
			}
		}
	}

	return false
}

// loadAllLevel0Entities returns all approved, active, level 0 entities from masterentitycash.
func (a *AuthService) loadAllLevel0Entities(ctx context.Context) []SessionEntity {
	rows, err := a.pool.Query(ctx, `
		SELECT
			m.entity_id,
			m.entity_name,
			COALESCE(m.entity_short_name, ''),
			COALESCE(m.parent_entity_name, ''),
			COALESCE(m.entity_level::text, ''),
			COALESCE(m.active_status, ''),
			COALESCE(ae.processing_status, '')
		FROM masterentitycash m
		LEFT JOIN LATERAL (
			SELECT a.processing_status
			FROM auditactionentity a
			WHERE a.entity_id = m.entity_id
			ORDER BY a.requested_at DESC
			LIMIT 1
		) ae ON TRUE
		WHERE m.entity_level = 0
		  AND (m.is_deleted = false OR m.is_deleted IS NULL)
		ORDER BY m.entity_name
	`)
	if err != nil {
		logger.LogError("[auth] failed to load all level 0 entities: %v", err)
		return nil
	}
	defer rows.Close()

	return a.scanSessionEntities(rows)
}

// loadUserRootEntities finds the level 0 root ancestor(s) for entities assigned to a user
// via user_entity_mappings. It walks up the hierarchy via parent_entity_name.
func (a *AuthService) loadUserRootEntities(ctx context.Context, userID string) []SessionEntity {
	// Use a recursive CTE to walk UP from the user's assigned entities to the level 0 roots.
	rows, err := a.pool.Query(ctx, `
		WITH RECURSIVE assigned AS (
			SELECT entity_id, entity_name
			FROM user_entity_mappings
			WHERE user_id = $1
		),
		ancestors AS (
			-- Start from the user's assigned entities
			SELECT m.entity_id, m.entity_name, m.entity_level, m.parent_entity_name
			FROM masterentitycash m
			WHERE m.entity_id IN (SELECT entity_id FROM assigned)
			  AND (m.is_deleted = false OR m.is_deleted IS NULL)

			UNION

			-- Walk up via parent_entity_name
			SELECT p.entity_id, p.entity_name, p.entity_level, p.parent_entity_name
			FROM masterentitycash p
			INNER JOIN ancestors a ON p.entity_name = a.parent_entity_name
			WHERE (p.is_deleted = false OR p.is_deleted IS NULL)
		)
		SELECT DISTINCT
			a.entity_id,
			a.entity_name,
			COALESCE(m.entity_short_name, ''),
			COALESCE(m.parent_entity_name, ''),
			COALESCE(m.entity_level::text, ''),
			COALESCE(m.active_status, ''),
			COALESCE(ae.processing_status, '')
		FROM ancestors a
		JOIN masterentitycash m ON m.entity_id = a.entity_id
		LEFT JOIN LATERAL (
			SELECT x.processing_status
			FROM auditactionentity x
			WHERE x.entity_id = m.entity_id
			ORDER BY x.requested_at DESC
			LIMIT 1
		) ae ON TRUE
		WHERE a.entity_level = 0
		  AND COALESCE(ae.processing_status, 'REJECTED') = 'APPROVED'
		ORDER BY a.entity_name
	`, userID)
	if err != nil {
		logger.LogError("[auth] failed to load root entities for user %s: %v", userID, err)
		return nil
	}
	defer rows.Close()

	return a.scanSessionEntities(rows)
}

// scanSessionEntities reads rows into a []SessionEntity slice.
func (a *AuthService) scanSessionEntities(rows pgx.Rows) []SessionEntity {
	entities := make([]SessionEntity, 0)
	for rows.Next() {
		var entity SessionEntity
		if err := rows.Scan(
			&entity.EntityID,
			&entity.EntityName,
			&entity.EntityShortName,
			&entity.ParentEntityName,
			&entity.EntityLevel,
			&entity.ActiveStatus,
			&entity.ApprovalStatus,
		); err != nil {
			logger.LogError("[auth] failed to scan session entity: %v", err)
			return entities
		}
		entities = append(entities, entity)
	}

	if err := rows.Err(); err != nil {
		logger.LogError("[auth] failed to iterate session entities: %v", err)
	}

	return entities
}

// HashPassword hashes a plaintext password with bcrypt.
func HashPassword(plaintext string) (string, error) {
	hashed, err := bcrypt.GenerateFromPassword([]byte(plaintext), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}
	return string(hashed), nil
}

var globalAuthService *AuthService

// OnLogoutHook, if set, is called after a user session is removed.
// Register from outside this package (e.g. catalog.ClearSystemNotifications)
// to avoid circular imports.  It is called with the departing UserID.
var OnLogoutHook func(userID string)

func SetGlobalAuthService(svc *AuthService) {
	globalAuthService = svc
}

// Pool returns the pgx pool used by this AuthService.
func (a *AuthService) Pool() *pgxpool.Pool {
	return a.pool
}

func GetActiveSessions() []*UserSession {
	if globalAuthService == nil {
		return nil
	}
	return globalAuthService.GetActiveSessions()
}

func (a *AuthService) GetActiveSessions() []*UserSession {
	a.mu.Lock()
	defer a.mu.Unlock()
	sessions := make([]*UserSession, 0, len(a.users))
	for _, s := range a.users {
		sessions = append(sessions, s)
	}
	return sessions
}

// Touch marks the session as active now, resetting its idle timer.
func (s *UserSession) Touch() {
	if s == nil {
		return
	}
	atomic.StoreInt64(&s.lastActivityUnix, time.Now().Unix())
}

// lastSeen returns the session's last activity time, falling back to its login
// time for sessions created before idle tracking was in place.
func (s *UserSession) lastSeen() (time.Time, bool) {
	if ts := atomic.LoadInt64(&s.lastActivityUnix); ts > 0 {
		return time.Unix(ts, 0), true
	}
	if s.LastLoginTime == "" {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339, s.LastLoginTime)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

func (a *AuthService) sessionCleaner() {
	period := time.Duration(a.SessionCleanerPeriod) * time.Minute
	if period <= 0 {
		return
	}
	ticker := time.NewTicker(period)
	defer ticker.Stop()
	for {
		select {
		case <-a.stopCh:
			return
		case <-ticker.C:
			if a.SessionTimeout > 0 {
				cutoff := time.Now().Add(-time.Duration(a.SessionTimeout) * time.Minute)
				a.mu.Lock()
				for sid, sess := range a.users {
					t, ok := sess.lastSeen()
					if !ok || !t.Before(cutoff) {
						continue
					}
					// Send SSE notification before removing session
					go func(userID string) {
						dashboard.SendToUser(userID, []byte(`{"type":"session_expired","reason":"session_expired"}`))
					}(sess.UserID)

					delete(a.users, sid)
					delete(a.userPointers, sess.UserID)
					if logger.GlobalLogger != nil {
						logger.GlobalLogger.LogAudit(fmt.Sprintf("Session expired and removed: %s (user: %s)", sid, sess.Email))
					}
				}
				a.mu.Unlock()
			}
		}
	}
}

func generateSessionID() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		logger.LogError("[auth] failed to generate secure session ID: %v", err)
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func (a *AuthService) LogDifferentIPRequest(userID string, clientIP string) {
	a.mu.Lock()
	defer a.mu.Unlock()

	for _, s := range a.users {
		if s.UserID == userID && s.IsLoggedIn {
			if s.ClientIP != clientIP {
				if logger.GlobalLogger != nil {
					logger.GlobalLogger.LogAudit(fmt.Sprintf("User %s made a request from different IP: %s (session IP: %s)", userID, clientIP, s.ClientIP))
				}
			}
			break
		}
	}
}
