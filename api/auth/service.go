package auth

import (
	"CimplrCorpSaas/internal/dashboard"
	"CimplrCorpSaas/internal/logger"
	"CimplrCorpSaas/internal/serviceiface"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

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
	SessionID       string
	UserID          string
	Name            string
	Email           string
	Role            string
	RoleCode        string
	LastLoginTime   string
	ClientIP        string
	IsLoggedIn      bool
	CreatedEntities []SessionEntity `json:"created_entities,omitempty"`
}

type failedAttempt struct {
	count    int
	lastTry  time.Time
	isLocked bool
	unlockAt time.Time
}

type AuthService struct {
	db                   *sql.DB
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

func NewAuthService(db *sql.DB, maxUsers int, SessionTimeout int, MaxLoginAttempts int, AccountLockDuration int, SessionCleanerPeriod int) serviceiface.Service {
	return &AuthService{
		db:                   db,
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
	StartTokenCleanup(a.db)
	return nil
}

func (a *AuthService) Stop() error {
	close(a.stopCh)
	return nil
}

// Login authenticates a user by email and password, returning the session and
// an mfaPending flag. Passwords are compared using bcrypt; if the stored hash
// is a legacy plaintext value that matches, it is automatically upgraded to bcrypt.
func (a *AuthService) Login(username, password string, clientIP string) (*UserSession, bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	var dbUserID, dbName, dbEmail string
	var dbPassword sql.NullString
	var dbStatus sql.NullString

	err := a.db.QueryRow(
		`SELECT id, employee_name, email, password, status FROM users WHERE email = $1`, username,
	).Scan(&dbUserID, &dbName, &dbEmail, &dbPassword, &dbStatus)
	if err != nil {
		if err == sql.ErrNoRows {
			LogSecurityEvent(a.db, "", "login_failed", "unknown user: "+username, clientIP)
			return nil, false, errors.New("Invalid Username or Password")
		}
		return nil, false, errors.New("internal error")
	}

	// Account lock check
	if a.MaxLoginAttempts > 0 {
		if fa, ok := a.failedAttempts[dbUserID]; ok {
			if fa.isLocked && time.Now().Before(fa.unlockAt) {
				LogSecurityEvent(a.db, dbUserID, "login_blocked", "account locked", clientIP)
				return nil, false, errors.New("Account has been locked due to multiple failed login attempts")
			}
			if fa.isLocked && time.Now().After(fa.unlockAt) {
				fa.isLocked = false
				fa.count = 0
			}
		}
	}

	// Password verification: bcrypt first, then plaintext fallback with auto-upgrade
	passwordValid := false
	if dbPassword.Valid && dbPassword.String != "" {
		if bcrypt.CompareHashAndPassword([]byte(dbPassword.String), []byte(password)) == nil {
			passwordValid = true
		} else if dbPassword.String == password {
			// Legacy plaintext match — upgrade to bcrypt
			passwordValid = true
			if hashed, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost); err == nil {
				_, _ = a.db.Exec(`UPDATE users SET password = $1 WHERE id = $2`, string(hashed), dbUserID)
				LogSecurityEvent(a.db, dbUserID, "password_upgraded", "plaintext→bcrypt", clientIP)
			}
		}
	}

	if !passwordValid {
		LogSecurityEvent(a.db, dbUserID, "login_failed", "wrong password", clientIP)
		if a.MaxLoginAttempts > 0 {
			fa, ok := a.failedAttempts[dbUserID]
			if !ok {
				fa = &failedAttempt{count: 0}
				a.failedAttempts[dbUserID] = fa
			}
			fa.count++
			fa.lastTry = time.Now()
			if fa.count >= a.MaxLoginAttempts {
				fa.isLocked = true
				if a.AccountLockDuration > 0 {
					fa.unlockAt = time.Now().Add(time.Duration(a.AccountLockDuration) * time.Minute)
				} else {
					fa.unlockAt = time.Now().Add(100 * 365 * 24 * time.Hour)
				}
				LogSecurityEvent(a.db, dbUserID, "account_locked", "", clientIP)
				return nil, false, errors.New("Account has been locked due to multiple failed login attempts")
			}
			attemptsLeft := a.MaxLoginAttempts - fa.count
			if attemptsLeft < 0 {
				attemptsLeft = 0
			}
			return nil, false, fmt.Errorf("Invalid credentials, %d/%d attempts left", attemptsLeft, a.MaxLoginAttempts)
		}
		return nil, false, errors.New("Invalid credentials")
	}

	// Force-logout existing sessions
	a.forceLogoutUser(dbUserID)

	if a.maxUsers > 0 && len(a.users) >= a.maxUsers {
		return nil, false, errors.New("maximum concurrent users reached")
	}

	var roleName, roleCode sql.NullString
	_ = a.db.QueryRow(
		`SELECT r.name, r.rolecode FROM user_roles ur JOIN roles r ON ur.role_id = r.id WHERE ur.user_id = $1 LIMIT 1`,
		dbUserID,
	).Scan(&roleName, &roleCode)

	sessionID := generateSessionID()
	session := &UserSession{
		SessionID:       sessionID,
		UserID:          dbUserID,
		Name:            dbName,
		Email:           dbEmail,
		Role:            roleName.String,
		RoleCode:        roleCode.String,
		LastLoginTime:   time.Now().Format(time.RFC3339),
		ClientIP:        clientIP,
		IsLoggedIn:      true,
		CreatedEntities: a.loadCreatedEntities(dbUserID, roleName.String, roleCode.String),
	}

	// Check MFA — only gate login if BOTH enabled AND secret is configured
	var mfaEnabled sql.NullBool
	var mfaSecret sql.NullString
	_ = a.db.QueryRow(`SELECT mfa_enabled, mfa_secret FROM users WHERE id = $1`, dbUserID).Scan(&mfaEnabled, &mfaSecret)
	if mfaEnabled.Valid && mfaEnabled.Bool && mfaSecret.Valid && mfaSecret.String != "" {
		session.IsLoggedIn = false
		a.users[sessionID] = session
		a.userPointers[dbUserID] = session
		LogSecurityEvent(a.db, dbUserID, "login_mfa_pending", username, clientIP)
		return session, true, nil
	}

	a.users[sessionID] = session
	a.userPointers[dbUserID] = session

	if a.MaxLoginAttempts > 0 {
		delete(a.failedAttempts, dbUserID)
	}

	LogSecurityEvent(a.db, dbUserID, "login_success", username, clientIP)
	return session, false, nil
}

func (a *AuthService) Logout(UserID string) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	found := false
	for sessionID, session := range a.users {
		if session.UserID == UserID {
			delete(a.users, sessionID)
			delete(a.userPointers, session.UserID)
			found = true
			LogSecurityEvent(a.db, UserID, "logout", session.Email, session.ClientIP)
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

func (a *AuthService) loadCreatedEntities(userID, roleName, roleCode string) []SessionEntity {
	if a.db == nil {
		return nil
	}

	// Check if user is admin (via ADMIN_USER_IDS or ADMIN_ROLES env vars)
	isAdmin := a.isAdminForEntities(userID, roleName, roleCode)

	if isAdmin {
		// Admin: return ALL level 0 (top-level) entities
		return a.loadAllLevel0Entities()
	}

	// Non-admin: find the level 0 root ancestor(s) of the entities assigned to this user
	return a.loadUserRootEntities(userID)
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
func (a *AuthService) loadAllLevel0Entities() []SessionEntity {
	rows, err := a.db.Query(`
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
		log.Printf("[auth] failed to load all level 0 entities: %v", err)
		return nil
	}
	defer rows.Close()

	return a.scanSessionEntities(rows)
}

// loadUserRootEntities finds the level 0 root ancestor(s) for entities assigned to a user
// via user_entity_mappings. It walks up the hierarchy via parent_entity_name.
func (a *AuthService) loadUserRootEntities(userID string) []SessionEntity {
	// Use a recursive CTE to walk UP from the user's assigned entities to the level 0 roots.
	rows, err := a.db.Query(`
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
		log.Printf("[auth] failed to load root entities for user %s: %v", userID, err)
		return nil
	}
	defer rows.Close()

	return a.scanSessionEntities(rows)
}

// scanSessionEntities reads rows into a []SessionEntity slice.
func (a *AuthService) scanSessionEntities(rows *sql.Rows) []SessionEntity {
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
			log.Printf("[auth] failed to scan session entity: %v", err)
			return entities
		}
		entities = append(entities, entity)
	}

	if err := rows.Err(); err != nil {
		log.Printf("[auth] failed to iterate session entities: %v", err)
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

// DB returns the database handle used by this AuthService.
func (a *AuthService) DB() *sql.DB {
	return a.db
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
					if sess.LastLoginTime != "" {
						if t, err := time.Parse(time.RFC3339, sess.LastLoginTime); err == nil {
							if t.Before(cutoff) {
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
						}
					}
				}
				a.mu.Unlock()
			}
		}
	}
}

func generateSessionID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
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
