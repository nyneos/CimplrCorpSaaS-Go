package auth

import (
	"CimplrCorpSaas/internal/dashboard"
	"CimplrCorpSaas/internal/logger"
	"CimplrCorpSaas/internal/serviceiface"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"
)

type UserSession struct {
	SessionID     string
	UserID        string
	Name          string
	Email         string
	Role          string
	RoleCode      string
	LastLoginTime string
	ClientIP      string
	IsLoggedIn    bool
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
	return nil
}

func (a *AuthService) Stop() error {
	close(a.stopCh)
	return nil
}

func (a *AuthService) Login(username, password string, clientIP string) (*UserSession, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	var dbUserID string
	var dbName string
	var dbEmail string
	var dbPassword sql.NullString
	var dbStatus sql.NullString

	err := a.db.QueryRow(`SELECT id, employee_name, email, password, status FROM users WHERE email = $1`, username).
		Scan(&dbUserID, &dbName, &dbEmail, &dbPassword, &dbStatus)
	if err != nil {
		if err == sql.ErrNoRows {
			if logger.GlobalLogger != nil {
				logger.GlobalLogger.LogAudit(fmt.Sprintf("Login attempt for unknown user: %s", username))
			}
			return nil, errors.New("Invalid Username or Password")
		}
		if logger.GlobalLogger != nil {
			logger.GlobalLogger.LogAudit(fmt.Sprintf("Login DB error for %s: %v", username, err))
		} else {
			fmt.Println("Login DB error:", err)
		}
		return nil, errors.New("internal error")
	}

	// Check if user is already logged in - force logout from all sessions
	for sid, session := range a.users {
		if session.IsLoggedIn && session.UserID == dbUserID {
			if logger.GlobalLogger != nil {
				if session.ClientIP == clientIP {
					logger.GlobalLogger.LogAudit(fmt.Sprintf("User %s logging in again from same IP - validating credentials", dbEmail))
				} else {
					logger.GlobalLogger.LogAudit(fmt.Sprintf("User %s is logging in from another device/IP", dbEmail))
				}
			}

			go func(oldUserID string, reason string) {

				dashboard.SendToUser(oldUserID, []byte(`{"type":"force_logout","reason":"login_from_other_ip"}`))

			}(session.UserID, "re_login")

			delete(a.users, sid)
			delete(a.userPointers, session.UserID)
			break
		}
	}

	if a.maxUsers > 0 && len(a.users) >= a.maxUsers {
		if logger.GlobalLogger != nil {
			logger.GlobalLogger.LogAudit("[ERROR] maximum concurrent users reached for login attempt: " + dbEmail)
		}
		return nil, errors.New("maximum concurrent users reached")
	}

	if a.MaxLoginAttempts > 0 {
		if fa, ok := a.failedAttempts[dbUserID]; ok {
			if fa.isLocked && time.Now().Before(fa.unlockAt) {
				return nil, errors.New("Account has been locked due to multiple failed login attempts")
			}

			if fa.isLocked && time.Now().After(fa.unlockAt) {
				fa.isLocked = false
				fa.count = 0
			}
		}
	}

	if !dbPassword.Valid || dbPassword.String != password {
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
				return nil, errors.New("Account has been locked due to multiple failed login attempts")
			}
			attemptsLeft := a.MaxLoginAttempts - fa.count
			if attemptsLeft < 0 {
				attemptsLeft = 0
			}
			return nil, fmt.Errorf("Invalid credentials, %d/%d attempts left", attemptsLeft, a.MaxLoginAttempts)
		}
		return nil, errors.New("Invalid credentials")
	}

	var roleID, roleName, roleCode sql.NullString
	_ = a.db.QueryRow(`SELECT r.id, r.name, r.rolecode FROM user_roles ur JOIN roles r ON ur.role_id = r.id WHERE ur.user_id = $1 LIMIT 1`, dbUserID).
		Scan(&roleID, &roleName, &roleCode)

	sessionID := generateSessionID()
	session := &UserSession{
		SessionID:     sessionID,
		UserID:        dbUserID,
		Name:          dbName,
		Email:         dbEmail,
		Role:          roleName.String,
		RoleCode:      roleCode.String,
		LastLoginTime: time.Now().Format(time.RFC3339),
		ClientIP:      clientIP,
		IsLoggedIn:    true,
	}

	a.users[sessionID] = session
	a.userPointers[dbUserID] = session

	if a.MaxLoginAttempts > 0 {
		delete(a.failedAttempts, dbUserID)
	}

	if logger.GlobalLogger != nil {
		logger.GlobalLogger.LogAudit(fmt.Sprintf("User logged in: %s", username))
	}

	return session, nil
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
			if logger.GlobalLogger != nil {
				logger.GlobalLogger.LogAudit(fmt.Sprintf("User logged out: %s (%s)", session.UserID, session.Email))
			}
		}
	}
	if !found {
		return errors.New("no active session found for user")
	}
	return nil
}

var globalAuthService *AuthService

func SetGlobalAuthService(svc *AuthService) {
	globalAuthService = svc
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
