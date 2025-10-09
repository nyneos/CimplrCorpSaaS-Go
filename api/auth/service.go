package auth

import (
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

type AuthService struct {
	db           *sql.DB
	maxUsers     int
	users        map[string]*UserSession
	userPointers map[string]*UserSession
	mu           sync.Mutex
	stopCh       chan struct{}
}

func NewAuthService(db *sql.DB, maxUsers int) serviceiface.Service {
	return &AuthService{
		db:           db,
		maxUsers:     maxUsers,
		users:        make(map[string]*UserSession),
		userPointers: make(map[string]*UserSession),
		stopCh:       make(chan struct{}),
	}
}

func (a *AuthService) Name() string { return "auth" }

func (a *AuthService) Start() error {
	go a.sessionCleaner()
	return nil
}

func (a *AuthService) Stop() error {
	close(a.stopCh)
	return nil
}

func (a *AuthService) Login(username, password string, clientIP string) (*UserSession, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Check if user already has active sessions (optional logging only)
	for _, session := range a.users {
		if session.Email == username && session.IsLoggedIn {
			if logger.GlobalLogger != nil {
				logger.GlobalLogger.LogAudit(fmt.Sprintf(
					"User %s is logging in from another device/IP", username))
			}
			// Do NOT return here; allow new session creation
			break
		}
	}

	// Enforce maximum concurrent sessions
	if len(a.users) >= a.maxUsers {
		if logger.GlobalLogger != nil {
			logger.GlobalLogger.LogAudit("[ERROR] maximum concurrent users reached for login attempt: " + username)
		}
		return nil, errors.New("maximum concurrent users reached")
	}

	// Fetch user and role from DB
	var (
		userID, name, email        string
		roleID, roleName, roleCode sql.NullString
		userStatus, roleStatus     sql.NullString
	)

	query := `
    SELECT
        u.id AS user_id,
        u.employee_name,
        u.email,
        u.status AS user_status,
        r.id AS role_id,
        r.name AS role_name,
        r.rolecode,
        r.status AS role_status
    FROM users u
    LEFT JOIN user_roles ur ON u.id = ur.user_id
    LEFT JOIN roles r ON ur.role_id = r.id
    WHERE u.email = $1 AND u.password = $2
    `

	err := a.db.QueryRow(query, username, password).Scan(
		&userID, &name, &email, &userStatus,
		&roleID, &roleName, &roleCode, &roleStatus,
	)
	if err != nil {
		if logger.GlobalLogger != nil {
			logger.GlobalLogger.LogAudit(fmt.Sprintf("Login DB error for %s: %v", username, err))
		} else {
			fmt.Println("Login DB error:", err)
		}
		return nil, errors.New("invalid credentials or user not found")
	}

	// Create new session
	sessionID := generateSessionID()
	session := &UserSession{
		SessionID:     sessionID,
		UserID:        userID,
		Name:          name,
		Email:         email,
		Role:          roleName.String,
		RoleCode:      roleCode.String,
		LastLoginTime: time.Now().Format(time.RFC3339),
		ClientIP:      clientIP,
		IsLoggedIn:    true,
	}

	a.users[sessionID] = session
	a.userPointers[userID] = session

	if logger.GlobalLogger != nil {
		logger.GlobalLogger.LogAudit(fmt.Sprintf("User logged in: %s", username))
	}

	return session, nil
}


func (a *AuthService) Logout(UserID string) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	found := false
	// Remove all sessions for this user
	for sessionID, session := range a.users {
		if session.UserID == UserID {
			delete(a.users, sessionID)
			delete(a.userPointers, session.UserID)
			found = true
			if logger.GlobalLogger != nil {
				logger.GlobalLogger.LogAudit("User logged out: " + session.UserID)
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
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-a.stopCh:
			return
		case <-ticker.C:
		}
	}
}

func generateSessionID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}



