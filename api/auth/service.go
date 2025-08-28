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

	for _, session := range a.users {
		if session.Email == username && session.IsLoggedIn {
			session.LastLoginTime = time.Now().Format(time.RFC3339)
			session.ClientIP = clientIP
			if logger.GlobalLogger != nil {
				logger.GlobalLogger.LogAudit(fmt.Sprintf("User %s re-logged in, Returning Existing session", username))
			}
			return session, nil
		}
	}

	if len(a.users) >= a.maxUsers {
		return nil, errors.New("maximum concurrent users reached")
	}

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
		return nil, errors.New("invalid credentials or user not found")
	}

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

func (a *AuthService) Logout(sessionID string) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	session, exists := a.users[sessionID]
	if !exists {
		return errors.New("session not found")
	}
	delete(a.users, sessionID)
	delete(a.userPointers, session.UserID)

	if logger.GlobalLogger != nil {
		logger.GlobalLogger.LogAudit("User logged out: " + session.UserID)
	}

	return nil
}

var globalAuthService *AuthService

// SetGlobalAuthService sets the global AuthService instance
func SetGlobalAuthService(svc *AuthService) {
	globalAuthService = svc
}

// GetActiveSessions returns active sessions from the global AuthService
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
			// session expiry logic can be added here
		}
	}
}

func generateSessionID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

// DB initialization helper
func InitDB() (*sql.DB, error) {
	connStr := fmt.Sprintf(
		"user=%s password=%s host=%s port=%s dbname=%s sslmode=disable",
		"postgres.sgryutycrupiuhovmbfo",
		"NyneOS@1234$",
		"aws-0-ap-south-1.pooler.supabase.com",
		"5432",
		"postgres",
	)
	return sql.Open("postgres", connStr)
}
