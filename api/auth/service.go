package auth

import (

	"CimplrCorpSaas/internal/logger"
	"CimplrCorpSaas/internal/serviceiface"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
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
	db                  *sql.DB
	maxUsers            int
	SessionTimeout      int
	MaxLoginAttempts    int
	AccountLockDuration int
	redisClient         *redis.Client
}

func NewAuthService(db *sql.DB, maxUsers int, SessionTimeout int, MaxLoginAttempts int, AccountLockDuration int, SessionCleanerPeriod int) serviceiface.Service {
	return &AuthService{
		db:                  db,
		maxUsers:            maxUsers,
		SessionTimeout:      SessionTimeout,
		MaxLoginAttempts:    MaxLoginAttempts,
		AccountLockDuration: AccountLockDuration,
	}
}

func (a *AuthService) Name() string { return "auth" }

func (a *AuthService) Start() error {
	return nil
}

func (a *AuthService) Stop() error {
	return nil
}

func (a *AuthService) SetRedisClient(client *redis.Client) {
	a.redisClient = client
}

func (a *AuthService) Login(username, password string, clientIP string) (*UserSession, error) {
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
		}
		return nil, errors.New("internal error")
	}

	// Check failed login attempts from Redis
	if a.MaxLoginAttempts > 0 {
		ctx := context.Background()
		faKey := fmt.Sprintf("failed:%s", dbUserID)
		faData, _ := a.redisClient.Get(ctx, faKey).Result()
		if faData != "" {
			var fa failedAttempt
			if err := json.Unmarshal([]byte(faData), &fa); err == nil {
				if fa.isLocked && time.Now().Before(fa.unlockAt) {
					return nil, errors.New("Account has been locked due to multiple failed login attempts")
				}
			}
		}
	}

	// Check password
	if !dbPassword.Valid || dbPassword.String != password {
		if a.MaxLoginAttempts > 0 {
			ctx := context.Background()
			faKey := fmt.Sprintf("failed:%s", dbUserID)
			faData, _ := a.redisClient.Get(ctx, faKey).Result()
			
			var fa failedAttempt
			if faData != "" {
				json.Unmarshal([]byte(faData), &fa)
			} else {
				fa = failedAttempt{count: 0}
			}

			fa.count++
			fa.lastTry = time.Now()
			
			if fa.count >= a.MaxLoginAttempts {
				fa.isLocked = true
				lockDuration := time.Duration(a.AccountLockDuration) * time.Minute
				if lockDuration <= 0 {
					lockDuration = 100 * 365 * 24 * time.Hour
				}
				fa.unlockAt = time.Now().Add(lockDuration)
				
				faJSON, _ := json.Marshal(fa)
				a.redisClient.Set(ctx, faKey, string(faJSON), lockDuration)
				return nil, errors.New("Account has been locked due to multiple failed login attempts")
			}

			faJSON, _ := json.Marshal(fa)
			a.redisClient.Set(ctx, faKey, string(faJSON), 30*time.Minute)
			
			attemptsLeft := a.MaxLoginAttempts - fa.count
			if attemptsLeft < 0 {
				attemptsLeft = 0
			}
			return nil, fmt.Errorf("Invalid credentials, %d/%d attempts left", attemptsLeft, a.MaxLoginAttempts)
		}
		return nil, errors.New("Invalid credentials")
	}

	// Password OK - get role and create session
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

	// Store session in Redis
	if a.redisClient != nil {
		ctx := context.Background()
		sessionJSON, _ := json.Marshal(session)
		ttl := time.Duration(a.SessionTimeout) * time.Minute
		if ttl <= 0 {
			ttl = 60 * time.Minute
		}
		a.redisClient.Set(ctx, fmt.Sprintf("session:%s", sessionID), string(sessionJSON), ttl)

		// Clear failed attempts
		if a.MaxLoginAttempts > 0 {
			a.redisClient.Del(ctx, fmt.Sprintf("failed:%s", dbUserID))
		}
	}

	if logger.GlobalLogger != nil {
		logger.GlobalLogger.LogAudit(fmt.Sprintf("User logged in: %s", username))
	}

	return session, nil
}

func (a *AuthService) Logout(UserID string) error {
	if a.redisClient == nil {
		return errors.New("Redis not available")
	}
	ctx := context.Background()
	// Scan all session keys for this userID and delete them
	iter := a.redisClient.Scan(ctx, 0, "session:*", 0).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()
		data, err := a.redisClient.Get(ctx, key).Result()
		if err != nil {
			continue
		}
		var session UserSession
		if err := json.Unmarshal([]byte(data), &session); err == nil {
			if session.UserID == UserID {
				a.redisClient.Del(ctx, key)
				if logger.GlobalLogger != nil {
					logger.GlobalLogger.LogAudit(fmt.Sprintf("User logged out: %s (%s)", session.UserID, session.Email))
				}
			}
		}
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
	if a.redisClient == nil {
		return nil
	}
	ctx := context.Background()
	var sessions []*UserSession
	iter := a.redisClient.Scan(ctx, 0, "session:*", 0).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()
		data, err := a.redisClient.Get(ctx, key).Result()
		if err != nil {
			continue
		}
		var session UserSession
		if err := json.Unmarshal([]byte(data), &session); err == nil {
			sessions = append(sessions, &session)
		}
	}
	return sessions
}

func generateSessionID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

func (a *AuthService) LogDifferentIPRequest(userID string, clientIP string) {
	if a.redisClient == nil {
		return
	}
	ctx := context.Background()
	iter := a.redisClient.Scan(ctx, 0, "session:*", 0).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()
		data, err := a.redisClient.Get(ctx, key).Result()
		if err != nil {
			continue
		}
		var session UserSession
		if err := json.Unmarshal([]byte(data), &session); err == nil {
			if session.UserID == userID && session.IsLoggedIn {
				if session.ClientIP != clientIP {
					if logger.GlobalLogger != nil {
						logger.GlobalLogger.LogAudit(fmt.Sprintf("User %s made a request from different IP: %s (session IP: %s)", userID, clientIP, session.ClientIP))
					}
				}
				break
			}
		}
	}
}
