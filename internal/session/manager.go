package session

import (
	"sync"
	"time"
)

type Session struct {
	ID        string
	UserID    string
	CreatedAt time.Time
	ExpiresAt time.Time
}

type Manager struct {
	sessions map[string]*Session
	mu       sync.Mutex
}

func NewManager() *Manager {
	return &Manager{
		sessions: make(map[string]*Session),
	}
}

func (m *Manager) CreateSession(userID string, duration time.Duration) *Session {
	m.mu.Lock()
	defer m.mu.Unlock()

	session := &Session{
		ID:        generateSessionID(),
		UserID:    userID,
		CreatedAt: time.Now(),
		ExpiresAt: time.Now().Add(duration),
	}
	m.sessions[session.ID] = session
	return session
}

func (m *Manager) GetSession(sessionID string) (*Session, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	session, exists := m.sessions[sessionID]
	return session, exists
}

func (m *Manager) DeleteSession(sessionID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.sessions, sessionID)
}

func (m *Manager) CleanupExpiredSessions() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for id, session := range m.sessions {
		if time.Now().After(session.ExpiresAt) {
			delete(m.sessions, id)
		}
	}
}

func generateSessionID() string {
	// Implementation for generating a unique session ID
	return "unique-session-id" // Placeholder
}