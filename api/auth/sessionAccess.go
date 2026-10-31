package auth

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"time"
)

const (
	streamTicketTTL = 60 * time.Second
	mfaTicketTTL    = 5 * time.Minute
)

type streamTicket struct {
	sessionID string
	expiresAt time.Time
}

var (
	streamTicketsMu sync.Mutex
	streamTickets   = map[string]streamTicket{}
)

func SessionByID(sessionID string) *UserSession {
	if globalAuthService == nil || sessionID == "" {
		return nil
	}
	return globalAuthService.SessionByID(sessionID)
}

func (a *AuthService) SessionByID(sessionID string) *UserSession {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.users[sessionID]
}

func LoggedInSessionForUser(userID string) *UserSession {
	if globalAuthService == nil || userID == "" {
		return nil
	}
	a := globalAuthService
	a.mu.Lock()
	defer a.mu.Unlock()
	s := a.userPointers[userID]
	if s == nil || !s.IsLoggedIn {
		return nil
	}
	return s
}

func (s *UserSession) LoggedIn() bool {
	if s == nil || globalAuthService == nil {
		return false
	}
	globalAuthService.mu.Lock()
	defer globalAuthService.mu.Unlock()
	return s.IsLoggedIn
}

func (a *AuthService) LogoutSession(ctx context.Context, sessionID string) error {
	a.mu.Lock()
	s, ok := a.users[sessionID]
	if ok {
		delete(a.users, sessionID)
		if a.userPointers[s.UserID] == s {
			delete(a.userPointers, s.UserID)
		}
	}
	a.mu.Unlock()
	if !ok {
		return errors.New("no active session found")
	}
	LogSecurityEvent(ctx, a.pool, s.UserID, "logout", s.Email, s.ClientIP)
	if OnLogoutHook != nil {
		OnLogoutHook(s.UserID)
	}
	return nil
}

func (a *AuthService) RevokeUserSessions(userID string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for sid, s := range a.users {
		if s.UserID == userID {
			delete(a.users, sid)
		}
	}
	delete(a.userPointers, userID)
}

func RevokeUserSessions(userID string) {
	if globalAuthService != nil {
		globalAuthService.RevokeUserSessions(userID)
	}
}

func IssueStreamTicket(sessionID string) (string, error) {
	return issueTicket(sessionID, streamTicketTTL)
}

func IssueMFATicket(sessionID string) (string, error) {
	return issueTicket(sessionID, mfaTicketTTL)
}

func issueTicket(sessionID string, ttl time.Duration) (string, error) {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	t := hex.EncodeToString(b)
	now := time.Now()
	streamTicketsMu.Lock()
	for k, v := range streamTickets {
		if now.After(v.expiresAt) {
			delete(streamTickets, k)
		}
	}
	streamTickets[t] = streamTicket{sessionID: sessionID, expiresAt: now.Add(ttl)}
	streamTicketsMu.Unlock()
	return t, nil
}

func RedeemStreamTicket(ticket string) *UserSession {
	s := ticketSession(ticket, true)
	if !s.LoggedIn() {
		return nil
	}
	return s
}

func ticketSession(ticket string, consume bool) *UserSession {
	if ticket == "" {
		return nil
	}
	streamTicketsMu.Lock()
	st, ok := streamTickets[ticket]
	if consume {
		delete(streamTickets, ticket)
	}
	streamTicketsMu.Unlock()
	if !ok || time.Now().After(st.expiresAt) {
		return nil
	}
	return SessionByID(st.sessionID)
}

func consumeTicket(ticket string) {
	streamTicketsMu.Lock()
	delete(streamTickets, ticket)
	streamTicketsMu.Unlock()
}
