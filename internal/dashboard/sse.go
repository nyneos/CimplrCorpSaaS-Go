package dashboard

import (
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/internal/logger"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const sseClientBuffer = 64

type SSEClient struct {
	userID    string
	out       chan []byte
	done      chan struct{}
	closeOnce sync.Once
}

func (c *SSEClient) close() {
	c.closeOnce.Do(func() { close(c.done) })
}

// enqueue queues a frame without blocking; a full queue closes the client.
func (c *SSEClient) enqueue(frame []byte) bool {
	select {
	case <-c.done:
		return false
	default:
	}
	select {
	case c.out <- frame:
		return true
	default:
		c.close()
		return false
	}
}

type SSEServer struct {
	mu         sync.RWMutex
	clients    map[string]*SSEClient
	pingTicker *time.Ticker
	stopCh     chan struct{}
	stopOnce   sync.Once
	// OnConnect is called in a goroutine after a user connects.
	// Typically wired to jobs.PushUnreadCountToUser to avoid an import cycle.
	OnConnect func(userID string)
	// Authenticate resolves the connecting user; ok=false rejects the request.
	Authenticate func(r *http.Request) (userID string, ok bool)
	// AllowedOrigin is sent as Access-Control-Allow-Origin when set.
	AllowedOrigin string
}

var globalSSEServer *SSEServer

func NewSSEServer(pool *pgxpool.Pool) *SSEServer {
	s := &SSEServer{
		clients: make(map[string]*SSEClient),
		stopCh:  make(chan struct{}),
	}
	globalSSEServer = s

	s.pingTicker = time.NewTicker(30 * time.Second)
	go s.pingClients()

	return s
}

func GetSSEServer() *SSEServer {
	return globalSSEServer
}

func encodeFrame(data interface{}) ([]byte, error) {
	b, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	return []byte(fmt.Sprintf("data: %s\n\n", b)), nil
}

// HandleSSE handles SSE connections. Only this goroutine writes to the response.
func (s *SSEServer) HandleSSE(w http.ResponseWriter, r *http.Request) {
	if s.AllowedOrigin != "" {
		w.Header().Set(constants.HeaderAccessControlAllowOrigin, s.AllowedOrigin)
		w.Header().Set("Vary", "Origin")
	}
	w.Header().Set(constants.HeaderAccessControlAllowHeaders, "Cache-Control")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	if s.Authenticate == nil {
		http.Error(w, constants.ErrInvalidSession, http.StatusUnauthorized)
		return
	}
	userID, ok := s.Authenticate(r)
	if !ok || userID == "" {
		http.Error(w, constants.ErrInvalidSession, http.StatusUnauthorized)
		return
	}

	w.Header().Set(constants.ContentTypeText, "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	client := &SSEClient{
		userID: userID,
		out:    make(chan []byte, sseClientBuffer),
		done:   make(chan struct{}),
	}

	s.mu.Lock()
	if existing, exists := s.clients[userID]; exists {
		existing.close()
	}
	s.clients[userID] = client
	s.mu.Unlock()

	logger.LogInfo("[SSE] connected user=%s", userID)

	defer func() {
		s.mu.Lock()
		if s.clients[userID] == client {
			delete(s.clients, userID)
		}
		s.mu.Unlock()
		client.close()
		logger.LogInfo("[SSE] disconnected user=%s", userID)
	}()

	if frame, err := encodeFrame(map[string]interface{}{
		"type":    "connected",
		"message": "SSE connection established",
		"time":    time.Now().Format(time.RFC3339),
	}); err == nil {
		client.enqueue(frame)
	}

	if s.OnConnect != nil {
		go func(uid string) {
			defer func() {
				if rec := recover(); rec != nil {
					logger.LogError("[SSE] OnConnect panic for user %s: %v", uid, rec)
				}
			}()
			time.Sleep(300 * time.Millisecond)
			s.OnConnect(uid)
		}(userID)
	}

	for {
		select {
		case frame := <-client.out:
			if _, err := w.Write(frame); err != nil {
				return
			}
			flusher.Flush()
		case <-client.done:
			for {
				select {
				case frame := <-client.out:
					if _, err := w.Write(frame); err != nil {
						return
					}
				default:
					flusher.Flush()
					return
				}
			}
		case <-r.Context().Done():
			return
		case <-s.stopCh:
			return
		}
	}
}

func (s *SSEServer) pingClients() {
	defer s.pingTicker.Stop()

	for {
		select {
		case <-s.pingTicker.C:
			frame, err := encodeFrame(map[string]interface{}{
				"type": "ping",
				"time": time.Now().Format(time.RFC3339),
			})
			if err != nil {
				continue
			}
			s.mu.RLock()
			for _, client := range s.clients {
				client.enqueue(frame)
			}
			s.mu.RUnlock()
		case <-s.stopCh:
			return
		}
	}
}

func (s *SSEServer) Stop() {
	s.stopOnce.Do(func() {
		close(s.stopCh)
		s.mu.Lock()
		for _, client := range s.clients {
			client.close()
		}
		s.clients = make(map[string]*SSEClient)
		s.mu.Unlock()
	})
}

func lookupClient(userID string) *SSEClient {
	if globalSSEServer == nil {
		return nil
	}
	globalSSEServer.mu.RLock()
	defer globalSSEServer.mu.RUnlock()
	return globalSSEServer.clients[userID]
}

// SendToUser sends a message to a specific user via SSE
func SendToUser(userID string, message []byte) {
	client := lookupClient(userID)
	if client == nil {
		return
	}

	var data interface{}
	if err := json.Unmarshal(message, &data); err != nil {
		data = map[string]interface{}{
			"type":    "message",
			"content": string(message),
			"time":    time.Now().Format(time.RFC3339),
		}
	}
	frame, err := encodeFrame(data)
	if err != nil {
		return
	}
	if !client.enqueue(frame) {
		logger.LogError("[SSE] dropped message for user %s", userID)
	}
}

// SendForceLogout sends a force logout message to a user and closes the stream.
func SendForceLogout(userID, reason, newIP string) {
	client := lookupClient(userID)
	if client == nil {
		return
	}

	frame, err := encodeFrame(map[string]interface{}{
		"type":   "force_logout",
		"reason": reason,
		"new_ip": newIP,
		"time":   time.Now().Format(time.RFC3339),
	})
	if err != nil {
		return
	}
	if client.enqueue(frame) && logger.GlobalLogger != nil {
		logger.GlobalLogger.LogAudit(fmt.Sprintf("Force logout sent via SSE to user %s (reason=%s)", userID, reason))
	}
	client.close()
}

// GetClients returns a list of connected user IDs
func GetClients() []string {
	if globalSSEServer == nil {
		return nil
	}

	globalSSEServer.mu.RLock()
	defer globalSSEServer.mu.RUnlock()

	ids := make([]string, 0, len(globalSSEServer.clients))
	for uid := range globalSSEServer.clients {
		ids = append(ids, uid)
	}
	return ids
}

// GetClientCount returns the number of connected clients
func GetClientCount() int {
	if globalSSEServer == nil {
		return 0
	}

	globalSSEServer.mu.RLock()
	defer globalSSEServer.mu.RUnlock()

	return len(globalSSEServer.clients)
}
