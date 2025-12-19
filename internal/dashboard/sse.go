package dashboard

import (
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/internal/logger"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

type SSEClient struct {
	userID   string
	writer   http.ResponseWriter
	flusher  http.Flusher
	done     chan bool
	lastPing time.Time
}

type SSEServer struct {
	mu         sync.RWMutex
	clients    map[string]*SSEClient
	pingTicker *time.Ticker
	stopCh     chan struct{}
}

var globalSSEServer *SSEServer

func NewSSEServer() *SSEServer {
	s := &SSEServer{
		clients: make(map[string]*SSEClient),
		stopCh:  make(chan struct{}),
	}
	globalSSEServer = s

	// Start ping routine to keep connections alive
	s.pingTicker = time.NewTicker(30 * time.Second)
	go s.pingClients()

	return s
}

func GetSSEServer() *SSEServer {
	return globalSSEServer
}

// HandleSSE handles SSE connections
func (s *SSEServer) HandleSSE(w http.ResponseWriter, r *http.Request) {
	// Set SSE headers
	w.Header().Set(constants.ContentTypeText, "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set(constants.HeaderAccessControlAllowOrigin, "*")
	w.Header().Set(constants.HeaderAccessControlAllowHeaders, "Cache-Control")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		http.Error(w, "user_id parameter required", http.StatusBadRequest)
		return
	}

	client := &SSEClient{
		userID:   userID,
		writer:   w,
		flusher:  flusher,
		done:     make(chan bool),
		lastPing: time.Now(),
	}

	s.mu.Lock()
	// Close existing connection for this user if any
	if existingClient, exists := s.clients[userID]; exists {
		close(existingClient.done)
	}
	s.clients[userID] = client
	s.mu.Unlock()

	fmt.Printf("[SSE] Connected user %s from %s\n", userID, r.RemoteAddr)

	// Send initial connection confirmation
	s.sendToClient(client, map[string]interface{}{
		"type":    "connected",
		"message": "SSE connection established",
		"time":    time.Now().Format(time.RFC3339),
	})

	// Keep connection alive until client disconnects or we close it
	defer func() {
		s.mu.Lock()
		if s.clients[userID] == client {
			delete(s.clients, userID)
		}
		s.mu.Unlock()
		fmt.Printf("[SSE] Disconnected user %s\n", userID)
	}()

	// Block until connection is closed
	select {
	case <-client.done:
		return
	case <-r.Context().Done():
		return
	case <-s.stopCh:
		return
	}
}

func (s *SSEServer) sendToClient(client *SSEClient, data interface{}) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintf(client.writer, "data: %s\n\n", jsonData)
	if err != nil {
		return err
	}

	client.flusher.Flush()
	return nil
}

func (s *SSEServer) pingClients() {
	defer s.pingTicker.Stop()

	for {
		select {
		case <-s.pingTicker.C:
			s.mu.RLock()
			for userID, client := range s.clients {
				err := s.sendToClient(client, map[string]interface{}{
					"type": "ping",
					"time": time.Now().Format(time.RFC3339),
				})
				if err != nil {
					fmt.Printf("[SSE] Ping failed for user %s: %v\n", userID, err)
					// Remove failed client
					go func(uid string, c *SSEClient) {
						s.mu.Lock()
						if s.clients[uid] == c {
							delete(s.clients, uid)
							close(c.done)
						}
						s.mu.Unlock()
					}(userID, client)
				} else {
					client.lastPing = time.Now()
				}
			}
			s.mu.RUnlock()
		case <-s.stopCh:
			return
		}
	}
}

func (s *SSEServer) Stop() {
	close(s.stopCh)
	s.mu.Lock()
	for _, client := range s.clients {
		close(client.done)
	}
	s.clients = make(map[string]*SSEClient)
	s.mu.Unlock()
}

// SendToUser sends a message to a specific user via SSE
func SendToUser(userID string, message []byte) {
	if globalSSEServer == nil {
		return
	}

	var data interface{}
	if err := json.Unmarshal(message, &data); err != nil {
		// If message is not JSON, wrap it
		data = map[string]interface{}{
			"type":    "message",
			"content": string(message),
			"time":    time.Now().Format(time.RFC3339),
		}
	}

	globalSSEServer.mu.RLock()
	client, exists := globalSSEServer.clients[userID]
	globalSSEServer.mu.RUnlock()

	if !exists {
		fmt.Printf("[SSE] User %s not connected\n", userID)
		return
	}

	err := globalSSEServer.sendToClient(client, data)
	if err != nil {
		fmt.Printf("[SSE] Failed to send message to user %s: %v\n", userID, err)
		// Remove failed client
		globalSSEServer.mu.Lock()
		if globalSSEServer.clients[userID] == client {
			delete(globalSSEServer.clients, userID)
			close(client.done)
		}
		globalSSEServer.mu.Unlock()
	}
}

// SendForceLogout sends a force logout message to a user
func SendForceLogout(userID, reason, newIP string) {
	if globalSSEServer == nil {
		return
	}

	message := map[string]interface{}{
		"type":   "force_logout",
		"reason": reason,
		"new_ip": newIP,
		"time":   time.Now().Format(time.RFC3339),
	}

	globalSSEServer.mu.RLock()
	client, exists := globalSSEServer.clients[userID]
	globalSSEServer.mu.RUnlock()

	if !exists {
		fmt.Printf("[SSE] User %s not connected for force logout\n", userID)
		return
	}

	err := globalSSEServer.sendToClient(client, message)
	if err != nil {
		fmt.Printf("[SSE] Failed to send force logout to user %s: %v\n", userID, err)
	} else {
		fmt.Printf("[SSE] Sent force_logout to user %s (reason=%s new_ip=%s)\n", userID, reason, newIP)
		if logger.GlobalLogger != nil {
			logger.GlobalLogger.LogAudit(fmt.Sprintf("Force logout sent via SSE to user %s (reason=%s)", userID, reason))
		}
	}

	// Close the connection after sending force logout
	globalSSEServer.mu.Lock()
	if globalSSEServer.clients[userID] == client {
		delete(globalSSEServer.clients, userID)
		close(client.done)
	}
	globalSSEServer.mu.Unlock()
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

// CleanupDeadConnections removes clients that haven't responded to pings
func (s *SSEServer) CleanupDeadConnections() {
	s.mu.Lock()
	defer s.mu.Unlock()

	cutoff := time.Now().Add(-2 * time.Minute) // 2 minutes timeout
	for userID, client := range s.clients {
		if client.lastPing.Before(cutoff) {
			fmt.Printf("[SSE] Removing dead connection for user %s\n", userID)
			close(client.done)
			delete(s.clients, userID)
		}
	}
}
