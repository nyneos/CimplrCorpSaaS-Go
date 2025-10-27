package api

import (
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/internal/dashboard"
	"CimplrCorpSaas/internal/logger"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"sync"
	"time"
)

// Global reference to AuthService (set from main or manager)
var (
	authService     *auth.AuthService
	authServiceOnce sync.Once
)

// SetAuthService allows wiring the AuthService from main/manager
func SetAuthService(svc *auth.AuthService) {
	authServiceOnce.Do(func() {
		authService = svc
	})
}

func extractClientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		return xff
	}
	return r.RemoteAddr
}

func withCORS(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		h(w, r)
	}
}

func GetSessionsHandler(w http.ResponseWriter, r *http.Request) {
	if authService == nil {
		http.Error(w, "Auth service unavailable", http.StatusInternalServerError)
		return
	}
	sessions := authService.GetActiveSessions()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(sessions)
}

// GetSessionByUserIDHandler returns session info for a specific user_id
func GetSessionByUserIDHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		UserID string `json:"user_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": "Missing user_id in body"})
		return
	}
	if authService == nil {
		http.Error(w, "Auth service unavailable", http.StatusInternalServerError)
		return
	}
	sessions := authService.GetActiveSessions()
	var found []interface{}
	for _, s := range sessions {
		if s.UserID == req.UserID {
			found = append(found, s)
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "sessions": found})
}

// LoginHandler handles POST /auth/login
func LoginHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	if authService == nil {
		http.Error(w, "Auth service unavailable", http.StatusInternalServerError)
		return
	}
	clientIP := extractClientIP(r)
	session, err := authService.Login(req.Username, req.Password, clientIP) // Pass IP here
	if err != nil {
		// Log and return JSON error to aid debugging (temporary)
		log.Printf("Login failed for %s from %s: %v", req.Username, clientIP, err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(session)
}

// LogoutHandler handles POST /auth/logout
// LogoutHandler handles POST /auth/logout
func LogoutHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	var raw map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	userID := ""
	if v, ok := raw["user_id"]; ok {
		switch val := v.(type) {
		case string:
			userID = val
		case float64:
			userID = fmt.Sprintf("%.0f", val)
		default:
			http.Error(w, "Invalid user_id type", http.StatusBadRequest)
			return
		}
	} else {
		http.Error(w, "user_id required", http.StatusBadRequest)
		return
	}
	if authService == nil {
		http.Error(w, "Auth service unavailable", http.StatusInternalServerError)
		return
	}
	err := authService.Logout(userID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"message":"logout successful"}`))
}

// createReverseProxy returns a reverse proxy handler for the given target URL
func createReverseProxy(target string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Handle preflight OPTIONS at the gateway
		if r.Method == "OPTIONS" {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
			w.WriteHeader(http.StatusOK)
			return
		}

		url, err := url.Parse(target)
		if err != nil {
			http.Error(w, "Bad target URL", http.StatusInternalServerError)
			return
		}
		proxy := httputil.NewSingleHostReverseProxy(url)

		// Set CORS headers ONLY in ModifyResponse for proxied requests
		proxy.ModifyResponse = func(resp *http.Response) error {
			resp.Header.Set("Access-Control-Allow-Origin", "*")
			resp.Header.Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
			resp.Header.Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
			return nil
		}

		proxy.ServeHTTP(w, r)
	}
}

// responseWriter wraps http.ResponseWriter to capture status code and response body
type responseWriter struct {
	http.ResponseWriter
	statusCode int
	body       bytes.Buffer
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *responseWriter) Write(b []byte) (int, error) {
	rw.body.Write(b)
	return rw.ResponseWriter.Write(b)
}

// LoggingMiddleware is a middleware for logging HTTP requests
func LoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/events" {
			// SSE endpoints don't need wrapped response writers
			// Just log the connection and pass through
			fmt.Printf("[GATEWAY] Incoming SSE connection request from %s query=%s\n", r.RemoteAddr, r.URL.RawQuery)
			next.ServeHTTP(w, r)
			return
		}
		start := time.Now()
		rw := &responseWriter{ResponseWriter: w, statusCode: 200}
		var body string
		if r.Method == "POST" || r.Method == "PUT" {
			b, _ := io.ReadAll(r.Body)
			body = string(b)
			r.Body = io.NopCloser(bytes.NewBuffer(b))
			// If there is a user_id field, call authService to log different-IP requests
			if authService != nil && len(b) > 0 {
				var raw map[string]interface{}
				if err := json.Unmarshal(b, &raw); err == nil {
					if uid, ok := raw["user_id"]; ok && uid != nil {
						var userID string
						switch v := uid.(type) {
						case string:
							userID = v
						case float64:
							userID = fmt.Sprintf("%.0f", v)
						}
						if userID != "" {
							clientIP := extractClientIP(r)
							go authService.LogDifferentIPRequest(userID, clientIP)
						}
					}
				}
				// restore body for downstream handlers
				r.Body = io.NopCloser(bytes.NewBuffer(b))
				body = string(b)
			}
		}
		next.ServeHTTP(rw, r)
		duration := time.Since(start)
		clientIP := extractClientIP(r)
		userAgent := r.Header.Get("User-Agent")
		fmt.Printf(
			"[REQ] %s %s | Status: %d | IP: %s | UA: %s | Duration: %v | Body: %s | RespSize: %d\n",
			r.Method, r.URL.Path, rw.statusCode, clientIP, userAgent, duration, body, rw.body.Len(),
		)
	})
}

// StartGateway starts the API gateway server
func StartGateway() {
	mux := http.NewServeMux()

	// Initialize and register the SSE server at /events
	sseServer := dashboard.NewSSEServer()
	mux.HandleFunc("/events", sseServer.HandleSSE)

	// Debug endpoint to force logout a user via SSE (for testing only)
	mux.HandleFunc("/debug/force-logout", withCORS(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			UserID string `json:"user_id"`
			Reason string `json:"reason"`
			NewIP  string `json:"new_ip"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			http.Error(w, "invalid body, require {user_id}", http.StatusBadRequest)
			return
		}
		dashboard.SendForceLogout(req.UserID, req.Reason, req.NewIP)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"ok":true}`))
	}))

	// Debug endpoint to list connected SSE clients
	mux.HandleFunc("/debug/sse-clients", withCORS(func(w http.ResponseWriter, r *http.Request) {
		clients := dashboard.GetClients()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"clients": clients,
			"count":   dashboard.GetClientCount(),
		})
	}))

	// Auth endpoints
	mux.HandleFunc("/auth/login", withCORS(LoginHandler))
	mux.HandleFunc("/auth/logout", withCORS(LogoutHandler))
	mux.HandleFunc("/get-sessions", withCORS(GetSessionsHandler))
	mux.HandleFunc("/auth/session", withCORS(GetSessionByUserIDHandler))
	mux.HandleFunc("/fx/", createReverseProxy("http://localhost:3143"))
	mux.HandleFunc("/dash/", createReverseProxy("http://localhost:4143"))
	mux.HandleFunc("/uam/", createReverseProxy("http://localhost:5143"))
	mux.HandleFunc("/cash/", createReverseProxy("http://localhost:6143"))
	mux.HandleFunc("/master/", createReverseProxy("http://localhost:2143"))

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("API Gateway is active"))
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		logr := logger.GlobalLogger
		msg := "[Gateway] [Error] " + r.URL.Path + " from " + r.RemoteAddr + " (route not found)"
		if logr != nil {
			logr.LogAudit(msg)
		} else {
			log.Println(msg)
		}
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("404 - Route not found"))
	})

	log.Println("API Gateway started on :8081")
	port := os.Getenv("PORT")
	if port == "" {
		port = "8081"
	}
	log.Printf("API Gateway started on :%s", port)
	// err := http.ListenAndServe(":"+port, mux)
	err := http.ListenAndServe(":"+port, LoggingMiddleware(mux))

	if err != nil {
		log.Fatalf("Gateway server failed: %v", err)
	}
}
