package api

import (
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/internal/logger"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"
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

func GetSessionsHandler(w http.ResponseWriter, r *http.Request) {
	if authService == nil {
		http.Error(w, "Auth service unavailable", http.StatusInternalServerError)
		return
	}
	sessions := authService.GetActiveSessions()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(sessions)
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
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(session)
}

// LogoutHandler handles POST /auth/logout
func LogoutHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		SessionID string `json:"session_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	if authService == nil {
		http.Error(w, "Auth service unavailable", http.StatusInternalServerError)
		return
	}
	err := authService.Logout(req.SessionID)
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
		logr := logger.GlobalLogger

		// Get client IP (prefer X-Forwarded-For)
		clientIP := r.RemoteAddr
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			clientIP = xff
		}

		// Try to extract userId from JSON body (if present)
		var userId string
		if r.Method == "POST" || r.Method == "PUT" {
			if r.Header.Get("Content-Type") == "application/json" {
				bodyBytes, err := io.ReadAll(r.Body)
				if err == nil && len(bodyBytes) > 0 {
					var bodyMap map[string]interface{}
					if err := json.Unmarshal(bodyBytes, &bodyMap); err == nil {
						if uid, ok := bodyMap["user_id"]; ok {
							userId, _ = uid.(string)
						}
					}
				}
				r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
			}
		}

		msg := fmt.Sprintf("[Gateway] Incoming request: %s %s from %s userId=%s", r.Method, r.URL.Path, clientIP, userId)
		if logr != nil {
			logr.LogAudit(msg)
		} else {
			log.Println(msg)
		}

		url, err := url.Parse(target)
		if err != nil {
			msg := fmt.Sprintf("[Gateway][ERROR] Proxy error: bad target URL %s for %s", target, r.URL.Path)
			if logr != nil {
				logr.LogAudit(msg)
			} else {
				log.Println(msg)
			}
			http.Error(w, "Bad target URL", http.StatusInternalServerError)
			return
		}
		proxy := httputil.NewSingleHostReverseProxy(url)

		rw := &responseWriter{ResponseWriter: w, statusCode: 200}
		proxy.ServeHTTP(rw, r)
		if rw.statusCode >= 400 {
			msg = fmt.Sprintf("[Gateway][ERROR] Proxied to %s for %s, status %d, error: %s", target, r.URL.Path, rw.statusCode, rw.body.String())
		} else {
			msg = fmt.Sprintf("[Gateway] Proxied to %s for %s, status %d", target, r.URL.Path, rw.statusCode)
		}
		if logr != nil {
			logr.LogAudit(msg)
		} else {
			log.Println(msg)
		}
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

// StartGateway starts the API gateway server
func StartGateway() {
	mux := http.NewServeMux()

	// Auth endpoints
	mux.HandleFunc("/auth/login", LoginHandler)
	mux.HandleFunc("/auth/logout", LogoutHandler)
	mux.HandleFunc("/get-sessions", GetSessionsHandler)
	mux.HandleFunc("/fx/", createReverseProxy("http://localhost:3143"))
	mux.HandleFunc("/dash/", createReverseProxy("http://localhost:4143"))
	mux.HandleFunc("/uam/", createReverseProxy("http://localhost:5143"))
	mux.HandleFunc("/cash/", createReverseProxy("http://localhost:6143"))

	mux.HandleFunc("/healt", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("API Gateway is healthy"))
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
	err := http.ListenAndServe(":8081", mux)
	if err != nil {
		log.Fatalf("Gateway server failed: %v", err)
	}
}
