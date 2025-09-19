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
		http.Error(w, err.Error(), http.StatusUnauthorized)
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
// type responseWriter struct {
// 	http.ResponseWriter
// 	statusCode int
// 	body       bytes.Buffer
// }

// func (rw *responseWriter) WriteHeader(code int) {
// 	rw.statusCode = code
// 	rw.ResponseWriter.WriteHeader(code)
// }

//	func (rw *responseWriter) Write(b []byte) (int, error) {
//		rw.body.Write(b)
//		return rw.ResponseWriter.Write(b)
//	}

type responseCapture struct {
	http.ResponseWriter
	statusCode int
	buf        bytes.Buffer
	cap        int
}

func (rc *responseCapture) WriteHeader(code int) {
	rc.statusCode = code
	rc.ResponseWriter.WriteHeader(code)
}

func (rc *responseCapture) Write(b []byte) (int, error) {
	// write through to underlying writer first
	n, err := rc.ResponseWriter.Write(b)
	if rc.cap > 0 && rc.buf.Len() < rc.cap {
		// write only up to cap bytes into buffer
		toWrite := b
		if remaining := rc.cap - rc.buf.Len(); len(b) > remaining {
			toWrite = b[:remaining]
		}
		rc.buf.Write(toWrite)
	}
	return n, err
}

// LoggingMiddleware is a middleware for logging HTTP requests
func LoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		// capture response written by downstream handlers (limit 64KB)
		rc := &responseCapture{ResponseWriter: w, statusCode: 200, cap: 64 * 1024}
		var body string
		if r.Method == "POST" || r.Method == "PUT" {
			b, _ := io.ReadAll(r.Body)
			body = string(b)
			r.Body = io.NopCloser(bytes.NewBuffer(b))
		}
		next.ServeHTTP(rc, r)
		duration := time.Since(start)
		clientIP := extractClientIP(r)
		userAgent := r.Header.Get("User-Agent")
		// prepare captured response body (truncate if larger than cap)
		respBody := rc.buf.String()
		if rc.buf.Len() >= rc.cap {
			respBody = respBody + "...(truncated)"
		}
		fmt.Printf(
			"[REQ] %s %s | Status: %d | IP: %s | UA: %s | Duration: %v | ReqBody: %s | RespBodySize: %d | RespBody: %s\n",
			r.Method, r.URL.Path, rc.statusCode, clientIP, userAgent, duration, body, rc.buf.Len(), respBody,
		)
	})
}

// StartGateway starts the API gateway server
func StartGateway() {
	mux := http.NewServeMux()

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
