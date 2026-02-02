package api

import (
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/internal/dashboard"
	"CimplrCorpSaas/internal/logger"
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

// CORS and common header constants
const (
	headerAccessControlAllowOrigin  = "Access-Control-Allow-Origin"
	headerAccessControlAllowMethods = "Access-Control-Allow-Methods"
	headerAccessControlAllowHeaders = "Access-Control-Allow-Headers"
	headerContentType               = constants.ContentTypeText
	contentTypeJSON                 = constants.ContentTypeJSON
	allowOriginAll                  = "*"
	allowMethodsAll                 = "GET, POST, PUT, DELETE, OPTIONS"
	allowHeadersAll                 = "*"
	errAuthServiceUnavailable       = "Auth service unavailable"
	errMethodNotAllowed             = constants.ErrMethodNotAllowed
)

// stripPathPrefix removes /cimplrapigateway from the request path before routing
func stripPathPrefix(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/cimplrapigateway/") {
			r.URL.Path = strings.TrimPrefix(r.URL.Path, "/cimplrapigateway")
			if !strings.HasPrefix(r.URL.Path, "/") {
				r.URL.Path = "/" + r.URL.Path
			}
		}
		next.ServeHTTP(w, r)
	})
}

// Global reference to AuthService (set from main or manager)
var (
	authService     *auth.AuthService
	authServiceOnce sync.Once
)

func isDevMode() bool {
	return strings.EqualFold(os.Getenv("DEVEL_MODE"), "true")
}

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
		w.Header().Set(headerAccessControlAllowOrigin, allowOriginAll)
		w.Header().Set(headerAccessControlAllowMethods, allowMethodsAll)
		w.Header().Set(headerAccessControlAllowHeaders, allowHeadersAll)
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		h(w, r)
	}
}

func GetSessionsHandler(w http.ResponseWriter, r *http.Request) {
	if authService == nil {
		http.Error(w, errAuthServiceUnavailable, http.StatusInternalServerError)
		return
	}
	sessions := authService.GetActiveSessions()
	w.Header().Set(headerContentType, contentTypeJSON)
	json.NewEncoder(w).Encode(sessions)
}

// GetSessionByUserIDHandler returns session info for a specific user_id
func GetSessionByUserIDHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, errMethodNotAllowed, http.StatusMethodNotAllowed)
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
		http.Error(w, errAuthServiceUnavailable, http.StatusInternalServerError)
		return
	}
	sessions := authService.GetActiveSessions()
	var found []interface{}
	for _, s := range sessions {
		if s.UserID == req.UserID {
			found = append(found, s)
		}
	}
	w.Header().Set(headerContentType, contentTypeJSON)
	json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "sessions": found})
}

// LoginHandler handles POST /auth/login
func LoginHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, errMethodNotAllowed, http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, constants.ErrInvalidJSONShort, http.StatusBadRequest)
		return
	}
	if authService == nil {
		http.Error(w, errAuthServiceUnavailable, http.StatusInternalServerError)
		return
	}
	clientIP := extractClientIP(r)
	session, err := authService.Login(req.Username, req.Password, clientIP) // Pass IP here
	if err != nil {
		// Log and return JSON error to aid debugging (temporary)
		log.Printf("Login failed for %s from %s: %v", req.Username, clientIP, err)
		w.Header().Set(headerContentType, contentTypeJSON)
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	w.Header().Set(headerContentType, contentTypeJSON)
	json.NewEncoder(w).Encode(session)
}

// LogoutHandler handles POST /auth/logout
// LogoutHandler handles POST /auth/logout
func LogoutHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, errMethodNotAllowed, http.StatusMethodNotAllowed)
		return
	}
	var raw map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		http.Error(w, constants.ErrInvalidJSONShort, http.StatusBadRequest)
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
		http.Error(w, errAuthServiceUnavailable, http.StatusInternalServerError)
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
			w.Header().Set(headerAccessControlAllowOrigin, allowOriginAll)
			w.Header().Set(headerAccessControlAllowMethods, allowMethodsAll)
			w.Header().Set(headerAccessControlAllowHeaders, allowHeadersAll)
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
			resp.Header.Set(headerAccessControlAllowOrigin, allowOriginAll)
			resp.Header.Set(headerAccessControlAllowMethods, allowMethodsAll)
			resp.Header.Set(headerAccessControlAllowHeaders, allowHeadersAll)
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

// encResponseWriter captures handler output so we can encrypt it if requested.
type encResponseWriter struct {
	http.ResponseWriter
	buf    bytes.Buffer
	status int
}

func (erw *encResponseWriter) Header() http.Header {
	return erw.ResponseWriter.Header()
}

func (erw *encResponseWriter) WriteHeader(code int) {
	erw.status = code
}

func (erw *encResponseWriter) Write(b []byte) (int, error) {
	return erw.buf.Write(b)
}

// decryptPayload unwraps AES-GCM encrypted request bodies when X-Payload-Enc=aes-gcm is present.
func decryptPayload(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isDevMode() {
			next.ServeHTTP(w, r)
			return
		}
		if r.Method == http.MethodGet || r.Header.Get("X-Payload-Enc") == "" {
			next.ServeHTTP(w, r)
			return
		}
		if r.Header.Get("X-Payload-Enc") != "aes-gcm" {
			writeJSONError(w, "unsupported encryption")
			return
		}

		keyB64 := os.Getenv("PAYLOAD_ENC_KEY")
		key, err := base64.StdEncoding.DecodeString(keyB64)
		if keyB64 == "" || err != nil || len(key) != 32 {
			writeJSONError(w, "encryption key not configured")
			return
		}

		var raw map[string]string
		if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
			writeJSONError(w, "invalid encrypted payload")
			return
		}

		var ct, iv, tag []byte
		if ed, ok := raw["ED"]; ok {
			parts := strings.Split(ed, ":")
			if len(parts) != 3 {
				writeJSONError(w, "invalid ED format")
				return
			}
			var err error
			if iv, err = hexDecode(parts[0]); err != nil {
				writeJSONError(w, "invalid iv")
				return
			}
			if tag, err = hexDecode(parts[1]); err != nil {
				writeJSONError(w, "invalid tag")
				return
			}
			if ct, err = hexDecode(parts[2]); err != nil {
				writeJSONError(w, "invalid ciphertext")
				return
			}
		} else {
			wrap := struct {
				Ciphertext string `json:"ciphertext"`
				IV         string `json:"iv"`
				Tag        string `json:"tag"`
			}{}
			if b, err := json.Marshal(raw); err == nil {
				_ = json.Unmarshal(b, &wrap)
			}
			ct, err = base64.StdEncoding.DecodeString(wrap.Ciphertext)
			if err != nil {
				writeJSONError(w, "invalid ciphertext")
				return
			}
			iv, err = base64.StdEncoding.DecodeString(wrap.IV)
			if err != nil {
				writeJSONError(w, "invalid iv")
				return
			}
			tag, err = base64.StdEncoding.DecodeString(wrap.Tag)
			if err != nil {
				writeJSONError(w, "invalid tag")
				return
			}
		}

		block, err := aes.NewCipher(key)
		if err != nil {
			writeJSONError(w, "cipher init failed")
			return
		}
		gcm, err := cipher.NewGCM(block)
		if err != nil {
			writeJSONError(w, "cipher init failed")
			return
		}
		if len(iv) != gcm.NonceSize() {
			writeJSONError(w, "invalid iv size")
			return
		}

		ciphertextWithTag := append(ct, tag...)
		plaintext, err := gcm.Open(nil, iv, ciphertextWithTag, nil)
		if err != nil {
			writeJSONError(w, "decryption failed")
			return
		}

		r.Body = io.NopCloser(bytes.NewReader(plaintext))
		r.ContentLength = int64(len(plaintext))
		next.ServeHTTP(w, r)
	})
}

// encryptResponse encrypts response bodies when X-Response-Enc=aes-gcm is requested.
func encryptResponse(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isDevMode() {
			next.ServeHTTP(w, r)
			return
		}
		shouldEncrypt := r.Header.Get("X-Response-Enc") == "aes-gcm"
		if !shouldEncrypt {
			next.ServeHTTP(w, r)
			return
		}

		erw := &encResponseWriter{ResponseWriter: w}
		next.ServeHTTP(erw, r)
		status := erw.status
		if status == 0 {
			status = http.StatusOK
		}

		keyB64 := os.Getenv("PAYLOAD_ENC_KEY")
		key, err := base64.StdEncoding.DecodeString(keyB64)
		if keyB64 == "" || err != nil || len(key) != 32 {
			writeJSONError(w, "encryption key not configured")
			return
		}

		ciphertext, err := encryptBytes(erw.buf.Bytes(), key)
		if err != nil {
			writeJSONError(w, "encryption failed")
			return
		}

		// Remove stale length/encoding headers because the body is now rewritten
		w.Header().Del("Content-Length")
		w.Header().Del("Content-Encoding")
		w.Header().Set(headerAccessControlAllowOrigin, allowOriginAll)
		w.Header().Set(headerAccessControlAllowMethods, allowMethodsAll)
		w.Header().Set(headerAccessControlAllowHeaders, allowHeadersAll)
		w.Header().Set(headerContentType, contentTypeJSON)
		w.WriteHeader(status)
		json.NewEncoder(w).Encode(map[string]string{"ED": ciphertext})
	})
}

func encryptBytes(plain []byte, key []byte) (string, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	iv := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(iv); err != nil {
		return "", err
	}
	sealed := gcm.Seal(nil, iv, plain, nil) // ciphertext||tag
	if len(sealed) < gcm.Overhead() {
		return "", fmt.Errorf("invalid sealed length")
	}
	ct := sealed[:len(sealed)-gcm.Overhead()]
	tag := sealed[len(sealed)-gcm.Overhead():]
	return fmt.Sprintf("%x:%x:%x", iv, tag, ct), nil
}

func writeJSONError(w http.ResponseWriter, msg string) {
	w.Header().Set(headerAccessControlAllowOrigin, allowOriginAll)
	w.Header().Set(headerAccessControlAllowMethods, allowMethodsAll)
	w.Header().Set(headerAccessControlAllowHeaders, allowHeadersAll)
	w.Header().Set(headerContentType, contentTypeJSON)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]any{"success": false, "error": msg})
}

func hexDecode(s string) ([]byte, error) {
	return hex.DecodeString(s)
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
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
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
		w.Header().Set(headerContentType, contentTypeJSON)
		w.Write([]byte(`{"ok":true}`))
	}))

	// Debug endpoint to list connected SSE clients
	mux.HandleFunc("/debug/sse-clients", withCORS(func(w http.ResponseWriter, r *http.Request) {
		clients := dashboard.GetClients()
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
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
	mux.HandleFunc("/investment/", createReverseProxy("http://localhost:7143"))

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
	log.Printf("API Gateway listening on :%s", port)
	handler := encryptResponse(LoggingMiddleware(decryptPayload(stripPathPrefix(mux))))
	cert := os.Getenv("TLS_CERT")
	key := os.Getenv("TLS_KEY")
	var err error
	if cert != "" && key != "" {
		err = http.ListenAndServeTLS(":"+port, cert, key, handler)
	} else {
		log.Printf("TLS_CERT or TLS_KEY not set; starting HTTP on :%s", port)
		err = http.ListenAndServe(":"+port, handler)
	}
	if err != nil {
		log.Fatalf("Gateway server failed: %v", err)
	}
}
