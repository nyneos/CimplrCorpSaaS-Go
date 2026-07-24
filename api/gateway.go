package api

import (
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/internal/dashboard"
	dinojobs "CimplrCorpSaas/internal/jobs/dino"
	"CimplrCorpSaas/internal/logger"
	"CimplrCorpSaas/internal/observability"
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// gatewayPool is the pgxpool used by the SSE server for in-app notifications.
// Set before calling StartGateway via SetGatewayPool.
var gatewayPool *pgxpool.Pool

// SetGatewayPool stores the pgxpool so that StartGateway can pass it to NewSSEServer.
func SetGatewayPool(pool *pgxpool.Pool) {
	gatewayPool = pool
}

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

// stripPathPrefix removes the configured path prefix from the request path before routing.
// The prefix is passed from services.yaml gateway config (path_prefix key).
func stripPathPrefix(next http.Handler, pathPrefix string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		prefix := strings.TrimRight(pathPrefix, "/")
		if prefix != "" && strings.HasPrefix(r.URL.Path, prefix+"/") {
			r.URL.Path = strings.TrimPrefix(r.URL.Path, prefix)
			if !strings.HasPrefix(r.URL.Path, "/") {
				r.URL.Path = "/" + r.URL.Path
			}
		}
		next.ServeHTTP(w, r)
	})
}

// The prefix is read from environment variable PATH_PREFIX.
// func stripPathPrefix(next http.Handler) http.Handler {
// 	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
// 		pathPrefix := os.Getenv("PATH_PREFIX")

//			prefix := strings.TrimRight(pathPrefix, "/")
//			if prefix != "" && strings.HasPrefix(r.URL.Path, prefix+"/") {
//				r.URL.Path = strings.TrimPrefix(r.URL.Path, prefix)
//				if !strings.HasPrefix(r.URL.Path, "/") {
//					r.URL.Path = "/" + r.URL.Path
//				}
//			}
//			next.ServeHTTP(w, r)
//		})
//	}
//
// Global reference to AuthService (set from main or manager)
var (
	authService     *auth.AuthService
	authServiceOnce sync.Once
	ssoConfig       *auth.SSOConfig
)

func isDevMode() bool {
	return strings.EqualFold(os.Getenv("DEVEL_MODE"), "true")
}

// SetAuthService allows wiring the AuthService from main/manager
func SetAuthService(svc *auth.AuthService) {
	authServiceOnce.Do(func() {
		authService = svc
		ssoConfig = auth.LoadSSOConfig()
	})
}

func extractClientIP(r *http.Request) string {
	return ClientIPFromRequest(r)
}

func SystemIfBlank(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "system"
	}
	return value
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
	RespondEnvelopeSuccess(w, "Success", sessions)
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
		RespondEnvelopeError(w, http.StatusBadRequest, "Missing user_id in body", "")
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
	RespondEnvelopeSuccessCompat(w, "Success", map[string]interface{}{"sessions": found})
}

// LoginHandler handles POST /auth/login
// After password verification, if MFA is enabled for the user, returns
// { "mfa_pending": true, "user_id": "..." } instead of a full session.
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
	session, mfaPending, err := authService.Login(r.Context(), req.Username, req.Password, clientIP)
	if err != nil {
		logger.LogError("Login failed for %s from %s: %v", req.Username, clientIP, err)
		RespondWithError(w, http.StatusUnauthorized, "Invalid credentials")
		return
	}

	if mfaPending {
		RespondEnvelopeSuccessCompat(w, "MFA verification required", map[string]interface{}{
			"mfa_pending": true,
			"user_id":     session.UserID,
		})
		return
	}

	// Flatten the session struct to the top level (via RespondEnvelopeSuccessCompat)
	// as well as nesting it under data — Login.tsx reads fields like IsLoggedIn/
	// UserID/SessionID directly off the response body without unwrapping data,
	// so a nest-only response breaks the login flow.
	sessionBytes, err := json.Marshal(session)
	if err != nil {
		LogErrorForResponse(w, "Login: failed to marshal session: %v", err)
		RespondEnvelopeError(w, http.StatusInternalServerError, "Login failed", "")
		return
	}
	var sessionFields map[string]interface{}
	if err := json.Unmarshal(sessionBytes, &sessionFields); err != nil {
		LogErrorForResponse(w, "Login: failed to unmarshal session: %v", err)
		RespondEnvelopeError(w, http.StatusInternalServerError, "Login failed", "")
		return
	}
	RespondEnvelopeSuccessCompat(w, "Success", sessionFields)
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
		http.Error(w, constants.ErrUserIDRequired, http.StatusBadRequest)
		return
	}
	if authService == nil {
		http.Error(w, errAuthServiceUnavailable, http.StatusInternalServerError)
		return
	}
	err := authService.Logout(r.Context(), userID)
	if err != nil {
		logger.LogError("Logout failed for userID %s: %v", userID, err)
		RespondWithError(w, http.StatusUnauthorized, "Logout failed")
		return
	}
	RespondEnvelopeSuccess(w, "Logout successful", nil)
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
			// Let browsers read policy pass summary set by runtime.Enforce.
			existing := resp.Header.Get("Access-Control-Expose-Headers")
			if existing == "" {
				resp.Header.Set("Access-Control-Expose-Headers", "X-Policy-Summary, X-Trace-Id")
			} else if !strings.Contains(strings.ToLower(existing), "x-policy-summary") {
				resp.Header.Set("Access-Control-Expose-Headers", existing+", X-Policy-Summary")
			}
			return nil
		}

		proxy.ErrorHandler = func(rw http.ResponseWriter, req *http.Request, err error) {
			logger.LogError("[Gateway] proxy error for %s %s -> %s: %v", req.Method, req.URL.Path, target, err)
			rw.Header().Set(headerAccessControlAllowOrigin, allowOriginAll)
			rw.Header().Set(headerAccessControlAllowMethods, allowMethodsAll)
			rw.Header().Set(headerAccessControlAllowHeaders, allowHeadersAll)
			rw.Header().Set(headerContentType, contentTypeJSON)
			rw.WriteHeader(http.StatusBadGateway)
			rw.Write([]byte(`{"success":false,"error":"upstream service unavailable"}`))
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

// Forward Flush to the underlying ResponseWriter if it supports http.Flusher
func (rw *responseWriter) Flush() {
	if f, ok := rw.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
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

// Forward Flush to the underlying ResponseWriter if it supports http.Flusher
func (erw *encResponseWriter) Flush() {
	if f, ok := erw.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
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
			writeJSONError(w, 400, "unsupported encryption")
			return
		}

		keyB64 := os.Getenv("PAYLOAD_ENC_KEY")
		key, err := base64.StdEncoding.DecodeString(keyB64)
		if keyB64 == "" || err != nil || len(key) != 32 {
			writeJSONError(w, 500, "encryption key not configured")
			return
		}

		var raw map[string]string
		if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
			writeJSONError(w, 400, "invalid encrypted payload")
			return
		}

		var ct, iv, tag []byte
		if ed, ok := raw["ED"]; ok {
			parts := strings.Split(ed, ":")
			if len(parts) != 3 {
				writeJSONError(w, 400, "invalid ED format")
				return
			}
			var err error
			if iv, err = hexDecode(parts[0]); err != nil {
				writeJSONError(w, 400, "invalid iv")
				return
			}
			if tag, err = hexDecode(parts[1]); err != nil {
				writeJSONError(w, 400, "invalid tag")
				return
			}
			if ct, err = hexDecode(parts[2]); err != nil {
				writeJSONError(w, 400, "invalid ciphertext")
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
				writeJSONError(w, 400, "invalid ciphertext")
				return
			}
			iv, err = base64.StdEncoding.DecodeString(wrap.IV)
			if err != nil {
				writeJSONError(w, 400, "invalid iv")
				return
			}
			tag, err = base64.StdEncoding.DecodeString(wrap.Tag)
			if err != nil {
				writeJSONError(w, 400, "invalid tag")
				return
			}
		}

		block, err := aes.NewCipher(key)
		if err != nil {
			writeJSONError(w, 500, "cipher init failed")
			return
		}
		gcm, err := cipher.NewGCM(block)
		if err != nil {
			writeJSONError(w, 500, "cipher init failed")
			return
		}
		if len(iv) != gcm.NonceSize() {
			writeJSONError(w, 400, "invalid iv size")
			return
		}

		ciphertextWithTag := append(ct, tag...)
		plaintext, err := gcm.Open(nil, iv, ciphertextWithTag, nil)
		if err != nil {
			writeJSONError(w, 400, "decryption failed")
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
			writeJSONError(w, 500, "encryption key not configured")
			return
		}

		ciphertext, err := encryptBytes(erw.buf.Bytes(), key)
		if err != nil {
			writeJSONError(w, 500, "encryption failed")
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
		// NOT migrated to the envelope: this is the wire contract for every
		// AES-GCM encrypted response body (X-Response-Enc: aes-gcm). Clients
		// decrypt by reading the top-level "ED" field directly; wrapping it in
		// {success,statusCode,message,data} would change what ciphertext callers
		// need to unwrap and break decryption for every encrypted response.
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

// writeJSONError sends the CLAUDE.md standard error envelope with the given
// HTTP status (previously always returned 200 regardless of failure).
func writeJSONError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set(headerAccessControlAllowOrigin, allowOriginAll)
	w.Header().Set(headerAccessControlAllowMethods, allowMethodsAll)
	w.Header().Set(headerAccessControlAllowHeaders, allowHeadersAll)
	LogErrorForResponse(w, "[GATEWAY] %s", msg)
	RespondEnvelopeError(w, status, msg, "")
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
			logger.LogInfoCtx(r.Context(), "[GATEWAY] Incoming SSE connection from %s query=%s", r.RemoteAddr, r.URL.RawQuery)
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
		logger.LogInfoCtx(r.Context(),
			"[REQ] %s %s status=%d ip=%s ua=%s duration=%v body=%s resp_size=%d",
			r.Method, r.URL.Path, rw.statusCode, clientIP, userAgent, duration, body, rw.body.Len(),
		)
	})
}

// StartGateway starts the API gateway server
func NewGatewayServer(port string, pathPrefix string) (*http.Server, string, string) {
	mux := http.NewServeMux()

	// Initialize and register the SSE server at /events
	sseServer := dashboard.NewSSEServer(gatewayPool)
	if gatewayPool != nil {
		sseServer.OnConnect = func(userID string) {
			if err := dinojobs.PushUnreadCountToUser(context.Background(), gatewayPool, userID); err != nil {
				logger.LogError("[SSE] PushUnreadCountToUser error for %s: %v", userID, err)
			}
		}
	}
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
		RespondEnvelopeSuccessCompat(w, "Success", map[string]interface{}{
			"clients": clients,
			"count":   dashboard.GetClientCount(),
		})
	}))

	// Debug endpoint — POST only; allowed when DEVEL_MODE=true or ENV=uat
	mux.HandleFunc("/debug/env", withCORS(DebugEnvHandler))

	// Auth endpoints
	mux.HandleFunc("/auth/login", withCORS(LoginHandler))
	mux.HandleFunc("/auth/logout", withCORS(LogoutHandler))
	mux.HandleFunc("/get-sessions", withCORS(GetSessionsHandler))
	mux.HandleFunc("/auth/session", withCORS(GetSessionByUserIDHandler))

	// SSO (Multi-provider: Microsoft Azure AD + Google)
	if ssoConfig != nil && ssoConfig.HasAnyProvider() {
		mux.HandleFunc("/auth/sso/providers", withCORS(auth.SSOProvidersHandler(ssoConfig)))
		mux.HandleFunc("/auth/sso/login", withCORS(auth.SSOLoginRedirect(ssoConfig)))
		if authService != nil {
			mux.HandleFunc("/auth/sso/callback/", withCORS(auth.SSOCallbackHandler(ssoConfig, authService.Pool())))
			mux.HandleFunc("/auth/sso/logout", withCORS(auth.SSOLogoutHandler(ssoConfig, authService.Pool())))
		}
		logger.LogInfo("SSO endpoints enabled")
	} else {
		logger.LogInfo("SSO endpoints disabled — set AZURE_CLIENT_ID or GOOGLE_CLIENT_ID to enable")
	}

	// MFA (TOTP)
	if authService != nil {
		mfaPool := authService.Pool()
		mux.HandleFunc("/auth/mfa/setup", withCORS(auth.MFASetupHandler(mfaPool)))
		mux.HandleFunc("/auth/mfa/confirm", withCORS(auth.MFAConfirmHandler(mfaPool)))
		mux.HandleFunc("/auth/mfa/verify", withCORS(auth.MFAVerifyHandler(mfaPool)))
		mux.HandleFunc("/auth/mfa/disable", withCORS(auth.MFADisableHandler(mfaPool)))
		mux.HandleFunc("/auth/mfa/status", withCORS(auth.MFAStatusHandler(mfaPool)))
		logger.LogInfo("MFA (TOTP) endpoints enabled")
	}

	// Password Reset (Forgot / Reset)
	if authService != nil {
		pwPool := authService.Pool()
		mux.HandleFunc("/auth/forgot-password", withCORS(auth.ForgotPasswordHandler(pwPool)))
		mux.HandleFunc("/auth/reset-password", withCORS(auth.ResetPasswordHandler(pwPool)))
		mux.HandleFunc("/auth/validate-reset-token", withCORS(auth.ValidateResetTokenHandler(pwPool)))
		logger.LogInfo("Password reset endpoints enabled")
	}

	mux.HandleFunc("/fx/", createReverseProxy("http://localhost:3143"))
	mux.HandleFunc("/dash/", createReverseProxy("http://localhost:4143"))
	mux.HandleFunc("/uam/", createReverseProxy("http://localhost:5143"))
	mux.HandleFunc("/cash/", createReverseProxy("http://localhost:6143"))
	mux.HandleFunc("/master/", createReverseProxy("http://localhost:2143"))
	mux.HandleFunc("/investment/", createReverseProxy("http://localhost:7143"))
	mux.HandleFunc("/notification/", createReverseProxy("http://localhost:9111"))
	mux.HandleFunc("/email/", createReverseProxy("http://localhost:8183"))
	mux.HandleFunc("/policy-engine/", createReverseProxy("http://localhost:8185"))
	mux.HandleFunc("/domain-catalog/", createReverseProxy("http://localhost:8185"))

	mux.HandleFunc("/health", withCORS(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("API Gateway is active"))
	}))

	mux.HandleFunc("/health/db", withCORS(func(w http.ResponseWriter, r *http.Request) {
		if gatewayPool == nil {
			RespondEnvelopeFailureCompat(w, http.StatusServiceUnavailable, "Database not configured", "", map[string]interface{}{
				"status": "unhealthy",
				"db":     "not configured",
			})
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()

		start := time.Now()
		err := gatewayPool.Ping(ctx)
		latency := time.Since(start)

		if err != nil {
			logger.LogError("Health check DB ping failed: %v", err)
			RespondEnvelopeFailureCompat(w, http.StatusServiceUnavailable, "Database unavailable", "", map[string]interface{}{
				"status":  "unhealthy",
				"db":      "disconnected",
				"latency": latency.String(),
			})
			return
		}

		stat := gatewayPool.Stat()
		RespondEnvelopeSuccessCompat(w, "Success", map[string]interface{}{
			"status":         "healthy",
			"db":             "connected",
			"latency":        latency.String(),
			"total_conns":    stat.TotalConns(),
			"idle_conns":     stat.IdleConns(),
			"acquired_conns": stat.AcquiredConns(),
		})
	}))

	mux.HandleFunc("/", withCORS(func(w http.ResponseWriter, r *http.Request) {
		logr := logger.GlobalLogger
		msg := "[Gateway] [Error] " + r.URL.Path + " from " + ClientIPFromRequest(r) + " (route not found)"
		if logr != nil {
			logr.LogAudit(msg)
		} else {
			logger.LogInfo("%s", msg)
		}
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("404 - Route not found"))
	}))

	u := os.Getenv("PORT")
	if port != (u) && u != "" {
		logger.LogInfo("Prioitizing env Port %s over yaml port %s (if deployment didn't have that port open)", os.Getenv("PORT"), port)
		port = os.Getenv("PORT")
	}
	mux.Handle("/gateway/metrics", observability.MetricsHandler("gateway"))
	logger.LogInfo("API Gateway listening on :%s (path prefix: %s)", port, pathPrefix)
	handler := observability.WrapHTTP("gateway", encryptResponse(LoggingMiddleware(decryptPayload(stripPathPrefix(mux, pathPrefix)))))
	// handler := encryptResponse(LoggingMiddleware(decryptPayload(stripPathPrefix(mux))))
	cert := os.Getenv("TLS_CERT")
	key := os.Getenv("TLS_KEY")
	server := &http.Server{
		Addr:    ":" + port,
		Handler: handler,
	}
	return server, cert, key
}

func StartGateway(port string, pathPrefix string) {
	server, cert, key := NewGatewayServer(port, pathPrefix)
	var err error
	if cert != "" && key != "" {
		err = server.ListenAndServeTLS(cert, key)
	} else {
		logger.LogInfo("TLS_CERT or TLS_KEY not set; starting HTTP on %s", server.Addr)
		err = server.ListenAndServe()
	}
	if err != nil && err != http.ErrServerClosed {
		logger.LogError("Gateway server failed: %v", err)
	}
}
