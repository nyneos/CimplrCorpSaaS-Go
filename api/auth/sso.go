package auth

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"CimplrCorpSaas/internal/logger"

	"github.com/golang-jwt/jwt/v5"
)

// ── SSO Provider abstraction ──────────────────────────────────────────────────

// SSOProvider represents a configured OAuth2/OIDC identity provider.
type SSOProvider struct {
	Name         string // "microsoft" or "google"
	ClientID     string
	ClientSecret string
	RedirectURI  string
	AuthURL      string
	TokenURL     string
	LogoutURL    string // IdP logout endpoint (single logout)
	Scopes       string // space-separated
}

// Enabled returns true when the minimum env vars are configured.
func (p *SSOProvider) Enabled() bool {
	return p.ClientID != "" && p.ClientSecret != "" && p.RedirectURI != ""
}

// SSOConfig holds all configured identity providers.
type SSOConfig struct {
	Providers map[string]*SSOProvider
}

// LoadSSOConfig reads SSO settings from the environment for all providers.
func LoadSSOConfig() *SSOConfig {
	cfg := &SSOConfig{Providers: make(map[string]*SSOProvider)}

	// Microsoft Azure AD / Entra ID
	tenant := os.Getenv("AZURE_TENANT_ID")
	if tenant == "" {
		tenant = "common"
	}
	ms := &SSOProvider{
		Name:         "microsoft",
		ClientID:     os.Getenv("AZURE_CLIENT_ID"),
		ClientSecret: os.Getenv("AZURE_CLIENT_SECRET"),
		RedirectURI:  os.Getenv("AZURE_REDIRECT_URI"),
		AuthURL:      fmt.Sprintf("https://login.microsoftonline.com/%s/oauth2/v2.0/authorize", tenant),
		TokenURL:     fmt.Sprintf("https://login.microsoftonline.com/%s/oauth2/v2.0/token", tenant),
		LogoutURL:    fmt.Sprintf("https://login.microsoftonline.com/%s/oauth2/v2.0/logout", tenant),
		Scopes:       "openid profile email",
	}
	if ms.Enabled() {
		cfg.Providers["microsoft"] = ms
	}

	// Google
	g := &SSOProvider{
		Name:         "google",
		ClientID:     os.Getenv("GOOGLE_CLIENT_ID"),
		ClientSecret: os.Getenv("GOOGLE_CLIENT_SECRET"),
		RedirectURI:  os.Getenv("GOOGLE_REDIRECT_URI"),
		AuthURL:      "https://accounts.google.com/o/oauth2/v2/auth",
		TokenURL:     "https://oauth2.googleapis.com/token",
		LogoutURL:    "",
		Scopes:       "openid profile email",
	}
	if g.Enabled() {
		cfg.Providers["google"] = g
	}

	return cfg
}

// HasAnyProvider returns true when at least one SSO provider is enabled.
func (c *SSOConfig) HasAnyProvider() bool {
	return len(c.Providers) > 0
}

// Get returns the provider by name, or nil.
func (c *SSOConfig) Get(name string) *SSOProvider {
	return c.Providers[strings.ToLower(name)]
}

// ── CSRF state store (in-memory, short-lived) ─────────────────────────────────

var (
	ssoStateStore = make(map[string]time.Time)
	ssoStateMu    sync.Mutex
	ssoStateTTL   = 10 * time.Minute
)

func storeSSOState(state string) {
	ssoStateMu.Lock()
	defer ssoStateMu.Unlock()
	now := time.Now()
	for k, exp := range ssoStateStore {
		if now.After(exp) {
			delete(ssoStateStore, k)
		}
	}
	ssoStateStore[state] = now.Add(ssoStateTTL)
}

func consumeSSOState(state string) bool {
	ssoStateMu.Lock()
	defer ssoStateMu.Unlock()
	exp, ok := ssoStateStore[state]
	if !ok {
		return false
	}
	delete(ssoStateStore, state)
	return time.Now().Before(exp)
}

// ── OIDC token response & ID-token claims ─────────────────────────────────────

type tokenResponse struct {
	AccessToken  string `json:"access_token"`
	IDToken      string `json:"id_token"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int    `json:"expires_in"`
	RefreshToken string `json:"refresh_token,omitempty"`
}

type idTokenClaims struct {
	Email             string
	PreferredUsername string
	Name              string
	Sub               string
}

// ── SSO Handlers ──────────────────────────────────────────────────────────────

// SSOProvidersHandler returns the list of enabled SSO providers.
// GET /auth/sso/providers
func SSOProvidersHandler(cfg *SSOConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		names := make([]string, 0, len(cfg.Providers))
		for k := range cfg.Providers {
			names = append(names, k)
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success":   true,
			"providers": names,
		})
	}
}

// SSOLoginRedirect builds the authorization URL for the requested provider.
// POST /auth/sso/login  { "provider": "microsoft" }
func SSOLoginRedirect(cfg *SSOConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Provider string `json:"provider"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			req.Provider = "microsoft"
		}
		if req.Provider == "" {
			req.Provider = "microsoft"
		}

		provider := cfg.Get(req.Provider)
		if provider == nil {
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{
				"success": false,
				"error":   fmt.Sprintf("SSO provider '%s' is not configured", req.Provider),
			})
			return
		}

		state, err := generateRandomState()
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
				"success": false, "error": "Failed to generate state",
			})
			return
		}
		storeSSOState(state)

		params := url.Values{}
		params.Set("client_id", provider.ClientID)
		params.Set("response_type", "code")
		params.Set("redirect_uri", provider.RedirectURI)
		params.Set("response_mode", "query")
		params.Set("scope", provider.Scopes)
		params.Set("state", state)
		if provider.Name == "google" {
			params.Set("access_type", "offline")
			params.Set("prompt", "select_account")
		}

		redirectURL := provider.AuthURL + "?" + params.Encode()

		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success":      true,
			"redirect_url": redirectURL,
			"state":        state,
			"provider":     provider.Name,
		})
	}
}

// SSOCallbackHandler exchanges the authorization code for tokens, looks up the
// user by email, and creates a session.
//
// GET  /auth/sso/callback/{provider}?code=...&state=...   ← IdP redirect
// POST /auth/sso/callback/{provider}  { "code":"...", "state":"..." }
func SSOCallbackHandler(cfg *SSOConfig, db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		providerName := extractProviderFromPath(r.URL.Path)
		if providerName == "" {
			providerName = "microsoft"
		}

		provider := cfg.Get(providerName)
		if provider == nil {
			msg := fmt.Sprintf("SSO provider '%s' is not configured", providerName)
			if r.Method == http.MethodGet {
				frontendRedirectError(w, r, msg)
			} else {
				writeJSON(w, http.StatusBadRequest, map[string]interface{}{"success": false, "error": msg})
			}
			return
		}

		var code, state string
		isGET := r.Method == http.MethodGet

		if isGET {
			code = r.URL.Query().Get("code")
			state = r.URL.Query().Get("state")
			if errMsg := r.URL.Query().Get("error_description"); errMsg != "" {
				LogSecurityEvent(db, "", "sso_callback_error", providerName+": "+errMsg, extractClientIPFromRequest(r))
				frontendRedirectError(w, r, errMsg)
				return
			}
		} else {
			var req struct {
				Code  string `json:"code"`
				State string `json:"state"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err == nil {
				code = req.Code
				state = req.State
			}
		}

		// Validate CSRF state
		if state == "" || !consumeSSOState(state) {
			msg := "Invalid or expired state parameter. Please try again."
			LogSecurityEvent(db, "", "sso_state_invalid", providerName, extractClientIPFromRequest(r))
			if isGET {
				frontendRedirectError(w, r, msg)
			} else {
				writeJSON(w, http.StatusBadRequest, map[string]interface{}{"success": false, "error": msg})
			}
			return
		}

		if code == "" {
			msg := "Missing authorization code"
			if isGET {
				frontendRedirectError(w, r, msg)
			} else {
				writeJSON(w, http.StatusBadRequest, map[string]interface{}{"success": false, "error": msg})
			}
			return
		}

		// Exchange code for tokens
		tokenResp, err := exchangeCodeForTokens(provider, code)
		if err != nil {
			LogSecurityEvent(db, "", "sso_token_exchange_failed", providerName+": "+err.Error(), extractClientIPFromRequest(r))
			if isGET {
				frontendRedirectError(w, r, "SSO authentication failed")
			} else {
				writeJSON(w, http.StatusUnauthorized, map[string]interface{}{"success": false, "error": "SSO authentication failed"})
			}
			return
		}

		// Parse ID-token claims
		claims, err := parseIDTokenClaims(tokenResp.IDToken)
		if err != nil {
			if isGET {
				frontendRedirectError(w, r, "Failed to read SSO identity")
			} else {
				writeJSON(w, http.StatusUnauthorized, map[string]interface{}{"success": false, "error": "Failed to read SSO identity"})
			}
			return
		}

		email := claims.Email
		if email == "" {
			email = claims.PreferredUsername
		}
		if email == "" {
			if isGET {
				frontendRedirectError(w, r, "No email found in SSO token")
			} else {
				writeJSON(w, http.StatusUnauthorized, map[string]interface{}{"success": false, "error": "No email found in SSO token"})
			}
			return
		}

		// Update sso_provider/sso_subject on user row
		_, _ = db.Exec(
			`UPDATE users SET sso_provider = $1, sso_subject = $2 WHERE LOWER(email) = LOWER($3)`,
			providerName, claims.Sub, email,
		)

		// Authenticate
		clientIP := extractClientIPFromRequest(r)
		session, mfaPending, err := globalAuthService.LoginViaSSO(email, clientIP)
		if err != nil {
			LogSecurityEvent(db, "", "sso_login_failed", email+": "+err.Error(), clientIP)
			if isGET {
				frontendRedirectError(w, r, err.Error())
			} else {
				writeJSON(w, http.StatusUnauthorized, map[string]interface{}{"success": false, "error": err.Error()})
			}
			return
		}

		LogSecurityEvent(db, session.UserID, "sso_login_success", providerName+": "+email, clientIP)

		// Return result
		if isGET {
			base := getFrontendURL()
			if mfaPending {
				target := fmt.Sprintf("%s/sso/callback?mfa_pending=true&user_id=%s&provider=%s",
					base, url.QueryEscape(session.UserID), providerName)
				http.Redirect(w, r, target, http.StatusFound)
			} else {
				target := fmt.Sprintf(
					"%s/sso/callback?success=true&user_id=%s&session_id=%s&name=%s&email=%s&role=%s&role_code=%s&provider=%s",
					base,
					url.QueryEscape(session.UserID),
					url.QueryEscape(session.SessionID),
					url.QueryEscape(session.Name),
					url.QueryEscape(session.Email),
					url.QueryEscape(session.Role),
					url.QueryEscape(session.RoleCode),
					providerName,
				)
				http.Redirect(w, r, target, http.StatusFound)
			}
			return
		}

		if mfaPending {
			writeJSON(w, http.StatusOK, map[string]interface{}{
				"success": true, "mfa_pending": true,
				"user_id": session.UserID, "message": "MFA verification required",
			})
			return
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{"success": true, "session": session})
	}
}

// SSOLogoutHandler performs single logout: clears local session and returns the
// IdP logout URL so the frontend can redirect there.
// POST /auth/sso/logout  { "user_id": "...", "provider": "microsoft" }
func SSOLogoutHandler(cfg *SSOConfig, db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string `json:"user_id"`
			Provider string `json:"provider"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{"success": false, "error": "user_id is required"})
			return
		}
		if req.Provider == "" {
			req.Provider = "microsoft"
		}

		if globalAuthService != nil {
			_ = globalAuthService.Logout(req.UserID)
		}

		LogSecurityEvent(db, req.UserID, "sso_logout", req.Provider, extractClientIPFromRequest(r))

		provider := cfg.Get(req.Provider)
		logoutURL := ""
		if provider != nil && provider.LogoutURL != "" {
			params := url.Values{}
			params.Set("post_logout_redirect_uri", getFrontendURL())
			logoutURL = provider.LogoutURL + "?" + params.Encode()
		}

		writeJSON(w, http.StatusOK, map[string]interface{}{
			"success":        true,
			"message":        "Logged out",
			"idp_logout_url": logoutURL,
		})
	}
}

// ── LoginViaSSO on AuthService ────────────────────────────────────────────────

func (a *AuthService) LoginViaSSO(email string, clientIP string) (*UserSession, bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	var dbUserID, dbName, dbEmail string
	var dbStatus sql.NullString

	err := a.db.QueryRow(
		`SELECT id, employee_name, email, status FROM users WHERE LOWER(email) = LOWER($1)`,
		email,
	).Scan(&dbUserID, &dbName, &dbEmail, &dbStatus)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, false, errors.New("no account linked to this identity")
		}
		return nil, false, errors.New("internal error")
	}

	if dbStatus.Valid && !strings.EqualFold(dbStatus.String, "Approved") &&
		!strings.EqualFold(dbStatus.String, "pending") {
		return nil, false, fmt.Errorf("account status is %s", dbStatus.String)
	}

	a.forceLogoutUser(dbUserID)

	if a.maxUsers > 0 && len(a.users) >= a.maxUsers {
		return nil, false, errors.New("maximum concurrent users reached")
	}

	var roleName, roleCode sql.NullString
	_ = a.db.QueryRow(
		`SELECT r.name, r.rolecode FROM user_roles ur JOIN roles r ON ur.role_id = r.id WHERE ur.user_id = $1 LIMIT 1`,
		dbUserID,
	).Scan(&roleName, &roleCode)

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

	// Check MFA — only pending if BOTH enabled AND secret is configured
	var mfaEnabled sql.NullBool
	var mfaSecret sql.NullString
	_ = a.db.QueryRow(`SELECT mfa_enabled, mfa_secret FROM users WHERE id = $1`, dbUserID).Scan(&mfaEnabled, &mfaSecret)
	if mfaEnabled.Valid && mfaEnabled.Bool && mfaSecret.Valid && mfaSecret.String != "" {
		session.IsLoggedIn = false
		a.users[sessionID] = session
		a.userPointers[dbUserID] = session
		if logger.GlobalLogger != nil {
			logger.GlobalLogger.LogAudit(fmt.Sprintf("SSO login (MFA pending) for %s", email))
		}
		return session, true, nil
	}

	a.users[sessionID] = session
	a.userPointers[dbUserID] = session
	if logger.GlobalLogger != nil {
		logger.GlobalLogger.LogAudit(fmt.Sprintf("SSO login successful for %s", email))
	}
	return session, false, nil
}

// ── Internal helpers ──────────────────────────────────────────────────────────

func extractProviderFromPath(path string) string {
	parts := strings.Split(strings.TrimRight(path, "/"), "/")
	if len(parts) >= 1 {
		last := strings.ToLower(parts[len(parts)-1])
		if last == "microsoft" || last == "google" {
			return last
		}
	}
	return ""
}

func exchangeCodeForTokens(provider *SSOProvider, code string) (*tokenResponse, error) {
	data := url.Values{}
	data.Set("client_id", provider.ClientID)
	data.Set("scope", provider.Scopes)
	data.Set("code", code)
	data.Set("redirect_uri", provider.RedirectURI)
	data.Set("grant_type", "authorization_code")
	data.Set("client_secret", provider.ClientSecret)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, provider.TokenURL, strings.NewReader(data.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("token request failed: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("token endpoint returned %d: %s", resp.StatusCode, string(body))
	}

	var tr tokenResponse
	if err := json.Unmarshal(body, &tr); err != nil {
		return nil, fmt.Errorf("failed to parse token response: %w", err)
	}
	return &tr, nil
}

func parseIDTokenClaims(rawToken string) (*idTokenClaims, error) {
	parser := jwt.NewParser(jwt.WithoutClaimsValidation())
	token, _, err := parser.ParseUnverified(rawToken, jwt.MapClaims{})
	if err != nil {
		return nil, fmt.Errorf("failed to parse id_token: %w", err)
	}
	mc, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return nil, errors.New("unexpected claims type")
	}
	claims := &idTokenClaims{}
	if v, ok := mc["email"].(string); ok {
		claims.Email = v
	}
	if v, ok := mc["preferred_username"].(string); ok {
		claims.PreferredUsername = v
	}
	if v, ok := mc["name"].(string); ok {
		claims.Name = v
	}
	if v, ok := mc["sub"].(string); ok {
		claims.Sub = v
	}
	return claims, nil
}

func generateRandomState() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.URLEncoding.EncodeToString(b), nil
}

func extractClientIPFromRequest(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		parts := strings.Split(xff, ",")
		return strings.TrimSpace(parts[0])
	}
	return r.RemoteAddr
}

func getFrontendURL() string {
	if u := os.Getenv("FRONTEND_URL"); u != "" {
		return strings.TrimRight(u, "/")
	}
	return "http://localhost:5173"
}

func frontendRedirectError(w http.ResponseWriter, r *http.Request, msg string) {
	base := getFrontendURL()
	target := fmt.Sprintf("%s/sso/callback?error=%s", base, url.QueryEscape(msg))
	http.Redirect(w, r, target, http.StatusFound)
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
