// Package oauthmail implements delegated OAuth2 (authorization code + refresh)
// for user mailboxes: Microsoft (personal + work via /common) and Google (Gmail API).
// Client credentials come from env:
//
//	MAIL_OAUTH_MS_CLIENT_ID / MAIL_OAUTH_MS_CLIENT_SECRET
//	MAIL_OAUTH_GOOGLE_CLIENT_ID / MAIL_OAUTH_GOOGLE_CLIENT_SECRET
package oauthmail

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const (
	ProviderMicrosoft = "microsoft"
	ProviderGoogle    = "google"
)

// Tokens is the result of an exchange or refresh.
type Tokens struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int    `json:"expires_in"`
	Scope        string `json:"scope"`
}

type providerConfig struct {
	authURL      string
	tokenURL     string
	scopes       string
	clientID     string
	clientSecret string
	extraAuth    url.Values
}

var httpClient = &http.Client{Timeout: 45 * time.Second}

func configFor(provider, transport string) (providerConfig, error) {
	transport = strings.ToLower(strings.TrimSpace(transport))
	if transport == "" {
		transport = "api"
	}
	switch strings.ToLower(strings.TrimSpace(provider)) {
	case ProviderMicrosoft:
		scopes := "offline_access User.Read Mail.Read"
		if transport == "imap" {
			scopes = "offline_access openid profile email https://outlook.office.com/IMAP.AccessAsUser.All"
		}
		cfg := providerConfig{
			authURL:      "https://login.microsoftonline.com/common/oauth2/v2.0/authorize",
			tokenURL:     "https://login.microsoftonline.com/common/oauth2/v2.0/token",
			scopes:       scopes,
			clientID:     strings.TrimSpace(os.Getenv("MAIL_OAUTH_MS_CLIENT_ID")),
			clientSecret: strings.TrimSpace(os.Getenv("MAIL_OAUTH_MS_CLIENT_SECRET")),
		}
		if cfg.clientID == "" || cfg.clientSecret == "" {
			return cfg, fmt.Errorf("microsoft mail oauth not configured (MAIL_OAUTH_MS_CLIENT_ID / MAIL_OAUTH_MS_CLIENT_SECRET)")
		}
		return cfg, nil
	case ProviderGoogle:
		scopes := "https://www.googleapis.com/auth/gmail.readonly openid email"
		if transport == "imap" {
			scopes = "https://mail.google.com/ openid email"
		}
		extra := url.Values{}
		extra.Set("access_type", "offline")
		extra.Set("prompt", "consent")
		cfg := providerConfig{
			authURL:      "https://accounts.google.com/o/oauth2/v2/auth",
			tokenURL:     "https://oauth2.googleapis.com/token",
			scopes:       scopes,
			clientID:     strings.TrimSpace(os.Getenv("MAIL_OAUTH_GOOGLE_CLIENT_ID")),
			clientSecret: strings.TrimSpace(os.Getenv("MAIL_OAUTH_GOOGLE_CLIENT_SECRET")),
			extraAuth:    extra,
		}
		if cfg.clientID == "" || cfg.clientSecret == "" {
			return cfg, fmt.Errorf("google mail oauth not configured (MAIL_OAUTH_GOOGLE_CLIENT_ID / MAIL_OAUTH_GOOGLE_CLIENT_SECRET)")
		}
		return cfg, nil
	default:
		return providerConfig{}, fmt.Errorf("unsupported oauth provider %q (use microsoft or google)", provider)
	}
}

// SupportedProvider reports whether the provider name is known.
func SupportedProvider(provider string) bool {
	p := strings.ToLower(strings.TrimSpace(provider))
	return p == ProviderMicrosoft || p == ProviderGoogle
}

// AuthorizeURL builds the browser consent URL.
func AuthorizeURL(provider, transport, redirectURI, state string) (string, error) {
	cfg, err := configFor(provider, transport)
	if err != nil {
		return "", err
	}
	q := url.Values{}
	q.Set("client_id", cfg.clientID)
	q.Set("response_type", "code")
	q.Set("redirect_uri", redirectURI)
	q.Set("scope", cfg.scopes)
	q.Set("state", state)
	for k, vs := range cfg.extraAuth {
		for _, v := range vs {
			q.Set(k, v)
		}
	}
	return cfg.authURL + "?" + q.Encode(), nil
}

// Exchange trades an authorization code for tokens.
func Exchange(ctx context.Context, provider, transport, code, redirectURI string) (Tokens, error) {
	cfg, err := configFor(provider, transport)
	if err != nil {
		return Tokens{}, err
	}
	form := url.Values{}
	form.Set("grant_type", "authorization_code")
	form.Set("code", code)
	form.Set("redirect_uri", redirectURI)
	form.Set("client_id", cfg.clientID)
	form.Set("client_secret", cfg.clientSecret)
	return tokenRequest(ctx, cfg.tokenURL, form)
}

// Refresh obtains a fresh access token. Providers may rotate the refresh token;
// callers should persist the returned RefreshToken when non-empty.
func Refresh(ctx context.Context, provider, transport, refreshToken string) (Tokens, error) {
	cfg, err := configFor(provider, transport)
	if err != nil {
		return Tokens{}, err
	}
	form := url.Values{}
	form.Set("grant_type", "refresh_token")
	form.Set("refresh_token", refreshToken)
	form.Set("client_id", cfg.clientID)
	form.Set("client_secret", cfg.clientSecret)
	form.Set("scope", cfg.scopes)
	tokens, err := tokenRequest(ctx, cfg.tokenURL, form)
	if err != nil {
		return tokens, err
	}
	if tokens.RefreshToken == "" {
		tokens.RefreshToken = refreshToken
	}
	return tokens, nil
}

func tokenRequest(ctx context.Context, tokenURL string, form url.Values) (Tokens, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return Tokens{}, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := httpClient.Do(req)
	if err != nil {
		return Tokens{}, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	var tr struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		ExpiresIn    int    `json:"expires_in"`
		Scope        string `json:"scope"`
		Error        string `json:"error"`
		ErrorDesc    string `json:"error_description"`
	}
	if err := json.Unmarshal(body, &tr); err != nil {
		return Tokens{}, fmt.Errorf("oauth token: status=%d body=%s", resp.StatusCode, truncate(string(body), 300))
	}
	if tr.AccessToken == "" {
		if tr.Error != "" {
			return Tokens{}, fmt.Errorf("oauth token: %s — %s", tr.Error, tr.ErrorDesc)
		}
		return Tokens{}, fmt.Errorf("oauth token: empty access_token status=%d body=%s", resp.StatusCode, truncate(string(body), 300))
	}
	if tr.ExpiresIn <= 0 {
		tr.ExpiresIn = 3600
	}
	return Tokens{
		AccessToken:  tr.AccessToken,
		RefreshToken: tr.RefreshToken,
		ExpiresIn:    tr.ExpiresIn,
		Scope:        tr.Scope,
	}, nil
}

// Identity returns the mailbox email address for a delegated access token.
func Identity(ctx context.Context, provider, accessToken string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(provider)) {
	case ProviderMicrosoft:
		var me struct {
			Mail              string `json:"mail"`
			UserPrincipalName string `json:"userPrincipalName"`
		}
		if err := getJSON(ctx, "https://graph.microsoft.com/v1.0/me", accessToken, &me); err != nil {
			return "", err
		}
		email := strings.TrimSpace(me.Mail)
		if email == "" {
			email = strings.TrimSpace(me.UserPrincipalName)
		}
		if email == "" {
			return "", fmt.Errorf("microsoft profile has no email")
		}
		return strings.ToLower(email), nil
	case ProviderGoogle:
		var profile struct {
			EmailAddress string `json:"emailAddress"`
		}
		if err := getJSON(ctx, "https://gmail.googleapis.com/gmail/v1/users/me/profile", accessToken, &profile); err != nil {
			return "", err
		}
		if strings.TrimSpace(profile.EmailAddress) == "" {
			return "", fmt.Errorf("gmail profile has no email")
		}
		return strings.ToLower(strings.TrimSpace(profile.EmailAddress)), nil
	default:
		return "", fmt.Errorf("unsupported oauth provider %q", provider)
	}
}

func getJSON(ctx context.Context, rawURL, accessToken string, out interface{}) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("oauth identity: status=%d body=%s", resp.StatusCode, truncate(string(body), 300))
	}
	return json.Unmarshal(body, out)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
