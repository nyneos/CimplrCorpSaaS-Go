package gmaildwd

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// ServiceAccountConfig holds Google Workspace domain-wide delegation credentials.
type ServiceAccountConfig struct {
	ServiceAccountEmail string `json:"service_account_email"`
	PrivateKey          string `json:"private_key"`
	ClientID            string `json:"client_id"`
}

func normalizePrivateKeyPEM(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return raw
	}
	raw = strings.ReplaceAll(raw, "\\n", "\n")
	return raw
}

func parseRSAPrivateKey(pemStr string) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode([]byte(normalizePrivateKeyPEM(pemStr)))
	if block == nil {
		return nil, fmt.Errorf("invalid service account private key PEM")
	}
	if key, err := x509.ParsePKCS8PrivateKey(block.Bytes); err == nil {
		if rsaKey, ok := key.(*rsa.PrivateKey); ok {
			return rsaKey, nil
		}
	}
	return x509.ParsePKCS1PrivateKey(block.Bytes)
}

// AccessToken returns a short-lived Gmail API token impersonating impersonateUser.
func AccessToken(ctx context.Context, cfg ServiceAccountConfig, impersonateUser string) (string, error) {
	impersonateUser = strings.TrimSpace(strings.ToLower(impersonateUser))
	saEmail := strings.TrimSpace(cfg.ServiceAccountEmail)
	if saEmail == "" || strings.TrimSpace(cfg.PrivateKey) == "" {
		return "", fmt.Errorf("service account email and private key are required")
	}
	if impersonateUser == "" {
		return "", fmt.Errorf("mailbox user email is required for domain-wide delegation")
	}
	key, err := parseRSAPrivateKey(cfg.PrivateKey)
	if err != nil {
		return "", err
	}
	now := time.Now().UTC()
	claims := jwt.MapClaims{
		"iss":   saEmail,
		"sub":   impersonateUser,
		"scope": "https://www.googleapis.com/auth/gmail.readonly",
		"aud":   "https://oauth2.googleapis.com/token",
		"iat":   now.Unix(),
		"exp":   now.Add(55 * time.Minute).Unix(),
	}
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	signed, err := token.SignedString(key)
	if err != nil {
		return "", fmt.Errorf("sign jwt: %w", err)
	}
	form := url.Values{}
	form.Set("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer")
	form.Set("assertion", signed)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://oauth2.googleapis.com/token", strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("google token status=%d body=%s", resp.StatusCode, truncate(string(body), 300))
	}
	var out struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return "", err
	}
	if strings.TrimSpace(out.AccessToken) == "" {
		return "", fmt.Errorf("google token response missing access_token")
	}
	return out.AccessToken, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
