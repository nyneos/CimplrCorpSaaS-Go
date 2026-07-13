package emailmailbox

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"CimplrCorpSaas/api/constants"
	emailcommon "CimplrCorpSaas/api/email/common"
	emailjobs "CimplrCorpSaas/internal/jobs/email"
	"CimplrCorpSaas/internal/services/mailruntime"

	"github.com/jackc/pgx/v5/pgxpool"
)

// MailboxOAuthFields are delegated OAuth credentials stored per inbox.
type MailboxOAuthFields struct {
	OAuthProvider      string     `json:"oauth_provider"`
	OAuthMailTransport string     `json:"oauth_mail_transport"` // api | imap
	OAuthRefreshToken  string     `json:"oauth_refresh_token,omitempty"`
	OAuthAccessToken   string     `json:"oauth_access_token,omitempty"`
	OAuthScopes        string     `json:"oauth_scopes,omitempty"`
	OAuthConnectedAt   *time.Time `json:"oauth_connected_at,omitempty"`
	OAuthTokenExpires  *time.Time `json:"oauth_token_expires_at,omitempty"`
	OAuthConnected     bool       `json:"oauth_connected,omitempty"`
}

func (f MailboxOAuthFields) ToRuntimeOAuth(accessToken string) mailruntime.OAuthConnection {
	return mailruntime.OAuthConnection{
		Provider:    strings.TrimSpace(f.OAuthProvider),
		AccessToken: strings.TrimSpace(accessToken),
	}
}

func (f MailboxOAuthFields) redacted() MailboxOAuthFields {
	out := f
	if out.OAuthRefreshToken != "" {
		out.OAuthRefreshToken = constants.RedactedPlaceholder
	}
	if out.OAuthAccessToken != "" {
		out.OAuthAccessToken = constants.RedactedPlaceholder
	}
	return out
}

func OAuthPatchPresent(p MailboxOAuthFields) bool {
	return strings.TrimSpace(p.OAuthProvider) != "" ||
		strings.TrimSpace(p.OAuthMailTransport) != "" ||
		(strings.TrimSpace(p.OAuthRefreshToken) != "" && p.OAuthRefreshToken != constants.RedactedPlaceholder)
}

func LoadMailboxOAuthFields(ctx context.Context, pool *pgxpool.Pool, inboxID string) (MailboxOAuthFields, error) {
	var fields MailboxOAuthFields
	err := pool.QueryRow(ctx, fmt.Sprintf(`
		SELECT %s, %s, %s, %s, oauth_connected_at, oauth_token_expires_at
		FROM email_svc.inbox_config
		WHERE inbox_id = $1::uuid
	`, emailjobs.SQLOAuthProvider, emailjobs.SQLOAuthMailTransport, emailjobs.SQLOAuthRefreshToken, emailjobs.SQLOAuthScopes),
		inboxID).Scan(&fields.OAuthProvider, &fields.OAuthMailTransport, &fields.OAuthRefreshToken, &fields.OAuthScopes, &fields.OAuthConnectedAt, &fields.OAuthTokenExpires)
	if err != nil {
		return fields, err
	}
	if strings.TrimSpace(fields.OAuthMailTransport) == "" {
		fields.OAuthMailTransport = "api"
	}
	fields.OAuthConnected = strings.TrimSpace(fields.OAuthProvider) != "" && strings.TrimSpace(fields.OAuthRefreshToken) != ""
	return fields, nil
}

// OAuthTokenSet groups the OAuth token fields persisted by SaveMailboxOAuthTokens,
// keeping the function signature under the project's parameter-count limit.
type OAuthTokenSet struct {
	Provider     string
	RefreshToken string
	AccessToken  string
	Scopes       string
	ExpiresAt    time.Time
}

func SaveMailboxOAuthTokens(ctx context.Context, pool *pgxpool.Pool, inboxID string, tokens OAuthTokenSet) error {
	_, err := pool.Exec(ctx, `
		UPDATE email_svc.inbox_config
		SET oauth_provider = $2,
		    oauth_refresh_token = $3,
		    oauth_access_token = $4,
		    oauth_scopes = $5,
		    oauth_token_expires_at = $6,
		    oauth_connected_at = COALESCE(oauth_connected_at, now()),
		    updated_at = now()
		WHERE inbox_id = $1::uuid
	`, inboxID, tokens.Provider, tokens.RefreshToken, tokens.AccessToken, tokens.Scopes, tokens.ExpiresAt.UTC())
	return err
}

func oauthFrontendRedirectBase() string {
	if u := strings.TrimSpace(os.Getenv("MAIL_OAUTH_FRONTEND_URL")); u != "" {
		return strings.TrimRight(u, "/")
	}
	return strings.TrimRight(strings.TrimSpace(os.Getenv("FRONTEND_URL")), "/")
}

func oauthCallbackRedirectURI(r *http.Request) string {
	if u := strings.TrimSpace(os.Getenv("MAIL_OAUTH_REDIRECT_URI")); u != "" {
		return u
	}
	scheme := "http"
	if r.TLS != nil || strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https") {
		scheme = "https"
	}
	host := r.Host
	if fwd := strings.TrimSpace(r.Header.Get("X-Forwarded-Host")); fwd != "" {
		host = fwd
	}
	return fmt.Sprintf("%s://%s/email/oauth/callback", scheme, host)
}

func randomOAuthState() (string, error) {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

func normalizeOAuthProvider(p string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(p)) {
	case "microsoft", "ms", "outlook":
		return "microsoft", nil
	case "google", "gmail":
		return "google", nil
	default:
		return "", fmt.Errorf("oauth provider must be microsoft or google")
	}
}

func normalizeOAuthTransport(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "imap", "xoauth2":
		return "imap"
	default:
		return "api"
	}
}

// HandleOAuthStart begins the browser OAuth consent flow.
// POST /email/oauth/start { "provider": "microsoft", "mailbox_hint": "...", "inbox_id": "..." }
func HandleOAuthStart(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			emailcommon.RespondMethodNotAllowed(w)
			return
		}
		var req struct {
			Provider     string `json:"provider"`
			Transport    string `json:"transport"`
			MailboxHint  string `json:"mailbox_hint"`
			InboxID      string `json:"inbox_id"`
			UserID       string `json:"user_id"`
			RedirectPath string `json:"redirect_path"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			emailcommon.RespondBadRequest(w, constants.ErrInvalidBody)
			return
		}
		provider, err := normalizeOAuthProvider(req.Provider)
		if err != nil {
			emailcommon.RespondBadRequest(w, err.Error())
			return
		}
		transport := normalizeOAuthTransport(req.Transport)
		userID, _, _, _ := emailcommon.RequestIdentity(r, req.UserID, "")
		if userID == "" {
			emailcommon.RespondUnauthorized(w, "user context missing")
			return
		}

		rt := mailruntime.NewRuntime()
		if !rt.Ready() {
			emailcommon.RespondFailPayload(w, constants.RouteOAuthStart, constants.ErrMailProcessingUnavailable, map[string]interface{}{"ok": false})
			return
		}

		state, err := randomOAuthState()
		if err != nil {
			emailcommon.RespondInternal(w, "failed to generate state")
			return
		}
		redirectURI := oauthCallbackRedirectURI(r)
		expires := time.Now().UTC().Add(15 * time.Minute)
		_, err = pool.Exec(r.Context(), `
			INSERT INTO email_svc.oauth_connect_state (state, provider, transport, user_id, mailbox_hint, redirect_uri, expires_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
		`, state, provider, transport, userID, strings.TrimSpace(req.MailboxHint), redirectURI, expires)
		if err != nil {
			emailcommon.RespondInternal(w, err.Error())
			return
		}

		authURL, err := rt.OAuthAuthorizeURL(r.Context(), provider, transport, redirectURI, state)
		if err != nil {
			emailcommon.RespondFailPayload(w, constants.RouteOAuthStart, err.Error(), map[string]interface{}{"ok": false})
			return
		}

		emailcommon.RespondPayload(w, constants.RouteOAuthStart, map[string]interface{}{
			"authorize_url": authURL,
			"provider":      provider,
			"transport":     transport,
			"state":         state,
			"inbox_id":      strings.TrimSpace(req.InboxID),
		})
	}
}

// HandleOAuthCallback completes OAuth after provider redirect.
// GET /email/oauth/callback?code=...&state=...
func HandleOAuthCallback(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		code := strings.TrimSpace(r.URL.Query().Get("code"))
		state := strings.TrimSpace(r.URL.Query().Get("state"))
		oauthErr := strings.TrimSpace(r.URL.Query().Get("error"))
		frontend := oauthFrontendRedirectBase()

		redirectFail := func(msg string) {
			if frontend == "" {
				http.Error(w, msg, http.StatusBadRequest)
				return
			}
			http.Redirect(w, r, frontend+"/mail-config?oauth=error&message="+urlQueryEscape(msg), http.StatusFound)
		}
		redirectOK := func(email, provider, inboxID string) {
			if frontend == "" {
				w.Header().Set("Content-Type", "text/html")
				fmt.Fprintf(w, "<html><body><p>OAuth connected for %s (%s). You may close this window.</p></body></html>", email, provider)
				return
			}
			q := fmt.Sprintf("oauth=success&provider=%s&email=%s", urlQueryEscape(provider), urlQueryEscape(email))
			if inboxID != "" {
				q += "&inbox_id=" + urlQueryEscape(inboxID)
			}
			http.Redirect(w, r, frontend+"/mail-config?"+q, http.StatusFound)
		}

		if oauthErr != "" {
			desc := strings.TrimSpace(r.URL.Query().Get("error_description"))
			if desc != "" {
				redirectFail(oauthErr + ": " + desc)
			} else {
				redirectFail(oauthErr)
			}
			return
		}
		if code == "" || state == "" {
			redirectFail("missing code or state")
			return
		}

		var provider, transport, userID, mailboxHint, redirectURI string
		var expiresAt time.Time
		err := pool.QueryRow(r.Context(), `
			SELECT provider, COALESCE(transport,'api'), user_id, mailbox_hint, redirect_uri, expires_at
			FROM email_svc.oauth_connect_state WHERE state = $1
		`, state).Scan(&provider, &transport, &userID, &mailboxHint, &redirectURI, &expiresAt)
		if err != nil || time.Now().After(expiresAt) {
			redirectFail("invalid or expired oauth state")
			return
		}
		_, _ = pool.Exec(r.Context(), `DELETE FROM email_svc.oauth_connect_state WHERE state = $1`, state)

		rt := mailruntime.NewRuntime()
		if !rt.Ready() {
			redirectFail(constants.ErrMailProcessingUnavailable)
			return
		}

		exchanged, err := rt.OAuthExchange(r.Context(), provider, transport, code, redirectURI)
		if err != nil {
			redirectFail(err.Error())
			return
		}
		if err := rt.VerifyOAuth(r.Context(), provider, exchanged.AccessToken); err != nil {
			redirectFail(err.Error())
			return
		}

		email := strings.ToLower(strings.TrimSpace(exchanged.Email))
		if email == "" {
			email = strings.ToLower(strings.TrimSpace(mailboxHint))
		}
		if email == "" {
			redirectFail("could not resolve mailbox email from provider")
			return
		}
		if hint := strings.ToLower(strings.TrimSpace(mailboxHint)); hint != "" && hint != email {
			redirectFail(fmt.Sprintf("connected account %s does not match expected %s", email, hint))
			return
		}

		expires := time.Now().UTC().Add(time.Duration(exchanged.ExpiresIn) * time.Second)
		inboxID := strings.TrimSpace(r.URL.Query().Get("inbox_id"))

		// Persist tokens on matching pending inbox or create a draft pending inbox.
		if inboxID == "" {
			_ = pool.QueryRow(r.Context(), `
				SELECT inbox_id::text FROM email_svc.inbox_config
				WHERE mailbox_address = $1 AND is_deleted = false
				  AND processing_status IN ('PENDING_APPROVAL','APPROVED')
				ORDER BY created_at DESC LIMIT 1
			`, email).Scan(&inboxID)
		}

		if inboxID != "" {
			_ = SaveMailboxOAuthTokens(r.Context(), pool, inboxID, OAuthTokenSet{
				Provider: provider, RefreshToken: exchanged.RefreshToken, AccessToken: exchanged.AccessToken, Scopes: exchanged.Scope, ExpiresAt: expires,
			})
			_, _ = pool.Exec(r.Context(), `
				UPDATE email_svc.inbox_config
				SET source_type = 'OAUTH',
				    mailbox_address = $2,
				    oauth_mail_transport = $3,
				    updated_at = now()
				WHERE inbox_id = $1::uuid
			`, inboxID, email, transport)
		} else {
			var newID string
			domain := email
			if at := strings.LastIndex(email, "@"); at > 0 {
				domain = email[at+1:]
			}
			err = pool.QueryRow(r.Context(), `
				INSERT INTO email_svc.inbox_config (
					mailbox_address, display_name, domain, filters_json, poll_interval_secs,
					is_active, processing_status, source_type,
					oauth_provider, oauth_mail_transport, oauth_refresh_token, oauth_access_token, oauth_scopes,
					oauth_token_expires_at, oauth_connected_at,
					owner_user_id, submitted_by
				) VALUES ($1, $2, $3, '{}'::jsonb, 60, false, 'PENDING_APPROVAL', 'OAUTH',
				          $4, $5, $6, $7, $8, $9, now(), $10, $10)
				RETURNING inbox_id::text
			`, email, email, domain, provider, transport, exchanged.RefreshToken, exchanged.AccessToken, exchanged.Scope, expires, userID).Scan(&newID)
			if err == nil {
				inboxID = newID
			}
		}

		redirectOK(email, provider, inboxID)
	}
}

// HandleOAuthStatus returns whether an inbox has OAuth connected.
// POST /email/oauth/status { "inbox_id": "..." }
func HandleOAuthStatus(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			emailcommon.RespondMethodNotAllowed(w)
			return
		}
		var req struct {
			InboxID string `json:"inbox_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			emailcommon.RespondBadRequest(w, constants.ErrInvalidBody)
			return
		}
		fields, err := LoadMailboxOAuthFields(r.Context(), pool, strings.TrimSpace(req.InboxID))
		if err != nil {
			emailcommon.RespondBadRequest(w, "inbox not found")
			return
		}
		emailcommon.RespondPayload(w, "oauth/status", fields.redacted())
	}
}

// HandleOAuthTest verifies stored or supplied OAuth tokens for an inbox.
// POST /email/oauth/test
func HandleOAuthTest(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			emailcommon.RespondMethodNotAllowed(w)
			return
		}
		var req struct {
			InboxID     string `json:"inbox_id"`
			Provider    string `json:"oauth_provider"`
			AccessToken string `json:"oauth_access_token"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			emailcommon.RespondBadRequest(w, constants.ErrInvalidBody)
			return
		}

		rt := mailruntime.NewRuntime()
		if !rt.Ready() {
			emailcommon.RespondFailPayload(w, constants.RouteOAuthTest, constants.ErrMailProcessingUnavailable, map[string]interface{}{"ok": false})
			return
		}

		provider := strings.TrimSpace(req.Provider)
		accessToken := strings.TrimSpace(req.AccessToken)
		transport := "api"
		var mailbox string
		var imapFields MailboxIMAPFields
		if inboxID := strings.TrimSpace(req.InboxID); inboxID != "" {
			fields, err := LoadMailboxOAuthFields(r.Context(), pool, inboxID)
			if err != nil {
				emailcommon.RespondBadRequest(w, "inbox not found")
				return
			}
			if provider == "" {
				provider = fields.OAuthProvider
			}
			transport = fields.OAuthMailTransport
			if accessToken == "" {
				refreshed, rErr := rt.OAuthRefresh(r.Context(), fields.OAuthProvider, fields.OAuthMailTransport, fields.OAuthRefreshToken)
				if rErr != nil {
					emailcommon.RespondFailPayload(w, constants.RouteOAuthTest, rErr.Error(), map[string]interface{}{"ok": false})
					return
				}
				accessToken = refreshed.AccessToken
				exp := time.Now().UTC().Add(time.Duration(refreshed.ExpiresIn) * time.Second)
				_ = SaveMailboxOAuthTokens(r.Context(), pool, inboxID, OAuthTokenSet{
					Provider: fields.OAuthProvider, RefreshToken: refreshed.RefreshToken, AccessToken: refreshed.AccessToken, Scopes: refreshed.Scope, ExpiresAt: exp,
				})
			}
			if strings.EqualFold(transport, "imap") {
				var loadErr error
				mailbox, imapFields, loadErr = LoadMailboxIMAPFields(r.Context(), pool, inboxID)
				if loadErr != nil {
					emailcommon.RespondBadRequest(w, loadErr.Error())
					return
				}
			}
		}
		if provider == "" || accessToken == "" {
			emailcommon.RespondBadRequest(w, "oauth provider and access token required")
			return
		}
		if strings.EqualFold(transport, "imap") {
			imapPayload := imapFields.ToRuntimeIMAP()
			imapPayload.AuthMode = "oauth"
			imapPayload.AccessToken = accessToken
			if err := rt.VerifyIMAP(r.Context(), mailbox, imapPayload); err != nil {
				emailcommon.RespondFailPayload(w, constants.RouteOAuthTest, err.Error(), map[string]interface{}{"ok": false})
				return
			}
		} else {
			if err := rt.VerifyOAuth(r.Context(), provider, accessToken); err != nil {
				emailcommon.RespondFailPayload(w, constants.RouteOAuthTest, err.Error(), map[string]interface{}{"ok": false})
				return
			}
		}
		emailcommon.RespondPayload(w, constants.RouteOAuthTest, map[string]interface{}{
			"ok":       true,
			"provider": provider,
			"message":  "OAuth connection successful",
		})
	}
}

func urlQueryEscape(s string) string {
	return url.QueryEscape(s)
}
