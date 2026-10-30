package session

import (
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/constants"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

// HeaderSessionID carries the session id issued at login.
const HeaderSessionID = "X-Session-Id"

var (
	ErrInvalidSession  = errors.New(constants.ErrInvalidSession)
	ErrSessionRequired = errors.New(constants.ErrInvalidSession)
	ErrUserIDRequired  = errors.New(constants.ErrUserIDRequired)
)

// RequireToken reports whether requests without a session header are refused.
func RequireToken() bool {
	return strings.EqualFold(strings.TrimSpace(os.Getenv("AUTH_REQUIRE_SESSION_TOKEN")), "true")
}

// HeaderSession returns the session named by the request header.
// present is false when the header is absent.
func HeaderSession(r *http.Request) (s *auth.UserSession, present bool) {
	sid := strings.TrimSpace(r.Header.Get(HeaderSessionID))
	if sid == "" {
		return nil, false
	}
	s = auth.SessionByID(sid)
	if !s.LoggedIn() {
		return nil, true
	}
	return s, true
}

// Identify resolves the calling session. body is the request body already read
// by the caller (nil when not read). With a session header the session decides
// the user and any user_id sent by the client is replaced by it; the returned
// body is what the caller must hand downstream. Without the header the legacy
// body/query user_id lookup is used unless RequireToken is set.
func Identify(r *http.Request, body []byte) (*auth.UserSession, []byte, error) {
	s, present := HeaderSession(r)
	if present {
		if s == nil {
			return nil, body, ErrInvalidSession
		}
		_ = ClaimedUserID(r, body)
		body = BindUserID(r, body, s.UserID)
		return s, body, nil
	}
	if RequireToken() {
		return nil, body, ErrSessionRequired
	}
	claimed := ClaimedUserID(r, body)
	if claimed == "" {
		return nil, body, ErrUserIDRequired
	}
	s = auth.LoggedInSessionForUser(claimed)
	if s == nil {
		return nil, body, ErrInvalidSession
	}
	return s, body, nil
}

// ClaimedUserID returns the user_id the client sent in the body, form or query.
func ClaimedUserID(r *http.Request, body []byte) string {
	if id := jsonUserID(body); id != "" {
		return id
	}
	if isMultipart(r) {
		if r.MultipartForm == nil {
			if body != nil {
				r.Body = io.NopCloser(bytes.NewReader(body))
			}
			_ = r.ParseMultipartForm(32 << 20)
			if body != nil {
				r.Body = io.NopCloser(bytes.NewReader(body))
			}
		}
		if r.MultipartForm != nil {
			if v := r.MultipartForm.Value[constants.KeyUserID]; len(v) > 0 && v[0] != "" {
				return v[0]
			}
		}
	} else if body != nil && !looksJSON(body) && r.PostForm == nil {
		r.Body = io.NopCloser(bytes.NewReader(body))
		_ = r.ParseForm()
		r.Body = io.NopCloser(bytes.NewReader(body))
	}
	if r.PostForm != nil {
		if v := r.PostForm.Get(constants.KeyUserID); v != "" {
			return v
		}
	}
	return r.URL.Query().Get(constants.KeyUserID)
}

// BindUserID replaces every client-supplied user_id with userID and returns the new body.
func BindUserID(r *http.Request, body []byte, userID string) []byte {
	q := r.URL.Query()
	if q.Has(constants.KeyUserID) {
		q.Set(constants.KeyUserID, userID)
		r.URL.RawQuery = q.Encode()
	}
	if r.Form != nil && r.Form.Has(constants.KeyUserID) {
		r.Form.Set(constants.KeyUserID, userID)
	}
	if r.PostForm != nil && r.PostForm.Has(constants.KeyUserID) {
		r.PostForm.Set(constants.KeyUserID, userID)
	}
	if r.MultipartForm != nil {
		if _, ok := r.MultipartForm.Value[constants.KeyUserID]; ok {
			r.MultipartForm.Value[constants.KeyUserID] = []string{userID}
		}
	}
	if !looksJSON(body) {
		return body
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(body, &m); err != nil {
		return body
	}
	enc, _ := json.Marshal(userID)
	if cur, ok := m[constants.KeyUserID]; ok && bytes.Equal(cur, enc) {
		return body
	}
	m[constants.KeyUserID] = enc
	out, err := json.Marshal(m)
	if err != nil {
		return body
	}
	r.ContentLength = int64(len(out))
	r.Header.Set("Content-Length", fmt.Sprint(len(out)))
	return out
}

func jsonUserID(body []byte) string {
	if !looksJSON(body) {
		return ""
	}
	var m map[string]json.RawMessage
	if json.Unmarshal(body, &m) != nil {
		return ""
	}
	raw, ok := m[constants.KeyUserID]
	if !ok {
		return ""
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return strings.TrimSpace(s)
	}
	var n json.Number
	if json.Unmarshal(raw, &n) == nil {
		return n.String()
	}
	return ""
}

func looksJSON(b []byte) bool {
	t := bytes.TrimSpace(b)
	return len(t) > 0 && t[0] == '{'
}

func isMultipart(r *http.Request) bool {
	return strings.Contains(strings.ToLower(r.Header.Get(constants.ContentTypeText)), constants.ContentTypeMultipart)
}
