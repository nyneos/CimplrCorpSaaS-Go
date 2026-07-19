package api

import (
	"CimplrCorpSaas/api/constants"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/url"
	"os"
	"strings"
)

var debugEnvSensitiveKeys = map[string]bool{
	"DB_PASSWORD":                 true,
	"PAYLOAD_ENC_KEY":             true,
	"RESPONSE_ENC_KEY":            true,
	"AWS_SECRET_ACCESS_KEY":       true,
	"EMAIL_SERVICE_KEY":           true,
	"POLICY_SERVICE_KEY":          true,
	"AZURE_CLIENT_SECRET":         true,
	"GOOGLE_CLIENT_SECRET":        true,
	"MAIL_OAUTH_MS_CLIENT_SECRET": true,
	"MAIL_OAUTH_GOOGLE_CLIENT_SECRET": true,
	"GOOGLE_DWD_PRIVATE_KEY":      true,
	"SMART_CAT_AI_KEY":            true,
	"CONVERT_SVC_KEY":             true,
	"VAPID_PRIVATE_KEY":           true,
}

var debugEnvProcessKeys = []string{
	"ENV", "APP_PORT", "DEVEL_MODE",
	"DB_USER", "DB_PASSWORD", "DB_HOST", "DB_PORT", "DB_NAME", "DB_SSLMODE",
	"UPLOAD_TO_STORAGE", "BANK_STMT_S3_ENABLED", "BANK_STMT_S3_BUCKET", "BANK_STMT_S3_REGION",
	"BANK_STMT_DEBUG_PARSE", "BANK_STMT_URL_EXPIRY_HOURS",
	"ENABLE_ADMIN_OVERRIDE", "ADMIN_USER_IDS", "ADMIN_ROLES",
	"PAYLOAD_ENC_KEY", "RESPONSE_ENC_KEY",
	"CATEGORIZATION_SCHEDULE", "CATEGORIZATION_BATCH_SIZE", "STREAM_ACCESS_KEYS",
	"FRONTEND_URL",
	"VAPID_PUBLIC_KEY", "VAPID_PRIVATE_KEY", "VAPID_SUBJECT",
	"BROWSER_PUSH_ENABLED", "BROWSER_PUSH_POLL_SECS", "BROWSER_PUSH_BATCH_SIZE",
	"BROWSER_PUSH_DIGEST_MINS", "BROWSER_PUSH_DIGEST_THROTTLE", "BROWSER_PUSH_DIGEST_TOP_N",
	"OUTBOX_WORKER_ENABLED", "OUTBOX_WORKER_POLL_SECS",
	"OUTBOX_WORKER_BATCH_SIZE", "OUTBOX_WORKER_TIMEOUT_SECS",
	"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY",
	"AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET", "AZURE_TENANT_ID", "AZURE_REDIRECT_URI", "AZURE_GRAPH_TENANT_LABEL",
	"GOOGLE_CLIENT_ID", "GOOGLE_CLIENT_SECRET", "GOOGLE_REDIRECT_URI",
	"GOOGLE_DWD_SERVICE_ACCOUNT_EMAIL", "GOOGLE_DWD_PRIVATE_KEY", "GOOGLE_DWD_CLIENT_ID", 	"GOOGLE_WORKSPACE_TENANT_LABEL",
	"EMAIL_SERVICE_KEY",
	"EMAIL_POLLER_ENABLED", "EMAIL_POLL_INTERVAL_SECS",
	"GRAPH_POLLER_ENABLED", "GRAPH_POLL_INTERVAL_SECS",
	"OAUTH_POLLER_ENABLED", "OAUTH_POLL_INTERVAL_SECS",
	"EMAIL_POLL_BATCH_SIZE",
	"EMAIL_INBOUND_S3_BUCKET", "EMAIL_INBOUND_S3_REGION",
	"EMAIL_INBOUND_S3_PREFIX", "EMAIL_INBOUND_S3_PARSED_PREFIX", "EMAIL_INBOUND_S3_ATTACH_PREFIX",
	"EMAIL_ALLOWED_MAILBOX_DOMAINS", "EMAIL_SES_RULE_SET_NAME", "EMAIL_SES_IAM_ROLE_ARN",
	"MAIL_OAUTH_MS_CLIENT_ID", "MAIL_OAUTH_MS_CLIENT_SECRET",
	"MAIL_OAUTH_GOOGLE_CLIENT_ID", "MAIL_OAUTH_GOOGLE_CLIENT_SECRET",
	"MAIL_OAUTH_REDIRECT_URI", "MAIL_OAUTH_FRONTEND_URL",
	"SMART_CAT_AI_URL", "SMART_CAT_AI_KEY",
}

func isDebugEnvAllowed() bool {
	if strings.EqualFold(strings.TrimSpace(os.Getenv("DEVEL_MODE")), "true") {
		return true
	}
	return strings.EqualFold(strings.TrimSpace(os.Getenv("ENV")), "uat")
}

func maskDebugEnvValue(key, value string) (display, hexOut, urlOut string) {
	if debugEnvSensitiveKeys[key] {
		if strings.TrimSpace(value) != "" {
			return "[set]", "", ""
		}
		return "", "", ""
	}
	return value, hex.EncodeToString([]byte(value)), url.QueryEscape(value)
}

func readDebugEnvFile() (chosen string, fileStr string) {
	candidatePaths := []string{".env", "/app/.env", os.Getenv("ENV_FILE_PATH")}
	for _, p := range candidatePaths {
		if p == "" {
			continue
		}
		if b, err := os.ReadFile(p); err == nil {
			return p, string(b)
		}
	}
	return "", ""
}

func parseDebugEnvFileEntries(fileStr string) []map[string]string {
	entries := make([]map[string]string, 0)
	for _, line := range strings.Split(fileStr, "\n") {
		l := strings.TrimSpace(line)
		if l == "" || strings.HasPrefix(l, "#") {
			continue
		}
		idx := strings.Index(l, "=")
		if idx <= 0 {
			continue
		}
		key := strings.TrimSpace(l[:idx])
		fv := strings.TrimSpace(l[idx+1:])
		if strings.HasPrefix(fv, "\"") && strings.HasSuffix(fv, "\"") && len(fv) >= 2 {
			fv = fv[1 : len(fv)-1]
		}
		envVal := os.Getenv(key)
		envOut, hexOut, urlOut := maskDebugEnvValue(key, envVal)
		if debugEnvSensitiveKeys[key] && fv != "" {
			fv = "[set]"
		}
		entries = append(entries, map[string]string{
			"key":                  key,
			"file_value":           fv,
			"env_value":            envOut,
			"env_value_hex":        hexOut,
			"env_value_urlencoded": urlOut,
		})
	}
	return entries
}

func buildDebugEnvProcessEntries(filter map[string]struct{}) []map[string]string {
	envEntries := make([]map[string]string, 0)
	for _, k := range debugEnvProcessKeys {
		if filter != nil {
			if _, ok := filter[k]; !ok {
				continue
			}
		}
		v := os.Getenv(k)
		ev, eh, eu := maskDebugEnvValue(k, v)
		envEntries = append(envEntries, map[string]string{
			"key":                  k,
			"env_value":            ev,
			"env_value_hex":        eh,
			"env_value_urlencoded": eu,
		})
	}
	return envEntries
}

// DebugEnvHandler exposes effective process env (and optional .env file) for UAT/dev verification.
// POST /debug/env  body: { "keys": ["EMAIL_SERVICE_KEY"] }  (keys optional — omit for full list)
func DebugEnvHandler(w http.ResponseWriter, r *http.Request) {
	if !isDebugEnvAllowed() {
		http.Error(w, constants.ErrMethodNotAllowed, http.StatusNotFound)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Keys []string `json:"keys"`
	}
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&req)
	}

	filter := map[string]struct{}{}
	if len(req.Keys) > 0 {
		for _, k := range req.Keys {
			k = strings.TrimSpace(k)
			if k != "" {
				filter[k] = struct{}{}
			}
		}
	}

	chosen, fileStr := readDebugEnvFile()
	var fileEntries []map[string]string
	if fileStr != "" {
		fileEntries = parseDebugEnvFileEntries(fileStr)
		if len(filter) > 0 {
			filtered := make([]map[string]string, 0)
			for _, e := range fileEntries {
				if _, ok := filter[e["key"]]; ok {
					filtered = append(filtered, e)
				}
			}
			fileEntries = filtered
		}
	}

	RespondEnvelopeSuccessCompat(w, "debug env snapshot", map[string]interface{}{
		"file_path":       chosen,
		"file_contents":   fileStr,
		"file_urlencoded": url.QueryEscape(fileStr),
		"entries":         fileEntries,
		"env":             buildDebugEnvProcessEntries(filterOrNil(filter)),
	})
}

func filterOrNil(filter map[string]struct{}) map[string]struct{} {
	if len(filter) == 0 {
		return nil
	}
	return filter
}
