package dbutil

import (
	"os"
	"strings"
)

// EffectiveSSLMode returns DB_SSLMODE when set, otherwise "require".
func EffectiveSSLMode(host string) string {
	if m := strings.TrimSpace(os.Getenv("DB_SSLMODE")); m != "" {
		return m
	}
	return "require"
}
