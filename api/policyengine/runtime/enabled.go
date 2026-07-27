package runtime

import (
	"os"
	"strings"
)

// PolicyChecksEnabled reports whether business handlers should run policy
// evaluation. Controlled by POLICY_ENGINE_ENABLED:
//
//	unset / empty / true / 1 / yes / on  → checks run (default)
//	false / 0 / no / off                 → Enforce / RunCheck no-op pass
func PolicyChecksEnabled() bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv("POLICY_ENGINE_ENABLED")))
	if v == "" {
		return true
	}
	switch v {
	case "0", "false", "no", "off":
		return false
	default:
		return true
	}
}
