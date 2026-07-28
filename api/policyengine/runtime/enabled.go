package runtime

import (
	"os"
	"strings"
)

// PolicyChecksEnabled reports whether business handlers should run policy
// evaluation. Controlled by POLICY_ENGINE_ENABLED:
//
//	true / 1 / yes / on                  → checks run
//	unset / empty / false / 0 / no / off → Enforce / RunCheck no-op pass (default off)
func PolicyChecksEnabled() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("POLICY_ENGINE_ENABLED"))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}
