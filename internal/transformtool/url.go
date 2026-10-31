package transformtool

import (
	"os"
	"strings"
	"sync"

	"CimplrCorpSaas/internal/logger"
)

var (
	missingURLOnce sync.Once
	misspelledOnce sync.Once
)

// legacy misspellings still present in some .env files.
var legacyURLEnvKeys = []string{
	"TRANSFORMATION_TOOL_URL", // common typo (extra "ATION")
	"TRANSFORMAT_TOOL_URL",    // alternate typo
}

// BaseURL returns the Transformation Tool API root.
// Requires TRANSFORM_TOOL_URL (fail closed: empty string when unset).
// Legacy misspelled env keys are accepted once with a warning log.
func BaseURL() string {
	if u := strings.TrimSpace(os.Getenv("TRANSFORM_TOOL_URL")); u != "" {
		return strings.TrimRight(u, "/")
	}
	for _, key := range legacyURLEnvKeys {
		if u := strings.TrimSpace(os.Getenv(key)); u != "" {
			misspelledOnce.Do(func() {
				logger.LogError("%s is misspelled; set TRANSFORM_TOOL_URL instead", key)
			})
			return strings.TrimRight(u, "/")
		}
	}
	missingURLOnce.Do(func() {
		logger.LogError("TRANSFORM_TOOL_URL is not set — transformation tool calls will fail closed")
	})
	return ""
}
