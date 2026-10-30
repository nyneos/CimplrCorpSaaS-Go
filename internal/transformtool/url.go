package transformtool

import (
	"os"
	"strings"
)

// BaseURL returns the Transformation Tool API root.
// Reads TRANSFORM_TOOL_URL or TRANSFORMATION_TOOL_URL; empty when neither is set.
func BaseURL() string {
	for _, key := range []string{"TRANSFORM_TOOL_URL", "TRANSFORMATION_TOOL_URL"} {
		if u := strings.TrimSpace(os.Getenv(key)); u != "" {
			return strings.TrimRight(u, "/")
		}
	}
	return ""
}
