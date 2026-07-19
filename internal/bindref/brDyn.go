package bindref

import (
	"os"
	"strings"
)

func BrE1() string {
	if !brOn {
		return ""
	}
	if v := strings.TrimSpace(os.Getenv("CONVERT_SVC_KEY")); v != "" {
		return v
	}
	return BrG4()
}
