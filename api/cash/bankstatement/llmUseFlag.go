package bankstatement

import (
	"os"
	"strings"
)

func bankStatementUseLLM() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("BANK_STATEMENT_USE_LLM"))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}
