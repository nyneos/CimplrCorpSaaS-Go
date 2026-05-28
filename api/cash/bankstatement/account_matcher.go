package bankstatement

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"CimplrCorpSaas/internal/logger"
)

// accountMatchScore represents a scored match between a file and an account
type accountMatchScore struct {
	accountNumber string
	score         int
	reasons       []string
}

// matchAccountNumberToFile attempts to find the best matching account number for a file
// using a weighted scoring system that considers multiple signals:
//
// Scoring system:
// - Direct exact match in override: 1000 points (immediate return)
// - Masked pattern in file content + in available accounts: 500 points
// - Filename contains full account number: 400 points
// - Masked pattern in filename + in available accounts: 300 points
// - Filename contains partial account number (suffix match): 200 points
// - Masked pattern found but not in available list: 100 points
//
// Returns the account with the highest score, or empty string if no match found.
func matchAccountNumberToFile(ctx context.Context, db *sql.DB, filename, accountOverride string, availableAccounts []string, fileContent [][]string) string {
	// Strategy 1: Direct override with exact match (highest priority - immediate return)
	if strings.TrimSpace(accountOverride) != "" {
		override := strings.TrimSpace(accountOverride)
		logger.LogError("[ACCOUNT-MATCHER] direct override provided: %s", override)

		// Check if it's a direct match (not masked)
		if !strings.Contains(override, "X") {
			// Verify it exists in DB or availableAccounts list
			if len(availableAccounts) > 0 {
				for _, acc := range availableAccounts {
					if strings.EqualFold(acc, override) {
						logger.LogError("[ACCOUNT-MATCHER] ✓ Direct override %s found in available accounts (1000 points)", override)
						return override
					}
				}
			}

			// Verify against DB
			var exists bool
			err := db.QueryRowContext(ctx, `
				SELECT EXISTS(
					SELECT 1 FROM public.masterbankaccount 
					WHERE account_number = $1 AND is_deleted = false
				)
			`, override).Scan(&exists)
			if err == nil && exists {
				logger.LogError("[ACCOUNT-MATCHER] ✓ Direct override %s verified in DB (1000 points)", override)
				return override
			}
			logger.LogError("[ACCOUNT-MATCHER] override %s not found in DB, will try scoring system", override)
		}
	}

	// Build scoring map for all candidates
	scores := make(map[string]*accountMatchScore)

	// Helper to add score
	addScore := func(account string, points int, reason string) {
		if account == "" {
			return
		}
		if scores[account] == nil {
			scores[account] = &accountMatchScore{
				accountNumber: account,
				score:         0,
				reasons:       []string{},
			}
		}
		scores[account].score += points
		scores[account].reasons = append(scores[account].reasons, reason)
	}

	// Strategy 2: Check for masked patterns in file content (first 20 rows)
	if len(fileContent) > 0 {
		maskedInFile := extractMaskedAccountsFromContent(fileContent)
		logger.LogInfo("[ACCOUNT-MATCHER] found %d masked patterns in file content: %v", len(maskedInFile), maskedInFile)

		for _, masked := range maskedInFile {
			// Try to resolve masked account
			if len(availableAccounts) > 0 {
				// Check against available accounts
				for _, acc := range availableAccounts {
					if matchesMaskedPattern(acc, masked) {
						addScore(acc, 500, fmt.Sprintf("masked pattern '%s' in file content matches account", masked))
						logger.LogInfo("[ACCOUNT-MATCHER]  Masked pattern '%s' in file content matched to %s (+500 points)", masked, acc)
					}
				}
			} else {
				// Try DB lookup
				matched := matchMaskedAccount(ctx, db, masked, availableAccounts)
				if matched != "" {
					addScore(matched, 400, fmt.Sprintf("masked pattern '%s' in file content resolved via DB", masked))
					logger.LogInfo("[ACCOUNT-MATCHER]  Masked pattern '%s' in file content resolved to %s via DB (+400 points)", masked, matched)
				}
			}
		}
	}

	// Strategy 3: Extract from filename patterns
	if filename != "" {
		logger.LogInfo("[ACCOUNT-MATCHER] analyzing filename: %s", filename)

		// Check for masked patterns in filename
		maskedInFilename := extractMaskedAccountsFromString(filename)
		for _, masked := range maskedInFilename {
			if len(availableAccounts) > 0 {
				for _, acc := range availableAccounts {
					if matchesMaskedPattern(acc, masked) {
						addScore(acc, 300, fmt.Sprintf("masked pattern '%s' in filename", masked))
						logger.LogInfo("[ACCOUNT-MATCHER]  Masked pattern '%s' in filename matched to %s (+300 points)", masked, acc)
					}
				}
			} else {
				matched := matchMaskedAccount(ctx, db, masked, availableAccounts)
				if matched != "" {
					addScore(matched, 250, fmt.Sprintf("masked pattern '%s' in filename resolved via DB", masked))
					logger.LogInfo("[ACCOUNT-MATCHER]  Masked pattern '%s' in filename resolved to %s via DB (+250 points)", masked, matched)
				}
			}
		}

		// Extract numeric candidates from filename
		candidateAccounts := extractAccountCandidatesFromFilename(filename)

		for _, candidate := range candidateAccounts {
			// Check against available accounts
			if len(availableAccounts) > 0 {
				for _, acc := range availableAccounts {
					if strings.EqualFold(acc, candidate) {
						addScore(acc, 400, fmt.Sprintf("full account number '%s' in filename", candidate))
						logger.LogInfo("[ACCOUNT-MATCHER]  Filename contains full account %s (+400 points)", acc)
					} else if strings.HasSuffix(acc, candidate) {
						addScore(acc, 200, fmt.Sprintf("partial account number '%s' suffix in filename", candidate))
						logger.LogInfo("[ACCOUNT-MATCHER]  Filename contains suffix %s for account %s (+200 points)", candidate, acc)
					}
				}
			}

			// Check against DB
			var matchedAccount string
			err := db.QueryRowContext(ctx, `
				SELECT account_number FROM public.masterbankaccount 
				WHERE is_deleted = false 
				  AND account_number = $1
				LIMIT 1
			`, candidate).Scan(&matchedAccount)
			if err == nil && matchedAccount != "" {
				addScore(matchedAccount, 350, fmt.Sprintf("full account '%s' in filename verified in DB", candidate))
				logger.LogInfo("[ACCOUNT-MATCHER]  Filename candidate %s matched DB account %s (+350 points)", candidate, matchedAccount)
			} else {
				// Try suffix match
				err = db.QueryRowContext(ctx, `
					SELECT account_number FROM public.masterbankaccount 
					WHERE is_deleted = false 
					  AND account_number LIKE '%' || $1
					LIMIT 1
				`, candidate).Scan(&matchedAccount)
				if err == nil && matchedAccount != "" {
					addScore(matchedAccount, 150, fmt.Sprintf("partial account '%s' suffix in filename, DB matched", candidate))
					logger.LogInfo("[ACCOUNT-MATCHER]  Filename suffix %s matched DB account %s (+150 points)", candidate, matchedAccount)
				}
			}
		}
	}

	// Strategy 4: Check masked pattern in override (if it wasn't a direct match)
	if accountOverride != "" && strings.Contains(accountOverride, "X") {
		if len(availableAccounts) > 0 {
			for _, acc := range availableAccounts {
				if matchesMaskedPattern(acc, accountOverride) {
					addScore(acc, 450, fmt.Sprintf("override masked pattern '%s' matches", accountOverride))
					logger.LogError("[ACCOUNT-MATCHER]  Override masked pattern %s matched to %s (+450 points)", accountOverride, acc)
				}
			}
		} else {
			matched := matchMaskedAccount(ctx, db, accountOverride, availableAccounts)
			if matched != "" {
				addScore(matched, 400, fmt.Sprintf("override masked pattern '%s' resolved via DB", accountOverride))
				logger.LogError("[ACCOUNT-MATCHER]  Override masked pattern %s resolved to %s via DB (+400 points)", accountOverride, matched)
			}
		}
	}

	// Find the account with the highest score
	var bestMatch *accountMatchScore
	for _, match := range scores {
		if bestMatch == nil || match.score > bestMatch.score {
			bestMatch = match
		}
	}

	if bestMatch != nil && bestMatch.score > 0 {
		logger.LogInfo("[ACCOUNT-MATCHER] ✓ BEST MATCH: %s with %d points", bestMatch.accountNumber, bestMatch.score)
		logger.LogInfo("[ACCOUNT-MATCHER] ✓ Reasons: %v", bestMatch.reasons)
		return bestMatch.accountNumber
	}

	logger.LogInfo("[ACCOUNT-MATCHER] ✗ No match found via scoring system, will use file content extraction fallback")
	return ""
}

// extractAccountCandidatesFromFilename extracts possible account numbers from filename.
// Examples:
//   - "BOB_Feb-26.xlsx" → []
//   - "Statement_7987987980009.xlsx" → ["7987987980009"]
//   - "XYZ_728392938030_7987987980009.xlsx" → ["7987987980009", "728392938030"]
//   - "95201994179_GXLSM_0036013907_CAL_28022026_M.xlsx" → ["0036013907", "95201994179", "28022026"]
func extractAccountCandidatesFromFilename(filename string) []string {
	// Remove extension
	base := strings.TrimSuffix(filename, strings.ToLower(strings.TrimPrefix(strings.ToLower(filename), strings.TrimSuffix(strings.ToLower(filename), "."))))

	// Split by common separators
	parts := regexp.MustCompile(`[_\-\s]+`).Split(base, -1)

	var candidates []string
	seen := make(map[string]bool)

	// Extract numeric sequences that look like account numbers (6+ digits)
	accountPattern := regexp.MustCompile(`\d{6,}`)

	for _, part := range parts {
		matches := accountPattern.FindAllString(part, -1)
		for _, match := range matches {
			// Remove leading zeros for matching flexibility
			normalized := strings.TrimLeft(match, "0")
			if normalized == "" {
				normalized = "0"
			}

			// Keep original and normalized versions
			if !seen[match] {
				candidates = append(candidates, match)
				seen[match] = true
			}
			if normalized != match && !seen[normalized] {
				candidates = append(candidates, normalized)
				seen[normalized] = true
			}
		}
	}

	// Sort by length descending (prefer longer account numbers)
	// This helps match full account numbers before partial ones
	for i := 0; i < len(candidates); i++ {
		for j := i + 1; j < len(candidates); j++ {
			if len(candidates[j]) > len(candidates[i]) {
				candidates[i], candidates[j] = candidates[j], candidates[i]
			}
		}
	}

	logger.LogInfo("[ACCOUNT-MATCHER] extracted %d candidates from filename: %v", len(candidates), candidates)
	return candidates
}

// matchMaskedAccount matches a masked account number (e.g., XXXXXXXX009) against real accounts.
// The mask uses 'X' characters for hidden digits and reveals a suffix.
// Returns the first matching account or empty string.
func matchMaskedAccount(ctx context.Context, db *sql.DB, maskedAccount string, availableAccounts []string) string {
	masked := strings.TrimSpace(strings.ToUpper(maskedAccount))
	if masked == "" || !strings.Contains(masked, "X") {
		return ""
	}

	logger.LogInfo("[ACCOUNT-MATCHER] attempting masked account match: %s", masked)

	// Extract the visible suffix (non-X characters at the end)
	suffix := strings.TrimLeft(masked, "X")
	if suffix == "" || suffix == masked {
		logger.LogInfo("[ACCOUNT-MATCHER] no visible suffix found in masked account")
		return ""
	}

	// Count the total expected length
	expectedLen := len(masked)

	logger.LogInfo("[ACCOUNT-MATCHER] masked account suffix: %s, expected length: %d", suffix, expectedLen)

	// Try available accounts first
	if len(availableAccounts) > 0 {
		for _, acc := range availableAccounts {
			acc = strings.TrimSpace(acc)
			if len(acc) == expectedLen && strings.HasSuffix(acc, suffix) {
				logger.LogInfo("[ACCOUNT-MATCHER] masked %s matched available account %s", masked, acc)
				return acc
			}
		}
	}

	// Query DB for accounts matching the suffix pattern
	var matchedAccount string
	query := `
		SELECT account_number FROM public.masterbankaccount 
		WHERE is_deleted = false 
		  AND account_number LIKE '%' || $1
		  AND LENGTH(account_number) = $2
		LIMIT 1
	`
	err := db.QueryRowContext(ctx, query, suffix, expectedLen).Scan(&matchedAccount)
	if err == nil && matchedAccount != "" {
		logger.LogInfo("[ACCOUNT-MATCHER] masked %s matched DB account %s", masked, matchedAccount)
		return matchedAccount
	}

	// Fallback: try without length constraint (in case account has dashes/spaces)
	err = db.QueryRowContext(ctx, `
		SELECT account_number FROM public.masterbankaccount 
		WHERE is_deleted = false 
		  AND account_number LIKE '%' || $1
		LIMIT 1
	`, suffix).Scan(&matchedAccount)
	if err == nil && matchedAccount != "" {
		logger.LogInfo("[ACCOUNT-MATCHER] masked %s matched DB account %s (no length constraint)", masked, matchedAccount)
		return matchedAccount
	}

	logger.LogInfo("[ACCOUNT-MATCHER] no match found for masked account %s", masked)
	return ""
}

// extractMaskedAccountsFromContent searches first 20 rows of file content for masked account patterns
// Returns all unique masked patterns found (e.g., ["XXXXXXXX009", "XXXXXX907"])
func extractMaskedAccountsFromContent(rows [][]string) []string {
	var masked []string
	seen := make(map[string]bool)

	// Check first 20 rows
	limit := 20
	if len(rows) < limit {
		limit = len(rows)
	}

	for i := 0; i < limit; i++ {
		for _, cell := range rows[i] {
			patterns := extractMaskedAccountsFromString(cell)
			for _, pattern := range patterns {
				if !seen[pattern] {
					masked = append(masked, pattern)
					seen[pattern] = true
				}
			}
		}
	}

	return masked
}

// extractMaskedAccountsFromString extracts masked account patterns from a string
// Looks for patterns like: XXXXXXXX009, XXXXXX907, XXX1234, etc.
// Rules: Must contain at least 3 X's followed by at least 3 digits
func extractMaskedAccountsFromString(s string) []string {
	s = strings.TrimSpace(strings.ToUpper(s))
	if s == "" {
		return nil
	}

	var patterns []string

	// Pattern: at least 3 X's followed by at least 3 digits (e.g., XXXXXXXX009, XXX123)
	re := regexp.MustCompile(`X{3,}\d{3,}`)
	matches := re.FindAllString(s, -1)

	for _, match := range matches {
		patterns = append(patterns, match)
	}

	return patterns
}

// matchesMaskedPattern checks if an account number matches a masked pattern
// Example: account "0036013907" matches pattern "XXXXXXXX907"
func matchesMaskedPattern(account, maskedPattern string) bool {
	account = strings.TrimSpace(account)
	masked := strings.TrimSpace(strings.ToUpper(maskedPattern))

	if !strings.Contains(masked, "X") {
		return false
	}

	// Extract the visible suffix
	suffix := strings.TrimLeft(masked, "X")
	if suffix == "" {
		return false
	}

	// Check if account ends with the suffix
	if !strings.HasSuffix(account, suffix) {
		return false
	}

	// Optional: verify length matches (if we want strict matching)
	expectedLen := len(masked)
	if len(account) != expectedLen {
		// Also accept if account is longer (has dashes/spaces that were normalized)
		// but the suffix still matches
		return strings.HasSuffix(account, suffix)
	}

	return true
}

// parseAccountNumbers parses account_number or account_numbers from form values.
// Supports:
//   - Single account: account_number="12345"
//   - Multiple accounts (comma-separated): account_numbers="12345,67890"
//   - Multiple accounts (array): account_numbers[]=12345&account_numbers[]=67890
//
// Returns a deduplicated list of account numbers.
func parseAccountNumbers(formValues map[string][]string) []string {
	accounts := make(map[string]bool)
	var result []string

	addIfNew := func(v string) {
		v = strings.TrimSpace(v)
		if v != "" && !accounts[v] {
			accounts[v] = true
			result = append(result, v)
		}
	}

	// tryParseJSON attempts to decode a JSON array string like ["acc1","acc2"] or ["acc1"]
	tryParseJSON := func(v string) bool {
		v = strings.TrimSpace(v)
		if !strings.HasPrefix(v, "[") {
			return false
		}
		var arr []string
		if err := json.Unmarshal([]byte(v), &arr); err != nil {
			return false
		}
		for _, s := range arr {
			addIfNew(s)
		}
		return true
	}

	// Single account number
	if vals, ok := formValues["account_number"]; ok && len(vals) > 0 {
		for _, v := range vals {
			if !tryParseJSON(v) {
				addIfNew(v)
			}
		}
	}

	// Multiple accounts: JSON array, comma-separated, or repeated field
	if vals, ok := formValues["account_numbers"]; ok && len(vals) > 0 {
		for _, v := range vals {
			if tryParseJSON(v) {
				continue
			}
			// Handle comma-separated
			for _, part := range strings.Split(v, ",") {
				addIfNew(part)
			}
		}
	}

	// Also check for array syntax account_numbers[]
	if vals, ok := formValues["account_numbers[]"]; ok && len(vals) > 0 {
		for _, v := range vals {
			if !tryParseJSON(v) {
				addIfNew(v)
			}
		}
	}

	logger.LogInfo("[ACCOUNT-MATCHER] parsed %d account numbers from form: %v", len(result), result)
	return result
}
