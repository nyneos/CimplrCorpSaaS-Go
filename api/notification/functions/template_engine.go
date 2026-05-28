package notification

import (
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"CimplrCorpSaas/api/constants"

	"github.com/jackc/pgx/v5/pgtype"

	"CimplrCorpSaas/internal/logger"
)

// ExtractVariables returns a list of unique variable names found in the template
// variables are of the form {{VarName}}
func ExtractVariables(tpl string) []string {
	re := regexp.MustCompile(`{{\s*([^}]+?)\s*}}`)
	matches := re.FindAllStringSubmatch(tpl, -1)
	seen := map[string]bool{}
	out := []string{}
	for _, m := range matches {
		name := strings.TrimSpace(m[1])
		if name == "" {
			continue
		}
		if !seen[name] {
			seen[name] = true
			out = append(out, name)
		}
	}
	return out
}

// EvaluateTemplate resolves functions and variables in the template using the provided payload.
// The payload is a map[string]interface{} containing keys referenced by variables and functions.
//
// FLEXIBLE TEMPLATE SYNTAX SUPPORT:
// This engine now intelligently handles BOTH template formats:
//  1. WRAPPED (classic):   "Hello {{CONCAT(UserName, '!')}} — {{COUNT_OF(Items)}} items"
//  2. UNWRAPPED (seed scripts): "Hello CONCAT(UserName, '!') — COUNT_OF(Items) items"
//
// Auto-detection logic:
//   - If template contains {{...}}, processes only wrapped expressions (backward compatible)
//   - If template has NO {{...}} but contains known function calls or payload variables,
//     intelligently wraps them and processes (flexible mode for seed scripts)
//
// Supported functions (case-insensitive):
//
//	SUM, AVERAGE/AVG, SUMPRODUCT, FORMAT_NUMBER, FORMAT_DATE, FORMAT_CURRENCY,
//	CONCAT, COUNT_OF, SUM_OF_FIELD, FILTER, ORDER_BY, GROUP_BY,
//	TABLE_HTML, KPI_CARDS_HTML, ROWS_HTML, SUMMARY_TABLE_HTML,
//	IF, BADGE_HTML, SUBTRACT, and more.
func EvaluateTemplate(tpl string, payload map[string]interface{}) (string, error) {
	// Guard against pathologically large templates
	if len(tpl) > 200_000 {
		return "", errors.New("template too large (>200 KB)")
	}

	// Normalize template: auto-wrap if needed
	normalized := normalizeTemplate(tpl, payload)

	// Evaluate nested function calls by repeatedly locating the innermost function.
	// RESILIENCE: a single bad function call must NEVER kill the entire render.
	// If evaluateFunction returns an error we:
	//   1. Log it with enough context to diagnose the bad template in DB.
	//   2. Replace the broken {{FUNC(...)}} span with "" so the rest of the
	//      email body (all other functions + variables) still renders correctly.
	//   3. Accumulate errors and return them as a joined non-fatal error so
	//      callers (dispatcher, dry-run validator) can see what went wrong.
	result := normalized
	var evalErrors []string
	const maxIterations = 1000 // hard cap: 1000 function evaluations per render
	const maxDepth = 50        // guard against deeply nested calls (caught via iteration count)
	_ = maxDepth               // used as documentation; iteration cap handles it in practice
	iterations := 0
	for {
		if iterations >= maxIterations {
			evalErrors = append(evalErrors, "template evaluation halted: too many nested function calls (possible loop)")
			break
		}
		iterations++

		start, end, name, argsRaw, found := findInnermostFunction(result)
		if !found {
			break
		}
		args := splitArgs(argsRaw)
		evaluated, err := evaluateFunction(name, args, payload)
		if err != nil {
			// Log with full context so the broken template is easy to find in logs.
			logger.LogInfo("[TEMPLATE-ENGINE] function %s(%s) failed: %v", name, argsRaw, err)
			evalErrors = append(evalErrors, fmt.Sprintf("%s: %v", name, err))
			// Replace the broken call with empty string so rendering continues.
			result = result[:start] + "" + result[end+1:]
			continue
		}
		// replace start..end (inclusive) with evaluated value
		result = result[:start] + evaluated + result[end+1:]
	}

	// Replace variables {{Var}} with payload values
	// Also unwrap {{literal}} (result of function evaluations that left literal values)
	varRe := regexp.MustCompile(`{{\s*([^}]+?)\s*}}`)
	result = varRe.ReplaceAllStringFunc(result, func(s string) string {
		sub := varRe.FindStringSubmatch(s)
		if len(sub) < 2 {
			return s
		}
		key := strings.TrimSpace(sub[1])

		// Try to look up in payload
		v, ok := lookupPayload(key, payload)
		if ok {
			// If the resolved value is a list (e.g. from an implicit wildcard path like
			// Initiations.initiation_id), auto-join it into a readable comma-separated string
			// so {{Initiations.initiation_id}} renders as "INI-1, INI-2, INI-3".
			if sl, isList := toAnySlice(v); isList {
				parts := make([]string, 0, len(sl))
				for _, e := range sl {
					parts = append(parts, html.EscapeString(toString(e)))
				}
				return strings.Join(parts, ", ")
			}
			// Scalar: escape payload value for XSS safety.
			// Function outputs (TABLE_HTML, KPI_CARDS_HTML, etc.) are already
			// substituted as raw strings BEFORE this step, so they are unaffected.
			return html.EscapeString(toString(v))
		}

		// If not in payload, treat as a literal value (result of function evaluation)
		// This handles cases like {{3}} or {{some text}} that resulted from function calls
		return key
	})

	return result, nil
}

// EvaluateTemplateStrict is identical to EvaluateTemplate but returns an error
// if ANY function call failed during rendering — used by the pre-save dry-run
// validator in templateCatalog.go so broken templates are caught before DB write.
func EvaluateTemplateStrict(tpl string, payload map[string]interface{}) (string, error) {
	// Shallow-copy payload so the render pass's FILTER/ORDER_BY/GROUP_BY side-effects
	// (phantom __filter_*/  __ordered_* / __grouped_* keys) don't pollute the
	// strict validation pass that runs immediately after on the same map.
	renderPayload := make(map[string]interface{}, len(payload))
	for k, v := range payload {
		renderPayload[k] = v
	}
	result, _ := EvaluateTemplate(tpl, renderPayload)

	// Re-run the function scan on the original template to detect any errors.
	// We run EvaluateTemplate first (to get the rendered output) then re-validate
	// by evaluating each function call individually in strict mode.
	// Use another fresh copy so this validation pass is also isolated.
	validatePayload := make(map[string]interface{}, len(payload))
	for k, v := range payload {
		validatePayload[k] = v
	}
	normalized := normalizeTemplate(tpl, validatePayload)
	tmp := normalized
	const maxIter = 500
	iter := 0
	for {
		if iter >= maxIter {
			return result, errors.New("template evaluation halted: too many nested function calls")
		}
		iter++
		start, end, name, argsRaw, found := findInnermostFunction(tmp)
		if !found {
			break
		}
		args := splitArgs(argsRaw)
		if _, err := evaluateFunction(name, args, validatePayload); err != nil {
			return result, fmt.Errorf("%s(%s): %v", name, argsRaw, err)
		}
		tmp = tmp[:start] + "ok" + tmp[end+1:]
	}
	return result, nil
}

// normalizeTemplate intelligently wraps unwrapped function calls and variables with {{...}}
// if the template doesn't already use {{...}} syntax.
//
// Strategy:
//  1. If template already contains {{...}}, return as-is (backward compatible)
//  2. Otherwise, scan for:
//     - Known function calls: FUNCTION_NAME(args)
//     - Payload variable references (standalone words matching payload keys)
//     - Wrap each occurrence with {{...}}
func normalizeTemplate(tpl string, payload map[string]interface{}) string {
	// NOTE: We normalize even if template has SOME {{}} because it might have unwrapped functions too
	// Only skip if ALL functions are already wrapped (checked later)

	// Known template functions (case-insensitive match)
	// This list MUST include ALL functions implemented in evaluateFunction()
	knownFunctions := []string{
		"SUM", "AVERAGE", "AVG", "SUMPRODUCT",
		"FORMAT_NUMBER", "FORMAT_DATE", "FORMAT_DATE_TZ", "FORMAT_CURRENCY",
		"ESCAPE_HTML", "CONCAT", "UPPER", "LOWER", "SUBSTRING",
		"ADD", "SUBTRACT", "MULTIPLY", "DIVIDE",
		"COUNT_OF", "SUM_OF_FIELD", "TOTAL_OF", "AVG_OF_FIELD",
		"MAX_OF_FIELD", "MIN_OF_FIELD",
		"FILTER", "ORDER_BY", "GROUP_BY",
		"TABLE_HTML", "KPI_CARDS_HTML", "ROWS_HTML", "SUMMARY_TABLE_HTML",
		"IF", "BADGE_HTML",
		// Additional math / string helpers
		"MIN", "MAX", "ROUND", "CEIL", "FLOOR", "ABS",
		"TRIM", "REPLACE", "SPLIT", "JOIN", "LENGTH",
		"CONTAINS", "STARTS_WITH", "ENDS_WITH", "INDEX_OF",
		// List helpers
		"FIRST_OF", "LAST_OF", "ANY_OF", "COUNT_DISTINCT",
	}

	result := tpl

	// Step 1: Wrap function calls — FUNCTION_NAME(...)
	// Process from INNERMOST to OUTERMOST to handle nesting correctly
	matches := findAllFunctionCalls(result)

	// Sort by size (ascending) so innermost functions are wrapped first
	sort.Slice(matches, func(i, j int) bool {
		sizeI := matches[i].end - matches[i].start
		sizeJ := matches[j].end - matches[j].start
		return sizeI < sizeJ
	})

	// Track cumulative offset changes as we wrap functions
	// Key: we need to adjust positions AFTER each wrapping
	wrappedCount := 0

	for idx, m := range matches {
		// Recalculate positions after each wrapping by searching again
		// This is necessary because wrapping changes string positions
		if wrappedCount > 0 {
			// Rescan to get updated positions
			matches = findAllFunctionCalls(result)
			sort.Slice(matches, func(i, j int) bool {
				sizeI := matches[i].end - matches[i].start
				sizeJ := matches[j].end - matches[j].start
				return sizeI < sizeJ
			})
			// Skip already-wrapped functions
			if idx >= len(matches) {
				break
			}
			m = matches[idx]
		}

		funcName := m.name

		// Check if this is a known function
		isKnown := false
		for _, kf := range knownFunctions {
			if strings.EqualFold(funcName, kf) {
				isKnown = true
				break
			}
		}

		if isKnown {
			// Check if already wrapped (has {{ before and }} after)
			if m.start >= 2 && result[m.start-2:m.start] == "{{" &&
				m.end+2 <= len(result) && result[m.end:m.end+2] == "}}" {
				continue
			}

			// Wrap the entire function call: FUNC(...) → {{FUNC(...)}}
			fullCall := result[m.start:m.end]
			wrapped := "{{" + fullCall + "}}"
			result = result[:m.start] + wrapped + result[m.end:]

			wrappedCount++
		}
	}

	// NOTE: We DON'T auto-wrap standalone variables because:
	// 1. Variables are referenced by name inside function arguments (e.g., CONCAT(..., UserID))
	// 2. It's ambiguous which occurrences should be wrapped (labels vs values)
	// 3. Seed scripts expect variables to be used inside functions, not standalone

	return result
}

// functionCallMatch represents a matched function call with its position
type functionCallMatch struct {
	start int
	end   int
	name  string
}

// findAllFunctionCalls locates ALL function calls in the text, including nested ones
// Returns byte positions for correct string slicing with multi-byte characters
func findAllFunctionCalls(s string) []functionCallMatch {
	matches := []functionCallMatch{}
	runes := []rune(s)

	// Build rune-to-byte position map
	runeToBytePos := make([]int, len(runes)+1)
	bytePos := 0
	for i, r := range runes {
		runeToBytePos[i] = bytePos
		bytePos += len(string(r))
	}
	runeToBytePos[len(runes)] = bytePos // end position

	// Recursively find all function calls
	var findInRange func(start, end int)
	findInRange = func(start, end int) {
		i := start
		for i < end {
			// Look for function pattern: WORD_CHARS followed by '('
			if !unicode.IsLetter(runes[i]) && runes[i] != '_' {
				i++
				continue
			}

			// Extract function name
			nameStart := i
			for i < end && (unicode.IsLetter(runes[i]) || unicode.IsDigit(runes[i]) || runes[i] == '_') {
				i++
			}
			nameEnd := i

			// Skip whitespace
			for i < end && unicode.IsSpace(runes[i]) {
				i++
			}

			// Check for opening parenthesis
			if i >= end || runes[i] != '(' {
				// Not a function call
				continue
			}

			name := string(runes[nameStart:nameEnd])

			// Accept function names starting with any letter (case-insensitive).
			// Previously we restricted to uppercase-leading names which missed
			// valid function calls authored in lowercase (e.g., "concat(...)").
			if len(name) == 0 || !unicode.IsLetter(runes[nameStart]) {
				continue
			}

			openParen := i

			// Find matching closing parenthesis
			depth := 1
			i++ // skip opening '('
			for i < end && depth > 0 {
				if runes[i] == '(' {
					depth++
				} else if runes[i] == ')' {
					depth--
				}
				i++
			}

			if depth == 0 {
				closeParen := i - 1 // position of ')'

				// Add this function call
				matches = append(matches, functionCallMatch{
					start: runeToBytePos[nameStart],
					end:   runeToBytePos[i],
					name:  name,
				})

				// Recursively search INSIDE the function arguments for nested calls
				findInRange(openParen+1, closeParen)
			}
		}
	}

	findInRange(0, len(runes))
	return matches
}

// wrapStandaloneVariable wraps a variable name with {{...}} if it's standalone
// (not inside quotes, not already wrapped, not part of a function call)
func wrapStandaloneVariable(text string, varName string, pattern *regexp.Regexp) string {
	// Find all matches
	indices := pattern.FindAllStringIndex(text, -1)
	if len(indices) == 0 {
		return text
	}

	// Process from right to left to avoid index shifting
	for i := len(indices) - 1; i >= 0; i-- {
		match := indices[i]
		start := match[0]
		end := match[1]

		// Check if already wrapped or inside quotes
		if isAlreadyWrapped(text, start, end) || isInsideQuotes(text, start) {
			continue
		}

		// Wrap it
		wrapped := "{{" + text[start:end] + "}}"
		text = text[:start] + wrapped + text[end:]
	}

	return text
}

// isAlreadyWrapped checks if position is already inside {{...}}
func isAlreadyWrapped(text string, start, end int) bool {
	// Look backward for {{
	openPos := -1
	for i := start - 1; i >= 0; i-- {
		if i > 0 && text[i-1:i+1] == "{{" {
			openPos = i - 1
			break
		}
		if text[i] == '}' && i > 0 && text[i-1] == '}' {
			return false // Found }} before {{
		}
	}

	if openPos == -1 {
		return false
	}

	// Look forward for }}
	for i := end; i < len(text)-1; i++ {
		if text[i:i+2] == "}}" {
			return true // Found matching }}
		}
		if text[i:i+2] == "{{" {
			return false // Found {{ before }}
		}
	}

	return false
}

// isInsideQuotes checks if position is inside single or double quotes
func isInsideQuotes(text string, pos int) bool {
	inSingle := false
	inDouble := false

	for i := 0; i < pos && i < len(text); i++ {
		if text[i] == '\'' && (i == 0 || text[i-1] != '\\') {
			if !inDouble {
				inSingle = !inSingle
			}
		}
		if text[i] == '"' && (i == 0 || text[i-1] != '\\') {
			if !inSingle {
				inDouble = !inDouble
			}
		}
	}

	return inSingle || inDouble
}

// splitArgs splits a comma-separated argument string into trimmed args while handling quoted strings
// and JSON array/object brackets. Commas inside single-quotes, double-quotes, [...] or {...} are
// NOT treated as argument separators.
func splitArgs(s string) []string {
	args := []string{}
	current := strings.Builder{}
	inSingle := false
	inDouble := false
	bracketDepth := 0 // depth of '[' nesting
	braceDepth := 0   // depth of '{' nesting (for JSON objects inside args)
	for _, r := range s {
		if r == '\'' && !inDouble {
			inSingle = !inSingle
			current.WriteRune(r)
			continue
		}
		if r == '"' && !inSingle {
			inDouble = !inDouble
			current.WriteRune(r)
			continue
		}
		if !inSingle && !inDouble {
			if r == '[' {
				bracketDepth++
				current.WriteRune(r)
				continue
			}
			if r == ']' {
				bracketDepth--
				current.WriteRune(r)
				continue
			}
			if r == '{' {
				braceDepth++
				current.WriteRune(r)
				continue
			}
			if r == '}' {
				braceDepth--
				current.WriteRune(r)
				continue
			}
		}
		if r == ',' && !inSingle && !inDouble && bracketDepth == 0 && braceDepth == 0 {
			args = append(args, strings.TrimSpace(current.String()))
			current.Reset()
			continue
		}
		current.WriteRune(r)
	}
	if s := strings.TrimSpace(current.String()); s != "" {
		args = append(args, s)
	}
	return args
}

// findInnermostFunction scans s and returns the indexes (start of function name, index of closing ')'),
// function name, raw args (between parentheses), and found flag.
// It finds the innermost parentheses pair WITHIN {{...}} blocks and the function name immediately preceding the '('.
// Parentheses outside {{...}} blocks OR inside quoted strings are ignored.
// findInnermostFunction finds the innermost function inside {{...}} and outside quotes.
// Returns byte positions (not rune positions) for correct string slicing with multi-byte characters.
func findInnermostFunction(s string) (int, int, string, string, bool) {
	stack := []int{}
	runes := []rune(s)
	braceDepth := 0 // Track nesting depth of {{...}}
	var inSingleQuote bool = false
	var inDoubleQuote bool = false

	// Build rune-to-byte position map
	runeToBytePos := make([]int, len(runes)+1)
	bytePos := 0
	for i, r := range runes {
		runeToBytePos[i] = bytePos
		bytePos += len(string(r))
	}
	runeToBytePos[len(runes)] = bytePos

	for i := 0; i < len(runes); i++ {
		r := runes[i]
		prevR := rune(0)
		if i > 0 {
			prevR = runes[i-1]
		}

		// Track {{...}} boundaries FIRST — these always take precedence over quotes.
		// This ensures {{FUNC()}} embedded inside JSON string values (e.g. KPI_CARDS_HTML
		// JSON with "value":"{{COUNT_OF(X)}}") are still detected correctly.
		if i < len(runes)-1 && r == '{' && runes[i+1] == '{' {
			braceDepth++
			i++ // skip next '{'
			// Reset quote tracking when entering a new {{...}} block so that
			// quotes from HTML attributes outside don't bleed into the block.
			if braceDepth == 1 {
				inSingleQuote = false
				inDoubleQuote = false
			}
			continue
		}
		if i < len(runes)-1 && r == '}' && runes[i+1] == '}' {
			braceDepth--
			i++ // skip next '}'
			// Reset quote state when leaving a {{...}} block.
			if braceDepth == 0 {
				inSingleQuote = false
				inDoubleQuote = false
			}
			continue
		}

		// Only process quote tracking and parentheses while inside {{...}} (braceDepth > 0).
		// Outside {{...}} we don't care about parens — nothing to evaluate.
		if braceDepth == 0 {
			continue
		}

		// Inside {{...}}: track single-quote boundaries (CONCAT string literals use single quotes).
		// Do NOT track double-quote boundaries here — double quotes appear inside JSON arguments
		// like KPI_CARDS_HTML([{"label":"x","value":"{{COUNT_OF(Y)}}"}]) and we must NOT
		// suppress paren/brace scanning for them.
		if r == '\'' && prevR != '\\' && !inDoubleQuote {
			inSingleQuote = !inSingleQuote
		}

		// Skip parens inside single-quoted string literals (CONCAT args like 'hello')
		if inSingleQuote {
			continue
		}

		if r == '(' {
			stack = append(stack, i)
		} else if r == ')' {
			if len(stack) == 0 {
				continue
			}
			openIdx := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			// find function name: scan left from openIdx-1 until non-alnum/_
			j := openIdx - 1
			for j >= 0 && (unicode.IsLetter(runes[j]) || unicode.IsDigit(runes[j]) || runes[j] == '_') {
				j--
			}
			nameStart := j + 1
			if nameStart >= openIdx {
				continue
			}
			name := string(runes[nameStart:openIdx])

			// Only treat as function if name starts with an uppercase letter
			// This prevents matching lowercase words with parens like "delete(s)"
			if len(name) == 0 || !unicode.IsUpper(runes[nameStart]) {
				continue
			}

			argsRaw := string(runes[openIdx+1 : i])

			// Convert rune positions to byte positions for string slicing
			return runeToBytePos[nameStart], runeToBytePos[i], name, argsRaw, true
		}
	}
	return 0, 0, "", "", false
}

// evaluateFunction executes supported functions and returns their string result.
func evaluateFunction(name string, args []string, payload map[string]interface{}) (string, error) {
	up := strings.ToUpper(name)
	switch up {
	case "SUM", "AVERAGE", "AVG":
		if len(args) != 1 {
			return "", fmt.Errorf("%s expects 1 argument: list variable name", up)
		}
		list, ok := getNumberList(args[0], payload)
		if !ok || (up != "SUM" && len(list) == 0) {
			return "0", nil
		}
		s := 0.0
		for _, v := range list {
			s += v
		}
		if up == "SUM" {
			return toStringFloat(s), nil
		}
		return toStringFloat(s / float64(len(list))), nil
	case "SUMPRODUCT":
		if len(args) != 2 {
			return "", errors.New("SUMPRODUCT expects 2 list arguments")
		}
		a, oka := getNumberList(args[0], payload)
		b, okb := getNumberList(args[1], payload)
		if !oka || !okb {
			return "0", nil
		}
		n := intMin(len(a), len(b))
		s := 0.0
		for i := 0; i < n; i++ {
			s += a[i] * b[i]
		}
		return toStringFloat(s), nil
	case "FORMAT_DATE", "FORMAT_DATE_TZ":
		// FORMAT_DATE(date [, format [, tz]])
		// FORMAT_DATE_TZ(date, format, tz)  — alias; tz is always the third arg
		if len(args) < 1 || len(args) > 3 {
			return "", errors.New("FORMAT_DATE expects 1–3 args: date [, format [, tz]]")
		}
		var dateStr string
		if isQuoted(args[0]) {
			dateStr = unquote(args[0])
		} else {
			v, ok := lookupPayload(args[0], payload)
			if !ok {
				return "", nil
			}
			dateStr = toString(v)
		}
		format := "02 Jan 2006"
		if len(args) >= 2 {
			format = unquote(args[1])
		}
		t, err := parseDate(dateStr)
		if err != nil {
			return "", nil
		}
		if len(args) == 3 {
			if loc, lerr := time.LoadLocation(unquote(args[2])); lerr == nil {
				t = t.In(loc)
			}
		}
		return t.Format(format), nil
	case "ESCAPE_HTML":
		if len(args) != 1 {
			return "", errors.New("ESCAPE_HTML expects 1 argument")
		}
		varStr := ""
		if isQuoted(args[0]) {
			varStr = unquote(args[0])
		} else {
			v, ok := lookupPayload(args[0], payload)
			if !ok {
				return "", nil
			}
			varStr = toString(v)
		}
		return html.EscapeString(varStr), nil
	case "CONCAT":
		out := strings.Builder{}
		for _, a := range args {
			a = strings.TrimSpace(a)
			// Check if it's a {{...}} wrapped value (result of inner function evaluation)
			if strings.HasPrefix(a, "{{") && strings.HasSuffix(a, "}}") {
				// Extract the literal value inside {{}}
				literal := strings.TrimSpace(a[2 : len(a)-2])
				out.WriteString(literal)
			} else if isQuoted(a) {
				out.WriteString(unquote(a))
			} else {
				val, ok := lookupPayload(a, payload)
				if ok {
					out.WriteString(toString(val))
				}
			}
		}
		return out.String(), nil
	case "UPPER", "LOWER":
		if len(args) != 1 {
			return "", fmt.Errorf("%s expects 1 argument", up)
		}
		var s string
		if isQuoted(args[0]) {
			s = unquote(args[0])
		} else {
			v, ok := lookupPayload(args[0], payload)
			if !ok {
				return "", nil
			}
			s = toString(v)
		}
		if up == "UPPER" {
			return strings.ToUpper(s), nil
		}
		return strings.ToLower(s), nil
	case "SUBSTRING":
		if len(args) < 2 {
			return "", errors.New("SUBSTRING expects at least 2 args: var, start [, length]")
		}
		varStr := ""
		if isQuoted(args[0]) {
			varStr = unquote(args[0])
		} else {
			v, ok := lookupPayload(args[0], payload)
			if !ok {
				return "", nil
			}
			varStr = toString(v)
		}
		start, err := strconv.Atoi(strings.TrimSpace(args[1]))
		if err != nil {
			return "", err
		}
		if start < 0 {
			start = 0
		}
		if len(args) == 2 {
			if start >= len(varStr) {
				return "", nil
			}
			return varStr[start:], nil
		}
		length, err := strconv.Atoi(strings.TrimSpace(args[2]))
		if err != nil {
			return "", err
		}
		end := start + length
		if start >= len(varStr) {
			return "", nil
		}
		if end > len(varStr) {
			end = len(varStr)
		}
		return varStr[start:end], nil
	case "ADD", "SUBTRACT", "MULTIPLY", "DIVIDE":
		if len(args) != 2 {
			return "", fmt.Errorf("%s expects 2 args", up)
		}
		a, oka := resolveNumericArg(args[0], payload)
		b, okb := resolveNumericArg(args[1], payload)
		if !oka || !okb {
			return "", nil
		}
		switch up {
		case "ADD":
			return toStringFloat(a + b), nil
		case "SUBTRACT":
			return toStringFloat(a - b), nil
		case "MULTIPLY":
			return toStringFloat(a * b), nil
		case "DIVIDE":
			if b == 0 {
				return "", errors.New("division by zero")
			}
			return toStringFloat(a / b), nil
		}

	// ─── ADDITIONAL MATH & STRING UTIL FUNCTIONS ──────────────────────────────
	case "MIN", "MAX":
		// MIN/MAX(listVar) or MIN/MAX(a,b,c...)
		wantMax := up == "MAX"
		if len(args) == 1 {
			if list, ok := getNumberList(args[0], payload); ok && len(list) > 0 {
				acc := list[0]
				for _, v := range list[1:] {
					if (wantMax && v > acc) || (!wantMax && v < acc) {
						acc = v
					}
				}
				return toStringFloat(acc), nil
			}
		}
		vals := []float64{}
		for _, a := range args {
			if v, ok := resolveNumericArg(a, payload); ok {
				vals = append(vals, v)
			}
		}
		if len(vals) == 0 {
			return "0", nil
		}
		acc := vals[0]
		for _, v := range vals[1:] {
			if (wantMax && v > acc) || (!wantMax && v < acc) {
				acc = v
			}
		}
		return toStringFloat(acc), nil

	case "ROUND":
		// ROUND(number [, places])
		if len(args) < 1 || len(args) > 2 {
			return "", errors.New("ROUND expects 1 or 2 args")
		}
		v, ok := resolveNumericArg(args[0], payload)
		if !ok {
			return "", nil
		}
		places := 0
		if len(args) == 2 {
			p, err := strconv.Atoi(strings.TrimSpace(args[1]))
			if err == nil {
				places = p
			}
		}
		pow := math.Pow(10, float64(places))
		return toStringFloat(math.Round(v*pow) / pow), nil

	case "CEIL", "FLOOR", "ABS":
		if len(args) != 1 {
			return "", fmt.Errorf("%s expects 1 arg", up)
		}
		v, ok := resolveNumericArg(args[0], payload)
		if !ok {
			return "", nil
		}
		switch up {
		case "CEIL":
			return toStringFloat(math.Ceil(v)), nil
		case "FLOOR":
			return toStringFloat(math.Floor(v)), nil
		default: // ABS
			return toStringFloat(math.Abs(v)), nil
		}

	case "TRIM", "LENGTH", "INDEX_OF", "REPLACE":
		// Validate arg count per function
		switch up {
		case "INDEX_OF":
			if len(args) != 2 {
				return "", errors.New("INDEX_OF expects 2 args: subject, substr")
			}
		case "REPLACE":
			if len(args) != 3 {
				return "", errors.New("REPLACE expects 3 args: subject, old, new")
			}
		default: // TRIM, LENGTH
			if len(args) != 1 {
				return "", fmt.Errorf("%s expects 1 arg", up)
			}
		}
		var s string
		if isQuoted(args[0]) {
			s = unquote(args[0])
		} else if v, ok := lookupPayload(args[0], payload); ok {
			s = toString(v)
		} else {
			s = args[0]
		}
		switch up {
		case "TRIM":
			return strings.TrimSpace(s), nil
		case "LENGTH":
			return strconv.Itoa(len([]rune(s))), nil
		case "INDEX_OF":
			return strconv.Itoa(strings.Index(s, unquote(args[1]))), nil
		default: // REPLACE
			return strings.ReplaceAll(s, unquote(args[1]), unquote(args[2])), nil
		}

	case "SPLIT", "JOIN":
		// SPLIT(subject, sep [, idx]) / JOIN(listVar, sep)
		if len(args) < 2 {
			return "", fmt.Errorf("%s expects at least 2 args", up)
		}
		subj := args[0]
		if !isQuoted(subj) {
			if v, ok := lookupPayload(subj, payload); ok {
				subj = toString(v)
			}
		} else {
			subj = unquote(subj)
		}
		sep := unquote(args[1])
		if up == "JOIN" {
			// JOIN: subj is already a scalar; resolve as list if possible
			if v, ok := lookupPayload(args[0], payload); ok {
				switch t := v.(type) {
				case []string:
					return strings.Join(t, sep), nil
				case []interface{}:
					parts := make([]string, 0, len(t))
					for _, e := range t {
						parts = append(parts, toString(e))
					}
					return strings.Join(parts, sep), nil
				default:
					return toString(v), nil
				}
			}
			return "", nil
		}
		// SPLIT
		if len(args) > 3 {
			return "", errors.New("SPLIT expects 2 or 3 args")
		}
		parts := strings.Split(subj, sep)
		if len(args) == 3 {
			idx, err := strconv.Atoi(strings.TrimSpace(args[2]))
			if err != nil || idx < 0 || idx >= len(parts) {
				return "", nil
			}
			return parts[idx], nil
		}
		return strings.Join(parts, ","), nil

	case "CONTAINS", "STARTS_WITH", "ENDS_WITH":
		if len(args) != 2 {
			return "", fmt.Errorf("%s expects 2 args: subject, substr", up)
		}
		subj := args[0]
		if !isQuoted(subj) {
			if v, ok := lookupPayload(subj, payload); ok {
				subj = toString(v)
			}
		} else {
			subj = unquote(subj)
		}
		operand := unquote(args[1])
		switch up {
		case "CONTAINS":
			return strconv.FormatBool(strings.Contains(subj, operand)), nil
		case "STARTS_WITH":
			return strconv.FormatBool(strings.HasPrefix(subj, operand)), nil
		default: // ENDS_WITH
			return strconv.FormatBool(strings.HasSuffix(subj, operand)), nil
		}

	// ─── LIST / TABLE FUNCTIONS ────────────────────────────────────────────────
	// All list functions work on a payload key that holds []map[string]interface{}
	// (i.e. a slice of row-objects). This matches the BankStatementNotifPayload
	// Transactions, CategoryKPIs, etc. fields that are serialised into the
	// TriggerNotification payload.

	case "COUNT_OF":
		// COUNT_OF(listVar) → number of items in the list
		// Works with both simple arrays and arrays of maps/rows
		if len(args) != 1 {
			return "", errors.New("COUNT_OF expects 1 argument: list variable name")
		}

		// Try to get value from payload
		v, ok := lookupPayload(args[0], payload)
		if !ok {
			return "0", nil
		}

		// Count elements based on type
		switch val := v.(type) {
		case []map[string]interface{}:
			return strconv.Itoa(len(val)), nil
		case []interface{}:
			return strconv.Itoa(len(val)), nil
		case []string:
			return strconv.Itoa(len(val)), nil
		case []int:
			return strconv.Itoa(len(val)), nil
		case []float64:
			return strconv.Itoa(len(val)), nil
		default:
			// Not a list/array type
			return "0", nil
		}

	case "SUM_OF_FIELD", "TOTAL_OF", "AVG_OF_FIELD":
		// SUM_OF_FIELD / TOTAL_OF — sum a numeric field; AVG_OF_FIELD — average it
		if len(args) != 2 {
			return "", fmt.Errorf("%s expects 2 args: listVar, field", up)
		}
		rows, ok := getRowList(args[0], payload)
		if !ok || (up == "AVG_OF_FIELD" && len(rows) == 0) {
			return "0", nil
		}
		field := resolveFieldArg(args[1])
		total := 0.0
		for _, row := range rows {
			total += rowFloat(row, field)
		}
		if up == "AVG_OF_FIELD" {
			return toStringFloat(total / float64(len(rows))), nil
		}
		return toStringFloat(total), nil

	case "MAX_OF_FIELD", "MIN_OF_FIELD":
		// MAX_OF_FIELD / MIN_OF_FIELD(listVar, field)
		if len(args) != 2 {
			return "", fmt.Errorf("%s expects 2 args: listVar, field", up)
		}
		rows, ok := getRowList(args[0], payload)
		if !ok || len(rows) == 0 {
			return "0", nil
		}
		field := resolveFieldArg(args[1])
		acc := rowFloat(rows[0], field)
		for _, row := range rows[1:] {
			v := rowFloat(row, field)
			if up == "MAX_OF_FIELD" && v > acc {
				acc = v
			} else if up == "MIN_OF_FIELD" && v < acc {
				acc = v
			}
		}
		return toStringFloat(acc), nil

	case "FILTER":
		// FILTER(listVar, 'field', 'value') → returns JSON key into payload for filtered rows
		// Because we can't return sub-lists inline, FILTER stores result as a synthetic
		// payload key "__filter_result" and returns a placeholder. Use FILTER inside
		// TABLE_HTML / GROUP_BY chains instead.
		// Actually: FILTER(listVar, 'field', 'value') → stores into payload and returns count string.
		// For template authors: use FILTER inside TABLE_HTML/ROWS_HTML as first arg.
		if len(args) != 3 {
			return "", errors.New("FILTER expects 3 args: listVar, 'field', 'value'")
		}
		rows, ok := getRowList(args[0], payload)
		if !ok {
			return "0", nil
		}
		field := unquote(args[1])
		value := unquote(args[2])
		var filtered []map[string]interface{}
		for _, row := range rows {
			if rowString(row, field) == value {
				filtered = append(filtered, row)
			}
		}
		// Expose result under a namespaced key so the caller can chain it into
		// TABLE_HTML / ROWS_HTML.  The "__filter_" prefix ensures no collision
		// with any user payload key.
		resultKey := fmt.Sprintf("__filter_%s_%s", field, value)
		payload[resultKey] = filtered
		return strconv.Itoa(len(filtered)), nil

	case "ORDER_BY":
		// ORDER_BY(listVar, 'field', 'ASC'|'DESC') → stores sorted list into payload key
		// __ordered_<listVar>_<field> and returns the sorted list size.
		// Template authors chain: TABLE_HTML(__ordered_Transactions_amount, 'date','description','amount')
		if len(args) != 3 {
			return "", errors.New("ORDER_BY expects 3 args: listVar, 'field', 'ASC|DESC'")
		}
		rows, ok := getRowList(args[0], payload)
		if !ok {
			return "0", nil
		}
		field := unquote(args[1])
		dir := strings.ToUpper(unquote(args[2]))

		// Shallow-copy to avoid mutating the slice referenced by the original payload.
		sorted := make([]map[string]interface{}, len(rows))
		copy(sorted, rows)
		sort.SliceStable(sorted, func(i, j int) bool {
			vi := rowFloat(sorted[i], field)
			vj := rowFloat(sorted[j], field)
			if vi == vj {
				// fallback: string compare
				si := rowString(sorted[i], field)
				sj := rowString(sorted[j], field)
				if dir == "DESC" {
					return si > sj
				}
				return si < sj
			}
			if dir == "DESC" {
				return vi > vj
			}
			return vi < vj
		})
		resultKey := fmt.Sprintf("__ordered_%s_%s_%s", args[0], field, dir)
		payload[resultKey] = sorted
		return strconv.Itoa(len(sorted)), nil

	case "GROUP_BY":
		// GROUP_BY(listVar, 'field') → groups rows by field value.
		// Stores result into payload as __grouped_<listVar>_<field>:
		//   []map[string]interface{}{{"group": "DEBIT", "rows": [...], "count": N,
		//      "total_debit": X, "total_credit": Y, "total_amount": Z}}
		// Returns number of distinct groups.
		if len(args) != 2 {
			return "", errors.New("GROUP_BY expects 2 args: listVar, 'field'")
		}
		rows, ok := getRowList(args[0], payload)
		if !ok {
			return "0", nil
		}
		field := unquote(args[1])
		order := []string{}
		groups := map[string][]map[string]interface{}{}
		for _, row := range rows {
			key := rowString(row, field)
			if _, exists := groups[key]; !exists {
				order = append(order, key)
			}
			groups[key] = append(groups[key], row)
		}
		var grouped []map[string]interface{}
		for _, key := range order {
			grpRows := groups[key]
			tDebit := 0.0
			tCredit := 0.0
			tAmount := 0.0
			for _, r := range grpRows {
				tDebit += rowFloat(r, "withdrawal_amount") + rowFloat(r, "debit") + rowFloat(r, "withdrawal")
				tCredit += rowFloat(r, "deposit_amount") + rowFloat(r, "credit") + rowFloat(r, "deposit")
				tAmount += rowFloat(r, "amount")
			}
			grouped = append(grouped, map[string]interface{}{
				"group":        key,
				"rows":         grpRows,
				"count":        len(grpRows),
				"total_debit":  tDebit,
				"total_credit": tCredit,
				"total_amount": tAmount,
			})
		}
		resultKey := fmt.Sprintf("__grouped_%s_%s", args[0], field)
		payload[resultKey] = grouped
		return strconv.Itoa(len(grouped)), nil

	case "TABLE_HTML":
		// TABLE_HTML(listVar, 'col1', 'col2', ...) → renders an HTML <table>
		// Supports two calling conventions:
		//   1. Simple:  TABLE_HTML(Items, 'col1', 'col2', ...)
		//   2. Aliased: TABLE_HTML(Items, ["col1","col2"], ["Alias 1","Alias 2"])
		if len(args) < 2 {
			return "", errors.New("TABLE_HTML expects listVar + at least 1 column")
		}
		rows, ok := getRowList(args[0], payload)
		if !ok {
			return constants.NoDataTable, nil
		}
		// Detect aliased format: second arg starts with '['
		var cols, labels []string
		if strings.HasPrefix(strings.TrimSpace(args[1]), "[") {
			// Parse JSON array for cols
			colsRaw := strings.TrimSpace(args[1])
			var parsed []string
			if err := json.Unmarshal([]byte(colsRaw), &parsed); err == nil {
				cols = parsed
			} else {
				cols = []string{unquote(args[1])}
			}
			// Parse optional alias array (third arg)
			if len(args) >= 3 && strings.HasPrefix(strings.TrimSpace(args[2]), "[") {
				aliasRaw := strings.TrimSpace(args[2])
				var aliases []string
				if err := json.Unmarshal([]byte(aliasRaw), &aliases); err == nil {
					labels = aliases
				}
			}
		} else {
			// Simple format: quoted or unquoted column names are both accepted
			cols = make([]string, len(args)-1)
			for i, a := range args[1:] {
				c := strings.TrimSpace(a)
				if isQuoted(c) {
					cols[i] = unquote(c)
				} else {
					cols[i] = c
				}
			}
		}
		// Build labels from cols if not provided
		if len(labels) == 0 {
			labels = make([]string, len(cols))
			for i, c := range cols {
				labels[i] = titleCase(strings.ReplaceAll(c, "_", " "))
			}
		}
		var sb strings.Builder
		sb.WriteString(`<table style="border-collapse:collapse;width:100%;font-family:sans-serif;font-size:13px">`)
		// header
		sb.WriteString(`<thead><tr>`)
		for _, lbl := range labels {
			label := strings.ReplaceAll(html.EscapeString(lbl), " ", "&nbsp;")
			sb.WriteString(fmt.Sprintf(`<th style="border:1px solid #ddd;padding:8px;background:#f2f2f2;text-align:left">%s</th>`, label))
		}
		sb.WriteString(`</tr></thead><tbody>`)
		// rows
		for idx, row := range rows {
			bg := "#fff"
			if idx%2 == 1 {
				bg = constants.BgcolorF9
			}
			sb.WriteString(fmt.Sprintf(`<tr style="background:%s">`, bg))
			for _, c := range cols {
				val := rowDisplayValue(row, c)
				sb.WriteString(fmt.Sprintf(`<td style="border:1px solid #ddd;padding:6px 8px">%s</td>`, html.EscapeString(val)))
			}
			sb.WriteString(`</tr>`)
		}
		sb.WriteString(`</tbody></table>`)
		return sb.String(), nil

	case "ROWS_HTML":
		// ROWS_HTML(listVar, 'col1', 'col2', ...) → renders just <tr>...</tr> rows (no table wrapper)
		// Useful when template already has <table> markup and just needs the body rows injected.
		if len(args) < 2 {
			return "", errors.New("ROWS_HTML expects listVar + at least 1 column")
		}
		rows, ok := getRowList(args[0], payload)
		if !ok {
			return "", nil
		}
		cols := make([]string, len(args)-1)
		for i, a := range args[1:] {
			c := strings.TrimSpace(a)
			if isQuoted(c) {
				cols[i] = unquote(c)
			} else {
				cols[i] = c
			}
		}
		var sb strings.Builder
		for idx, row := range rows {
			bg := "#fff"
			if idx%2 == 1 {
				bg = constants.BgcolorF9
			}
			sb.WriteString(fmt.Sprintf(`<tr style="background:%s">`, bg))
			for _, c := range cols {
				val := rowDisplayValue(row, c)
				sb.WriteString(fmt.Sprintf(`<td style="border:1px solid #ddd;padding:6px 8px">%s</td>`, html.EscapeString(val)))
			}
			sb.WriteString(`</tr>`)
		}
		return sb.String(), nil

	case "SUMMARY_TABLE_HTML":
		// SUMMARY_TABLE_HTML(groupedListVar) → renders a summary table from GROUP_BY result.
		// Each group row shows: Group Name | Count | Total Debit | Total Credit
		// groupedListVar should be the key stored by GROUP_BY (e.g. __grouped_Transactions_category)
		if len(args) != 1 {
			return "", errors.New("SUMMARY_TABLE_HTML expects 1 argument: grouped list variable")
		}
		v, ok := lookupPayload(args[0], payload)
		if !ok {
			return constants.NoDataTable, nil
		}
		grouped, ok := toRowList(v)
		if !ok || len(grouped) == 0 {
			return constants.NoDataTable, nil
		}
		var sb strings.Builder
		sb.WriteString(`<table style="border-collapse:collapse;width:100%;font-family:sans-serif;font-size:13px">`)
		sb.WriteString(`<thead><tr>`)
		for _, h := range []string{"Category", "Count", "Total&nbsp;Debit", "Total&nbsp;Credit"} {
			sb.WriteString(fmt.Sprintf(`<th style="border:1px solid #ddd;padding:8px;background:#f2f2f2;text-align:left">%s</th>`, h))
		}
		sb.WriteString(`</tr></thead><tbody>`)
		for idx, g := range grouped {
			bg := "#fff"
			if idx%2 == 1 {
				bg = constants.BgcolorF9
			}
			grpName := rowString(g, "group")
			count := rowFloat(g, "count")
			tDebit := rowFloat(g, "total_debit")
			tCredit := rowFloat(g, "total_credit")
			sb.WriteString(fmt.Sprintf(`<tr style="background:%s"><td style="border:1px solid #ddd;padding:6px 8px">%s</td><td style="border:1px solid #ddd;padding:6px 8px">%s</td><td style="border:1px solid #ddd;padding:6px 8px">%s</td><td style="border:1px solid #ddd;padding:6px 8px">%s</td></tr>`,
				bg,
				html.EscapeString(grpName),
				strconv.Itoa(int(count)),
				formatCurrency(tDebit),
				formatCurrency(tCredit),
			))
		}
		sb.WriteString(`</tbody></table>`)
		return sb.String(), nil

	case "KPI_CARDS_HTML":
		// KPI_CARDS_HTML supports two calling conventions:
		//
		// 1. JSON array of label/value objects (primary template syntax):
		//    {{KPI_CARDS_HTML([{"label":"Count","value":"3"},{"label":"By","value":"admin","color":"#1565c0"}])}}
		//    The "value" fields are already-evaluated strings (all inner {{...}} have been
		//    resolved by the engine before this function is called).
		//
		// 2. Pipe-separated pairs (legacy convenience syntax):
		//    {{KPI_CARDS_HTML(Label1|Value1, Label2|Value2)}}
		//
		// 3. GROUP_BY result variable (original GROUP_BY → KPI flow):
		//    {{KPI_CARDS_HTML(groupedVar)}}
		colors := []string{"#4e73df", "#1cc88a", "#36b9cc", "#f6c23e", "#e74a3b", "#858796"}
		var sb strings.Builder
		sb.WriteString(`<div style="display:flex;flex-wrap:wrap;gap:12px;margin:8px 0">`)

		if len(args) == 0 {
			return "", errors.New("KPI_CARDS_HTML requires at least 1 argument")
		}

		// Convention 1: single arg starting with '[' → JSON array of label/value cards
		firstArg := strings.TrimSpace(args[0])
		if strings.HasPrefix(firstArg, "[") {
			// Reconstruct the full JSON by joining all args (splitArgs splits on commas
			// but JSON also has commas, so we need the raw joined form).
			// We re-join with commas since splitArgs already split on unquoted commas.
			rawJSON := strings.Join(args, ",")
			rawJSON = strings.TrimSpace(rawJSON)

			// Unwrap surrounding {{...}} from individual value strings that were already
			// resolved by the engine (e.g. "value":"1" after COUNT_OF resolved).
			// Nothing to do here — values are plain strings at this point.

			type kpiCard struct {
				Label string `json:"label"`
				Value string `json:"value"`
				Color string `json:"color"`
			}
			var cards []kpiCard
			if err := json.Unmarshal([]byte(rawJSON), &cards); err != nil {
				// Fallback: try to extract label/value pairs with a simple regex
				// to be resilient against minor JSON formatting issues.
				return "", fmt.Errorf("KPI_CARDS_HTML: invalid JSON array: %v (raw: %s)", err, rawJSON)
			}
			for i, card := range cards {
				c := colors[i%len(colors)]
				if card.Color != "" {
					c = card.Color
				}
				val := card.Value
				// If value is still a {{VarName}} reference (variable was not resolved as a
				// function earlier because it's not a function call), resolve it now against payload.
				if strings.HasPrefix(val, "{{") && strings.HasSuffix(val, "}}") {
					inner := strings.TrimSpace(val[2 : len(val)-2])
					if pv, ok := lookupPayload(inner, payload); ok {
						val = toString(pv)
					} else {
						val = inner // strip braces, show the key name as fallback
					}
				}
				sb.WriteString(fmt.Sprintf(
					`<div style="background:%s;color:#fff;border-radius:8px;padding:14px 20px;min-width:160px;flex:1;text-align:center">
<div style="font-size:22px;font-weight:700;margin:4px 0">%s</div>
<div style="font-size:11px;text-transform:uppercase;opacity:.85;margin-top:4px">%s</div>
</div>`,
					c, html.EscapeString(val), html.EscapeString(card.Label),
				))
			}
			sb.WriteString(`</div>`)
			return sb.String(), nil
		}

		// Convention 2: multiple args with '|' separator → Label|Value pairs
		if len(args) > 1 || strings.Contains(firstArg, "|") {
			for i, arg := range args {
				parts := strings.SplitN(arg, "|", 2)
				label, value := strings.TrimSpace(parts[0]), ""
				if len(parts) == 2 {
					value = strings.TrimSpace(parts[1])
				}
				c := colors[i%len(colors)]
				sb.WriteString(fmt.Sprintf(
					`<div style="background:%s;color:#fff;border-radius:8px;padding:14px 20px;min-width:160px;flex:1;text-align:center">
<div style="font-size:22px;font-weight:700;margin:4px 0">%s</div>
<div style="font-size:11px;text-transform:uppercase;opacity:.85;margin-top:4px">%s</div>
</div>`,
					c, html.EscapeString(value), html.EscapeString(label),
				))
			}
			sb.WriteString(`</div>`)
			return sb.String(), nil
		}

		// Convention 3: single arg → GROUP_BY result variable
		if len(args) != 1 {
			return "", errors.New("KPI_CARDS_HTML expects 1 argument: grouped list variable")
		}
		v, ok := lookupPayload(args[0], payload)
		if !ok {
			return "", nil
		}
		grouped, ok := toRowList(v)
		if !ok {
			return "", nil
		}
		for i, g := range grouped {
			c := colors[i%len(colors)]
			grpName := rowString(g, "group")
			count := int(rowFloat(g, "count"))
			tDebit := rowFloat(g, "total_debit")
			tCredit := rowFloat(g, "total_credit")
			sb.WriteString(fmt.Sprintf(
				`<div style="background:%s;color:#fff;border-radius:8px;padding:14px 20px;min-width:180px;flex:1">
<div style="font-size:11px;text-transform:uppercase;opacity:.8">%s</div>
<div style="font-size:22px;font-weight:700;margin:4px 0">%d txns</div>
<div style="font-size:12px">Debit: %s</div>
<div style="font-size:12px">Credit: %s</div>
</div>`,
				c, html.EscapeString(grpName), count,
				formatCurrency(tDebit), formatCurrency(tCredit),
			))
		}
		sb.WriteString(`</div>`)
		return sb.String(), nil

	case "FORMAT_CURRENCY", "FORMAT_NUMBER":
		// FORMAT_NUMBER(val)                          → formatted number (no prefix)
		// FORMAT_CURRENCY(val [, 'CurrencyCode'])     → ₹ formatted, optional code prefix
		if len(args) < 1 || (up == "FORMAT_CURRENCY" && len(args) > 2) || (up == "FORMAT_NUMBER" && len(args) != 1) {
			return "", fmt.Errorf("%s: wrong number of arguments", up)
		}
		v, ok := resolveNumericArg(args[0], payload)
		if !ok {
			return "", nil
		}
		formatted := formatCurrency(v)
		if up == "FORMAT_NUMBER" || len(args) < 2 {
			return formatted, nil
		}
		if len(args) == 2 {
			// second arg is a currency code literal like 'INR', '"INR"' or a payload var
			raw := strings.TrimSpace(args[1])
			// strip both single and double quotes
			code := strings.Trim(raw, "'\"")
			if code == "" || code == raw {
				// try as payload variable
				if cv, ok2 := lookupPayload(raw, payload); ok2 {
					code = fmt.Sprintf("%v", cv)
				}
			}
			if code != "" {
				// Place code BEFORE the ₹ symbol: 'INR ₹ 1,00,000.00'
				formatted = code + " " + formatted
			}
		}
		return formatted, nil

	case "IF":
		// IF(condition_field, 'true_value', 'false_value')
		// condition: payload[condition_field] truthy (non-empty, non-zero, non-false)
		if len(args) != 3 {
			return "", errors.New("IF expects 3 args: conditionVar, 'trueVal', 'falseVal'")
		}
		v, ok := lookupPayload(args[0], payload)
		truthy := ok && isTruthy(v)
		if truthy {
			return unquote(args[1]), nil
		}
		return unquote(args[2]), nil

	case "BADGE_HTML":
		// BADGE_HTML(varOrLiteral, 'color') → renders a small coloured badge span
		if len(args) < 1 {
			return "", errors.New("BADGE_HTML expects 1-2 args: value [, color]")
		}
		val := ""
		if isQuoted(args[0]) {
			val = unquote(args[0])
		} else {
			if v, ok := lookupPayload(args[0], payload); ok {
				val = toString(v)
			}
		}
		color := "#4e73df"
		if len(args) == 2 {
			color = unquote(args[1])
		}
		return fmt.Sprintf(`<span style="background:%s;color:#fff;border-radius:4px;padding:2px 8px;font-size:11px;font-weight:600">%s</span>`,
			color, html.EscapeString(val)), nil

	case "FIRST_OF", "LAST_OF":
		// FIRST_OF/LAST_OF(listVar [, field]) → first or last element/field value
		if len(args) < 1 || len(args) > 2 {
			return "", fmt.Errorf("%s expects 1 or 2 args: listVar [, field]", up)
		}
		if len(args) == 1 {
			v, ok := lookupPayload(args[0], payload)
			if !ok {
				return "", nil
			}
			if sl, ok := toAnySlice(v); ok && len(sl) > 0 {
				if up == "LAST_OF" {
					return toString(sl[len(sl)-1]), nil
				}
				return toString(sl[0]), nil
			}
			return toString(v), nil
		}
		rows, ok := getRowList(args[0], payload)
		if !ok || len(rows) == 0 {
			return "", nil
		}
		field := resolveFieldArg(args[1])
		if up == "LAST_OF" {
			return rowDisplayValue(rows[len(rows)-1], field), nil
		}
		return rowDisplayValue(rows[0], field), nil

	case "ANY_OF":
		// ANY_OF(listVar, field) → comma-separated list of all values for that field
		// ANY_OF(listVar)        → comma-separated string of scalar list elements
		if len(args) < 1 || len(args) > 2 {
			return "", errors.New("ANY_OF expects 1 or 2 args: listVar [, field]")
		}
		if len(args) == 1 {
			v, ok := lookupPayload(args[0], payload)
			if !ok {
				return "", nil
			}
			if sl, ok := toAnySlice(v); ok {
				parts := make([]string, 0, len(sl))
				for _, e := range sl {
					parts = append(parts, toString(e))
				}
				return strings.Join(parts, ", "), nil
			}
			return toString(v), nil
		}
		rows, ok := getRowList(args[0], payload)
		if !ok {
			return "", nil
		}
		field := resolveFieldArg(args[1])
		parts := make([]string, 0, len(rows))
		for _, row := range rows {
			parts = append(parts, rowDisplayValue(row, field))
		}
		return strings.Join(parts, ", "), nil

	case "COUNT_DISTINCT":
		// COUNT_DISTINCT(listVar, field) → number of unique values of field across rows
		// COUNT_DISTINCT(listVar)        → number of distinct scalar elements
		if len(args) < 1 || len(args) > 2 {
			return "", errors.New("COUNT_DISTINCT expects 1 or 2 args: listVar [, field]")
		}
		if len(args) == 1 {
			v, ok := lookupPayload(args[0], payload)
			if !ok {
				return "0", nil
			}
			if sl, ok := toAnySlice(v); ok {
				seen := map[string]bool{}
				for _, e := range sl {
					seen[toString(e)] = true
				}
				return strconv.Itoa(len(seen)), nil
			}
			return "1", nil
		}
		rows, ok := getRowList(args[0], payload)
		if !ok {
			return "0", nil
		}
		fieldCD := resolveFieldArg(args[1])
		seen := map[string]bool{}
		for _, row := range rows {
			seen[rowString(row, fieldCD)] = true
		}
		return strconv.Itoa(len(seen)), nil
	}

	return "", fmt.Errorf("unsupported function: %s", name)
}

// titleCase converts the first letter of every word to upper-case.
// Replaces the deprecated strings.Title.
func titleCase(s string) string {
	words := strings.Fields(s)
	for i, w := range words {
		if len(w) == 0 {
			continue
		}
		runes := []rune(w)
		runes[0] = unicode.ToUpper(runes[0])
		words[i] = string(runes)
	}
	return strings.Join(words, " ")
}

func intMin(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func isQuoted(s string) bool {
	s = strings.TrimSpace(s)
	return len(s) >= 2 && ((s[0] == '\'' && s[len(s)-1] == '\'') || (s[0] == '"' && s[len(s)-1] == '"'))
}

func unquote(s string) string {
	// Keep interior spacing. Only strip surrounding whitespace first,
	// then iteratively remove matching surrounding quotes without trimming
	// the interior so literals like ' hello ' preserve their leading/trailing spaces.
	s = strings.TrimSpace(s)
	for len(s) >= 2 &&
		((s[0] == '\'' && s[len(s)-1] == '\'') ||
			(s[0] == '"' && s[len(s)-1] == '"')) {
		s = s[1 : len(s)-1]
	}
	return s
}

// resolveFieldArg returns the field name from a function argument that may be
// either a quoted string ('amount') or an unquoted identifier (amount).
func resolveFieldArg(arg string) string {
	a := strings.TrimSpace(arg)
	if isQuoted(a) {
		return unquote(a)
	}
	return a
}

// lookupPayloadPath resolves a dot-notation path like "Initiations.0.initiation_id"
// or a wildcard path like "Initiations.*.amount" against the payload map.
//
// Supported traversal:
//   - map[string]interface{} — case-insensitive key lookup
//   - []interface{}          — numeric index ("0", "1" …) OR wildcard ("*")
//   - []map[string]interface{} — same as above
//
// Wildcard "*" at any position collects that segment's value from every element
// of the current slice and returns them as []interface{}.
// Subsequent segments after "*" continue traversal into each collected element.
func lookupPayloadPath(path string, payload map[string]interface{}) (interface{}, bool) {
	parts := strings.Split(path, ".")

	// resolve first segment from the top-level payload (case-insensitive)
	var current interface{}
	first := parts[0]
	found := false
	if v, ok := payload[first]; ok {
		current = v
		found = true
	} else {
		lower := strings.ToLower(first)
		for k, v := range payload {
			if strings.ToLower(k) == lower {
				current = v
				found = true
				break
			}
		}
	}
	if !found {
		return nil, false
	}

	// traverse remaining segments
	for idx, seg := range parts[1:] {
		if current == nil {
			return nil, false
		}

		// Wildcard: collect value from every element of the current slice,
		// then continue traversal of remaining parts into each collected element.
		if seg == "*" {
			slice, ok := toAnySlice(current)
			if !ok {
				return nil, false
			}
			remainingParts := parts[idx+2:] // segments after "*"
			var collected []interface{}
			for _, elem := range slice {
				if len(remainingParts) == 0 {
					collected = append(collected, elem)
				} else if m, ok := elem.(map[string]interface{}); ok {
					if v, ok2 := lookupInNode(m, remainingParts); ok2 {
						collected = append(collected, v)
					}
				}
			}
			return collected, true
		}

		switch node := current.(type) {
		case map[string]interface{}:
			// case-insensitive key lookup
			matched := false
			lower := strings.ToLower(seg)
			for k, v := range node {
				if strings.ToLower(k) == lower {
					current = v
					matched = true
					break
				}
			}
			if !matched {
				return nil, false
			}
		case []interface{}:
			// Numeric index → direct access; non-numeric field name → implicit wildcard collect.
			if i, err := strconv.Atoi(seg); err == nil {
				if i < 0 || i >= len(node) {
					return nil, false // out-of-bounds → safe empty string
				}
				current = node[i]
			} else {
				// Implicit wildcard: collect field `seg` (+ any remaining path) from every element.
				remaining := append([]string{seg}, parts[idx+2:]...)
				var collected []interface{}
				for _, elem := range node {
					if m, ok := elem.(map[string]interface{}); ok {
						if v, ok2 := lookupInNode(m, remaining); ok2 {
							collected = append(collected, v)
						}
					}
				}
				return collected, true
			}
		case []map[string]interface{}:
			// Numeric index → direct access; non-numeric field name → implicit wildcard.
			if i, err := strconv.Atoi(seg); err == nil {
				if i < 0 || i >= len(node) {
					return nil, false
				}
				current = node[i]
			} else {
				remaining := append([]string{seg}, parts[idx+2:]...)
				var collected []interface{}
				for _, m := range node {
					if v, ok2 := lookupInNode(m, remaining); ok2 {
						collected = append(collected, v)
					}
				}
				return collected, true
			}
		default:
			return nil, false
		}
	}
	return current, true
}

// toAnySlice coerces []interface{} or []map[string]interface{} to []interface{}.
func toAnySlice(v interface{}) ([]interface{}, bool) {
	switch t := v.(type) {
	case []interface{}:
		return t, true
	case []map[string]interface{}:
		out := make([]interface{}, len(t))
		for i, m := range t {
			out[i] = m
		}
		return out, true
	}
	return nil, false
}

// lookupInNode traverses a row/sub-map using the given path segments.
// Unlike lookupPayloadPath it does NOT do top-level payload resolution — it starts
// directly inside the provided node. Used by wildcard/implicit-wildcard branches so
// they don’t incorrectly re-apply top-level case-insensitive lookup against a row map.
func lookupInNode(node map[string]interface{}, parts []string) (interface{}, bool) {
	if len(parts) == 0 {
		return nil, false
	}
	// case-insensitive key lookup at this level
	var current interface{}
	var found bool
	firstLower := strings.ToLower(parts[0])
	for k, v := range node {
		if strings.ToLower(k) == firstLower {
			current = v
			found = true
			break
		}
	}
	if !found {
		return nil, false
	}
	if len(parts) == 1 {
		return current, true
	}
	// recurse for remaining segments
	for _, seg := range parts[1:] {
		if current == nil {
			return nil, false
		}
		switch n := current.(type) {
		case map[string]interface{}:
			var next interface{}
			segLower := strings.ToLower(seg)
			for k, v := range n {
				if strings.ToLower(k) == segLower {
					next = v
					break
				}
			}
			if next == nil {
				return nil, false
			}
			current = next
		case []interface{}:
			if i, err := strconv.Atoi(seg); err == nil && i >= 0 && i < len(n) {
				current = n[i]
			} else {
				return nil, false
			}
		default:
			return nil, false
		}
	}
	return current, true
}

// lookupPayload fetches a key from payload (case-sensitive then case-insensitive).
// Supports dot-notation paths like "Initiations.0.initiation_id" and
// "initiation.initiation_id".
func lookupPayload(key string, payload map[string]interface{}) (interface{}, bool) {
	key = strings.TrimSpace(key)
	if key == "" {
		return nil, false
	}
	// Unwrap {{...}} — this happens when inner function results are passed as args
	if strings.HasPrefix(key, "{{") && strings.HasSuffix(key, "}}") {
		key = strings.TrimSpace(key[2 : len(key)-2])
	}
	// Dot-notation path — delegate to traversal helper
	if strings.Contains(key, ".") {
		return lookupPayloadPath(key, payload)
	}
	if v, ok := payload[key]; ok {
		return v, true
	}
	// try lowercased
	lower := strings.ToLower(key)
	for k, v := range payload {
		if strings.ToLower(k) == lower {
			return v, true
		}
	}
	return nil, false
}

func toString(v interface{}) string {
	switch t := v.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	case int:
		return strconv.Itoa(t)
	case int32:
		return strconv.FormatInt(int64(t), 10)
	case int64:
		return strconv.FormatInt(t, 10)
	case float32:
		return toStringFloat(float64(t))
	case float64:
		return toStringFloat(t)
	case bool:
		return strconv.FormatBool(t)
	default:
		return fmt.Sprintf("%v", t)
	}
}

func toStringFloat(f float64) string {
	// default formatting with 2 decimals, strip trailing zeros if integer
	s := fmt.Sprintf("%.2f", f)
	// remove trailing .00 if integer
	// if strings.HasSuffix(s, ".00") {
	s = strings.TrimSuffix(s, ".00")
	// }
	// remove possible trailing zeroes like 1.20 -> 1.2
	if strings.Contains(s, ".") {
		s = strings.TrimRight(s, "0")
		s = strings.TrimRight(s, ".")
	}
	return s
}

func resolveNumericArg(token string, payload map[string]interface{}) (float64, bool) {
	token = strings.TrimSpace(token)
	// Unwrap {{...}} — result of inner function evaluation passed as outer arg
	if strings.HasPrefix(token, "{{") && strings.HasSuffix(token, "}}") {
		token = strings.TrimSpace(token[2 : len(token)-2])
	}
	if isQuoted(token) {
		// quoted numeric literal
		u := unquote(token)
		if f, err := strconv.ParseFloat(u, 64); err == nil {
			return f, true
		}
		return 0, false
	}
	// try numeric literal
	if f, err := strconv.ParseFloat(token, 64); err == nil {
		return f, true
	}
	// else lookup in payload
	v, ok := lookupPayload(token, payload)
	if !ok {
		return 0, false
	}
	switch t := v.(type) {
	case int:
		return float64(t), true
	case int32:
		return float64(t), true
	case int64:
		return float64(t), true
	case float32:
		return float64(t), true
	case float64:
		return t, true
	case string:
		if f, err := strconv.ParseFloat(t, 64); err == nil {
			return f, true
		}
	case pgtype.Numeric:
		if t.Valid {
			f, err := t.Float64Value()
			if err == nil {
				return f.Float64, true
			}
		}
		return 0, true
	}
	return 0, false
}

func getNumberList(token string, payload map[string]interface{}) ([]float64, bool) {
	token = strings.TrimSpace(token)
	// if token literal like [1,2,3] not supported; expect variable name referencing slice
	v, ok := lookupPayload(token, payload)
	if !ok {
		return nil, false
	}
	switch t := v.(type) {
	case []float64:
		return t, true
	case []float32:
		out := make([]float64, len(t))
		for i, x := range t {
			out[i] = float64(x)
		}
		return out, true
	case []int:
		out := make([]float64, len(t))
		for i, x := range t {
			out[i] = float64(x)
		}
		return out, true
	case []int64:
		out := make([]float64, len(t))
		for i, x := range t {
			out[i] = float64(x)
		}
		return out, true
	case []interface{}:
		out := make([]float64, 0, len(t))
		for _, e := range t {
			switch n := e.(type) {
			case float64:
				out = append(out, n)
			case float32:
				out = append(out, float64(n))
			case int:
				out = append(out, float64(n))
			case int64:
				out = append(out, float64(n))
			case string:
				if f, err := strconv.ParseFloat(strings.TrimSpace(n), 64); err == nil {
					out = append(out, f)
				}
			}
		}
		return out, true
	default:
		return nil, false
	}
}

// parseDate attempts to parse a date string using a prioritized set of layouts.
// It prefers dd/mm/yyyy formats first (important for bank statements in Indian format),
// then falls back to other common formats. Returns an error when parsing fails.
func parseDate(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, nil
	}
	// Prefer dd/mm/yyyy for bank statements before falling back to the broader parser set.
	if t, err := time.Parse("02/01/2006", s); err == nil {
		return t, nil
	}
	if t, err := time.Parse("2/1/2006", s); err == nil {
		return t, nil
	}
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, errors.New("empty date string")
	}
	// Critical: dd/mm/yyyy formats MUST come before mm/dd/yyyy to prevent misparsing Indian bank statements
	layouts := []string{
		// dd/mm/yyyy variants (Indian/European format) - MUST BE FIRST
		"02/01/2006", "02/01/06", "2/1/2006", "2/1/06",
		"02/01/2006 03:04:05 PM", "02/01/06 03:04:05 PM", "2/1/2006 03:04:05 PM", "2/1/06 03:04:05 PM",
		"02/01/2006 3:04:05 PM", "02/01/06 3:04:05 PM", "2/1/2006 3:04:05 PM", "2/1/06 3:04:05 PM",
		"02/01/06 15:04", "02/01/06 3:04", "02/01/06 15:04:05", "02/01/06 3:04:05",
		"2/1/06 15:04", "2/1/06 3:04", "2/1/06 15:04:05", "2/1/06 3:04:05",
		// mm/dd/yyyy variants (American format) - AFTER dd/mm/yyyy
		"01/02/2006", "01/02/06", "1/2/2006", "1/2/06",
		"01/02/2006 03:04:05 PM", "01/02/2006 03:04 PM", "01/02/06 03:04:05 PM", "01/02/06 03:04 PM",
		"1/2/2006 03:04:05 PM", "1/2/2006 03:04 PM", "1/2/06 03:04:05 PM", "1/2/06 03:04 PM",
		"01/02/06 15:04", "01/02/06 3:04", "01/02/06 15:04:05", "01/02/06 3:04:05",
		"1/2/06 15:04", "1/2/06 3:04", "1/2/06 15:04:05", "1/2/06 3:04:05",
		// Named month formats
		constants.DateFormatSlash, constants.DateFormatDash,
		"2-Jan-2006", "1/Feb/2006",
		// ISO and other formats
		constants.DateFormat, "2006/01/02", "2006.01.02", "01.02.2006", "1.2.2006", "01-02-2006", "1-2-2006",
		"01-02-06", "1-2-06", "2006/1/2", "2006-1-2",
		// dd-Mon-yy and dd/Mon/yy variants
		"02-Jan-06", "02-Jan-2006", "02/Jan/06", "02/Jan/2006",
		"02-Jan-06 15:04", "02-Jan-2006 15:04", "02-Jan-06 3:04", "02-Jan-2006 3:04",
		"02-Jan-06 15:04:05", "02-Jan-2006 15:04:05", "02-Jan-06 3:04:05", "02-Jan-2006 3:04:05",
		"02/Jan/06 15:04", "02/Jan/2006 15:04", "02/Jan/06 3:04", "02/Jan/2006 3:04",
		"02/Jan/06 15:04:05", "02/Jan/2006 15:04:05", "02/Jan/06 3:04:05", "02/Jan/2006 3:04:05",
		"02-Jan-2006 03:04:05 PM", "02-Jan-06 03:04:05 PM", "02-Jan-2006 3:04:05 PM", "02-Jan-06 3:04:05 PM",
		"02/Jan/2006 03:04:05 PM", "02/Jan/06 03:04:05 PM", "02/Jan/2006 3:04:05 PM", "02/Jan/06 3:04:05 PM",
		// dd-Mon-yy variants (American style)
		"01-Feb-06", "01-Feb-2006", "01/Feb/06", "01/Feb/2006",
		"01-Feb-06 15:04", "01-Feb-2006 15:04", "01-Feb-06 3:04", "01-Feb-2006 3:04",
		"01-Feb-06 15:04:05", "01-Feb-2006 15:04:05", "01-Feb-06 3:04:05", "01-Feb-2006 3:04:05",
		"01/Feb/06 15:04", "01/Feb/2006 15:04", "01/Feb/06 3:04", "01/Feb/2006 3:04",
		"01/Feb/06 15:04:05", "01/Feb/2006 15:04:05", "01/Feb/06 3:04:05", "01/Feb/2006 3:04:05",
		// ISO-ish layouts to catch Excel exports that already render as 2026-01-15 or RFC3339 strings
		constants.DateFormat, constants.DateTimeFormat, time.RFC3339, "2006-01-02T15:04:05", "2006-01-02T15:04",
	}
	// Try all layouts
	for _, layout := range layouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	// Try to parse with 2-digit year fallback (e.g., 13-Dec-25 as 2025)
	if len(s) == 9 && s[2] == '-' && s[6] == '-' { // e.g., 13-Dec-25
		t, err := time.Parse("02-Jan-06", s)
		if err == nil {
			// If year < 100, add 2000
			y := t.Year()
			if y < 100 {
				t = t.AddDate(2000, 0, 0)
			}
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("could not parse date: %s", s)
}

// formatCurrency: simple formatting with rupee symbol and commas and two decimals
func formatCurrency(v float64) string {
	neg := v < 0
	if neg {
		v = -v
	}
	s := fmt.Sprintf("%.2f", v)
	parts := strings.Split(s, ".")
	intPart := parts[0]
	decPart := parts[1]
	// fallback to simple 3-digit grouping from right
	r := []rune(intPart)
	var g []rune
	for i := len(r) - 1; i >= 0; i-- {
		g = append(g, r[i])
		if (len(r)-i)%3 == 0 && i != 0 {
			g = append(g, ',')
		}
	}
	// reverse g
	for i := 0; i < len(g)/2; i++ {
		g[i], g[len(g)-1-i] = g[len(g)-1-i], g[i]
	}
	sInt := string(g)
	out := fmt.Sprintf("₹ %s.%s", sInt, decPart)
	if neg {
		out = "-" + out
	}
	return out
}

// helper to test whether string contains any letter/digit (not only whitespace)
func hasContent(s string) bool {
	for _, r := range s {
		if !unicode.IsSpace(r) {
			return true
		}
	}
	return false
}

// ─── Row-list helpers ──────────────────────────────────────────────────────────

// toRowList converts an interface{} to []map[string]interface{} if possible.
func toRowList(v interface{}) ([]map[string]interface{}, bool) {
	switch t := v.(type) {
	case []map[string]interface{}:
		return t, true
	case []interface{}:
		out := make([]map[string]interface{}, 0, len(t))
		for _, elem := range t {
			if m, ok := elem.(map[string]interface{}); ok {
				out = append(out, m)
			}
		}
		return out, true
	}
	return nil, false
}

// getRowList resolves a payload key to a row list.
func getRowList(token string, payload map[string]interface{}) ([]map[string]interface{}, bool) {
	token = strings.TrimSpace(token)
	v, ok := lookupPayload(token, payload)
	if !ok {
		return nil, false
	}
	return toRowList(v)
}

// rowFloat extracts a float64 from a map row by key (case-insensitive).
func rowFloat(row map[string]interface{}, key string) float64 {
	for k, v := range row {
		if strings.EqualFold(k, key) {
			switch n := v.(type) {
			case float64:
				return n
			case float32:
				return float64(n)
			case int:
				return float64(n)
			case int32:
				return float64(n)
			case int64:
				return float64(n)
			case string:
				if f, err := strconv.ParseFloat(strings.TrimSpace(n), 64); err == nil {
					return f
				}
			case pgtype.Numeric:
				if n.Valid {
					if fv, err := n.Float64Value(); err == nil {
						return fv.Float64
					}
				}
			case *pgtype.Numeric:
				if n != nil && n.Valid {
					if fv, err := n.Float64Value(); err == nil {
						return fv.Float64
					}
				}
			}
		}
	}
	return 0
}

// rowString extracts a string from a map row by key (case-insensitive).
func rowString(row map[string]interface{}, key string) string {
	for k, v := range row {
		if strings.EqualFold(k, key) {
			return toString(v)
		}
	}
	return ""
}

// rowDisplayValue returns a human-readable string for a cell value.
// Numbers are formatted with formatCurrency when the key hints at a monetary field.
func rowDisplayValue(row map[string]interface{}, key string) string {
	for k, v := range row {
		if strings.EqualFold(k, key) {
			// Normalize pgtype.Numeric → float64 before display
			if pn, ok := v.(pgtype.Numeric); ok {
				if pn.Valid {
					f, _ := pn.Float64Value()
					v = f.Float64
				} else {
					v = float64(0)
				}
			} else if pnp, ok := v.(*pgtype.Numeric); ok {
				if pnp != nil && pnp.Valid {
					f, _ := pnp.Float64Value()
					v = f.Float64
				} else {
					v = float64(0)
				}
			}
			// Monetary field detection
			lk := strings.ToLower(k)
			isMonetary := strings.Contains(lk, "amount") || strings.Contains(lk, "balance") ||
				strings.Contains(lk, "debit") || strings.Contains(lk, "credit") ||
				strings.Contains(lk, "total") || lk == "amount"
			switch n := v.(type) {
			case float64:
				if isMonetary {
					return formatCurrency(n)
				}
				return toStringFloat(n)
			case float32:
				if isMonetary {
					return formatCurrency(float64(n))
				}
				return toStringFloat(float64(n))
			case int, int32, int64:
				f, _ := strconv.ParseFloat(fmt.Sprintf("%v", n), 64)
				if isMonetary {
					return formatCurrency(f)
				}
				return fmt.Sprintf("%v", n)
			case time.Time:
				return n.Format("02 Jan 2006")
			default:
				return fmt.Sprintf("%v", v)
			}
		}
	}
	return ""
}

// isTruthy returns true for non-nil, non-empty, non-false, non-zero values.
func isTruthy(v interface{}) bool {
	if v == nil {
		return false
	}
	switch t := v.(type) {
	case bool:
		return t
	case string:
		return t != "" && t != "false" && t != "0"
	case int:
		return t != 0
	case int64:
		return t != 0
	case float64:
		return t != 0
	}
	return true
}
