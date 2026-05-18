package catalog

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/constants"
	notificationFunctions "CimplrCorpSaas/api/notification/functions"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// simple error responder
func respondWithErrorTemplate(w http.ResponseWriter, status int, errMsg string) {
	w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": errMsg})
}

func getRequesterEmailTemplate() string {
	for _, s := range auth.GetActiveSessions() {
		if s.Email != "" {
			return s.Email
		}
	}
	return ""
}

// ─────────────────────────────────────────────────────────────────────────────
// Template validation — run before any DB write
// ─────────────────────────────────────────────────────────────────────────────

// knownTemplateFunctions is the canonical set of functions the template engine
// supports. ANY other UPPERCASE_NAME() in a template is flagged as unknown.
var knownTemplateFunctions = map[string]struct{}{
	"SUM": {}, "AVERAGE": {}, "AVG": {}, "SUMPRODUCT": {},
	"FORMAT_NUMBER": {}, "FORMAT_DATE": {}, "FORMAT_DATE_TZ": {}, "FORMAT_CURRENCY": {},
	"ESCAPE_HTML": {}, "CONCAT": {}, "UPPER": {}, "LOWER": {}, "SUBSTRING": {},
	"ADD": {}, "SUBTRACT": {}, "MULTIPLY": {}, "DIVIDE": {},
	"COUNT_OF": {}, "SUM_OF_FIELD": {}, "TOTAL_OF": {}, "AVG_OF_FIELD": {},
	"MAX_OF_FIELD": {}, "MIN_OF_FIELD": {},
	"FILTER": {}, "ORDER_BY": {}, "GROUP_BY": {},
	"TABLE_HTML": {}, "KPI_CARDS_HTML": {}, "ROWS_HTML": {}, "SUMMARY_TABLE_HTML": {},
	"IF": {}, "BADGE_HTML": {},
	// Math helpers
	"MIN": {}, "MAX": {}, "ROUND": {}, "CEIL": {}, "FLOOR": {}, "ABS": {},
	// String helpers
	"TRIM": {}, "REPLACE": {}, "SPLIT": {}, "JOIN": {},
	"LENGTH": {}, "CONTAINS": {}, "STARTS_WITH": {}, "ENDS_WITH": {}, "INDEX_OF": {},
	// List helpers
	"FIRST_OF": {}, "LAST_OF": {}, "ANY_OF": {}, "COUNT_DISTINCT": {},
}

// functionCallRe matches UPPER_SNAKE_NAME( anywhere in text.
var functionCallRe = regexp.MustCompile(`\b([A-Z][A-Z0-9_]*)\s*\(`)

// maliciousPatterns are patterns that should never appear in a template body.
// They catch: script injection, server-side template injection, SQL fragments, etc.
//
// IMPORTANT: the event-handler pattern must only match ACTUAL HTML event attributes
// (onclick=, onload=, onerror=, etc.) — NOT words that happen to contain "on" like
// "content=", "font=", "action=" etc.  We anchor with a word boundary \b and require
// the match to start after whitespace or a tag-open < so "content=" is never flagged.
var maliciousPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)<script`),
	regexp.MustCompile(`(?i)<iframe`),
	regexp.MustCompile(`(?i)<object`),
	regexp.MustCompile(`(?i)<embed`),
	regexp.MustCompile(`(?i)javascript\s*:`),
	// Event handler attributes: must be preceded by whitespace or be at start of a tag
	// attribute list. This prevents "content=", "font=", "action=" false positives.
	// Matches: onclick=  onload=  onerror=  onmouseover=  onsubmit=  etc.
	// Does NOT match: content=  font-size  action=  section=
	regexp.MustCompile(`(?i)(?:^|[\s<])on[a-z]+\s*=`),
	regexp.MustCompile(`(?i)vbscript\s*:`),
	regexp.MustCompile(`(?i)data:\s*text/html`),
	regexp.MustCompile(`(?i)expression\s*\(`),        // CSS expression()
	regexp.MustCompile(`(?i)\bexec\s*\(`),            // SQL EXEC(
	regexp.MustCompile(`(?i)\bdrop\s+table\b`),       // SQL DROP TABLE
	regexp.MustCompile(`(?i)\bdelete\s+from\b`),      // SQL DELETE FROM
	regexp.MustCompile(`(?i)\binsert\s+into\b`),      // SQL INSERT INTO
	regexp.MustCompile(`(?i)\bupdate\s+\w+\s+set\b`), // SQL UPDATE SET
	regexp.MustCompile(`(?i)\bunion\s+select\b`),     // SQL UNION SELECT
	regexp.MustCompile(`(?i)\bxp_cmdshell\b`),
	regexp.MustCompile(`\x00`), // null bytes
}

// templateValidation holds a set of errors found during pre-flight validation.
type templateValidation struct {
	field  string
	errors []string
}

// validateTemplateContent performs a full pre-flight check on a single template field
// (subject, body_text, or body_html).  It:
//  1. Rejects malicious injection patterns.
//  2. Detects unknown FUNCTION() calls that the engine can't handle.
//  3. Verifies unbalanced {{  }} braces.
//  4. Runs a dry-run EvaluateTemplate with a synthetic payload to catch
//     hard arg-count errors (TABLE_HTML with wrong cols, FORMAT_DATE with 1 arg, etc.)
//     before they hit production recipients.
func validateTemplateContent(field, content string) []string {
	if content == "" {
		return nil
	}
	var errs []string

	// 1. UTF-8 safety
	if !utf8.ValidString(content) {
		errs = append(errs, field+": contains invalid UTF-8 bytes")
	}

	// 2. Malicious pattern check
	for _, pat := range maliciousPatterns {
		if pat.MatchString(content) {
			errs = append(errs, field+": contains forbidden pattern: "+pat.String())
		}
	}

	// 3. Unknown function check — scan all UPPER_NAME( calls inside {{ }}
	wrappedRe := regexp.MustCompile(`{{([^}]+)}}`)
	for _, block := range wrappedRe.FindAllString(content, -1) {
		for _, m := range functionCallRe.FindAllStringSubmatch(block, -1) {
			name := m[1]
			if _, ok := knownTemplateFunctions[name]; !ok {
				errs = append(errs, fmt.Sprintf("%s: unknown template function %q — will fail at runtime", field, name))
			}
		}
	}
	// Also check unwrapped function calls (seed-script style — engine auto-wraps them)
	if !strings.Contains(content, "{{") {
		for _, m := range functionCallRe.FindAllStringSubmatch(content, -1) {
			name := m[1]
			if _, ok := knownTemplateFunctions[name]; !ok {
				errs = append(errs, fmt.Sprintf("%s: unknown template function %q — will fail at runtime", field, name))
			}
		}
	}

	// 4. Balanced {{ }} check
	open := strings.Count(content, "{{")
	close := strings.Count(content, "}}")
	if open != close {
		errs = append(errs, fmt.Sprintf("%s: unbalanced template braces — %d {{ but %d }}", field, open, close))
	}

	// 5. Dry-run EvaluateTemplate with a synthetic payload.
	// We populate every variable referenced in {{VarName}} with a safe dummy value
	// so that missing-variable errors don't mask real arg-count errors.
	syntheticPayload := buildSyntheticPayload(content)
	if _, err := notificationFunctions.EvaluateTemplateStrict(content, syntheticPayload); err != nil {
		errs = append(errs, field+": "+humanReadableTemplateError(err.Error()))
	}

	return errs
}

// listFunctions is the set of functions whose FIRST argument must be a
// []map[string]interface{} row-list in the synthetic payload so the dry-run
// doesn't short-circuit to "No data" before checking column args.
var listFunctions = map[string]struct{}{
	"TABLE_HTML": {}, "ROWS_HTML": {}, "SUMMARY_TABLE_HTML": {},
	"SUM_OF_FIELD": {}, "TOTAL_OF": {}, "AVG_OF_FIELD": {},
	"MAX_OF_FIELD": {}, "MIN_OF_FIELD": {},
	"FILTER": {}, "ORDER_BY": {}, "GROUP_BY": {},
	"COUNT_OF": {}, "SUM": {}, "AVERAGE": {}, "AVG": {}, "SUMPRODUCT": {},
	// New list helpers also consume row-lists as first arg
	"FIRST_OF": {}, "LAST_OF": {}, "ANY_OF": {}, "COUNT_DISTINCT": {},
}

// numericFunctions is the set of functions whose FIRST argument must be a number.
var numericFunctions = map[string]struct{}{
	"FORMAT_NUMBER": {}, "FORMAT_CURRENCY": {},
	"ADD": {}, "SUBTRACT": {}, "MULTIPLY": {}, "DIVIDE": {},
}

// dateFunctions is the set of functions whose FIRST argument must be a date string.
var dateFunctions = map[string]struct{}{
	"FORMAT_DATE": {}, "FORMAT_DATE_TZ": {},
}

// syntheticRowList is the dummy row injected for all list-function first args.
var syntheticRowList = []map[string]interface{}{
	{
		"name": "Sample Item", "description": "Test row", "category": "DEBIT",
		"date": constants.DateFormat, "amount": float64(1000),
		"withdrawal_amount": float64(1000), "deposit_amount": float64(500),
		"debit": float64(1000), "credit": float64(500),
		"balance": float64(50000), "total": float64(1500),
		"count": float64(1), "status": "PENDING",
	},
}

// buildSyntheticPayload scans a template for all variable references and function
// first-args and seeds each with a type-appropriate dummy value so the dry-run
// EvaluateTemplateStrict can catch REAL arg-count / arg-type errors rather than
// being fooled by missing-variable short-circuits.
func buildSyntheticPayload(content string) map[string]interface{} {
	payload := map[string]interface{}{}

	// ── 1. Seed plain {{VarName}} references ────────────────────────────────
	varRe := regexp.MustCompile(`{{\s*([A-Za-z_][A-Za-z0-9_]*)\s*}}`)
	for _, m := range varRe.FindAllStringSubmatch(content, -1) {
		key := strings.TrimSpace(m[1])
		if _, exists := payload[key]; exists {
			continue
		}
		lower := strings.ToLower(key)
		switch {
		case strings.Contains(lower, "date"):
			payload[key] = constants.DateFormat
		case strings.Contains(lower, "amount") || strings.Contains(lower, "total") ||
			strings.Contains(lower, "balance") || strings.Contains(lower, "count") ||
			strings.Contains(lower, "number") || strings.Contains(lower, "rate"):
			payload[key] = float64(1000)
		default:
			payload[key] = "DummyValue"
		}
	}

	// ── 2. Seed function first-args with type-correct dummies ───────────────
	// Pattern: {{ FUNCNAME( firstArg , ... ) }} — capture FUNCNAME and firstArg
	funcArgRe := regexp.MustCompile(`{{\s*([A-Z][A-Z0-9_]*)\s*\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*[,)]`)
	for _, m := range funcArgRe.FindAllStringSubmatch(content, -1) {
		funcName := strings.ToUpper(m[1])
		argKey := strings.TrimSpace(m[2])
		if argKey == "" {
			continue
		}
		if _, ok := listFunctions[funcName]; ok {
			// Must be a row-list regardless of key name
			payload[argKey] = syntheticRowList
		} else if _, ok := numericFunctions[funcName]; ok {
			if _, exists := payload[argKey]; !exists {
				payload[argKey] = float64(1000)
			}
		} else if _, ok := dateFunctions[funcName]; ok {
			if _, exists := payload[argKey]; !exists {
				payload[argKey] = constants.DateFormat
			}
		} else {
			if _, exists := payload[argKey]; !exists {
				payload[argKey] = "DummyValue"
			}
		}
	}

	// ── 3. Unwrapped style (no {{ }}) — seed first arg of any known function ─
	if !strings.Contains(content, "{{") {
		bareRe := regexp.MustCompile(`\b([A-Z][A-Z0-9_]*)\s*\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*[,)]`)
		for _, m := range bareRe.FindAllStringSubmatch(content, -1) {
			funcName := strings.ToUpper(m[1])
			argKey := strings.TrimSpace(m[2])
			if argKey == "" {
				continue
			}
			if _, ok := listFunctions[funcName]; ok {
				payload[argKey] = syntheticRowList
			} else if _, ok := numericFunctions[funcName]; ok {
				if _, exists := payload[argKey]; !exists {
					payload[argKey] = float64(1000)
				}
			} else if _, ok := dateFunctions[funcName]; ok {
				if _, exists := payload[argKey]; !exists {
					payload[argKey] = constants.DateFormat
				}
			} else {
				if _, exists := payload[argKey]; !exists {
					payload[argKey] = "DummyValue"
				}
			}
		}
	}

	return payload
}

// humanReadableTemplateError converts a raw engine error string into a plain-English
// message that clearly tells a template author exactly what they did wrong and how to fix it.
func humanReadableTemplateError(raw string) string {
	r := strings.ToLower(raw)

	// ── arg-count errors ─────────────────────────────────────────────────────
	if strings.Contains(r, "table_html expects listvarlist") || strings.Contains(raw, "TABLE_HTML expects listVar + at least 1 column") {
		return `TABLE_HTML is missing column names. Correct usage: {{TABLE_HTML(YourList, 'column1', 'column2')}} — you must pass the list variable AND at least one column name in quotes.`
	}
	if strings.Contains(raw, "ROWS_HTML expects listVar") {
		return `ROWS_HTML is missing column names. Correct usage: {{ROWS_HTML(YourList, 'column1', 'column2')}}`
	}
	if strings.Contains(raw, "SUMMARY_TABLE_HTML expects 1 argument") {
		return `SUMMARY_TABLE_HTML needs exactly 1 argument (the grouped list variable). Example: {{SUMMARY_TABLE_HTML(GroupedItems)}}`
	}
	if strings.Contains(raw, "FORMAT_DATE expects 2 args") {
		return `FORMAT_DATE needs 2 arguments: the date variable and a format string. Example: {{FORMAT_DATE(TransactionDate, '02 Jan 2006')}}`
	}
	if strings.Contains(raw, "FORMAT_DATE_TZ expects 3 args") {
		return `FORMAT_DATE_TZ needs 3 arguments: date variable, format string, timezone. Example: {{FORMAT_DATE_TZ(TransactionDate, '02 Jan 2006', 'Asia/Kolkata')}}`
	}
	if strings.Contains(raw, "FORMAT_NUMBER expects 1 numeric argument") {
		return `FORMAT_NUMBER needs exactly 1 numeric argument. Example: {{FORMAT_NUMBER(Amount)}}`
	}
	if strings.Contains(raw, "FORMAT_CURRENCY expects 1 or 2 arguments") {
		return `FORMAT_CURRENCY needs 1 or 2 arguments: the value and optionally a currency code. Example: {{FORMAT_CURRENCY(Amount, 'INR')}}`
	}
	if strings.Contains(raw, "SUM expects 1 argument") {
		return `SUM needs exactly 1 argument — the list variable name. Example: {{SUM(AmountList)}}`
	}
	if strings.Contains(raw, "AVERAGE expects 1 argument") || strings.Contains(raw, "AVG expects 1 argument") {
		return `AVERAGE/AVG needs exactly 1 argument — the list variable name. Example: {{AVERAGE(ScoreList)}}`
	}
	if strings.Contains(raw, "SUMPRODUCT expects 2 list arguments") {
		return `SUMPRODUCT needs exactly 2 list arguments. Example: {{SUMPRODUCT(Quantities, Prices)}}`
	}
	if strings.Contains(raw, "COUNT_OF expects 1 argument") {
		return `COUNT_OF needs exactly 1 argument — the list variable name. Example: {{COUNT_OF(Transactions)}}`
	}
	if strings.Contains(raw, "SUM_OF_FIELD expects 2 args") {
		return `SUM_OF_FIELD needs 2 arguments: the list variable and the field name in quotes. Example: {{SUM_OF_FIELD(Transactions, 'amount')}}`
	}
	if strings.Contains(raw, "TOTAL_OF expects 2 args") {
		return `TOTAL_OF needs 2 arguments: the list variable and the field name in quotes. Example: {{TOTAL_OF(Items, 'amount')}}`
	}
	if strings.Contains(raw, "AVG_OF_FIELD expects 2 args") {
		return `AVG_OF_FIELD needs 2 arguments: the list variable and the field name in quotes. Example: {{AVG_OF_FIELD(Transactions, 'amount')}}`
	}
	if strings.Contains(raw, "MAX_OF_FIELD expects 2 args") {
		return `MAX_OF_FIELD needs 2 arguments: the list variable and the field name in quotes. Example: {{MAX_OF_FIELD(Items, 'amount')}}`
	}
	if strings.Contains(raw, "MIN_OF_FIELD expects 2 args") {
		return `MIN_OF_FIELD needs 2 arguments: the list variable and the field name in quotes. Example: {{MIN_OF_FIELD(Items, 'amount')}}`
	}
	if strings.Contains(raw, "FILTER expects 3 args") {
		return `FILTER needs 3 arguments: list variable, field name, and value. Example: {{FILTER(Transactions, 'category', 'DEBIT')}}`
	}
	if strings.Contains(raw, "ORDER_BY expects 3 args") {
		return `ORDER_BY needs 3 arguments: list variable, field name, and direction. Example: {{ORDER_BY(Transactions, 'amount', 'DESC')}}`
	}
	if strings.Contains(raw, "GROUP_BY expects 2 args") {
		return `GROUP_BY needs 2 arguments: the list variable and the field to group by. Example: {{GROUP_BY(Transactions, 'category')}}`
	}
	if strings.Contains(raw, "IF expects 3 args") {
		return `IF needs exactly 3 arguments: condition variable, true value, false value. Example: {{IF(IsApproved, 'Approved', 'Pending')}}`
	}
	if strings.Contains(raw, "BADGE_HTML expects 1-2 args") {
		return `BADGE_HTML needs 1 or 2 arguments: a value and optionally a color. Example: {{BADGE_HTML(Status, '#1cc88a')}}`
	}
	if strings.Contains(raw, "KPI_CARDS_HTML requires at least 1 argument") {
		return `KPI_CARDS_HTML needs at least 1 argument. Example: {{KPI_CARDS_HTML(Label1|Value1, Label2|Value2)}}`
	}
	if strings.Contains(raw, "KPI_CARDS_HTML: invalid JSON array") {
		return `KPI_CARDS_HTML received invalid JSON. The JSON array format must be valid. Example: {{KPI_CARDS_HTML([{"label":"Count","value":"5"}])}}`
	}
	if strings.Contains(raw, "UPPER expects 1 argument") {
		return `UPPER needs exactly 1 argument. Example: {{UPPER(Status)}}`
	}
	if strings.Contains(raw, "LOWER expects 1 argument") {
		return `LOWER needs exactly 1 argument. Example: {{LOWER(Status)}}`
	}
	if strings.Contains(raw, "ESCAPE_HTML expects 1 argument") {
		return `ESCAPE_HTML needs exactly 1 argument. Example: {{ESCAPE_HTML(UserNote)}}`
	}
	if strings.Contains(raw, "SUBSTRING expects at least 2 args") {
		return `SUBSTRING needs at least 2 arguments: the variable and a start index. Example: {{SUBSTRING(Description, 0, 50)}}`
	}
	if strings.Contains(r, "expects 2 args") && (strings.Contains(r, "add") || strings.Contains(r, "subtract") || strings.Contains(r, "multiply") || strings.Contains(r, "divide")) {
		return fmt.Sprintf(`Math function error: %s. All math functions (ADD, SUBTRACT, MULTIPLY, DIVIDE) need exactly 2 numeric arguments. Example: {{SUBTRACT(TotalAmount, PaidAmount)}}`, raw)
	}
	if strings.Contains(raw, "division by zero") {
		return `DIVIDE: cannot divide by zero. Make sure the second argument (divisor) is never 0.`
	}

	// ── new math helpers ─────────────────────────────────────────────────────
	if strings.Contains(raw, "ROUND expects") {
		return "ROUND(number) or ROUND(number, decimalPlaces) — e.g. {{ROUND(Amount, 2)}}"
	}
	if strings.Contains(raw, "CEIL expects") {
		return "CEIL(number) — rounds up to nearest integer — e.g. {{CEIL(Amount)}}"
	}
	if strings.Contains(raw, "FLOOR expects") {
		return "FLOOR(number) — rounds down to nearest integer — e.g. {{FLOOR(Amount)}}"
	}
	if strings.Contains(raw, "ABS expects") {
		return "ABS(number) — absolute value — e.g. {{ABS(Variance)}}"
	}

	// ── new string helpers ───────────────────────────────────────────────────
	if strings.Contains(raw, "TRIM expects") {
		return "TRIM(varOrLiteral) — strips leading/trailing whitespace — e.g. {{TRIM(Name)}}"
	}
	if strings.Contains(raw, "REPLACE expects") {
		return "REPLACE(subject, 'old', 'new') — e.g. {{REPLACE(Status, 'PENDING', 'Pending')}}"
	}
	if strings.Contains(raw, "SPLIT expects") {
		return "SPLIT(subject, 'sep') or SPLIT(subject, 'sep', index) — e.g. {{SPLIT(FullName, ' ', 0)}}"
	}
	if strings.Contains(raw, "JOIN expects") {
		return "JOIN(listVar, 'separator') — e.g. {{JOIN(Tags, ', ')}}"
	}
	if strings.Contains(raw, "LENGTH expects") {
		return "LENGTH(varOrLiteral) — character count or list length — e.g. {{LENGTH(Items)}}"
	}
	if strings.Contains(raw, "CONTAINS expects") {
		return "CONTAINS(subject, 'substring') — returns true/false — e.g. {{CONTAINS(Status, 'APPROVED')}}"
	}
	if strings.Contains(raw, "STARTS_WITH expects") {
		return "STARTS_WITH(subject, 'prefix') — returns true/false — e.g. {{STARTS_WITH(RefID, 'INI-')}}"
	}
	if strings.Contains(raw, "ENDS_WITH expects") {
		return "ENDS_WITH(subject, 'suffix') — returns true/false — e.g. {{ENDS_WITH(FileName, '.csv')}}"
	}
	if strings.Contains(raw, "INDEX_OF expects") {
		return "INDEX_OF(subject, 'substring') — returns position (-1 if not found) — e.g. {{INDEX_OF(Email, '@')}}"
	}

	// ── new list helpers ─────────────────────────────────────────────────────
	if strings.Contains(raw, "FIRST_OF expects") {
		return "FIRST_OF(listVar) or FIRST_OF(listVar, field) — e.g. {{FIRST_OF(Transactions, amount)}}"
	}
	if strings.Contains(raw, "LAST_OF expects") {
		return "LAST_OF(listVar) or LAST_OF(listVar, field) — e.g. {{LAST_OF(Transactions, date)}}"
	}
	if strings.Contains(raw, "ANY_OF expects") {
		return "ANY_OF(listVar) or ANY_OF(listVar, field) — comma-joined values — e.g. {{ANY_OF(Items, status)}}"
	}
	if strings.Contains(raw, "COUNT_DISTINCT expects") {
		return "COUNT_DISTINCT(listVar) or COUNT_DISTINCT(listVar, field) — unique count — e.g. {{COUNT_DISTINCT(Items, category)}}"
	}

	// ── unknown function ─────────────────────────────────────────────────────
	if strings.Contains(r, "unsupported function") {
		// extract function name from "unsupported function: FUNC_NAME"
		parts := strings.SplitN(raw, ":", 2)
		funcName := strings.TrimSpace(parts[len(parts)-1])
		return fmt.Sprintf(`Unknown function "%s". Check for typos — supported functions are: FORMAT_NUMBER, FORMAT_DATE, FORMAT_CURRENCY, CONCAT, TABLE_HTML, IF, BADGE_HTML, COUNT_OF, SUM_OF_FIELD, MIN, MAX, TRIM, REPLACE, SPLIT, JOIN, LENGTH, CONTAINS, STARTS_WITH, ENDS_WITH, INDEX_OF, FIRST_OF, LAST_OF, ANY_OF, COUNT_DISTINCT, and others. Function names must be ALL_CAPS.`, funcName)
	}

	// ── loop guard ───────────────────────────────────────────────────────────
	if strings.Contains(r, "too many nested function calls") {
		return `Template has too many nested function calls. Simplify by splitting complex expressions across multiple lines or variables.`
	}

	// ── fallback: return the raw message but strip internal Go noise ─────────
	return "Template error: " + raw
}

// validateTemplateRequest validates subject, body_text, and body_html for a
// single template being created or edited. Returns a structured list of
// human-readable validation errors, or nil if everything is valid.
func validateTemplateRequest(subject, bodyText, bodyHTML string, isHTML bool) []map[string]string {
	var findings []map[string]string

	add := func(field, message string) {
		findings = append(findings, map[string]string{"field": field, "error": message})
	}

	// ── subject ──────────────────────────────────────────────────────────────
	if subject == "" {
		add("subject", "Subject must not be empty.")
	} else {
		for _, pat := range maliciousPatterns {
			if pat.MatchString(subject) {
				add("subject", "Subject contains a forbidden/dangerous pattern and cannot be saved.")
			}
		}
		if strings.Count(subject, "{{") != strings.Count(subject, "}}") {
			add("subject", fmt.Sprintf("Subject has unbalanced template braces: %d opening {{ but %d closing }}. Every {{ must have a matching }}.", strings.Count(subject, "{{"), strings.Count(subject, "}}")))
		}
		synth := buildSyntheticPayload(subject)
		if _, err := notificationFunctions.EvaluateTemplateStrict(subject, synth); err != nil {
			add("subject", humanReadableTemplateError(err.Error()))
		}
	}

	// ── body_text ─────────────────────────────────────────────────────────────
	for _, e := range validateTemplateContent("body_text", bodyText) {
		// e is already "field: message" — split for structured output
		add("body_text", strings.TrimPrefix(e, "body_text: "))
	}

	// ── body_html ─────────────────────────────────────────────────────────────
	if isHTML || strings.TrimSpace(bodyHTML) != "" {
		for _, e := range validateTemplateContent("body_html", bodyHTML) {
			add("body_html", strings.TrimPrefix(e, "body_html: "))
		}
	}

	return findings
}

// respondValidationErrors writes a 422 JSON response listing all validation problems
// in a clean, human-readable format.
func respondValidationErrors(w http.ResponseWriter, findings []map[string]string) {
	type fieldError struct {
		Field   string `json:"field"`
		Message string `json:"message"`
	}
	var errs []fieldError
	for _, f := range findings {
		errs = append(errs, fieldError{Field: f["field"], Message: f["error"]})
	}
	w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
	w.WriteHeader(http.StatusUnprocessableEntity)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": false,
		"error":   fmt.Sprintf("Template validation failed with %d error(s). Fix all issues and try again.", len(errs)),
		"errors":  errs,
	})
}

// CreateTemplateSingle inserts a template master row and an audit_template CREATE row
func CreateTemplateSingle(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID       string `json:"user_id"`
			EventID      string `json:"event_id"`
			Channel      string `json:"channel"`
			RoleScope    string `json:"role_scope"`
			TemplateName string `json:"template_name"`
			Description  string `json:"description"`
			Subject      string `json:"subject"`
			BodyText     string `json:"body_text"`
			BodyHTML     string `json:"body_html"`
			IsHTML       *bool  `json:"is_html_enabled"`
			Formula      any    `json:"formula_steps"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithPayload(w, false, constants.ErrInvalidRequestBody, nil)
			return
		}
		if req.EventID == "" || req.Channel == "" || req.TemplateName == "" {
			api.RespondWithPayload(w, false, constants.ErrEventIDChannelTemplateNameRequired, nil)
			return
		}
		if req.IsHTML == nil {
			defaultHTML := false
			req.IsHTML = &defaultHTML
		}
		userEmail := getRequesterEmailTemplate()
		if userEmail == "" {
			api.RespondWithPayload(w, false, constants.ErrInvalidSessionCapitalized, nil)
			return
		}

		// ── Pre-flight template validation (before any DB write) ─────────────────
		isHTMLBool := req.IsHTML != nil && *req.IsHTML
		if findings := validateTemplateRequest(req.Subject, req.BodyText, req.BodyHTML, isHTMLBool); len(findings) > 0 {
			respondValidationErrors(w, findings)
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}
		defer tx.Rollback(ctx)

		insertQ := `INSERT INTO notification_svc.template (event_id, channel, role_scope, template_name, description, created_by) VALUES ($1,$2,$3,$4,$5,$6) RETURNING template_id`
		var tplID string
		if err := tx.QueryRow(ctx, insertQ, req.EventID, strings.ToUpper(req.Channel), req.RoleScope, req.TemplateName, req.Description, userEmail).Scan(&tplID); err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}

		// insert audit_template record with CREATE (compute version_label = 'v' || (COUNT(*)+1) for this template)
		// ensure formula_steps and is_html_enabled are non-null
		isHTMLVal := false
		if req.IsHTML != nil {
			isHTMLVal = *req.IsHTML
		}
		var formulaVal interface{} = []byte("[]")
		if req.Formula != nil {
			if jf, jerr := json.Marshal(req.Formula); jerr == nil {
				formulaVal = jf
			}
		}
		auditQ := `INSERT INTO notification_svc.audit_template (template_id, action_type, processing_status, subject, body_text, body_html, is_html_enabled, formula_steps, version_label, requested_by, requested_at) VALUES ($1,'CREATE','PENDING_APPROVAL',$2,$3,$4,$5,$6,(SELECT 'v' || (COUNT(*)+1) FROM notification_svc.audit_template WHERE template_id = $8),$7,now())`
		if _, err := tx.Exec(ctx, auditQ, tplID, req.Subject, req.BodyText, req.BodyHTML, isHTMLVal, formulaVal, userEmail, tplID); err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{"template_id": tplID, "requested": userEmail})
	}
}

// CreateTemplate handles bulk create of templates
func CreateTemplate(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			Rows   []struct {
				EventID      string `json:"event_id"`
				Channel      string `json:"channel"`
				RoleScope    string `json:"role_scope"`
				TemplateName string `json:"template_name"`
				Description  string `json:"description"`
				Subject      string `json:"subject"`
				BodyText     string `json:"body_text"`
				BodyHTML     string `json:"body_html"`
				IsHTML       *bool  `json:"is_html_enabled"`
				Formula      any    `json:"formula_steps"`
			} `json:"rows"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithPayload(w, false, constants.ErrInvalidRequestBody, nil)
			return
		}
		if len(req.Rows) == 0 {
			api.RespondWithPayload(w, false, "rows required", nil)
			return
		}
		userEmail := getRequesterEmailTemplate()
		if userEmail == "" {
			api.RespondWithPayload(w, false, constants.ErrInvalidSessionCapitalized, nil)
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}
		defer tx.Rollback(ctx)

		// Batch insert templates (include created_by)
		valueStrings := make([]string, 0, len(req.Rows))
		valueArgs := make([]interface{}, 0, len(req.Rows)*6)
		for i, rrow := range req.Rows {
			pos := i*6 + 1
			valueStrings = append(valueStrings, fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d)", pos, pos+1, pos+2, pos+3, pos+4, pos+5))
			valueArgs = append(valueArgs, rrow.EventID, strings.ToUpper(rrow.Channel), rrow.RoleScope, rrow.TemplateName, rrow.Description, userEmail)
		}
		batchQ := fmt.Sprintf(`INSERT INTO notification_svc.template (event_id, channel, role_scope, template_name, description, created_by) VALUES %s RETURNING template_id, template_name`, strings.Join(valueStrings, ","))
		rows, err := tx.Query(ctx, batchQ, valueArgs...)
		if err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}
		defer rows.Close()
		var inserted []map[string]interface{}
		var templateIDs []string
		for rows.Next() {
			var id, name string
			if err := rows.Scan(&id, &name); err == nil {
				inserted = append(inserted, map[string]interface{}{"template_id": id, "template_name": name})
				templateIDs = append(templateIDs, id)
			}
		}

		// insert audit rows for created templates (compute version_label per template)
		if len(templateIDs) > 0 {
			for _, id := range templateIDs {
				aq := `INSERT INTO notification_svc.audit_template (template_id, action_type, processing_status, requested_by, version_label, requested_at) VALUES ($1,'CREATE','PENDING_APPROVAL',$2,(SELECT 'v' || (COUNT(*)+1) FROM notification_svc.audit_template WHERE template_id = $3),now())`
				if _, err := tx.Exec(ctx, aq, id, userEmail, id); err != nil {
					api.RespondWithPayload(w, false, err.Error(), nil)
					return
				}
			}
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{"inserted": inserted})
	}
}

// recipientSubquery resolves recipients for template alias t.
// NULLIF guards against empty-string → uuid cast (SQLSTATE 22P02).
const recipientSubquery = `
	COALESCE((
		SELECT json_agg(json_build_object(
			'recipient_type',    tr.recipient_type,
			'recipient_role',    COALESCE(tr.recipient_role,''),
			'recipient_user_id', COALESCE(tr.recipient_user_id,''),
			'emails', CASE
				WHEN tr.recipient_type = 'USER' AND NULLIF(TRIM(COALESCE(tr.recipient_user_id,'')), '') IS NOT NULL
					THEN COALESCE(
						(SELECT json_agg(u.email) FROM users u
						 WHERE u.id::text = NULLIF(TRIM(tr.recipient_user_id),'')),
						'[]'::json)
				WHEN tr.recipient_type = 'ROLE' AND NULLIF(TRIM(COALESCE(tr.recipient_role,'')), '') IS NOT NULL
					THEN COALESCE(
						(SELECT json_agg(u2.email)
						 FROM users u2
						 JOIN user_roles ur ON ur.user_id = u2.id
						 JOIN roles ro ON ro.id = ur.role_id
						 WHERE ro.name = tr.recipient_role OR ro.rolecode = tr.recipient_role),
						'[]'::json)
				ELSE '[]'::json END
		)) FROM notification_svc.template_recipient tr
		WHERE tr.template_id = t.template_id AND tr.is_active = true
	), '[]'::json)
`

// GetTemplatesWithAudit — /notification/template/all
// One row per template. Latest audit = DISTINCT ON requested_at DESC.
// Flat history columns: created_by/at, edited_by/at, deleted_by/at (like interestTypeMaster).
func GetTemplatesWithAudit(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		buNames, isAdmin := callerContext(ctx)

		// Entity filter: restrict to templates whose parent event is in the caller's accessible pool.
		// buNames is already resolved by PreValidation middleware — no extra DB call.
		entityFilterSQL := ""
		var entityArgs []interface{}
		if !isAdmin && len(buNames) > 0 {
			entityFilterSQL = ` AND (COALESCE(e.entity_name,'') = '' OR COALESCE(e.entity_name,'') = ANY($1::text[]))`
			entityArgs = []interface{}{buNames}
		}

		q := `
			WITH latest_audit AS (
				SELECT DISTINCT ON (a.template_id)
					a.template_id,
					a.audit_id,
					a.action_type,
					a.processing_status,
					a.version_label,
					a.subject,
					a.body_text,
					a.body_html,
					a.is_html_enabled,
					a.requested_by,
					a.requested_at,
					a.checker_by,
					a.checker_at,
					a.checker_comment,
					a.is_deleted
				FROM notification_svc.audit_template a
				ORDER BY a.template_id,
				         GREATEST(COALESCE(a.checker_at, '1970-01-01'::timestamptz),
				                  COALESCE(a.requested_at, '1970-01-01'::timestamptz)) DESC NULLS LAST
			),
			-- history CTE gives flat created/edited/deleted by+at
			history AS (
				SELECT
					template_id,
					MAX(CASE WHEN action_type='CREATE' THEN requested_by  END) AS created_by,
					MAX(CASE WHEN action_type='CREATE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS created_at,
					MAX(CASE WHEN action_type='EDIT'   THEN requested_by  END) AS edited_by,
					MAX(CASE WHEN action_type='EDIT'   THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS edited_at,
					MAX(CASE WHEN action_type='DELETE' THEN requested_by  END) AS deleted_by,
					MAX(CASE WHEN action_type='DELETE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS deleted_at
				FROM notification_svc.audit_template
				GROUP BY template_id
			)
			SELECT
				t.template_id,
				COALESCE(t.event_id,'')      AS event_id,
				COALESCE(t.channel,'')       AS channel,
				COALESCE(t.role_scope,'')    AS role_scope,
				COALESCE(t.template_name,'') AS template_name,
				COALESCE(t.description,'')   AS description,
				t.is_active,

				COALESCE(l.audit_id::text,'')                                 AS audit_id,
				COALESCE(l.action_type,'')                                    AS action_type,
				COALESCE(l.processing_status,'')                              AS processing_status,
				COALESCE(l.version_label,'')                                  AS version_label,
				COALESCE(l.subject,'')                                        AS subject,
				COALESCE(l.body_text,'')                                      AS body_text,
				COALESCE(l.body_html,'')                                      AS body_html,
				COALESCE(l.is_html_enabled,false)                             AS is_html_enabled,
				COALESCE(TO_CHAR(l.requested_at,'YYYY-MM-DD HH24:MI:SS'),'') AS requested_at,
				COALESCE(l.requested_by,'')                                   AS requested_by,
				COALESCE(TO_CHAR(l.checker_at,'YYYY-MM-DD HH24:MI:SS'),'')   AS checker_at,
				COALESCE(l.checker_by,'')                                     AS checker_by,
				COALESCE(l.checker_comment,'')                                AS checker_comment,
				COALESCE(l.is_deleted, false)                                 AS version_is_deleted,

				COALESCE(h.created_by,'')        AS created_by,
				COALESCE(h.created_at,'')        AS created_at,
				COALESCE(h.edited_by,'')         AS edited_by,
				COALESCE(h.edited_at,'')         AS edited_at,
				COALESCE(h.deleted_by,'')        AS deleted_by,
				COALESCE(h.deleted_at,'')        AS deleted_at,

				-- old_* come from the audit row's own snapshot (stored at edit time)
				COALESCE(l.old_subject,'')           AS old_subject,
				COALESCE(l.old_body_text,'')         AS old_body_text,
				COALESCE(l.old_body_html,'')         AS old_body_html,
				false                                AS old_is_html_enabled,
				''                                   AS old_version_label,
				COALESCE(l.old_recipients, '{}')     AS old_recipients,

				` + recipientSubquery + ` AS recipients

			FROM notification_svc.template t
			LEFT JOIN notification_svc.event e ON e.event_id = t.event_id
			LEFT JOIN latest_audit l ON l.template_id = t.template_id
			LEFT JOIN history h ON h.template_id = t.template_id
		` + entityFilterSQL + `
			ORDER BY GREATEST(COALESCE(l.requested_at,'1970-01-01'::timestamptz), COALESCE(l.checker_at,'1970-01-01'::timestamptz)) DESC
		`
		var rows pgx.Rows
		var err error
		if len(entityArgs) > 0 {
			rows, err = pgxPool.Query(ctx, q, entityArgs...)
		} else {
			rows, err = pgxPool.Query(ctx, q)
		}
		if err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}
		defer rows.Close()

		fields := rows.FieldDescriptions()
		out := make([]map[string]interface{}, 0)
		for rows.Next() {
			vals, _ := rows.Values()
			row := make(map[string]interface{}, len(fields))
			for i, f := range fields {
				if vals[i] == nil {
					row[string(f.Name)] = ""
				} else {
					row[string(f.Name)] = vals[i]
				}
			}
			out = append(out, row)
		}
		if rows.Err() != nil {
			api.RespondWithPayload(w, false, "row scan error: "+rows.Err().Error(), nil)
			return
		}
		api.RespondWithPayload(w, true, "", out)
	}
}

// GetTemplateVersions — /notification/template/versions
// Flat: one row per audit_template entry (every version of every template).
// Same template_id appears multiple times — one row per version, newest first.
// history CTE gives created_by/at, edited_by/at, deleted_by/at on every row.
// Optional body filter: { "template_id": "TPL-xxx", "version_label": "v2" }
func GetTemplateVersions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			TemplateID   string `json:"template_id"`
			VersionLabel string `json:"version_label"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		ctx := r.Context()

		// Entity bifurcation — uses PreValidation middleware-provided org pool.
		// Admin callers bypass filtering; non-admin callers see only templates
		// whose parent event belongs to their accessible business-unit pool.
		buNames, isAdmin := callerContext(ctx)
		entityFilterSQL := ""
		var args []interface{}
		if !isAdmin && len(buNames) > 0 {
			// buNames goes in as $1 so subsequent args offset correctly.
			args = append(args, buNames)
			entityFilterSQL = fmt.Sprintf(` AND (COALESCE(e.entity_name,'') = '' OR COALESCE(e.entity_name,'') = ANY($%d::text[]))`, len(args))
		}

		// Build dynamic filter clause with correct positional args
		var filterParts []string
		if req.TemplateID != "" {
			args = append(args, req.TemplateID)
			filterParts = append(filterParts, fmt.Sprintf("AND a.template_id = $%d", len(args)))
		}
		if req.VersionLabel != "" {
			args = append(args, req.VersionLabel)
			filterParts = append(filterParts, fmt.Sprintf("AND a.version_label = $%d", len(args)))
		}
		filterClause := ""
		if len(filterParts) > 0 {
			filterClause = " " + strings.Join(filterParts, " ")
		}

		q := `
			WITH history AS (
				SELECT
					template_id,
					MAX(CASE WHEN action_type='CREATE' THEN requested_by  END) AS created_by,
					MAX(CASE WHEN action_type='CREATE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS created_at,
					MAX(CASE WHEN action_type='EDIT'   THEN requested_by  END) AS edited_by,
					MAX(CASE WHEN action_type='EDIT'   THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS edited_at,
					MAX(CASE WHEN action_type='DELETE' THEN requested_by  END) AS deleted_by,
					MAX(CASE WHEN action_type='DELETE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS deleted_at
				FROM notification_svc.audit_template
				GROUP BY template_id
			)
			SELECT
				t.template_id,
				COALESCE(t.event_id,'')      AS event_id,
				COALESCE(t.channel,'')       AS channel,
				COALESCE(t.role_scope,'')    AS role_scope,
				COALESCE(t.template_name,'') AS template_name,
				COALESCE(t.description,'')   AS description,
				t.is_active,

				COALESCE(a.audit_id::text,'')                                 AS audit_id,
				COALESCE(a.version_label,'')                                  AS version_label,
				COALESCE(a.action_type,'')                                    AS action_type,
				COALESCE(a.processing_status,'')                              AS processing_status,
				COALESCE(a.subject,'')                                        AS subject,
				COALESCE(a.body_text,'')                                      AS body_text,
				COALESCE(a.body_html,'')                                      AS body_html,
				COALESCE(a.is_html_enabled,false)                             AS is_html_enabled,
				COALESCE(TO_CHAR(a.requested_at,'YYYY-MM-DD HH24:MI:SS'),'') AS requested_at,
				COALESCE(a.requested_by,'')                                   AS requested_by,
				COALESCE(TO_CHAR(a.checker_at,'YYYY-MM-DD HH24:MI:SS'),'')   AS checker_at,
				COALESCE(a.checker_by,'')                                     AS checker_by,
				COALESCE(a.checker_comment,'')                                AS checker_comment,

				COALESCE(h.created_by,'')        AS created_by,
				COALESCE(h.created_at,'')        AS created_at,
				COALESCE(h.edited_by,'')         AS edited_by,
				COALESCE(h.edited_at,'')         AS edited_at,
				COALESCE(h.deleted_by,'')        AS deleted_by,
				COALESCE(h.deleted_at,'')        AS deleted_at,

				-- each row's own stored snapshot (correct per-version, not the current live version)
				COALESCE(a.old_subject,'')        AS old_subject,
				COALESCE(a.old_body_text,'')      AS old_body_text,
				COALESCE(a.old_body_html,'')      AS old_body_html,
				false                             AS old_is_html_enabled,
				''                                AS old_version_label,
				COALESCE(a.is_deleted, false)     AS version_is_deleted,
				COALESCE(a.old_recipients, '{}')  AS old_recipients,

				` + recipientSubquery + ` AS recipients,

				-- Event fields joined from notification_svc.event
				COALESCE(e.event_display_name,'') AS event_name,
				COALESCE(e.event_code,'')         AS event_code,
				COALESCE(e.source_route,'')       AS event_source_route,
				COALESCE(e.entity_name,'')        AS event_entity_name,
				COALESCE(e.event_id,'')           AS event_event_id

			FROM notification_svc.audit_template a
			JOIN notification_svc.template t ON t.template_id = a.template_id
			LEFT JOIN history h ON h.template_id = a.template_id
			LEFT JOIN notification_svc.event e ON e.event_id = t.event_id
			WHERE a.is_deleted = false
			AND 1=1
			` + entityFilterSQL + filterClause + `
			ORDER BY GREATEST(COALESCE(a.requested_at, '1970-01-01'::timestamptz), COALESCE(a.checker_at, '1970-01-01'::timestamptz)) DESC NULLS LAST
		`

		var rows pgx.Rows
		var err error
		if len(args) > 0 {
			rows, err = pgxPool.Query(ctx, q, args...)
		} else {
			rows, err = pgxPool.Query(ctx, q)
		}
		if err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}
		defer rows.Close()

		fields := rows.FieldDescriptions()
		out := make([]map[string]interface{}, 0)
		for rows.Next() {
			vals, _ := rows.Values()
			row := make(map[string]interface{}, len(fields))
			for i, f := range fields {
				if vals[i] == nil {
					row[string(f.Name)] = ""
				} else {
					row[string(f.Name)] = vals[i]
				}
			}
			out = append(out, row)
		}
		if rows.Err() != nil {
			api.RespondWithPayload(w, false, "row scan error: "+rows.Err().Error(), nil)
			return
		}
		api.RespondWithPayload(w, true, "", out)
	}
}

// EditTemplateSingle — POST /notification/template/edit
//
// Accepts the same shape as CreateTemplateWithRecipients plus template_id.
// Snapshots the current APPROVED audit row's content into old_subject/body/html/formula
// and the live template_recipient rows into old_recipients (JSONB).
// Inserts a new audit_template row with action_type='EDIT', processing_status='PENDING_EDIT_APPROVAL'.
// Also accepts optional recipient_strategy to record the intended recipient change.
// The template master row and recipient rows are NOT modified until a checker approves.
func EditTemplateSingle(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			TemplateID string                 `json:"template_id"`
			Subject    string                 `json:"subject"`
			BodyText   string                 `json:"body_text"`
			BodyHTML   string                 `json:"body_html"`
			IsHTML     *bool                  `json:"is_html_enabled"`
			Formula    any                    `json:"formula_steps"`
			ChangeNote string                 `json:"change_note"`
			Strategy   map[string]interface{} `json:"recipient_strategy"`
			// template-level fields (optional — update template master on approval)
			TemplateName string `json:"template_name"`
			Description  string `json:"description"`
			RoleScope    string `json:"role_scope"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithPayload(w, false, constants.ErrInvalidRequestBody, nil)
			return
		}
		if req.TemplateID == "" {
			api.RespondWithPayload(w, false, "template_id is required", nil)
			return
		}
		if req.IsHTML == nil {
			def := false
			req.IsHTML = &def
		}
		editor := getRequesterEmailTemplate()
		if editor == "" {
			api.RespondWithPayload(w, false, constants.ErrInvalidSessionCapitalized, nil)
			return
		}

		// ── Pre-flight template validation (before any DB write) ─────────────────
		isHTMLBool := req.IsHTML != nil && *req.IsHTML
		if findings := validateTemplateRequest(req.Subject, req.BodyText, req.BodyHTML, isHTMLBool); len(findings) > 0 {
			respondValidationErrors(w, findings)
			return
		}

		var formulaVal interface{} = []byte("[]")
		if req.Formula != nil {
			if jf, jerr := json.Marshal(req.Formula); jerr == nil {
				formulaVal = jf
			}
		}

		ctx := r.Context()

		// ── Step 1: verify template exists ──────────────────────────────────────
		var exists bool
		if err := pgxPool.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM notification_svc.template WHERE template_id=$1)`,
			req.TemplateID).Scan(&exists); err != nil || !exists {
			api.RespondWithPayload(w, false, "template not found", nil)
			return
		}

		// ── Step 2: snapshot current APPROVED audit row (old_* fields) ──────────
		var oldSubject, oldBodyText, oldBodyHTML sql.NullString
		var oldIsHTML sql.NullBool
		var oldFormula []byte
		_ = pgxPool.QueryRow(ctx,
			`SELECT COALESCE(subject,''), COALESCE(body_text,''), COALESCE(body_html,''),
			        COALESCE(is_html_enabled,false), COALESCE(formula_steps,'[]'::jsonb)
			 FROM notification_svc.audit_template
			 WHERE template_id = $1 AND processing_status = 'APPROVED'
			   AND COALESCE(is_deleted,false) = false
			 ORDER BY requested_at DESC LIMIT 1`,
			req.TemplateID,
		).Scan(&oldSubject, &oldBodyText, &oldBodyHTML, &oldIsHTML, &oldFormula)
		// Not fatal if no approved version yet — old_* will just be empty

		// ── Step 3: snapshot current live recipients as JSONB ────────────────────
		var oldRecipientsJSON []byte
		_ = pgxPool.QueryRow(ctx,
			`SELECT COALESCE(
				(SELECT json_agg(json_build_object(
					'recipient_type',    tr.recipient_type,
					'recipient_role',    COALESCE(tr.recipient_role,''),
					'recipient_user_id', COALESCE(tr.recipient_user_id,''),
					'recipient_priority', tr.recipient_priority,
					'is_active',         tr.is_active
				))
				FROM notification_svc.template_recipient tr
				WHERE tr.template_id = $1 AND tr.is_active = true
			), '[]'::json)::jsonb`,
			req.TemplateID,
		).Scan(&oldRecipientsJSON)
		if len(oldRecipientsJSON) == 0 {
			oldRecipientsJSON = []byte("[]")
		}

		// ── Step 4: marshal recipient_strategy for storage ───────────────────────
		// Always embed the strategy using a sentinel prefix so the approval handler
		// can reliably extract and apply it even when a human change_note is present.
		var strategyJSON []byte
		if req.Strategy != nil {
			if sj, err := json.Marshal(req.Strategy); err == nil {
				strategyJSON = sj
			}
		}
		const strategyPrefix = "__STRATEGY__:"
		changeNote := req.ChangeNote
		if len(strategyJSON) > 0 {
			// Sentinel prefix + strategy JSON, then optional human note separated by newline
			if changeNote == "" {
				changeNote = strategyPrefix + string(strategyJSON)
			} else {
				changeNote = strategyPrefix + string(strategyJSON) + "\n" + changeNote
			}
		}

		// ── Step 5: insert new EDIT audit row with all old_* snapshots ───────────
		auditQ := `
			INSERT INTO notification_svc.audit_template
				(template_id, action_type, processing_status,
				 subject, body_text, body_html, is_html_enabled, formula_steps,
				 version_label, requested_by, requested_at, change_note,
				 old_subject, old_body_text, old_body_html, old_formula_steps,
				 old_recipients)
			VALUES
				($1, 'EDIT', 'PENDING_EDIT_APPROVAL',
				 $2, $3, $4, $5, $6,
				 (SELECT 'v' || (COUNT(*)+1) FROM notification_svc.audit_template WHERE template_id = $14::varchar),
				 $7, now(), $8,
				 $9, $10, $11, $12,
				 $13)
			RETURNING audit_id::text, version_label
		`
		var newAuditID, newVersion string
		if err := pgxPool.QueryRow(ctx, auditQ,
			req.TemplateID,                                                   // $1
			req.Subject, req.BodyText, req.BodyHTML, *req.IsHTML, formulaVal, // $2–$6
			editor, changeNote, // $7, $8
			oldSubject.String, oldBodyText.String, oldBodyHTML.String, oldFormula, // $9–$12
			oldRecipientsJSON, // $13
			req.TemplateID,    // $14 — subquery ref (avoids 42P08)
		).Scan(&newAuditID, &newVersion); err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"template_id":    req.TemplateID,
			"audit_id":       newAuditID,
			"version_label":  newVersion,
			"action_type":    "EDIT",
			"status":         "PENDING_EDIT_APPROVAL",
			"requested_by":   editor,
			"old_recipients": string(oldRecipientsJSON),
		})
	}
}

// GetTemplate returns a single template with its full audit history and resolved recipients
func GetTemplate(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			TemplateID string `json:"template_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.TemplateID == "" {
			api.RespondWithPayload(w, false, constants.ErrTemplateIDRequired, nil)
			return
		}
		ctx := r.Context()
		q := fmt.Sprintf(`
			SELECT t.template_id, t.event_id, t.channel, t.role_scope, t.template_name, t.description, t.is_active,
				COALESCE((SELECT json_agg(json_build_object(
					'audit_id', a.audit_id, 'action_type', a.action_type, 'processing_status', a.processing_status,
					'subject', COALESCE(a.subject,''), 'body_text', COALESCE(a.body_text,''), 'body_html', COALESCE(a.body_html,''),
					'is_html_enabled', COALESCE(a.is_html_enabled,false),
					'formula_steps', a.formula_steps, 'version_label', COALESCE(a.version_label,''),
					'requested_by', COALESCE(a.requested_by,''), 'requested_at', TO_CHAR(a.requested_at,'YYYY-MM-DD HH24:MI:SS'),
					'checker_by', COALESCE(a.checker_by,''), 'checker_at', TO_CHAR(a.checker_at,'YYYY-MM-DD HH24:MI:SS'),
					'checker_comment', COALESCE(a.checker_comment,''),
					'old_recipients', COALESCE(a.old_recipients, '{}')
				) ORDER BY a.requested_at DESC) FROM notification_svc.audit_template a WHERE a.template_id = t.template_id AND COALESCE(a.is_deleted, false) = false), '[]'::json) AS audits,
				%s AS recipients
			FROM notification_svc.template t WHERE t.template_id = $1 LIMIT 1`, recipientSubquery)
		row := pgxPool.QueryRow(ctx, q, req.TemplateID)
		var tplID, eventID, channel, roleScope, name, desc string
		var isActive bool
		var audits json.RawMessage
		var recipients json.RawMessage
		if err := row.Scan(&tplID, &eventID, &channel, &roleScope, &name, &desc, &isActive, &audits, &recipients); err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}
		var auditsVal interface{}
		var recVal interface{}
		_ = json.Unmarshal(audits, &auditsVal)
		_ = json.Unmarshal(recipients, &recVal)
		out := map[string]interface{}{
			"template_id":   tplID,
			"event_id":      eventID,
			"channel":       channel,
			"role_scope":    roleScope,
			"template_name": name,
			"description":   desc,
			"is_active":     isActive,
			"audits":        auditsVal,
			"recipients":    recVal,
		}
		api.RespondWithPayload(w, true, "", out)
	}
}

// GetTemplatesApprovedActive returns active templates with latest approved audit content and resolved recipients
func GetTemplatesApprovedActive(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		buNames, isAdmin := callerContext(ctx)

		// Entity filter via parent event's entity_name.
		// buNames is already resolved by PreValidation middleware — no extra DB call.
		entityFilterSQL := ""
		var entityArgs []interface{}
		if !isAdmin && len(buNames) > 0 {
			entityFilterSQL = ` AND (COALESCE(e.entity_name,'') = '' OR COALESCE(e.entity_name,'') = ANY($1::text[]))`
			entityArgs = []interface{}{buNames}
		}

		q := `
			WITH latest_audit AS (
				SELECT DISTINCT ON (a.template_id)
					a.template_id, a.processing_status, a.action_type, a.audit_id,
					a.requested_by, a.requested_at, a.checker_by, a.checker_at, a.checker_comment,
					a.version_label, a.subject, a.body_text, a.body_html, a.is_html_enabled, a.formula_steps
				FROM notification_svc.audit_template a
				WHERE a.processing_status = 'APPROVED' AND COALESCE(a.is_deleted, false) = false
				ORDER BY a.template_id, a.requested_at DESC NULLS LAST
			)
			SELECT
				t.template_id, t.event_id, t.channel, t.role_scope, t.template_name, t.description, t.is_active,
				COALESCE(l.subject,'') AS subject, COALESCE(l.body_text,'') AS body_text,
				COALESCE(l.body_html,'') AS body_html, COALESCE(l.is_html_enabled,false) AS is_html_enabled,
				COALESCE(l.version_label,'') AS version_label,
				` + recipientSubquery + ` AS recipients
			FROM notification_svc.template t
			JOIN notification_svc.event e ON e.event_id = t.event_id
			LEFT JOIN latest_audit l ON l.template_id = t.template_id
			WHERE t.is_active = true
		` + entityFilterSQL + ` ORDER BY t.template_name`
		var rows pgx.Rows
		var err error
		if len(entityArgs) > 0 {
			rows, err = pgxPool.Query(ctx, q, entityArgs...)
		} else {
			rows, err = pgxPool.Query(ctx, q)
		}
		if err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}
		defer rows.Close()
		out := make([]map[string]interface{}, 0)
		for rows.Next() {
			var tplID, eventID, channel, roleScope, tplName, desc string
			var isActive bool
			var subject, bodyText, bodyHTML string
			var isHTML bool
			var versionLabel string
			var recipients json.RawMessage
			if err := rows.Scan(&tplID, &eventID, &channel, &roleScope, &tplName, &desc, &isActive, &subject, &bodyText, &bodyHTML, &isHTML, &versionLabel, &recipients); err == nil {
				var recVal interface{}
				_ = json.Unmarshal(recipients, &recVal)
				out = append(out, map[string]interface{}{
					"template_id":     tplID,
					"event_id":        eventID,
					"channel":         channel,
					"role_scope":      roleScope,
					"template_name":   tplName,
					"description":     desc,
					"is_active":       isActive,
					"subject":         subject,
					"body_text":       bodyText,
					"body_html":       bodyHTML,
					"is_html_enabled": isHTML,
					"version_label":   versionLabel,
					"recipients":      recVal,
				})
			}
		}
		api.RespondWithPayload(w, true, "", out)
	}
}

// GetTemplateAuditHistory returns audit rows for a given template_id
func GetTemplateAuditHistory(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			TemplateID string `json:"template_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.TemplateID == "" {
			api.RespondWithPayload(w, false, constants.ErrTemplateIDRequired, nil)
			return
		}
		ctx := r.Context()
		buNames, isAdmin := callerContext(ctx)

		// Access check: verify the template's parent event is in caller's pool.
		if !isAdmin && len(buNames) > 0 {
			var evtEntity string
			_ = pgxPool.QueryRow(ctx,
				`SELECT COALESCE(e.entity_name,'') FROM notification_svc.template t JOIN notification_svc.event e ON e.event_id=t.event_id WHERE t.template_id=$1`,
				req.TemplateID).Scan(&evtEntity)
			if !entityInPool(evtEntity, buNames) {
				api.RespondWithPayload(w, false, "you do not have access to this template", nil)
				return
			}
		}

		q := `SELECT audit_id, template_id, action_type, processing_status, subject, body_text, body_html, is_html_enabled, formula_steps, version_label, requested_by, requested_at, checker_by, checker_at, checker_comment, old_subject, old_body_text, old_body_html, old_formula_steps, COALESCE(is_deleted,false) AS is_deleted, COALESCE(old_recipients,'{}') AS old_recipients FROM notification_svc.audit_template WHERE template_id = $1 ORDER BY requested_at DESC`
		rows, err := pgxPool.Query(ctx, q, req.TemplateID)
		if err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}
		defer rows.Close()
		out := make([]map[string]interface{}, 0)
		for rows.Next() {
			var auditID, templateID, action, proc string
			var subject, bodyText, bodyHTML, requestedBy, checkerBy, checkerComment *string
			var isHTML *bool
			var formula interface{}
			var versionLabel *string
			var requestedAt, checkerAt interface{}
			var oldSubject, oldBodyText, oldBodyHTML *string
			var oldFormula interface{}
			var isDeleted bool
			var oldRecipients interface{}
			if err := rows.Scan(&auditID, &templateID, &action, &proc, &subject, &bodyText, &bodyHTML, &isHTML, &formula, &versionLabel, &requestedBy, &requestedAt, &checkerBy, &checkerAt, &checkerComment, &oldSubject, &oldBodyText, &oldBodyHTML, &oldFormula, &isDeleted, &oldRecipients); err == nil {
				out = append(out, map[string]interface{}{
					"audit_id":          auditID,
					"template_id":       templateID,
					"action_type":       action,
					"processing_status": proc,
					"subject":           subject,
					"body_text":         bodyText,
					"body_html":         bodyHTML,
					"is_html_enabled":   isHTML,
					"formula_steps":     formula,
					"version_label":     versionLabel,
					"requested_by":      requestedBy,
					"requested_at":      requestedAt,
					"checker_by":        checkerBy,
					"checker_at":        checkerAt,
					"checker_comment":   checkerComment,
					"old_subject":       oldSubject,
					"old_body_text":     oldBodyText,
					"old_body_html":     oldBodyHTML,
					"old_formula_steps": oldFormula,
					"is_deleted":        isDeleted,
					"old_recipients":    oldRecipients,
				})
			}
		}
		api.RespondWithPayload(w, true, "", out)
	}
}

func BulkApproveTemplate(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		var req struct {
			UserID   string   `json:"user_id"`
			AuditIDs []string `json:"audit_ids"`
			Comment  string   `json:"comment"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithPayload(w, false, constants.ErrInvalidRequestBody, nil)
			return
		}

		if len(req.AuditIDs) == 0 {
			api.RespondWithPayload(w, false, constants.ErrAuditIDsRequired, nil)
			return
		}

		userEmail := getRequesterEmailTemplate()
		if userEmail == "" {
			api.RespondWithPayload(w, false, constants.ErrInvalidSessionCapitalized, nil)
			return
		}

		ctx := r.Context()

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}
		defer tx.Rollback(ctx)

		rows, err := tx.Query(ctx, `
			SELECT audit_id::text, template_id, action_type, processing_status,
			       COALESCE(change_note, '') AS change_note
			FROM notification_svc.audit_template
			WHERE audit_id = ANY($1::uuid[])
			FOR UPDATE`,
			req.AuditIDs,
		)
		if err != nil {
			api.RespondWithPayload(w, false, "lookup failed: "+err.Error(), nil)
			return
		}
		defer rows.Close()

		type targetRow struct {
			AuditID    string
			TemplateID string
			ActionType string
			Status     string
			ChangeNote string
		}

		var targets []targetRow

		for rows.Next() {
			var t targetRow
			if err := rows.Scan(&t.AuditID, &t.TemplateID, &t.ActionType, &t.Status, &t.ChangeNote); err != nil {
				api.RespondWithPayload(w, false, err.Error(), nil)
				return
			}

			if !strings.Contains(t.Status, "PENDING") {
				api.RespondWithPayload(w, false,
					fmt.Sprintf("audit %s is not in a PENDING state (current: %s)", t.AuditID, t.Status),
					nil)
				return
			}

			targets = append(targets, t)
		}

		if rows.Err() != nil {
			api.RespondWithPayload(w, false, rows.Err().Error(), nil)
			return
		}

		if len(targets) == 0 {
			api.RespondWithPayload(w, false, "no eligible rows", nil)
			return
		}

		const approvalStrategyPrefix = "__STRATEGY__:"

		for _, t := range targets {

			isDeleted := false
			if t.ActionType == "DELETE" {
				isDeleted = true
			}

			_, err = tx.Exec(ctx, `
				UPDATE notification_svc.audit_template
				SET processing_status = 'APPROVED',
				    checker_by        = $1,
				    checker_at        = now(),
				    checker_comment   = $2,
				    is_deleted        = $3
				WHERE audit_id = $4`,
				userEmail,
				req.Comment,
				isDeleted,
				t.AuditID,
			)

			if err != nil {

				// Handle unique constraint violation cleanly
				if strings.Contains(err.Error(), "uniq_one_live_version") {
					api.RespondWithPayload(w, false,
						"another approved live version already exists for this template. reject or archive the existing live version before approving this one.",
						nil)
					return
				}

				api.RespondWithPayload(w, false, "approve failed: "+err.Error(), nil)
				return
			}

			// Update template active flag
			newIsActive := t.ActionType != "DELETE"

			_, err = tx.Exec(ctx, `
				UPDATE notification_svc.template
				SET is_active = $1
				WHERE template_id = $2`,
				newIsActive,
				t.TemplateID,
			)

			if err != nil {
				api.RespondWithPayload(w, false, "template update failed: "+err.Error(), nil)
				return
			}

			// ── Apply recipient_strategy on EDIT approval ────────────────────────
			// The strategy was embedded in change_note with a sentinel prefix during
			// EditTemplateSingle. Extract it here and sync template_recipient.
			if t.ActionType == "EDIT" && strings.HasPrefix(t.ChangeNote, approvalStrategyPrefix) {
				// Strip the sentinel prefix; human note (if any) follows after a newline
				rawAfterPrefix := strings.TrimPrefix(t.ChangeNote, approvalStrategyPrefix)
				strategyRaw := rawAfterPrefix
				if idx := strings.Index(rawAfterPrefix, "\n"); idx >= 0 {
					strategyRaw = rawAfterPrefix[:idx]
				}
				var strategy map[string]interface{}
				if jerr := json.Unmarshal([]byte(strategyRaw), &strategy); jerr == nil && len(strategy) > 0 {
					// Deactivate all current live recipients for this template
					if _, derr := tx.Exec(ctx,
						`UPDATE notification_svc.template_recipient SET is_active = false WHERE template_id = $1`,
						t.TemplateID,
					); derr != nil {
						api.RespondWithPayload(w, false, "recipient deactivation failed: "+derr.Error(), nil)
						return
					}
					// Insert new recipients per the stored strategy
					if _, rerr := populateRecipientsOnTx(ctx, tx, pgxPool, t.TemplateID, strategy, userEmail); rerr != nil {
						api.RespondWithPayload(w, false, "recipient apply failed: "+rerr.Error(), nil)
						return
					}
				}
			}
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"approved_count": len(targets),
			"checker":        userEmail,
		})
	}
}

// BulkRejectTemplate rejects specific audit_template versions by audit_id.
// Accepts { "audit_ids": ["uuid1","uuid2"], "comment": "" }
// Returns per-ID result so caller knows exactly which ones were skipped and why.
func BulkRejectTemplate(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string   `json:"user_id"`
			AuditIDs []string `json:"audit_ids"`
			Comment  string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithPayload(w, false, constants.ErrInvalidRequestBody, nil)
			return
		}
		if len(req.AuditIDs) == 0 {
			api.RespondWithPayload(w, false, constants.ErrAuditIDsRequired, nil)
			return
		}
		userEmail := getRequesterEmailTemplate()
		if userEmail == "" {
			api.RespondWithPayload(w, false, constants.ErrInvalidSessionCapitalized, nil)
			return
		}
		ctx := r.Context()

		// Fetch current status of every supplied audit_id in one round-trip
		// statusRows, err := pgxPool.Query(ctx,
		// 	`SELECT audit_id::text, processing_status, COALESCE(is_deleted, false)
		// 	 FROM notification_svc.audit_template
		// 	 WHERE audit_id = ANY($1::uuid[])`,
		// 	req.AuditIDs)
		// if err != nil {
		// 	api.RespondWithPayload(w, false, "lookup failed: "+err.Error(), nil)
		// 	return
		// }
		// defer statusRows.Close()

		// type rowInfo struct {
		// 	status  string
		// 	deleted bool
		// }
		// current := make(map[string]rowInfo, len(req.AuditIDs))
		// for statusRows.Next() {
		// 	var id, status string
		// 	var deleted bool
		// 	if err := statusRows.Scan(&id, &status, &deleted); err != nil {
		// 		api.RespondWithPayload(w, false, "scan failed: "+err.Error(), nil)
		// 		return
		// 	}
		// 	current[id] = rowInfo{status, deleted}
		// }
		// if statusRows.Err() != nil {
		// 	api.RespondWithPayload(w, false, statusRows.Err().Error(), nil)
		// 	return
		// }

		// var eligible []string
		var results []map[string]interface{}
		// for _, id := range req.AuditIDs {
		// 	info, found := current[id]
		// 	if !found {
		// 		results = append(results, map[string]interface{}{
		// 			"audit_id": id, "success": false, "reason": "not found",
		// 		})
		// 		continue
		// 	}
		// 	if info.deleted {
		// 		results = append(results, map[string]interface{}{
		// 			"audit_id": id, "success": false, "reason": "already deleted",
		// 		})
		// 		continue
		// 	}
		// 	if !strings.Contains(info.status, "PENDING") {
		// 		results = append(results, map[string]interface{}{
		// 			"audit_id": id, "success": false,
		// 			"reason": fmt.Sprintf("cannot reject — current status is '%s'", info.status),
		// 		})
		// 		continue
		// 	}
		// 	eligible = append(eligible, id)
		// }

		// if len(eligible) > 0 {
		_, err := pgxPool.Exec(ctx,
			`UPDATE notification_svc.audit_template
				 SET processing_status = 'REJECTED', checker_by = $1, checker_at = now(), checker_comment = $2
				 WHERE audit_id = ANY($3::uuid[])
				   AND processing_status LIKE '%PENDING%'
				   AND COALESCE(is_deleted, false) = false`,
			userEmail, req.Comment, req.AuditIDs)
		if err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}
		for _, id := range req.AuditIDs {
			results = append(results, map[string]interface{}{
				"audit_id": id, "success": true, "status": "REJECTED",
			})
		}
		// api.RespondWithPayload(w, true, "", map[string]interface{}{
		// 	"rejected_count": tag.RowsAffected(),
		// 	"checker":        userEmail,
		// 	"results":        results,
		// })
		// return
		// }

		// All IDs were ineligible — return detailed failures
		// api.RespondWithPayload(w, false, "no eligible audit rows to reject", results)
		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"checker": userEmail,
			"results": results,
		})
	}
}

// DeleteTemplateVersion raises a PENDING_DELETE_APPROVAL request for each audit_id.
// Accepts { "audit_ids": ["uuid1","uuid2"], "comment": "" }
// Any status (PENDING, APPROVED, REJECTED) can be queued for deletion — only REJECTED cannot be.
// Actual soft-delete (is_deleted=true on audit_template + is_active/is_deleted on template) is applied when a checker calls BulkApproveTemplate.
func DeleteTemplateVersion(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string   `json:"user_id"`
			AuditIDs []string `json:"audit_ids"`
			Reason   string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithPayload(w, false, constants.ErrInvalidRequestBody, nil)
			return
		}
		if len(req.AuditIDs) == 0 {
			api.RespondWithPayload(w, false, constants.ErrAuditIDsRequired, nil)
			return
		}
		userEmail := getRequesterEmailTemplate()
		if userEmail == "" {
			api.RespondWithPayload(w, false, constants.ErrInvalidSessionCapitalized, nil)
			return
		}
		ctx := r.Context()

		// Verify all supplied audit_ids exist and are not already deleted.
		// Any status (PENDING, APPROVED, REJECTED) is allowed to be queued for deletion.
		// rows, err := pgxPool.Query(ctx,
		// 	`SELECT audit_id::text, template_id, COALESCE(version_label,''), processing_status
		// 	 FROM notification_svc.audit_template
		// 	 WHERE audit_id = ANY($1::uuid[])
		// 	   AND COALESCE(is_deleted, false) = false`,
		// 	req.AuditIDs)
		// if err != nil {
		// 	api.RespondWithPayload(w, false, "lookup failed: "+err.Error(), nil)
		// 	return
		// }
		// defer rows.Close()

		type auditRow struct {
			auditID, templateID, versionLabel, status string
		}
		// var valid []auditRow
		// var blocked []map[string]interface{}
		// for rows.Next() {
		// 	var ar auditRow
		// 	if err := rows.Scan(&ar.auditID, &ar.templateID, &ar.versionLabel, &ar.status); err != nil {
		// 		api.RespondWithPayload(w, false, "scan failed: "+err.Error(), nil)
		// 		return
		// 	}
		// 	// All statuses (PENDING, APPROVED, REJECTED) can be queued for deletion
		// 	valid = append(valid, ar)
		// }
		// if rows.Err() != nil {
		// 	api.RespondWithPayload(w, false, rows.Err().Error(), nil)
		// 	return
		// }
		// if len(valid) == 0 {
		// 	api.RespondWithPayload(w, false, "no eligible audit rows found (already deleted?)", blocked)
		// 	return
		// }

		// ULTRA FAST: single UPDATE — marks all eligible rows as PENDING_DELETE_APPROVAL in one query
		validIDs := make([]string, len(req.AuditIDs))
		copy(validIDs, req.AuditIDs)
		if _, err := pgxPool.Exec(ctx,
			`UPDATE notification_svc.audit_template
			 SET processing_status = 'PENDING_DELETE_APPROVAL',
			     requested_by      = $1,
			     requested_at      = now(),
			     reason            = $2,
				 action_type       = 'DELETE'
			 WHERE audit_id = ANY($3::uuid[])`,
			userEmail, req.Reason, validIDs); err != nil {
			api.RespondWithPayload(w, false, constants.ErrUpdateFailed+err.Error(), nil)
			return
		}

		var results []map[string]interface{}
		for _, v := range req.AuditIDs {
			results = append(results, map[string]interface{}{
				"audit_id":      v,
				"template_id":   v,
				"version_label": v,
				"status":        "PENDING_DELETE_APPROVAL",
				"requested_by":  userEmail,
			})
		}
		api.RespondWithPayload(w, true, "", results)
	}
}

// CreateTemplateRecipient inserts a recipient
func CreateTemplateRecipient(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			TemplateID        string `json:"template_id"`
			RecipientType     string `json:"recipient_type"`
			RecipientUserID   string `json:"recipient_user_id"`
			RecipientRole     string `json:"recipient_role"`
			IsActive          *bool  `json:"is_active"`
			RecipientPriority *int   `json:"recipient_priority"` // 1=highest urgency, default 3
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithPayload(w, false, constants.ErrInvalidRequestBody, nil)
			return
		}
		if req.TemplateID == "" || req.RecipientType == "" {
			api.RespondWithPayload(w, false, "template_id and recipient_type required", nil)
			return
		}
		if strings.ToUpper(req.RecipientType) == "USER" && req.RecipientUserID == "" {
			api.RespondWithPayload(w, false, "recipient_user_id required for USER type", nil)
			return
		}
		if strings.ToUpper(req.RecipientType) == "ROLE" && req.RecipientRole == "" {
			api.RespondWithPayload(w, false, "recipient_role required for ROLE type", nil)
			return
		}
		if req.IsActive == nil {
			d := true
			req.IsActive = &d
		}
		priority := 3
		if req.RecipientPriority != nil && *req.RecipientPriority > 0 {
			priority = *req.RecipientPriority
		}
		userEmail := getRequesterEmailTemplate()
		if userEmail == "" {
			api.RespondWithPayload(w, false, constants.ErrInvalidSessionCapitalized, nil)
			return
		}

		ctx := r.Context()
		q := `INSERT INTO notification_svc.template_recipient
			(template_id, recipient_type, recipient_user_id, recipient_role, is_active, created_by, recipient_priority)
			VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING recipient_id`
		var rid string
		if err := pgxPool.QueryRow(ctx, q,
			req.TemplateID, strings.ToUpper(req.RecipientType),
			nullableStr(req.RecipientUserID), nullableStr(req.RecipientRole),
			req.IsActive, userEmail, priority,
		).Scan(&rid); err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}
		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"recipient_id":       rid,
			"recipient_priority": priority,
		})
	}
}

// GetRecipientsByTemplate returns recipients for a template
func GetRecipientsByTemplate(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			TemplateID string `json:"template_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.TemplateID == "" {
			api.RespondWithPayload(w, false, constants.ErrTemplateIDRequired, nil)
			return
		}
		ctx := r.Context()
		q := `SELECT recipient_id, template_id, recipient_type,
			     COALESCE(recipient_user_id,'') AS recipient_user_id,
			     COALESCE(recipient_role,'')    AS recipient_role,
			     is_active, created_at, created_by,
			     COALESCE(recipient_priority,3) AS recipient_priority
			  FROM notification_svc.template_recipient WHERE template_id=$1
			  ORDER BY recipient_priority ASC, created_at ASC`
		rows, err := pgxPool.Query(ctx, q, req.TemplateID)
		if err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}
		defer rows.Close()
		out := make([]map[string]interface{}, 0)
		for rows.Next() {
			var rid, tid, rtype, ruser, rrole, createdBy string
			var isActive bool
			var recipPriority int
			var createdAt interface{}
			if err := rows.Scan(&rid, &tid, &rtype, &ruser, &rrole, &isActive, &createdAt, &createdBy, &recipPriority); err == nil {
				out = append(out, map[string]interface{}{
					"recipient_id":       rid,
					"template_id":        tid,
					"recipient_type":     rtype,
					"recipient_user_id":  ruser,
					"recipient_role":     rrole,
					"is_active":          isActive,
					"created_at":         createdAt,
					"created_by":         createdBy,
					"recipient_priority": recipPriority,
				})
			}
		}
		api.RespondWithPayload(w, true, "", out)
	}
}

// DeleteTemplateRecipient soft-deletes (sets is_active=false) for a recipient
func DeleteTemplateRecipient(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			RecipientID string `json:"recipient_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.RecipientID == "" {
			api.RespondWithPayload(w, false, "recipient_id required", nil)
			return
		}
		ctx := r.Context()
		q := `UPDATE notification_svc.template_recipient SET is_active=false WHERE recipient_id=$1`
		if _, err := pgxPool.Exec(ctx, q, req.RecipientID); err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}
		api.RespondWithPayload(w, true, "", map[string]interface{}{"recipient_id": req.RecipientID})
	}
}

// UpdateTemplateRecipient patches is_active and/or recipient_priority for a single recipient.
//
//	{ "recipient_id": "RCP-xxx", "is_active": true, "recipient_priority": 2 }
func UpdateTemplateRecipient(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			RecipientID       string `json:"recipient_id"`
			IsActive          *bool  `json:"is_active"`
			RecipientPriority *int   `json:"recipient_priority"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.RecipientID == "" {
			api.RespondWithPayload(w, false, "recipient_id required", nil)
			return
		}
		if req.IsActive == nil && req.RecipientPriority == nil {
			api.RespondWithPayload(w, false, "at least one of is_active or recipient_priority is required", nil)
			return
		}

		var sets []string
		var args []interface{}
		pos := 1
		if req.IsActive != nil {
			sets = append(sets, fmt.Sprintf("is_active = $%d", pos))
			args = append(args, *req.IsActive)
			pos++
		}
		if req.RecipientPriority != nil {
			sets = append(sets, fmt.Sprintf("recipient_priority = $%d", pos))
			args = append(args, *req.RecipientPriority)
			pos++
		}
		args = append(args, req.RecipientID)

		q := fmt.Sprintf(`UPDATE notification_svc.template_recipient SET %s WHERE recipient_id = $%d
			RETURNING recipient_id, is_active, recipient_priority`,
			strings.Join(sets, ", "), pos)

		ctx := r.Context()
		var rid string
		var isActiveOut bool
		var priorityOut int
		if err := pgxPool.QueryRow(ctx, q, args...).Scan(&rid, &isActiveOut, &priorityOut); err != nil {
			api.RespondWithPayload(w, false, "recipient not found or update failed: "+err.Error(), nil)
			return
		}
		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"recipient_id":       rid,
			"is_active":          isActiveOut,
			"recipient_priority": priorityOut,
		})
	}
}

// BulkCreateRecipients creates multiple template_recipient rows in a single call.
// Accepts an array of recipient objects, all sharing the same template_id.
//
//	{
//	  "template_id": "TPL-xxx",
//	  "recipients": [
//	    { "recipient_type": "USER", "recipient_user_id": "uid-1", "recipient_priority": 1 },
//	    { "recipient_type": "ROLE", "recipient_role": "TREASURY", "recipient_priority": 2 },
//	    { "recipient_type": "USER", "recipient_user_id": "uid-2" }
//	  ]
//	}
func BulkCreateRecipients(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			TemplateID string `json:"template_id"`
			Recipients []struct {
				RecipientType     string `json:"recipient_type"`
				RecipientUserID   string `json:"recipient_user_id"`
				RecipientRole     string `json:"recipient_role"`
				IsActive          *bool  `json:"is_active"`
				RecipientPriority *int   `json:"recipient_priority"`
			} `json:"recipients"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithPayload(w, false, constants.ErrInvalidRequestBody, nil)
			return
		}
		if req.TemplateID == "" {
			api.RespondWithPayload(w, false, constants.ErrTemplateIDRequired, nil)
			return
		}
		if len(req.Recipients) == 0 {
			api.RespondWithPayload(w, false, "recipients array must not be empty", nil)
			return
		}
		userEmail := getRequesterEmailTemplate()
		if userEmail == "" {
			api.RespondWithPayload(w, false, constants.ErrInvalidSessionCapitalized, nil)
			return
		}

		ctx := r.Context()
		buNames, isAdmin := callerContext(ctx)

		// For USER recipients, validate that each user's business_unit_name is in
		// the caller's accessible pool (already resolved by middleware — no CTE needed).
		// We batch-query all user IDs at once to avoid N+1 queries.
		if !isAdmin && len(buNames) > 0 {
			var userIDs []string
			for _, rec := range req.Recipients {
				if strings.ToUpper(strings.TrimSpace(rec.RecipientType)) == "USER" && rec.RecipientUserID != "" &&
					!strings.Contains(rec.RecipientUserID, "@") { // skip raw-email recipients
					userIDs = append(userIDs, rec.RecipientUserID)
				}
			}
			if len(userIDs) > 0 {
				// Check that each recipient user has at least one entity_mapping
				// that falls within the caller's accessible entity pool.
				ueRows, ueErr := pgxPool.Query(ctx,
					`SELECT user_id, entity_name FROM user_entity_mappings WHERE user_id = ANY($1::text[])`,
					userIDs)
				if ueErr != nil {
					api.RespondWithPayload(w, false, "could not validate recipient entities: "+ueErr.Error(), nil)
					return
				}
				defer ueRows.Close()
				// Collect all entity names per user
				userEntities := make(map[string][]string)
				for ueRows.Next() {
					var uid, uEntity string
					if err := ueRows.Scan(&uid, &uEntity); err != nil {
						continue
					}
					userEntities[uid] = append(userEntities[uid], uEntity)
				}
				ueRows.Close()
				// Each USER recipient must have at least one entity in the caller's pool
				for _, uid := range userIDs {
					entities := userEntities[uid]
					if len(entities) == 0 {
						api.RespondWithPayload(w, false,
							fmt.Sprintf("recipient user %q has no entity mappings", uid), nil)
						return
					}
					inPool := false
					for _, e := range entities {
						if entityInPool(e, buNames) {
							inPool = true
							break
						}
					}
					if !inPool {
						api.RespondWithPayload(w, false,
							fmt.Sprintf("recipient user %q has no entity in your accessible org pool (entities: %v)", uid, entities), nil)
						return
					}
				}
			}
		}

		// Validate and build rows
		rows := make([][]interface{}, 0, len(req.Recipients))
		var validationErrors []string
		for i, rec := range req.Recipients {
			rtype := strings.ToUpper(strings.TrimSpace(rec.RecipientType))
			if rtype == "" {
				validationErrors = append(validationErrors, fmt.Sprintf("recipients[%d]: recipient_type required", i))
				continue
			}
			if rtype == "USER" && rec.RecipientUserID == "" {
				validationErrors = append(validationErrors, fmt.Sprintf("recipients[%d]: recipient_user_id required for USER type", i))
				continue
			}
			if rtype == "ROLE" && rec.RecipientRole == "" {
				validationErrors = append(validationErrors, fmt.Sprintf("recipients[%d]: recipient_role required for ROLE type", i))
				continue
			}
			isActive := true
			if rec.IsActive != nil {
				isActive = *rec.IsActive
			}
			priority := 3
			if rec.RecipientPriority != nil && *rec.RecipientPriority > 0 {
				priority = *rec.RecipientPriority
			}
			rows = append(rows, []interface{}{
				req.TemplateID,
				rtype,
				nullableStr(rec.RecipientUserID),
				nullableStr(rec.RecipientRole),
				isActive,
				userEmail,
				priority,
			})
		}
		if len(validationErrors) > 0 {
			api.RespondWithPayload(w, false, strings.Join(validationErrors, "; "), nil)
			return
		}

		// Insert via transaction
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithPayload(w, false, "db error: "+err.Error(), nil)
			return
		}
		defer tx.Rollback(ctx)

		if err := batchInsertRecipientsOnTx(ctx, tx, rows); err != nil {
			api.RespondWithPayload(w, false, "insert failed: "+err.Error(), nil)
			return
		}
		if err := tx.Commit(ctx); err != nil {
			api.RespondWithPayload(w, false, constants.ErrCommitFailed+err.Error(), nil)
			return
		}
		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"template_id":    req.TemplateID,
			"inserted_count": len(rows),
		})
	}
}

// nullableStr returns nil for empty strings so they land as SQL NULL.
func nullableStr(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

// batchInsertRecipientsOnTx inserts recipient rows using an already-open pgx transaction.
// All rows land in the same tx as the parent template insert, guaranteeing atomicity.
func batchInsertRecipientsOnTx(ctx context.Context, tx pgx.Tx, rows [][]interface{}) error {
	if len(rows) == 0 {
		return nil
	}
	singleQ := `INSERT INTO notification_svc.template_recipient (template_id, recipient_type, recipient_user_id, recipient_role, is_active, created_by, recipient_priority) VALUES ($1::varchar,$2::varchar,$3::varchar,$4::varchar,$5::boolean,$6::varchar,$7::int)`
	for idx, r := range rows {
		args := make([]interface{}, 7)
		for i := 0; i < 7; i++ {
			if i < len(r) && r[i] != nil {
				args[i] = r[i]
			} else {
				switch i {
				case 4:
					args[i] = true
				case 2, 3:
					args[i] = nil
				case 6:
					args[i] = 3 // default recipient_priority
				default:
					args[i] = ""
				}
			}
		}
		if _, err := tx.Exec(ctx, singleQ, args...); err != nil {
			api.LogError("batchInsertRecipientsOnTx row %d failed: %v", idx, err)
			return err
		}
	}
	return nil
}

// helper: batch insert recipients
func batchInsertRecipients(ctx context.Context, pgxPool *pgxpool.Pool, templateID string, rows [][]interface{}) error {
	if len(rows) == 0 {
		return nil
	}
	// Try fast, type-safe bulk insert using pgx CopyFrom
	conn, err := pgxPool.Acquire(ctx)
	if err == nil {
		defer conn.Release()
		// prepare rows for CopyFrom
		cfRows := make([][]interface{}, 0, len(rows))
		for _, r := range rows {
			// ensure length 7 and normalize nils
			row := make([]interface{}, 7)
			for i := 0; i < 7; i++ {
				if i < len(r) {
					if r[i] == nil {
						// replace nils with appropriate typed zero-values or NULL
						if i == 4 { // is_active
							row[i] = true
						} else if i == 2 || i == 3 {
							// recipient_user_id and recipient_role should be NULL when absent
							row[i] = nil
						} else if i == 6 { // recipient_priority
							row[i] = 3
						} else {
							row[i] = ""
						}
					} else {
						row[i] = r[i]
					}
				} else {
					if i == 4 {
						row[i] = true
					} else if i == 2 || i == 3 {
						row[i] = nil
					} else if i == 6 {
						row[i] = 3
					} else {
						row[i] = ""
					}
				}
			}
			cfRows = append(cfRows, row)
		}
		_, err = conn.CopyFrom(ctx, pgx.Identifier{"notification_svc", "template_recipient"}, []string{"template_id", "recipient_type", "recipient_user_id", "recipient_role", "is_active", "created_by", "recipient_priority"}, pgx.CopyFromRows(cfRows))
		if err == nil {
			return nil
		}
		// log and fallthrough to Exec fallback
		api.LogError("batchInsertRecipients CopyFrom failed: err=%v", err)
	} else {
		api.LogError("batchInsertRecipients acquire failed: %v", err)
	}

	// Fallback: insert rows one-by-one inside a transaction to avoid Postgres
	// ambiguous type inference across multi-row parameter lists (SQLSTATE 42P08).
	singleQ := `INSERT INTO notification_svc.template_recipient (template_id, recipient_type, recipient_user_id, recipient_role, is_active, created_by, recipient_priority) VALUES ($1::varchar,$2::varchar,$3::varchar,$4::varchar,$5::boolean,$6::varchar,$7::int)`
	tx, tErr := pgxPool.Begin(ctx)
	if tErr != nil {
		api.LogError("batchInsertRecipients begin tx failed: %v", tErr)
		// fallback to executing each on pool (best-effort)
		for idx, r := range rows {
			args := make([]interface{}, 7)
			for i := 0; i < 7; i++ {
				if i < len(r) && r[i] != nil {
					args[i] = r[i]
				} else {
					if i == 4 {
						args[i] = true
					} else if i == 2 || i == 3 {
						args[i] = nil
					} else if i == 6 {
						args[i] = 3
					} else {
						args[i] = ""
					}
				}
			}
			if _, serr := pgxPool.Exec(ctx, singleQ, args...); serr != nil {
				api.LogError("batchInsertRecipients single row (pool) failed idx=%d args=%v err=%v", idx, args, serr)
				return serr
			}
		}
		return nil
	}
	defer func() {
		// If tx still open, rollback to be safe
		_ = tx.Rollback(ctx)
	}()

	for idx, r := range rows {
		args := make([]interface{}, 7)
		for i := 0; i < 7; i++ {
			if i < len(r) && r[i] != nil {
				args[i] = r[i]
			} else {
				if i == 4 {
					args[i] = true
				} else if i == 2 || i == 3 {
					args[i] = nil
				} else if i == 6 {
					args[i] = 3
				} else {
					args[i] = ""
				}
			}
		}
		if _, serr := tx.Exec(ctx, singleQ, args...); serr != nil {
			api.LogError("batchInsertRecipients tx single row failed idx=%d args=%v err=%v", idx, args, serr)
			_ = tx.Rollback(ctx)
			return serr
		}
	}
	if cerr := tx.Commit(ctx); cerr != nil {
		api.LogError("batchInsertRecipients tx commit failed: %v", cerr)
		return cerr
	}
	return nil
}

// populateRecipientsForTemplate populates recipients according to strategy
func populateRecipientsForTemplate(ctx context.Context, pgxPool *pgxpool.Pool, templateID string, strategy map[string]interface{}, createdBy string) (int, error) {
	api.LogError("[DEBUG] populateRecipientsForTemplate: templateID=%s strategy=%+v createdBy=%s", templateID, strategy, createdBy)
	modeRaw, _ := strategy["mode"].(string)
	mode := strings.ToUpper(strings.TrimSpace(modeRaw))
	api.LogError("[DEBUG] populateRecipientsForTemplate: mode=%s", mode)
	switch mode {
	case "EXPLICIT":
		// expect 'recipients' : [{recipient_type, recipient_user_id, recipient_role, is_active}]
		recs, ok := strategy["recipients"].([]interface{})
		if !ok || len(recs) == 0 {
			return 0, fmt.Errorf("no explicit recipients provided")
		}
		rows := make([][]interface{}, 0, len(recs))
		for _, ri := range recs {
			rmap, ok := ri.(map[string]interface{})
			if !ok {
				continue
			}
			rtype, _ := rmap["recipient_type"].(string)
			ruserRaw, _ := rmap["recipient_user_id"]
			rroleRaw, _ := rmap["recipient_role"]
			var ruser interface{}
			var rrole interface{}
			if s, ok := ruserRaw.(string); ok && s != "" {
				// If the value looks like an email (external user), try to resolve to internal user ID.
				// This handles callers who pass an email address instead of a CIMPLR UUID.
				if strings.Contains(s, "@") {
					var resolvedUID string
					lookupErr := pgxPool.QueryRow(ctx,
						`SELECT id::text FROM users WHERE email = $1 LIMIT 1`, s,
					).Scan(&resolvedUID)
					if lookupErr == nil && resolvedUID != "" {
						// Found the internal user — store their UUID
						api.LogInfo("[NOTIF-RECIPIENT] email=%s resolved to user_id=%s", s, resolvedUID)
						ruser = resolvedUID
					} else {
						// External or unknown email — store it directly so the
						// dispatcher can use it as recipient_email fallback.
						api.LogInfo("[NOTIF-RECIPIENT] email=%s not found in users table (external recipient); storing email directly", s)
						ruser = s // stored in recipient_user_id column; dispatcher will detect it
					}
				} else {
					ruser = s
				}
			} else {
				ruser = nil
			}
			if s, ok := rroleRaw.(string); ok && s != "" {
				rrole = s
			} else {
				rrole = nil
			}
			isActive := true
			if v, ok := rmap["is_active"].(bool); ok {
				isActive = v
			}
			recipPriority := 3
			if v, ok := rmap["recipient_priority"].(float64); ok && v > 0 {
				recipPriority = int(v)
			}
			rows = append(rows, []interface{}{templateID, strings.ToUpper(rtype), ruser, rrole, isActive, createdBy, recipPriority})
		}
		if err := batchInsertRecipients(ctx, pgxPool, templateID, rows); err != nil {
			return 0, err
		}
		return len(rows), nil
	case "ROLE":
		// Expand ROLE into USER recipients: find users having the role and insert USER rows
		roleName, _ := strategy["role"].(string)
		if roleName == "" {
			return 0, fmt.Errorf("role required for ROLE mode")
		}
		// find users for the role
		rowsU, err := pgxPool.Query(ctx, `SELECT u.id FROM users u JOIN user_roles ur ON ur.user_id=u.id JOIN roles ro ON ro.id=ur.role_id WHERE ro.name=$1 OR ro.rolecode=$1`, roleName)
		if err != nil {
			api.LogError("populateRecipientsForTemplate ROLE: failed to query users for role %s: %v", roleName, err)
			return 0, err
		}
		defer rowsU.Close()
		userRows := make([][]interface{}, 0)
		for rowsU.Next() {
			var uid string
			if err := rowsU.Scan(&uid); err == nil {
				userRows = append(userRows, []interface{}{templateID, "USER", uid, nil, true, createdBy})
			}
		}
		if len(userRows) > 0 {
			api.LogError("populateRecipientsForTemplate ROLE: inserting %d users for role %s", len(userRows), roleName)
			if err := batchInsertRecipients(ctx, pgxPool, templateID, userRows); err != nil {
				return 0, err
			}
			return len(userRows), nil
		}
		// fallback: insert a ROLE recipient row so the role is recorded
		q := `INSERT INTO notification_svc.template_recipient (template_id, recipient_type, recipient_role, is_active, created_by) VALUES ($1::varchar,'ROLE',$2::varchar,true,$3::varchar)`
		api.LogError("populateRecipientsForTemplate ROLE: no users found for role %s, inserting ROLE row", roleName)
		if _, err := pgxPool.Exec(ctx, q, templateID, roleName, createdBy); err != nil {
			api.LogError("populateRecipientsForTemplate ROLE: failed to insert ROLE row for %s: %v", roleName, err)
			return 0, err
		}
		return 1, nil
	case "USER":
		// expect 'user_ids' : ["u1","u2"]
		uids, ok := strategy["user_ids"].([]interface{})
		if !ok || len(uids) == 0 {
			return 0, fmt.Errorf("user_ids required for USER mode")
		}
		rows := make([][]interface{}, 0, len(uids))
		for _, ui := range uids {
			uid, _ := ui.(string)
			rows = append(rows, []interface{}{templateID, "USER", uid, nil, true, createdBy})
		}
		if err := batchInsertRecipients(ctx, pgxPool, templateID, rows); err != nil {
			return 0, err
		}
		return len(rows), nil
	case "ALL":
		// select approved users from users table
		rowsU, err := pgxPool.Query(ctx, `SELECT id FROM users WHERE COALESCE(status,'') IN ('approved','active')`)
		if err != nil {
			return 0, err
		}
		defer rowsU.Close()
		rows := make([][]interface{}, 0)
		for rowsU.Next() {
			var uid string
			if err := rowsU.Scan(&uid); err == nil {
				rows = append(rows, []interface{}{templateID, "USER", uid, nil, true, createdBy})
			}
		}
		if len(rows) == 0 {
			return 0, nil
		}
		if err := batchInsertRecipients(ctx, pgxPool, templateID, rows); err != nil {
			return 0, err
		}
		return len(rows), nil
	default:
		return 0, fmt.Errorf("unsupported recipient population mode: %s", mode)
	}
}

// populateRecipientsOnTx is the tx-bound version of populateRecipientsForTemplate.
// All writes go through tx; read-only lookups (e.g. role→user expansion) use pgxPool.
func populateRecipientsOnTx(ctx context.Context, tx pgx.Tx, pgxPool *pgxpool.Pool, templateID string, strategy map[string]interface{}, createdBy string) (int, error) {
	modeRaw, _ := strategy["mode"].(string)
	mode := strings.ToUpper(strings.TrimSpace(modeRaw))
	switch mode {
	case "EXPLICIT":
		recs, ok := strategy["recipients"].([]interface{})
		if !ok || len(recs) == 0 {
			return 0, fmt.Errorf("no explicit recipients provided")
		}
		var rows [][]interface{}
		for _, ri := range recs {
			rmap, ok := ri.(map[string]interface{})
			if !ok {
				continue
			}
			rtype, _ := rmap["recipient_type"].(string)
			var ruser, rrole interface{}
			if s, ok := rmap["recipient_user_id"].(string); ok && s != "" {
				ruser = s
			}
			if s, ok := rmap["recipient_role"].(string); ok && s != "" {
				rrole = s
			}
			isActive := true
			if v, ok := rmap["is_active"].(bool); ok {
				isActive = v
			}
			recipPriority := 3
			if v, ok := rmap["recipient_priority"].(float64); ok && v > 0 {
				recipPriority = int(v)
			}
			rows = append(rows, []interface{}{templateID, strings.ToUpper(rtype), ruser, rrole, isActive, createdBy, recipPriority})
		}
		return len(rows), batchInsertRecipientsOnTx(ctx, tx, rows)

	case "ROLE":
		roleName, _ := strategy["role"].(string)
		if roleName == "" {
			return 0, fmt.Errorf("role required for ROLE mode")
		}
		rollePriority := 3
		if v, ok := strategy["recipient_priority"].(float64); ok && v > 0 {
			rollePriority = int(v)
		}
		// Read-only lookup on pool (role→users) — this is fine, no write conflict
		rowsU, err := pgxPool.Query(ctx,
			`SELECT u.id FROM users u
			  JOIN user_roles ur ON ur.user_id = u.id
			  JOIN roles ro ON ro.id = ur.role_id
			 WHERE ro.name = $1 OR ro.rolecode = $1`, roleName)
		if err != nil {
			return 0, err
		}
		defer rowsU.Close()
		var userRows [][]interface{}
		for rowsU.Next() {
			var uid string
			if err := rowsU.Scan(&uid); err == nil {
				userRows = append(userRows, []interface{}{templateID, "USER", uid, nil, true, createdBy, rollePriority})
			}
		}
		rowsU.Close()
		if len(userRows) > 0 {
			return len(userRows), batchInsertRecipientsOnTx(ctx, tx, userRows)
		}
		// fallback: insert a ROLE sentinel row
		err = batchInsertRecipientsOnTx(ctx, tx, [][]interface{}{{templateID, "ROLE", nil, roleName, true, createdBy, rollePriority}})
		if err != nil {
			return 0, err
		}
		return 1, nil

	case "USER":
		uids, ok := strategy["user_ids"].([]interface{})
		if !ok || len(uids) == 0 {
			return 0, fmt.Errorf("user_ids required for USER mode")
		}
		userPriority := 3
		if v, ok := strategy["recipient_priority"].(float64); ok && v > 0 {
			userPriority = int(v)
		}
		var rows [][]interface{}
		for _, ui := range uids {
			if uid, ok := ui.(string); ok && uid != "" {
				rows = append(rows, []interface{}{templateID, "USER", uid, nil, true, createdBy, userPriority})
			}
		}
		return len(rows), batchInsertRecipientsOnTx(ctx, tx, rows)

	case "ALL":
		allPriority := 3
		if v, ok := strategy["recipient_priority"].(float64); ok && v > 0 {
			allPriority = int(v)
		}
		rowsU, err := pgxPool.Query(ctx, `SELECT id FROM users WHERE COALESCE(status,'') IN ('approved','active')`)
		if err != nil {
			return 0, err
		}
		defer rowsU.Close()
		var rows [][]interface{}
		for rowsU.Next() {
			var uid string
			if err := rowsU.Scan(&uid); err == nil {
				rows = append(rows, []interface{}{templateID, "USER", uid, nil, true, createdBy, allPriority})
			}
		}
		rowsU.Close()
		return len(rows), batchInsertRecipientsOnTx(ctx, tx, rows)

	default:
		return 0, fmt.Errorf("unsupported recipient population mode: %s", mode)
	}
}

// CreateTemplateWithRecipients creates a template and populates recipients per strategy
func CreateTemplateWithRecipients(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID       string                 `json:"user_id"`
			EventID      string                 `json:"event_id"`
			Channel      string                 `json:"channel"`
			RoleScope    string                 `json:"role_scope"`
			TemplateName string                 `json:"template_name"`
			Description  string                 `json:"description"`
			Subject      string                 `json:"subject"`
			BodyText     string                 `json:"body_text"`
			BodyHTML     string                 `json:"body_html"`
			IsHTML       *bool                  `json:"is_html_enabled"`
			Formula      any                    `json:"formula_steps"`
			Strategy     map[string]interface{} `json:"recipient_strategy"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithPayload(w, false, constants.ErrInvalidRequestBody, nil)
			return
		}
		if req.EventID == "" || req.Channel == "" || req.TemplateName == "" {
			api.RespondWithPayload(w, false, constants.ErrEventIDChannelTemplateNameRequired, nil)
			return
		}
		if req.IsHTML == nil {
			def := false
			req.IsHTML = &def
		}
		creator := getRequesterEmailTemplate()
		if creator == "" {
			api.RespondWithPayload(w, false, constants.ErrInvalidSessionCapitalized, nil)
			return
		}

		ctx := r.Context()
		buNames, isAdmin := callerContext(ctx)

		// Entity scope check: caller must be able to access the event they're creating a template for.
		if !isAdmin && len(buNames) > 0 {
			var evtEntity string
			_ = pgxPool.QueryRow(ctx, `SELECT COALESCE(entity_name,'') FROM notification_svc.event WHERE event_id=$1`, req.EventID).Scan(&evtEntity)
			if !entityInPool(evtEntity, buNames) {
				api.RespondWithPayload(w, false, "event entity is not in your accessible org pool", nil)
				return
			}
		}

		// ── Pre-flight template validation (before any DB write) ─────────────────
		isHTMLBool := req.IsHTML != nil && *req.IsHTML
		if findings := validateTemplateRequest(req.Subject, req.BodyText, req.BodyHTML, isHTMLBool); len(findings) > 0 {
			respondValidationErrors(w, findings)
			return
		}

		// ── single transaction: template + audit_template + recipients ────────
		// If recipients fail → whole tx rolls back → no dangling template row.
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}
		defer tx.Rollback(ctx) // no-op after Commit

		// 1. Insert template master row
		insertQ := `INSERT INTO notification_svc.template (event_id, channel, role_scope, template_name, description, created_by) VALUES ($1,$2,$3,$4,$5,$6) RETURNING template_id`
		var tplID string
		if err := tx.QueryRow(ctx, insertQ,
			req.EventID, strings.ToUpper(req.Channel), req.RoleScope,
			req.TemplateName, req.Description, creator,
		).Scan(&tplID); err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}

		// 2. Insert audit_template row
		isHTMLVal := false
		if req.IsHTML != nil {
			isHTMLVal = *req.IsHTML
		}
		var formulaVal interface{} = []byte("[]")
		if req.Formula != nil {
			if jf, jerr := json.Marshal(req.Formula); jerr == nil {
				formulaVal = jf
			}
		}
		auditQ := `INSERT INTO notification_svc.audit_template
			(template_id, action_type, processing_status, subject, body_text, body_html,
			 is_html_enabled, formula_steps, version_label, requested_by, requested_at)
			VALUES ($1,'CREATE','PENDING_APPROVAL',$2,$3,$4,$5,$6,
			        (SELECT 'v' || (COUNT(*)+1) FROM notification_svc.audit_template WHERE template_id = $8),
			        $7, now())`
		if _, err := tx.Exec(ctx, auditQ,
			tplID, req.Subject, req.BodyText, req.BodyHTML,
			isHTMLVal, formulaVal, creator, tplID,
		); err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}

		// 3. Insert recipients inside the SAME tx (FK satisfied because template row
		//    is visible within the transaction — Postgres sees intra-tx uncommitted rows).
		//    populateRecipientsOnTx does read-only lookups on pgxPool and all writes via tx.
		added := 0
		if req.Strategy != nil {
			c, err := populateRecipientsOnTx(ctx, tx, pgxPool, tplID, req.Strategy, creator)
			if err != nil {
				// tx.Rollback fires via defer — nothing is committed
				api.RespondWithPayload(w, false, "recipients failed — rolled back: "+err.Error(), nil)
				return
			}
			added = c
		}

		// 4. Commit everything atomically
		if err := tx.Commit(ctx); err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{"template_id": tplID, "recipients_added": added, "requested": creator})
	}
}

// CreateTemplateWithRecipientsBulk creates multiple templates and populates recipients per-row strategy
func CreateTemplateWithRecipientsBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			Rows   []struct {
				EventID      string                 `json:"event_id"`
				Channel      string                 `json:"channel"`
				RoleScope    string                 `json:"role_scope"`
				TemplateName string                 `json:"template_name"`
				Description  string                 `json:"description"`
				Subject      string                 `json:"subject"`
				BodyText     string                 `json:"body_text"`
				BodyHTML     string                 `json:"body_html"`
				IsHTML       *bool                  `json:"is_html_enabled"`
				Formula      any                    `json:"formula_steps"`
				Strategy     map[string]interface{} `json:"recipient_strategy"`
			} `json:"rows"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithPayload(w, false, constants.ErrInvalidRequestBody, nil)
			return
		}
		if len(req.Rows) == 0 {
			api.RespondWithPayload(w, false, "rows required", nil)
			return
		}
		if len(req.Rows) > 100 {
			api.RespondWithPayload(w, false, "too many rows: max 100 templates per bulk request", nil)
			return
		}
		creator := getRequesterEmailTemplate()
		if creator == "" {
			api.RespondWithPayload(w, false, constants.ErrInvalidSessionCapitalized, nil)
			return
		}

		// Entity scope check: batch-fetch all event entities and verify access before any DB write.
		{
			ctxCheck := r.Context()
			buNamesCheck, isAdminCheck := callerContext(ctxCheck)
			if !isAdminCheck && len(buNamesCheck) > 0 {
				// Collect unique event IDs
				evtIDSet := make(map[string]struct{})
				for _, rrow := range req.Rows {
					if rrow.EventID != "" {
						evtIDSet[rrow.EventID] = struct{}{}
					}
				}
				evtIDs := make([]string, 0, len(evtIDSet))
				for id := range evtIDSet {
					evtIDs = append(evtIDs, id)
				}
				// Single batch query for all event entities
				evtEntityRows, qErr := pgxPool.Query(ctxCheck,
					`SELECT event_id::text, COALESCE(entity_name,'') FROM notification_svc.event WHERE event_id = ANY($1::text[])`,
					evtIDs)
				if qErr == nil {
					for evtEntityRows.Next() {
						var eid, eEntity string
						if scanErr := evtEntityRows.Scan(&eid, &eEntity); scanErr == nil {
							if !entityInPool(eEntity, buNamesCheck) {
								evtEntityRows.Close()
								api.RespondWithPayload(w, false,
									fmt.Sprintf("event %q entity %q is not in your accessible org pool", eid, eEntity), nil)
								return
							}
						}
					}
					evtEntityRows.Close()
				}
			}
		}

		// ── Pre-flight validation: check every row BEFORE touching the DB ────────
		type rowValidationError struct {
			Index int    `json:"index"`
			Name  string `json:"template_name"`
			Error string `json:"error"`
		}
		var validationErrors []rowValidationError
		for i, rrow := range req.Rows {
			if rrow.EventID == "" || rrow.Channel == "" || rrow.TemplateName == "" {
				validationErrors = append(validationErrors, rowValidationError{
					Index: i, Name: rrow.TemplateName,
					Error: constants.ErrEventIDChannelTemplateNameRequired,
				})
				continue
			}
			isHTMLBool := rrow.IsHTML != nil && *rrow.IsHTML
			if findings := validateTemplateRequest(rrow.Subject, rrow.BodyText, rrow.BodyHTML, isHTMLBool); len(findings) > 0 {
				// Flatten per-field errors into a single readable string for this row
				var msgs []string
				for _, f := range findings {
					msgs = append(msgs, "["+f["field"]+"] "+f["error"])
				}
				validationErrors = append(validationErrors, rowValidationError{
					Index: i, Name: rrow.TemplateName,
					Error: strings.Join(msgs, " | "),
				})
			}
		}
		if len(validationErrors) > 0 {
			w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
			w.WriteHeader(http.StatusUnprocessableEntity)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success":           false,
				"error":             fmt.Sprintf("%d row(s) failed validation. Fix all errors and retry — no templates were saved.", len(validationErrors)),
				"validation_errors": validationErrors,
			})
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}
		defer tx.Rollback(ctx)

		// Build batch insert for templates
		valueStrings := make([]string, 0, len(req.Rows))
		valueArgs := make([]interface{}, 0, len(req.Rows)*6)
		for i, rrow := range req.Rows {
			if rrow.IsHTML == nil {
				def := false
				rrow.IsHTML = &def
			}
			pos := i*6 + 1
			valueStrings = append(valueStrings, fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d)", pos, pos+1, pos+2, pos+3, pos+4, pos+5))
			valueArgs = append(valueArgs, rrow.EventID, strings.ToUpper(rrow.Channel), rrow.RoleScope, rrow.TemplateName, rrow.Description, creator)
		}

		batchQ := fmt.Sprintf(`INSERT INTO notification_svc.template (event_id, channel, role_scope, template_name, description, created_by) VALUES %s RETURNING template_id, template_name`, strings.Join(valueStrings, ","))
		api.LogError("[DEBUG] Template batch insert: sql=%s args=%v", batchQ, valueArgs)
		rowsOut, err := tx.Query(ctx, batchQ, valueArgs...)
		if err != nil {
			api.LogError("[DEBUG] Template batch insert FAILED: sql=%s args=%v err=%v", batchQ, valueArgs, err)
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}
		defer rowsOut.Close()
		var inserted []map[string]interface{}
		var templateIDs []string
		for rowsOut.Next() {
			var id, name string
			if err := rowsOut.Scan(&id, &name); err == nil {
				inserted = append(inserted, map[string]interface{}{"template_id": id, "template_name": name})
				templateIDs = append(templateIDs, id)
			}
		}

		// insert audit rows for created templates (compute version_label per template)
		if len(templateIDs) > 0 {
			for idx, id := range templateIDs {
				// map back to request row at same index
				rrow := req.Rows[idx]
				isHTMLVal := false
				if rrow.IsHTML != nil {
					isHTMLVal = *rrow.IsHTML
				}
				var formulaVal interface{} = []byte("[]")
				if rrow.Formula != nil {
					if jf, jerr := json.Marshal(rrow.Formula); jerr == nil {
						formulaVal = jf
					}
				}
				aq := `INSERT INTO notification_svc.audit_template (template_id, action_type, processing_status, subject, body_text, body_html, is_html_enabled, formula_steps, version_label, requested_by, requested_at) VALUES ($1,'CREATE','PENDING_APPROVAL',$2,$3,$4,$5,$6,(SELECT 'v' || (COUNT(*)+1) FROM notification_svc.audit_template WHERE template_id = $8),$7,now())`
				api.LogError("[DEBUG] Audit template insert: template=%s subject=%v is_html=%v formula=%v", id, rrow.Subject, isHTMLVal, formulaVal)
				if _, err := tx.Exec(ctx, aq, id, rrow.Subject, rrow.BodyText, rrow.BodyHTML, isHTMLVal, formulaVal, creator, id); err != nil {
					api.LogError("[DEBUG] Audit template insert FAILED: sql=%s err=%v", aq, err)
					api.RespondWithPayload(w, false, err.Error(), nil)
					return
				}
			}
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}

		// Now populate recipients per-row (outside tx — each uses its own pool conn)
		totalAdded := 0
		summary := make([]map[string]interface{}, 0, len(templateIDs))
		for i, tplID := range templateIDs {
			strat := req.Rows[i].Strategy
			api.LogError("[DEBUG] Bulk populate: template=%s strategy=%+v", tplID, strat)
			if strat == nil {
				summary = append(summary, map[string]interface{}{"template_id": tplID, "added": 0, "error": "no strategy"})
				continue
			}
			c, err := populateRecipientsForTemplate(ctx, pgxPool, tplID, strat, creator)
			if err != nil {
				api.LogError("populateRecipientsForTemplate failed for template %s: %v", tplID, err)
				summary = append(summary, map[string]interface{}{"template_id": tplID, "added": 0, "error": err.Error()})
				continue
			}
			totalAdded += c
			summary = append(summary, map[string]interface{}{"template_id": tplID, "added": c, "error": ""})
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{"inserted": inserted, "recipients_added": totalAdded, "recipient_summary": summary})
	}
}
