package notification

import (
	"errors"
	"fmt"
	"html"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"CimplrCorpSaas/api/constants"
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
func EvaluateTemplate(tpl string, payload map[string]interface{}) (string, error) {
	// Evaluate nested function calls by repeatedly locating the innermost function
	result := tpl
	for {
		start, end, name, argsRaw, found := findInnermostFunction(result)
		if !found {
			break
		}
		args := splitArgs(argsRaw)
		evaluated, err := evaluateFunction(name, args, payload)
		if err != nil {
			return "", err
		}
		// replace start..end (inclusive) with evaluated value
		result = result[:start] + evaluated + result[end+1:]
	}

	// Replace variables {{Var}} with payload values
	varRe := regexp.MustCompile(`{{\s*([^}]+?)\s*}}`)
	result = varRe.ReplaceAllStringFunc(result, func(s string) string {
		sub := varRe.FindStringSubmatch(s)
		if len(sub) < 2 {
			return s
		}
		key := strings.TrimSpace(sub[1])
		v, ok := lookupPayload(key, payload)
		if !ok {
			return ""
		}
		return toString(v)
	})

	return result, nil
}

// splitArgs splits a comma-separated argument string into trimmed args while handling quoted strings.
func splitArgs(s string) []string {
	args := []string{}
	current := strings.Builder{}
	inSingle := false
	inDouble := false
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
		if r == ',' && !inSingle && !inDouble {
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

// findInnermostFunction scans s and returns the indexes (start of name, index of closing ')'),
// function name, raw args (between parentheses), and found flag.
// It finds the innermost parentheses pair and the function name immediately preceding the '('.
func findInnermostFunction(s string) (int, int, string, string, bool) {
	stack := []int{}
	runes := []rune(s)
	for i, r := range runes {
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
			argsRaw := string(runes[openIdx+1 : i])
			return nameStart, i, name, argsRaw, true
		}
	}
	return 0, 0, "", "", false
}

// evaluateFunction executes supported functions and returns their string result.
func evaluateFunction(name string, args []string, payload map[string]interface{}) (string, error) {
	up := strings.ToUpper(name)
	switch up {
	case "SUM":
		if len(args) != 1 {
			return "", errors.New("SUM expects 1 argument: list variable name")
		}
		list, ok := getNumberList(args[0], payload)
		if !ok {
			return "0", nil
		}
		s := 0.0
		for _, v := range list {
			s += v
		}
		return toStringFloat(s), nil
	case "AVERAGE", "AVG":
		if len(args) != 1 {
			return "", errors.New("AVERAGE expects 1 argument: list variable name")
		}
		list, ok := getNumberList(args[0], payload)
		if !ok || len(list) == 0 {
			return "0", nil
		}
		s := 0.0
		for _, v := range list {
			s += v
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
		n := min(len(a), len(b))
		s := 0.0
		for i := 0; i < n; i++ {
			s += a[i] * b[i]
		}
		return toStringFloat(s), nil
	case "FORMAT_NUMBER":
		if len(args) != 1 {
			return "", errors.New("FORMAT_NUMBER expects 1 numeric argument")
		}
		v, ok := resolveNumericArg(args[0], payload)
		if !ok {
			return "", nil
		}
		return formatCurrency(v), nil
	case "FORMAT_DATE":
		// FORMAT_DATE(dateVarOrLiteral, format)
		if len(args) != 2 {
			return "", errors.New("FORMAT_DATE expects 2 args: date, format")
		}
		dateStr := ""
		if isQuoted(args[0]) {
			dateStr = unquote(args[0])
		} else {
			v, ok := lookupPayload(args[0], payload)
			if !ok {
				return "", nil
			}
			dateStr = toString(v)
		}
		format := unquote(args[1])
		// try parse common layouts
		t, err := parseDate(dateStr)
		if err != nil {
			return "", nil
		}
		return t.Format(format), nil
	case "FORMAT_DATE_TZ":
		// FORMAT_DATE_TZ(dateVarOrLiteral, format, timezone)
		if len(args) != 3 {
			return "", errors.New("FORMAT_DATE_TZ expects 3 args: date, format, tz")
		}
		dateStr := ""
		if isQuoted(args[0]) {
			dateStr = unquote(args[0])
		} else {
			v, ok := lookupPayload(args[0], payload)
			if !ok {
				return "", nil
			}
			dateStr = toString(v)
		}
		format := unquote(args[1])
		tz := unquote(args[2])
		t, err := parseDate(dateStr)
		if err != nil {
			return "", nil
		}
		loc, lerr := time.LoadLocation(tz)
		if lerr == nil {
			t = t.In(loc)
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
			if isQuoted(a) {
				out.WriteString(unquote(a))
			} else {
				val, ok := lookupPayload(a, payload)
				if ok {
					out.WriteString(toString(val))
				}
			}
		}
		return out.String(), nil
	case "UPPER":
		if len(args) != 1 {
			return "", errors.New("UPPER expects 1 argument")
		}
		if isQuoted(args[0]) {
			return strings.ToUpper(unquote(args[0])), nil
		}
		v, ok := lookupPayload(args[0], payload)
		if !ok {
			return "", nil
		}
		return strings.ToUpper(toString(v)), nil
	case "LOWER":
		if len(args) != 1 {
			return "", errors.New("LOWER expects 1 argument")
		}
		if isQuoted(args[0]) {
			return strings.ToLower(unquote(args[0])), nil
		}
		v, ok := lookupPayload(args[0], payload)
		if !ok {
			return "", nil
		}
		return strings.ToLower(toString(v)), nil
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

	// ─── LIST / TABLE FUNCTIONS ────────────────────────────────────────────────
	// All list functions work on a payload key that holds []map[string]interface{}
	// (i.e. a slice of row-objects). This matches the BankStatementNotifPayload
	// Transactions, CategoryKPIs, etc. fields that are serialised into the
	// TriggerNotification payload.

	case "COUNT_OF":
		// COUNT_OF(listVar) → number of items in the list
		if len(args) != 1 {
			return "", errors.New("COUNT_OF expects 1 argument: list variable name")
		}
		rows, ok := getRowList(args[0], payload)
		if !ok {
			return "0", nil
		}
		return strconv.Itoa(len(rows)), nil

	case "SUM_OF_FIELD":
		// SUM_OF_FIELD(listVar, 'field') → sum numeric field across all rows
		if len(args) != 2 {
			return "", errors.New("SUM_OF_FIELD expects 2 args: listVar, 'field'")
		}
		rows, ok := getRowList(args[0], payload)
		if !ok {
			return "0", nil
		}
		field := unquote(args[1])
		total := 0.0
		for _, row := range rows {
			total += rowFloat(row, field)
		}
		return toStringFloat(total), nil

	case "TOTAL_OF":
		// Alias of SUM_OF_FIELD for more natural template language
		// TOTAL_OF(listVar, 'field')
		if len(args) != 2 {
			return "", errors.New("TOTAL_OF expects 2 args: listVar, 'field'")
		}
		rows, ok := getRowList(args[0], payload)
		if !ok {
			return "0", nil
		}
		field := unquote(args[1])
		total := 0.0
		for _, row := range rows {
			total += rowFloat(row, field)
		}
		return toStringFloat(total), nil

	case "AVG_OF_FIELD":
		// AVG_OF_FIELD(listVar, 'field') → average of numeric field
		if len(args) != 2 {
			return "", errors.New("AVG_OF_FIELD expects 2 args: listVar, 'field'")
		}
		rows, ok := getRowList(args[0], payload)
		if !ok || len(rows) == 0 {
			return "0", nil
		}
		field := unquote(args[1])
		total := 0.0
		for _, row := range rows {
			total += rowFloat(row, field)
		}
		return toStringFloat(total / float64(len(rows))), nil

	case "MAX_OF_FIELD":
		// MAX_OF_FIELD(listVar, 'field')
		if len(args) != 2 {
			return "", errors.New("MAX_OF_FIELD expects 2 args: listVar, 'field'")
		}
		rows, ok := getRowList(args[0], payload)
		if !ok || len(rows) == 0 {
			return "0", nil
		}
		field := unquote(args[1])
		mx := rowFloat(rows[0], field)
		for _, row := range rows[1:] {
			if v := rowFloat(row, field); v > mx {
				mx = v
			}
		}
		return toStringFloat(mx), nil

	case "MIN_OF_FIELD":
		// MIN_OF_FIELD(listVar, 'field')
		if len(args) != 2 {
			return "", errors.New("MIN_OF_FIELD expects 2 args: listVar, 'field'")
		}
		rows, ok := getRowList(args[0], payload)
		if !ok || len(rows) == 0 {
			return "0", nil
		}
		field := unquote(args[1])
		mn := rowFloat(rows[0], field)
		for _, row := range rows[1:] {
			if v := rowFloat(row, field); v < mn {
				mn = v
			}
		}
		return toStringFloat(mn), nil

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
		// Store back into payload so subsequent functions in the same evaluation chain
		// can read __filter_result. Also return count.
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
		// with headers = column names, rows = values from each map row.
		// TABLE_HTML(Transactions, 'tran_date', 'description', 'withdrawal_amount', 'deposit_amount', 'category')
		if len(args) < 2 {
			return "", errors.New("TABLE_HTML expects listVar + at least 1 column")
		}
		rows, ok := getRowList(args[0], payload)
		if !ok {
			return "<table><tr><td>No data</td></tr></table>", nil
		}
		cols := make([]string, len(args)-1)
		for i, a := range args[1:] {
			cols[i] = unquote(a)
		}
		var sb strings.Builder
		sb.WriteString(`<table style="border-collapse:collapse;width:100%;font-family:sans-serif;font-size:13px">`)
		// header
		sb.WriteString(`<thead><tr>`)
		for _, c := range cols {
			label := strings.ReplaceAll(strings.Title(strings.ReplaceAll(c, "_", " ")), " ", "&nbsp;")
			sb.WriteString(fmt.Sprintf(`<th style="border:1px solid #ddd;padding:8px;background:#f2f2f2;text-align:left">%s</th>`, label))
		}
		sb.WriteString(`</tr></thead><tbody>`)
		// rows
		for idx, row := range rows {
			bg := "#fff"
			if idx%2 == 1 {
				bg = "#f9f9f9"
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
			cols[i] = unquote(a)
		}
		var sb strings.Builder
		for idx, row := range rows {
			bg := "#fff"
			if idx%2 == 1 {
				bg = "#f9f9f9"
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
			return "<table><tr><td>No data</td></tr></table>", nil
		}
		grouped, ok := toRowList(v)
		if !ok || len(grouped) == 0 {
			return "<table><tr><td>No data</td></tr></table>", nil
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
				bg = "#f9f9f9"
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
		// KPI_CARDS_HTML(groupedListVar) → renders colourful KPI summary cards from GROUP_BY result.
		// Each card: category name, transaction count, debit total, credit total.
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
		colors := []string{"#4e73df", "#1cc88a", "#36b9cc", "#f6c23e", "#e74a3b", "#858796"}
		var sb strings.Builder
		sb.WriteString(`<div style="display:flex;flex-wrap:wrap;gap:12px;margin:8px 0">`)
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

	case "FORMAT_CURRENCY":
		// FORMAT_CURRENCY(val) or FORMAT_CURRENCY(val, 'CurrencyCode')
		// The optional second argument is a currency-code label (e.g. 'INR') that
		// is prepended to the formatted number. It is accepted but not required.
		if len(args) < 1 || len(args) > 2 {
			return "", errors.New("FORMAT_CURRENCY expects 1 or 2 arguments: value [, 'currency_code']")
		}
		v, ok := resolveNumericArg(args[0], payload)
		if !ok {
			return "", nil
		}
		formatted := formatCurrency(v)
		if len(args) == 2 {
			// second arg is a currency code literal like 'INR' or a payload var
			code := unquote(args[1])
			if code == "" {
				if cv, ok2 := lookupPayload(args[1], payload); ok2 {
					code = fmt.Sprintf("%v", cv)
				}
			}
			if code != "" {
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
	}

	return "", fmt.Errorf("unsupported function: %s", name)
}

func min(a, b int) int {
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
	s = strings.TrimSpace(s)
	if isQuoted(s) {
		return s[1 : len(s)-1]
	}
	return s
}

// lookupPayload fetches a key from payload (case-sensitive then case-insensitive)
func lookupPayload(key string, payload map[string]interface{}) (interface{}, bool) {
	key = strings.TrimSpace(key)
	if key == "" {
		return nil, false
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
