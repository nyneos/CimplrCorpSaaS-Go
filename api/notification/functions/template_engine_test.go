package notification

import (
	"strings"
	"testing"
)

// ═══════════════════════════════════════════════════════════════════════════
// FLEXIBLE TEMPLATE ENGINE TESTS
// Tests both WRAPPED {{...}} and UNWRAPPED (seed script) template formats
// ═══════════════════════════════════════════════════════════════════════════

// TestUnwrappedTemplate_BasicFunctions tests unwrapped function calls (seed script format)
func TestUnwrappedTemplate_BasicFunctions(t *testing.T) {
	tests := []struct {
		name     string
		template string
		payload  map[string]interface{}
		expected string
	}{
		{
			name:     "COUNT_OF unwrapped",
			template: "You have COUNT_OF(BankStatementIDs) statement(s)",
			payload:  map[string]interface{}{"BankStatementIDs": []interface{}{"BST-1", "BST-2", "BST-3"}},
			expected: "You have 3 statement(s)",
		},
		{
			name:     "CONCAT unwrapped",
			template: "CONCAT(COUNT_OF(Items), ' items by ', UserID)",
			payload:  map[string]interface{}{"Items": []interface{}{1, 2}, "UserID": "admin"},
			expected: "2 items by admin",
		},
		{
			name:     "FORMAT_NUMBER unwrapped",
			template: "Total: FORMAT_NUMBER(Amount)",
			payload:  map[string]interface{}{"Amount": 12345.67},
			expected: "Total: ₹ 12,345.67",
		},
		{
			name:     "Mixed functions unwrapped",
			template: "Bank Statement Rejected — COUNT_OF(BankStatementIDs) Statement(s) — Action: {{Action}}",
			payload:  map[string]interface{}{"BankStatementIDs": []interface{}{"BST-1"}, "Action": "REJECT"},
			expected: "Bank Statement Rejected — 1 Statement(s) — Action: REJECT",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := EvaluateTemplate(tt.template, tt.payload)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if result != tt.expected {
				t.Errorf("expected '%s', got '%s'", tt.expected, result)
			}
		})
	}
}

// TestUnwrappedTemplate_SeedScriptExamples tests exact examples from seed scripts
func TestUnwrappedTemplate_SeedScriptExamples(t *testing.T) {
	// From seed_notification_events.sh BS_DELETE templates
	t.Run("BS_DELETE subject", func(t *testing.T) {
		tpl := "CONCAT(COUNT_OF(BankStatementIDs), ' stmt delete(s) awaiting approval')"
		payload := map[string]interface{}{
			"BankStatementIDs": []interface{}{"BST-87D6801"},
		}
		result, err := EvaluateTemplate(tpl, payload)
		if err != nil {
			t.Fatalf("error: %v", err)
		}
		expected := "1 stmt delete(s) awaiting approval"
		if result != expected {
			t.Errorf("expected '%s', got '%s'", expected, result)
		}
	})

	t.Run("BS_DELETE body", func(t *testing.T) {
		tpl := "CONCAT(COUNT_OF(BankStatementIDs), ' delete request(s) by ', UserID, ' awaiting approval.')"
		payload := map[string]interface{}{
			"BankStatementIDs": []interface{}{"BST-87D6801"},
			"UserID":           "1",
		}
		result, err := EvaluateTemplate(tpl, payload)
		if err != nil {
			t.Fatalf("error: %v", err)
		}
		expected := "1 delete request(s) by 1 awaiting approval."
		if result != expected {
			t.Errorf("expected '%s', got '%s'", expected, result)
		}
	})

	t.Run("BS_REJECT SMS", func(t *testing.T) {
		tpl := "CONCAT(COUNT_OF(BankStatementIDs), ' bank statement(s) REJECTED by ', UserID)"
		payload := map[string]interface{}{
			"BankStatementIDs": []interface{}{"BST-1", "BST-2"},
			"UserID":           "admin@example.com",
		}
		result, err := EvaluateTemplate(tpl, payload)
		if err != nil {
			t.Fatalf("error: %v", err)
		}
		expected := "2 bank statement(s) REJECTED by admin@example.com"
		if result != expected {
			t.Errorf("expected '%s', got '%s'", expected, result)
		}
	})
}

// TestWrappedTemplate_BackwardCompatibility ensures wrapped templates still work
func TestWrappedTemplate_BackwardCompatibility(t *testing.T) {
	tests := []struct {
		name     string
		template string
		payload  map[string]interface{}
		expected string
	}{
		{
			name:     "Wrapped COUNT_OF",
			template: "You have {{COUNT_OF(BankStatementIDs)}} statement(s)",
			payload:  map[string]interface{}{"BankStatementIDs": []interface{}{"BST-1", "BST-2"}},
			expected: "You have 2 statement(s)",
		},
		{
			name:     "Wrapped CONCAT",
			template: "{{CONCAT('Total: ', COUNT_OF(Items))}}",
			payload:  map[string]interface{}{"Items": []interface{}{1, 2, 3}},
			expected: "Total: 3",
		},
		{
			name:     "Wrapped variables",
			template: "Hello {{UserName}}, Policy {{PolicyID}}",
			payload:  map[string]interface{}{"UserName": "John", "PolicyID": "P-123"},
			expected: "Hello John, Policy P-123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := EvaluateTemplate(tt.template, tt.payload)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if result != tt.expected {
				t.Errorf("expected '%s', got '%s'", tt.expected, result)
			}
		})
	}
}

// TestMixedTemplate_WrappedAndUnwrapped should NOT auto-wrap if {{}} exists
func TestMixedTemplate_PreservesWrappedFormat(t *testing.T) {
	// If template has ANY {{...}}, it's treated as wrapped format
	// Unwrapped portions are NOT auto-wrapped (backward compatible)
	tpl := "{{COUNT_OF(Items)}} items - Status is Pending"
	payload := map[string]interface{}{
		"Items":  []interface{}{1, 2},
		"Status": "APPROVED",
	}
	result, err := EvaluateTemplate(tpl, payload)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	// "Status" should NOT be auto-wrapped because template contains {{}}
	expected := "2 items - Status is Pending"
	if result != expected {
		t.Errorf("expected '%s', got '%s'", expected, result)
	}
}

func TestEvaluateTemplate_Nested(t *testing.T) {
	tpl := "Result: ADD( ADD(1,2) , MULTIPLY(3,4) )"
	out, err := EvaluateTemplate(tpl, nil)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if out != "Result: 15" {
		t.Fatalf("expected Result: 15, got '%s'", out)
	}
}

func TestEvaluateTemplate_DateAndEscape(t *testing.T) {
	tpl := "Due: FORMAT_DATE('2026-05-12','2006-01-02') | Escaped: ESCAPE_HTML('<b>Hi</b>')"
	out, err := EvaluateTemplate(tpl, nil)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	expected := "Due: 2026-05-12 | Escaped: &lt;b&gt;Hi&lt;/b&gt;"
	if out != expected {
		t.Fatalf("expected '%s', got '%s'", expected, out)
	}
}

func TestEvaluateTemplate_VarsAndSum(t *testing.T) {
	tpl := "Total: FORMAT_NUMBER(SUM(Premiums))"
	payload := map[string]interface{}{
		"Premiums": []interface{}{500.25, 750.25},
	}
	out, err := EvaluateTemplate(tpl, payload)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	expected := "Total: ₹ 1,250.50"
	if out != expected {
		t.Fatalf("expected '%s', got '%s'", expected, out)
	}
}

func TestExtractVariables(t *testing.T) {
	tpl := "Hello {{UserName}}, Policy {{ PolicyID }}"
	vars := ExtractVariables(tpl)
	if len(vars) != 2 {
		t.Fatalf("expected 2 vars, got %d", len(vars))
	}
}

// ─── New list / table function tests ──────────────────────────────────────────

func testTxns() []map[string]interface{} {
	return []map[string]interface{}{
		{"tran_date": "2026-01-10", "description": "Salary", "withdrawal_amount": 0.0, "deposit_amount": 50000.0, "category_name": "CREDIT", "type": "CREDIT", "amount": 50000.0},
		{"tran_date": "2026-01-12", "description": "Rent", "withdrawal_amount": 20000.0, "deposit_amount": 0.0, "category_name": "DEBIT", "type": "DEBIT", "amount": 20000.0},
		{"tran_date": "2026-01-15", "description": "Groceries", "withdrawal_amount": 3000.0, "deposit_amount": 0.0, "category_name": "DEBIT", "type": "DEBIT", "amount": 3000.0},
		{"tran_date": "2026-01-20", "description": "Bonus", "withdrawal_amount": 0.0, "deposit_amount": 10000.0, "category_name": "CREDIT", "type": "CREDIT", "amount": 10000.0},
	}
}

func TestCountOf(t *testing.T) {
	payload := map[string]interface{}{"Transactions": testTxns()}
	out, err := EvaluateTemplate("COUNT_OF(Transactions)", payload)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if out != "4" {
		t.Fatalf("expected 4, got '%s'", out)
	}
}

func TestSumOfField(t *testing.T) {
	payload := map[string]interface{}{"Transactions": testTxns()}
	out, err := EvaluateTemplate("SUM_OF_FIELD(Transactions, 'deposit_amount')", payload)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if out != "60000" {
		t.Fatalf("expected 60000, got '%s'", out)
	}
}

func TestTotalOf(t *testing.T) {
	payload := map[string]interface{}{"Transactions": testTxns()}
	out, err := EvaluateTemplate("TOTAL_OF(Transactions, 'withdrawal_amount')", payload)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if out != "23000" {
		t.Fatalf("expected 23000, got '%s'", out)
	}
}

func TestAvgOfField(t *testing.T) {
	payload := map[string]interface{}{"Transactions": testTxns()}
	out, err := EvaluateTemplate("AVG_OF_FIELD(Transactions, 'amount')", payload)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	// (50000+20000+3000+10000)/4 = 20750
	if out != "20750" {
		t.Fatalf("expected 20750, got '%s'", out)
	}
}

func TestMaxMinOfField(t *testing.T) {
	payload := map[string]interface{}{"Transactions": testTxns()}
	mx, _ := EvaluateTemplate("MAX_OF_FIELD(Transactions, 'amount')", payload)
	mn, _ := EvaluateTemplate("MIN_OF_FIELD(Transactions, 'amount')", payload)
	if mx != "50000" {
		t.Fatalf("expected max 50000, got '%s'", mx)
	}
	if mn != "3000" {
		t.Fatalf("expected min 3000, got '%s'", mn)
	}
}

func TestFilter(t *testing.T) {
	payload := map[string]interface{}{"Transactions": testTxns()}
	// FILTER stores result and returns count
	out, err := EvaluateTemplate("FILTER(Transactions, 'type', 'CREDIT')", payload)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if out != "2" {
		t.Fatalf("expected 2, got '%s'", out)
	}
	// The filtered list is now in payload under __filter_type_CREDIT
	filtered, ok := payload["__filter_type_CREDIT"]
	if !ok {
		t.Fatalf("expected __filter_type_CREDIT key in payload")
	}
	rows, ok2 := filtered.([]map[string]interface{})
	if !ok2 || len(rows) != 2 {
		t.Fatalf("expected 2 rows in filtered list")
	}
}

func TestOrderBy(t *testing.T) {
	payload := map[string]interface{}{"Transactions": testTxns()}
	_, err := EvaluateTemplate("ORDER_BY(Transactions, 'amount', 'DESC')", payload)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	key := "__ordered_Transactions_amount_DESC"
	sorted, ok := payload[key]
	if !ok {
		t.Fatalf("expected %s key in payload", key)
	}
	rows := sorted.([]map[string]interface{})
	if len(rows) != 4 {
		t.Fatalf("expected 4 sorted rows")
	}
	first := rows[0]["amount"].(float64)
	if first != 50000.0 {
		t.Fatalf("expected first row amount=50000, got %v", first)
	}
}

func TestGroupBy(t *testing.T) {
	payload := map[string]interface{}{"Transactions": testTxns()}
	out, err := EvaluateTemplate("GROUP_BY(Transactions, 'type')", payload)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if out != "2" {
		t.Fatalf("expected 2 groups, got '%s'", out)
	}
	key := "__grouped_Transactions_type"
	grouped, ok := payload[key]
	if !ok {
		t.Fatalf("expected %s key in payload", key)
	}
	rows := grouped.([]map[string]interface{})
	if len(rows) != 2 {
		t.Fatalf("expected 2 group entries")
	}
}

func TestTableHTML(t *testing.T) {
	payload := map[string]interface{}{"Transactions": testTxns()}
	out, err := EvaluateTemplate("TABLE_HTML(Transactions, 'tran_date', 'description', 'deposit_amount')", payload)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !strings.Contains(out, "<table") || !strings.Contains(out, "Salary") {
		t.Fatalf("expected html table with Salary row, got: %s", out)
	}
	if !strings.Contains(out, "Deposit") {
		t.Fatalf("expected 'Deposit' column header, got: %s", out)
	}
}

func TestRowsHTML(t *testing.T) {
	payload := map[string]interface{}{"Transactions": testTxns()}
	out, err := EvaluateTemplate("ROWS_HTML(Transactions, 'tran_date', 'description')", payload)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !strings.Contains(out, "<tr") || !strings.Contains(out, "Bonus") {
		t.Fatalf("expected tr rows with Bonus, got: %s", out)
	}
}

func TestSummaryTableHTML(t *testing.T) {
	payload := map[string]interface{}{"Transactions": testTxns()}
	// first group, then render summary
	EvaluateTemplate("GROUP_BY(Transactions, 'type')", payload)
	out, err := EvaluateTemplate("SUMMARY_TABLE_HTML(__grouped_Transactions_type)", payload)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !strings.Contains(out, "CREDIT") || !strings.Contains(out, "DEBIT") {
		t.Fatalf("expected CREDIT and DEBIT in summary table, got: %s", out)
	}
}

func TestKPICardsHTML(t *testing.T) {
	payload := map[string]interface{}{"Transactions": testTxns()}
	EvaluateTemplate("GROUP_BY(Transactions, 'type')", payload)
	out, err := EvaluateTemplate("KPI_CARDS_HTML(__grouped_Transactions_type)", payload)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !strings.Contains(out, "txns") {
		t.Fatalf("expected txns in kpi cards, got: %s", out)
	}
}

func TestFormatCurrency(t *testing.T) {
	payload := map[string]interface{}{"Amount": 1234567.89}
	out, err := EvaluateTemplate("FORMAT_CURRENCY(Amount)", payload)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if out != "₹ 1,234,567.89" {
		t.Fatalf("expected ₹ 1,234,567.89, got '%s'", out)
	}
}

func TestIFFunction(t *testing.T) {
	payload := map[string]interface{}{"IsApproved": true}
	out, err := EvaluateTemplate("IF(IsApproved, 'Approved', 'Pending')", payload)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if out != "Approved" {
		t.Fatalf("expected Approved, got '%s'", out)
	}
}

func TestBadgeHTML(t *testing.T) {
	payload := map[string]interface{}{"Status": "PENDING"}
	out, err := EvaluateTemplate("BADGE_HTML(Status, '#f6c23e')", payload)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !strings.Contains(out, "PENDING") || !strings.Contains(out, "#f6c23e") {
		t.Fatalf("expected badge with PENDING and yellow color, got: %s", out)
	}
}

// TestKPICardsHTML_NestedFuncInJSON tests that {{FUNC()}} inside a JSON string
// value inside KPI_CARDS_HTML is evaluated correctly.
// This reproduces the real-world bug where the EMAIL outbox showed unresolved
// {{COUNT_OF(BankStatementIDs)}} because findInnermostFunction was skipping
// inside double-quoted JSON strings.
func TestKPICardsHTML_NestedFuncInJSON(t *testing.T) {
	payload := map[string]interface{}{
		"BankStatementIDs": []interface{}{"BST-001"},
		"UserID":           "admin",
		"Action":           "REJECTED",
		"RecipientName":    "Hardik",
	}
	// Simulate the exact template stored in audit_template body_html
	tpl := `<p>Dear <strong>{{RecipientName}}</strong>,</p>` +
		`{{KPI_CARDS_HTML([{"label":"Rejected Count","value":"{{COUNT_OF(BankStatementIDs)}}"},{"label":"Rejected By","value":"{{UserID}}"},{"label":"Action","value":"{{Action}}"}])}}`

	out, err := EvaluateTemplate(tpl, payload)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	// RecipientName must be resolved
	if strings.Contains(out, "{{RecipientName}}") {
		t.Errorf("RecipientName was not resolved; got: %s", out)
	}
	if !strings.Contains(out, "Hardik") {
		t.Errorf("expected 'Hardik' in output; got: %s", out)
	}
	// KPI cards must be rendered (COUNT_OF → 1)
	if strings.Contains(out, "COUNT_OF") {
		t.Errorf("COUNT_OF was not resolved inside KPI JSON; got: %s", out)
	}
	if !strings.Contains(out, "1") {
		t.Errorf("expected count '1' in KPI card output; got: %s", out)
	}
	if !strings.Contains(out, "admin") {
		t.Errorf("expected UserID 'admin' in output; got: %s", out)
	}
	if !strings.Contains(out, "REJECTED") {
		t.Errorf("expected Action 'REJECTED' in output; got: %s", out)
	}
}

func TestHTMLAttributeQuotes_DoNotBreakVarResolution(t *testing.T) {
	// HTML attributes with single quotes must NOT prevent {{VarName}} resolution.
	// This was a bug where style='...' caused inSingleQuote=true for the rest of the body.
	payload := map[string]interface{}{
		"RecipientName": "Hardik",
		"UserID":        "admin",
		"Comment":       "ok",
	}
	tpl := `<div style='padding:24px'>` +
		`<p>Dear <strong>{{RecipientName}}</strong>,</p>` +
		`<tr><td style='border:1px solid #e5e7eb'>{{UserID}}</td></tr>` +
		`<tr><td>{{Comment}}</td></tr>` +
		`</div>`

	out, err := EvaluateTemplate(tpl, payload)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if strings.Contains(out, "{{RecipientName}}") || strings.Contains(out, "{{UserID}}") || strings.Contains(out, "{{Comment}}") {
		t.Errorf("variables not resolved in HTML with single-quote attributes; got: %s", out)
	}
	if !strings.Contains(out, "Hardik") || !strings.Contains(out, "admin") || !strings.Contains(out, "ok") {
		t.Errorf("expected resolved values in output; got: %s", out)
	}
}

func TestChainedFilterAndSum(t *testing.T) {
	// Real-world pattern: filter CREDIT txns then sum their deposit
	payload := map[string]interface{}{"Transactions": testTxns()}
	// Step 1: filter (stores result in payload, returns count)
	EvaluateTemplate("FILTER(Transactions, 'type', 'CREDIT')", payload)
	// Step 2: sum the filtered list
	out, err := EvaluateTemplate("FORMAT_CURRENCY(SUM_OF_FIELD(__filter_type_CREDIT, 'deposit_amount'))", payload)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if out != "₹ 60,000.00" {
		t.Fatalf("expected ₹ 60,000.00, got '%s'", out)
	}
}

// TestTableHTML_AliasedFormat verifies TABLE_HTML with JSON array column + alias args.
// This uses the calling convention:  TABLE_HTML(Items, ["col1","col2"], ["Alias 1","Alias 2"])
// splitArgs must NOT split on commas inside the [...] arrays.
func TestTableHTML_AliasedFormat(t *testing.T) {
	payload := map[string]interface{}{
		"Items": []map[string]interface{}{
			{"item_code": "INV-001", "amount": 5000.0, "due_date": "2024-01-15"},
			{"item_code": "INV-002", "amount": 12500.0, "due_date": "2024-01-20"},
		},
	}
	// Aliased format with JSON arrays
	tpl := `{{TABLE_HTML(Items,["item_code","amount","due_date"],["Invoice","Amount","Due Date"])}}`
	out, err := EvaluateTemplate(tpl, payload)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !strings.Contains(out, "<table") {
		t.Fatalf("expected html table, got: %s", out)
	}
	// Headers should be the aliases, not raw column names
	if !strings.Contains(out, "Invoice") {
		t.Fatalf("expected 'Invoice' alias header, got: %s", out)
	}
	if !strings.Contains(out, "Due&nbsp;Date") {
		t.Fatalf("expected 'Due Date' alias header (nbsp-escaped), got: %s", out)
	}
	// Column headers must NOT appear twice
	headerCount := strings.Count(out, "Invoice")
	if headerCount != 1 {
		t.Fatalf("expected 'Invoice' header exactly once, got %d times in: %s", headerCount, out)
	}
	// Data rows must be present
	if !strings.Contains(out, "INV-001") || !strings.Contains(out, "INV-002") {
		t.Fatalf("expected row data in table, got: %s", out)
	}
}

// TestSplitArgs_BracketDepth verifies that splitArgs does not split on commas inside [...].
func TestSplitArgs_BracketDepth(t *testing.T) {
	args := splitArgs(`Items, ["col1","col2"], ["Alias 1","Alias 2"]`)
	if len(args) != 3 {
		t.Fatalf("expected 3 args, got %d: %v", len(args), args)
	}
	if args[0] != "Items" {
		t.Errorf("args[0] want 'Items', got %q", args[0])
	}
	if args[1] != `["col1","col2"]` {
		t.Errorf("args[1] want '[\"col1\",\"col2\"]', got %q", args[1])
	}
	if args[2] != `["Alias 1","Alias 2"]` {
		t.Errorf("args[2] want '[\"Alias 1\",\"Alias 2\"]', got %q", args[2])
	}
}

