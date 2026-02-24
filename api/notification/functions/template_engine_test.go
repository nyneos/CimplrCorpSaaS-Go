package notification

import (
	"strings"
	"testing"
)

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
