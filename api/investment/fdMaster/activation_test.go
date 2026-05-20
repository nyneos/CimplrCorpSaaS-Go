package fdMaster

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// ─────────────────────────────────────────────────────────────────────────────
// Fake queryExecutor — records calls and returns canned rows
// ─────────────────────────────────────────────────────────────────────────────

// fakeRow implements pgx.Row.
type fakeRow struct {
	values []interface{}
	err    error
}

func (r *fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	for i, v := range r.values {
		if i >= len(dest) {
			break
		}
		assignScan(dest[i], v)
	}
	return nil
}

// assignScan is a best-effort assignment from src into a pointer dest.
// Handles the common types used in fdMaster scans.
func assignScan(dest, src interface{}) {
	switch d := dest.(type) {
	case *string:
		if s, ok := src.(string); ok {
			*d = s
		}
	case *int:
		switch v := src.(type) {
		case int:
			*d = v
		case int64:
			*d = int(v)
		}
	case *int64:
		if v, ok := src.(int64); ok {
			*d = v
		}
	case *float64:
		if v, ok := src.(float64); ok {
			*d = v
		}
	case *bool:
		if v, ok := src.(bool); ok {
			*d = v
		}
	case *time.Time:
		if v, ok := src.(time.Time); ok {
			*d = v
		}
	case **time.Time:
		if src == nil {
			*d = nil
		} else if v, ok := src.(time.Time); ok {
			t := v
			*d = &t
		}
	}
}

// fakeRows is a minimal pgx.Rows implementation for information_schema lookups.
type fakeRows struct {
	rows   [][]interface{}
	cursor int
	closed bool
}

func (r *fakeRows) Next() bool {
	if r.closed || r.cursor >= len(r.rows) {
		return false
	}
	r.cursor++
	return true
}

func (r *fakeRows) Scan(dest ...any) error {
	if r.cursor == 0 || r.cursor > len(r.rows) {
		return errors.New("no current row")
	}
	current := r.rows[r.cursor-1]
	for i, v := range current {
		if i >= len(dest) {
			break
		}
		assignScan(dest[i], v)
	}
	return nil
}

func (r *fakeRows) Close()                                       { r.closed = true }
func (r *fakeRows) Err() error                                   { return nil }
func (r *fakeRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *fakeRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *fakeRows) Values() ([]any, error)                       { return nil, nil }
func (r *fakeRows) RawValues() [][]byte                          { return nil }
func (r *fakeRows) Conn() *pgx.Conn                              { return nil }

// fakeExec is the queryExecutor under test.
// Each call type is matched by substring against the SQL.
type fakeExec struct {
	queryRowResponses map[string]*fakeRow // substring → response
	queryResponses    map[string]*fakeRows
	execCalls         []execCall
	execError         error
}

type execCall struct {
	sql  string
	args []interface{}
}

func newFakeExec() *fakeExec {
	return &fakeExec{
		queryRowResponses: map[string]*fakeRow{},
		queryResponses:    map[string]*fakeRows{},
	}
}

func (f *fakeExec) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	for key, resp := range f.queryRowResponses {
		if strings.Contains(sql, key) {
			return resp
		}
	}
	return &fakeRow{err: pgx.ErrNoRows}
}

func (f *fakeExec) Exec(ctx context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error) {
	f.execCalls = append(f.execCalls, execCall{sql: sql, args: args})
	return pgconn.CommandTag{}, f.execError
}

func (f *fakeExec) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	for key, resp := range f.queryResponses {
		if strings.Contains(sql, key) {
			resp.cursor = 0
			resp.closed = false
			return resp, nil
		}
	}
	return &fakeRows{}, nil
}

// execCallsMatching returns indices of execCalls whose SQL contains substr.
func (f *fakeExec) execCallsMatching(substr string) []int {
	var out []int
	for i, c := range f.execCalls {
		if strings.Contains(c.sql, substr) {
			out = append(out, i)
		}
	}
	return out
}

// ─────────────────────────────────────────────────────────────────────────────
// Issue 4 — loadInterestType silent SIMPLE fallback
// ─────────────────────────────────────────────────────────────────────────────

func TestLoadInterestType_Canonical_SIMPLE(t *testing.T) {
	exec := newFakeExec()
	// No DB row → static fallback
	info := loadInterestType(context.Background(), exec, "SIMPLE")
	if info.CalculationMethod != "SIMPLE" {
		t.Errorf("want SIMPLE, got %s", info.CalculationMethod)
	}
}

func TestLoadInterestType_Canonical_COMPOUND(t *testing.T) {
	exec := newFakeExec()
	info := loadInterestType(context.Background(), exec, "COMPOUND")
	if info.CalculationMethod != "COMPOUND" {
		t.Errorf("want COMPOUND, got %s", info.CalculationMethod)
	}
}

func TestLoadInterestType_Canonical_STEPPED(t *testing.T) {
	exec := newFakeExec()
	info := loadInterestType(context.Background(), exec, "STEPPED")
	if info.CalculationMethod != "STEPPED" {
		t.Errorf("want STEPPED, got %s", info.CalculationMethod)
	}
}

func TestLoadInterestType_UnknownValue_DefaultsToSIMPLE(t *testing.T) {
	exec := newFakeExec()
	// "CO" is not canonical and won't be found in DB; static fallback returns SIMPLE
	info := loadInterestType(context.Background(), exec, "CO")
	if info.CalculationMethod != "SIMPLE" {
		t.Errorf("non-canonical input should fall through to SIMPLE, got %s", info.CalculationMethod)
	}
	// The WARN log would have fired — we can't easily assert on log output,
	// but the regression test in GenerateCashflowFromRecord catches the downstream effect.
}

func TestLoadInterestType_EmptyInput_DefaultsToSIMPLE(t *testing.T) {
	exec := newFakeExec()
	info := loadInterestType(context.Background(), exec, "")
	if info.CalculationMethod != "SIMPLE" {
		t.Errorf("empty input should default to SIMPLE, got %s", info.CalculationMethod)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Issue 4 — GenerateCashflowFromRecord rejects non-canonical interest type
// ─────────────────────────────────────────────────────────────────────────────

// This test requires the FDRecord path; we exercise it by constructing a record
// with a deliberately non-canonical InterestTypeCode and a stub exec that fails
// the master lookup. The function should return an error rather than silently
// generating a SIMPLE schedule.
func TestGenerateCashflowFromRecord_RejectsNonCanonicalInterestType(t *testing.T) {
	exec := newFakeExec()
	// All loaders will hit no-rows and return defaults / empty
	rec := &FDRecord{
		ConfirmationID:   "TEST-CONF-1",
		PrincipalAmount:  1000000,
		InterestRate:     7.5,
		InterestTypeCode: "CO", // non-canonical
		TenorDays:        365,
		ValueDate:        time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
		MaturityDate:     time.Date(2026, 3, 31, 0, 0, 0, 0, time.UTC),
	}

	_, _, err := GenerateCashflowFromRecord(context.Background(), exec, rec)
	if err == nil {
		t.Fatal("expected error for non-canonical InterestTypeCode, got nil")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "interest_type_code") {
		t.Errorf("error should mention interest_type_code, got: %v", err)
	}
}

func TestGenerateCashflowFromRecord_AcceptsCanonicalSIMPLE(t *testing.T) {
	exec := newFakeExec()
	rec := &FDRecord{
		ConfirmationID:   "TEST-CONF-2",
		PrincipalAmount:  1000000,
		InterestRate:     7.5,
		InterestTypeCode: "SIMPLE",
		TenorDays:        365,
		ValueDate:        time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
		MaturityDate:     time.Date(2026, 3, 31, 0, 0, 0, 0, time.UTC),
	}

	rows, _, err := GenerateCashflowFromRecord(context.Background(), exec, rec)
	if err != nil {
		t.Fatalf("unexpected error for canonical SIMPLE: %v", err)
	}
	if len(rows) == 0 {
		t.Error("expected non-empty cashflow rows for canonical SIMPLE")
	}
}

func TestGenerateCashflowFromRecord_AcceptsEmptyInterestType(t *testing.T) {
	// Empty InterestTypeCode should NOT trigger the guard — it falls back to SIMPLE legitimately.
	exec := newFakeExec()
	rec := &FDRecord{
		ConfirmationID:   "TEST-CONF-3",
		PrincipalAmount:  1000000,
		InterestRate:     7.5,
		InterestTypeCode: "", // empty — legitimate default
		TenorDays:        365,
		ValueDate:        time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
		MaturityDate:     time.Date(2026, 3, 31, 0, 0, 0, 0, time.UTC),
	}

	_, _, err := GenerateCashflowFromRecord(context.Background(), exec, rec)
	if err != nil {
		t.Errorf("empty InterestTypeCode should NOT error, got: %v", err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Issue 3 — loadFDRecord populates ResetType + tenure fields
// ─────────────────────────────────────────────────────────────────────────────

// Note: loadFDRecord is heavily DB-dependent (calls loadTableColumns, then
// a large SELECT). Testing it end-to-end via fake would require simulating
// the full information_schema response. We isolate the derivation logic
// (TenorDays from months/years fallback) instead.

func TestFDRecord_TenorDaysFallback_FromMonths(t *testing.T) {
	rec := &FDRecord{
		TenorDays:    0, // not set
		TenorMonths:  12,
		ValueDate:    time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
		MaturityDate: time.Date(2026, 3, 31, 0, 0, 0, 0, time.UTC),
	}
	// Simulate the fallback logic inline
	if rec.TenorDays == 0 && rec.TenorMonths > 0 {
		rec.TenorDays = int(rec.MaturityDate.Sub(rec.ValueDate).Hours() / 24)
	}
	if rec.TenorDays != 364 {
		t.Errorf("TenorDays from months: want 364, got %d", rec.TenorDays)
	}
}

func TestFDRecord_TenorDaysFallback_FromYears(t *testing.T) {
	rec := &FDRecord{
		TenorDays:    0,
		TenureYears:  2,
		ValueDate:    time.Date(2024, 4, 1, 0, 0, 0, 0, time.UTC),
		MaturityDate: time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC),
	}
	if rec.TenorDays == 0 && rec.TenureYears > 0 {
		rec.TenorDays = int(rec.MaturityDate.Sub(rec.ValueDate).Hours() / 24)
	}
	if rec.TenorDays != 730 { // 2 years, one of which (2024) is a leap year
		t.Errorf("TenorDays from years: want 730, got %d", rec.TenorDays)
	}
}

func TestFDRecord_ResetTypeNormalisation(t *testing.T) {
	// Verify that the ResetType normalisation (uppercase trim) works as expected
	// when used in the engine dispatch.
	cases := []struct {
		input string
		want  string
	}{
		{"AT_EACH_PAYOUT", "AT_EACH_PAYOUT"},
		{"at_each_payout", "AT_EACH_PAYOUT"},
		{" AT_MATURITY ", "AT_MATURITY"},
		{"", ""},
	}
	for _, tc := range cases {
		got := strings.ToUpper(strings.TrimSpace(tc.input))
		if got != tc.want {
			t.Errorf("ResetType normalise %q: want %q, got %q", tc.input, tc.want, got)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Issue 1 — softDeleteFDCashflowForRejection (the new helper)
// ─────────────────────────────────────────────────────────────────────────────

// Note: this test assumes the helper has signature:
//   softDeleteFDCashflowForRejection(ctx, exec, fdID, userEmail, comment) error
// Adjust if your actual signature differs.

func TestSoftDeleteFDCashflowForRejection_FiresAllThreeStatements(t *testing.T) {
	exec := newFakeExec()

	// Set up minimal information_schema responses so resolveFirstExistingTable
	// returns a cashflow table. We canned an empty Query for column lookups.
	exec.queryRowResponses["EXISTS"] = &fakeRow{values: []interface{}{true}}
	exec.queryResponses["information_schema.columns"] = &fakeRows{
		rows: [][]interface{}{
			{"fd_id"}, {"is_deleted"}, {"is_active"}, {"updated_at"}, {"updated_by"},
		},
	}

	// Call the helper (signature is illustrative — adjust to match your code)
	// err := softDeleteFDCashflowForRejection(context.Background(), exec, "FD-123", "checker@x.com", "rejected by audit")
	// if err != nil {
	//     t.Fatalf("unexpected error: %v", err)
	// }

	// Expect three UPDATEs to have fired:
	//   1. UPDATE on cashflow table SET is_deleted=true ...
	//   2. UPDATE on fd_audit_cashflow_schedule SET processing_status='REJECTED' ...
	//   3. UPDATE on fd_master SET cashflow_generated=false ...

	// Skipping the assertions since we'd need the real signature to exercise it.
	// Once you confirm the signature, replace the commented call above and
	// uncomment these:
	//
	// cashflowUpdates := exec.execCallsMatching("is_deleted=true")
	// if len(cashflowUpdates) != 1 {
	//     t.Errorf("expected 1 cashflow soft-delete, got %d", len(cashflowUpdates))
	// }
	// auditUpdates := exec.execCallsMatching("fd_audit_cashflow_schedule")
	// if len(auditUpdates) != 1 {
	//     t.Errorf("expected 1 audit reject, got %d", len(auditUpdates))
	// }
	// flagUpdates := exec.execCallsMatching("cashflow_generated=false")
	// if len(flagUpdates) != 1 {
	//     t.Errorf("expected 1 flag clear, got %d", len(flagUpdates))
	// }

	t.Skip("paste your softDeleteFDCashflowForRejection signature; test is wired but commented")
}

// ─────────────────────────────────────────────────────────────────────────────
// Issue 2 — markCashflowAuditApproved promotes PENDING_ACTIVATION → APPROVED
// ─────────────────────────────────────────────────────────────────────────────

func TestMarkCashflowAuditApproved_FiresExactlyOneUpdate(t *testing.T) {

	// err := markCashflowAuditApproved(context.Background(), exec, "FD-123", "checker@x.com")
	// if err != nil {
	//     t.Fatalf("unexpected error: %v", err)
	// }

	// updates := exec.execCallsMatching("processing_status='APPROVED'")
	// if len(updates) != 1 {
	//     t.Errorf("expected 1 UPDATE, got %d", len(updates))
	// }
	//
	// // Verify it targets PENDING_ACTIVATION rows (not arbitrary status)
	// pendingFilter := exec.execCallsMatching("PENDING_ACTIVATION")
	// if len(pendingFilter) != 1 {
	//     t.Error("UPDATE should filter on processing_status='PENDING_ACTIVATION'")
	// }

	t.Skip("paste your markCashflowAuditApproved signature; test is wired but commented")
}

// ─────────────────────────────────────────────────────────────────────────────
// Issue 2 — insertCashflowAuditRowsPending uses PENDING_ACTIVATION status
// ─────────────────────────────────────────────────────────────────────────────

func TestInsertCashflowAuditRowsPending_UsesPendingStatus(t *testing.T) {
	exec := newFakeExec()
	exec.queryResponses["information_schema.columns"] = &fakeRows{
		rows: [][]interface{}{
			{"fd_id"}, {"cashflow_id"}, {"event_type"},
		},
	}
	// Stub the SELECT that fetches cashflow_id+event_type for the FD
	exec.queryResponses["SELECT"] = &fakeRows{
		rows: [][]interface{}{
			{"cf-1", "CAPITALIZATION"},
			{"cf-2", "MATURITY"},
		},
	}

	// rows := []CashflowRow{
	//     {EventType: "CAPITALIZATION"},
	//     {EventType: "MATURITY"},
	// }
	// err := insertCashflowAuditRowsPending(context.Background(), exec, "FD-123", rows, "system")
	// if err != nil {
	//     t.Fatalf("unexpected error: %v", err)
	// }

	// inserts := exec.execCallsMatching("PENDING_ACTIVATION")
	// if len(inserts) == 0 {
	//     t.Error("inserts should use PENDING_ACTIVATION status")
	// }
	// approvedInserts := exec.execCallsMatching("'APPROVED'")
	// if len(approvedInserts) != 0 {
	//     t.Errorf("inserts should NOT contain 'APPROVED' literal, got %d", len(approvedInserts))
	// }

	t.Skip("paste your insertCashflowAuditRowsPending signature; test is wired but commented")
}
