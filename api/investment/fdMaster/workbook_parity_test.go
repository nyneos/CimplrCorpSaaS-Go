// workbook_parity_test.go — validates the engine row-by-row against every
// scenario extracted from FD_Scenarios_v7.xlsx via gen_fixtures.py.
// Run:  go test ./api/investment/fdMaster/... -run TestWorkbookParity -v
package fdMaster

import (
	"fmt"
	"math"
	"strings"
	"testing"
	"time"
)

// ── Fixture types (populated by workbook_fixtures_gen.go) ─────────────────

// CanonicalScenario is one workbook sheet's worth of ground-truth data.
type CanonicalScenario struct {
	SheetName    string
	InterestType string // "SIMPLE" | "COMPOUND"
	CapFreq      string // "" | "Q" | "H" | "Y"
	PayoutFreq   string // "M" | "Q" | "H" | "Y" | "MAT"
	AccrualFreq  string // "M" | "Q" | "H" | "Y"
	ResetType    string // "AT_EACH_PAYOUT" | "AT_MATURITY"
	Start        time.Time
	Maturity     time.Time
	Principal    float64
	AnnualRate   float64 // 0.1 = 10 %
	TDSRate      float64 // 0.1 = 10 %
	DayCountConv string  // "Actual/365"
	ExpectedRows []ExpectedRow
}

// ExpectedRow is one workbook row (after stripping TDS_DEDUCTION rows which
// the workbook doesn't surface as separate lines).
type ExpectedRow struct {
	EventType  string
	EventDate  string
	ValueDate  string
	Principal  float64
	Gross      float64
	Tenor      int
	TDS        float64
	Net        float64
	AccrRevK   float64
	TDSRevL    float64
	DueNotAccr float64
	NetAmount  float64
}

// ── Engine param builder ──────────────────────────────────────────────────

func buildParamsFromScenario(sc CanonicalScenario) CashflowScheduleParams {
	fd := &FDRecord{
		ConfirmationID:  sc.SheetName,
		PrincipalAmount: sc.Principal,
		InterestRate:    sc.AnnualRate * 100, // engine uses percentage
		TenorDays:       int(sc.Maturity.Sub(sc.Start).Hours() / 24),
		ValueDate:       sc.Start,
		MaturityDate:    sc.Maturity,
		ResetType:       sc.ResetType,
	}

	if sc.InterestType == "COMPOUND" {
		fd.InterestTypeCode = "COMPOUND"
	} else {
		fd.InterestTypeCode = "SIMPLE"
	}

	cfg := &BankConfig{
		DayCountCode:             "DC-ACT-365",
		InterestRoundingDecimals: 0,
		RoundingMethod:           "ROUND",
		RoundingFrequency:        "EACH_PERIOD",
		WeekendAccrual:           true,
		HolidayAccrual:           true,
	}

	// Map payout/cap freq codes → FrequencyType
	codeToFT := func(code string) string {
		switch strings.ToUpper(code) {
		case "M":
			return "MONTHLY"
		case "Q":
			return "QUARTERLY"
		case "H":
			return "HALF_YEARLY"
		case "Y":
			return "YEARLY"
		case "MAT", "":
			return "AT_MATURITY"
		}
		return ""
	}

	// Compounding/cap frequency
	var freq *CompoundingFreq
	if sc.InterestType == "COMPOUND" {
		freq = &CompoundingFreq{FrequencyType: codeToFT(sc.CapFreq)}
	} else {
		freq = &CompoundingFreq{FrequencyType: codeToFT(sc.PayoutFreq)}
	}

	// Payout frequency override (CO only)
	var payoutFreqOverride *CompoundingFreq
	if sc.InterestType == "COMPOUND" {
		payoutFreqOverride = &CompoundingFreq{
			FrequencyID:   sc.PayoutFreq, // non-empty so override is honoured
			FrequencyType: codeToFT(sc.PayoutFreq),
		}
	}

	// TDS config — timing: RECEIPT for SI, "" (ACCRUAL) for CO
	tdsDeductionTiming := ""
	if sc.InterestType == "SIMPLE" {
		tdsDeductionTiming = "RECEIPT"
	}
	tds := &TDSConfig{
		TDSPlanID:       "wb-tds",
		TDSRate:         sc.TDSRate * 100, // engine uses percentage
		ThresholdAmount: 0,
		DeductionTiming: tdsDeductionTiming,
	}

	// Accrual frequency in months
	accrualFreqMonths := freqTypeToMonths(codeToFT(sc.AccrualFreq))
	if accrualFreqMonths <= 0 {
		accrualFreqMonths = 1
	}

	return CashflowScheduleParams{
		FD:                 fd,
		Cfg:                cfg,
		Freq:               freq,
		TDSCfg:             tds,
		DCInfo:             DayCountInfo{ConventionType: "ACT_365", DayCountCode: "DC-ACT-365"},
		CalInfo:            HolidayCalendarInfo{HolidayDates: make(map[string]bool)},
		PayoutFreqOverride: payoutFreqOverride,
		AccrualFreqMonths:  accrualFreqMonths,
	}
}

// ── Row-level field extractor matching workbook column layout ─────────────

// wbEventLabel maps engine EventType → workbook "event" column value.
func wbEventLabel(et string) string {
	switch et {
	case "INITIAL_INVESTMENT":
		return "INITIAL_INVESTMENT"
	case "ACCRUAL":
		return "ACCRUAL"
	case "INTEREST_RECEIPT":
		return "INTEREST_PAYOUT" // workbook calls it "Interest Payout"
	case "CAPITALIZATION":
		return "CAPITALIZATION"
	case "MATURITY":
		return "MATURITY"
	case "PRINCIPAL_RETURN":
		return "PRINCIPAL_RETURN"
	}
	return et
}

func r0(v float64) float64 { return math.Round(v) }
func dateS(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format("2006-01-02")
}

// engineRowToExpected converts one CashflowRow to an ExpectedRow so we can
// diff it against the workbook fixture.
func engineRowToExpected(r CashflowRow, isCompound bool) ExpectedRow {
	var gross, net float64
	var principal float64
	tenor := r.PeriodDays

	switch r.EventType {
	case "INITIAL_INVESTMENT":
		gross = -r.OpeningPrincipal
		if isCompound {
			net = r.ClosingPrincipal
		} else {
			net = r.NetCashFlow
		}
		tenor = 0
		principal = r.OpeningPrincipal

	case "PRINCIPAL_RETURN":
		gross = r.NetCashFlow
		net = r.NetCashFlow
		principal = r.OpeningPrincipal

	case "ACCRUAL":
		gross = r.InterestAccrued
		if isCompound {
			net = r.InterestAccrued - r.TDSAmount
		} else {
			net = r.InterestAccrued
		}
		principal = r.OpeningPrincipal

	case "CAPITALIZATION":
		gross = r.InterestAccrued
		net = r.ClosingPrincipal
		principal = r.OpeningPrincipal

	case "INTEREST_RECEIPT", "MATURITY":
		gross = r.InterestAccrued
		net = r.NetCashFlow
		if isCompound {
			principal = 0 // CO payout rows: principal is blank in workbook
		} else {
			principal = r.OpeningPrincipal
		}
	}

	return ExpectedRow{
		EventType:  wbEventLabel(r.EventType),
		EventDate:  dateS(r.EventDate),
		ValueDate:  dateS(r.ValueDate),
		Principal:  r0(principal),
		Gross:      r0(gross),
		Tenor:      tenor,
		TDS:        r0(r.TDSAmount),
		Net:        r0(net),
		AccrRevK:   r0(r.AccrRevK),
		TDSRevL:    r0(r.TDSRevL),
		DueNotAccr: r0(r.DueNotAccrued),
		NetAmount:  r0(r.NetAmount),
	}
}

// ── Comparison helpers ─────────────────────────────────────────────────────

const wbEpsilon = 0.5     // standard tolerance for most monetary fields
const wbPrincipalEps = 5.0 // wider tolerance for principal/closing-balance: Python
// float literals like 1.09202e+06 can lose up to ±5 vs the true integer value.

func wbFloatEq(want, got float64) bool {
	if want == 0 && got == 0 {
		return true
	}
	return math.Abs(want-got) < wbEpsilon
}

func compareRows(t *testing.T, scenario string, idx int, want ExpectedRow, got ExpectedRow) {
	t.Helper()
	fail := false

	// Use wider tolerance for principal/closing-balance: Python float literals
	// like 1.09202e+06 can be off by up to ±5 from the true integer value.
	checkF := func(field string, w, g float64, eps float64) {
		if !(math.Abs(w-g) < eps) {
			t.Errorf("%s row[%d] %s: want %.0f got %.0f  (event=%s date=%s)",
				scenario, idx, field, w, g, want.EventType, want.EventDate)
			fail = true
		}
	}
	check := func(field string, w, g float64) { checkF(field, w, g, wbEpsilon) }
	checkP := func(field string, w, g float64) { checkF(field, w, g, wbPrincipalEps) }
	checkS := func(field, w, g string) {
		if w != "" && w != g {
			t.Errorf("%s row[%d] %s: want %q got %q  (event=%s date=%s)",
				scenario, idx, field, w, g, want.EventType, want.EventDate)
			fail = true
		}
	}
	checkI := func(field string, w, g int) {
		if w != 0 && w != g {
			t.Errorf("%s row[%d] %s: want %d got %d  (event=%s date=%s)",
				scenario, idx, field, w, g, want.EventType, want.EventDate)
			fail = true
		}
	}

	checkS("event_type", want.EventType, got.EventType)
	checkS("event_date", want.EventDate, got.EventDate)
	checkS("value_date", want.ValueDate, got.ValueDate)
	checkP("principal", want.Principal, got.Principal) // wider: compound growing principal
	check("gross", want.Gross, got.Gross)
	checkI("tenor", want.Tenor, got.Tenor)
	check("tds", want.TDS, got.TDS)
	// net on CAPITALIZATION = closing principal (can have Python truncation error)
	if want.EventType == "CAPITALIZATION" {
		checkP("net", want.Net, got.Net)
	} else {
		check("net", want.Net, got.Net)
	}
	// Only check K/L/due_not_accr when the workbook has non-zero values
	if want.AccrRevK != 0 {
		check("accr_rev_k", want.AccrRevK, got.AccrRevK)
	}
	if want.TDSRevL != 0 {
		check("tds_rev_l", want.TDSRevL, got.TDSRevL)
	}
	if want.DueNotAccr != 0 {
		check("due_not_accr", want.DueNotAccr, got.DueNotAccr)
	}
	if want.NetAmount != 0 {
		checkP("net_amount", want.NetAmount, got.NetAmount) // wider: compound closing balance
	}
	_ = fail
}

// ── Main parity test ──────────────────────────────────────────────────────

func TestWorkbookParity(t *testing.T) {
	for _, sc := range CanonicalScenarios {
		sc := sc // capture
		t.Run(sc.SheetName, func(t *testing.T) {
			params := buildParamsFromScenario(sc)
			isCompound := sc.InterestType == "COMPOUND"

			got := generateCashflowSchedule(params)

			// Strip TDS_DEDUCTION rows — workbook doesn't show them separately
			var filtered []CashflowRow
			for _, r := range got {
				if r.EventType != "TDS_DEDUCTION" {
					filtered = append(filtered, r)
				}
			}

			if len(sc.ExpectedRows) != len(filtered) {
				t.Errorf("%s: row count mismatch: workbook=%d engine=%d",
					sc.SheetName, len(sc.ExpectedRows), len(filtered))
				t.Logf("workbook events: %v", wbEventList(sc.ExpectedRows))
				t.Logf("engine   events: %v", engEventList(filtered))
				// Fall through — compare whatever we can
			}

			limit := len(sc.ExpectedRows)
			if len(filtered) < limit {
				limit = len(filtered)
			}
			for i := 0; i < limit; i++ {
				got := engineRowToExpected(filtered[i], isCompound)
				compareRows(t, sc.SheetName, i, sc.ExpectedRows[i], got)
			}
		})
	}
}

func wbEventList(rows []ExpectedRow) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = fmt.Sprintf("%s(%s)", r.EventType, r.EventDate)
	}
	return out
}

func engEventList(rows []CashflowRow) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = fmt.Sprintf("%s(%s)", r.EventType, r.EventDate.Format("2006-01-02"))
	}
	return out
}
