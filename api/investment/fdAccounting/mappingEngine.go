package fdAccounting

import (
	"context"
	"fmt"
	"math"
	"strings"
)

// AP-08 phase 3 — apply an ACTIVE GL mapping to a producer's figures instead of
// the built-in literals. Producers call LoadActiveMapping; a nil result means no
// mapping is configured and the caller keeps its own hardcoded lines.

// AmountSet carries the producer's figures. A mapping line picks one of them
// through its amount_basis.
type AmountSet struct {
	Full    float64
	TDS     float64
	Net     float64
	Penalty float64
}

// MappingLine is one configured line of an ACTIVE mapping.
type MappingLine struct {
	LineNumber   int
	Leg          string
	GLCode       string
	GLName       string
	AccountType  string
	AmountBasis  string
	CostCenter   string
	ProfitCenter string
	ProjectCode  string
	TaxCode      string
	Narration    string
}

// ResolvedMapping is the ACTIVE mapping for one (entity, bank, event) key.
type ResolvedMapping struct {
	MappingID         string
	MappingVersion    int
	RoundingDecimals  int
	RoundingMethod    string
	NarrationTemplate string
	Lines             []MappingLine
}

// GeneratedLine is a journal line built from a mapping line.
type GeneratedLine struct {
	LineNumber    int
	AccountNumber string
	AccountName   string
	AccountType   string
	Debit         float64
	Credit        float64
	Narration     string
	CostCenter    string
	ProfitCenter  string
	ProjectCode   string
	TaxCode       string
}

// Version stamps the journal's gl_mapping_version column.
func (m *ResolvedMapping) Version() string {
	return fmt.Sprintf("%s v%d", m.MappingID, m.MappingVersion)
}

// LoadActiveMapping returns the ACTIVE mapping for the key — bank-specific
// first, then entity-wide — or nil when none is configured.
func LoadActiveMapping(ctx context.Context, exec dbExec, entityID, bankID, eventType string) (*ResolvedMapping, error) {
	mappingID, err := ResolveActiveMapping(ctx, exec, entityID, bankID, eventType)
	if err != nil || mappingID == "" {
		return nil, err
	}
	m := &ResolvedMapping{MappingID: mappingID}
	if err := exec.QueryRow(ctx, `
		SELECT COALESCE(mapping_version,1), COALESCE(rounding_decimals,2),
		       COALESCE(rounding_method,'ROUND'), COALESCE(narration_template,'')
		FROM `+glMappingTable+` WHERE mapping_id = $1`, mappingID).
		Scan(&m.MappingVersion, &m.RoundingDecimals, &m.RoundingMethod, &m.NarrationTemplate); err != nil {
		return nil, err
	}
	rows, err := exec.Query(ctx, `
		SELECT line_number, COALESCE(leg,''), COALESCE(gl_account_code,''), COALESCE(gl_account_name,''),
		       COALESCE(account_type,''), COALESCE(amount_basis,'FULL_AMOUNT'), COALESCE(cost_center,''),
		       COALESCE(profit_center,''), COALESCE(project_code,''), COALESCE(tax_code,''), COALESCE(line_narration,'')
		FROM `+glMappingLineTable+` WHERE mapping_id = $1 ORDER BY line_number`, mappingID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var l MappingLine
		if err := rows.Scan(&l.LineNumber, &l.Leg, &l.GLCode, &l.GLName, &l.AccountType, &l.AmountBasis,
			&l.CostCenter, &l.ProfitCenter, &l.ProjectCode, &l.TaxCode, &l.Narration); err != nil {
			return nil, err
		}
		m.Lines = append(m.Lines, l)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(m.Lines) == 0 {
		return nil, nil
	}
	return m, nil
}

// MappedJournal is the outcome of applying a mapping to a producer's figures.
// Mapped is false when no ACTIVE mapping exists — the caller keeps its built-in
// lines. An unbalanced or empty mapping is an error: a journal that cannot
// balance must never be written.
type MappedJournal struct {
	Mapped  bool
	Lines   []GeneratedLine
	Debit   float64
	Credit  float64
	Version string
}

// BuildMappedJournal is the one call a producer needs: resolve the ACTIVE
// mapping for the key, build its lines from the given amounts, and verify the
// result balances.
func BuildMappedJournal(ctx context.Context, exec dbExec, entityID, bankID, eventType string,
	amounts AmountSet, narration string) (MappedJournal, error) {
	m, err := LoadActiveMapping(ctx, exec, entityID, bankID, eventType)
	if err != nil {
		return MappedJournal{}, fmt.Errorf("resolve gl mapping for %s: %w", eventType, err)
	}
	if m == nil {
		return MappedJournal{}, nil
	}
	lines, dr, cr := m.BuildLines(amounts, narration)
	if len(lines) == 0 {
		return MappedJournal{}, fmt.Errorf("GL mapping %s generates no journal lines; check the configured amount bases", m.Version())
	}
	if math.Abs(dr-cr) > 0.005 {
		return MappedJournal{}, fmt.Errorf("GL mapping %s generates an unbalanced journal: debit %.2f vs credit %.2f", m.Version(), dr, cr)
	}
	return MappedJournal{Mapped: true, Lines: lines, Debit: dr, Credit: cr, Version: m.Version()}, nil
}

// InsertLines writes the generated lines for an entry.
func (j MappedJournal) InsertLines(ctx context.Context, exec dbExec, entryID string) error {
	for _, l := range j.Lines {
		if _, err := exec.Exec(ctx, `
			INSERT INTO `+journalLineTable+` (entry_id, line_number, account_number, account_name, account_type, debit_amount, credit_amount, narration)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
			entryID, l.LineNumber, l.AccountNumber, l.AccountName, l.AccountType, l.Debit, l.Credit, l.Narration); err != nil {
			return err
		}
	}
	return nil
}

// RoundAmount applies the mapping's rounding method and precision.
func RoundAmount(v float64, decimals int, method string) float64 {
	if decimals < 0 {
		decimals = 2
	}
	f := math.Pow(10, float64(decimals))
	switch strings.ToUpper(strings.TrimSpace(method)) {
	case "FLOOR":
		return math.Floor(v*f) / f
	case "CEIL":
		return math.Ceil(v*f) / f
	default:
		return math.Round(v*f) / f
	}
}

// amountFor resolves a line's configured basis against the producer's figures.
func (m *ResolvedMapping) amountFor(basis string, a AmountSet) float64 {
	switch strings.ToUpper(strings.TrimSpace(basis)) {
	case "TDS_AMOUNT":
		return a.TDS
	case "NET_AMOUNT":
		return a.Net
	case "PENALTY_AMOUNT":
		return a.Penalty
	default:
		return a.Full
	}
}

// BuildLines turns the mapping into journal lines, one per mapping line, with
// each amount resolved by basis and rounded to the mapping's precision. Lines
// that resolve to zero are dropped — a TDS line on a receipt with no TDS should
// not post an empty row. Returns the lines plus the debit and credit totals.
func (m *ResolvedMapping) BuildLines(a AmountSet, narration string) ([]GeneratedLine, float64, float64) {
	out := make([]GeneratedLine, 0, len(m.Lines))
	var totalDr, totalCr float64
	n := 0
	for _, l := range m.Lines {
		amt := RoundAmount(m.amountFor(l.AmountBasis, a), m.RoundingDecimals, m.RoundingMethod)
		if amt == 0 {
			continue
		}
		n++
		g := GeneratedLine{
			LineNumber:    n,
			AccountNumber: l.GLCode,
			AccountName:   l.GLName,
			AccountType:   l.AccountType,
			Narration:     strings.TrimSpace(l.Narration + " " + narration),
			CostCenter:    l.CostCenter,
			ProfitCenter:  l.ProfitCenter,
			ProjectCode:   l.ProjectCode,
			TaxCode:       l.TaxCode,
		}
		if strings.EqualFold(strings.TrimSpace(l.Leg), "CREDIT") {
			g.Credit = amt
			totalCr += amt
		} else {
			g.Debit = amt
			totalDr += amt
		}
		out = append(out, g)
	}
	return out, totalDr, totalCr
}
