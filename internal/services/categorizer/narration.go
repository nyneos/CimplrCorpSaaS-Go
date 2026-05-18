package categorizer

import (
	"regexp"
	"strings"
	"unicode"
)

// IndianBankChannel is the payment channel detected from the narration prefix.
type IndianBankChannel string

const (
	ChannelNEFT     IndianBankChannel = "NEFT"
	ChannelRTGS     IndianBankChannel = "RTGS"
	ChannelIMPS     IndianBankChannel = "IMPS"
	ChannelUPI      IndianBankChannel = "UPI"
	ChannelNACH     IndianBankChannel = "NACH"
	ChannelACH      IndianBankChannel = "ACH"
	ChannelCheque   IndianBankChannel = "CHEQUE"
	ChannelInternal IndianBankChannel = "INTERNAL"
	ChannelUnknown  IndianBankChannel = "UNKNOWN"
)

// NarrationResult holds all derived fields from a raw bank narration.
type NarrationResult struct {
	Raw     string
	Clean   string            // prefix-stripped, ref-removed
	Stemmed string            // space-joined stemmed tokens; used for trigram similarity
	Channel IndianBankChannel // detected payment channel
	Ref     string            // extracted reference / UTR / invoice number
}

// ─────────────────────────────────────────────────────────────
// Compiled regexes
// ─────────────────────────────────────────────────────────────

var (
	// Strip leading payment-channel prefix and its separator
	channelPrefixRe = regexp.MustCompile(
		`(?i)^(NEFT|RTGS|IMPS|UPI|NACH|ACH|CLG|CHQ|TRF|FT|IFT|SWT|REV|INT)[\s/\-_:]+`,
	)

	// Labeled reference: "REF 123456789012" / "UTR:HDFC0012345" / "INV#123"
	labeledRefRe = regexp.MustCompile(
		`(?i)\b(INV|REF|TXN|UTR|RRN|CRN|NO|ID)[\/\s:#-]?([A-Z0-9]{8,})\b`,
	)

	// Any standalone long alphanumeric code (≥14 chars) — likely a bank reference
	longCodeRe = regexp.MustCompile(`\b[A-Z0-9]{14,}\b`)

	// Splits narration into tokens
	tokenSplitRe = regexp.MustCompile(`[^a-zA-Z0-9]+`)
)

// ─────────────────────────────────────────────────────────────
// Financial abbreviation map (Indian bank corpus)
// Key: uppercase abbreviation → lowercase expanded first-token
// ─────────────────────────────────────────────────────────────
var finAbbrev = map[string]string{
	"ELEC":   "electric",
	"ELECTR": "electric",
	"INSUR":  "insurance",
	"INS":    "insurance",
	"MAINT":  "maintenance",
	"MFEE":   "maintenance",
	"SAL":    "salary",
	"SALR":   "salary",
	"PYMNT":  "payment",
	"PYMT":   "payment",
	"PMT":    "payment",
	"RCVD":   "received",
	"RCV":    "received",
	"TFR":    "transfer",
	"XFER":   "transfer",
	"CHRG":   "charge",
	"CHG":    "charge",
	"INT":    "interest",
	"INTR":   "interest",
	"INTT":   "interest",
	"ADV":    "advance",
	"ADVNC":  "advance",
	"COMM":   "commission",
	"COMN":   "commission",
	"REFND":  "refund",
	"RFND":   "refund",
	"TRNSF":  "transfer",
	"BNKCHG": "charge",
	"MISC":   "miscellaneous",
	"MISCL":  "miscellaneous",
	"RENTL":  "rent",
	"UTIL":   "utility",
	"UTILS":  "utility",
	"TELCO":  "telecom",
	"TEL":    "telecom",
	"GOVT":   "government",
	"GVTX":   "government",
	"CORP":   "corporate",
	"GRP":    "group",
	"MGMT":   "management",
}

// stopWords for financial narrations — add no categorization signal
var stopWords = map[string]bool{
	"the": true, "a": true, "an": true, "of": true, "for": true,
	"in": true, "on": true, "at": true, "to": true, "by": true,
	"and": true, "or": true, "with": true, "from": true, "via": true,
	"dt": true, "date": true, "ref": true, "no": true, "id": true,
	"txn": true, "utr": true, "rrn": true, "crn": true, "inv": true,
	"ltd": true, "pvt": true, "llp": true, "inc": true, "co": true,
	"be": true, "is": true, "was": true, "are": true,
}

// stemSuffixes: simple Porter-style suffix stripping, longest first.
// Only strip if the remaining stem is ≥3 chars.
var stemSuffixes = []string{
	"ational", "tional", "ization", "isation",
	"ations", "nesses", "ments", "ings",
	"ation", "izing", "ising", "ness", "ment",
	"edly", "ying", "ers", "ies", "ied",
	"ing", "ful", "ous", "ive", "ize", "ise",
	"al", "er", "ed", "es", "s",
}

// ─────────────────────────────────────────────────────────────
// Public API
// ─────────────────────────────────────────────────────────────

// ProcessNarration cleans, normalises, and stems a raw bank narration.
// This is the single entry point for all narration pre-processing.
func ProcessNarration(raw string) NarrationResult {
	channel, afterPrefix := detectChannel(raw)
	ref := extractRef(raw)

	clean := afterPrefix

	// Recognise "BRN-{ref}:{description}" format common in SBI/Indian bank charge narrations.
	// Strip the "BRN-{ref}:" prefix so only the human-readable description body survives.
	if ref != "" {
		brnPrefix := "BRN-" + ref + ":"
		if strings.HasPrefix(strings.ToUpper(clean), strings.ToUpper(brnPrefix)) {
			clean = clean[len(brnPrefix):]
		}
	}

	// Remove ref from clean text to avoid polluting similarity tokens
	if ref != "" {
		clean = strings.ReplaceAll(clean, ref, " ")
	}
	// Trim separator artifacts left at the front after ref removal.
	// e.g. "/ foo bar" → "foo bar", " - WWS" → "WWS", ":CHRG" → "CHRG"
	clean = strings.TrimLeft(clean, "/\\-: ")
	// Collapse whitespace
	clean = strings.Join(strings.Fields(clean), " ")
	clean = strings.TrimSpace(clean)

	stemmed := stemNarration(clean)

	return NarrationResult{
		Raw:     raw,
		Clean:   clean,
		Stemmed: stemmed,
		Channel: channel,
		Ref:     ref,
	}
}

// ─────────────────────────────────────────────────────────────
// Internal helpers
// ─────────────────────────────────────────────────────────────

func detectChannel(raw string) (IndianBankChannel, string) {
	upper := strings.ToUpper(strings.TrimSpace(raw))
	for _, ch := range []IndianBankChannel{
		ChannelNEFT, ChannelRTGS, ChannelIMPS, ChannelUPI,
		ChannelNACH, ChannelACH,
	} {
		if strings.HasPrefix(upper, string(ch)) {
			stripped := channelPrefixRe.ReplaceAllString(raw, "")
			return ch, strings.TrimSpace(stripped)
		}
	}
	if strings.Contains(upper, "CHEQUE") || strings.Contains(upper, "/CHQ/") {
		return ChannelCheque, raw
	}
	if strings.Contains(upper, "INTERNAL") || strings.Contains(upper, "INT TRF") {
		return ChannelInternal, raw
	}
	return ChannelUnknown, raw
}

func extractRef(narration string) string {
	upper := strings.ToUpper(narration)
	if m := labeledRefRe.FindStringSubmatch(upper); len(m) >= 3 {
		return m[2]
	}
	if m := longCodeRe.FindString(upper); m != "" {
		return m
	}
	return ""
}

// stemNarration tokenises, expands abbreviations, stems, removes stop-words.
// Returns a space-joined string of processed tokens — used for pg_trgm similarity.
func stemNarration(clean string) string {
	tokens := tokenSplitRe.Split(strings.ToLower(clean), -1)
	out := make([]string, 0, len(tokens))
	for _, tok := range tokens {
		// Trim non-alphanumeric edges
		tok = strings.TrimFunc(tok, func(r rune) bool {
			return !unicode.IsLetter(r) && !unicode.IsDigit(r)
		})
		if len(tok) < 2 {
			continue
		}
		if stopWords[tok] {
			continue
		}
		// Check abbreviation map first (use uppercase key)
		if expanded, ok := finAbbrev[strings.ToUpper(tok)]; ok {
			tok = expanded
		}
		// Apply simple Porter-style stemming
		tok = porterStem(tok)
		if tok != "" {
			out = append(out, tok)
		}
	}
	return strings.Join(out, " ")
}

// porterStem applies simple English suffix stripping.
// For financial narrations, this gives better cluster grouping than raw tokens.
// Example: "payments" → "payment", "paying" → "pay", "electrical" → "electr"
func porterStem(word string) string {
	if len(word) <= 3 {
		return word
	}
	for _, suffix := range stemSuffixes {
		if strings.HasSuffix(word, suffix) {
			stem := word[:len(word)-len(suffix)]
			if len(stem) >= 3 {
				return stem
			}
		}
	}
	return word
}
