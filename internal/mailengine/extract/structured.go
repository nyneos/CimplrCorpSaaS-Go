package extract

import (
	"fmt"
	"regexp"
	"strings"

	"CimplrCorpSaas/internal/mailengine/model"
)

var (
	reHTMLTable    = regexp.MustCompile(`(?is)<table[^>]*>(.*?)</table>`)
	reHTMLRow      = regexp.MustCompile(`(?is)<tr[^>]*>(.*?)</tr>`)
	reHTMLCell     = regexp.MustCompile(`(?is)<t[dh][^>]*>(.*?)</t[dh]>`)
	reHTMLTag      = regexp.MustCompile(`(?is)<[^>]+>`)
	reHTMLBreak    = regexp.MustCompile(`(?is)<br\s*/?>`)
	reKVLine       = regexp.MustCompile(`^\s*([A-Za-z][A-Za-z0-9 #/()._-]{0,60}?)\s*[:：\-–—]\s+(.+?)\s*$`)
	reBulletLine   = regexp.MustCompile(`^\s*[-•*]\s+(.+?)\s*$`)
	reNumberedLine = regexp.MustCompile(`^\s*\d+[.)]\s+(.+?)\s*$`)
)

// MaxExtractBodyRunes is the largest body processed as one complete unit.
// Larger bodies must be rejected before extraction; they are never truncated.
const MaxExtractBodyRunes = 512000

// MinExtractBodyRunes rejects empty or trivial bodies before extraction.
const MinExtractBodyRunes = 20

func extractionText(parsed model.ParsedEmail) string {
	text := strings.TrimSpace(parsed.Body.TextPlain)
	if text == "" {
		text = stripHTMLBasic(strings.TrimSpace(parsed.Body.TextHTML))
	}
	return text
}

// ValidateBody rejects unsupported lengths before extraction starts, preventing
// partial metadata from being returned or persisted.
func ValidateBody(parsed model.ParsedEmail) error {
	bodyRunes := len([]rune(extractionText(parsed)))
	if bodyRunes < MinExtractBodyRunes {
		return fmt.Errorf(
			"email body too short for extraction (%d chars, minimum %d)",
			bodyRunes,
			MinExtractBodyRunes,
		)
	}
	if bodyRunes > MaxExtractBodyRunes {
		return fmt.Errorf(
			"email body too large for complete extraction (%d chars, maximum %d)",
			bodyRunes,
			MaxExtractBodyRunes,
		)
	}
	return nil
}

// Run produces structured metadata from a parsed email.
// Output always includes intent + envelope fields; adds tables and key_values when detected.
func Run(module string, parsed model.ParsedEmail) (intent string, meta map[string]interface{}, confidence float64) {
	intent = intentForModule(module)
	text := extractionText(parsed)
	html := strings.TrimSpace(parsed.Body.TextHTML)

	bodyRunes := len([]rune(text))
	meta = map[string]interface{}{
		"intent":         intent,
		"subject":        parsed.Envelope.Subject,
		"from":           parsed.Envelope.From,
		"to":             parsed.Envelope.To,
		"date":           parsed.Envelope.Date,
		"body_char_len":  bodyRunes,
		"attachment_cnt": len(parsed.Attachments),
	}
	if bodyRunes < MinExtractBodyRunes {
		meta["extract_skipped"] = "body_too_short"
		return intent, meta, 0
	}
	preview := text
	if len(preview) > 2000 {
		preview = string([]rune(preview)[:2000])
	}
	meta["body_preview"] = preview

	tables := extractHTMLTables(html)
	if len(tables) == 0 && text != "" {
		tables = extractDelimitedTable(text)
	}
	if len(tables) > 0 {
		meta["tables"] = tables
	}

	kv := extractKeyValues(text)
	if len(kv) > 0 {
		meta["key_values"] = kv
	}

	ordered := extractOrderedList(text)
	if len(ordered) > 0 {
		meta["ordered_items"] = ordered
	}

	confidence = 0.55
	if len(tables) > 0 || len(kv) >= 3 {
		confidence = 0.75
	}
	if len(tables) > 0 && len(kv) >= 2 {
		confidence = 0.85
	}

	return intent, meta, confidence
}

func intentForModule(module string) string {
	switch strings.ToLower(strings.TrimSpace(module)) {
	case "fd-rate-negotiation":
		return "fd_rate_offer"
	case "bank-statement":
		return "bank_statement_notification"
	default:
		return "generic_email"
	}
}

func stripHTMLBasic(s string) string {
	s = reHTMLBreak.ReplaceAllString(s, "\n")
	s = reHTMLTag.ReplaceAllString(s, " ")
	s = strings.Join(strings.Fields(s), " ")
	return strings.TrimSpace(s)
}

func cellText(raw string) string {
	raw = reHTMLTag.ReplaceAllString(raw, " ")
	raw = strings.ReplaceAll(raw, "&nbsp;", " ")
	raw = strings.ReplaceAll(raw, "&amp;", "&")
	raw = strings.ReplaceAll(raw, "&lt;", "<")
	raw = strings.ReplaceAll(raw, "&gt;", ">")
	return strings.TrimSpace(strings.Join(strings.Fields(raw), " "))
}

// extractHTMLTables parses <table> blocks into row arrays or header-keyed objects.
func extractHTMLTables(html string) []map[string]interface{} {
	if html == "" {
		return nil
	}
	var out []map[string]interface{}
	for i, tableMatch := range reHTMLTable.FindAllStringSubmatch(html, -1) {
		rowsRaw := reHTMLRow.FindAllStringSubmatch(tableMatch[1], -1)
		if len(rowsRaw) == 0 {
			continue
		}
		var rows [][]string
		for _, rowMatch := range rowsRaw {
			cells := reHTMLCell.FindAllStringSubmatch(rowMatch[1], -1)
			if len(cells) == 0 {
				continue
			}
			var row []string
			for _, c := range cells {
				row = append(row, cellText(c[1]))
			}
			if hasNonEmpty(row) {
				rows = append(rows, row)
			}
		}
		if len(rows) == 0 {
			continue
		}
		entry := map[string]interface{}{
			"index": i,
			"rows":  rows,
		}
		if len(rows) >= 2 && looksLikeHeaderRow(rows[0]) {
			entry["records"] = rowsToRecords(rows[0], rows[1:])
			delete(entry, "rows")
		}
		out = append(out, entry)
	}
	return out
}

func looksLikeHeaderRow(cells []string) bool {
	if len(cells) < 2 {
		return false
	}
	nonNumeric := 0
	for _, c := range cells {
		c = strings.TrimSpace(c)
		if c == "" {
			continue
		}
		if !isMostlyNumeric(c) {
			nonNumeric++
		}
	}
	return nonNumeric >= len(cells)/2
}

func isMostlyNumeric(s string) bool {
	digits := 0
	for _, r := range s {
		if r >= '0' && r <= '9' {
			digits++
		}
	}
	return digits > len(s)/2
}

func rowsToRecords(header []string, data [][]string) []map[string]string {
	var records []map[string]string
	for _, row := range data {
		rec := map[string]string{}
		for i, h := range header {
			key := slugKey(h, i)
			if key == "" {
				continue
			}
			val := ""
			if i < len(row) {
				val = row[i]
			}
			if val != "" {
				rec[key] = val
			}
		}
		if len(rec) > 0 {
			records = append(records, rec)
		}
	}
	return records
}

func slugKey(h string, idx int) string {
	h = strings.TrimSpace(strings.ToLower(h))
	h = strings.ReplaceAll(h, " ", "_")
	h = regexp.MustCompile(`[^a-z0-9_]`).ReplaceAllString(h, "")
	if h == "" {
		return fmt.Sprintf("col_%d", idx)
	}
	return h
}

func hasNonEmpty(row []string) bool {
	for _, c := range row {
		if strings.TrimSpace(c) != "" {
			return true
		}
	}
	return false
}

// extractDelimitedTable handles pipe- or tab-separated text blocks.
func extractDelimitedTable(text string) []map[string]interface{} {
	lines := splitLines(text)
	var blocks [][][]string
	var current [][]string
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			if len(current) >= 2 {
				blocks = append(blocks, current)
			}
			current = nil
			continue
		}
		var parts []string
		if strings.Contains(line, "|") {
			for _, p := range strings.Split(line, "|") {
				p = strings.TrimSpace(p)
				if p != "" {
					parts = append(parts, p)
				}
			}
		} else if strings.Contains(line, "\t") {
			for _, p := range strings.Split(line, "\t") {
				p = strings.TrimSpace(p)
				if p != "" {
					parts = append(parts, p)
				}
			}
		}
		if len(parts) >= 2 {
			current = append(current, parts)
		}
	}
	if len(current) >= 2 {
		blocks = append(blocks, current)
	}

	var out []map[string]interface{}
	for i, block := range blocks {
		entry := map[string]interface{}{"index": i, "rows": block}
		if looksLikeHeaderRow(block[0]) {
			entry["records"] = rowsToRecords(block[0], block[1:])
			delete(entry, "rows")
		}
		out = append(out, entry)
	}
	return out
}

func extractKeyValues(text string) map[string]string {
	out := map[string]string{}
	for _, line := range splitLines(text) {
		line = strings.TrimSpace(line)
		if line == "" || len(line) > 300 {
			continue
		}
		if m := reKVLine.FindStringSubmatch(line); len(m) == 3 {
			key := slugKey(m[1], len(out))
			val := strings.TrimSpace(m[2])
			if key != "" && val != "" && !isNoiseKey(key) {
				out[key] = val
			}
		}
	}
	return out
}

func isNoiseKey(key string) bool {
	switch key {
	case "http", "https", "www", "com", "the", "and", "or":
		return true
	default:
		return false
	}
}

func extractOrderedList(text string) []string {
	var items []string
	for _, line := range splitLines(text) {
		line = strings.TrimSpace(line)
		if m := reBulletLine.FindStringSubmatch(line); len(m) == 2 {
			items = append(items, strings.TrimSpace(m[1]))
			continue
		}
		if m := reNumberedLine.FindStringSubmatch(line); len(m) == 2 {
			items = append(items, strings.TrimSpace(m[1]))
		}
	}
	return items
}

func splitLines(s string) []string {
	s = strings.ReplaceAll(s, "\r\n", "\n")
	s = strings.ReplaceAll(s, "\r", "\n")
	return strings.Split(s, "\n")
}
