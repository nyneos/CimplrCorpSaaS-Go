package dmsjobs

import (
	"fmt"
	htmlpkg "html"
	"regexp"
	"strconv"
	"strings"
)

// colorKPIDarkGreen is the default KPI card accent — the soft system dark green
// used both as the fallback accent and as the normalized replacement for loud accents.
const colorKPIDarkGreen = "#0b3d2e"

var kpiDivRe = regexp.MustCompile(`(?is)<div\b([^>]*)\bdata-dms-kpi(?:=["']([^"']*)["'])?([^>]*)>.*?</div>`)

// expandKPIPlaceholders replaces data-dms-kpi slots with a styled KPI card grid
// computed from the filtered pool (COUNT / SUM / AVG / MIN / MAX of a measure).
// Native DMS power — does NOT depend on notification EvaluateTemplate formulas.
func expandKPIPlaceholders(html string, poolRows []map[string]any) string {
	if !strings.Contains(strings.ToLower(html), "data-dms-kpi") {
		return html
	}
	return kpiDivRe.ReplaceAllStringFunc(html, func(match string) string {
		sub := kpiDivRe.FindStringSubmatch(match)
		attrs := ""
		if len(sub) >= 4 {
			attrs = strings.TrimSpace(sub[1] + " " + sub[2] + " " + sub[3])
		}
		op := strings.ToUpper(strings.TrimSpace(attrFromBlob(attrs, "data-op")))
		if op == "" {
			op = "COUNT"
		}
		measure := strings.TrimSpace(attrFromBlob(attrs, "data-measure"))
		label := strings.TrimSpace(attrFromBlob(attrs, "data-label"))
		if label == "" {
			label = op
			if measure != "" {
				label = op + " · " + labelizeCode(measure)
			}
		}
		accent := strings.TrimSpace(attrFromBlob(attrs, "data-accent"))
		if accent == "" {
			accent = colorKPIDarkGreen
		}
		val := computeKPI(poolRows, op, measure)
		// Soft system-green KPI tiles (not loud rainbow blocks).
		accent = normalizeKPIAccent(accent)
		return fmt.Sprintf(
			`<div class="dms-kpi-card" style="display:inline-block;min-width:148px;margin:6px 10px 6px 0;padding:12px 14px;border-radius:10px;background:#f0faf4;border:1px solid #b7e0c8;color:#0b3d2e;vertical-align:top">
  <div style="font-size:10px;letter-spacing:0.1em;text-transform:uppercase;color:%s;font-weight:700">%s</div>
  <div style="font-size:20px;font-weight:700;margin-top:4px;color:#0b3d2e">%s</div>
</div>`,
			htmlpkg.EscapeString(accent),
			htmlpkg.EscapeString(label),
			htmlpkg.EscapeString(formatCompactKPI(val)),
		)
	})
}

func computeKPI(rows []map[string]any, op, measure string) float64 {
	if len(rows) == 0 {
		return 0
	}
	switch op {
	case "COUNT":
		return float64(len(rows))
	case "SUM", "AVG", "MIN", "MAX":
		if measure == "" {
			return float64(len(rows))
		}
		n := 0.0
		sum := 0.0
		minV := 0.0
		maxV := 0.0
		first := true
		for _, row := range rows {
			if row == nil {
				continue
			}
			v := parseFloatLoose(row[measure])
			n++
			sum += v
			if first {
				minV, maxV, first = v, v, false
			} else {
				if v < minV {
					minV = v
				}
				if v > maxV {
					maxV = v
				}
			}
		}
		if n == 0 {
			return 0
		}
		switch op {
		case "SUM":
			return sum
		case "AVG":
			return sum / n
		case "MIN":
			return minV
		case "MAX":
			return maxV
		}
	}
	return float64(len(rows))
}

func labelizeCode(code string) string {
	parts := strings.Split(code, "_")
	for i, p := range parts {
		if p == "" {
			continue
		}
		parts[i] = strings.ToUpper(p[:1]) + p[1:]
	}
	return strings.Join(parts, " ")
}

// normalizeKPIAccent maps loud blues/oranges to soft system greens.
func normalizeKPIAccent(accent string) string {
	a := strings.ToLower(strings.TrimSpace(accent))
	switch a {
	case "", "#1d4ed8", "#2563eb", "#3b82f6", "#0000ff", "blue":
		return colorKPIDarkGreen
	case "#ea580c", "#f97316", "#fb923c", "orange":
		return "#0f766e"
	default:
		if a == "" {
			return colorKPIDarkGreen
		}
		return accent
	}
}

// kpiSheetTokenRe matches the compact sheet form the editor writes into cells:
// {{KPI:COUNT}} or {{KPI:SUM:principal_amount}}.
var kpiSheetTokenRe = regexp.MustCompile(`(?i)\{\{\s*KPI\s*:\s*([A-Z_]+)\s*(?::\s*([^}]+?)\s*)?\}\}`)

// expandKPISheetCells resolves KPI slots that live in spreadsheet cells — both
// the compact {{KPI:OP[:measure]}} token and the older data-dms-kpi card markup
// saved by earlier templates. The slot is replaced by the computed number at
// full precision, not the compact card form, because a sheet cell is meant to
// hold a usable value.
func expandKPISheetCells(rows [][]string, poolRows []map[string]any) [][]string {
	if len(rows) == 0 {
		return rows
	}
	for r, row := range rows {
		for c, cell := range row {
			lower := strings.ToLower(cell)
			hasToken := strings.Contains(lower, "{{kpi:")
			hasMarkup := strings.Contains(lower, "data-dms-kpi")
			if !hasToken && !hasMarkup {
				continue
			}
			out := cell
			if hasToken {
				out = kpiSheetTokenRe.ReplaceAllStringFunc(out, func(match string) string {
					sub := kpiSheetTokenRe.FindStringSubmatch(match)
					if len(sub) < 2 {
						return match
					}
					op := strings.ToUpper(strings.TrimSpace(sub[1]))
					measure := ""
					if len(sub) >= 3 {
						measure = strings.TrimSpace(sub[2])
					}
					return formatSheetKPI(computeKPI(poolRows, op, measure))
				})
			}
			if hasMarkup {
				out = kpiDivRe.ReplaceAllStringFunc(out, func(match string) string {
					sub := kpiDivRe.FindStringSubmatch(match)
					attrs := ""
					if len(sub) >= 4 {
						attrs = strings.TrimSpace(sub[1] + " " + sub[2] + " " + sub[3])
					}
					op := strings.ToUpper(strings.TrimSpace(attrFromBlob(attrs, "data-op")))
					if op == "" {
						op = "COUNT"
					}
					measure := strings.TrimSpace(attrFromBlob(attrs, "data-measure"))
					return formatSheetKPI(computeKPI(poolRows, op, measure))
				})
			}
			rows[r][c] = strings.TrimSpace(out)
		}
	}
	return rows
}

func formatSheetKPI(v float64) string {
	if v == float64(int64(v)) {
		return strconv.FormatInt(int64(v), 10)
	}
	return strconv.FormatFloat(v, 'f', 2, 64)
}

func formatCompactKPI(v float64) string {
	if v == 0 {
		return "0"
	}
	abs := v
	if abs < 0 {
		abs = -abs
	}
	switch {
	case abs >= 1_000_000:
		return fmt.Sprintf("%.2fM", v/1_000_000)
	case abs >= 10_000:
		return fmt.Sprintf("%.1fK", v/1_000)
	case abs >= 100:
		return fmt.Sprintf("%.2f", v)
	default:
		return fmt.Sprintf("%.4g", v)
	}
}
