package dmsjobs

import (
	"context"
	"fmt"
	htmlpkg "html"
	"regexp"
	"strings"

	dashboardbuilder "CimplrCorpSaas/api/dash/dashboardBuilder"

	"github.com/jackc/pgx/v5/pgxpool"
)

var chartDivRe = regexp.MustCompile(`(?is)<div\b([^>]*)\bdata-dms-chart(?:=["']([^"']*)["'])?([^>]*)>.*?</div>`)

// expandChartPlaceholders replaces data-dms-chart slots with a title, legend,
// and an embedded PNG chart built from the rule's filtered pool (or a named
// dashboard source).
func expandChartPlaceholders(
	ctx context.Context,
	pool *pgxpool.Pool,
	html string,
	headerRow map[string]any,
	genCtx attachmentGenCtx,
) (string, error) {
	if !strings.Contains(strings.ToLower(html), "data-dms-chart") {
		return html, nil
	}
	var expandErr error
	out := chartDivRe.ReplaceAllStringFunc(html, func(match string) string {
		if expandErr != nil {
			return match
		}
		sub := chartDivRe.FindStringSubmatch(match)
		attrs := ""
		if len(sub) >= 4 {
			attrs = strings.TrimSpace(sub[1] + " " + sub[2] + " " + sub[3])
		}
		chartType := attrFromBlob(attrs, "data-chart-type")
		if chartType == "" {
			chartType = "bar"
		}
		source := attrFromBlob(attrs, "data-data-source")
		dimension := attrFromBlob(attrs, "data-dimension")
		measure := attrFromBlob(attrs, "data-measure")
		label := attrFromBlob(attrs, "data-label")
		if label == "" {
			label = attrFromBlob(attrs, "data-dms-chart")
		}
		if label == "" {
			label = "Chart"
		}
		if dimension == "" {
			expandErr = fmt.Errorf("chart %q: missing dimension field", label)
			return match
		}

		rows := genCtx.PoolRows
		usePool := source == "" || strings.EqualFold(source, "pool") ||
			(genCtx.SourceKey != "" && strings.EqualFold(source, genCtx.SourceKey))
		// Named dashboard source — only replace pool when fetch returns rows.
		if (!usePool || len(rows) == 0) && pool != nil && source != "" && !strings.EqualFold(source, "pool") {
			fetched, _, err := dashboardbuilder.FetchSourceData(ctx, pool, genCtx.poolDataRequest(source))
			if err != nil {
				expandErr = fmt.Errorf("chart %q: fetch source %q: %w", label, source, err)
				return match
			}
			if len(fetched) > 0 {
				rows = fetched
			}
		}
		if len(rows) == 0 && headerRow != nil {
			rows = []map[string]any{headerRow}
		}

		series := aggregateChartSeries(rows, dimension, measure, 12)
		ct := strings.ToLower(strings.TrimSpace(chartType))
		if ct == "pie" || ct == "donut" || ct == "radial_bar" {
			series = aggregateChartSeries(rows, dimension, measure, 8)
		}
		if len(series) == 0 {
			var b strings.Builder
			b.WriteString(`<div class="dms-chart-block" style="margin:14px 0 22px;page-break-inside:avoid">`)
			b.WriteString(`<h3 style="margin:0 0 8px;font-size:14px;color:#0b3d2e">`)
			b.WriteString(htmlpkg.EscapeString(label))
			b.WriteString(`</h3><p><em>No data for this chart.</em></p></div>`)
			return b.String()
		}
		pngBytes, err := renderChartPNG(chartType, series)
		// Hard-fail: do not emit a "generated" PDF with broken chart placeholders.
		if err != nil {
			expandErr = fmt.Errorf("chart %q (%s): %w", label, chartType, err)
			return match
		}

		var b strings.Builder
		b.WriteString(`<div class="dms-chart-block" style="margin:14px 0 22px;page-break-inside:avoid">`)
		b.WriteString(`<h3 style="margin:0 0 8px;font-size:14px;color:#0b3d2e">`)
		b.WriteString(htmlpkg.EscapeString(label))
		b.WriteString(`</h3>`)
		b.WriteString(`<img class="dms-chart-image" style="max-width:100%;height:auto;border:1px solid #e2e8f0;border-radius:8px" alt="`)
		b.WriteString(htmlpkg.EscapeString(label))
		b.WriteString(`" src="`)
		b.WriteString(chartPNGDataURI(pngBytes))
		b.WriteString(`" />`)
		// Legend directly under the image (no page-break) so PDF keeps chart + key together.
		seriesTotal := 0.0
		for _, p := range series {
			if p.Value > 0 {
				seriesTotal += p.Value
			}
		}
		b.WriteString(`<table class="dms-table" style="width:100%;margin-top:8px;font-size:11px;border-collapse:collapse">`)
		b.WriteString(`<tr><th style="text-align:left;padding:4px 8px;background:#0b3d2e;color:#fff">Series</th>`)
		b.WriteString(`<th style="text-align:right;padding:4px 8px;background:#0b3d2e;color:#fff">Value</th>`)
		b.WriteString(`<th style="text-align:right;padding:4px 8px;background:#0b3d2e;color:#fff">Share</th></tr>`)
		for i, p := range series {
			if i >= 12 {
				b.WriteString(fmt.Sprintf(`<tr><td colspan="3" style="padding:4px 8px">+%d more</td></tr>`, len(series)-12))
				break
			}
			share := ""
			if seriesTotal > 0 && p.Value > 0 {
				share = fmt.Sprintf("%.1f%%", p.Value/seriesTotal*100)
			}
			b.WriteString(`<tr><td style="padding:4px 8px;border-bottom:1px solid #e2e8f0">`)
			b.WriteString(htmlpkg.EscapeString(p.Label))
			b.WriteString(`</td><td style="padding:4px 8px;text-align:right;border-bottom:1px solid #e2e8f0">`)
			b.WriteString(htmlpkg.EscapeString(formatFieldValue(p.Value)))
			b.WriteString(`</td><td style="padding:4px 8px;text-align:right;border-bottom:1px solid #e2e8f0">`)
			b.WriteString(share)
			b.WriteString(`</td></tr>`)
		}
		b.WriteString(`</table></div>`)
		return b.String()
	})
	return out, expandErr
}
