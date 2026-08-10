package dmsjobs

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/wcharczuk/go-chart/v2"
	"github.com/wcharczuk/go-chart/v2/drawing"
)

type chartSeriesPoint struct {
	Label string
	Value float64
}

// aggregateChartSeries groups pool rows by dimension and sums measure.
// For pie/donut-style charts, collapses a long tail into "Other".
func aggregateChartSeries(rows []map[string]any, dimension, measure string, maxBars int) []chartSeriesPoint {
	dimension = strings.TrimSpace(dimension)
	measure = strings.TrimSpace(measure)
	if dimension == "" || len(rows) == 0 {
		return nil
	}
	sums := map[string]float64{}
	order := make([]string, 0)
	for _, row := range rows {
		if row == nil {
			continue
		}
		label := strings.TrimSpace(formatFieldValue(lookupRowField(row, dimension)))
		if label == "" {
			label = "(blank)"
		}
		val := 1.0
		if measure != "" {
			val = parseFloatLoose(lookupRowField(row, measure))
		}
		if _, ok := sums[label]; !ok {
			order = append(order, label)
		}
		sums[label] += val
	}
	out := make([]chartSeriesPoint, 0, len(order))
	for _, label := range order {
		out = append(out, chartSeriesPoint{Label: label, Value: sums[label]})
	}
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].Value > out[j].Value
	})
	if maxBars > 0 && len(out) > maxBars {
		other := 0.0
		for _, p := range out[maxBars:] {
			other += p.Value
		}
		out = out[:maxBars]
		if other > 0 {
			out = append(out, chartSeriesPoint{Label: "Other", Value: other})
		}
	}
	return out
}

func parseFloatLoose(v any) float64 {
	if v == nil {
		return 0
	}
	switch t := v.(type) {
	case float64:
		return t
	case float32:
		return float64(t)
	case int:
		return float64(t)
	case int32:
		return float64(t)
	case int64:
		return float64(t)
	case uint64:
		return float64(t)
	case uint32:
		return float64(t)
	case json.Number:
		f, _ := t.Float64()
		return f
	case pgtype.Numeric:
		return numericToFloat(t)
	case *pgtype.Numeric:
		if t == nil {
			return 0
		}
		return numericToFloat(*t)
	case []byte:
		s := strings.ReplaceAll(strings.TrimSpace(string(t)), ",", "")
		f, _ := strconv.ParseFloat(s, 64)
		return f
	case string:
		s := strings.ReplaceAll(strings.TrimSpace(t), ",", "")
		f, _ := strconv.ParseFloat(s, 64)
		return f
	default:
		s := strings.ReplaceAll(strings.TrimSpace(formatFieldValue(v)), ",", "")
		if s == "" || s == "<nil>" {
			return 0
		}
		f, err := strconv.ParseFloat(s, 64)
		if err == nil {
			return f
		}
		s2 := strings.ReplaceAll(strings.TrimSpace(fmt.Sprint(v)), ",", "")
		f2, _ := strconv.ParseFloat(s2, 64)
		return f2
	}
}

func numericToFloat(t pgtype.Numeric) float64 {
	if !t.Valid {
		return 0
	}
	f, err := t.Float64Value()
	if err != nil || !f.Valid {
		return 0
	}
	return f.Float64
}

// lookupRowField finds a map value by exact key, then case-insensitive match.
func lookupRowField(row map[string]any, key string) any {
	if row == nil || key == "" {
		return nil
	}
	if v, ok := row[key]; ok {
		return v
	}
	lk := strings.ToLower(strings.TrimSpace(key))
	for k, v := range row {
		if strings.ToLower(strings.TrimSpace(k)) == lk {
			return v
		}
	}
	return nil
}

func renderChartPNG(chartType string, series []chartSeriesPoint) ([]byte, error) {
	if len(series) == 0 {
		return renderEmptyChartPNG("No data")
	}
	switch strings.ToLower(strings.TrimSpace(chartType)) {
	case "pie", "donut", "radial_bar":
		return renderPieChartPNG(series)
	case "gauge":
		return renderGaugeChartPNG(series)
	case "line", "scatter":
		return renderLineChartPNG(series)
	case "double_bar", "stacked_bar":
		return renderBarChartPNG(series)
	default:
		return renderBarChartPNG(series)
	}
}

func chartBrandPalette() []drawing.Color {
	// Mint/teal/sky/amber/coral/violet — blends with Cimplr logo greens + blues.
	return []drawing.Color{
		drawing.ColorFromHex("0b3d2e"),
		drawing.ColorFromHex("0f766e"),
		drawing.ColorFromHex("1d4ed8"),
		drawing.ColorFromHex("f59e0b"),
		drawing.ColorFromHex("ef4444"),
		drawing.ColorFromHex("7c3aed"),
		drawing.ColorFromHex("06b6d4"),
		drawing.ColorFromHex("65a30d"),
		drawing.ColorFromHex("db2777"),
		drawing.ColorFromHex("475569"),
	}
}

func truncateLabel(s string, n int) string {
	s = strings.TrimSpace(s)
	if n <= 0 || len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}

func formatAxisValue(v float64) string {
	abs := math.Abs(v)
	switch {
	case abs >= 1_000_000_000:
		return fmt.Sprintf("%.1fB", v/1_000_000_000)
	case abs >= 1_000_000:
		return fmt.Sprintf("%.1fM", v/1_000_000)
	case abs >= 1_000:
		return fmt.Sprintf("%.1fK", v/1_000)
	case abs >= 100:
		return fmt.Sprintf("%.0f", v)
	default:
		return fmt.Sprintf("%.2f", v)
	}
}

func renderEmptyChartPNG(msg string) ([]byte, error) {
	graph := chart.BarChart{
		Title:      msg,
		TitleStyle: chart.Style{FontSize: 14, FontColor: drawing.ColorFromHex("64748b")},
		Width:      720,
		Height:     280,
		BarWidth:   40,
		Background: chart.Style{
			Padding:   chart.Box{Top: 40, Left: 20, Right: 20, Bottom: 20},
			FillColor: drawing.ColorFromHex("f8fafc"),
		},
		// go-chart BarChart needs a non-zero Y range (single tiny bar alone fails).
		YAxis: chart.YAxis{
			Range: &chart.ContinuousRange{Min: 0, Max: 1},
		},
		Bars: []chart.Value{{Value: 0.0001, Label: "—", Style: chart.Style{FillColor: drawing.ColorFromHex("e2e8f0")}}},
	}
	var buf bytes.Buffer
	if err := graph.Render(chart.PNG, &buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func chartSeriesAllZero(series []chartSeriesPoint) bool {
	for _, p := range series {
		if p.Value != 0 {
			return false
		}
	}
	return len(series) > 0
}

func chartYRange(series []chartSeriesPoint) *chart.ContinuousRange {
	maxV := 0.0
	for _, p := range series {
		if p.Value > maxV {
			maxV = p.Value
		}
		if -p.Value > maxV {
			maxV = -p.Value
		}
	}
	if maxV <= 0 {
		maxV = 1
	}
	// Pad so a single large bar (e.g. principal 123456789) still gets a valid range.
	return &chart.ContinuousRange{Min: 0, Max: maxV * 1.15}
}

func renderBarChartPNG(series []chartSeriesPoint) ([]byte, error) {
	if chartSeriesAllZero(series) {
		return renderEmptyChartPNG("No data")
	}
	palette := chartBrandPalette()
	bars := make([]chart.Value, 0, len(series))
	for i, p := range series {
		v := p.Value
		if v < 0 {
			v = 0 // bar chart Y range starts at 0
		}
		bars = append(bars, chart.Value{
			Value: v,
			Label: truncateLabel(p.Label, 18),
			Style: chart.Style{FillColor: palette[i%len(palette)], StrokeColor: palette[i%len(palette)]},
		})
	}
	graph := chart.BarChart{
		Title: " ",
		Background: chart.Style{
			Padding:   chart.Box{Top: 28, Left: 56, Right: 20, Bottom: 20},
			FillColor: drawing.ColorWhite,
		},
		Width:    760,
		Height:   380,
		BarWidth: 36,
		XAxis: chart.Style{
			FontSize:  9,
			FontColor: drawing.ColorFromHex("334155"),
		},
		YAxis: chart.YAxis{
			AxisType: chart.YAxisSecondary,
			Range:    chartYRange(series),
			ValueFormatter: func(v interface{}) string {
				if f, ok := v.(float64); ok {
					return formatAxisValue(f)
				}
				return fmt.Sprint(v)
			},
			Style: chart.Style{
				FontSize:  10,
				FontColor: drawing.ColorFromHex("475569"),
			},
		},
		Bars: bars,
	}
	var buf bytes.Buffer
	if err := graph.Render(chart.PNG, &buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func renderLineChartPNG(series []chartSeriesPoint) ([]byte, error) {
	if len(series) == 0 || chartSeriesAllZero(series) {
		return renderEmptyChartPNG("No data")
	}
	// go-chart ContinuousSeries needs a non-zero X span (≥2 points); single-point
	// series used to fail with "zero x-range delta" and kill BANK_BALANCE renders.
	if len(series) == 1 {
		series = []chartSeriesPoint{series[0], {Label: series[0].Label, Value: series[0].Value}}
	}
	xs := make([]float64, len(series))
	ys := make([]float64, len(series))
	ticks := make([]chart.Tick, 0, len(series))
	minY, maxY := series[0].Value, series[0].Value
	for i, p := range series {
		xs[i] = float64(i)
		ys[i] = p.Value
		if p.Value < minY {
			minY = p.Value
		}
		if p.Value > maxY {
			maxY = p.Value
		}
		ticks = append(ticks, chart.Tick{Value: float64(i), Label: truncateLabel(p.Label, 12)})
	}
	if maxY <= minY {
		maxY = minY + 1
	}
	pad := (maxY - minY) * 0.1
	graph := chart.Chart{
		Width:  760,
		Height: 380,
		Background: chart.Style{
			Padding:   chart.Box{Top: 28, Left: 48, Right: 28, Bottom: 20},
			FillColor: drawing.ColorWhite,
		},
		XAxis: chart.XAxis{
			Ticks: ticks,
			Style: chart.Style{FontSize: 9, FontColor: drawing.ColorFromHex("334155")},
			Range: &chart.ContinuousRange{Min: 0, Max: float64(len(series) - 1)},
		},
		YAxis: chart.YAxis{
			AxisType: chart.YAxisSecondary,
			Range:    &chart.ContinuousRange{Min: minY - pad, Max: maxY + pad},
			ValueFormatter: func(v interface{}) string {
				if f, ok := v.(float64); ok {
					return formatAxisValue(f)
				}
				return fmt.Sprint(v)
			},
			Style: chart.Style{FontSize: 10, FontColor: drawing.ColorFromHex("475569")},
		},
		Series: []chart.Series{
			chart.ContinuousSeries{
				Name:    "Value",
				XValues: xs,
				YValues: ys,
				Style: chart.Style{
					StrokeColor: drawing.ColorFromHex("0f766e"),
					StrokeWidth: 2.5,
					DotColor:    drawing.ColorFromHex("0b3d2e"),
					DotWidth:    5,
				},
			},
		},
	}
	graph.Elements = []chart.Renderable{chart.Legend(&graph)}
	var buf bytes.Buffer
	if err := graph.Render(chart.PNG, &buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func renderPieChartPNG(series []chartSeriesPoint) ([]byte, error) {
	// Keep pie readable: top 8 + Other already applied by caller; ensure positive values.
	palette := chartBrandPalette()
	values := make([]chart.Value, 0, len(series))
	for i, p := range series {
		if p.Value <= 0 {
			continue
		}
		values = append(values, chart.Value{
			Value: p.Value,
			Label: fmt.Sprintf("%s (%s)", truncateLabel(p.Label, 22), formatAxisValue(p.Value)),
			Style: chart.Style{FillColor: palette[i%len(palette)], StrokeColor: drawing.ColorWhite, StrokeWidth: 1},
		})
	}
	if len(values) == 0 {
		return renderEmptyChartPNG("No positive values")
	}
	pie := chart.PieChart{
		Width:  640,
		Height: 480,
		Values: values,
		Background: chart.Style{
			Padding:   chart.Box{Top: 20, Left: 20, Right: 20, Bottom: 20},
			FillColor: drawing.ColorWhite,
		},
	}
	var buf bytes.Buffer
	if err := pie.Render(chart.PNG, &buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func renderGaugeChartPNG(series []chartSeriesPoint) ([]byte, error) {
	// Gauge = primary value vs max of series (or sum). Drawn as a slim bar + label.
	if len(series) == 0 {
		return renderEmptyChartPNG("No data")
	}
	val := series[0].Value
	maxV := series[0].Value
	for _, p := range series {
		if p.Value > maxV {
			maxV = p.Value
		}
	}
	if maxV <= 0 {
		maxV = 1
	}
	pct := (val / maxV) * 100
	bars := []chart.Value{
		{Value: val, Label: truncateLabel(series[0].Label, 24) + " · " + formatAxisValue(val),
			Style: chart.Style{FillColor: drawing.ColorFromHex("0f766e")}},
		{Value: maxV - val, Label: "Remaining",
			Style: chart.Style{FillColor: drawing.ColorFromHex("d1fae5")}},
	}
	_ = pct
	graph := chart.BarChart{
		Title:      fmt.Sprintf("Gauge · %.0f%% of peak", pct),
		TitleStyle: chart.Style{FontSize: 13, FontColor: drawing.ColorFromHex("0b3d2e")},
		Width:      720,
		Height:     220,
		BarWidth:   80,
		Background: chart.Style{Padding: chart.Box{Top: 40, Left: 20, Right: 20, Bottom: 20}, FillColor: drawing.ColorWhite},
		YAxis: chart.YAxis{
			Range: &chart.ContinuousRange{Min: 0, Max: maxV * 1.15},
			ValueFormatter: func(v interface{}) string {
				if f, ok := v.(float64); ok {
					return formatAxisValue(f)
				}
				return fmt.Sprint(v)
			},
		},
		Bars: bars,
	}
	var buf bytes.Buffer
	if err := graph.Render(chart.PNG, &buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func chartPNGDataURI(pngBytes []byte) string {
	return "data:image/png;base64," + base64.StdEncoding.EncodeToString(pngBytes)
}
