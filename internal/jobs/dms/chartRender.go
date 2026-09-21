package dmsjobs

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"image"
	"image/color"
	"image/draw"
	"image/png"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/golang/freetype"
	"github.com/golang/freetype/truetype"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/wcharczuk/go-chart/v2"
	"github.com/wcharczuk/go-chart/v2/drawing"
	"golang.org/x/image/math/fixed"
)

// msgNoData is the placeholder caption rendered into an empty chart PNG
// when there is nothing to plot.
const msgNoData = "No data"

type chartSeriesPoint struct {
	Label string
	Value float64
}

// aggregateChartSeries groups pool rows by dimension and sums measure.
// maxBars <= 0 keeps every category; a positive cap collapses the tail into
// "Other".
func aggregateChartSeries(rows []map[string]any, dimension, measure string, maxBars int) []chartSeriesPoint {
	dimension = strings.TrimSpace(dimension)
	measure = strings.TrimSpace(measure)
	if dimension == "" || len(rows) == 0 {
		return nil
	}
	sums := map[string]float64{}
	counts := map[string]float64{}
	order := make([]string, 0)
	measureNumeric := false
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
			f, ok := numericValue(lookupRowField(row, measure))
			if ok {
				measureNumeric = true
			}
			val = f
		}
		if _, ok := sums[label]; !ok {
			order = append(order, label)
		}
		sums[label] += val
		counts[label]++
	}
	// A measure that never resolves to a number (a text column, or one absent
	// from the source) would otherwise plot as a flat zero series and render as
	// "No data"; fall back to how many rows each category holds.
	picked := sums
	if measure != "" && !measureNumeric {
		picked = counts
	}
	out := make([]chartSeriesPoint, 0, len(order))
	for _, label := range order {
		out = append(out, chartSeriesPoint{Label: label, Value: picked[label]})
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

// numericValue is parseFloatLoose plus whether the value was a number at all,
// which separates a real zero from a field that carries no measurable value.
func numericValue(v any) (float64, bool) {
	if v == nil {
		return 0, false
	}
	switch t := v.(type) {
	case float64, float32, int, int32, int64, uint32, uint64:
		return parseFloatLoose(v), true
	case json.Number:
		f, err := t.Float64()
		return f, err == nil
	case pgtype.Numeric:
		return numericToFloat(t), t.Valid
	case *pgtype.Numeric:
		if t == nil {
			return 0, false
		}
		return numericToFloat(*t), t.Valid
	}
	s := strings.ReplaceAll(strings.TrimSpace(formatFieldValue(v)), ",", "")
	if s == "" || s == "-" || strings.EqualFold(s, "null") {
		return 0, false
	}
	f, err := strconv.ParseFloat(s, 64)
	return f, err == nil
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
		return renderEmptyChartPNG(msgNoData)
	}
	switch strings.ToLower(strings.TrimSpace(chartType)) {
	case "pie":
		return renderPieChartPNG(series)
	case "donut", "radial_bar":
		return renderDonutChartPNG(series)
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

// trimZeroDecimal renders one decimal place, dropping it when it is zero so
// axis ticks read "11000 Cr" rather than "11000.0 Cr".
func trimZeroDecimal(v float64) string {
	s := fmt.Sprintf("%.1f", v)
	return strings.TrimSuffix(s, ".0")
}

func formatAxisValue(v float64) string {
	abs := math.Abs(v)
	switch {
	case abs >= 10_000_000:
		return trimZeroDecimal(v/10_000_000) + " Cr"
	case abs >= 100_000:
		return trimZeroDecimal(v/100_000) + " L"
	case abs >= 1_000:
		return trimZeroDecimal(v/1_000) + " K"
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

const (
	catChartBaseWidth  = 760
	catChartBaseHeight = 470
	catChartMaxWidth   = 2400
	catChartGutter     = 170
	catChartMinPitch   = 13
)

// categoryChartSize widens the canvas so every category keeps a usable slice of
// the x-axis. go-chart silently clips whatever does not fit the image, so a
// fixed width would drop the tail categories instead of drawing them.
func categoryChartSize(n int) (width, height int) {
	width = catChartGutter + n*catChartMinPitch
	if width < catChartBaseWidth {
		width = catChartBaseWidth
	}
	if width > catChartMaxWidth {
		width = catChartMaxWidth
	}
	grow := math.Min(1.8, float64(width)/float64(catChartBaseWidth))
	return width, int(math.Round(float64(catChartBaseHeight) * grow))
}

// barChartBarMetrics picks an explicit bar width and spacing that fit the plot
// exactly. go-chart's own fit rounds every bar up, so the accumulated overflow
// pushes the last bars past the right edge of the image.
func barChartBarMetrics(n, width int) (barW, spacing int) {
	if n <= 0 {
		return 36, 16
	}
	pitch := (width - catChartGutter) / n
	if pitch < 3 {
		pitch = 3
	}
	gap := pitch / 5
	if gap < 1 {
		gap = 1
	}
	barW = pitch - gap
	if barW > 36 {
		barW = 36
	}
	if barW < 2 {
		barW = 2
	}
	return barW, pitch - barW
}

func renderBarChartPNG(series []chartSeriesPoint) ([]byte, error) {
	if chartSeriesAllZero(series) {
		return renderEmptyChartPNG(msgNoData)
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
			Style: chart.Style{FillColor: palette[i%len(palette)], StrokeColor: palette[i%len(palette)]},
		})
	}
	width, height := categoryChartSize(len(series))
	barW, barSpacing := barChartBarMetrics(len(series), width)
	graph := chart.BarChart{
		Title: " ",
		Background: chart.Style{
			Padding:   chart.Box{Top: 28, Left: 68, Right: 20, Bottom: 96},
			FillColor: drawing.ColorWhite,
		},
		Width:      width,
		Height:     height,
		BarWidth:   barW,
		BarSpacing: barSpacing,
		XAxis: chart.Style{
			Hidden: true,
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
				FontSize:    10,
				FontColor:   drawing.ColorFromHex("475569"),
				StrokeColor: drawing.ColorFromHex("94a3b8"),
				StrokeWidth: 1,
			},
			GridMajorStyle: chart.Style{
				StrokeColor: drawing.ColorFromHex("e2e8f0"),
				StrokeWidth: 1,
			},
		},
		Bars: bars,
	}
	var buf bytes.Buffer
	if err := graph.Render(chart.PNG, &buf); err != nil {
		return nil, err
	}
	return drawBarCategoryLabels(buf.Bytes(), graph, series)
}

// barChartGeometry mirrors go-chart's internal canvas maths so category labels
// can be painted at the exact centre of each bar.
func barChartGeometry(graph chart.BarChart) (left, bottom, barW, spacing int) {
	left = graph.Background.Padding.GetLeft(20)
	right := graph.GetWidth() - graph.Background.Padding.GetRight(10)
	bottom = graph.GetHeight() - graph.Background.Padding.GetBottom(50)

	canvasW := right - left
	n := len(graph.Bars)
	if n == 0 {
		return left, bottom, 0, 0
	}
	spacing = graph.GetBarSpacing()
	if n*(graph.GetBarWidth()+spacing) > canvasW {
		less := canvasW - (n * graph.GetBarWidth())
		if less > 0 {
			spacing = int(math.Ceil(float64(less) / float64(n)))
		} else {
			spacing = 0
		}
	}
	barW = graph.GetBarWidth()
	if n*(barW+spacing) > canvasW {
		less := canvasW - (n * spacing)
		if less > 0 {
			barW = int(math.Ceil(float64(less) / float64(n)))
		} else {
			barW = 0
		}
	}
	return left, bottom, barW, spacing
}

// detectBarBand locates the painted bars by their exact palette colours, so
// label placement follows what go-chart actually drew rather than a re-derived
// copy of its internal axis-fitting maths. Returns the first bar's left edge,
// the plot floor, and the bar pitch.
func detectBarBand(img *image.RGBA, palette []drawing.Color, n, minBarPx int) (firstLeft, floor, slot, barW int, ok bool) {
	if n <= 0 {
		return 0, 0, 0, 0, false
	}
	want := make(map[uint32]bool, len(palette))
	for _, c := range palette {
		want[uint32(c.R)<<16|uint32(c.G)<<8|uint32(c.B)] = true
	}
	b := img.Bounds()
	barKey := func(x, y int) (uint32, bool) {
		r, g, bb, a := img.At(x, y).RGBA()
		if a == 0 {
			return 0, false
		}
		k := uint32(r>>8)<<16 | uint32(g>>8)<<8 | uint32(bb>>8)
		return k, want[k]
	}
	// Runs on a row, keeping only those wide enough to be a bar rather than an
	// antialiased glyph edge from the y-axis tick labels. A run also ends where
	// the fill colour changes, so bars that touch once the pitch is tight still
	// count as one run each instead of merging into a single band.
	runs := func(y int) (starts, widths []int) {
		runStart, runKey := -1, uint32(0)
		flush := func(end int) {
			if runStart >= 0 && end-runStart >= minBarPx {
				starts = append(starts, runStart)
				widths = append(widths, end-runStart)
			}
			runStart = -1
		}
		for x := b.Min.X; x <= b.Max.X; x++ {
			k, isBar := uint32(0), false
			if x < b.Max.X {
				k, isBar = barKey(x, y)
			}
			switch {
			case !isBar:
				flush(x)
			case runStart < 0:
				runStart, runKey = x, k
			case k != runKey:
				flush(x)
				runStart, runKey = x, k
			}
		}
		return starts, widths
	}
	// Lowest row carrying at least the expected number of bar-width runs is the
	// plot floor; rows below it hold only axis text.
	floor = -1
	var starts, widths []int
	for y := b.Max.Y - 1; y >= b.Min.Y; y-- {
		st, wd := runs(y)
		if len(st) >= n {
			floor, starts, widths = y, st, wd
			break
		}
	}
	if floor < 0 {
		return 0, 0, 0, 0, false
	}
	firstLeft = starts[0]
	barW = widths[0]
	if len(starts) > 1 {
		slot = int(math.Round(float64(starts[len(starts)-1]-starts[0]) / float64(len(starts)-1)))
	} else {
		slot = barW
	}
	return firstLeft, floor, slot, barW, true
}

// drawBarCategoryLabels paints the dimension label under each bar. go-chart's
// own x-axis is unusable here: getAdjustedCanvasBox clamps the label band with
// MinInt against a 5px seed, so labels land outside the image and disappear.
func drawBarCategoryLabels(pngBytes []byte, graph chart.BarChart, series []chartSeriesPoint) ([]byte, error) {
	src, err := png.Decode(bytes.NewReader(pngBytes))
	if err != nil {
		return pngBytes, nil
	}
	canvas := image.NewRGBA(src.Bounds())
	draw.Draw(canvas, src.Bounds(), src, src.Bounds().Min, draw.Src)

	ttf, err := chart.GetDefaultFont()
	if err != nil {
		return pngBytes, nil
	}
	const fontPx = 9.0
	fc := freetype.NewContext()
	fc.SetDPI(92)
	fc.SetFont(ttf)
	fc.SetFontSize(fontPx)
	fc.SetClip(canvas.Bounds())
	fc.SetDst(canvas)
	fc.SetSrc(image.NewUniform(color.RGBA{R: 0x33, G: 0x41, B: 0x55, A: 0xff}))

	left, bottom, barW, spacing := barChartGeometry(graph)
	if barW <= 0 {
		return pngBytes, nil
	}
	slot := barW + spacing
	// go-chart grows the canvas to fit the y-axis, so the drawn bars sit well
	// right of the raw padding. Read their true positions off the rendered
	// image rather than re-deriving that adjustment.
	minBarPx := barW / 2
	if minBarPx < 4 {
		minBarPx = 4
	}
	if fl, fy, sl, bw, ok := detectBarBand(canvas, chartBrandPalette(), len(series), minBarPx); ok {
		left, bottom = fl, fy
		if sl > 0 {
			slot = sl
		}
		if bw > 0 {
			barW = bw
		}
	}
	// Widest label decides whether horizontal labels fit; if any would be
	// clipped, every label turns 90° so the full id is always readable.
	widest := 0
	for _, p := range series {
		if w := measureTextPx(fc, p.Label); w > widest {
			widest = w
		}
	}
	avail := canvas.Bounds().Dy() - bottom - 8

	if widest <= slot-4 {
		for i, p := range series {
			w := measureTextPx(fc, p.Label)
			tx := left + i*slot + (barW-w)/2
			if tx < 2 {
				tx = 2
			}
			fc.DrawString(p.Label, fixed.P(tx, bottom+14))
		}
	} else {
		rpx := rotatedLabelFontPx(slot)
		for i, p := range series {
			strip, sw, sh := renderTextStrip(ttf, rpx, fitLabelToPx(p.Label, rpx, avail),
				color.RGBA{R: 0x33, G: 0x41, B: 0x55, A: 0xff})
			if strip == nil {
				continue
			}
			// Rotated 90° CCW: strip width becomes vertical extent.
			dx := left + i*slot + (barW-sh)/2
			blitRotated90(canvas, strip, dx, bottom+6, sw, sh)
		}
	}

	var out bytes.Buffer
	if err := png.Encode(&out, canvas); err != nil {
		return pngBytes, nil
	}
	return out.Bytes(), nil
}

// rotatedLabelFontPx shrinks the category font until a 90°-turned label fits
// the per-category pitch, so removing the "Other" cap cannot make neighbouring
// labels collide.
func rotatedLabelFontPx(slot int) float64 {
	px := float64(slot) / 1.7
	if px > 9 {
		px = 9
	}
	if px < 5 {
		px = 5
	}
	return px
}

// fitLabelToPx ellipsises a label that is longer than the band reserved for it
// so it is shortened rather than dropped.
func fitLabelToPx(s string, fontPx float64, availPx int) string {
	n := int(float64(availPx-4) / (fontPx * 0.62))
	if n < 3 {
		n = 3
	}
	return truncateLabel(s, n)
}

func maxLabelChars(slot int, fontPx float64) int {
	// 0.58em is the average advance of the default sans face at these sizes;
	// leave 2px of gutter so neighbouring labels never touch.
	n := int(float64(slot-2) / (fontPx * 0.58))
	if n < 3 {
		return 3
	}
	return n
}

// renderTextStrip draws s horizontally onto a transparent image sized to the
// glyphs, returning the strip plus its used width and height. Rotating a
// finished strip keeps glyph rasterisation identical to the horizontal case.
func renderTextStrip(ttf *truetype.Font, fontPx float64, s string, col color.RGBA) (*image.RGBA, int, int) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, 0, 0
	}
	h := int(math.Ceil(fontPx * 1.6))
	w := int(math.Ceil(float64(len([]rune(s)))*fontPx)) + 16
	strip := image.NewRGBA(image.Rect(0, 0, w, h))

	fc := freetype.NewContext()
	fc.SetDPI(92)
	fc.SetFont(ttf)
	fc.SetFontSize(fontPx)
	fc.SetClip(strip.Bounds())
	fc.SetDst(strip)
	fc.SetSrc(image.NewUniform(col))
	baseline := int(math.Round(fontPx * 1.15))
	end, err := fc.DrawString(s, fixed.P(1, baseline))
	if err != nil {
		return nil, 0, 0
	}
	used := end.X.Round() + 1
	if used > w {
		used = w
	}
	return strip, used, h
}

// blitRotated90 copies the strip onto dst rotated 90° clockwise, so the text
// reads top-to-bottom starting at (dx, dy).
func blitRotated90(dst *image.RGBA, strip *image.RGBA, dx, dy, sw, sh int) {
	for sx := 0; sx < sw; sx++ {
		for sy := 0; sy < sh; sy++ {
			c := strip.RGBAAt(sx, sy)
			if c.A == 0 {
				continue
			}
			tx := dx + (sh - 1 - sy)
			ty := dy + sx
			if !(image.Point{tx, ty}.In(dst.Bounds())) {
				continue
			}
			dst.Set(tx, ty, c)
		}
	}
}

func measureTextPx(fc *freetype.Context, s string) int {
	p, err := fc.DrawString(s, fixed.P(0, -1000))
	if err != nil {
		return len(s) * 5
	}
	return p.X.Round()
}

// wrapLabelLines splits a category label into at most maxLines chunks of
// perLine runes, ellipsising whatever will not fit. Labels that carry a
// separator (FDBR-8A3ABE2, FREQ_721B589) break there first so the identifying
// tail survives instead of being chopped mid-token.
func wrapLabelLines(s string, perLine, maxLines int) []string {
	s = strings.TrimSpace(s)
	if s == "" || perLine <= 0 {
		return nil
	}
	if maxLines >= 2 && len([]rune(s)) > perLine {
		if i := strings.LastIndexAny(s, "-_ /"); i > 0 && i < len(s)-1 {
			head, tail := strings.TrimSpace(s[:i]), strings.TrimSpace(s[i+1:])
			if hr, tr := []rune(head), []rune(tail); len(hr) <= perLine && len(tr) <= perLine {
				return []string{head, tail}
			}
		}
	}
	r := []rune(s)
	var lines []string
	for len(r) > 0 && len(lines) < maxLines {
		n := perLine
		if n > len(r) {
			n = len(r)
		}
		if len(lines) == maxLines-1 && len(r) > n {
			if n > 1 {
				lines = append(lines, string(r[:n-1])+"…")
			} else {
				lines = append(lines, "…")
			}
			return lines
		}
		lines = append(lines, string(r[:n]))
		r = r[n:]
	}
	return lines
}

func renderLineChartPNG(series []chartSeriesPoint) ([]byte, error) {
	if len(series) == 0 || chartSeriesAllZero(series) {
		return renderEmptyChartPNG(msgNoData)
	}
	// go-chart ContinuousSeries needs a non-zero X span (≥2 points); single-point
	// series used to fail with "zero x-range delta" and kill BANK_BALANCE renders.
	if len(series) == 1 {
		series = []chartSeriesPoint{series[0], {Label: series[0].Label, Value: series[0].Value}}
	}
	xs := make([]float64, len(series))
	ys := make([]float64, len(series))
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
	}
	// Every point equal (one category, or a constant measure) gives a zero span,
	// which repeats one tick label down the whole axis; anchor it at zero so the
	// value can still be read off.
	if maxY <= minY {
		if maxY > 0 {
			minY = 0
		} else if maxY < 0 {
			maxY = 0
		} else {
			maxY = minY + 1
		}
	}
	pad := (maxY - minY) * 0.1
	width, height := categoryChartSize(len(series))
	graph := chart.Chart{
		Width:  width,
		Height: height,
		Background: chart.Style{
			Padding:   chart.Box{Top: 28, Left: 48, Right: 28, Bottom: 96},
			FillColor: drawing.ColorWhite,
		},
		XAxis: chart.XAxis{
			Style: chart.Style{Hidden: true},
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
					DotColor:    lineDotColor,
					DotWidth:    lineDotWidth(len(series)),
				},
			},
		},
	}
	graph.Elements = []chart.Renderable{chart.Legend(&graph)}
	var buf bytes.Buffer
	if err := graph.Render(chart.PNG, &buf); err != nil {
		return nil, err
	}
	return drawLineCategoryLabels(buf.Bytes(), graph, series)
}

// lineDotColor is unique to the plotted points — the legend sample and the line
// itself use the stroke colour — so the dots can be located by exact colour.
var lineDotColor = drawing.ColorFromHex("0b3d2e")

func lineDotWidth(n int) float64 {
	if n > 30 {
		return 3
	}
	return 5
}

// detectDotColumns returns the x centre of every run of dot-coloured pixels,
// left to right, which locates the points go-chart actually plotted.
func detectDotColumns(img *image.RGBA, dot drawing.Color) []int {
	b := img.Bounds()
	if b.Dx() <= 0 {
		return nil
	}
	hit := make([]bool, b.Dx())
	for y := b.Min.Y; y < b.Max.Y; y++ {
		for x := b.Min.X; x < b.Max.X; x++ {
			c := img.RGBAAt(x, y)
			if c.A == 0xff && c.R == dot.R && c.G == dot.G && c.B == dot.B {
				hit[x-b.Min.X] = true
			}
		}
	}
	var centers []int
	start := -1
	for i := 0; i <= len(hit); i++ {
		on := i < len(hit) && hit[i]
		if on && start < 0 {
			start = i
		} else if !on && start >= 0 {
			centers = append(centers, b.Min.X+(start+i-1)/2)
			start = -1
		}
	}
	return centers
}

// drawLineCategoryLabels paints each point's dimension label turned 90° under
// the plot. go-chart can only draw x-axis ticks horizontally, which runs long
// ids into each other, so the axis is hidden and the labels painted here.
func drawLineCategoryLabels(pngBytes []byte, graph chart.Chart, series []chartSeriesPoint) ([]byte, error) {
	src, err := png.Decode(bytes.NewReader(pngBytes))
	if err != nil {
		return pngBytes, nil
	}
	canvas := image.NewRGBA(src.Bounds())
	draw.Draw(canvas, src.Bounds(), src, src.Bounds().Min, draw.Src)

	ttf, err := chart.GetDefaultFont()
	if err != nil {
		return pngBytes, nil
	}
	centers := detectDotColumns(canvas, lineDotColor)
	if len(centers) < 2 || len(series) < 2 {
		return pngBytes, nil
	}
	// Dots can merge once the pitch is tighter than the dot itself, so the band
	// is rebuilt from its ends rather than trusting one run per point.
	first := centers[0]
	step := float64(centers[len(centers)-1]-first) / float64(len(series)-1)
	if step <= 0 {
		return pngBytes, nil
	}
	bottom := graph.GetHeight() - graph.Background.Padding.GetBottom(20)
	avail := canvas.Bounds().Dy() - bottom - 8
	if avail <= 8 {
		return pngBytes, nil
	}
	fontPx := rotatedLabelFontPx(int(step))
	col := color.RGBA{R: 0x33, G: 0x41, B: 0x55, A: 0xff}
	for i, p := range series {
		strip, sw, sh := renderTextStrip(ttf, fontPx, fitLabelToPx(p.Label, fontPx, avail), col)
		if strip == nil {
			continue
		}
		dx := first + int(math.Round(float64(i)*step)) - sh/2
		blitRotated90(canvas, strip, dx, bottom+6, sw, sh)
	}

	var out bytes.Buffer
	if err := png.Encode(&out, canvas); err != nil {
		return pngBytes, nil
	}
	return out.Bytes(), nil
}

// sliceChartValues builds pie/donut slices. Captions are drawn only on slices
// wide enough to hold them — thinner ones used to stack on top of each other at
// the centre and render as unreadable overlapping text. The legend table under
// the image still names and quantifies every series.
func sliceChartValues(series []chartSeriesPoint, percentOnly bool) ([]chart.Value, bool) {
	palette := chartBrandPalette()
	total := 0.0
	for _, p := range series {
		if p.Value > 0 {
			total += p.Value
		}
	}
	if total <= 0 {
		return nil, false
	}
	values := make([]chart.Value, 0, len(series))
	for i, p := range series {
		if p.Value <= 0 {
			continue
		}
		share := p.Value / total
		label := ""
		if share >= 0.06 {
			if percentOnly {
				label = fmt.Sprintf("%.1f%%", share*100)
			} else {
				label = fmt.Sprintf("%s · %.1f%%", truncateLabel(p.Label, 16), share*100)
			}
		}
		values = append(values, chart.Value{
			Value: p.Value,
			Label: label,
			Style: chart.Style{
				FillColor:   palette[i%len(palette)],
				StrokeColor: drawing.ColorWhite,
				StrokeWidth: 2,
				FontColor:   drawing.ColorWhite,
				FontSize:    10,
			},
		})
	}
	return values, len(values) > 0
}

// splitLoneSlice works around go-chart drawing a single-value pie or donut as a
// path it never fills, which comes out as a blank image. Two halves of the one
// colour paint the same full circle through the multi-slice path that does
// fill; their strokes match the fill so the seam stays invisible.
func splitLoneSlice(values []chart.Value) []chart.Value {
	if len(values) != 1 {
		return values
	}
	first := values[0]
	first.Value = values[0].Value / 2
	first.Style.StrokeColor = first.Style.FillColor
	second := first
	second.Label = ""
	return []chart.Value{first, second}
}

func renderPieChartPNG(series []chartSeriesPoint) ([]byte, error) {
	// Keep pie readable: top 8 + Other already applied by caller; ensure positive values.
	values, ok := sliceChartValues(series, false)
	if !ok {
		return renderEmptyChartPNG("No positive values")
	}
	pie := chart.PieChart{
		Width:  720,
		Height: 520,
		Values: splitLoneSlice(values),
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

func renderDonutChartPNG(series []chartSeriesPoint) ([]byte, error) {
	// go-chart puts donut captions just outside the ring, on the page ground, so
	// they must be dark and short; the legend table carries the series names.
	values, ok := sliceChartValues(series, true)
	if !ok {
		return renderEmptyChartPNG("No positive values")
	}
	for i := range values {
		values[i].Style.FontColor = drawing.ColorFromHex("0b3d2e")
	}
	donut := chart.DonutChart{
		Width:  720,
		Height: 520,
		Values: splitLoneSlice(values),
		Background: chart.Style{
			Padding:   chart.Box{Top: 30, Left: 60, Right: 60, Bottom: 30},
			FillColor: drawing.ColorWhite,
		},
	}
	var buf bytes.Buffer
	if err := donut.Render(chart.PNG, &buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func renderGaugeChartPNG(series []chartSeriesPoint) ([]byte, error) {
	// Gauge = primary value vs max of series (or sum). Drawn as a slim bar + label.
	if len(series) == 0 {
		return renderEmptyChartPNG(msgNoData)
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
