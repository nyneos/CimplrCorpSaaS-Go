package dmsjobs

import (
	"archive/zip"
	"bytes"
	_ "embed"
	"fmt"
	htmlpkg "html"
	"regexp"
	"strings"

	"github.com/phpdave11/gofpdf"
	"github.com/xuri/excelize/v2"
)

//go:embed fonts/DejaVuSans.ttf
var dejavuSansTTF []byte

// Bold face: reuse the regular TTF bytes. gofpdf still accepts style "B" with the
// same file for glyph metrics; visual weight comes from larger heading sizes.

var (
	htmlTagRe     = regexp.MustCompile(`(?s)<[^>]+>`)
	htmlWSRe      = regexp.MustCompile(`[ \t\x0a\x0d]+`)
	htmlTableRe   = regexp.MustCompile(`(?is)<table\b[^>]*>.*?</table>`)
	htmlHeadingRe = regexp.MustCompile(`(?is)<h([1-6])\b[^>]*>(.*?)</h[1-6]>`)
	htmlParaRe    = regexp.MustCompile(`(?is)<p\b[^>]*>(.*?)</p>`)
	htmlChartRe   = regexp.MustCompile(`(?is)<div[^>]*data-dms-chart[^>]*>.*?</div>`)
	htmlBRRe      = regexp.MustCompile(`(?i)<br\s*/?>`)
)

// renderedFile is one generated attachment payload ready for S3.
type renderedFile struct {
	Bytes       []byte
	Ext         string // including leading dot, e.g. ".pdf"
	ContentType string
	Format      string // HTML | PDF | DOCX | XLSX
}

// renderMergedOutput turns merged HTML (+ optional spreadsheet tokens) into the
// requested output format.
func renderMergedOutput(format, mergedHTML string, mergeValues map[string]string, sheetTokens []string, kind string) (renderedFile, error) {
	format = strings.ToUpper(strings.TrimSpace(format))
	switch format {
	case "HTML", "":
		doc := wrapHTMLDocument(mergedHTML)
		return renderedFile{
			Bytes:       []byte(doc),
			Ext:         ".html",
			ContentType: "text/html; charset=utf-8",
			Format:      "HTML",
		}, nil
	case "PDF":
		b, err := renderPDF(mergedHTML)
		if err != nil {
			return renderedFile{}, err
		}
		return renderedFile{
			Bytes:       b,
			Ext:         ".pdf",
			ContentType: "application/pdf",
			Format:      "PDF",
		}, nil
	case "DOCX":
		b, err := renderDOCX(mergedHTML)
		if err != nil {
			return renderedFile{}, err
		}
		return renderedFile{
			Bytes:       b,
			Ext:         ".docx",
			ContentType: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
			Format:      "DOCX",
		}, nil
	case "XLSX":
		b, err := renderXLSX(mergedHTML, mergeValues, sheetTokens, kind)
		if err != nil {
			return renderedFile{}, err
		}
		return renderedFile{
			Bytes:       b,
			Ext:         ".xlsx",
			ContentType: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
			Format:      "XLSX",
		}, nil
	default:
		return renderedFile{}, fmt.Errorf("unsupported output format %q", format)
	}
}

type pdfBlockKind int

const (
	pdfBlockPara pdfBlockKind = iota
	pdfBlockHeading
	pdfBlockTable
)

type pdfBlock struct {
	kind    pdfBlockKind
	level   int // heading level 1-6
	text    string
	table   [][]string
}

// htmlToParagraphs converts TipTap/HTML body into plain paragraphs for DOCX/XLSX fallbacks.
func htmlToParagraphs(html string) []string {
	blocks := parseHTMLBlocks(html)
	out := make([]string, 0, len(blocks))
	for _, b := range blocks {
		switch b.kind {
		case pdfBlockHeading, pdfBlockPara:
			if t := strings.TrimSpace(b.text); t != "" {
				out = append(out, t)
			}
		case pdfBlockTable:
			for _, row := range b.table {
				out = append(out, strings.Join(row, "  ·  "))
			}
		}
	}
	return out
}

func parseHTMLBlocks(html string) []pdfBlock {
	html = strings.TrimSpace(html)
	if html == "" {
		return nil
	}
	// Drop chart placeholders — they are not rendered in PDF yet.
	html = htmlChartRe.ReplaceAllString(html, "")

	var blocks []pdfBlock
	rest := html
	for len(rest) > 0 {
		tableLoc := htmlTableRe.FindStringIndex(rest)
		headLoc := htmlHeadingRe.FindStringIndex(rest)
		paraLoc := htmlParaRe.FindStringIndex(rest)

		nextStart := -1
		kind := ""
		if tableLoc != nil && (nextStart < 0 || tableLoc[0] < nextStart) {
			nextStart = tableLoc[0]
			kind = "table"
		}
		if headLoc != nil && (nextStart < 0 || headLoc[0] < nextStart) {
			nextStart = headLoc[0]
			kind = "heading"
		}
		if paraLoc != nil && (nextStart < 0 || paraLoc[0] < nextStart) {
			nextStart = paraLoc[0]
			kind = "para"
		}
		if nextStart < 0 {
			// Trailing loose text
			if t := stripTagsToText(rest); t != "" {
				blocks = append(blocks, pdfBlock{kind: pdfBlockPara, text: t})
			}
			break
		}
		if nextStart > 0 {
			if t := stripTagsToText(rest[:nextStart]); t != "" {
				blocks = append(blocks, pdfBlock{kind: pdfBlockPara, text: t})
			}
		}
		switch kind {
		case "table":
			raw := rest[tableLoc[0]:tableLoc[1]]
			blocks = append(blocks, pdfBlock{kind: pdfBlockTable, table: parseHTMLTable(raw)})
			rest = rest[tableLoc[1]:]
		case "heading":
			m := htmlHeadingRe.FindStringSubmatch(rest[headLoc[0]:headLoc[1]])
			level := 2
			text := ""
			if len(m) >= 3 {
				fmt.Sscanf(m[1], "%d", &level)
				text = stripTagsToText(m[2])
			}
			blocks = append(blocks, pdfBlock{kind: pdfBlockHeading, level: level, text: text})
			rest = rest[headLoc[1]:]
		case "para":
			m := htmlParaRe.FindStringSubmatch(rest[paraLoc[0]:paraLoc[1]])
			text := ""
			if len(m) >= 2 {
				text = stripTagsToText(htmlBRRe.ReplaceAllString(m[1], "\n"))
			}
			// Collapse "Label | Label |" pipe rows into spaced lines
			text = normalizePipeLine(text)
			if text != "" {
				blocks = append(blocks, pdfBlock{kind: pdfBlockPara, text: text})
			}
			rest = rest[paraLoc[1]:]
		default:
			rest = rest[1:]
		}
	}
	return blocks
}

func parseHTMLTable(tableHTML string) [][]string {
	rowRe := regexp.MustCompile(`(?is)<tr\b[^>]*>(.*?)</tr>`)
	cellRe := regexp.MustCompile(`(?is)<t[hd]\b[^>]*>(.*?)</t[hd]>`)
	var rows [][]string
	for _, rm := range rowRe.FindAllStringSubmatch(tableHTML, -1) {
		if len(rm) < 2 {
			continue
		}
		var cells []string
		for _, cm := range cellRe.FindAllStringSubmatch(rm[1], -1) {
			if len(cm) < 2 {
				continue
			}
			cells = append(cells, stripTagsToText(cm[1]))
		}
		if len(cells) > 0 {
			rows = append(rows, cells)
		}
	}
	return rows
}

func stripTagsToText(s string) string {
	s = htmlBRRe.ReplaceAllString(s, "\n")
	s = htmlTagRe.ReplaceAllString(s, "")
	s = htmlpkg.UnescapeString(s)
	s = strings.ReplaceAll(s, "\u00a0", " ")
	// TipTap / markdown leftovers
	s = strings.TrimLeft(s, "> \t")
	s = htmlWSRe.ReplaceAllString(s, " ")
	return strings.TrimSpace(s)
}

func normalizePipeLine(s string) string {
	if !strings.Contains(s, "|") {
		return s
	}
	parts := strings.Split(s, "|")
	cleaned := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			cleaned = append(cleaned, p)
		}
	}
	return strings.Join(cleaned, "   ·   ")
}

func renderPDF(html string) ([]byte, error) {
	blocks := parseHTMLBlocks(html)
	if len(blocks) == 0 {
		blocks = []pdfBlock{{kind: pdfBlockPara, text: "(empty document)"}}
	}

	pdf := gofpdf.New("P", "mm", "A4", "")
	pdf.SetMargins(18, 18, 18)
	pdf.SetAutoPageBreak(true, 18)
	pdf.AddUTF8FontFromBytes("dejavu", "", dejavuSansTTF)
	pdf.AddUTF8FontFromBytes("dejavu", "B", dejavuSansTTF)
	pdf.AddPage()

	pageW, _ := pdf.GetPageSize()
	left, _, right, _ := pdf.GetMargins()
	contentW := pageW - left - right

	for _, b := range blocks {
		switch b.kind {
		case pdfBlockHeading:
			size := 16.0
			switch b.level {
			case 1:
				size = 18
			case 2:
				size = 14
			case 3:
				size = 12
			default:
				size = 11
			}
			pdf.SetFont("dejavu", "B", size)
			pdf.SetTextColor(20, 40, 80)
			pdf.MultiCell(contentW, size*0.55, b.text, "", "", false)
			pdf.Ln(4)
			pdf.SetTextColor(0, 0, 0)

		case pdfBlockTable:
			drawPDFTable(pdf, b.table, contentW)

		case pdfBlockPara:
			pdf.SetFont("dejavu", "", 11)
			for _, line := range strings.Split(b.text, "\n") {
				line = strings.TrimSpace(line)
				if line == "" {
					pdf.Ln(2)
					continue
				}
				pdf.MultiCell(contentW, 6, line, "", "", false)
				pdf.Ln(2)
			}
			pdf.Ln(2)

		}
	}
	if pdf.Error() != nil {
		return nil, pdf.Error()
	}
	var buf bytes.Buffer
	if err := pdf.Output(&buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func drawPDFTable(pdf *gofpdf.Fpdf, rows [][]string, contentW float64) {
	if len(rows) == 0 {
		return
	}
	cols := 0
	for _, r := range rows {
		if len(r) > cols {
			cols = len(r)
		}
	}
	if cols == 0 {
		return
	}
	colW := contentW / float64(cols)
	rowH := 8.0

	for i, row := range rows {
		// header row styling
		if i == 0 {
			pdf.SetFillColor(232, 238, 248)
			pdf.SetFont("dejavu", "B", 10)
		} else {
			pdf.SetFillColor(255, 255, 255)
			pdf.SetFont("dejavu", "", 10)
		}
		pdf.SetDrawColor(180, 190, 210)
		x := pdf.GetX()
		y := pdf.GetY()
		// page break if needed
		_, pageH := pdf.GetPageSize()
		_, _, _, bottom := pdf.GetMargins()
		if y+rowH > pageH-bottom {
			pdf.AddPage()
			y = pdf.GetY()
		}
		for c := 0; c < cols; c++ {
			cell := ""
			if c < len(row) {
				cell = row[c]
			}
			pdf.Rect(x+float64(c)*colW, y, colW, rowH, "FD")
			pdf.SetXY(x+float64(c)*colW+1.5, y+1.5)
			pdf.CellFormat(colW-3, rowH-3, cell, "", 0, "L", false, 0, "")
		}
		pdf.SetXY(x, y+rowH)
	}
	pdf.Ln(6)
}

func renderDOCX(html string) ([]byte, error) {
	blocks := parseHTMLBlocks(html)
	if len(blocks) == 0 {
		blocks = []pdfBlock{{kind: pdfBlockPara, text: "(empty document)"}}
	}

	var body strings.Builder
	body.WriteString(`<?xml version="1.0" encoding="UTF-8" standalone="yes"?>`)
	body.WriteString(`<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">`)
	body.WriteString(`<w:body>`)
	for _, b := range blocks {
		switch b.kind {
		case pdfBlockHeading:
			sz := 28 // half-points → 14pt
			switch b.level {
			case 1:
				sz = 36
			case 2:
				sz = 28
			case 3:
				sz = 24
			default:
				sz = 22
			}
			body.WriteString(`<w:p><w:pPr><w:spacing w:after="120"/></w:pPr>`)
			body.WriteString(`<w:r><w:rPr><w:b/><w:sz w:val="`)
			body.WriteString(fmt.Sprintf("%d", sz))
			body.WriteString(`"/><w:color w:val="142850"/></w:rPr><w:t xml:space="preserve">`)
			body.WriteString(xmlEscape(b.text))
			body.WriteString(`</w:t></w:r></w:p>`)
		case pdfBlockTable:
			writeDOCXTable(&body, b.table)
		case pdfBlockPara:
			for _, line := range strings.Split(b.text, "\n") {
				line = strings.TrimSpace(line)
				body.WriteString(`<w:p><w:pPr><w:spacing w:after="80"/></w:pPr><w:r><w:t xml:space="preserve">`)
				body.WriteString(xmlEscape(line))
				body.WriteString(`</w:t></w:r></w:p>`)
			}
		}
	}
	body.WriteString(`<w:sectPr><w:pgSz w:w="12240" w:h="15840"/><w:pgMar w:top="1440" w:right="1440" w:bottom="1440" w:left="1440"/></w:sectPr>`)
	body.WriteString(`</w:body></w:document>`)

	contentTypes := `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>
</Types>`

	rels := `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/>
</Relationships>`

	docRels := `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"/>`

	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	files := map[string]string{
		"[Content_Types].xml":          contentTypes,
		"_rels/.rels":                  rels,
		"word/document.xml":            body.String(),
		"word/_rels/document.xml.rels": docRels,
	}
	for name, content := range files {
		w, err := zw.Create(name)
		if err != nil {
			return nil, err
		}
		if _, err := w.Write([]byte(content)); err != nil {
			return nil, err
		}
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func renderXLSX(html string, mergeValues map[string]string, sheetTokens []string, kind string) ([]byte, error) {
	f := excelize.NewFile()
	defer func() { _ = f.Close() }()

	const sheet = "Sheet1"
	_ = f.SetSheetName("Sheet1", sheet)

	if strings.EqualFold(kind, "SPREADSHEET") && len(sheetTokens) > 0 {
		for i, token := range sheetTokens {
			col, err := excelize.ColumnNumberToName(i + 1)
			if err != nil {
				return nil, err
			}
			_ = f.SetCellValue(sheet, col+"1", token)
			_ = f.SetCellValue(sheet, col+"2", mergeValues[token])
		}
	} else {
		row := 1
		if len(mergeValues) > 0 {
			_ = f.SetCellValue(sheet, "A1", "Field")
			_ = f.SetCellValue(sheet, "B1", "Value")
			row = 2
			for k, v := range mergeValues {
				_ = f.SetCellValue(sheet, fmt.Sprintf("A%d", row), k)
				_ = f.SetCellValue(sheet, fmt.Sprintf("B%d", row), v)
				row++
			}
			row++
		}
		for _, b := range parseHTMLBlocks(html) {
			switch b.kind {
			case pdfBlockHeading, pdfBlockPara:
				if t := strings.TrimSpace(b.text); t != "" {
					_ = f.SetCellValue(sheet, fmt.Sprintf("A%d", row), t)
					row++
				}
			case pdfBlockTable:
				for _, tr := range b.table {
					for c, cell := range tr {
						col, err := excelize.ColumnNumberToName(c + 1)
						if err != nil {
							return nil, err
						}
						_ = f.SetCellValue(sheet, fmt.Sprintf("%s%d", col, row), cell)
					}
					row++
				}
				row++ // blank spacer after table
			}
		}
	}

	buf, err := f.WriteToBuffer()
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func writeDOCXTable(body *strings.Builder, rows [][]string) {
	if len(rows) == 0 {
		return
	}
	cols := 0
	for _, r := range rows {
		if len(r) > cols {
			cols = len(r)
		}
	}
	if cols == 0 {
		return
	}
	// ~6.5" usable width in twips (1440*6.5 ≈ 9360), split evenly.
	colW := 9360 / cols
	body.WriteString(`<w:tbl>`)
	body.WriteString(`<w:tblPr><w:tblW w:w="9360" w:type="dxa"/>`)
	body.WriteString(`<w:tblBorders>`)
	for _, edge := range []string{"top", "left", "bottom", "right", "insideH", "insideV"} {
		body.WriteString(`<w:`)
		body.WriteString(edge)
		body.WriteString(` w:val="single" w:sz="4" w:space="0" w:color="B4BED2"/>`)
	}
	body.WriteString(`</w:tblBorders></w:tblPr>`)
	body.WriteString(`<w:tblGrid>`)
	for c := 0; c < cols; c++ {
		body.WriteString(fmt.Sprintf(`<w:gridCol w:w="%d"/>`, colW))
	}
	body.WriteString(`</w:tblGrid>`)
	for i, row := range rows {
		body.WriteString(`<w:tr>`)
		for c := 0; c < cols; c++ {
			cell := ""
			if c < len(row) {
				cell = row[c]
			}
			fill := ""
			bold := ""
			if i == 0 {
				fill = `<w:shd w:val="clear" w:color="auto" w:fill="E8EEF8"/>`
				bold = `<w:b/>`
			}
			body.WriteString(`<w:tc><w:tcPr><w:tcW w:w="`)
			body.WriteString(fmt.Sprintf("%d", colW))
			body.WriteString(`" w:type="dxa"/>`)
			body.WriteString(fill)
			body.WriteString(`</w:tcPr><w:p><w:r><w:rPr>`)
			body.WriteString(bold)
			body.WriteString(`</w:rPr><w:t xml:space="preserve">`)
			body.WriteString(xmlEscape(cell))
			body.WriteString(`</w:t></w:r></w:p></w:tc>`)
		}
		body.WriteString(`</w:tr>`)
	}
	body.WriteString(`</w:tbl>`)
	body.WriteString(`<w:p><w:pPr><w:spacing w:after="160"/></w:pPr></w:p>`)
}

func xmlEscape(s string) string {
	r := strings.NewReplacer(
		"&", "&amp;",
		"<", "&lt;",
		">", "&gt;",
		`"`, "&quot;",
		"'", "&apos;",
	)
	return r.Replace(s)
}
