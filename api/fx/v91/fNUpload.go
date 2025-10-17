package exposures

import (
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"math"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/transform"
)

type CanonicalRow struct {
	Source           string                   `json:"Source"`
	CompanyCode      string                   `json:"CompanyCode"`
	Party            string                   `json:"Party"`
	DocumentCurrency string                   `json:"DocumentCurrency"`
	DocumentNumber   string                   `json:"DocumentNumber"`
	DocumentDate     string                   `json:"DocumentDate"`
	PostingDate      string                   `json:"PostingDate"`
	NetDueDate       string                   `json:"NetDueDate"`
	AmountDoc        decimal.Decimal          `json:"AmountDoc"` // for final write + validation
	AmountFloat      float64                  `json:"-"`         // internal: hot-loop allocation
	LineItems        []map[string]interface{} `json:"LineItems,omitempty"`
	// Structured non-qualified metadata (preferred)
	IsNonQualified     bool   `json:"is_non_qualified,omitempty"`
	NonQualifiedReason string `json:"non_qualified_reason,omitempty"`

	// Backing raw mapped values (legacy/compat)
	_raw map[string]interface{}
}

type NonQualified struct {
	Row    CanonicalRow `json:"row"`
	Issues []string     `json:"issues"`
}

type UploadResult struct {
	FileName      string                `json:"file_name"`
	Source        string                `json:"source"`
	BatchID       uuid.UUID             `json:"batch_id"`
	TotalRows     int                   `json:"total_rows"`
	InsertedCount int                   `json:"inserted_count"`
	LineItemsRows int                   `json:"line_items_inserted"`
	NonQualified  []NonQualified        `json:"non_qualified"`
	Rows          []CanonicalPreviewRow `json:"rows"` // detailed per-row view
	Errors        []string              `json:"errors"`
	Warnings      []string              `json:"warnings,omitempty"`
}

type CanonicalPreviewRow struct {
	DocumentNumber string          `json:"document_number"`
	CompanyCode    string          `json:"company_code"`
	Party          string          `json:"party"`
	Currency       string          `json:"currency"`
	Source         string          `json:"source"`
	DocumentDate   string          `json:"document_date,omitempty"`
	PostingDate    string          `json:"posting_date,omitempty"`
	NetDueDate     string          `json:"net_due_date,omitempty"`
	Amount         decimal.Decimal `json:"amount"`
	Status         string          `json:"status"` // "ok", "non_qualified", "knocked_off"
	Issues         []string        `json:"issues,omitempty"`
	Knockoffs      []KnockoffInfo  `json:"knockoffs,omitempty"`
}

type KnockoffInfo struct {
	BaseDoc  string          `json:"base"`
	KnockDoc string          `json:"knock"`
	AmtAbs   decimal.Decimal `json:"amt_abs"`
}

type chanCopySource struct {
	ch   <-chan []any
	cur  []any
	err  error
	done bool
}

func (c *chanCopySource) Next() bool {
	if c.done {
		return false
	}
	row, ok := <-c.ch
	if !ok {
		c.done = true
		return false
	}
	c.cur = row
	return true
}

func (c *chanCopySource) Values() ([]any, error) {
	return c.cur, nil
}

func (c *chanCopySource) Err() error { return c.err }

type dateNormalizer struct {
	mu sync.Mutex
	m  map[string]string
}

func newDateNormalizer() *dateNormalizer {
	return &dateNormalizer{m: make(map[string]string)}
}

func (d *dateNormalizer) NormalizeCached(s string) string {
	if s == "" {
		return ""
	}
	d.mu.Lock()
	if v, ok := d.m[s]; ok {
		d.mu.Unlock()
		return v
	}
	d.mu.Unlock()
	n, _ := NormalizeDate(s)
	d.mu.Lock()
	d.m[s] = n
	d.mu.Unlock()
	return n
}

func BatchUploadStagingData(pool *pgxpool.Pool) http.HandlerFunc {
	runtime.GOMAXPROCS(runtime.NumCPU())

	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ctx := r.Context()

		if err := r.ParseMultipartForm(1024 << 20); err != nil {
			httpError(w, http.StatusBadRequest, "multipart parse error: "+err.Error())
			return
		}

		files := r.MultipartForm.File["files"]
		sources := r.MultipartForm.Value["source"]
		mappings := r.MultipartForm.Value["mapping"]
		// ----- Logic modes and currency aliases -----
		receivableLogic := strings.ToLower(strings.TrimSpace(r.FormValue("receivable_logic")))
		payableLogic := strings.ToLower(strings.TrimSpace(r.FormValue("payable_logic")))
		currencyAliasesJSON := r.FormValue("currency_aliases")

		currencyAliases := map[string]string{}
		if strings.TrimSpace(currencyAliasesJSON) != "" {
			if err := json.Unmarshal([]byte(currencyAliasesJSON), &currencyAliases); err != nil {
				log.Printf("[WARN] invalid currency_aliases JSON: %v", err)
			} else {
				// normalize keys and values to uppercase to make lookups case-insensitive
				norm := make(map[string]string, len(currencyAliases))
				for k, v := range currencyAliases {
					kk := strings.ToUpper(strings.TrimSpace(k))
					vv := strings.ToUpper(strings.TrimSpace(v))
					if kk == "" || vv == "" {
						continue
					}
					norm[kk] = vv
				}
				currencyAliases = norm
			}
		}

		// default fallbacks
		if receivableLogic == "" {
			receivableLogic = "standard"
		}
		if payableLogic == "" {
			payableLogic = "standard"
		}

		userID := r.FormValue("user_id")
		if userID == "" {
			userID = "1"
		}
		if len(files) == 0 {
			httpError(w, http.StatusBadRequest, "no files uploaded")
			return
		}

		entityMap := map[string]string{}
		{
			// rows, err := pool.Query(ctx, `SELECT unique_identifier, entity_name FROM public.masterentity WHERE is_deleted IS NOT TRUE`)
			// if err == nil {
			// 	for rows.Next() {
			// 		var uid, name string
			// 		if err := rows.Scan(&uid, &name); err == nil {
			// 			entityMap[strings.TrimSpace(uid)] = strings.TrimSpace(name)
			// 		}
			// 	}
			// 	rows.Close()
			// }
			rows, err := pool.Query(ctx, `
	SELECT COALESCE(NULLIF(unique_identifier,''), entity_id) AS uid,
	       TRIM(entity_name),
	       TRIM(COALESCE(company_name, entity_name, '')) AS cname,
	       TRIM(entity_id)
	FROM public.masterentity
	WHERE is_deleted IS NOT TRUE`)
			if err == nil {
				for rows.Next() {
					var uid, entityName, companyName, eid string
					if err := rows.Scan(&uid, &entityName, &companyName, &eid); err == nil {
						key1 := strings.TrimSpace(uid)
						key2 := strings.TrimSpace(companyName)
						key3 := strings.TrimSpace(eid)
						if key1 != "" {
							entityMap[key1] = entityName
						}
						if key2 != "" {
							entityMap[key2] = entityName
						}
						if key3 != "" {
							entityMap[key3] = entityName
						}
					}
				}
				rows.Close()
			}
		}

		results := make([]UploadResult, 0, len(files))

		for i, fh := range files {
			src := ""
			if i < len(sources) {
				src = strings.ToUpper(strings.TrimSpace(sources[i]))
			}
			var mappingRaw []byte
			if i < len(mappings) && strings.TrimSpace(mappings[i]) != "" {
				mappingRaw = []byte(mappings[i])
			}

			fileWarnings := make([]string, 0)
			fileErrors := make([]string, 0)

			f, err := fh.Open()
			if err != nil {
				httpError(w, http.StatusBadRequest, "open file: "+err.Error())
				return
			}
			tmpPath, fileHash, err := saveTempAndHash(f, fh.Filename)
			f.Close()
			if err != nil {
				httpError(w, http.StatusInternalServerError, "temp save: "+err.Error())
				return
			}
			defer os.Remove(tmpPath)

			tmpFile, err := os.Open(tmpPath)
			if err != nil {
				httpError(w, http.StatusInternalServerError, "open tmp: "+err.Error())
				return
			}
			utf8Reader := transform.NewReader(tmpFile, charmap.Windows1252.NewDecoder())
			csvR := csv.NewReader(utf8Reader)

			csvR.FieldsPerRecord = -1

			headersRec, err := csvR.Read()
			if err != nil {
				tmpFile.Close()
				httpError(w, http.StatusBadRequest, "csv read header: "+err.Error())
				return
			}
			headers := make([]string, len(headersRec))
			for idx, h := range headersRec {
				headers[idx] = strings.TrimSpace(h)
			}

			headerLower := map[string]string{}
			for _, h := range headers {
				headerLower[strings.ToLower(h)] = h
			}

			headerMap := map[string]string{}
			lineItemMap := map[string]string{}
			if len(mappingRaw) > 0 {
				var candidate map[string]interface{}
				if err := json.Unmarshal(mappingRaw, &candidate); err == nil {
					for k, v := range candidate {
						if strings.EqualFold(k, "LineItems") {
							if sub, ok := v.(map[string]interface{}); ok {
								for sk, sv := range sub {
									lineItemMap[sk] = fmt.Sprintf("%v", sv)
								}
							}
							continue
						}
						headerMap[k] = fmt.Sprintf("%v", v)
					}
				} else {
					var simple map[string]string
					_ = json.Unmarshal(mappingRaw, &simple)
					for k, v := range simple {
						headerMap[k] = v
					}
				}
			}

			batchID := uuid.New()
			conn, err := pool.Acquire(ctx)
			if err != nil {
				tmpFile.Close()
				httpError(w, 500, "db acquire: "+err.Error())
				return
			}
			tx, err := conn.Begin(ctx)
			if err != nil {
				conn.Release()
				tmpFile.Close()
				httpError(w, 500, "tx begin: "+err.Error())
				return
			}
			committed := false
			defer func() {
				if !committed {
					_ = tx.Rollback(ctx)
				}
				conn.Release()
				tmpFile.Close()
			}()

			if _, err := tx.Exec(ctx, `
				INSERT INTO public.staging_batches_exposures
				(batch_id, ingestion_source, status, total_records, file_hash, file_name, uploaded_by, mapping_json)
				VALUES ($1,$2,'processing',$3,$4,$5,$6,$7)
			`, batchID, src, 0, fileHash, fh.Filename, userID, string(mappingRaw)); err != nil {
				httpError(w, 500, "insert batch: "+err.Error())
				return
			}

			stagingCh := make(chan []any, 4096)
			stagingSrc := &chanCopySource{ch: stagingCh}
			var stagingErr error
			wgCopy := sync.WaitGroup{}
			wgCopy.Add(1)
			go func() {
				defer wgCopy.Done()
				_, stagingErr = tx.CopyFrom(ctx,
					pgx.Identifier{"public", "staging_exposures"},
					[]string{"staging_id", "batch_id", "exposure_source", "raw_payload", "mapped_payload", "ingestion_timestamp", "status"},
					stagingSrc)
			}()

			canonicals := make([]CanonicalRow, 0, 1024)
			totalRows := 0
			dn := newDateNormalizer()

			for {
				rec, err := csvR.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					close(stagingCh)
					wgCopy.Wait()
					httpError(w, http.StatusBadRequest, "csv parse row: "+err.Error())
					return
				}
				totalRows++

				rowMap := make(map[string]string, len(headers))
				for idx, h := range headers {
					val := ""
					if idx < len(rec) {
						val = strings.TrimSpace(rec[idx])
					}
					rowMap[h] = val
				}

				mapped := fastMapWithHeaderLower(rowMap, headerLower, headerMap)

				if len(lineItemMap) > 0 {
					li := make(map[string]interface{})
					for liTarget, csvHeader := range lineItemMap {
						v := ""
						if mv, ok := mapped[csvHeader]; ok {
							v = fmt.Sprintf("%v", mv)
						} else if rawV, ok := rowMap[csvHeader]; ok {
							v = rawV
						} else if csvHeaderLower, ok := headerLower[strings.ToLower(csvHeader)]; ok {
							v = rowMap[csvHeaderLower]
						}
						li[liTarget] = strings.TrimSpace(v)
					}
					mapped["LineItems"] = []map[string]interface{}{li}
				}

				rawB, _ := json.Marshal(map[string]string(rowMap))
				mappedB, _ := json.Marshal(mapped)

				stagingRow := []any{uuid.New(), batchID, src, rawB, mappedB, time.Now(), "pending"}
				select {
				case stagingCh <- stagingRow:
				default:
					stagingCh <- stagingRow
				}

				// c, _ := mapObjectToCanonical(mapped, src)
				c, _ := mapObjectToCanonical(mapped, src, currencyAliases)

				if s := fmt.Sprintf("%v", mapped["AmountDoc"]); strings.TrimSpace(s) != "" {
					s = strings.ReplaceAll(s, ",", "")
					if f, err := strconv.ParseFloat(s, 64); err == nil {
						c.AmountFloat = f
						c.AmountDoc = decimal.NewFromFloat(f)
					} else {
						if d, derr := decimal.NewFromString(s); derr == nil {
							c.AmountDoc = d
							f2, _ := d.Float64()
							c.AmountFloat = f2
						} else {
							c.AmountDoc = decimal.Zero
							c.AmountFloat = 0.0
						}
					}
				} else {
					c.AmountDoc = decimal.Zero
					c.AmountFloat = 0.0
				}

				// normalized date caching
				if c.DocumentDate != "" {
					c.DocumentDate = dn.NormalizeCached(c.DocumentDate)
				}
				if c.NetDueDate != "" {
					c.NetDueDate = dn.NormalizeCached(c.NetDueDate)
				}
				if c.PostingDate != "" {
					c.PostingDate = dn.NormalizeCached(c.PostingDate)
				}

				c._raw = mapped

				_, _ = validateSingleExposure(c)

				canonicals = append(canonicals, c)
			}

			close(stagingCh)
			wgCopy.Wait()
			if stagingErr != nil {
				httpError(w, http.StatusInternalServerError, "copy staging_exposures: "+stagingErr.Error())
				return
			}

			if _, err := tx.Exec(ctx, `UPDATE public.staging_batches_exposures SET total_records=$1 WHERE batch_id=$2`, totalRows, batchID); err != nil {
				httpError(w, 500, "update batch total_records: "+err.Error())
				return
			}

			// exposuresFloat, knocksFloat := allocateFIFOFloat(canonicals)
			exposuresFloat, knocksFloat := allocateFIFOFloat(canonicals, receivableLogic, payableLogic)

			// --- Apply Non-Qualified rules based on net exposure per (Source,CompanyCode,Party) ---
			// Rule: For vendors (FBL1N & FBL3N) if net > 0 => Non-Qualified
			//       For customers (FBL5N) if net < 0 => Non-Qualified
			netMap := make(map[string]float64)
			for _, e := range exposuresFloat {
				key := fmt.Sprintf("%s|%s|%s", e.Source, e.CompanyCode, e.Party)
				netMap[key] += e.AmountFloat
			}
			for i := range exposuresFloat {
				e := &exposuresFloat[i]
				key := fmt.Sprintf("%s|%s|%s", e.Source, e.CompanyCode, e.Party)
				net := netMap[key]
				switch e.Source {
				case "FBL1N", "FBL3N":
					if net > 0 {
						// set structured fields
						e.IsNonQualified = true
						e.NonQualifiedReason = fmt.Sprintf("Vendor net exposure %.4f > 0", net)
						// preserve legacy raw map keys for compatibility
						if e._raw == nil {
							e._raw = make(map[string]interface{})
						}
						e._raw["is_non_qualified"] = true
						e._raw["non_qualified_reason"] = e.NonQualifiedReason
					}
				case "FBL5N":
					if net < 0 {
						// set structured fields
						e.IsNonQualified = true
						e.NonQualifiedReason = fmt.Sprintf("Customer net exposure %.4f < 0", net)
						// preserve legacy raw map keys for compatibility
						if e._raw == nil {
							e._raw = make(map[string]interface{})
						}
						e._raw["is_non_qualified"] = true
						e._raw["non_qualified_reason"] = e.NonQualifiedReason
					}
				}
			}

			exposures := make([]CanonicalRow, 0, len(exposuresFloat))
			for _, e := range exposuresFloat {
				if e.AmountFloat == 0 {
					e.AmountDoc = decimal.Zero
				} else {
					efmt := strconv.FormatFloat(e.AmountFloat, 'f', 4, 64)
					if d, derr := decimal.NewFromString(efmt); derr == nil {
						e.AmountDoc = d
					} else {
						e.AmountDoc = decimal.NewFromFloat(e.AmountFloat)
					}
				}
				exposures = append(exposures, e)
			}

			knockMap := map[string][]KnockoffInfo{}
			for _, kf := range knocksFloat {
				afmt := strconv.FormatFloat(kf.AmtFloat, 'f', 4, 64)
				d, _ := decimal.NewFromString(afmt)
				k := KnockoffInfo{
					BaseDoc:  kf.BaseDoc,
					KnockDoc: kf.KnockDoc,
					AmtAbs:   d,
				}
				knockMap[kf.BaseDoc] = append(knockMap[kf.BaseDoc], k)
			}

			qualified, nonQualified := validateExposures(exposures)

			headerCols := []string{
				"exposure_header_id", "company_code", "entity", "entity1", "entity2", "entity3",
				"exposure_type", "document_id", "document_date", "counterparty_type", "counterparty_code",
				"counterparty_name", "currency", "total_original_amount", "total_open_amount",
				"value_date", "status", "is_active", "created_at", "updated_at", "approval_status",
				"exposure_creation_status",
				"approval_comment", "approved_by", "delete_comment", "requested_by", "rejection_comment",
				"approved_at", "rejected_by", "rejected_at", "time_based", "amount_in_local_currency",
				"posting_date", "text", "gl_account", "reference", "additional_header_details",
				"exposure_category",
			}

			docToID := make(map[string]string, len(qualified))

			debugLogLimit := 10
			headerSrc := pgx.CopyFromSlice(len(qualified), func(i int) ([]any, error) {
				q := qualified[i]
				var docDate interface{}
				var valDate interface{}
				var postDate interface{}
				if q.DocumentDate != "" {
					if t, terr := time.Parse("2006-01-02", q.DocumentDate); terr == nil {
						docDate = t
					}
				}
				if q.NetDueDate != "" {
					if t, terr := time.Parse("2006-01-02", q.NetDueDate); terr == nil {
						valDate = t
					}
				}
				if q.PostingDate != "" {
					if t, terr := time.Parse("2006-01-02", q.PostingDate); terr == nil {
						postDate = t
					}
				}

				addtl, _ := json.Marshal(q._raw)

				// entityName := ""
				// if n, ok := entityMap[strings.TrimSpace(q.CompanyCode)]; ok {
				// 	entityName = n
				// } else if uidRaw, ok := q._raw["unique_identifier"]; ok {
				// 	if uid := strings.TrimSpace(fmt.Sprintf("%v", uidRaw)); uid != "" {
				// 		if n2, ok2 := entityMap[uid]; ok2 {
				// 			entityName = n2
				// 		}
				// 	}
				// }
				entityName := ""
				cc := strings.TrimSpace(q.CompanyCode)
				if n, ok := entityMap[cc]; ok {
					entityName = n
				} else {
					candidates := []string{}
					if v, ok := q._raw["unique_identifier"]; ok {
						candidates = append(candidates, fmt.Sprintf("%v", v))
					}
					if v, ok := q._raw["company_name"]; ok {
						candidates = append(candidates, fmt.Sprintf("%v", v))
					}
					if v, ok := q._raw["entity_name"]; ok {
						candidates = append(candidates, fmt.Sprintf("%v", v))
					}
					for _, c := range candidates {
						if n, ok := entityMap[strings.TrimSpace(c)]; ok {
							entityName = n
							break
						}
					}
				}

				srcUpper := strings.ToUpper(strings.TrimSpace(q.Source))
				exposureCategory := srcUpper
				exposureType := detectExposureCategory(srcUpper)

				if v, ok := q._raw["Category"]; ok {
					if s := strings.TrimSpace(fmt.Sprintf("%v", v)); s != "" && !strings.EqualFold(s, "Exposure") {
						exposureType = s
					}
				}

				if i < debugLogLimit {
					rawCat := ""
					if v, ok := q._raw["Category"]; ok {
						rawCat = fmt.Sprintf("%v", v)
					}
					log.Printf("[FBUP] header-row[%d] doc=%s rawCategory='%s' source='%s' exposureType='%s' exposureCategory='%s' company='%s' amount=%s",
						i, q.DocumentNumber, rawCat, q.Source, exposureType, exposureCategory, q.CompanyCode, q.AmountDoc.String())
				}

				id := uuid.New()
				docToID[q.DocumentNumber] = id.String()

				totalOrig := q.AmountDoc.Abs().StringFixed(4)
				totalOpen := q.AmountDoc.StringFixed(4)

				return []any{
					id,                     // exposure_header_id
					q.CompanyCode,          // company_code
					entityName,             // entity
					entityName,             // entity1
					nil,                    // entity2
					nil,                    // entity3
					exposureType,           // exposure_type
					q.DocumentNumber,       // document_id
					docDate,                // document_date
					nil,                    // counterparty_type
					q.Party,                // counterparty_code
					nil,                    // counterparty_name
					q.DocumentCurrency,     // currency
					totalOrig,              // total_original_amount
					totalOpen,              // total_open_amount
					valDate,                // value_date
					"Open",                 // status
					true,                   // is_active
					time.Now(), time.Now(), // created_at, updated_at
					"Pending",               // approval_status
					"Pending",               // exposure_creation_status
					nil, nil, nil, nil, nil, // approval_comment, approved_by, delete_comment, requested_by, rejection_comment
					nil, nil, // approved_at, rejected_by
					nil,           // rejected_at
					time.Now(),    // time_based
					nil,           // amount_in_local_currency
					postDate,      // posting_date
					nil, nil, nil, // text, gl_account, reference
					addtl,            // additional_header_details
					exposureCategory, // exposure_category
				}, nil
			})

			log.Printf("[FBUP] about to COPY %d exposure_headers for batch %s file=%s", len(qualified), batchID.String(), fh.Filename)
			if _, err := tx.CopyFrom(ctx, pgx.Identifier{"public", "exposure_headers"}, headerCols, headerSrc); err != nil {
				httpError(w, 500, "copy headers: "+err.Error())
				return
			}
			log.Printf("[FBUP] finished COPY exposure_headers for batch %s file=%s", batchID.String(), fh.Filename)

			lineItemCols := []string{
				"line_item_id", "exposure_header_id", "line_number", "product_id", "product_description",
				"quantity", "unit_of_measure", "unit_price", "line_item_amount", "plant_code",
				"delivery_date", "payment_terms", "inco_terms", "additional_line_details", "created_at",
			}

			liRows := make([][]any, 0)
			for _, q := range qualified {
				hidStr, ok := docToID[q.DocumentNumber]
				if !ok {
					continue
				}
				hid, _ := uuid.Parse(hidStr)

				if len(q.LineItems) > 0 {
					for _, lit := range q.LineItems {
						// build columns
						lineNumber := asString(lit["line_number"])
						productID := asString(lit["product_id"])
						productDesc := asString(lit["product_description"])
						cleanQuantity := strings.ReplaceAll(asString(lit["quantity"]), ",", "")
						cleanUnitPrice := strings.ReplaceAll(asString(lit["unit_price"]), ",", "")
						cleanLineAmount := strings.ReplaceAll(asString(lit["line_item_amount"]), ",", "")

						quantity := asDecimalOrZero(cleanQuantity)
						unitOfMeasure := asString(lit["unit_of_measure"])
						unitPrice := asDecimalOrZero(cleanUnitPrice)
						lineAmount := asDecimalOrZero(cleanLineAmount)

						plant := asString(lit["plant_code"])
						deliveryDate := parseDateOrNil(asString(lit["delivery_date"]))
						paymentTerms := asString(lit["payment_terms"])
						inco := asString(lit["inco_terms"])

						addtlJSON, _ := json.Marshal(lit)
						row := []any{
							uuid.New(), // line_item_id
							hid,        // exposure_header_id
							lineNumber,
							productID,
							productDesc,
							nullableNumeric(quantity),
							unitOfMeasure,
							nullableNumeric(unitPrice),
							nullableNumeric(lineAmount),
							plant,
							deliveryDate,
							paymentTerms,
							inco,
							addtlJSON,
							time.Now(),
						}
						liRows = append(liRows, row)
					}
				}
			}

			lineItemsInserted := 0
			if len(liRows) > 0 {
				liSrc := pgx.CopyFromSlice(len(liRows), func(i int) ([]any, error) {
					return liRows[i], nil
				})
				if _, err := tx.CopyFrom(ctx, pgx.Identifier{"public", "exposure_line_items"}, lineItemCols, liSrc); err != nil {
					// log but don't fail entire batch — choose behavior per your appetite
					log.Printf("[FBUP] error copying line items: %v", err)
					fileWarnings = append(fileWarnings, fmt.Sprintf("line items copy failed: %v", err))
				} else {
					lineItemsInserted = len(liRows)
				}
			}

			// --- If some knockoff docs weren't created in this batch (i.e. docToID missing) query DB for ids ---
			if len(knockMap) > 0 {
				needLookup := make([]string, 0)
				for _, ks := range knockMap {
					for _, k := range ks {
						if _, ok := docToID[k.BaseDoc]; !ok {
							needLookup = append(needLookup, k.BaseDoc)
						}
						if _, ok := docToID[k.KnockDoc]; !ok {
							needLookup = append(needLookup, k.KnockDoc)
						}
					}
				}
				if len(needLookup) > 0 {
					uniq := map[string]struct{}{}
					finalLookup := make([]string, 0)
					for _, d := range needLookup {
						if d == "" {
							continue
						}
						if _, ok := uniq[d]; ok {
							continue
						}
						uniq[d] = struct{}{}
						finalLookup = append(finalLookup, d)
					}
					if len(finalLookup) > 0 {
						rows2, err := tx.Query(ctx, `SELECT document_id, exposure_header_id::text FROM public.exposure_headers WHERE document_id = ANY($1)`, finalLookup)
						if err != nil {
							httpError(w, 500, "lookup headers for knockoffs: "+err.Error())
							return
						}
						for rows2.Next() {
							var doc, id string
							if err := rows2.Scan(&doc, &id); err == nil {
								docToID[doc] = id
							}
						}
						rows2.Close()
					}
				}
			}

			// Insert rollovers
			for _, ks := range knockMap {
				for _, k := range ks {
					pidStr, ok1 := docToID[k.BaseDoc]
					cidStr, ok2 := docToID[k.KnockDoc]
					if !ok1 || !ok2 {
						// skip missing
						continue
					}
					pid, perr := uuid.Parse(pidStr)
					cid, cerr := uuid.Parse(cidStr)
					if perr != nil || cerr != nil {
						continue
					}
					amtStr := k.AmtAbs.StringFixed(4)
					if _, err := tx.Exec(ctx, `
						INSERT INTO public.exposure_rollover_log
						(rollover_id, parent_header_id, child_header_id, rollover_amount, rollover_date, created_at)
						VALUES ($1,$2,$3,$4,current_date,now())
						ON CONFLICT (parent_header_id, child_header_id) DO NOTHING`, uuid.New(), pid, cid, amtStr); err != nil {
						fileWarnings = append(fileWarnings, fmt.Sprintf("rollover insert failed for parent=%s child=%s: %v", k.BaseDoc, k.KnockDoc, err))
					}
				}
			}

			// update batch processed/failed counts & commit
			log.Printf("[FBUP] updating staging batch counts processed=%d failed=%d batch=%s file=%s", len(qualified), len(nonQualified), batchID.String(), fh.Filename)
			if _, err := tx.Exec(ctx, `UPDATE public.staging_batches_exposures SET processed_records=$1, failed_records=$2, status='completed' WHERE batch_id=$3`, len(qualified), len(nonQualified), batchID); err != nil {
				httpError(w, 500, "update batch: "+err.Error())
				return
			}

			if err := tx.Commit(ctx); err != nil {
				httpError(w, 500, "commit: "+err.Error())
				return
			}
			committed = true
			log.Printf("[FBUP] committed batch %s file=%s processed=%d failed=%d warnings=%d line_items=%d", batchID.String(), fh.Filename, len(qualified), len(nonQualified), len(fileWarnings), lineItemsInserted)

			// prepare response: map knockMap into response structure
			respKnock := knockMap
			if respKnock == nil {
				respKnock = map[string][]KnockoffInfo{}
			}
			if len(fileWarnings) == 0 {
				fileWarnings = make([]string, 0)
			}
			if len(fileErrors) == 0 {
				fileErrors = make([]string, 0)
			}

			// truncate nonQualified list if too big
			const maxValidationErrors = 200
			respNonQ := nonQualified
			if len(nonQualified) > maxValidationErrors {
				respNonQ = nonQualified[:maxValidationErrors]
			}

			previewRows := make([]CanonicalPreviewRow, 0, len(qualified)+len(nonQualified))

			// Add qualified rows first
			for _, q := range qualified {
				status := "ok"
				klist := []KnockoffInfo{}
				// attach knockoffs if exist
				if ks, ok := knockMap[q.DocumentNumber]; ok {
					status = "knocked_off"
					for _, k := range ks {
						klist = append(klist, KnockoffInfo{
							BaseDoc:  k.BaseDoc,
							KnockDoc: k.KnockDoc,
							AmtAbs:   k.AmtAbs,
						})
					}
				}
				previewRows = append(previewRows, CanonicalPreviewRow{
					DocumentNumber: q.DocumentNumber,
					CompanyCode:    q.CompanyCode,
					Party:          q.Party,
					Currency:       q.DocumentCurrency,
					Source:         q.Source,
					DocumentDate:   q.DocumentDate,
					PostingDate:    q.PostingDate,
					NetDueDate:     q.NetDueDate,
					Amount:         q.AmountDoc,
					Status:         status,
					Knockoffs:      klist,
				})
			}

			// Add non-qualified rows
			for _, n := range nonQualified {
				previewRows = append(previewRows, CanonicalPreviewRow{
					DocumentNumber: n.Row.DocumentNumber,
					CompanyCode:    n.Row.CompanyCode,
					Party:          n.Row.Party,
					Currency:       n.Row.DocumentCurrency,
					Source:         n.Row.Source,
					DocumentDate:   n.Row.DocumentDate,
					PostingDate:    n.Row.PostingDate,
					NetDueDate:     n.Row.NetDueDate,
					Amount:         n.Row.AmountDoc,
					Status:         "non_qualified",
					Issues:         n.Issues,
				})
			}

			// ---------- Append final result ----------
			results = append(results, UploadResult{
				FileName:      fh.Filename,
				Source:        src,
				BatchID:       batchID,
				TotalRows:     totalRows,
				InsertedCount: len(qualified),
				NonQualified:  respNonQ,
				LineItemsRows: lineItemsInserted,
				Rows:          previewRows,
				Errors:        append(fileErrors, fileWarnings...),
				Warnings:      fileWarnings,
			})

		} // end files loop

		writeJSON(w, map[string]interface{}{
			"success":      true,
			"results":      results,
			"duration_sec": time.Since(start).Seconds(),
		})
	}
}

// ---------- Utility: allocation using float64 ----------
type knockFloatInput struct {
	BaseDoc  string
	KnockDoc string
	AmtFloat float64
}

// func allocateFIFOFloat(rows []CanonicalRow) ([]CanonicalRow, []knockFloatInput)
//
//		bySrc := map[string][]CanonicalRow{}
//		for _, r := range rows {
//			bySrc[r.Source] = append(bySrc[r.Source], r)
//		}
//		allExps := make([]CanonicalRow, 0)
//		allKnocks := make([]knockFloatInput, 0)
//		for src, arr := range bySrc {
//			credits := filterBySignFloat(arr, -1)
//			debits := filterBySignFloat(arr, +1)
//			exps, knocks := allocateFIFOFloatCore(credits, debits)
//			if src == "FBL5N" {
//				debits2 := filterBySignFloat(arr, +1)
//				credits2 := filterBySignFloat(arr, -1)
//				exps2, knocks2 := allocateFIFOFloatCore(debits2, credits2)
//				exps = append(exps, exps2...)
//				knocks = append(knocks, knocks2...)
//			}
//			allExps = append(allExps, exps...)
//			allKnocks = append(allKnocks, knocks...)
//		}
//		return allExps, allKnocks
//	}
func allocateFIFOFloat(rows []CanonicalRow, receivableLogic, payableLogic string) ([]CanonicalRow, []knockFloatInput) {
	bySrc := map[string][]CanonicalRow{}
	for _, r := range rows {
		bySrc[r.Source] = append(bySrc[r.Source], r)
	}

	allExps := make([]CanonicalRow, 0)
	allKnocks := make([]knockFloatInput, 0)

	for src, arr := range bySrc {
		credits := filterBySignFloat(arr, -1)
		debits := filterBySignFloat(arr, +1)

		switch src {
		case "FBL1N": // Payables (vendors)
			// Standard for vendors: DEBIT (+ve) values are considered first
			// Reverse logic flips the direction
			if strings.EqualFold(payableLogic, "reverse") {
				exps, knocks := allocateFIFOFloatCore(credits, debits)
				allExps = append(allExps, exps...)
				allKnocks = append(allKnocks, knocks...)
			} else {
				exps, knocks := allocateFIFOFloatCore(debits, credits)
				allExps = append(allExps, exps...)
				allKnocks = append(allKnocks, knocks...)
			}

		case "FBL5N": // Receivables (customers)
			// Standard for customers: CREDIT (-ve) values are considered first
			// Reverse logic flips the direction
			if strings.EqualFold(receivableLogic, "reverse") {
				exps, knocks := allocateFIFOFloatCore(debits, credits)
				allExps = append(allExps, exps...)
				allKnocks = append(allKnocks, knocks...)
			} else {
				exps, knocks := allocateFIFOFloatCore(credits, debits)
				allExps = append(allExps, exps...)
				allKnocks = append(allKnocks, knocks...)
			}

		case "FBL3N": // GR/IR (payable-like)
			// Treat like vendors: DEBIT (+ve) first in standard mode
			if strings.EqualFold(payableLogic, "reverse") {
				exps, knocks := allocateFIFOFloatCore(credits, debits)
				allExps = append(allExps, exps...)
				allKnocks = append(allKnocks, knocks...)
			} else {
				exps, knocks := allocateFIFOFloatCore(debits, credits)
				allExps = append(allExps, exps...)
				allKnocks = append(allKnocks, knocks...)
			}

		default:
			exps, knocks := allocateFIFOFloatCore(credits, debits)
			allExps = append(allExps, exps...)
			allKnocks = append(allKnocks, knocks...)
		}
	}

	return allExps, allKnocks
}

func filterBySignFloat(rows []CanonicalRow, sign int) []CanonicalRow {
	out := make([]CanonicalRow, 0)
	for _, r := range rows {
		if sign < 0 && r.AmountFloat < 0 {
			out = append(out, r)
		}
		if sign > 0 && r.AmountFloat > 0 {
			out = append(out, r)
		}
	}
	return out
}

func allocateFIFOFloatCore(baseItems []CanonicalRow, knocks []CanonicalRow) ([]CanonicalRow, []knockFloatInput) {
	type it struct {
		ref    CanonicalRow
		amtAbs float64
	}
	b := make([]it, len(baseItems))
	for i, v := range baseItems {
		b[i] = it{ref: v, amtAbs: math.Abs(v.AmountFloat)}
	}
	k := make([]it, len(knocks))
	for i, v := range knocks {
		k[i] = it{ref: v, amtAbs: math.Abs(v.AmountFloat)}
	}
	exposures := make([]CanonicalRow, 0)
	knockoffs := make([]knockFloatInput, 0)
	for i := range b {
		remaining := b[i].amtAbs
		for j := range k {
			if remaining <= 0 {
				break
			}
			if k[j].amtAbs <= 0 {
				continue
			}
			if remaining <= k[j].amtAbs+1e-12 {
				knockoffs = append(knockoffs, knockFloatInput{BaseDoc: b[i].ref.DocumentNumber, KnockDoc: k[j].ref.DocumentNumber, AmtFloat: remaining})
				k[j].amtAbs = k[j].amtAbs - remaining
				remaining = 0
			} else {
				knockoffs = append(knockoffs, knockFloatInput{BaseDoc: b[i].ref.DocumentNumber, KnockDoc: k[j].ref.DocumentNumber, AmtFloat: k[j].amtAbs})
				remaining = remaining - k[j].amtAbs
				k[j].amtAbs = 0
			}
		}
		if remaining > 0 {
			e := b[i].ref
			if b[i].ref.AmountFloat < 0 {
				e.AmountFloat = -remaining
			} else {
				e.AmountFloat = remaining
			}
			exposures = append(exposures, e)
		}
	}
	return exposures, knockoffs
}

func saveTempAndHash(f multipart.File, filename string) (string, string, error) {
	tmp, err := os.CreateTemp("", "upload-*"+filepath.Ext(filename))
	if err != nil {
		return "", "", err
	}
	defer tmp.Close()
	h := sha256.New()
	if _, err := io.Copy(io.MultiWriter(tmp, h), f); err != nil {
		return "", "", err
	}
	return tmp.Name(), hex.EncodeToString(h.Sum(nil)), nil
}

func fastMapWithHeaderLower(row map[string]string, headerLower map[string]string, headerMap map[string]string) map[string]interface{} {
	out := make(map[string]interface{})
	if len(headerMap) > 0 {
		for canon, header := range headerMap {
			if v, ok := row[header]; ok {
				out[canon] = strings.TrimSpace(v)
				continue
			}
			if orig, ok := headerLower[strings.ToLower(strings.TrimSpace(header))]; ok {
				out[canon] = strings.TrimSpace(row[orig])
			} else {
				out[canon] = ""
			}
		}
		return out
	}
	lrow := map[string]string{}
	for k, v := range row {
		lrow[strings.ToLower(strings.TrimSpace(k))] = v
	}
	guesses := map[string][]string{
		"CompanyCode":      {"company code", "bukrs", "company_code", "company"},
		"Party":            {"party", "account", "vendor", "customer"},
		"DocumentCurrency": {"document currency", "currency", "waers"},
		"DocumentNumber":   {"document number", "belnr", "document", "docno"},
		"DocumentDate":     {"document date", "bldat"},
		"PostingDate":      {"posting date", "budat"},
		"NetDueDate":       {"net due date", "due date", "baseline date", "clearing date"},
		"AmountDoc":        {"amount", "wrbtr", "amount in doc. curr.", "amount in doc curr", "amount in local currency", "amount in local curr"},
	}
	for canon, list := range guesses {
		for _, cand := range list {
			if v, ok := lrow[strings.ToLower(cand)]; ok {
				out[canon] = strings.TrimSpace(v)
				break
			}
		}
	}
	return out
}

// func mapObjectToCanonical(obj map[string]interface{}, src string) (CanonicalRow, error)
func mapObjectToCanonical(obj map[string]interface{}, src string, aliasMap map[string]string) (CanonicalRow, error) {
	getS := func(k string) string {
		if v, ok := obj[k]; ok {
			return strings.TrimSpace(fmt.Sprintf("%v", v))
		}
		return ""
	}
	getD := func(k string) decimal.Decimal {
		if v, ok := obj[k]; ok {
			switch t := v.(type) {
			case float64:
				return decimal.NewFromFloat(t)
			case string:
				s := strings.ReplaceAll(strings.TrimSpace(t), ",", "")
				if s == "" {
					return decimal.Zero
				}
				d, _ := decimal.NewFromString(s)
				return d
			default:
				d, _ := decimal.NewFromString(fmt.Sprintf("%v", t))
				return d
			}
		}
		return decimal.Zero
	}
	cur := strings.TrimSpace(strings.ToUpper(getS("DocumentCurrency")))
	if v, ok := aliasMap[cur]; ok {
		cur = v
	}
	c := CanonicalRow{
		Source:      src,
		CompanyCode: getS("CompanyCode"),
		Party:       getS("Party"),
		// DocumentCurrency: getS("DocumentCurrency"),
		DocumentCurrency: cur,
		DocumentNumber:   getS("DocumentNumber"),
		DocumentDate:     getS("DocumentDate"),
		PostingDate:      getS("PostingDate"),
		NetDueDate:       getS("NetDueDate"),
		AmountDoc:        getD("AmountDoc"),
		_raw:             obj,
	}
	if v, ok := obj["LineItems"]; ok {
		switch t := v.(type) {
		case []map[string]interface{}:
			c.LineItems = t
		case []interface{}:
			arr := make([]map[string]interface{}, 0, len(t))
			for _, it := range t {
				if mm, ok := it.(map[string]interface{}); ok {
					arr = append(arr, mm)
				}
			}
			c.LineItems = arr
		}
	}
	return c, nil
}

func NormalizeDate(dateStr string) (string, error) {
	s := strings.TrimSpace(dateStr)
	if s == "" {
		return "", nil
	}
	layouts := []string{"2006-01-02", "02-01-2006", "01/02/2006", "2006/01/02", "2 Jan 2006", time.RFC3339, "20060102", "02-Jan-2006"}
	for _, l := range layouts {
		if t, err := time.Parse(l, s); err == nil {
			return t.Format("2006-01-02"), nil
		}
	}
	if len(s) >= 10 {
		if t, err := time.Parse("2006-01-02", s[:10]); err == nil {
			return t.Format("2006-01-02"), nil
		}
	}
	return "", fmt.Errorf("unparseable date: %s", dateStr)
}

func validateSingleExposure(it CanonicalRow) (CanonicalRow, []string) {
	issues := make([]string, 0, 4)
	if strings.TrimSpace(it.CompanyCode) == "" {
		issues = append(issues, "CompanyCode missing")
	}
	if strings.TrimSpace(it.Party) == "" {
		issues = append(issues, "Party missing")
	}
	if strings.TrimSpace(it.DocumentCurrency) == "" {
		issues = append(issues, "Currency missing")
	}
	if it.AmountDoc.Equal(decimal.Zero) {
		issues = append(issues, "Amount invalid or zero")
	}
	return it, issues
}

func validateExposures(inputs []CanonicalRow) ([]CanonicalRow, []NonQualified) {
	ok := make([]CanonicalRow, 0)
	bad := make([]NonQualified, 0)
	for _, it := range inputs {
		issues := make([]string, 0)
		if strings.TrimSpace(it.CompanyCode) == "" {
			issues = append(issues, "CompanyCode missing")
		}
		if strings.TrimSpace(it.Party) == "" {
			issues = append(issues, "Party missing")
		}
		if strings.TrimSpace(it.DocumentCurrency) == "" {
			issues = append(issues, "Currency missing")
		}
		if strings.TrimSpace(it.NetDueDate) == "" {
			issues = append(issues, "Due date missing or invalid")
		}
		if it.AmountDoc.Equal(decimal.Zero) {
			issues = append(issues, "Amount invalid or zero")
		}
		// honor programmatic non-qualified flag (structured)
		if it.IsNonQualified {
			if it.NonQualifiedReason != "" {
				issues = append(issues, it.NonQualifiedReason)
			} else {
				issues = append(issues, "Marked non-qualified by rules")
			}
		}
		if len(issues) > 0 {
			bad = append(bad, NonQualified{Row: it, Issues: issues})
		} else {
			ok = append(ok, it)
		}
	}
	return ok, bad
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func httpError(w http.ResponseWriter, status int, msg string) {
	w.WriteHeader(status)
	writeJSON(w, map[string]interface{}{"success": false, "error": msg})
}

func detectExposureCategory(src string) string {
	switch src {
	case "FBL1N":
		return "Creditor"
	case "FBL5N":
		return "Debitor"
	case "FBL3N":
		return "GRN"
	default:
		return "Unknown"
	}
}

func asString(v interface{}) string {
	if v == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprintf("%v", v))
}
func asDecimalOrZero(v interface{}) decimal.Decimal {
	if v == nil {
		return decimal.Zero
	}
	switch t := v.(type) {
	case decimal.Decimal:
		return t
	case float64:
		return decimal.NewFromFloat(t)
	case string:
		s := strings.ReplaceAll(strings.TrimSpace(t), ",", "")
		if s == "" {
			return decimal.Zero
		}
		d, _ := decimal.NewFromString(s)
		if d.IsZero() {
			return decimal.Zero
		}
		return d
	default:
		d, _ := decimal.NewFromString(fmt.Sprintf("%v", t))
		return d
	}
}
func nullableNumeric(d decimal.Decimal) interface{} {
	if d.IsZero() {
		return nil
	}
	return d.StringFixed(4)
}
func parseDateOrNil(s string) interface{} {
	if s == "" {
		return nil
	}
	if t, err := time.Parse("2006-01-02", s); err == nil {
		return t
	}
	if t, err := time.Parse("02-01-2006", s); err == nil {
		return t
	}
	return nil
}
