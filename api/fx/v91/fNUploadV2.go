package exposures

import (
	"CimplrCorpSaas/api/constants"
	"context"
	"database/sql"

	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"
	"github.com/xuri/excelize/v2"
)

// ------------------------- Types -------------------------

type CanonicalRow struct {
	Source           string                   `json:"Source"`
	CompanyCode      string                   `json:"CompanyCode"`
	Party            string                   `json:"Party"`
	DocumentCurrency string                   `json:"DocumentCurrency"`
	DocumentNumber   string                   `json:"DocumentNumber"`
	DocumentDate     string                   `json:"DocumentDate"`
	PostingDate      string                   `json:"PostingDate"`
	NetDueDate       string                   `json:"NetDueDate"`
	AmountDoc        decimal.Decimal          `json:"AmountDoc"` // canonical decimal
	AmountFloat      float64                  `json:"-"`         // hot-loop float
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
	Info          []string              `json:"info,omitempty"`
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

// internal shape used by allocateFIFOFloat
type knockFloatInput struct {
	BaseDoc  string
	KnockDoc string
	AmtFloat float64
}

// used to stream rows into pgx.CopyFrom from a channel
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

// date normalizer with caching for heavy uploads
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

		// build entity map
		entityMap := map[string]string{}
		{
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

		// Build currency map (active or with approved audit action)
		currencyMap := map[string]struct{}{}
		rowsCur, err := pool.Query(ctx, `
			SELECT mc.currency_code
			FROM public.mastercurrency mc
			WHERE (lower(mc.status) = 'active'
			   OR EXISTS (
			       SELECT 1 FROM public.auditactioncurrency a
			       WHERE a.currency_id = mc.currency_id AND a.processing_status = 'APPROVED'
			   )
			)
		`)
		if err == nil {
			for rowsCur.Next() {
				var code string
				if err := rowsCur.Scan(&code); err == nil {
					if code = strings.TrimSpace(code); code != "" {
						currencyMap[strings.ToUpper(code)] = struct{}{}
					}
				}
			}
			rowsCur.Close()
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
			fileInfo := make([]string, 0)

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
			defer tmpFile.Close()

			fileExt := strings.ToLower(filepath.Ext(fh.Filename))
			allRows, err := ubParseUploadFile(tmpFile, fileExt)
			if err != nil {
				httpError(w, http.StatusBadRequest, "file parse error: "+err.Error())
				return
			}
			if len(allRows) == 0 {
				httpError(w, http.StatusBadRequest, "empty file or no data rows")
				return
			}

			// headers
			headersRec := allRows[0]
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

			// start DB tx
			batchID := uuid.New()
			conn, err := pool.Acquire(ctx)
			if err != nil {
				httpError(w, 500, "db acquire: "+err.Error())
				return
			}
			tx, err := conn.Begin(ctx)
			if err != nil {
				conn.Release()
				httpError(w, 500, "tx begin: "+err.Error())
				return
			}
			committed := false
			defer func() {
				if !committed {
					_ = tx.Rollback(ctx)
				}
				conn.Release()
			}()

			if _, err := tx.Exec(ctx, `
				INSERT INTO public.staging_batches_exposures
				(batch_id, ingestion_source, status, total_records, file_hash, file_name, uploaded_by, mapping_json)
				VALUES ($1,$2,'processing',$3,$4,$5,$6,$7)
			`, batchID, src, 0, fileHash, fh.Filename, userID, string(mappingRaw)); err != nil {
				httpError(w, 500, "insert batch: "+err.Error())
				return
			}

			// staging copy
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

			// process rows
			for rowIdx := 1; rowIdx < len(allRows); rowIdx++ {
				rec := allRows[rowIdx]
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

				c, _ := mapObjectToCanonical(mapped, src, currencyAliases)

				// parse amount into both decimal and float
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

				_, _ = validateSingleExposure(c) // keep old behavior (we don't stop on single issues here)

				canonicals = append(canonicals, c)
			}

			// finish staging copy
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

			log.Printf("[DEBUG] After parsing: canonicals=%d, batch=%s file=%s", len(canonicals), batchID.String(), fh.Filename)
			if len(canonicals) > 0 {
				sample := canonicals[0]
				log.Printf("[DEBUG] Sample canonical[0]: CompanyCode='%s', Party='%s', Currency='%s', Amount=%s, AmountFloat=%f, NetDueDate='%s'",
					sample.CompanyCode, sample.Party, sample.DocumentCurrency, sample.AmountDoc.String(), sample.AmountFloat, sample.NetDueDate)
			}

			// allocate (produces unallocated exposures and knockoffs)
			exposuresFloat, knocksFloat := allocateFIFOFloat(canonicals, receivableLogic, payableLogic)

			// ---- Informational Summary for Auto-Offset / Self-Allocation ----
			autoKnockCount := len(knocksFloat)
			selfKnockCount := 0
			for _, k := range knocksFloat {
				if k.BaseDoc == k.KnockDoc {
					selfKnockCount++
				}
			}

			if autoKnockCount > 0 {
				msg := fmt.Sprintf(
					"Auto-offset applied: %d knock-off(s) detected, including %d self-allocation(s). "+
						"These were automatically netted within the same Company/Party/Currency group. "+
						"Remaining open amounts, if any, were inserted as exposures.",
					autoKnockCount, selfKnockCount)
				fileInfo = append(fileInfo, msg)
				log.Printf("[INFO] %s", msg)
			}

			log.Printf("[DEBUG] After allocation: exposuresFloat=%d, knocksFloat=%d, batch=%s", len(exposuresFloat), len(knocksFloat), batchID.String())

			// net-exposure non-qualified pass
			netMap := make(map[string]float64)
			for _, e := range exposuresFloat {
				key := fmt.Sprintf("%s|%s|%s", e.Source, e.CompanyCode, e.Party)
				netMap[key] += e.AmountFloat
			}
			flaggedCount := 0
			for i := range exposuresFloat {
				e := &exposuresFloat[i]
				key := fmt.Sprintf("%s|%s|%s", e.Source, e.CompanyCode, e.Party)
				net := netMap[key]
				switch e.Source {
				case "FBL1N", "FBL3N":
					if net > 0 {
						e.IsNonQualified = true
						e.NonQualifiedReason = fmt.Sprintf("Vendor net exposure %.4f > 0", net)
						if e._raw == nil {
							e._raw = make(map[string]interface{})
						}
						e._raw["is_non_qualified"] = true
						e._raw["non_qualified_reason"] = e.NonQualifiedReason
						flaggedCount++
					}
				case "FBL5N":
					if net < 0 {
						e.IsNonQualified = true
						e.NonQualifiedReason = fmt.Sprintf("Customer net exposure %.4f < 0", net)
						if e._raw == nil {
							e._raw = make(map[string]interface{})
						}
						e._raw["is_non_qualified"] = true
						e._raw["non_qualified_reason"] = e.NonQualifiedReason
						flaggedCount++
					}
				}
			}
			log.Printf("[DEBUG] After net-exposure pass: flagged=%d out of %d exposuresFloat, batch=%s", flaggedCount, len(exposuresFloat), batchID.String())

			// build canonical exposures slice (decimal amounts) - ONLY documents with remaining > 0
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

			// entity / currency non-qualified pass
			entityMiss := 0
			currencyMiss := 0
			if len(exposures) > 0 {
				for i := range exposures {
					e := &exposures[i]
					cc := strings.TrimSpace(e.CompanyCode)
					if _, ok := entityMap[cc]; !ok || cc == "" {
						e.IsNonQualified = true
						reason := fmt.Sprintf("No entity found for company_code: %s", cc)
						if e.NonQualifiedReason != "" {
							e.NonQualifiedReason = e.NonQualifiedReason + "; " + reason
						} else {
							e.NonQualifiedReason = reason
						}
						if e._raw == nil {
							e._raw = make(map[string]interface{})
						}
						e._raw["is_non_qualified"] = true
						e._raw["non_qualified_reason"] = e.NonQualifiedReason
						entityMiss++
					}
					cur := strings.ToUpper(strings.TrimSpace(e.DocumentCurrency))
					if len(currencyMap) > 0 {
						if _, ok := currencyMap[cur]; !ok {
							e.IsNonQualified = true
							reason := fmt.Sprintf("Currency %s not found or inactive/approved", cur)
							if e.NonQualifiedReason != "" {
								e.NonQualifiedReason = e.NonQualifiedReason + "; " + reason
							} else {
								e.NonQualifiedReason = reason
							}
							if e._raw == nil {
								e._raw = make(map[string]interface{})
							}
							e._raw["is_non_qualified"] = true
							e._raw["non_qualified_reason"] = e.NonQualifiedReason
							currencyMiss++
						}
					}
				}
				if entityMiss > 0 || currencyMiss > 0 {
					msg2 := fmt.Sprintf("Marked %d rows non-qualified due to missing entity and %d due to missing currency (company_code/currency).", entityMiss, currencyMiss)
					fileWarnings = append(fileWarnings, msg2)
					log.Printf("[WARN] %s", msg2)
				}
			}

			// build knockMap for preview (knockoffs grouped by base doc)
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

			// validation: separate qualified and nonQualified (struct NonQualified)
			qualified, nonQualified := validateExposures(exposures)

			log.Printf("[DEBUG] After validation: qualified=%d, nonQualified=%d, batch=%s", len(qualified), len(nonQualified), batchID.String())
			if len(nonQualified) > 0 && len(nonQualified) <= 5 {
				for i, nq := range nonQualified {
					log.Printf("[DEBUG] nonQualified[%d]: doc=%s, issues=%v", i, nq.Row.DocumentNumber, nq.Issues)
				}
			}

			if len(exposures) == 0 {
				if len(knocksFloat) > 0 {
					msg := fmt.Sprintf("No exposures were written: allocation fully matched all rows (knock events=%d). This commonly occurs when incoming debit/credit rows for the same Source|CompanyCode|Party fully net to zero, or because of receivable/payable logic settings (receivable_logic=%s, payable_logic=%s). If you expected inserts, check mapping, amount signs, and NetDueDate values; or run a small unbalanced test file to verify behavior.", len(knocksFloat), receivableLogic, payableLogic)
					fileWarnings = append(fileWarnings, msg)
					log.Printf("[WARN] %s", msg)
				} else {
					msg := "No exposures were written: allocation produced no base or knock items. Likely causes: amounts all share the same sign (no debits or no credits), amounts parsed as zero, or mapping produced empty AmountDoc values. Check mapping, amount signs (+/-), and NetDueDate; try 'receivable_logic'/'payable_logic' = reverse to flip allocation direction or upload a small unbalanced test file."
					fileWarnings = append(fileWarnings, msg)
					log.Printf("[WARN] %s", msg)
				}
			}

			// ------------------ Prepare headers COPY (includes batch_id + file_hash) ------------------
			headerCols := []string{
				"exposure_header_id", "company_code", "entity", "entity1", "entity2", "entity3",
				"exposure_type", "document_id", "document_date", "counterparty_type", "counterparty_code",
				"counterparty_name", "currency", "total_original_amount", "total_open_amount",
				"value_date", "status", "is_active", "created_at", "updated_at", "approval_status",
				"exposure_creation_status",
				"approval_comment", "approved_by", "delete_comment", "requested_by", "rejection_comment",
				"approved_at", "rejected_by", "rejected_at", "time_based", "amount_in_local_currency",
				"posting_date", "text", "gl_account", "reference", "additional_header_details",
				"exposure_category", "batch_id", "file_hash",
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
				counterpartyType := ""
				switch srcUpper {
				case "FBL1N", "FBL3N":
					counterpartyType = "Vendor"
				case "FBL5N":
					counterpartyType = "Customer"
				}

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

				// order here must match headerCols above
				var counterpartyVal interface{}
				if counterpartyType != "" {
					counterpartyVal = counterpartyType
				}
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
					counterpartyVal,        // counterparty_type
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
					"Approved",              // exposure_creation_status
					nil, nil, nil, nil, nil, // approval_comment, approved_by, delete_comment, requested_by, rejection_comment
					nil, nil, // approved_at, rejected_by
					nil,           // rejected_at
					time.Now(),    // time_based
					nil,           // amount_in_local_currency
					postDate,      // posting_date
					nil, nil, nil, // text, gl_account, reference
					addtl,            // additional_header_details
					exposureCategory, // exposure_category
					batchID,          // batch_id (new)
					fileHash,         // file_hash (new)
				}, nil
			})

			log.Printf("[FBUP] about to COPY %d exposure_headers for batch %s file=%s", len(qualified), batchID.String(), fh.Filename)
			if _, err := tx.CopyFrom(ctx, pgx.Identifier{"public", "exposure_headers"}, headerCols, headerSrc); err != nil {
				httpError(w, 500, "copy headers: "+err.Error())
				return
			}
			log.Printf("[FBUP] finished COPY exposure_headers for batch %s file=%s", batchID.String(), fh.Filename)

			// Build line items (unchanged) - we'll keep insertion code in Part 3
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


			log.Printf("[FBUP] about to COPY %d exposure_line_items for batch %s", len(liRows), batchID.String())
			if len(liRows) > 0 {
				if _, err := tx.CopyFrom(ctx,
					pgx.Identifier{"public", "exposure_line_items"},
					lineItemCols,
					pgx.CopyFromRows(liRows)); err != nil {
					fileErrors = append(fileErrors, "copy line items: "+err.Error())
					log.Printf("[ERROR] copy line items: %v", err)
				}
			}
			log.Printf("[FBUP] finished COPY exposure_line_items for batch %s", batchID.String())

			// ------------------ Build and COPY exposure_allocations ------------------
			allocCols := []string{
				"allocation_id", "batch_id", "file_hash",
				"base_document_id", "knockoff_document_id",
				"allocation_amount", "allocation_currency",
				"allocation_amount_signed", "allocation_date",
				"created_at", "created_by", "notes",
			}
			allocRows := make([][]any, 0, len(knocksFloat))
			for _, k := range knocksFloat {
				if k.AmtFloat == 0 {
					continue
				}
				allocRows = append(allocRows, []any{
					uuid.New(),
					batchID,
					fileHash,
					k.BaseDoc,
					k.KnockDoc,
					decimal.NewFromFloat(math.Abs(k.AmtFloat)).StringFixed(4),
					nil,
					decimal.NewFromFloat(k.AmtFloat).StringFixed(4),
					time.Now(),
					time.Now(),
					userID,
					nil,
				})
			}
			if len(allocRows) > 0 {
				if _, err := tx.CopyFrom(ctx,
					pgx.Identifier{"public", "exposure_allocations"},
					allocCols,
					pgx.CopyFromRows(allocRows)); err != nil {
					fileErrors = append(fileErrors, "copy allocations: "+err.Error())
					log.Printf("[ERROR] copy allocations: %v", err)
				}
			}

			// ------------------ Build and COPY exposure_unallocated ------------------
			unallocCols := []string{
				"unallocated_id", "batch_id", "file_hash",
				"document_number", "company_code", "party",
				"currency", "source", "document_date",
				"posting_date", "net_due_date", "effective_due_date",
				"amount", "amount_signed", "exchange_rate",
				"amount_local_signed", "allocation_status",
				"mapped_payload", "created_at",
			}
			unallocRows := make([][]any, 0, len(exposures))
			for _, e := range exposures {
				// Determine allocation status based on knockoffs
				allocStatus := "unallocated"
				if len(knockMap[e.DocumentNumber]) > 0 {
					allocStatus = "partially_allocated"
				}

				mp, _ := json.Marshal(e._raw)
				unallocRows = append(unallocRows, []any{
					uuid.New(),
					batchID,
					fileHash,
					e.DocumentNumber,
					e.CompanyCode,
					e.Party,
					e.DocumentCurrency,
					e.Source,
					parseDateOrNil(e.DocumentDate),
					parseDateOrNil(e.PostingDate),
					parseDateOrNil(e.NetDueDate),
					parseDateOrNil(e.NetDueDate), // effective_due_date (same now)
					e.AmountDoc.Abs().StringFixed(4),
					e.AmountDoc.StringFixed(4),
					nil, // exchange_rate
					nil, // amount_local_signed
					allocStatus,
					mp,
					time.Now(),
				})
			}
			if len(unallocRows) > 0 {
				if _, err := tx.CopyFrom(ctx,
					pgx.Identifier{"public", "exposure_unallocated"},
					unallocCols,
					pgx.CopyFromRows(unallocRows)); err != nil {
					fileErrors = append(fileErrors, "copy unallocated: "+err.Error())
					log.Printf("[ERROR] copy unallocated: %v", err)
				}
			}

			// ------------------ Build and COPY exposure_unqualified ------------------
			unqualCols := []string{
				"unqualified_id", "batch_id", "file_hash",
				"document_number", "company_code", "party",
				"currency", "source", "document_date",
				"posting_date", "net_due_date", "amount",
				"issues", "non_qualified_reason",
				"mapped_payload", "created_at",
			}
			unqualRows := make([][]any, 0, len(nonQualified))
			for _, nq := range nonQualified {
				mp, _ := json.Marshal(nq.Row._raw)
				reason := strings.Join(nq.Issues, "; ")
				unqualRows = append(unqualRows, []any{
					uuid.New(),
					batchID,
					fileHash,
					nq.Row.DocumentNumber,
					nq.Row.CompanyCode,
					nq.Row.Party,
					nq.Row.DocumentCurrency,
					nq.Row.Source,
					parseDateOrNil(nq.Row.DocumentDate),
					parseDateOrNil(nq.Row.PostingDate),
					parseDateOrNil(nq.Row.NetDueDate),
					nq.Row.AmountDoc.StringFixed(4),
					nq.Issues,
					reason,
					mp,
					time.Now(),
				})
			}
			if len(unqualRows) > 0 {
				if _, err := tx.CopyFrom(ctx,
					pgx.Identifier{"public", "exposure_unqualified"},
					unqualCols,
					pgx.CopyFromRows(unqualRows)); err != nil {
					fileErrors = append(fileErrors, "copy unqualified: "+err.Error())
					log.Printf("[ERROR] copy unqualified: %v", err)
				}
			}

			if _, err := tx.Exec(ctx, `
				UPDATE public.staging_batches_exposures
				SET status='completed',
					processed_records=$1,
					failed_records=$2,
					error_message=$3
				WHERE batch_id=$4
			`, len(qualified), len(nonQualified), strings.Join(fileErrors, "; "), batchID); err != nil {
				httpError(w, 500, "update batch: "+err.Error())
				return
			}

			if err := tx.Commit(ctx); err != nil {
				httpError(w, 500, "commit failed: "+err.Error())
				return
			}
			committed = true

			log.Printf("[FBUP] committed batch %s for file %s", batchID.String(), fh.Filename)

			allOriginalDocs := make(map[string]CanonicalRow)
			for _, c := range canonicals {
				allOriginalDocs[c.DocumentNumber] = c
			}

			qualifiedDocs := make(map[string]bool, len(qualified))
			for _, q := range qualified {
				qualifiedDocs[q.DocumentNumber] = true
			}

			// Include ALL documents in response: 37 with remaining amounts + 12 fully knocked off
			rowsPreview := make([]CanonicalPreviewRow, 0, len(allOriginalDocs))
			for docNum, origDoc := range allOriginalDocs {
				status := "ok"

				// Check if this doc is in qualified list (has remaining amount)
				if !qualifiedDocs[docNum] {
					// Not in qualified = either non-qualified OR fully knocked off
					// Check if it's in nonQualified list
					isNonQual := false
					for _, nq := range nonQualified {
						if nq.Row.DocumentNumber == docNum {
							isNonQual = true
							break
						}
					}
					if isNonQual {
						status = "non_qualified"
					} else {
						// It's fully knocked off (has knockoffs and not in qualified)
						if len(knockMap[docNum]) > 0 {
							status = "knocked_off"
						}
					}
				}

				pr := CanonicalPreviewRow{
					DocumentNumber: origDoc.DocumentNumber,
					CompanyCode:    origDoc.CompanyCode,
					Party:          origDoc.Party,
					Currency:       origDoc.DocumentCurrency,
					Source:         origDoc.Source,
					DocumentDate:   origDoc.DocumentDate,
					PostingDate:    origDoc.PostingDate,
					NetDueDate:     origDoc.NetDueDate,
					Amount:         origDoc.AmountDoc, // original amount from CSV
					Status:         status,
				}
				if kos, ok := knockMap[docNum]; ok {
					pr.Knockoffs = kos
				}
				if origDoc.IsNonQualified {
					pr.Issues = []string{origDoc.NonQualifiedReason}
				}
				rowsPreview = append(rowsPreview, pr)
			}

			res := UploadResult{
				FileName:      fh.Filename,
				Source:        src,
				BatchID:       batchID,
				TotalRows:     totalRows,
				InsertedCount: len(qualified),
				LineItemsRows: len(liRows),
				NonQualified:  nonQualified,
				Rows:          rowsPreview,
				Errors:        fileErrors,
				Warnings:      fileWarnings,
				Info:          fileInfo,
			}
			results = append(results, res)
		} // end per-file loop

		elapsed := time.Since(start)
		log.Printf("[FBUP] total upload completed in %s, files=%d", elapsed.String(), len(results))
		writeJSON(w, map[string]interface{}{
			"success": true,
			"results": results,
			"duration": map[string]interface{}{
				"seconds": elapsed.Seconds(),
			},
		})
	}
}

// ------------------------- Utilities -------------------------

func allocateFIFOFloat(rows []CanonicalRow, receivableLogic, payableLogic string) ([]CanonicalRow, []knockFloatInput) {
	// Group by (CompanyCode | Party | Currency | Source)
	grouped := make(map[string][]CanonicalRow)
	for _, r := range rows {
		key := strings.Join([]string{
			strings.ToUpper(strings.TrimSpace(r.CompanyCode)),
			strings.ToUpper(strings.TrimSpace(r.Party)),
			strings.ToUpper(strings.TrimSpace(r.DocumentCurrency)),
			strings.ToUpper(strings.TrimSpace(r.Source)),
		}, "|")
		grouped[key] = append(grouped[key], r)
	}

	allExps := make([]CanonicalRow, 0)
	allKnocks := make([]knockFloatInput, 0)

	for key, arr := range grouped {
		if len(arr) == 0 {
			continue
		}
		parts := strings.Split(key, "|")
		company := parts[0]
		party := parts[1]
		curr := parts[2]
		src := parts[3]

		// Split by sign
		credits := filterBySignFloat(arr, -1)
		debits := filterBySignFloat(arr, +1)

		var exps []CanonicalRow
		var knocks []knockFloatInput

		switch src {
		case "FBL1N": // Payables
			if strings.EqualFold(payableLogic, "reverse") {
				exps, knocks = allocateFIFOFloatCore(debits, credits)
			} else {
				exps, knocks = allocateFIFOFloatCore(credits, debits)
			}
		case "FBL5N": // Receivables
			if strings.EqualFold(receivableLogic, "reverse") {
				exps, knocks = allocateFIFOFloatCore(credits, debits)
			} else {
				exps, knocks = allocateFIFOFloatCore(debits, credits)
			}
		case "FBL3N": // GR/IR
			if strings.EqualFold(payableLogic, "reverse") {
				exps, knocks = allocateFIFOFloatCore(debits, credits)
			} else {
				exps, knocks = allocateFIFOFloatCore(credits, debits)
			}
		default:
			exps, knocks = allocateFIFOFloatCore(credits, debits)
		}

		// Re-attach grouping metadata
		for i := range exps {
			exps[i].CompanyCode = company
			exps[i].Party = party
			exps[i].DocumentCurrency = curr
			exps[i].Source = src
		}

		allExps = append(allExps, exps...)
		allKnocks = append(allKnocks, knocks...)

		// Optional: debug info
		if len(exps) > 0 {
			log.Printf("[FIFO] Group %s → exposures=%d, knockoffs=%d", key, len(exps), len(knocks))
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
		Source:           src,
		CompanyCode:      getS("CompanyCode"),
		Party:            getS("Party"),
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

// Helper: parse uploaded file into [][]string
func ubParseUploadFile(file multipart.File, ext string) ([][]string, error) {
	if ext == ".csv" {
		r := csv.NewReader(file)
		return r.ReadAll()
	}
	if ext == ".xlsx" || ext == ".xls" {
		f, err := excelize.OpenReader(file)
		if err != nil {
			return nil, err
		}
		sheet := f.GetSheetName(0)
		rows, err := f.GetRows(sheet)
		if err != nil {
			return nil, err
		}
		return rows, nil
	}
	return nil, errors.New("unsupported file type")
}

// --- Request payload ---
type EditAllocationRequest struct {
    UserID  string `json:"user_id"`
    BatchID string `json:"batch_id"`
    Groups  []struct {
        Source      string `json:"source"`
        CompanyCode string `json:"company_code"`
        Party       string `json:"party"`
        Currency    string `json:"currency"`
        Allocations []struct {
            BaseDoc                string   `json:"base_document_id"`
            KnockDoc               string   `json:"knockoff_document_id"`
            AllocationAmountAbs    float64  `json:"allocation_amount_abs"`
            AllocationAmountSigned *float64 `json:"allocation_amount_signed"`
            Note                   string   `json:"note,omitempty"`
        } `json:"allocations"`
    } `json:"groups"`
}


func EditAllocationHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ctx := context.Background()

		var req EditAllocationRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeEditResponse(w, uuid.Nil, nil, []string{"invalid JSON payload: " + err.Error()}, nil, 0, 0, start)
			return
		}

		if len(req.Groups) == 0 {
			writeEditResponse(w, uuid.Nil, nil, []string{"no groups provided in request"}, nil, 0, 0, start)
			return
		}

		errorsList := []string{}
		infoList := []string{}
		totalInserted := 0

		conn, err := pool.Acquire(ctx)
		if err != nil {
			writeEditResponse(w, uuid.Nil, nil, []string{"db acquire failed: " + err.Error()}, nil, 0, 0, start)
			return
		}
		defer conn.Release()

		tx, err := conn.Begin(ctx)
		if err != nil {
			writeEditResponse(w, uuid.Nil, nil, []string{"tx begin failed: " + err.Error()}, nil, 0, 0, start)
			return
		}
		defer tx.Rollback(ctx)

		// Parse batch ID
		batchID, err := uuid.Parse(req.BatchID)
		if err != nil {
			writeEditResponse(w, uuid.Nil, nil, []string{"invalid batch_id: " + err.Error()}, nil, 0, 0, start)
			return
		}

		// Get file_hash for adjustment records
		var fileHash string
		_ = tx.QueryRow(ctx, `SELECT file_hash FROM public.exposure_unallocated WHERE batch_id=$1 LIMIT 1`, batchID).Scan(&fileHash)

		// Process each group
		for gi, grp := range req.Groups {
			groupKey := fmt.Sprintf("%s|%s|%s|%s", grp.Source, grp.CompanyCode, grp.Party, grp.Currency)
			log.Printf("[EDIT] Processing group[%d]: %s with %d allocations", gi, groupKey, len(grp.Allocations))

			// Check if group exists in batch
			var exists bool
			err := tx.QueryRow(ctx, `
				SELECT EXISTS (
					SELECT 1 FROM public.exposure_unallocated
					WHERE batch_id=$1 AND company_code=$2 AND party=$3 AND currency=$4 AND source=$5
				)
			`, batchID, grp.CompanyCode, grp.Party, grp.Currency, grp.Source).Scan(&exists)
			if err != nil {
				errorsList = append(errorsList, fmt.Sprintf("Group %s check failed: %v", groupKey, err))
				continue
			}
			if !exists {
				errorsList = append(errorsList, fmt.Sprintf("Group %s not found in batch %s", groupKey, req.BatchID))
				continue
			}

			// Delete old allocations for this group
			_, err = tx.Exec(ctx, `
				DELETE FROM public.exposure_allocations
				WHERE batch_id=$1
				  AND base_document_id IN (
					SELECT document_number FROM public.exposure_unallocated
					WHERE batch_id=$1 AND company_code=$2 AND party=$3 AND currency=$4 AND source=$5
				  )
			`, batchID, grp.CompanyCode, grp.Party, grp.Currency, grp.Source)
			if err != nil {
				errorsList = append(errorsList, fmt.Sprintf("Group %s: failed to delete old allocations: %v", groupKey, err))
				continue
			}

			// Insert new allocations from payload + save adjustment records
			groupInserted := 0
			for _, a := range grp.Allocations {
				if a.BaseDoc == "" || a.KnockDoc == "" {
					errorsList = append(errorsList, fmt.Sprintf("Group %s: skipped invalid allocation (missing doc)", groupKey))
					continue
				}
				if a.AllocationAmountAbs == 0 {
					continue // skip zero allocations
				}

				allocID := uuid.New()
				note := "manual-edit"
				if a.Note != "" {
					note = a.Note
				}
				_, err := tx.Exec(ctx, `
					INSERT INTO public.exposure_allocations
					(allocation_id, batch_id, file_hash, base_document_id, knockoff_document_id,
					allocation_amount, allocation_currency, allocation_amount_signed,
					allocation_date, created_at, created_by, notes)
					SELECT $1,$2, u.file_hash, $3,$4,$5,$6,$7,now(),now(),$8,$9
					FROM public.exposure_unallocated u
					WHERE u.batch_id=$2 AND u.document_number=$3
					LIMIT 1
				`, allocID, batchID, a.BaseDoc, a.KnockDoc, a.AllocationAmountAbs, grp.Currency, a.AllocationAmountSigned, req.UserID, note)
				if err != nil {
					errorsList = append(errorsList, fmt.Sprintf("Group %s: insert %s→%s failed: %v", groupKey, a.BaseDoc, a.KnockDoc, err))
				} else {
					groupInserted++

					// Save adjustment record
					adjustmentJSON, _ := json.Marshal(map[string]interface{}{
						"base_document":     a.BaseDoc,
						"knockoff_document": a.KnockDoc,
						"allocation_abs":    a.AllocationAmountAbs,
						"allocation_signed": a.AllocationAmountSigned,
						"currency":          grp.Currency,
						"group":             groupKey,
					})
					_, _ = tx.Exec(ctx, `
						INSERT INTO public.exposure_adjustments
						(batch_id, file_hash, reference_document_number, adjustment_type,
						adjustment_json, adjustment_amount, created_by, remarks)
						VALUES ($1,$2,$3,'manual_allocation',$4,$5,$6,$7)
					`, batchID, fileHash, a.BaseDoc, adjustmentJSON, a.AllocationAmountAbs, req.UserID, note)
				}
			}
			totalInserted += groupInserted
			if groupInserted > 0 {
				infoList = append(infoList, fmt.Sprintf("Group %s: inserted %d allocation(s)", groupKey, groupInserted))
			}
		}

		// Refresh batch exposures using canonical tables
		docStates, docOrder, err := recalcBatchExposures(ctx, tx, batchID)
		if err != nil {
			errorsList = append(errorsList, "recalculate batch failed: "+err.Error())
			writeEditResponse(w, batchID, nil, errorsList, infoList, totalInserted, 0, start)
			return
		}

		nonQualMap, nonQualOrder, err := loadNonQualifiedDocs(ctx, tx, batchID)
		if err != nil {
			errorsList = append(errorsList, "fetch non-qualified exposures failed: "+err.Error())
			writeEditResponse(w, batchID, nil, errorsList, infoList, totalInserted, 0, start)
			return
		}

		knockMap, err := loadKnockoffMap(ctx, tx, batchID)
		if err != nil {
			errorsList = append(errorsList, "fetch allocations failed: "+err.Error())
			writeEditResponse(w, batchID, nil, errorsList, infoList, totalInserted, 0, start)
			return
		}

		stagingOrder, stagingMeta, err := loadStagingMeta(ctx, tx, batchID)
		if err != nil {
			log.Printf("[WARN] load staging meta failed: %v", err)
		}

		rowsPreview := buildEditPreview(docStates, docOrder, nonQualMap, nonQualOrder, knockMap, stagingOrder, stagingMeta)

		if err := tx.Commit(ctx); err != nil {
			errorsList = append(errorsList, "commit failed: "+err.Error())
			writeEditResponse(w, batchID, nil, errorsList, infoList, totalInserted, 0, start)
			return
		}

		infoList = append(infoList, fmt.Sprintf("Successfully processed %d group(s), total %d allocations", len(req.Groups), totalInserted))
		writeEditResponse(w, batchID, rowsPreview, errorsList, infoList, totalInserted, len(rowsPreview), start)
	}
}

// --- Helper consistent response ---
func writeEditResponse(w http.ResponseWriter, batchID uuid.UUID, rows []CanonicalPreviewRow, errors, info []string, inserted, total int, start time.Time) {
	if rows == nil {
		rows = []CanonicalPreviewRow{}
	}
	if errors == nil {
		errors = []string{}
	}
	if info == nil {
		info = []string{}
	}

	result := UploadResult{
		FileName:      "manual_edit",
		Source:        "edit-allocation",
		BatchID:       batchID,
		TotalRows:     total,
		InsertedCount: inserted,
		LineItemsRows: 0,
		NonQualified:  []NonQualified{},
		Rows:          rows,
		Errors:        errors,
		Info:          info,
	}

	resp := map[string]interface{}{
		"duration": map[string]interface{}{"seconds": time.Since(start).Seconds()},
		"results":  []UploadResult{result},
		"success":  len(errors) == 0,
	}
	writeJSON(w, resp)
}

type docState struct {
	Meta      headerDocMeta
	Remaining decimal.Decimal
}

type headerDocMeta struct {
	DocNumber    string
	Company      string
	Party        string
	Currency     string
	Source       string
	DocumentDate string
	PostingDate  string
	NetDueDate   string
	Original     decimal.Decimal
	FileHash     string
	Raw          map[string]interface{}
}

type nonQualifiedDoc struct {
	Company      string
	Party        string
	Currency     string
	Source       string
	DocumentDate string
	PostingDate  string
	NetDueDate   string
	Amount       decimal.Decimal
	Issues       []string
}

type stagingDocMeta struct {
	Company      string
	Party        string
	Currency     string
	Source       string
	DocumentDate string
	PostingDate  string
	NetDueDate   string
	Amount       decimal.Decimal
}

func recalcBatchExposures(ctx context.Context, tx pgx.Tx, batchID uuid.UUID) (map[string]*docState, []string, error) {
	rows, err := tx.Query(ctx, `
		SELECT document_id, company_code, counterparty_code, currency, COALESCE(exposure_category,''),
		       document_date, posting_date, value_date, additional_header_details, file_hash
		FROM public.exposure_headers
		WHERE batch_id = $1
	`, batchID)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	docStates := make(map[string]*docState)
	docOrder := make([]string, 0)

	for rows.Next() {
		var (
			docID, company, party, currency, category, fileHash string
			docDate, postingDate, valueDate                     *time.Time
			addtlBytes                                          []byte
		)
		if err := rows.Scan(&docID, &company, &party, &currency, &category, &docDate, &postingDate, &valueDate, &addtlBytes, &fileHash); err != nil {
			return nil, nil, err
		}

		meta, err := buildHeaderMeta(docID, company, party, currency, category, fileHash, docDate, postingDate, valueDate, addtlBytes)
		if err != nil {
			return nil, nil, err
		}

		docStates[docID] = &docState{Meta: meta, Remaining: meta.Original}
		docOrder = append(docOrder, docID)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}

	sort.Strings(docOrder)

	baseSums, err := loadAllocationSums(ctx, tx, batchID, true)
	if err != nil {
		return nil, nil, err
	}
	knockSums, err := loadAllocationSums(ctx, tx, batchID, false)
	if err != nil {
		return nil, nil, err
	}

	epsilon := decimal.NewFromFloat(0.00005)
	for docID, state := range docStates {
		if sum, ok := baseSums[docID]; ok {
			if state.Meta.Original.Sign() >= 0 {
				state.Remaining = state.Remaining.Sub(sum)
			} else {
				state.Remaining = state.Remaining.Add(sum)
			}
		}
		if sum, ok := knockSums[docID]; ok {
			if state.Meta.Original.Sign() >= 0 {
				state.Remaining = state.Remaining.Sub(sum)
			} else {
				state.Remaining = state.Remaining.Add(sum)
			}
		}
		if state.Remaining.Abs().LessThan(epsilon) {
			state.Remaining = decimal.Zero
		}
	}

	if err := syncBatchUnallocated(ctx, tx, batchID, docStates); err != nil {
		return nil, nil, err
	}

	return docStates, docOrder, nil
}

func loadAllocationSums(ctx context.Context, tx pgx.Tx, batchID uuid.UUID, byBase bool) (map[string]decimal.Decimal, error) {
	column := "base_document_id"
	if !byBase {
		column = "knockoff_document_id"
	}
	query := fmt.Sprintf(`
		SELECT %s, COALESCE(SUM(allocation_amount),0)
		FROM public.exposure_allocations
		WHERE batch_id=$1
		GROUP BY %s
	`, column, column)

	rows, err := tx.Query(ctx, query, batchID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]decimal.Decimal)
	for rows.Next() {
		var docID, amtStr string
		if err := rows.Scan(&docID, &amtStr); err != nil {
			return nil, err
		}
		dec, err := decimal.NewFromString(strings.TrimSpace(amtStr))
		if err != nil {
			continue
		}
		result[docID] = dec
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func syncBatchUnallocated(ctx context.Context, tx pgx.Tx, batchID uuid.UUID, docStates map[string]*docState) error {
	if _, err := tx.Exec(ctx, `DELETE FROM public.exposure_unallocated WHERE batch_id=$1`, batchID); err != nil {
		return err
	}

	unallocCols := []string{
		"unallocated_id", "batch_id", "file_hash",
		"document_number", "company_code", "party",
		"currency", "source", "document_date",
		"posting_date", "net_due_date", "effective_due_date",
		"amount", "amount_signed", "exchange_rate",
		"amount_local_signed", "allocation_status",
		"mapped_payload", "created_at",
	}

	rows := make([][]any, 0, len(docStates))
	now := time.Now()
	for _, state := range docStates {
		if state.Remaining.IsZero() {
			continue
		}

		status := "unallocated"
		if state.Meta.Original.Abs().GreaterThan(state.Remaining.Abs()) {
			status = "partially_allocated"
		}

		rawCopy := copyStringInterfaceMap(state.Meta.Raw)
		rawCopy["AmountDoc"] = state.Remaining.StringFixed(4)
		mappedPayload, _ := json.Marshal(rawCopy)

		rows = append(rows, []any{
			uuid.New(),
			batchID,
			state.Meta.FileHash,
			state.Meta.DocNumber,
			state.Meta.Company,
			state.Meta.Party,
			state.Meta.Currency,
			state.Meta.Source,
			parseDateOrNil(state.Meta.DocumentDate),
			parseDateOrNil(state.Meta.PostingDate),
			parseDateOrNil(state.Meta.NetDueDate),
			parseDateOrNil(state.Meta.NetDueDate),
			state.Remaining.Abs().StringFixed(4),
			state.Remaining.StringFixed(4),
			nil,
			nil,
			status,
			mappedPayload,
			now,
		})
	}

	if len(rows) > 0 {
		if _, err := tx.CopyFrom(ctx, pgx.Identifier{"public", "exposure_unallocated"}, unallocCols, pgx.CopyFromRows(rows)); err != nil {
			return err
		}
	}

	if len(docStates) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	for _, state := range docStates {
		status := "Open"
		open := state.Remaining.StringFixed(4)
		if state.Remaining.IsZero() {
			status = "Closed"
			open = "0.0000"
		}
		batch.Queue(`UPDATE public.exposure_headers SET total_open_amount=$1, status=$2, updated_at=now() WHERE batch_id=$3 AND document_id=$4`,
			open, status, batchID, state.Meta.DocNumber)
	}

	br := tx.SendBatch(ctx, batch)
	if br != nil {
		if err := br.Close(); err != nil {
			return err
		}
	}

	return nil
}

func loadNonQualifiedDocs(ctx context.Context, tx pgx.Tx, batchID uuid.UUID) (map[string]*nonQualifiedDoc, []string, error) {
	rows, err := tx.Query(ctx, `
		SELECT document_number, company_code, party, currency, source,
		       document_date, posting_date, net_due_date, amount, issues, mapped_payload
		FROM public.exposure_unqualified
		WHERE batch_id=$1
	`, batchID)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	result := make(map[string]*nonQualifiedDoc)
	order := make([]string, 0)

	for rows.Next() {
		var (
			docNum, company, party, currency, source string
			docDate, postingDate, netDueDate         *time.Time
			amountStr                                string
			issues                                   []string
			payload                                  []byte
		)
		if err := rows.Scan(&docNum, &company, &party, &currency, &source, &docDate, &postingDate, &netDueDate, &amountStr, &issues, &payload); err != nil {
			return nil, nil, err
		}

		metaMap := map[string]interface{}{}
		_ = json.Unmarshal(payload, &metaMap)

		doc := &nonQualifiedDoc{
			Company:      company,
			Party:        party,
			Currency:     currency,
			Source:       firstNonEmpty(source, stringFromAny(metaMap["Source"])),
			DocumentDate: firstNonEmpty(formatDate(docDate), stringFromAny(metaMap["DocumentDate"])),
			PostingDate:  firstNonEmpty(formatDate(postingDate), stringFromAny(metaMap["PostingDate"])),
			NetDueDate:   firstNonEmpty(formatDate(netDueDate), stringFromAny(metaMap["NetDueDate"])),
			Amount:       parseDecimalFromString(amountStr),
			Issues:       append([]string(nil), issues...),
		}

		result[docNum] = doc
		order = append(order, docNum)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}

	sort.Strings(order)
	return result, order, nil
}

func loadKnockoffMap(ctx context.Context, tx pgx.Tx, batchID uuid.UUID) (map[string][]KnockoffInfo, error) {
	rows, err := tx.Query(ctx, `
		SELECT base_document_id, knockoff_document_id, allocation_amount
		FROM public.exposure_allocations
		WHERE batch_id=$1
		ORDER BY base_document_id, allocation_date
	`, batchID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	knockMap := make(map[string][]KnockoffInfo)
	for rows.Next() {
		var base, knock, amtStr string
		if err := rows.Scan(&base, &knock, &amtStr); err != nil {
			return nil, err
		}
		amt, err := decimal.NewFromString(strings.TrimSpace(amtStr))
		if err != nil {
			continue
		}
		knockMap[base] = append(knockMap[base], KnockoffInfo{BaseDoc: base, KnockDoc: knock, AmtAbs: amt})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return knockMap, nil
}

func buildEditPreview(
	docStates map[string]*docState,
	docOrder []string,
	nonQualMap map[string]*nonQualifiedDoc,
	nonQualOrder []string,
	knockMap map[string][]KnockoffInfo,
	stagingOrder []string,
	stagingMeta map[string]*stagingDocMeta,
) []CanonicalPreviewRow {
	rows := make([]CanonicalPreviewRow, 0, len(stagingOrder)+len(docOrder)+len(nonQualOrder))
	added := make(map[string]bool)

	knockParticipants := make(map[string]bool)
	for base, kos := range knockMap {
		if len(kos) > 0 && docStates[base] == nil {
			knockParticipants[base] = true
		}
		for _, ko := range kos {
			knockParticipants[ko.KnockDoc] = true
		}
	}

	appendDoc := func(doc string) {
		doc = strings.TrimSpace(doc)
		if doc == "" || added[doc] {
			return
		}
		state := docStates[doc]
		nq := nonQualMap[doc]
		meta := stagingMeta[doc]
		row := composePreviewRow(doc, state, nq, meta, knockMap, knockParticipants)
		rows = append(rows, row)
		added[doc] = true
	}

	for _, doc := range stagingOrder {
		appendDoc(doc)
	}

	for _, doc := range docOrder {
		appendDoc(doc)
	}

	for _, doc := range nonQualOrder {
		appendDoc(doc)
	}

	for _, kos := range knockMap {
		for _, ko := range kos {
			appendDoc(ko.KnockDoc)
		}
	}

	return rows
}

func composePreviewRow(doc string, state *docState, nq *nonQualifiedDoc, meta *stagingDocMeta, knockMap map[string][]KnockoffInfo, knockParticipants map[string]bool) CanonicalPreviewRow {
	pr := CanonicalPreviewRow{DocumentNumber: doc}

	company := ""
	party := ""
	currency := ""
	source := ""
	docDate := ""
	postDate := ""
	dueDate := ""
	amount := decimal.Zero

	if state != nil {
		company = state.Meta.Company
		party = state.Meta.Party
		currency = state.Meta.Currency
		source = state.Meta.Source
		docDate = state.Meta.DocumentDate
		postDate = state.Meta.PostingDate
		dueDate = state.Meta.NetDueDate
		amount = state.Meta.Original
	}

	if meta != nil {
		if company == "" {
			company = meta.Company
		}
		if party == "" {
			party = meta.Party
		}
		if currency == "" {
			currency = meta.Currency
		}
		if source == "" {
			source = meta.Source
		}
		if docDate == "" {
			docDate = meta.DocumentDate
		}
		if postDate == "" {
			postDate = meta.PostingDate
		}
		if dueDate == "" {
			dueDate = meta.NetDueDate
		}
		if amount.IsZero() {
			amount = meta.Amount
		}
	}

	if nq != nil {
		if company == "" {
			company = nq.Company
		}
		if party == "" {
			party = nq.Party
		}
		if currency == "" {
			currency = nq.Currency
		}
		if source == "" {
			source = nq.Source
		}
		if docDate == "" {
			docDate = nq.DocumentDate
		}
		if postDate == "" {
			postDate = nq.PostingDate
		}
		if dueDate == "" {
			dueDate = nq.NetDueDate
		}
		if amount.IsZero() {
			amount = nq.Amount
		}
	}

	pr.CompanyCode = company
	pr.Party = party
	pr.Currency = currency
	pr.Source = source
	pr.DocumentDate = docDate
	pr.PostingDate = postDate
	pr.NetDueDate = dueDate
	pr.Amount = amount

	switch {
	case nq != nil:
		pr.Status = "non_qualified"
		pr.Issues = append([]string(nil), nq.Issues...)
	case state != nil:
		if state.Remaining.IsZero() {
			pr.Status = "knocked_off"
		} else {
			pr.Status = "ok"
		}
	default:
		if knockParticipants[doc] {
			pr.Status = "knocked_off"
		} else {
			pr.Status = "ok"
		}
	}

	if pr.Status == "knocked_off" && amount.IsZero() && meta != nil {
		pr.Amount = meta.Amount
	}

	if kos, ok := knockMap[doc]; ok {
		pr.Knockoffs = kos
	}

	return pr
}

func loadStagingMeta(ctx context.Context, tx pgx.Tx, batchID uuid.UUID) ([]string, map[string]*stagingDocMeta, error) {
	rows, err := tx.Query(ctx, `
		SELECT mapped_payload, ingestion_timestamp
		FROM public.staging_exposures
		WHERE batch_id=$1
		ORDER BY ingestion_timestamp
	`, batchID)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	order := make([]string, 0)
	meta := make(map[string]*stagingDocMeta)

	for rows.Next() {
		var (
			payload []byte
			ts      time.Time
		)
		if err := rows.Scan(&payload, &ts); err != nil {
			return nil, nil, err
		}

		mapped := make(map[string]interface{})
		if err := json.Unmarshal(payload, &mapped); err != nil {
			continue
		}

		docNum := strings.TrimSpace(stringFromAny(mapped["DocumentNumber"]))
		if docNum == "" {
			continue
		}
		if _, exists := meta[docNum]; exists {
			continue
		}

		md := &stagingDocMeta{
			Company:      firstNonEmpty(stringFromAny(mapped["CompanyCode"]), stringFromAny(mapped["company_code"])),
			Party:        firstNonEmpty(stringFromAny(mapped["Party"]), stringFromAny(mapped["party"])),
			Currency:     strings.ToUpper(firstNonEmpty(stringFromAny(mapped["DocumentCurrency"]), stringFromAny(mapped["document_currency"]))),
			Source:       firstNonEmpty(stringFromAny(mapped["Source"]), stringFromAny(mapped["source"])),
			DocumentDate: firstNonEmpty(stringFromAny(mapped["DocumentDate"]), stringFromAny(mapped["document_date"])),
			PostingDate:  firstNonEmpty(stringFromAny(mapped["PostingDate"]), stringFromAny(mapped["posting_date"])),
			NetDueDate:   firstNonEmpty(stringFromAny(mapped["NetDueDate"]), stringFromAny(mapped["net_due_date"])),
			Amount:       parseDecimalFromAny(mapped["AmountDoc"]),
		}
		if md.Amount.IsZero() {
			md.Amount = parseDecimalFromAny(mapped["amount_doc"])
		}
		if md.Amount.IsZero() {
			md.Amount = parseDecimalFromAny(mapped["amount"])
		}

		meta[docNum] = md
		order = append(order, docNum)
	}

	if err := rows.Err(); err != nil {
		return nil, nil, err
	}

	return order, meta, nil
}

func buildHeaderMeta(docID, company, party, currency, category, fileHash string, docDate, postingDate, valueDate *time.Time, addtl []byte) (headerDocMeta, error) {
	meta := headerDocMeta{
		DocNumber:    docID,
		Company:      company,
		Party:        party,
		Currency:     currency,
		Source:       category,
		DocumentDate: formatDate(docDate),
		PostingDate:  formatDate(postingDate),
		NetDueDate:   formatDate(valueDate),
		Original:     decimal.Zero,
		FileHash:     fileHash,
		Raw:          map[string]interface{}{},
	}

	if len(addtl) > 0 {
		if err := json.Unmarshal(addtl, &meta.Raw); err != nil {
			return meta, err
		}
	}

	if v := stringFromAny(meta.Raw["Source"]); v != "" {
		meta.Source = v
	}
	if v := stringFromAny(meta.Raw["DocumentDate"]); v != "" {
		meta.DocumentDate = v
	}
	if v := stringFromAny(meta.Raw["PostingDate"]); v != "" {
		meta.PostingDate = v
	}
	if v := stringFromAny(meta.Raw["NetDueDate"]); v != "" {
		meta.NetDueDate = v
	}

	if dec := parseDecimalFromAny(meta.Raw["AmountDoc"]); !dec.IsZero() {
		meta.Original = dec
	}

	if meta.Source == "" {
		meta.Source = category
	}

	return meta, nil
}

func copyStringInterfaceMap(src map[string]interface{}) map[string]interface{} {
	if src == nil {
		return map[string]interface{}{}
	}
	dst := make(map[string]interface{}, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func stringFromAny(v interface{}) string {
	switch t := v.(type) {
	case string:
		return strings.TrimSpace(t)
	case fmt.Stringer:
		return strings.TrimSpace(t.String())
	case float64:
		return strings.TrimSpace(strconv.FormatFloat(t, 'f', -1, 64))
	case int, int32, int64:
		return strings.TrimSpace(fmt.Sprintf("%v", t))
	default:
		return ""
	}
}

func parseDecimalFromAny(v interface{}) decimal.Decimal {
	switch t := v.(type) {
	case string:
		return parseDecimalFromString(t)
	case float64:
		return decimal.NewFromFloat(t)
	case json.Number:
		if f, err := t.Float64(); err == nil {
			return decimal.NewFromFloat(f)
		}
	}
	return decimal.Zero
}

func parseDecimalFromString(s string) decimal.Decimal {
	clean := strings.ReplaceAll(strings.TrimSpace(s), ",", "")
	if clean == "" {
		return decimal.Zero
	}
	dec, err := decimal.NewFromString(clean)
	if err != nil {
		return decimal.Zero
	}
	return dec
}

func formatDate(t *time.Time) string {
	if t == nil {
		return ""
	}
	return t.Format("2006-01-02")
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}


func EditAllocationsHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		// parse request
		var req EditAllocationRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			httpError(w, http.StatusBadRequest, "invalid json: "+err.Error())
			return
		}
		if req.BatchID == "" {
			httpError(w, http.StatusBadRequest, "batch_id required")
			return
		}
		batchUUID, err := uuid.Parse(req.BatchID)
		if err != nil {
			httpError(w, http.StatusBadRequest, "invalid batch_id")
			return
		}

		// acquire connection and begin tx
		conn, err := pool.Acquire(ctx)
		if err != nil {
			httpError(w, 500, "db acquire: "+err.Error())
			return
		}
		defer conn.Release()

		tx, err := conn.Begin(ctx)
		if err != nil {
			httpError(w, 500, "tx begin: "+err.Error())
			return
		}
		committed := false
		defer func() {
			if !committed {
				_ = tx.Rollback(ctx)
			}
		}()

		// ensure batch exists and is completed; fetch file_hash for inserts
		var batchStatus string
		var fileHash sql.NullString
		if err := tx.QueryRow(ctx, `SELECT status, file_hash FROM public.staging_batches_exposures WHERE batch_id=$1`, batchUUID).Scan(&batchStatus, &fileHash); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				httpError(w, 404, "batch not found")
				return
			}
			httpError(w, 500, "batch lookup: "+err.Error())
			return
		}
		if strings.ToLower(batchStatus) != "completed" {
			httpError(w, http.StatusBadRequest, "batch not in 'completed' status")
			return
		}

		// Build validation structures from request:
		type reqAllocItem struct {
			BaseDoc string
			Knock   string
			AbsAmt  decimal.Decimal
			SignAmt *decimal.Decimal // optional, nil if not provided
			Group   struct {
				Source      string
				CompanyCode string
				Party       string
				Currency    string
			}
		}
		reqAllocs := make([]reqAllocItem, 0, 128)
		reqSum := map[string]decimal.Decimal{} // base -> sum(abs)

		for _, g := range req.Groups {
			for _, a := range g.Allocations {
				abs := decimal.NewFromFloat(0)
				if a.AllocationAmountAbs != 0 {
					abs = decimal.NewFromFloat(math.Abs(a.AllocationAmountAbs))
				}
				var signPtr *decimal.Decimal
				if a.AllocationAmountSigned != nil {
					sd := decimal.NewFromFloat(*a.AllocationAmountSigned)
					signPtr = &sd
				}
				item := reqAllocItem{
					BaseDoc: strings.TrimSpace(a.BaseDoc),
					Knock:   strings.TrimSpace(a.KnockDoc),
					AbsAmt:  abs,
					SignAmt: signPtr,
				}
				item.Group.Source = g.Source
				item.Group.CompanyCode = g.CompanyCode
				item.Group.Party = g.Party
				item.Group.Currency = g.Currency

				reqAllocs = append(reqAllocs, item)
				if _, ok := reqSum[item.BaseDoc]; !ok {
					reqSum[item.BaseDoc] = decimal.Zero
				}
				reqSum[item.BaseDoc] = reqSum[item.BaseDoc].Add(abs)
			}
		}

		// If no allocations requested -> return preview (no-op)
		if len(reqAllocs) == 0 {
			_ = tx.Rollback(ctx)
			previewRes, statusCode, err := buildPreviewForBatch(pool, ctx, batchUUID)
			if err != nil {
				httpError(w, 500, "preview build: "+err.Error())
				return
			}
			previewRes.Errors = []string{"no allocations supplied"}
			writeJSON(w, map[string]interface{}{
				"success": false,
				"results": []UploadResult{previewRes},
			})
			w.WriteHeader(statusCode)
			return
		}

		// Query DB to fetch availability/currency for every base doc
		baseDocsArr := make([]string, 0, len(reqSum))
		for k := range reqSum {
			baseDocsArr = append(baseDocsArr, strings.TrimSpace(k))
		}

		// robust fetch: prefer exposure_unallocated.amount when present else compute from headers - allocations
		rows, err := tx.Query(ctx, `
      SELECT
        h.document_id,
        COALESCE(u.amount,
                 GREATEST( (ABS(h.total_original_amount) - COALESCE(a.allocated_abs,0) ), 0 )
        ) AS available_abs,
        u.amount_signed AS remaining_signed,
        COALESCE(u.currency, h.currency) AS currency
      FROM public.exposure_headers h
      LEFT JOIN public.exposure_unallocated u
        ON u.batch_id = $1 AND u.document_number = h.document_id
      LEFT JOIN (
        SELECT base_document_id, SUM(ABS(allocation_amount::numeric)) AS allocated_abs
        FROM public.exposure_allocations
        WHERE batch_id = $1 AND base_document_id = ANY($2::text[])
        GROUP BY base_document_id
      ) a ON a.base_document_id = h.document_id
      WHERE h.batch_id = $1 AND h.document_id = ANY($2::text[])
    `, batchUUID, baseDocsArr)
		if err != nil {
			httpError(w, 500, "fetch headers/unallocated for validation: "+err.Error())
			return
		}
		defer rows.Close()

		type baseInfo struct {
			Amount          decimal.Decimal  // available absolute (>=0)
			RemainingSigned *decimal.Decimal // if exposure_unallocated.amount_signed exists (nil otherwise)
			Currency        string
		}
		dbBase := map[string]baseInfo{}
		for rows.Next() {
			var doc string
			var avail sql.NullString
			var remSigned sql.NullFloat64
			var cur sql.NullString
			if err := rows.Scan(&doc, &avail, &remSigned, &cur); err != nil {
				continue
			}

			availDec := decimal.Zero
			if avail.Valid && strings.TrimSpace(avail.String) != "" {
				if d, derr := decimal.NewFromString(avail.String); derr == nil {
					availDec = d
				}
			}

			var remPtr *decimal.Decimal
			if remSigned.Valid {
				s := decimal.NewFromFloat(remSigned.Float64)
				remPtr = &s
			} else {
				remPtr = nil
			}

			dbBase[doc] = baseInfo{
				Amount:          availDec,
				RemainingSigned: remPtr,
				Currency:        cur.String,
			}
		}

		// Validation checks: missing base, over-allocation, cross-currency (if group.currency provided)
		errorsList := make([]string, 0)
		for base, reqTotal := range reqSum {
			info, ok := dbBase[base]
			if !ok {
				errorsList = append(errorsList, fmt.Sprintf("Base document not found in batch: %s", base))
				continue
			}
			// compare absolute sums
			if reqTotal.GreaterThan(info.Amount) {
				errorsList = append(errorsList, fmt.Sprintf("Allocation exceeds base amount for document %s (requested abs %s > available %s)", base, reqTotal.String(), info.Amount.String()))
			}
		}
		// cross-currency validation per-allocation if group currency present
		for _, a := range reqAllocs {
			if a.Group.Currency != "" {
				if info, ok := dbBase[a.BaseDoc]; ok && info.Currency != "" {
					if !strings.EqualFold(info.Currency, a.Group.Currency) {
						errorsList = append(errorsList, fmt.Sprintf("Cross-currency allocation not allowed for base %s -> knock %s (%s != %s)", a.BaseDoc, a.Knock, info.Currency, a.Group.Currency))
					}
				}
			}
		}

		// If validation errors — rollback and return preview with errors (full rows)
		if len(errorsList) > 0 {
			_ = tx.Rollback(ctx) // don't apply anything
			previewRes, _, err := buildPreviewForBatch(pool, ctx, batchUUID)
			if err != nil {
				httpError(w, 500, "preview build after validation error: "+err.Error())
				return
			}
			previewRes.Errors = errorsList
			writeJSON(w, map[string]interface{}{
				"success": false,
				"results": []UploadResult{previewRes},
			})
			return
		}

		// Passed validation -> apply replace semantics (delete existing allocations for this batch, insert new ones)
		if _, err := tx.Exec(ctx, `DELETE FROM public.exposure_allocations WHERE batch_id = $1`, batchUUID); err != nil {
			httpError(w, 500, "delete old allocations: "+err.Error())
			return
		}

		// Prepare insert statement
		insertAllocStmt := `
      INSERT INTO public.exposure_allocations
      (allocation_id, batch_id, file_hash, base_document_id, knockoff_document_id, allocation_amount, allocation_currency, allocation_amount_signed, allocation_date, created_at, created_by, notes)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
    `
		insertedCount := 0
		for _, a := range reqAllocs {
			amtAbsStr := a.AbsAmt.StringFixed(4)
			allocCurr := sql.NullString{Valid: false}
			if a.Group.Currency != "" {
				allocCurr = sql.NullString{String: a.Group.Currency, Valid: true}
			}

			// compute allocation_amount_signed: prefer provided, else infer sign from DB remaining_signed
			var allocSigned decimal.Decimal
			if a.SignAmt != nil {
				allocSigned = *a.SignAmt
			} else {
				if info, ok := dbBase[a.BaseDoc]; ok {
					if info.RemainingSigned == nil || (*info.RemainingSigned).IsZero() {
						// fallback: positive
						allocSigned = a.AbsAmt
					} else {
						// infer sign from remaining_signed sign
						signFloat := (*info.RemainingSigned).InexactFloat64()
						allocSigned = a.AbsAmt.Mul(decimal.NewFromFloat(math.Copysign(1.0, signFloat)))
					}
				} else {
					allocSigned = a.AbsAmt
				}
			}
			allocSignedStr := allocSigned.StringFixed(4)

			if _, err := tx.Exec(ctx, insertAllocStmt,
				uuid.New(),
				batchUUID,
				fileHash.String,
				a.BaseDoc,
				a.Knock,
				amtAbsStr,
				func() interface{} {
					if allocCurr.Valid {
						return allocCurr.String
					}
					return nil
				}(),
				allocSignedStr,
				time.Now(),
				time.Now(),
				req.UserID,
				nil,
			); err != nil {
				_ = tx.Rollback(ctx)
				httpError(w, 500, "insert allocation: "+err.Error())
				return
			}
			insertedCount++
		}

		// Recompute allocation sums and update exposure_unallocated (remaining signed) and allocation_status
		updateUnallocSQL := `
      UPDATE public.exposure_unallocated u
      SET
        amount_signed = (COALESCE(u.amount_signed,0) - COALESCE(sub.allocated_sum_signed,0))::numeric(19,4),
        allocation_status = CASE
          WHEN COALESCE(sub.allocated_sum_abs,0) = 0 THEN 'unallocated'
          WHEN COALESCE(sub.allocated_sum_abs,0) >= ABS(COALESCE(u.amount,0))::numeric - 0.0001 THEN 'fully_allocated'
          ELSE 'partially_allocated'
        END
      FROM (
        SELECT base_document_id,
               SUM(COALESCE(allocation_amount_signed::numeric,0)) AS allocated_sum_signed,
               SUM(ABS(COALESCE(allocation_amount::numeric,0))) AS allocated_sum_abs
        FROM public.exposure_allocations
        WHERE batch_id = $1
        GROUP BY base_document_id
      ) sub
      WHERE u.batch_id = $1 AND u.document_number = sub.base_document_id
    `
		if _, err := tx.Exec(ctx, updateUnallocSQL, batchUUID); err != nil {
			httpError(w, 500, "update unallocated: "+err.Error())
			return
		}

		// IMPORTANT: update exposure_headers.total_open_amount to reflect remaining absolute open amount (parallel update)
		// Use ABS(amount_signed) if present, else fall back to ABS(amount) (amount holds original absolute)
		updateHeaderSQL := `
      UPDATE public.exposure_headers h
      SET total_open_amount = COALESCE(ABS(u.amount_signed)::numeric, ABS(u.amount)::numeric, 0)
      FROM public.exposure_unallocated u
      WHERE h.batch_id = $1 AND h.document_id = u.document_number AND u.batch_id = $1
    `
		if _, err := tx.Exec(ctx, updateHeaderSQL, batchUUID); err != nil {
			httpError(w, 500, "update headers: "+err.Error())
			return
		}

		// Update processed_records in staging_batches_exposures (best-effort)
		if _, err := tx.Exec(ctx, `UPDATE public.staging_batches_exposures SET processed_records = (
        SELECT COUNT(*) FROM public.exposure_headers WHERE batch_id = $1
      ) WHERE batch_id = $1`, batchUUID); err != nil {
			log.Printf("[WARN] update processed_records failed: %v", err)
		}

		// commit tx
		if err := tx.Commit(ctx); err != nil {
			httpError(w, 500, "commit failed: "+err.Error())
			return
		}
		committed = true

		// Build fresh preview after commit
		previewRes, _, err := buildPreviewForBatch(pool, ctx, batchUUID)
		if err != nil {
			httpError(w, 500, "preview build after commit: "+err.Error())
			return
		}
		previewRes.Info = append(previewRes.Info, fmt.Sprintf("Edit applied: %d allocations inserted (replaced previous allocations) for batch %s by user %s", insertedCount, batchUUID.String(), req.UserID))

		writeJSON(w, map[string]interface{}{
			"success": true,
			"results": []UploadResult{previewRes},
		})
	}
}

// buildPreviewForBatch builds an UploadResult preview for the batch. It reads current DB state (no writes).
// Returns UploadResult, httpStatus (useful for passing along non-200 if needed), error
func buildPreviewForBatch(pool *pgxpool.Pool, ctx context.Context, batchUUID uuid.UUID) (UploadResult, int, error) {
	var res UploadResult
	// acquire a connection for read-only preview building
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return res, 500, err
	}
	defer conn.Release()

	// fetch staging batch metadata
	var totalRecords sql.NullInt64
	var fileName sql.NullString
	var ingestionSource sql.NullString
	var fileHash sql.NullString
	if err := conn.QueryRow(ctx, `SELECT total_records, file_name, ingestion_source, file_hash FROM public.staging_batches_exposures WHERE batch_id=$1`, batchUUID).
		Scan(&totalRecords, &fileName, &ingestionSource, &fileHash); err != nil {
		return res, 500, err
	}

	// count line items inserted for this batch
	var liCount int
	if err := conn.QueryRow(ctx, `
      SELECT COALESCE(COUNT(li.*),0) FROM public.exposure_line_items li
      JOIN public.exposure_headers h ON li.exposure_header_id = h.exposure_header_id
      WHERE h.batch_id = $1
    `, batchUUID).Scan(&liCount); err != nil {
		return res, 500, err
	}

	// fetch headers + unallocated info
	rows, err := conn.Query(ctx, `
      SELECT h.document_id, COALESCE(h.company_code,'') AS company_code,
             COALESCE(u.party, h.counterparty_code, '') AS party,
             COALESCE(h.currency, u.currency, '') AS currency,
             COALESCE(h.additional_header_details->>'Source', sb.ingestion_source, '') AS source,
             h.document_date, h.posting_date, u.net_due_date,
             COALESCE(u.amount_signed::text, NULL) AS amount_signed_text,
             COALESCE(h.total_original_amount::text, '') AS total_original_amount_text,
             COALESCE(u.allocation_status, 'unallocated') AS allocation_status
      FROM public.exposure_headers h
      LEFT JOIN public.exposure_unallocated u ON u.batch_id = h.batch_id AND u.document_number = h.document_id
      LEFT JOIN public.staging_batches_exposures sb ON sb.batch_id = h.batch_id
      WHERE h.batch_id = $1
      ORDER BY h.document_id
    `, batchUUID)
	if err != nil {
		return res, 500, err
	}
	defer rows.Close()

	// load allocations grouped by base doc
	allocMap := map[string][]KnockoffInfo{}
	allocRows, err := conn.Query(ctx, `
      SELECT base_document_id, knockoff_document_id, allocation_amount
      FROM public.exposure_allocations
      WHERE batch_id = $1
      ORDER BY base_document_id
    `, batchUUID)
	if err == nil {
		for allocRows.Next() {
			var base, knock string
			var amtStr string
			if err := allocRows.Scan(&base, &knock, &amtStr); err != nil {
				continue
			}
			if d, derr := decimal.NewFromString(amtStr); derr == nil {
				allocMap[base] = append(allocMap[base], KnockoffInfo{
					BaseDoc:  base,
					KnockDoc: knock,
					AmtAbs:   d,
				})
			} else {
				// fallback zero
				allocMap[base] = append(allocMap[base], KnockoffInfo{
					BaseDoc:  base,
					KnockDoc: knock,
					AmtAbs:   decimal.Zero,
				})
			}
		}
		allocRows.Close()
	}

	previewRows := make([]CanonicalPreviewRow, 0)
	insertedCountPreview := 0
	for rows.Next() {
		var docID, company, party, currency, source, amountSignedText, totalOrigText, allocationStatus sql.NullString
		var docDate, postDate, netDue sql.NullTime
		if err := rows.Scan(&docID, &company, &party, &currency, &source, &docDate, &postDate, &netDue, &amountSignedText, &totalOrigText, &allocationStatus); err != nil {
			continue
		}

		amountStr := ""
		// prefer amount_signed_text if present, otherwise fall back to negative total_original_amount for payables
		if amountSignedText.Valid && strings.TrimSpace(amountSignedText.String) != "" {
			amountStr = strings.TrimSpace(amountSignedText.String)
		} else if totalOrigText.Valid && strings.TrimSpace(totalOrigText.String) != "" {
			// infer sign by source if possible (FBL1N/FBL3N => negative)
			src := strings.TrimSpace(source.String)
			if src != "" && (strings.EqualFold(src, "FBL1N") || strings.EqualFold(src, "FBL3N")) {
				amountStr = "-" + strings.TrimSpace(totalOrigText.String)
			} else {
				amountStr = strings.TrimSpace(totalOrigText.String)
			}
		}

		statusVal := "ok"
		if allocationStatus.Valid {
			switch allocationStatus.String {
			case "fully_allocated":
				statusVal = "knocked_off"
			case "partially_allocated":
				statusVal = "ok"
			case "unallocated":
				statusVal = "ok"
			}
		}

		pr := CanonicalPreviewRow{
			DocumentNumber: docID.String,
			CompanyCode:    company.String,
			Party:          party.String,
			Currency:       currency.String,
			Source:         source.String,
			DocumentDate:   "",
			PostingDate:    "",
			NetDueDate:     "",
			Status:         statusVal,
			Issues:         nil,
			Knockoffs:      nil,
		}
		if docDate.Valid {
			pr.DocumentDate = docDate.Time.Format(constants.DateFormat)
		}
		if postDate.Valid {
			pr.PostingDate = postDate.Time.Format(constants.DateFormat)
		}
		if netDue.Valid {
			pr.NetDueDate = netDue.Time.Format(constants.DateFormat)
		}
		// parse amountStr into decimal
		if amountStr != "" {
			if d, derr := decimal.NewFromString(amountStr); derr == nil {
				pr.Amount = d
			} else {
				pr.Amount = decimal.Zero
			}
		} else {
			pr.Amount = decimal.Zero
		}

		if kos, ok := allocMap[pr.DocumentNumber]; ok {
			pr.Knockoffs = kos
		}

		if statusVal != "non_qualified" {
			insertedCountPreview++
		}
		previewRows = append(previewRows, pr)
	}

	// build result
	res = UploadResult{
		FileName:      fileName.String,
		Source:        ingestionSource.String,
		BatchID:       batchUUID,
		TotalRows:     int(totalRecords.Int64),
		InsertedCount: insertedCountPreview,
		LineItemsRows: liCount,
		NonQualified:  []NonQualified{},
		Rows:          previewRows,
		Errors:        []string{},
		Warnings:      []string{},
		Info:          []string{},
	}

	return res, 200, nil
}
