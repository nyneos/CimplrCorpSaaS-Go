package exposures

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/approvalengine"
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/api/fx/auditutil"
	fxexposures "CimplrCorpSaas/api/fx/exposures"
	fxnotif "CimplrCorpSaas/api/fx/notification"
	"CimplrCorpSaas/api/policyengine/common"
	policyruntime "CimplrCorpSaas/api/policyengine/runtime"
	s3storage "CimplrCorpSaas/api/utils/s3storage"
	"context"
	"database/sql"

	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"

	"strconv"
	"strings"
	"sync"
	"unicode/utf8"

	"time"

	"golang.org/x/text/encoding/charmap"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"
	"github.com/xuri/excelize/v2"

	"CimplrCorpSaas/internal/logger"
)

// const duplicateExposureUploadMessage = "This exposure file was already uploaded earlier. Please upload a different file."

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

type UploadRouteResult struct {
	UploadResult
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
	BaseDoc      string          `json:"base"`
	KnockDoc     string          `json:"knock"`
	AmtAbs       decimal.Decimal `json:"amt_abs"`
	Currency     string          `json:"currency,omitempty"`
	SignedAmt    decimal.Decimal `json:"-"`
	DocumentDate string          `json:"-"`
	PostingDate  string          `json:"-"`
	NetDueDate   string          `json:"-"`
}

// internal shape used by allocateFIFOFloat
type knockFloatInput struct {
	BaseDoc  string
	KnockDoc string
	AmtFloat float64
	Currency string
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

// sanitizeUTF8 replaces invalid UTF-8 sequences with the Unicode replacement character
func sanitizeUTF8(b []byte) []byte {
	return []byte(sanitizeUTF8String(string(b)))
}

// sanitizeUTF8String cleans text for PostgreSQL UTF8 columns.
// SAP/Excel exports often contain Windows-1252 bytes (e.g. 0xA0 NBSP) that are invalid in UTF-8.
func sanitizeUTF8String(s string) string {
	if s == "" {
		return s
	}
	if !utf8.ValidString(s) {
		if decoded, err := charmap.Windows1252.NewDecoder().String(s); err == nil {
			s = decoded
		}
	}
	s = strings.ToValidUTF8(s, " ")
	return strings.ReplaceAll(s, "\u00a0", " ")
}

func decodeUploadText(raw []byte) string {
	if len(raw) == 0 {
		return ""
	}
	// Strip UTF-8 BOM if present.
	raw = bytesTrimPrefix(raw, []byte{0xEF, 0xBB, 0xBF})
	if utf8.Valid(raw) {
		return sanitizeUTF8String(string(raw))
	}
	if decoded, err := charmap.Windows1252.NewDecoder().Bytes(raw); err == nil {
		return sanitizeUTF8String(string(decoded))
	}
	return sanitizeUTF8String(string(raw))
}

func bytesTrimPrefix(b, prefix []byte) []byte {
	if len(b) >= len(prefix) && string(b[:len(prefix)]) == string(prefix) {
		return b[len(prefix):]
	}
	return b
}

func BatchUploadStagingData(pool *pgxpool.Pool) http.HandlerFunc {
	runtime.GOMAXPROCS(runtime.NumCPU())

	return func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseMultipartForm(1024 << 20); err != nil {
			httpError(w, http.StatusBadRequest, "multipart parse error: "+err.Error())
			return
		}
		if !policyruntime.Enforce(r.Context(), w, r, pool, policyruntime.EnforceInput{
			EventCode:           common.TriggerPreUpload,
			ModuleCode:          common.ModuleFX,
			SubModule:           "EXPOSURE_CREATION",
			ActorUserID:         r.FormValue(constants.KeyUserID),
			HandlerName:         "BatchUploadStagingData",
			APIPath:             "/fx/exposures/v91/upload",
			DefaultBlockMessage: "Exposure upload blocked by policy",
			Fields: map[string]interface{}{
				"file_count": len(r.MultipartForm.File["files"]),
			},
		}) {
			return
		}
		results, elapsed, status, err := processBatchUploadStagingData(r.Context(), pool, r)
		if err != nil {
			httpError(w, status, err.Error())
			return
		}
		respondEnvelopeSuccess(w, "Upload processed successfully", map[string]interface{}{
			"results": results,
			"duration": map[string]interface{}{
				"seconds": elapsed.Seconds(),
			},
		})
	}
}

func processBatchUploadStagingData(ctx context.Context, pool *pgxpool.Pool, r *http.Request) ([]UploadRouteResult, time.Duration, int, error) {
	start := time.Now()
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
			logger.LogError("[WARN] invalid currency_aliases JSON: %v", err)
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

	userID := r.FormValue(constants.KeyUserID)
	if userID == "" {
		return nil, 0, http.StatusBadRequest, errors.New(constants.ErrUserIDRequired)
	}
	userName := ""
	makerEmail := ""
	for _, s := range auth.GetActiveSessions() {
		if s.UserID == userID {
			userName = s.Name
			makerEmail = s.Email
			break
		}
	}
	if len(files) == 0 {
		return nil, 0, http.StatusBadRequest, errors.New("no files uploaded")
	}

	// build entity map (use masterentitycash + canonical fields)
	entityMap := map[string]string{}
	{
		allowedIDs := api.GetEntityIDsFromCtx(ctx)
		// Base SQL
		baseSQL := `
		SELECT COALESCE(NULLIF(me.unique_identifier,''), me.entity_id) AS uid,
			   TRIM(me.entity_name),
			   TRIM(COALESCE(me.entity_short_name, me.entity_name, '')) AS cname,
			   TRIM(me.entity_id)
		FROM public.masterentitycash me
		WHERE COALESCE(me.is_deleted, false) IS NOT TRUE`
		var rows pgx.Rows
		var err error
		if len(allowedIDs) > 0 {
			// restrict masterentitycash load to approved entity ids from prevalidation middleware
			rows, err = pool.Query(ctx, baseSQL+" AND me.entity_id = ANY($1)", allowedIDs)
			logger.LogInfo("[FBUP-ENT] restricting entityMap load to %d approved entity IDs", len(allowedIDs))
		} else {
			rows, err = pool.Query(ctx, baseSQL)
		}
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
		} else {
			logger.LogError("[FUBG]: error querying masterentitycash: %v", err)
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

	// Debug: inspect entityMap contents and SQL-derived keys
	{
		logger.LogInfo("[FBUP-ENT] entityMap built, count=%d", len(entityMap))
		// presence check for common company codes seen in test files (e.g. '3700')
		if v, ok := entityMap["3700"]; ok {
			logger.LogInfo("[FBUP-ENT] entityMap contains key '3700' -> %s", v)
		} else {
			logger.LogInfo("[FBUP-ENT] entityMap does NOT contain key '3700' (this is likely why many rows are non-qualified)")
		}
		// print up to 12 sample entries to inspect format
		sample := 0
		for k, v := range entityMap {
			if sample >= 12 {
				break
			}
			logger.LogInfo("[FBUP-ENT] sample[%d] key='%s' -> entity='%s'", sample, k, v)
			sample++
		}
	}

	results := make([]UploadRouteResult, 0, len(files))

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
			return nil, 0, http.StatusBadRequest, fmt.Errorf("open file: %w", err)
		}
		tmpPath, fileHash, err := saveTempAndHash(f, fh.Filename)
		f.Close()
		if err != nil {
			return nil, 0, http.StatusInternalServerError, fmt.Errorf("temp save: %w", err)
		}
		defer os.Remove(tmpPath)

		tmpFile, err := os.Open(tmpPath)
		if err != nil {
			return nil, 0, http.StatusInternalServerError, fmt.Errorf("open tmp: %w", err)
		}
		defer tmpFile.Close()

		fileExt := strings.ToLower(filepath.Ext(fh.Filename))
		allRows, err := ubParseUploadFile(tmpFile, fileExt)
		if err != nil {
			return nil, 0, http.StatusBadRequest, fmt.Errorf("file parse error: %w", err)
		}
		if len(allRows) == 0 {
			return nil, 0, http.StatusBadRequest, errors.New("empty file or no data rows")
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
		storedFileName, s3Key, err := fxExposureS3Key(fh.Filename, userName, src)
		if err != nil {
			return nil, 0, http.StatusBadRequest, err
		}
		uploadedNewObject := false
		if s3storage.IsS3UploadEnabled() {
			fileBytes, err := os.ReadFile(tmpPath)
			if err != nil {
				return nil, 0, http.StatusInternalServerError, fmt.Errorf("read temp file for s3 upload: %w", err)
			}
			// Duplicate file-hash protection is temporarily disabled for repeated
			// v91 upload testing. Re-enable this guard before production use.
			//
			// var duplicateExists bool
			// if err := pool.QueryRow(ctx, `
			// 	SELECT EXISTS (
			// 		SELECT 1
			// 		FROM public.staging_batches_exposures
			// 		WHERE file_hash = $1 AND status = 'processing'
			// 	) OR EXISTS (
			// 		SELECT 1
			// 		FROM public.exposure_headers
			// 		WHERE file_hash = $1
			// 		  AND COALESCE(is_deleted, false) = false
			// 	)
			// `, fileHash).Scan(&duplicateExists); err != nil {
			// 	return nil, 0, http.StatusInternalServerError, fmt.Errorf("failed to check duplicate exposure upload: %w", err)
			// }
			// if duplicateExists {
			// 	return nil, 0, http.StatusBadRequest, errors.New(duplicateExposureUploadMessage)
			// }
			if err = s3storage.PutObjectToS3(ctx, s3Key, fileBytes, s3storage.DetectContentType(fileBytes)); err != nil {
				return nil, 0, http.StatusInternalServerError, fmt.Errorf("failed to store original file to s3: %w", err)
			}
			uploadedNewObject = true
		}
		conn, err := pool.Acquire(ctx)
		if err != nil {
			if uploadedNewObject {
				if cleanupErr := s3storage.DeleteFromS3(ctx, s3Key); cleanupErr != nil {
					logger.LogError("[FBUP-S3] cleanup after acquire failure failed for key=%s: %v", s3Key, cleanupErr)
				}
			}
			return nil, 0, http.StatusInternalServerError, fmt.Errorf("%s%s", constants.ErrDBAcquire, err.Error())
		}
		tx, err := conn.Begin(ctx)
		if err != nil {
			conn.Release()
			if uploadedNewObject {
				if cleanupErr := s3storage.DeleteFromS3(ctx, s3Key); cleanupErr != nil {
					logger.LogError("[FBUP-S3] cleanup after tx begin failure failed for key=%s: %v", s3Key, cleanupErr)
				}
			}
			return nil, 0, http.StatusInternalServerError, fmt.Errorf("%s%s", constants.ErrTxBegin, err.Error())
		}
		committed := false
		cleanupUploadedObject := uploadedNewObject
		defer func() {
			if !committed {
				_ = tx.Rollback(ctx)
			}
			conn.Release()
			if cleanupUploadedObject && s3Key != "" {
				if cleanupErr := s3storage.DeleteFromS3(ctx, s3Key); cleanupErr != nil {
					logger.LogError("[FBUP-S3] cleanup failed for key=%s: %v", s3Key, cleanupErr)
				}
			}
		}()

		// validate mapping JSON early to provide a clearer error than the DB
		if len(mappingRaw) > 0 {
			var temp interface{}
			if err := json.Unmarshal(mappingRaw, &temp); err != nil {
				return nil, 0, http.StatusBadRequest, fmt.Errorf("invalid mapping JSON: %w", err)
			}
		}

		if _, err := tx.Exec(ctx, `
				INSERT INTO public.staging_batches_exposures
				(batch_id, ingestion_source, status, total_records, file_hash, file_name, uploaded_by, mapping_json, upload_s3_key)
				VALUES ($1,$2,'processing',$3,$4,$5,$6,$7,$8)
			`, batchID, src, 0, fileHash, storedFileName, userName, string(mappingRaw), s3Key); err != nil {
			return nil, 0, http.StatusInternalServerError, fmt.Errorf("insert batch: %w", err)
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
				[]string{"staging_id", "batch_id", "exposure_source", "raw_payload", "mapped_payload", "ingestion_timestamp", constants.KeyStatus},
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
			return nil, 0, http.StatusInternalServerError, fmt.Errorf("copy staging_exposures: %w", stagingErr)
		}

		if _, err := tx.Exec(ctx, `UPDATE public.staging_batches_exposures SET total_records=$1 WHERE batch_id=$2`, totalRows, batchID); err != nil {
			return nil, 0, http.StatusInternalServerError, fmt.Errorf("update batch total_records: %w", err)
		}

		logger.LogInfo("[DEBUG] After parsing: canonicals=%d, batch=%s file=%s", len(canonicals), batchID.String(), fh.Filename)
		if len(canonicals) > 0 {
			sample := canonicals[0]
			logger.LogInfo("[DEBUG] Sample canonical[0]: CompanyCode='%s', Party='%s', Currency='%s', Amount=%s, AmountFloat=%f, NetDueDate='%s'",
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
			logger.LogInfo("%s", msg)
		}

		logger.LogInfo("[DEBUG] After allocation: exposuresFloat=%d, knocksFloat=%d, batch=%s", len(exposuresFloat), len(knocksFloat), batchID.String())

		// net-exposure non-qualified pass
		netMap := make(map[string]float64)
		for _, e := range exposuresFloat {
			key := fmt.Sprintf(constants.FormatPipelineTriple, e.Source, e.CompanyCode, e.Party)
			netMap[key] += e.AmountFloat
		}
		flaggedCount := 0
		for i := range exposuresFloat {
			e := &exposuresFloat[i]
			key := fmt.Sprintf(constants.FormatPipelineTriple, e.Source, e.CompanyCode, e.Party)
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
		logger.LogInfo("[DEBUG] After net-exposure pass: flagged=%d out of %d exposuresFloat, batch=%s", flaggedCount, len(exposuresFloat), batchID.String())

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
						reason := fmt.Sprintf("Currency '%s' is not recognized (inactive or not approved in master data)", cur)
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
				msg2 := fmt.Sprintf("%d row(s) marked as non-qualified due to unmapped entity codes and %d row(s) due to invalid or unrecognized currency codes.", entityMiss, currencyMiss)
				fileWarnings = append(fileWarnings, msg2)
				logger.LogInfo(constants.LogWarn, msg2)
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

		logger.LogInfo("[DEBUG] After validation: qualified=%d, nonQualified=%d, batch=%s", len(qualified), len(nonQualified), batchID.String())
		if len(nonQualified) > 0 && len(nonQualified) <= 5 {
			for i, nq := range nonQualified {
				logger.LogInfo("[DEBUG] nonQualified[%d]: doc=%s, issues=%v", i, nq.Row.DocumentNumber, nq.Issues)
			}
		}

		if len(exposures) == 0 {
			if len(knocksFloat) > 0 {
				msg := fmt.Sprintf("No exposures were written: allocation fully matched all rows (knock events=%d). This commonly occurs when incoming debit/credit rows for the same Source|CompanyCode|Party fully net to zero, or because of receivable/payable logic settings (receivable_logic=%s, payable_logic=%s). If you expected inserts, check mapping, amount signs, and NetDueDate values; or run a small unbalanced test file to verify behavior.", len(knocksFloat), receivableLogic, payableLogic)
				fileWarnings = append(fileWarnings, msg)
				logger.LogInfo(constants.LogWarn, msg)
			} else {
				msg := "No exposures were written: allocation produced no base or knock items. Likely causes: amounts all share the same sign (no debits or no credits), amounts parsed as zero, or mapping produced empty AmountDoc values. Check mapping, amount signs (+/-), and NetDueDate; try 'receivable_logic'/'payable_logic' = reverse to flip allocation direction or upload a small unbalanced test file."
				fileWarnings = append(fileWarnings, msg)
				logger.LogInfo(constants.LogWarn, msg)
			}
		}

		// ------------------ Prepare headers COPY (includes batch_id + file_hash) ------------------
		headerCols := []string{
			"exposure_header_id", "company_code", "entity", "entity1", "entity2", "entity3",
			"exposure_type", "document_id", "document_date", "counterparty_type", "counterparty_code",
			"counterparty_name", "currency", "total_original_amount", "total_open_amount",
			"value_date", constants.KeyStatus, "is_active", "created_at", "updated_at", "approval_status",
			"exposure_creation_status",
			"approval_comment", "approved_by", "delete_comment", "requested_by", "rejection_comment",
			"approved_at", "rejected_by", "rejected_at", "time_based", "amount_in_local_currency",
			"posting_date", "text", "gl_account", "reference", "additional_header_details",
			"exposure_category", "batch_id", "file_hash", "upload_s3_key",
		}

		docToID := make(map[string]string, len(qualified))

		debugLogLimit := 10
		headerSrc := pgx.CopyFromSlice(len(qualified), func(i int) ([]any, error) {
			q := qualified[i]
			var docDate interface{}
			var valDate interface{}
			var postDate interface{}
			if q.DocumentDate != "" {
				if t, terr := time.Parse(constants.DateFormat, q.DocumentDate); terr == nil {
					docDate = t
				}
			}
			if q.NetDueDate != "" {
				if t, terr := time.Parse(constants.DateFormat, q.NetDueDate); terr == nil {
					valDate = t
				}
			}
			if q.PostingDate != "" {
				if t, terr := time.Parse(constants.DateFormat, q.PostingDate); terr == nil {
					postDate = t
				}
			}

			addtl, _ := json.Marshal(q._raw)
			addtl = sanitizeUTF8(addtl)

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
				logger.LogInfo("[FBUP] header-row[%d] doc=%s rawCategory='%s' source='%s' exposureType='%s' exposureCategory='%s' company='%s' amount=%s",
					i, q.DocumentNumber, rawCat, q.Source, exposureType, exposureCategory, q.CompanyCode, q.AmountDoc.String())
			}

			id := uuid.New()
			docToID[q.DocumentNumber] = id.String()

			totalOrig := q.AmountDoc.Abs().StringFixed(4)
			totalOpen := q.AmountDoc.StringFixed(4)
			textVal := nullableTrimmedString(rawFieldString(q._raw, "Text", "text", "Item Text"))
			glVal := nullableTrimmedString(rawFieldString(q._raw, "GLAccount", "gl_account", "G/L Account"))
			refVal := nullableTrimmedString(rawFieldString(q._raw, "Reference", "Assignment", "reference"))
			cpNameVal := nullableTrimmedString(rawFieldString(q._raw, "Party Name", "counterparty_name", "Name"))

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
				cpNameVal,              // counterparty_name
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
				nil,                    // rejected_at
				time.Now(),             // time_based
				nil,                    // amount_in_local_currency
				postDate,               // posting_date
				textVal, glVal, refVal, // text, gl_account, reference
				addtl,            // additional_header_details
				exposureCategory, // exposure_category
				batchID,          // batch_id (new)
				fileHash,         // file_hash (new)
				s3Key,            // upload_s3_key
			}, nil
		})

		logger.LogInfo("[FBUP] about to COPY %d exposure_headers for batch %s file=%s", len(qualified), batchID.String(), fh.Filename)
		if _, err := tx.CopyFrom(ctx, pgx.Identifier{"public", "exposure_headers"}, headerCols, headerSrc); err != nil {
			return nil, 0, http.StatusInternalServerError, fmt.Errorf("copy headers: %w", err)
		}
		logger.LogInfo("[FBUP] finished COPY exposure_headers for batch %s file=%s", batchID.String(), fh.Filename)

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
					addtlJSON = sanitizeUTF8(addtlJSON)
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

		logger.LogInfo("[FBUP] about to COPY %d exposure_line_items for batch %s", len(liRows), batchID.String())
		if len(liRows) > 0 {
			if _, err := tx.CopyFrom(ctx,
				pgx.Identifier{"public", "exposure_line_items"},
				lineItemCols,
				pgx.CopyFromRows(liRows)); err != nil {
				fileErrors = append(fileErrors, "copy line items: "+err.Error())
				logger.LogError("copy line items: %v", err)
				tx.Rollback(ctx)
				return nil, 0, http.StatusInternalServerError, fmt.Errorf("copy line items failed: %w", err)
			}
		}
		logger.LogInfo("[FBUP] finished COPY exposure_line_items for batch %s", batchID.String())

		// ------------------ Build and COPY exposure_allocations ------------------
		docCompany := make(map[string]string, len(canonicals))
		docParty := make(map[string]string, len(canonicals))
		// additional per-doc metadata maps used when inserting allocation rows
		docSource := make(map[string]string, len(canonicals))
		docDocDate := make(map[string]string, len(canonicals))
		docPostingDate := make(map[string]string, len(canonicals))
		docNetDue := make(map[string]string, len(canonicals))
		docEffDue := make(map[string]string, len(canonicals))
		docMappedPayload := make(map[string][]byte, len(canonicals))

		for _, c := range canonicals {
			docCompany[c.DocumentNumber] = c.CompanyCode
			docParty[c.DocumentNumber] = c.Party
			docSource[c.DocumentNumber] = c.Source
			docDocDate[c.DocumentNumber] = c.DocumentDate
			docPostingDate[c.DocumentNumber] = c.PostingDate
			docNetDue[c.DocumentNumber] = c.NetDueDate
			docEffDue[c.DocumentNumber] = c.NetDueDate
			if c._raw != nil {
				if b, err := json.Marshal(c._raw); err == nil {
					docMappedPayload[c.DocumentNumber] = sanitizeUTF8(b)
				}
			}
		}
		for _, q := range qualified {
			if q.DocumentNumber != "" {
				docCompany[q.DocumentNumber] = q.CompanyCode
				docParty[q.DocumentNumber] = q.Party
			}
		}

		allocCols := []string{
			"allocation_id", "batch_id", "file_hash",
			"base_document_id", "knockoff_document_id",
			"allocation_amount", "allocation_currency",
			"allocation_amount_signed", "allocation_date",
			"created_at", "created_by", "notes",
			"company_code", "counterparty_code",
			// additional metadata columns added to table
			"source", "document_date", "posting_date", "net_due_date", "effective_due_date",
			"exchange_rate", "amount_local_signed", "mapped_payload",
		}
		docSignByNumber := make(map[string]float64, len(canonicals))
		for _, c := range canonicals {
			if c.DocumentNumber == "" {
				continue
			}
			sign := 1.0
			if c.AmountFloat < 0 {
				sign = -1
			} else if c.AmountFloat > 0 {
				sign = 1
			}
			docSignByNumber[c.DocumentNumber] = sign
		}

		allocRows := make([][]any, 0, len(knocksFloat))
		for _, k := range knocksFloat {
			if k.AmtFloat == 0 {
				continue
			}
			var companyVal interface{}
			var partyVal interface{}
			if cc, ok := docCompany[k.BaseDoc]; ok && strings.TrimSpace(cc) != "" {
				companyVal = cc
			}
			if pp, ok := docParty[k.BaseDoc]; ok && strings.TrimSpace(pp) != "" {
				partyVal = pp
			}
			// lookup additional metadata (source, dates, mapped payload) from doc maps if available
			var srcVal interface{}
			var docDateVal interface{}
			var postDateVal interface{}
			var netDueVal interface{}
			var effDueVal interface{}
			var mappedPayload interface{}
			if v, ok := docSource[k.BaseDoc]; ok && v != "" {
				srcVal = v
			} else if v, ok := docSource[k.KnockDoc]; ok && v != "" {
				srcVal = v
			}
			if v, ok := docDocDate[k.BaseDoc]; ok && v != "" {
				docDateVal = parseDateOrNil(v)
			} else if v, ok := docDocDate[k.KnockDoc]; ok && v != "" {
				docDateVal = parseDateOrNil(v)
			}
			if v, ok := docPostingDate[k.BaseDoc]; ok && v != "" {
				postDateVal = parseDateOrNil(v)
			} else if v, ok := docPostingDate[k.KnockDoc]; ok && v != "" {
				postDateVal = parseDateOrNil(v)
			}
			if v, ok := docNetDue[k.BaseDoc]; ok && v != "" {
				netDueVal = parseDateOrNil(v)
			} else if v, ok := docNetDue[k.KnockDoc]; ok && v != "" {
				netDueVal = parseDateOrNil(v)
			}
			if v, ok := docEffDue[k.BaseDoc]; ok && v != "" {
				effDueVal = parseDateOrNil(v)
			} else if v, ok := docEffDue[k.KnockDoc]; ok && v != "" {
				effDueVal = parseDateOrNil(v)
			}
			if v, ok := docMappedPayload[k.BaseDoc]; ok {
				mappedPayload = v
			} else if v, ok := docMappedPayload[k.KnockDoc]; ok {
				mappedPayload = v
			}

			baseSign := docSignByNumber[k.BaseDoc]
			if baseSign == 0 {
				baseSign = 1
			}
			signedAlloc := decimal.NewFromFloat(math.Abs(k.AmtFloat) * baseSign)

			allocRows = append(allocRows, []any{
				uuid.New(),
				batchID,
				fileHash,
				k.BaseDoc,
				k.KnockDoc,
				decimal.NewFromFloat(math.Abs(k.AmtFloat)).StringFixed(4),
				k.Currency,
				signedAlloc.StringFixed(4),
				time.Now(),
				time.Now(),
				userName,
				nil,
				companyVal,
				partyVal,
				srcVal,
				docDateVal,
				postDateVal,
				netDueVal,
				effDueVal,
				nil, // exchange_rate (not available at upload time)
				nil, // amount_local_signed (not available at upload time)
				mappedPayload,
			})
		}
		if len(allocRows) > 0 {
			if _, err := tx.CopyFrom(ctx,
				pgx.Identifier{"public", "exposure_allocations"},
				allocCols,
				pgx.CopyFromRows(allocRows)); err != nil {
				fileErrors = append(fileErrors, "copy allocations: "+err.Error())
				logger.LogError("copy allocations: %v", err)
				tx.Rollback(ctx)
				return nil, 0, http.StatusInternalServerError, fmt.Errorf("copy allocations failed: %w", err)
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
			mp = sanitizeUTF8(mp)
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
				logger.LogError("copy unallocated: %v", err)
				tx.Rollback(ctx)
				return nil, 0, http.StatusInternalServerError, fmt.Errorf("copy unallocated failed: %w", err)
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
			mp = sanitizeUTF8(mp)
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
				logger.LogError("copy unqualified: %v", err)
				tx.Rollback(ctx)
				return nil, 0, http.StatusInternalServerError, fmt.Errorf("copy unqualified failed: %w", err)
			}
		}

		// One consolidated CREATE audit per file upload (batch-scoped), not per exposure header.
		if strings.TrimSpace(userName) != "" {
			if _, err := tx.Exec(ctx, `
				INSERT INTO public.auditactionexposure
					(exposure_header_id, actiontype, processing_status, reason,
					 requested_by, requested_at, requested_ip, old_values,
					 new_values, change_summary)
				VALUES (
					$1::text,
					'CREATE',
					$2::text,
					'SAP file upload',
					$3::text,
					now(),
					NULLIF($4::text, ''),
					NULL,
					jsonb_build_object(
						'batch_id', $1::text,
						'file_name', $5::text,
						'upload_s3_key', $6::text,
						'qualified_headers', $7::int,
						'unqualified_rows', $8::int,
						'source', $9::text
					),
					NULL
				)
			`, batchID.String(), constants.StatusPendingApproval, userName,
				api.ClientIPFromContext(ctx), fh.Filename, s3Key, len(docToID), len(nonQualified), src); err != nil {
				return nil, 0, http.StatusInternalServerError, fmt.Errorf("insert exposure create audit row: %w", err)
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
			return nil, 0, http.StatusInternalServerError, fmt.Errorf("update batch: %w", err)
		}

		if err := tx.Commit(ctx); err != nil {
			persisted, persistErr := exposureBatchUploadS3KeyPersisted(ctx, pool, batchID, s3Key)
			if persistErr != nil {
				cleanupUploadedObject = false
				return nil, 0, http.StatusInternalServerError, fmt.Errorf("%s%s (unable to verify persisted upload_s3_key: %v)", constants.ErrCommitFailed, err.Error(), persistErr)
			}
			if persisted {
				cleanupUploadedObject = false
			}
			return nil, 0, http.StatusInternalServerError, fmt.Errorf("%s%s", constants.ErrCommitFailed, err.Error())
		}
		committed = true
		cleanupUploadedObject = false

		logger.LogInfo("[FBUP] committed batch %s for file %s", batchID.String(), fh.Filename)

		fxnotif.NotifyExposureUpload(ctx, pool, fxnotif.SourceRouteV91Upload, batchID.String(), userID, userName)

		// Evaluate the EXPOSURE_CREATION create policy per header so a
		// TriggerApproval rule (e.g. Exposure Amount) can pin the approval matrix
		// its breach selected. Headers are already committed here, so this cannot
		// block the upload — it exists to carry the matrix into CreateInstance,
		// which previously received none and so created no instance at all.
		createMatrices := make(map[string]string, len(docToID))
		createEntities := make(map[string]string, len(docToID))
		for _, hidStr := range docToID {
			creationRow, rowErr := fxexposures.LoadExposureCreationRow(ctx, pool, hidStr)
			if rowErr != nil {
				logger.LogError("[FBUP] exposure create policy load failed for %s: %v", hidStr, rowErr)
				continue
			}
			createEntities[hidStr] = creationRow.Entity
			okPolicy, msgPolicy, tID := policyruntime.EnforceInlineWithMatrix(ctx, r, pool, policyruntime.EnforceInput{
				EventCode:           common.TriggerPreCreate,
				ModuleCode:          common.ModuleFX,
				SubModule:           "EXPOSURE_CREATION",
				EntityCode:          creationRow.Entity,
				ActorUserID:         userID,
				HandlerName:         "BatchUploadStagingData",
				APIPath:             "/fx/exposures/v91/upload",
				DefaultBlockMessage: "Exposure creation blocked by policy",
				Fields:              fxexposures.BuildExposureCreationPolicyFields(creationRow),
			})
			if !okPolicy {
				logger.LogError("[FBUP] exposure create policy breach for %s: %s", hidStr, msgPolicy)
				continue
			}
			createMatrices[hidStr] = tID
		}
		// Create approval instances for all new headers
		go func(docs map[string]string, email string, matrices, entities map[string]string) {
			bgCtx := context.Background()
			for _, hidStr := range docs {
				_, _ = approvalengine.CreateInstance(bgCtx, pool, approvalengine.InstanceRequest{
					ModuleCode:       "FX",
					EntityCode:       entities[hidStr],
					TransactionType:  "FX_EXPOSURE_CREATE",
					RecordID:         hidStr,
					MatrixID:         matrices[hidStr],
					SubmittedByEmail: email,
				})
			}
		}(docToID, makerEmail, createMatrices, createEntities)

		// Authoritative preview: use DB-driven preview builder instead of in-memory approximation.
		// The old in-memory preview (based on canonicals) is intentionally removed to ensure
		// upload responses match the edit/preview responses produced after persisting data.
		previewRes, _, err := buildPreviewForBatch(pool, ctx, batchID, nil)
		if err != nil {
			return nil, 0, http.StatusInternalServerError, fmt.Errorf("preview build after commit: %w", err)
		}
		// merge file-level warnings/info into preview result for visibility
		if len(fileWarnings) > 0 {
			previewRes.Warnings = append(previewRes.Warnings, fileWarnings...)
		}
		if len(fileInfo) > 0 {
			previewRes.Info = append(previewRes.Info, fileInfo...)
		}
		if len(fileErrors) > 0 {
			previewRes.Errors = append(previewRes.Errors, fileErrors...)
		}
		// ensure filename/source reflect the uploaded file
		if previewRes.FileName == "" {
			previewRes.FileName = fh.Filename
		}
		if previewRes.Source == "" {
			previewRes.Source = src
		}
		results = append(results, UploadRouteResult{
			UploadResult: previewRes,
		})
	} // end per-file loop

	elapsed := time.Since(start)
	logger.LogInfo("[FBUP] total upload completed in %s, files=%d", elapsed.String(), len(results))
	return results, elapsed, 0, nil
}

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
		// V7 FIFO order: posting date first, then document number.
		// Stable ordering is essential because changing row order changes which
		// documents are knocked and therefore the resulting exposure values.
		sort.SliceStable(arr, func(i, j int) bool {
			leftDate := strings.TrimSpace(arr[i].PostingDate)
			rightDate := strings.TrimSpace(arr[j].PostingDate)
			if leftDate != rightDate {
				return leftDate < rightDate
			}
			return strings.TrimSpace(arr[i].DocumentNumber) <
				strings.TrimSpace(arr[j].DocumentNumber)
		})
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
		case "FBL1N", "FBL3N": // Payables and GR/IR
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
		// ensure knock entries carry the group's currency
		for ki := range knocks {
			knocks[ki].Currency = curr
		}
		allKnocks = append(allKnocks, knocks...)

		// Optional: debug info
		if len(exps) > 0 {
			logger.LogInfo("[FIFO] Group %s → exposures=%d, knockoffs=%d", key, len(exps), len(knocks))
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

func fxExposureS3Bucket() string {
	if b := strings.TrimSpace(os.Getenv("BANK_STMT_S3_BUCKET")); b != "" {
		return b
	}
	return "cimplr"
}

func fxExposureS3Region() string {
	if r := strings.TrimSpace(os.Getenv("BANK_STMT_S3_REGION")); r != "" {
		return r
	}
	return "ap-south-1"
}

func fxExposureS3PresignExpiry() time.Duration {
	expiryDuration := 7 * 24 * time.Hour
	if envExpiry := strings.TrimSpace(os.Getenv("BANK_STMT_URL_EXPIRY_HOURS")); envExpiry != "" {
		if hours, parseErr := strconv.Atoi(envExpiry); parseErr == nil && hours > 0 {
			expiryDuration = time.Duration(hours) * time.Hour
			max := 7 * 24 * time.Hour
			if expiryDuration > max {
				expiryDuration = max
			}
		}
	}
	return expiryDuration
}

func fxExposureS3Key(filename, uploadedBy, source string) (string, string, error) {
	folder, err := v91UploadSourceFolder(source)
	if err != nil {
		return "", "", err
	}
	storedFileName := s3storage.BuildUploadedFilename(filename, uploadedBy, time.Now().UTC())
	return storedFileName, s3storage.BuildNamedS3Key("fx/v91", folder, storedFileName), nil
}

func v91UploadSourceFolder(source string) (string, error) {
	src := strings.ToUpper(strings.TrimSpace(source))
	switch src {
	case "FBL1N", "FBL3N", "FBL5N":
		return src, nil
	default:
		return "", fmt.Errorf("invalid v91 source '%s': expected one of FBL1N, FBL3N, FBL5N", source)
	}
}

func exposureBatchUploadS3KeyPersisted(ctx context.Context, pool *pgxpool.Pool, batchID uuid.UUID, uploadS3Key string) (bool, error) {
	var exists bool
	err := pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM public.staging_batches_exposures
			WHERE batch_id = $1
			  AND COALESCE(upload_s3_key, '') = $2
		)
	`, batchID, uploadS3Key).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
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
		"CompanyCode":       {"company code", "bukrs", "company_code", "company", "company name"},
		"Party":             {"party", "account", "vendor", "customer", "account number", "supplier", "vendor code", "supplier code"},
		"DocumentCurrency":  {"document currency", "doc. curr.", "waers", "document_currency", "doccurrency"},
		"DocumentNumber":    {"document number", "belnr", "document", "document_number", "docno"},
		"DocumentDate":      {"document date", "bldat", "document_date", "doc. date"},
		"PostingDate":       {constants.TransactionPostingDate, "budat", "posting_date", "pstng date"},
		"NetDueDate":        {"net due date", "net due date", "baseline date", "due date", "net_due_date", "clearing date"},
		"AmountDoc":         {"amount in doc. curr.", "amount in doc. curr", "wrbtr", "amt in doc. curr.", "amount_in_doc_curr", "amount", "amount in local currency", "dmbtr", "amount_in_local_currency"},
		"Assignment":        {"assignment", "zuonr", "reference"},
		"DocumentType":      {"document type", "blart", "document_type"},
		"SpecialGL":         {"special g/l ind.", "umskz", "special_gl_ind"},
		"Text":              {"text", "sgtxt", "item text"},
		"BusinessArea":      {"business area", "gsber", "business_area"},
		"PaymentBlock":      {"payment block", "zlspr", "payment_block"},
		"GLAccount":         {"g/l account", "hkont", "gl_account", "g/l"},
		"ClearingDocument":  {"clearing document", "augbl", "clearing_document"},
		"ClearingDate":      {"clearing date", "augdt", "clearing_date"},
		"LocalCurrency":     {"local currency", "loc. curr.", "hwaer", "local_currency"},
		"OffsettingAccount": {"offsetting account", "offsetting_account"},
		"PANN":              {"pann", "pan"},
		"PostingKey":        {"posting key", "bschl", "posting_key"},
		"BankReference":     {"bankreference", "bank_reference"},
		"LinkedId":          {"linkedid", "linked_id"},
		"Reference":         {"reference"},
	}
	for canon, list := range guesses {
		found := false
		for _, cand := range list {
			if v, ok := lrow[strings.ToLower(cand)]; ok {
				out[canon] = strings.TrimSpace(v)
				found = true
				break
			}
		}
		if !found {
			out[canon] = ""
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
	dateStr = strings.TrimSpace(dateStr)
	if dateStr == "" {
		return "", nil
	}

	// Normalize spaces
	dateStr = regexp.MustCompile(`\s+`).ReplaceAllString(dateStr, " ")

	// Try common layouts first
	layouts := []string{
		// ISO formats
		constants.DateFormat,
		"2006/01/02",
		"2006.01.02",
		time.RFC3339,
		constants.DateTimeFormat,
		constants.DateFormatISO,
		"2006-01-02T15:04:05Z",
		"2006-01-02T15:04:05.000Z",

		// DD-MM-YYYY formats
		constants.DateFormatAlt,
		"02/01/2006",
		"02.01.2006",
		"02-01-2006 15:04:05",
		"02/01/2006 15:04:05",
		"02.01.2006 15:04:05",

		// MM-DD-YYYY formats
		"01-02-2006",
		"01/02/2006",
		"01.02.2006",
		"01-02-2006 15:04:05",
		"01/02/2006 15:04:05",
		"01.02.2006 15:04:05",

		// Text month formats
		constants.DateFormatDash,
		"02-Jan-06",
		"2-Jan-2006",
		"2-Jan-06",
		"02-Jan-2006 15:04:05",
		"02 Jan 2006",
		"2 Jan 2006",
		"02 Jan 06",
		"2 Jan 06",
		"Jan 02, 2006",
		"Jan 2, 2006",
		"January 02, 2006",
		"January 2, 2006",

		// Single digit day/month formats
		"2-1-2006",
		"2/1/2006",
		"2.1.2006",
		"1-2-2006",
		"1/2/2006",
		"1.2.2006",

		// Short year formats
		"02-01-06",
		"02/01/06",
		"02.01.06",
		"01-02-06",
		"01/02/06",
		"01.02.06",
		"2-1-06",
		"2/1/06",
		"1-2-06",
		"1/2-06",

		// compact
		"20060102",
	}

	for _, l := range layouts {
		if t, err := time.Parse(l, dateStr); err == nil {
			if t.Year() < 1900 || t.Year() > 9999 {
				continue
			}
			return t.Format(constants.DateFormat), nil
		}
	}

	// If the string is purely numeric try several heuristics
	digits := true
	for _, r := range dateStr {
		if r < '0' || r > '9' {
			digits = false
			break
		}
	}

	if digits {
		// YYYYMMDD
		if len(dateStr) == 8 {
			if y, err := strconv.Atoi(dateStr[0:4]); err == nil {
				if m, err := strconv.Atoi(dateStr[4:6]); err == nil {
					if d, err := strconv.Atoi(dateStr[6:8]); err == nil {
						if y >= 1900 && y <= 9999 {
							return time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.UTC).Format(constants.DateFormat), nil
						}
					}
				}
			}
		}

		if v, err := strconv.ParseInt(dateStr, 10, 64); err == nil {
			var t time.Time
			switch {
			case v >= 1e17:
				// nanoseconds since epoch
				t = time.Unix(0, v)
			case v >= 1e14:
				// microseconds -> ns
				t = time.Unix(0, v*1000)
			case v >= 1e11:
				// milliseconds -> ns
				t = time.Unix(0, v*1000000)
			case v >= 1e9:
				// seconds
				t = time.Unix(v, 0)
			default:
				// Treat as Excel serial date (days since 1899-12-30)
				base := time.Date(1899, 12, 30, 0, 0, 0, 0, time.UTC)
				t = base.AddDate(0, 0, int(v))
			}
			if t.Year() >= 1900 && t.Year() <= 9999 {
				return t.Format(constants.DateFormat), nil
			}
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
			issues = append(issues, "Company code is required")
		}
		if strings.TrimSpace(it.Party) == "" {
			issues = append(issues, "Party/counterparty code is required")
		}
		if strings.TrimSpace(it.DocumentCurrency) == "" {
			issues = append(issues, "Document currency is required")
		}
		if strings.TrimSpace(it.NetDueDate) == "" {
			issues = append(issues, "Net due date is required and must be a valid date")
		}
		if it.AmountDoc.Equal(decimal.Zero) {
			issues = append(issues, "Document amount must be non-zero")
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

func httpError(w http.ResponseWriter, status int, msg string) {
	respondEnvelopeError(w, status, msg, v91ErrorCode(status))
}

func respondEnvelopeError(w http.ResponseWriter, status int, message, code string) {
	api.RespondEnvelopeError(w, status, message, code)
}

func respondEnvelopeFailureWithData(w http.ResponseWriter, status int, message, code string, data interface{}) {
	api.RespondEnvelopeFailureWithData(w, status, message, code, data)
}

func respondEnvelopeSuccess(w http.ResponseWriter, message string, data interface{}) {
	api.RespondEnvelopeSuccess(w, message, data)
}

func v91ErrorCode(status int) string {
	return api.EnvelopeErrorCode(status)
}

func GetExposureDownloadURL(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			BatchID string `json:"batch_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			httpError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}

		batchID := strings.TrimSpace(req.BatchID)
		if batchID == "" {
			httpError(w, http.StatusBadRequest, constants.ErrBatchIDRequired)
			return
		}

		batchUUID, err := uuid.Parse(batchID)
		if err != nil {
			httpError(w, http.StatusBadRequest, constants.ErrInvalidBatchID)
			return
		}

		var uploadS3Key string
		err = pool.QueryRow(r.Context(), `
			SELECT COALESCE(upload_s3_key, '')
			FROM public.staging_batches_exposures
			WHERE batch_id = $1
			ORDER BY ingestion_timestamp DESC
			LIMIT 1
		`, batchUUID).Scan(&uploadS3Key)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				httpError(w, http.StatusNotFound, constants.ErrBatchNotFound)
				return
			}
			httpError(w, http.StatusInternalServerError, "failed to fetch batch")
			return
		}

		uploadS3Key = strings.TrimSpace(uploadS3Key)
		if uploadS3Key == "" {
			httpError(w, http.StatusNotFound, "no file available for download")
			return
		}

		downloadURL, err := s3storage.GetDownloadPresignedURL(r.Context(), uploadS3Key, 15*time.Minute)
		if err != nil {
			httpError(w, http.StatusInternalServerError, "failed to generate download url")
			return
		}
		recordExposureBatchDownloadAuditPGX(r.Context(), pool, batchUUID, auditutil.ActorFromContext(r.Context()), uploadS3Key)

		respondEnvelopeSuccess(w, "Download URL generated successfully", map[string]interface{}{
			"download_url": downloadURL,
		})
	}
}

func normalizeBulkIDs(ids []string) []string {
	seen := make(map[string]struct{}, len(ids))
	out := make([]string, 0, len(ids))
	for _, raw := range ids {
		id := strings.TrimSpace(raw)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

func recordExposureBatchDownloadAuditPGX(ctx context.Context, pool *pgxpool.Pool, batchID uuid.UUID, requestedBy, uploadS3Key string) {
	if requestedBy == "" || uploadS3Key == "" {
		return
	}

	rows, err := pool.Query(ctx, `
		SELECT exposure_header_id::text
		FROM public.exposure_headers
		WHERE batch_id = $1
	`, batchID)
	if err != nil {
		logger.LogError("fx exposure download audit lookup failed batch=%s: %v", batchID.String(), err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var exposureHeaderID string
		if err := rows.Scan(&exposureHeaderID); err != nil {
			logger.LogError("fx exposure download audit scan failed batch=%s: %v", batchID.String(), err)
			continue
		}
		auditutil.RecordDownloadPGX(ctx, pool, auditutil.DownloadParams{TableName: auditutil.TableExposureDownloads, ParentColumn: "exposure_header_id", ParentID: exposureHeaderID, RequestedBy: requestedBy, UploadS3Key: uploadS3Key, ExtraColumns: nil})
	}
	if err := rows.Err(); err != nil {
		logger.LogError("fx exposure download audit rows failed batch=%s: %v", batchID.String(), err)
	}
}

func writeBulkDownloadResponse(w http.ResponseWriter, files []map[string]string, failedIDs []string) {
	payload := map[string]interface{}{
		"files":      files,
		"failed_ids": failedIDs,
	}
	if len(files) == 0 {
		respondEnvelopeFailureWithData(w, http.StatusNotFound, "no downloadable files found", v91ErrorCode(http.StatusNotFound), payload)
		return
	}

	respondEnvelopeSuccess(w, "Bulk download URLs generated successfully", payload)
}

func GetExposureBulkDownloadURL(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			BatchIDs []string `json:"batch_ids"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			httpError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}

		ids := normalizeBulkIDs(req.BatchIDs)
		if len(ids) == 0 {
			httpError(w, http.StatusBadRequest, "batch_ids is required")
			return
		}

		ctx := r.Context()
		files := make([]map[string]string, 0, len(ids))
		failedIDs := make([]string, 0)

		for _, batchID := range ids {
			batchUUID, err := uuid.Parse(batchID)
			if err != nil {
				failedIDs = append(failedIDs, batchID)
				continue
			}

			var uploadS3Key string
			err = pool.QueryRow(ctx, `
				SELECT COALESCE(upload_s3_key, '')
				FROM public.staging_batches_exposures
				WHERE batch_id = $1
				ORDER BY ingestion_timestamp DESC
				LIMIT 1
			`, batchUUID).Scan(&uploadS3Key)
			if err != nil {
				failedIDs = append(failedIDs, batchID)
				continue
			}

			uploadS3Key = strings.TrimSpace(uploadS3Key)
			if uploadS3Key == "" {
				failedIDs = append(failedIDs, batchID)
				continue
			}

			downloadURL, err := s3storage.GetDownloadPresignedURL(ctx, uploadS3Key, 15*time.Minute)
			if err != nil {
				failedIDs = append(failedIDs, batchID)
				continue
			}

			files = append(files, map[string]string{
				"batch_id":     batchID,
				"download_url": downloadURL,
			})
			recordExposureBatchDownloadAuditPGX(ctx, pool, batchUUID, auditutil.ActorFromContext(ctx), uploadS3Key)
		}

		writeBulkDownloadResponse(w, files, failedIDs)
	}
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

func rawFieldString(raw map[string]interface{}, keys ...string) string {
	if raw == nil {
		return ""
	}
	for _, key := range keys {
		if v, ok := raw[key]; ok {
			if s := strings.TrimSpace(asString(v)); s != "" && s != "<nil>" {
				return s
			}
		}
	}
	return ""
}

func nullableTrimmedString(s string) interface{} {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	return s
}

func asString(v interface{}) string {
	if v == nil {
		return ""
	}
	return sanitizeUTF8String(strings.TrimSpace(fmt.Sprintf("%v", v)))
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
	normalized, err := NormalizeDate(s)
	if err != nil || normalized == "" {
		return nil
	}
	if t, err := time.Parse(constants.DateFormat, normalized); err == nil {
		return t
	}
	return nil
}

// Helper: parse uploaded file into [][]string
func ubParseUploadFile(file multipart.File, ext string) ([][]string, error) {
	if ext == ".csv" {
		raw, err := io.ReadAll(file)
		if err != nil {
			return nil, err
		}
		r := csv.NewReader(strings.NewReader(decodeUploadText(raw)))
		r.LazyQuotes = true
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
	return nil, errors.New(constants.ErrUnsupportedFileType)
}

// --- Request payload ---
type EditAllocationRequest struct {
	UserID          string            `json:"user_id"`
	BatchID         string            `json:"batch_id"`
	CurrencyAliases map[string]string `json:"currency_aliases,omitempty"`
	Groups          []struct {
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

// --- Helper consistent response ---

// EditResponseParams groups parameters for writeEditResponse to keep parameter count low
type EditResponseParams struct {
	BatchID  uuid.UUID
	Rows     []CanonicalPreviewRow
	Errors   []string
	Info     []string
	Inserted int
	Total    int
	Start    time.Time
}

// HeaderMetaParams groups parameters for buildHeaderMeta to reduce parameter count
type HeaderMetaParams struct {
	DocID       string
	Company     string
	Party       string
	Currency    string
	Category    string
	FileHash    string
	DocDate     *time.Time
	PostingDate *time.Time
	ValueDate   *time.Time
	Addtl       []byte
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

func originalAmountFromMappedPayload(payload []byte) (decimal.Decimal, bool) {
	if len(payload) == 0 {
		return decimal.Zero, false
	}
	var mp map[string]interface{}
	if err := json.Unmarshal(payload, &mp); err != nil {
		return decimal.Zero, false
	}
	amt := parseDecimalFromAny(mp["AmountDoc"])
	if amt.IsZero() {
		amt = parseDecimalFromAny(mp["Amount"])
	}
	if amt.IsZero() {
		return decimal.Zero, false
	}
	return amt, true
}

type stagingDocMeta struct {
	Company, Party, Currency, Source  string
	DocumentDate, PostingDate, NetDue string
	MappedPayload                     []byte
}

func stagingMetaFromPayload(payload []byte) (docNum string, meta stagingDocMeta, ok bool) {
	if len(payload) == 0 {
		return "", stagingDocMeta{}, false
	}
	var mp map[string]interface{}
	if err := json.Unmarshal(payload, &mp); err != nil {
		return "", stagingDocMeta{}, false
	}
	docNum = strings.TrimSpace(stringFromAny(mp["DocumentNumber"]))
	if docNum == "" {
		docNum = strings.TrimSpace(stringFromAny(mp["DocumentID"]))
	}
	if docNum == "" {
		return "", stagingDocMeta{}, false
	}
	meta = stagingDocMeta{
		Company:       stringFromAny(mp["CompanyCode"]),
		Party:         stringFromAny(mp["Party"]),
		Currency:      strings.ToUpper(stringFromAny(mp["DocumentCurrency"])),
		Source:        stringFromAny(mp["Source"]),
		DocumentDate:  normalizeStagingDate(stringFromAny(mp["DocumentDate"])),
		PostingDate:   normalizeStagingDate(stringFromAny(mp["PostingDate"])),
		NetDue:        normalizeStagingDate(stringFromAny(mp["NetDueDate"])),
		MappedPayload: payload,
	}
	return docNum, meta, true
}

func loadStagingMetaForBatch(ctx context.Context, conn *pgxpool.Conn, batchUUID uuid.UUID) map[string]stagingDocMeta {
	out := map[string]stagingDocMeta{}
	rows, err := conn.Query(ctx, `SELECT mapped_payload FROM public.staging_exposures WHERE batch_id=$1`, batchUUID)
	if err != nil {
		return out
	}
	defer rows.Close()
	for rows.Next() {
		var payload []byte
		if scanErr := rows.Scan(&payload); scanErr != nil {
			continue
		}
		docNum, meta, ok := stagingMetaFromPayload(payload)
		if !ok {
			continue
		}
		out[docNum] = meta
	}
	return out
}

func normalizeStagingDate(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if normalized, err := NormalizeDate(raw); err == nil && normalized != "" {
		return normalized
	}
	return raw
}

func applyStagingMetaToPreview(stagingMeta map[string]stagingDocMeta, doc string, pr *CanonicalPreviewRow) {
	sm, ok := stagingMeta[doc]
	if !ok {
		return
	}
	if pr.CompanyCode == "" && sm.Company != "" {
		pr.CompanyCode = sm.Company
	}
	if pr.Party == "" && sm.Party != "" {
		pr.Party = sm.Party
	}
	if pr.Currency == "" && sm.Currency != "" {
		pr.Currency = sm.Currency
	}
	if pr.Source == "" && sm.Source != "" {
		pr.Source = sm.Source
	}
	if pr.DocumentDate == "" && sm.DocumentDate != "" {
		pr.DocumentDate = sm.DocumentDate
	}
	if pr.PostingDate == "" && sm.PostingDate != "" {
		pr.PostingDate = sm.PostingDate
	}
	if pr.NetDueDate == "" && sm.NetDue != "" {
		pr.NetDueDate = sm.NetDue
	}
}

func EditAllocationsHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req EditAllocationRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			httpError(w, http.StatusBadRequest, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}

		// if req.UserID == "" {
		// 	api.RespondWithError(w, http.StatusBadRequest, constants.ErrUserIDRequired)
		// 	return
		// }
		userName := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userName = s.Name
				break
			}
		}
		if req.BatchID == "" {
			httpError(w, http.StatusBadRequest, constants.BatchIDsRequired)
			return
		}
		batchUUID, err := uuid.Parse(req.BatchID)
		if err != nil {
			httpError(w, http.StatusBadRequest, constants.ErrInvalidBatchID)
			return
		}
		if !policyruntime.Enforce(ctx, w, r, pool, policyruntime.EnforceInput{
			EventCode:           common.TriggerPreEdit,
			ModuleCode:          common.ModuleFX,
			SubModule:           "EXPOSURE_CREATION",
			EntityCode:          req.BatchID,
			ActorUserID:         req.UserID,
			HandlerName:         "EditAllocationsHandler",
			APIPath:             r.URL.Path,
			DefaultBlockMessage: "Exposure allocation edit blocked by policy",
			Fields: map[string]interface{}{
				"batch_id": req.BatchID,
			},
		}) {
			return
		}

		conn, err := pool.Acquire(ctx)
		if err != nil {
			httpError(w, 500, constants.ErrDBAcquire+err.Error())
			return
		}
		defer conn.Release()

		tx, err := conn.Begin(ctx)
		if err != nil {
			httpError(w, 500, constants.ErrTxBegin+err.Error())
			return
		}
		committed := false
		defer func() {
			if !committed {
				_ = tx.Rollback(ctx)
			}
		}()

		var batchStatus string
		var fileHash sql.NullString
		if err := tx.QueryRow(ctx, `SELECT status,file_hash FROM public.staging_batches_exposures WHERE batch_id=$1`, batchUUID).Scan(&batchStatus, &fileHash); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				httpError(w, 404, constants.ErrBatchNotFound)
				return
			}
			httpError(w, 500, "batch lookup: "+err.Error())
			return
		}
		if strings.ToLower(batchStatus) != "completed" {
			httpError(w, http.StatusBadRequest, "batch not in 'completed' status")
			return
		}

		type reqAllocItem struct {
			BaseDoc string
			Knock   string
			AbsAmt  decimal.Decimal
			SignAmt *decimal.Decimal
			Group   struct {
				Source, CompanyCode, Party, Currency string
			}
		}

		reqAllocs := make([]reqAllocItem, 0, 128)
		reqSum := map[string]decimal.Decimal{}
		errorsList := make([]string, 0)
		overrideSource := map[string]string{}

		for _, g := range req.Groups {
			cur := strings.ToUpper(strings.TrimSpace(g.Currency))
			if cur == "" {
				errorsList = append(errorsList, fmt.Sprintf("Currency is required for allocation group (Company: %s, Party: %s, Source: %s)", g.CompanyCode, g.Party, g.Source))
				continue
			}
			for _, a := range g.Allocations {
				base := strings.TrimSpace(a.BaseDoc)
				knock := strings.TrimSpace(a.KnockDoc)
				if base == "" || knock == "" {
					errorsList = append(errorsList, "invalid allocation: base_document_id or knockoff_document_id missing")
					continue
				}
				abs := decimal.NewFromFloat(math.Abs(a.AllocationAmountAbs))
				var signPtr *decimal.Decimal
				if a.AllocationAmountSigned != nil {
					sd := decimal.NewFromFloat(*a.AllocationAmountSigned)
					signPtr = &sd
				}
				item := reqAllocItem{BaseDoc: base, Knock: knock, AbsAmt: abs, SignAmt: signPtr}
				item.Group = struct {
					Source, CompanyCode, Party, Currency string
				}{strings.ToUpper(g.Source), g.CompanyCode, g.Party, cur}
				reqAllocs = append(reqAllocs, item)
				reqSum[item.BaseDoc] = reqSum[item.BaseDoc].Add(abs)
				overrideSource[base] = g.Source
				overrideSource[knock] = g.Source
			}
		}

		if len(reqAllocs) == 0 {
			_ = tx.Rollback(ctx)
			previewRes, statusCode, err := buildPreviewForBatch(pool, ctx, batchUUID, overrideSource)
			if err != nil {
				httpError(w, 500, constants.ErrPreviewBuild+err.Error())
				return
			}
			previewRes.Errors = append(previewRes.Errors, "no allocations supplied")
			if statusCode <= 0 {
				statusCode = http.StatusUnprocessableEntity
			}
			respondEnvelopeFailureWithData(w, statusCode, "no allocations supplied", "VALIDATION_FAILED", map[string]interface{}{
				"results": []UploadResult{previewRes},
			})
			return
		}

		if len(errorsList) > 0 {
			_ = tx.Rollback(ctx)
			previewRes, _, err := buildPreviewForBatch(pool, ctx, batchUUID, overrideSource)
			if err != nil {
				httpError(w, 500, constants.ErrPreviewBuild+err.Error())
				return
			}
			previewRes.Errors = append(previewRes.Errors, errorsList...)
			respondEnvelopeFailureWithData(w, http.StatusUnprocessableEntity, "allocation validation failed", "VALIDATION_FAILED", map[string]interface{}{
				"results": []UploadResult{previewRes},
			})
			return
		}

		baseDocsArr := make([]string, 0, len(reqSum))
		for k := range reqSum {
			baseDocsArr = append(baseDocsArr, k)
		}

		// Original document capacity = remaining (post-FIFO) + existing allocations.
		// Fully knocked docs have remaining 0 and only live in exposure_allocations;
		// validating against remaining alone incorrectly rejects valid re-saves (V7 uses originals).
		rows, err := tx.Query(ctx, `
WITH all_docs AS (
    SELECT h.batch_id, h.document_id AS doc_id
      FROM public.exposure_headers h
     WHERE h.batch_id = $1
    UNION
    SELECT u.batch_id, u.document_number AS doc_id
      FROM public.exposure_unallocated u
     WHERE u.batch_id = $1
    UNION
    SELECT a.batch_id, a.base_document_id AS doc_id
      FROM public.exposure_allocations a
     WHERE a.batch_id = $1
    UNION
    SELECT a.batch_id, a.knockoff_document_id AS doc_id
      FROM public.exposure_allocations a
     WHERE a.batch_id = $1
),
alloc_as_base AS (
    SELECT base_document_id AS doc_id,
           SUM(ABS(allocation_amount::numeric)) AS abs_sum,
           SUM(COALESCE(allocation_amount_signed::numeric, 0)) AS signed_sum
      FROM public.exposure_allocations
     WHERE batch_id = $1
     GROUP BY base_document_id
),
alloc_as_knock AS (
    SELECT knockoff_document_id AS doc_id,
           SUM(ABS(allocation_amount::numeric)) AS abs_sum
      FROM public.exposure_allocations
     WHERE batch_id = $1
     GROUP BY knockoff_document_id
)
SELECT
  ad.doc_id AS document_number,
  COALESCE(
      h.company_code,
      u.company_code,
      MAX(a.company_code) FILTER (WHERE a.company_code IS NOT NULL),
      ''
  ) AS company_code,
  COALESCE(
      u.party,
      h.counterparty_code,
      MAX(a.counterparty_code) FILTER (WHERE a.counterparty_code IS NOT NULL),
      ''
  ) AS party,
  COALESCE(
      h.currency,
      u.currency,
      MAX(a.allocation_currency) FILTER (WHERE a.allocation_currency IS NOT NULL),
      ''
  ) AS currency,
  COALESCE(
      h.additional_header_details->>'Source',
      u.source,
      sb.ingestion_source,
      ''
  ) AS source,
  COALESCE(u.amount_signed::numeric, h.total_open_amount, 0)::text AS remaining_signed,
  (
      ABS(COALESCE(u.amount_signed::numeric, h.total_open_amount, 0))
      + COALESCE(MAX(ab.abs_sum), 0)
      + CASE
          WHEN COALESCE(MAX(ab.abs_sum), 0) > 0 THEN 0::numeric
          ELSE COALESCE(MAX(ak.abs_sum), 0)
        END
  )::text AS original_abs,
  (
      CASE
        WHEN COALESCE(MAX(ab.abs_sum), 0) > 0 THEN
          SIGN(COALESCE(u.amount_signed::numeric, h.total_open_amount, -1::numeric))
          * (
              ABS(COALESCE(u.amount_signed::numeric, h.total_open_amount, 0))
              + COALESCE(MAX(ab.abs_sum), 0)
            )
        WHEN COALESCE(MAX(ak.abs_sum), 0) > 0 THEN
          SIGN(COALESCE(NULLIF(u.amount_signed::numeric, 0), NULLIF(h.total_open_amount, 0), 1::numeric))
          * (
              ABS(COALESCE(u.amount_signed::numeric, h.total_open_amount, 0))
              + COALESCE(MAX(ak.abs_sum), 0)
            )
        ELSE
          COALESCE(u.amount_signed::numeric, h.total_open_amount, 0)
      END
  )::text AS original_signed,
  u.mapped_payload AS unalloc_mapped_payload
FROM all_docs ad
LEFT JOIN public.exposure_headers h
  ON h.batch_id = ad.batch_id AND h.document_id = ad.doc_id
LEFT JOIN public.exposure_unallocated u
  ON u.batch_id = ad.batch_id AND u.document_number = ad.doc_id
LEFT JOIN public.exposure_allocations a
  ON a.batch_id = ad.batch_id AND (a.base_document_id = ad.doc_id OR a.knockoff_document_id = ad.doc_id)
LEFT JOIN alloc_as_base ab ON ab.doc_id = ad.doc_id
LEFT JOIN alloc_as_knock ak ON ak.doc_id = ad.doc_id
LEFT JOIN public.staging_batches_exposures sb
  ON sb.batch_id = ad.batch_id
GROUP BY
  ad.doc_id, h.company_code, u.company_code, u.party, h.counterparty_code,
  h.currency, u.currency, sb.ingestion_source,
  u.amount_signed, h.total_open_amount, h.additional_header_details, u.source,
  u.mapped_payload
`, batchUUID)

		if err != nil {
			httpError(w, 500, "fetch base docs: "+err.Error())
			return
		}
		defer rows.Close()

		type docMeta struct {
			RemainingSigned decimal.Decimal
			OriginalAbs     decimal.Decimal
			OriginalSigned  decimal.Decimal
			Currency        string
			CompanyCode     string
			Party           string
		}
		dbBase := map[string]docMeta{}

		for rows.Next() {
			var (
				docNum, company, party, currency, source        string
				remainingStr, originalAbsStr, originalSignedStr string
				unallocPayload                                  []byte
			)

			if err := rows.Scan(&docNum, &company, &party, &currency, &source,
				&remainingStr, &originalAbsStr, &originalSignedStr, &unallocPayload); err != nil {
				errorsList = append(errorsList, fmt.Sprintf("scan base row failed: %v", err))
				continue
			}

			meta := docMeta{
				RemainingSigned: parseDecimalFromString(remainingStr),
				OriginalAbs:     parseDecimalFromString(originalAbsStr),
				OriginalSigned:  parseDecimalFromString(originalSignedStr),
				Currency:        strings.ToUpper(currency),
				CompanyCode:     company,
				Party:           party,
			}
			if origSigned, ok := originalAmountFromMappedPayload(unallocPayload); ok {
				meta.OriginalSigned = origSigned
				meta.OriginalAbs = origSigned.Abs()
			}
			dbBase[docNum] = meta
		}

		reqColSum := map[string]decimal.Decimal{}
		for _, a := range reqAllocs {
			reqColSum[a.Knock] = reqColSum[a.Knock].Add(a.AbsAmt)
		}

		eps := decimal.NewFromFloat(0.01)
		for base, reqTotal := range reqSum {
			info, ok := dbBase[base]
			if !ok {
				errorsList = append(errorsList, "missing base document: "+base)
				continue
			}
			maxBase := info.OriginalAbs
			if reqTotal.GreaterThan(maxBase.Add(eps)) {
				errorsList = append(errorsList, fmt.Sprintf("allocation exceeds base %s (available %s < requested %s)", base, maxBase.StringFixed(2), reqTotal.StringFixed(2)))
			}
		}
		for knock, reqTotal := range reqColSum {
			info, ok := dbBase[knock]
			if !ok {
				errorsList = append(errorsList, "missing knock document: "+knock)
				continue
			}
			maxKnock := info.OriginalAbs
			if reqTotal.GreaterThan(maxKnock.Add(eps)) {
				errorsList = append(errorsList, fmt.Sprintf("allocation exceeds knock %s (available %s < requested %s)", knock, maxKnock.StringFixed(2), reqTotal.StringFixed(2)))
			}
		}

		if len(errorsList) > 0 {
			_ = tx.Rollback(ctx)
			previewRes, _, err := buildPreviewForBatch(pool, ctx, batchUUID, overrideSource)
			if err != nil {
				httpError(w, 500, constants.ErrPreviewBuild+err.Error())
				return
			}
			previewRes.Errors = append(previewRes.Errors, errorsList...)
			respondEnvelopeFailureWithData(w, http.StatusUnprocessableEntity, "allocation validation failed", "VALIDATION_FAILED", map[string]interface{}{
				"results": []UploadResult{previewRes},
			})
			return
		}

		stagingByDoc := loadStagingMetaForBatch(ctx, conn, batchUUID)

		type prevAllocMeta struct {
			source                            string
			amountAbs, amountSigned           string
			docDate, postDate, netDue, effDue interface{}
			mappedPayload                     []byte
		}
		prevAllocMetaMap := map[string]prevAllocMeta{}
		prevRows, prevErr := tx.Query(ctx, `
			SELECT base_document_id, knockoff_document_id, source,
			       COALESCE(allocation_amount::text,'0'),
			       COALESCE(allocation_amount_signed::text,'0'),
			       document_date, posting_date, net_due_date, effective_due_date, mapped_payload
			  FROM public.exposure_allocations
			 WHERE batch_id=$1
		`, batchUUID)
		if prevErr == nil {
			for prevRows.Next() {
				var baseID, knockID string
				var src sql.NullString
				var amountAbs, amountSigned string
				var docDate, postDate, netDue, effDue sql.NullTime
				var mappedPayload []byte
				if scanErr := prevRows.Scan(&baseID, &knockID, &src, &amountAbs, &amountSigned,
					&docDate, &postDate, &netDue, &effDue, &mappedPayload); scanErr != nil {
					continue
				}
				key := strings.TrimSpace(baseID) + "|" + strings.TrimSpace(knockID)
				pm := prevAllocMeta{amountAbs: amountAbs, amountSigned: amountSigned, mappedPayload: mappedPayload}
				if src.Valid {
					pm.source = src.String
				}
				if docDate.Valid {
					pm.docDate = docDate.Time
				}
				if postDate.Valid {
					pm.postDate = postDate.Time
				}
				if netDue.Valid {
					pm.netDue = netDue.Time
				}
				if effDue.Valid {
					pm.effDue = effDue.Time
				}
				prevAllocMetaMap[key] = pm
			}
			prevRows.Close()
		}

		if len(baseDocsArr) > 0 {
			_, _ = tx.Exec(ctx, `DELETE FROM public.exposure_allocations WHERE batch_id=$1 AND base_document_id=ANY($2::text[])`, batchUUID, baseDocsArr)
		}

		// stmt := `
		// 	INSERT INTO public.exposure_allocations
		// 	(allocation_id,batch_id,file_hash,base_document_id,knockoff_document_id,
		// 	 allocation_amount,allocation_currency,allocation_amount_signed,
		// 	 allocation_date,created_at,created_by)
		// 	VALUES ($1,$2,$3,$4,$5,$6,$7,$8,now(),now(),$9)
		// `
		stmt := `
INSERT INTO public.exposure_allocations
(allocation_id,batch_id,file_hash,base_document_id,knockoff_document_id,
 allocation_amount,allocation_currency,allocation_amount_signed,
 company_code,counterparty_code, source,
 document_date, posting_date, net_due_date, effective_due_date,
 allocation_date,created_at,created_by, mapped_payload)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,now(),now(),$16,$17)
`

		insertedCount := 0
		newAllocSignedByBase := map[string]decimal.Decimal{}
		newAllocAbsByBase := map[string]decimal.Decimal{}
		for _, a := range reqAllocs {
			var allocSigned decimal.Decimal
			if a.SignAmt != nil {
				allocSigned = *a.SignAmt
			} else {
				info, ok := dbBase[a.BaseDoc]
				sign := float64(1)
				if ok {
					if !info.OriginalSigned.IsZero() {
						sign = math.Copysign(1, info.OriginalSigned.InexactFloat64())
					} else if !info.RemainingSigned.IsZero() {
						sign = math.Copysign(1, info.RemainingSigned.InexactFloat64())
					} else {
						// FBL1N/FBL3N standard bases are credits (negative).
						src := strings.ToUpper(a.Group.Source)
						if src == "FBL1N" || src == "FBL3N" {
							sign = -1
						}
					}
				}
				allocSigned = a.AbsAmt.Mul(decimal.NewFromFloat(sign))
			}

			// fallback: if group-level company_code/party are empty, infer from base doc
			if a.Group.CompanyCode == "" {
				if info, ok := dbBase[a.BaseDoc]; ok && info.CompanyCode != "" {
					a.Group.CompanyCode = info.CompanyCode
				}
			}
			if a.Group.Party == "" {
				if info, ok := dbBase[a.BaseDoc]; ok && info.Party != "" {
					a.Group.Party = info.Party
				}
			}

			metaKey := a.BaseDoc + "|" + a.Knock
			var srcVal interface{}
			var docDateVal, postDateVal, netDueVal, effDueVal interface{}
			var mappedPayload interface{}
			if pm, ok := prevAllocMetaMap[metaKey]; ok {
				if pm.source != "" {
					srcVal = pm.source
				}
				if pm.docDate != nil {
					docDateVal = pm.docDate
				}
				if pm.postDate != nil {
					postDateVal = pm.postDate
				}
				if pm.netDue != nil {
					netDueVal = pm.netDue
				}
				if pm.effDue != nil {
					effDueVal = pm.effDue
				}
				if len(pm.mappedPayload) > 0 {
					mappedPayload = pm.mappedPayload
				}
			}
			if srcVal == nil && a.Group.Source != "" {
				srcVal = a.Group.Source
			}
			if sm, ok := stagingByDoc[a.BaseDoc]; ok {
				if srcVal == nil && sm.Source != "" {
					srcVal = sm.Source
				}
				if docDateVal == nil && sm.DocumentDate != "" {
					docDateVal = parseDateOrNil(sm.DocumentDate)
				}
				if postDateVal == nil && sm.PostingDate != "" {
					postDateVal = parseDateOrNil(sm.PostingDate)
				}
				if netDueVal == nil && sm.NetDue != "" {
					netDueVal = parseDateOrNil(sm.NetDue)
				}
				if effDueVal == nil && sm.NetDue != "" {
					effDueVal = parseDateOrNil(sm.NetDue)
				}
				if mappedPayload == nil && len(sm.MappedPayload) > 0 {
					mappedPayload = sm.MappedPayload
				}
			}

			_, err := tx.Exec(ctx, stmt,
				uuid.New(), batchUUID, fileHash.String,
				a.BaseDoc, a.Knock,
				a.AbsAmt.StringFixed(4), a.Group.Currency,
				allocSigned.StringFixed(4),
				a.Group.CompanyCode, a.Group.Party,
				srcVal, docDateVal, postDateVal, netDueVal, effDueVal,
				userName, mappedPayload,
			)
			if err != nil {
				_ = tx.Rollback(ctx)
				httpError(w, 500, "insert alloc: "+err.Error())
				return
			}
			insertedCount++
			newAllocSignedByBase[a.BaseDoc] = newAllocSignedByBase[a.BaseDoc].Add(allocSigned)
			newAllocAbsByBase[a.BaseDoc] = newAllocAbsByBase[a.BaseDoc].Add(a.AbsAmt)

			// Persist an adjustment record for audit / history (mirror single-edit behavior)
			adjustmentJSON, _ := json.Marshal(map[string]interface{}{
				"base_document":     a.BaseDoc,
				"knockoff_document": a.Knock,
				"allocation_abs":    a.AbsAmt.StringFixed(4),
				"allocation_signed": allocSigned.StringFixed(4),
				"currency":          a.Group.Currency,
				"group":             fmt.Sprintf("%s|%s|%s|%s", a.Group.Source, a.Group.CompanyCode, a.Group.Party, a.Group.Currency),
			})
			_, _ = tx.Exec(ctx, `
				INSERT INTO public.exposure_adjustments
				(batch_id, file_hash, reference_document_number, adjustment_type,
				adjustment_json, adjustment_amount, created_by, remarks)
				VALUES ($1,$2,$3,'manual_allocation',$4,$5,$6,$7)
			`, batchUUID, fileHash.String, a.BaseDoc, adjustmentJSON, a.AbsAmt.StringFixed(4), userName, "manual-edit")
		}

		// Recompute remaining from ORIGINAL (remaining+old allocs), not by subtracting again
		// from already-net amount_signed (that double-counted on every edit).
		for baseDoc, original := range dbBase {
			if _, touched := reqSum[baseDoc]; !touched {
				continue
			}
			newSigned := original.OriginalSigned.Sub(newAllocSignedByBase[baseDoc])
			newAbs := newSigned.Abs()
			allocAbs := newAllocAbsByBase[baseDoc]
			status := "unallocated"
			if allocAbs.GreaterThan(decimal.Zero) {
				if newAbs.LessThanOrEqual(eps) {
					status = "fully_allocated"
				} else {
					status = "partially_allocated"
				}
			}
			_, _ = tx.Exec(ctx, `
				UPDATE public.exposure_unallocated
				   SET amount_signed = $3::numeric,
				       amount = $4::numeric,
				       allocation_status = $5
				 WHERE batch_id = $1 AND document_number = $2
			`, batchUUID, baseDoc, newSigned.StringFixed(4), newAbs.StringFixed(4), status)

			_, _ = tx.Exec(ctx, `
				UPDATE public.exposure_headers
				   SET total_open_amount = $3::numeric
				 WHERE batch_id = $1 AND document_id = $2
			`, batchUUID, baseDoc, newAbs.StringFixed(4))
		}

		// One consolidated EDIT audit for the upload with allocation before/after diff.
		if strings.TrimSpace(userName) != "" {
			oldAllocMap := make(map[string]string, len(prevAllocMetaMap))
			for key, pm := range prevAllocMetaMap {
				oldAllocMap[key] = pm.amountSigned
				if oldAllocMap[key] == "" || oldAllocMap[key] == "0" {
					oldAllocMap[key] = pm.amountAbs
				}
			}
			newAllocMap := make(map[string]string, len(reqAllocs))
			for _, a := range reqAllocs {
				key := strings.TrimSpace(a.BaseDoc) + "|" + strings.TrimSpace(a.Knock)
				var signedStr string
				if a.SignAmt != nil {
					signedStr = a.SignAmt.StringFixed(4)
				} else {
					signedStr = a.AbsAmt.StringFixed(4)
				}
				newAllocMap[key] = signedStr
			}

			allKeys := make([]string, 0, len(oldAllocMap)+len(newAllocMap))
			seen := map[string]struct{}{}
			for k := range oldAllocMap {
				if _, ok := seen[k]; !ok {
					seen[k] = struct{}{}
					allKeys = append(allKeys, k)
				}
			}
			for k := range newAllocMap {
				if _, ok := seen[k]; !ok {
					seen[k] = struct{}{}
					allKeys = append(allKeys, k)
				}
			}
			sort.Strings(allKeys)

			changeSummary := make([]map[string]interface{}, 0)
			for _, key := range allKeys {
				oldAmt := strings.TrimSpace(oldAllocMap[key])
				newAmt := strings.TrimSpace(newAllocMap[key])
				if oldAmt == newAmt {
					continue
				}
				parts := strings.SplitN(key, "|", 2)
				baseDoc, knockDoc := key, ""
				if len(parts) == 2 {
					baseDoc, knockDoc = parts[0], parts[1]
				}
				fieldLabel := "Allocation"
				if knockDoc != "" {
					fieldLabel = fmt.Sprintf("Allocation %s → %s", baseDoc, knockDoc)
				}
				changeSummary = append(changeSummary, map[string]interface{}{
					"field":     fieldLabel,
					"old_value": oldAmt,
					"new_value": newAmt,
				})
			}

			oldValuesJSON, _ := json.Marshal(oldAllocMap)
			newValuesJSON, _ := json.Marshal(newAllocMap)
			changeSummaryJSON, _ := json.Marshal(changeSummary)

			_, _ = tx.Exec(ctx, `
				INSERT INTO public.auditactionexposure
					(exposure_header_id, actiontype, processing_status, reason,
					 requested_by, requested_at, requested_ip, old_values,
					 new_values, change_summary)
				VALUES (
					$1::text,
					'EDIT',
					$2::text,
					'manual FIFO allocation override',
					$3::text,
					now(),
					NULLIF($4::text, ''),
					$5::jsonb,
					$6::jsonb,
					$7::jsonb
				)
			`, batchUUID.String(), constants.StatusPendingEditApproval, userName,
				api.ClientIPFromContext(ctx), string(oldValuesJSON), string(newValuesJSON), string(changeSummaryJSON))

			adjustmentBatchJSON, _ := json.Marshal(map[string]interface{}{
				"batch_id":          batchUUID.String(),
				"action":            "manual_allocation_edit",
				"allocations_count": insertedCount,
				"base_documents":    baseDocsArr,
			})
			_, _ = tx.Exec(ctx, `
				INSERT INTO public.exposure_adjustments
				(batch_id, file_hash, reference_document_number, adjustment_type,
				adjustment_json, adjustment_amount, created_by, remarks)
				VALUES ($1,$2,$3,'manual_allocation_batch',$4,$5,$6,$7)
			`, batchUUID, fileHash.String, batchUUID.String(), adjustmentBatchJSON,
				fmt.Sprintf("%d", insertedCount), userName, "manual-edit-apply")
		}

		if err := tx.Commit(ctx); err != nil {
			httpError(w, 500, constants.ErrCommitFailed+err.Error())
			return
		}
		committed = true

		previewRes, _, err := buildPreviewForBatch(pool, ctx, batchUUID, overrideSource)
		if err != nil {
			httpError(w, 500, constants.ErrPreviewBuild+err.Error())
			return
		}
		previewRes.Info = append(previewRes.Info,
			fmt.Sprintf("Edit applied: %d allocations inserted (replaced previous allocations) for batch %s by user %s",
				insertedCount, batchUUID.String(), userName))
		respondEnvelopeSuccess(w, "Allocation edit applied successfully", map[string]interface{}{
			"results": []UploadResult{previewRes},
		})

		exposureIDs := fxnotif.FetchExposureIDsByBatch(ctx, pool, batchUUID.String())
		fxnotif.NotifyExposureBulkAction(ctx, pool, fxnotif.BulkActionNotifyInput{
			SourceRoute: fxnotif.SourceRouteV91EditAllocation, Action: fxnotif.ActionUpdate, UserID: req.UserID, RequestedBy: userName, CheckerComment: "",
			ExposureIDs: exposureIDs, ResultBuckets: nil,
		})
	}
}

func buildPreviewForBatch(pool *pgxpool.Pool, ctx context.Context, batchUUID uuid.UUID, overrideSource map[string]string) (UploadResult, int, error) {
	var res UploadResult
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return res, 500, err
	}
	defer conn.Release()

	var totalRecords sql.NullInt64
	var fileName, ingestionSource sql.NullString
	if err := conn.QueryRow(ctx, `SELECT total_records,file_name,ingestion_source FROM public.staging_batches_exposures WHERE batch_id=$1`, batchUUID).Scan(&totalRecords, &fileName, &ingestionSource); err != nil {
		return res, 500, err
	}

	var liCount int
	_ = conn.QueryRow(ctx, `SELECT COUNT(li.*) FROM public.exposure_line_items li JOIN public.exposure_headers h ON li.exposure_header_id=h.exposure_header_id WHERE h.batch_id=$1`, batchUUID).Scan(&liCount)

	headers := map[string]struct {
		Company, Party, Currency, Source, AllocationStatus string
		DocDate, PostDate, NetDue                          sql.NullTime
		AmountSignedText, TotalOrigText                    sql.NullString
	}{}

	// collect errors encountered so we can return them in UploadResult.Errors
	errorsList := make([]string, 0)

	rows, err := conn.Query(ctx, `
		SELECT h.document_id,COALESCE(h.company_code,''),COALESCE(u.party,h.counterparty_code,''),COALESCE(h.currency,u.currency,''),COALESCE(h.additional_header_details->>'Source',sb.ingestion_source,''),h.document_date,h.posting_date,u.net_due_date,u.amount_signed::text,h.total_original_amount::text,COALESCE(u.allocation_status,'unallocated')
		FROM public.exposure_headers h
		LEFT JOIN public.exposure_unallocated u ON u.batch_id=h.batch_id AND u.document_number=h.document_id
		LEFT JOIN public.staging_batches_exposures sb ON sb.batch_id=h.batch_id
		WHERE h.batch_id=$1
	`, batchUUID)
	if err != nil {
		errorsList = append(errorsList, fmt.Sprintf("query headers: %v", err))
	} else {
		for rows.Next() {
			var id, comp, party, curr, src, alloc string
			var d1, d2, d3 sql.NullTime
			var a1, a2 sql.NullString
			if scanErr := rows.Scan(&id, &comp, &party, &curr, &src, &d1, &d2, &d3, &a1, &a2, &alloc); scanErr != nil {
				errorsList = append(errorsList, fmt.Sprintf("scan header row: %v", scanErr))
				continue
			}
			headers[id] = struct {
				Company, Party, Currency, Source, AllocationStatus string
				DocDate, PostDate, NetDue                          sql.NullTime
				AmountSignedText, TotalOrigText                    sql.NullString
			}{comp, party, curr, src, alloc, d1, d2, d3, a1, a2}
		}
		rows.Close()
	}

	// allocMap: outgoing (base -> []knockoffs) with currency attached
	allocMap := map[string][]KnockoffInfo{}
	// reverseAllocMap: incoming (knock -> []bases) with currency attached
	reverseAllocMap := map[string][]KnockoffInfo{}

	// allocation-level metadata fallbacks (filled from allocations/unallocated/unqualified mapped_payload)
	stagingMeta := map[string]struct {
		Company, Party, Currency, Source  string
		DocumentDate, PostingDate, NetDue string
		Amount                            decimal.Decimal
	}{}
	// allocation-level metadata fallbacks (filled from allocations query)
	allocSourceByBase := map[string]string{}
	allocCompanyByBase := map[string]string{}
	allocPartyByBase := map[string]string{}
	allocSourceByKnock := map[string]string{}
	allocCompanyByKnock := map[string]string{}
	allocPartyByKnock := map[string]string{}
	allocRows, err := conn.Query(ctx, `
		SELECT base_document_id,
			   knockoff_document_id,
			   allocation_amount::text,
			   allocation_currency,
			   company_code,
			   counterparty_code,
			   source,
			   document_date,
			   posting_date,
			   net_due_date,
			   allocation_amount_signed::text,
			   amount_local_signed::text,
			   mapped_payload
		FROM public.exposure_allocations
		WHERE batch_id=$1
	`, batchUUID)
	if err != nil {
		errorsList = append(errorsList, fmt.Sprintf("query allocations: %v", err))
	} else {
		for allocRows.Next() {
			var base, knock, curr, company, party, src sql.NullString
			var docDate, postDate, netDue sql.NullTime
			var amtText, amtSignedText, amtLocalText sql.NullString
			var mappedPayload []byte
			if scanErr := allocRows.Scan(&base, &knock, &amtText, &curr, &company, &party, &src, &docDate, &postDate, &netDue, &amtSignedText, &amtLocalText, &mappedPayload); scanErr != nil {
				errorsList = append(errorsList, fmt.Sprintf("scan alloc row: %v", scanErr))
				continue
			}
			baseStr := strings.TrimSpace(base.String)
			knockStr := strings.TrimSpace(knock.String)
			currStr := strings.ToUpper(strings.TrimSpace(curr.String))

			// parse numeric text into decimals (handle NULLs)
			amt := decimal.Zero
			if amtText.Valid && strings.TrimSpace(amtText.String) != "" {
				if d, derr := decimal.NewFromString(strings.TrimSpace(amtText.String)); derr == nil {
					amt = d
				}
			}
			signedAmt := decimal.Zero
			if amtSignedText.Valid && strings.TrimSpace(amtSignedText.String) != "" {
				if d, derr := decimal.NewFromString(strings.TrimSpace(amtSignedText.String)); derr == nil {
					signedAmt = d
				}
			}

			info := KnockoffInfo{
				BaseDoc:   baseStr,
				KnockDoc:  knockStr,
				AmtAbs:    amt,
				Currency:  currStr,
				SignedAmt: signedAmt,
			}

			// attach allocation-level dates if available
			docDateStr := ""
			if docDate.Valid {
				docDateStr = docDate.Time.Format(constants.DateFormat)
			}
			postDateStr := ""
			if postDate.Valid {
				postDateStr = postDate.Time.Format(constants.DateFormat)
			}
			netDueStr := ""
			if netDue.Valid {
				netDueStr = netDue.Time.Format(constants.DateFormat)
			}
			info.DocumentDate = docDateStr
			info.PostingDate = postDateStr
			info.NetDueDate = netDueStr

			if baseStr != "" {
				allocMap[baseStr] = append(allocMap[baseStr], info)
				if strings.TrimSpace(company.String) != "" && allocCompanyByBase[baseStr] == "" {
					allocCompanyByBase[baseStr] = strings.TrimSpace(company.String)
				}
				if strings.TrimSpace(party.String) != "" && allocPartyByBase[baseStr] == "" {
					allocPartyByBase[baseStr] = strings.TrimSpace(party.String)
				}
				if strings.TrimSpace(src.String) != "" && allocSourceByBase[baseStr] == "" {
					allocSourceByBase[baseStr] = strings.TrimSpace(src.String)
				}
			}
			if knockStr != "" {
				reverseAllocMap[knockStr] = append(reverseAllocMap[knockStr], info)
				if strings.TrimSpace(company.String) != "" && allocCompanyByKnock[knockStr] == "" {
					allocCompanyByKnock[knockStr] = strings.TrimSpace(company.String)
				}
				if strings.TrimSpace(party.String) != "" && allocPartyByKnock[knockStr] == "" {
					allocPartyByKnock[knockStr] = strings.TrimSpace(party.String)
				}
				if strings.TrimSpace(src.String) != "" && allocSourceByKnock[knockStr] == "" {
					allocSourceByKnock[knockStr] = strings.TrimSpace(src.String)
				}
			}

			// If this allocation row carries the mapped_payload (original CSV mapping),
			// extract the original document amount and metadata so preview can show
			// the CSV amount for docs that don't have an exposure_headers row.
			if len(mappedPayload) > 0 {
				var mp map[string]interface{}
				if merr := json.Unmarshal(mappedPayload, &mp); merr == nil {
					docNum := strings.TrimSpace(stringFromAny(mp["DocumentNumber"]))
					if docNum == "" {
						// try alternative keys
						docNum = strings.TrimSpace(stringFromAny(mp["DocumentID"]))
					}
					if docNum != "" {
						amt := parseDecimalFromAny(mp["AmountDoc"])
						if amt.IsZero() {
							amt = parseDecimalFromAny(mp["Amount"])
						}
						// store if not present
						if _, ok := stagingMeta[docNum]; !ok {
							stagingMeta[docNum] = struct {
								Company, Party, Currency, Source  string
								DocumentDate, PostingDate, NetDue string
								Amount                            decimal.Decimal
							}{
								Company:      stringFromAny(mp["CompanyCode"]),
								Party:        stringFromAny(mp["Party"]),
								Currency:     stringFromAny(mp["DocumentCurrency"]),
								Source:       stringFromAny(mp["Source"]),
								DocumentDate: stringFromAny(mp["DocumentDate"]),
								PostingDate:  stringFromAny(mp["PostingDate"]),
								NetDue:       stringFromAny(mp["NetDueDate"]),
								Amount:       amt,
							}
						}
					}
				}
			}
		}
		allocRows.Close()
	}

	// Read non-qualified rows and keep the NonQualified struct filled
	nonQualMap := map[string]NonQualified{}
	nqRows, err := conn.Query(ctx, `SELECT document_number, issues, non_qualified_reason, mapped_payload FROM public.exposure_unqualified WHERE batch_id=$1`, batchUUID)
	if err != nil {
		// non-critical — collect and continue
		errorsList = append(errorsList, fmt.Sprintf("query unqualified: %v", err))
	} else {
		for nqRows.Next() {
			var doc sql.NullString
			var issues []string
			var reason sql.NullString
			var payload []byte
			if scanErr := nqRows.Scan(&doc, &issues, &reason, &payload); scanErr != nil {
				// some older schemas store differently; try fallback scanned earlier in your version
				// We'll fallback to more permissive scan:
				var doc2 sql.NullString
				var issues2 []string
				_ = nqRows.Scan(&doc2, &issues2)
				if doc2.Valid {
					nonQualMap[strings.TrimSpace(doc2.String)] = NonQualified{Row: CanonicalRow{}, Issues: issues2}
				}
				continue
			}
			if !doc.Valid {
				continue
			}
			nq := NonQualified{Row: CanonicalRow{DocumentNumber: strings.TrimSpace(doc.String)}, Issues: issues}
			// attach the non_qualified_reason to the Row._raw / Row.NonQualifiedReason for completeness
			if reason.Valid {
				nq.Row.NonQualifiedReason = reason.String
			}
			// parse mapped_payload if present to capture original CSV amount/metadata and populate Row fields
			if len(payload) > 0 {
				var mp map[string]interface{}
				if merr := json.Unmarshal(payload, &mp); merr == nil {
					docNum := strings.TrimSpace(stringFromAny(mp["DocumentNumber"]))
					if docNum == "" {
						docNum = strings.TrimSpace(stringFromAny(mp["DocumentID"]))
					}
					// Populate CanonicalRow fields directly from mapped_payload
					nq.Row.CompanyCode = stringFromAny(mp["CompanyCode"])
					nq.Row.Party = stringFromAny(mp["Party"])
					nq.Row.DocumentCurrency = strings.ToUpper(stringFromAny(mp["DocumentCurrency"]))
					nq.Row.Source = stringFromAny(mp["Source"])
					nq.Row.DocumentDate = stringFromAny(mp["DocumentDate"])
					nq.Row.PostingDate = stringFromAny(mp["PostingDate"])
					nq.Row.NetDueDate = stringFromAny(mp["NetDueDate"])
					if docNum != "" {
						amt := parseDecimalFromAny(mp["AmountDoc"])
						if amt.IsZero() {
							amt = parseDecimalFromAny(mp["Amount"])
						}
						if !amt.IsZero() {
							nq.Row.AmountDoc = amt
						}
						if _, ok := stagingMeta[docNum]; !ok {
							stagingMeta[docNum] = struct {
								Company, Party, Currency, Source  string
								DocumentDate, PostingDate, NetDue string
								Amount                            decimal.Decimal
							}{
								Company:      nq.Row.CompanyCode,
								Party:        nq.Row.Party,
								Currency:     nq.Row.DocumentCurrency,
								Source:       nq.Row.Source,
								DocumentDate: nq.Row.DocumentDate,
								PostingDate:  nq.Row.PostingDate,
								NetDue:       nq.Row.NetDueDate,
								Amount:       amt,
							}
						}
					}
					// Store the raw payload for completeness
					nq.Row._raw = mp
				}
			}
			nonQualMap[strings.TrimSpace(doc.String)] = nq
		}
		nqRows.Close()
	}

	// Populate stagingMeta from exposure_unallocated and exposure_unqualified (and allocations above)
	// so we avoid reading the staging_exposures table which is not allowed in this context.
	// exposure_allocations mapped_payloads were parsed above and populated into stagingMeta.
	// Read exposure_unallocated mapped_payloads next.
	unallocRows, uerr := conn.Query(ctx, `SELECT document_number, mapped_payload, amount::text FROM public.exposure_unallocated WHERE batch_id=$1`, batchUUID)
	if uerr == nil {
		for unallocRows.Next() {
			var doc sql.NullString
			var payload []byte
			var amtText sql.NullString
			if uscan := unallocRows.Scan(&doc, &payload, &amtText); uscan != nil {
				continue
			}
			if !doc.Valid {
				continue
			}
			docNum := strings.TrimSpace(doc.String)
			if docNum == "" {
				continue
			}
			if len(payload) > 0 {
				var mp map[string]interface{}
				if merr := json.Unmarshal(payload, &mp); merr == nil {
					amt := parseDecimalFromAny(mp["AmountDoc"])
					if amt.IsZero() && amtText.Valid {
						if d, derr := decimal.NewFromString(strings.TrimSpace(amtText.String)); derr == nil {
							amt = d
						}
					}
					if _, ok := stagingMeta[docNum]; !ok {
						stagingMeta[docNum] = struct {
							Company, Party, Currency, Source  string
							DocumentDate, PostingDate, NetDue string
							Amount                            decimal.Decimal
						}{
							Company:      stringFromAny(mp["CompanyCode"]),
							Party:        stringFromAny(mp["Party"]),
							Currency:     strings.ToUpper(stringFromAny(mp["DocumentCurrency"])),
							Source:       stringFromAny(mp["Source"]),
							DocumentDate: stringFromAny(mp["DocumentDate"]),
							PostingDate:  stringFromAny(mp["PostingDate"]),
							NetDue:       stringFromAny(mp["NetDueDate"]),
							Amount:       amt,
						}
					}
				}
			}
		}
		unallocRows.Close()
	}

	stagingByDoc := loadStagingMetaForBatch(ctx, conn, batchUUID)
	for docNum, sm := range stagingByDoc {
		if _, ok := stagingMeta[docNum]; ok {
			continue
		}
		amt := decimal.Zero
		if orig, ok := originalAmountFromMappedPayload(sm.MappedPayload); ok {
			amt = orig
		}
		stagingMeta[docNum] = struct {
			Company, Party, Currency, Source  string
			DocumentDate, PostingDate, NetDue string
			Amount                            decimal.Decimal
		}{
			Company:      sm.Company,
			Party:        sm.Party,
			Currency:     sm.Currency,
			Source:       sm.Source,
			DocumentDate: sm.DocumentDate,
			PostingDate:  sm.PostingDate,
			NetDue:       sm.NetDue,
			Amount:       amt,
		}
	}

	// Build union of all docs to preview
	allDocsSet := map[string]struct{}{}
	for d := range headers {
		allDocsSet[d] = struct{}{}
	}
	for base, kos := range allocMap {
		allDocsSet[base] = struct{}{}
		for _, k := range kos {
			if k.KnockDoc != "" {
				allDocsSet[k.KnockDoc] = struct{}{}
			}
		}
	}
	for d := range nonQualMap {
		allDocsSet[d] = struct{}{}
	}
	for d := range reverseAllocMap {
		allDocsSet[d] = struct{}{}
	}

	docs := make([]string, 0, len(allDocsSet))
	postingDateByDoc := map[string]string{}
	for d := range allDocsSet {
		docs = append(docs, d)
		if sm, ok := stagingByDoc[d]; ok && sm.PostingDate != "" {
			postingDateByDoc[d] = sm.PostingDate
		}
	}
	sort.Slice(docs, func(i, j int) bool {
		pi := postingDateByDoc[docs[i]]
		pj := postingDateByDoc[docs[j]]
		if pi != pj {
			if pi == "" {
				return false
			}
			if pj == "" {
				return true
			}
			return pi < pj
		}
		return docs[i] < docs[j]
	})

	previewRows := make([]CanonicalPreviewRow, 0, len(docs))
	insertedCount := 0

	for _, doc := range docs {
		var pr CanonicalPreviewRow
		pr.DocumentNumber = doc

		// prefer header metadata
		if h, ok := headers[doc]; ok {
			pr.CompanyCode, pr.Party, pr.Currency, pr.Source = h.Company, h.Party, h.Currency, h.Source
			if h.DocDate.Valid {
				pr.DocumentDate = h.DocDate.Time.Format(constants.DateFormat)
			}
			if h.PostDate.Valid {
				pr.PostingDate = h.PostDate.Time.Format(constants.DateFormat)
			}
			if h.NetDue.Valid {
				pr.NetDueDate = h.NetDue.Time.Format(constants.DateFormat)
			}
			amt := decimal.Zero
			if h.AmountSignedText.Valid {
				if d, derr := decimal.NewFromString(strings.TrimSpace(h.AmountSignedText.String)); derr == nil {
					amt = d
				}
			}
			if amt.Equal(decimal.Zero) && h.TotalOrigText.Valid {
				if d, derr := decimal.NewFromString(strings.TrimSpace(h.TotalOrigText.String)); derr == nil {
					amt = d
				}
			}
			pr.Amount = amt
			switch h.AllocationStatus {
			case "fully_allocated":
				pr.Status = "knocked_off"
			case "partially_allocated":
				pr.Status = "ok"
			default:
				pr.Status = "ok"
			}
		} else if nq, ok := nonQualMap[doc]; ok {
			// non-qualified only
			pr.Status = "non_qualified"
			pr.Issues = append(pr.Issues, nq.Issues...)
		} else {
			pr.Status = "ok"
		}

		// attach outgoing and incoming allocations (with currency)
		if kos, ok := allocMap[doc]; ok {
			pr.Knockoffs = append(pr.Knockoffs, kos...)
		}
		if rin, ok := reverseAllocMap[doc]; ok {
			pr.Knockoffs = append(pr.Knockoffs, rin...)
		}

		applyStagingMetaToPreview(stagingByDoc, doc, &pr)

		// If parent row lacks date metadata, try to fill from allocation-level knockoffs
		if pr.DocumentDate == "" || pr.PostingDate == "" || pr.NetDueDate == "" {
			if kos, ok := allocMap[doc]; ok {
				for _, k := range kos {
					if pr.DocumentDate == "" && k.DocumentDate != "" {
						pr.DocumentDate = k.DocumentDate
					}
					if pr.PostingDate == "" && k.PostingDate != "" {
						pr.PostingDate = k.PostingDate
					}
					if pr.NetDueDate == "" && k.NetDueDate != "" {
						pr.NetDueDate = k.NetDueDate
					}
					if pr.DocumentDate != "" && pr.PostingDate != "" && pr.NetDueDate != "" {
						break
					}
				}
			}
			if pr.DocumentDate == "" || pr.PostingDate == "" || pr.NetDueDate == "" {
				if rin, ok := reverseAllocMap[doc]; ok {
					for _, k := range rin {
						if pr.DocumentDate == "" && k.DocumentDate != "" {
							pr.DocumentDate = k.DocumentDate
						}
						if pr.PostingDate == "" && k.PostingDate != "" {
							pr.PostingDate = k.PostingDate
						}
						if pr.NetDueDate == "" && k.NetDueDate != "" {
							pr.NetDueDate = k.NetDueDate
						}
						if pr.DocumentDate != "" && pr.PostingDate != "" && pr.NetDueDate != "" {
							break
						}
					}
				}
			}
		}

		// Currency priority:
		// 1) header currency (already set if header existed)
		// 2) allocation currency where doc is base (prefer first non-empty)
		// 3) allocation currency where doc is knock (prefer first non-empty)
		// 4) if still empty -> leave as empty but we will add an error to Errors (currency is required)
		if strings.TrimSpace(pr.Currency) == "" {
			// try outgoing allocations
			if kos, ok := allocMap[doc]; ok {
				for _, k := range kos {
					if strings.TrimSpace(k.Currency) != "" {
						pr.Currency = k.Currency
						break
					}
				}
			}
		}
		if strings.TrimSpace(pr.Currency) == "" {
			// try incoming allocations
			if rin, ok := reverseAllocMap[doc]; ok {
				for _, k := range rin {
					if strings.TrimSpace(k.Currency) != "" {
						pr.Currency = k.Currency
						break
					}
				}
			}
		}
		// If currency still empty, mark an error in errorsList (currency is required per your note)
		if strings.TrimSpace(pr.Currency) == "" {
			errorsList = append(errorsList, fmt.Sprintf("currency missing for document %s", doc))
		}

		// If header fields are empty, try to fall back to allocation-level metadata
		if strings.TrimSpace(pr.CompanyCode) == "" {
			if v, ok := allocCompanyByBase[doc]; ok && v != "" {
				pr.CompanyCode = v
			} else if v, ok := allocCompanyByKnock[doc]; ok && v != "" {
				pr.CompanyCode = v
			}
		}
		if strings.TrimSpace(pr.Party) == "" {
			if v, ok := allocPartyByBase[doc]; ok && v != "" {
				pr.Party = v
			} else if v, ok := allocPartyByKnock[doc]; ok && v != "" {
				pr.Party = v
			}
		}
		if strings.TrimSpace(pr.Source) == "" {
			if v, ok := allocSourceByBase[doc]; ok && v != "" {
				pr.Source = v
			} else if v, ok := allocSourceByKnock[doc]; ok && v != "" {
				pr.Source = v
			}
		}
		// Apply override source if provided
		if overrideSource != nil {
			if s, ok := overrideSource[doc]; ok && s != "" {
				pr.Source = s
			}
		}
		if pr.Source == "" {
			pr.Source = ingestionSource.String
		}

		// If allocations exist for this document (as base or knock) and the amount is zero,
		// treat it as knocked_off for preview consistency with the in-memory result users expect.
		if pr.Status != "non_qualified" {
			if pr.Amount.IsZero() && (len(allocMap[doc]) > 0 || len(reverseAllocMap[doc]) > 0) {
				pr.Status = "knocked_off"
			}
		}

		// If we still don't have an amount (common for knocked docs with no header),
		// fall back to the original mapped payload from staging_exposures collected earlier.
		if pr.Amount.IsZero() {
			if sm, ok := stagingMeta[doc]; ok {
				if !sm.Amount.IsZero() {
					pr.Amount = sm.Amount
				}
				// also fill missing metadata from staging if available
				if pr.CompanyCode == "" && sm.Company != "" {
					pr.CompanyCode = sm.Company
				}
				if pr.Party == "" && sm.Party != "" {
					pr.Party = sm.Party
				}
				if pr.Currency == "" && sm.Currency != "" {
					pr.Currency = sm.Currency
				}
				if pr.DocumentDate == "" && sm.DocumentDate != "" {
					pr.DocumentDate = sm.DocumentDate
				}
				if pr.PostingDate == "" && sm.PostingDate != "" {
					pr.PostingDate = sm.PostingDate
				}
				if pr.NetDueDate == "" && sm.NetDue != "" {
					pr.NetDueDate = sm.NetDue
				}
			}
		}

		// If still zero and we have incoming allocations (reverseAllocMap), sum their signed amounts
		// to reconstruct the original document amount (common when header is missing but allocations exist).
		if pr.Amount.IsZero() {
			if rin, ok := reverseAllocMap[doc]; ok && len(rin) > 0 {
				signedSum := decimal.Zero
				for _, k := range rin {
					signedSum = signedSum.Add(k.SignedAmt)
				}
				if !signedSum.IsZero() {
					pr.Amount = signedSum
				}
			}
		}

		if pr.Status != "non_qualified" {
			insertedCount++
		}
		previewRows = append(previewRows, pr)
	}

	// Build NonQualified slice for the result from nonQualMap
	nonQualifiedList := make([]NonQualified, 0, len(nonQualMap))
	for doc, nq := range nonQualMap {
		// populate canonical row fields from available sources: headers -> allocations -> staging meta
		nq.Row.DocumentNumber = doc
		if h, ok := headers[doc]; ok {
			nq.Row.CompanyCode = h.Company
			nq.Row.Party = h.Party
			nq.Row.DocumentCurrency = h.Currency
			if h.DocDate.Valid {
				nq.Row.DocumentDate = h.DocDate.Time.Format(constants.DateFormat)
			}
			if h.PostDate.Valid {
				nq.Row.PostingDate = h.PostDate.Time.Format(constants.DateFormat)
			}
			if h.NetDue.Valid {
				nq.Row.NetDueDate = h.NetDue.Time.Format(constants.DateFormat)
			}
		} else {
			// try to infer currency and dates from allocations as best-effort
			if kos, ok := allocMap[doc]; ok && len(kos) > 0 {
				nq.Row.DocumentCurrency = kos[0].Currency
				if nq.Row.DocumentDate == "" && kos[0].DocumentDate != "" {
					nq.Row.DocumentDate = kos[0].DocumentDate
				}
				if nq.Row.PostingDate == "" && kos[0].PostingDate != "" {
					nq.Row.PostingDate = kos[0].PostingDate
				}
				if nq.Row.NetDueDate == "" && kos[0].NetDueDate != "" {
					nq.Row.NetDueDate = kos[0].NetDueDate
				}
			} else if rin, ok := reverseAllocMap[doc]; ok && len(rin) > 0 {
				nq.Row.DocumentCurrency = rin[0].Currency
				if nq.Row.DocumentDate == "" && rin[0].DocumentDate != "" {
					nq.Row.DocumentDate = rin[0].DocumentDate
				}
				if nq.Row.PostingDate == "" && rin[0].PostingDate != "" {
					nq.Row.PostingDate = rin[0].PostingDate
				}
				if nq.Row.NetDueDate == "" && rin[0].NetDueDate != "" {
					nq.Row.NetDueDate = rin[0].NetDueDate
				}
			}
			// if still empty, try stagingMeta
			if sm, ok := stagingMeta[doc]; ok {
				if nq.Row.DocumentCurrency == "" && sm.Currency != "" {
					nq.Row.DocumentCurrency = sm.Currency
				}
				if nq.Row.DocumentDate == "" && sm.DocumentDate != "" {
					nq.Row.DocumentDate = sm.DocumentDate
				}
				if nq.Row.PostingDate == "" && sm.PostingDate != "" {
					nq.Row.PostingDate = sm.PostingDate
				}
				if nq.Row.NetDueDate == "" && sm.NetDue != "" {
					nq.Row.NetDueDate = sm.NetDue
				}
				if nq.Row.Source == "" && sm.Source != "" {
					nq.Row.Source = sm.Source
				}
				if nq.Row.CompanyCode == "" && sm.Company != "" {
					nq.Row.CompanyCode = sm.Company
				}
				if nq.Row.Party == "" && sm.Party != "" {
					nq.Row.Party = sm.Party
				}
				if !sm.Amount.IsZero() {
					nq.Row.AmountDoc = sm.Amount
				}
			}
		}
		nonQualifiedList = append(nonQualifiedList, nq)
	}

	res = UploadResult{
		FileName:      fileName.String,
		Source:        ingestionSource.String,
		BatchID:       batchUUID,
		TotalRows:     len(previewRows),
		InsertedCount: insertedCount,
		LineItemsRows: liCount,
		NonQualified:  nonQualifiedList,
		Rows:          previewRows,
		Errors:        errorsList,
	}
	return res, 200, nil
}

type BatchDetailRequest struct {
	BatchID string `json:"batch_id"`
}

type BatchMeta struct {
	BatchID    uuid.UUID `json:"batch_id"`
	FileName   string    `json:"file_name"`
	FileHash   string    `json:"file_hash"`
	Source     string    `json:"source"`
	Status     string    `json:"status"`
	UploadedBy string    `json:"uploaded_by,omitempty"`
	UploadedAt string    `json:"uploaded_at,omitempty"`
	S3Key      string    `json:"upload_s3_key,omitempty"`
}

type UnallocatedRow struct {
	DocumentNumber   string `json:"document_number"`
	CompanyCode      string `json:"company_code"`
	Party            string `json:"party"`
	Currency         string `json:"currency"`
	Source           string `json:"source"`
	AmountSigned     string `json:"amount_signed"`
	AllocationStatus string `json:"allocation_status"`
	PostingDate      string `json:"posting_date,omitempty"`
	DocumentDate     string `json:"document_date,omitempty"`
	NetDueDate       string `json:"net_due_date,omitempty"`
}

type AllocationRow struct {
	BaseDocumentID         string `json:"base_document_id"`
	KnockoffDocumentID     string `json:"knockoff_document_id"`
	AllocationAmount       string `json:"allocation_amount"`
	AllocationAmountSigned string `json:"allocation_amount_signed"`
	AllocationCurrency     string `json:"allocation_currency"`
	CompanyCode            string `json:"company_code,omitempty"`
	CounterpartyCode       string `json:"counterparty_code,omitempty"`
	Source                 string `json:"source,omitempty"`
	PostingDate            string `json:"posting_date,omitempty"`
	DocumentDate           string `json:"document_date,omitempty"`
	NetDueDate             string `json:"net_due_date,omitempty"`
	CreatedBy              string `json:"created_by,omitempty"`
}

type AdjustmentRow struct {
	ReferenceDocument string          `json:"reference_document_number"`
	AdjustmentType    string          `json:"adjustment_type"`
	AdjustmentAmount  string          `json:"adjustment_amount"`
	Remarks           string          `json:"remarks,omitempty"`
	CreatedBy         string          `json:"created_by,omitempty"`
	AdjustmentJSON    json.RawMessage `json:"adjustment_json,omitempty"`
}

type ExposureAuditRow struct {
	ExposureHeaderID string          `json:"exposure_header_id"`
	DocumentID       string          `json:"document_id,omitempty"`
	ActionType       string          `json:"actiontype"`
	ProcessingStatus string          `json:"processing_status"`
	Reason           string          `json:"reason,omitempty"`
	RequestedBy      string          `json:"requested_by,omitempty"`
	RequestedAt      string          `json:"requested_at,omitempty"`
	NewValues        json.RawMessage `json:"new_values,omitempty"`
}

type BatchDetailResult struct {
	Batch            BatchMeta                `json:"batch"`
	Preview          UploadResult             `json:"preview"`
	HeadersLineItems []map[string]interface{} `json:"headers_line_items"`
	Unallocated      []UnallocatedRow         `json:"unallocated"`
	Allocations      []AllocationRow          `json:"allocations"`
	Adjustments      []AdjustmentRow          `json:"adjustments"`
	Audit            []ExposureAuditRow       `json:"audit"`
}

const batchHeadersLineItemsSQL = `
			SELECT
				h.exposure_header_id::text AS exposure_header_id, h.company_code, h.entity, h.entity1, h.entity2, h.entity3,
				h.exposure_type, h.document_id, h.document_date, h.counterparty_type,
				h.counterparty_code,
				COALESCE(NULLIF(TRIM(h.counterparty_name), ''), NULLIF(TRIM(h.additional_header_details->>'Party Name'), ''), NULLIF(TRIM(h.additional_header_details->>'Name'), '')) AS counterparty_name,
				h.currency,
				h.total_original_amount, h.total_open_amount, h.value_date,
				h.status, h.is_active, h.created_at, h.updated_at,
				h.approval_status, h.approval_comment, h.approved_by,
				h.delete_comment, h.requested_by, h.rejection_comment,
				h.approved_at, h.rejected_by, h.rejected_at,
				h.amount_in_local_currency, h.posting_date, h.net_due_date,
				COALESCE(NULLIF(TRIM(h.text), ''), NULLIF(TRIM(h.additional_header_details->>'Text'), ''), NULLIF(TRIM(h.additional_header_details->>'text'), ''), NULLIF(TRIM(h.additional_header_details->>'Item Text'), '')) AS text,
				COALESCE(NULLIF(TRIM(h.gl_account), ''), NULLIF(TRIM(h.additional_header_details->>'GLAccount'), ''), NULLIF(TRIM(h.additional_header_details->>'gl_account'), ''), NULLIF(TRIM(h.additional_header_details->>'G/L Account'), '')) AS gl_account,
				COALESCE(NULLIF(TRIM(h.reference), ''), NULLIF(TRIM(h.additional_header_details->>'Reference'), ''), NULLIF(TRIM(h.additional_header_details->>'Assignment'), '')) AS reference,
				h.additional_header_details,
				h.exposure_category, h.exposure_creation_status, h.batch_id::text AS batch_id,
				COALESCE(NULLIF(TRIM(h.upload_s3_key), ''), NULLIF(TRIM((
					SELECT sb.upload_s3_key
					  FROM public.staging_batches_exposures sb
					 WHERE sb.batch_id = h.batch_id
					 ORDER BY sb.ingestion_timestamp DESC
					 LIMIT 1
				)), '')) AS upload_s3_key,
				h.file_hash,
				l.line_item_id::text AS line_item_id, l.line_number, l.product_id, l.product_description,
				l.quantity, l.unit_of_measure, l.unit_price, l.line_item_amount,
				l.plant_code, l.delivery_date, l.payment_terms, l.inco_terms,
				l.additional_line_details, l.created_at AS line_created_at
			FROM public.exposure_headers h
			LEFT JOIN public.exposure_line_items l ON l.exposure_header_id = h.exposure_header_id
			WHERE h.batch_id = $1
			  AND h.is_deleted IS NOT TRUE
			ORDER BY h.document_id, l.line_number NULLS FIRST
`

func decodeJSONLikeCell(col string, raw []byte) interface{} {
	if len(raw) == 0 {
		if col == "additional_header_details" || col == "additional_line_details" {
			return map[string]interface{}{}
		}
		return ""
	}
	var obj interface{}
	if err := json.Unmarshal(raw, &obj); err == nil {
		return obj
	}
	return strings.TrimSpace(string(raw))
}

func normalizePGXCell(col string, val interface{}) interface{} {
	if val == nil {
		if col == "additional_header_details" || col == "additional_line_details" {
			return map[string]interface{}{}
		}
		return ""
	}
	switch v := val.(type) {
	case time.Time:
		return v.Format(time.RFC3339)
	case []byte:
		if col == "additional_header_details" || col == "additional_line_details" {
			return decodeJSONLikeCell(col, v)
		}
		if s := strings.TrimSpace(string(v)); s != "" {
			if f, err := strconv.ParseFloat(s, 64); err == nil {
				return f
			}
		}
		return string(v)
	case map[string]interface{}:
		return v
	case []interface{}:
		if len(v) > 0 {
			allNums := true
			for _, item := range v {
				switch item.(type) {
				case float64, int, int32, int64, uint8:
				default:
					allNums = false
					break
				}
			}
			if allNums {
				buf := make([]byte, len(v))
				for i, item := range v {
					switch n := item.(type) {
					case float64:
						buf[i] = byte(n)
					case int:
						buf[i] = byte(n)
					case int32:
						buf[i] = byte(n)
					case int64:
						buf[i] = byte(n)
					case uint8:
						buf[i] = n
					}
				}
				if col == "additional_header_details" || col == "additional_line_details" {
					return decodeJSONLikeCell(col, buf)
				}
				return string(buf)
			}
		}
		return v
	default:
		return v
	}
}

func loadBatchHeadersLineItems(ctx context.Context, pool *pgxpool.Pool, batchUUID uuid.UUID) ([]map[string]interface{}, error) {
	rows, err := pool.Query(ctx, batchHeadersLineItemsSQL, batchUUID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]map[string]interface{}, 0)
	flds := rows.FieldDescriptions()
	for rows.Next() {
		vals, scanErr := rows.Values()
		if scanErr != nil {
			continue
		}
		rowMap := map[string]interface{}{}
		for i, fd := range flds {
			col := string(fd.Name)
			rowMap[col] = normalizePGXCell(col, vals[i])
		}
		out = append(out, rowMap)
	}
	return out, rows.Err()
}

func GetBatchDetailV91(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req BatchDetailRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.BatchID) == "" {
			respondEnvelopeError(w, http.StatusBadRequest, constants.ErrBatchIDRequired, v91ErrorCode(http.StatusBadRequest))
			return
		}
		batchUUID, err := uuid.Parse(strings.TrimSpace(req.BatchID))
		if err != nil {
			respondEnvelopeError(w, http.StatusBadRequest, constants.ErrInvalidBatchID, v91ErrorCode(http.StatusBadRequest))
			return
		}

		var meta BatchMeta
		meta.BatchID = batchUUID
		var uploadedAt sql.NullTime
		var uploadedBy sql.NullString
		err = pool.QueryRow(ctx, `
			SELECT COALESCE(file_name,''), COALESCE(file_hash,''), COALESCE(ingestion_source,''),
			       COALESCE(status,''), COALESCE(uploaded_by,''), ingestion_timestamp,
			       COALESCE(upload_s3_key,'')
			  FROM public.staging_batches_exposures
			 WHERE batch_id=$1
			 LIMIT 1
		`, batchUUID).Scan(&meta.FileName, &meta.FileHash, &meta.Source, &meta.Status,
			&uploadedBy, &uploadedAt, &meta.S3Key)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				respondEnvelopeError(w, http.StatusNotFound, constants.ErrBatchNotFound, v91ErrorCode(http.StatusNotFound))
				return
			}
			respondEnvelopeError(w, http.StatusInternalServerError, "batch lookup: "+err.Error(), v91ErrorCode(http.StatusInternalServerError))
			return
		}
		if uploadedBy.Valid {
			meta.UploadedBy = uploadedBy.String
		}
		if uploadedAt.Valid {
			meta.UploadedAt = uploadedAt.Time.Format(time.RFC3339)
		}

		preview, _, err := buildPreviewForBatch(pool, ctx, batchUUID, nil)
		if err != nil {
			respondEnvelopeError(w, http.StatusInternalServerError, constants.ErrPreviewBuild+err.Error(), v91ErrorCode(http.StatusInternalServerError))
			return
		}

		unallocated := make([]UnallocatedRow, 0)
		urows, uerr := pool.Query(ctx, `
			SELECT document_number, COALESCE(company_code,''), COALESCE(party,''),
			       COALESCE(currency,''), COALESCE(source,''),
			       COALESCE(amount_signed::text,'0'), COALESCE(allocation_status,''),
			       posting_date, document_date, net_due_date
			  FROM public.exposure_unallocated
			 WHERE batch_id=$1
			 ORDER BY posting_date NULLS LAST, document_number
		`, batchUUID)
		if uerr == nil {
			for urows.Next() {
				var row UnallocatedRow
				var postDate, docDate, netDue sql.NullTime
				if scanErr := urows.Scan(&row.DocumentNumber, &row.CompanyCode, &row.Party,
					&row.Currency, &row.Source, &row.AmountSigned, &row.AllocationStatus,
					&postDate, &docDate, &netDue); scanErr == nil {
					if postDate.Valid {
						row.PostingDate = postDate.Time.Format(constants.DateFormat)
					}
					if docDate.Valid {
						row.DocumentDate = docDate.Time.Format(constants.DateFormat)
					}
					if netDue.Valid {
						row.NetDueDate = netDue.Time.Format(constants.DateFormat)
					}
					unallocated = append(unallocated, row)
				}
			}
			urows.Close()
		}

		allocations := make([]AllocationRow, 0)
		arows, aerr := pool.Query(ctx, `
			SELECT base_document_id, knockoff_document_id,
			       COALESCE(allocation_amount::text,'0'),
			       COALESCE(allocation_amount_signed::text,'0'),
			       COALESCE(allocation_currency,''),
			       COALESCE(company_code,''), COALESCE(counterparty_code,''),
			       COALESCE(source,''), posting_date, document_date, net_due_date,
			       COALESCE(created_by,'')
			  FROM public.exposure_allocations
			 WHERE batch_id=$1
			 ORDER BY posting_date NULLS LAST, base_document_id, knockoff_document_id
		`, batchUUID)
		if aerr == nil {
			for arows.Next() {
				var row AllocationRow
				var postDate, docDate, netDue sql.NullTime
				if scanErr := arows.Scan(&row.BaseDocumentID, &row.KnockoffDocumentID,
					&row.AllocationAmount, &row.AllocationAmountSigned, &row.AllocationCurrency,
					&row.CompanyCode, &row.CounterpartyCode, &row.Source,
					&postDate, &docDate, &netDue, &row.CreatedBy); scanErr == nil {
					if postDate.Valid {
						row.PostingDate = postDate.Time.Format(constants.DateFormat)
					}
					if docDate.Valid {
						row.DocumentDate = docDate.Time.Format(constants.DateFormat)
					}
					if netDue.Valid {
						row.NetDueDate = netDue.Time.Format(constants.DateFormat)
					}
					allocations = append(allocations, row)
				}
			}
			arows.Close()
		}

		adjustments := make([]AdjustmentRow, 0)
		jrows, jerr := pool.Query(ctx, `
			SELECT COALESCE(reference_document_number,''), COALESCE(adjustment_type,''),
			       COALESCE(adjustment_amount::text,'0'), COALESCE(remarks,''),
			       COALESCE(created_by,''), adjustment_json
			  FROM public.exposure_adjustments
			 WHERE batch_id=$1
			 ORDER BY created_at DESC
		`, batchUUID)
		if jerr == nil {
			for jrows.Next() {
				var row AdjustmentRow
				var adjJSON []byte
				if scanErr := jrows.Scan(&row.ReferenceDocument, &row.AdjustmentType,
					&row.AdjustmentAmount, &row.Remarks, &row.CreatedBy, &adjJSON); scanErr == nil {
					if len(adjJSON) > 0 {
						row.AdjustmentJSON = json.RawMessage(adjJSON)
					}
					adjustments = append(adjustments, row)
				}
			}
			jrows.Close()
		}

		auditRows := make([]ExposureAuditRow, 0)
		batchIDStr := batchUUID.String()
		audRows, audErr := pool.Query(ctx, `
			SELECT a.exposure_header_id::text, COALESCE(h.document_id,''),
			       COALESCE(a.actiontype,''), COALESCE(a.processing_status,''),
			       COALESCE(a.reason,''), COALESCE(a.requested_by,''), a.requested_at,
			       a.new_values
			  FROM public.auditactionexposure a
			  LEFT JOIN public.exposure_headers h
			    ON h.exposure_header_id::text = a.exposure_header_id::text
			 WHERE h.batch_id::text = $1
			    OR a.exposure_header_id::text = $1
			 ORDER BY a.requested_at DESC NULLS LAST
		`, batchIDStr)
		if audErr == nil {
			for audRows.Next() {
				var row ExposureAuditRow
				var reqAt sql.NullTime
				var newVals []byte
				if scanErr := audRows.Scan(&row.ExposureHeaderID, &row.DocumentID,
					&row.ActionType, &row.ProcessingStatus, &row.Reason,
					&row.RequestedBy, &reqAt, &newVals); scanErr == nil {
					if reqAt.Valid {
						row.RequestedAt = reqAt.Time.Format(time.RFC3339)
					}
					if len(newVals) > 0 {
						row.NewValues = json.RawMessage(newVals)
					}
					auditRows = append(auditRows, row)
				}
			}
			audRows.Close()
		}

		headersLineItems, hlErr := loadBatchHeadersLineItems(ctx, pool, batchUUID)
		if hlErr != nil {
			logger.LogError("[v91 batch-detail] headers_line_items: %v", hlErr)
			headersLineItems = []map[string]interface{}{}
		}

		respondEnvelopeSuccess(w, "Batch detail fetched successfully", BatchDetailResult{
			Batch:            meta,
			Preview:          preview,
			HeadersLineItems: headersLineItems,
			Unallocated:      unallocated,
			Allocations:      allocations,
			Adjustments:      adjustments,
			Audit:            auditRows,
		})
	}
}

func GetBatchExposureAuditV91(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req BatchDetailRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.BatchID) == "" {
			respondEnvelopeError(w, http.StatusBadRequest, constants.ErrBatchIDRequired, v91ErrorCode(http.StatusBadRequest))
			return
		}
		batchUUID, err := uuid.Parse(strings.TrimSpace(req.BatchID))
		if err != nil {
			respondEnvelopeError(w, http.StatusBadRequest, constants.ErrInvalidBatchID, v91ErrorCode(http.StatusBadRequest))
			return
		}
		batchIDStr := batchUUID.String()

		rows, err := pool.Query(ctx, `
			SELECT a.action_id AS audit_row_id,
			       a.exposure_header_id::text AS exposure_header_id,
			       COALESCE(
			         NULLIF(h.document_id, ''),
			         CASE WHEN a.exposure_header_id::text = $1 THEN 'Upload' ELSE '' END
			       ) AS document_id,
			       COALESCE(a.actiontype, '') AS actiontype,
			       COALESCE(a.processing_status, '') AS processing_status,
			       COALESCE(a.requested_by, '') AS requested_by,
			       a.requested_at,
			       COALESCE(a.requested_ip, '') AS requested_ip,
			       COALESCE(a.checker_by, '') AS checker_by,
			       a.checker_at,
			       COALESCE(a.checker_ip, '') AS checker_ip,
			       COALESCE(a.checker_comment, '') AS checker_comment,
			       COALESCE(a.reason, '') AS reason,
			       a.old_values,
			       a.new_values,
			       a.change_summary
			  FROM public.auditactionexposure a
			  LEFT JOIN public.exposure_headers h
			    ON h.exposure_header_id::text = a.exposure_header_id::text
			 WHERE h.batch_id::text = $1
			    OR a.exposure_header_id::text = $1
			 ORDER BY a.requested_at DESC NULLS LAST, a.action_id DESC
		`, batchIDStr)
		if err != nil {
			respondEnvelopeError(w, http.StatusInternalServerError, "failed to fetch batch audit: "+err.Error(), v91ErrorCode(http.StatusInternalServerError))
			return
		}
		defer rows.Close()

		logs := make([]map[string]interface{}, 0)
		for rows.Next() {
			var (
				auditRowID, exposureHeaderID, documentID, actionType, processingStatus string
				requestedBy, requestedIP, checkerBy, checkerIP, checkerComment, reason string
				requestedAt, checkerAt                                                 sql.NullTime
				oldVals, newVals, changeSummary                                        []byte
			)
			if scanErr := rows.Scan(&auditRowID, &exposureHeaderID, &documentID, &actionType, &processingStatus,
				&requestedBy, &requestedAt, &requestedIP, &checkerBy, &checkerAt, &checkerIP,
				&checkerComment, &reason, &oldVals, &newVals, &changeSummary); scanErr != nil {
				continue
			}
			entry := map[string]interface{}{
				"audit_row_id":       auditRowID,
				"exposure_header_id": exposureHeaderID,
				"document_id":        documentID,
				"actiontype":         actionType,
				"processing_status":  processingStatus,
				"requested_by":       requestedBy,
				"requested_ip":       requestedIP,
				"checker_by":         checkerBy,
				"checker_ip":         checkerIP,
				"checker_comment":    checkerComment,
				"reason":             reason,
			}
			if requestedAt.Valid {
				entry["requested_at"] = requestedAt.Time.Format(time.RFC3339)
			}
			if checkerAt.Valid {
				entry["checker_at"] = checkerAt.Time.Format(time.RFC3339)
			}
			if len(oldVals) > 0 {
				entry["old_values"] = json.RawMessage(oldVals)
			}
			if len(newVals) > 0 {
				entry["new_values"] = json.RawMessage(newVals)
			}
			if len(changeSummary) > 0 {
				entry["change_summary"] = json.RawMessage(changeSummary)
			}
			logs = append(logs, entry)
		}
		if err := rows.Err(); err != nil {
			respondEnvelopeError(w, http.StatusInternalServerError, "batch audit scan: "+err.Error(), v91ErrorCode(http.StatusInternalServerError))
			return
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		respondEnvelopeSuccess(w, "Upload audit fetched successfully", map[string]interface{}{
			"audit_logs": logs,
		})
	}
}
