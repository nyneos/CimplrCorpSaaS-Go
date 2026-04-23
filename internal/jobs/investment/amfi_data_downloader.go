package jobs

import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	"math"
	"net/http"
	"strings"
	"sync"
	"time"

	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/internal/config"
	"CimplrCorpSaas/internal/logger"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/robfig/cron/v3"
)

type Config struct {
	DefaultSchemeURL      string
	DefaultNavURL         string
	DefaultSchemeSchedule string
	DefaultTimeZone       string
	BatchSize             int
	MaxRetries            int
	RetryDelay            time.Duration
}

// CircuitBreakerState represents the state of circuit breaker
type CircuitBreakerState int32

const (
	StateClosed CircuitBreakerState = iota
	StateOpen
	StateHalfOpen
)

// CircuitBreaker implements circuit breaker pattern
type CircuitBreaker struct {
	maxFailures  int32
	resetTimeout time.Duration
	failures     int32
	lastFailTime time.Time
	state        CircuitBreakerState
	mutex        sync.RWMutex
}

// NewCircuitBreaker creates a new circuit breaker
func NewCircuitBreaker(maxFailures int32, resetTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		maxFailures:  maxFailures,
		resetTimeout: resetTimeout,
		state:        StateClosed,
	}
}

// Execute runs the function with circuit breaker protection
func (cb *CircuitBreaker) Execute(fn func() error) error {
	cb.mutex.RLock()
	state := cb.state
	cb.mutex.RUnlock()

	if state == StateOpen {
		if time.Since(cb.lastFailTime) > cb.resetTimeout {
			cb.mutex.Lock()
			cb.state = StateHalfOpen
			cb.mutex.Unlock()
		} else {
			return fmt.Errorf("circuit breaker is open")
		}
	}

	err := fn()

	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	if err != nil {
		cb.failures++
		cb.lastFailTime = time.Now()
		if cb.failures >= cb.maxFailures {
			cb.state = StateOpen
		}
		return err
	}

	// Success - reset circuit breaker
	cb.failures = 0
	cb.state = StateClosed
	return nil
}

// RetryWithBackoff executes a function with exponential backoff retry logic
func RetryWithBackoff(maxRetries int, initialDelay time.Duration, fn func() error) error {
	var lastErr error

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			// Exponential backoff with jitter
			delay := time.Duration(math.Pow(2, float64(attempt-1))) * initialDelay
			logger.GlobalLogger.LogAudit(fmt.Sprintf("Retrying after %v (attempt %d/%d)", delay, attempt, maxRetries))
			time.Sleep(delay)
		}

		lastErr = fn()
		if lastErr == nil {
			return nil
		}

		logger.GlobalLogger.LogAudit(fmt.Sprintf("Attempt %d failed: %v", attempt+1, lastErr))
	}

	return fmt.Errorf("failed after %d attempts: %v", maxRetries+1, lastErr)
}

// NewDefaultConfig creates a new Config with default values from config package
func NewDefaultConfig() *Config {
	return &Config{
		DefaultSchemeURL:      config.DefaultSchemeURL,
		DefaultNavURL:         config.DefaultNavURL,
		DefaultSchemeSchedule: config.DefaultSchemeSchedule,
		DefaultTimeZone:       config.DefaultTimeZone,
		BatchSize:             1000, // Reduced batch size for better performance
		MaxRetries:            3,
		RetryDelay:            2 * time.Second,
	}
}

func RunAMFIDataDownloader(cfg *Config, db *pgxpool.Pool) error {
	// Set default values if not provided
	if cfg.DefaultSchemeURL == "" {
		cfg.DefaultSchemeURL = config.DefaultSchemeURL
	}
	if cfg.DefaultNavURL == "" {
		cfg.DefaultNavURL = config.DefaultNavURL
	}
	if cfg.DefaultSchemeSchedule == "" {
		cfg.DefaultSchemeSchedule = config.DefaultSchemeSchedule
	}
	if cfg.DefaultTimeZone == "" {
		cfg.DefaultTimeZone = config.DefaultTimeZone
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 1000
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = 3
	}
	if cfg.RetryDelay == 0 {
		cfg.RetryDelay = 2 * time.Second
	}

	loc, err := time.LoadLocation(cfg.DefaultTimeZone)
	if err != nil {
		return fmt.Errorf("invalid timezone for AMFI Data Downloader: %v", err)
	}

	// Create circuit breakers for HTTP and DB operations
	httpCircuitBreaker := NewCircuitBreaker(5, 30*time.Second)
	dbCircuitBreaker := NewCircuitBreaker(3, 60*time.Second)

	c := cron.New(cron.WithLocation(loc))
	_, err = c.AddFunc(cfg.DefaultSchemeSchedule, func() {
		successMsg := fmt.Sprintf("Running AMFI Data Downloader at %s", time.Now().In(loc))
		logger.GlobalLogger.LogAudit(successMsg)

		// Use goroutines and waitgroup for concurrent processing
		var wg sync.WaitGroup
		var schemeErr, navErr error

		// Process scheme data concurrently
		wg.Add(1)
		go func() {
			defer wg.Done()
			schemeErr = RetryWithBackoff(cfg.MaxRetries, cfg.RetryDelay, func() error {
				return processSchemeDataWithCircuitBreaker(cfg.DefaultSchemeURL, db, cfg.BatchSize, httpCircuitBreaker, dbCircuitBreaker)
			})

			if schemeErr != nil {
				logger.GlobalLogger.LogAudit(fmt.Sprintf("AMFI Scheme Sync failed: %v", schemeErr))
			} else {
				logger.GlobalLogger.LogAudit("AMFI Scheme data sync completed at " + time.Now().In(loc).String())
			}
		}()

		// Process NAV data concurrently
		wg.Add(1)
		go func() {
			defer wg.Done()
			navErr = RetryWithBackoff(cfg.MaxRetries, cfg.RetryDelay, func() error {
				return processNAVDataWithCircuitBreaker(cfg.DefaultNavURL, db, cfg.BatchSize, httpCircuitBreaker, dbCircuitBreaker)
			})

			if navErr != nil {
				logger.GlobalLogger.LogAudit(fmt.Sprintf("AMFI NAV details sync failed: %v", navErr))
			} else {
				logger.GlobalLogger.LogAudit("AMFI NAV data sync completed at " + time.Now().In(loc).String())
			}
		}()

		// Wait for both operations to complete
		wg.Wait()

		// Log final status
		if schemeErr != nil || navErr != nil {
			logger.GlobalLogger.LogAudit("AMFI Data sync completed with errors")
		} else {
			logger.GlobalLogger.LogAudit("AMFI Data sync completed successfully at " + time.Now().In(loc).String())
		}
	})

	if err != nil {
		return fmt.Errorf("failed to schedule AMFI cron job: %v", err)
	}

	c.Start()
	logger.GlobalLogger.LogAudit("AMFI Data Download and Sync Job scheduled for " + cfg.DefaultSchemeSchedule + " (" + cfg.DefaultTimeZone + ")")
	return nil
}

func processSchemeDataWithCircuitBreaker(url string, db *pgxpool.Pool, batchSize int, httpCB, dbCB *CircuitBreaker) error {
	logger.GlobalLogger.LogAudit("Downloading AMFI Scheme Data from: " + url + " ...")

	var records [][]string

	// HTTP request with circuit breaker protection
	err := httpCB.Execute(func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return fmt.Errorf("error creating request: %v", err)
		}

		client := &http.Client{Timeout: 30 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("error fetching AMFI scheme data: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("HTTP error: %d", resp.StatusCode)
		}

		reader := csv.NewReader(resp.Body)
		reader.FieldsPerRecord = -1
		reader.LazyQuotes = true // Handle malformed quotes more gracefully
		records, err = reader.ReadAll()
		if err != nil {
			return fmt.Errorf("error parsing AMFI scheme CSV: %v", err)
		}
		return nil
	})

	if err != nil {
		return err
	}

	// Process records in batches with circuit breaker protection
	logger.GlobalLogger.LogAudit(fmt.Sprintf("Downloaded %d scheme records, processing in batches...", len(records)))
	return dbCB.Execute(func() error {
		return processSchemeBatches(records, db, batchSize)
	})
}

func processSchemeBatches(records [][]string, db *pgxpool.Pool, batchSize int) error {
	totalRecords := len(records)
	logger.GlobalLogger.LogAudit(fmt.Sprintf("Starting bulk scheme processing for %d records", totalRecords))

	// Create context with longer timeout for bulk operations
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	// Drop temp table if it exists, then create new one
	_, _ = db.Exec(ctx, "DROP TABLE IF EXISTS temp_scheme_staging")

	_, err := db.Exec(ctx, `
		CREATE TEMP TABLE temp_scheme_staging (
			amc_name TEXT,
			scheme_code TEXT,
			scheme_name TEXT,
			scheme_type TEXT,
			scheme_category TEXT,
			scheme_sub_category TEXT,
			scheme_nav_name TEXT,
			scheme_minimum_amount TEXT,
			launch_date DATE,
			closure_date DATE,
			isin_div_payout_growth TEXT,
			isin_div_reinvestment TEXT,
			file_date DATE DEFAULT CURRENT_DATE
		)`)
	if err != nil {
		return fmt.Errorf("error creating temp scheme table: %v", err)
	}

	// Prepare data for bulk copy
	var validRecords [][]interface{}
	for _, rec := range records {
		if len(rec) < 9 {
			continue
		}

		// Skip header row and invalid data
		firstField := strings.ToLower(strings.TrimSpace(rec[0]))
		secondField := strings.ToLower(strings.TrimSpace(rec[1]))

		// Check multiple header patterns
		if firstField == "amc name" || firstField == "amc_name" || firstField == "unique no" ||
			secondField == "code" || secondField == "scheme code" ||
			strings.Contains(firstField, "unique") || strings.Contains(secondField, "isin") {
			continue
		}

		amcName := strings.TrimSpace(rec[0])
		code := strings.TrimSpace(rec[1])

		// Skip if code is empty or non-numeric
		if code == "" || !isNumeric(code) {
			continue
		}

		schemeName := rec[2]
		schemeType := rec[3]

		// Parse scheme category and sub-category from field like "Equity Scheme - Large & Mid Cap Fund"
		fullCategory := rec[4]
		schemeCategory := fullCategory
		schemeSubCategory := ""
		if strings.Contains(fullCategory, " - ") {
			parts := strings.SplitN(fullCategory, " - ", 2)
			schemeCategory = strings.TrimSpace(parts[0])
			schemeSubCategory = strings.TrimSpace(parts[1])
		}

		schemeNavName := rec[5]
		minAmount := parseMinAmount(rec[6])
		launchDate := parseDate(rec[7])
		closureDate := parseDate(rec[8])

		isinField := ""
		isinReinvest := ""
		if len(rec) > 9 {
			full := strings.TrimSpace(rec[9])
			if len(full) > 12 {
				if len(full) >= 24 {
					isinField = full[:12]
					isinReinvest = full[12:]
				} else {
					isinField = full
				}
			}
		}

		validRecords = append(validRecords, []interface{}{
			amcName, code, schemeName, schemeType, schemeCategory, schemeSubCategory,
			schemeNavName, minAmount, launchDate, closureDate,
			isinField, isinReinvest,
		})
	}

	logger.GlobalLogger.LogAudit(fmt.Sprintf("Filtered to %d valid scheme records for bulk insert", len(validRecords)))

	// Bulk copy to temp table
	start := time.Now()
	_, err = db.CopyFrom(ctx, pgx.Identifier{"temp_scheme_staging"},
		[]string{"amc_name", "scheme_code", "scheme_name", "scheme_type", "scheme_category", "scheme_sub_category",
			"scheme_nav_name", "scheme_minimum_amount", "launch_date", "closure_date",
			"isin_div_payout_growth", "isin_div_reinvestment"},
		pgx.CopyFromRows(validRecords))

	if err != nil {
		return fmt.Errorf("error bulk copying scheme data: %v", err)
	}

	copyDuration := time.Since(start)
	logger.GlobalLogger.LogAudit(fmt.Sprintf("Bulk copy completed in %v", copyDuration))

	// Now perform upsert from temp table to main table
	start = time.Now()
	result, err := db.Exec(ctx, `
		INSERT INTO investment.amfi_scheme_master_staging
		(amc_name, scheme_code, scheme_name, scheme_type, scheme_category, scheme_sub_category,
		scheme_nav_name, scheme_minimum_amount, launch_date, closure_date,
		isin_div_payout_growth, isin_div_reinvestment, file_date)
		SELECT amc_name, 
			   scheme_code::bigint, 
			   scheme_name, 
			   scheme_type, 
			   scheme_category,
			   scheme_sub_category,
			   scheme_nav_name, 
			   CASE 
				   WHEN scheme_minimum_amount IS NULL OR scheme_minimum_amount = '' THEN NULL
				   ELSE scheme_minimum_amount::numeric(18,2)
			   END,
			   launch_date, 
			   closure_date,
			   isin_div_payout_growth, 
			   isin_div_reinvestment, 
			   file_date
		FROM temp_scheme_staging
		WHERE scheme_code ~ '^[0-9]+$'
		ON CONFLICT (scheme_code) DO UPDATE SET
			amc_name = EXCLUDED.amc_name,
			scheme_name = EXCLUDED.scheme_name,
			scheme_type = EXCLUDED.scheme_type,
			scheme_category = EXCLUDED.scheme_category,
			scheme_sub_category = EXCLUDED.scheme_sub_category,
			scheme_nav_name = EXCLUDED.scheme_nav_name,
			scheme_minimum_amount = EXCLUDED.scheme_minimum_amount,
			launch_date = EXCLUDED.launch_date,
			closure_date = EXCLUDED.closure_date,
			isin_div_payout_growth = EXCLUDED.isin_div_payout_growth,
			isin_div_reinvestment = EXCLUDED.isin_div_reinvestment,
			file_date = EXCLUDED.file_date`)

	if err != nil {
		return fmt.Errorf("error upserting scheme data: %v", err)
	}

	upsertDuration := time.Since(start)
	rowsAffected := result.RowsAffected()

	logger.GlobalLogger.LogAudit(fmt.Sprintf("Bulk upsert completed in %v, %d rows affected", upsertDuration, rowsAffected))
	logger.GlobalLogger.LogAudit(fmt.Sprintf("Total scheme processing time: %v", copyDuration+upsertDuration))

	// Drop temp table
	_, err = db.Exec(ctx, "DROP TABLE IF EXISTS temp_scheme_staging")
	if err != nil {
		logger.GlobalLogger.LogAudit(fmt.Sprintf("Warning: Failed to drop temp scheme table: %v", err))
	}

	return nil
}

func processNAVDataWithCircuitBreaker(url string, db *pgxpool.Pool, batchSize int, httpCB, dbCB *CircuitBreaker) error {
	logger.GlobalLogger.LogAudit("Downloading AMFI NAV Data from: " + url + " ...")

	var navRecords []NAVRecord

	// HTTP request with circuit breaker protection
	err := httpCB.Execute(func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return fmt.Errorf("error creating request: %v", err)
		}

		client := &http.Client{Timeout: 30 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("error fetching AMFI NAV data: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("HTTP error: %d", resp.StatusCode)
		}

		scanner := bufio.NewScanner(resp.Body)
		currentAMC := ""

		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}
			if !strings.Contains(line, ";") {
				currentAMC = line
				continue
			}

			fields := strings.Split(line, ";")
			if len(fields) < 6 {
				continue
			}

			// Skip header rows and validate scheme code
			schemeCode := strings.TrimSpace(fields[0])
			if strings.ToLower(schemeCode) == "scheme code" || schemeCode == "Scheme Code" ||
				!isNumeric(schemeCode) || schemeCode == "" {
				continue
			}

			navRecords = append(navRecords, NAVRecord{
				SchemeCode:   schemeCode,
				ISINGrowth:   fields[1],
				ISINReinvest: fields[2],
				SchemeName:   fields[3],
				NAVValue:     fields[4],
				NAVDate:      parseDate(fields[5]),
				AMCName:      currentAMC,
			})
		}

		if err := scanner.Err(); err != nil {
			return fmt.Errorf("error reading NAV data: %v", err)
		}

		return nil
	})

	if err != nil {
		return err
	}

	// Process records in batches with circuit breaker protection
	logger.GlobalLogger.LogAudit(fmt.Sprintf("Downloaded %d NAV records, processing in batches...", len(navRecords)))
	return dbCB.Execute(func() error {
		return processNAVBatches(navRecords, db, batchSize)
	})
}

type NAVRecord struct {
	SchemeCode   string
	ISINGrowth   string
	ISINReinvest string
	SchemeName   string
	NAVValue     string
	NAVDate      *string
	AMCName      string
}

func processNAVBatches(navRecords []NAVRecord, db *pgxpool.Pool, batchSize int) error {
	totalRecords := len(navRecords)
	logger.GlobalLogger.LogAudit(fmt.Sprintf("Starting bulk NAV processing for %d records", totalRecords))

	// First, create a temporary table for bulk insert
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	// Drop temp table if it exists, then create new one
	_, _ = db.Exec(ctx, "DROP TABLE IF EXISTS temp_nav_staging")

	_, err := db.Exec(ctx, `
		CREATE TEMP TABLE temp_nav_staging (
			scheme_code TEXT,
			isin_div_payout_growth TEXT,
			isin_div_reinvestment TEXT,
			scheme_name TEXT,
			nav_value TEXT,
			nav_date DATE,
			amc_name TEXT,
			file_date DATE DEFAULT CURRENT_DATE
		)`)
	if err != nil {
		return fmt.Errorf("error creating temp table: %v", err)
	}

	// Prepare data for bulk copy
	var validRecords [][]interface{}
	for _, record := range navRecords {
		// Skip if scheme code is empty or invalid
		if record.SchemeCode == "" || strings.TrimSpace(record.SchemeCode) == "" {
			continue
		}

		validRecords = append(validRecords, []interface{}{
			record.SchemeCode,
			record.ISINGrowth,
			record.ISINReinvest,
			record.SchemeName,
			record.NAVValue,
			record.NAVDate,
			record.AMCName,
		})
	}

	logger.GlobalLogger.LogAudit(fmt.Sprintf("Filtered to %d valid NAV records for bulk insert", len(validRecords)))

	// Bulk copy to temp table
	start := time.Now()
	_, err = db.CopyFrom(ctx, pgx.Identifier{"temp_nav_staging"},
		[]string{"scheme_code", "isin_div_payout_growth", "isin_div_reinvestment", "scheme_name", "nav_value", "nav_date", "amc_name"},
		pgx.CopyFromRows(validRecords))

	if err != nil {
		return fmt.Errorf("error bulk copying NAV data: %v", err)
	}

	copyDuration := time.Since(start)
	logger.GlobalLogger.LogAudit(fmt.Sprintf("Bulk copy completed in %v", copyDuration))

	// STEP 1: Check for new schemes and insert them into amfi_scheme_master_staging
	logger.GlobalLogger.LogAudit("Checking for new schemes not in master database...")

	var newSchemesCount int
	err = db.QueryRow(ctx, `
		SELECT COUNT(DISTINCT t.scheme_code)
		FROM temp_nav_staging t
		LEFT JOIN investment.amfi_scheme_master_staging s ON t.scheme_code::bigint = s.scheme_code
		WHERE t.scheme_code ~ '^[0-9]+$' AND s.scheme_code IS NULL`).Scan(&newSchemesCount)
	if err != nil {
		newSchemesCount = 0
	}

	if newSchemesCount > 0 {
		logger.GlobalLogger.LogAudit(fmt.Sprintf("Found %d new schemes, adding to scheme master...", newSchemesCount))

		// Insert new schemes into amfi_scheme_master_staging
		schemeResult, err := db.Exec(ctx, `
			INSERT INTO investment.amfi_scheme_master_staging
			(amc_name, scheme_code, scheme_name, isin_div_payout_growth, isin_div_reinvestment, file_date)
			SELECT DISTINCT
				   t.amc_name,
				   t.scheme_code::bigint,
				   t.scheme_name,
				   t.isin_div_payout_growth,
				   t.isin_div_reinvestment,
				   t.file_date
			FROM temp_nav_staging t
			LEFT JOIN investment.amfi_scheme_master_staging s ON t.scheme_code::bigint = s.scheme_code
			WHERE t.scheme_code ~ '^[0-9]+$' AND s.scheme_code IS NULL
			ON CONFLICT (scheme_code) DO UPDATE SET
				amc_name = EXCLUDED.amc_name,
				scheme_name = EXCLUDED.scheme_name,
				isin_div_payout_growth = EXCLUDED.isin_div_payout_growth,
				isin_div_reinvestment = EXCLUDED.isin_div_reinvestment,
				file_date = EXCLUDED.file_date`)

		if err != nil {
			logger.GlobalLogger.LogAudit(fmt.Sprintf("Warning: Failed to insert new schemes: %v", err))
		} else {
			addedSchemes := schemeResult.RowsAffected()
			logger.GlobalLogger.LogAudit(fmt.Sprintf("Added %d new schemes to scheme master database", addedSchemes))
		}
	}

	// Count distinct new AMCs for reporting
	var newAmcCount int
	err = db.QueryRow(ctx, `
		SELECT COUNT(DISTINCT t.amc_name)
		FROM temp_nav_staging t
		LEFT JOIN investment.amfi_scheme_master_staging s ON t.amc_name = s.amc_name
		WHERE s.amc_name IS NULL AND t.amc_name IS NOT NULL AND t.amc_name != ''`).Scan(&newAmcCount)
	if err != nil {
		newAmcCount = 0
	}

	if newAmcCount > 0 {
		logger.GlobalLogger.LogAudit(fmt.Sprintf("Found %d new AMC(s)", newAmcCount))
	}

	// STEP 2: Now perform upsert for NAV data for all schemes (existing + newly added)
	start = time.Now()
	logger.GlobalLogger.LogAudit("Processing NAV data for all matched schemes...")

	// Insert NAV records for all schemes that now exist in the master (including newly added ones)
	result, err := db.Exec(ctx, `
		INSERT INTO investment.amfi_nav_staging
		(scheme_code, isin_div_payout_growth, isin_div_reinvestment,
		scheme_name, nav_value, nav_date, amc_name, file_date)
		SELECT t.scheme_code::bigint, 
			   t.isin_div_payout_growth, 
			   t.isin_div_reinvestment,
			   t.scheme_name, 
			   CASE 
				   WHEN t.nav_value IS NULL OR t.nav_value = '' THEN NULL
				   ELSE t.nav_value::numeric(18,4)
			   END,
			   t.nav_date, 
			   t.amc_name, 
			   t.file_date
		FROM temp_nav_staging t
		INNER JOIN investment.amfi_scheme_master_staging s ON t.scheme_code::bigint = s.scheme_code
		WHERE t.scheme_code ~ '^[0-9]+$'
		ON CONFLICT (scheme_code, nav_date) DO UPDATE SET
			isin_div_payout_growth = EXCLUDED.isin_div_payout_growth,
			isin_div_reinvestment = EXCLUDED.isin_div_reinvestment,
			scheme_name = EXCLUDED.scheme_name,
			nav_value = EXCLUDED.nav_value,
			amc_name = EXCLUDED.amc_name,
			file_date = EXCLUDED.file_date`)

	if err != nil {
		return fmt.Errorf("error upserting NAV data: %v", err)
	}

	upsertDuration := time.Since(start)
	rowsAffected := result.RowsAffected()

	// Count unmatched records
	var unmatchedCount int
	err = db.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM temp_nav_staging t
		LEFT JOIN investment.amfi_scheme_master_staging s ON t.scheme_code::bigint = s.scheme_code
		WHERE t.scheme_code ~ '^[0-9]+$' AND s.scheme_code IS NULL`).Scan(&unmatchedCount)
	if err != nil {
		unmatchedCount = 0
	}

	logger.GlobalLogger.LogAudit(fmt.Sprintf("Processed and inserted %d NAV records into NAV History", rowsAffected))
	if unmatchedCount > 0 {
		logger.GlobalLogger.LogAudit(fmt.Sprintf("Found %d scheme codes in NAV file with no match in local database", unmatchedCount))
	}
	logger.GlobalLogger.LogAudit(fmt.Sprintf("Total NAV processing time: %v", copyDuration+upsertDuration))

	// Drop temp table
	_, err = db.Exec(ctx, "DROP TABLE IF EXISTS temp_nav_staging")
	if err != nil {
		logger.GlobalLogger.LogAudit(fmt.Sprintf("Warning: Failed to drop temp table: %v", err))
	}

	return nil
}

func parseDate(input string) *string {
	if input == "" {
		return nil
	}
	layout := constants.DateFormatDash
	t, err := time.Parse(layout, input)
	if err != nil {
		return nil
	}
	// Format as YYYY-MM-DD for Supabase compatibility
	formatted := t.Format(constants.DateFormat)
	return &formatted
}

// isNumeric checks if a string contains only digits
func isNumeric(s string) bool {
	if s == "" {
		return false
	}
	for _, char := range s {
		if char < '0' || char > '9' {
			return false
		}
	}
	return true
}

// parseMinAmount extracts numeric value from minimum amount string
// Handles cases like "Rs. 1000/ and any amount thereafter", "1000", "5000", etc.
func parseMinAmount(input string) *string {
	if input == "" {
		return nil
	}

	// Remove common prefixes and suffixes
	cleaned := strings.ToLower(strings.TrimSpace(input))
	cleaned = strings.ReplaceAll(cleaned, "rs.", "")
	cleaned = strings.ReplaceAll(cleaned, "rs", "")
	cleaned = strings.ReplaceAll(cleaned, "inr", "")
	cleaned = strings.ReplaceAll(cleaned, "₹", "")
	cleaned = strings.TrimSpace(cleaned)

	// Find the first sequence of digits (possibly with commas)
	var numStr strings.Builder
	foundDigit := false

	for _, char := range cleaned {
		if char >= '0' && char <= '9' {
			numStr.WriteRune(char)
			foundDigit = true
		} else if char == ',' && foundDigit {
			// Skip commas in numbers like "1,000"
			continue
		} else if foundDigit {
			// Stop at first non-digit after we've found digits
			break
		}
	}

	result := numStr.String()
	if result == "" {
		return nil
	}

	return &result
}

// RunAMFIDataDownloaderOnce runs the AMFI data downloader job once without scheduling
func RunAMFIDataDownloaderOnce(cfg *Config, db *pgxpool.Pool) error {
	// Set default values if not provided
	if cfg.DefaultSchemeURL == "" {
		cfg.DefaultSchemeURL = config.DefaultSchemeURL
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 1000
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = 3
	}
	if cfg.RetryDelay == 0 {
		cfg.RetryDelay = 2 * time.Second
	}

	// Create circuit breakers for HTTP and DB operations
	httpCircuitBreaker := NewCircuitBreaker(5, 30*time.Second)
	dbCircuitBreaker := NewCircuitBreaker(3, 60*time.Second)

	// Process scheme data with retry logic
	return RetryWithBackoff(cfg.MaxRetries, cfg.RetryDelay, func() error {
		return processSchemeDataWithCircuitBreaker(cfg.DefaultSchemeURL, db, cfg.BatchSize, httpCircuitBreaker, dbCircuitBreaker)
	})
}

// RunAMFINAVSyncOnce runs the AMFI NAV synchronization job once without scheduling
func RunAMFINAVSyncOnce(cfg *Config, db *pgxpool.Pool) error {
	// Set default values if not provided
	if cfg.DefaultNavURL == "" {
		cfg.DefaultNavURL = config.DefaultNavURL
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 1000
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = 3
	}
	if cfg.RetryDelay == 0 {
		cfg.RetryDelay = 2 * time.Second
	}

	// Create circuit breakers for HTTP and DB operations
	httpCircuitBreaker := NewCircuitBreaker(5, 30*time.Second)
	dbCircuitBreaker := NewCircuitBreaker(3, 60*time.Second)

	// Process NAV data with retry logic
	return RetryWithBackoff(cfg.MaxRetries, cfg.RetryDelay, func() error {
		return processNAVDataWithCircuitBreaker(cfg.DefaultNavURL, db, cfg.BatchSize, httpCircuitBreaker, dbCircuitBreaker)
	})
}
