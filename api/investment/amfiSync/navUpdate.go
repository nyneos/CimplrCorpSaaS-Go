package amfisync

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"CimplrCorpSaas/internal/config"
	investmentjobs "CimplrCorpSaas/internal/jobs/investment"
	"CimplrCorpSaas/internal/logger"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// NAVSyncResponse represents the API response structure for NAV sync
type NAVSyncResponse struct {
	Success bool        `json:"success"`
	Status  string      `json:"status"`
	Data    NAVSyncData `json:"data"`
	Logs    []LogEntry  `json:"logs"`
}

// NAVSyncData represents the NAV sync operation statistics
type NAVSyncData struct {
	RecordsProcessed int    `json:"records_processed"`
	RecordsUpdated   int    `json:"records_updated"`
	RecordsInserted  int    `json:"records_inserted"`
	LastSynced       string `json:"last_synced"`
}

// UpdateNAVHandler manually triggers the AMFI NAV synchronization with optimized bulk processing
func UpdateNAVHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}

		// Initialize response
		response := NAVSyncResponse{
			Success: false,
			Status:  constants.ValueError,
			Data: NAVSyncData{
				LastSynced: time.Now().Format("02-01-2006 15:04"),
			},
			Logs: []LogEntry{},
		}

		// Add initial logs
		response.Logs = append(response.Logs, LogEntry{
			Level:   "INFO",
			Message: "Starting Daily NAV Update job...",
		})

		response.Logs = append(response.Logs, LogEntry{
			Level:   "INFO",
			Message: "Connecting to AMFI URL: https://www.amfiindia.com/spages/NAVAll.txt",
		})

		response.Logs = append(response.Logs, LogEntry{
			Level:   "INFO",
			Message: "File downloaded successfully.",
		})

		response.Logs = append(response.Logs, LogEntry{
			Level:   "INFO",
			Message: "Begin parsing file...",
		})

		// Run the optimized NAV sync
		recordsProcessed, err := runOptimizedNAVSync(pool, &response.Logs)
		if err != nil {
			response.Logs = append(response.Logs, LogEntry{
				Level:   "ERROR",
				Message: fmt.Sprintf("NAV synchronization failed: %v", err),
			})
			api.RespondEnvelopeFailureCompat(w, http.StatusInternalServerError, "NAV synchronization failed: "+err.Error(), "", map[string]interface{}{
				"status": response.Status,
				"data":   response.Data,
				"logs":   response.Logs,
			})
			return
		}

		// Add final success log
		response.Logs = append(response.Logs, LogEntry{
			Level:   "INFO",
			Message: "Job completed successfully.",
		})

		// Update response with success data
		response.Success = true
		response.Status = constants.ValueSuccess
		response.Data.RecordsProcessed = recordsProcessed
		response.Data.RecordsInserted = recordsProcessed
		response.Data.RecordsUpdated = 0

		api.RespondEnvelopeSuccessCompat(w, "Job completed successfully.", map[string]interface{}{
			"status": response.Status,
			"data":   response.Data,
			"logs":   response.Logs,
		})
	}
}

// NAVRecord represents a single NAV record for bulk processing
type NAVRecord struct {
	SchemeCode   string
	ISINGrowth   string
	ISINReinvest string
	SchemeName   string
	NAVValue     string
	NAVDate      *string
	AMCName      string
}

// runOptimizedNAVSync runs the optimized bulk NAV sync
func runOptimizedNAVSync(pool *pgxpool.Pool, logs *[]LogEntry) (int, error) {
	httpCircuitBreaker := investmentjobs.NewCircuitBreaker(5, 30*time.Second)

	navURL := config.DefaultNavURL
	if navURL == "" {
		navURL = "https://www.amfiindia.com/spages/NAVAll.txt"
	}

	var recordsProcessed int
	err := investmentjobs.RetryWithBackoff(3, 2*time.Second, func() error {
		processed, err := processOptimizedNAVData(navURL, pool, 5000, httpCircuitBreaker, logs)
		recordsProcessed = processed
		return err
	})

	return recordsProcessed, err
}

// processOptimizedNAVData processes NAV data with bulk operations for maximum speed
func processOptimizedNAVData(url string, db *pgxpool.Pool, batchSize int, httpCB *investmentjobs.CircuitBreaker, logs *[]LogEntry) (int, error) {
	var navRecords []NAVRecord

	// HTTP request with circuit breaker protection
	err := httpCB.Execute(func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()

		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return fmt.Errorf("error creating request: %v", err)
		}

		client := &http.Client{Timeout: 120 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("error fetching AMFI NAV data: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("HTTP error: %d", resp.StatusCode)
		}

		scanner := bufio.NewScanner(resp.Body)
		scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
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
		return 0, err
	}

	// Process records — do not wrap deterministic SQL errors in the DB circuit
	// breaker; that hid the real upsert failure behind "circuit breaker is open".
	processed, err := processBulkNAVData(navRecords, db, batchSize, logs)
	return processed, err
}

// processBulkNAVData performs bulk insert/upsert operations for global lookup processing
func processBulkNAVData(navRecords []NAVRecord, db *pgxpool.Pool, batchSize int, logs *[]LogEntry) (int, error) {
	// Create context with longer timeout for bulk operations
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	conn, err := db.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("acquire connection for NAV staging: %w", err)
	}
	defer conn.Release()

	// Drop and create temp table for bulk operations
	_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS temp_nav_staging")
	_, err = conn.Exec(ctx, `
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
		return 0, fmt.Errorf("error creating temp table: %v", err)
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

	// Bulk copy to temp table
	_, err = conn.CopyFrom(ctx, pgx.Identifier{"temp_nav_staging"},
		[]string{"scheme_code", "isin_div_payout_growth", "isin_div_reinvestment", "scheme_name", "nav_value", "nav_date", "amc_name"},
		pgx.CopyFromRows(validRecords))
	if err != nil {
		return 0, fmt.Errorf("error bulk copying NAV data: %v", err)
	}

	// STEP 1: Check for new schemes and insert them into amfi_scheme_master_staging
	var newSchemesCount int
	err = conn.QueryRow(ctx, `
		SELECT COUNT(DISTINCT t.scheme_code)
		FROM temp_nav_staging t
		LEFT JOIN investment.amfi_scheme_master_staging s ON t.scheme_code::bigint = s.scheme_code
		WHERE t.scheme_code ~ '^[0-9]+$' AND s.scheme_code IS NULL`).Scan(&newSchemesCount)
	if err != nil {
		newSchemesCount = 0
	}

	if newSchemesCount > 0 {
		// Insert new schemes into amfi_scheme_master_staging
		schemeResult, err := conn.Exec(ctx, `
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
			*logs = append(*logs, LogEntry{
				Level:   "WARN",
				Message: fmt.Sprintf("Failed to insert new schemes: %v", err),
			})
		} else {
			addedSchemes := schemeResult.RowsAffected()
			if addedSchemes > 0 {
				*logs = append(*logs, LogEntry{
					Level:   "SUCCESS",
					Message: fmt.Sprintf("Created %d new scheme records in Global Lookups.", addedSchemes),
				})
			}
		}
	}

	// Count distinct AMCs for reporting (but they're stored in scheme master, not separate table)
	var newAmcCount int
	err = conn.QueryRow(ctx, `
		SELECT COUNT(DISTINCT t.amc_name)
		FROM temp_nav_staging t
		LEFT JOIN investment.amfi_scheme_master_staging s ON t.amc_name = s.amc_name
		WHERE s.amc_name IS NULL AND t.amc_name IS NOT NULL AND t.amc_name != ''`).Scan(&newAmcCount)
	if err != nil {
		newAmcCount = 0
	}

	if newAmcCount > 0 {
		*logs = append(*logs, LogEntry{
			Level:   "SUCCESS",
			Message: fmt.Sprintf("Found %d new AMC(s).", newAmcCount),
		})
	}

	// STEP 2: Now process NAV data for all schemes (existing + newly added)
	// Insert NAV records for all schemes that now exist in the master (including newly added ones)
	result, err := conn.Exec(ctx, `
		INSERT INTO investment.amfi_nav_staging
		(scheme_code, isin_div_payout_growth, isin_div_reinvestment,
		scheme_name, nav_value, nav_date, amc_name, file_date)
		SELECT DISTINCT ON (t.scheme_code, t.nav_date)
			   t.scheme_code::bigint,
			   t.isin_div_payout_growth,
			   t.isin_div_reinvestment,
			   t.scheme_name,
			   CASE
				   WHEN t.nav_value ~ '^-?[0-9]+(\.[0-9]+)?$' THEN t.nav_value::numeric(18,4)
				   ELSE NULL
			   END,
			   t.nav_date,
			   t.amc_name,
			   t.file_date
		FROM temp_nav_staging t
		INNER JOIN investment.amfi_scheme_master_staging s ON t.scheme_code::bigint = s.scheme_code
		WHERE t.scheme_code ~ '^[0-9]+$'
		  AND t.nav_date IS NOT NULL
		ORDER BY t.scheme_code, t.nav_date, t.file_date DESC NULLS LAST
		ON CONFLICT (scheme_code, nav_date) DO UPDATE SET
			isin_div_payout_growth = EXCLUDED.isin_div_payout_growth,
			isin_div_reinvestment = EXCLUDED.isin_div_reinvestment,
			scheme_name = EXCLUDED.scheme_name,
			nav_value = EXCLUDED.nav_value,
			amc_name = EXCLUDED.amc_name,
			file_date = EXCLUDED.file_date`)

	if err != nil {
		return 0, fmt.Errorf("error upserting NAV data: %v", err)
	}

	processedRows := int(result.RowsAffected())

	// Count unmatched records (should be minimal or zero now)
	var unmatchedCount int
	err = conn.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM temp_nav_staging t
		LEFT JOIN investment.amfi_scheme_master_staging s ON t.scheme_code::bigint = s.scheme_code
		WHERE t.scheme_code ~ '^[0-9]+$' AND s.scheme_code IS NULL`).Scan(&unmatchedCount)
	if err != nil {
		unmatchedCount = 0 // If query fails, just set to 0
	}

	// Log results in expected format
	*logs = append(*logs, LogEntry{
		Level:   "SUCCESS",
		Message: fmt.Sprintf("Processed and inserted %d NAV records into NAV History.", processedRows),
	})

	if unmatchedCount > 0 {
		*logs = append(*logs, LogEntry{
			Level:   "WARN",
			Message: fmt.Sprintf("Found %d scheme codes in NAV file with no match in local database. Check error logs.", unmatchedCount),
		})
	}

	// Clean up temp table
	_, err = conn.Exec(ctx, "DROP TABLE IF EXISTS temp_nav_staging")
	if err != nil {
		logger.GlobalLogger.LogAudit(fmt.Sprintf("Warning: Failed to drop temp table: %v", err))
	}

	return processedRows, nil
}

// parseDate parses date string in DD-MMM-YYYY format to YYYY-MM-DD
func parseDate(input string) *string {
	if input == "" {
		return nil
	}
	layout := constants.DateFormatDash
	t, err := time.Parse(layout, input)
	if err != nil {
		return nil
	}
	// Format as YYYY-MM-DD for PostgreSQL compatibility
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
