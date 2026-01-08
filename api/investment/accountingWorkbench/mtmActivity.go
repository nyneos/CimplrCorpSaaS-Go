package accountingworkbench

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/constants"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ---------------------------
// Request/Response Types for MTM
// ---------------------------

type CreateMTMRequest struct {
	UserID           string  `json:"user_id"`
	ActivityType     string  `json:"activity_type"`     // "MTM"
	EffectiveDate    string  `json:"effective_date"`    // YYYY-MM-DD
	AccountingPeriod string  `json:"accounting_period"` // YYYY-MM-DD
	DataSource       string  `json:"data_source"`       // "Manual" or "Auto"
	SchemeID         string  `json:"scheme_id"`
	FolioID          string  `json:"folio_id,omitempty"`
	DematID          string  `json:"demat_id,omitempty"`
	Units            float64 `json:"units"`
	PrevNAV          float64 `json:"prev_nav"`
	CurrNAV          float64 `json:"curr_nav"`
	NAVDate          string  `json:"nav_date"` // YYYY-MM-DD
	PrevValue        float64 `json:"prev_value"`
	CurrValue        float64 `json:"curr_value"`
	UnrealizedGL     float64 `json:"unrealized_gain_loss"`
	UnrealizedGLPct  float64 `json:"unrealized_gain_loss_pct,omitempty"`
}

type UpdateMTMRequest struct {
	UserID string                 `json:"user_id"`
	MTMID  int64                  `json:"mtm_id"`
	Fields map[string]interface{} `json:"fields"`
	Reason string                 `json:"reason"`
}

// ---------------------------
// CreateMTMSingle
// ---------------------------

func CreateMTMSingle(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req CreateMTMRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}

		// Validate required fields
		if strings.TrimSpace(req.SchemeID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "scheme_id is required")
			return
		}
		if req.Units <= 0 || req.PrevNAV <= 0 || req.CurrNAV <= 0 {
			api.RespondWithError(w, http.StatusBadRequest, "units, prev_nav, and curr_nav must be greater than 0")
			return
		}

		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Name
				break
			}
		}
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxBeginFailedCapitalized+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		// Create main activity
		activityType := "MTM"
		if strings.TrimSpace(req.ActivityType) != "" {
			activityType = req.ActivityType
		}

		dataSource := "Manual"
		if strings.TrimSpace(req.DataSource) != "" {
			dataSource = req.DataSource
		}

		var activityID string
		if err := tx.QueryRow(ctx, `
			INSERT INTO investment.accounting_activity (
				activity_type, effective_date, accounting_period, data_source, status
			) VALUES ($1, $2, $3, $4, 'DRAFT')
			RETURNING activity_id
		`, activityType, req.EffectiveDate, nullIfEmptyString(req.AccountingPeriod), dataSource).Scan(&activityID); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrActivityInsertFailed+err.Error())
			return
		}

		// Insert MTM record
		var mtmID int64
		if err := tx.QueryRow(ctx, `
			INSERT INTO investment.accounting_mtm (
				activity_id, scheme_id, folio_id, demat_id, units, prev_nav, curr_nav, nav_date,
				prev_value, curr_value, unrealized_gain_loss, unrealized_gain_loss_pct
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
			RETURNING mtm_id
		`, activityID, req.SchemeID, nullIfEmptyString(req.FolioID), nullIfEmptyString(req.DematID),
			req.Units, req.PrevNAV, req.CurrNAV, req.NAVDate, req.PrevValue, req.CurrValue,
			req.UnrealizedGL, nullIfZeroFloat(req.UnrealizedGLPct)).Scan(&mtmID); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrMTMInsertFailed+err.Error())
			return
		}

		// Create audit trail
		if _, err := tx.Exec(ctx, `
			INSERT INTO investment.auditactionaccountingactivity (activity_id, actiontype, processing_status, requested_by, requested_at)
			VALUES ($1, 'CREATE', 'PENDING_APPROVAL', $2, now())
		`, activityID, userEmail); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrAuditInsertFailed+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailedCapitalized+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]any{
			"activity_id": activityID,
			"mtm_id":      mtmID,
			"requested":   userEmail,
		})
	}
}

// ---------------------------
// CreateMTMBulk - AUTO-CURATED VERSION
// ---------------------------
// User provides only accounting_period and effective_date.
// System auto-fetches holdings from portfolio_snapshot, retrieves NAVs from AMFI, and calculates all MTM values.

func CreateMTMBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID           string `json:"user_id"`
			ActivityType     string `json:"activity_type"`     // "MTM"
			EffectiveDate    string `json:"effective_date"`    // YYYY-MM-DD
			AccountingPeriod string `json:"accounting_period"` // Text format: "JAN 2025", "FEB 2025", etc.
			DataSource       string `json:"data_source"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			log.Printf("CreateMTMBulk: invalid JSON request: %v", err)
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}

		// Validate required fields
		if strings.TrimSpace(req.AccountingPeriod) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "accounting_period is required (e.g., 'JAN 2025')")
			return
		}
		if strings.TrimSpace(req.EffectiveDate) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "effective_date is required (YYYY-MM-DD)")
			return
		}

		// Parse accounting period - supports two formats:
		// 1. "JAN 2025" or "JANUARY 2025" (text format)
		// 2. "2025-12" (YYYY-MM format)
		var monthNum, year string
		accountingPeriod := strings.TrimSpace(req.AccountingPeriod)

		// Check if it's in YYYY-MM format
		if strings.Contains(accountingPeriod, "-") && len(accountingPeriod) == 7 {
			// Format: "2025-12"
			parts := strings.Split(accountingPeriod, "-")
			if len(parts) != 2 {
				api.RespondWithError(w, http.StatusBadRequest, "accounting_period format should be 'YYYY-MM' or 'MONTH YYYY' (e.g., '2025-12' or 'JAN 2025')")
				return
			}
			year = parts[0]
			monthNum = parts[1]

			// Validate month is between 01-12
			monthInt := 0
			fmt.Sscanf(monthNum, "%d", &monthInt)
			if monthInt < 1 || monthInt > 12 {
				api.RespondWithError(w, http.StatusBadRequest, "Invalid month in accounting_period (must be 01-12)")
				return
			}
		} else {
			// Text format: "JAN 2025" or "JANUARY 2025"
			monthMap := map[string]string{
				"JAN": "01", "JANUARY": "01",
				"FEB": "02", "FEBRUARY": "02",
				"MAR": "03", "MARCH": "03",
				"APR": "04", "APRIL": "04",
				"MAY": "05",
				"JUN": "06", "JUNE": "06",
				"JUL": "07", "JULY": "07",
				"AUG": "08", "AUGUST": "08",
				"SEP": "09", "SEPTEMBER": "09",
				"OCT": "10", "OCTOBER": "10",
				"NOV": "11", "NOVEMBER": "11",
				"DEC": "12", "DECEMBER": "12",
			}

			parts := strings.Fields(strings.ToUpper(accountingPeriod))
			if len(parts) != 2 {
				api.RespondWithError(w, http.StatusBadRequest, "accounting_period format should be 'MONTH YYYY' or 'YYYY-MM' (e.g., 'JAN 2025' or '2025-12')")
				return
			}

			var ok bool
			monthNum, ok = monthMap[parts[0]]
			if !ok {
				api.RespondWithError(w, http.StatusBadRequest, "Invalid month name in accounting_period")
				return
			}
			year = parts[1]
		}

		// Construct start and end dates for the month
		startDate := fmt.Sprintf("%s-%s-01", year, monthNum)

		// Calculate last day of month
		var endDate string
		switch monthNum {
		case "01", "03", "05", "07", "08", "10", "12":
			endDate = fmt.Sprintf("%s-%s-31", year, monthNum)
		case "04", "06", "09", "11":
			endDate = fmt.Sprintf("%s-%s-30", year, monthNum)
		case "02":
			// Leap year check
			yearInt := 0
			fmt.Sscanf(year, "%d", &yearInt)
			if yearInt%4 == 0 && (yearInt%100 != 0 || yearInt%400 == 0) {
				endDate = fmt.Sprintf("%s-02-29", year)
			} else {
				endDate = fmt.Sprintf("%s-02-28", year)
			}
		}

		// Validate not future month
		currentTime := time.Now()
		requestMonth, _ := time.Parse(constants.DateFormatYearMonth, fmt.Sprintf("%s-%s", year, monthNum))
		if requestMonth.After(currentTime) {
			api.RespondWithError(w, http.StatusBadRequest, fmt.Sprintf("Cannot process future month '%s'. Current month is %s", req.AccountingPeriod, currentTime.Format(constants.DateFormatYearMonth)))
			return
		}

		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Name
				break
			}
		}
		if userEmail == "" {
			log.Printf("CreateMTMBulk: invalid session for user_id=%s", req.UserID)
			api.RespondWithError(w, http.StatusUnauthorized, "Invalid user session")
			return
		}

		ctx := r.Context()

		// Single transaction for entire batch
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			log.Printf("CreateMTMBulk: failed to begin transaction: %v", err)
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxBeginFailedCapitalized+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		// STEP 1: Aggregate holdings from onboard_transaction (similar to redemption.go pattern)
		// Calculate net units and avg_nav based on transaction history up to accounting period
		holdingsQuery := `
			WITH ot AS (
				SELECT
					t.*,
					ms.scheme_name AS ms_scheme_name, 
					ms.isin AS ms_isin, 
					ms.scheme_id AS ms_scheme_id, 
					ms.internal_scheme_code AS ms_internal_code
				FROM investment.onboard_transaction t
				LEFT JOIN investment.masterscheme ms ON (
  (t.scheme_id IS NOT NULL AND ms.scheme_id = t.scheme_id)
  OR (t.scheme_internal_code IS NOT NULL AND ms.internal_scheme_code = t.scheme_internal_code)
)
				WHERE t.transaction_date <= $1::date  -- Only transactions up to start of accounting period
				  AND LOWER(COALESCE(t.transaction_type,'')) IN ('buy','purchase','subscription','sell','redemption')
			)
			SELECT
				COALESCE(ot.folio_number,'') AS folio_number,
				COALESCE(ot.demat_acc_number,'') AS demat_acc_number,
				COALESCE(ot.scheme_id, ot.scheme_internal_code, ot.ms_scheme_id) AS scheme_id,
				COALESCE(ot.ms_scheme_name, ot.scheme_internal_code) AS scheme_name,
				COALESCE(ot.ms_isin,'') AS isin,
				COALESCE(ot.ms_internal_code,'') AS internal_code,
				-- net units: buys (positive) minus sells (absolute)
				SUM(CASE 
					WHEN LOWER(COALESCE(ot.transaction_type,'')) IN ('buy','purchase','subscription') 
					THEN COALESCE(ot.units,0) 
					ELSE 0 
				END) - SUM(CASE 
					WHEN LOWER(COALESCE(ot.transaction_type,'')) IN ('sell','redemption') 
					THEN ABS(COALESCE(ot.units,0)) 
					ELSE 0 
				END) AS total_units,
				-- avg_nav based only on buy transactions (weighted average cost basis)
				CASE 
					WHEN SUM(CASE 
						WHEN LOWER(COALESCE(ot.transaction_type,'')) IN ('buy','purchase','subscription') 
						THEN COALESCE(ot.units,0) 
						ELSE 0 
					END) = 0 THEN 0
					ELSE SUM(CASE 
						WHEN LOWER(COALESCE(ot.transaction_type,'')) IN ('buy','purchase','subscription') 
						THEN COALESCE(ot.nav,0) * COALESCE(ot.units,0) 
						ELSE 0 
					END) / SUM(CASE 
						WHEN LOWER(COALESCE(ot.transaction_type,'')) IN ('buy','purchase','subscription') 
						THEN COALESCE(ot.units,0) 
						ELSE 0 
					END)
				END AS avg_nav
			FROM ot
			GROUP BY 
				COALESCE(ot.folio_number,''), 
				COALESCE(ot.demat_acc_number,''), 
				COALESCE(ot.scheme_id, ot.scheme_internal_code, ot.ms_scheme_id), 
				COALESCE(ot.ms_scheme_name, ot.scheme_internal_code), 
				COALESCE(ot.ms_isin,''),
				COALESCE(ot.ms_internal_code,'')
			HAVING (
				SUM(CASE 
					WHEN LOWER(COALESCE(ot.transaction_type,'')) IN ('buy','purchase','subscription') 
					THEN COALESCE(ot.units,0) 
					ELSE 0 
				END) - SUM(CASE 
					WHEN LOWER(COALESCE(ot.transaction_type,'')) IN ('sell','redemption') 
					THEN ABS(COALESCE(ot.units,0)) 
					ELSE 0 
				END)
			) > 0
			ORDER BY COALESCE(ot.folio_number,''), COALESCE(ot.demat_acc_number,''), COALESCE(ot.scheme_id, ot.scheme_internal_code, ot.ms_scheme_id)
		`

		holdingsRows, err := tx.Query(ctx, holdingsQuery, startDate)
		if err != nil {
			log.Printf("CreateMTMBulk: holdings query failed: %v", err)
			api.RespondWithError(w, http.StatusInternalServerError, "Holdings fetch failed: "+err.Error())
			return
		}
		defer holdingsRows.Close()

		// Collect holdings
		type HoldingData struct {
			FolioNumber    string
			DematAccNumber string
			SchemeID       string
			SchemeName     string
			ISIN           string
			InternalCode   string
			Units          float64
			AvgNAV         float64 // Weighted average cost basis from buy transactions
		}

		holdings := []HoldingData{}
		for holdingsRows.Next() {
			var h HoldingData
			if err := holdingsRows.Scan(&h.FolioNumber, &h.DematAccNumber, &h.SchemeID, &h.SchemeName, &h.ISIN, &h.InternalCode, &h.Units, &h.AvgNAV); err != nil {
				log.Printf("CreateMTMBulk: holdings scan failed: %v", err)
				api.RespondWithError(w, http.StatusInternalServerError, "Holdings scan failed: "+err.Error())
				return
			}
			holdings = append(holdings, h)
		}

		if len(holdings) == 0 {
			// Check if onboard_transaction table has any data at all
			var txCount int
			tx.QueryRow(ctx, "SELECT COUNT(*) FROM investment.onboard_transaction WHERE LOWER(COALESCE(transaction_type,'')) IN ('buy','purchase','subscription')").Scan(&txCount)

			if txCount == 0 {
				api.RespondWithError(w, http.StatusBadRequest, fmt.Sprintf("No investment transactions found. Please upload transaction data before running MTM for %s", req.AccountingPeriod))
			} else {
				api.RespondWithError(w, http.StatusBadRequest, fmt.Sprintf("No active holdings found for accounting period %s (start date: %s). Found %d total buy transactions but all were after this date or fully redeemed.", req.AccountingPeriod, startDate, txCount))
			}
			return
		}

		// Create main activity
		activityType := "MTM"
		if strings.TrimSpace(req.ActivityType) != "" {
			activityType = req.ActivityType
		}

		dataSource := "Auto"
		if strings.TrimSpace(req.DataSource) != "" {
			dataSource = req.DataSource
		}

		var activityID string
		if err := tx.QueryRow(ctx, `
			INSERT INTO investment.accounting_activity (
				activity_type, effective_date, accounting_period, data_source, status
			) VALUES ($1, $2, $3, $4, 'DRAFT')
			RETURNING activity_id
		`, activityType, req.EffectiveDate, req.AccountingPeriod, dataSource).Scan(&activityID); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrActivityInsertFailed+err.Error())
			return
		}

		results := make([]map[string]interface{}, 0, len(holdings))
		hadError := false

		// STEP 2: For each holding, fetch historical NAV and calculate MTM
		for _, h := range holdings {
			// Determine previous NAV (prev_nav). Prefer the previous day's market NAV (relative
			// to the end NAV date). Fall back to amfi_nav_staging, and finally to avg_nav (cost basis).
			var prevNAV float64
			// We'll compute prevNAV after we have actualEndDate (set below when fetching end NAV).

			// Try to fetch current/end-of-month NAV from MFapi.in using ISIN or internal code
			// MFapi uses scheme_code which maps to AMFI codes
			var endNAV float64
			var actualEndDate string
			var navFetchError string

			// First, try to find scheme_code from our masterscheme table (if it has amfi_scheme_code)
			var amfiCode string
			if err := tx.QueryRow(ctx, `
				SELECT COALESCE(amfi_scheme_code, '') FROM investment.masterscheme 
				WHERE scheme_id = $1 OR isin = $2 OR internal_scheme_code = $3 
				LIMIT 1
			`, h.SchemeID, h.ISIN, h.InternalCode).Scan(&amfiCode); err != nil || amfiCode == "" {
				// Fallback: Try using internal_scheme_code as AMFI code
				amfiCode = h.InternalCode
			}

			if amfiCode != "" {
				// Call MFapi.in to get historical NAV
				// Format: https://api.mfapi.in/mf/{scheme_code}?startDate=YYYY-MM-DD&endDate=YYYY-MM-DD
				apiURL := fmt.Sprintf("https://api.mfapi.in/mf/%s?startDate=%s&endDate=%s", amfiCode, startDate, endDate)

				resp, err := http.Get(apiURL)
				if err == nil && resp.StatusCode == 200 {
					defer resp.Body.Close()
					body, _ := io.ReadAll(resp.Body)

					var apiResp struct {
						Meta struct {
							SchemeName string `json:"scheme_name"`
						} `json:"meta"`
						Data []struct {
							Date string `json:"date"`
							NAV  string `json:"nav"`
						} `json:"data"`
						Status string `json:"status"`
					}

					if err := json.Unmarshal(body, &apiResp); err == nil && len(apiResp.Data) > 0 {
						// Get the latest NAV from the period (last entry)
						lastEntry := apiResp.Data[len(apiResp.Data)-1]
						// Parse possible MFAPI date formats into YYYY-MM-DD
						parsed := time.Time{}
						var parseErr error
						// Try a few common formats MFAPI returns
						for _, f := range []string{"2006-01-02", "02-01-2006", "02-Jan-2006", "02-Jan-06"} {
							parsed, parseErr = time.Parse(f, lastEntry.Date)
							if parseErr == nil {
								break
							}
						}
						if parseErr != nil {
							// If we cannot parse, log and mark as fetch error
							log.Printf("CreateMTMBulk: unable to parse NAV date '%s' for scheme %s: %v", lastEntry.Date, amfiCode, parseErr)
							navFetchError = "Invalid NAV date format"
						} else {
							actualEndDate = parsed.Format("2006-01-02")
						}

						if navVal, err := strconv.ParseFloat(lastEntry.NAV, 64); err == nil {
							endNAV = navVal
						} else {
							navFetchError = "Failed to parse NAV value"
						}
					} else {
						navFetchError = "No NAV data found in response"
					}
				} else if resp != nil {
					navFetchError = fmt.Sprintf("MFapi returned status %d", resp.StatusCode)
				} else {
					navFetchError = "Failed to fetch from MFapi: " + err.Error()
				}
			} else {
				navFetchError = "No AMFI code found for scheme"
			}

			// If MFapi failed, try local amfi_nav_staging as fallback
			if endNAV == 0 {
				// Try to use numeric AMFI code (amfiCode) for fallback staging table
				if amfiCode != "" {
					if amfiInt, err := strconv.ParseInt(amfiCode, 10, 64); err == nil {
						var localNAV float64
						if err := tx.QueryRow(ctx, `
							SELECT COALESCE(nav_value, 0) 
							FROM investment.amfi_nav_staging 
							WHERE scheme_code = $1 
								AND nav_date BETWEEN $2::date AND $3::date
							ORDER BY nav_date DESC 
							LIMIT 1
						`, amfiInt, startDate, endDate).Scan(&localNAV); err == nil && localNAV > 0 {
							endNAV = localNAV
							actualEndDate = endDate
							navFetchError = "" // Clear error, fallback succeeded
						} else if err != nil {
							log.Printf("CreateMTMBulk: amfi_nav_staging lookup failed for amfiCode=%d scheme=%s: %v", amfiInt, h.SchemeID, err)
						}
					} else {
						log.Printf("CreateMTMBulk: amfiCode '%s' for scheme %s is not numeric, cannot query amfi_nav_staging", amfiCode, h.SchemeID)
					}
				}
			}

			// If still no end NAV, record error and skip
			if endNAV == 0 {
				log.Printf("CreateMTMBulk: unable to fetch end NAV for scheme %s (%s): %s", h.SchemeID, h.SchemeName, navFetchError)
				results = append(results, map[string]interface{}{
					"success":     false,
					"scheme_id":   h.SchemeID,
					"scheme_name": h.SchemeName,
					"error":       fmt.Sprintf("Unable to fetch end NAV: %s", navFetchError),
				})
				continue
			}

			// At this point endNAV and actualEndDate should be set. Choose prevNAV as
			// the most recent available market NAV on or before actualEndDate.
			// Strategy:
			// 1. Query MFapi for a small historical window (last 7 days up to actualEndDate)
			//    and pick the latest entry with date <= actualEndDate.
			// 2. Fallback to investment.amfi_nav_staging with `nav_date <= actualEndDate` ordered desc.
			// 3. Final fallback to avg_nav (cost basis).
			if actualEndDate != "" {
				parsedEnd, perr := time.Parse("2006-01-02", actualEndDate)
				if perr == nil {
					startRange := parsedEnd.AddDate(0, 0, -7).Format("2006-01-02")
					// Try MFapi for range startRange..actualEndDate
					if amfiCode != "" {
						apiRangeURL := fmt.Sprintf("https://api.mfapi.in/mf/%s?startDate=%s&endDate=%s", amfiCode, startRange, actualEndDate)
						if respRange, err := http.Get(apiRangeURL); err == nil && respRange.StatusCode == 200 {
							defer respRange.Body.Close()
							bodyRange, _ := io.ReadAll(respRange.Body)
							var apiRangeResp struct {
								Data []struct {
									Date string `json:"date"`
									NAV  string `json:"nav"`
								} `json:"data"`
							}
							if err := json.Unmarshal(bodyRange, &apiRangeResp); err == nil && len(apiRangeResp.Data) > 0 {
								// Find the latest entry with date <= parsedEnd
								var bestDate time.Time
								var bestNAV float64
								for _, e := range apiRangeResp.Data {
									var d time.Time
									var dErr error
									for _, f := range []string{"2006-01-02", "02-01-2006", "02-Jan-2006", "02-Jan-06"} {
										d, dErr = time.Parse(f, e.Date)
										if dErr == nil {
											break
										}
									}
									if dErr != nil {
										continue
									}
									if d.After(parsedEnd) {
										continue
									}
									if bestDate.IsZero() || d.After(bestDate) {
										if navVal, nErr := strconv.ParseFloat(e.NAV, 64); nErr == nil {
											bestDate = d
											bestNAV = navVal
										}
									}
								}
								if !bestDate.IsZero() {
									prevNAV = bestNAV
								}
							}
						} else if respRange != nil {
							log.Printf("CreateMTMBulk: MFapi range returned status %d for amfiCode=%s", respRange.StatusCode, amfiCode)
						} else if err != nil {
							log.Printf("CreateMTMBulk: MFapi range request failed for amfiCode=%s: %v", amfiCode, err)
						}
					}

					// Fallback to staging: latest nav_date <= actualEndDate
					if prevNAV == 0 && amfiCode != "" {
						if amfiInt, err := strconv.ParseInt(amfiCode, 10, 64); err == nil {
							var stagedPrev float64
							if err := tx.QueryRow(ctx, `
								SELECT COALESCE(nav_value,0) FROM investment.amfi_nav_staging
								WHERE scheme_code = $1 AND nav_date <= $2::date
								ORDER BY nav_date DESC LIMIT 1
							`, amfiInt, actualEndDate).Scan(&stagedPrev); err == nil && stagedPrev > 0 {
								prevNAV = stagedPrev
							} else if err != nil {
								log.Printf("CreateMTMBulk: amfi_nav_staging lookup failed for amfiCode=%d scheme=%s up to date=%s: %v", amfiInt, h.SchemeID, actualEndDate, err)
							}
						}
					}
				} else {
					log.Printf("CreateMTMBulk: cannot parse actualEndDate '%s' for scheme %s: %v", actualEndDate, h.SchemeID, perr)
				}
			}

			// Final fallback to avg_nav (cost basis) if prevNAV is still zero or invalid
			if prevNAV <= 0 {
				prevNAV = h.AvgNAV
				if prevNAV <= 0 {
					results = append(results, map[string]interface{}{
						"success":     false,
						"scheme_id":   h.SchemeID,
						"scheme_name": h.SchemeName,
						"error":       "Invalid avg_nav (prev_nav fallback <= 0)",
					})
					hadError = true
					log.Printf("CreateMTMBulk: no valid prev_nav found for scheme %s; avg_nav also invalid", h.SchemeID)
					continue
				}
			}

			// Calculate MTM values (previous NAV -> current market NAV)
			prevValue := h.Units * prevNAV
			currValue := h.Units * endNAV
			unrealizedGL := currValue - prevValue
			var unrealizedGLPct float64
			if prevValue > 0 {
				unrealizedGLPct = (unrealizedGL / prevValue) * 100
			}

			// Lookup folio_id and demat_id from master tables
			var folioID, dematID *string
			if h.FolioNumber != "" {
				var fid string
				if err := tx.QueryRow(ctx, `SELECT folio_id FROM investment.masterfolio WHERE folio_number = $1 AND COALESCE(is_deleted,false) = false LIMIT 1`, h.FolioNumber).Scan(&fid); err == nil {
					folioID = &fid
				} else {
					log.Printf("CreateMTMBulk: no active folio_id found for folio_number=%s: %v", h.FolioNumber, err)
				}
			}
			if h.DematAccNumber != "" {
				var did string
				if err := tx.QueryRow(ctx, `SELECT demat_id FROM investment.masterdemataccount WHERE demat_account_number = $1 AND COALESCE(is_deleted,false) = false LIMIT 1`, h.DematAccNumber).Scan(&did); err == nil {
					dematID = &did
				} else {
					log.Printf("CreateMTMBulk: no active demat_id found for demat_account_number=%s: %v", h.DematAccNumber, err)
				}
			}

			var mtmID int64
			if err := tx.QueryRow(ctx, `
				INSERT INTO investment.accounting_mtm (
					activity_id, scheme_id, folio_id, demat_id, units, prev_nav, curr_nav, nav_date,
					prev_value, curr_value, unrealized_gain_loss, unrealized_gain_loss_pct
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
				RETURNING mtm_id
			`, activityID, h.SchemeID, folioID, dematID,
				h.Units, prevNAV, endNAV, actualEndDate,
				prevValue, currValue, unrealizedGL, unrealizedGLPct).Scan(&mtmID); err != nil {
				// Mark that we encountered a DB error for this row. Rollback will be performed
				// before attempting any further audit/commit operations so we don't try to execute
				// commands on an aborted transaction.
				hadError = true
				log.Printf("CreateMTMBulk: failed to insert MTM row for scheme %s: %v", h.SchemeID, err)
				results = append(results, map[string]interface{}{
					"success":     false,
					"scheme_id":   h.SchemeID,
					"scheme_name": h.SchemeName,
					"error":       constants.ErrMTMInsertFailed + err.Error(),
				})
				// continue processing remaining holdings to collect errors/results, but
				// we will rollback and return the aggregated results below instead of
				// attempting the audit insert on a possibly-aborted transaction.
				continue
			}

			results = append(results, map[string]interface{}{
				"success":                  true,
				"mtm_id":                   mtmID,
				"scheme_id":                h.SchemeID,
				"scheme_name":              h.SchemeName,
				"units":                    h.Units,
				"prev_nav":                 prevNAV,
				"curr_nav":                 endNAV,
				"nav_date":                 actualEndDate,
				"unrealized_gain_loss":     unrealizedGL,
				"unrealized_gain_loss_pct": unrealizedGLPct,
			})
		}

		// If any per-row insert failed, the transaction may be in an error state.
		// Abort and return partial results instead of trying to insert audit record
		// on an aborted transaction (which causes SQLSTATE 25P02).
		if hadError {
			// Log detailed row errors for debugging
			for _, r := range results {
				if success, ok := r["success"].(bool); ok && !success {
					log.Printf("CreateMTMBulk: row error detail: scheme=%v error=%v", r["scheme_id"], r["error"])
				}
			}
			log.Printf("CreateMTMBulk: encountered row-level errors, rolling back transaction. activity_id=%s", activityID)
			_ = tx.Rollback(ctx)
			api.RespondWithPayload(w, api.IsBulkSuccess(results), "Partial results due to row-level errors", map[string]any{
				"activity_id":       activityID,
				"accounting_period": req.AccountingPeriod,
				"holdings_found":    len(holdings),
				"mtm_records":       results,
			})
			return
		}

		// Create single audit trail for batch
		if _, err := tx.Exec(ctx, `
			INSERT INTO investment.auditactionaccountingactivity (activity_id, actiontype, processing_status, requested_by, requested_at)
			VALUES ($1, 'CREATE', 'PENDING_APPROVAL', $2, now())
		`, activityID, userEmail); err != nil {
			log.Printf("CreateMTMBulk: failed to insert audit row for activity_id=%s: %v", activityID, err)
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrAuditInsertFailed+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			log.Printf("CreateMTMBulk: failed to commit transaction for activity_id=%s: %v", activityID, err)
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailedCapitalized+err.Error())
			return
		}

		api.RespondWithPayload(w, api.IsBulkSuccess(results), "", map[string]any{
			"activity_id":       activityID,
			"accounting_period": req.AccountingPeriod,
			"holdings_found":    len(holdings),
			"mtm_records":       results,
		})
	}
}

// ---------------------------
// UpdateMTM
// ---------------------------

func UpdateMTM(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req UpdateMTMRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}
		if req.MTMID == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "mtm_id required")
			return
		}
		if len(req.Fields) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "no fields to update")
			return
		}

		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Name
				break
			}
		}
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxBeginFailedCapitalized+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		// Fetch existing values
		sel := `
			SELECT activity_id, scheme_id, folio_id, demat_id, units, prev_nav, curr_nav, nav_date,
			       prev_value, curr_value, unrealized_gain_loss, unrealized_gain_loss_pct
			FROM investment.accounting_mtm
			WHERE mtm_id=$1
			FOR UPDATE
		`
		var activityID string
		var oldVals [11]interface{}
		if err := tx.QueryRow(ctx, sel, req.MTMID).Scan(
			&activityID, &oldVals[0], &oldVals[1], &oldVals[2], &oldVals[3],
			&oldVals[4], &oldVals[5], &oldVals[6], &oldVals[7], &oldVals[8],
			&oldVals[9], &oldVals[10],
		); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "fetch failed: "+err.Error())
			return
		}

		fieldPairs := map[string]int{
			"scheme_id":                0,
			"folio_id":                 1,
			"demat_id":                 2,
			"units":                    3,
			"prev_nav":                 4,
			"curr_nav":                 5,
			"nav_date":                 6,
			"prev_value":               7,
			"curr_value":               8,
			"unrealized_gain_loss":     9,
			"unrealized_gain_loss_pct": 10,
		}

		var sets []string
		var args []interface{}
		pos := 1

		for k, v := range req.Fields {
			lk := strings.ToLower(k)
			if idx, ok := fieldPairs[lk]; ok {
				oldField := "old_" + lk
				sets = append(sets, fmt.Sprintf("%s=$%d, %s=$%d", lk, pos, oldField, pos+1))
				args = append(args, v, oldVals[idx])
				pos += 2
			}
		}

		if len(sets) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "no valid updatable fields found")
			return
		}

		q := fmt.Sprintf("UPDATE investment.accounting_mtm SET %s, updated_at=now() WHERE mtm_id=$%d", strings.Join(sets, ", "), pos)
		args = append(args, req.MTMID)
		if _, err := tx.Exec(ctx, q, args...); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "update failed: "+err.Error())
			return
		}

		// Audit
		if _, err := tx.Exec(ctx, `
			INSERT INTO investment.auditactionaccountingactivity (activity_id, actiontype, processing_status, reason, requested_by, requested_at)
			VALUES ($1, 'EDIT', 'PENDING_EDIT_APPROVAL', $2, $3, now())
		`, activityID, req.Reason, userEmail); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrAuditInsertFailed+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailedCapitalized+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]any{
			"mtm_id":      req.MTMID,
			"activity_id": activityID,
			"requested":   userEmail,
		})
	}
}

// ---------------------------
// GetMTMWithAudit
// ---------------------------

func GetMTMWithAudit(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		q := `
			WITH latest_audit AS (
				SELECT DISTINCT ON (a.activity_id)
					a.activity_id,
					a.actiontype,
					a.processing_status,
					a.action_id,
					a.requested_by,
					a.requested_at,
					a.checker_by,
					a.checker_at,
					a.checker_comment,
					a.reason
				FROM investment.auditactionaccountingactivity a
				ORDER BY a.activity_id, a.requested_at DESC
			)
			SELECT
				m.mtm_id,
				m.activity_id,
				act.activity_type,
				TO_CHAR(NULLIF(act.effective_date::text, '')::date, 'YYYY-MM-DD') AS effective_date,
				COALESCE(act.accounting_period::text, '') AS accounting_period,
				act.data_source,
				act.status,
				
				-- Entity enrichment from folio master
				COALESCE(f.entity_name, '') AS entity_name,
				COALESCE(sch.scheme_name, m.scheme_id) AS scheme_name,
				
				m.scheme_id,
				m.old_scheme_id,
				COALESCE(m.folio_id,'') AS folio_id,
				COALESCE(m.old_folio_id,'') AS old_folio_id,
				COALESCE(m.demat_id,'') AS demat_id,
				COALESCE(m.old_demat_id,'') AS old_demat_id,
				m.units,
				COALESCE(m.old_units,0) AS old_units,
				m.prev_nav,
				COALESCE(m.old_prev_nav,0) AS old_prev_nav,
				m.curr_nav,
				COALESCE(m.old_curr_nav,0) AS old_curr_nav,
				TO_CHAR(NULLIF(m.nav_date::text, '')::date, 'YYYY-MM-DD') AS nav_date,
				TO_CHAR(NULLIF(m.old_nav_date::text, '')::date, 'YYYY-MM-DD') AS old_nav_date,
				m.prev_value,
				COALESCE(m.old_prev_value,0) AS old_prev_value,
				m.curr_value,
				COALESCE(m.old_curr_value,0) AS old_curr_value,
				m.unrealized_gain_loss,
				COALESCE(m.old_unrealized_gain_loss,0) AS old_unrealized_gain_loss,
				COALESCE(m.unrealized_gain_loss_pct,0) AS unrealized_gain_loss_pct,
				COALESCE(m.old_unrealized_gain_loss_pct,0) AS old_unrealized_gain_loss_pct,
				m.is_deleted,
				TO_CHAR(NULLIF(m.updated_at::text, '')::timestamp, 'YYYY-MM-DD HH24:MI:SS') AS updated_at,
				
				COALESCE(l.actiontype,'') AS action_type,
				COALESCE(l.processing_status,'') AS processing_status,
				COALESCE(l.action_id::text,'') AS action_id,
				COALESCE(l.requested_by,'') AS audit_requested_by,
				TO_CHAR(NULLIF(l.requested_at::text, '')::timestamp,'YYYY-MM-DD HH24:MI:SS') AS requested_at,
				COALESCE(l.checker_by,'') AS checker_by,
				TO_CHAR(NULLIF(l.checker_at::text, '')::timestamp,'YYYY-MM-DD HH24:MI:SS') AS checker_at,
				COALESCE(l.checker_comment,'') AS checker_comment,
				COALESCE(l.reason,'') AS reason
			FROM investment.accounting_mtm m
			JOIN investment.accounting_activity act ON act.activity_id = m.activity_id
			LEFT JOIN latest_audit l ON l.activity_id = m.activity_id
			LEFT JOIN investment.masterfolio f ON f.folio_id = m.folio_id
			LEFT JOIN investment.masterscheme sch ON sch.scheme_id = m.scheme_id
			WHERE COALESCE(m.is_deleted, false) = false
				AND COALESCE(act.is_deleted, false) = false
			ORDER BY m.updated_at DESC, m.mtm_id;
		`

		rows, err := pgxPool.Query(ctx, q)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		fields := rows.FieldDescriptions()
		out := make([]map[string]interface{}, 0, 1000)
		for rows.Next() {
			vals, _ := rows.Values()
			rec := make(map[string]interface{}, len(fields))
			for i, f := range fields {
				rec[string(f.Name)] = vals[i]
			}
			out = append(out, rec)
		}

		if rows.Err() != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrRowsScanFailed+rows.Err().Error())
			return
		}

		api.RespondWithPayload(w, true, "", out)
	}
}

// ---------------------------
// NEW TWO-PHASE MTM ENDPOINTS
// ---------------------------

// MTMPreviewRecord represents a single MTM calculation preview
type MTMPreviewRecord struct {
	SchemeID        string               `json:"scheme_id"`
	SchemeName      string               `json:"scheme_name"`
	ISIN            string               `json:"isin"`
	InternalCode    string               `json:"internal_code"`
	FolioNumber     string               `json:"folio_number"`
	DematAccNumber  string               `json:"demat_acc_number"`
	Units           float64              `json:"units"`
	AvgNAV          float64              `json:"avg_nav"`
	PrevNAV         float64              `json:"prev_nav"`
	CurrNAV         float64              `json:"curr_nav"`
	NAVDate         string               `json:"nav_date"`
	PrevValue       float64              `json:"prev_value"`
	CurrValue       float64              `json:"curr_value"`
	UnrealizedGL    float64              `json:"unrealized_gain_loss"`
	UnrealizedGLPct float64              `json:"unrealized_gain_loss_pct"`
	NAVSource       string               `json:"nav_source"` // "MFapi" or "Local"
	Success         bool                 `json:"success"`
	Error           string               `json:"error,omitempty"`
	HistoricalNAVs  []HistoricalNAV      `json:"historical_navs,omitempty"`
	Transactions    []TransactionSummary `json:"transactions,omitempty"`
}

// HistoricalNAV represents historical NAV data point
type HistoricalNAV struct {
	Date string  `json:"date"`
	NAV  float64 `json:"nav"`
}

// TransactionSummary represents a single transaction that contributes to a holding
type TransactionSummary struct {
	TransactionDate string  `json:"transaction_date"`
	TransactionType string  `json:"transaction_type"`
	NAV             float64 `json:"nav"`
	Units           float64 `json:"units"`
	Amount          float64 `json:"amount"`
	Reference       string  `json:"reference,omitempty"`
}

// MTMCommitRecord represents the payload for committing MTM records
type MTMCommitRecord struct {
	SchemeID        string  `json:"scheme_id"`
	FolioNumber     string  `json:"folio_number,omitempty"`
	DematAccNumber  string  `json:"demat_acc_number,omitempty"`
	Units           float64 `json:"units"`
	PrevNAV         float64 `json:"prev_nav"`
	CurrNAV         float64 `json:"curr_nav"`
	NAVDate         string  `json:"nav_date"`
	PrevValue       float64 `json:"prev_value"`
	CurrValue       float64 `json:"curr_value"`
	UnrealizedGL    float64 `json:"unrealized_gain_loss"`
	UnrealizedGLPct float64 `json:"unrealized_gain_loss_pct"`
}

// ---------------------------
// PreviewMTMBulk - Phase 1: Calculate and preview MTM data
// ---------------------------
func PreviewMTMBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID              string `json:"user_id"`
			AccountingPeriod    string `json:"accounting_period"`    // Text format: "JAN 2025", "FEB 2025", etc. or "YYYY-MM"
			IncludeHistory      bool   `json:"include_history"`      // Whether to fetch historical NAV data
			IncludeTransactions bool   `json:"include_transactions"` // Whether to include underlying transactions per holding
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}

		// Validate required fields
		if strings.TrimSpace(req.AccountingPeriod) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "accounting_period is required (e.g., 'JAN 2025' or '2025-01')")
			return
		}

		// Parse accounting period - supports two formats:
		// 1. "JAN 2025" or "JANUARY 2025" (text format)
		// 2. "2025-01" (YYYY-MM format)
		var monthNum, year string
		accountingPeriod := strings.TrimSpace(req.AccountingPeriod)

		// Check if it's in YYYY-MM format
		if strings.Contains(accountingPeriod, "-") && len(accountingPeriod) == 7 {
			// Format: "2025-01"
			parts := strings.Split(accountingPeriod, "-")
			if len(parts) != 2 {
				api.RespondWithError(w, http.StatusBadRequest, "accounting_period format should be 'YYYY-MM' or 'MONTH YYYY' (e.g., '2025-01' or 'JAN 2025')")
				return
			}
			year = parts[0]
			monthNum = parts[1]

			// Validate month is between 01-12
			monthInt := 0
			fmt.Sscanf(monthNum, "%d", &monthInt)
			if monthInt < 1 || monthInt > 12 {
				api.RespondWithError(w, http.StatusBadRequest, "Invalid month in accounting_period (must be 01-12)")
				return
			}
		} else {
			// Text format: "JAN 2025" or "JANUARY 2025"
			monthMap := map[string]string{
				"JAN": "01", "JANUARY": "01",
				"FEB": "02", "FEBRUARY": "02",
				"MAR": "03", "MARCH": "03",
				"APR": "04", "APRIL": "04",
				"MAY": "05",
				"JUN": "06", "JUNE": "06",
				"JUL": "07", "JULY": "07",
				"AUG": "08", "AUGUST": "08",
				"SEP": "09", "SEPTEMBER": "09",
				"OCT": "10", "OCTOBER": "10",
				"NOV": "11", "NOVEMBER": "11",
				"DEC": "12", "DECEMBER": "12",
			}

			parts := strings.Fields(strings.ToUpper(accountingPeriod))
			if len(parts) != 2 {
				api.RespondWithError(w, http.StatusBadRequest, "accounting_period format should be 'MONTH YYYY' or 'YYYY-MM' (e.g., 'JAN 2025' or '2025-01')")
				return
			}

			var ok bool
			monthNum, ok = monthMap[parts[0]]
			if !ok {
				api.RespondWithError(w, http.StatusBadRequest, "Invalid month name in accounting_period")
				return
			}
			year = parts[1]
		}

		// Construct start and end dates for the month
		startDate := fmt.Sprintf("%s-%s-01", year, monthNum)

		// Calculate last day of month
		var endDate string
		switch monthNum {
		case "01", "03", "05", "07", "08", "10", "12":
			endDate = fmt.Sprintf("%s-%s-31", year, monthNum)
		case "04", "06", "09", "11":
			endDate = fmt.Sprintf("%s-%s-30", year, monthNum)
		case "02":
			// Leap year check
			yearInt := 0
			fmt.Sscanf(year, "%d", &yearInt)
			if yearInt%4 == 0 && (yearInt%100 != 0 || yearInt%400 == 0) {
				endDate = fmt.Sprintf("%s-02-29", year)
			} else {
				endDate = fmt.Sprintf("%s-02-28", year)
			}
		}

		// Validate not future month
		currentTime := time.Now()
		requestMonth, _ := time.Parse(constants.DateFormatYearMonth, fmt.Sprintf("%s-%s", year, monthNum))
		if requestMonth.After(currentTime) {
			api.RespondWithError(w, http.StatusBadRequest, fmt.Sprintf("Cannot process future month '%s'. Current month is %s", req.AccountingPeriod, currentTime.Format(constants.DateFormatYearMonth)))
			return
		}

		ctx := r.Context()

		// Fetch holdings from transaction history (read-only, no transaction needed)
		holdingsQuery := `
			WITH ot AS (
				SELECT
					t.*,
					ms.scheme_name AS ms_scheme_name, 
					ms.isin AS ms_isin, 
					ms.scheme_id AS ms_scheme_id, 
					ms.internal_scheme_code AS ms_internal_code
				FROM investment.onboard_transaction t
				LEFT JOIN investment.masterscheme ms ON (
					ms.scheme_id = t.scheme_id 
					OR ms.internal_scheme_code = t.scheme_internal_code 
					OR ms.isin = t.scheme_id
				)
				WHERE t.transaction_date <= $1::date
				  AND LOWER(COALESCE(t.transaction_type,'')) IN ('buy','purchase','subscription','sell','redemption')
			)
			SELECT
				COALESCE(ot.folio_number,'') AS folio_number,
				COALESCE(ot.demat_acc_number,'') AS demat_acc_number,
				COALESCE(ot.scheme_id, ot.scheme_internal_code, ot.ms_scheme_id) AS scheme_id,
				COALESCE(ot.ms_scheme_name, ot.scheme_internal_code) AS scheme_name,
				COALESCE(ot.ms_isin,'') AS isin,
				COALESCE(ot.ms_internal_code,'') AS internal_code,
				SUM(CASE 
					WHEN LOWER(COALESCE(ot.transaction_type,'')) IN ('buy','purchase','subscription') 
					THEN COALESCE(ot.units,0) 
					ELSE 0 
				END) - SUM(CASE 
					WHEN LOWER(COALESCE(ot.transaction_type,'')) IN ('sell','redemption') 
					THEN ABS(COALESCE(ot.units,0)) 
					ELSE 0 
				END) AS total_units,
				CASE 
					WHEN SUM(CASE 
						WHEN LOWER(COALESCE(ot.transaction_type,'')) IN ('buy','purchase','subscription') 
						THEN COALESCE(ot.units,0) 
						ELSE 0 
					END) = 0 THEN 0
					ELSE SUM(CASE 
						WHEN LOWER(COALESCE(ot.transaction_type,'')) IN ('buy','purchase','subscription') 
						THEN COALESCE(ot.nav,0) * COALESCE(ot.units,0) 
						ELSE 0 
					END) / SUM(CASE 
						WHEN LOWER(COALESCE(ot.transaction_type,'')) IN ('buy','purchase','subscription') 
						THEN COALESCE(ot.units,0) 
						ELSE 0 
					END)
				END AS avg_nav
			FROM ot
			GROUP BY 
				COALESCE(ot.folio_number,''), 
				COALESCE(ot.demat_acc_number,''), 
				COALESCE(ot.scheme_id, ot.scheme_internal_code, ot.ms_scheme_id), 
				COALESCE(ot.ms_scheme_name, ot.scheme_internal_code), 
				COALESCE(ot.ms_isin,''),
				COALESCE(ot.ms_internal_code,'')
			HAVING (
				SUM(CASE 
					WHEN LOWER(COALESCE(ot.transaction_type,'')) IN ('buy','purchase','subscription') 
					THEN COALESCE(ot.units,0) 
					ELSE 0 
				END) - SUM(CASE 
					WHEN LOWER(COALESCE(ot.transaction_type,'')) IN ('sell','redemption') 
					THEN ABS(COALESCE(ot.units,0)) 
					ELSE 0 
				END)
			) > 0
			ORDER BY COALESCE(ot.folio_number,''), COALESCE(ot.demat_acc_number,''), COALESCE(ot.scheme_id, ot.scheme_internal_code, ot.ms_scheme_id)
		`

		holdingsRows, err := pgxPool.Query(ctx, holdingsQuery, startDate)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Holdings fetch failed: "+err.Error())
			return
		}
		defer holdingsRows.Close()

		// Collect holdings
		type HoldingData struct {
			FolioNumber    string
			DematAccNumber string
			SchemeID       string
			SchemeName     string
			ISIN           string
			InternalCode   string
			Units          float64
			AvgNAV         float64
		}

		holdings := []HoldingData{}
		for holdingsRows.Next() {
			var h HoldingData
			if err := holdingsRows.Scan(&h.FolioNumber, &h.DematAccNumber, &h.SchemeID, &h.SchemeName, &h.ISIN, &h.InternalCode, &h.Units, &h.AvgNAV); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Holdings scan failed: "+err.Error())
				return
			}
			holdings = append(holdings, h)
		}

		if len(holdings) == 0 {
			// Check if onboard_transaction table has any data at all
			var txCount int
			pgxPool.QueryRow(ctx, "SELECT COUNT(*) FROM investment.onboard_transaction WHERE LOWER(COALESCE(transaction_type,'')) IN ('buy','purchase','subscription')").Scan(&txCount)

			if txCount == 0 {
				api.RespondWithError(w, http.StatusBadRequest, fmt.Sprintf("No investment transactions found. Please upload transaction data before running MTM for %s", req.AccountingPeriod))
			} else {
				api.RespondWithError(w, http.StatusBadRequest, fmt.Sprintf("No active holdings found for accounting period %s (start date: %s). Found %d total buy transactions but all were after this date or fully redeemed.", req.AccountingPeriod, startDate, txCount))
			}
			return
		}

		previews := make([]MTMPreviewRecord, 0, len(holdings))

		// Calculate MTM for each holding
		for _, h := range holdings {
			preview := MTMPreviewRecord{
				SchemeID:       h.SchemeID,
				SchemeName:     h.SchemeName,
				ISIN:           h.ISIN,
				InternalCode:   h.InternalCode,
				FolioNumber:    h.FolioNumber,
				DematAccNumber: h.DematAccNumber,
				Units:          h.Units,
				AvgNAV:         h.AvgNAV,
				PrevNAV:        h.AvgNAV, // Cost basis
				Success:        false,
			}

			// Validate prev_nav
			if preview.PrevNAV <= 0 {
				preview.Error = "Invalid avg_nav (prev_nav <= 0)"
				previews = append(previews, preview)
				continue
			}

			// Try to fetch current NAV and optionally historical NAVs
			var amfiCode string
			if err := pgxPool.QueryRow(ctx, `
				SELECT COALESCE(amfi_scheme_code, '') FROM investment.masterscheme 
				WHERE scheme_id = $1 OR isin = $2 OR internal_scheme_code = $3 
				LIMIT 1
			`, h.SchemeID, h.ISIN, h.InternalCode).Scan(&amfiCode); err != nil || amfiCode == "" {
				amfiCode = h.InternalCode
			}

			var endNAV float64
			var actualEndDate string
			var navSource string
			var historicalNAVs []HistoricalNAV

			if amfiCode != "" {
				// Call MFapi.in to get NAV data
				apiURL := fmt.Sprintf("https://api.mfapi.in/mf/%s", amfiCode)

				resp, err := http.Get(apiURL)
				if err == nil && resp.StatusCode == 200 {
					defer resp.Body.Close()
					body, _ := io.ReadAll(resp.Body)

					var apiResp struct {
						Meta struct {
							SchemeName string `json:"scheme_name"`
						} `json:"meta"`
						Data []struct {
							Date string `json:"date"`
							NAV  string `json:"nav"`
						} `json:"data"`
						Status string `json:"status"`
					}

					if err := json.Unmarshal(body, &apiResp); err == nil && len(apiResp.Data) > 0 {
						navSource = "MFapi"

						// Get the latest NAV from the response (first entry is latest)
						latestEntry := apiResp.Data[0]
						actualEndDate = latestEntry.Date
						if navVal, err := strconv.ParseFloat(latestEntry.NAV, 64); err == nil {
							endNAV = navVal
						}

						// If include_history is true, collect historical NAV data for the accounting period
						if req.IncludeHistory {
							historicalNAVs = make([]HistoricalNAV, 0)
							startParsed, _ := time.Parse(constants.DateFormat, startDate)
							endParsed, _ := time.Parse(constants.DateFormat, endDate)

							for _, entry := range apiResp.Data {
								entryDate, err := time.Parse("02-01-2006", entry.Date) // MFapi format: DD-MM-YYYY
								if err != nil {
									continue
								}

								// Include NAVs within the accounting period
								if (entryDate.Equal(startParsed) || entryDate.After(startParsed)) &&
									(entryDate.Equal(endParsed) || entryDate.Before(endParsed)) {
									if navVal, err := strconv.ParseFloat(entry.NAV, 64); err == nil {
										historicalNAVs = append(historicalNAVs, HistoricalNAV{
											Date: entryDate.Format(constants.DateFormat),
											NAV:  navVal,
										})
									}
								}
							}
						}
					} else {
						preview.Error = "No NAV data found in MFapi response"
					}
				} else if resp != nil {
					preview.Error = fmt.Sprintf("MFapi returned status %d", resp.StatusCode)
				} else {
					preview.Error = "Failed to fetch from MFapi: " + err.Error()
				}
			} else {
				preview.Error = "No AMFI code found for scheme"
			}

			// If MFapi failed, try local amfi_nav_staging as fallback
			if endNAV == 0 {
				var localNAV float64
				if err := pgxPool.QueryRow(ctx, `
					SELECT COALESCE(nav_value, 0) 
					FROM investment.amfi_nav_staging 
					WHERE scheme_code = $1 
						AND nav_date BETWEEN $2::date AND $3::date
					ORDER BY nav_date DESC 
					LIMIT 1
				`, h.SchemeID, startDate, endDate).Scan(&localNAV); err == nil && localNAV > 0 {
					endNAV = localNAV
					actualEndDate = endDate
					navSource = "Local"
					preview.Error = "" // Clear previous error

					// Fetch historical NAVs from local if requested
					if req.IncludeHistory {
						localHistRows, err := pgxPool.Query(ctx, `
							SELECT nav_date, nav_value
							FROM investment.amfi_nav_staging
							WHERE scheme_code = $1
								AND nav_date BETWEEN $2::date AND $3::date
							ORDER BY nav_date DESC
						`, h.SchemeID, startDate, endDate)
						if err == nil {
							defer localHistRows.Close()
							historicalNAVs = make([]HistoricalNAV, 0)
							for localHistRows.Next() {
								var navDate time.Time
								var navValue float64
								if err := localHistRows.Scan(&navDate, &navValue); err == nil {
									historicalNAVs = append(historicalNAVs, HistoricalNAV{
										Date: navDate.Format(constants.DateFormat),
										NAV:  navValue,
									})
								}
							}
						}
					}
				}
			}

			// If still no end NAV, record error and skip
			if endNAV == 0 {
				if preview.Error == "" {
					preview.Error = "Unable to fetch end NAV from any source"
				}
				previews = append(previews, preview)
				continue
			}

			// Calculate MTM values
			preview.CurrNAV = endNAV
			preview.NAVDate = actualEndDate
			preview.NAVSource = navSource
			preview.PrevValue = h.Units * preview.PrevNAV
			preview.CurrValue = h.Units * preview.CurrNAV
			preview.UnrealizedGL = preview.CurrValue - preview.PrevValue
			if preview.PrevValue > 0 {
				preview.UnrealizedGLPct = (preview.UnrealizedGL / preview.PrevValue) * 100
			}
			// Optionally fetch underlying transactions that contributed to this holding
			if req.IncludeTransactions {
				txRows, err := pgxPool.Query(ctx, `
					SELECT transaction_date, COALESCE(transaction_type,''), COALESCE(nav,0), COALESCE(units,0), COALESCE(amount,0)
					FROM investment.onboard_transaction
					WHERE (scheme_id = $1 OR scheme_internal_code = $1 OR scheme_id = $1)
					  AND COALESCE(folio_number,'') = $2
					  AND COALESCE(demat_acc_number,'') = $3
					  AND transaction_date <= $4::date
					  AND LOWER(COALESCE(transaction_type,'')) NOT IN ('sell','redemption')
					ORDER BY transaction_date ASC
				`, h.SchemeID, h.FolioNumber, h.DematAccNumber, endDate)
				if err == nil {
					defer txRows.Close()
					txs := make([]TransactionSummary, 0)
					for txRows.Next() {
						var td time.Time
						var ttype string
						var tnav, tunits, tamt float64
						if err := txRows.Scan(&td, &ttype, &tnav, &tunits, &tamt); err == nil {
							txs = append(txs, TransactionSummary{
								TransactionDate: td.Format(constants.DateFormat),
								TransactionType: ttype,
								NAV:             tnav,
								Units:           tunits,
								Amount:          tunits * tnav,
							})
						}
					}
					preview.Transactions = txs
				} else {
					log.Printf("PreviewMTMBulk: transactions query failed for scheme=%s folio=%s demat=%s: %v", h.SchemeID, h.FolioNumber, h.DematAccNumber, err)
				}
			}
			preview.Success = true
			preview.HistoricalNAVs = historicalNAVs

			previews = append(previews, preview)
		}

		// Calculate summary statistics
		successCount := 0
		failureCount := 0
		totalUnrealizedGL := 0.0
		for _, p := range previews {
			if p.Success {
				successCount++
				totalUnrealizedGL += p.UnrealizedGL
			} else {
				failureCount++
			}
		}

		api.RespondWithPayload(w, true, "", map[string]any{
			"accounting_period":   req.AccountingPeriod,
			"start_date":          startDate,
			"end_date":            endDate,
			"holdings_found":      len(holdings),
			"success_count":       successCount,
			"failure_count":       failureCount,
			"total_unrealized_gl": totalUnrealizedGL,
			"include_history":     req.IncludeHistory,
			"records":             previews,
		})
	}
}

// ---------------------------
// CommitMTMBulk - Phase 2: Commit the MTM records to database
// ---------------------------
func CommitMTMBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID           string            `json:"user_id"`
			ActivityType     string            `json:"activity_type"`     // "MTM"
			EffectiveDate    string            `json:"effective_date"`    // YYYY-MM-DD
			AccountingPeriod string            `json:"accounting_period"` // Text format: "JAN 2025", "FEB 2025", etc.
			DataSource       string            `json:"data_source"`       // "Auto" or "Manual"
			Records          []MTMCommitRecord `json:"records"`           // Pre-calculated MTM records from preview
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}

		// Validate required fields
		if strings.TrimSpace(req.AccountingPeriod) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "accounting_period is required")
			return
		}
		if strings.TrimSpace(req.EffectiveDate) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "effective_date is required (YYYY-MM-DD)")
			return
		}
		if len(req.Records) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "records array is required and cannot be empty")
			return
		}

		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Name
				break
			}
		}
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, "Invalid user session")
			return
		}

		ctx := r.Context()

		// Single transaction for entire batch
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxBeginFailedCapitalized+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		// Create main activity
		activityType := "MTM"
		if strings.TrimSpace(req.ActivityType) != "" {
			activityType = req.ActivityType
		}

		dataSource := "Auto"
		if strings.TrimSpace(req.DataSource) != "" {
			dataSource = req.DataSource
		}

		var activityID string
		if err := tx.QueryRow(ctx, `
			INSERT INTO investment.accounting_activity (
				activity_type, effective_date, accounting_period, data_source, status
			) VALUES ($1, $2, $3, $4, 'DRAFT')
			RETURNING activity_id
		`, activityType, req.EffectiveDate, req.AccountingPeriod, dataSource).Scan(&activityID); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrActivityInsertFailed+err.Error())
			return
		}

		results := make([]map[string]interface{}, 0, len(req.Records))

		// Insert each MTM record
		for _, record := range req.Records {
			// Validate record
			if record.Units <= 0 || record.PrevNAV <= 0 || record.CurrNAV <= 0 {
				results = append(results, map[string]interface{}{
					"success":   false,
					"scheme_id": record.SchemeID,
					"error":     "Invalid record: units, prev_nav, and curr_nav must be greater than 0",
				})
				continue
			}

			// Lookup folio_id and demat_id from master tables
			var folioID, dematID *string
			if record.FolioNumber != "" {
				var fid string
				if err := tx.QueryRow(ctx, `SELECT folio_id FROM investment.masterfolio WHERE folio_number = $1 LIMIT 1`, record.FolioNumber).Scan(&fid); err == nil {
					folioID = &fid
				}
			}
			if record.DematAccNumber != "" {
				var did string
				if err := tx.QueryRow(ctx, `SELECT demat_id FROM investment.masterdemataccount WHERE demat_account_number = $1 LIMIT 1`, record.DematAccNumber).Scan(&did); err == nil {
					dematID = &did
				}
			}

			var mtmID int64
			if err := tx.QueryRow(ctx, `
				INSERT INTO investment.accounting_mtm (
					activity_id, scheme_id, folio_id, demat_id, units, prev_nav, curr_nav, nav_date,
					prev_value, curr_value, unrealized_gain_loss, unrealized_gain_loss_pct
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
				RETURNING mtm_id
			`, activityID, record.SchemeID, folioID, dematID,
				record.Units, record.PrevNAV, record.CurrNAV, record.NAVDate,
				record.PrevValue, record.CurrValue, record.UnrealizedGL, record.UnrealizedGLPct).Scan(&mtmID); err != nil {
				results = append(results, map[string]interface{}{
					"success":   false,
					"scheme_id": record.SchemeID,
					"error":     constants.ErrMTMInsertFailed + err.Error(),
				})
				continue
			}

			results = append(results, map[string]interface{}{
				"success":              true,
				"mtm_id":               mtmID,
				"scheme_id":            record.SchemeID,
				"units":                record.Units,
				"prev_nav":             record.PrevNAV,
				"curr_nav":             record.CurrNAV,
				"unrealized_gain_loss": record.UnrealizedGL,
			})
		}

		// Create single audit trail for batch
		if _, err := tx.Exec(ctx, `
			INSERT INTO investment.auditactionaccountingactivity (activity_id, actiontype, processing_status, requested_by, requested_at)
			VALUES ($1, 'CREATE', 'PENDING_APPROVAL', $2, now())
		`, activityID, userEmail); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrAuditInsertFailed+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailedCapitalized+err.Error())
			return
		}

		// Calculate summary
		successCount := 0
		for _, r := range results {
			if success, ok := r["success"].(bool); ok && success {
				successCount++
			}
		}

		api.RespondWithPayload(w, api.IsBulkSuccess(results), "", map[string]any{
			"activity_id":       activityID,
			"accounting_period": req.AccountingPeriod,
			"records_submitted": len(req.Records),
			"records_inserted":  successCount,
			"records_failed":    len(req.Records) - successCount,
			"mtm_records":       results,
		})
	}
}
