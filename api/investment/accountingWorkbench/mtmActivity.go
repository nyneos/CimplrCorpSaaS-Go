package accountingworkbench

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"encoding/json"
	"fmt"
	"io"
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
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
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
			api.RespondWithError(w, http.StatusUnauthorized, "Invalid session")
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "TX begin failed: "+err.Error())
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
			api.RespondWithError(w, http.StatusInternalServerError, "Activity insert failed: "+err.Error())
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
			api.RespondWithError(w, http.StatusInternalServerError, "MTM insert failed: "+err.Error())
			return
		}

		// Create audit trail
		if _, err := tx.Exec(ctx, `
			INSERT INTO investment.auditactionaccountingactivity (activity_id, actiontype, processing_status, requested_by, requested_at)
			VALUES ($1, 'CREATE', 'PENDING_APPROVAL', $2, now())
		`, activityID, userEmail); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Audit insert failed: "+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Commit failed: "+err.Error())
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
			ActivityType     string `json:"activity_type"` // "MTM"
			EffectiveDate    string `json:"effective_date"`    // YYYY-MM-DD
			AccountingPeriod string `json:"accounting_period"` // Text format: "JAN 2025", "FEB 2025", etc.
			DataSource       string `json:"data_source"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON body")
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
				"JAN": "01", "JANUARY":   "01",
				"FEB": "02", "FEBRUARY":  "02",
				"MAR": "03", "MARCH":     "03",
				"APR": "04", "APRIL":     "04",
				"MAY": "05",
				"JUN": "06", "JUNE":      "06",
				"JUL": "07", "JULY":      "07",
				"AUG": "08", "AUGUST":    "08",
				"SEP": "09", "SEPTEMBER": "09",
				"OCT": "10", "OCTOBER":   "10",
				"NOV": "11", "NOVEMBER":  "11",
				"DEC": "12", "DECEMBER":  "12",
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
		requestMonth, _ := time.Parse("2006-01", fmt.Sprintf("%s-%s", year, monthNum))
		if requestMonth.After(currentTime) {
			api.RespondWithError(w, http.StatusBadRequest, fmt.Sprintf("Cannot process future month '%s'. Current month is %s", req.AccountingPeriod, currentTime.Format("2006-01")))
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
			api.RespondWithError(w, http.StatusInternalServerError, "TX begin failed: "+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		// STEP 1: Fetch all active holdings from portfolio_snapshot as of start of month
		// We need holdings that existed at the beginning of the accounting period
		holdingsQuery := `
			WITH month_start_snapshot AS (
				SELECT DISTINCT ON (entity_name, scheme_id, COALESCE(folio_number,''), COALESCE(demat_acc_number,''))
					entity_name,
					scheme_id,
					folio_number,
					demat_acc_number,
					total_units,
					current_nav,
					created_at
				FROM investment.portfolio_snapshot
				WHERE created_at <= $1::date  -- Start of month
					AND total_units > 0
				ORDER BY entity_name, scheme_id, COALESCE(folio_number,''), COALESCE(demat_acc_number,''), created_at DESC
			)
			SELECT
				ms.scheme_id,
				ms.folio_number,
				ms.demat_acc_number,
				ms.total_units,
				ms.current_nav AS start_nav,
				COALESCE(sch.scheme_name, ms.scheme_id) AS scheme_name,
				COALESCE(sch.isin, '') AS isin,
				COALESCE(sch.internal_scheme_code, '') AS internal_code
			FROM month_start_snapshot ms
			LEFT JOIN investment.masterscheme sch ON sch.scheme_id = ms.scheme_id
			WHERE ms.total_units > 0
			ORDER BY ms.entity_name, ms.scheme_id
		`

		holdingsRows, err := tx.Query(ctx, holdingsQuery, startDate)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Holdings fetch failed: "+err.Error())
			return
		}
		defer holdingsRows.Close()

		// Collect holdings
		type HoldingData struct {
			SchemeID        string
			FolioNumber     *string
			DematAccNumber  *string
			Units           float64
			StartNAV        float64  // NAV at start of month
			SchemeName      string
			ISIN            string
			InternalCode    string
		}

		holdings := []HoldingData{}
		for holdingsRows.Next() {
			var h HoldingData
			if err := holdingsRows.Scan(&h.SchemeID, &h.FolioNumber, &h.DematAccNumber, &h.Units, &h.StartNAV, &h.SchemeName, &h.ISIN, &h.InternalCode); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Holdings scan failed: "+err.Error())
				return
			}
			holdings = append(holdings, h)
		}

		if len(holdings) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No active holdings found for the given accounting_period")
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
			api.RespondWithError(w, http.StatusInternalServerError, "Activity insert failed: "+err.Error())
			return
		}

		results := make([]map[string]interface{}, 0, len(holdings))

		// STEP 2: For each holding, fetch historical NAV and calculate MTM
		for _, h := range holdings {
			// Validate start NAV
			if h.StartNAV <= 0 {
				results = append(results, map[string]interface{}{
					"success":     false,
					"scheme_id":   h.SchemeID,
					"scheme_name": h.SchemeName,
					"error":       "Invalid start NAV (start_nav <= 0)",
				})
			continue
			}

			// Try to fetch end-of-month NAV from MFapi.in using ISIN or internal code
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
						actualEndDate = lastEntry.Date
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
				var localNAV float64
				if err := tx.QueryRow(ctx, `
					SELECT COALESCE(nav_value, 0) 
					FROM investment.amfi_nav_staging 
					WHERE scheme_code = $1 
						AND nav_date BETWEEN $2::date AND $3::date
					ORDER BY nav_date DESC 
					LIMIT 1
				`, h.SchemeID, startDate, endDate).Scan(&localNAV); err == nil && localNAV > 0 {
					endNAV = localNAV
					actualEndDate = endDate
					navFetchError = "" // Clear error, fallback succeeded
				}
			}

			// If still no end NAV, record error and skip
			if endNAV == 0 {
				results = append(results, map[string]interface{}{
					"success":     false,
					"scheme_id":   h.SchemeID,
					"scheme_name": h.SchemeName,
					"error":       fmt.Sprintf("Unable to fetch end NAV: %s", navFetchError),
				})
				continue
			}

			// Calculate MTM values (start of month → end of month)
			prevValue := h.Units * h.StartNAV
			currValue := h.Units * endNAV
			unrealizedGL := currValue - prevValue
			var unrealizedGLPct float64
			if prevValue > 0 {
				unrealizedGLPct = (unrealizedGL / prevValue) * 100
			}

			// Lookup folio_id and demat_id from master tables
			var folioID, dematID *string
			if h.FolioNumber != nil && *h.FolioNumber != "" {
				var fid string
				if err := tx.QueryRow(ctx, `SELECT folio_id FROM investment.masterfolio WHERE folio_number = $1 LIMIT 1`, *h.FolioNumber).Scan(&fid); err == nil {
					folioID = &fid
				}
			}
			if h.DematAccNumber != nil && *h.DematAccNumber != "" {
				var did string
				if err := tx.QueryRow(ctx, `SELECT demat_id FROM investment.masterdemat WHERE demat_acc_number = $1 LIMIT 1`, *h.DematAccNumber).Scan(&did); err == nil {
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
			`, activityID, h.SchemeID, folioID, dematID,
				h.Units, h.StartNAV, endNAV, actualEndDate,
				prevValue, currValue, unrealizedGL, unrealizedGLPct).Scan(&mtmID); err != nil {
				results = append(results, map[string]interface{}{
					"success":     false,
					"scheme_id":   h.SchemeID,
					"scheme_name": h.SchemeName,
					"error":       "MTM insert failed: " + err.Error(),
				})
				continue
			}

			results = append(results, map[string]interface{}{
				"success":               true,
				"mtm_id":                mtmID,
				"scheme_id":             h.SchemeID,
				"scheme_name":           h.SchemeName,
				"units":                 h.Units,
				"start_nav":             h.StartNAV,
				"end_nav":               endNAV,
				"nav_date":              actualEndDate,
				"unrealized_gain_loss": unrealizedGL,
				"unrealized_gain_loss_pct": unrealizedGLPct,
			})
		}

		// Create single audit trail for batch
		if _, err := tx.Exec(ctx, `
			INSERT INTO investment.auditactionaccountingactivity (activity_id, actiontype, processing_status, requested_by, requested_at)
			VALUES ($1, 'CREATE', 'PENDING_APPROVAL', $2, now())
		`, activityID, userEmail); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Audit insert failed: "+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Commit failed: "+err.Error())
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
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
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
			api.RespondWithError(w, http.StatusUnauthorized, "invalid session")
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "tx begin failed: "+err.Error())
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
			api.RespondWithError(w, http.StatusInternalServerError, "audit insert failed: "+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "commit failed: "+err.Error())
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
				TO_CHAR(act.effective_date, 'YYYY-MM-DD') AS effective_date,
				TO_CHAR(act.accounting_period, 'YYYY-MM-DD') AS accounting_period,
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
				TO_CHAR(m.nav_date, 'YYYY-MM-DD') AS nav_date,
				TO_CHAR(m.old_nav_date, 'YYYY-MM-DD') AS old_nav_date,
				m.prev_value,
				COALESCE(m.old_prev_value,0) AS old_prev_value,
				m.curr_value,
				COALESCE(m.old_curr_value,0) AS old_curr_value,
				m.unrealized_gain_loss,
				COALESCE(m.old_unrealized_gain_loss,0) AS old_unrealized_gain_loss,
				COALESCE(m.unrealized_gain_loss_pct,0) AS unrealized_gain_loss_pct,
				COALESCE(m.old_unrealized_gain_loss_pct,0) AS old_unrealized_gain_loss_pct,
				m.is_deleted,
				TO_CHAR(m.updated_at, 'YYYY-MM-DD HH24:MI:SS') AS updated_at,
				
				COALESCE(l.actiontype,'') AS action_type,
				COALESCE(l.processing_status,'') AS processing_status,
				COALESCE(l.action_id::text,'') AS action_id,
				COALESCE(l.requested_by,'') AS audit_requested_by,
				TO_CHAR(l.requested_at,'YYYY-MM-DD HH24:MI:SS') AS requested_at,
				COALESCE(l.checker_by,'') AS checker_by,
				TO_CHAR(l.checker_at,'YYYY-MM-DD HH24:MI:SS') AS checker_at,
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
			api.RespondWithError(w, http.StatusInternalServerError, "query failed: "+err.Error())
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
			api.RespondWithError(w, http.StatusInternalServerError, "rows scan failed: "+rows.Err().Error())
			return
		}

		api.RespondWithPayload(w, true, "", out)
	}
}
