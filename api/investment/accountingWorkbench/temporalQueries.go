package accountingworkbench

import (
	"CimplrCorpSaas/api"
	"encoding/json"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// GetSchemeDataAsOf returns scheme and portfolio data as of a specific date
// This is the KEY HANDLER for historical/temporal queries
//
// Use cases:
// - Generate report for Jan 5, 2026 when scheme was renamed on Jan 10
// - Show portfolio holdings before split was applied
// - Compare multiple snapshots across different dates
//
// Request body:
//
//	{
//	  "scheme_id": "SCH001",
//	  "as_of_date": "2026-01-05"  // Optional, defaults to today
//	}
//
// Response:
//
//	{
//	  "success": true,
//	  "message": "",
//	  "data": {
//	    "as_of_date": "2026-01-05",
//	    "scheme": {
//	      "scheme_id": "SCH001",
//	      "scheme_name": "HDFC Equity Fund",  // OLD NAME before Jan 10 change
//	      "isin": "INF179K01234"
//	    },
//	    "portfolio": [
//	      {
//	        "entity_name": "Entity A",
//	        "folio_number": "FOL001",
//	        "total_units": 1000.000,  // Before split
//	        "avg_nav": 120.50
//	      }
//	    ],
//	    "data_source": "snapshot" | "current"  // Shows if data came from historical snapshot
//	  }
//	}
func GetSchemeDataAsOf(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			SchemeID string `json:"scheme_id"`
			AsOfDate string `json:"as_of_date,omitempty"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON: "+err.Error())
			return
		}

		if req.SchemeID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "scheme_id is required")
			return
		}

		// Default to today if no date specified
		asOfDate := req.AsOfDate
		if asOfDate == "" {
			asOfDate = time.Now().Format("2006-01-02")
		}

		ctx := r.Context()

		// Use the GetDataAsOf helper to retrieve historical or current data
		schemeData, portfolioData, err := GetDataAsOf(ctx, pgxPool, req.SchemeID, asOfDate)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to retrieve data: "+err.Error())
			return
		}

		// Determine if data came from snapshot or current tables
		dataSource := "current"

		// Check if there's a snapshot affecting this date
		var snapshotExists bool
		err = pgxPool.QueryRow(ctx, `
			SELECT EXISTS(
				SELECT 1
				FROM investment.auditactionaccountingactivity aud
				JOIN investment.accounting_activity a ON a.activity_id = aud.activity_id
				WHERE
				 aud.processing_status = 'APPROVED'
				  AND a.effective_date > $1::date
				  AND aud.snapshot_before ? 'masterscheme'
				  AND aud.snapshot_before->'masterscheme' ? $2
			)
		`, asOfDate, req.SchemeID).Scan(&snapshotExists)

		if err == nil && snapshotExists {
			dataSource = "snapshot"
		}

		response := map[string]interface{}{
			"as_of_date":  asOfDate,
			"scheme":      schemeData,
			"portfolio":   portfolioData,
			"data_source": dataSource,
		}

		api.RespondWithPayload(w, true, "", response)
	}
}

// GetMTMReportAsOf generates MTM report as of a specific date
// This demonstrates how to use snapshots in real reports
//
// Request: POST /api/accounting/mtm-report-as-of
//
//	{
//	  "as_of_date": "2026-01-05",
//	  "entity_name": "Entity A"  // Optional filter
//	}
func GetMTMReportAsOf(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			AsOfDate   string `json:"as_of_date"`
			EntityName string `json:"entity_name,omitempty"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON: "+err.Error())
			return
		}

		if req.AsOfDate == "" {
			api.RespondWithError(w, http.StatusBadRequest, "as_of_date is required")
			return
		}

		ctx := r.Context()

		// Get all schemes
		rows, err := pgxPool.Query(ctx, `SELECT scheme_id FROM investment.masterscheme`)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to fetch schemes: "+err.Error())
			return
		}
		defer rows.Close()

		var schemeIDs []string
		for rows.Next() {
			var schemeID string
			if err := rows.Scan(&schemeID); err == nil {
				schemeIDs = append(schemeIDs, schemeID)
			}
		}

		// Build MTM report using historical data
		var reportData []map[string]interface{}

		for _, schemeID := range schemeIDs {
			schemeData, portfolioData, err := GetDataAsOf(ctx, pgxPool, schemeID, req.AsOfDate)
			if err != nil {
				continue
			}

			// Filter by entity if specified
			for _, portfolio := range *portfolioData {
				if req.EntityName != "" && portfolio.EntityName != req.EntityName {
					continue
				}

				reportData = append(reportData, map[string]interface{}{
					"scheme_id":        schemeData.SchemeID,
					"scheme_name":      schemeData.SchemeName, // Historical name
					"isin":             schemeData.ISIN,
					"entity_name":      portfolio.EntityName,
					"folio_number":     portfolio.FolioNumber,
					"total_units":      portfolio.TotalUnits, // Historical units
					"avg_nav":          portfolio.AvgNAV,     // Historical cost
					"total_cost_basis": portfolio.TotalCostBasis,
				})
			}
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"as_of_date":   req.AsOfDate,
			"report_data":  reportData,
			"record_count": len(reportData),
		})
	}
}

// Example: How to query multiple effective dates (timeline view)
// This shows how to handle multiple approved activities with different effective_dates
//
// SQL Query Pattern:
// SELECT
//   a.activity_id,
//   a.effective_date,
//   aud.snapshot_before->'masterscheme'->'SCH001'->>'scheme_name' as scheme_name_before,
//   ca.action_type
// FROM investment.auditactionaccountingactivity aud
// JOIN investment.accounting_activity a ON a.activity_id = aud.activity_id
// LEFT JOIN investment.accounting_corporate_action ca ON ca.activity_id = a.activity_id
// WHERE aud.actiontype = 'APPROVE'
//   AND aud.processing_status = 'APPROVED'
//   AND aud.snapshot_before ? 'masterscheme'
//   AND aud.snapshot_before->'masterscheme' ? 'SCH001'
// ORDER BY a.effective_date;
//
// This returns ALL changes to SCH001 over time:
// - Jan 10: Split 2:1 (scheme_name_before = "HDFC Equity Fund")
// - Jan 15: Name change (scheme_name_before = "HDFC Equity Fund")
// - Jan 20: Bonus 1:2 (scheme_name_before = "HDFC Equity Growth Fund")
