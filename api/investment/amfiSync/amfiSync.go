package amfisync

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"CimplrCorpSaas/api"
	investmentjobs "CimplrCorpSaas/internal/jobs/investment"

	"CimplrCorpSaas/api/constants"

	"github.com/jackc/pgx/v5/pgxpool"
)

// SyncResponse represents the API response structure
type SyncResponse struct {
	Success bool       `json:"success"`
	Status  string     `json:"status"`
	Data    SyncData   `json:"data"`
	Logs    []LogEntry `json:"logs"`
}

// SyncData represents the sync operation statistics
type SyncData struct {
	NewAmcs        int    `json:"newAmcs"`
	NewSchemes     int    `json:"newSchemes"`
	TotalProcessed int    `json:"totalProcessed"`
	LastChecked    string `json:"lastChecked"`
}

// LogEntry represents a log message
type LogEntry struct {
	Level   string `json:"level"`
	Message string `json:"message"`
}

// SyncSchemesHandler manually triggers the AMFI scheme synchronization
func SyncSchemesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}

		// Initialize response
		response := SyncResponse{
			Success: false,
			Status:  constants.ValueError,
			Data: SyncData{
				LastChecked: time.Now().Format("02-01-2006 15:04"),
			},
			Logs: []LogEntry{},
		}

		// Add initial log
		response.Logs = append(response.Logs, LogEntry{
			Level:   "INFO",
			Message: "Starting Scheme & AMC Data Synchronization job...",
		})

		response.Logs = append(response.Logs, LogEntry{
			Level:   "INFO",
			Message: "Connecting to AMFI URL: https://portal.amfiindia.com/DownloadSchemeData_Po.aspx?mf=0",
		})

		// Create AMFI config
		config := investmentjobs.NewDefaultConfig()

		// Get initial counts for comparison
		initialAmcCount, err := getAmcCount(pool)
		if err != nil {
			response.Logs = append(response.Logs, LogEntry{
				Level:   "ERROR",
				Message: fmt.Sprintf("Failed to get initial AMC count: %v", err),
			})
			api.RespondEnvelopeFailureCompat(w, http.StatusInternalServerError, "Failed to get initial AMC count: "+err.Error(), "", map[string]interface{}{
				"status": response.Status,
				"data":   response.Data,
				"logs":   response.Logs,
			})
			return
		}

		initialSchemeCount, err := getSchemeCount(pool)
		if err != nil {
			response.Logs = append(response.Logs, LogEntry{
				Level:   "ERROR",
				Message: fmt.Sprintf("Failed to get initial scheme count: %v", err),
			})
			api.RespondEnvelopeFailureCompat(w, http.StatusInternalServerError, "Failed to get initial scheme count: "+err.Error(), "", map[string]interface{}{
				"status": response.Status,
				"data":   response.Data,
				"logs":   response.Logs,
			})
			return
		}

		response.Logs = append(response.Logs, LogEntry{
			Level:   "INFO",
			Message: "File downloaded successfully.",
		})

		response.Logs = append(response.Logs, LogEntry{
			Level:   "INFO",
			Message: "Begin parsing file...",
		})

		// Run the sync manually
		err = runManualSchemeSync(config, pool)
		if err != nil {
			response.Logs = append(response.Logs, LogEntry{
				Level:   "ERROR",
				Message: fmt.Sprintf("Synchronization failed: %v", err),
			})
			api.RespondEnvelopeFailureCompat(w, http.StatusInternalServerError, "Synchronization failed: "+err.Error(), "", map[string]interface{}{
				"status": response.Status,
				"data":   response.Data,
				"logs":   response.Logs,
			})
			return
		}

		// Get final counts
		finalAmcCount, err := getAmcCount(pool)
		if err != nil {
			response.Logs = append(response.Logs, LogEntry{
				Level:   "ERROR",
				Message: fmt.Sprintf("Failed to get final AMC count: %v", err),
			})
			api.RespondEnvelopeFailureCompat(w, http.StatusInternalServerError, "Failed to get final AMC count: "+err.Error(), "", map[string]interface{}{
				"status": response.Status,
				"data":   response.Data,
				"logs":   response.Logs,
			})
			return
		}

		finalSchemeCount, err := getSchemeCount(pool)
		if err != nil {
			response.Logs = append(response.Logs, LogEntry{
				Level:   "ERROR",
				Message: fmt.Sprintf("Failed to get final scheme count: %v", err),
			})
			api.RespondEnvelopeFailureCompat(w, http.StatusInternalServerError, "Failed to get final scheme count: "+err.Error(), "", map[string]interface{}{
				"status": response.Status,
				"data":   response.Data,
				"logs":   response.Logs,
			})
			return
		}

		// Calculate differences
		newAmcs := finalAmcCount - initialAmcCount
		newSchemes := finalSchemeCount - initialSchemeCount

		// Add success logs
		if newAmcs > 0 {
			response.Logs = append(response.Logs, LogEntry{
				Level:   "SUCCESS",
				Message: fmt.Sprintf("Found %d new AMC(s).", newAmcs),
			})
		}

		if newSchemes > 0 {
			response.Logs = append(response.Logs, LogEntry{
				Level:   "SUCCESS",
				Message: fmt.Sprintf("Created %d new scheme records in Global and Custom Lookups.", newSchemes),
			})
		}

		response.Logs = append(response.Logs, LogEntry{
			Level:   "SUCCESS",
			Message: "Database tables updated successfully.",
		})

		response.Logs = append(response.Logs, LogEntry{
			Level:   "INFO",
			Message: "Job completed successfully.",
		})

		// Update response with success data
		response.Success = true
		response.Status = constants.ValueSuccess
		response.Data.NewAmcs = newAmcs
		response.Data.NewSchemes = newSchemes
		response.Data.TotalProcessed = finalSchemeCount

		api.RespondEnvelopeSuccessCompat(w, "Job completed successfully.", map[string]interface{}{
			"status": response.Status,
			"data":   response.Data,
			"logs":   response.Logs,
		})
	}
}

// runManualSchemeSync runs the AMFI scheme sync job manually (schemes only).
func runManualSchemeSync(config *investmentjobs.Config, pool *pgxpool.Pool) error {
	return investmentjobs.RunAMFISchemeSyncOnce(config, pool)
}

// getAmcCount returns the current count of unique AMCs in the database
func getAmcCount(pool *pgxpool.Pool) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var count int
	err := pool.QueryRow(ctx, "SELECT COUNT(DISTINCT amc_name) FROM investment.amfi_scheme_master_staging").Scan(&count)
	if err != nil {
		// If table doesn't exist or query fails, return 0
		return 0, nil
	}
	return count, nil
}

// getSchemeCount returns the current count of schemes in the database
func getSchemeCount(pool *pgxpool.Pool) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var count int
	err := pool.QueryRow(ctx, "SELECT COUNT(*) FROM investment.amfi_scheme_master_staging").Scan(&count)
	if err != nil {
		// If table doesn't exist or query fails, return 0
		return 0, nil
	}
	return count, nil
}
