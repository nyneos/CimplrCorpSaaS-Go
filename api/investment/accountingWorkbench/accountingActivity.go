package accountingworkbench

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	// "time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ---------------------------
// Journal Entry Generator Wrapper
// ---------------------------

// GenerateAndSaveJournalEntries fetches activity details and generates journal entries (standalone)
func GenerateAndSaveJournalEntries(ctx context.Context, pool *pgxpool.Pool, activityID string) error {
	// Get activity details
	var activityType, activitySubtype string
	err := pool.QueryRow(ctx, `
		SELECT activity_type, activity_subtype
		FROM investment.accounting_activity
		WHERE activity_id = $1 AND status = 'APPROVED'
	`, activityID).Scan(&activityType, &activitySubtype)
	
	if err != nil {
		return fmt.Errorf("failed to fetch activity: %w", err)
	}

	// Get cached settings
	settings := GetCachedSettings()

	// Route to appropriate generator based on activity_type
	switch activityType {
	case "MTM":
		return generateMTMJournal(ctx, pool, settings, activityID)
	case "DIVIDEND":
		return generateDividendJournal(ctx, pool, settings, activityID)
	case "CORPORATE_ACTION":
		return generateCorporateActionJournal(ctx, pool, settings, activityID, activitySubtype)
	case "FVO":
		return generateFVOJournal(ctx, pool, settings, activityID)
	default:
		return fmt.Errorf("unsupported activity type: %s", activityType)
	}
}

// GenerateAndSaveJournalEntriesInTx generates journal entries within an existing transaction
// Used during approval to ensure atomicity - if journal generation fails, approval rolls back
func GenerateAndSaveJournalEntriesInTx(ctx context.Context, tx DBExecutor, pool *pgxpool.Pool, activityID string) error {
	// Get activity details
	var activityType, activitySubtype string
	err := tx.QueryRow(ctx, `
		SELECT activity_type, activity_subtype
		FROM investment.accounting_activity
		WHERE activity_id = $1 AND status = 'APPROVED'
	`, activityID).Scan(&activityType, &activitySubtype)
	
	if err != nil {
		return fmt.Errorf("failed to fetch activity: %w", err)
	}

	// Get cached settings
	settings := GetCachedSettings()

	// Fetch and generate journal entries based on activity type
	switch activityType {
	case "MTM":
		return generateMTMJournalInTx(ctx, tx, pool, settings, activityID)
	case "DIVIDEND":
		return generateDividendJournalInTx(ctx, tx, pool, settings, activityID)
	case "CORPORATE_ACTION":
		return generateCorporateActionJournalInTx(ctx, tx, pool, settings, activityID, activitySubtype)
	case "FVO":
		return generateFVOJournalInTx(ctx, tx, pool, settings, activityID)
	default:
		return fmt.Errorf("unsupported activity type: %s", activityType)
	}
}

// generateMTMJournal generates journal entries for MTM activities
func generateMTMJournal(ctx context.Context, pool *pgxpool.Pool, settings *SettingsCache, activityID string) error {
	// Start transaction for journal entry generation
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction failed: %w", err)
	}
	defer tx.Rollback(ctx)

	// Fetch MTM data
	rows, err := tx.Query(ctx, `
		SELECT m.mtm_id, m.scheme_id, m.folio_id, m.demat_id, m.market_nav,
		       m.cost_nav, m.total_units, m.unrealized_gain_loss, m.entity_id
		FROM investment.accounting_mtm m
		WHERE m.activity_id = $1
	`, activityID)
	if err != nil {
		return fmt.Errorf("fetch MTM data failed: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var mtmID, schemeID, entityID string
		var folioID, dematID *string
		var marketNav, costNav, totalUnits, unrealizedGL float64
		
		if err := rows.Scan(&mtmID, &schemeID, &folioID, &dematID, &marketNav, &costNav, &totalUnits, &unrealizedGL, &entityID); err != nil {
			return fmt.Errorf("scan MTM data failed: %w", err)
		}

		// Prepare data map
		data := map[string]interface{}{
			"mtm_id":                mtmID,
			"scheme_id":             schemeID,
			"folio_id":              folioID,
			"demat_id":              dematID,
			"market_nav":            marketNav,
			"cost_nav":              costNav,
			"total_units":           totalUnits,
			"unrealized_gain_loss":  unrealizedGL,
			"entity_id":             entityID,
		}

		// Generate journal entry
		je, err := GenerateJournalEntryForMTM(ctx, pool, settings, activityID, data)
		if err != nil {
			return fmt.Errorf("generate MTM journal failed: %w", err)
		}

		// Save to database
		if err := SaveJournalEntry(ctx, tx, je); err != nil {
			return fmt.Errorf("save MTM journal failed: %w", err)
		}
	}

	if err := rows.Err(); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// generateDividendJournal generates journal entries for Dividend activities
func generateDividendJournal(ctx context.Context, pool *pgxpool.Pool, settings *SettingsCache, activityID string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction failed: %w", err)
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		SELECT d.dividend_id, d.scheme_id, d.folio_id, d.dividend_amount,
		       d.reinvest_units, d.transaction_type, d.entity_id
		FROM investment.accounting_dividend d
		WHERE d.activity_id = $1
	`, activityID)
	if err != nil {
		return fmt.Errorf("fetch Dividend data failed: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var dividendID, schemeID, folioID, transactionType, entityID string
		var dividendAmount float64
		var reinvestUnits *float64
		
		if err := rows.Scan(&dividendID, &schemeID, &folioID, &dividendAmount, &reinvestUnits, &transactionType, &entityID); err != nil {
			return fmt.Errorf("scan Dividend data failed: %w", err)
		}

		data := map[string]interface{}{
			"dividend_id":       dividendID,
			"scheme_id":         schemeID,
			"folio_id":          folioID,
			"dividend_amount":   dividendAmount,
			"reinvest_units":    reinvestUnits,
			"transaction_type":  transactionType,
			"entity_id":         entityID,
		}

		je, err := GenerateJournalEntryForDividend(ctx, pool, settings, activityID, data)
		if err != nil {
			return fmt.Errorf("generate Dividend journal failed: %w", err)
		}

		if err := SaveJournalEntry(ctx, tx, je); err != nil {
			return fmt.Errorf("save Dividend journal failed: %w", err)
		}
	}

	if err := rows.Err(); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// generateCorporateActionJournal generates journal entries for Corporate Action activities
func generateCorporateActionJournal(ctx context.Context, pool *pgxpool.Pool, settings *SettingsCache, activityID, subtype string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction failed: %w", err)
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		SELECT ca.corporate_action_id, ca.action_type, ca.source_scheme_id,
		       ca.target_scheme_id, ca.folio_id, ca.exchange_ratio_num,
		       ca.exchange_ratio_denom, ca.source_units_affected, ca.new_units_issued, ca.entity_id
		FROM investment.accounting_corporate_action ca
		WHERE ca.activity_id = $1
	`, activityID)
	if err != nil {
		return fmt.Errorf("fetch Corporate Action data failed: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var caID, actionType, sourceSchemeID, folioID, entityID string
		var targetSchemeID *string
		var ratioNum, ratioDenom, sourceUnits, newUnits float64
		
		if err := rows.Scan(&caID, &actionType, &sourceSchemeID, &targetSchemeID, &folioID, &ratioNum, &ratioDenom, &sourceUnits, &newUnits, &entityID); err != nil {
			return fmt.Errorf("scan Corporate Action data failed: %w", err)
		}

		data := map[string]interface{}{
			"corporate_action_id":     caID,
			"action_type":             actionType,
			"source_scheme_id":        sourceSchemeID,
			"target_scheme_id":        targetSchemeID,
			"folio_id":                folioID,
			"exchange_ratio_num":      ratioNum,
			"exchange_ratio_denom":    ratioDenom,
			"source_units_affected":   sourceUnits,
			"new_units_issued":        newUnits,
			"entity_id":               entityID,
		}

		je, err := GenerateJournalEntryForCorporateAction(ctx, pool, settings, activityID, data)
		if err != nil {
			return fmt.Errorf("generate Corporate Action journal failed: %w", err)
		}

		if err := SaveJournalEntry(ctx, tx, je); err != nil {
			return fmt.Errorf("save Corporate Action journal failed: %w", err)
		}
	}

	if err := rows.Err(); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// generateFVOJournal generates journal entries for FVO activities
func generateFVOJournal(ctx context.Context, pool *pgxpool.Pool, settings *SettingsCache, activityID string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction failed: %w", err)
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		SELECT f.fvo_id, f.scheme_id, f.folio_id, f.demat_id, f.market_nav,
		       f.override_nav, f.variance, f.variance_pct, f.units_affected,
		       f.justification, f.entity_id
		FROM investment.accounting_fvo f
		WHERE f.activity_id = $1
	`, activityID)
	if err != nil {
		return fmt.Errorf("fetch FVO data failed: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var fvoID, schemeID, justification, entityID string
		var folioID, dematID *string
		var marketNav, overrideNav, variance, variancePct, unitsAffected float64
		
		if err := rows.Scan(&fvoID, &schemeID, &folioID, &dematID, &marketNav, &overrideNav, &variance, &variancePct, &unitsAffected, &justification, &entityID); err != nil {
			return fmt.Errorf("scan FVO data failed: %w", err)
		}

		data := map[string]interface{}{
			"fvo_id":          fvoID,
			"scheme_id":       schemeID,
			"folio_id":        folioID,
			"demat_id":        dematID,
			"market_nav":      marketNav,
			"override_nav":    overrideNav,
			"variance":        variance,
			"variance_pct":    variancePct,
			"units_affected":  unitsAffected,
			"justification":   justification,
			"entity_id":       entityID,
		}

		je, err := GenerateJournalEntryForFVO(ctx, pool, settings, activityID, data)
		if err != nil {
			return fmt.Errorf("generate FVO journal failed: %w", err)
		}

		if err := SaveJournalEntry(ctx, tx, je); err != nil {
			return fmt.Errorf("save FVO journal failed: %w", err)
		}
	}

	if err := rows.Err(); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// ---------------------------
// Transaction-aware journal generation functions
// These use an existing transaction (for atomic approval + journal generation)
// ---------------------------

// generateMTMJournalInTx generates journal entries for MTM within existing transaction
func generateMTMJournalInTx(ctx context.Context, tx DBExecutor, pool *pgxpool.Pool, settings *SettingsCache, activityID string) error {
	rows, err := tx.Query(ctx, `
		SELECT m.mtm_id, m.scheme_id, m.folio_id, m.demat_id, m.market_nav,
		       m.cost_nav, m.total_units, m.unrealized_gain_loss, m.entity_id
		FROM investment.accounting_mtm m
		WHERE m.activity_id = $1
	`, activityID)
	if err != nil {
		return fmt.Errorf("fetch MTM data failed: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var mtmID, schemeID, entityID string
		var folioID, dematID *string
		var marketNav, costNav, totalUnits, unrealizedGL float64
		
		if err := rows.Scan(&mtmID, &schemeID, &folioID, &dematID, &marketNav, &costNav, &totalUnits, &unrealizedGL, &entityID); err != nil {
			return fmt.Errorf("scan MTM data failed: %w", err)
		}

		data := map[string]interface{}{
			"mtm_id":                mtmID,
			"scheme_id":             schemeID,
			"folio_id":              folioID,
			"demat_id":              dematID,
			"market_nav":            marketNav,
			"cost_nav":              costNav,
			"total_units":           totalUnits,
			"unrealized_gain_loss":  unrealizedGL,
			"entity_id":             entityID,
		}

		je, err := GenerateJournalEntryForMTM(ctx, pool, settings, activityID, data)
		if err != nil {
			return fmt.Errorf("generate MTM journal failed: %w", err)
		}

		if err := SaveJournalEntry(ctx, tx, je); err != nil {
			return fmt.Errorf("save MTM journal failed: %w", err)
		}
	}

	return rows.Err()
}

// generateDividendJournalInTx generates journal entries for Dividend within existing transaction
func generateDividendJournalInTx(ctx context.Context, tx DBExecutor, pool *pgxpool.Pool, settings *SettingsCache, activityID string) error {
	rows, err := tx.Query(ctx, `
		SELECT d.dividend_id, d.scheme_id, d.folio_id, d.dividend_amount,
		       d.reinvest_units, d.transaction_type, d.entity_id
		FROM investment.accounting_dividend d
		WHERE d.activity_id = $1
	`, activityID)
	if err != nil {
		return fmt.Errorf("fetch Dividend data failed: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var dividendID, schemeID, folioID, transactionType, entityID string
		var dividendAmount float64
		var reinvestUnits *float64
		
		if err := rows.Scan(&dividendID, &schemeID, &folioID, &dividendAmount, &reinvestUnits, &transactionType, &entityID); err != nil {
			return fmt.Errorf("scan Dividend data failed: %w", err)
		}

		data := map[string]interface{}{
			"dividend_id":       dividendID,
			"scheme_id":         schemeID,
			"folio_id":          folioID,
			"dividend_amount":   dividendAmount,
			"reinvest_units":    reinvestUnits,
			"transaction_type":  transactionType,
			"entity_id":         entityID,
		}

		je, err := GenerateJournalEntryForDividend(ctx, pool, settings, activityID, data)
		if err != nil {
			return fmt.Errorf("generate Dividend journal failed: %w", err)
		}

		if err := SaveJournalEntry(ctx, tx, je); err != nil {
			return fmt.Errorf("save Dividend journal failed: %w", err)
		}
	}

	return rows.Err()
}

// generateCorporateActionJournalInTx generates journal entries for Corporate Action within existing transaction
func generateCorporateActionJournalInTx(ctx context.Context, tx DBExecutor, pool *pgxpool.Pool, settings *SettingsCache, activityID, subtype string) error {
	rows, err := tx.Query(ctx, `
		SELECT ca.corporate_action_id, ca.action_type, ca.source_scheme_id,
		       ca.target_scheme_id, ca.folio_id, ca.exchange_ratio_num,
		       ca.exchange_ratio_denom, ca.source_units_affected, ca.new_units_issued, ca.entity_id
		FROM investment.accounting_corporate_action ca
		WHERE ca.activity_id = $1
	`, activityID)
	if err != nil {
		return fmt.Errorf("fetch Corporate Action data failed: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var caID, actionType, sourceSchemeID, folioID, entityID string
		var targetSchemeID *string
		var ratioNum, ratioDenom, sourceUnits, newUnits float64
		
		if err := rows.Scan(&caID, &actionType, &sourceSchemeID, &targetSchemeID, &folioID, &ratioNum, &ratioDenom, &sourceUnits, &newUnits, &entityID); err != nil {
			return fmt.Errorf("scan Corporate Action data failed: %w", err)
		}

		data := map[string]interface{}{
			"corporate_action_id":     caID,
			"action_type":             actionType,
			"source_scheme_id":        sourceSchemeID,
			"target_scheme_id":        targetSchemeID,
			"folio_id":                folioID,
			"exchange_ratio_num":      ratioNum,
			"exchange_ratio_denom":    ratioDenom,
			"source_units_affected":   sourceUnits,
			"new_units_issued":        newUnits,
			"entity_id":               entityID,
		}

		je, err := GenerateJournalEntryForCorporateAction(ctx, pool, settings, activityID, data)
		if err != nil {
			return fmt.Errorf("generate Corporate Action journal failed: %w", err)
		}

		if err := SaveJournalEntry(ctx, tx, je); err != nil {
			return fmt.Errorf("save Corporate Action journal failed: %w", err)
		}
	}

	return rows.Err()
}

// generateFVOJournalInTx generates journal entries for FVO within existing transaction
func generateFVOJournalInTx(ctx context.Context, tx DBExecutor, pool *pgxpool.Pool, settings *SettingsCache, activityID string) error {
	rows, err := tx.Query(ctx, `
		SELECT f.fvo_id, f.scheme_id, f.folio_id, f.demat_id, f.market_nav,
		       f.override_nav, f.variance, f.variance_pct, f.units_affected,
		       f.justification, f.entity_id
		FROM investment.accounting_fvo f
		WHERE f.activity_id = $1
	`, activityID)
	if err != nil {
		return fmt.Errorf("fetch FVO data failed: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var fvoID, schemeID, justification, entityID string
		var folioID, dematID *string
		var marketNav, overrideNav, variance, variancePct, unitsAffected float64
		
		if err := rows.Scan(&fvoID, &schemeID, &folioID, &dematID, &marketNav, &overrideNav, &variance, &variancePct, &unitsAffected, &justification, &entityID); err != nil {
			return fmt.Errorf("scan FVO data failed: %w", err)
		}

		data := map[string]interface{}{
			"fvo_id":          fvoID,
			"scheme_id":       schemeID,
			"folio_id":        folioID,
			"demat_id":        dematID,
			"market_nav":      marketNav,
			"override_nav":    overrideNav,
			"variance":        variance,
			"variance_pct":    variancePct,
			"units_affected":  unitsAffected,
			"justification":   justification,
			"entity_id":       entityID,
		}

		je, err := GenerateJournalEntryForFVO(ctx, pool, settings, activityID, data)
		if err != nil {
			return fmt.Errorf("generate FVO journal failed: %w", err)
		}

		if err := SaveJournalEntry(ctx, tx, je); err != nil {
			return fmt.Errorf("save FVO journal failed: %w", err)
		}
	}

	return rows.Err()
}

// ---------------------------
// Request/Response Types
// ---------------------------

type CreateActivityRequest struct {
	UserID           string `json:"user_id"`
	ActivityType     string `json:"activity_type"`
	ActivitySubtype  string `json:"activity_subtype,omitempty"`
	EffectiveDate    string `json:"effective_date"` // YYYY-MM-DD
	AccountingPeriod string `json:"accounting_period,omitempty"`
	DataSource       string `json:"data_source,omitempty"`
	Status           string `json:"status,omitempty"`
}

type UpdateActivityRequest struct {
	UserID     string                 `json:"user_id"`
	ActivityID string                 `json:"activity_id"`
	Fields     map[string]interface{} `json:"fields"`
	Reason     string                 `json:"reason"`
}

// ---------------------------
// CreateActivitySingle
// ---------------------------

func CreateActivitySingle(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req CreateActivityRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}

		// Validate required fields
		if strings.TrimSpace(req.ActivityType) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "activity_type is required")
			return
		}
		if strings.TrimSpace(req.EffectiveDate) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "effective_date is required")
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

		status := "DRAFT"
		if strings.TrimSpace(req.Status) != "" {
			status = req.Status
		}

		dataSource := "Manual"
		if strings.TrimSpace(req.DataSource) != "" {
			dataSource = req.DataSource
		}

		insertQ := `
			INSERT INTO investment.accounting_activity (
				activity_type, activity_subtype, effective_date, accounting_period, 
				data_source, status
			) VALUES ($1, $2, $3, $4, $5, $6)
			RETURNING activity_id
		`
		var activityID string
		if err := tx.QueryRow(ctx, insertQ,
			req.ActivityType,
			nullIfEmptyString(req.ActivitySubtype),
			req.EffectiveDate,
			nullIfEmptyString(req.AccountingPeriod),
			dataSource,
			status,
		).Scan(&activityID); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Insert failed: "+err.Error())
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
			"requested":   userEmail,
		})
	}
}

// ---------------------------
// CreateActivityBulk
// ---------------------------

func CreateActivityBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			Rows   []struct {
				ActivityType     string `json:"activity_type"`
				ActivitySubtype  string `json:"activity_subtype,omitempty"`
				EffectiveDate    string `json:"effective_date"`
				AccountingPeriod string `json:"accounting_period,omitempty"`
				DataSource       string `json:"data_source,omitempty"`
				Status           string `json:"status,omitempty"`
			} `json:"rows"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON body")
			return
		}

		if len(req.Rows) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No rows provided")
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
		results := make([]map[string]interface{}, 0, len(req.Rows))

		for _, row := range req.Rows {
			if strings.TrimSpace(row.ActivityType) == "" {
				results = append(results, map[string]interface{}{"success": false, "error": "activity_type is required"})
				continue
			}
			if strings.TrimSpace(row.EffectiveDate) == "" {
				results = append(results, map[string]interface{}{"success": false, "error": "effective_date is required"})
				continue
			}

			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				results = append(results, map[string]interface{}{"success": false, "error": "TX begin failed: " + err.Error()})
				continue
			}
			defer tx.Rollback(ctx)

			status := "DRAFT"
			if strings.TrimSpace(row.Status) != "" {
				status = row.Status
			}

			dataSource := "Manual"
			if strings.TrimSpace(row.DataSource) != "" {
				dataSource = row.DataSource
			}

			var activityID string
			if err := tx.QueryRow(ctx, `
				INSERT INTO investment.accounting_activity (
					activity_type, activity_subtype, effective_date, accounting_period, 
					data_source, status
				) VALUES ($1, $2, $3, $4, $5, $6)
				RETURNING activity_id
			`, row.ActivityType, nullIfEmptyString(row.ActivitySubtype), row.EffectiveDate,
				nullIfEmptyString(row.AccountingPeriod), dataSource, status).Scan(&activityID); err != nil {
				results = append(results, map[string]interface{}{"success": false, "error": "Insert failed: " + err.Error()})
				continue
			}

			if _, err := tx.Exec(ctx, `
				INSERT INTO investment.auditactionaccountingactivity (activity_id, actiontype, processing_status, requested_by, requested_at)
				VALUES ($1, 'CREATE', 'PENDING_APPROVAL', $2, now())
			`, activityID, userEmail); err != nil {
				results = append(results, map[string]interface{}{"success": false, "error": "Audit insert failed: " + err.Error()})
				continue
			}

			if err := tx.Commit(ctx); err != nil {
				results = append(results, map[string]interface{}{"success": false, "error": "Commit failed: " + err.Error()})
				continue
			}

			results = append(results, map[string]interface{}{
				"success":     true,
				"activity_id": activityID,
			})
		}

		api.RespondWithPayload(w, api.IsBulkSuccess(results), "", results)
	}
}

// ---------------------------
// UpdateActivity
// ---------------------------

func UpdateActivity(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req UpdateActivityRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}
		if strings.TrimSpace(req.ActivityID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "activity_id required")
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
			SELECT activity_type, activity_subtype, effective_date, accounting_period, 
			       data_source, status
			FROM investment.accounting_activity
			WHERE activity_id=$1
			FOR UPDATE
		`
		var oldVals [6]interface{}
		if err := tx.QueryRow(ctx, sel, req.ActivityID).Scan(
			&oldVals[0], &oldVals[1], &oldVals[2], &oldVals[3],
			&oldVals[4], &oldVals[5],
		); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "fetch failed: "+err.Error())
			return
		}

		fieldPairs := map[string]int{
			"activity_type":     0,
			"activity_subtype":  1,
			"effective_date":    2,
			"accounting_period": 3,
			"data_source":       4,
			"status":            5,
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

		q := fmt.Sprintf("UPDATE investment.accounting_activity SET %s, updated_at=now() WHERE activity_id=$%d", strings.Join(sets, ", "), pos)
		args = append(args, req.ActivityID)
		if _, err := tx.Exec(ctx, q, args...); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "update failed: "+err.Error())
			return
		}

		// Audit
		if _, err := tx.Exec(ctx, `
			INSERT INTO investment.auditactionaccountingactivity (activity_id, actiontype, processing_status, reason, requested_by, requested_at)
			VALUES ($1, 'EDIT', 'PENDING_EDIT_APPROVAL', $2, $3, now())
		`, req.ActivityID, req.Reason, userEmail); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "audit insert failed: "+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "commit failed: "+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]any{
			"activity_id": req.ActivityID,
			"requested":   userEmail,
		})
	}
}

// ---------------------------
// DeleteActivity
// ---------------------------

func DeleteActivity(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID      string   `json:"user_id"`
			ActivityIDs []string `json:"activity_ids"`
			Reason      string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}
		if len(req.ActivityIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "activity_ids required")
			return
		}

		requestedBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				requestedBy = s.Name
				break
			}
		}
		if requestedBy == "" {
			api.RespondWithError(w, http.StatusUnauthorized, "Invalid session")
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "tx begin failed: "+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		for _, id := range req.ActivityIDs {
			if _, err := tx.Exec(ctx, `
				INSERT INTO investment.auditactionaccountingactivity (activity_id, actiontype, processing_status, reason, requested_by, requested_at)
				VALUES ($1, 'DELETE', 'PENDING_DELETE_APPROVAL', $2, $3, now())
			`, id, req.Reason, requestedBy); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "audit insert failed: "+err.Error())
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "commit failed: "+err.Error())
			return
		}
		api.RespondWithPayload(w, true, "", map[string]any{"delete_requested": req.ActivityIDs})
	}
}

// ---------------------------
// BulkApproveActivityActions
// ---------------------------

func BulkApproveActivityActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID      string   `json:"user_id"`
			ActivityIDs []string `json:"activity_ids"`
			Comment     string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}
		checkerBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				checkerBy = s.Name
				break
			}
		}
		if checkerBy == "" {
			api.RespondWithError(w, http.StatusUnauthorized, "invalid session")
			return
		}

		ctx := context.Background()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "tx begin failed: "+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		sel := `
			SELECT DISTINCT ON (activity_id) action_id, activity_id, actiontype, processing_status
			FROM investment.auditactionaccountingactivity
			WHERE activity_id = ANY($1)
			ORDER BY activity_id, requested_at DESC
		`
		rows, err := tx.Query(ctx, sel, req.ActivityIDs)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "query failed: "+err.Error())
			return
		}
		defer rows.Close()

		var toApprove []string
		var toApproveActivityIDs []string
		var toDeleteActionIDs []string
		var deleteActivityIDs []string

		for rows.Next() {
			var aid, actid, atype, pstatus string
			if err := rows.Scan(&aid, &actid, &atype, &pstatus); err != nil {
				continue
			}
			ps := strings.ToUpper(strings.TrimSpace(pstatus))
			if ps == "APPROVED" {
				continue
			}
			if ps == "PENDING_DELETE_APPROVAL" {
				toDeleteActionIDs = append(toDeleteActionIDs, aid)
				deleteActivityIDs = append(deleteActivityIDs, actid)
				continue
			}
			if ps == "PENDING_APPROVAL" || ps == "PENDING_EDIT_APPROVAL" {
				toApprove = append(toApprove, aid)
				toApproveActivityIDs = append(toApproveActivityIDs, actid)
			}
		}

		if len(toApprove) == 0 && len(toDeleteActionIDs) == 0 {
			api.RespondWithPayload(w, false, "No approvable actions found", map[string]any{
				"approved_action_ids":  []string{},
				"deleted_activity_ids": []string{},
			})
			return
		}

		if len(toApprove) > 0 {
			if _, err := tx.Exec(ctx, `
				UPDATE investment.auditactionaccountingactivity
				SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2
				WHERE action_id = ANY($3)
			`, checkerBy, req.Comment, toApprove); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "approve failed: "+err.Error())
				return
			}

			// Update activity status to APPROVED
			if _, err := tx.Exec(ctx, `
				UPDATE investment.accounting_activity
				SET status='APPROVED'
				WHERE activity_id = ANY($1)
			`, toApproveActivityIDs); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "update activity status failed: "+err.Error())
				return
			}

			// Generate journal entries WITHIN SAME TRANSACTION (atomic operation)
			// If journal generation fails, entire approval rolls back
			for _, actID := range toApproveActivityIDs {
				if err := GenerateAndSaveJournalEntriesInTx(ctx, tx, pgxPool, actID); err != nil {
					api.RespondWithError(w, http.StatusInternalServerError, fmt.Sprintf("journal generation failed for activity %s: %v", actID, err))
					return
				}
			}
		}

		if len(toDeleteActionIDs) > 0 {
			if _, err := tx.Exec(ctx, `
				UPDATE investment.auditactionaccountingactivity
				SET processing_status='DELETED', checker_by=$1, checker_at=now(), checker_comment=$2
				WHERE action_id = ANY($3)
			`, checkerBy, req.Comment, toDeleteActionIDs); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "delete action update failed: "+err.Error())
				return
			}
			if _, err := tx.Exec(ctx, `
				UPDATE investment.accounting_activity
				SET is_deleted=true, status='DELETED', updated_at=now()
				WHERE activity_id = ANY($1)
			`, deleteActivityIDs); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "delete activity failed: "+err.Error())
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "commit failed: "+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]any{
			"approved_action_ids":  toApprove,
			"deleted_activity_ids": deleteActivityIDs,
		})
	}
}

// ---------------------------
// BulkRejectActivityActions
// ---------------------------

func BulkRejectActivityActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID      string   `json:"user_id"`
			ActivityIDs []string `json:"activity_ids"`
			Comment     string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}
		checkerBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				checkerBy = s.Name
				break
			}
		}
		if checkerBy == "" {
			api.RespondWithError(w, http.StatusUnauthorized, "invalid session")
			return
		}

		ctx := context.Background()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "tx begin failed: "+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		sel := `
			SELECT DISTINCT ON (activity_id) action_id, activity_id, processing_status
			FROM investment.auditactionaccountingactivity
			WHERE activity_id = ANY($1)
			ORDER BY activity_id, requested_at DESC
		`
		rows, err := tx.Query(ctx, sel, req.ActivityIDs)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "query failed: "+err.Error())
			return
		}
		defer rows.Close()

		actionIDs := []string{}
		cannotReject := []string{}
		found := map[string]bool{}
		for rows.Next() {
			var aid, actid, ps string
			if err := rows.Scan(&aid, &actid, &ps); err != nil {
				continue
			}
			found[actid] = true
			if strings.ToUpper(strings.TrimSpace(ps)) == "APPROVED" {
				cannotReject = append(cannotReject, actid)
			} else {
				actionIDs = append(actionIDs, aid)
			}
		}

		missing := []string{}
		for _, id := range req.ActivityIDs {
			if !found[id] {
				missing = append(missing, id)
			}
		}
		if len(missing) > 0 || len(cannotReject) > 0 {
			msg := ""
			if len(missing) > 0 {
				msg += fmt.Sprintf("no audit action found for activity_ids: %v. ", missing)
			}
			if len(cannotReject) > 0 {
				msg += fmt.Sprintf("cannot reject already approved activity_ids: %v", cannotReject)
			}
			api.RespondWithError(w, http.StatusBadRequest, msg)
			return
		}

		if _, err := tx.Exec(ctx, `
			UPDATE investment.auditactionaccountingactivity
			SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2
			WHERE action_id = ANY($3)
		`, checkerBy, req.Comment, actionIDs); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "update failed: "+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "commit failed: "+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]any{"rejected_action_ids": actionIDs})
	}
}

// ---------------------------
// GetActivitiesWithAudit
// ---------------------------

func GetActivitiesWithAudit(pgxPool *pgxpool.Pool) http.HandlerFunc {
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
			),
			history AS (
				SELECT 
					activity_id,
					MAX(CASE WHEN actiontype='CREATE' THEN requested_by END) AS created_by,
					MAX(CASE WHEN actiontype='CREATE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS created_at,
					MAX(CASE WHEN actiontype='EDIT' THEN requested_by END) AS edited_by,
					MAX(CASE WHEN actiontype='EDIT' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS edited_at,
					MAX(CASE WHEN actiontype='DELETE' THEN requested_by END) AS deleted_by,
					MAX(CASE WHEN actiontype='DELETE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS deleted_at
				FROM investment.auditactionaccountingactivity
				GROUP BY activity_id
			)
			SELECT
				m.activity_id,
				m.activity_type,
				m.old_activity_type,
				COALESCE(m.activity_subtype,'') AS activity_subtype,
				COALESCE(m.old_activity_subtype,'') AS old_activity_subtype,
				TO_CHAR(m.effective_date, 'YYYY-MM-DD') AS effective_date,
				TO_CHAR(m.old_effective_date, 'YYYY-MM-DD') AS old_effective_date,
				TO_CHAR(m.accounting_period, 'YYYY-MM-DD') AS accounting_period,
				TO_CHAR(m.old_accounting_period, 'YYYY-MM-DD') AS old_accounting_period,
				m.data_source,
				COALESCE(m.old_data_source,'') AS old_data_source,
				m.status,
				COALESCE(m.old_status,'') AS old_status,
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
				COALESCE(l.reason,'') AS reason,
				
				COALESCE(h.created_by,'') AS created_by,
				COALESCE(h.created_at,'') AS created_at,
				COALESCE(h.edited_by,'') AS edited_by,
				COALESCE(h.edited_at,'') AS edited_at,
				COALESCE(h.deleted_by,'') AS deleted_by,
				COALESCE(h.deleted_at,'') AS deleted_at
			FROM investment.accounting_activity m
			LEFT JOIN latest_audit l ON l.activity_id = m.activity_id
			LEFT JOIN history h ON h.activity_id = m.activity_id
			WHERE COALESCE(m.is_deleted, false) = false
			ORDER BY m.updated_at DESC, m.activity_id;
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

// ---------------------------
// GetApprovedActivities
// ---------------------------

func GetApprovedActivities(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		q := `
			WITH latest AS (
				SELECT DISTINCT ON (activity_id)
					activity_id,
					processing_status
				FROM investment.auditactionaccountingactivity
				ORDER BY activity_id, requested_at DESC
			)
			SELECT
				m.activity_id,
				m.activity_type,
				COALESCE(m.activity_subtype,'') AS activity_subtype,
				TO_CHAR(m.effective_date, 'YYYY-MM-DD') AS effective_date,
				TO_CHAR(m.accounting_period, 'YYYY-MM-DD') AS accounting_period,
				m.data_source,
				m.status
			FROM investment.accounting_activity m
			JOIN latest l ON l.activity_id = m.activity_id
			WHERE 
				UPPER(l.processing_status) = 'APPROVED'
				AND COALESCE(m.is_deleted,false)=false
			ORDER BY m.updated_at DESC;
		`

		rows, err := pgxPool.Query(ctx, q)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "query failed: "+err.Error())
			return
		}
		defer rows.Close()

		fields := rows.FieldDescriptions()
		out := make([]map[string]interface{}, 0, 100)
		for rows.Next() {
			vals, _ := rows.Values()
			rec := make(map[string]interface{}, len(fields))
			for i, f := range fields {
				rec[string(f.Name)] = vals[i]
			}
			out = append(out, rec)
		}

		if rows.Err() != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "rows error: "+rows.Err().Error())
			return
		}

		api.RespondWithPayload(w, true, "", out)
	}
}

// ---------------------------
// GetJournalEntries
// ---------------------------

// GetJournalEntries returns all journal entries with lines (optional entity_name filter)
func GetJournalEntries(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		// Parse request body for optional entity_name filter
		type FilterRequest struct {
			EntityName string `json:"entity_name,omitempty"`
		}
		var req FilterRequest
		if r.Body != nil && r.Method == "POST" {
			json.NewDecoder(r.Body).Decode(&req)
		}

		// Build query with optional filter
		whereClause := "WHERE je.is_deleted = false"
		args := []interface{}{}
		
		if req.EntityName != "" {
			whereClause += " AND je.entity_name = $1"
			args = append(args, req.EntityName)
		}

		query := fmt.Sprintf(`
			SELECT
				je.entry_id,
				je.activity_id,
				je.entity_id,
				je.entity_name,
				COALESCE(je.folio_id, '') AS folio_id,
				COALESCE(je.demat_id, '') AS demat_id,
				TO_CHAR(je.entry_date, 'YYYY-MM-DD') AS entry_date,
				je.accounting_period,
				je.entry_type,
				COALESCE(je.description, '') AS description,
				je.total_debit,
				je.total_credit,
				je.status,
				TO_CHAR(je.created_at, 'YYYY-MM-DD HH24:MI:SS') AS created_at,
				COALESCE(je.created_by, '') AS created_by,
				
				-- Aggregate line items as JSON array
				COALESCE(
					json_agg(
						json_build_object(
							'line_id', jel.line_id,
							'line_number', jel.line_number,
							'account_number', jel.account_number,
							'account_name', jel.account_name,
							'account_type', jel.account_type,
							'debit_amount', jel.debit_amount,
							'credit_amount', jel.credit_amount,
							'scheme_id', COALESCE(jel.scheme_id, ''),
							'folio_id', COALESCE(jel.folio_id, ''),
							'demat_id', COALESCE(jel.demat_id, ''),
							'narration', COALESCE(jel.narration, '')
						) ORDER BY jel.line_number
					) FILTER (WHERE jel.line_id IS NOT NULL),
					'[]'::json
				) AS line_items
			FROM investment.accounting_journal_entry je
			LEFT JOIN investment.accounting_journal_entry_line jel ON jel.entry_id = je.entry_id
			%s
			GROUP BY je.entry_id, je.activity_id, je.entity_id, je.entity_name, je.folio_id, 
			         je.demat_id, je.entry_date, je.accounting_period, je.entry_type, 
			         je.description, je.total_debit, je.total_credit, je.status, 
			         je.created_at, je.created_by
			ORDER BY je.created_at DESC, je.entry_id
		`, whereClause)

		rows, err := pgxPool.Query(ctx, query, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "query failed: "+err.Error())
			return
		}
		defer rows.Close()

		fields := rows.FieldDescriptions()
		out := make([]map[string]interface{}, 0, 100)
		
		for rows.Next() {
			vals, _ := rows.Values()
			rec := make(map[string]interface{}, len(fields))
			for i, f := range fields {
				rec[string(f.Name)] = vals[i]
			}
			out = append(out, rec)
		}

		if rows.Err() != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "rows error: "+rows.Err().Error())
			return
		}

		api.RespondWithPayload(w, true, "", out)
	}
}

// ---------------------------
// Helper functions
// ---------------------------

func nullIfEmptyString(s string) interface{} {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return s
}

func nullIfZeroFloat(f float64) interface{} {
	if f == 0 {
		return nil
	}
	return f
}

func nullIfZeroInt(i int64) interface{} {
	if i == 0 {
		return nil
	}
	return i
}
