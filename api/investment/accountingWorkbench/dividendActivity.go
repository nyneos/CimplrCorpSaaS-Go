package accountingworkbench

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/constants"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ---------------------------
// Request/Response Types for Dividend
// ---------------------------

type CreateDividendRequest struct {
	UserID           string  `json:"user_id"`
	ActivityType     string  `json:"activity_type"` // "DIVIDEND"
	EffectiveDate    string  `json:"effective_date"`
	AccountingPeriod string  `json:"accounting_period,omitempty"`
	DataSource       string  `json:"data_source"`
	SchemeID         string  `json:"scheme_id"`
	FolioID          string  `json:"folio_id,omitempty"`
	ExDate           string  `json:"ex_date"`          // YYYY-MM-DD
	RecordDate       string  `json:"record_date"`      // YYYY-MM-DD
	PaymentDate      string  `json:"payment_date"`     // YYYY-MM-DD
	TransactionType  string  `json:"transaction_type"` // PAYOUT / REINVEST
	DividendAmount   float64 `json:"dividend_amount"`
	ReinvestNAV      float64 `json:"reinvest_nav,omitempty"`
	ReinvestUnits    float64 `json:"reinvest_units,omitempty"`
}

type UpdateDividendRequest struct {
	UserID     string                 `json:"user_id"`
	DividendID string                 `json:"dividend_id"`
	Fields     map[string]interface{} `json:"fields"`
	Reason     string                 `json:"reason"`
}

// ---------------------------
// CreateDividendSingle
// ---------------------------

func CreateDividendSingle(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req CreateDividendRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}

		// Validate required fields
		if strings.TrimSpace(req.EffectiveDate) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "effective_date is required")
			return
		}
		if strings.TrimSpace(req.SchemeID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "scheme_id is required")
			return
		}
		if strings.TrimSpace(req.TransactionType) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "transaction_type is required")
			return
		}
		if req.DividendAmount <= 0 {
			api.RespondWithError(w, http.StatusBadRequest, "dividend_amount must be greater than 0")
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
		activityType := "DIVIDEND"
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

		// Insert Dividend record
		var dividendID string
		if err := tx.QueryRow(ctx, `
			INSERT INTO investment.accounting_dividend (
				activity_id, scheme_id, folio_id, ex_date, record_date, payment_date,
				transaction_type, dividend_amount, reinvest_nav, reinvest_units
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
			RETURNING dividend_id
		`, activityID, req.SchemeID, nullIfEmptyString(req.FolioID), req.ExDate, req.RecordDate,
			req.PaymentDate, req.TransactionType, req.DividendAmount,
			nullIfZeroFloat(req.ReinvestNAV), nullIfZeroFloat(req.ReinvestUnits)).Scan(&dividendID); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Dividend insert failed: "+err.Error())
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
			"dividend_id": dividendID,
			"requested":   userEmail,
		})
	}
}

// ---------------------------
// CreateDividendBulk
// ---------------------------

func CreateDividendBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID           string `json:"user_id"`
			ActivityType     string `json:"activity_type"`
			EffectiveDate    string `json:"effective_date"`
			AccountingPeriod string `json:"accounting_period,omitempty"`
			DataSource       string `json:"data_source"`
			Rows             []struct {
				SchemeID        string  `json:"scheme_id"`
				FolioID         string  `json:"folio_id,omitempty"`
				ExDate          string  `json:"ex_date"`
				RecordDate      string  `json:"record_date"`
				PaymentDate     string  `json:"payment_date"`
				TransactionType string  `json:"transaction_type"`
				DividendAmount  float64 `json:"dividend_amount"`
				ReinvestNAV     float64 `json:"reinvest_nav,omitempty"`
				ReinvestUnits   float64 `json:"reinvest_units,omitempty"`
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

		// Single transaction for entire batch
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "TX begin failed: "+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		// Create main activity
		activityType := "DIVIDEND"
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

		results := make([]map[string]interface{}, 0, len(req.Rows))

		for _, row := range req.Rows {
			// Validate
			if strings.TrimSpace(row.SchemeID) == "" {
				results = append(results, map[string]interface{}{"success": false, "error": "scheme_id is required"})
				continue
			}
			if strings.TrimSpace(row.TransactionType) == "" {
				results = append(results, map[string]interface{}{"success": false, "error": "transaction_type is required"})
				continue
			}
			if row.DividendAmount <= 0 {
				results = append(results, map[string]interface{}{"success": false, "error": "dividend_amount must be > 0"})
				continue
			}

			var dividendID string
			if err := tx.QueryRow(ctx, `
				INSERT INTO investment.accounting_dividend (
					activity_id, scheme_id, folio_id, ex_date, record_date, payment_date,
					transaction_type, dividend_amount, reinvest_nav, reinvest_units
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
				RETURNING dividend_id
			`, activityID, row.SchemeID, nullIfEmptyString(row.FolioID), row.ExDate, row.RecordDate,
				row.PaymentDate, row.TransactionType, row.DividendAmount,
				nullIfZeroFloat(row.ReinvestNAV), nullIfZeroFloat(row.ReinvestUnits)).Scan(&dividendID); err != nil {
				results = append(results, map[string]interface{}{"success": false, "error": "Dividend insert failed: " + err.Error()})
				continue
			}

			results = append(results, map[string]interface{}{
				"success":     true,
				"dividend_id": dividendID,
				"scheme_id":   row.SchemeID,
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
			"activity_id":      activityID,
			"dividend_records": results,
		})
	}
}

// ---------------------------
// UpdateDividend
// ---------------------------

func UpdateDividend(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req UpdateDividendRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}
		if strings.TrimSpace(req.DividendID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "dividend_id required")
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
			SELECT activity_id, scheme_id, folio_id, ex_date, record_date, payment_date,
			       transaction_type, dividend_amount, reinvest_nav, reinvest_units
			FROM investment.accounting_dividend
			WHERE dividend_id=$1
			FOR UPDATE
		`
		var activityID string
		var oldVals [9]interface{}
		if err := tx.QueryRow(ctx, sel, req.DividendID).Scan(
			&activityID, &oldVals[0], &oldVals[1], &oldVals[2], &oldVals[3],
			&oldVals[4], &oldVals[5], &oldVals[6], &oldVals[7], &oldVals[8],
		); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "fetch failed: "+err.Error())
			return
		}

		fieldPairs := map[string]int{
			"scheme_id":        0,
			"folio_id":         1,
			"ex_date":          2,
			"record_date":      3,
			"payment_date":     4,
			"transaction_type": 5,
			"dividend_amount":  6,
			"reinvest_nav":     7,
			"reinvest_units":   8,
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

		q := fmt.Sprintf("UPDATE investment.accounting_dividend SET %s, updated_at=now() WHERE dividend_id=$%d", strings.Join(sets, ", "), pos)
		args = append(args, req.DividendID)
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
			"dividend_id": req.DividendID,
			"activity_id": activityID,
			"requested":   userEmail,
		})
	}
}

// ---------------------------
// GetDividendsWithAudit
// ---------------------------

func GetDividendsWithAudit(pgxPool *pgxpool.Pool) http.HandlerFunc {
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
				d.dividend_id,
				d.activity_id,
				act.activity_type,
				TO_CHAR(act.effective_date, 'YYYY-MM-DD') AS effective_date,
				TO_CHAR(act.accounting_period, 'YYYY-MM-DD') AS accounting_period,
				act.data_source,
				act.status,
				d.scheme_id,
				d.old_scheme_id,
				COALESCE(d.folio_id,'') AS folio_id,
				COALESCE(d.old_folio_id,'') AS old_folio_id,
				TO_CHAR(d.ex_date, 'YYYY-MM-DD') AS ex_date,
				TO_CHAR(d.old_ex_date, 'YYYY-MM-DD') AS old_ex_date,
				TO_CHAR(d.record_date, 'YYYY-MM-DD') AS record_date,
				TO_CHAR(d.old_record_date, 'YYYY-MM-DD') AS old_record_date,
				TO_CHAR(d.payment_date, 'YYYY-MM-DD') AS payment_date,
				TO_CHAR(d.old_payment_date, 'YYYY-MM-DD') AS old_payment_date,
				d.transaction_type,
				COALESCE(d.old_transaction_type,'') AS old_transaction_type,
				d.dividend_amount,
				COALESCE(d.old_dividend_amount,0) AS old_dividend_amount,
				COALESCE(d.reinvest_nav,0) AS reinvest_nav,
				COALESCE(d.old_reinvest_nav,0) AS old_reinvest_nav,
				COALESCE(d.reinvest_units,0) AS reinvest_units,
				COALESCE(d.old_reinvest_units,0) AS old_reinvest_units,
				d.is_deleted,
				TO_CHAR(d.updated_at, 'YYYY-MM-DD HH24:MI:SS') AS updated_at,
				
				COALESCE(l.actiontype,'') AS action_type,
				COALESCE(l.processing_status,'') AS processing_status,
				COALESCE(l.action_id::text,'') AS action_id,
				COALESCE(l.requested_by,'') AS audit_requested_by,
				TO_CHAR(l.requested_at,'YYYY-MM-DD HH24:MI:SS') AS requested_at,
				COALESCE(l.checker_by,'') AS checker_by,
				TO_CHAR(l.checker_at,'YYYY-MM-DD HH24:MI:SS') AS checker_at,
				COALESCE(l.checker_comment,'') AS checker_comment,
				COALESCE(l.reason,'') AS reason
			FROM investment.accounting_dividend d
			JOIN investment.accounting_activity act ON act.activity_id = d.activity_id
			LEFT JOIN latest_audit l ON l.activity_id = d.activity_id
			WHERE COALESCE(d.is_deleted, false) = false
				AND COALESCE(act.is_deleted, false) = false
			ORDER BY d.updated_at DESC, d.dividend_id;
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
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrRowsScanFailed+rows.Err().Error())
			return
		}

		api.RespondWithPayload(w, true, "", out)
	}
}
