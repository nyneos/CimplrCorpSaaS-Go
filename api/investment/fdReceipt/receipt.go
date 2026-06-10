package fdReceipt

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/approvalengine"
	"CimplrCorpSaas/api/constants"
	notifcatalog "CimplrCorpSaas/api/notification/catalog"
	"CimplrCorpSaas/internal/ctxutil"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ─── internal helpers ─────────────────────────────────────────────────────────

func resolveUserEmail(ctx context.Context) string {
	if s := api.GetSessionFromCtx(ctx); s != nil {
		return s.Email
	}
	return ""
}

func nullStr(v string) interface{} {
	if v == "" {
		return nil
	}
	return v
}

func appendFDReceiptScopeFilters(ctx context.Context, w http.ResponseWriter, query *string, args *[]interface{}, argIdx *int, entityCol, bankCol, entityID, bankID string) bool {
	scope := ctxutil.FromContext(ctx)
	entityID = strings.TrimSpace(entityID)
	bankID = strings.TrimSpace(bankID)

	if entityID != "" {
		if !scope.HasEntityAccess(entityID) {
			api.RespondWithError(w, http.StatusForbidden, fmt.Sprintf("Entity ID '%s' is not within your authorized access scope.", entityID))
			return false
		}
		*query += fmt.Sprintf(" AND %s=$%d", entityCol, *argIdx)
		*args = append(*args, entityID)
		*argIdx++
	} else if len(scope.EntityIDs) > 0 {
		*query += fmt.Sprintf(" AND %s = ANY($%d::text[])", entityCol, *argIdx)
		*args = append(*args, scope.EntityIDs)
		*argIdx++
	}

	if bankID != "" {
		if !scope.HasApprovedBank(bankID) {
			api.RespondWithError(w, http.StatusForbidden, fmt.Sprintf("Bank '%s' is not within your approved bank scope.", bankID))
			return false
		}
		*query += fmt.Sprintf(" AND %s=$%d", bankCol, *argIdx)
		*args = append(*args, bankID)
		*argIdx++
	} else if bankIDs := scope.BankIDs(); len(bankIDs) > 0 {
		*query += fmt.Sprintf(" AND (%s = '' OR %s = ANY($%d::text[]))", bankCol, bankCol, *argIdx)
		*args = append(*args, bankIDs)
		*argIdx++
	}

	return true
}

func requireFDReceiptEntityScope(ctx context.Context, w http.ResponseWriter, entityID string) bool {
	entityID = strings.TrimSpace(entityID)
	if entityID == "" || ctxutil.FromContext(ctx).HasEntityAccess(entityID) {
		return true
	}
	api.RespondWithError(w, http.StatusForbidden, fmt.Sprintf("Entity ID '%s' is not within your authorized access scope.", entityID))
	return false
}

func requireFDReceiptIDScope(ctx context.Context, pool *pgxpool.Pool, w http.ResponseWriter, receiptIDs, tdsIDs []string) bool {
	if len(receiptIDs) == 0 && len(tdsIDs) == 0 {
		return true
	}
	rows, err := pool.Query(ctx, `
		SELECT entity_id, COALESCE(bank_id,'') AS bank_id
		FROM investment.fd_interest_receipt
		WHERE receipt_id = ANY($1::text[]) AND is_deleted=false
		UNION ALL
		SELECT entity_id, COALESCE(bank_id,'') AS bank_id
		FROM investment.fd_tds_receipt
		WHERE tds_id = ANY($2::text[]) AND is_deleted=false`,
		receiptIDs, tdsIDs,
	)
	if err != nil {
		api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
		return false
	}
	defer rows.Close()

	scope := ctxutil.FromContext(ctx)
	for rows.Next() {
		var entityID, bankID string
		if err := rows.Scan(&entityID, &bankID); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrRowError+err.Error())
			return false
		}
		if !scope.HasEntityAccess(entityID) {
			api.RespondWithError(w, http.StatusForbidden, fmt.Sprintf("Entity ID '%s' is not within your authorized access scope.", entityID))
			return false
		}
		if bankID != "" && !scope.HasApprovedBank(bankID) {
			api.RespondWithError(w, http.StatusForbidden, fmt.Sprintf("Bank '%s' is not within your approved bank scope.", bankID))
			return false
		}
	}
	if rows.Err() != nil {
		api.RespondWithError(w, http.StatusInternalServerError, constants.ErrRowError+rows.Err().Error())
		return false
	}
	return true
}

type receiptSchemaQueryer interface {
	QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row
}

func fdReceiptTableColumns(ctx context.Context, q receiptSchemaQueryer, tableName string) (map[string]bool, error) {
	var raw []byte
	err := q.QueryRow(ctx, `
		SELECT COALESCE(jsonb_object_agg(column_name, true), '{}'::jsonb)
		FROM information_schema.columns
		WHERE table_schema='investment' AND table_name=$1`, tableName).Scan(&raw)
	if err != nil {
		return nil, err
	}
	out := map[string]bool{}
	if len(raw) == 0 {
		return out, nil
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func nullTime(t time.Time) interface{} {
	if t.IsZero() {
		return nil
	}
	return t
}

func rowsToMapSlice(rows pgx.Rows) ([]map[string]interface{}, error) {
	fields := rows.FieldDescriptions()
	var out []map[string]interface{}
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, err
		}
		row := make(map[string]interface{}, len(fields))
		for i, f := range fields {
			if vals[i] == nil {
				row[string(f.Name)] = ""
			} else {
				row[string(f.Name)] = vals[i]
			}
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func toFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case float32:
		return float64(val), true
	case int:
		return float64(val), true
	case int64:
		return float64(val), true
	case json.Number:
		f, err := val.Float64()
		return f, err == nil
	}
	return 0, false
}

// checkFDDates validates that date strings fall inside the FD start-to-maturity window.
// Returns a human-friendly error message, or "" when all dates are valid.
func checkFDDates(fdStart, fdMaturity time.Time, labels, values []string) string {
	for i, v := range values {
		if v == "" {
			continue
		}
		t, err := time.Parse(constants.DateFormat, v)
		if err != nil {
			return fmt.Sprintf("%s must be in YYYY-MM-DD format", labels[i])
		}
		if t.Before(fdStart) || t.After(fdMaturity) {
			return fmt.Sprintf(
				"%s (%s) must be within this FD's window (%s to %s)",
				labels[i], v, fdStart.Format(constants.DateFormat), fdMaturity.Format(constants.DateFormat))
		}
	}
	return ""
}

func checkFDPeriodDates(fdStart, fdMaturity time.Time, periodStart, periodEnd string) string {
	if errMsg := checkFDDates(fdStart, fdMaturity,
		[]string{"period_start", "period_end"},
		[]string{periodStart, periodEnd}); errMsg != "" {
		return errMsg
	}
	if periodStart == "" || periodEnd == "" {
		return ""
	}
	ps, psErr := time.Parse(constants.DateFormat, periodStart)
	pe, peErr := time.Parse(constants.DateFormat, periodEnd)
	if psErr != nil || peErr != nil {
		return ""
	}
	if ps.After(pe) {
		return fmt.Sprintf("period_start (%s) cannot be after period_end (%s)", periodStart, periodEnd)
	}
	return ""
}

// ─── HANDLER 1: CreateReceipt ─────────────────────────────────────────────────

func CreateReceipt(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID                string  `json:"user_id"`
			FdID                  string  `json:"fd_id"`
			ReceiptDate           string  `json:"receipt_date"`
			PeriodStart           string  `json:"period_start"`
			PeriodEnd             string  `json:"period_end"`
			GrossInterestReceived float64 `json:"gross_interest_received"`
			TdsAmountDeducted     float64 `json:"tds_amount_deducted"`
			OtherCharges          float64 `json:"other_charges"`
			CashflowID            string  `json:"cashflow_id"`
			BankReferenceNo       string  `json:"bank_reference_no"`
			Narration             string  `json:"narration"`
			Attachment            string  `json:"attachment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.FdID == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrFDIDRequired)
			return
		}
		if req.ReceiptDate == "" {
			api.RespondWithError(w, http.StatusBadRequest, "receipt_date is required")
			return
		}
		if req.GrossInterestReceived <= 0 {
			api.RespondWithError(w, http.StatusBadRequest, "gross_interest_received must be > 0")
			return
		}
		if req.TdsAmountDeducted < 0 {
			api.RespondWithError(w, http.StatusBadRequest, "tds_amount_deducted must be >= 0")
			return
		}
		if req.OtherCharges < 0 {
			api.RespondWithError(w, http.StatusBadRequest, "other_charges must be >= 0")
			return
		}
		receiptDate, err := time.Parse(constants.DateFormat, req.ReceiptDate)
		if err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "receipt_date must be YYYY-MM-DD")
			return
		}
		userEmail := resolveUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		net := req.GrossInterestReceived - req.TdsAmountDeducted - req.OtherCharges
		if net < 0 {
			api.RespondWithError(w, http.StatusBadRequest, "net_amount_received cannot be negative")
			return
		}

		ctx := r.Context()

		// Fetch FD master
		var fdRefNo, entityID, entityName, bankID, bankName, fdStatus string
		var tdsPlanID *string
		var fdStart, fdMaturity time.Time
		err = pool.QueryRow(ctx, `
			SELECT bank_fd_ref_no, entity_id, entity_name, bank_id, bank_name, fd_status, tds_plan_id,
			       start_date, maturity_date
			FROM investment.fd_master
			WHERE fd_id=$1 AND is_deleted=false`, req.FdID).Scan(
			&fdRefNo, &entityID, &entityName, &bankID, &bankName, &fdStatus, &tdsPlanID,
			&fdStart, &fdMaturity)
		if err != nil {
			if err == pgx.ErrNoRows {
				api.RespondWithError(w, http.StatusNotFound, constants.ErrFDNotFound)
				return
			}
			api.RespondWithError(w, http.StatusInternalServerError, "FD lookup failed: "+err.Error())
			return
		}
		if fdStatus != constants.StatusActive && fdStatus != "MATURED" {
			api.RespondWithError(w, http.StatusBadRequest, "FD must be ACTIVE or MATURED")
			return
		}

		receiptScope := ctxutil.FromContext(r.Context())
		if !receiptScope.HasEntityAccess(entityID) {
			api.RespondWithError(w, http.StatusForbidden,
				fmt.Sprintf("Entity ID '%s' is not within your authorized access scope.", entityID))
			return
		}
		if !receiptScope.HasApprovedBank(bankID) {
			api.RespondWithError(w, http.StatusForbidden,
				fmt.Sprintf("Bank '%s' is not within your approved bank scope.", bankID))
			return
		}
		if errMsg := checkFDDates(fdStart, fdMaturity,
			[]string{"receipt_date"},
			[]string{req.ReceiptDate}); errMsg != "" {
			api.RespondWithError(w, http.StatusBadRequest, errMsg)
			return
		}
		if errMsg := checkFDPeriodDates(fdStart, fdMaturity, req.PeriodStart, req.PeriodEnd); errMsg != "" {
			api.RespondWithError(w, http.StatusBadRequest, errMsg)
			return
		}

		// Verify cashflow_id if provided
		if req.CashflowID != "" {
			var cfID string
			err = pool.QueryRow(ctx, `
				SELECT cashflow_id FROM investment.fd_cashflow_schedule
				WHERE cashflow_id=$1 AND fd_id=$2 AND is_deleted=false`, req.CashflowID, req.FdID).Scan(&cfID)
			if err != nil {
				api.RespondWithError(w, http.StatusBadRequest, "cashflow_id does not belong to this FD")
				return
			}
		}

		// Fetch TDS plan if available
		var tdsRate float64
		var tdsSection string
		var panNumber string
		var hasPAN *bool
		if tdsPlanID != nil && *tdsPlanID != "" {
			var tdsSecStr *string
			var hasPanVal bool
			err = pool.QueryRow(ctx, `
				SELECT tds_rate, tds_section, has_pan
				FROM investment.fd_tds_plan_master
				WHERE tds_plan_id=$1 AND is_deleted=false`, *tdsPlanID).Scan(&tdsRate, &tdsSecStr, &hasPanVal)
			if err == nil {
				if tdsSecStr != nil {
					tdsSection = *tdsSecStr
				}
				hasPAN = &hasPanVal
			}
		}
		_ = panNumber
		_ = tdsSection

		var periodStartArg, periodEndArg interface{}
		if req.PeriodStart != "" {
			periodStartArg = req.PeriodStart
		}
		if req.PeriodEnd != "" {
			periodEndArg = req.PeriodEnd
		}

		tx, err := pool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Transaction begin failed: "+err.Error())
			return
		}
		defer tx.Rollback(ctx) //nolint:errcheck

		var receiptID string
		err = tx.QueryRow(ctx, `
			INSERT INTO investment.fd_interest_receipt (
				receipt_id, fd_id, fd_ref_no, entity_id, entity_name,
				bank_id, bank_name,
				receipt_date, period_start, period_end,
				gross_interest_received, tds_amount_deducted, other_charges, net_amount_received,
				currency, bank_reference_no, narration, attachment,
				ingestion_mode,
				receipt_status, reconcile_status,
				is_active, is_deleted
			) VALUES (
				'IREC-' || UPPER(SUBSTR(REPLACE(gen_random_uuid()::TEXT,'-',''),1,7)),
				$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,'INR',$14,$15,$16,
				'MANUAL',
				'CAPTURED','PENDING',
				true,false
			) RETURNING receipt_id`,
			req.FdID, fdRefNo, entityID, entityName,
			bankID, bankName,
			receiptDate, periodStartArg, periodEndArg,
			req.GrossInterestReceived, req.TdsAmountDeducted, req.OtherCharges, net,
			nullStr(req.BankReferenceNo), nullStr(req.Narration), nullStr(req.Attachment),
		).Scan(&receiptID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Receipt insert failed: "+err.Error())
			return
		}

		// Audit
		_, err = tx.Exec(ctx, `
			INSERT INTO investment.fd_interest_receipt_audit (
				receipt_id, action_type, processing_status, requested_by, requested_at, requested_ip
			) VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now(),$3)`, receiptID, api.SystemIfBlank(userEmail), api.SystemIfBlank(api.ClientIPFromContext(ctx)))
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrAuditInsertFailed+err.Error())
			return
		}

		// Auto-create TDS row
		tdsID := ""
		if req.TdsAmountDeducted > 0 {
			tdsExpected := req.GrossInterestReceived * tdsRate / 100
			tdsVariance := req.TdsAmountDeducted - tdsExpected
			tdsRateApplied := tdsRate

			var tdsPlanIDStr interface{}
			if tdsPlanID != nil {
				tdsPlanIDStr = *tdsPlanID
			}

			err = tx.QueryRow(ctx, `
				INSERT INTO investment.fd_tds_receipt (
					tds_id, receipt_id, fd_id, fd_ref_no, entity_id, bank_id,
					ingestion_source,
					period_start, period_end, deduction_date,
					gross_interest, tds_rate_applied, tds_rate_expected,
					tds_expected, tds_deducted_actual, tds_variance,
					tds_plan_id, has_pan, tds_section,
					tds_status, exception_raised, is_active, is_deleted
				) VALUES (
					'TDSR-' || UPPER(SUBSTR(REPLACE(gen_random_uuid()::TEXT,'-',''),1,7)),
					$1,$2,$3,$4,$5,
					'INTEREST_RECEIPT',
					$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,
					'CAPTURED',false,true,false
				) RETURNING tds_id`,
				receiptID, req.FdID, fdRefNo, entityID, bankID,
				periodStartArg, periodEndArg, receiptDate,
				req.GrossInterestReceived, tdsRateApplied, tdsRateApplied,
				tdsExpected, req.TdsAmountDeducted, tdsVariance,
				tdsPlanIDStr, hasPAN, nullStr(tdsSection),
			).Scan(&tdsID)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "TDS receipt insert failed: "+err.Error())
				return
			}

			_, err = tx.Exec(ctx, `
				INSERT INTO investment.fd_tds_receipt_audit (
					tds_id, action_type, processing_status, requested_by, requested_at, requested_ip
				) VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now(),$3)`, tdsID, api.SystemIfBlank(userEmail), api.SystemIfBlank(api.ClientIPFromContext(ctx)))
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "TDS audit insert failed: "+err.Error())
				return
			}
		}

		if err = tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailed+err.Error())
			return
		}

		api.LogInfo("[FDReceipt] Created receipt_id=%s fd=%s gross=%.4f tds=%.4f", receiptID, req.FdID, req.GrossInterestReceived, req.TdsAmountDeducted)

		go func(rID, eID, uEmail string, amount float64) {
			bgCtx := context.Background()
			instID, instErr := approvalengine.CreateInstance(bgCtx, pool, approvalengine.InstanceRequest{
				ModuleCode:       "FIXED_DEPOSIT",
				EntityCode:       eID,
				TransactionType:  "FD_RECEIPT_CREATE",
				RecordID:         rID,
				RecordTable:      constants.QuerryInterestReceipt,
				AuditTable:       constants.QuerryAuditInterestReceipt,
				AuditIDColumn:    "receipt_id",
				ActionType:       "CREATE",
				Amount:           amount,
				SubmittedBy:      req.UserID,
				SubmittedByEmail: uEmail,
			})
			if instErr != nil {
				api.LogError("[FDReceipt] CreateInstance CREATE failed: %v", instErr)
			} else if instID != "" {
				api.LogInfo("[FDReceipt] CreateInstance CREATE created instance=%s", instID)
			}
		}(receiptID, entityID, userEmail, req.GrossInterestReceived)

		go func(rID, fdID, uEmail string, gross float64) {
			notifcatalog.TriggerNotification(context.Background(), pool, "/investment/fd/receipt/create", rID, map[string]interface{}{
				"record_id":   rID,
				"fd_id":       fdID,
				"event":       "FD_RECEIPT_CAPTURED",
				"actor_email": uEmail,
				"amount":      gross,
			})
		}(receiptID, req.FdID, userEmail, req.GrossInterestReceived)

		resp := map[string]interface{}{
			"success":                 true,
			"receipt_id":              receiptID,
			"fd_id":                   req.FdID,
			"fd_ref_no":               fdRefNo,
			"gross_interest_received": req.GrossInterestReceived,
			"tds_amount_deducted":     req.TdsAmountDeducted,
			"net_amount_received":     net,
			"receipt_status":          "CAPTURED",
		}
		if tdsID != "" {
			resp["tds_id"] = tdsID
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(resp)
	}
}

// ─── HANDLER 2: UpdateReceipt ─────────────────────────────────────────────────

func UpdateReceipt(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID    string                 `json:"user_id"`
			ReceiptID string                 `json:"receipt_id"`
			Fields    map[string]interface{} `json:"fields"`
			Reason    string                 `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.ReceiptID == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrReceiptIDRequired)
			return
		}
		if len(req.Fields) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "fields must not be empty")
			return
		}
		if req.Reason == "" {
			api.RespondWithError(w, http.StatusBadRequest, "reason is required")
			return
		}
		userEmail := resolveUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()

		// Status gate
		var currentStatus string
		err := pool.QueryRow(ctx, `
			SELECT receipt_status FROM investment.fd_interest_receipt
			WHERE receipt_id=$1 AND is_deleted=false`, req.ReceiptID).Scan(&currentStatus)
		if err != nil {
			api.RespondWithError(w, http.StatusNotFound, constants.ReceiptNotFound)
			return
		}
		if currentStatus != "CAPTURED" && currentStatus != constants.StatusRejected {
			api.RespondWithError(w, http.StatusBadRequest, "Receipt can only be edited in CAPTURED or REJECTED status")
			return
		}

		// Fetch existing date context for approval engine and date validation.
		var entityID, fdIDForReceipt, bankID string
		var currentReceiptDate, currentPeriodStart, currentPeriodEnd *time.Time
		err = pool.QueryRow(ctx, `
			SELECT entity_id, fd_id, COALESCE(bank_id,''), receipt_date, period_start, period_end
			FROM investment.fd_interest_receipt
			WHERE receipt_id=$1 AND is_deleted=false`,
			req.ReceiptID).Scan(&entityID, &fdIDForReceipt, &bankID, &currentReceiptDate, &currentPeriodStart, &currentPeriodEnd)
		if err != nil {
			api.RespondWithError(w, http.StatusNotFound, constants.ReceiptNotFound)
			return
		}

		updateReceiptScope := ctxutil.FromContext(r.Context())
		if !updateReceiptScope.HasEntityAccess(entityID) {
			api.RespondWithError(w, http.StatusForbidden,
				fmt.Sprintf("Entity ID '%s' is not within your authorized access scope.", entityID))
			return
		}
		if !updateReceiptScope.HasApprovedBank(bankID) {
			api.RespondWithError(w, http.StatusForbidden,
				fmt.Sprintf("Bank '%s' is not within your approved bank scope.", bankID))
			return
		}

		// Validate updated date fields against FD's active period
		if fdIDForReceipt != "" {
			var fdStartU, fdMaturityU time.Time
			if scanErr := pool.QueryRow(ctx,
				`SELECT start_date, maturity_date FROM investment.fd_master WHERE fd_id=$1 AND is_deleted=false`,
				fdIDForReceipt).Scan(&fdStartU, &fdMaturityU); scanErr != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "FD date lookup failed: "+scanErr.Error())
				return
			}
			receiptDateForValidation := ""
			periodStartForValidation := ""
			periodEndForValidation := ""
			if currentReceiptDate != nil {
				receiptDateForValidation = currentReceiptDate.Format(constants.DateFormat)
			}
			if currentPeriodStart != nil {
				periodStartForValidation = currentPeriodStart.Format(constants.DateFormat)
			}
			if currentPeriodEnd != nil {
				periodEndForValidation = currentPeriodEnd.Format(constants.DateFormat)
			}
			for _, key := range []string{"receipt_date", "period_start", "period_end"} {
				val, ok := req.Fields[key]
				if !ok {
					continue
				}
				s, ok := val.(string)
				if !ok {
					api.RespondWithError(w, http.StatusBadRequest, key+" must be in YYYY-MM-DD format")
					return
				}
				switch key {
				case "receipt_date":
					receiptDateForValidation = s
				case "period_start":
					periodStartForValidation = s
				case "period_end":
					periodEndForValidation = s
				}
			}
			if errMsg := checkFDDates(fdStartU, fdMaturityU,
				[]string{"receipt_date"},
				[]string{receiptDateForValidation}); errMsg != "" {
				api.RespondWithError(w, http.StatusBadRequest, errMsg)
				return
			}
			if errMsg := checkFDPeriodDates(fdStartU, fdMaturityU, periodStartForValidation, periodEndForValidation); errMsg != "" {
				api.RespondWithError(w, http.StatusBadRequest, errMsg)
				return
			}
		}

		tx, err := pool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTransactionFailed)
			return
		}
		defer tx.Rollback(ctx) //nolint:errcheck

		// FOR UPDATE snapshot
		var oldStatus string
		var oldReceiptDate, oldPeriodStart, oldPeriodEnd *time.Time
		var oldGross, oldTds, oldOther, oldNet float64
		var oldBankRef, oldNarration *string
		var oldIsActive bool
		err = tx.QueryRow(ctx, `
			SELECT receipt_status, receipt_date, period_start, period_end,
			       gross_interest_received, tds_amount_deducted, other_charges,
			       net_amount_received, bank_reference_no, narration, is_active
			FROM investment.fd_interest_receipt WHERE receipt_id=$1 FOR UPDATE`, req.ReceiptID).Scan(
			&oldStatus, &oldReceiptDate, &oldPeriodStart, &oldPeriodEnd,
			&oldGross, &oldTds, &oldOther, &oldNet,
			&oldBankRef, &oldNarration, &oldIsActive)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Snapshot failed: "+err.Error())
			return
		}

		allowed := map[string]bool{
			"receipt_date": true, "period_start": true, "period_end": true,
			"gross_interest_received": true, "tds_amount_deducted": true, "other_charges": true,
			"bank_reference_no": true, "narration": true,
			"attachment": true, "is_active": true,
		}

		setClauses := []string{}
		args := []interface{}{}
		idx := 1
		needsNetRecompute := false
		newGross, newTds, newOther := oldGross, oldTds, oldOther

		for key, val := range req.Fields {
			if !allowed[key] {
				continue
			}
			switch key {
			case "gross_interest_received":
				if v, ok := toFloat64(val); ok {
					newGross = v
					needsNetRecompute = true
				}
			case "tds_amount_deducted":
				if v, ok := toFloat64(val); ok {
					newTds = v
					needsNetRecompute = true
				}
			case "other_charges":
				if v, ok := toFloat64(val); ok {
					newOther = v
					needsNetRecompute = true
				}
			}
			setClauses = append(setClauses, fmt.Sprintf("%s=$%d", key, idx))
			args = append(args, val)
			idx++
		}
		if needsNetRecompute {
			newNet := newGross - newTds - newOther
			setClauses = append(setClauses, fmt.Sprintf("net_amount_received=$%d", idx))
			args = append(args, newNet)
			idx++
		}
		if len(setClauses) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "no valid updatable fields provided")
			return
		}
		args = append(args, req.ReceiptID)
		updateSQL := fmt.Sprintf("UPDATE investment.fd_interest_receipt SET %s WHERE receipt_id=$%d",
			strings.Join(setClauses, ","), idx)

		if _, err = tx.Exec(ctx, updateSQL, args...); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrUpdateFailed+err.Error())
			return
		}

		// Audit with all old_* fields
		_, err = tx.Exec(ctx, `
			INSERT INTO investment.fd_interest_receipt_audit (
				receipt_id, action_type, processing_status, reason, requested_by, requested_at, requested_ip,
				old_receipt_status, old_receipt_date, old_period_start, old_period_end,
				old_gross_interest_received, old_tds_amount_deducted, old_other_charges,
				old_net_amount_received, old_bank_reference_no, old_narration,
				old_is_active
			) VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL',$2,$3,now(),$4,
				$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`,
			req.ReceiptID, req.Reason, api.SystemIfBlank(userEmail), api.SystemIfBlank(api.ClientIPFromContext(ctx)),
			oldStatus, oldReceiptDate, oldPeriodStart, oldPeriodEnd,
			oldGross, oldTds, oldOther, oldNet,
			oldBankRef, oldNarration, oldIsActive)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrAuditInsertFailed+err.Error())
			return
		}

		if err = tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailed+err.Error())
			return
		}

		go func() {
			bgCtx := context.Background()
			instID, instErr := approvalengine.CreateInstance(bgCtx, pool, approvalengine.InstanceRequest{
				ModuleCode:       "FIXED_DEPOSIT",
				EntityCode:       entityID,
				TransactionType:  "FD_RECEIPT_EDIT",
				RecordID:         req.ReceiptID,
				RecordTable:      constants.QuerryInterestReceipt,
				AuditTable:       constants.QuerryAuditInterestReceipt,
				AuditIDColumn:    "receipt_id",
				ActionType:       "EDIT",
				Amount:           newGross,
				SubmittedBy:      req.UserID,
				SubmittedByEmail: userEmail,
			})
			if instErr != nil {
				api.LogError("[FDReceipt] CreateInstance EDIT failed: %v", instErr)
			} else if instID != "" {
				api.LogInfo("[FDReceipt] CreateInstance EDIT created instance=%s", instID)
			}
		}()

		go func(rID, uEmail string, amount float64) {
			notifcatalog.TriggerNotification(context.Background(), pool, "/investment/fd/receipt/update", rID, map[string]interface{}{
				"record_id":   rID,
				"event":       "FD_RECEIPT_EDIT_SUBMITTED",
				"actor_email": uEmail,
				"amount":      amount,
			})
		}(req.ReceiptID, userEmail, newGross)

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":    true,
			"receipt_id": req.ReceiptID,
			"updated_by": userEmail,
		})
	}
}

// ─── HANDLER 3: DeleteReceipt ─────────────────────────────────────────────────

func DeleteReceipt(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID     string   `json:"user_id"`
			ReceiptIDs []string `json:"receipt_ids"`
			Reason     string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		userEmail := resolveUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		results := make([]map[string]interface{}, 0, len(req.ReceiptIDs))
		validIDs := []string{}

		for _, rid := range req.ReceiptIDs {
			var status string
			err := pool.QueryRow(ctx, `
				SELECT receipt_status FROM investment.fd_interest_receipt
				WHERE receipt_id=$1 AND is_deleted=false`, rid).Scan(&status)
			if err != nil {
				results = append(results, map[string]interface{}{"receipt_id": rid, "success": false, "error": "not found"})
				continue
			}
			if status != "CAPTURED" && status != constants.StatusRejected {
				results = append(results, map[string]interface{}{"receipt_id": rid, "success": false, "error": "Cannot delete receipt in status " + status})
				continue
			}
			validIDs = append(validIDs, rid)
			results = append(results, map[string]interface{}{"receipt_id": rid, "success": true})
		}

		if len(validIDs) > 0 {
			tx, err := pool.Begin(ctx)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTransactionFailed)
				return
			}
			defer tx.Rollback(ctx) //nolint:errcheck

			for _, rid := range validIDs {
				_, err = tx.Exec(ctx, `
					INSERT INTO investment.fd_interest_receipt_audit (
						receipt_id, action_type, processing_status, reason, requested_by, requested_at, requested_ip
					) VALUES ($1,'DELETE','PENDING_DELETE_APPROVAL',$2,$3,now(),$4)`, rid, req.Reason, api.SystemIfBlank(userEmail), api.SystemIfBlank(api.ClientIPFromContext(ctx)))
				if err != nil {
					api.RespondWithError(w, http.StatusInternalServerError, constants.ErrAuditInsertFailed+err.Error())
					return
				}
			}

			if err = tx.Commit(ctx); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailed+err.Error())
				return
			}

			for _, rid := range validIDs {
				rid := rid
				go func() {
					bgCtx := context.Background()
					// Fetch entity_id for this receipt
					var eID string
					var amount float64
					pool.QueryRow(bgCtx, `SELECT COALESCE(entity_id,''), COALESCE(gross_interest_received, 0) FROM investment.fd_interest_receipt WHERE receipt_id=$1`, rid).Scan(&eID, &amount) //nolint:errcheck
					instID, instErr := approvalengine.CreateInstance(bgCtx, pool, approvalengine.InstanceRequest{
						ModuleCode:       "FIXED_DEPOSIT",
						EntityCode:       eID,
						TransactionType:  "FD_RECEIPT_DELETE",
						RecordID:         rid,
						RecordTable:      constants.QuerryInterestReceipt,
						AuditTable:       constants.QuerryAuditInterestReceipt,
						AuditIDColumn:    "receipt_id",
						ActionType:       "DELETE",
						Amount:           amount,
						SubmittedBy:      req.UserID,
						SubmittedByEmail: userEmail,
					})
					if instErr != nil {
						api.LogError("[FDReceipt] CreateInstance DELETE failed: %v", instErr)
					} else if instID != "" {
						api.LogInfo("[FDReceipt] CreateInstance DELETE created instance=%s", instID)
					}
				}()

				go func(rID, uEmail string) {
					notifcatalog.TriggerNotification(context.Background(), pool, "/investment/fd/receipt/delete", rID, map[string]interface{}{
						"record_id":   rID,
						"event":       "FD_RECEIPT_DELETE_SUBMITTED",
						"actor_email": uEmail,
					})
				}(rid, userEmail)
			}
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"results": results,
		})
	}
}

// ─── HANDLER 4: SubmitReceiptForApproval ──────────────────────────────────────
// Deprecated: In the new schema there is no SUBMIT step — CREATE already sets
// processing_status = PENDING_APPROVAL. This handler is kept for backward
// compatibility but is a no-op: it just confirms the receipt exists and is CAPTURED.

func SubmitReceiptForApproval(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID    string `json:"user_id"`
			ReceiptID string `json:"receipt_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.ReceiptID == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrReceiptIDRequired)
			return
		}
		userEmail := resolveUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		var receiptStatus string
		err := pool.QueryRow(ctx,
			`SELECT receipt_status FROM investment.fd_interest_receipt WHERE receipt_id=$1 AND is_deleted=false`,
			req.ReceiptID).Scan(&receiptStatus)
		if err != nil {
			api.RespondWithError(w, http.StatusNotFound, constants.ReceiptNotFound)
			return
		}

		// New schema: CREATE already inserts audit with PENDING_APPROVAL — no separate submit needed.
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":        true,
			"receipt_id":     req.ReceiptID,
			"receipt_status": receiptStatus,
			"message":        "Receipt is already in approval queue from creation. No separate submit step required.",
		})
	}
}

// ─── HANDLER 5: BulkApproveReceipt ───────────────────────────────────────────

func BulkApproveReceipt(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID     string   `json:"user_id"`
			ReceiptIDs []string `json:"receipt_ids"`
			Comment    string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		userEmail := resolveUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		directReceiptIDs := make([]string, 0, len(req.ReceiptIDs))
		engineActed := 0
		var approvalErrors []string
		for _, receiptID := range req.ReceiptIDs {
			actionRes, actionErr := approvalengine.ActOnPendingOrDiagnose(ctx, pool, approvalengine.ActOnPendingRequest{ModuleCode: "FIXED_DEPOSIT", RecordID: receiptID, UserID: req.UserID, UserEmail: userEmail, RoleID: "", Action: approvalengine.ActionApproved, Comment: req.Comment})
			if actionErr != nil {
				approvalErrors = append(approvalErrors, receiptID+": "+actionErr.Error())
				continue
			}
			if actionRes.Acted {
				engineActed++
				continue
			}
			if actionRes.CancelledStale {
				api.LogInfo("[FDReceipt] Cancelled stale approval instance for receipt=%s: %s", receiptID, actionRes.Reason)
			} else if actionRes.Reason != "" {
				approvalErrors = append(approvalErrors, receiptID+": "+actionRes.Reason)
				continue
			}
			directReceiptIDs = append(directReceiptIDs, receiptID)
		}
		if len(directReceiptIDs) == 0 {
			success := engineActed > 0
			resp := map[string]interface{}{
				"success":        engineActed > 0,
				"approved_count": engineActed,
				"engine_acted":   engineActed,
				"direct_acted":   0,
				"errors":         approvalErrors,
				"checker":        userEmail,
			}
			if !success {
				resp["error"] = api.BulkActionErrorMessage("No receipts were approved", approvalErrors)
			}
			w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
			json.NewEncoder(w).Encode(resp)
			for _, rID := range req.ReceiptIDs {
				go func(id, uEmail string) {
					notifcatalog.TriggerNotification(context.Background(), pool, "/investment/fd/receipt/bulk-approve", id, map[string]interface{}{
						"record_id":   id,
						"event":       "FD_RECEIPT_APPROVED",
						"actor_email": uEmail,
					})
				}(rID, userEmail)
			}
			return
		}
		tx, err := pool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTransactionFailed)
			return
		}
		defer tx.Rollback(ctx) //nolint:errcheck

		// Step 1: Approve only the latest pending audit row per receipt.
		_, err = tx.Exec(ctx, `
			WITH latest_pending AS (
				SELECT DISTINCT ON (a.receipt_id)
					a.audit_id
				FROM investment.fd_interest_receipt_audit a
				WHERE a.receipt_id = ANY($3)
				  AND a.processing_status IN ('PENDING_APPROVAL', 'PENDING_EDIT_APPROVAL', 'PENDING_DELETE_APPROVAL')
				ORDER BY a.receipt_id,
					GREATEST(
						COALESCE(a.requested_at,'1970-01-01'::timestamptz),
						COALESCE(a.checker_at,'1970-01-01'::timestamptz)
					) DESC,
					a.audit_id DESC
			)
			UPDATE investment.fd_interest_receipt_audit a
			SET processing_status='APPROVED', checker_by=$1,
			    checker_at=now(), checker_comment=$2, checker_ip=$4
			FROM latest_pending lp
			WHERE a.audit_id = lp.audit_id`, api.SystemIfBlank(userEmail), req.Comment, directReceiptIDs, api.SystemIfBlank(api.ClientIPFromContext(ctx)))
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Audit update failed: "+err.Error())
			return
		}

		// Step 2: Receipt status
		_, err = tx.Exec(ctx, `
			WITH latest_actions AS (
				SELECT DISTINCT ON (a.receipt_id)
					a.receipt_id, a.action_type, a.processing_status
				FROM investment.fd_interest_receipt_audit a
				WHERE a.receipt_id = ANY($1)
				ORDER BY a.receipt_id,
					GREATEST(
						COALESCE(a.requested_at,'1970-01-01'::timestamptz),
						COALESCE(a.checker_at,'1970-01-01'::timestamptz)
					) DESC,
					a.audit_id DESC
			)
			UPDATE investment.fd_interest_receipt r
			SET receipt_status='APPROVED'
			FROM latest_actions la
			WHERE r.receipt_id = la.receipt_id
			  AND r.receipt_id = ANY($1)
			  AND la.action_type <> 'DELETE'
			  AND la.processing_status = 'APPROVED'
			  AND r.receipt_status='CAPTURED'`, directReceiptIDs)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Receipt update failed: "+err.Error())
			return
		}

		// Step 2b: Approve TDS audit rows for these receipts.
		// fd_tds_receipt_audit is keyed by tds_id (not receipt_id) — join through fd_tds_receipt.
		_, err = tx.Exec(ctx, `
			WITH latest_pending_tds AS (
				SELECT DISTINCT ON (a.tds_id)
					a.audit_id
				FROM investment.fd_tds_receipt_audit a
				JOIN investment.fd_tds_receipt t ON t.tds_id = a.tds_id
				WHERE t.receipt_id = ANY($3)
				  AND a.processing_status IN ('PENDING_APPROVAL', 'PENDING_EDIT_APPROVAL', 'PENDING_DELETE_APPROVAL')
				ORDER BY a.tds_id,
					GREATEST(
						COALESCE(a.requested_at,'1970-01-01'::timestamptz),
						COALESCE(a.checker_at,'1970-01-01'::timestamptz)
					) DESC,
					a.audit_id DESC
			)
			UPDATE investment.fd_tds_receipt_audit a
			SET processing_status='APPROVED', checker_by=$1,
			    checker_at=now(), checker_comment=$2, checker_ip=$4
			FROM latest_pending_tds lp
			WHERE a.audit_id = lp.audit_id`, api.SystemIfBlank(userEmail), req.Comment, directReceiptIDs, api.SystemIfBlank(api.ClientIPFromContext(ctx)))
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "TDS audit update failed: "+err.Error())
			return
		}

		// Step 2c: Update TDS status to APPROVED
		_, err = tx.Exec(ctx, `
			WITH latest_tds_actions AS (
				SELECT DISTINCT ON (a.tds_id)
					a.tds_id, a.action_type, a.processing_status
				FROM investment.fd_tds_receipt_audit a
				JOIN investment.fd_tds_receipt t ON t.tds_id = a.tds_id
				WHERE t.receipt_id = ANY($1)
				ORDER BY a.tds_id,
					GREATEST(
						COALESCE(a.requested_at,'1970-01-01'::timestamptz),
						COALESCE(a.checker_at,'1970-01-01'::timestamptz)
					) DESC,
					a.audit_id DESC
			)
			UPDATE investment.fd_tds_receipt t
			SET tds_status='APPROVED'
			FROM latest_tds_actions lta
			WHERE t.tds_id = lta.tds_id
			  AND t.receipt_id = ANY($1)
			  AND lta.action_type <> 'DELETE'
			  AND lta.processing_status = 'APPROVED'
			  AND t.tds_status='CAPTURED'`, directReceiptIDs)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "TDS status update failed: "+err.Error())
			return
		}

		// Step 3: Flip is_deleted for approved DELETEs
		_, err = tx.Exec(ctx, `
			UPDATE investment.fd_interest_receipt SET is_deleted=true
			WHERE receipt_id IN (
				SELECT DISTINCT a.receipt_id FROM investment.fd_interest_receipt_audit a
				WHERE a.receipt_id=ANY($1) AND a.action_type='DELETE'
				  AND a.processing_status='APPROVED'
			)`, directReceiptIDs)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Delete flip failed: "+err.Error())
			return
		}

		if err = tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailed+err.Error())
			return
		}

		api.LogInfo("[FDReceipt] BulkApprove: direct=%d engine=%d errors=%d by=%s", len(directReceiptIDs), engineActed, len(approvalErrors), userEmail)

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":        true,
			"approved_count": len(directReceiptIDs) + engineActed,
			"engine_acted":   engineActed,
			"direct_acted":   len(directReceiptIDs),
			"errors":         approvalErrors,
			"checker":        userEmail,
		})
		for _, rID := range req.ReceiptIDs {
			go func(id, uEmail string) {
				notifcatalog.TriggerNotification(context.Background(), pool, "/investment/fd/receipt/bulk-approve", id, map[string]interface{}{
					"record_id":   id,
					"event":       "FD_RECEIPT_APPROVED",
					"actor_email": uEmail,
				})
			}(rID, userEmail)
		}
	}
}

// ─── HANDLER 6: BulkRejectReceipt ────────────────────────────────────────────

func BulkRejectReceipt(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID     string   `json:"user_id"`
			ReceiptIDs []string `json:"receipt_ids"`
			Comment    string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		userEmail := resolveUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		directReceiptIDs := make([]string, 0, len(req.ReceiptIDs))
		engineActed := 0
		var approvalErrors []string
		for _, receiptID := range req.ReceiptIDs {
			actionRes, actionErr := approvalengine.ActOnPendingOrDiagnose(ctx, pool, approvalengine.ActOnPendingRequest{ModuleCode: "FIXED_DEPOSIT", RecordID: receiptID, UserID: req.UserID, UserEmail: userEmail, RoleID: "", Action: approvalengine.ActionRejected, Comment: req.Comment})
			if actionErr != nil {
				approvalErrors = append(approvalErrors, receiptID+": "+actionErr.Error())
				continue
			}
			if actionRes.Acted {
				engineActed++
				continue
			}
			if actionRes.CancelledStale {
				api.LogInfo("[FDReceipt] Cancelled stale approval instance for receipt=%s: %s", receiptID, actionRes.Reason)
			} else if actionRes.Reason != "" {
				approvalErrors = append(approvalErrors, receiptID+": "+actionRes.Reason)
				continue
			}
			directReceiptIDs = append(directReceiptIDs, receiptID)
		}
		if len(directReceiptIDs) == 0 {
			success := engineActed > 0
			resp := map[string]interface{}{
				"success":        engineActed > 0,
				"rejected_count": engineActed,
				"engine_acted":   engineActed,
				"direct_acted":   0,
				"errors":         approvalErrors,
				"checker":        userEmail,
			}
			if !success {
				resp["error"] = api.BulkActionErrorMessage("No receipts were rejected", approvalErrors)
			}
			w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
			json.NewEncoder(w).Encode(resp)
			for _, rID := range req.ReceiptIDs {
				go func(id, uEmail string) {
					notifcatalog.TriggerNotification(context.Background(), pool, "/investment/fd/receipt/bulk-reject", id, map[string]interface{}{
						"record_id":   id,
						"event":       "FD_RECEIPT_REJECTED",
						"actor_email": uEmail,
					})
				}(rID, userEmail)
			}
			return
		}
		tx, err := pool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTransactionFailed)
			return
		}
		defer tx.Rollback(ctx) //nolint:errcheck

		_, err = tx.Exec(ctx, `
			WITH latest_pending AS (
				SELECT DISTINCT ON (a.receipt_id)
					a.audit_id
				FROM investment.fd_interest_receipt_audit a
				WHERE a.receipt_id = ANY($3)
				  AND a.processing_status IN ('PENDING_APPROVAL', 'PENDING_EDIT_APPROVAL', 'PENDING_DELETE_APPROVAL')
				ORDER BY a.receipt_id,
					GREATEST(
						COALESCE(a.requested_at,'1970-01-01'::timestamptz),
						COALESCE(a.checker_at,'1970-01-01'::timestamptz)
					) DESC,
					a.audit_id DESC
			)
			UPDATE investment.fd_interest_receipt_audit a
			SET processing_status='REJECTED', checker_by=$1,
			    checker_at=now(), checker_comment=$2, checker_ip=$4
			FROM latest_pending lp
			WHERE a.audit_id = lp.audit_id`, api.SystemIfBlank(userEmail), req.Comment, directReceiptIDs, api.SystemIfBlank(api.ClientIPFromContext(ctx)))
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Audit update failed: "+err.Error())
			return
		}

		_, err = tx.Exec(ctx, `
			WITH latest_actions AS (
				SELECT DISTINCT ON (a.receipt_id)
					a.receipt_id, a.action_type, a.processing_status
				FROM investment.fd_interest_receipt_audit a
				WHERE a.receipt_id = ANY($1)
				ORDER BY a.receipt_id,
					GREATEST(
						COALESCE(a.requested_at,'1970-01-01'::timestamptz),
						COALESCE(a.checker_at,'1970-01-01'::timestamptz)
					) DESC,
					a.audit_id DESC
			)
			UPDATE investment.fd_interest_receipt r
			SET receipt_status='REJECTED'
			FROM latest_actions la
			WHERE r.receipt_id = la.receipt_id
			  AND r.receipt_id = ANY($1)
			  AND la.action_type <> 'DELETE'
			  AND la.processing_status = 'REJECTED'
			  AND r.receipt_status='CAPTURED'`, directReceiptIDs)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Receipt update failed: "+err.Error())
			return
		}

		if err = tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailed+err.Error())
			return
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":        true,
			"rejected_count": len(directReceiptIDs) + engineActed,
			"engine_acted":   engineActed,
			"direct_acted":   len(directReceiptIDs),
			"errors":         approvalErrors,
			"checker":        userEmail,
		})
		for _, rID := range req.ReceiptIDs {
			go func(id, uEmail string) {
				notifcatalog.TriggerNotification(context.Background(), pool, "/investment/fd/receipt/bulk-reject", id, map[string]interface{}{
					"record_id":   id,
					"event":       "FD_RECEIPT_REJECTED",
					"actor_email": uEmail,
				})
			}(rID, userEmail)
		}
	}
}

// ─── HANDLER 7: GetReceiptsWithAudit ─────────────────────────────────────────

func GetReceiptsWithAudit(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID        string `json:"user_id"`
			FdID          string `json:"fd_id"`
			EntityID      string `json:"entity_id"`
			BankID        string `json:"bank_id"`
			ReceiptStatus string `json:"receipt_status"`
			FromDate      string `json:"from_date"`
			ToDate        string `json:"to_date"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		userEmail := resolveUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		baseSQL := `
WITH latest_audit AS (
  SELECT DISTINCT ON (a.receipt_id)
    a.receipt_id, a.action_type, a.processing_status,
    a.audit_id, a.requested_by, a.requested_at, a.requested_ip,
    a.checker_by, a.checker_at, a.checker_ip, a.checker_comment, a.reason,
    a.old_receipt_status, a.old_gross_interest_received,
    a.old_tds_amount_deducted, a.old_net_amount_received,
    a.old_receipt_date
  FROM investment.fd_interest_receipt_audit a
  ORDER BY a.receipt_id,
    GREATEST(
      COALESCE(a.requested_at,'1970-01-01'::timestamptz),
      COALESCE(a.checker_at,'1970-01-01'::timestamptz)
    ) DESC,
    a.audit_id DESC
),
history AS (
  SELECT
    receipt_id,
    MAX(CASE WHEN action_type='CREATE' THEN requested_by END)                                   AS created_by_audit,
    MAX(CASE WHEN action_type='CREATE' THEN TO_CHAR((requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS') END) AS created_at_audit,
    MAX(CASE WHEN action_type='EDIT'   THEN requested_by END)                                   AS edited_by,
    MAX(CASE WHEN action_type='EDIT'   THEN TO_CHAR((requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS') END) AS edited_at,
    MAX(CASE WHEN action_type='DELETE' THEN requested_by END)                                   AS deleted_by,
    MAX(CASE WHEN action_type='DELETE' THEN TO_CHAR((requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS') END) AS deleted_at
  FROM investment.fd_interest_receipt_audit
  GROUP BY receipt_id
)
SELECT
  r.receipt_id,
  COALESCE(r.fd_id,'')                AS fd_id,
  COALESCE(r.fd_ref_no,'')            AS fd_ref_no,
  COALESCE(r.entity_id,'')            AS entity_id,
  COALESCE(r.entity_name,'')          AS entity_name,
  COALESCE(r.bank_id,'')              AS bank_id,
  COALESCE(r.bank_name,'')            AS bank_name,
	''                                  AS cashflow_id,
  TO_CHAR(r.receipt_date,'YYYY-MM-DD') AS receipt_date,
  TO_CHAR(r.period_start,'YYYY-MM-DD') AS period_start,
  TO_CHAR(r.period_end,'YYYY-MM-DD')   AS period_end,
  COALESCE(r.gross_interest_received,0) AS gross_interest_received,
  COALESCE(r.tds_amount_deducted,0)    AS tds_amount_deducted,
  COALESCE(r.other_charges,0)          AS other_charges,
  COALESCE(r.net_amount_received,0)    AS net_amount_received,
  COALESCE(r.currency,'INR')           AS currency,
  COALESCE(r.bank_reference_no,'')     AS bank_reference_no,
  COALESCE(r.narration,'')             AS narration,
  COALESCE(r.receipt_status,'')        AS receipt_status,
  COALESCE(r.reconcile_status,'')      AS reconcile_status,
  COALESCE(r.reconcile_run_id,'')      AS reconcile_run_id,
  COALESCE(r.journal_entry_id,'')      AS journal_entry_id,
  COALESCE(r.is_active,true)           AS is_active,
  'INTEREST'                           AS receipt_type,
  COALESCE(l.processing_status,'')     AS processing_status,
  COALESCE(l.action_type,'')           AS action_type,
  COALESCE(l.audit_id::text,'')        AS audit_id,
  COALESCE(l.requested_by,'')          AS requested_by,
  COALESCE(TO_CHAR((l.requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'),'') AS requested_at,
  COALESCE(l.requested_ip,'')          AS requested_ip,
  COALESCE(l.checker_by,'')            AS checker_by,
  COALESCE(TO_CHAR((l.checker_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'),'')   AS checker_at,
  COALESCE(l.checker_ip,'')            AS checker_ip,
  COALESCE(l.checker_comment,'')       AS checker_comment,
  COALESCE(l.reason,'')                AS reason,
  COALESCE(l.old_receipt_status,'')    AS old_receipt_status,
  COALESCE(l.old_gross_interest_received,0) AS old_gross_interest_received,
  COALESCE(l.old_tds_amount_deducted,0)     AS old_tds_amount_deducted,
  COALESCE(l.old_net_amount_received,0)     AS old_net_amount_received,
  COALESCE(h.created_by_audit,'')      AS created_by_audit,
  COALESCE(h.created_at_audit,'')      AS created_at_audit,
  COALESCE(h.edited_by,'')             AS edited_by,
  COALESCE(h.edited_at,'')             AS edited_at,
  COALESCE(h.deleted_by,'')            AS deleted_by,
  COALESCE(h.deleted_at,'')            AS deleted_at,
  COALESCE(ai.instance_id,'')          AS approval_instance_id,
  COALESCE(ai.status,'')               AS approval_engine_status,
  COALESCE(aie.instance_eye_id,'')     AS current_eye_id,
  COALESCE(aie.position::text,'')      AS current_eye_position,
  COALESCE(aie.approvals_required,0)   AS approvals_required,
  COALESCE(aie.approvals_received,0)   AS approvals_received,
  aie.sla_deadline,
  COALESCE(aie.is_escalated,false)     AS is_escalated
FROM investment.fd_interest_receipt r
LEFT JOIN latest_audit l ON l.receipt_id = r.receipt_id
LEFT JOIN history h ON h.receipt_id = r.receipt_id
LEFT JOIN uam.approval_instance ai
  ON ai.record_id = r.receipt_id
  AND ai.module_code = 'FIXED_DEPOSIT'
  AND ai.status = 'PENDING'
  AND ai.is_deleted = false
LEFT JOIN uam.approval_instance_eye aie
  ON aie.instance_id = ai.instance_id AND aie.status = 'ACTIVE'
WHERE r.is_deleted = false`

		args := []interface{}{}
		argIdx := 1

		if req.FdID != "" {
			baseSQL += fmt.Sprintf(" AND r.fd_id=$%d", argIdx)
			args = append(args, req.FdID)
			argIdx++
		}
		if !appendFDReceiptScopeFilters(ctx, w, &baseSQL, &args, &argIdx, "r.entity_id", "r.bank_id", req.EntityID, req.BankID) {
			return
		}
		if req.ReceiptStatus != "" {
			baseSQL += fmt.Sprintf(" AND r.receipt_status=$%d", argIdx)
			args = append(args, req.ReceiptStatus)
			argIdx++
		}
		if req.FromDate != "" {
			baseSQL += fmt.Sprintf(" AND r.receipt_date>=$%d", argIdx)
			args = append(args, req.FromDate)
			argIdx++
		}
		if req.ToDate != "" {
			baseSQL += fmt.Sprintf(" AND r.receipt_date<=$%d", argIdx)
			args = append(args, req.ToDate)
			argIdx++
		}

		baseSQL += ` ORDER BY GREATEST(
  COALESCE(l.requested_at,'1970-01-01'::timestamptz),
  COALESCE(l.checker_at,'1970-01-01'::timestamptz)) DESC`

		rows, err := pool.Query(ctx, baseSQL, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()
		out, err := rowsToMapSlice(rows)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Scan failed: "+err.Error())
			return
		}
		if rows.Err() != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Rows error: "+rows.Err().Error())
			return
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"rows":    out,
			"count":   len(out),
		})
	}
}

// GetApprovedActiveReceipts returns receipts whose latest audit row is APPROVED
// Useful for listing receipts that have completed approval and are active.
func GetApprovedActiveReceipts(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string `json:"user_id"`
			EntityID string `json:"entity_id"`
			FDID     string `json:"fd_id"`
			BankID   string `json:"bank_id"`
		}
		// Accept filters from either query-string or JSON body
		_ = json.NewDecoder(r.Body).Decode(&req) // ignore error; fall back to query params
		if req.UserID == "" {
			req.UserID = r.URL.Query().Get("user_id")
		}
		if req.EntityID == "" {
			req.EntityID = r.URL.Query().Get("entity_id")
		}
		if req.FDID == "" {
			req.FDID = r.URL.Query().Get("fd_id")
		}
		if req.BankID == "" {
			req.BankID = r.URL.Query().Get("bank_id")
		}

		ctx := r.Context()
		baseSQL := `
WITH latest_audit AS (
  SELECT DISTINCT ON (a.receipt_id)
	a.receipt_id, a.action_type, a.requested_by, a.requested_at, a.checker_by, a.checker_at, a.processing_status
  FROM investment.fd_interest_receipt_audit a
  WHERE a.processing_status = 'APPROVED'
  ORDER BY a.receipt_id,
	GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamptz), COALESCE(a.checker_at,'1970-01-01'::timestamptz)) DESC,
	a.audit_id DESC
),
history AS (
  SELECT
    receipt_id,
    MAX(CASE WHEN action_type='CREATE' THEN requested_by END)                                   AS created_by_audit,
    MAX(CASE WHEN action_type='CREATE' THEN TO_CHAR((requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS') END) AS created_at_audit,
    MAX(CASE WHEN action_type='EDIT'   THEN requested_by END)                                   AS edited_by,
    MAX(CASE WHEN action_type='EDIT'   THEN TO_CHAR((requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS') END) AS edited_at,
    MAX(CASE WHEN action_type='DELETE' THEN requested_by END)                                   AS deleted_by,
    MAX(CASE WHEN action_type='DELETE' THEN TO_CHAR((requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS') END) AS deleted_at
  FROM investment.fd_interest_receipt_audit
  GROUP BY receipt_id
)
SELECT
  r.receipt_id,
  COALESCE(r.fd_id,'')                AS fd_id,
  COALESCE(r.fd_ref_no,'')            AS fd_ref_no,
  COALESCE(r.entity_id,'')            AS entity_id,
  COALESCE(r.entity_name,'')          AS entity_name,
  COALESCE(r.bank_id,'')              AS bank_id,
  COALESCE(r.bank_name,'')            AS bank_name,
  TO_CHAR(r.receipt_date,'YYYY-MM-DD') AS receipt_date,
  TO_CHAR(r.period_start,'YYYY-MM-DD') AS period_start,
  TO_CHAR(r.period_end,'YYYY-MM-DD')   AS period_end,
  COALESCE(r.gross_interest_received,0) AS gross_interest_received,
  COALESCE(r.tds_amount_deducted,0)    AS tds_amount_deducted,
  COALESCE(r.net_amount_received,0)    AS net_amount_received,
  COALESCE(r.receipt_status,'')        AS receipt_status,
  COALESCE(r.reconcile_status,'')      AS reconcile_status,
  COALESCE(r.reconcile_run_id,'')      AS reconcile_run_id,
  COALESCE(l.requested_by,'')          AS requested_by,
  COALESCE(TO_CHAR((l.requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'),'') AS requested_at,
  COALESCE(l.checker_by,'')            AS checker_by,
  COALESCE(TO_CHAR((l.checker_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'),'')   AS checker_at,
  COALESCE(l.processing_status,'')     AS processing_status,
  COALESCE(h.created_by_audit,'')      AS created_by_audit,
  COALESCE(h.created_at_audit,'')      AS created_at_audit,
  COALESCE(h.edited_by,'')             AS edited_by,
  COALESCE(h.edited_at,'')             AS edited_at,
  COALESCE(h.deleted_by,'')            AS deleted_by,
  COALESCE(h.deleted_at,'')            AS deleted_at
FROM investment.fd_interest_receipt r
LEFT JOIN latest_audit l ON l.receipt_id = r.receipt_id
LEFT JOIN history h ON h.receipt_id = r.receipt_id
WHERE r.is_deleted = false AND l.processing_status = 'APPROVED'`

		args := []interface{}{}
		argIdx := 1
		if req.FDID != "" {
			baseSQL += fmt.Sprintf(" AND r.fd_id = $%d", argIdx)
			args = append(args, req.FDID)
			argIdx++
		}
		if !appendFDReceiptScopeFilters(ctx, w, &baseSQL, &args, &argIdx, "r.entity_id", "r.bank_id", req.EntityID, req.BankID) {
			return
		}

		baseSQL += ` ORDER BY GREATEST(COALESCE(l.requested_at,'1970-01-01'::timestamptz), COALESCE(l.checker_at,'1970-01-01'::timestamptz)) DESC`

		rows, err := pool.Query(ctx, baseSQL, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		out, err := rowsToMapSlice(rows)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrRowError+err.Error())
			return
		}
		if out == nil {
			out = []map[string]interface{}{}
		}

		// Return consistent list shape: { success, count, rows }
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"count":   len(out),
			"rows":    out,
		})
	}
}

// GetApprovedActiveTDS returns TDS receipts whose latest audit row is APPROVED
func GetApprovedActiveTDS(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string `json:"user_id"`
			EntityID string `json:"entity_id"`
			FDID     string `json:"fd_id"`
			BankID   string `json:"bank_id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.UserID == "" {
			req.UserID = r.URL.Query().Get("user_id")
		}
		if req.EntityID == "" {
			req.EntityID = r.URL.Query().Get("entity_id")
		}
		if req.FDID == "" {
			req.FDID = r.URL.Query().Get("fd_id")
		}
		if req.BankID == "" {
			req.BankID = r.URL.Query().Get("bank_id")
		}

		ctx := r.Context()
		baseSQL := `
WITH latest_audit AS (
  SELECT DISTINCT ON (a.tds_id)
	a.tds_id, a.action_type, a.requested_by, a.requested_at, a.checker_by, a.checker_at, a.processing_status
  FROM investment.fd_tds_receipt_audit a
  WHERE a.processing_status = 'APPROVED'
  ORDER BY a.tds_id,
	GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamptz), COALESCE(a.checker_at,'1970-01-01'::timestamptz)) DESC,
	a.audit_id DESC
),
history AS (
  SELECT
    tds_id,
    MAX(CASE WHEN action_type='CREATE' THEN requested_by END)                                   AS created_by_audit,
    MAX(CASE WHEN action_type='CREATE' THEN TO_CHAR((requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS') END) AS created_at_audit,
    MAX(CASE WHEN action_type='EDIT'   THEN requested_by END)                                   AS edited_by,
    MAX(CASE WHEN action_type='EDIT'   THEN TO_CHAR((requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS') END) AS edited_at,
    MAX(CASE WHEN action_type='DELETE' THEN requested_by END)                                   AS deleted_by,
    MAX(CASE WHEN action_type='DELETE' THEN TO_CHAR((requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS') END) AS deleted_at
  FROM investment.fd_tds_receipt_audit
  GROUP BY tds_id
)
SELECT
  t.tds_id,
  COALESCE(t.fd_id,'')                AS fd_id,
  COALESCE(t.fd_ref_no,'')            AS fd_ref_no,
  COALESCE(t.entity_id,'')            AS entity_id,
  COALESCE(m.entity_name,'')          AS entity_name,
  COALESCE(t.bank_id,'')              AS bank_id,
  COALESCE(m.bank_name,'')            AS bank_name,
  TO_CHAR(t.deduction_date,'YYYY-MM-DD') AS deduction_date,
  COALESCE(t.tds_deducted_actual,0)    AS actual_amount,
  COALESCE(t.tds_status,'')            AS tds_status,
  COALESCE(l.requested_by,'')          AS requested_by,
  COALESCE(TO_CHAR((l.requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'),'') AS requested_at,
  COALESCE(l.checker_by,'')            AS checker_by,
  COALESCE(TO_CHAR((l.checker_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'),'')   AS checker_at,
  COALESCE(l.processing_status,'')     AS processing_status,
  COALESCE(h.created_by_audit,'')      AS created_by_audit,
  COALESCE(h.created_at_audit,'')      AS created_at_audit,
  COALESCE(h.edited_by,'')             AS edited_by,
  COALESCE(h.edited_at,'')             AS edited_at,
  COALESCE(h.deleted_by,'')            AS deleted_by,
  COALESCE(h.deleted_at,'')            AS deleted_at
FROM investment.fd_tds_receipt t
LEFT JOIN investment.fd_master m ON m.fd_id = t.fd_id AND COALESCE(m.is_deleted,false) = false
LEFT JOIN latest_audit l ON l.tds_id = t.tds_id
LEFT JOIN history h ON h.tds_id = t.tds_id
WHERE t.is_deleted = false AND l.processing_status = 'APPROVED'`

		args := []interface{}{}
		argIdx := 1
		if req.FDID != "" {
			baseSQL += fmt.Sprintf(" AND t.fd_id = $%d", argIdx)
			args = append(args, req.FDID)
			argIdx++
		}
		if !appendFDReceiptScopeFilters(ctx, w, &baseSQL, &args, &argIdx, "t.entity_id", "t.bank_id", req.EntityID, req.BankID) {
			return
		}

		baseSQL += ` ORDER BY GREATEST(COALESCE(l.requested_at,'1970-01-01'::timestamptz), COALESCE(l.checker_at,'1970-01-01'::timestamptz)) DESC`

		rows, err := pool.Query(ctx, baseSQL, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		out, err := rowsToMapSlice(rows)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrRowError+err.Error())
			return
		}
		if out == nil {
			out = []map[string]interface{}{}
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"count":   len(out),
			"rows":    out,
		})
	}
}

// ─── HANDLER 7b: GetTDSReceiptsAll ───────────────────────────────────────────
// POST /investment/fd/receipt/tds/all
// Returns ALL TDS receipts (any status) enriched with fd_master data.
// Filters: entity_id, fd_id, bank_id, tds_status, from_date, to_date — all optional.
// Ordered by deduction_date DESC.

func GetTDSReceiptsAll(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID    string `json:"user_id"`
			EntityID  string `json:"entity_id"`
			FDID      string `json:"fd_id"`
			BankID    string `json:"bank_id"`
			TDSStatus string `json:"tds_status"`
			FromDate  string `json:"from_date"`
			ToDate    string `json:"to_date"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		userEmail := resolveUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		baseSQL := `
WITH latest_audit AS (
  SELECT DISTINCT ON (a.tds_id)
    a.tds_id, a.action_type, a.processing_status,
    a.requested_by, a.requested_at,
    a.checker_by, a.checker_at, a.checker_comment, a.reason
  FROM investment.fd_tds_receipt_audit a
  ORDER BY a.tds_id,
    GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamptz),COALESCE(a.checker_at,'1970-01-01'::timestamptz)) DESC
),
history AS (
  SELECT
    tds_id,
    MAX(CASE WHEN action_type='CREATE' THEN requested_by END)                                   AS created_by_audit,
    MAX(CASE WHEN action_type='CREATE' THEN TO_CHAR((requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS') END) AS created_at_audit,
    MAX(CASE WHEN action_type='EDIT'   THEN requested_by END)                                   AS edited_by,
    MAX(CASE WHEN action_type='EDIT'   THEN TO_CHAR((requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS') END) AS edited_at,
    MAX(CASE WHEN action_type='DELETE' THEN requested_by END)                                   AS deleted_by,
    MAX(CASE WHEN action_type='DELETE' THEN TO_CHAR((requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS') END) AS deleted_at
  FROM investment.fd_tds_receipt_audit
  GROUP BY tds_id
)
SELECT
	t.tds_id,
	COALESCE(t.receipt_id,'')              AS receipt_id,
	t.fd_id,
	COALESCE(t.fd_ref_no,'')               AS fd_ref_no,
	t.entity_id,
	COALESCE(m.entity_name, t.entity_id, '') AS entity_name,
	COALESCE(t.bank_id,'')                 AS bank_id,
	COALESCE(m.bank_name, t.bank_id, '')   AS bank_name,
	COALESCE(m.principal_amount, 0)        AS fd_principal_amount,
	COALESCE(m.interest_rate, 0)           AS fd_interest_rate,
	COALESCE(m.maturity_date::text,'')     AS fd_maturity_date,
	COALESCE(m.fd_status,'')               AS fd_status,
	TO_CHAR(t.period_start,'YYYY-MM-DD')   AS period_start,
	TO_CHAR(t.period_end,'YYYY-MM-DD')     AS period_end,
	TO_CHAR(t.deduction_date,'YYYY-MM-DD') AS deduction_date,
	COALESCE(t.gross_interest, 0)          AS gross_interest,
	COALESCE(t.tds_rate_applied, 0)        AS tds_rate_applied,
	COALESCE(t.tds_expected, 0)            AS tds_expected,
	COALESCE(t.tds_deducted_actual, 0)     AS tds_deducted_actual,
	COALESCE(t.tds_variance, 0)            AS tds_variance,
	COALESCE(t.tds_section,'')             AS tds_section,
	COALESCE(t.tds_plan_id,'')             AS tds_plan_id,
	COALESCE(t.has_pan, false)             AS has_pan,
	COALESCE(t.tds_status,'CAPTURED')      AS tds_status,
	COALESCE(t.exception_raised, false)    AS exception_raised,
	COALESCE(t.reconcile_status,'')        AS reconcile_status,
	COALESCE(t.reconcile_run_id,'')        AS reconcile_run_id,
	COALESCE(t.ingestion_source,'')        AS ingestion_source,
	COALESCE(t.is_active, true)            AS is_active,
	'TDS'                                  AS receipt_type,
	-- latest audit snapshot
	COALESCE(la.processing_status,'')      AS processing_status,
	COALESCE(la.action_type,'')            AS action_type,
	COALESCE(la.requested_by,'')           AS requested_by,
	COALESCE(TO_CHAR((la.requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'),'') AS requested_at,
	COALESCE(la.checker_by,'')             AS checker_by,
	COALESCE(TO_CHAR((la.checker_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'),'')   AS checker_at,
	COALESCE(la.checker_comment,'')        AS checker_comment,
	COALESCE(la.reason,'')                 AS reason,
	-- history pivot
	COALESCE(h.created_by_audit,'')        AS created_by_audit,
	COALESCE(h.created_at_audit,'')        AS created_at_audit,
	COALESCE(h.edited_by,'')               AS edited_by,
	COALESCE(h.edited_at,'')               AS edited_at,
	COALESCE(h.deleted_by,'')              AS deleted_by,
	COALESCE(h.deleted_at,'')              AS deleted_at
FROM investment.fd_tds_receipt t
LEFT JOIN investment.fd_master m ON m.fd_id = t.fd_id AND m.is_deleted = false
LEFT JOIN latest_audit la ON la.tds_id = t.tds_id
LEFT JOIN history h ON h.tds_id = t.tds_id
WHERE t.is_deleted = false`

		args := []interface{}{}
		argIdx := 1

		if req.FDID != "" {
			baseSQL += fmt.Sprintf(" AND t.fd_id=$%d", argIdx)
			args = append(args, req.FDID)
			argIdx++
		}
		if !appendFDReceiptScopeFilters(ctx, w, &baseSQL, &args, &argIdx, "t.entity_id", "t.bank_id", req.EntityID, req.BankID) {
			return
		}
		if req.TDSStatus != "" {
			baseSQL += fmt.Sprintf(" AND t.tds_status=$%d", argIdx)
			args = append(args, req.TDSStatus)
			argIdx++
		}
		if req.FromDate != "" {
			baseSQL += fmt.Sprintf(" AND t.deduction_date>=$%d::date", argIdx)
			args = append(args, req.FromDate)
			argIdx++
		}
		if req.ToDate != "" {
			baseSQL += fmt.Sprintf(" AND t.deduction_date<=$%d::date", argIdx)
			args = append(args, req.ToDate)
			argIdx++
		}
		baseSQL += " ORDER BY t.deduction_date  DESC"

		rows, err := pool.Query(ctx, baseSQL, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()
		out, _ := rowsToMapSlice(rows)
		if out == nil {
			out = []map[string]interface{}{}
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"rows":    out,
			"count":   len(out),
		})
	}
}

// ─── HANDLER 8: GetReceiptDetail ─────────────────────────────────────────────

func GetReceiptDetail(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID    string `json:"user_id"`
			ReceiptID string `json:"receipt_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.ReceiptID == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrReceiptIDRequired)
			return
		}
		userEmail := resolveUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()

		// 1. Main receipt
		receiptRows, err := pool.Query(ctx, `
			SELECT r.*, a.action_type, a.processing_status, a.checker_by, a.checker_comment, a.reason
			FROM investment.fd_interest_receipt r
			LEFT JOIN LATERAL (
				SELECT action_type, processing_status, checker_by, checker_comment, reason
				FROM investment.fd_interest_receipt_audit
				WHERE receipt_id=r.receipt_id
				ORDER BY GREATEST(COALESCE(requested_at,'1970-01-01'::timestamptz),COALESCE(checker_at,'1970-01-01'::timestamptz)) DESC
				LIMIT 1
			) a ON true
			WHERE r.receipt_id=$1 AND r.is_deleted=false`, req.ReceiptID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Receipt query failed: "+err.Error())
			return
		}
		defer receiptRows.Close()
		receiptList, _ := rowsToMapSlice(receiptRows)
		var receipt interface{}
		if len(receiptList) > 0 {
			receiptRow := receiptList[0]
			if !requireFDReceiptEntityScope(ctx, w, fmt.Sprint(receiptRow["entity_id"])) {
				return
			}
			receipt = receiptRow
		} else {
			api.RespondWithError(w, http.StatusNotFound, "receipt not found")
			return
		}

		// 2. TDS row
		tdsRows, err := pool.Query(ctx, `SELECT * FROM investment.fd_tds_receipt WHERE receipt_id=$1`, req.ReceiptID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "TDS query failed: "+err.Error())
			return
		}
		defer tdsRows.Close()
		tdsData, _ := rowsToMapSlice(tdsRows)

		// 3. Reconcile result
		reconcileRows, err := pool.Query(ctx, `SELECT * FROM investment.fd_receipt_reconcile_result WHERE receipt_id=$1`, req.ReceiptID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Reconcile query failed: "+err.Error())
			return
		}
		defer reconcileRows.Close()
		reconcileData, _ := rowsToMapSlice(reconcileRows)

		// 4. Exceptions
		exceptionRows, err := pool.Query(ctx, `SELECT * FROM investment.fd_receipt_exception WHERE receipt_id=$1 AND is_deleted=false`, req.ReceiptID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Exception query failed: "+err.Error())
			return
		}
		defer exceptionRows.Close()
		exceptionData, _ := rowsToMapSlice(exceptionRows)

		// 5. Journal entries
		jeRows, err := pool.Query(ctx, `
			SELECT je.*, json_agg(jel.*) AS lines
			FROM investment.accounting_journal_entry je
			LEFT JOIN investment.accounting_journal_entry_line jel ON jel.entry_id=je.entry_id
			WHERE je.receipt_id=$1
			GROUP BY je.entry_id
			ORDER BY je.entry_date`, req.ReceiptID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Journal query failed: "+err.Error())
			return
		}
		defer jeRows.Close()
		jeData, _ := rowsToMapSlice(jeRows)

		// ── Approval workflow ────────────────────────────────────────────────
		var approvalWorkflow interface{}
		{
			var instanceID string
			_ = pool.QueryRow(ctx, `
				SELECT instance_id FROM uam.approval_instance
				WHERE record_id = $1 AND module_code = 'FIXED_DEPOSIT' AND is_deleted = false
				ORDER BY submitted_at DESC LIMIT 1`, req.ReceiptID,
			).Scan(&instanceID)

			if instanceID == "" {
				var pendingActionType, submittedBy, entityID string
				var amount float64
				scanErr := pool.QueryRow(ctx, `
					SELECT a.action_type, COALESCE(a.requested_by,''),
					       COALESCE(r.entity_id,''), COALESCE(r.gross_interest_received,0)
					FROM investment.fd_interest_receipt_audit a
					JOIN investment.fd_interest_receipt r ON r.receipt_id = a.receipt_id
					WHERE a.receipt_id = $1 AND a.processing_status LIKE '%PENDING%'
					ORDER BY a.requested_at DESC LIMIT 1`, req.ReceiptID,
				).Scan(&pendingActionType, &submittedBy, &entityID, &amount)
				if scanErr == nil && pendingActionType != "" {
					txType := map[string]string{
						"CREATE": "FD_RECEIPT_CREATE",
						"EDIT":   "FD_RECEIPT_EDIT",
						"DELETE": "FD_RECEIPT_DELETE",
					}[pendingActionType]
					if txType == "" {
						txType = "FD_RECEIPT_CREATE"
					}
					newInstID, instErr := approvalengine.CreateInstance(ctx, pool, approvalengine.InstanceRequest{
						ModuleCode:       "FIXED_DEPOSIT",
						EntityCode:       entityID,
						TransactionType:  txType,
						RecordID:         req.ReceiptID,
						RecordTable:      constants.QuerryInterestReceipt,
						AuditTable:       constants.QuerryAuditInterestReceipt,
						AuditIDColumn:    "receipt_id",
						ActionType:       pendingActionType,
						Amount:           amount,
						SubmittedBy:      submittedBy,
						SubmittedByEmail: submittedBy,
					})
					if instErr == nil && newInstID != "" {
						instanceID = newInstID
					}
				}
			}

			viewerUserID := api.GetUserIDFromCtx(ctx)
			if instanceID != "" {
				richDetail, richErr := approvalengine.GetRichInstanceDetail(ctx, pool, instanceID, viewerUserID)
				if richErr == nil {
					approvalWorkflow = richDetail
				}
			}
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":           true,
			"receipt":           receipt,
			"tds":               tdsData,
			"reconcile_results": reconcileData,
			"exceptions":        exceptionData,
			"journal_entries":   jeData,
			"approval_workflow": approvalWorkflow,
		})
	}
}

// ─── HANDLER 9: GetReceiptAuditHistory ───────────────────────────────────────

func GetReceiptAuditHistory(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID    string `json:"user_id"`
			ReceiptID string `json:"receipt_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		userEmail := resolveUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		baseSQL := `
			SELECT a.*, r.receipt_status AS current_status, r.gross_interest_received AS current_gross,
			       r.net_amount_received AS current_net
			FROM investment.fd_interest_receipt_audit a
			JOIN investment.fd_interest_receipt r ON r.receipt_id = a.receipt_id
			WHERE 1=1`
		args := []interface{}{}
		if req.ReceiptID != "" {
			args = append(args, req.ReceiptID)
			baseSQL += " AND a.receipt_id=$1"
		}
		baseSQL += ` ORDER BY GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamptz),
			COALESCE(a.checker_at,'1970-01-01'::timestamptz)) DESC LIMIT 1000`

		rows, err := pool.Query(ctx, baseSQL, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()
		out, _ := rowsToMapSlice(rows)

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"rows":    out,
			"count":   len(out),
		})
	}
}

// ─── HANDLER 10: GetTDSRegister ───────────────────────────────────────────────

func GetTDSRegister(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string `json:"user_id"`
			FdID     string `json:"fd_id"`
			EntityID string `json:"entity_id"`
			FromDate string `json:"from_date"`
			ToDate   string `json:"to_date"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		userEmail := resolveUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		baseSQL := `SELECT * FROM investment.fd_tds_receipt WHERE is_deleted=false`
		summarySQL := `SELECT COUNT(*) AS total_rows, SUM(tds_expected) AS total_expected,
		SUM(tds_deducted_actual) AS total_deducted, SUM(tds_variance) AS total_variance,
			COUNT(*) FILTER (WHERE exception_raised=true) AS exception_count
			FROM investment.fd_tds_receipt WHERE is_deleted=false`

		args := []interface{}{}
		argIdx := 1

		if req.FdID != "" {
			cond := fmt.Sprintf(" AND fd_id=$%d", argIdx)
			baseSQL += cond
			summarySQL += cond
			args = append(args, req.FdID)
			argIdx++
		}
		if strings.TrimSpace(req.EntityID) != "" {
			if !requireFDReceiptEntityScope(ctx, w, req.EntityID) {
				return
			}
			cond := fmt.Sprintf(constants.QuerryEntityID, argIdx)
			baseSQL += cond
			summarySQL += cond
			args = append(args, req.EntityID)
			argIdx++
		} else if scope := ctxutil.FromContext(ctx); len(scope.EntityIDs) > 0 {
			cond := fmt.Sprintf(" AND entity_id = ANY($%d::text[])", argIdx)
			baseSQL += cond
			summarySQL += cond
			args = append(args, scope.EntityIDs)
			argIdx++
		}
		if req.FromDate != "" {
			cond := fmt.Sprintf(" AND period_start>=$%d", argIdx)
			baseSQL += cond
			summarySQL += cond
			args = append(args, req.FromDate)
			argIdx++
		}
		if req.ToDate != "" {
			cond := fmt.Sprintf(" AND period_end<=$%d", argIdx)
			baseSQL += cond
			summarySQL += cond
			args = append(args, req.ToDate)
			argIdx++
		}
		baseSQL += " ORDER BY deduction_date DESC, fd_id"

		rows, err := pool.Query(ctx, baseSQL, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()
		out, _ := rowsToMapSlice(rows)

		var totalRows int
		var totalExpected, totalDeducted, totalVariance float64
		var exceptionCount int
		pool.QueryRow(ctx, summarySQL, args...).Scan(&totalRows, &totalExpected, &totalDeducted, &totalVariance, &exceptionCount) //nolint:errcheck

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"rows":    out,
			"count":   len(out),
			"summary": map[string]interface{}{
				"total_rows":      totalRows,
				"total_expected":  totalExpected,
				"total_deducted":  totalDeducted,
				"total_variance":  totalVariance,
				"exception_count": exceptionCount,
			},
		})
	}
}

// ─── HANDLER 11: RunReconciliation (preview / dry-run) ────────────────────────
//
// POST /investment/fd/reconcile/run
// Creates a reconcile_run row in PREVIEW status, runs the engine in dry-run mode
// (no junction table INSERTs, no result INSERTs, no receipt UPDATEs), and returns
// the projected outcome immediately for the UI to display.
// The user then calls /reconcile/ingest with the same run_id to commit.

// ─── HANDLER 11a: RunReconciliation (pure preview — zero DB writes) ───────────
//
// POST /investment/fd/reconcile/run
// Takes receipt_ids and/or tds_ids (or entity_id + period for bulk).
// Derives the period directly from those receipt rows — caller does NOT need to
// supply period_start/period_end when passing specific IDs.
// Returns a full preview inline.  Nothing is persisted.

func RunReconciliation(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID        string   `json:"user_id"`
			EntityID      string   `json:"entity_id"`
			EntityName    string   `json:"entity_name"`
			BankIDFilter  string   `json:"bank_id_filter"`
			PeriodStart   string   `json:"period_start"`
			PeriodEnd     string   `json:"period_end"`
			MatchingBasis string   `json:"matching_basis"`
			ReceiptIDs    []string `json:"receipt_ids"`
			TDSIDs        []string `json:"tds_ids"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		hasIDs := len(req.ReceiptIDs) > 0 || len(req.TDSIDs) > 0
		if !hasIDs && (req.EntityID == "" || req.PeriodStart == "" || req.PeriodEnd == "") {
			api.RespondWithError(w, http.StatusBadRequest, "Provide either receipt_ids/tds_ids or (entity_id + period_start + period_end)")
			return
		}
		userEmail := resolveUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		if req.MatchingBasis == "" {
			req.MatchingBasis = "BOTH"
		}

		ctx := r.Context()
		if hasIDs {
			if !requireFDReceiptIDScope(ctx, pool, w, req.ReceiptIDs, req.TDSIDs) {
				return
			}
		} else {
			scope := ctxutil.FromContext(ctx)
			if !scope.HasEntityAccess(req.EntityID) {
				api.RespondWithError(w, http.StatusForbidden, fmt.Sprintf("Entity ID '%s' is not within your authorized access scope.", req.EntityID))
				return
			}
			if req.BankIDFilter != "" && !scope.HasApprovedBank(req.BankIDFilter) {
				api.RespondWithError(w, http.StatusForbidden, fmt.Sprintf("Bank '%s' is not within your approved bank scope.", req.BankIDFilter))
				return
			}
		}

		var preview *ReconcilePreviewSummary
		var previewErr error

		if hasIDs {
			// Fast path: specific IDs — derive period from those rows, no run row needed.
			preview, previewErr = reconcilePreviewDirect(ctx, pool, req.MatchingBasis, req.ReceiptIDs, req.TDSIDs)
		} else {
			// Bulk path: entity + period — use the engine via a temporary ephemeral run row
			// that gets deleted after the preview is computed.
			var runID string
			insErr := pool.QueryRow(ctx, `
				INSERT INTO investment.fd_receipt_reconcile_run (
					reconcile_run_id, entity_id, entity_name, bank_id_filter,
					period_start, period_end, matching_basis,
					triggered_by, triggered_at, trigger_mode, run_status
				) VALUES (
					'RRUN-' || UPPER(SUBSTR(REPLACE(gen_random_uuid()::TEXT,'-',''),1,7)),
					$1,$2,$3,$4,$5,$6,$7,now(),'MANUAL','PREVIEW'
				) RETURNING reconcile_run_id`,
				req.EntityID, req.EntityName, nullStr(req.BankIDFilter),
				req.PeriodStart, req.PeriodEnd, req.MatchingBasis, userEmail,
			).Scan(&runID)
			if insErr != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Reconcile preview failed: "+insErr.Error())
				return
			}
			preview, previewErr = reconcileEngine(ctx, pool, runID, true, nil, nil)
			// Clean up the ephemeral preview run row — it's not needed
			pool.Exec(ctx, `DELETE FROM investment.fd_receipt_reconcile_run WHERE reconcile_run_id=$1 AND run_status='PREVIEW'`, runID) //nolint:errcheck
		}

		if previewErr != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Preview computation failed: "+previewErr.Error())
			return
		}

		api.LogInfo("[FDReceipt] ReconcilePreview (no DB write) entity=%s i=%d tds=%d",
			req.EntityID, preview.Interest.Processed, preview.TDS.Processed)

		go func(eID, email string) {
			notifcatalog.TriggerNotification(context.Background(), pool,
				"/investment/fd/reconcile/run", eID, map[string]interface{}{
					"entity_id":   eID,
					"event":       "FD_RCPT_RECONCILE",
					"actor_email": email,
				})
		}(req.EntityID, userEmail)

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"message": "Dry-run preview. Call /reconcile/ingest with the same payload to commit.",
			"preview": preview,
		})
	}
}

// ─── HANDLER 11b: IngestReconciliation (actual commit) ────────────────────────
//
// POST /investment/fd/reconcile/ingest
// Accepts the SAME payload as /reconcile/run.
// Creates a fresh run row, then fires the full reconciliation goroutine (writes
// junction table rows, result rows, exception rows, updates receipts).
// Returns run_id immediately; poll /reconcile/status for progress.

func IngestReconciliation(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID        string `json:"user_id"`
			EntityID      string `json:"entity_id"`
			EntityName    string `json:"entity_name"`
			BankIDFilter  string `json:"bank_id_filter"`
			PeriodStart   string `json:"period_start"`
			PeriodEnd     string `json:"period_end"`
			MatchingBasis string `json:"matching_basis"`
			// Optionally target specific receipts/TDS IDs instead of all for entity+period
			ReceiptIDs []string `json:"receipt_ids"`
			TDSIDs     []string `json:"tds_ids"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		hasIDs := len(req.ReceiptIDs) > 0 || len(req.TDSIDs) > 0
		if !hasIDs && (req.EntityID == "" || req.PeriodStart == "" || req.PeriodEnd == "") {
			api.RespondWithError(w, http.StatusBadRequest, "Provide either receipt_ids/tds_ids or (entity_id + period_start + period_end)")
			return
		}
		userEmail := resolveUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		if req.MatchingBasis == "" {
			req.MatchingBasis = "BOTH"
		}
		if req.PeriodStart == "" {
			req.PeriodStart = constants.DateMin
		}
		if req.PeriodEnd == "" {
			req.PeriodEnd = constants.DateMax
		}

		ctx := r.Context()
		if hasIDs {
			if !requireFDReceiptIDScope(ctx, pool, w, req.ReceiptIDs, req.TDSIDs) {
				return
			}
		} else {
			scope := ctxutil.FromContext(ctx)
			if !scope.HasEntityAccess(req.EntityID) {
				api.RespondWithError(w, http.StatusForbidden, fmt.Sprintf("Entity ID '%s' is not within your authorized access scope.", req.EntityID))
				return
			}
			if req.BankIDFilter != "" && !scope.HasApprovedBank(req.BankIDFilter) {
				api.RespondWithError(w, http.StatusForbidden, fmt.Sprintf("Bank '%s' is not within your approved bank scope.", req.BankIDFilter))
				return
			}
		}

		// Create a fresh RUNNING run row
		var runID string
		err := pool.QueryRow(ctx, `
			INSERT INTO investment.fd_receipt_reconcile_run (
				reconcile_run_id, entity_id, entity_name, bank_id_filter,
				period_start, period_end, matching_basis,
				triggered_by, triggered_at, trigger_mode, run_status
			) VALUES (
				'RRUN-' || UPPER(SUBSTR(REPLACE(gen_random_uuid()::TEXT,'-',''),1,7)),
				$1,$2,$3,$4,$5,$6,$7,now(),'MANUAL','RUNNING'
			) RETURNING reconcile_run_id`,
			req.EntityID, req.EntityName, nullStr(req.BankIDFilter),
			req.PeriodStart, req.PeriodEnd, req.MatchingBasis, userEmail,
		).Scan(&runID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Reconcile run insert failed: "+err.Error())
			return
		}

		// Capture for goroutine
		filterReceiptIDs := req.ReceiptIDs
		filterTDSIDs := req.TDSIDs

		go func(rID, eID, uEmail string) {
			defer func() {
				if rec := recover(); rec != nil {
					api.LogError("[FDReceipt] IngestReconciliation panic run=%s: %v", rID, rec)
					pool.Exec(context.Background(),
						`UPDATE investment.fd_receipt_reconcile_run
						 SET run_status='FAILED', error_message=$1, completed_at=now()
						 WHERE reconcile_run_id=$2`,
						fmt.Sprintf("panic: %v", rec), rID) //nolint:errcheck
				}
			}()
			bgCtx := context.Background()
			if rErr := runReconciliation(bgCtx, pool, rID, filterReceiptIDs, filterTDSIDs); rErr != nil {
				api.LogError("[FDReceipt] IngestReconciliation failed run=%s: %v", rID, rErr)
				pool.Exec(bgCtx,
					`UPDATE investment.fd_receipt_reconcile_run
					 SET run_status='FAILED', error_message=$1, completed_at=now()
					 WHERE reconcile_run_id=$2`, rErr.Error(), rID) //nolint:errcheck
			}
		}(runID, req.EntityID, userEmail)

		go func(rID, eID, uEmail string) {
			defer func() {
				if rec := recover(); rec != nil {
					api.LogError("[FDReceipt] IngestReconciliation engine panic run=%s: %v", rID, rec)
				}
			}()
			bgCtx := context.Background()
			_, _ = approvalengine.CreateInstance(bgCtx, pool, approvalengine.InstanceRequest{
				ModuleCode:       "FIXED_DEPOSIT",
				EntityCode:       eID,
				TransactionType:  "FD_RECONCILE_CREATE",
				RecordID:         rID,
				RecordTable:      "investment.fd_receipt_reconcile_run",
				AuditTable:       "investment.fd_receipt_reconcile_run",
				AuditIDColumn:    "reconcile_run_id",
				ActionType:       "CREATE",
				SubmittedBy:      uEmail,
				SubmittedByEmail: uEmail,
			})
		}(runID, req.EntityID, userEmail)

		go func(rID, eID, uEmail string) {
			defer func() {
				if rec := recover(); rec != nil {
					api.LogError("[FDReceipt] IngestReconciliation notification panic run=%s: %v", rID, rec)
				}
			}()
			notifcatalog.TriggerNotification(context.Background(), pool, "/investment/fd/reconcile/ingest", rID, map[string]interface{}{
				"entity_id":   eID,
				"record_id":   rID,
				"event":       "FD_RECONCILIATION_INGESTED",
				"actor_email": uEmail,
			})
		}(runID, req.EntityID, userEmail)

		api.LogInfo("[FDReceipt] IngestReconciliation started: run_id=%s entity=%s period=%s→%s i=%d tds=%d",
			runID, req.EntityID, req.PeriodStart, req.PeriodEnd,
			len(req.ReceiptIDs), len(req.TDSIDs))
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":          true,
			"reconcile_run_id": runID,
			"run_status":       "RUNNING",
			"message":          "Reconciliation ingestion started. Poll /reconcile/status for progress.",
		})
	}
}

// ─── HANDLER 12: GetReconcileRunStatus ───────────────────────────────────────
// Returns a list of reconcile runs (no run_id required).
// Filters: entity_id, run_status, reconcile_run_id (any combination, all optional).
// Results ordered by triggered_at DESC (latest first).

func GetReconcileRunStatus(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Accept filters from either query-string or JSON body
		var req struct {
			UserID         string `json:"user_id"`
			EntityID       string `json:"entity_id"`
			RunStatus      string `json:"run_status"`
			ReconcileRunID string `json:"reconcile_run_id"`
			Limit          int    `json:"limit"`
			Offset         int    `json:"offset"`
		}
		// Try JSON body first, fall back to query params
		if r.Body != nil {
			json.NewDecoder(r.Body).Decode(&req) //nolint:errcheck
		}
		if req.EntityID == "" {
			req.EntityID = r.URL.Query().Get("entity_id")
		}
		if req.RunStatus == "" {
			req.RunStatus = r.URL.Query().Get("run_status")
		}
		if req.ReconcileRunID == "" {
			req.ReconcileRunID = r.URL.Query().Get("reconcile_run_id")
		}
		ctx := r.Context()

		baseSQL := `
			SELECT reconcile_run_id, entity_id, entity_name,
			       COALESCE(bank_id_filter,'')             AS bank_id_filter,
			       period_start::text, period_end::text,
			       matching_basis,
			       run_status, trigger_mode, triggered_by,
			       triggered_at::text                      AS triggered_at,
			       COALESCE(completed_at::text,'')         AS completed_at,
			       COALESCE(error_message,'')              AS error_message,
			       COALESCE(interest_processed,0)          AS interest_processed,
			       COALESCE(interest_matched,0)            AS interest_matched,
			       COALESCE(interest_partial,0)            AS interest_partial,
			       COALESCE(interest_unmatched,0)          AS interest_unmatched,
			       COALESCE(interest_exception,0)          AS interest_exception,
			       COALESCE(tds_processed,0)               AS tds_processed,
			       COALESCE(tds_matched,0)                 AS tds_matched,
			       COALESCE(tds_partial,0)                 AS tds_partial,
			       COALESCE(tds_unmatched,0)               AS tds_unmatched,
			       COALESCE(tds_exception,0)               AS tds_exception,
			       COALESCE(total_expected_interest,0)     AS total_expected_interest,
			       COALESCE(total_received_interest,0)     AS total_received_interest,
			       COALESCE(total_interest_variance,0)     AS total_interest_variance,
			       COALESCE(total_expected_tds,0)          AS total_expected_tds,
			       COALESCE(total_received_tds,0)          AS total_received_tds,
			       COALESCE(total_tds_variance,0)          AS total_tds_variance
			FROM investment.fd_receipt_reconcile_run
			WHERE 1=1`

		args := []interface{}{}
		argIdx := 1

		if req.ReconcileRunID != "" {
			baseSQL += fmt.Sprintf(" AND reconcile_run_id=$%d", argIdx)
			args = append(args, req.ReconcileRunID)
			argIdx++
		}
		if strings.TrimSpace(req.EntityID) != "" {
			if !requireFDReceiptEntityScope(ctx, w, req.EntityID) {
				return
			}
			baseSQL += fmt.Sprintf(" AND entity_id=$%d", argIdx)
			args = append(args, req.EntityID)
			argIdx++
		} else if scope := ctxutil.FromContext(ctx); len(scope.EntityIDs) > 0 {
			baseSQL += fmt.Sprintf(" AND entity_id = ANY($%d::text[])", argIdx)
			args = append(args, scope.EntityIDs)
			argIdx++
		}
		if req.RunStatus != "" {
			baseSQL += fmt.Sprintf(" AND run_status=$%d", argIdx)
			args = append(args, req.RunStatus)
			argIdx++
		}

		baseSQL += " ORDER BY triggered_at DESC"

		rows, err := pool.Query(ctx, baseSQL, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()
		out, _ := rowsToMapSlice(rows)
		if out == nil {
			out = []map[string]interface{}{}
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"runs":    out,
			"count":   len(out),
		})
	}
}

// ─── HANDLER 13: GetReconcileResults ─────────────────────────────────────────
// Returns reconcile result rows enriched with FD master data + cashflow/accrual
// detail lines.  Matches the same shape as the preview line.
// Filters: reconcile_run_id, entity_id, fd_id, receipt_id, tds_id, match_status
// — all optional.  Ordered by created_at DESC (latest first).

func GetReconcileResults(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID         string `json:"user_id"`
			ReconcileRunID string `json:"reconcile_run_id"`
			EntityID       string `json:"entity_id"`
			FDID           string `json:"fd_id"`
			ReceiptID      string `json:"receipt_id"`
			TDSID          string `json:"tds_id"`
			MatchStatus    string `json:"match_status"`
			MatchingBasis  string `json:"matching_basis"`
			Limit          int    `json:"limit"`
			Offset         int    `json:"offset"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		userEmail := resolveUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		if req.MatchingBasis == "" {
			req.MatchingBasis = "BOTH"
		}

		ctx := r.Context()

		// ── 1. Fetch base result rows enriched with FD master ─────────────────
		baseSQL := `
			SELECT
				rr.result_id,
				rr.reconcile_run_id,
				rr.result_type,
				rr.fd_id,
				COALESCE(m.bank_fd_ref_no, rr.fd_ref_no, '') AS fd_ref_no,
				rr.entity_id,
				COALESCE(m.entity_name, rr.entity_id, '')     AS entity_name,
				rr.bank_id,
				COALESCE(m.bank_name, '')                      AS bank_name,
				COALESCE(m.principal_amount,   0)              AS principal_amount,
				COALESCE(m.interest_rate,      0)              AS interest_rate,
				COALESCE(m.maturity_date::text,'')             AS maturity_date,
				COALESCE(m.fd_status,'')                       AS fd_status,
				rr.period_start::text,
				rr.period_end::text,
				rr.matching_basis,
				COALESCE(rr.receipt_id,'')                     AS receipt_id,
				COALESCE(rr.tds_id,'')                         AS tds_id,
				COALESCE(rr.expected_amount,    0)             AS expected_amount,
				COALESCE(rr.received_amount,    0)             AS received_amount,
				COALESCE(rr.amount_variance,    0)             AS amount_variance,
				COALESCE(rr.amount_variance_pct,0)             AS amount_variance_pct,
				rr.match_status,
				COALESCE(rr.match_type,'')                     AS match_type,
				COALESCE(rr.has_exception, false)              AS has_exception,
				COALESCE(rr.exception_id,'')                   AS exception_id,
				rr.created_at::text                            AS created_at
			FROM investment.fd_receipt_reconcile_result rr
			LEFT JOIN investment.fd_master m
			       ON m.fd_id = rr.fd_id AND m.is_deleted = false
			WHERE 1=1`

		args := []interface{}{}
		argIdx := 1

		if req.ReconcileRunID != "" {
			baseSQL += fmt.Sprintf(" AND rr.reconcile_run_id=$%d", argIdx)
			args = append(args, req.ReconcileRunID)
			argIdx++
		}
		if strings.TrimSpace(req.EntityID) != "" {
			if !requireFDReceiptEntityScope(ctx, w, req.EntityID) {
				return
			}
			baseSQL += fmt.Sprintf(" AND rr.entity_id=$%d", argIdx)
			args = append(args, req.EntityID)
			argIdx++
		} else if scope := ctxutil.FromContext(ctx); len(scope.EntityIDs) > 0 {
			baseSQL += fmt.Sprintf(" AND rr.entity_id = ANY($%d::text[])", argIdx)
			args = append(args, scope.EntityIDs)
			argIdx++
		}
		if req.FDID != "" {
			baseSQL += fmt.Sprintf(" AND rr.fd_id=$%d", argIdx)
			args = append(args, req.FDID)
			argIdx++
		}
		if req.ReceiptID != "" {
			baseSQL += fmt.Sprintf(" AND rr.receipt_id=$%d", argIdx)
			args = append(args, req.ReceiptID)
			argIdx++
		}
		if req.TDSID != "" {
			baseSQL += fmt.Sprintf(" AND rr.tds_id=$%d", argIdx)
			args = append(args, req.TDSID)
			argIdx++
		}
		if req.MatchStatus != "" {
			baseSQL += fmt.Sprintf(" AND rr.match_status=$%d", argIdx)
			args = append(args, req.MatchStatus)
			argIdx++
		}

		baseSQL += " ORDER BY rr.created_at DESC"

		rows, err := pool.Query(ctx, baseSQL, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		type ExceptionLine struct {
			ExceptionID       string  `json:"exception_id"`
			ExceptionType     string  `json:"exception_type"`
			Severity          string  `json:"severity"`
			ExceptionStatus   string  `json:"exception_status"`
			ExpectedAmount    float64 `json:"expected_amount"`
			ReceivedAmount    float64 `json:"received_amount"`
			VarianceAmount    float64 `json:"variance_amount"`
			RaisedBy          string  `json:"raised_by"`
			RaisedAt          string  `json:"raised_at"`
			ResolvedBy        string  `json:"resolved_by,omitempty"`
			ResolvedAt        string  `json:"resolved_at,omitempty"`
			ResolutionRemarks string  `json:"resolution_remarks,omitempty"`
		}

		type ResultLine struct {
			ResultID        string  `json:"result_id"`
			ReconcileRunID  string  `json:"reconcile_run_id"`
			ResultType      string  `json:"result_type"`
			FDID            string  `json:"fd_id"`
			FdRefNo         string  `json:"fd_ref_no"`
			EntityID        string  `json:"entity_id"`
			EntityName      string  `json:"entity_name"`
			BankID          string  `json:"bank_id"`
			BankName        string  `json:"bank_name"`
			PrincipalAmount float64 `json:"principal_amount"`
			InterestRate    float64 `json:"interest_rate"`
			MaturityDate    string  `json:"maturity_date"`
			FDStatus        string  `json:"fd_status"`
			PeriodStart     string  `json:"period_start"`
			PeriodEnd       string  `json:"period_end"`
			MatchingBasis   string  `json:"matching_basis"`
			ReceiptID       string  `json:"receipt_id,omitempty"`
			TDSID           string  `json:"tds_id,omitempty"`
			ExpectedAmount  float64 `json:"expected_amount"`
			ReceivedAmount  float64 `json:"received_amount"`
			Variance        float64 `json:"variance"`
			VariancePct     float64 `json:"variance_pct"`
			MatchStatus     string  `json:"match_status"`
			MatchType       string  `json:"match_type"`
			HasException    bool    `json:"has_exception"`
			ExceptionID     string  `json:"exception_id,omitempty"`
			CreatedAt       string  `json:"created_at"`
			// Live receipt/TDS posting status (not frozen at reconcile time)
			ReceiptStatus     string `json:"receipt_status,omitempty"`
			ReconcileStatus   string `json:"reconcile_status,omitempty"`
			JournalEntryID    string `json:"journal_entry_id,omitempty"`
			TDSStatus         string `json:"tds_status,omitempty"`
			TDSJournalEntryID string `json:"tds_journal_entry_id,omitempty"`
			// Detail arrays — same shape as preview
			Cashflows     []CashflowLine      `json:"cashflows"`
			AccrualLedger []AccrualLedgerLine `json:"accrual_ledger"`
			// Exceptions embedded so they can be resolved inline
			Exceptions []ExceptionLine `json:"exceptions"`
		}

		var results []ResultLine
		for rows.Next() {
			var rl ResultLine
			if e := rows.Scan(
				&rl.ResultID, &rl.ReconcileRunID, &rl.ResultType,
				&rl.FDID, &rl.FdRefNo,
				&rl.EntityID, &rl.EntityName,
				&rl.BankID, &rl.BankName,
				&rl.PrincipalAmount, &rl.InterestRate,
				&rl.MaturityDate, &rl.FDStatus,
				&rl.PeriodStart, &rl.PeriodEnd,
				&rl.MatchingBasis,
				&rl.ReceiptID, &rl.TDSID,
				&rl.ExpectedAmount, &rl.ReceivedAmount,
				&rl.Variance, &rl.VariancePct,
				&rl.MatchStatus, &rl.MatchType,
				&rl.HasException, &rl.ExceptionID,
				&rl.CreatedAt,
			); e == nil {
				rl.Cashflows = []CashflowLine{}
				rl.AccrualLedger = []AccrualLedgerLine{}
				rl.Exceptions = []ExceptionLine{}
				results = append(results, rl)
			}
		}
		rows.Close()

		// ── 2. Enrich each result with cashflow + accrual detail lines ────────
		for i := range results {
			rl := &results[i]
			if rl.PeriodStart == "" || rl.PeriodEnd == "" || rl.FDID == "" {
				continue
			}
			ps, pe := parseDateRange(rl.PeriodStart, rl.PeriodEnd)
			matchBasis := rl.MatchingBasis
			if matchBasis == "" {
				matchBasis = req.MatchingBasis
			}
			forTDS := rl.ResultType == "TDS"

			if matchBasis == "CASHFLOW" || matchBasis == "BOTH" {
				if cf := loadCashflowsForReceipt(ctx, pool, rl.FDID, ps, pe, forTDS); cf != nil {
					rl.Cashflows = cf
				}
			}
			if matchBasis == "ACCRUAL" || matchBasis == "BOTH" {
				if al := loadAccrualLedgerForReceipt(ctx, pool, rl.FDID, ps, pe, forTDS); al != nil {
					rl.AccrualLedger = al
				}
			}

			// ── 3. Load exceptions for this result row ────────────────────────
			if rl.HasException || rl.ExceptionID != "" {
				exRows, exErr := pool.Query(ctx, `
					SELECT
						exception_id,
						COALESCE(exception_type,'')    AS exception_type,
						COALESCE(severity,'')          AS severity,
						COALESCE(exception_status,'')  AS exception_status,
						COALESCE(expected_amount,0)    AS expected_amount,
						COALESCE(received_amount,0)    AS received_amount,
						COALESCE(variance_amount,0)    AS variance_amount,
						COALESCE(raised_by,'')         AS raised_by,
						COALESCE(raised_at::text,'')   AS raised_at,
						COALESCE(resolved_by,'')       AS resolved_by,
						COALESCE(resolved_at::text,'') AS resolved_at,
						COALESCE(resolution_remarks,'') AS resolution_remarks
					FROM investment.fd_receipt_exception
					WHERE result_id = $1 AND is_deleted = false
					ORDER BY raised_at DESC`, rl.ResultID)
				if exErr == nil {
					for exRows.Next() {
						var ex ExceptionLine
						if se := exRows.Scan(
							&ex.ExceptionID, &ex.ExceptionType, &ex.Severity, &ex.ExceptionStatus,
							&ex.ExpectedAmount, &ex.ReceivedAmount, &ex.VarianceAmount,
							&ex.RaisedBy, &ex.RaisedAt,
							&ex.ResolvedBy, &ex.ResolvedAt, &ex.ResolutionRemarks,
						); se == nil {
							rl.Exceptions = append(rl.Exceptions, ex)
						}
					}
					exRows.Close()
				}
			}

			applyReconcilePostingEnrichment(ctx, pool, rl.ReceiptID, rl.TDSID, rl.ResultType,
				reconcileEnrichOutputs{ReceiptStatus: &rl.ReceiptStatus, ReconcileStatus: &rl.ReconcileStatus, JournalEntryID: &rl.JournalEntryID, TDSStatus: &rl.TDSStatus, TDSJournalEntryID: &rl.TDSJournalEntryID, Cashflows: &rl.Cashflows})
		}

		if results == nil {
			results = []ResultLine{}
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"rows":    results,
			"count":   len(results),
		})
	}
}

// ─── HANDLER 13b: GetReconcileDetail ────────────────────────────────────────
// POST /investment/fd/reconcile/detail
// Returns full run metadata + every enriched result row for a given reconcile_run_id.
// Includes:
//   - run header: entity, period, trigger info, counters, audit snapshot
//   - is_rerunnable: false once run_status=COMPLETED or any row has match_status=MATCHED
//   - per-result: receipt_type (INTEREST/TDS), period dates, audit snapshot,
//                 fd_master enrichment, cashflows, accrual ledger, exceptions

func GetReconcileDetail(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID         string `json:"user_id"`
			ReconcileRunID string `json:"reconcile_run_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.ReconcileRunID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "reconcile_run_id is required")
			return
		}
		userEmail := resolveUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()

		// ── 1. Run header ────────────────────────────────────────────────────────
		type RunHeader struct {
			ReconcileRunID    string  `json:"reconcile_run_id"`
			EntityID          string  `json:"entity_id"`
			EntityName        string  `json:"entity_name"`
			BankIDFilter      string  `json:"bank_id_filter"`
			PeriodStart       string  `json:"period_start"`
			PeriodEnd         string  `json:"period_end"`
			MatchingBasis     string  `json:"matching_basis"`
			RunStatus         string  `json:"run_status"`
			TriggerMode       string  `json:"trigger_mode"`
			TriggeredBy       string  `json:"triggered_by"`
			TriggeredAt       string  `json:"triggered_at"`
			CompletedAt       string  `json:"completed_at"`
			ErrorMessage      string  `json:"error_message"`
			InterestProcessed int     `json:"interest_processed"`
			InterestMatched   int     `json:"interest_matched"`
			InterestPartial   int     `json:"interest_partial"`
			InterestUnmatched int     `json:"interest_unmatched"`
			InterestException int     `json:"interest_exception"`
			TDSProcessed      int     `json:"tds_processed"`
			TDSMatched        int     `json:"tds_matched"`
			TDSPartial        int     `json:"tds_partial"`
			TDSUnmatched      int     `json:"tds_unmatched"`
			TDSException      int     `json:"tds_exception"`
			TotalExpectedInt  float64 `json:"total_expected_interest"`
			TotalReceivedInt  float64 `json:"total_received_interest"`
			TotalIntVariance  float64 `json:"total_interest_variance"`
			TotalExpectedTDS  float64 `json:"total_expected_tds"`
			TotalReceivedTDS  float64 `json:"total_received_tds"`
			TotalTDSVariance  float64 `json:"total_tds_variance"`
			// Derived flag
			IsRerunnable bool `json:"is_rerunnable"`
		}

		var run RunHeader
		err := pool.QueryRow(ctx, `
			SELECT
				reconcile_run_id,
				COALESCE(entity_id,'')              AS entity_id,
				COALESCE(entity_name,'')             AS entity_name,
				COALESCE(bank_id_filter,'')          AS bank_id_filter,
				COALESCE(period_start::text,'')      AS period_start,
				COALESCE(period_end::text,'')        AS period_end,
				COALESCE(matching_basis,'BOTH')      AS matching_basis,
				COALESCE(run_status,'')              AS run_status,
				COALESCE(trigger_mode,'')            AS trigger_mode,
				COALESCE(triggered_by,'')            AS triggered_by,
				COALESCE(triggered_at::text,'')      AS triggered_at,
				COALESCE(completed_at::text,'')      AS completed_at,
				COALESCE(error_message,'')           AS error_message,
				COALESCE(interest_processed,0),
				COALESCE(interest_matched,0),
				COALESCE(interest_partial,0),
				COALESCE(interest_unmatched,0),
				COALESCE(interest_exception,0),
				COALESCE(tds_processed,0),
				COALESCE(tds_matched,0),
				COALESCE(tds_partial,0),
				COALESCE(tds_unmatched,0),
				COALESCE(tds_exception,0),
				COALESCE(total_expected_interest,0),
				COALESCE(total_received_interest,0),
				COALESCE(total_interest_variance,0),
				COALESCE(total_expected_tds,0),
				COALESCE(total_received_tds,0),
				COALESCE(total_tds_variance,0)
			FROM investment.fd_receipt_reconcile_run
			WHERE reconcile_run_id=$1`, req.ReconcileRunID).Scan(
			&run.ReconcileRunID, &run.EntityID, &run.EntityName, &run.BankIDFilter,
			&run.PeriodStart, &run.PeriodEnd, &run.MatchingBasis,
			&run.RunStatus, &run.TriggerMode, &run.TriggeredBy, &run.TriggeredAt,
			&run.CompletedAt, &run.ErrorMessage,
			&run.InterestProcessed, &run.InterestMatched, &run.InterestPartial,
			&run.InterestUnmatched, &run.InterestException,
			&run.TDSProcessed, &run.TDSMatched, &run.TDSPartial,
			&run.TDSUnmatched, &run.TDSException,
			&run.TotalExpectedInt, &run.TotalReceivedInt, &run.TotalIntVariance,
			&run.TotalExpectedTDS, &run.TotalReceivedTDS, &run.TotalTDSVariance,
		)
		if err != nil {
			api.RespondWithError(w, http.StatusNotFound, "Reconcile run not found: "+err.Error())
			return
		}

		// is_rerunnable = false if COMPLETED or any result row is already MATCHED
		run.IsRerunnable = run.RunStatus != "COMPLETED" && run.InterestMatched == 0 && run.TDSMatched == 0

		// ── 2. Result rows fully enriched ───────────────────────────────────────
		resultRows, err := pool.Query(ctx, `
			SELECT
				rr.result_id,
				rr.reconcile_run_id,
				-- receipt_type flag: INTEREST or TDS
				rr.result_type                              AS receipt_type,
				rr.fd_id,
				COALESCE(m.bank_fd_ref_no, rr.fd_ref_no,'') AS fd_ref_no,
				rr.entity_id,
				COALESCE(m.entity_name, rr.entity_id,'')   AS entity_name,
				COALESCE(rr.bank_id,'')                    AS bank_id,
				COALESCE(m.bank_name,'')                   AS bank_name,
				COALESCE(m.principal_amount,0)             AS principal_amount,
				COALESCE(m.interest_rate,0)                AS interest_rate,
				COALESCE(m.maturity_date::text,'')         AS maturity_date,
				COALESCE(m.fd_status,'')                   AS fd_status,
				-- period dates from result row
				COALESCE(rr.period_start::text,'')         AS period_start,
				COALESCE(rr.period_end::text,'')           AS period_end,
				rr.matching_basis,
				COALESCE(rr.receipt_id,'')                 AS receipt_id,
				COALESCE(rr.tds_id,'')                    AS tds_id,
				COALESCE(rr.expected_amount,0)             AS expected_amount,
				COALESCE(rr.received_amount,0)             AS received_amount,
				COALESCE(rr.amount_variance,0)             AS variance,
				COALESCE(rr.amount_variance_pct,0)         AS variance_pct,
				rr.match_status,
				COALESCE(rr.match_type,'')                 AS match_type,
				COALESCE(rr.has_exception,false)           AS has_exception,
				COALESCE(rr.exception_id,'')               AS exception_id,
				COALESCE(rr.created_at::text,'')           AS created_at,
				-- audit snapshot for the underlying receipt/tds row
				COALESCE(la.processing_status,'')          AS audit_processing_status,
				COALESCE(la.action_type,'')                AS audit_action_type,
				COALESCE(la.requested_by,'')               AS audit_requested_by,
				COALESCE(TO_CHAR((la.requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'),'') AS audit_requested_at,
				COALESCE(la.checker_by,'')                 AS audit_checker_by,
				COALESCE(TO_CHAR((la.checker_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'),'')   AS audit_checker_at,
				COALESCE(la.checker_comment,'')            AS audit_checker_comment,
				COALESCE(la.reason,'')                     AS audit_reason
			FROM investment.fd_receipt_reconcile_result rr
			LEFT JOIN investment.fd_master m ON m.fd_id = rr.fd_id AND m.is_deleted = false
			LEFT JOIN LATERAL (
				-- pick audit from the correct audit table based on result_type
				SELECT processing_status, action_type, requested_by, requested_at,
				       checker_by, checker_at, checker_comment, reason
				FROM investment.fd_interest_receipt_audit a
				WHERE rr.result_type = 'INTEREST' AND a.receipt_id = rr.receipt_id
				UNION ALL
				SELECT processing_status, action_type, requested_by, requested_at,
				       checker_by, checker_at, checker_comment, reason
				FROM investment.fd_tds_receipt_audit a
				WHERE rr.result_type = 'TDS' AND a.tds_id = rr.tds_id
				ORDER BY GREATEST(COALESCE(requested_at,'1970-01-01'::timestamptz),
				                  COALESCE(checker_at,'1970-01-01'::timestamptz)) DESC
				LIMIT 1
			) la ON true
			WHERE rr.reconcile_run_id = $1
			ORDER BY rr.result_type, rr.created_at`, req.ReconcileRunID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer resultRows.Close()

		type ExLine struct {
			ExceptionID       string  `json:"exception_id"`
			ExceptionType     string  `json:"exception_type"`
			Severity          string  `json:"severity"`
			ExceptionStatus   string  `json:"exception_status"`
			ExpectedAmount    float64 `json:"expected_amount"`
			ReceivedAmount    float64 `json:"received_amount"`
			VarianceAmount    float64 `json:"variance_amount"`
			RaisedBy          string  `json:"raised_by"`
			RaisedAt          string  `json:"raised_at"`
			ResolvedBy        string  `json:"resolved_by"`
			ResolvedAt        string  `json:"resolved_at"`
			ResolutionRemarks string  `json:"resolution_remarks"`
		}

		type ResultRow struct {
			ResultID              string              `json:"result_id"`
			ReconcileRunID        string              `json:"reconcile_run_id"`
			ReceiptType           string              `json:"receipt_type"`
			FDID                  string              `json:"fd_id"`
			FdRefNo               string              `json:"fd_ref_no"`
			EntityID              string              `json:"entity_id"`
			EntityName            string              `json:"entity_name"`
			BankID                string              `json:"bank_id"`
			BankName              string              `json:"bank_name"`
			PrincipalAmount       float64             `json:"principal_amount"`
			InterestRate          float64             `json:"interest_rate"`
			MaturityDate          string              `json:"maturity_date"`
			FDStatus              string              `json:"fd_status"`
			PeriodStart           string              `json:"period_start"`
			PeriodEnd             string              `json:"period_end"`
			MatchingBasis         string              `json:"matching_basis"`
			ReceiptID             string              `json:"receipt_id"`
			TDSID                 string              `json:"tds_id"`
			ExpectedAmount        float64             `json:"expected_amount"`
			ReceivedAmount        float64             `json:"received_amount"`
			Variance              float64             `json:"variance"`
			VariancePct           float64             `json:"variance_pct"`
			MatchStatus           string              `json:"match_status"`
			MatchType             string              `json:"match_type"`
			HasException          bool                `json:"has_exception"`
			ExceptionID           string              `json:"exception_id"`
			CreatedAt             string              `json:"created_at"`
			ReceiptStatus         string              `json:"receipt_status,omitempty"`
			ReconcileStatus       string              `json:"reconcile_status,omitempty"`
			JournalEntryID        string              `json:"journal_entry_id,omitempty"`
			TDSStatus             string              `json:"tds_status,omitempty"`
			TDSJournalEntryID     string              `json:"tds_journal_entry_id,omitempty"`
			AuditProcessingStatus string              `json:"audit_processing_status"`
			AuditActionType       string              `json:"audit_action_type"`
			AuditRequestedBy      string              `json:"audit_requested_by"`
			AuditRequestedAt      string              `json:"audit_requested_at"`
			AuditCheckerBy        string              `json:"audit_checker_by"`
			AuditCheckerAt        string              `json:"audit_checker_at"`
			AuditCheckerComment   string              `json:"audit_checker_comment"`
			AuditReason           string              `json:"audit_reason"`
			Cashflows             []CashflowLine      `json:"cashflows"`
			AccrualLedger         []AccrualLedgerLine `json:"accrual_ledger"`
			Exceptions            []ExLine            `json:"exceptions"`
		}

		var results []ResultRow
		for resultRows.Next() {
			var rr ResultRow
			if e := resultRows.Scan(
				&rr.ResultID, &rr.ReconcileRunID, &rr.ReceiptType,
				&rr.FDID, &rr.FdRefNo, &rr.EntityID, &rr.EntityName,
				&rr.BankID, &rr.BankName, &rr.PrincipalAmount, &rr.InterestRate,
				&rr.MaturityDate, &rr.FDStatus, &rr.PeriodStart, &rr.PeriodEnd,
				&rr.MatchingBasis, &rr.ReceiptID, &rr.TDSID,
				&rr.ExpectedAmount, &rr.ReceivedAmount, &rr.Variance, &rr.VariancePct,
				&rr.MatchStatus, &rr.MatchType, &rr.HasException, &rr.ExceptionID, &rr.CreatedAt,
				&rr.AuditProcessingStatus, &rr.AuditActionType,
				&rr.AuditRequestedBy, &rr.AuditRequestedAt,
				&rr.AuditCheckerBy, &rr.AuditCheckerAt,
				&rr.AuditCheckerComment, &rr.AuditReason,
			); e == nil {
				rr.Cashflows = []CashflowLine{}
				rr.AccrualLedger = []AccrualLedgerLine{}
				rr.Exceptions = []ExLine{}
				results = append(results, rr)
			}
		}
		resultRows.Close()

		// ── 3. Enrich each result with cashflows + accrual + exceptions ─────────
		for i := range results {
			row := &results[i]
			if row.FDID == "" || row.PeriodStart == "" || row.PeriodEnd == "" {
				continue
			}
			ps, pe := parseDateRange(row.PeriodStart, row.PeriodEnd)
			forTDS := row.ReceiptType == "TDS"
			basis := row.MatchingBasis
			if basis == "" {
				basis = run.MatchingBasis
			}

			if basis == "CASHFLOW" || basis == "BOTH" {
				if cf := loadCashflowsForReceipt(ctx, pool, row.FDID, ps, pe, forTDS); cf != nil {
					row.Cashflows = cf
				}
			}
			if basis == "ACCRUAL" || basis == "BOTH" {
				if al := loadAccrualLedgerForReceipt(ctx, pool, row.FDID, ps, pe, forTDS); al != nil {
					row.AccrualLedger = al
				}
			}

			if row.HasException || row.ExceptionID != "" {
				exRows, exErr := pool.Query(ctx, `
					SELECT
						exception_id,
						COALESCE(exception_type,'')     AS exception_type,
						COALESCE(severity,'')           AS severity,
						COALESCE(exception_status,'')   AS exception_status,
						COALESCE(expected_amount,0)     AS expected_amount,
						COALESCE(received_amount,0)     AS received_amount,
						COALESCE(variance_amount,0)     AS variance_amount,
						COALESCE(raised_by,'')          AS raised_by,
						COALESCE(raised_at::text,'')    AS raised_at,
						COALESCE(resolved_by,'')        AS resolved_by,
						COALESCE(resolved_at::text,'')  AS resolved_at,
						COALESCE(resolution_remarks,'') AS resolution_remarks
					FROM investment.fd_receipt_exception
					WHERE result_id = $1 AND is_deleted = false
					ORDER BY raised_at DESC`, row.ResultID)
				if exErr == nil {
					for exRows.Next() {
						var ex ExLine
						if se := exRows.Scan(
							&ex.ExceptionID, &ex.ExceptionType, &ex.Severity, &ex.ExceptionStatus,
							&ex.ExpectedAmount, &ex.ReceivedAmount, &ex.VarianceAmount,
							&ex.RaisedBy, &ex.RaisedAt,
							&ex.ResolvedBy, &ex.ResolvedAt, &ex.ResolutionRemarks,
						); se == nil {
							row.Exceptions = append(row.Exceptions, ex)
						}
					}
					exRows.Close()
				}
			}

			applyReconcilePostingEnrichment(ctx, pool, row.ReceiptID, row.TDSID, row.ReceiptType,
				reconcileEnrichOutputs{ReceiptStatus: &row.ReceiptStatus, ReconcileStatus: &row.ReconcileStatus, JournalEntryID: &row.JournalEntryID, TDSStatus: &row.TDSStatus, TDSJournalEntryID: &row.TDSJournalEntryID, Cashflows: &row.Cashflows})
		}

		if results == nil {
			results = []ResultRow{}
		}

		var approvalWorkflow *approvalengine.RichInstanceDetail
		var instanceID string
		_ = pool.QueryRow(ctx, `
			SELECT instance_id
			FROM uam.approval_instance
			WHERE record_id = $1
			  AND module_code = 'FIXED_DEPOSIT'
			  AND status = 'PENDING'
			  AND is_deleted = false
			LIMIT 1`, req.ReconcileRunID).Scan(&instanceID)
		if instanceID != "" {
			viewerUserID := api.GetUserIDFromCtx(ctx)
			if richDetail, err := approvalengine.GetRichInstanceDetail(ctx, pool, instanceID, viewerUserID); err == nil {
				approvalWorkflow = richDetail
			} else {
				api.LogError("[FDReceipt] GetRichInstanceDetail failed reconcile=%s: %v", req.ReconcileRunID, err)
			}
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":           true,
			"run":               run,
			"result_count":      len(results),
			"results":           results,
			"approval_workflow": approvalWorkflow,
		})
	}
}

// ─── HANDLER 13c: GetReconcileCandidates ────────────────────────────────────
// POST /investment/fd/reconcile/candidates
//
// Feeder API for /reconcile/run and /reconcile/ingest.
// Takes only user_id + optional filters (entity_id, fd_id, bank_id,
// period_start, period_end).
//
// Returns:
//   - All APPROVED interest receipts that have NO row in fd_receipt_reconcile_result
//     (i.e. never been reconciled or all prior runs for them are non-COMPLETED).
//   - All APPROVED TDS receipts in the same state.
//
// Each row carries:
//   - receipt_type:          INTEREST | TDS
//   - is_already_reconciled: false  (always false here — filtered out)
//   - period_start/end, fd enrichment from fd_master
//   - full latest audit snapshot
//
// Validation rule enforced:
//   Once a receipt/TDS ID appears in fd_receipt_reconcile_result it is
//   excluded — it CANNOT be re-run through /reconcile/ingest.

func GetReconcileCandidates(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID      string `json:"user_id"`
			EntityID    string `json:"entity_id"`
			FDID        string `json:"fd_id"`
			BankID      string `json:"bank_id"`
			PeriodStart string `json:"period_start"`
			PeriodEnd   string `json:"period_end"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		userEmail := resolveUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()

		// ── INTEREST receipts: APPROVED + never reconciled ───────────────────────
		interestSQL := `
SELECT
  r.receipt_id                            AS id,
  'INTEREST'                              AS receipt_type,
  false                                   AS is_already_reconciled,
  r.fd_id,
  COALESCE(m.bank_fd_ref_no, r.fd_ref_no,'') AS fd_ref_no,
  r.entity_id,
  COALESCE(m.entity_name, r.entity_name,'')  AS entity_name,
  COALESCE(r.bank_id,'')                  AS bank_id,
  COALESCE(m.bank_name, r.bank_name,'')   AS bank_name,
  COALESCE(m.principal_amount,0)          AS fd_principal_amount,
  COALESCE(m.interest_rate,0)             AS fd_interest_rate,
  COALESCE(m.maturity_date::text,'')      AS fd_maturity_date,
  COALESCE(m.fd_status,'')               AS fd_status,
  TO_CHAR(r.receipt_date,'YYYY-MM-DD')   AS receipt_date,
  TO_CHAR(r.period_start,'YYYY-MM-DD')   AS period_start,
  TO_CHAR(r.period_end,'YYYY-MM-DD')     AS period_end,
  COALESCE(r.gross_interest_received,0)  AS gross_interest_received,
  COALESCE(r.tds_amount_deducted,0)      AS tds_amount_deducted,
  COALESCE(r.net_amount_received,0)      AS net_amount_received,
  COALESCE(r.receipt_status,'')          AS receipt_status,
  COALESCE(r.reconcile_status,'')        AS reconcile_status,
  COALESCE(r.bank_reference_no,'')       AS bank_reference_no,
  COALESCE(r.narration,'')              AS narration,
  COALESCE(r.ingestion_mode,'')         AS ingestion_mode,
  -- latest audit snapshot
  COALESCE(la.processing_status,'')     AS audit_processing_status,
  COALESCE(la.action_type,'')           AS audit_action_type,
  COALESCE(la.requested_by,'')          AS audit_requested_by,
  COALESCE(TO_CHAR((la.requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'),'') AS audit_requested_at,
  COALESCE(la.checker_by,'')            AS audit_checker_by,
  COALESCE(TO_CHAR((la.checker_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'),'')   AS audit_checker_at,
  COALESCE(la.checker_comment,'')       AS audit_checker_comment,
  COALESCE(la.reason,'')               AS audit_reason
FROM investment.fd_interest_receipt r
LEFT JOIN investment.fd_master m ON m.fd_id = r.fd_id AND m.is_deleted = false
LEFT JOIN LATERAL (
  SELECT processing_status, action_type, requested_by, requested_at,
         checker_by, checker_at, checker_comment, reason
  FROM investment.fd_interest_receipt_audit a
  WHERE a.receipt_id = r.receipt_id
  ORDER BY GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamptz),
                    COALESCE(a.checker_at,'1970-01-01'::timestamptz)) DESC
  LIMIT 1
) la ON true
WHERE r.is_deleted = false
  AND la.processing_status = 'APPROVED'
  -- not yet in any reconcile result
  AND NOT EXISTS (
    SELECT 1 FROM investment.fd_receipt_reconcile_result rr
    WHERE rr.receipt_id = r.receipt_id
  )`

		iArgs := []interface{}{}
		iIdx := 1
		if req.FDID != "" {
			interestSQL += fmt.Sprintf(" AND r.fd_id=$%d", iIdx)
			iArgs = append(iArgs, req.FDID)
			iIdx++
		}
		if !appendFDReceiptScopeFilters(ctx, w, &interestSQL, &iArgs, &iIdx, "r.entity_id", "r.bank_id", req.EntityID, req.BankID) {
			return
		}
		if req.PeriodStart != "" {
			interestSQL += fmt.Sprintf(" AND r.period_start>=$%d::date", iIdx)
			iArgs = append(iArgs, req.PeriodStart)
			iIdx++
		}
		if req.PeriodEnd != "" {
			interestSQL += fmt.Sprintf(" AND r.period_end<=$%d::date", iIdx)
			iArgs = append(iArgs, req.PeriodEnd)
			iIdx++
		}
		interestSQL += " ORDER BY r.receipt_date DESC"
		_ = iIdx

		iRows, err := pool.Query(ctx, interestSQL, iArgs...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer iRows.Close()
		interestCandidates, _ := rowsToMapSlice(iRows)
		if interestCandidates == nil {
			interestCandidates = []map[string]interface{}{}
		}
		iRows.Close()

		// ── TDS receipts: APPROVED + never reconciled ────────────────────────────
		tdsSQL := `
SELECT
  t.tds_id                                AS id,
  'TDS'                                   AS receipt_type,
  false                                   AS is_already_reconciled,
  t.fd_id,
  COALESCE(m.bank_fd_ref_no, t.fd_ref_no,'') AS fd_ref_no,
  t.entity_id,
  COALESCE(m.entity_name, t.entity_id,'') AS entity_name,
  COALESCE(t.bank_id,'')                  AS bank_id,
  COALESCE(m.bank_name, t.bank_id,'')     AS bank_name,
  COALESCE(m.principal_amount,0)          AS fd_principal_amount,
  COALESCE(m.interest_rate,0)             AS fd_interest_rate,
  COALESCE(m.maturity_date::text,'')      AS fd_maturity_date,
  COALESCE(m.fd_status,'')               AS fd_status,
  TO_CHAR(t.deduction_date,'YYYY-MM-DD') AS receipt_date,
  TO_CHAR(t.period_start,'YYYY-MM-DD')   AS period_start,
  TO_CHAR(t.period_end,'YYYY-MM-DD')     AS period_end,
  COALESCE(t.gross_interest,0)           AS gross_interest,
  COALESCE(t.tds_rate_applied,0)         AS tds_rate_applied,
  COALESCE(t.tds_expected,0)             AS tds_expected,
  COALESCE(t.tds_deducted_actual,0)      AS tds_deducted_actual,
  COALESCE(t.tds_variance,0)             AS tds_variance,
  COALESCE(t.tds_section,'')            AS tds_section,
  COALESCE(t.tds_status,'')             AS tds_status,
  COALESCE(t.reconcile_status,'')       AS reconcile_status,
  COALESCE(t.ingestion_source,'')       AS ingestion_source,
  -- latest audit snapshot
  COALESCE(la.processing_status,'')     AS audit_processing_status,
  COALESCE(la.action_type,'')           AS audit_action_type,
  COALESCE(la.requested_by,'')          AS audit_requested_by,
  COALESCE(TO_CHAR((la.requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'),'') AS audit_requested_at,
  COALESCE(la.checker_by,'')            AS audit_checker_by,
  COALESCE(TO_CHAR((la.checker_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'),'')   AS audit_checker_at,
  COALESCE(la.checker_comment,'')       AS audit_checker_comment,
  COALESCE(la.reason,'')               AS audit_reason
FROM investment.fd_tds_receipt t
LEFT JOIN investment.fd_master m ON m.fd_id = t.fd_id AND m.is_deleted = false
LEFT JOIN LATERAL (
  SELECT processing_status, action_type, requested_by, requested_at,
         checker_by, checker_at, checker_comment, reason
  FROM investment.fd_tds_receipt_audit a
  WHERE a.tds_id = t.tds_id
  ORDER BY GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamptz),
                    COALESCE(a.checker_at,'1970-01-01'::timestamptz)) DESC
  LIMIT 1
) la ON true
WHERE t.is_deleted = false
  AND la.processing_status = 'APPROVED'
  -- not yet in any reconcile result
  AND NOT EXISTS (
    SELECT 1 FROM investment.fd_receipt_reconcile_result rr
    WHERE rr.tds_id = t.tds_id
  )`

		tArgs := []interface{}{}
		tIdx := 1
		if req.FDID != "" {
			tdsSQL += fmt.Sprintf(" AND t.fd_id=$%d", tIdx)
			tArgs = append(tArgs, req.FDID)
			tIdx++
		}
		if !appendFDReceiptScopeFilters(ctx, w, &tdsSQL, &tArgs, &tIdx, "t.entity_id", "t.bank_id", req.EntityID, req.BankID) {
			return
		}
		if req.PeriodStart != "" {
			tdsSQL += fmt.Sprintf(" AND t.period_start>=$%d::date", tIdx)
			tArgs = append(tArgs, req.PeriodStart)
			tIdx++
		}
		if req.PeriodEnd != "" {
			tdsSQL += fmt.Sprintf(" AND t.period_end<=$%d::date", tIdx)
			tArgs = append(tArgs, req.PeriodEnd)
			tIdx++
		}
		tdsSQL += " ORDER BY t.deduction_date DESC"
		_ = tIdx

		tRows, err := pool.Query(ctx, tdsSQL, tArgs...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer tRows.Close()
		tdsCandidates, _ := rowsToMapSlice(tRows)
		if tdsCandidates == nil {
			tdsCandidates = []map[string]interface{}{}
		}
		tRows.Close()

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":             true,
			"interest_candidates": interestCandidates,
			"interest_count":      len(interestCandidates),
			"tds_candidates":      tdsCandidates,
			"tds_count":           len(tdsCandidates),
			"total_count":         len(interestCandidates) + len(tdsCandidates),
			"note":                "Only APPROVED receipts/TDS with no prior reconcile result. Feed receipt_ids/tds_ids from here into /reconcile/run or /reconcile/ingest.",
		})
	}
}

// ─── HANDLER 14: GetExceptions ────────────────────────────────────────────────

func GetExceptions(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID          string `json:"user_id"`
			ReconcileRunID  string `json:"reconcile_run_id"`
			FdID            string `json:"fd_id"`
			ExceptionStatus string `json:"exception_status"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		userEmail := resolveUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		scope := ctxutil.FromContext(ctx)
		out, err := loadVarianceListRows(ctx, pool, varianceListFilters{
			ReconcileRunID:  req.ReconcileRunID,
			FdID:            req.FdID,
			ExceptionStatus: req.ExceptionStatus,
			EntityIDs:       scope.EntityIDs,
			BankIDs:         scope.BankIDs(),
		})
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		if out == nil {
			out = []map[string]interface{}{}
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"rows":    out,
			"count":   len(out),
		})
	}
}

// ─── HANDLER 15: ResolveException ────────────────────────────────────────────

func ResolveException(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID             string `json:"user_id"`
			ExceptionID        string `json:"exception_id"`
			ProposedResolution string `json:"proposed_resolution"`
			ReasonCode         string `json:"reason_code"`
			ResolutionRemarks  string `json:"resolution_remarks"`
			Attachment         string `json:"attachment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.ExceptionID == "" || req.ProposedResolution == "" || req.ReasonCode == "" || req.ResolutionRemarks == "" {
			api.RespondWithError(w, http.StatusBadRequest, "exception_id, proposed_resolution, reason_code, resolution_remarks are required")
			return
		}
		userEmail := resolveUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		var exStatus, entityID, bankID string
		err := pool.QueryRow(ctx, `
			SELECT e.exception_status, COALESCE(m.entity_id,''), COALESCE(m.bank_id,'')
			FROM investment.fd_receipt_exception e
			LEFT JOIN investment.fd_master m ON m.fd_id = e.fd_id AND COALESCE(m.is_deleted,false)=false
			WHERE e.exception_id=$1 AND e.is_deleted=false`, req.ExceptionID).Scan(&exStatus, &entityID, &bankID)
		if err != nil {
			api.RespondWithError(w, http.StatusNotFound, constants.ErrExceptionNotFound)
			return
		}
		scope := ctxutil.FromContext(ctx)
		if !scope.HasEntityAccess(entityID) {
			api.RespondWithError(w, http.StatusForbidden, fmt.Sprintf("Entity ID '%s' is not within your authorized access scope.", entityID))
			return
		}
		if bankID != "" && !scope.HasApprovedBank(bankID) {
			api.RespondWithError(w, http.StatusForbidden, fmt.Sprintf("Bank '%s' is not within your approved bank scope.", bankID))
			return
		}
		if exStatus != "OPEN" && exStatus != "IN_REVIEW" {
			api.RespondWithError(w, http.StatusBadRequest, "Exception must be OPEN or IN_REVIEW")
			return
		}

		tx, err := pool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTransactionFailed)
			return
		}
		defer tx.Rollback(ctx) //nolint:errcheck

		cols, err := fdReceiptTableColumns(ctx, tx, "fd_receipt_exception")
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "schema lookup failed: "+err.Error())
			return
		}
		sets := []string{"exception_status='IN_REVIEW'", "proposed_resolution=$1", "reason_code=$2", "resolution_remarks=$3", "attachment=$4"}
		args := []interface{}{req.ProposedResolution, req.ReasonCode, req.ResolutionRemarks, nullStr(req.Attachment)}
		if cols["reviewed_by"] {
			args = append(args, userEmail)
			sets = append(sets, fmt.Sprintf("reviewed_by=$%d", len(args)))
		}
		if cols["reviewed_at"] {
			sets = append(sets, "reviewed_at=now()")
		}
		if cols["updated_at"] {
			sets = append(sets, "updated_at=now()")
		}
		args = append(args, req.ExceptionID)
		updateSQL := fmt.Sprintf(`
			UPDATE investment.fd_receipt_exception SET %s
			WHERE exception_id=$%d AND exception_status IN ('OPEN','IN_REVIEW')`,
			strings.Join(sets, ", "), len(args))
		_, err = tx.Exec(ctx, updateSQL, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrUpdateFailed+err.Error())
			return
		}

		if err = tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailed+err.Error())
			return
		}

		go func(eID, uEmail string) {
			notifcatalog.TriggerNotification(context.Background(), pool,
				"/investment/fd/exception/resolve", eID, map[string]interface{}{
					"record_id":   eID,
					"event":       "FD_EXCEPTION_RESOLVED",
					"actor_email": uEmail,
				})
		}(req.ExceptionID, userEmail)

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":          true,
			"exception_id":     req.ExceptionID,
			"exception_status": "IN_REVIEW",
		})
	}
}

// ─── HANDLER 16: ApproveException ────────────────────────────────────────────

// ApproveException delegates to approveVarianceHandler which accepts both
// exception_id (single) and exception_ids (bulk array).
func ApproveException(pool *pgxpool.Pool) http.HandlerFunc { return approveVarianceHandler(pool) }

// ─── HANDLER 17: CloseException ──────────────────────────────────────────────

// CloseException delegates to closeVarianceHandler which accepts both
// exception_id (single) and exception_ids (bulk array).
func CloseException(pool *pgxpool.Pool) http.HandlerFunc { return closeVarianceHandler(pool) }

// ─── HANDLER 18: PostReceiptJournals ─────────────────────────────────────────

func PostReceiptJournals(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID     string   `json:"user_id"`
			ReceiptIDs []string `json:"receipt_ids"`
			ReceiptID  string   `json:"receipt_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		userEmail := resolveUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		// Accept single `receipt_id` for convenience (legacy clients). Convert
		// it into the ReceiptIDs slice used by the handler.
		if len(req.ReceiptIDs) == 0 && strings.TrimSpace(req.ReceiptID) != "" {
			req.ReceiptIDs = append(req.ReceiptIDs, strings.TrimSpace(req.ReceiptID))
		}

		ctx := r.Context()
		posted, skipped := 0, 0
		results := make([]map[string]interface{}, 0)

		for _, rid := range req.ReceiptIDs {
			var rec ReceiptForPosting
			var receiptDateRaw, periodStartRaw, periodEndRaw *time.Time
			var journalEntryID *string
			err := pool.QueryRow(ctx, `
				SELECT receipt_id, fd_id, fd_ref_no, entity_id, entity_name,
				       receipt_date, period_start, period_end,
				       gross_interest_received, tds_amount_deducted, net_amount_received,
				       receipt_status, journal_entry_id
				FROM investment.fd_interest_receipt WHERE receipt_id=$1`, rid).Scan(
				&rec.ReceiptID, &rec.FDID, &rec.FdRefNo, &rec.EntityID, &rec.EntityName,
				&receiptDateRaw, &periodStartRaw, &periodEndRaw,
				&rec.GrossInterestReceived, &rec.TDSAmountDeducted, &rec.NetAmountReceived,
				&rec.ReceiptStatus, &journalEntryID)
			if err != nil {
				skipped++
				results = append(results, map[string]interface{}{"receipt_id": rid, "success": false, "error": "not found"})
				continue
			}
			if rec.ReceiptStatus != constants.StatusApproved {
				skipped++
				results = append(results, map[string]interface{}{"receipt_id": rid, "success": false, "error": "receipt_status is not APPROVED"})
				continue
			}
			if blockMsg := checkReceiptPostingEligibility(ctx, pool, rid); blockMsg != "" {
				skipped++
				results = append(results, map[string]interface{}{"receipt_id": rid, "success": false, "error": blockMsg})
				continue
			}
			if journalEntryID != nil && *journalEntryID != "" {
				skipped++
				results = append(results, map[string]interface{}{"receipt_id": rid, "success": false, "error": "already posted"})
				continue
			}
			if receiptDateRaw != nil {
				rec.ReceiptDate = *receiptDateRaw
			}
			if periodStartRaw != nil {
				rec.PeriodStart = *periodStartRaw
			}
			if periodEndRaw != nil {
				rec.PeriodEnd = *periodEndRaw
			}

			interestEntryID, tdsEntryID, jErr := postReceiptJournals(ctx, pool, rec, userEmail)
			if jErr != nil {
				skipped++
				results = append(results, map[string]interface{}{"receipt_id": rid, "success": false, "error": jErr.Error()})
				continue
			}
			posted++
			results = append(results, map[string]interface{}{
				"receipt_id":        rid,
				"success":           true,
				"interest_entry_id": interestEntryID,
				"tds_entry_id":      tdsEntryID,
			})
		}

		api.LogInfo("[FDReceipt] PostJournals: %d posted %d skipped by %s", posted, skipped, userEmail)

		for _, rID := range req.ReceiptIDs {
			go func(id, uEmail string) {
				notifcatalog.TriggerNotification(context.Background(), pool, "/investment/fd/receipt/post-journals", id, map[string]interface{}{
					"record_id":   id,
					"event":       "FD_RECEIPT_JOURNALS_POSTED",
					"actor_email": uEmail,
				})
			}(rID, userEmail)
		}

		resultErrors := make([]string, 0)
		for _, result := range results {
			if ok, _ := result["success"].(bool); ok {
				continue
			}
			receiptID, _ := result["receipt_id"].(string)
			errMsg, _ := result["error"].(string)
			if strings.TrimSpace(errMsg) == "" {
				continue
			}
			if strings.TrimSpace(receiptID) != "" {
				resultErrors = append(resultErrors, receiptID+": "+errMsg)
			} else {
				resultErrors = append(resultErrors, errMsg)
			}
		}

		success := posted > 0
		resp := map[string]interface{}{
			"success": success,
			"posted":  posted,
			"skipped": skipped,
			"results": results,
		}
		if len(resultErrors) > 0 {
			summary := "Some receipt journals were not posted"
			if posted == 0 {
				summary = "No receipt journals were posted"
			}
			resp["error"] = api.BulkActionErrorMessage(summary, resultErrors)
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(resp)
	}
}

// ─── HANDLER 19: UpdateTDS ────────────────────────────────────────────────────

func UpdateTDS(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string                 `json:"user_id"`
			TdsID  string                 `json:"tds_id"`
			Fields map[string]interface{} `json:"fields"`
			Reason string                 `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.TdsID == "" || len(req.Fields) == 0 || req.Reason == "" {
			api.RespondWithError(w, http.StatusBadRequest, "tds_id, fields, and reason are required")
			return
		}
		userEmail := resolveUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		var currentStatus, fdIDForTDS string
		var currentPeriodStart, currentPeriodEnd, currentDeductionDate *time.Time
		err := pool.QueryRow(ctx, `
			SELECT tds_status, fd_id, period_start, period_end, deduction_date
			FROM investment.fd_tds_receipt
			WHERE tds_id=$1 AND is_deleted=false`,
			req.TdsID).Scan(&currentStatus, &fdIDForTDS, &currentPeriodStart, &currentPeriodEnd, &currentDeductionDate)
		if err != nil {
			api.RespondWithError(w, http.StatusNotFound, "TDS record not found")
			return
		}
		if currentStatus != "CAPTURED" {
			api.RespondWithError(w, http.StatusBadRequest, "TDS can only be edited when tds_status=CAPTURED")
			return
		}
		if fdIDForTDS != "" {
			var fdStartU, fdMaturityU time.Time
			if scanErr := pool.QueryRow(ctx,
				`SELECT start_date, maturity_date FROM investment.fd_master WHERE fd_id=$1 AND is_deleted=false`,
				fdIDForTDS).Scan(&fdStartU, &fdMaturityU); scanErr != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "FD date lookup failed: "+scanErr.Error())
				return
			}
			periodStartForValidation := ""
			periodEndForValidation := ""
			deductionDateForValidation := ""
			if currentPeriodStart != nil {
				periodStartForValidation = currentPeriodStart.Format(constants.DateFormat)
			}
			if currentPeriodEnd != nil {
				periodEndForValidation = currentPeriodEnd.Format(constants.DateFormat)
			}
			if currentDeductionDate != nil {
				deductionDateForValidation = currentDeductionDate.Format(constants.DateFormat)
			}
			for _, key := range []string{"period_start", "period_end", "deduction_date"} {
				val, ok := req.Fields[key]
				if !ok {
					continue
				}
				s, ok := val.(string)
				if !ok {
					api.RespondWithError(w, http.StatusBadRequest, key+" must be in YYYY-MM-DD format")
					return
				}
				switch key {
				case "period_start":
					periodStartForValidation = s
				case "period_end":
					periodEndForValidation = s
				case "deduction_date":
					deductionDateForValidation = s
				}
			}
			if errMsg := checkFDPeriodDates(fdStartU, fdMaturityU, periodStartForValidation, periodEndForValidation); errMsg != "" {
				api.RespondWithError(w, http.StatusBadRequest, errMsg)
				return
			}
			if errMsg := checkFDDates(fdStartU, fdMaturityU,
				[]string{"deduction_date"},
				[]string{deductionDateForValidation}); errMsg != "" {
				api.RespondWithError(w, http.StatusBadRequest, errMsg)
				return
			}
		}

		tx, err := pool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTransactionFailed)
			return
		}
		defer tx.Rollback(ctx) //nolint:errcheck

		// FOR UPDATE snapshot
		var oldStatus string
		var oldActual, oldRateApplied, oldVariance float64
		var oldExceptionRaised, oldIsActive bool
		var oldBankTDSRef *string
		err = tx.QueryRow(ctx, `
			SELECT tds_status, tds_deducted_actual, tds_rate_applied,
			       tds_variance, exception_raised, bank_tds_reference, is_active
			FROM investment.fd_tds_receipt WHERE tds_id=$1 FOR UPDATE`, req.TdsID).Scan(
			&oldStatus, &oldActual, &oldRateApplied,
			&oldVariance, &oldExceptionRaised, &oldBankTDSRef, &oldIsActive)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Snapshot failed: "+err.Error())
			return
		}

		allowedTDS := map[string]bool{
			"tds_deducted_actual": true, "tds_rate_applied": true,
			"bank_tds_reference": true, "period_start": true, "period_end": true,
			"deduction_date": true, "exception_raised": true, "exception_reason": true,
			"pan_number": true, "is_active": true,
		}

		setClauses := []string{}
		args := []interface{}{}
		idx := 1
		newActual := oldActual

		for key, val := range req.Fields {
			if !allowedTDS[key] {
				continue
			}
			if key == "tds_deducted_actual" {
				if v, ok := toFloat64(val); ok {
					newActual = v
				}
			}
			setClauses = append(setClauses, fmt.Sprintf("%s=$%d", key, idx))
			args = append(args, val)
			idx++
		}

		// Recompute variance if actual changed
		if newActual != oldActual {
			var tdsExpected float64
			tx.QueryRow(ctx, `SELECT tds_expected FROM investment.fd_tds_receipt WHERE tds_id=$1`, req.TdsID).Scan(&tdsExpected) //nolint:errcheck
			newVariance := newActual - tdsExpected
			setClauses = append(setClauses, fmt.Sprintf("tds_variance=$%d", idx))
			args = append(args, newVariance)
			idx++
		}

		setClauses = append(setClauses, fmt.Sprintf("updated_by=$%d", idx), fmt.Sprintf("updated_at=$%d", idx+1))
		args = append(args, userEmail, time.Now())
		idx += 2
		args = append(args, req.TdsID)
		updateSQL := fmt.Sprintf("UPDATE investment.fd_tds_receipt SET %s WHERE tds_id=$%d",
			strings.Join(setClauses, ","), idx)

		if _, err = tx.Exec(ctx, updateSQL, args...); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrUpdateFailed+err.Error())
			return
		}

		_, err = tx.Exec(ctx, `
			INSERT INTO investment.fd_tds_receipt_audit (
				tds_id, action_type, processing_status, reason, requested_by, requested_at, requested_ip,
				old_tds_status, old_tds_deducted_actual, old_tds_rate_applied,
				old_tds_variance, old_exception_raised, old_bank_tds_reference, old_is_active
			) VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL',$2,$3,now(),$4,$5,$6,$7,$8,$9,$10,$11)`,
			req.TdsID, req.Reason, api.SystemIfBlank(userEmail), api.SystemIfBlank(api.ClientIPFromContext(ctx)),
			oldStatus, oldActual, oldRateApplied, oldVariance,
			oldExceptionRaised, oldBankTDSRef, oldIsActive)
		var entityID string
		_ = pool.QueryRow(ctx, `SELECT COALESCE(entity_id,'') FROM investment.fd_tds_receipt WHERE tds_id=$1`, req.TdsID).Scan(&entityID)

		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrAuditInsertFailed+err.Error())
			return
		}

		if err = tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailed+err.Error())
			return
		}

		go func(tID, uEmail string) {
			defer func() {
				if rec := recover(); rec != nil {
					api.LogError("[FDReceipt] UpdateTDS engine panic for %s: %v", tID, rec)
				}
			}()
			bgCtx := context.Background()
			var eID string
			_ = pool.QueryRow(bgCtx,
				`SELECT COALESCE(entity_id,'') FROM investment.fd_tds_receipt WHERE tds_id=$1`, tID,
			).Scan(&eID)
			if err := approvalengine.CancelPendingInstances(bgCtx, pool, "FIXED_DEPOSIT", tID, uEmail); err != nil {
				api.LogError("[FDReceipt] UpdateTDS CancelPendingInstances failed tds=%s: %v", tID, err)
				return
			}
			instID, instErr := approvalengine.CreateInstance(bgCtx, pool, approvalengine.InstanceRequest{
				ModuleCode:       "FIXED_DEPOSIT",
				EntityCode:       eID,
				TransactionType:  "FD_TDS_EDIT",
				RecordID:         tID,
				RecordTable:      constants.QuerryTDSReceipt,
				AuditTable:       constants.QuerryAuditTDSReceipt,
				AuditIDColumn:    "tds_id",
				ActionType:       "EDIT",
				SubmittedBy:      uEmail,
				SubmittedByEmail: uEmail,
			})
			if instErr != nil {
				api.LogError("[FDReceipt] UpdateTDS CreateInstance failed tds=%s: %v", tID, instErr)
				return
			}
			if instID != "" {
				api.LogInfo("[FDReceipt] UpdateTDS engine instance %s for tds %s", instID, tID)
			}
		}(req.TdsID, userEmail)

		go func(tID, uEmail, eID string) {
			defer func() {
				if rec := recover(); rec != nil {
					api.LogError("[FDReceipt] UpdateTDS notification panic for %s: %v", tID, rec)
				}
			}()
			notifcatalog.TriggerNotification(context.Background(), pool,
				"/investment/fd/receipt/tds/update", tID, map[string]interface{}{
					"record_id":   tID,
					"entity_id":   eID,
					"event":       "FD_TDS_EDIT_SUBMITTED",
					"actor_email": uEmail,
				})
		}(req.TdsID, userEmail, entityID)

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":    true,
			"tds_id":     req.TdsID,
			"updated_by": userEmail,
		})
	}
}

// ─── HANDLER 20: GetTDSDetail ─────────────────────────────────────────────────

func GetTDSDetail(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			TdsID  string `json:"tds_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.TdsID == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrTDSIDRequired)
			return
		}
		userEmail := resolveUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()

		// TDS + receipt context
		tdsRows, err := pool.Query(ctx, `
			SELECT t.*, r.receipt_date, r.gross_interest_received AS receipt_gross,
			       r.net_amount_received, r.receipt_status, r.entity_name, r.bank_name
			FROM investment.fd_tds_receipt t
			JOIN investment.fd_interest_receipt r ON r.receipt_id = t.receipt_id
			WHERE t.tds_id=$1`, req.TdsID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer tdsRows.Close()
		tdsData, _ := rowsToMapSlice(tdsRows)
		var tds interface{}
		if len(tdsData) > 0 {
			tdsRow := tdsData[0]
			if !requireFDReceiptEntityScope(ctx, w, fmt.Sprint(tdsRow["entity_id"])) {
				return
			}
			if bankID := fmt.Sprint(tdsRow["bank_id"]); bankID != "" && !ctxutil.FromContext(ctx).HasApprovedBank(bankID) {
				api.RespondWithError(w, http.StatusForbidden, fmt.Sprintf("Bank '%s' is not within your approved bank scope.", bankID))
				return
			}
			tds = tdsRow
		} else {
			api.RespondWithError(w, http.StatusNotFound, "tds receipt not found")
			return
		}

		// Audit trail
		auditRows, err := pool.Query(ctx, `
			SELECT * FROM investment.fd_tds_receipt_audit
			WHERE tds_id=$1 ORDER BY requested_at DESC`, req.TdsID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Audit query failed: "+err.Error())
			return
		}
		defer auditRows.Close()
		auditData, _ := rowsToMapSlice(auditRows)

		var approvalWorkflow *approvalengine.RichInstanceDetail
		var instanceID string
		_ = pool.QueryRow(ctx, `
			SELECT instance_id
			FROM uam.approval_instance
			WHERE record_id = $1
			  AND module_code = 'FIXED_DEPOSIT'
			  AND status = 'PENDING'
			  AND is_deleted = false
			LIMIT 1`, req.TdsID).Scan(&instanceID)
		if instanceID != "" {
			viewerUserID := api.GetUserIDFromCtx(ctx)
			if richDetail, err := approvalengine.GetRichInstanceDetail(ctx, pool, instanceID, viewerUserID); err == nil {
				approvalWorkflow = richDetail
			} else {
				api.LogError("[FDReceipt] GetRichInstanceDetail failed tds=%s: %v", req.TdsID, err)
			}
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":           true,
			"tds":               tds,
			"audit_history":     auditData,
			"approval_workflow": approvalWorkflow,
		})
	}
}

// ─── HANDLER 21: GetTDSAuditHistory ──────────────────────────────────────────

func GetTDSAuditHistory(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			TdsID  string `json:"tds_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		userEmail := resolveUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		baseSQL := `
			SELECT a.*, t.tds_status AS current_status, t.tds_deducted_actual AS current_actual,
			       t.tds_variance AS current_variance
			FROM investment.fd_tds_receipt_audit a
			JOIN investment.fd_tds_receipt t ON t.tds_id=a.tds_id
			WHERE 1=1`
		args := []interface{}{}
		if req.TdsID != "" {
			args = append(args, req.TdsID)
			baseSQL += " AND a.tds_id=$1"
		}
		baseSQL += " ORDER BY a.requested_at DESC LIMIT 1000"

		rows, err := pool.Query(ctx, baseSQL, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()
		out, _ := rowsToMapSlice(rows)

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"rows":    out,
			"count":   len(out),
		})
	}
}
