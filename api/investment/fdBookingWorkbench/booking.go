package fdBooking

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/approvalengine"
	"CimplrCorpSaas/api/constants"
	notifcatalog "CimplrCorpSaas/api/notification/catalog"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ─── CreateBookingSingle ──────────────────────────────────────────────────────

func CreateBookingSingle(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID               string  `json:"user_id"`
			EntityID             string  `json:"entity_id"`
			EntityName           string  `json:"entity_name"`
			BankID               string  `json:"bank_id"`
			BankName             string  `json:"bank_name"`
			BankAccountID        string  `json:"bank_account_id"` // → source_account_id
			SourceAccountNumber  string  `json:"source_account_number"`
			BankConfigID         string  `json:"bank_config_id"`
			PrincipalAmount      float64 `json:"principal_amount"`
			ValueType            string  `json:"value_type"`
			InterestRate         float64 `json:"interest_rate"`
			TenorDays            int     `json:"tenor_days"`   // → tenure_days
			TenorMonths          int     `json:"tenor_months"` // → tenure_months
			TenorType            string  `json:"tenor_type"`
			TenureYears          int     `json:"tenure_years"`
			ExpectedStartDate    string  `json:"expected_start_date"` // NOT NULL
			ValueDate            string  `json:"value_date"`          // NOT NULL
			MaturityDate         string  `json:"maturity_date"`       // → expected_maturity_date NOT NULL
			InterestType         string  `json:"interest_type"`       // → interest_type_code NOT NULL
			InterestTypeID       string  `json:"interest_type_id"`
			FrequencyID          string  `json:"frequency_id"`              // → frequency_id
			InterestPayoutFreq   string  `json:"interest_payout_frequency"` // alias for frequency_id
			DayCountCode         string  `json:"day_count_code"`            // → day_count_code
			DayCountConvention   string  `json:"day_count_convention"`      // alias for day_count_code
			TdsPlanID            string  `json:"tds_plan_id"`
			ProductCode          string  `json:"product_code"`
			AutoRenewal          bool    `json:"auto_renewal"`
			RenewalInstructions  string  `json:"renewal_instructions"` // kept for compat, maps to auto_renewal
			Notes                string  `json:"notes"`                // → booking_remarks
			BookingRemarks       string  `json:"booking_remarks"`
			OfferValidTill       string  `json:"offer_valid_till"` // YYYY-MM-DD; bank offer validity date
			AccrualFrequencyCode string  `json:"accrual_frequency_code"`
			ResetType            string  `json:"reset_type"` // AT_MATURITY | AT_EACH_PAYOUT
			PayoutFrequencyID    string  `json:"payout_frequency_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}

		if req.EntityID == "" || req.BankID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "entity_id and bank_id are required")
			return
		}
		if req.PrincipalAmount <= 0 || req.InterestRate <= 0 || (req.TenorDays <= 0 && req.TenorMonths <= 0 && req.TenureYears <= 0) {
			api.RespondWithError(w, http.StatusBadRequest, "principal_amount, interest_rate and tenor (days/months/years) must be positive")
			return
		}
		if req.ValueDate == "" || req.MaturityDate == "" {
			api.RespondWithError(w, http.StatusBadRequest, "value_date and maturity_date are required")
			return
		}

		userEmail := api.GetUserEmailFromCtx(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			msg, status := getUserFriendlyFDError(err, constants.ErrTransactionFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx) //nolint:errcheck

		bookingColumns, err := loadFDTableColumns(ctx, tx, "investment", "fd_booking_request")
		if err != nil {
			msg, status := getUserFriendlyFDError(err, constants.ErrLoadBookingSchemaFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		accountColumn := pickFirstExistingFDColumn(bookingColumns, "source_account_id", "bank_account_id", "account_id", "bank_account")
		if accountColumn == "" {
			api.RespondWithError(w, http.StatusInternalServerError, "FD booking table is missing a bank account reference column")
			return
		}

		var bookingID string
		// entity_name: prefer what the caller sends; fall back to master lookup
		entityName := req.EntityName
		if entityName == "" {
			entityName = getEntityName(ctx, tx, req.EntityID)
		}
		// expected_start_date: prefer explicit field, fall back to value_date
		expectedStartDate := req.ExpectedStartDate
		if expectedStartDate == "" {
			expectedStartDate = req.ValueDate
		}
		// frequency_id: prefer explicit field, fall back to interest_payout_frequency alias
		frequencyID := req.FrequencyID
		if frequencyID == "" {
			frequencyID = req.InterestPayoutFreq
		}
		// day_count_code: prefer explicit field, fall back to day_count_convention alias
		dayCountCode := req.DayCountCode
		if dayCountCode == "" {
			dayCountCode = req.DayCountConvention
		}
		// booking_remarks: prefer explicit field, fall back to notes alias
		bookingRemarks := req.BookingRemarks
		if bookingRemarks == "" {
			bookingRemarks = req.Notes
		}
		// auto_renewal: true if field set OR renewal_instructions contains AUTO
		autoRenewal := req.AutoRenewal || strings.Contains(strings.ToUpper(req.RenewalInstructions), "AUTO")
		resetType := strings.ToUpper(strings.TrimSpace(req.ResetType))
		if resetType == "" {
			resetType = "AT_MATURITY"
		}
		// Normalize tenor: compute tenure_days from tenor_type/months/years if needed
		tenorTypeNorm := strings.ToUpper(strings.TrimSpace(req.TenorType))
		computedTenorDays := req.TenorDays
		if computedTenorDays == 0 {
			if req.TenorMonths > 0 {
				computedTenorDays = req.TenorMonths * 30
			} else if req.TenureYears > 0 {
				computedTenorDays = req.TenureYears * 365
			}
		}

		insertValues := map[string]interface{}{
			"entity_id":              req.EntityID,
			"entity_name":            entityName,
			"bank_id":                req.BankID,
			"bank_name":              nullIfEmpty(req.BankName),
			accountColumn:            req.BankAccountID,
			"source_account_number":  nullIfEmpty(req.SourceAccountNumber),
			"bank_config_id":         nullIfEmpty(req.BankConfigID),
			"principal_amount":       req.PrincipalAmount,
			"value_type":             normalizePrincipalValueType(req.ValueType),
			"interest_rate":          req.InterestRate,
			"tenure_days":            computedTenorDays,
			"tenure_months":          req.TenorMonths,
			"tenor_type":             nullIfEmpty(tenorTypeNorm),
			"tenure_years":           req.TenureYears,
			"interest_type_code":     req.InterestType,
			"interest_type_id":       nullIfEmpty(req.InterestTypeID),
			"expected_start_date":    coerceDateValue(expectedStartDate),
			"expected_maturity_date": coerceDateValue(req.MaturityDate),
			"value_date":             coerceDateValue(req.ValueDate),
			"frequency_id":           nullIfEmpty(frequencyID),
			"day_count_code":         nullIfEmpty(dayCountCode),
			"tds_plan_id":            nullIfEmpty(req.TdsPlanID),
			"product_code":           nullIfEmpty(req.ProductCode),
			"auto_renewal":           autoRenewal,
			"booking_remarks":        nullIfEmpty(bookingRemarks),
			"offer_valid_till":       nullIfEmpty(req.OfferValidTill),
			"accrual_frequency_code": nullIfEmpty(strings.ToUpper(strings.TrimSpace(req.AccrualFrequencyCode))),
			"reset_type":             resetType,
			"payout_frequency_id":    nullIfEmpty(req.PayoutFrequencyID),
			"booking_status":         "DRAFT",
			"created_by":             userEmail,
		}
		preferredCols := []string{
			"entity_id", "entity_name", "bank_id", "bank_name", accountColumn,
			"source_account_number", "bank_config_id",
			"principal_amount", "value_type", "interest_rate", "tenure_days", "tenure_months",
			"tenor_type", "tenure_years",
			"interest_type_code", "interest_type_id", "expected_start_date", "expected_maturity_date", "value_date",
			"frequency_id", "day_count_code", "tds_plan_id", "product_code",
			"auto_renewal", "booking_remarks", "offer_valid_till",
			"accrual_frequency_code", "reset_type", "payout_frequency_id",
			"booking_status", "created_by",
		}
		insertQ, insertArgs, returningColumn, ok := buildFDDynamicInsert(
			constants.QuerryBookingRequest,
			bookingColumns,
			preferredCols,
			insertValues,
			[]string{"booking_id"},
		)
		if !ok || returningColumn == "" {
			api.RespondWithError(w, http.StatusInternalServerError, "Unable to build FD booking insert for the live schema")
			return
		}
		err = tx.QueryRow(ctx, insertQ, insertArgs...).Scan(&bookingID)
		if err != nil {
			logDBError(err, "CreateBookingSingle insert")
			msg, status := getUserFriendlyFDError(err, "Insert booking failed")
			api.RespondWithError(w, status, msg)
			return
		}

		if _, err = tx.Exec(ctx, `
			INSERT INTO investment.fd_audit_booking_request
				(booking_id, action_type, processing_status, requested_by, requested_at, requested_ip)
			VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now(),$3)`,
			bookingID, api.SystemIfBlank(userEmail), api.SystemIfBlank(api.ClientIPFromContext(ctx)),
		); err != nil {
			msg, status := getUserFriendlyFDError(err, constants.ErrAuditInsertFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		if err = tx.Commit(ctx); err != nil {
			logDBError(err, "CreateBookingSingle commit")
			msg, status := getUserFriendlyFDError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}

		go func(bID, uID, uEmail, entityID string, amount float64) {
			defer func() {
				if rec := recover(); rec != nil {
					api.LogError("[FDBooking] CreateInstance goroutine panic for booking %s: %v", bID, rec)
				}
			}()
			bgCtx := context.Background()
			instID, err := approvalengine.CreateInstance(bgCtx, pgxPool, approvalengine.InstanceRequest{
				ModuleCode:       "FIXED_DEPOSIT",
				EntityCode:       entityID,
				TransactionType:  "FD_BOOKING",
				RecordID:         bID,
				RecordTable:      constants.QuerryBookingRequest,
				AuditTable:       constants.QuerryAuditBookingRequest,
				AuditIDColumn:    "booking_id",
				ActionType:       "CREATE",
				Amount:           amount,
				SubmittedBy:      uID,
				SubmittedByEmail: uEmail,
			})
			if err != nil {
				api.LogError("[FDBooking] CreateInstance failed for booking %s: %v", bID, err)
				return
			}
			if instID != "" {
				// Instance created — flip booking_status to APPROVAL_PENDING
				if _, uerr := pgxPool.Exec(bgCtx,
					`UPDATE investment.fd_booking_request SET booking_status = 'APPROVAL_PENDING' WHERE booking_id = $1`,
					bID); uerr != nil {
					api.LogError("[FDBooking] Status→PENDING_APPROVAL failed for %s: %v", bID, uerr)
				} else {
					api.LogInfo("[FDBooking] CreateInstance %s → booking %s PENDING_APPROVAL", instID, bID)
				}
			} else {
				api.LogInfo("[FDBooking] No matrix for booking %s — stays DRAFT", bID)
			}
		}(bookingID, req.UserID, userEmail, req.EntityID, req.PrincipalAmount)

		go func(bID, eID, uEmail string, amount float64) {
			defer func() {
				if rec := recover(); rec != nil {
					api.LogError(constants.ErrFDBookingPanic, bID, rec)
				}
			}()
			notifcatalog.TriggerNotification(context.Background(), pgxPool, "/investment/fd/booking/create", bID, map[string]interface{}{
				"entity_id":   eID,
				"EntityID":    eID,
				"record_id":   bID,
				"event":       "FD_BOOKING_SUBMITTED",
				"actor_email": uEmail,
				"UserID":      uEmail,
				"amount":      amount,
			})
		}(bookingID, req.EntityID, userEmail, req.PrincipalAmount)

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"booking_id": bookingID,
			"entity_id":  req.EntityID,
			"requested":  userEmail,
		})
		api.LogInfo("[FDBooking] Single booking created: id=%s entity=%s", bookingID, req.EntityID)
	}
}

// ─── CreateBookingBulk ───────────────────────────────────────────────────────

func CreateBookingBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			Rows   []struct {
				EntityID            string  `json:"entity_id"`
				EntityName          string  `json:"entity_name"`
				BankID              string  `json:"bank_id"`
				BankName            string  `json:"bank_name"`
				BankAccountID       string  `json:"bank_account_id"` // → source_account_id
				SourceAccountNumber string  `json:"source_account_number"`
				BankConfigID        string  `json:"bank_config_id"`
				PrincipalAmount     float64 `json:"principal_amount"`
				ValueType           string  `json:"value_type"`
				InterestRate        float64 `json:"interest_rate"`
				TenorDays           int     `json:"tenor_days"`   // → tenure_days
				TenorMonths         int     `json:"tenor_months"` // → tenure_months
				TenorType           string  `json:"tenor_type"`
				TenureYears         int     `json:"tenure_years"`
				ExpectedStartDate   string  `json:"expected_start_date"` // NOT NULL
				ValueDate           string  `json:"value_date"`          // NOT NULL
				MaturityDate        string  `json:"maturity_date"`       // → expected_maturity_date NOT NULL
				InterestType        string  `json:"interest_type"`       // → interest_type_code NOT NULL
				InterestTypeID      string  `json:"interest_type_id"`
				FrequencyID         string  `json:"frequency_id"`
				InterestPayoutFreq  string  `json:"interest_payout_frequency"` // alias for frequency_id
				DayCountCode        string  `json:"day_count_code"`
				DayCountConvention  string  `json:"day_count_convention"` // alias for day_count_code
				TdsPlanID           string  `json:"tds_plan_id"`
				ProductCode         string  `json:"product_code"`
				AutoRenewal         bool    `json:"auto_renewal"`
				RenewalInstructions string  `json:"renewal_instructions"` // compat: maps to auto_renewal
				Notes               string  `json:"notes"`                // → booking_remarks
				BookingRemarks      string  `json:"booking_remarks"`
			} `json:"rows"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.Rows) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrNoRowsProvided)
			return
		}

		userEmail := api.GetUserEmailFromCtx(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		results := make([]map[string]interface{}, 0, len(req.Rows))

		for i, row := range req.Rows {
			if row.EntityID == "" || row.BankID == "" ||
				row.PrincipalAmount <= 0 || row.InterestRate <= 0 || (row.TenorDays <= 0 && row.TenorMonths <= 0 && row.TenureYears <= 0) ||
				row.ValueDate == "" || row.MaturityDate == "" {
				results = append(results, map[string]interface{}{
					"row_index":            i,
					constants.ValueSuccess: false,
					constants.ValueError:   "Missing required fields: entity_id, bank_id, principal_amount, interest_rate, tenor_days, value_date, maturity_date",
				})
				continue
			}

			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				results = append(results, map[string]interface{}{
					"row_index": i, "entity_id": row.EntityID,
					constants.ValueSuccess: false, constants.ValueError: "tx begin: " + err.Error(),
				})
				continue
			}

			bookingColumns, schemaErr := loadFDTableColumns(ctx, tx, "investment", "fd_booking_request")
			if schemaErr != nil {
				tx.Rollback(ctx) //nolint:errcheck
				logDBError(schemaErr, fmt.Sprintf("CreateBookingBulk row %d load schema", i))
				msg, _ := getUserFriendlyFDError(schemaErr, constants.ErrLoadBookingSchemaFailed)
				results = append(results, map[string]interface{}{
					"row_index": i, "entity_id": row.EntityID,
					constants.ValueSuccess: false, constants.ValueError: msg,
				})
				continue
			}
			accountColumn := pickFirstExistingFDColumn(bookingColumns, "source_account_id", "bank_account_id", "account_id", "bank_account")
			if accountColumn == "" {
				tx.Rollback(ctx) //nolint:errcheck
				logDBError(fmt.Errorf("fd_booking_request missing bank account column (row %d)", i), fmt.Sprintf("CreateBookingBulk row %d schema", i))
				results = append(results, map[string]interface{}{
					"row_index": i, "entity_id": row.EntityID,
					constants.ValueSuccess: false, constants.ValueError: "FD booking table is missing a bank account reference column",
				})
				continue
			}

			var bookingID string
			// entity_name: prefer what caller sends; fall back to master lookup
			entityName := row.EntityName
			if entityName == "" {
				entityName = getEntityName(ctx, tx, row.EntityID)
			}
			// expected_start_date: prefer explicit field, fall back to value_date
			expectedStartDate := row.ExpectedStartDate
			if expectedStartDate == "" {
				expectedStartDate = row.ValueDate
			}
			// frequency_id: prefer explicit field, fall back to interest_payout_frequency alias
			frequencyID := row.FrequencyID
			if frequencyID == "" {
				frequencyID = row.InterestPayoutFreq
			}
			// day_count_code: prefer explicit field, fall back to day_count_convention alias
			dayCountCode := row.DayCountCode
			if dayCountCode == "" {
				dayCountCode = row.DayCountConvention
			}
			// booking_remarks: prefer explicit field, fall back to notes alias
			bookingRemarks := row.BookingRemarks
			if bookingRemarks == "" {
				bookingRemarks = row.Notes
			}
			// auto_renewal: true if field set OR renewal_instructions contains AUTO
			autoRenewal := row.AutoRenewal || strings.Contains(strings.ToUpper(row.RenewalInstructions), "AUTO")
			// normalize tenor for this row
			tenorTypeNorm := strings.ToUpper(strings.TrimSpace(row.TenorType))
			computedTenorDays := row.TenorDays
			if computedTenorDays == 0 {
				if row.TenorMonths > 0 {
					computedTenorDays = row.TenorMonths * 30
				} else if row.TenureYears > 0 {
					computedTenorDays = row.TenureYears * 365
				}
			}
			insertValues := map[string]interface{}{
				"entity_id":              row.EntityID,
				"entity_name":            entityName,
				"bank_id":                row.BankID,
				"bank_name":              nullIfEmpty(row.BankName),
				accountColumn:            row.BankAccountID,
				"source_account_number":  nullIfEmpty(row.SourceAccountNumber),
				"bank_config_id":         nullIfEmpty(row.BankConfigID),
				"principal_amount":       row.PrincipalAmount,
				"value_type":             normalizePrincipalValueType(row.ValueType),
				"interest_rate":          row.InterestRate,
				"tenure_days":            computedTenorDays,
				"tenure_months":          row.TenorMonths,
				"tenor_type":             nullIfEmpty(tenorTypeNorm),
				"tenure_years":           row.TenureYears,
				"interest_type_code":     row.InterestType,
				"interest_type_id":       nullIfEmpty(row.InterestTypeID),
				"expected_start_date":    coerceDateValue(expectedStartDate),
				"expected_maturity_date": coerceDateValue(row.MaturityDate),
				"value_date":             coerceDateValue(row.ValueDate),
				"frequency_id":           nullIfEmpty(frequencyID),
				"day_count_code":         nullIfEmpty(dayCountCode),
				"tds_plan_id":            nullIfEmpty(row.TdsPlanID),
				"product_code":           nullIfEmpty(row.ProductCode),
				"auto_renewal":           autoRenewal,
				"booking_remarks":        nullIfEmpty(bookingRemarks),
				"booking_status":         "DRAFT",
				"created_by":             userEmail,
			}
			bulkPreferredCols := []string{
				"entity_id", "entity_name", "bank_id", "bank_name", accountColumn,
				"source_account_number", "bank_config_id",
				"principal_amount", "value_type", "interest_rate", "tenure_days", "tenure_months",
				"tenor_type", "tenure_years",
				"interest_type_code", "interest_type_id", "expected_start_date", "expected_maturity_date", "value_date",
				"frequency_id", "day_count_code", "tds_plan_id", "product_code",
				"auto_renewal", "booking_remarks", "booking_status", "created_by",
			}
			insertQ, insertArgs, returningColumn, ok := buildFDDynamicInsert(
				constants.QuerryBookingRequest,
				bookingColumns,
				bulkPreferredCols,
				insertValues,
				[]string{"booking_id"},
			)
			if !ok || returningColumn == "" {
				tx.Rollback(ctx) //nolint:errcheck
				results = append(results, map[string]interface{}{
					"row_index": i, "entity_id": row.EntityID,
					constants.ValueSuccess: false, constants.ValueError: "Unable to build FD booking insert for the live schema",
				})
				continue
			}
			err = tx.QueryRow(ctx, insertQ, insertArgs...).Scan(&bookingID)
			if err != nil {
				tx.Rollback(ctx) //nolint:errcheck
				logDBError(err, fmt.Sprintf("CreateBookingBulk row %d insert", i))
				msg, _ := getUserFriendlyFDError(err, "Insert failed")
				results = append(results, map[string]interface{}{
					"row_index": i, "entity_id": row.EntityID,
					constants.ValueSuccess: false, constants.ValueError: msg,
				})
				continue
			}

			if _, err = tx.Exec(ctx, `
				INSERT INTO investment.fd_audit_booking_request
					(booking_id, action_type, processing_status, requested_by, requested_at, requested_ip)
				VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now(),$3)`,
				bookingID, api.SystemIfBlank(userEmail), api.SystemIfBlank(api.ClientIPFromContext(ctx)),
			); err != nil {
				tx.Rollback(ctx) //nolint:errcheck
				msg, _ := getUserFriendlyFDError(err, constants.ErrAuditInsertFailed)
				results = append(results, map[string]interface{}{
					"row_index": i, "entity_id": row.EntityID,
					constants.ValueSuccess: false, constants.ValueError: msg,
				})
				continue
			}

			if err = tx.Commit(ctx); err != nil {
				logDBError(err, fmt.Sprintf("CreateBookingBulk row %d commit", i))
				msg, _ := getUserFriendlyFDError(err, constants.ErrCommitFailedCapitalized)
				results = append(results, map[string]interface{}{
					"row_index": i, "entity_id": row.EntityID,
					constants.ValueSuccess: false, constants.ValueError: msg,
				})
				continue
			}

			results = append(results, map[string]interface{}{
				"row_index": i, "booking_id": bookingID, "entity_id": row.EntityID,
				constants.ValueSuccess: true,
			})

			go func(bID, uID, uEmail, entityID string, amount float64) {
				defer func() {
					if rec := recover(); rec != nil {
						api.LogError("[FDBooking] CreateInstance goroutine panic for booking %s: %v", bID, rec)
					}
				}()
				bgCtx := context.Background()
				instID, err := approvalengine.CreateInstance(bgCtx, pgxPool, approvalengine.InstanceRequest{
					ModuleCode: "FIXED_DEPOSIT", EntityCode: entityID,
					TransactionType: "FD_BOOKING", RecordID: bID,
					RecordTable: constants.QuerryBookingRequest, AuditTable: constants.QuerryAuditBookingRequest,
					AuditIDColumn: "booking_id", ActionType: "CREATE",
					Amount: amount, SubmittedBy: uID, SubmittedByEmail: uEmail,
				})
				if err != nil {
					api.LogError("[FDBooking] CreateInstance failed for booking %s: %v", bID, err)
					return
				}
				if instID != "" {
					if _, uerr := pgxPool.Exec(bgCtx,
						`UPDATE investment.fd_booking_request SET booking_status = 'APPROVAL_PENDING' WHERE booking_id = $1`,
						bID); uerr != nil {
						api.LogError("[FDBooking] Status→PENDING_APPROVAL failed for %s: %v", bID, uerr)
					} else {
						api.LogInfo("[FDBooking] CreateInstance %s → booking %s PENDING_APPROVAL", instID, bID)
					}
				} else {
					api.LogInfo("[FDBooking] No matrix for booking %s — stays DRAFT", bID)
				}
			}(bookingID, req.UserID, userEmail, row.EntityID, row.PrincipalAmount)

			go func(bID, eID, uEmail string, amount float64) {
				defer func() {
					if rec := recover(); rec != nil {
						api.LogError(constants.ErrFDBookingPanic, bID, rec)
					}
				}()
				notifcatalog.TriggerNotification(context.Background(), pgxPool, "/investment/fd/booking/create-bulk", bID, map[string]interface{}{
					"entity_id":   eID,
					"EntityID":    eID,
					"record_id":   bID,
					"event":       "FD_BOOKING_SUBMITTED",
					"actor_email": uEmail,
					"UserID":      uEmail,
					"amount":      amount,
				})
			}(bookingID, row.EntityID, userEmail, row.PrincipalAmount)
		}

		success := false
		for _, res := range results {
			if s, ok := res[constants.ValueSuccess].(bool); ok && s {
				success = true
				break
			}
		}
		api.RespondWithPayload(w, success, "", results)
		api.LogInfo("[FDBooking] Bulk create: %d rows", len(req.Rows))
	}
}

// ─── UpdateBooking ───────────────────────────────────────────────────────────

func UpdateBooking(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID    string                 `json:"user_id"`
			BookingID string                 `json:"booking_id"`
			Fields    map[string]interface{} `json:"fields"`
			Reason    string                 `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.BookingID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "booking_id is required")
			return
		}
		if len(req.Fields) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrNoFieldsToUpdateUser)
			return
		}

		userEmail := api.GetUserEmailFromCtx(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		// validate before transaction
		if errMsg := validateBookingFields(req.Fields); errMsg != "" {
			api.RespondWithError(w, http.StatusBadRequest, errMsg)
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			msg, status := getUserFriendlyFDError(err, constants.ErrTransactionFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx) //nolint:errcheck

		// Select FOR UPDATE and verify status gate
		var currentStatus, entityID string
		var oldPrincipal, oldRate float64
		var oldTenorDays int
		var oldTenorMonths int
		var oldTenureYears int
		var oldTenorType string
		var oldValueDate, oldMaturityDate, oldBankID, oldBankAccountID string
		var oldInterestTypeID string
		accountExpr, err := resolveFDBookingAccountExpression(ctx, tx, "fd_booking_request")
		if err != nil {
			msg, status := getUserFriendlyFDError(err, constants.ErrLoadBookingSchemaFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		err = tx.QueryRow(ctx, fmt.Sprintf(`
			SELECT booking_status, entity_id,
				   COALESCE(principal_amount,0), COALESCE(interest_rate,0), COALESCE(tenure_days,0), COALESCE(tenure_months,0), COALESCE(tenure_years,0), COALESCE(tenor_type,''),
				   COALESCE(TO_CHAR(value_date,'YYYY-MM-DD'),''), COALESCE(TO_CHAR(expected_maturity_date,'YYYY-MM-DD'),''),
				   COALESCE(bank_id,''), %s,
				   COALESCE(interest_type_id,'')
			FROM investment.fd_booking_request
			WHERE booking_id = $1 AND COALESCE(is_deleted,false) = false
			FOR UPDATE`, accountExpr),
			req.BookingID,
		).Scan(&currentStatus, &entityID, &oldPrincipal, &oldRate, &oldTenorDays, &oldTenorMonths, &oldTenureYears, &oldTenorType,
			&oldValueDate, &oldMaturityDate, &oldBankID, &oldBankAccountID, &oldInterestTypeID)
		if err != nil {
			msg, status := getUserFriendlyFDError(err, "Fetch booking failed")
			api.RespondWithError(w, status, msg)
			return
		}
		if currentStatus != "DRAFT" && currentStatus != constants.StatusRejected {
			api.RespondWithError(w, http.StatusBadRequest,
				fmt.Sprintf("Cannot update booking in status '%s'. Only DRAFT or REJECTED bookings can be updated.", currentStatus))
			return
		}

		// Build dynamic SET clause
		// allowedFields maps accepted client-side field names to their actual DB column names.
		// Aliases (e.g. interest_payout_frequency, compounding_frequency) resolve to the same
		// underlying column so we must de-duplicate them when building the SET clause.
		allowedFields := map[string]string{
			"principal_amount":       "principal_amount",
			"value_type":             "value_type",
			"interest_rate":          "interest_rate",
			"tenure_days":            "tenure_days",
			"value_date":             "value_date",
			"expected_maturity_date": "expected_maturity_date",
			"maturity_amount":        "maturity_amount",
			// interest_type / interest_type_id are two client aliases for the same columns
			"interest_type":    "interest_type_code", // alias → DB column
			"interest_type_id": "interest_type_id",
			// frequency aliases – both map to frequency_id; de-dup handled below
			"interest_payout_frequency": "frequency_id",
			"compounding_frequency":     "frequency_id",
			"frequency_id":              "frequency_id",
			// day_count alias
			"day_count_convention": "day_count_code",
			"day_count_code":       "day_count_code",
			// other fields
			"currency":             "currency",
			"tds_plan_id":          "tds_plan_id",
			"penalty_structure_id": "penalty_structure_id",
			// renewal_instructions is a string alias for the boolean auto_renewal column;
			// handled specially below — NOT included here to avoid direct column SET.
			"notes":           "booking_remarks", // alias → DB column
			"booking_remarks": "booking_remarks",
			"bank_config_id":  "bank_config_id",
			// offer_valid_till is a date column on fd_booking_request
			"offer_valid_till": "offer_valid_till",
		}

		setClauses := make([]string, 0)
		setArgs := make([]interface{}, 0)
		argIdx := 1
		usedColumns := make(map[string]bool) // prevent duplicate SET for aliased columns
		for k, v := range req.Fields {
			colName, ok := allowedFields[k]
			if !ok {
				continue
			}
			if usedColumns[colName] {
				continue // already added this DB column via another alias
			}
			usedColumns[colName] = true
			if k == "value_date" || k == "expected_maturity_date" || k == "offer_valid_till" {
				v = coerceDateValue(v)
			}
			if k == "value_type" {
				if sv, ok := v.(string); ok {
					v = normalizePrincipalValueType(sv)
				}
			}
			setClauses = append(setClauses, fmt.Sprintf("%s = $%d", colName, argIdx))
			setArgs = append(setArgs, v)
			argIdx++
		}
		// Special: renewal_instructions (string) → auto_renewal (bool)
		if ri, ok := req.Fields["renewal_instructions"]; ok && !usedColumns["auto_renewal"] {
			autoRenewalVal := false
			switch v := ri.(type) {
			case bool:
				autoRenewalVal = v
			case string:
				autoRenewalVal = strings.ToUpper(strings.TrimSpace(v)) == "AUTO" || strings.ToUpper(strings.TrimSpace(v)) == "TRUE"
			}
			setClauses = append(setClauses, fmt.Sprintf("auto_renewal = $%d", argIdx))
			setArgs = append(setArgs, autoRenewalVal)
			usedColumns["auto_renewal"] = true
			argIdx++
		}

		if len(setClauses) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No valid fields to update")
			return
		}

		setClauses = append(setClauses, fmt.Sprintf("booking_status = $%d", argIdx))
		setArgs = append(setArgs, "DRAFT")
		argIdx++
		setArgs = append(setArgs, req.BookingID)

		updateQ := fmt.Sprintf(`UPDATE investment.fd_booking_request SET %s WHERE booking_id = $%d`,
			strings.Join(setClauses, ", "), argIdx)
		if _, err = tx.Exec(ctx, updateQ, setArgs...); err != nil {
			logDBError(err, fmt.Sprintf("UpdateBooking exec query=%s args=%v", updateQ, setArgs))
			msg, status := getUserFriendlyFDError(err, "Update failed")
			api.RespondWithError(w, status, msg)
			return
		}

		if _, err = tx.Exec(ctx, `
			INSERT INTO investment.fd_audit_booking_request (
				booking_id, action_type, processing_status, requested_by, requested_at, requested_ip, reason,
				old_principal_amount, old_interest_rate, old_interest_type_id, old_tenure_days, old_tenor_type, old_tenure_years,
				old_value_date, old_expected_maturity_date, old_source_account_id
			) VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL',$2,now(),$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
			req.BookingID, api.SystemIfBlank(userEmail), api.SystemIfBlank(api.ClientIPFromContext(ctx)), req.Reason,
			oldPrincipal, oldRate, nullIfEmpty(oldInterestTypeID), oldTenorDays, nullIfEmpty(oldTenorType), oldTenureYears,
			coerceDateValue(oldValueDate), coerceDateValue(oldMaturityDate),
			oldBankAccountID,
		); err != nil {
			logDBError(err, fmt.Sprintf("UpdateBooking audit insert booking_id=%s", req.BookingID))
			msg, status := getUserFriendlyFDError(err, constants.ErrAuditInsertFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		if err = tx.Commit(ctx); err != nil {
			logDBError(err, "UpdateBooking commit")
			msg, status := getUserFriendlyFDError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}

		go func(bID, uID, uEmail, eID string, amount float64) {
			defer func() {
				if rec := recover(); rec != nil {
					api.LogError("[FDBooking] UpdateBooking engine goroutine panic for booking %s: %v", bID, rec)
				}
			}()
			bgCtx := context.Background()
			// Cancel any in-flight approval so the edit resets all previous approvals.
			if err := approvalengine.CancelPendingInstances(bgCtx, pgxPool, "FIXED_DEPOSIT", bID, uEmail); err != nil {
				api.LogError("[FDBooking] CancelPendingInstances failed for booking %s: %v", bID, err)
				return
			}
			instID, err := approvalengine.CreateInstance(bgCtx, pgxPool, approvalengine.InstanceRequest{
				ModuleCode: "FIXED_DEPOSIT", EntityCode: eID,
				TransactionType: "FD_BOOKING_EDIT", RecordID: bID,
				RecordTable: constants.QuerryBookingRequest, AuditTable: constants.QuerryAuditBookingRequest,
				AuditIDColumn: "booking_id", ActionType: "EDIT",
				Amount: amount, SubmittedBy: uID, SubmittedByEmail: uEmail,
			})
			if err != nil {
				api.LogError("[FDBooking] CreateInstance(EDIT) failed for booking %s: %v", bID, err)
				return
			}
			if instID != "" {
				if _, uerr := pgxPool.Exec(bgCtx,
					`UPDATE investment.fd_booking_request SET booking_status = 'APPROVAL_PENDING' WHERE booking_id = $1`,
					bID); uerr != nil {
					api.LogError("[FDBooking] Status→APPROVAL_PENDING failed for %s: %v", bID, uerr)
				} else {
					api.LogInfo("[FDBooking] CreateInstance(EDIT) %s → booking %s APPROVAL_PENDING", instID, bID)
				}
			}
		}(req.BookingID, req.UserID, userEmail, entityID, oldPrincipal)

		go func(bID, eID, uEmail string, amount float64) {
			defer func() {
				if rec := recover(); rec != nil {
					api.LogError(constants.ErrFDBookingPanic, bID, rec)
				}
			}()
			notifcatalog.TriggerNotification(context.Background(), pgxPool, "/investment/fd/booking/update", bID, map[string]interface{}{
				"entity_id":   eID,
				"EntityID":    eID,
				"record_id":   bID,
				"event":       "FD_BOOKING_EDIT_SUBMITTED",
				"actor_email": uEmail,
				"UserID":      uEmail,
				"amount":      amount,
			})
		}(req.BookingID, entityID, userEmail, oldPrincipal)

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"booking_id": req.BookingID, "requested": userEmail,
		})
		api.LogInfo("[FDBooking] Booking updated: id=%s by=%s", req.BookingID, userEmail)
	}
}

// ─── DeleteBooking ───────────────────────────────────────────────────────────

func DeleteBooking(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID     string   `json:"user_id"`
			BookingIDs []string `json:"booking_ids"`
			Reason     string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.BookingIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrBookingIDsRequired)
			return
		}

		userEmail := api.GetUserEmailFromCtx(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			msg, status := getUserFriendlyFDError(err, constants.ErrTransactionFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx) //nolint:errcheck

		// APPROVED bookings cannot be deleted (system depends on them).
		// Allow: DRAFT, REJECTED, APPROVAL_PENDING (pending can be cancelled/deleted).
		rows, err := tx.Query(ctx, `
			SELECT booking_id, entity_id, principal_amount
			FROM investment.fd_booking_request
			WHERE booking_id = ANY($1::text[])
			  AND booking_status IN ('DRAFT','REJECTED','APPROVAL_PENDING')
			  AND NOT EXISTS (
				  SELECT 1 FROM investment.fd_confirmation c
				  WHERE c.booking_id = fd_booking_request.booking_id
				    AND COALESCE(c.is_deleted,false) = false
			  )
			  AND NOT EXISTS (
				  SELECT 1 FROM investment.fd_master fm
				  WHERE fm.booking_id = fd_booking_request.booking_id
				    AND COALESCE(fm.is_deleted,false) = false
			  )
			  AND COALESCE(is_deleted,false) = false`,
			req.BookingIDs,
		)
		if err != nil {
			msg, status := getUserFriendlyFDError(err, "Verify bookings failed")
			api.RespondWithError(w, status, msg)
			return
		}
		defer rows.Close()

		type bookingMeta struct {
			id     string
			entity string
			amount float64
		}
		var validBookings []bookingMeta
		for rows.Next() {
			var bm bookingMeta
			if err := rows.Scan(&bm.id, &bm.entity, &bm.amount); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Scan error: "+err.Error())
				return
			}
			validBookings = append(validBookings, bm)
		}
		if err := rows.Err(); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrRowError+err.Error())
			return
		}
		rows.Close()

		if len(validBookings) == 0 {
			api.RespondWithPayload(w, false, "No eligible bookings found (must be DRAFT/REJECTED/APPROVAL_PENDING and must not have active confirmation or FD activation)", nil)
			return
		}

		validIDs := make([]string, len(validBookings))
		for i, bm := range validBookings {
			validIDs[i] = bm.id
		}

		auditVals := make([]string, len(validIDs))
		auditArgs := make([]interface{}, 0, len(validIDs)*4)
		for i, id := range validIDs {
			auditVals[i] = fmt.Sprintf("($%d,'DELETE','PENDING_DELETE_APPROVAL',$%d,$%d,now(),$%d)", i*4+1, i*4+2, i*4+3, i*4+4)
			auditArgs = append(auditArgs, id, api.SystemIfBlank(userEmail), req.Reason, api.SystemIfBlank(api.ClientIPFromContext(ctx)))
		}
		auditQ := fmt.Sprintf(`
			INSERT INTO investment.fd_audit_booking_request
				(booking_id, action_type, processing_status, requested_by, reason, requested_at, requested_ip)
			VALUES %s`, strings.Join(auditVals, ","))
		if _, err = tx.Exec(ctx, auditQ, auditArgs...); err != nil {
			msg, status := getUserFriendlyFDError(err, constants.ErrAuditInsertFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		if err = tx.Commit(ctx); err != nil {
			logDBError(err, "DeleteBooking commit")
			msg, status := getUserFriendlyFDError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}

		// Fire engine goroutines after commit — cancel prior instances first (like update)
		for _, bm := range validBookings {
			go func(bID, uID, uEmail, eID string, amount float64) {
				defer func() {
					if rec := recover(); rec != nil {
						api.LogError("[FDBooking] DeleteBooking engine goroutine panic for booking %s: %v", bID, rec)
					}
				}()
				bgCtx := context.Background()
				// Cancel any in-flight approval chain before submitting DELETE
				if err := approvalengine.CancelPendingInstances(bgCtx, pgxPool, "FIXED_DEPOSIT", bID, uEmail); err != nil {
					api.LogError("[FDBooking] CancelPendingInstances(DELETE) failed for booking %s: %v", bID, err)
					return
				}
				instID, err := approvalengine.CreateInstance(bgCtx, pgxPool, approvalengine.InstanceRequest{
					ModuleCode: "FIXED_DEPOSIT", EntityCode: eID,
					TransactionType: "FD_BOOKING_DELETE", RecordID: bID,
					RecordTable: constants.QuerryBookingRequest, AuditTable: constants.QuerryAuditBookingRequest,
					AuditIDColumn: "booking_id", ActionType: "DELETE",
					Amount: amount, SubmittedBy: uID, SubmittedByEmail: uEmail,
				})
				if err != nil {
					api.LogError("[FDBooking] CreateInstance(DELETE) failed for booking %s: %v", bID, err)
					return
				}
				if instID != "" {
					if _, uerr := pgxPool.Exec(bgCtx,
						`UPDATE investment.fd_booking_request SET booking_status = 'APPROVAL_PENDING' WHERE booking_id = $1`,
						bID); uerr != nil {
						api.LogError("[FDBooking] Status→PENDING_DELETE_APPROVAL failed for %s: %v", bID, uerr)
					} else {
						api.LogInfo("[FDBooking] CreateInstance(DELETE) %s → booking %s PENDING_DELETE_APPROVAL", instID, bID)
					}
				}
			}(bm.id, req.UserID, userEmail, bm.entity, bm.amount)

			go func(bID, eID, uEmail string) {
				defer func() {
					if rec := recover(); rec != nil {
						api.LogError(constants.ErrFDBookingPanic, bID, rec)
					}
				}()
				notifcatalog.TriggerNotification(context.Background(), pgxPool, "/investment/fd/booking/delete", bID, map[string]interface{}{
					"entity_id":   eID,
					"EntityID":    eID,
					"record_id":   bID,
					"event":       "FD_BOOKING_DELETE_SUBMITTED",
					"actor_email": uEmail,
					"UserID":      uEmail,
				})
			}(bm.id, bm.entity, userEmail)
		}

		validSet := make(map[string]bool)
		for _, id := range validIDs {
			validSet[id] = true
		}
		results := make([]map[string]interface{}, 0, len(req.BookingIDs))
		for _, id := range req.BookingIDs {
			if validSet[id] {
				results = append(results, map[string]interface{}{constants.ValueSuccess: true, "booking_id": id})
			} else {
				results = append(results, map[string]interface{}{
					constants.ValueSuccess: false, "booking_id": id,
					constants.ValueError: "Not found, wrong status, or already has active confirmation/FD activation",
				})
			}
		}
		api.RespondWithPayload(w, len(validIDs) > 0, "", results)
		api.LogInfo("[FDBooking] Delete requests created: %d/%d bookings", len(validIDs), len(req.BookingIDs))
	}
}

// ─── BulkApproveBooking ──────────────────────────────────────────────────────
// Engine-first: if an approval instance is PENDING for this booking and the
// user is an eligible approver, calls RecordAction (which respects eye
// sequence and handles audit stamping + status flip via finalizeRecord).
// Falls back to direct DB stamp only when no engine instance exists.
// func BulkApproveBooking(pgxPool *pgxpool.Pool) http.HandlerFunc {
// 	return func(w http.ResponseWriter, r *http.Request) {

// 		var req struct {
// 			UserID     string   `json:"user_id"`
// 			BookingIDs []string `json:"booking_ids"`
// 			Comment    string   `json:"comment"`
// 		}

// 		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
// 			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
// 			return
// 		}

// 		if len(req.BookingIDs) == 0 {
// 			api.RespondWithError(w, http.StatusBadRequest, constants.ErrBookingIDsRequired)
// 			return
// 		}

// 		// ---------------- USER VALIDATION ----------------
// 		userEmail := ""
// 		for _, s := range auth.GetActiveSessions() {
// 			if s.UserID == req.UserID {
// 				userEmail = s.Email
// 				break
// 			}
// 		}

// 		if userEmail == "" {
// 			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
// 			return
// 		}

// 		ctx := r.Context()
// 		engineActed := 0
// 		directActed := 0
// 		var errors []string

// 		for _, bID := range req.BookingIDs {

// 			// ---------------- ENGINE PATH ----------------
// 			var instanceEyeID string

// 			engineErr := pgxPool.QueryRow(ctx, `
// 				SELECT ie.instance_eye_id
// 				FROM uam.approval_instance i
// 				JOIN uam.approval_instance_eye ie
// 					ON ie.instance_id = i.instance_id AND ie.status = 'ACTIVE'
// 				JOIN uam.approval_matrix_eye_member m
// 					ON m.eye_id = ie.matrix_eye_id
// 					AND m.member_type = 'APPROVER'
// 					AND m.is_active = true
// 					AND m.is_deleted = false
// 					AND m.assignment_type IN ('USER_ONLY','ROLE_USER')
// 					AND m.user_id = $2
// 				WHERE i.record_id = $1
// 				  AND i.module_code = 'FIXED_DEPOSIT'
// 				  AND i.status = 'PENDING'
// 				ORDER BY ie.position ASC
// 				LIMIT 1
// 			`, bID, req.UserID).Scan(&instanceEyeID)

// 			if engineErr == nil && instanceEyeID != "" {

// 				// 🔥 APPROVAL ENGINE ACTION
// 				if err := approvalengine.RecordAction(ctx, pgxPool, approvalengine.ActionRequest{
// 					InstanceEyeID: instanceEyeID,
// 					ActorUserID:   req.UserID,
// 					ActorEmail:    userEmail,
// 					ActionType:    approvalengine.ActionApproved,
// 					Comment:       req.Comment,
// 				}); err != nil {
// 					api.LogError("[FDBooking] RecordAction approve failed for booking %s: %v", bID, err)
// 					errors = append(errors, bID+": "+err.Error())
// 					continue
// 				}

// 				// 🔥 CHECK FINAL APPROVAL
// 				var instStatus string
// 				_ = pgxPool.QueryRow(ctx, `
// 					SELECT i.status
// 					FROM uam.approval_instance i
// 					JOIN uam.approval_instance_eye ie
// 						ON ie.instance_id = i.instance_id
// 					WHERE ie.instance_eye_id = $1
// 				`, instanceEyeID).Scan(&instStatus)

// 				if instStatus == constants.StatusApproved {

// 					// ✅ FINAL STATUS CHANGE (UPDATED)
// 					_, _ = pgxPool.Exec(ctx, `
// 						UPDATE investment.fd_booking_request
// 						SET booking_status = 'SENT_TO_BANK',
// 						    approved_at = NOW(),
// 						    sent_to_bank_at = NOW()
// 						WHERE booking_id = $1
// 						  AND booking_status <> 'SENT_TO_BANK'
// 					`, bID)

// 					// delete handling
// 					_, _ = pgxPool.Exec(ctx, `
// 						UPDATE investment.fd_booking_request
// 						SET is_deleted = true
// 						WHERE booking_id IN (
// 							SELECT DISTINCT a.booking_id
// 							FROM investment.fd_audit_booking_request a
// 							WHERE a.booking_id = $1
// 							  AND a.action_type = 'DELETE'
// 							  AND a.processing_status = 'APPROVED'
// 						)
// 					`, bID)
// 				}

// 				engineActed++
// 				continue
// 			}

// 			// ---------------- DIRECT PATH ----------------
// 			var anyInstance int
// 			_ = pgxPool.QueryRow(ctx, `
// 				SELECT COUNT(*)
// 				FROM uam.approval_instance
// 				WHERE record_id = $1
// 				  AND module_code = 'FIXED_DEPOSIT'
// 				  AND status = 'PENDING'
// 			`, bID).Scan(&anyInstance)

// 			if anyInstance > 0 {
// 				errors = append(errors, bID+": not your turn in approval sequence")
// 				continue
// 			}

// 			tx, err := pgxPool.Begin(ctx)
// 			if err != nil {
// 				errors = append(errors, bID+": tx begin failed")
// 				continue
// 			}

// 			_, err1 := tx.Exec(ctx, `
// 				UPDATE investment.fd_audit_booking_request
// 				SET processing_status='APPROVED',
// 				    checker_by=$1,
// 				    checker_at=NOW(),
// 				    checker_comment=$2
// 				WHERE booking_id=$3
// 				  AND processing_status LIKE '%PENDING%'
// 			`, userEmail, req.Comment, bID)

// 			// ✅ FINAL STATUS CHANGE (UPDATED)
// 			_, err2 := tx.Exec(ctx, `
// 				UPDATE investment.fd_booking_request
// 				SET booking_status='SENT_TO_BANK',
// 				    approved_at = NOW(),
// 				    sent_to_bank_at = NOW()
// 				WHERE booking_id=$1
// 				  AND booking_status <> 'SENT_TO_BANK'
// 			`, bID)

// 			_, err3 := tx.Exec(ctx, `
// 				UPDATE investment.fd_booking_request
// 				SET is_deleted=true
// 				WHERE booking_id IN (
// 					SELECT DISTINCT a.booking_id
// 					FROM investment.fd_audit_booking_request a
// 					WHERE a.booking_id=$1
// 					  AND a.action_type='DELETE'
// 					  AND a.processing_status='APPROVED'
// 				)
// 			`, bID)

// 			if err1 != nil || err2 != nil || err3 != nil {
// 				_ = tx.Rollback(ctx)
// 				errors = append(errors, bID+": direct stamp failed")
// 				continue
// 			}

// 			if err := tx.Commit(ctx); err != nil {
// 				errors = append(errors, bID+constants.ErrCommitFailed)
// 				continue
// 			}

// 			directActed++
// 		}

// 		// ---------------- RESPONSE ----------------
// 		api.RespondWithPayload(w, true, "", map[string]interface{}{
// 			"engine_acted": engineActed,
// 			"direct_acted": directActed,
// 			"errors":       errors,
// 			"checker":      userEmail,
// 		})

// 		// ---------------- NOTIFICATIONS ----------------
// 		for _, bID := range req.BookingIDs {
// 			go func(id, uEmail string) {
// 				notifcatalog.TriggerNotification(context.Background(), pgxPool,
// 					"/investment/fd/booking/approve",
// 					id,
// 					map[string]interface{}{
// 						"record_id":   id,
// 						"event":       "FD_BOOKING_SENT_TO_BANK", // ✅ updated event
// 						"actor_email": uEmail,
// 					})
// 			}(bID, userEmail)
// 		}

// 		api.LogInfo("[FDBooking] BulkApproveBooking: engine=%d direct=%d errors=%d by=%s",
// 			engineActed, directActed, len(errors), userEmail)
// 	}
// }

func BulkApproveBooking(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID     string   `json:"user_id"`
			BookingIDs []string `json:"booking_ids"`
			Comment    string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.BookingIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrBookingIDsRequired)
			return
		}

		userEmail := api.GetUserEmailFromCtx(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		userID := api.GetUserIDFromCtx(r.Context())

		ctx := r.Context()
		engineActed := 0
		directActed := 0
		var errors []string

		for _, bID := range req.BookingIDs {
			actionRes, actionErr := approvalengine.ActOnPendingOrDiagnose(ctx, pgxPool, approvalengine.ActOnPendingRequest{ModuleCode: "FIXED_DEPOSIT", RecordID: bID, UserID: req.UserID, UserEmail: userEmail, RoleID: "", Action: approvalengine.ActionApproved, Comment: req.Comment})
			if actionErr != nil {
				api.LogError("[FDBooking] RecordAction approve failed for booking %s: %v", bID, actionErr)
				errors = append(errors, bID+": "+actionErr.Error())
				continue
			}
			if actionRes.Acted {
				// Engine path: RecordAction handles audit stamp + status flip for final eye.
				engineActed++
				// If the engine fully approved (last eye done), flip booking status.
				if actionRes.InstanceStatus == constants.StatusApproved {
					if _, execErr := pgxPool.Exec(ctx,
						`UPDATE investment.fd_booking_request
						 SET booking_status = 'SENT_TO_BANK'
						 WHERE booking_id = $1 AND booking_status NOT IN ('SENT_TO_BANK')`, bID,
					); execErr != nil {
						api.LogError("[FDBooking] booking_status→SENT_TO_BANK failed for %s: %v", bID, execErr)
					}
					if _, execErr := pgxPool.Exec(ctx,
						`UPDATE investment.fd_booking_request SET is_deleted = true
						 WHERE booking_id IN (
							 SELECT DISTINCT a.booking_id FROM investment.fd_audit_booking_request a
							 WHERE a.booking_id = $1 AND a.action_type = 'DELETE' AND a.processing_status = 'APPROVED'
						 )`, bID,
					); execErr != nil {
						api.LogError("[FDBooking] is_deleted flip failed for %s: %v", bID, execErr)
					}

				}
			} else {
				if actionRes.CancelledStale {
					api.LogInfo("[FDBooking] Cancelled stale approval instance for booking=%s: %s", bID, actionRes.Reason)
				} else if actionRes.Reason != "" {
					errors = append(errors, bID+": "+actionRes.Reason)
					continue
				}
				// No matrix/instance — direct stamp (legacy / no-matrix path).
				tx, err := pgxPool.Begin(ctx)
				if err != nil {
					errors = append(errors, bID+": tx begin failed")
					continue
				}
				tag1, err1 := tx.Exec(ctx, `UPDATE investment.fd_audit_booking_request
					SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2, checker_ip=$3
					WHERE booking_id=$4 AND processing_status LIKE '%PENDING%'`,
					api.SystemIfBlank(userEmail), req.Comment, api.SystemIfBlank(api.ClientIPFromContext(ctx)), bID)
				_, err2 := tx.Exec(ctx, `UPDATE investment.fd_booking_request SET booking_status='SENT_TO_BANK'
					WHERE booking_id=$1 AND booking_status NOT IN ('SENT_TO_BANK','APPROVED')`, bID)
				_, err3 := tx.Exec(ctx, `UPDATE investment.fd_booking_request SET is_deleted=true
					WHERE booking_id IN (
						SELECT DISTINCT a.booking_id FROM investment.fd_audit_booking_request a
						WHERE a.booking_id=$1 AND a.action_type='DELETE' AND a.processing_status='APPROVED'
					)`, bID)
				if err1 != nil || err2 != nil || err3 != nil {
					_ = tx.Rollback(ctx)
					errors = append(errors, bID+": direct stamp failed")
					continue
				}
				if tag1.RowsAffected() == 0 {
					_ = tx.Rollback(ctx)
					errors = append(errors, bID+": no pending audit action found (already approved or not found)")
					continue
				}
				if cerr := tx.Commit(ctx); cerr != nil {
					errors = append(errors, bID+constants.ErrCommitFailed)
					continue
				}
				directActed++
			}
		}

		totalActed := engineActed + directActed
		success := totalActed > 0 || len(errors) == 0
		msg := ""
		if !success {
			msg = "No bookings were approved"
		}
		api.RespondWithPayload(w, success, msg, map[string]interface{}{
			"engine_acted": engineActed, "direct_acted": directActed,
			"errors": errors, "checker": userEmail,
		})
		for _, bID := range req.BookingIDs {
			go func(id, uEmail, uID string) {
				defer func() {
					if rec := recover(); rec != nil {
						api.LogError("[FDBooking] approve notification goroutine panic for booking %s: %v", id, rec)
					}
				}()
				notifcatalog.TriggerNotification(context.Background(), pgxPool, "/investment/fd/booking/approve", id, map[string]interface{}{
					"record_id":   id,
					"event":       "FD_BOOKING_APPROVED",
					"user_id":     uID,
					"actor_email": uEmail,
				})
			}(bID, userEmail, userID)
		}
		api.LogInfo("[FDBooking] BulkApproveBooking: engine=%d direct=%d errors=%d by=%s",
			engineActed, directActed, len(errors), userEmail)
	}
}

// ─── BulkRejectBooking ───────────────────────────────────────────────────────

func BulkRejectBooking(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID     string   `json:"user_id"`
			BookingIDs []string `json:"booking_ids"`
			Comment    string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.BookingIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrBookingIDsRequired)
			return
		}

		userEmail := api.GetUserEmailFromCtx(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		userID := api.GetUserIDFromCtx(r.Context())

		ctx := r.Context()
		engineActed := 0
		directActed := 0
		var errors []string

		for _, bID := range req.BookingIDs {
			actionRes, actionErr := approvalengine.ActOnPendingOrDiagnose(ctx, pgxPool, approvalengine.ActOnPendingRequest{ModuleCode: "FIXED_DEPOSIT", RecordID: bID, UserID: req.UserID, UserEmail: userEmail, RoleID: "", Action: approvalengine.ActionRejected, Comment: req.Comment})
			if actionErr != nil {
				api.LogError("[FDBooking] RecordAction reject failed for booking %s: %v", bID, actionErr)
				errors = append(errors, bID+": "+actionErr.Error())
				continue
			}
			if actionRes.Acted {
				// finalizeRecord already set processing_status=REJECTED; flip booking status.
				if _, execErr := pgxPool.Exec(ctx,
					`UPDATE investment.fd_booking_request SET booking_status='REJECTED'
					 WHERE booking_id = $1 AND booking_status NOT IN ('SENT_TO_BANK','APPROVED')`, bID,
				); execErr != nil {
					api.LogError("[FDBooking] booking_status→REJECTED failed for %s: %v", bID, execErr)
				}
				engineActed++
			} else {
				if actionRes.CancelledStale {
					api.LogInfo("[FDBooking] Cancelled stale approval instance for booking=%s: %s", bID, actionRes.Reason)
				} else if actionRes.Reason != "" {
					errors = append(errors, bID+": "+actionRes.Reason)
					continue
				}
				tx, err := pgxPool.Begin(ctx)
				if err != nil {
					errors = append(errors, bID+": tx begin failed")
					continue
				}
				tag1, err1 := tx.Exec(ctx, `UPDATE investment.fd_audit_booking_request
					SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2, checker_ip=$3
					WHERE booking_id=$4 AND processing_status LIKE '%PENDING%'`,
					api.SystemIfBlank(userEmail), req.Comment, api.SystemIfBlank(api.ClientIPFromContext(ctx)), bID)
				_, err2 := tx.Exec(ctx, `UPDATE investment.fd_booking_request SET booking_status='REJECTED'
					WHERE booking_id=$1 AND booking_status NOT IN ('SENT_TO_BANK','APPROVED')`, bID)
				if err1 != nil || err2 != nil {
					_ = tx.Rollback(ctx)
					errors = append(errors, bID+": direct stamp failed")
					continue
				}
				if tag1.RowsAffected() == 0 {
					_ = tx.Rollback(ctx)
					errors = append(errors, bID+": no pending audit action found (already rejected or not found)")
					continue
				}
				if cerr := tx.Commit(ctx); cerr != nil {
					errors = append(errors, bID+constants.ErrCommitFailed)
					continue
				}
				directActed++
			}
		}
		totalActed := engineActed + directActed
		success := totalActed > 0 || len(errors) == 0
		msg := ""
		if !success {
			msg = "No bookings were rejected"
		}
		api.RespondWithPayload(w, success, msg, map[string]interface{}{
			"engine_acted": engineActed, "direct_acted": directActed,
			"errors": errors, "checker": userEmail,
		})
		for _, bID := range req.BookingIDs {
			go func(id, uEmail, uID string) {
				defer func() {
					if rec := recover(); rec != nil {
						api.LogError("[FDBooking] reject notification goroutine panic for booking %s: %v", id, rec)
					}
				}()
				notifcatalog.TriggerNotification(context.Background(), pgxPool, "/investment/fd/booking/reject", id, map[string]interface{}{
					"record_id":   id,
					"event":       "FD_BOOKING_REJECTED",
					"user_id":     uID,
					"actor_email": uEmail,
				})
			}(bID, userEmail, userID)
		}
		api.LogInfo("[FDBooking] BulkRejectBooking: engine=%d direct=%d errors=%d by=%s",
			engineActed, directActed, len(errors), userEmail)
	}
}

// ─── GetBookingsWithAudit ────────────────────────────────────────────────────

func GetBookingsWithAudit(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		accountExpr, err := resolveFDBookingAccountExpression(ctx, pgxPool, "m")
		if err != nil {
			msg, status := getUserFriendlyFDError(err, constants.ErrLoadBookingSchemaFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		q := fmt.Sprintf(`
			WITH latest_audit AS (
				SELECT DISTINCT ON (a.booking_id)
					a.booking_id,
					a.audit_id,
					a.action_type,
					a.processing_status,
					a.requested_by,
					a.requested_at,
					a.checker_by,
					a.checker_at,
					a.checker_comment,
					a.reason,
					a.old_principal_amount,
					a.old_interest_rate,
						a.old_tenure_days,
						a.old_tenor_type,
						a.old_tenure_years,
					a.old_value_date,
					a.old_expected_maturity_date,
					a.old_source_account_id,
					a.old_interest_type_code,
					a.old_interest_type_id,
					a.old_expected_start_date,
					a.old_frequency_id,
					a.old_day_count_code,
					a.old_tds_plan_id,
					a.old_booking_remarks,
					a.old_auto_renewal
				FROM investment.fd_audit_booking_request a
				ORDER BY a.booking_id,
				         GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp),
				                  COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC
			),
			history AS (
				SELECT
					booking_id,
					MAX(CASE WHEN action_type='CREATE' THEN requested_by END) AS created_by,
					MAX(CASE WHEN action_type='CREATE' THEN TO_CHAR((requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS') END) AS created_at,
					MAX(CASE WHEN action_type='EDIT'   THEN requested_by END) AS edited_by,
					MAX(CASE WHEN action_type='EDIT'   THEN TO_CHAR((requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS') END) AS edited_at,
					MAX(CASE WHEN action_type='DELETE' THEN requested_by END) AS deleted_by,
					MAX(CASE WHEN action_type='DELETE' THEN TO_CHAR((requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS') END) AS deleted_at
				FROM investment.fd_audit_booking_request
				GROUP BY booking_id
			)
			SELECT
				m.booking_id,
				COALESCE(m.entity_id,'')                                            AS entity_id,
				COALESCE(m.entity_name,'')                                          AS entity_name,
				COALESCE(m.bank_id,'')                                              AS bank_id,
				COALESCE(m.bank_name,'')                                            AS bank_name,
				%s                                                                  AS bank_account_id,
				COALESCE(m.source_account_number,'')                                AS source_account_number,
				COALESCE(m.bank_config_id,'')                                       AS bank_config_id,
				COALESCE(m.principal_amount,0)                                      AS principal_amount,
				COALESCE(NULLIF(m.value_type,''),'Actual')                          AS value_type,
				COALESCE(m.interest_rate,0)                                         AS interest_rate,
				COALESCE(m.interest_type_code,'')                                   AS interest_type,
				COALESCE(m.interest_type_id,'')                                     AS interest_type_id,
				COALESCE(m.tenure_days,0)                                           AS tenor_days,
				COALESCE(m.tenure_months,0)                                         AS tenor_months,
				COALESCE(m.tenure_years,0)                                          AS tenor_years,
				COALESCE(m.tenor_type,'')                                           AS tenor_type,
				COALESCE(TO_CHAR(m.expected_start_date,'YYYY-MM-DD'),'')            AS expected_start_date,
				COALESCE(TO_CHAR(m.value_date,'YYYY-MM-DD'),'')                     AS value_date,
				COALESCE(TO_CHAR(m.expected_maturity_date,'YYYY-MM-DD'),'')         AS maturity_date,
				COALESCE(m.frequency_id,'')                                         AS frequency_id,
				COALESCE(m.day_count_code,'')                                       AS day_count_code,
				COALESCE(m.tds_plan_id,'')                                          AS tds_plan_id,
				COALESCE(m.product_code,'')                                         AS product_code,
				COALESCE(m.auto_renewal,false)                                      AS auto_renewal,
				COALESCE(m.booking_remarks,'')                                      AS booking_remarks,
				COALESCE(TO_CHAR(m.offer_valid_till,'YYYY-MM-DD'),'')              AS offer_valid_till,
				COALESCE(m.booking_status,'')                                       AS booking_status,
				COALESCE(m.is_deleted,false)                                        AS is_deleted,
				COALESCE(TO_CHAR(m.created_at,'YYYY-MM-DD HH24:MI:SS'),'')         AS record_created_at,
				COALESCE(m.created_by,'')                                           AS record_created_by,

				COALESCE(l.audit_id::text,'')                                       AS audit_id,
				COALESCE(l.action_type,'')                                          AS action_type,
				COALESCE(l.processing_status,'')                                    AS processing_status,
				COALESCE(l.requested_by,'')                                         AS requested_by,
				COALESCE(TO_CHAR((l.requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'),'')       AS requested_at,
				COALESCE(l.checker_by,'')                                           AS checker_by,
				COALESCE(TO_CHAR((l.checker_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'),'')         AS checker_at,
				COALESCE(l.checker_comment,'')                                      AS checker_comment,
				COALESCE(l.reason,'')                                               AS reason,
				COALESCE(l.old_principal_amount,0)                                  AS old_principal_amount,
				COALESCE(l.old_interest_rate,0)                                     AS old_interest_rate,
				COALESCE(l.old_interest_type_code,'')                               AS old_interest_type,
				COALESCE(l.old_interest_type_id,'')                                 AS old_interest_type_id,
				COALESCE(l.old_tenure_days,0)                                       AS old_tenor_days,
				COALESCE(l.old_tenor_type,'')                                       AS old_tenor_type,
				COALESCE(l.old_tenure_years,0)                                      AS old_tenure_years,
				COALESCE(TO_CHAR(l.old_expected_start_date,'YYYY-MM-DD'),'')        AS old_expected_start_date,
				COALESCE(TO_CHAR(l.old_value_date,'YYYY-MM-DD'),'')                 AS old_value_date,
				COALESCE(TO_CHAR(l.old_expected_maturity_date,'YYYY-MM-DD'),'')     AS old_maturity_date,
				COALESCE(l.old_source_account_id,'')                                AS old_bank_account_id,
				COALESCE(l.old_frequency_id,'')                                     AS old_frequency_id,
				COALESCE(l.old_day_count_code,'')                                   AS old_day_count_code,
				COALESCE(l.old_tds_plan_id,'')                                      AS old_tds_plan_id,
				COALESCE(l.old_booking_remarks,'')                                  AS old_booking_remarks,
				COALESCE(l.old_auto_renewal,false)                                  AS old_auto_renewal,

				COALESCE(h.created_by,'')                                           AS created_by,
				COALESCE(h.created_at,'')                                           AS created_at,
				COALESCE(h.edited_by,'')                                            AS edited_by,
				COALESCE(h.edited_at,'')                                            AS edited_at,
				COALESCE(h.deleted_by,'')                                           AS deleted_by,
				COALESCE(h.deleted_at,'')                                           AS deleted_at,

				-- Approval engine columns
				COALESCE(ai.instance_id,'')                                         AS approval_instance_id,
				COALESCE(ai.status,'')                                              AS approval_engine_status,
				COALESCE(aie.instance_eye_id,'')                                    AS current_eye_id,
				COALESCE(aie.position::text,'')                                     AS current_eye_position,
				COALESCE(aie.approvals_required,0)                                  AS approvals_required,
				COALESCE(aie.approvals_received,0)                                  AS approvals_received,
				aie.sla_deadline                                                    AS sla_deadline,
				COALESCE(aie.is_escalated,false)                                    AS is_escalated,

				COALESCE(bc.holiday_calendar_code,'')                               AS holiday_calendar_code,
				COALESCE(bc.tds_deduction_timing,'')                                AS tds_deduction_timing,
				COALESCE(bc.rounding_method,'')                                     AS rounding_method,
				COALESCE(bc.rounding_frequency,'')                                  AS rounding_frequency,
				COALESCE(bc.interest_rounding_decimals,2)                           AS interest_rounding_decimals,
				COALESCE(bc.broken_period_method,'')                                AS broken_period_method,
				COALESCE(bc.broken_period_location,'')                              AS broken_period_location,
				COALESCE(bc.grace_period_days,0)                                    AS grace_period_days,
				COALESCE(bc.grace_period_rate_type,'')                              AS grace_period_rate_type,
				COALESCE(bc.capitalization_schedule_type,'')                        AS capitalization_schedule_type,
				COALESCE(bc.capitalization_date_adjustment,'')                        AS capitalization_date_adjustment,
				COALESCE(bc.weekend_accrual,true)                                     AS weekend_accrual,
				COALESCE(bc.holiday_accrual,true)                                     AS holiday_accrual,
				COALESCE(bc.accrual_start_convention,'')                              AS accrual_start_convention,
				COALESCE(bc.accrual_end_convention,'')                              AS accrual_end_convention,
				COALESCE(bc.period_boundary_definition,'')                          AS period_boundary_definition,
				COALESCE(bc.quarter_definition,'')                                  AS quarter_definition,
				COALESCE(m.accrual_frequency_code,'')                                 AS accrual_frequency_code,
				COALESCE(NULLIF(m.reset_type,''),'AT_MATURITY')                       AS reset_type,
				COALESCE(NULLIF(m.payout_frequency_id,''), m.frequency_id, '')      AS payout_frequency_id,
				COALESCE(TO_CHAR(c.first_payout_date,'YYYY-MM-DD'),'')               AS first_payout_date,
				COALESCE(TO_CHAR(c.first_capitalization_date,'YYYY-MM-DD'),'')       AS first_capitalization_date,
				COALESCE(c.accrual_frequency_code,'')                                 AS confirmed_accrual_frequency_code,
				COALESCE(c.reset_type,'')                                             AS confirmed_reset_type,
				COALESCE(c.actual_principal,0)                                        AS actual_principal,
				COALESCE(c.confirmed_rate,0)                                          AS confirmed_rate,
				COALESCE(TO_CHAR(c.actual_start_date,'YYYY-MM-DD'),'')                AS actual_start_date,
				COALESCE(TO_CHAR(c.actual_maturity_date,'YYYY-MM-DD'),'')             AS actual_maturity_date,
				COALESCE(c.confirmed_interest_type_code,'')                           AS confirmed_interest_type

			FROM investment.fd_booking_request m
			LEFT JOIN investment.fd_confirmation c
				ON c.booking_id = m.booking_id AND COALESCE(c.is_deleted,false) = false
			LEFT JOIN investment.fd_bank_config_master bc
				ON bc.config_id = m.bank_config_id AND COALESCE(bc.is_deleted,false) = false
			LEFT JOIN latest_audit l    ON l.booking_id = m.booking_id
			LEFT JOIN history h         ON h.booking_id = m.booking_id
			LEFT JOIN uam.approval_instance ai
				ON ai.record_id = m.booking_id
				AND ai.module_code = 'FIXED_DEPOSIT'
				AND ai.status = 'PENDING'
				AND ai.is_deleted = false
			LEFT JOIN uam.approval_instance_eye aie
				ON aie.instance_id = ai.instance_id
				AND aie.status = 'ACTIVE'
			WHERE COALESCE(m.is_deleted,false) = false
			ORDER BY GREATEST(
				COALESCE(l.requested_at,'1970-01-01'::timestamp),
				COALESCE(l.checker_at,'1970-01-01'::timestamp)
			) DESC`, accountExpr)

		rows, err := pgxPool.Query(ctx, q)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		fields := rows.FieldDescriptions()
		out := make([]map[string]interface{}, 0, 100)
		for rows.Next() {
			vals, _ := rows.Values()
			row := make(map[string]interface{}, len(fields))
			for i, f := range fields {
				if vals[i] == nil {
					row[string(f.Name)] = ""
				} else {
					row[string(f.Name)] = vals[i]
				}
			}
			AttachSimulateDiffInput(row)
			out = append(out, row)
		}
		if err := rows.Err(); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Row iteration error: "+err.Error())
			return
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]any{constants.ValueSuccess: true, "rows": out}) //nolint:errcheck
		api.LogInfo("[FDBooking] GetBookingsWithAudit: %d rows", len(out))
	}
}

// ─── GetBookingDetail ────────────────────────────────────────────────────────

func GetBookingDetail(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		bookingID := r.URL.Query().Get("booking_id")
		if bookingID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "booking_id is required")
			return
		}
		ctx := r.Context()
		accountExpr, err := resolveFDBookingAccountExpression(ctx, pgxPool, "fd_booking_request")
		if err != nil {
			msg, status := getUserFriendlyFDError(err, constants.ErrLoadBookingSchemaFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		// Main booking row — use dynamic column scan to avoid any future schema drift
		var bk map[string]interface{}
		{
			detailRows, err := pgxPool.Query(ctx, fmt.Sprintf(`
				SELECT
					booking_id,
					COALESCE(entity_id,'')                                        AS entity_id,
					COALESCE(entity_name,'')                                       AS entity_name,
					COALESCE(bank_id,'')                                           AS bank_id,
					COALESCE(bank_name,'')                                         AS bank_name,
					%s                                                             AS bank_account_id,
					COALESCE(source_account_number,'')                             AS source_account_number,
					COALESCE(bank_config_id,'')                                    AS bank_config_id,
					COALESCE(principal_amount,0)                                   AS principal_amount,
					COALESCE(NULLIF(value_type,''),'Actual')                         AS value_type,
					COALESCE(interest_rate,0)                                      AS interest_rate,
					COALESCE(interest_type_code,'')                                AS interest_type,
					COALESCE(interest_type_id,'')                                  AS interest_type_id,
					COALESCE(tenure_days,0)                                        AS tenor_days,
					COALESCE(tenure_months,0)                                      AS tenor_months,
					COALESCE(tenure_years,0)                                       AS tenor_years,
					COALESCE(tenor_type,'')                                         AS tenor_type,
					COALESCE(TO_CHAR(expected_start_date,'YYYY-MM-DD'),'')         AS expected_start_date,
					COALESCE(TO_CHAR(value_date,'YYYY-MM-DD'),'')                  AS value_date,
					COALESCE(TO_CHAR(expected_maturity_date,'YYYY-MM-DD'),'')      AS maturity_date,
					COALESCE(frequency_id,'')                                      AS frequency_id,
					COALESCE(day_count_code,'')                                    AS day_count_code,
					COALESCE(tds_plan_id,'')                                       AS tds_plan_id,
					COALESCE(product_code,'')                                      AS product_code,
					COALESCE(auto_renewal,false)                                   AS auto_renewal,
					COALESCE(booking_remarks,'')                                   AS booking_remarks,
					COALESCE(TO_CHAR(offer_valid_till,'YYYY-MM-DD'),'')            AS offer_valid_till,
					COALESCE(booking_status,'')                                    AS booking_status,
					COALESCE(is_deleted,false)                                     AS is_deleted,
					COALESCE(TO_CHAR(created_at,'YYYY-MM-DD HH24:MI:SS'),'')      AS record_created_at,
					COALESCE(created_by,'')                                        AS record_created_by
				FROM investment.fd_booking_request
				WHERE booking_id = $1`, accountExpr), bookingID)
			if err != nil {
				msg, httpStatus := getUserFriendlyFDError(err, constants.ErrQueryFailed)
				api.RespondWithError(w, httpStatus, msg)
				return
			}
			defer detailRows.Close()
			if !detailRows.Next() {
				api.RespondWithError(w, http.StatusNotFound, "Booking not found")
				return
			}
			dFields := detailRows.FieldDescriptions()
			dVals, _ := detailRows.Values()
			bk = make(map[string]interface{}, len(dFields))
			for i, f := range dFields {
				if dVals[i] == nil {
					bk[string(f.Name)] = ""
				} else {
					bk[string(f.Name)] = dVals[i]
				}
			}
			detailRows.Close()
		}

		// Audit history
		auditRows, err := pgxPool.Query(ctx, `
			SELECT
				a.audit_id::text, a.action_type, a.processing_status,
				COALESCE(a.requested_by,'')                                        AS requested_by,
				COALESCE(TO_CHAR((a.requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'),'')      AS requested_at,
				COALESCE(a.checker_by,'')                                          AS checker_by,
				COALESCE(TO_CHAR((a.checker_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'),'')        AS checker_at,
				COALESCE(a.checker_comment,'')                                     AS checker_comment,
				COALESCE(a.reason,'')                                              AS reason,
				COALESCE(a.old_principal_amount,0)                                 AS old_principal_amount,
				COALESCE(a.old_interest_rate,0)                                    AS old_interest_rate,
				COALESCE(a.old_interest_type_code,'')                              AS old_interest_type,
				COALESCE(a.old_interest_type_id,'')                                AS old_interest_type_id,
				COALESCE(a.old_tenure_days,0)                                      AS old_tenor_days,
				COALESCE(a.old_tenor_type,'')                                     AS old_tenor_type,
				COALESCE(a.old_tenure_years,0)                                     AS old_tenure_years,
				COALESCE(TO_CHAR(a.old_expected_start_date,'YYYY-MM-DD'),'')       AS old_expected_start_date,
				COALESCE(TO_CHAR(a.old_value_date,'YYYY-MM-DD'),'')                AS old_value_date,
				COALESCE(TO_CHAR(a.old_expected_maturity_date,'YYYY-MM-DD'),'')    AS old_maturity_date,
				COALESCE(a.old_source_account_id,'')                               AS old_bank_account_id,
				COALESCE(a.old_frequency_id,'')                                    AS old_frequency_id,
				COALESCE(a.old_day_count_code,'')                                  AS old_day_count_code,
				COALESCE(a.old_tds_plan_id,'')                                     AS old_tds_plan_id,
				COALESCE(a.old_booking_remarks,'')                                 AS old_booking_remarks,
				COALESCE(a.old_auto_renewal,false)                                 AS old_auto_renewal,
				COALESCE(a.old_booking_status,'')                                  AS old_booking_status
			FROM investment.fd_audit_booking_request a
			WHERE a.booking_id = $1
			ORDER BY GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp),COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC`,
			bookingID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer auditRows.Close()

		auditHistory := make([]map[string]interface{}, 0)
		auditFields := auditRows.FieldDescriptions()
		for auditRows.Next() {
			vals, _ := auditRows.Values()
			row := make(map[string]interface{}, len(auditFields))
			for i, f := range auditFields {
				if vals[i] == nil {
					row[string(f.Name)] = ""
				} else {
					row[string(f.Name)] = vals[i]
				}
			}
			auditHistory = append(auditHistory, row)
		}
		if err := auditRows.Err(); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Audit row error: "+err.Error())
			return
		}

		// Approval workflow — look up the instance for this booking then fetch rich detail.
		// viewer_user_id is optional: pass it to get viewer_can_act / viewer_active_eye_id.
		viewerUserID := r.URL.Query().Get("user_id")

		var approvalWorkflow interface{}
		{
			// Find the most-recent instance for this booking in the FIXED_DEPOSIT module
			var instanceID string
			_ = pgxPool.QueryRow(ctx, `
				SELECT instance_id
				FROM uam.approval_instance
				WHERE record_id = $1 AND module_code = 'FIXED_DEPOSIT' AND is_deleted = false
				ORDER BY submitted_at DESC LIMIT 1`, bookingID,
			).Scan(&instanceID)

			// ── Self-heal: if no instance exists yet but the booking has a pending
			// audit row, create the instance now (handles bookings created before
			// the engine was active, or where the goroutine failed silently).
			if instanceID == "" {
				var pendingActionType, submittedBy, entityID string
				var principalAmount float64
				scanErr := pgxPool.QueryRow(ctx, `
					SELECT a.action_type,
					       COALESCE(a.requested_by,''),
					       COALESCE(b.entity_id,''),
					       COALESCE(b.principal_amount,0)
					FROM investment.fd_audit_booking_request a
					JOIN investment.fd_booking_request b ON b.booking_id = a.booking_id
					WHERE a.booking_id = $1
					  AND a.processing_status LIKE '%PENDING%'
					ORDER BY a.requested_at DESC LIMIT 1`, bookingID,
				).Scan(&pendingActionType, &submittedBy, &entityID, &principalAmount)

				if scanErr == nil && pendingActionType != "" {
					txType := map[string]string{
						"CREATE": "FD_BOOKING",
						"EDIT":   "FD_BOOKING_EDIT",
						"DELETE": "FD_BOOKING_DELETE",
					}[pendingActionType]
					if txType == "" {
						txType = "FD_BOOKING"
					}
					newInstID, instErr := approvalengine.CreateInstance(ctx, pgxPool, approvalengine.InstanceRequest{
						ModuleCode:       "FIXED_DEPOSIT",
						EntityCode:       entityID,
						TransactionType:  txType,
						RecordID:         bookingID,
						RecordTable:      constants.QuerryBookingRequest,
						AuditTable:       constants.QuerryAuditBookingRequest,
						AuditIDColumn:    "booking_id",
						ActionType:       pendingActionType,
						Amount:           principalAmount,
						SubmittedBy:      submittedBy,
						SubmittedByEmail: submittedBy,
					})
					if instErr != nil {
						api.LogError("[FDBooking] Self-heal CreateInstance for %s: %v", bookingID, instErr)
					} else if newInstID != "" {
						instanceID = newInstID
						api.LogInfo("[FDBooking] Self-heal: created instance %s for booking %s", newInstID, bookingID)
					}
				}
			}

			if instanceID != "" {
				richDetail, richErr := approvalengine.GetRichInstanceDetail(ctx, pgxPool, instanceID, viewerUserID)
				if richErr != nil {
					api.LogError("[FDBooking] GetRichInstanceDetail failed for instance=%s booking=%s: %v", instanceID, bookingID, richErr)
				} else {
					approvalWorkflow = richDetail
				}
			}
		}

		resp := map[string]interface{}{
			"booking":           bk,
			"audit_history":     auditHistory,
			"approval_workflow": approvalWorkflow,
		}

		api.RespondWithPayload(w, true, "", resp)
		api.LogInfo("[FDBooking] GetBookingDetail: booking_id=%s", bookingID)
	}
}

// ─── GetBookingAuditHistory ──────────────────────────────────────────────────

func GetBookingAuditHistory(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		bookingID := r.URL.Query().Get("booking_id")

		var q string
		var args []interface{}
		auditSelect := `
				SELECT
					a.audit_id, a.booking_id, a.action_type, a.processing_status,
					COALESCE(a.requested_by,'')                                       AS requested_by,
					COALESCE(TO_CHAR((a.requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'),'')     AS requested_at,
					COALESCE(a.checker_by,'')                                         AS checker_by,
					COALESCE(TO_CHAR((a.checker_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'),'')       AS checker_at,
					COALESCE(a.checker_comment,'')                                    AS checker_comment,
					COALESCE(a.reason,'')                                             AS reason,
					COALESCE(m.entity_id,'')                                          AS entity_id,
					COALESCE(m.entity_name,'')                                        AS entity_name,
					COALESCE(m.bank_id,'')                                            AS bank_id,
					COALESCE(m.bank_name,'')                                          AS bank_name,
					COALESCE(m.booking_status,'')                                     AS booking_status,
					COALESCE(m.principal_amount,0)                                    AS principal_amount,
					COALESCE(m.interest_rate,0)                                       AS interest_rate,
					COALESCE(m.interest_type_code,'')                                 AS interest_type,
					COALESCE(m.interest_type_id,'')                                   AS interest_type_id,
					COALESCE(m.tenure_days,0)                                         AS tenor_days,
					COALESCE(TO_CHAR(m.expected_start_date,'YYYY-MM-DD'),'')          AS expected_start_date,
					COALESCE(TO_CHAR(m.value_date,'YYYY-MM-DD'),'')                   AS value_date,
					COALESCE(TO_CHAR(m.expected_maturity_date,'YYYY-MM-DD'),'')       AS maturity_date,
					COALESCE(a.old_principal_amount,0)                                AS old_principal_amount,
					COALESCE(a.old_interest_rate,0)                                   AS old_interest_rate,
					COALESCE(a.old_interest_type_code,'')                             AS old_interest_type,
					COALESCE(a.old_interest_type_id,'')                               AS old_interest_type_id,
					COALESCE(a.old_tenure_days,0)                                     AS old_tenor_days,
					COALESCE(a.old_tenor_type,'')                                     AS old_tenor_type,
					COALESCE(a.old_tenure_years,0)                                    AS old_tenure_years,
					COALESCE(TO_CHAR(a.old_expected_start_date,'YYYY-MM-DD'),'')      AS old_expected_start_date,
					COALESCE(TO_CHAR(a.old_value_date,'YYYY-MM-DD'),'')               AS old_value_date,
					COALESCE(TO_CHAR(a.old_expected_maturity_date,'YYYY-MM-DD'),'')   AS old_maturity_date,
					COALESCE(a.old_source_account_id,'')                              AS old_bank_account_id,
					COALESCE(a.old_frequency_id,'')                                   AS old_frequency_id,
					COALESCE(a.old_day_count_code,'')                                 AS old_day_count_code,
					COALESCE(a.old_tds_plan_id,'')                                    AS old_tds_plan_id,
					COALESCE(a.old_booking_remarks,'')                                AS old_booking_remarks,
					COALESCE(a.old_auto_renewal,false)                                AS old_auto_renewal,
					COALESCE(a.old_booking_status,'')                                 AS old_booking_status
				FROM investment.fd_audit_booking_request a
				LEFT JOIN investment.fd_booking_request m ON m.booking_id = a.booking_id`
		if bookingID != "" {
			q = auditSelect + `
				WHERE a.booking_id = $1
				ORDER BY GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp),COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC`
			args = append(args, bookingID)
		} else {
			q = auditSelect + `
				ORDER BY GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp),COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC
				LIMIT 1000`
		}

		rows, err := pgxPool.Query(ctx, q, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		fields := rows.FieldDescriptions()
		out := make([]map[string]interface{}, 0)
		for rows.Next() {
			vals, _ := rows.Values()
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
		if err := rows.Err(); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrRowError+err.Error())
			return
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]any{constants.ValueSuccess: true, "audit_logs": out}) //nolint:errcheck
		api.LogInfo("[FDBooking] GetBookingAuditHistory: %d records", len(out))
	}
}

// ─── GetApprovedActiveBookings ───────────────────────────────────────────────
// Returns all bookings in the post-approval lifecycle:
//   APPROVED      → internally approved, pending physical dispatch to bank
//   SENT_TO_BANK  → dispatched to bank, awaiting bank confirmation
//   CONFIRMED     → bank has confirmed, FD is live (fd_master may exist)
//
// Optional query params:
//   entity_id   – filter by entity
//   status      – filter by specific booking_status (comma-separated or single)

func GetApprovedActiveBookings(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		entityID := r.URL.Query().Get("entity_id")
		statusFilter := r.URL.Query().Get("status") // optional single-status override

		baseSelect := `
			SELECT
				m.booking_id,
				COALESCE(m.entity_id,'')                                       AS entity_id,
				COALESCE(m.entity_name,'')                                     AS entity_name,
				COALESCE(m.bank_id,'')                                         AS bank_id,
				COALESCE(m.bank_name,'')                                       AS bank_name,
				COALESCE(m.source_account_id,'')                               AS bank_account_id,
				COALESCE(m.source_account_number,'')                           AS bank_account_number,
				COALESCE(m.principal_amount,0)                                 AS principal_amount,
				COALESCE(NULLIF(m.value_type,''),'Actual')                     AS value_type,
				COALESCE(m.interest_rate,0)                                    AS interest_rate,
				COALESCE(m.interest_type_code,'')                              AS interest_type,
				COALESCE(m.interest_type_id,'')                                AS interest_type_id,
				COALESCE(m.tenure_days,0)                                      AS tenor_days,
				COALESCE(m.tenure_months,0)                                    AS tenor_months,
				COALESCE(m.tenure_years,0)                                     AS tenor_years,
				COALESCE(m.tenor_type,'')                                       AS tenor_type,
				COALESCE(TO_CHAR(m.expected_start_date,'YYYY-MM-DD'),'')       AS expected_start_date,
				COALESCE(TO_CHAR(m.value_date,'YYYY-MM-DD'),'')                AS value_date,
				COALESCE(TO_CHAR(m.expected_maturity_date,'YYYY-MM-DD'),'')    AS expected_maturity_date,
				COALESCE(m.tds_plan_id,'')                                     AS tds_plan_id,
				COALESCE(m.auto_renewal,false)                                 AS auto_renewal,
				COALESCE(m.booking_status,'')                                  AS booking_status,
				-- confirmation data (NULL until CONFIRMED)
				COALESCE(c.confirmation_id,'')                                 AS confirmation_id,
				COALESCE(c.bank_fd_ref_no,'')                                  AS bank_fd_ref_no,
				COALESCE(NULLIF(BTRIM(c.bank_reference_number::text),''),
				         c.bank_fd_ref_no, '')                                 AS bank_reference_number,
				COALESCE(c.actual_principal,0)                                 AS actual_principal,
				COALESCE(c.confirmed_rate,0)                                   AS confirmed_rate,
				COALESCE(c.confirmed_interest_type_code,'')                    AS confirmed_interest_type,
				COALESCE(TO_CHAR(c.actual_start_date,'YYYY-MM-DD'),'')         AS actual_start_date,
				COALESCE(TO_CHAR(c.actual_maturity_date,'YYYY-MM-DD'),'')      AS actual_maturity_date,
				COALESCE(c.confirmation_status,'')                             AS confirmation_status,
				COALESCE(c.variance_flag,false)                                AS variance_flag,
				COALESCE(c.variance_type,'')                                   AS variance_type,
				-- fd_master data (NULL until ACTIVE)
				COALESCE(fm.fd_id,'')                                          AS fd_id,
				COALESCE(fm.fd_status,'')                                      AS fd_status,
				COALESCE(fm.bank_fd_ref_no,'')                                 AS fd_ref_no,
				-- latest audit: who approved/checked and when (pick most-recent)
				COALESCE(aud.requested_by, aud.checker_by, '')                AS approved_by,
				COALESCE(TO_CHAR(
					GREATEST(
						COALESCE(aud.requested_at,'1970-01-01'::timestamp),
						COALESCE(aud.checker_at,'1970-01-01'::timestamp)
					), 'YYYY-MM-DD HH24:MI'), '')                                AS approved_at,
				-- simulator-ready fields from booking
				COALESCE(m.bank_config_id,'')                                  AS bank_config_id,
				COALESCE(m.frequency_id,'')                                    AS frequency_id,
				COALESCE(m.day_count_code,'')                                  AS day_count_code,
				-- bank config detail columns (flat, used in simulator_input)
				COALESCE(bc.holiday_calendar_code,'')                          AS holiday_calendar_code,
				COALESCE(bc.tds_deduction_timing,'')                           AS tds_deduction_timing,
				COALESCE(bc.rounding_method,'')                                AS rounding_method,
				COALESCE(bc.rounding_frequency,'')                             AS rounding_frequency,
				COALESCE(bc.interest_rounding_decimals,2)                      AS interest_rounding_decimals,
				COALESCE(bc.broken_period_method,'')                           AS broken_period_method,
				COALESCE(bc.broken_period_location,'')                         AS broken_period_location,
				COALESCE(bc.grace_period_days,0)                               AS grace_period_days,
				COALESCE(bc.grace_period_rate_type,'')                         AS grace_period_rate_type,
				COALESCE(bc.capitalization_schedule_type,'')                   AS capitalization_schedule_type,
				COALESCE(bc.capitalization_date_adjustment,'')                 AS capitalization_date_adjustment,
				COALESCE(bc.weekend_accrual,true)                              AS weekend_accrual,
				COALESCE(bc.holiday_accrual,true)                              AS holiday_accrual,
				COALESCE(bc.accrual_start_convention,'')                       AS accrual_start_convention,
				COALESCE(bc.accrual_end_convention,'')                         AS accrual_end_convention,
				COALESCE(bc.period_boundary_definition,'')                     AS period_boundary_definition,
				COALESCE(bc.quarter_definition,'')                             AS quarter_definition,
				COALESCE(m.accrual_frequency_code,'')                          AS accrual_frequency_code,
				COALESCE(NULLIF(m.reset_type,''),'AT_MATURITY')                AS reset_type,
				COALESCE(NULLIF(m.payout_frequency_id,''), m.frequency_id, '')  AS payout_frequency_id,
				COALESCE(TO_CHAR(c.first_payout_date,'YYYY-MM-DD'),'')          AS first_payout_date,
				COALESCE(TO_CHAR(c.first_capitalization_date,'YYYY-MM-DD'),'') AS first_capitalization_date,
				COALESCE(c.accrual_frequency_code,'')                          AS confirmed_accrual_frequency_code,
				COALESCE(c.confirmed_frequency_id, '')  AS confirmed_payout_frequency_id,
				COALESCE(NULLIF(c.reset_type,''),'AT_MATURITY')                AS confirmed_reset_type,
				COALESCE(c.confirmed_frequency_id,'')                          AS confirmed_frequency_id
			FROM investment.fd_booking_request m
			LEFT JOIN investment.fd_confirmation c
				ON c.booking_id = m.booking_id AND COALESCE(c.is_deleted,false) = false
			LEFT JOIN investment.fd_master fm
				ON fm.booking_id = m.booking_id AND COALESCE(fm.is_deleted,false) = false
			LEFT JOIN investment.fd_bank_config_master bc
				ON bc.config_id = m.bank_config_id AND COALESCE(bc.is_deleted,false) = false
			LEFT JOIN LATERAL (
				SELECT requested_by, requested_at, checker_by, checker_at
				FROM investment.fd_audit_booking_request
				WHERE booking_id = m.booking_id AND processing_status = 'APPROVED'
				ORDER BY GREATEST(COALESCE(requested_at,'1970-01-01'::timestamp), COALESCE(checker_at,'1970-01-01'::timestamp)) DESC
				LIMIT 1
			) aud ON true
			WHERE COALESCE(m.is_deleted,false) = false`

		var q string
		var args []interface{}
		argIdx := 1

		// determine which statuses to show
		if statusFilter != "" {
			// caller wants a specific status
			q = baseSelect + ` AND m.booking_status = $` + fmt.Sprintf("%d", argIdx)
			args = append(args, statusFilter)
			argIdx++
		} else {
			// default: bookings eligible for first-time capture — APPROVED and SENT_TO_BANK
			// only, excluding rows that already have a live confirmation (pending approval,
			// variance, confirmed, etc.). REJECTED / VARIANCE_REJECTED may reappear for re-capture.
			q = baseSelect + ` AND m.booking_status IN ('APPROVED','SENT_TO_BANK')
			  AND NOT EXISTS (
			    SELECT 1
			    FROM investment.fd_confirmation c
			    WHERE c.booking_id = m.booking_id
			      AND COALESCE(c.is_deleted, false) = false
			      AND UPPER(COALESCE(c.confirmation_status, '')) NOT IN ('REJECTED', 'VARIANCE_REJECTED')
			  )`
		}

		if entityID != "" {
			q += fmt.Sprintf(` AND m.entity_id = $%d`, argIdx)
			args = append(args, entityID)
			argIdx++
		}

		// Order by the most-recent approval/check timestamp (newest first).
		q += ` ORDER BY GREATEST(COALESCE(aud.requested_at,'1970-01-01'::timestamp), COALESCE(aud.checker_at,'1970-01-01'::timestamp)) DESC`

		rows, err := pgxPool.Query(ctx, q, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		fields := rows.FieldDescriptions()
		out := make([]map[string]interface{}, 0)
		for rows.Next() {
			vals, _ := rows.Values()
			row := make(map[string]interface{}, len(fields))
			for i, f := range fields {
				if vals[i] == nil {
					row[string(f.Name)] = ""
				} else {
					row[string(f.Name)] = vals[i]
				}
			}

			AttachSimulateDiffInput(row)
			out = append(out, row)
		}
		if err := rows.Err(); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrRowError+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", out)
		api.LogInfo("[FDBooking] GetApprovedActiveBookings: %d records (entity=%s status=%s)", len(out), entityID, statusFilter)
	}
}

// ─── MarkAsSentToBank ────────────────────────────────────────────────────────

func MarkAsSentToBank(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID     string   `json:"user_id"`
			BookingIDs []string `json:"booking_ids"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.BookingIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrBookingIDsRequired)
			return
		}

		userEmail := api.GetUserEmailFromCtx(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			msg, status := getUserFriendlyFDError(err, constants.ErrTransactionFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx) //nolint:errcheck

		tag, err := tx.Exec(ctx, `
			UPDATE investment.fd_booking_request
			SET booking_status = 'SENT_TO_BANK'
			WHERE booking_id = ANY($1::text[]) AND booking_status = 'APPROVED'`,
			req.BookingIDs,
		)
		if err != nil {
			msg, status := getUserFriendlyFDError(err, "Status update failed")
			api.RespondWithError(w, status, msg)
			return
		}

		if _, err = tx.Exec(ctx, `
			INSERT INTO investment.fd_audit_booking_request
				(booking_id, action_type, processing_status, requested_by, requested_at, requested_ip, old_booking_status)
			SELECT booking_id, 'EDIT', 'APPROVED', $1, now(), $3, 'APPROVED'
			FROM investment.fd_booking_request
			WHERE booking_id = ANY($2::text[]) AND booking_status = 'SENT_TO_BANK'`,
			api.SystemIfBlank(userEmail), req.BookingIDs, api.SystemIfBlank(api.ClientIPFromContext(ctx)),
		); err != nil {
			msg, status := getUserFriendlyFDError(err, constants.ErrAuditInsertFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		if err = tx.Commit(ctx); err != nil {
			logDBError(err, "MarkAsSentToBank commit")
			msg, status := getUserFriendlyFDError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}

		go func(uEmail string) {
			defer func() {
				if rec := recover(); rec != nil {
					api.LogError("[FDBooking] MarkAsSentToBank notification goroutine panic: %v", rec)
				}
			}()
			notifcatalog.TriggerNotification(context.Background(), pgxPool, "/investment/fd/booking/mark-sent-to-bank", "", map[string]interface{}{
				"event":       "FD_BOOKING_SENT_TO_BANK",
				"actor_email": uEmail,
			})
		}(userEmail)
		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"updated_count": tag.RowsAffected(), "marked_by": userEmail,
		})
		api.LogInfo("[FDBooking] MarkAsSentToBank: %d bookings by %s", tag.RowsAffected(), userEmail)
	}
}

// ─── nullIfEmpty helper ──────────────────────────────────────────────────────

// nullIfEmpty returns nil if the string is empty, otherwise returns the string
// as an interface. Used to avoid storing empty-string UUIDs in FK columns.
func nullIfEmpty(s string) interface{} {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return s
}

// strVal coerces an interface{} map value to string.
// It handles pgx-scanned text/varchar types and returns "" for nil.
func strVal(v interface{}) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}

// isZeroNumeric returns true when v is nil, the integer 0, or the float64 0.
func isZeroNumeric(v interface{}) bool {
	if v == nil {
		return true
	}
	switch n := v.(type) {
	case float64:
		return n == 0
	case float32:
		return n == 0
	case int:
		return n == 0
	case int32:
		return n == 0
	case int64:
		return n == 0
	}
	return false
}
