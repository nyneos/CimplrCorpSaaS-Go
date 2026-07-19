package allMaster

import (
	"CimplrCorpSaas/api"

	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/api/master/bulkuploadaudit"
	"CimplrCorpSaas/api/master/mastererrors"
	"CimplrCorpSaas/api/utils/s3storage"
	"CimplrCorpSaas/internal/dependency"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// getUserFriendlyRateCardError converts database errors to user-friendly messages
func getUserFriendlyRateCardError(err error, context string) (string, int) {
	if err == nil {
		return "", http.StatusOK
	}

	if msg, ok := mastererrors.TryUniqueViolation(err); ok {
		return msg, http.StatusOK
	}
	errStr := strings.ToLower(err.Error())

	if strings.Contains(errStr, constants.ErrTxBeginFailed) ||
		strings.Contains(errStr, constants.ErrAuditInsertFailed) {
		return err.Error(), http.StatusOK
	}

	if strings.Contains(errStr, "inv_rate_card_deposit_type_chk") ||
		(strings.Contains(errStr, constants.CheckConstraint) && strings.Contains(errStr, "deposit_type")) {
		return "Invalid deposit_type. Must be one of: REGULAR, BULK, SENIOR_CITIZEN, SPECIAL.", http.StatusBadRequest
	}
	if strings.Contains(errStr, "inv_rate_card_interest_rate_chk") ||
		(strings.Contains(errStr, constants.CheckConstraint) && strings.Contains(errStr, "interest_rate")) {
		return "interest_rate must be > 0 and <= 100.", http.StatusBadRequest
	}
	if strings.Contains(errStr, "inv_rate_card_tenor_range_chk") {
		return "max_tenor_days must be >= min_tenor_days.", http.StatusBadRequest
	}
	if strings.Contains(errStr, "inv_rate_card_amount_range_chk") {
		return "max_amount must be >= min_amount when both are set.", http.StatusBadRequest
	}
	if strings.Contains(errStr, "inv_rate_card_effective_date_chk") {
		return "effective_to must be >= effective_from when set.", http.StatusBadRequest
	}
	if strings.Contains(errStr, "inv_rate_card_rate_source_chk") ||
		(strings.Contains(errStr, constants.CheckConstraint) && strings.Contains(errStr, "rate_source")) {
		return "Invalid rate_source. Must be one of: WEBSITE, RM, EMAIL.", http.StatusBadRequest
	}
	if strings.Contains(errStr, "inv_rate_card_penalty_consistency_chk") {
		return "penalty_percentage can only be set when premature_withdrawal_allowed is true.", http.StatusBadRequest
	}
	if strings.Contains(errStr, "inv_rate_card_penalty_range_chk") {
		return "penalty_percentage must be between 0 and 100.", http.StatusBadRequest
	}
	if strings.Contains(errStr, "inv_rate_card_offer_details_chk") {
		return "offer_details can only be set when special_offer is true.", http.StatusBadRequest
	}
	if strings.Contains(errStr, constants.ErrDuplicateKeyC) {
		return "Duplicate entry detected. This record already exists.", http.StatusOK
	}
	if strings.Contains(errStr, "foreign key constraint") || strings.Contains(errStr, "violates foreign key") {
		return "Referenced record not found or invalid.", http.StatusOK
	}
	if strings.Contains(errStr, "connection") || strings.Contains(errStr, "timeout") {
		return "Database connection issue", http.StatusServiceUnavailable
	}

	return "Internal server error: " + context, http.StatusInternalServerError
}

// --- Request Types ---
// NOTE: effective_from and effective_to are stored as strings (YYYY-MM-DD).
// NEVER scan DATE columns into time.Time; always use TO_CHAR(...) in SQL and scan into string.
type BankRateCardInput struct {
	BankCode                   string   `json:"bank_code"`
	DepositType                string   `json:"deposit_type"` // REGULAR|BULK|SENIOR_CITIZEN|SPECIAL
	MinTenorDays               int      `json:"min_tenor_days"`
	MaxTenorDays               int      `json:"max_tenor_days"`
	InterestRate               float64  `json:"interest_rate"`
	MinAmount                  *float64 `json:"min_amount"`
	MaxAmount                  *float64 `json:"max_amount"`
	IsCallable                 *bool    `json:"is_callable"`
	PrematureWithdrawalAllowed bool     `json:"premature_withdrawal_allowed"`
	PenaltyPercentage          *float64 `json:"penalty_percentage"`
	EffectiveFrom              string   `json:"effective_from"` // YYYY-MM-DD string â†’ DATE
	EffectiveTo                *string  `json:"effective_to"`   // nullable YYYY-MM-DD string â†’ DATE
	RateSource                 *string  `json:"rate_source"`    // WEBSITE|RM|EMAIL|nil
	SpecialOffer               bool     `json:"special_offer"`
	OfferDetails               *string  `json:"offer_details"`
	IsActive                   *bool    `json:"is_active"`
}

type CreateBankRateCardSingleRequest struct {
	UserID string `json:"user_id"`
	BankRateCardInput
}

type CreateBankRateCardRequest struct {
	UserID string              `json:"user_id"`
	Rows   []BankRateCardInput `json:"rows"`
}

type UpdateBankRateCardRequest struct {
	UserID     string                 `json:"user_id"`
	RateCardID string                 `json:"rate_card_id"`
	Fields     map[string]interface{} `json:"fields"`
	Reason     string                 `json:"reason"`
}

var validDepositTypes = map[string]bool{
	"REGULAR":        true,
	"BULK":           true,
	"SENIOR_CITIZEN": true,
	"SPECIAL":        true,
}

var validRateSources = map[string]bool{
	"WEBSITE": true,
	"RM":      true,
	"EMAIL":   true,
}

func validateRateCardFields(input BankRateCardInput) error {
	if strings.TrimSpace(input.BankCode) == "" {
		return errors.New("bank_code is required")
	}
	dt := strings.ToUpper(strings.TrimSpace(input.DepositType))
	if !validDepositTypes[dt] {
		return errors.New("deposit_type must be one of: REGULAR, BULK, SENIOR_CITIZEN, SPECIAL")
	}
	if input.MinTenorDays < 0 {
		return errors.New("min_tenor_days must be >= 0")
	}
	if input.MaxTenorDays < input.MinTenorDays {
		return errors.New("max_tenor_days must be >= min_tenor_days")
	}
	if input.InterestRate <= 0 || input.InterestRate > 100 {
		return errors.New("interest_rate must be > 0 and <= 100")
	}
	if strings.TrimSpace(input.EffectiveFrom) == "" {
		return errors.New("effective_from is required (YYYY-MM-DD)")
	}
	if input.RateSource != nil {
		rs := strings.ToUpper(strings.TrimSpace(*input.RateSource))
		if rs != "" && !validRateSources[rs] {
			return errors.New("rate_source must be one of: WEBSITE, RM, EMAIL")
		}
	}
	return nil
}

// rateCardExists checks whether an equivalent active (not deleted) rate card
// already exists matching the unique index uniq_fd_rate_card_active semantics.
func rateCardExists(ctx context.Context, querier interface {
	QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row
}, input BankRateCardInput) (bool, error) {
	// Nullness flags for min/max amount
	minAmtNull := input.MinAmount == nil
	maxAmtNull := input.MaxAmount == nil

	// For comparison of effective_to use COALESCE(effective_to, '9999-12-31')
	q := `SELECT rate_card_id FROM investment.fd_bank_rate_card_master
		WHERE bank_code = $1
		  AND deposit_type = $2
		  AND min_tenor_days = $3
		  AND max_tenor_days = $4
		  AND ((min_amount IS NULL) = $5) AND min_amount IS NOT DISTINCT FROM $6
		  AND ((max_amount IS NULL) = $7) AND max_amount IS NOT DISTINCT FROM $8
		  AND effective_from = $9
		  AND COALESCE(effective_to, '9999-12-31'::date) = COALESCE($10::date, '9999-12-31'::date)
		  AND COALESCE(is_deleted,false) = false
		LIMIT 1`

	row := querier.QueryRow(ctx, q,
		input.BankCode,
		strings.ToUpper(strings.TrimSpace(input.DepositType)),
		input.MinTenorDays,
		input.MaxTenorDays,
		minAmtNull, input.MinAmount,
		maxAmtNull, input.MaxAmount,
		input.EffectiveFrom,
		input.EffectiveTo,
	)
	var id string
	if err := row.Scan(&id); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// rateCardFieldPairs maps JSON field names to scan position indices
var rateCardFieldPairs = map[string]int{
	"bank_code":                    0,
	"deposit_type":                 1,
	"min_tenor_days":               2,
	"max_tenor_days":               3,
	"interest_rate":                4,
	"min_amount":                   5,
	"max_amount":                   6,
	"is_callable":                  7,
	"premature_withdrawal_allowed": 8,
	"penalty_percentage":           9,
	"effective_from":               10,
	"effective_to":                 11,
	"rate_source":                  12,
	"special_offer":                13,
	"offer_details":                14,
	"is_active":                    15,
}

// UploadBankRateCardSimple handles CSV/XLSX upload for bank rate cards
func UploadBankRateCardSimple(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseMultipartForm(10 << 20); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Failed to parse form: "+err.Error())
			return
		}

		userID := r.FormValue("user_id")
		if userID == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrUserIIsRequired)
			return
		}

		file, handler, err := r.FormFile("file")
		if err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Failed to get file: "+err.Error())
			return
		}
		fileBytes, err := io.ReadAll(file)
		file.Close()
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrFailedToReadFile+err.Error())
			return
		}
		contentType := s3storage.DetectContentType(fileBytes)

		ext := strings.ToLower(filepath.Ext(handler.Filename))
		if ext != ".csv" && ext != ".xlsx" {
			api.RespondWithError(w, http.StatusBadRequest, "Only .csv and .xlsx files are supported")
			return
		}

		userEmail := api.GetUserEmailFromCtx(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		var data [][]string
		if ext == ".csv" {
			data, err = parseCSVFile(newBytesMultipartFile(fileBytes))
		} else {
			data, err = parseXLSXFile(newBytesMultipartFile(fileBytes))
		}
		if err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Failed to parse file: "+err.Error())
			return
		}

		if len(data) < 2 {
			api.RespondWithError(w, http.StatusBadRequest, "File must have a header row and at least one data row")
			return
		}

		header := data[0]
		rows := data[1:]

		// normalize header keys: lowercase, trim, remove non-alphanumeric chars
		normalize := func(s string) string {
			s = strings.ToLower(strings.TrimSpace(s))
			out := make([]rune, 0, len(s))
			for _, r := range s {
				if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
					out = append(out, r)
				}
			}
			return string(out)
		}

		colMap := make(map[string]int)
		for i, col := range header {
			colMap[normalize(col)] = i
		}

		requiredCols := []string{"bank_code", "deposit_type", "min_tenor_days", "max_tenor_days", "interest_rate", "effective_from"}
		for _, col := range requiredCols {
			if _, ok := colMap[col]; !ok {
				api.RespondWithError(w, http.StatusBadRequest, "Missing required column: "+col)
				return
			}
		}

		ctx := r.Context()

		// Fail-fast send helper
		sendFail := func(row int, msg string) {
			summary := fmt.Sprintf("RateCard upload aborted: row %d failed validation: %s", row, msg)
			api.RespondWithPayload(w, false, summary, nil)
		}

		var validInputs []BankRateCardInput
		seenKeysInFile := make(map[string]int)

		for ri, row := range rows {
			if len(row) == 0 {
				continue
			}
			get := func(col string) string { return getColumnValue(row, colMap, normalize(col)) }

			minTenorPtr, _ := parseIntPtr(get("min_tenor_days"))
			maxTenorPtr, _ := parseIntPtr(get("max_tenor_days"))
			interestRatePtr, _ := parseFloatPtr(get("interest_rate"))

			minTenor := 0
			if minTenorPtr != nil {
				minTenor = *minTenorPtr
			}
			maxTenor := 0
			if maxTenorPtr != nil {
				maxTenor = *maxTenorPtr
			}
			interestRate := 0.0
			if interestRatePtr != nil {
				interestRate = *interestRatePtr
			}

			efFromRaw := get("effective_from")
			efFrom, dateErr := parseMasterDate(efFromRaw)
			if dateErr != nil || efFrom == "" {
				sendFail(ri+2, "effective_from must be a valid date (YYYY-MM-DD)")
				return
			}

			var efToPtr *string
			if etRaw := get("effective_to"); etRaw != "" {
				efTo, dateErr2 := parseMasterDate(etRaw)
				if dateErr2 != nil {
					sendFail(ri+2, "effective_to must be a valid date (YYYY-MM-DD)")
					return
				}
				efToPtr = &efTo
				if efToPtr != nil && efTo != "" && efTo < efFrom {
					sendFail(ri+2, "effective_to must be >= effective_from")
					return
				}
			}

			input := BankRateCardInput{
				BankCode:      strings.TrimSpace(get("bank_code")),
				DepositType:   strings.ToUpper(strings.TrimSpace(get("deposit_type"))),
				MinTenorDays:  minTenor,
				MaxTenorDays:  maxTenor,
				InterestRate:  interestRate,
				EffectiveFrom: efFrom,
				EffectiveTo:   efToPtr,
			}

			input.MinAmount, _ = parseFloatPtr(get("min_amount"))
			input.MaxAmount, _ = parseFloatPtr(get("max_amount"))
			input.IsCallable, _ = parseBoolPtr(get("is_callable"))
			input.PenaltyPercentage, _ = parseFloatPtr(get("penalty_percentage"))

			pwaStr := get("premature_withdrawal_allowed")
			pwaPtr, _ := parseBoolPtr(pwaStr)
			if pwaPtr != nil {
				input.PrematureWithdrawalAllowed = *pwaPtr
			} else {
				input.PrematureWithdrawalAllowed = true
			}

			if rs := get("rate_source"); rs != "" {
				rsUpper := strings.ToUpper(rs)
				input.RateSource = &rsUpper
			}
			soStr := get("special_offer")
			soPtr, _ := parseBoolPtr(soStr)
			if soPtr != nil {
				input.SpecialOffer = *soPtr
			}
			if od := get("offer_details"); od != "" {
				input.OfferDetails = &od
			}
			input.IsActive, _ = parseBoolPtr(get("is_active"))
			if input.IsActive == nil {
				t := true
				input.IsActive = &t
			}

			// basic validations
			if err := validateRateCardFields(input); err != nil {
				sendFail(ri+2, err.Error())
				return
			}

			// cross-field checks: min/max amount
			if input.MinAmount != nil && input.MaxAmount != nil {
				if *input.MinAmount > *input.MaxAmount {
					sendFail(ri+2, "min_amount must be <= max_amount when both provided")
					return
				}
			}

			// Resolve bank code (bank_id or bank_name accepted)
			if input.BankCode == "" {
				sendFail(ri+2, "bank_code is required")
				return
			}
			if name, _ := bankNameShortFromCode(ctx, input.BankCode); name == "" {
				if code, ok := bankCodeFromName(ctx, input.BankCode); ok {
					input.BankCode = code
				} else {
					sendFail(ri+2, "bank identifier not recognized (not an approved bank id or name): "+input.BankCode)
					return
				}
			}

			// uniqueness pre-check

			// in-file duplicate check
			minAmtVal, maxAmtVal := "null", "null"
			if input.MinAmount != nil {
				minAmtVal = fmt.Sprintf("%v", *input.MinAmount)
			}
			if input.MaxAmount != nil {
				maxAmtVal = fmt.Sprintf("%v", *input.MaxAmount)
			}
			efToVal := "null"
			if input.EffectiveTo != nil {
				efToVal = *input.EffectiveTo
			}
			key := fmt.Sprintf("%s|%s|%d|%d|%s|%s|%s|%s", input.BankCode, input.DepositType, input.MinTenorDays, input.MaxTenorDays, minAmtVal, maxAmtVal, input.EffectiveFrom, efToVal)
			if prevRow, dup := seenKeysInFile[key]; dup {
				sendFail(ri+2, fmt.Sprintf("duplicate data in file: row %d conflicts with row %d (matching unique keys)", ri+2, prevRow))
				return
			}
			seenKeysInFile[key] = ri + 2

			exists, err := rateCardExists(ctx, pgxPool, input)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, constants.ErrFailedToValidateUniqueness+err.Error())
				return
			}
			if exists {
				sendFail(ri+2, "conflicts with existing active rate card")
				return
			}

			validInputs = append(validInputs, input)
		}

		if len(validInputs) == 0 {
			api.RespondWithPayload(w, false, constants.ErrAllRowsFailedValidation, nil)
			return
		}

		s3Key, storedFileName := "", ""
		if s3storage.IsS3UploadEnabled() {
			folder := s3storage.GetStoragePrefix("master-bank-rate-card")
			storedFileName = s3storage.BuildUploadedFilename(handler.Filename, userEmail, time.Now().UTC())
			s3Key = s3storage.BuildNamedS3Key(folder, "", storedFileName)
			if err = s3storage.PutObjectToS3(ctx, s3Key, fileBytes, contentType); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, constants.ErrFailedToStoreFile+err.Error())
				return
			}
		}

		// Transactional per-row insertion to avoid multi-row RETURNING issues
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			if s3Key != "" {
				_ = s3storage.DeleteFromS3(ctx, s3Key)
			}
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTransactionFailed+err.Error())
			return
		}
		committed := false
		defer func() {
			if !committed {
				tx.Rollback(ctx)
				if s3Key != "" {
					_ = s3storage.DeleteFromS3(ctx, s3Key)
				}
			}
		}()

		var insertedIDs []string
		var insertedRecords []map[string]interface{}
		for _, input := range validInputs {
			var id string
			err := tx.QueryRow(ctx, `
				INSERT INTO investment.fd_bank_rate_card_master (
					bank_code, deposit_type, min_tenor_days, max_tenor_days, interest_rate,
					min_amount, max_amount, is_callable, premature_withdrawal_allowed, penalty_percentage,
					effective_from, effective_to, rate_source, special_offer, offer_details, is_active
				) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::date,$12::date,$13,$14,$15,$16)
				RETURNING rate_card_id
			`,
				input.BankCode, input.DepositType, input.MinTenorDays, input.MaxTenorDays, input.InterestRate,
				input.MinAmount, input.MaxAmount, input.IsCallable, input.PrematureWithdrawalAllowed, input.PenaltyPercentage,
				input.EffectiveFrom, input.EffectiveTo, input.RateSource, input.SpecialOffer, input.OfferDetails, input.IsActive,
			).Scan(&id)
			if err != nil {
				msg, status := getUserFriendlyRateCardError(err, "Insert failed")
				api.RespondWithError(w, status, msg)
				return
			}
			insertedIDs = append(insertedIDs, id)
			insertedRecords = append(insertedRecords, map[string]interface{}{constants.ValueSuccess: true, "rate_card_id": id})
		}

		if len(insertedIDs) > 0 {
			auditValues := make([]string, len(insertedIDs))
			auditArgs := make([]interface{}, 0, len(insertedIDs)*2)
			for i, id := range insertedIDs {
				auditValues[i] = fmt.Sprintf("($%d,'CREATE','PENDING_APPROVAL',$%d,now())", i*2+1, i*2+2)
				auditArgs = append(auditArgs, id, userEmail)
			}
			auditQ := fmt.Sprintf(`
				INSERT INTO investment.fd_audit_bank_rate_card
					(rate_card_id, action_type, processing_status, requested_by, requested_at)
				VALUES %s
			`, strings.Join(auditValues, ","))
			if _, err := tx.Exec(ctx, auditQ, auditArgs...); err != nil {
				msg, status := getUserFriendlyRateCardError(err, "Audit insert failed")
				api.RespondWithError(w, status, msg)
				return
			}
		}

		if s3Key != "" && len(insertedIDs) > 0 {
			if _, err := tx.Exec(ctx, `UPDATE investment.fd_bank_rate_card_master SET upload_s3_key = $1 WHERE rate_card_id = ANY($2)`, s3Key, insertedIDs); err != nil {
				api.LogError("Failed to store upload_s3_key: %v", err)
			}
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyRateCardError(err, constants.ErrCommitFailedUser)
			api.RespondWithError(w, status, msg)
			return
		}
		committed = true
		bulkuploadaudit.Record(ctx, pgxPool, bulkuploadaudit.Entry{
			ModuleKey:        "master-bank-rate-card",
			OriginalFileName: handler.Filename,
			StoredFileName:   storedFileName,
			UploadS3Key:      s3Key,
			ContentType:      contentType,
			FileSize:         int64(len(fileBytes)),
			TotalRows:        len(data) - 1,
			InsertedCount:    len(insertedRecords),
			ErrorCount:       (len(data) - 1) - len(insertedRecords),
			Status:           bulkuploadaudit.StatusFor(len(insertedRecords), (len(data)-1)-len(insertedRecords)),
			UploadedBy:       userEmail,
			UploadedAt:       time.Now().UTC(),
		})

		api.RespondWithPayload(w, len(insertedRecords) > 0, "", insertedRecords)
		api.LogInfo("RateCard upload: %d inserted from %s", len(insertedRecords), handler.Filename)
	}
}

// CreateBankRateCardSingle creates a single bank rate card record
func CreateBankRateCardSingle(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req CreateBankRateCardSingleRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}

		req.DepositType = strings.ToUpper(strings.TrimSpace(req.DepositType))
		if req.IsActive == nil {
			t := true
			req.IsActive = &t
		}

		if err := validateRateCardFields(req.BankRateCardInput); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, err.Error())
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
			msg, status := getUserFriendlyRateCardError(err, constants.ErrTransactionFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx)

		// Check uniqueness before attempting insert to provide a friendly error
		if exists, err := rateCardExists(ctx, tx, req.BankRateCardInput); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrFailedToValidateUniqueness+err.Error())
			return
		} else if exists {
			api.RespondWithPayload(w, false, "Rate card create aborted: a matching active rate card already exists", nil)
			return
		}

		var rateCardID string
		err = tx.QueryRow(ctx, `
			INSERT INTO investment.fd_bank_rate_card_master (
				bank_code, deposit_type, min_tenor_days, max_tenor_days, interest_rate,
				min_amount, max_amount, is_callable, premature_withdrawal_allowed, penalty_percentage,
				effective_from, effective_to, rate_source, special_offer, offer_details, is_active
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::date,$12::date,$13,$14,$15,$16)
			RETURNING rate_card_id
		`,
			req.BankCode, req.DepositType, req.MinTenorDays, req.MaxTenorDays, req.InterestRate,
			req.MinAmount, req.MaxAmount, req.IsCallable, req.PrematureWithdrawalAllowed, req.PenaltyPercentage,
			req.EffectiveFrom, req.EffectiveTo, req.RateSource,
			req.SpecialOffer, req.OfferDetails, req.IsActive,
		).Scan(&rateCardID)
		if err != nil {
			msg, status := getUserFriendlyRateCardError(err, "Insert failed")
			api.RespondWithError(w, status, msg)
			return
		}

		if _, err := tx.Exec(ctx, `
			INSERT INTO investment.fd_audit_bank_rate_card
				(rate_card_id, action_type, processing_status, requested_by, requested_at)
			VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now())
		`, rateCardID, userEmail); err != nil {
			msg, status := getUserFriendlyRateCardError(err, constants.ErrAuditInsertFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyRateCardError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			constants.ValueSuccess: true,
			"rate_card_id":         rateCardID,
			"bank_code":            req.BankCode,
			"requested_by":         userEmail,
		})
		api.LogInfo("RateCard created: id=%s", rateCardID)
	}
}

// CreateBankRateCard handles bulk creation of bank rate cards
func CreateBankRateCard(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req CreateBankRateCardRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.Rows) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No rows provided")
			return
		}

		userEmail := api.GetUserEmailFromCtx(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()

		var validRows []BankRateCardInput
		var validRowIndices []int
		var errResults []map[string]interface{}

		for i, row := range req.Rows {
			row.DepositType = strings.ToUpper(strings.TrimSpace(row.DepositType))
			if row.IsActive == nil {
				t := true
				row.IsActive = &t
			}
			if err := validateRateCardFields(row); err != nil {
				errResults = append(errResults, map[string]interface{}{
					"row_index":            i,
					constants.ValueSuccess: false,
					constants.ValueError:   err.Error(),
				})
			} else {
				validRows = append(validRows, row)
				validRowIndices = append(validRowIndices, i)
			}
		}

		if len(validRows) == 0 {
			api.RespondWithPayload(w, false, constants.ErrAllRowsFailedValidation, errResults)
			return
		}

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTransactionFailed+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		// Pre-check uniqueness per valid row inside the transaction and perform per-row inserts.
		var toInsert []BankRateCardInput
		var toInsertIndices []int
		for idx, input := range validRows {
			origIdx := validRowIndices[idx]
			exists, err := rateCardExists(ctx, tx, input)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, constants.ErrFailedToValidateUniqueness+err.Error())
				return
			}
			if exists {
				errResults = append(errResults, map[string]interface{}{
					"row_index":            origIdx,
					constants.ValueSuccess: false,
					constants.ValueError:   "conflicts with existing active rate card",
				})
				continue
			}
			toInsert = append(toInsert, input)
			toInsertIndices = append(toInsertIndices, origIdx)
		}

		var insertedIDs []string
		var insertedRecords []map[string]interface{}
		for _, input := range toInsert {
			var id string
			err := tx.QueryRow(ctx, `
				INSERT INTO investment.fd_bank_rate_card_master (
					bank_code, deposit_type, min_tenor_days, max_tenor_days, interest_rate,
					min_amount, max_amount, is_callable, premature_withdrawal_allowed, penalty_percentage,
					effective_from, effective_to, rate_source, special_offer, offer_details, is_active
				) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::date,$12::date,$13,$14,$15,$16)
				RETURNING rate_card_id
			`,
				input.BankCode, input.DepositType, input.MinTenorDays, input.MaxTenorDays, input.InterestRate,
				input.MinAmount, input.MaxAmount, input.IsCallable, input.PrematureWithdrawalAllowed, input.PenaltyPercentage,
				input.EffectiveFrom, input.EffectiveTo, input.RateSource, input.SpecialOffer, input.OfferDetails, input.IsActive,
			).Scan(&id)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Insert failed: "+err.Error())
				return
			}
			insertedIDs = append(insertedIDs, id)
			insertedRecords = append(insertedRecords, map[string]interface{}{constants.ValueSuccess: true, "rate_card_id": id})
		}

		if len(insertedIDs) > 0 {
			auditValues := make([]string, len(insertedIDs))
			auditArgs := make([]interface{}, 0, len(insertedIDs)*2)
			for i, id := range insertedIDs {
				auditValues[i] = fmt.Sprintf("($%d,'CREATE','PENDING_APPROVAL',$%d,now())", i*2+1, i*2+2)
				auditArgs = append(auditArgs, id, userEmail)
			}
			auditQ := fmt.Sprintf(`
				INSERT INTO investment.fd_audit_bank_rate_card
					(rate_card_id, action_type, processing_status, requested_by, requested_at)
				VALUES %s
			`, strings.Join(auditValues, ","))
			if _, err := tx.Exec(ctx, auditQ, auditArgs...); err != nil {
				msg, status := getUserFriendlyRateCardError(err, constants.ErrBatchAuditFailed)
				api.RespondWithError(w, status, msg)
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyRateCardError(err, constants.ErrCommitFailedUser)
			api.RespondWithError(w, status, msg)
			return
		}

		allResults := append(insertedRecords, errResults...)
		api.RespondWithPayload(w, len(insertedRecords) > 0, "", allResults)
		api.LogInfo("RateCard bulk create: %d inserted, %d errors", len(insertedRecords), len(errResults))
	}
}

// UpdateBankRateCard handles single-record update
func UpdateBankRateCard(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req UpdateBankRateCardRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.RateCardID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "rate_card_id is required")
			return
		}
		if len(req.Fields) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No fields provided for update")
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
			msg, status := getUserFriendlyRateCardError(err, constants.ErrTxStartFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx)

		// Fetch existing for old values â€” use TO_CHAR for DATE columns
		var oldBankCode, oldDepositType, oldEffectiveFrom string
		var oldEffectiveTo, oldRateSource, oldOfferDetails *string
		var oldMinTenor, oldMaxTenor int
		var oldInterestRate float64
		var oldMinAmount, oldMaxAmount, oldPenaltyPct *float64
		var oldIsCallable *bool
		var oldPrematureWd, oldSpecialOffer, oldIsActive bool

		err = tx.QueryRow(ctx, `
			SELECT bank_code, deposit_type, min_tenor_days, max_tenor_days, interest_rate,
			       min_amount, max_amount, is_callable, premature_withdrawal_allowed, penalty_percentage,
			       TO_CHAR(effective_from,'YYYY-MM-DD') AS effective_from,
			       TO_CHAR(effective_to,'YYYY-MM-DD') AS effective_to,
			       rate_source, special_offer, offer_details, is_active
			FROM investment.fd_bank_rate_card_master
			WHERE rate_card_id=$1 FOR UPDATE
		`, req.RateCardID).Scan(
			&oldBankCode, &oldDepositType, &oldMinTenor, &oldMaxTenor, &oldInterestRate,
			&oldMinAmount, &oldMaxAmount, &oldIsCallable, &oldPrematureWd, &oldPenaltyPct,
			&oldEffectiveFrom, &oldEffectiveTo,
			&oldRateSource, &oldSpecialOffer, &oldOfferDetails, &oldIsActive,
		)
		if err != nil {
			msg, status := getUserFriendlyRateCardError(err, "Fetch failed")
			api.RespondWithError(w, status, msg)
			return
		}

		oldVals := []interface{}{
			oldBankCode, oldDepositType, oldMinTenor, oldMaxTenor, oldInterestRate,
			oldMinAmount, oldMaxAmount, oldIsCallable, oldPrematureWd, oldPenaltyPct,
			oldEffectiveFrom, oldEffectiveTo, oldRateSource, oldSpecialOffer, oldOfferDetails, oldIsActive,
		}

		var sets []string
		var args []interface{}
		pos := 1

		for k, v := range req.Fields {
			k = strings.ToLower(k)
			if _, ok := rateCardFieldPairs[k]; ok {
				// DATE fields need explicit cast
				if k == "effective_from" || k == "effective_to" {
					sets = append(sets, fmt.Sprintf("%s=$%d::date", k, pos))
				} else {
					sets = append(sets, fmt.Sprintf(constants.FormatSQLColumnArgAlt, k, pos))
				}
				args = append(args, v)
				pos++
			}
		}

		if len(sets) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No valid updatable fields found")
			return
		}

		q := fmt.Sprintf("UPDATE investment.fd_bank_rate_card_master SET %s WHERE rate_card_id=$%d",
			strings.Join(sets, ", "), pos)
		args = append(args, req.RateCardID)

		if _, err := tx.Exec(ctx, q, args...); err != nil {
			msg, status := getUserFriendlyRateCardError(err, constants.ErrUpdateFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		// Build audit with old values
		auditCols := []string{"rate_card_id", "action_type", "processing_status", "reason", "requested_by", "requested_at"}
		auditVals := []interface{}{req.RateCardID, "EDIT", constants.StatusPendingEditApproval, req.Reason, userEmail}
		auditParams := []string{"$1", "$2", "$3", "$4", "$5", "now()"}
		paramPos := 6

		for k := range req.Fields {
			k = strings.ToLower(k)
			if idx, ok := rateCardFieldPairs[k]; ok {
				auditCols = append(auditCols, "old_"+k)
				auditVals = append(auditVals, oldVals[idx])
				auditParams = append(auditParams, fmt.Sprintf("$%d", paramPos))
				paramPos++
			}
		}

		auditQuery := fmt.Sprintf("INSERT INTO investment.fd_audit_bank_rate_card (%s) VALUES (%s)",
			strings.Join(auditCols, ", "), strings.Join(auditParams, ", "))
		if _, err := tx.Exec(ctx, auditQuery, auditVals...); err != nil {
			msg, status := getUserFriendlyRateCardError(err, constants.ErrAuditInsertFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyRateCardError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			constants.ValueSuccess: true,
			"rate_card_id":         req.RateCardID,
			"requested_by":         userEmail,
		})
		api.LogInfo("RateCard updated: id=%s", req.RateCardID)
	}
}

// UpdateBankRateCardBulk handles bulk update of bank rate cards
func UpdateBankRateCardBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			Rows   []struct {
				RateCardID string                 `json:"rate_card_id"`
				Fields     map[string]interface{} `json:"fields"`
				Reason     string                 `json:"reason"`
			} `json:"rows"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.Rows) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No rows provided")
			return
		}

		userEmail := api.GetUserEmailFromCtx(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()

		type validUpdate struct {
			RateCardID string
			Fields     map[string]interface{}
			Reason     string
		}

		var validUpdates []validUpdate
		var errResults []map[string]interface{}
		allIDs := make([]string, 0, len(req.Rows))

		for i, row := range req.Rows {
			if row.RateCardID == "" {
				errResults = append(errResults, map[string]interface{}{
					"row_index":            i,
					constants.ValueSuccess: false,
					constants.ValueError:   "Missing rate_card_id",
				})
				continue
			}
			if len(row.Fields) == 0 {
				errResults = append(errResults, map[string]interface{}{
					"row_index":            i,
					"rate_card_id":         row.RateCardID,
					constants.ValueSuccess: false,
					constants.ValueError:   "No fields to update",
				})
				continue
			}
			validUpdates = append(validUpdates, validUpdate{row.RateCardID, row.Fields, row.Reason})
			allIDs = append(allIDs, row.RateCardID)
		}

		if len(validUpdates) == 0 {
			api.RespondWithPayload(w, false, constants.ErrAllRowsFailedValidation, errResults)
			return
		}

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTransactionFailed+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		// Fetch old values â€” TO_CHAR for DATE fields
		oldRows, err := tx.Query(ctx, `
			SELECT rate_card_id, bank_code, deposit_type, min_tenor_days, max_tenor_days, interest_rate,
			       min_amount, max_amount, is_callable, premature_withdrawal_allowed, penalty_percentage,
			       TO_CHAR(effective_from,'YYYY-MM-DD'), TO_CHAR(effective_to,'YYYY-MM-DD'),
			       rate_source, special_offer, offer_details, is_active
			FROM investment.fd_bank_rate_card_master
			WHERE rate_card_id = ANY($1::text[])
		`, allIDs)
		if err != nil {
			msg, status := getUserFriendlyRateCardError(err, "Fetch old values failed")
			api.RespondWithError(w, status, msg)
			return
		}
		defer oldRows.Close()

		type oldRec struct {
			vals [16]interface{}
		}
		oldMap := make(map[string]oldRec)

		for oldRows.Next() {
			var id string
			var rec oldRec
			// vars for scanning
			var v0, v1 string
			var v2, v3 int
			var v4 float64
			var v5, v6, v9 *float64
			var v7 *bool
			var v8, v13, v15 bool
			var v10 string
			var v11, v12, v14 *string

			if err := oldRows.Scan(
				&id, &v0, &v1, &v2, &v3, &v4,
				&v5, &v6, &v7, &v8, &v9,
				&v10, &v11, &v12, &v13, &v14, &v15,
			); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Old values scan failed: "+err.Error())
				return
			}
			rec.vals = [16]interface{}{v0, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15}
			oldMap[id] = rec
		}
		oldRows.Close()

		var successResults []map[string]interface{}

		for _, update := range validUpdates {
			old, exists := oldMap[update.RateCardID]
			if !exists {
				errResults = append(errResults, map[string]interface{}{
					"rate_card_id":         update.RateCardID,
					constants.ValueSuccess: false,
					constants.ValueError:   "Record not found",
				})
				continue
			}

			var sets []string
			var args []interface{}
			pos := 1
			for k, v := range update.Fields {
				k = strings.ToLower(k)
				if _, ok := rateCardFieldPairs[k]; ok {
					if k == "effective_from" || k == "effective_to" {
						sets = append(sets, fmt.Sprintf("%s=$%d::date", k, pos))
					} else {
						sets = append(sets, fmt.Sprintf(constants.FormatSQLColumnArgAlt, k, pos))
					}
					args = append(args, v)
					pos++
				}
			}

			if len(sets) > 0 {
				q := fmt.Sprintf("UPDATE investment.fd_bank_rate_card_master SET %s WHERE rate_card_id=$%d",
					strings.Join(sets, ", "), pos)
				args = append(args, update.RateCardID)
				if _, err := tx.Exec(ctx, q, args...); err != nil {
					errResults = append(errResults, map[string]interface{}{
						"rate_card_id":         update.RateCardID,
						constants.ValueSuccess: false,
						constants.ValueError:   constants.ErrUpdateFailed + err.Error(),
					})
					continue
				}
			}

			// Build audit
			auditCols := []string{"rate_card_id", "action_type", "processing_status", "reason", "requested_by", "requested_at"}
			auditVals := []interface{}{update.RateCardID, "EDIT", constants.StatusPendingEditApproval, update.Reason, userEmail}
			auditParams := []string{"$1", "$2", "$3", "$4", "$5", "now()"}
			paramPos := 6
			for k := range update.Fields {
				k = strings.ToLower(k)
				if idx, ok := rateCardFieldPairs[k]; ok {
					auditCols = append(auditCols, "old_"+k)
					auditVals = append(auditVals, old.vals[idx])
					auditParams = append(auditParams, fmt.Sprintf("$%d", paramPos))
					paramPos++
				}
			}
			auditQuery := fmt.Sprintf("INSERT INTO investment.fd_audit_bank_rate_card (%s) VALUES (%s)",
				strings.Join(auditCols, ", "), strings.Join(auditParams, ", "))
			if _, err := tx.Exec(ctx, auditQuery, auditVals...); err != nil {
				errResults = append(errResults, map[string]interface{}{
					"rate_card_id":         update.RateCardID,
					constants.ValueSuccess: false,
					constants.ValueError:   "Audit failed: " + err.Error(),
				})
				continue
			}

			successResults = append(successResults, map[string]interface{}{
				constants.ValueSuccess: true,
				"rate_card_id":         update.RateCardID,
			})
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyRateCardError(err, constants.ErrCommitFailedUser)
			api.RespondWithError(w, status, msg)
			return
		}

		allResults := append(successResults, errResults...)
		api.RespondWithPayload(w, len(successResults) > 0, "", allResults)
		api.LogInfo("RateCard bulk update: %d updated, %d errors", len(successResults), len(errResults))
	}
}

// DeleteBankRateCard creates delete requests (pending approval)
func DeleteBankRateCard(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID      string   `json:"user_id"`
			RateCardIDs []string `json:"rate_card_ids"`
			Reason      string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.RateCardIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrNoRateCardIDsProvided)
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
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTransactionFailed+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		verifyRows, err := tx.Query(ctx, `
			SELECT rate_card_id FROM investment.fd_bank_rate_card_master
			WHERE rate_card_id = ANY($1::text[]) AND COALESCE(is_deleted,false) = false
		`, req.RateCardIDs)
		if err != nil {
			msg, status := getUserFriendlyRateCardError(err, "Verification failed")
			api.RespondWithError(w, status, msg)
			return
		}
		defer verifyRows.Close()

		var validIDs []string
		for verifyRows.Next() {
			var id string
			if err := verifyRows.Scan(&id); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Scan failed: "+err.Error())
				return
			}
			validIDs = append(validIDs, id)
		}
		verifyRows.Close()

		if len(validIDs) > 0 {
			auditValues := make([]string, len(validIDs))
			auditArgs := make([]interface{}, 0, len(validIDs)*3)
			for i, id := range validIDs {
				auditValues[i] = fmt.Sprintf("($%d,'DELETE','PENDING_DELETE_APPROVAL',$%d,$%d,now())", i*3+1, i*3+2, i*3+3)
				auditArgs = append(auditArgs, id, userEmail, req.Reason)
			}
			auditQ := fmt.Sprintf(`
				INSERT INTO investment.fd_audit_bank_rate_card
					(rate_card_id, action_type, processing_status, requested_by, reason, requested_at)
				VALUES %s
			`, strings.Join(auditValues, ","))
			if _, err := tx.Exec(ctx, auditQ, auditArgs...); err != nil {
				msg, status := getUserFriendlyRateCardError(err, constants.ErrBatchAuditFailed)
				api.RespondWithError(w, status, msg)
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyRateCardError(err, constants.ErrCommitFailedUser)
			api.RespondWithError(w, status, msg)
			return
		}

		validSet := make(map[string]bool)
		for _, id := range validIDs {
			validSet[id] = true
		}
		var results []map[string]interface{}
		for _, id := range req.RateCardIDs {
			if validSet[id] {
				results = append(results, map[string]interface{}{constants.ValueSuccess: true, "rate_card_id": id})
			} else {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, "rate_card_id": id, constants.ValueError: "Not found or already deleted"})
			}
		}
		api.RespondWithPayload(w, len(validIDs) > 0, "", results)
		api.LogInfo("RateCard delete requested: %d valid, %d total", len(validIDs), len(req.RateCardIDs))
	}
}

// BulkApproveBankRateCard approves pending bank rate card records
func BulkApproveBankRateCard(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID      string   `json:"user_id"`
			RateCardIDs []string `json:"rate_card_ids"`
			Comment     string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		// [AUTO] Partial Success Filter for DELETE approvals only
		pendingDel := dependency.GetPendingDeleteIDs(r.Context(), pgxPool, "investment.fd_audit_bank_rate_card", "rate_card_id", req.RateCardIDs)
		blocked := dependency.CheckBulkBlockers(r.Context(), pgxPool, "fd_bank_rate_card_master", pendingDel)
		if len(blocked) > 0 {
			blockedSet := make(map[string]bool)
			for _, b := range blocked {
				blockedSet[b["id"].(string)] = true
			}
			var unblocked []string
			for _, id := range req.RateCardIDs {
				if !blockedSet[id] {
					unblocked = append(unblocked, id)
				}
			}
			req.RateCardIDs = unblocked
		}
		w = dependency.NewBulkResponseInterceptor(w, blocked)
		if len(req.RateCardIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrNoRateCardIDsProvided)
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
			msg, status := getUserFriendlyRateCardError(err, constants.ErrTransactionFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx)

		_, err = tx.Exec(ctx, `
			UPDATE investment.fd_audit_bank_rate_card
			SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2
			WHERE rate_card_id = ANY($3::text[]) AND processing_status LIKE '%PENDING%'
		`, userEmail, req.Comment, req.RateCardIDs)
		if err != nil {
			msg, status := getUserFriendlyRateCardError(err, "Approval failed")
			api.RespondWithError(w, status, msg)
			return
		}

		// Set is_deleted=true for DELETE approvals
		_, err = tx.Exec(ctx, `
			UPDATE investment.fd_bank_rate_card_master
			SET is_deleted=true
			WHERE rate_card_id IN (
				SELECT DISTINCT a.rate_card_id
				FROM investment.fd_audit_bank_rate_card a
				WHERE a.rate_card_id = ANY($1::text[])
				  AND a.action_type='DELETE'
				  AND a.processing_status='APPROVED'
				  AND a.action_type IN ('CREATE','EDIT','DELETE')
			)
		`, req.RateCardIDs)
		if err != nil {
			msg, status := getUserFriendlyRateCardError(err, "Delete execution failed")
			api.RespondWithError(w, status, msg)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyRateCardError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			constants.ValueSuccess: true,
			"approved_count":       len(req.RateCardIDs),
			"checker":              userEmail,
		})
		api.LogInfo("RateCard bulk approve: %d approved by %s", len(req.RateCardIDs), userEmail)
	}
}

// BulkRejectBankRateCard rejects pending bank rate card records
func BulkRejectBankRateCard(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID      string   `json:"user_id"`
			RateCardIDs []string `json:"rate_card_ids"`
			Comment     string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.RateCardIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrNoRateCardIDsProvided)
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
			msg, status := getUserFriendlyRateCardError(err, constants.ErrTransactionFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx)

		_, err = tx.Exec(ctx, `
			UPDATE investment.fd_audit_bank_rate_card
			SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2
			WHERE rate_card_id = ANY($3::text[]) AND processing_status LIKE '%PENDING%'
		`, userEmail, req.Comment, req.RateCardIDs)
		if err != nil {
			msg, status := getUserFriendlyRateCardError(err, "Rejection failed")
			api.RespondWithError(w, status, msg)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyRateCardError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			constants.ValueSuccess: true,
			"rejected_count":       len(req.RateCardIDs),
			"checker":              userEmail,
		})
		api.LogInfo("RateCard bulk reject: %d rejected by %s", len(req.RateCardIDs), userEmail)
	}
}

// GetBankRateCardsApprovedActive returns active approved bank rate cards
func GetBankRateCardsApprovedActive(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		// Use TO_CHAR for DATE columns to avoid time.Time scan issues
		rows, err := pgxPool.Query(ctx, `
			SELECT DISTINCT ON (m.rate_card_id)
				m.rate_card_id, m.bank_code,
				COALESCE(mb.bank_name,'')       AS bank_name,
				COALESCE(mb.bank_short_name,'') AS bank_short_name,
				m.deposit_type,
				m.min_tenor_days, m.max_tenor_days, m.interest_rate,
				m.min_amount, m.max_amount, m.is_callable,
				m.premature_withdrawal_allowed, m.penalty_percentage,
				TO_CHAR(m.effective_from,'YYYY-MM-DD') AS effective_from,
				TO_CHAR(m.effective_to,'YYYY-MM-DD') AS effective_to,
				COALESCE(m.rate_source,'') AS rate_source,
				m.special_offer, COALESCE(m.offer_details,'') AS offer_details,
				m.is_active
			FROM investment.fd_bank_rate_card_master m
			INNER JOIN investment.fd_audit_bank_rate_card a ON a.rate_card_id = m.rate_card_id
			LEFT JOIN masterbank mb ON mb.bank_id::text = m.bank_code
			WHERE a.processing_status='APPROVED' AND a.action_type IN ('CREATE','EDIT','DELETE') AND m.is_active=true AND COALESCE(m.is_deleted,false)=false
			ORDER BY m.rate_card_id, m.bank_code
		`)
		if err != nil {
			msg, status := getUserFriendlyRateCardError(err, constants.ErrQueryFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer rows.Close()

		out := make([]map[string]interface{}, 0)
		for rows.Next() {
			var id, bankCode, bankName, bankShortName, depositType string
			var minTenor, maxTenor int
			var interestRate float64
			var minAmt, maxAmt, penaltyPct *float64
			var isCallable *bool
			var prematureWd, specialOffer, isActive bool
			var effectiveFrom, rateSource, offerDetails string
			var effectiveTo *string

			if err := rows.Scan(
				&id, &bankCode, &bankName, &bankShortName, &depositType,
				&minTenor, &maxTenor, &interestRate,
				&minAmt, &maxAmt, &isCallable,
				&prematureWd, &penaltyPct,
				&effectiveFrom, &effectiveTo,
				&rateSource, &specialOffer, &offerDetails, &isActive,
			); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Scan error: "+err.Error())
				return
			}

			out = append(out, map[string]interface{}{
				"rate_card_id":                 id,
				"bank_code":                    bankCode,
				"bank_name":                    bankName,
				"bank_short_name":              bankShortName,
				"deposit_type":                 depositType,
				"min_tenor_days":               minTenor,
				"max_tenor_days":               maxTenor,
				"interest_rate":                interestRate,
				"min_amount":                   minAmt,
				"max_amount":                   maxAmt,
				"is_callable":                  isCallable,
				"premature_withdrawal_allowed": prematureWd,
				"penalty_percentage":           penaltyPct,
				"effective_from":               effectiveFrom,
				"effective_to":                 effectiveTo,
				"rate_source":                  rateSource,
				"special_offer":                specialOffer,
				"offer_details":                offerDetails,
				"is_active":                    isActive,
			})
		}
		api.RespondWithPayload(w, true, "", out)
		api.LogInfo("RateCard approved-active: returned %d records", len(out))
	}
}

// GetBankRateCardsWithAudit returns all non-deleted bank rate cards with latest audit info
func GetBankRateCardsWithAudit(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		// DATE columns converted to string via TO_CHAR throughout
		q := `
			WITH latest_audit AS (
				SELECT DISTINCT ON (a.rate_card_id)
					a.rate_card_id,
					a.processing_status,
					a.action_type,
					a.audit_id,
					a.requested_by,
					a.requested_at,
					a.checker_by,
					a.checker_at,
					a.checker_comment,
					a.reason,
					a.old_bank_code,
					a.old_deposit_type,
					a.old_min_tenor_days,
					a.old_max_tenor_days,
					a.old_interest_rate,
					a.old_min_amount,
					a.old_max_amount,
					a.old_is_callable,
					a.old_premature_withdrawal_allowed,
					a.old_penalty_percentage,
					TO_CHAR(a.old_effective_from,'YYYY-MM-DD') AS old_effective_from,
					TO_CHAR(a.old_effective_to,'YYYY-MM-DD') AS old_effective_to,
					a.old_rate_source,
					a.old_special_offer,
					a.old_offer_details,
					a.old_is_active
				FROM investment.fd_audit_bank_rate_card a
				WHERE a.action_type IN ('CREATE','EDIT','DELETE')
				ORDER BY a.rate_card_id,
				         GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp), COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC
			),
			history AS (
				SELECT
					rate_card_id,
					MAX(CASE WHEN action_type='CREATE' THEN requested_by END) AS created_by,
					MAX(CASE WHEN action_type='CREATE' THEN TO_CHAR(requested_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') END) AS created_at,
					MAX(CASE WHEN action_type='EDIT'   THEN requested_by END) AS edited_by,
					MAX(CASE WHEN action_type='EDIT'   THEN TO_CHAR(requested_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') END) AS edited_at,
					MAX(CASE WHEN action_type='DELETE' THEN requested_by END) AS deleted_by,
					MAX(CASE WHEN action_type='DELETE' THEN TO_CHAR(requested_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') END) AS deleted_at
				FROM investment.fd_audit_bank_rate_card
				GROUP BY rate_card_id
			)
			SELECT
				m.rate_card_id,
				COALESCE(m.bank_code,'')                      AS bank_code,
				COALESCE(l.old_bank_code,'')                  AS old_bank_code,
				COALESCE(mb.bank_name,'')                     AS bank_name,
				COALESCE(mb.bank_short_name,'')               AS bank_short_name,
				COALESCE(m.deposit_type,'')                   AS deposit_type,
				COALESCE(l.old_deposit_type,'')               AS old_deposit_type,
				COALESCE(m.min_tenor_days,0)                  AS min_tenor_days,
				COALESCE(l.old_min_tenor_days,0)              AS old_min_tenor_days,
				COALESCE(m.max_tenor_days,0)                  AS max_tenor_days,
				COALESCE(l.old_max_tenor_days,0)              AS old_max_tenor_days,
				COALESCE(m.interest_rate,0)                   AS interest_rate,
				COALESCE(l.old_interest_rate,0)               AS old_interest_rate,
				m.min_amount,
				l.old_min_amount,
				m.max_amount,
				l.old_max_amount,
				m.is_callable,
				l.old_is_callable,
				COALESCE(m.premature_withdrawal_allowed,true)  AS premature_withdrawal_allowed,
				COALESCE(l.old_premature_withdrawal_allowed,true) AS old_premature_withdrawal_allowed,
				m.penalty_percentage,
				l.old_penalty_percentage,
				COALESCE(m.upload_s3_key,'') AS upload_s3_key,
				TO_CHAR(m.effective_from,'YYYY-MM-DD')        AS effective_from,
				COALESCE(l.old_effective_from,'')             AS old_effective_from,
				TO_CHAR(m.effective_to,'YYYY-MM-DD')          AS effective_to,
				COALESCE(l.old_effective_to,'')               AS old_effective_to,
				COALESCE(m.rate_source,'')                    AS rate_source,
				COALESCE(l.old_rate_source,'')                AS old_rate_source,
				COALESCE(m.special_offer,false)               AS special_offer,
				COALESCE(l.old_special_offer,false)           AS old_special_offer,
				COALESCE(m.offer_details,'')                  AS offer_details,
				COALESCE(l.old_offer_details,'')              AS old_offer_details,
				COALESCE(m.is_active,false)                   AS is_active,
				COALESCE(l.old_is_active,false)               AS old_is_active,
				COALESCE(m.is_deleted,false)                  AS is_deleted,

				COALESCE(l.processing_status,'')              AS processing_status,
				COALESCE(l.action_type,'')                    AS action_type,
				COALESCE(l.audit_id::text,'')                 AS audit_id,
				COALESCE(l.requested_by,'')                   AS requested_by,
				TO_CHAR(l.requested_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS requested_at,
				COALESCE(l.checker_by,'')                     AS checker_by,
				TO_CHAR(l.checker_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"')   AS checker_at,
				COALESCE(l.checker_comment,'')                AS checker_comment,
				COALESCE(l.reason,'')                         AS reason,

				COALESCE(h.created_by,'')                     AS created_by,
				COALESCE(h.created_at,'')                     AS created_at,
				COALESCE(h.edited_by,'')                      AS edited_by,
				COALESCE(h.edited_at,'')                      AS edited_at,
				COALESCE(h.deleted_by,'')                     AS deleted_by,
				COALESCE(h.deleted_at,'')                     AS deleted_at

			FROM investment.fd_bank_rate_card_master m
			LEFT JOIN latest_audit l ON l.rate_card_id = m.rate_card_id
			LEFT JOIN history h      ON h.rate_card_id = m.rate_card_id
			LEFT JOIN masterbank mb  ON mb.bank_id::text = m.bank_code
			WHERE COALESCE(m.is_deleted,false) = false
			ORDER BY GREATEST(COALESCE(l.requested_at,'1970-01-01'::timestamp), COALESCE(l.checker_at,'1970-01-01'::timestamp)) DESC
		`

		rows, err := pgxPool.Query(ctx, q)
		if err != nil {
			msg, status := getUserFriendlyRateCardError(err, constants.ErrQueryFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer rows.Close()

		fields := rows.FieldDescriptions()
		out := make([]map[string]interface{}, 0, 1000)

		for rows.Next() {
			vals, err := rows.Values()
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Row value error: "+err.Error())
				return
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

		if rows.Err() != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Row scan error: "+rows.Err().Error())
			return
		}

		api.RespondEnvelopeSuccessCompat(w, "Success", map[string]interface{}{"rows": out})
		api.LogInfo("RateCard WithAudit: returned %d records", len(out))
	}
}

// GetBankRateCard returns a single bank rate card by ID
func GetBankRateCard(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID     string `json:"user_id"`
			RateCardID string `json:"rate_card_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.RateCardID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "rate_card_id is required")
			return
		}

		userEmail := api.GetUserEmailFromCtx(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		// Use TO_CHAR for DATE columns
		row := pgxPool.QueryRow(ctx, `
			SELECT m.rate_card_id, m.bank_code,
			       COALESCE(mb.bank_name,'') AS bank_name,
			       COALESCE(mb.bank_short_name,'') AS bank_short_name,
			       m.deposit_type, m.min_tenor_days, m.max_tenor_days, m.interest_rate,
			       m.min_amount, m.max_amount, m.is_callable, m.premature_withdrawal_allowed, m.penalty_percentage,
			       TO_CHAR(m.effective_from,'YYYY-MM-DD') AS effective_from,
			       TO_CHAR(m.effective_to,'YYYY-MM-DD') AS effective_to,
			       COALESCE(m.rate_source,'') AS rate_source,
			       m.special_offer, COALESCE(m.offer_details,'') AS offer_details,
			       m.is_active, COALESCE(m.is_deleted,false) AS is_deleted
			FROM investment.fd_bank_rate_card_master m
			LEFT JOIN masterbank mb ON mb.bank_id::text = m.bank_code
			WHERE m.rate_card_id = $1
		`, req.RateCardID)

		var id, bankCode, bankName, bankShortName, depositType, effectiveFrom, rateSource, offerDetails string
		var minTenor, maxTenor int
		var interestRate float64
		var minAmt, maxAmt, penaltyPct *float64
		var isCallable *bool
		var prematureWd, specialOffer, isActive, isDeleted bool
		var effectiveTo *string

		err := row.Scan(
			&id, &bankCode, &bankName, &bankShortName, &depositType, &minTenor, &maxTenor, &interestRate,
			&minAmt, &maxAmt, &isCallable, &prematureWd, &penaltyPct,
			&effectiveFrom, &effectiveTo, &rateSource, &specialOffer, &offerDetails,
			&isActive, &isDeleted,
		)
		if err != nil {
			if err.Error() == "no rows in result set" {
				api.RespondWithError(w, http.StatusNotFound, "Bank rate card not found")
			} else {
				msg, status := getUserFriendlyRateCardError(err, "Get failed")
				api.RespondWithError(w, status, msg)
			}
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"rate_card_id":                 id,
			"bank_code":                    bankCode,
			"bank_name":                    bankName,
			"bank_short_name":              bankShortName,
			"deposit_type":                 depositType,
			"min_tenor_days":               minTenor,
			"max_tenor_days":               maxTenor,
			"interest_rate":                interestRate,
			"min_amount":                   minAmt,
			"max_amount":                   maxAmt,
			"is_callable":                  isCallable,
			"premature_withdrawal_allowed": prematureWd,
			"penalty_percentage":           penaltyPct,
			"effective_from":               effectiveFrom,
			"effective_to":                 effectiveTo,
			"rate_source":                  rateSource,
			"special_offer":                specialOffer,
			"offer_details":                offerDetails,
			"is_active":                    isActive,
			"is_deleted":                   isDeleted,
		})
		api.LogInfo("GetBankRateCard: id=%s by %s", id, userEmail)
	}
}
