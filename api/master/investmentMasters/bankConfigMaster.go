package allMaster

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/api/master/bulkuploadaudit"
	"CimplrCorpSaas/api/utils/s3storage"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// parseMasterDate normalises any common date string to "YYYY-MM-DD".
// It accepts Indian/ISO/Excel date formats and returns "" when the input is blank.
// If no layout matches it returns an error so callers can reject the row.
func parseMasterDate(s string) (string, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", nil
	}
	// Fast-path: already YYYY-MM-DD
	if len(s) == 10 && s[4] == '-' && s[7] == '-' {
		if _, err := time.Parse(constants.DateFormat, s); err == nil {
			return s, nil
		}
	}
	// Prefer dd/mm/yyyy for Indian bank/master data before falling back
	for _, layout := range []string{"02/01/2006", "2/1/2006"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.Format(constants.DateFormat), nil
		}
	}
	layouts := []string{
		// dd/mm/yyyy variants (Indian/European) â€” MUST be before mm/dd
		"02/01/2006", "02/01/06", "2/1/2006", "2/1/06",
		// mm/dd/yyyy variants (American)
		"01/02/2006", "01/02/06", "1/2/2006", "1/2/06",
		// Named-month formats
		constants.DateFormatSlash, // 29/Aug/2025
		constants.DateFormatDash,  // 29-Aug-2025
		"2-Jan-2006", "1/Feb/2006",
		// ISO and common variants
		constants.DateFormat, // 2006-01-02
		"2006/01/02", "2006.01.02",
		"01.02.2006", "1.2.2006",
		"01-02-2006", "1-2-2006",
		"01-02-06", "1-2-06",
		"2006/1/2", "2006-1-2",
		// dd-Mon-yy / dd/Mon/yy
		"02-Jan-06", "02-Jan-2006", "02/Jan/06", "02/Jan/2006",
		"01-Feb-06", "01-Feb-2006", "01/Feb/06", "01/Feb/2006",
		// ISO with time (strip time component)
		"2006-01-02T15:04:05", "2006-01-02T15:04",
		constants.DateTimeFormat, time.RFC3339,
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t.Format(constants.DateFormat), nil
		}
	}
	// 2-digit year fallback e.g. "13-Dec-25"
	if len(s) == 9 && s[2] == '-' && s[6] == '-' {
		if t, err := time.Parse("02-Jan-06", s); err == nil {
			if t.Year() < 100 {
				t = t.AddDate(2000, 0, 0)
			}
			return t.Format(constants.DateFormat), nil
		}
	}
	return "", fmt.Errorf("cannot parse date %q â€” use YYYY-MM-DD or DD/MM/YYYY", s)
}

func logBankConfigDBError(err error, context string) {
	if err == nil {
		return
	}
	api.LogError("%s: %v", context, err)
	api.LogError("Error string: %s", err.Error())
	api.LogError("Verbose error: %#v", err)
	for u := errors.Unwrap(err); u != nil; u = errors.Unwrap(u) {
		api.LogError("Unwrapped: %T -> %v", u, u)
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		api.LogError("Postgres error detail: Code=%s Message=%s Detail=%s Where=%s Constraint=%s Table=%s Column=%s",
			pgErr.Code, pgErr.Message, pgErr.Detail, pgErr.Where, pgErr.ConstraintName, pgErr.TableName, pgErr.ColumnName)
	}
}

// getUserFriendlyBankConfigError converts database errors to user-friendly messages
func getUserFriendlyBankConfigError(err error, context string) (string, int) {
	if err == nil {
		return "", http.StatusOK
	}
	errStr := strings.ToLower(err.Error())

	if strings.Contains(errStr, constants.ErrTxBeginFailed) ||
		strings.Contains(errStr, constants.ErrAuditInsertFailed) {
		return err.Error(), http.StatusOK
	}
	if strings.Contains(errStr, "violates foreign key") || strings.Contains(errStr, "foreign key constraint") {
		return "Referenced record not found or invalid (e.g. bank_code, day_count_code, or holiday_calendar_code does not exist).", http.StatusBadRequest
	}
	if strings.Contains(errStr, constants.ErrDuplicateKeyC) {
		return "Duplicate entry detected. This record already exists.", http.StatusConflict
	}
	if strings.Contains(errStr, "connection") || strings.Contains(errStr, "timeout") {
		return "Database connection issue", http.StatusServiceUnavailable
	}
	return "Internal server error: " + context, http.StatusInternalServerError
}

// --- Request / input types ---

// BankConfigInput holds all fields for creating/uploading a bank config record.
// effective_from / effective_to are DATE columns â€” always scan via TO_CHAR, pass as ::date.
type BankConfigInput struct {
	BankCode                     string   `json:"bank_code"`
	ProductType                  *string  `json:"product_type"`
	MinimumAmount                *float64 `json:"minimum_amount"`
	MaximumAmount                *float64 `json:"maximum_amount"`
	DayCountCode                 string   `json:"day_count_code"`
	CapitalizationScheduleType   string   `json:"capitalization_schedule_type"`
	CapitalizationDateAdjustment string   `json:"capitalization_date_adjustment"`
	AccrualStartConvention       string   `json:"accrual_start_convention"`
	AccrualEndConvention         string   `json:"accrual_end_convention"`
	PeriodBoundaryDefinition     string   `json:"period_boundary_definition"`
	WeekendAccrual               bool     `json:"weekend_accrual"`
	HolidayAccrual               bool     `json:"holiday_accrual"`
	HolidayCalendarCode          string   `json:"holiday_calendar_code"`
	BrokenPeriodMethod           string   `json:"broken_period_method"`
	BrokenPeriodLocation         string   `json:"broken_period_location"`
	InterestRoundingDecimals     int      `json:"interest_rounding_decimals"`
	RoundingMethod               string   `json:"rounding_method"`
	RoundingFrequency            string   `json:"rounding_frequency"`
	GracePeriodDays              *int     `json:"grace_period_days"`
	GracePeriodRateType          *string  `json:"grace_period_rate_type"`
	MinimumCompoundingPeriodDays *int     `json:"minimum_compounding_period_days"`
	QuarterDefinition            *string  `json:"quarter_definition"`
	TdsDeductionTiming           string   `json:"tds_deduction_timing"`
	EffectiveFrom                string   `json:"effective_from"` // YYYY-MM-DD â†’ DATE
	EffectiveTo                  *string  `json:"effective_to"`   // nullable YYYY-MM-DD â†’ DATE
	ConfigNotes                  *string  `json:"config_notes"`
	IsActive                     *bool    `json:"is_active"`
}

type CreateBankConfigSingleRequest struct {
	UserID string `json:"user_id"`
	BankConfigInput
}

type CreateBankConfigRequest struct {
	UserID string            `json:"user_id"`
	Rows   []BankConfigInput `json:"rows"`
}

type UpdateBankConfigRequest struct {
	UserID   string                 `json:"user_id"`
	ConfigID string                 `json:"config_id"`
	Fields   map[string]interface{} `json:"fields"`
	Reason   string                 `json:"reason"`
}

// bankConfigFieldPairs maps JSON field names â†’ scan position indices (0-based, mirrors SELECT order after config_id)
var bankConfigFieldPairs = map[string]int{
	"bank_code":                       0,
	"product_type":                    1,
	"minimum_amount":                  2,
	"maximum_amount":                  3,
	"day_count_code":                  4,
	"capitalization_schedule_type":    5,
	"capitalization_date_adjustment":  6,
	"accrual_start_convention":        7,
	"accrual_end_convention":          8,
	"period_boundary_definition":      9,
	"weekend_accrual":                 10,
	"holiday_accrual":                 11,
	"holiday_calendar_code":           12,
	"broken_period_method":            13,
	"broken_period_location":          14,
	"interest_rounding_decimals":      15,
	"rounding_method":                 16,
	"rounding_frequency":              17,
	"grace_period_days":               18,
	"grace_period_rate_type":          19,
	"minimum_compounding_period_days": 20,
	"quarter_definition":              21,
	"tds_deduction_timing":            22,
	"effective_from":                  23,
	"effective_to":                    24,
	"config_notes":                    25,
	"is_active":                       26,
}

func validateBankConfigFields(input BankConfigInput) error {
	if strings.TrimSpace(input.BankCode) == "" {
		return fmt.Errorf("bank_code is required")
	}
	if strings.TrimSpace(input.DayCountCode) == "" {
		return fmt.Errorf(constants.ErrDayCountCodeRequired)
	}
	if strings.TrimSpace(input.CapitalizationScheduleType) == "" {
		return fmt.Errorf("capitalization_schedule_type is required")
	}
	if strings.TrimSpace(input.CapitalizationDateAdjustment) == "" {
		return fmt.Errorf("capitalization_date_adjustment is required")
	}
	if strings.TrimSpace(input.AccrualStartConvention) == "" {
		return fmt.Errorf("accrual_start_convention is required")
	}
	if strings.TrimSpace(input.AccrualEndConvention) == "" {
		return fmt.Errorf("accrual_end_convention is required")
	}
	if strings.TrimSpace(input.PeriodBoundaryDefinition) == "" {
		return fmt.Errorf("period_boundary_definition is required")
	}
	if strings.TrimSpace(input.BrokenPeriodMethod) == "" {
		return fmt.Errorf("broken_period_method is required")
	}
	if strings.TrimSpace(input.BrokenPeriodLocation) == "" {
		return fmt.Errorf("broken_period_location is required")
	}
	if strings.TrimSpace(input.RoundingMethod) == "" {
		return fmt.Errorf("rounding_method is required")
	}
	if strings.TrimSpace(input.RoundingFrequency) == "" {
		return fmt.Errorf("rounding_frequency is required")
	}
	if strings.TrimSpace(input.TdsDeductionTiming) == "" {
		return fmt.Errorf("tds_deduction_timing is required")
	}
	if strings.TrimSpace(input.EffectiveFrom) == "" {
		return fmt.Errorf("effective_from is required (YYYY-MM-DD)")
	}
	return nil
}

// insertBankConfigArgs builds the ordered argument slice for an INSERT
// Order must match the INSERT column list used throughout.
func insertBankConfigArgs(input BankConfigInput) []interface{} {
	isActive := true
	if input.IsActive != nil {
		isActive = *input.IsActive
	}
	return []interface{}{
		input.BankCode,
		input.ProductType,
		input.MinimumAmount,
		input.MaximumAmount,
		input.DayCountCode,
		input.CapitalizationScheduleType,
		input.CapitalizationDateAdjustment,
		input.AccrualStartConvention,
		input.AccrualEndConvention,
		input.PeriodBoundaryDefinition,
		input.WeekendAccrual,
		input.HolidayAccrual,
		input.HolidayCalendarCode,
		input.BrokenPeriodMethod,
		input.BrokenPeriodLocation,
		input.InterestRoundingDecimals,
		input.RoundingMethod,
		input.RoundingFrequency,
		input.GracePeriodDays,
		input.GracePeriodRateType,
		input.MinimumCompoundingPeriodDays,
		input.QuarterDefinition,
		input.TdsDeductionTiming,
		input.EffectiveFrom, // ::date
		input.EffectiveTo,   // ::date nullable
		input.ConfigNotes,
		isActive,
	}
}

// bankConfigExists checks whether an equivalent active (not deleted) bank config
// already exists matching the unique index uniq_bank_config_active semantics.
// Returns (exists, existingConfigID, error)
func bankConfigExists(ctx context.Context, querier interface {
	QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row
}, input BankConfigInput) (bool, string, error) {
	prodNull := input.ProductType == nil
	minAmtNull := input.MinimumAmount == nil
	maxAmtNull := input.MaximumAmount == nil

	q := `SELECT config_id FROM investment.fd_bank_config_master
		WHERE bank_code = $1
		  AND ((product_type IS NULL) = $2) AND product_type IS NOT DISTINCT FROM $3
		  AND ((minimum_amount IS NULL) = $4) AND minimum_amount IS NOT DISTINCT FROM $5
		  AND ((maximum_amount IS NULL) = $6) AND maximum_amount IS NOT DISTINCT FROM $7
		  AND day_count_code = $8
		  AND capitalization_schedule_type = $9
		  AND capitalization_date_adjustment = $10
		  AND accrual_start_convention = $11
		  AND accrual_end_convention = $12
		  AND period_boundary_definition = $13
		  AND holiday_calendar_code = $14
		  AND broken_period_method = $15
		  AND broken_period_location = $16
		  AND rounding_method = $17
		  AND rounding_frequency = $18
		  AND tds_deduction_timing = $19
		  AND effective_from = $20
		  AND COALESCE(effective_to, '9999-12-31'::date) = COALESCE($21::date, '9999-12-31'::date)
		  AND COALESCE(is_deleted,false) = false
		LIMIT 1`

	row := querier.QueryRow(ctx, q,
		input.BankCode,
		prodNull, input.ProductType,
		minAmtNull, input.MinimumAmount,
		maxAmtNull, input.MaximumAmount,
		input.DayCountCode,
		input.CapitalizationScheduleType,
		input.CapitalizationDateAdjustment,
		input.AccrualStartConvention,
		input.AccrualEndConvention,
		input.PeriodBoundaryDefinition,
		input.HolidayCalendarCode,
		input.BrokenPeriodMethod,
		input.BrokenPeriodLocation,
		input.RoundingMethod,
		input.RoundingFrequency,
		input.TdsDeductionTiming,
		input.EffectiveFrom,
		input.EffectiveTo,
	)
	var id string
	if err := row.Scan(&id); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, "", nil
		}
		return false, "", err
	}

	return true, id, nil
}

const bankConfigInsertCols = `bank_code, product_type, minimum_amount, maximum_amount,
	day_count_code, capitalization_schedule_type, capitalization_date_adjustment,
	accrual_start_convention, accrual_end_convention, period_boundary_definition,
	weekend_accrual, holiday_accrual, holiday_calendar_code,
	broken_period_method, broken_period_location,
	interest_rounding_decimals, rounding_method, rounding_frequency,
	grace_period_days, grace_period_rate_type, minimum_compounding_period_days, quarter_definition,
	tds_deduction_timing, effective_from, effective_to, config_notes, is_active`

// paramPlaceholders27 is the parameterized placeholder for 27 fields per row.
// effective_from=$24::date, effective_to=$25::date
func bankConfigRowPlaceholder(base int) string {
	return fmt.Sprintf(
		"($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d::date,$%d::date,$%d,$%d)",
		base+1, base+2, base+3, base+4, base+5, base+6, base+7,
		base+8, base+9, base+10, base+11, base+12, base+13,
		base+14, base+15, base+16, base+17, base+18,
		base+19, base+20, base+21, base+22, base+23,
		base+24, base+25, base+26, base+27,
	)
}

// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
// UploadBankConfigSimple  â€” CSV / XLSX upload
// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

func UploadBankConfigSimple(pgxPool *pgxpool.Pool) http.HandlerFunc {
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
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to read file: "+err.Error())
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

		requiredCols := []string{"bank_code", "day_count_code", "holiday_calendar_code",
			"capitalization_schedule_type", "capitalization_date_adjustment",
			"accrual_start_convention", "accrual_end_convention", "period_boundary_definition",
			"broken_period_method", "broken_period_location",
			"rounding_method", "rounding_frequency", "tds_deduction_timing", "effective_from"}
		for _, col := range requiredCols {
			if _, ok := colMap[col]; !ok {
				api.RespondWithError(w, http.StatusBadRequest, "Missing required column: "+col)
				return
			}
		}

		ctx := r.Context()
		var validInputs []BankConfigInput
		// Fail-fast helper
		sendFail := func(row int, msg string) {
			summary := fmt.Sprintf("BankConfig upload aborted: row %d failed validation: %s", row, msg)
			api.RespondWithPayload(w, false, summary, nil)
		}

		for i, row := range data[1:] {
			if len(row) == 0 {
				continue
			}

			get := func(col string) string { return getColumnValue(row, colMap, normalize(col)) }

			input := BankConfigInput{
				BankCode:                     strings.TrimSpace(get("bank_code")),
				DayCountCode:                 strings.TrimSpace(get("day_count_code")),
				HolidayCalendarCode:          strings.TrimSpace(get("holiday_calendar_code")),
				CapitalizationScheduleType:   strings.TrimSpace(get("capitalization_schedule_type")),
				CapitalizationDateAdjustment: strings.TrimSpace(get("capitalization_date_adjustment")),
				AccrualStartConvention:       strings.TrimSpace(get("accrual_start_convention")),
				AccrualEndConvention:         strings.TrimSpace(get("accrual_end_convention")),
				PeriodBoundaryDefinition:     strings.TrimSpace(get("period_boundary_definition")),
				BrokenPeriodMethod:           strings.TrimSpace(get("broken_period_method")),
				BrokenPeriodLocation:         strings.TrimSpace(get("broken_period_location")),
				RoundingMethod:               strings.TrimSpace(get("rounding_method")),
				RoundingFrequency:            strings.TrimSpace(get("rounding_frequency")),
				TdsDeductionTiming:           strings.TrimSpace(get("tds_deduction_timing")),
			}

			// â”€â”€ Date sanitisation â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
			// Parse effective_from â€” accepts any common format, normalises to YYYY-MM-DD
			efFrom, dateErr := parseMasterDate(get("effective_from"))
			if dateErr != nil {
				sendFail(i+2, "effective_from: "+dateErr.Error())
				return
			}
			input.EffectiveFrom = efFrom

			// Parse effective_to (optional)
			if etRaw := getColumnValue(row, colMap, "effective_to"); etRaw != "" {
				efTo, dateErr2 := parseMasterDate(etRaw)
				if dateErr2 != nil {
					sendFail(i+2, "effective_to: "+dateErr2.Error())
					return
				}
				input.EffectiveTo = &efTo
				if *input.EffectiveTo < input.EffectiveFrom {
					sendFail(i+2, "effective_to must be >= effective_from")
					return
				}
			}

			if pt := getColumnValue(row, colMap, "product_type"); pt != "" {
				input.ProductType = &pt
			}
			input.MinimumAmount, _ = parseFloatPtr(get("minimum_amount"))
			input.MaximumAmount, _ = parseFloatPtr(get("maximum_amount"))
			if input.MinimumAmount != nil && input.MaximumAmount != nil {
				if *input.MinimumAmount > *input.MaximumAmount {
					sendFail(i+2, "minimum_amount must be <= maximum_amount when both provided")
					return
				}
			}

			waPtr, _ := parseBoolPtr(getColumnValue(row, colMap, "weekend_accrual"))
			if waPtr != nil {
				input.WeekendAccrual = *waPtr
			}
			haPtr, _ := parseBoolPtr(getColumnValue(row, colMap, "holiday_accrual"))
			if haPtr != nil {
				input.HolidayAccrual = *haPtr
			}

			rdPtr, _ := parseIntPtr(get("interest_rounding_decimals"))
			if rdPtr != nil {
				input.InterestRoundingDecimals = *rdPtr
			} else {
				input.InterestRoundingDecimals = 2
			}

			// interest rounding decimals sanity check (0..6)
			if input.InterestRoundingDecimals < 0 || input.InterestRoundingDecimals > 6 {
				sendFail(i+2, "interest_rounding_decimals must be between 0 and 6")
				return
			}

			input.GracePeriodDays, _ = parseIntPtr(getColumnValue(row, colMap, "grace_period_days"))
			if gpt := getColumnValue(row, colMap, "grace_period_rate_type"); gpt != "" {
				input.GracePeriodRateType = &gpt
			}
			input.MinimumCompoundingPeriodDays, _ = parseIntPtr(getColumnValue(row, colMap, "minimum_compounding_period_days"))
			if qd := getColumnValue(row, colMap, "quarter_definition"); qd != "" {
				input.QuarterDefinition = &qd
			}
			if cn := getColumnValue(row, colMap, "config_notes"); cn != "" {
				input.ConfigNotes = &cn
			}
			input.IsActive, _ = parseBoolPtr(getColumnValue(row, colMap, "is_active"))
			if input.IsActive == nil {
				t := true
				input.IsActive = &t
			}

			if err := validateBankConfigFields(input); err != nil {
				sendFail(i+2, err.Error())
				return
			}
			// Resolve bank code from name if needed
			if input.BankCode == "" {
				sendFail(i+2, "bank_code is required")
				return
			}
			if name, _ := bankNameShortFromCode(ctx, input.BankCode); name == "" {
				if code, ok := bankCodeFromName(ctx, input.BankCode); ok {
					input.BankCode = code
				} else {
					sendFail(i+2, "bank identifier not recognized (not an approved bank id or name): "+input.BankCode)
					return
				}
			}

			// uniqueness pre-check (fail-fast)
			if exists, existingID, err := bankConfigExists(ctx, pgxPool, input); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, constants.ErrFailedToValidateUniqueness+err.Error())
				return
			} else if exists {
				cols := "bank_code, product_type, minimum_amount, maximum_amount, day_count_code, capitalization_schedule_type, capitalization_date_adjustment, accrual_start_convention, accrual_end_convention, period_boundary_definition, holiday_calendar_code, broken_period_method, broken_period_location, rounding_method, rounding_frequency, tds_deduction_timing, effective_from, effective_to"
				sendFail(i+2, fmt.Sprintf("conflicts with existing active bank config %s: matching unique key columns: %s", existingID, cols))
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
			folder := s3storage.GetStoragePrefix("master-bank-config")
			storedFileName = s3storage.BuildUploadedFilename(handler.Filename, userEmail, time.Now().UTC())
			s3Key = s3storage.BuildNamedS3Key(folder, "", storedFileName)
			if err = s3storage.PutObjectToS3(ctx, s3Key, fileBytes, contentType); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Failed to store file: "+err.Error())
				return
			}
		}

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

		const fieldsPerRow = 27
		valueStrings := make([]string, len(validInputs))
		valueArgs := make([]interface{}, 0, len(validInputs)*fieldsPerRow)
		for i, input := range validInputs {
			valueStrings[i] = bankConfigRowPlaceholder(i * fieldsPerRow)
			valueArgs = append(valueArgs, insertBankConfigArgs(input)...)
		}

		batchInsertQuery := fmt.Sprintf(
			constants.QuerryInsertBankConfig,
			bankConfigInsertCols, strings.Join(valueStrings, ","),
		)

		insertRows, err := tx.Query(ctx, batchInsertQuery, valueArgs...)
		if err != nil {
			msg, status := getUserFriendlyBankConfigError(err, "Batch insert failed")
			api.RespondWithError(w, status, msg)
			return
		}
		defer insertRows.Close()

		var insertedIDs []string
		var insertedRecords []map[string]interface{}
		for insertRows.Next() {
			var id string
			if err := insertRows.Scan(&id); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Insert scan failed: "+err.Error())
				return
			}
			insertedIDs = append(insertedIDs, id)
			insertedRecords = append(insertedRecords, map[string]interface{}{
				constants.ValueSuccess: true,
				"config_id":            id,
			})
		}
		insertRows.Close()

		if len(insertedIDs) > 0 {
			auditValues := make([]string, len(insertedIDs))
			auditArgs := make([]interface{}, 0, len(insertedIDs)*2)
			for i, id := range insertedIDs {
				auditValues[i] = fmt.Sprintf("($%d,'CREATE','PENDING_APPROVAL',$%d,now())", i*2+1, i*2+2)
				auditArgs = append(auditArgs, id, userEmail)
			}
			auditQ := fmt.Sprintf(`
				INSERT INTO investment.fd_audit_bank_config
					(config_id, action_type, processing_status, requested_by, requested_at)
				VALUES %s`, strings.Join(auditValues, ","))
			if _, err := tx.Exec(ctx, auditQ, auditArgs...); err != nil {
				msg, status := getUserFriendlyBankConfigError(err, "Audit insert failed")
				api.RespondWithError(w, status, msg)
				return
			}
		}

		if s3Key != "" && len(insertedIDs) > 0 {
			if _, err := tx.Exec(ctx, `UPDATE investment.fd_bank_config_master SET upload_s3_key = $1 WHERE config_id = ANY($2)`, s3Key, insertedIDs); err != nil {
				api.LogError("Failed to store upload_s3_key: %v", err)
			}
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyBankConfigError(err, constants.ErrCommitFailedUser)
			api.RespondWithError(w, status, msg)
			return
		}
		committed = true
		bulkuploadaudit.Record(ctx, pgxPool, bulkuploadaudit.Entry{
			ModuleKey:        "master-bank-config",
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
		api.LogInfo("BankConfig upload: %d inserted from %s", len(insertedRecords), handler.Filename)
	}
}

// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
// CreateBankConfigSingle â€” single record
// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

func CreateBankConfigSingle(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req CreateBankConfigSingleRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.IsActive == nil {
			t := true
			req.IsActive = &t
		}
		if err := validateBankConfigFields(req.BankConfigInput); err != nil {
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
			msg, status := getUserFriendlyBankConfigError(err, constants.ErrTransactionFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx)

		// uniqueness pre-check to provide friendly error
		if exists, existingID, err := bankConfigExists(ctx, tx, req.BankConfigInput); err != nil {
			msg, status := getUserFriendlyBankConfigError(err, "failed to validate uniqueness")
			api.RespondWithError(w, status, msg)
			return
		} else if exists {
			cols := "bank_code, product_type, minimum_amount, maximum_amount, day_count_code, capitalization_schedule_type, capitalization_date_adjustment, accrual_start_convention, accrual_end_convention, period_boundary_definition, holiday_calendar_code, broken_period_method, broken_period_location, rounding_method, rounding_frequency, tds_deduction_timing, effective_from, effective_to"
			api.RespondWithPayload(w, false, fmt.Sprintf("Create aborted: a matching active bank config already exists (config_id=%s) matching columns: %s", existingID, cols), nil)
			return
		}

		var configID string
		args := insertBankConfigArgs(req.BankConfigInput)
		err = tx.QueryRow(ctx, fmt.Sprintf(
			constants.QuerryInsertBankConfig,
			bankConfigInsertCols, bankConfigRowPlaceholder(0),
		), args...).Scan(&configID)
		if err != nil {
			logBankConfigDBError(err, "CreateBankConfigSingle insert failed")
			msg, status := getUserFriendlyBankConfigError(err, "Insert failed")
			api.RespondWithError(w, status, msg)
			return
		}

		if _, err := tx.Exec(ctx, `
			INSERT INTO investment.fd_audit_bank_config
				(config_id, action_type, processing_status, requested_by, requested_at)
			VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now())
		`, configID, userEmail); err != nil {
			msg, status := getUserFriendlyBankConfigError(err, constants.ErrAuditInsertFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			logBankConfigDBError(err, "CreateBankConfigSingle insert failed")
			msg, status := getUserFriendlyBankConfigError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			constants.ValueSuccess: true,
			"config_id":            configID,
			"bank_code":            req.BankCode,
			"requested_by":         userEmail,
		})
		api.LogInfo("BankConfig created: id=%s", configID)
	}
}

// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
// CreateBankConfig â€” bulk creation
// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

func CreateBankConfig(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req CreateBankConfigRequest
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
		var validRows []BankConfigInput
		var errResults []map[string]interface{}

		for i, row := range req.Rows {
			if row.IsActive == nil {
				t := true
				row.IsActive = &t
			}
			if err := validateBankConfigFields(row); err != nil {
				errResults = append(errResults, map[string]interface{}{
					"row_index":            i,
					constants.ValueSuccess: false,
					constants.ValueError:   err.Error(),
				})
			} else {
				validRows = append(validRows, row)
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

		const fieldsPerRow = 27
		valueStrings := make([]string, len(validRows))
		valueArgs := make([]interface{}, 0, len(validRows)*fieldsPerRow)
		for i, input := range validRows {
			valueStrings[i] = bankConfigRowPlaceholder(i * fieldsPerRow)
			valueArgs = append(valueArgs, insertBankConfigArgs(input)...)
		}

		batchInsertQuery := fmt.Sprintf(
			constants.QuerryInsertBankConfig,
			bankConfigInsertCols, strings.Join(valueStrings, ","),
		)

		insertRows, err := tx.Query(ctx, batchInsertQuery, valueArgs...)
		if err != nil {
			msg, status := getUserFriendlyBankConfigError(err, "Batch insert failed")
			api.RespondWithError(w, status, msg)
			return
		}
		defer insertRows.Close()

		var insertedIDs []string
		var insertedRecords []map[string]interface{}
		for insertRows.Next() {
			var id string
			if err := insertRows.Scan(&id); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Scan failed: "+err.Error())
				return
			}
			insertedIDs = append(insertedIDs, id)
			insertedRecords = append(insertedRecords, map[string]interface{}{
				constants.ValueSuccess: true,
				"config_id":            id,
			})
		}
		insertRows.Close()

		if len(insertedIDs) > 0 {
			auditValues := make([]string, len(insertedIDs))
			auditArgs := make([]interface{}, 0, len(insertedIDs)*2)
			for i, id := range insertedIDs {
				auditValues[i] = fmt.Sprintf("($%d,'CREATE','PENDING_APPROVAL',$%d,now())", i*2+1, i*2+2)
				auditArgs = append(auditArgs, id, userEmail)
			}
			auditQ := fmt.Sprintf(`
				INSERT INTO investment.fd_audit_bank_config
					(config_id, action_type, processing_status, requested_by, requested_at)
				VALUES %s`, strings.Join(auditValues, ","))
			if _, err := tx.Exec(ctx, auditQ, auditArgs...); err != nil {
				msg, status := getUserFriendlyBankConfigError(err, constants.ErrBatchAuditFailed)
				api.RespondWithError(w, status, msg)
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyBankConfigError(err, constants.ErrCommitFailedUser)
			api.RespondWithError(w, status, msg)
			return
		}

		allResults := append(insertedRecords, errResults...)
		api.RespondWithPayload(w, len(insertedRecords) > 0, "", allResults)
		api.LogInfo("BankConfig bulk create: %d inserted, %d errors", len(insertedRecords), len(errResults))
	}
}

// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
// UpdateBankConfig â€” single record update
// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

func UpdateBankConfig(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req UpdateBankConfigRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.ConfigID == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrConfigIDRequired)
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
			msg, status := getUserFriendlyBankConfigError(err, "Transaction start failed")
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx)

		// Fetch existing record for old-value audit; TO_CHAR for DATE columns
		var oldBankCode, oldDayCountCode, oldCapSchedType, oldCapDateAdj string
		var oldAccrualStart, oldAccrualEnd, oldPeriodBoundary string
		var oldHolidayCalCode, oldBrokenMethod, oldBrokenLoc, oldRoundingMethod, oldRoundingFreq string
		var oldTdsDeductionTiming, oldEffectiveFrom string
		var oldProductType, oldGracePeriodRateType, oldQuarterDef, oldConfigNotes, oldEffectiveTo *string
		var oldMinAmt, oldMaxAmt *float64
		var oldWeekendAccrual, oldHolidayAccrual, oldIsActive bool
		var oldRoundingDecimals int
		var oldGracePeriodDays, oldMinCompoundingPeriodDays *int

		err = tx.QueryRow(ctx, `
			SELECT
				bank_code, product_type, minimum_amount, maximum_amount, day_count_code,
				capitalization_schedule_type, capitalization_date_adjustment,
				accrual_start_convention, accrual_end_convention, period_boundary_definition,
				weekend_accrual, holiday_accrual, holiday_calendar_code,
				broken_period_method, broken_period_location,
				interest_rounding_decimals, rounding_method, rounding_frequency,
				grace_period_days, grace_period_rate_type, minimum_compounding_period_days, quarter_definition,
				tds_deduction_timing,
				TO_CHAR(effective_from,'YYYY-MM-DD') AS effective_from,
				TO_CHAR(effective_to,'YYYY-MM-DD') AS effective_to,
				config_notes, is_active
			FROM investment.fd_bank_config_master
			WHERE config_id=$1 FOR UPDATE
		`, req.ConfigID).Scan(
			&oldBankCode, &oldProductType, &oldMinAmt, &oldMaxAmt, &oldDayCountCode,
			&oldCapSchedType, &oldCapDateAdj,
			&oldAccrualStart, &oldAccrualEnd, &oldPeriodBoundary,
			&oldWeekendAccrual, &oldHolidayAccrual, &oldHolidayCalCode,
			&oldBrokenMethod, &oldBrokenLoc,
			&oldRoundingDecimals, &oldRoundingMethod, &oldRoundingFreq,
			&oldGracePeriodDays, &oldGracePeriodRateType, &oldMinCompoundingPeriodDays, &oldQuarterDef,
			&oldTdsDeductionTiming,
			&oldEffectiveFrom, &oldEffectiveTo,
			&oldConfigNotes, &oldIsActive,
		)
		if err != nil {
			msg, status := getUserFriendlyBankConfigError(err, "Fetch failed")
			api.RespondWithError(w, status, msg)
			return
		}

		oldVals := []interface{}{
			oldBankCode, oldProductType, oldMinAmt, oldMaxAmt, oldDayCountCode,
			oldCapSchedType, oldCapDateAdj,
			oldAccrualStart, oldAccrualEnd, oldPeriodBoundary,
			oldWeekendAccrual, oldHolidayAccrual, oldHolidayCalCode,
			oldBrokenMethod, oldBrokenLoc,
			oldRoundingDecimals, oldRoundingMethod, oldRoundingFreq,
			oldGracePeriodDays, oldGracePeriodRateType, oldMinCompoundingPeriodDays, oldQuarterDef,
			oldTdsDeductionTiming,
			oldEffectiveFrom, oldEffectiveTo,
			oldConfigNotes, oldIsActive,
		}

		var sets []string
		var args []interface{}
		pos := 1
		for k, v := range req.Fields {
			k = strings.ToLower(k)
			if _, ok := bankConfigFieldPairs[k]; ok {
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

		q := fmt.Sprintf("UPDATE investment.fd_bank_config_master SET %s WHERE config_id=$%d",
			strings.Join(sets, ", "), pos)
		args = append(args, req.ConfigID)
		if _, err := tx.Exec(ctx, q, args...); err != nil {
			msg, status := getUserFriendlyBankConfigError(err, constants.ErrUpdateFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		// Build audit INSERT with old values
		auditCols := []string{"config_id", "action_type", "processing_status", "reason", "requested_by", "requested_at"}
		auditVals := []interface{}{req.ConfigID, "EDIT", "PENDING_EDIT_APPROVAL", req.Reason, userEmail}
		auditParams := []string{"$1", "$2", "$3", "$4", "$5", "now()"}
		paramPos := 6
		for k := range req.Fields {
			k = strings.ToLower(k)
			if idx, ok := bankConfigFieldPairs[k]; ok {
				auditCols = append(auditCols, "old_"+k)
				auditVals = append(auditVals, oldVals[idx])
				auditParams = append(auditParams, fmt.Sprintf("$%d", paramPos))
				paramPos++
			}
		}

		auditQuery := fmt.Sprintf("INSERT INTO investment.fd_audit_bank_config (%s) VALUES (%s)",
			strings.Join(auditCols, ", "), strings.Join(auditParams, ", "))
		if _, err := tx.Exec(ctx, auditQuery, auditVals...); err != nil {
			msg, status := getUserFriendlyBankConfigError(err, constants.ErrAuditInsertFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyBankConfigError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			constants.ValueSuccess: true,
			"config_id":            req.ConfigID,
			"requested_by":         userEmail,
		})
		api.LogInfo("BankConfig updated: id=%s", req.ConfigID)
	}
}

// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
// UpdateBankConfigBulk â€” bulk update
// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

func UpdateBankConfigBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			Rows   []struct {
				ConfigID string                 `json:"config_id"`
				Fields   map[string]interface{} `json:"fields"`
				Reason   string                 `json:"reason"`
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
			ConfigID string
			Fields   map[string]interface{}
			Reason   string
		}

		var validUpdates []validUpdate
		var errResults []map[string]interface{}
		allIDs := make([]string, 0, len(req.Rows))

		for i, row := range req.Rows {
			if row.ConfigID == "" {
				errResults = append(errResults, map[string]interface{}{
					"row_index":            i,
					constants.ValueSuccess: false,
					constants.ValueError:   "Missing config_id",
				})
				continue
			}
			if len(row.Fields) == 0 {
				errResults = append(errResults, map[string]interface{}{
					"row_index":            i,
					"config_id":            row.ConfigID,
					constants.ValueSuccess: false,
					constants.ValueError:   "No fields to update",
				})
				continue
			}
			validUpdates = append(validUpdates, validUpdate{row.ConfigID, row.Fields, row.Reason})
			allIDs = append(allIDs, row.ConfigID)
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
			SELECT
				config_id, bank_code, product_type, minimum_amount, maximum_amount, day_count_code,
				capitalization_schedule_type, capitalization_date_adjustment,
				accrual_start_convention, accrual_end_convention, period_boundary_definition,
				weekend_accrual, holiday_accrual, holiday_calendar_code,
				broken_period_method, broken_period_location,
				interest_rounding_decimals, rounding_method, rounding_frequency,
				grace_period_days, grace_period_rate_type, minimum_compounding_period_days, quarter_definition,
				tds_deduction_timing,
				TO_CHAR(effective_from,'YYYY-MM-DD'), TO_CHAR(effective_to,'YYYY-MM-DD'),
				config_notes, is_active
			FROM investment.fd_bank_config_master
			WHERE config_id = ANY($1::text[])
		`, allIDs)
		if err != nil {
			msg, status := getUserFriendlyBankConfigError(err, "Fetch old values failed")
			api.RespondWithError(w, status, msg)
			return
		}
		defer oldRows.Close()

		type oldRec struct {
			vals [27]interface{}
		}
		oldMap := make(map[string]oldRec)

		for oldRows.Next() {
			var id string
			var rec oldRec
			// 27 scan targets (indices 0-26)
			var v0 string         // bank_code
			var v1 *string        // product_type
			var v2, v3 *float64   // minimum_amount, maximum_amount
			var v4 string         // day_count_code
			var v5, v6 string     // cap_sched, cap_date_adj
			var v7, v8, v9 string // accrual_start, end, period_boundary
			var v10, v11 bool     // weekend_accrual, holiday_accrual
			var v12 string        // holiday_calendar_code
			var v13, v14 string   // broken_period_method, location
			var v15 int           // interest_rounding_decimals
			var v16, v17 string   // rounding_method, frequency
			var v18 *int          // grace_period_days
			var v19 *string       // grace_period_rate_type
			var v20 *int          // minimum_compounding_period_days
			var v21 *string       // quarter_definition
			var v22 string        // tds_deduction_timing
			var v23 string        // effective_from (TO_CHAR)
			var v24 *string       // effective_to (TO_CHAR)
			var v25 *string       // config_notes
			var v26 bool          // is_active

			if err := oldRows.Scan(
				&id, &v0, &v1, &v2, &v3, &v4, &v5, &v6, &v7, &v8, &v9,
				&v10, &v11, &v12, &v13, &v14, &v15, &v16, &v17,
				&v18, &v19, &v20, &v21, &v22, &v23, &v24, &v25, &v26,
			); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Old values scan failed: "+err.Error())
				return
			}
			rec.vals = [27]interface{}{
				v0, v1, v2, v3, v4, v5, v6, v7, v8, v9,
				v10, v11, v12, v13, v14, v15, v16, v17,
				v18, v19, v20, v21, v22, v23, v24, v25, v26,
			}
			oldMap[id] = rec
		}
		oldRows.Close()

		var successResults []map[string]interface{}

		for _, update := range validUpdates {
			old, exists := oldMap[update.ConfigID]
			if !exists {
				// This record was not found in the DB â€” abort the whole transaction
				tx.Rollback(ctx)
				api.RespondWithError(w, http.StatusBadRequest, fmt.Sprintf(
					"Record not found: config_id=%s â€” the entire bulk update has been rolled back", update.ConfigID))
				return
			}

			var sets []string
			var args []interface{}
			pos := 1
			for k, v := range update.Fields {
				k = strings.ToLower(k)
				if _, ok := bankConfigFieldPairs[k]; ok {
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
				q := fmt.Sprintf("UPDATE investment.fd_bank_config_master SET %s WHERE config_id=$%d",
					strings.Join(sets, ", "), pos)
				args = append(args, update.ConfigID)
				if _, err := tx.Exec(ctx, q, args...); err != nil {
					// ACID: roll back entire batch on any single failure
					tx.Rollback(ctx)
					msg, status := getUserFriendlyBankConfigError(err, "Update failed")
					api.RespondWithError(w, status, fmt.Sprintf(
						"Update failed for config_id=%s: %s â€” the entire bulk update has been rolled back",
						update.ConfigID, msg))
					return
				}
			}

			auditCols := []string{"config_id", "action_type", "processing_status", "reason", "requested_by", "requested_at"}
			auditVals := []interface{}{update.ConfigID, "EDIT", "PENDING_EDIT_APPROVAL", update.Reason, userEmail}
			auditParams := []string{"$1", "$2", "$3", "$4", "$5", "now()"}
			paramPos := 6
			for k := range update.Fields {
				k = strings.ToLower(k)
				if idx, ok := bankConfigFieldPairs[k]; ok {
					auditCols = append(auditCols, "old_"+k)
					auditVals = append(auditVals, old.vals[idx])
					auditParams = append(auditParams, fmt.Sprintf("$%d", paramPos))
					paramPos++
				}
			}
			auditQuery := fmt.Sprintf("INSERT INTO investment.fd_audit_bank_config (%s) VALUES (%s)",
				strings.Join(auditCols, ", "), strings.Join(auditParams, ", "))
			if _, err := tx.Exec(ctx, auditQuery, auditVals...); err != nil {
				// ACID: roll back entire batch on audit failure
				tx.Rollback(ctx)
				msg, status := getUserFriendlyBankConfigError(err, constants.ErrAuditInsertFailed)
				api.RespondWithError(w, status, fmt.Sprintf(
					"Audit insert failed for config_id=%s: %s â€” the entire bulk update has been rolled back",
					update.ConfigID, msg))
				return
			}

			successResults = append(successResults, map[string]interface{}{
				constants.ValueSuccess: true,
				"config_id":            update.ConfigID,
			})
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyBankConfigError(err, constants.ErrCommitFailedUser)
			api.RespondWithError(w, status, msg)
			return
		}

		// errResults here contains only pre-validation rejections (missing config_id, no fields).
		// Any DB-level error aborted the whole tx above.
		allResults := append(successResults, errResults...)
		api.RespondWithPayload(w, len(successResults) > 0, "", allResults)
		api.LogInfo("BankConfig bulk update: %d updated, %d pre-validation errors", len(successResults), len(errResults))
	}
}

// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
// DeleteBankConfig â€” create delete request (pending approval)
// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

func DeleteBankConfig(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID    string   `json:"user_id"`
			ConfigIDs []string `json:"config_ids"`
			Reason    string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.ConfigIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrNoConfigIDsProvided)
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
			SELECT config_id FROM investment.fd_bank_config_master
			WHERE config_id = ANY($1::text[]) AND COALESCE(is_deleted,false) = false
		`, req.ConfigIDs)
		if err != nil {
			msg, status := getUserFriendlyBankConfigError(err, "Verification failed")
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
				INSERT INTO investment.fd_audit_bank_config
					(config_id, action_type, processing_status, requested_by, reason, requested_at)
				VALUES %s`, strings.Join(auditValues, ","))
			if _, err := tx.Exec(ctx, auditQ, auditArgs...); err != nil {
				msg, status := getUserFriendlyBankConfigError(err, constants.ErrBatchAuditFailed)
				api.RespondWithError(w, status, msg)
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyBankConfigError(err, constants.ErrCommitFailedUser)
			api.RespondWithError(w, status, msg)
			return
		}

		validSet := make(map[string]bool)
		for _, id := range validIDs {
			validSet[id] = true
		}
		var results []map[string]interface{}
		for _, id := range req.ConfigIDs {
			if validSet[id] {
				results = append(results, map[string]interface{}{constants.ValueSuccess: true, "config_id": id})
			} else {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, "config_id": id, constants.ValueError: "Not found or already deleted"})
			}
		}
		api.RespondWithPayload(w, len(validIDs) > 0, "", results)
		api.LogInfo("BankConfig delete requested: %d valid, %d total", len(validIDs), len(req.ConfigIDs))
	}
}

// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
// BulkApproveBankConfig
// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

func BulkApproveBankConfig(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID    string   `json:"user_id"`
			ConfigIDs []string `json:"config_ids"`
			Comment   string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.ConfigIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrNoConfigIDsProvided)
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
			msg, status := getUserFriendlyBankConfigError(err, constants.ErrTransactionFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx)

		_, err = tx.Exec(ctx, `
			UPDATE investment.fd_audit_bank_config
			SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2
			WHERE config_id = ANY($3::text[]) AND processing_status LIKE '%PENDING%'
		`, userEmail, req.Comment, req.ConfigIDs)
		if err != nil {
			msg, status := getUserFriendlyBankConfigError(err, "Approval failed")
			api.RespondWithError(w, status, msg)
			return
		}

		// Mark soft-deleted for approved DELETE requests
		_, err = tx.Exec(ctx, `
			UPDATE investment.fd_bank_config_master
			SET is_deleted=true
			WHERE config_id IN (
				SELECT DISTINCT a.config_id
				FROM investment.fd_audit_bank_config a
				WHERE a.config_id = ANY($1::text[])
				  AND a.action_type='DELETE'
				  AND a.processing_status='APPROVED'
			)
		`, req.ConfigIDs)
		if err != nil {
			msg, status := getUserFriendlyBankConfigError(err, "Delete execution failed")
			api.RespondWithError(w, status, msg)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyBankConfigError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			constants.ValueSuccess: true,
			"approved_count":       len(req.ConfigIDs),
			"checker":              userEmail,
		})
		api.LogInfo("BankConfig bulk approve: %d approved by %s", len(req.ConfigIDs), userEmail)
	}
}

// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
// BulkRejectBankConfig
// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

func BulkRejectBankConfig(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID    string   `json:"user_id"`
			ConfigIDs []string `json:"config_ids"`
			Comment   string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.ConfigIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrNoConfigIDsProvided)
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
			msg, status := getUserFriendlyBankConfigError(err, constants.ErrTransactionFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx)

		_, err = tx.Exec(ctx, `
			UPDATE investment.fd_audit_bank_config
			SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2
			WHERE config_id = ANY($3::text[]) AND processing_status LIKE '%PENDING%'
		`, userEmail, req.Comment, req.ConfigIDs)
		if err != nil {
			msg, status := getUserFriendlyBankConfigError(err, "Rejection failed")
			api.RespondWithError(w, status, msg)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyBankConfigError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			constants.ValueSuccess: true,
			"rejected_count":       len(req.ConfigIDs),
			"checker":              userEmail,
		})
		api.LogInfo("BankConfig bulk reject: %d rejected by %s", len(req.ConfigIDs), userEmail)
	}
}

// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
// GetBankConfigsApprovedActive â€” approved + active + not deleted
// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

func GetBankConfigsApprovedActive(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		rows, err := pgxPool.Query(ctx, `
			SELECT DISTINCT ON (m.config_id)
				m.config_id, m.bank_code,
				COALESCE(mb.bank_name,'')       AS bank_name,
				COALESCE(mb.bank_short_name,'') AS bank_short_name,
				m.product_type,
				m.minimum_amount, m.maximum_amount,
				m.day_count_code,
				m.capitalization_schedule_type, m.capitalization_date_adjustment,
				m.accrual_start_convention, m.accrual_end_convention, m.period_boundary_definition,
				m.weekend_accrual, m.holiday_accrual, m.holiday_calendar_code,
				m.broken_period_method, m.broken_period_location,
				m.interest_rounding_decimals, m.rounding_method, m.rounding_frequency,
				m.grace_period_days, m.grace_period_rate_type,
				m.minimum_compounding_period_days, m.quarter_definition,
				m.tds_deduction_timing,
				TO_CHAR(m.effective_from,'YYYY-MM-DD') AS effective_from,
				TO_CHAR(m.effective_to,'YYYY-MM-DD')   AS effective_to,
				COALESCE(m.config_notes,'') AS config_notes,
				m.is_active
			FROM investment.fd_bank_config_master m
			INNER JOIN investment.fd_audit_bank_config a ON a.config_id = m.config_id
			LEFT JOIN masterbank mb ON mb.bank_id::text = m.bank_code
			WHERE a.processing_status='APPROVED'
			  AND m.is_active=true
			  AND COALESCE(m.is_deleted,false)=false
			ORDER BY m.config_id, m.bank_code
		`)
		if err != nil {
			msg, status := getUserFriendlyBankConfigError(err, constants.ErrQueryFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer rows.Close()

		out := make([]map[string]interface{}, 0)
		for rows.Next() {
			var configID, bankCode, bankName, bankShortName, dayCountCode, capSchedType, capDateAdj string
			var accrualStart, accrualEnd, periodBoundary, holidayCalCode string
			var brokenMethod, brokenLoc, roundingMethod, roundingFreq, tdsDeduction string
			var effectiveFrom, configNotes string
			var productType, gracePeriodRateType, quarterDef, effectiveTo *string
			var minAmt, maxAmt *float64
			var weekendAccrual, holidayAccrual, isActive bool
			var roundingDecimals int
			var gracePeriodDays, minCompoundingPeriodDays *int

			if err := rows.Scan(
				&configID, &bankCode, &bankName, &bankShortName, &productType,
				&minAmt, &maxAmt,
				&dayCountCode,
				&capSchedType, &capDateAdj,
				&accrualStart, &accrualEnd, &periodBoundary,
				&weekendAccrual, &holidayAccrual, &holidayCalCode,
				&brokenMethod, &brokenLoc,
				&roundingDecimals, &roundingMethod, &roundingFreq,
				&gracePeriodDays, &gracePeriodRateType,
				&minCompoundingPeriodDays, &quarterDef,
				&tdsDeduction,
				&effectiveFrom, &effectiveTo,
				&configNotes, &isActive,
			); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Scan error: "+err.Error())
				return
			}

			out = append(out, map[string]interface{}{
				"config_id":                       configID,
				"bank_code":                       bankCode,
				"bank_name":                       bankName,
				"bank_short_name":                 bankShortName,
				"product_type":                    productType,
				"minimum_amount":                  minAmt,
				"maximum_amount":                  maxAmt,
				"day_count_code":                  dayCountCode,
				"capitalization_schedule_type":    capSchedType,
				"capitalization_date_adjustment":  capDateAdj,
				"accrual_start_convention":        accrualStart,
				"accrual_end_convention":          accrualEnd,
				"period_boundary_definition":      periodBoundary,
				"weekend_accrual":                 weekendAccrual,
				"holiday_accrual":                 holidayAccrual,
				"holiday_calendar_code":           holidayCalCode,
				"broken_period_method":            brokenMethod,
				"broken_period_location":          brokenLoc,
				"interest_rounding_decimals":      roundingDecimals,
				"rounding_method":                 roundingMethod,
				"rounding_frequency":              roundingFreq,
				"grace_period_days":               gracePeriodDays,
				"grace_period_rate_type":          gracePeriodRateType,
				"minimum_compounding_period_days": minCompoundingPeriodDays,
				"quarter_definition":              quarterDef,
				"tds_deduction_timing":            tdsDeduction,
				"effective_from":                  effectiveFrom,
				"effective_to":                    effectiveTo,
				"config_notes":                    configNotes,
				"is_active":                       isActive,
			})
		}
		api.RespondWithPayload(w, true, "", out)
		api.LogInfo("BankConfig approved-active: returned %d records", len(out))
	}
}

// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
// GetBankConfigsWithAudit â€” all non-deleted with latest audit (FieldDescriptions pattern)
// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

func GetBankConfigsWithAudit(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		q := `
			WITH latest_audit AS (
				SELECT DISTINCT ON (a.config_id)
					a.config_id,
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
					a.old_product_type,
					a.old_minimum_amount,
					a.old_maximum_amount,
					a.old_day_count_code,
					a.old_capitalization_schedule_type,
					a.old_capitalization_date_adjustment,
					a.old_accrual_start_convention,
					a.old_accrual_end_convention,
					a.old_period_boundary_definition,
					a.old_weekend_accrual,
					a.old_holiday_accrual,
					a.old_holiday_calendar_code,
					a.old_broken_period_method,
					a.old_broken_period_location,
					a.old_interest_rounding_decimals,
					a.old_rounding_method,
					a.old_rounding_frequency,
					a.old_grace_period_days,
					a.old_grace_period_rate_type,
					a.old_minimum_compounding_period_days,
					a.old_quarter_definition,
					a.old_tds_deduction_timing,
					TO_CHAR(a.old_effective_from,'YYYY-MM-DD') AS old_effective_from,
					TO_CHAR(a.old_effective_to,'YYYY-MM-DD')   AS old_effective_to,
					a.old_config_notes,
					a.old_is_active
				FROM investment.fd_audit_bank_config a
				ORDER BY a.config_id,
				         GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp), COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC
			),
			history AS (
				SELECT
					config_id,
					MAX(CASE WHEN action_type='CREATE' THEN requested_by END) AS created_by,
					MAX(CASE WHEN action_type='CREATE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS created_at,
					MAX(CASE WHEN action_type='EDIT'   THEN requested_by END) AS edited_by,
					MAX(CASE WHEN action_type='EDIT'   THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS edited_at,
					MAX(CASE WHEN action_type='DELETE' THEN requested_by END) AS deleted_by,
					MAX(CASE WHEN action_type='DELETE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS deleted_at
				FROM investment.fd_audit_bank_config
				GROUP BY config_id
			)
			SELECT
				m.config_id,
				COALESCE(m.bank_code,'')                               AS bank_code,
				COALESCE(l.old_bank_code,'')                           AS old_bank_code,
				COALESCE(mb.bank_name,'')                              AS bank_name,
				COALESCE(mb.bank_short_name,'')                        AS bank_short_name,
				COALESCE(m.product_type,'')                            AS product_type,
				COALESCE(l.old_product_type,'')                        AS old_product_type,
				m.minimum_amount,
				l.old_minimum_amount,
				m.maximum_amount,
				l.old_maximum_amount,
				COALESCE(m.day_count_code,'')                          AS day_count_code,
				COALESCE(l.old_day_count_code,'')                      AS old_day_count_code,
				COALESCE(m.capitalization_schedule_type,'')            AS capitalization_schedule_type,
				COALESCE(l.old_capitalization_schedule_type,'')        AS old_capitalization_schedule_type,
				COALESCE(m.capitalization_date_adjustment,'')          AS capitalization_date_adjustment,
				COALESCE(l.old_capitalization_date_adjustment,'')      AS old_capitalization_date_adjustment,
				COALESCE(m.accrual_start_convention,'')                AS accrual_start_convention,
				COALESCE(l.old_accrual_start_convention,'')            AS old_accrual_start_convention,
				COALESCE(m.accrual_end_convention,'')                  AS accrual_end_convention,
				COALESCE(l.old_accrual_end_convention,'')              AS old_accrual_end_convention,
				COALESCE(m.period_boundary_definition,'')              AS period_boundary_definition,
				COALESCE(l.old_period_boundary_definition,'')          AS old_period_boundary_definition,
				COALESCE(m.weekend_accrual,false)                      AS weekend_accrual,
				COALESCE(l.old_weekend_accrual,false)                  AS old_weekend_accrual,
				COALESCE(m.holiday_accrual,false)                      AS holiday_accrual,
				COALESCE(l.old_holiday_accrual,false)                  AS old_holiday_accrual,
				COALESCE(m.holiday_calendar_code,'')                   AS holiday_calendar_code,
				COALESCE(l.old_holiday_calendar_code,'')               AS old_holiday_calendar_code,
				COALESCE(m.broken_period_method,'')                    AS broken_period_method,
				COALESCE(l.old_broken_period_method,'')                AS old_broken_period_method,
				COALESCE(m.broken_period_location,'')                  AS broken_period_location,
				COALESCE(l.old_broken_period_location,'')              AS old_broken_period_location,
				COALESCE(m.interest_rounding_decimals,2)               AS interest_rounding_decimals,
				COALESCE(l.old_interest_rounding_decimals,2)           AS old_interest_rounding_decimals,
				COALESCE(m.rounding_method,'')                         AS rounding_method,
				COALESCE(l.old_rounding_method,'')                     AS old_rounding_method,
				COALESCE(m.rounding_frequency,'')                      AS rounding_frequency,
				COALESCE(l.old_rounding_frequency,'')                  AS old_rounding_frequency,
				m.grace_period_days,
				l.old_grace_period_days,
				COALESCE(m.grace_period_rate_type,'')                  AS grace_period_rate_type,
				COALESCE(l.old_grace_period_rate_type,'')              AS old_grace_period_rate_type,
				m.minimum_compounding_period_days,
				l.old_minimum_compounding_period_days,
				COALESCE(m.quarter_definition,'')                      AS quarter_definition,
				COALESCE(l.old_quarter_definition,'')                  AS old_quarter_definition,
				COALESCE(m.tds_deduction_timing,'')                    AS tds_deduction_timing,
				COALESCE(l.old_tds_deduction_timing,'')                AS old_tds_deduction_timing,
				TO_CHAR(m.effective_from,'YYYY-MM-DD')                 AS effective_from,
				COALESCE(l.old_effective_from,'')                      AS old_effective_from,
				TO_CHAR(m.effective_to,'YYYY-MM-DD')                   AS effective_to,
				COALESCE(l.old_effective_to,'')                        AS old_effective_to,
				COALESCE(m.config_notes,'')                            AS config_notes,
				COALESCE(l.old_config_notes,'')                        AS old_config_notes,
				COALESCE(m.is_active,false)                            AS is_active,
				COALESCE(l.old_is_active,false)                        AS old_is_active,
				COALESCE(m.is_deleted,false)                           AS is_deleted,

				COALESCE(l.processing_status,'')                       AS processing_status,
				COALESCE(l.action_type,'')                             AS action_type,
				COALESCE(l.audit_id::text,'')                          AS audit_id,
				COALESCE(l.requested_by,'')                            AS requested_by,
				TO_CHAR(l.requested_at,'YYYY-MM-DD HH24:MI:SS')        AS requested_at,
				COALESCE(l.checker_by,'')                              AS checker_by,
				TO_CHAR(l.checker_at,'YYYY-MM-DD HH24:MI:SS')          AS checker_at,
				COALESCE(l.checker_comment,'')                         AS checker_comment,
				COALESCE(l.reason,'')                                  AS reason,

				COALESCE(h.created_by,'')                              AS created_by,
				COALESCE(h.created_at,'')                              AS created_at,
				COALESCE(h.edited_by,'')                               AS edited_by,
				COALESCE(h.edited_at,'')                               AS edited_at,
				COALESCE(h.deleted_by,'')                              AS deleted_by,
				COALESCE(h.deleted_at,'')                              AS deleted_at

			FROM investment.fd_bank_config_master m
			LEFT JOIN latest_audit l ON l.config_id = m.config_id
			LEFT JOIN history h      ON h.config_id = m.config_id
			LEFT JOIN masterbank mb  ON mb.bank_id::text = m.bank_code
			WHERE COALESCE(m.is_deleted,false) = false
			ORDER BY GREATEST(COALESCE(l.requested_at,'1970-01-01'::timestamp), COALESCE(l.checker_at,'1970-01-01'::timestamp)) DESC
		`

		rows, err := pgxPool.Query(ctx, q)
		if err != nil {
			msg, status := getUserFriendlyBankConfigError(err, constants.ErrQueryFailed)
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

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]any{
			constants.ValueSuccess: true,
			"rows":                 out,
		})
		api.LogInfo("BankConfig WithAudit: returned %d records", len(out))
	}
}

// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
// GetBankConfig â€” single record by config_id
// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

func GetBankConfig(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string `json:"user_id"`
			ConfigID string `json:"config_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.ConfigID == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrConfigIDRequired)
			return
		}

		userEmail := api.GetUserEmailFromCtx(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		row := pgxPool.QueryRow(ctx, `
			SELECT
				m.config_id, m.bank_code,
				COALESCE(mb.bank_name,'') AS bank_name,
				COALESCE(mb.bank_short_name,'') AS bank_short_name,
				m.product_type, m.minimum_amount, m.maximum_amount, m.day_count_code,
				m.capitalization_schedule_type, m.capitalization_date_adjustment,
				m.accrual_start_convention, m.accrual_end_convention, m.period_boundary_definition,
				m.weekend_accrual, m.holiday_accrual, m.holiday_calendar_code,
				m.broken_period_method, m.broken_period_location,
				m.interest_rounding_decimals, m.rounding_method, m.rounding_frequency,
				m.grace_period_days, m.grace_period_rate_type, m.minimum_compounding_period_days, m.quarter_definition,
				m.tds_deduction_timing,
				TO_CHAR(m.effective_from,'YYYY-MM-DD') AS effective_from,
				TO_CHAR(m.effective_to,'YYYY-MM-DD')   AS effective_to,
				COALESCE(m.config_notes,'') AS config_notes,
				m.is_active, COALESCE(m.is_deleted,false) AS is_deleted
			FROM investment.fd_bank_config_master m
			LEFT JOIN masterbank mb ON mb.bank_id::text = m.bank_code
			WHERE m.config_id = $1
		`, req.ConfigID)

		var configID, bankCode, bankName, bankShortName, dayCountCode, capSchedType, capDateAdj string
		var accrualStart, accrualEnd, periodBoundary, holidayCalCode string
		var brokenMethod, brokenLoc, roundingMethod, roundingFreq, tdsDeduction string
		var effectiveFrom, configNotes string
		var productType, gracePeriodRateType, quarterDef, effectiveTo *string
		var minAmt, maxAmt *float64
		var weekendAccrual, holidayAccrual, isActive, isDeleted bool
		var roundingDecimals int
		var gracePeriodDays, minCompoundingPeriodDays *int

		err := row.Scan(
			&configID, &bankCode, &bankName, &bankShortName, &productType, &minAmt, &maxAmt, &dayCountCode,
			&capSchedType, &capDateAdj,
			&accrualStart, &accrualEnd, &periodBoundary,
			&weekendAccrual, &holidayAccrual, &holidayCalCode,
			&brokenMethod, &brokenLoc,
			&roundingDecimals, &roundingMethod, &roundingFreq,
			&gracePeriodDays, &gracePeriodRateType, &minCompoundingPeriodDays, &quarterDef,
			&tdsDeduction,
			&effectiveFrom, &effectiveTo,
			&configNotes, &isActive, &isDeleted,
		)
		if err != nil {
			if err.Error() == "no rows in result set" {
				api.RespondWithError(w, http.StatusNotFound, "Bank config not found")
			} else {
				msg, status := getUserFriendlyBankConfigError(err, "Get failed")
				api.RespondWithError(w, status, msg)
			}
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"config_id":                       configID,
			"bank_code":                       bankCode,
			"bank_name":                       bankName,
			"bank_short_name":                 bankShortName,
			"product_type":                    productType,
			"minimum_amount":                  minAmt,
			"maximum_amount":                  maxAmt,
			"day_count_code":                  dayCountCode,
			"capitalization_schedule_type":    capSchedType,
			"capitalization_date_adjustment":  capDateAdj,
			"accrual_start_convention":        accrualStart,
			"accrual_end_convention":          accrualEnd,
			"period_boundary_definition":      periodBoundary,
			"weekend_accrual":                 weekendAccrual,
			"holiday_accrual":                 holidayAccrual,
			"holiday_calendar_code":           holidayCalCode,
			"broken_period_method":            brokenMethod,
			"broken_period_location":          brokenLoc,
			"interest_rounding_decimals":      roundingDecimals,
			"rounding_method":                 roundingMethod,
			"rounding_frequency":              roundingFreq,
			"grace_period_days":               gracePeriodDays,
			"grace_period_rate_type":          gracePeriodRateType,
			"minimum_compounding_period_days": minCompoundingPeriodDays,
			"quarter_definition":              quarterDef,
			"tds_deduction_timing":            tdsDeduction,
			"effective_from":                  effectiveFrom,
			"effective_to":                    effectiveTo,
			"config_notes":                    configNotes,
			"is_active":                       isActive,
			"is_deleted":                      isDeleted,
		})
		api.LogInfo("GetBankConfig: id=%s by %s", configID, userEmail)
	}
}
