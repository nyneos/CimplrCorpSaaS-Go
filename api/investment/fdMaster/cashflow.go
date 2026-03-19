package fdMaster

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

type FDRecord struct {
	FDID                    string
	ConfirmationID          string
	BookingID               string
	EntityID                string
	BankID                  string
	BankAccountID           string
	BankConfigID            string
	PrincipalAmount         float64
	InterestRate            float64
	TenorDays               int
	ValueDate               time.Time
	MaturityDate            time.Time
	MaturityAmount          float64
	InterestType            string
	InterestPayoutFrequency string
	CompoundingFrequency    string
	DayCountConvention      string
	Currency                string
	TDSPlanID               string
	BankFDReference         string
	ReceiptDate             time.Time
	ConfirmationStatus      string
}

type BankConfig struct {
	ConfigID                   string
	DayCountCode               string
	CapitalizationScheduleType string
	QuarterDefinition          string
	TDSDeductionTiming         string
	RoundingMethod             string
	InterestRoundingDecimals   int
}

type CompoundingFreq struct {
	FrequencyID               string
	FrequencyCode             string
	FrequencyName             string
	FrequencyType             string
	CompoundingPeriodsPerYear int
	DaysPerPeriod             int
}

type TDSConfig struct {
	TDSPlanID       string
	TDSRate         float64
	ThresholdAmount float64
	ThresholdType   string
	DeductionTiming string
}

type CashflowRow struct {
	PeriodNumber     int       `json:"period_number"`
	CashflowDate     time.Time `json:"cashflow_date"`
	EventType        string    `json:"event_type"`
	OpeningPrincipal float64   `json:"opening_principal"`
	InterestAmount   float64   `json:"interest_amount"`
	TDSAmount        float64   `json:"tds_amount"`
	NetCashflow      float64   `json:"net_cashflow"`
	ClosingPrincipal float64   `json:"closing_principal"`
}

func loadFDRecord(ctx context.Context, exec queryExecutor, confirmationID string) (*FDRecord, error) {
	rec := &FDRecord{}
	err := exec.QueryRow(ctx, `
		SELECT
			c.confirmation_id,
			COALESCE(c.booking_id, '') AS booking_id,
			COALESCE(b.entity_id, '') AS entity_id,
			COALESCE(b.bank_id, '') AS bank_id,
			COALESCE(b.source_account_id, b.bank_account_id, b.account_id, '') AS bank_account_id,
			COALESCE(b.bank_config_id, '') AS bank_config_id,
			COALESCE(c.actual_principal, 0),
			COALESCE(c.confirmed_rate, 0),
			COALESCE(b.tenure_days, 0),
			COALESCE(c.actual_start_date, b.expected_start_date),
			COALESCE(c.actual_maturity_date, b.expected_maturity_date),
			0,
			COALESCE(b.interest_type_code, ''),
			COALESCE(b.frequency_id, '') AS interest_payout_frequency,
			'' AS compounding_frequency,
			COALESCE(b.day_count_code, '') AS day_count_convention,
			COALESCE(c.currency, '') AS currency,
			COALESCE(b.tds_plan_id, ''),
			COALESCE(c.bank_fd_ref_no, c.bank_reference_number, ''),
			COALESCE(c.confirmation_received_date, c.actual_start_date),
			COALESCE(c.confirmation_status, '')
		FROM investment.fd_confirmation c
		JOIN investment.fd_booking_request b ON b.booking_id = c.booking_id
		WHERE c.confirmation_id = $1
		  AND COALESCE(c.is_deleted, false) = false
		  AND COALESCE(b.is_deleted, false) = false
	`, confirmationID).Scan(
		&rec.ConfirmationID,
		&rec.BookingID,
		&rec.EntityID,
		&rec.BankID,
		&rec.BankAccountID,
		&rec.BankConfigID,
		&rec.PrincipalAmount,
		&rec.InterestRate,
		&rec.TenorDays,
		&rec.ValueDate,
		&rec.MaturityDate,
		&rec.MaturityAmount,
		&rec.InterestType,
		&rec.InterestPayoutFrequency,
		&rec.CompoundingFrequency,
		&rec.DayCountConvention,
		&rec.Currency,
		&rec.TDSPlanID,
		&rec.BankFDReference,
		&rec.ReceiptDate,
		&rec.ConfirmationStatus,
	)
	if err != nil {
		return nil, fmt.Errorf("load FD record: %w", err)
	}
	return rec, nil
}

func loadFDRecordByFDID(ctx context.Context, exec queryExecutor, fdID string) (*FDRecord, error) {
	masterCols, err := loadTableColumns(ctx, exec, "investment", "fd_master")
	if err != nil {
		return nil, err
	}
	keyCol := pickFirstExistingColumn(masterCols, "fd_id", "master_id", "confirmation_id")
	confirmationCol := pickFirstExistingColumn(masterCols, "confirmation_id")
	if keyCol == "" || confirmationCol == "" {
		return nil, fmt.Errorf("fd_master metadata incomplete")
	}

	var confirmationID string
	if err := exec.QueryRow(ctx, fmt.Sprintf(
		"SELECT %s FROM investment.fd_master WHERE %s = $1 AND COALESCE(is_deleted,false)=false",
		confirmationCol, keyCol,
	), fdID).Scan(&confirmationID); err != nil {
		return nil, fmt.Errorf("load fd_master confirmation: %w", err)
	}

	rec, err := loadFDRecord(ctx, exec, confirmationID)
	if err != nil {
		return nil, err
	}
	rec.FDID = fdID
	return rec, nil
}

func loadBankConfig(ctx context.Context, exec queryExecutor, bankConfigID string) (*BankConfig, error) {
	if strings.TrimSpace(bankConfigID) == "" {
		return &BankConfig{}, nil
	}

	cfg := &BankConfig{}
	err := exec.QueryRow(ctx, `
		SELECT
			config_id,
			COALESCE(day_count_code, ''),
			COALESCE(capitalization_schedule_type, ''),
			COALESCE(quarter_definition, ''),
			COALESCE(tds_deduction_timing, ''),
			COALESCE(rounding_method, ''),
			COALESCE(interest_rounding_decimals, 2)
		FROM investment.fd_bank_config_master
		WHERE config_id = $1
		  AND COALESCE(is_deleted, false) = false
	`, bankConfigID).Scan(
		&cfg.ConfigID,
		&cfg.DayCountCode,
		&cfg.CapitalizationScheduleType,
		&cfg.QuarterDefinition,
		&cfg.TDSDeductionTiming,
		&cfg.RoundingMethod,
		&cfg.InterestRoundingDecimals,
	)
	if err != nil {
		return &BankConfig{}, nil
	}
	return cfg, nil
}

func loadCompoundingFreq(ctx context.Context, exec queryExecutor, frequencyRef string) (*CompoundingFreq, error) {
	if strings.TrimSpace(frequencyRef) == "" {
		return &CompoundingFreq{}, nil
	}

	freq := &CompoundingFreq{}
	err := exec.QueryRow(ctx, `
		SELECT
			frequency_id,
			COALESCE(frequency_code, ''),
			COALESCE(frequency_name, ''),
			COALESCE(frequency_type, ''),
			COALESCE(compounding_periods_per_year, 0),
			COALESCE(days_per_period, 0)
		FROM investment.fd_compounding_frequency_master
		WHERE COALESCE(is_deleted, false) = false
		  AND (frequency_id = $1 OR frequency_code = $1 OR frequency_name = $1)
		LIMIT 1
	`, frequencyRef).Scan(
		&freq.FrequencyID,
		&freq.FrequencyCode,
		&freq.FrequencyName,
		&freq.FrequencyType,
		&freq.CompoundingPeriodsPerYear,
		&freq.DaysPerPeriod,
	)
	if err != nil {
		return &CompoundingFreq{}, nil
	}
	return freq, nil
}

func loadTDSConfig(ctx context.Context, exec queryExecutor, planID string) (*TDSConfig, error) {
	if strings.TrimSpace(planID) == "" {
		return &TDSConfig{}, nil
	}

	tds := &TDSConfig{}
	err := exec.QueryRow(ctx, `
		SELECT
			tds_plan_id,
			COALESCE(tds_rate, 0),
			COALESCE(threshold_amount, 0),
			COALESCE(threshold_type, ''),
			COALESCE(deduction_timing, '')
		FROM investment.fd_tds_plan_master
		WHERE tds_plan_id = $1
		  AND COALESCE(is_deleted, false) = false
	`, planID).Scan(
		&tds.TDSPlanID,
		&tds.TDSRate,
		&tds.ThresholdAmount,
		&tds.ThresholdType,
		&tds.DeductionTiming,
	)
	if err != nil {
		return &TDSConfig{}, nil
	}
	return tds, nil
}

func getDivisor(dayCount string) float64 {
	switch strings.ToUpper(strings.TrimSpace(dayCount)) {
	case "ACT_360", "ACT/360", "30_360", "30/360":
		return 360
	default:
		return 365
	}
}

func countDays30_360(start, end time.Time) int {
	y1, m1, d1 := start.Date()
	y2, m2, d2 := end.Date()
	if d1 == 31 {
		d1 = 30
	}
	if d2 == 31 && d1 >= 30 {
		d2 = 30
	}
	return (y2-y1)*360 + int(m2-m1)*30 + (d2 - d1)
}

func roundAmount(value float64, decimals int) float64 {
	if decimals < 0 {
		decimals = 2
	}
	pow := math.Pow(10, float64(decimals))
	return math.Round(value*pow) / pow
}

func nextQuarterEnd(date time.Time) time.Time {
	month := date.Month()
	year := date.Year()
	switch {
	case month <= 3:
		return time.Date(year, 3, 31, 0, 0, 0, 0, date.Location())
	case month <= 6:
		return time.Date(year, 6, 30, 0, 0, 0, 0, date.Location())
	case month <= 9:
		return time.Date(year, 9, 30, 0, 0, 0, 0, date.Location())
	default:
		return time.Date(year, 12, 31, 0, 0, 0, 0, date.Location())
	}
}

func buildPeriodEndDates(fd *FDRecord, cfg *BankConfig, freq *CompoundingFreq) []time.Time {
	if fd == nil {
		return nil
	}

	payout := strings.ToUpper(strings.TrimSpace(fd.InterestPayoutFrequency))
	if payout == "" {
		payout = strings.ToUpper(strings.TrimSpace(freq.FrequencyType))
	}

	var dates []time.Time
	current := fd.ValueDate
	for current.Before(fd.MaturityDate) {
		var next time.Time
		switch payout {
		case "MONTHLY":
			next = current.AddDate(0, 1, 0)
		case "QUARTERLY":
			if strings.EqualFold(cfg.QuarterDefinition, "CALENDAR_QUARTER") {
				next = nextQuarterEnd(current)
				if !next.After(current) {
					next = nextQuarterEnd(current.AddDate(0, 1, 0))
				}
			} else {
				next = current.AddDate(0, 3, 0)
			}
		case "HALF_YEARLY", "SEMI_ANNUAL":
			next = current.AddDate(0, 6, 0)
		case "ANNUAL", "YEARLY":
			next = current.AddDate(1, 0, 0)
		case "DAILY":
			next = current.AddDate(0, 0, 1)
		default:
			next = fd.MaturityDate
		}

		if !next.Before(fd.MaturityDate) {
			break
		}
		dates = append(dates, next)
		current = next
	}

	dates = append(dates, fd.MaturityDate)
	sort.SliceStable(dates, func(i, j int) bool { return dates[i].Before(dates[j]) })

	out := make([]time.Time, 0, len(dates))
	var last time.Time
	for _, d := range dates {
		if last.IsZero() || !d.Equal(last) {
			out = append(out, d)
			last = d
		}
	}
	return out
}

func generateCashflowSchedule(fd *FDRecord, cfg *BankConfig, freq *CompoundingFreq, tdsCfg *TDSConfig) []CashflowRow {
	if fd == nil {
		return nil
	}

	divisor := getDivisor(firstNonEmpty(fd.DayCountConvention, cfg.DayCountCode))
	decimals := cfg.InterestRoundingDecimals
	if decimals == 0 {
		decimals = 2
	}

	periodEnds := buildPeriodEndDates(fd, cfg, freq)
	rows := make([]CashflowRow, 0, len(periodEnds))
	openingPrincipal := fd.PrincipalAmount
	periodStart := fd.ValueDate
	compound := strings.Contains(strings.ToUpper(fd.InterestType), "COMPOUND") || strings.ToUpper(fd.InterestPayoutFrequency) == "AT_MATURITY"

	for index, periodEnd := range periodEnds {
		days := int(periodEnd.Sub(periodStart).Hours() / 24)
		if strings.EqualFold(firstNonEmpty(fd.DayCountConvention, cfg.DayCountCode), "30_360") {
			days = countDays30_360(periodStart, periodEnd)
		}
		if days <= 0 {
			days = 1
		}

		interest := roundAmount(openingPrincipal*fd.InterestRate*float64(days)/(divisor*100), decimals)
		tdsAmount := 0.0
		if tdsCfg != nil && tdsCfg.TDSRate > 0 {
			tdsAmount = roundAmount(interest*tdsCfg.TDSRate/100, decimals)
		}
		netCashflow := roundAmount(interest-tdsAmount, decimals)
		closingPrincipal := openingPrincipal
		eventType := "INTEREST"

		if compound && periodEnd.Before(fd.MaturityDate) {
			closingPrincipal = roundAmount(openingPrincipal+netCashflow, decimals)
			netCashflow = 0
			eventType = "COMPOUNDING"
		}
		if periodEnd.Equal(fd.MaturityDate) {
			eventType = "MATURITY"
			netCashflow = roundAmount(netCashflow+closingPrincipal, decimals)
		}

		rows = append(rows, CashflowRow{
			PeriodNumber:     index + 1,
			CashflowDate:     periodEnd,
			EventType:        eventType,
			OpeningPrincipal: openingPrincipal,
			InterestAmount:   interest,
			TDSAmount:        tdsAmount,
			NetCashflow:      netCashflow,
			ClosingPrincipal: closingPrincipal,
		})

		openingPrincipal = closingPrincipal
		periodStart = periodEnd
	}

	return rows
}

func GenerateCashflowForFD(ctx context.Context, exec queryExecutor, fdID string, confirmationID string) ([]CashflowRow, *FDRecord, error) {
	var fd *FDRecord
	var err error
	if strings.TrimSpace(fdID) != "" {
		fd, err = loadFDRecordByFDID(ctx, exec, fdID)
	} else {
		fd, err = loadFDRecord(ctx, exec, confirmationID)
	}
	if err != nil {
		return nil, nil, err
	}

	cfg, _ := loadBankConfig(ctx, exec, fd.BankConfigID)
	freq, _ := loadCompoundingFreq(ctx, exec, firstNonEmpty(fd.CompoundingFrequency, fd.InterestPayoutFrequency))
	tds, _ := loadTDSConfig(ctx, exec, fd.TDSPlanID)

	return generateCashflowSchedule(fd, cfg, freq, tds), fd, nil
}

func SaveCashflowSchedule(ctx context.Context, exec queryExecutor, fdID string, rows []CashflowRow) error {
	if strings.TrimSpace(fdID) == "" || len(rows) == 0 {
		return nil
	}

	table := resolveFirstExistingTable(ctx, exec, []string{
		"investment.fd_cashflow_schedule",
		"investment.fd_cashflow",
		"investment.fd_master_cashflow_schedule",
	})
	if table == "" {
		return nil
	}

	schemaName, tableName := splitQualifiedTable(table)
	cols, err := loadTableColumns(ctx, exec, schemaName, tableName)
	if err != nil {
		return err
	}
	fdCol := pickFirstExistingColumn(cols, "fd_id", "master_id")
	if fdCol == "" {
		return nil
	}

	if pickFirstExistingColumn(cols, "period_number") != "" {
		_, _ = exec.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE %s = $1", table, fdCol), fdID)
	}

	for _, row := range rows {
		valueMap := map[string]interface{}{
			fdCol:               fdID,
			"period_number":     row.PeriodNumber,
			"cashflow_date":     row.CashflowDate,
			"event_type":        row.EventType,
			"opening_principal": row.OpeningPrincipal,
			"interest_amount":   row.InterestAmount,
			"tds_amount":        row.TDSAmount,
			"net_cashflow":      row.NetCashflow,
			"net_amount":        row.NetCashflow,
			"closing_principal": row.ClosingPrincipal,
			"created_at":        time.Now(),
		}
		preferredCols := []string{fdCol, "period_number", "cashflow_date", "event_type", "opening_principal", "interest_amount", "tds_amount", "net_cashflow", "net_amount", "closing_principal", "created_at"}
		insertSQL, args, _, ok := buildDynamicInsert(table, cols, preferredCols, valueMap, nil)
		if !ok {
			continue
		}
		if _, err := exec.Exec(ctx, insertSQL, args...); err != nil {
			return fmt.Errorf("save cashflow schedule: %w", err)
		}
	}

	return nil
}
