package fdBooking

// BuildSimulateDiffInput maps a flat booking/confirmation row into the payload for
// POST /investment/fd/simulator/cashflow/diff (booking vs confirmation legs).
func BuildSimulateDiffInput(row map[string]interface{}) map[string]interface{} {
	bookingStart := strVal(row["value_date"])
	if bookingStart == "" {
		bookingStart = strVal(row["expected_start_date"])
	}
	bookingMaturity := strVal(row["expected_maturity_date"])
	if bookingMaturity == "" {
		bookingMaturity = strVal(row["maturity_date"])
	}

	confirmedStart := strVal(row["actual_start_date"])
	if confirmedStart == "" {
		confirmedStart = strVal(row["confirmed_value_date"])
	}
	if confirmedStart == "" {
		confirmedStart = bookingStart
	}
	confirmedMaturity := strVal(row["actual_maturity_date"])
	if confirmedMaturity == "" {
		confirmedMaturity = strVal(row["confirmed_maturity_date"])
	}
	if confirmedMaturity == "" {
		confirmedMaturity = bookingMaturity
	}

	confirmedPrincipal := row["actual_principal"]
	if isZeroNumeric(confirmedPrincipal) {
		confirmedPrincipal = row["confirmed_principal_amount"]
	}
	if isZeroNumeric(confirmedPrincipal) {
		confirmedPrincipal = row["principal_amount"]
	}
	confirmedRate := row["confirmed_rate"]
	if isZeroNumeric(confirmedRate) {
		confirmedRate = row["confirmed_interest_rate"]
	}
	if isZeroNumeric(confirmedRate) {
		confirmedRate = row["interest_rate"]
	}
	confirmedInterestType := strVal(row["confirmed_interest_type"])
	if confirmedInterestType == "" {
		confirmedInterestType = strVal(row["interest_type"])
	}

	frequencyID := strVal(row["frequency_id"])
	payoutFreqID := strVal(row["payout_frequency_id"])
	if payoutFreqID == "" {
		payoutFreqID = frequencyID
	}

	accrualFreq := strVal(row["accrual_frequency_code"])
	confirmedAccrualFreq := strVal(row["confirmed_accrual_frequency_code"])
	if confirmedAccrualFreq == "" {
		confirmedAccrualFreq = accrualFreq
	}

	resetType := strVal(row["reset_type"])
	if resetType == "" {
		resetType = "AT_MATURITY"
	}
	confirmedReset := strVal(row["confirmed_reset_type"])
	if confirmedReset == "" {
		confirmedReset = resetType
	}

	configBlock := map[string]interface{}{
		"bank_config_id":                 row["bank_config_id"],
		"frequency_id":                   frequencyID,
		"payout_frequency_id":            payoutFreqID,
		"day_count_code":                 row["day_count_code"],
		"tds_plan_id":                    row["tds_plan_id"],
		"holiday_calendar_code":          row["holiday_calendar_code"],
		"tds_deduction_timing":           row["tds_deduction_timing"],
		"rounding_method":                row["rounding_method"],
		"rounding_frequency":             row["rounding_frequency"],
		"interest_rounding_decimals":     row["interest_rounding_decimals"],
		"broken_period_method":           row["broken_period_method"],
		"broken_period_location":         row["broken_period_location"],
		"grace_period_days":              row["grace_period_days"],
		"grace_period_rate_type":         row["grace_period_rate_type"],
		"capitalization_schedule_type":   row["capitalization_schedule_type"],
		"capitalization_date_adjustment": row["capitalization_date_adjustment"],
		"weekend_accrual":                row["weekend_accrual"],
		"holiday_accrual":                row["holiday_accrual"],
		"accrual_start_convention":       row["accrual_start_convention"],
		"accrual_end_convention":         row["accrual_end_convention"],
		"period_boundary_definition":     row["period_boundary_definition"],
		"quarter_definition":             row["quarter_definition"],
		"accrual_frequency_code":         accrualFreq,
		"reset_type":                     resetType,
	}

	bookingLeg := map[string]interface{}{
		"principal_amount": row["principal_amount"],
		"interest_rate":    row["interest_rate"],
		"start_date":       bookingStart,
		"maturity_date":    bookingMaturity,
		"tenor_days":       firstNonZero(row["tenor_days"], row["tenure_days"]),
		"tenor_months":     firstNonZero(row["tenor_months"], row["tenure_months"]),
		"tenor_years":      firstNonZero(row["tenor_years"], row["tenure_years"]),
		"interest_type":    strVal(row["interest_type"]),
	}
	for k, v := range configBlock {
		bookingLeg[k] = v
	}

	confirmationLeg := map[string]interface{}{
		"principal_amount": confirmedPrincipal,
		"interest_rate":    confirmedRate,
		"start_date":       confirmedStart,
		"maturity_date":    confirmedMaturity,
		"tenor_days":       firstNonZero(row["tenor_days"], row["tenure_days"], row["confirmed_tenor_days"]),
		"tenor_months":     firstNonZero(row["tenor_months"], row["tenure_months"], row["confirmed_tenor_months"]),
		"tenor_years":      firstNonZero(row["tenor_years"], row["tenure_years"], row["confirmed_tenor_years"]),
		"interest_type":    confirmedInterestType,
		"accrual_frequency_code": confirmedAccrualFreq,
		"reset_type":             confirmedReset,
	}
	for k, v := range configBlock {
		confirmationLeg[k] = v
	}
	if fp := strVal(row["first_payout_date"]); fp != "" {
		confirmationLeg["first_payout_date"] = fp
	}
	if fc := strVal(row["first_capitalization_date"]); fc != "" {
		confirmationLeg["first_capitalization_date"] = fc
	}

	return map[string]interface{}{
		"booking":      bookingLeg,
		"confirmation": confirmationLeg,
	}
}

func firstNonZero(vals ...interface{}) interface{} {
	for _, v := range vals {
		if !isZeroNumeric(v) {
			return v
		}
	}
	return 0
}

// AttachSimulateDiffInput adds simulate_diff_input and strips duplicated config keys.
func AttachSimulateDiffInput(row map[string]interface{}) {
	row["simulate_diff_input"] = BuildSimulateDiffInput(row)
	for _, k := range []string{
		"tds_deduction_timing", "rounding_method", "rounding_frequency",
		"interest_rounding_decimals", "broken_period_method", "broken_period_location",
		"grace_period_days", "grace_period_rate_type",
		"capitalization_schedule_type", "capitalization_date_adjustment",
		"weekend_accrual", "holiday_accrual",
		"accrual_start_convention", "accrual_end_convention",
		"period_boundary_definition", "quarter_definition",
	} {
		delete(row, k)
	}
}
