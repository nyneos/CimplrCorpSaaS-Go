// bankConfigMaster_validation_test.go — BRD enum validation checks for the
// Bank FD Configuration Master (create / edit inputs).
package allMaster

import "testing"

func validTestInput() BankConfigInput {
	return BankConfigInput{
		BankCode:                     "HDFC001",
		DayCountCode:                 "DC-ACT-ACT",
		CapitalizationScheduleType:   "ANNIVERSARY",
		CapitalizationDateAdjustment: "FOLLOWING_WD",
		AccrualStartConvention:       "INCLUDE",
		AccrualEndConvention:         "EXCLUDE",
		PeriodBoundaryDefinition:     "INCL_START_EXCL_END",
		BrokenPeriodMethod:           "SIMPLE",
		BrokenPeriodLocation:         "LAST",
		RoundingMethod:               "ROUND",
		RoundingFrequency:            "EACH_PERIOD",
		TdsDeductionTiming:           "ACCRUAL_ANNUAL",
		EffectiveFrom:                "2025-01-01",
	}
}

func TestValidateBankConfigFieldsValidBRDInput(t *testing.T) {
	if err := validateBankConfigFields(validTestInput()); err != nil {
		t.Fatalf("BRD-conformant input must validate, got: %v", err)
	}
}

func TestValidateBankConfigFieldsBRDSynonyms(t *testing.T) {
	in := validTestInput()
	in.CapitalizationScheduleType = "CALENDAR_QTR_END"
	in.RoundingMethod = "FLOOR" // BRD token, engine maps to ROUND_DOWN
	in.RoundingFrequency = "FINAL_ONLY"
	in.AccrualStartConvention = "NEXT_WD"
	in.AccrualEndConvention = "PRECEDING_WD"
	in.PeriodBoundaryDefinition = "EXCL_BOTH"
	in.BrokenPeriodMethod = "HYBRID"
	qd := "90_DAYS"
	in.QuarterDefinition = &qd
	if err := validateBankConfigFields(in); err != nil {
		t.Fatalf("BRD synonym values must validate, got: %v", err)
	}
}

func TestValidateBankConfigFieldsRejectsInvalidEnums(t *testing.T) {
	cases := []func(*BankConfigInput){
		func(i *BankConfigInput) { i.CapitalizationScheduleType = "QUARTERLYISH" },
		func(i *BankConfigInput) { i.CapitalizationDateAdjustment = "MOVE_SOMEWHERE" },
		func(i *BankConfigInput) { i.AccrualStartConvention = "MAYBE" },
		func(i *BankConfigInput) { i.AccrualEndConvention = "MAYBE" },
		func(i *BankConfigInput) { i.PeriodBoundaryDefinition = "INCL_NOTHING" },
		func(i *BankConfigInput) { i.BrokenPeriodMethod = "MAGIC" },
		func(i *BankConfigInput) { i.BrokenPeriodLocation = "MIDDLE" },
		func(i *BankConfigInput) { i.RoundingMethod = "GUESS" },
		func(i *BankConfigInput) { i.RoundingFrequency = "SOMETIMES" },
		func(i *BankConfigInput) { i.TdsDeductionTiming = "WHENEVER" },
		func(i *BankConfigInput) { bad := "13_WEEKS"; i.QuarterDefinition = &bad },
		func(i *BankConfigInput) { bad := "DOUBLE_RATE"; i.GracePeriodRateType = &bad },
	}
	for idx, mutate := range cases {
		in := validTestInput()
		mutate(&in)
		if err := validateBankConfigFields(in); err == nil {
			t.Errorf("case %d: invalid enum value must be rejected", idx)
		}
	}
}

func TestValidateBankConfigFieldsMapForEdits(t *testing.T) {
	if err := validateBankConfigFieldsMap(map[string]interface{}{
		"broken_period_method": "COMPOUND",
		"config_notes":         "free text is not enum-checked",
	}); err != nil {
		t.Fatalf("valid edit map must pass, got: %v", err)
	}
	if err := validateBankConfigFieldsMap(map[string]interface{}{
		"broken_period_method": "SOMETIMES_COMPOUND",
	}); err == nil {
		t.Fatal("invalid enum in edit map must be rejected")
	}
}
