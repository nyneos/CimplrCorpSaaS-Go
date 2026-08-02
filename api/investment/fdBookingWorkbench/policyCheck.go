package fdBooking

import (
	"context"
	"fmt"
	"net/http"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/policyengine/common"
	"CimplrCorpSaas/api/policyengine/runtime"
	"CimplrCorpSaas/internal/observability"

	"github.com/jackc/pgx/v5/pgxpool"
)

// enforceCtx groups the per-call identifiers shared by the fdBooking
// Enforce/EnforceInline variants (event/actor/handler routing info).
// CorrelationID is optional — empty means the callee resolves its own.
type enforceCtx struct {
	EventCode, HandlerName, APIPath, EntityCode, Actor, CorrelationID string
}

// enforceFDBookingPolicy builds CDM variables from the booking field map
// (domain_catalog → cdm_path) and runs RunCheck. Returns false when the
// handler should abort (response already written with full policy_results).
func enforceFDBookingPolicy(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	pool *pgxpool.Pool,
	cc enforceCtx,
	fields map[string]interface{},
) bool {
	vars, err := runtime.BuildFdBookingVariables(ctx, pool, fields)
	if err != nil {
		api.LogErrorForResponse(w, "fd booking build CDM vars: %v", err)
		api.RespondEnvelopeError(w, http.StatusBadGateway, "Policy check failed — could not map booking fields to CDM", "POLICY_CDM_MAP")
		return false
	}
	if len(vars) == 0 {
		api.LogErrorForResponse(w, "fd booking policy check: empty CDM variable map for %s", cc.HandlerName)
		api.RespondEnvelopeError(w, http.StatusBadGateway, "Policy check failed — no CDM variables mapped from booking", "POLICY_CDM_EMPTY")
		return false
	}

	traceID := observability.TraceIDFromContext(ctx)
	correlationID := cc.CorrelationID
	if correlationID == "" {
		correlationID = common.ResolveCorrelationID(r, "")
	}

	result, err := runtime.RunCheck(ctx, pool, runtime.CheckRequest{
		EventCode:     cc.EventCode,
		ModuleCode:    common.ModuleInvestmentFD,
		SubModule:     "FD_BOOKING",
		EntityCode:    cc.EntityCode,
		ActorUserID:   cc.Actor,
		HandlerName:   cc.HandlerName,
		APIPath:       cc.APIPath,
		CorrelationID: correlationID,
		TraceID:       traceID,
		Variables:     vars,
	})
	if err != nil {
		api.LogErrorForResponse(w, "fd booking policy check: %v", err)
		api.RespondEnvelopeError(w, http.StatusBadGateway, "Policy check failed — please try again later", "POLICY_SERVICE_ERROR")
		return false
	}
	if result.BlocksSubmit() {
		msg := result.ClientMessage("FD booking blocked by policy")
		api.RespondEnvelopeFailureWithData(w, http.StatusUnprocessableEntity, msg, "POLICY_BREACH", result.BlockPayload())
		return false
	}
	result.WriteSummaryHeader(w)
	return true
}

// enforceFDBookingPolicyInline is for bulk loops — returns (ok, errorMessage, result)
// without writing HTTP. Caller records the error on that row; result carries all
// related policy pass/fail outcomes for that item.
func enforceFDBookingPolicyInline(
	ctx context.Context,
	r *http.Request,
	pool *pgxpool.Pool,
	cc enforceCtx,
	fields map[string]interface{},
) (bool, string, runtime.CheckResult) {
	vars, err := runtime.BuildFdBookingVariables(ctx, pool, fields)
	if err != nil {
		return false, fmt.Sprintf("policy CDM map failed: %v", err), runtime.CheckResult{}
	}
	if len(vars) == 0 {
		return false, "policy CDM map empty — check domain_catalog FD_BOOKING cdm_path", runtime.CheckResult{}
	}
	result, err := runtime.RunCheck(ctx, pool, runtime.CheckRequest{
		EventCode:     cc.EventCode,
		ModuleCode:    common.ModuleInvestmentFD,
		SubModule:     "FD_BOOKING",
		EntityCode:    cc.EntityCode,
		ActorUserID:   cc.Actor,
		HandlerName:   cc.HandlerName,
		APIPath:       cc.APIPath,
		CorrelationID: common.ResolveCorrelationID(r, ""),
		TraceID:       observability.TraceIDFromContext(ctx),
		Variables:     vars,
	})
	if err != nil {
		return false, "policy check failed — please try again later", runtime.CheckResult{}
	}
	if result.BlocksSubmit() {
		msg := result.ClientMessage("FD booking blocked by policy")
		return false, msg, result
	}
	return true, "", result
}

// EnforceFDConfirmationPolicy is used by confirmation handlers (shared module).
func EnforceFDConfirmationPolicy(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	pool *pgxpool.Pool,
	cc enforceCtx,
	fields map[string]interface{},
) bool {
	return runtime.Enforce(ctx, w, r, pool, runtime.EnforceInput{
		EventCode:           cc.EventCode,
		ModuleCode:          common.ModuleInvestmentFD,
		SubModule:           "FD_CONFIRMATION",
		EntityCode:          cc.EntityCode,
		ActorUserID:         cc.Actor,
		HandlerName:         cc.HandlerName,
		APIPath:             cc.APIPath,
		Fields:              fields,
		RequireVariables:    false,
		DefaultBlockMessage: "FD confirmation blocked by policy",
	})
}

// loadFDBookingCDMFields loads a booking row into a field map for CDM mapping.
// Covers the full real column set on fd_booking_request so Approve/Reject/Delete
// see the same field universe as Create/Update, not just the original curated subset.
func loadFDBookingCDMFields(ctx context.Context, pool *pgxpool.Pool, bookingID string) (map[string]interface{}, string, error) {
	var (
		entityID, entityName, bankID, bankName, interestTypeCode, interestTypeID string
		principal, rate                                                          float64
		tenorDays, tenorMonths, tenureYears                                      int
		valueDate, maturity, startDate, tenorType, valueType                     string
		frequencyID, payoutFrequencyID, accrualFrequencyCode, resetType          string
		dayCountCode, tdsPlanID, bankConfigID                                    string
		sourceAccountID, sourceAccountNumber, productCode                        string
		bookingRemarks, bookingStatus, offerValidTill                            string
		autoRenewal                                                              bool
		requestedAt                                                              interface{}
	)
	err := pool.QueryRow(ctx, `
		SELECT COALESCE(entity_id,''), COALESCE(entity_name,''),
		       COALESCE(bank_id,''), COALESCE(bank_name,''),
		       COALESCE(principal_amount,0), COALESCE(interest_rate,0),
		       COALESCE(interest_type_code,''), COALESCE(interest_type_id,''),
		       COALESCE(tenure_days,0), COALESCE(tenure_months,0), COALESCE(tenure_years,0),
		       COALESCE(TO_CHAR(value_date,'YYYY-MM-DD'),''),
		       COALESCE(TO_CHAR(expected_maturity_date,'YYYY-MM-DD'),''),
		       COALESCE(TO_CHAR(expected_start_date,'YYYY-MM-DD'),''),
		       COALESCE(tenor_type,''), COALESCE(value_type,''),
		       COALESCE(frequency_id,''), COALESCE(payout_frequency_id,''),
		       COALESCE(accrual_frequency_code,''), COALESCE(reset_type,''),
		       COALESCE(day_count_code,''), COALESCE(tds_plan_id,''), COALESCE(bank_config_id,''),
		       COALESCE(source_account_id,''), COALESCE(source_account_number,''), COALESCE(product_code,''),
		       COALESCE(booking_remarks,''), COALESCE(booking_status,''), COALESCE(TO_CHAR(offer_valid_till,'YYYY-MM-DD'),''),
		       COALESCE(auto_renewal,false),
		       requested_at
		FROM investment.fd_booking_request
		WHERE booking_id = $1 AND COALESCE(is_deleted,false) = false`, bookingID).Scan(
		&entityID, &entityName, &bankID, &bankName, &principal, &rate,
		&interestTypeCode, &interestTypeID, &tenorDays, &tenorMonths, &tenureYears,
		&valueDate, &maturity, &startDate, &tenorType, &valueType,
		&frequencyID, &payoutFrequencyID, &accrualFrequencyCode, &resetType,
		&dayCountCode, &tdsPlanID, &bankConfigID,
		&sourceAccountID, &sourceAccountNumber, &productCode,
		&bookingRemarks, &bookingStatus, &offerValidTill,
		&autoRenewal, &requestedAt,
	)
	if err != nil {
		return nil, "", fmt.Errorf("load booking for policy: %w", err)
	}
	fields := map[string]interface{}{
		"booking_id":             bookingID,
		"entity_id":              entityID,
		"entity_code":            entityID,
		"entity_name":            entityName,
		"bank_id":                bankID,
		"bank_name":              bankName,
		"principal_amount":       principal,
		"interest_rate":          rate,
		"interest_type_code":     interestTypeCode,
		"interest_type_id":       interestTypeID,
		"tenor_days":             tenorDays,
		"tenure_days":            tenorDays,
		"tenor_months":           tenorMonths,
		"tenure_months":          tenorMonths,
		"tenure_years":           tenureYears,
		"tenor_type":             tenorType,
		"value_date":             valueDate,
		"maturity_date":          maturity,
		"expected_start_date":    startDate,
		"value_type":             valueType,
		"frequency_id":           frequencyID,
		"payout_frequency_id":    payoutFrequencyID,
		"accrual_frequency_code": accrualFrequencyCode,
		"reset_type":             resetType,
		"day_count_code":         dayCountCode,
		"tds_plan_id":            tdsPlanID,
		"bank_config_id":         bankConfigID,
		"source_account_id":      sourceAccountID,
		"source_account_number":  sourceAccountNumber,
		"product_code":           productCode,
		"booking_remarks":        bookingRemarks,
		"booking_status":         bookingStatus,
		"offer_valid_till":       offerValidTill,
		"auto_renewal":           autoRenewal,
		"requested_at":           requestedAt,
	}
	return fields, entityID, nil
}
