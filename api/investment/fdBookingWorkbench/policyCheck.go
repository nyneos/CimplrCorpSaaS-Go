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

// enforceFDBookingPolicy builds CDM variables from the booking field map
// (domain_catalog → cdm_path) and runs RunCheck. Returns false when the
// handler should abort (response already written with full policy_results).
func enforceFDBookingPolicy(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	pool *pgxpool.Pool,
	eventCode string,
	handlerName string,
	apiPath string,
	entityCode string,
	actorEmail string,
	correlationID string,
	fields map[string]interface{},
) bool {
	vars, err := runtime.BuildFdBookingVariables(ctx, pool, fields)
	if err != nil {
		api.LogErrorForResponse(w, "fd booking build CDM vars: %v", err)
		api.RespondEnvelopeError(w, http.StatusBadGateway, "Policy check failed — could not map booking fields to CDM", "POLICY_CDM_MAP")
		return false
	}
	if len(vars) == 0 {
		api.LogErrorForResponse(w, "fd booking policy check: empty CDM variable map for %s", handlerName)
		api.RespondEnvelopeError(w, http.StatusBadGateway, "Policy check failed — no CDM variables mapped from booking", "POLICY_CDM_EMPTY")
		return false
	}

	traceID := observability.TraceIDFromContext(ctx)
	if correlationID == "" {
		correlationID = common.ResolveCorrelationID(r, "")
	}

	result, err := runtime.RunCheck(ctx, pool, runtime.CheckRequest{
		EventCode:     eventCode,
		ModuleCode:    common.ModuleInvestmentFD,
		SubModule:     "FD_BOOKING",
		EntityCode:    entityCode,
		ActorUserID:   actorEmail,
		HandlerName:   handlerName,
		APIPath:       apiPath,
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
	eventCode string,
	handlerName string,
	apiPath string,
	entityCode string,
	actorEmail string,
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
		EventCode:     eventCode,
		ModuleCode:    common.ModuleInvestmentFD,
		SubModule:     "FD_BOOKING",
		EntityCode:    entityCode,
		ActorUserID:   actorEmail,
		HandlerName:   handlerName,
		APIPath:       apiPath,
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
	eventCode, handlerName, apiPath, entityCode, actorEmail string,
	fields map[string]interface{},
) bool {
	return runtime.Enforce(ctx, w, r, pool, runtime.EnforceInput{
		EventCode:           eventCode,
		ModuleCode:          common.ModuleInvestmentFD,
		SubModule:           "FD_CONFIRMATION",
		EntityCode:          entityCode,
		ActorUserID:         actorEmail,
		HandlerName:         handlerName,
		APIPath:             apiPath,
		Fields:              fields,
		RequireVariables:    false,
		DefaultBlockMessage: "FD confirmation blocked by policy",
	})
}

// loadFDBookingCDMFields loads a booking row into a field map for CDM mapping.
func loadFDBookingCDMFields(ctx context.Context, pool *pgxpool.Pool, bookingID string) (map[string]interface{}, string, error) {
	var (
		entityID, bankID, interestType string
		principal, rate                float64
		tenorDays, tenorMonths         int
		valueDate, maturity, startDate string
		requestedAt                    interface{}
	)
	err := pool.QueryRow(ctx, `
		SELECT COALESCE(entity_id,''), COALESCE(bank_id,''),
		       COALESCE(principal_amount,0), COALESCE(interest_rate,0),
		       COALESCE(interest_type_code,''),
		       COALESCE(tenure_days,0), COALESCE(tenure_months,0),
		       COALESCE(TO_CHAR(value_date,'YYYY-MM-DD'),''),
		       COALESCE(TO_CHAR(expected_maturity_date,'YYYY-MM-DD'),''),
		       COALESCE(TO_CHAR(expected_start_date,'YYYY-MM-DD'),''),
		       requested_at
		FROM investment.fd_booking_request
		WHERE booking_id = $1 AND COALESCE(is_deleted,false) = false`, bookingID).Scan(
		&entityID, &bankID, &principal, &rate, &interestType,
		&tenorDays, &tenorMonths, &valueDate, &maturity, &startDate, &requestedAt,
	)
	if err != nil {
		return nil, "", fmt.Errorf("load booking for policy: %w", err)
	}
	fields := map[string]interface{}{
		"booking_id":          bookingID,
		"entity_id":           entityID,
		"entity_code":         entityID,
		"bank_id":             bankID,
		"principal_amount":    principal,
		"interest_rate":       rate,
		"interest_type_code":  interestType,
		"tenor_days":          tenorDays,
		"tenure_days":         tenorDays,
		"tenor_months":        tenorMonths,
		"tenure_months":       tenorMonths,
		"value_date":          valueDate,
		"maturity_date":       maturity,
		"expected_start_date": startDate,
		"requested_at":        requestedAt,
	}
	return fields, entityID, nil
}
