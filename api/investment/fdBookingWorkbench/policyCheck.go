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
// handler should abort (response already written).
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
		api.RespondWithError(w, http.StatusBadGateway, "Policy check failed — could not map booking fields to CDM")
		return false
	}
	if len(vars) == 0 {
		api.LogErrorForResponse(w, "fd booking policy check: empty CDM variable map for %s", handlerName)
		api.RespondWithError(w, http.StatusBadGateway, "Policy check failed — no CDM variables mapped from booking")
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
		api.RespondWithError(w, http.StatusBadGateway, "Policy check failed — please try again later")
		return false
	}
	if result.BlocksSubmit() {
		msg := result.FirstBreachMessage()
		if msg == "" {
			msg = "FD booking blocked by policy"
		}
		api.RespondWithError(w, http.StatusUnprocessableEntity, msg)
		return false
	}
	return true
}

// enforceFDBookingPolicyInline is for bulk loops — returns (ok, errorMessage)
// without writing HTTP. Caller records the error on that row.
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
) (bool, string) {
	vars, err := runtime.BuildFdBookingVariables(ctx, pool, fields)
	if err != nil {
		return false, fmt.Sprintf("policy CDM map failed: %v", err)
	}
	if len(vars) == 0 {
		return false, "policy CDM map empty — check domain_catalog FD_BOOKING cdm_path"
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
		return false, "policy check failed — please try again later"
	}
	if result.BlocksSubmit() {
		msg := result.FirstBreachMessage()
		if msg == "" {
			msg = "FD booking blocked by policy"
		}
		return false, msg
	}
	return true, ""
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
