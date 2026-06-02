package fdMaturityAndRollover

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/approvalengine"
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/api/investment/fdMaster"
	"CimplrCorpSaas/api/investment/fdNotifications"
	notifcatalog "CimplrCorpSaas/api/notification/catalog"
	s3storage "CimplrCorpSaas/api/utils/s3storage"
	"CimplrCorpSaas/api/varianceengine"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	cimplrClosureModule = "FIXED_DEPOSIT"

	// Granular FD Closure Approval Types
	txClosureInitiatePayoutCreate   = "FD_CLOSURE_INITIATE_PAYOUT_CREATE"
	txClosureInitiatePayoutEdit     = "FD_CLOSURE_INITIATE_PAYOUT_EDIT"
	txClosureInitiatePayoutDelete   = "FD_CLOSURE_INITIATE_PAYOUT_DELETE"
	txClosureInitiateRolloverCreate = "FD_CLOSURE_INITIATE_ROLLOVER_CREATE"
	txClosureInitiateRolloverEdit   = "FD_CLOSURE_INITIATE_ROLLOVER_EDIT"
	txClosureInitiateRolloverDelete = "FD_CLOSURE_INITIATE_ROLLOVER_DELETE"

	txClosureConfirmPayoutCreate   = "FD_CLOSURE_CONFIRM_PAYOUT_CREATE"
	txClosureConfirmPayoutEdit     = "FD_CLOSURE_CONFIRM_PAYOUT_EDIT"
	txClosureConfirmPayoutDelete   = "FD_CLOSURE_CONFIRM_PAYOUT_DELETE"
	txClosureConfirmRolloverCreate = "FD_CLOSURE_CONFIRM_ROLLOVER_CREATE"
	txClosureConfirmRolloverEdit   = "FD_CLOSURE_CONFIRM_ROLLOVER_EDIT"
	txClosureConfirmRolloverDelete = "FD_CLOSURE_CONFIRM_ROLLOVER_DELETE"

	txClosurePrematureCreate = "FD_CLOSURE_PREMATURE_CREATE"
	txClosurePrematureEdit   = "FD_CLOSURE_PREMATURE_EDIT"
	txClosurePrematureDelete = "FD_CLOSURE_PREMATURE_DELETE"
)

// --- Notification Trigger Helpers for FD Closure V2 ---
func cimplrGetClosureTypes(ctx context.Context, pool *pgxpool.Pool, table string, ids []string) map[string]string {
	out := make(map[string]string)
	if len(ids) == 0 {
		return out
	}
	idCol := "closure_initiate_id"
	if table == "cimplr.fd_closure_confirm" {
		idCol = "closure_confirm_id"
	}

	// Create placeholders for the query
	placeholders := make([]string, len(ids))
	args := make([]interface{}, len(ids))
	for i, id := range ids {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = id
	}

	q := fmt.Sprintf(`SELECT %s, closure_type FROM %s WHERE %s IN (%s)`, idCol, table, idCol, strings.Join(placeholders, ","))
	rows, err := pool.Query(ctx, q, args...)
	if err != nil {
		api.LogError("[CimplrFDClosure] fetch types failed: %v", err)
		return out
	}
	defer rows.Close()
	for rows.Next() {
		var id, cType string
		if err := rows.Scan(&id, &cType); err == nil {
			out[id] = strings.ToUpper(strings.TrimSpace(cType))
		}
	}
	return out
}

func triggerClosureBulkNotif(ctx context.Context, pool *pgxpool.Pool, ids []string, table, stage, action, email string) {
	if len(ids) == 0 {
		return
	}
	// Premature is a direct flow
	if stage == "premature" {
		route := fmt.Sprintf("/investment/fd/closure/premature/%s", action)
		payload := fdNotifications.BuildCimplrClosureConfirmNotifPayload(context.Background(), pool, ids, strings.ToUpper(action), email).ToMap()
		notifcatalog.TriggerNotification(context.Background(), pool, route, ids[0], payload)
		return
	}

	typeMap := cimplrGetClosureTypes(ctx, pool, table, ids)
	payoutIDs := []string{}
	rolloverIDs := []string{}
	prematureIDs := []string{}
	for _, id := range ids {
		ctype := typeMap[id]
		if ctype == "ROLLOVER" {
			rolloverIDs = append(rolloverIDs, id)
		} else if ctype == "PREMATURE" {
			prematureIDs = append(prematureIDs, id)
		} else {
			// default to payout
			payoutIDs = append(payoutIDs, id)
		}
	}

	dispatch := func(batch []string, subtype string) {
		if len(batch) == 0 {
			return
		}
		route := fmt.Sprintf("/investment/fd/closure/%s/%s/%s", stage, subtype, action)
		var payload map[string]interface{}
		if stage == "initiate" {
			payload = fdNotifications.BuildCimplrClosureInitiateNotifPayload(context.Background(), pool, batch, strings.ToUpper(action), email).ToMap()
		} else {
			payload = fdNotifications.BuildCimplrClosureConfirmNotifPayload(context.Background(), pool, batch, strings.ToUpper(action), email).ToMap()
		}
		notifcatalog.TriggerNotification(context.Background(), pool, route, batch[0], payload)
	}
	dispatch(payoutIDs, "payout")
	dispatch(rolloverIDs, "rollover")

	if len(prematureIDs) > 0 {
		route := fmt.Sprintf("/investment/fd/closure/premature/%s", action)
		payload := fdNotifications.BuildCimplrClosureConfirmNotifPayload(context.Background(), pool, prematureIDs, strings.ToUpper(action), email).ToMap()
		notifcatalog.TriggerNotification(context.Background(), pool, route, prematureIDs[0], payload)
	}
}

func getClosureTxCode(stage, subtype, action string) string {
	subtype = strings.ToUpper(subtype)
	action = strings.ToUpper(action)
	if stage == "premature" {
		return fmt.Sprintf("FD_CLOSURE_PREMATURE_%s", action)
	}
	return fmt.Sprintf("FD_CLOSURE_%s_%s_%s", strings.ToUpper(stage), subtype, action)
}

// ------------------------------------------------------

type cimplrRowQuerier interface {
	QueryRow(ctx context.Context, sql string, arguments ...interface{}) pgx.Row
}

type cimplrClosureInitiateRequest struct {
	UserID                  string  `json:"user_id"`
	ClosureInitiateID       string  `json:"closure_initiate_id"`
	FDID                    string  `json:"fd_id"`
	ClosureType             string  `json:"closure_type"`
	ActionAtMaturity        string  `json:"action_at_maturity"`
	RequestedClosureDate    string  `json:"requested_closure_date"`
	PrincipalAmount         float64 `json:"principal_amount"`
	ExpectedMaturityValue   float64 `json:"expected_maturity_value"`
	AccruedInterestTillDate float64 `json:"accrued_interest_till_date"`
	TDSExpected             float64 `json:"tds_expected"`
	NetExpectedAmount       float64 `json:"net_expected_amount"`
	AutoRenewalFlag         *bool   `json:"auto_renewal_flag"`
	MaturityStatus          string  `json:"maturity_status"`
	ActionRequired          *bool   `json:"action_required"`
	RolloverType            string  `json:"rollover_type"`
	RolloverBankType        string  `json:"rollover_bank_type"`
	NewBankID               string  `json:"new_bank_id"`
	NewBankName             string  `json:"new_bank_name"`
	TentativeNewTenorDays   int     `json:"tentative_new_tenor_days"`
	Remarks                 string  `json:"remarks"`
	Reason                  string  `json:"reason"`
}

type cimplrClosureConfirmRequest struct {
	UserID               string  `json:"user_id"`
	FDID                 string  `json:"fd_id"`
	ClosureConfirmID     string  `json:"closure_confirm_id"`
	ClosureInitiateID    string  `json:"closure_initiate_id"`
	ConfirmationMode     string  `json:"confirmation_mode"`
	BankReferenceNo      string  `json:"bank_reference_no"`
	ActualPayoutDate     string  `json:"actual_payout_date"`
	RequestedClosureDate string  `json:"requested_closure_date"`
	PrematureReason      string  `json:"premature_reason"`
	PrincipalExpected    float64 `json:"principal_expected"`
	InterestExpected     float64 `json:"interest_expected"`
	TDSExpected          float64 `json:"tds_expected"`
	NetExpected          float64 `json:"net_expected"`
	PrincipalReceived    float64 `json:"principal_received"`
	InterestReceived     float64 `json:"interest_received"`
	TDSDeducted          float64 `json:"tds_deducted"`
	NetAmountReceived    float64 `json:"net_amount_received"`
	VarianceType         string  `json:"variance_type"`
	ResolutionAction     string  `json:"resolution_action"`
	Remarks              string  `json:"remarks"`
	Reason               string  `json:"reason"`

	DaysHeld              int     `json:"days_held"`
	ContractedRate        float64 `json:"contracted_rate"`
	ApplicableRate        float64 `json:"applicable_rate"`
	PenaltyApplicable     *bool   `json:"penalty_applicable"`
	PenaltyID             string  `json:"penalty_id"`
	PenaltyType           string  `json:"penalty_type"`
	PenaltyValue          float64 `json:"penalty_value"`
	PenaltyAmount         float64 `json:"penalty_amount"`
	NoInterestFlag        bool    `json:"no_interest_flag"`
	RevisedInterestAmount float64 `json:"revised_interest_amount"`
	RevisedMaturityValue  float64 `json:"revised_maturity_value"`
	NetPayout             float64 `json:"net_payout"`

	RolloverAmountBasis    string  `json:"rollover_amount_basis"`
	ClosureAmount          float64 `json:"closure_amount"`
	NewBankID              string  `json:"new_bank_id"`
	NewBankName            string  `json:"new_bank_name"`
	NewAccountID           string  `json:"new_account_id"`
	NewFDAmount            float64 `json:"new_fd_amount"`
	NewTenorDays           int     `json:"new_tenor_days"`
	NewInterestRate        float64 `json:"new_interest_rate"`
	ExpectedStartDate      string  `json:"expected_start_date"`
	ExpectedMaturityDate   string  `json:"expected_maturity_date"`
	RateDetermination      string  `json:"rate_determination"`
	RolloverApprovalStatus string  `json:"rollover_approval_status"`
	NewFDReferenceNo       string  `json:"new_fd_reference_no"`
}

type cimplrClosureIDsRequest struct {
	UserID             string   `json:"user_id"`
	ClosureInitiateID  string   `json:"closure_initiate_id"`
	ClosureInitiateIDs []string `json:"closure_initiate_ids"`
	ClosureConfirmID   string   `json:"closure_confirm_id"`
	ClosureConfirmIDs  []string `json:"closure_confirm_ids"`
	Comment            string   `json:"comment"`
}

type cimplrClosureListRequest struct {
	Page        int    `json:"page"`
	PageSize    int    `json:"page_size"`
	Status      string `json:"status"`
	FDID        string `json:"fd_id"`
	EntityID    string `json:"entity_id"`
	ClosureType string `json:"closure_type"`
}

type cimplrClosureUploadRequest struct {
	UserID            string `json:"user_id"`
	ClosureInitiateID string `json:"closure_initiate_id"`
	ClosureConfirmID  string `json:"closure_confirm_id"`
	FileType          string `json:"file_type"`
	StoredFileName    string `json:"stored_file_name"`
	OriginalFileName  string `json:"original_file_name"`
	ContentType       string `json:"content_type"`
	FileSize          int64  `json:"file_size"`
	FileHash          string `json:"file_hash"`
	UploadS3Key       string `json:"upload_s3_key"`
	Reason            string `json:"reason"`
}

type cimplrFDSource struct {
	FDID             string
	BookingID        string
	ConfirmationID   string
	EntityID         string
	EntityName       string
	BankID           string
	BankName         string
	FDRefNo          string
	BankFDRefNo      string
	Principal        float64
	InterestRate     float64
	InterestTypeCode string
	MaturityDate     time.Time
	StartDate        time.Time
	TenureDays       int
	DayCountCode     string
	FrequencyID      string
	TDSPlanID        string
	BankConfigID     string
	SourceAccountID  string
}

type cimplrClosureCalc struct {
	ClosureType           string
	CalculationDate       time.Time
	AccruedDays           int
	AccruedInterest       float64
	TDSAmount             float64
	PenaltyID             string
	PenaltyType           string
	PenaltyValue          float64
	PenaltyAmount         float64
	ApplicableRate        float64
	NoInterestFlag        bool
	PenaltyApplicable     bool
	ExpectedMaturityValue float64
	RevisedInterestAmount float64
	RevisedMaturityValue  float64
	NetPayout             float64
}

func cimplrCalcToMap(c cimplrClosureCalc) map[string]interface{} {
	return map[string]interface{}{
		"closure_type":            c.ClosureType,
		"calculation_date":        c.CalculationDate,
		"accrued_days":            c.AccruedDays,
		"accrued_interest":        c.AccruedInterest,
		"tds_amount":              c.TDSAmount,
		"penalty_id":              c.PenaltyID,
		"penalty_type":            c.PenaltyType,
		"penalty_value":           c.PenaltyValue,
		"penalty_amount":          c.PenaltyAmount,
		"applicable_rate":         c.ApplicableRate,
		"no_interest_flag":        c.NoInterestFlag,
		"penalty_applicable":      c.PenaltyApplicable,
		"expected_maturity_value": c.ExpectedMaturityValue,
		"revised_interest_amount": c.RevisedInterestAmount,
		"revised_maturity_value":  c.RevisedMaturityValue,
		"net_payout":              c.NetPayout,
	}
}

func init() {
	// Register Initiate Hooks
	approvalengine.RegisterPostFinalizeHook(txClosureInitiatePayoutCreate, cimplrInitiatePostFinalizeHook)
	approvalengine.RegisterPostFinalizeHook(txClosureInitiatePayoutEdit, cimplrInitiatePostFinalizeHook)
	approvalengine.RegisterPostFinalizeHook(txClosureInitiatePayoutDelete, cimplrInitiatePostFinalizeHook)
	approvalengine.RegisterPostFinalizeHook(txClosureInitiateRolloverCreate, cimplrInitiatePostFinalizeHook)
	approvalengine.RegisterPostFinalizeHook(txClosureInitiateRolloverEdit, cimplrInitiatePostFinalizeHook)
	approvalengine.RegisterPostFinalizeHook(txClosureInitiateRolloverDelete, cimplrInitiatePostFinalizeHook)

	// Register Confirm Hooks
	approvalengine.RegisterPostFinalizeHook(txClosureConfirmPayoutCreate, cimplrConfirmPostFinalizeHook)
	approvalengine.RegisterPostFinalizeHook(txClosureConfirmPayoutEdit, cimplrConfirmPostFinalizeHook)
	approvalengine.RegisterPostFinalizeHook(txClosureConfirmPayoutDelete, cimplrConfirmPostFinalizeHook)
	approvalengine.RegisterPostFinalizeHook(txClosureConfirmRolloverCreate, cimplrConfirmPostFinalizeHook)
	approvalengine.RegisterPostFinalizeHook(txClosureConfirmRolloverEdit, cimplrConfirmPostFinalizeHook)
	approvalengine.RegisterPostFinalizeHook(txClosureConfirmRolloverDelete, cimplrConfirmPostFinalizeHook)
	
	// Register Premature Hooks
	approvalengine.RegisterPostFinalizeHook(txClosurePrematureCreate, cimplrConfirmPostFinalizeHook)
	approvalengine.RegisterPostFinalizeHook(txClosurePrematureEdit, cimplrConfirmPostFinalizeHook)
	approvalengine.RegisterPostFinalizeHook(txClosurePrematureDelete, cimplrConfirmPostFinalizeHook)
}

func CimplrInitiateCreate(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req cimplrClosureInitiateRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		userEmail := getUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		if strings.TrimSpace(req.UserID) == "" {
			req.UserID = userEmail
		}
		req.FDID = strings.TrimSpace(req.FDID)
		req.ClosureType = normalizeCimplrClosureType(req.ClosureType, req.ActionAtMaturity)
		if req.FDID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "fd_id is required")
			return
		}
		if !isValidCimplrClosureType(req.ClosureType) {
			api.RespondWithError(w, http.StatusBadRequest, "closure_type must be PAYOUT, ROLLOVER or PREMATURE")
			return
		}
		if req.ClosureType == "PREMATURE" {
			api.RespondWithError(w, http.StatusBadRequest, "PREMATURE is a direct one-step flow; use /investment/fd/closure/premature/create")
			return
		}

		ctx := r.Context()
		src, err := loadCimplrFDSource(ctx, pool, req.FDID)
		if err != nil {
			api.RespondWithError(w, http.StatusNotFound, constants.ErrFDNotFound)
			return
		}
		calc, err := calculateCimplrClosure(ctx, pool, src, req.ClosureType, cimplrDefaultCalcDate(src, req.ClosureType, req.RequestedClosureDate), false)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrClosureCalculationFailed+err.Error())
			return
		}

		principal := chooseFloat(req.PrincipalAmount, src.Principal)
		accrued := chooseFloat(req.AccruedInterestTillDate, calc.AccruedInterest)
		tds := chooseFloat(req.TDSExpected, calc.TDSAmount)
		expectedMaturity := chooseFloat(req.ExpectedMaturityValue, calc.ExpectedMaturityValue)
		netExpected := chooseFloat(req.NetExpectedAmount, calc.NetPayout)
		maturityStatus := firstNonEmpty(strings.ToUpper(strings.TrimSpace(req.MaturityStatus)), deriveCimplrMaturityStatus(src.MaturityDate))
		actionRequired := true
		if req.ActionRequired != nil {
			actionRequired = *req.ActionRequired
		}
		autoRenewal := false
		if req.AutoRenewalFlag != nil {
			autoRenewal = *req.AutoRenewalFlag
		}
		rolloverNewBankID, rolloverNewBankName := cimplrResolveInitiateRolloverBank(ctx, pool, req, src)

		tx, err := pool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxStartFailed)
			return
		}
		defer tx.Rollback(ctx) //nolint:errcheck

		var closureInitiateID string
		err = tx.QueryRow(ctx, `
			INSERT INTO cimplr.fd_closure_initiate (
				fd_id, booking_id, confirmation_id, entity_id, entity_name,
				bank_id, bank_name, fd_ref_no, bank_fd_ref_no,
				closure_type, action_at_maturity, maturity_date, requested_closure_date,
				principal_amount, interest_type_code, interest_rate, expected_maturity_value,
				accrued_interest_till_date, tds_expected, net_expected_amount,
				auto_renewal_flag, maturity_status, action_required,
				rollover_type, rollover_bank_type, rollover_new_bank_id, rollover_new_bank_name,
				tentative_new_tenor_days, remarks
			) VALUES (
				$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,NULLIF($11,''),$12,$13::date,
				$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,NULLIF($24,''),NULLIF($25,''),NULLIF($26,''),NULLIF($27,''),NULLIF($28,0),$29
			) RETURNING closure_initiate_id`,
			src.FDID, nullStrOrNil(src.BookingID), nullStrOrNil(src.ConfirmationID), nullStrOrNil(src.EntityID), nullStrOrNil(src.EntityName),
			nullStrOrNil(src.BankID), nullStrOrNil(src.BankName), nullStrOrNil(src.FDRefNo), nullStrOrNil(src.BankFDRefNo),
			req.ClosureType, strings.ToUpper(strings.TrimSpace(req.ActionAtMaturity)), src.MaturityDate, nullDateArg(req.RequestedClosureDate),
			principal, src.InterestTypeCode, src.InterestRate, expectedMaturity,
			accrued, tds, netExpected,
			autoRenewal, maturityStatus, actionRequired,
			strings.ToUpper(strings.TrimSpace(req.RolloverType)), strings.ToUpper(strings.TrimSpace(req.RolloverBankType)),
			nullStrOrNil(rolloverNewBankID), nullStrOrNil(rolloverNewBankName), req.TentativeNewTenorDays, req.Remarks,
		).Scan(&closureInitiateID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "initiate create failed: "+err.Error())
			return
		}

		if err := insertCimplrCalculation(ctx, tx, closureInitiateID, "", src, calc); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCalculationSnapshotFailed+err.Error())
			return
		}
		if err := insertCimplrInitiateAudit(ctx, tx, initiateAuditEntry{ID: closureInitiateID, Action: constants.AuditActionCreate, Status: constants.StatusPendingApproval, Reason: firstNonEmpty(req.Reason, "Create FD closure initiate"), RequestedBy: req.UserID, Old: nil}); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "initiate audit failed: "+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailed)
			return
		}

		varianceSummary, err := persistCimplrInitiateVariances(ctx, pool, closureInitiateID, req, src, calc)
		if err != nil {
			api.LogError("[CimplrFDClosure] initiate variance failed: %v", err)
		}
		instanceID, instErr := createCimplrApprovalInstance(ctx, pool, approvalInstanceRequest{TxType: getClosureTxCode("initiate", req.ClosureType, "CREATE"), Action: constants.AuditActionCreate, RecordID: closureInitiateID, RecordTable: constants.QuerryClosureInitiate, AuditTable: constants.QuerryAuditClosureInitiate, AuditIDColumn: "closure_initiate_id", EntityID: src.EntityID, Amount: principal, UserID: req.UserID, UserEmail: userEmail})
		if instErr != nil {
			api.LogError("[CimplrFDClosure] initiate approval create failed: %v", instErr)
		}
		if instanceID != "" {
			_, _ = pool.Exec(ctx, `UPDATE cimplr.fd_closure_initiate SET approval_instance_id=$1 WHERE closure_initiate_id=$2`, instanceID, closureInitiateID)
		}

		go func(id, cType, email string) {
			route := fmt.Sprintf("/investment/fd/closure/initiate/%s/create", strings.ToLower(cType))
			payload := fdNotifications.BuildCimplrClosureInitiateNotifPayload(context.Background(), pool, []string{id}, "CREATE", email).ToMap()
			notifcatalog.TriggerNotification(context.Background(), pool, route, id, payload)
		}(closureInitiateID, req.ClosureType, userEmail)

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"closure_initiate_id":  closureInitiateID,
			"approval_instance_id": instanceID,
			"calculation":          cimplrCalcToMap(calc),
			"variance":             varianceSummary,
		})
	}
}

func CimplrInitiateValidate(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req cimplrClosureInitiateRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		userEmail := getUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		if req.UserID == "" {
			req.UserID = userEmail
		}
		req.FDID = strings.TrimSpace(req.FDID)
		if req.FDID == "" && req.ClosureInitiateID != "" {
			old, err := loadCimplrInitiateOld(r.Context(), pool, req.ClosureInitiateID)
			if err == nil {
				req.FDID = fmt.Sprint(old["fd_id"])
				if req.ClosureType == "" {
					req.ClosureType = fmt.Sprint(old["closure_type"])
				}
			}
		}
		if req.FDID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "fd_id or closure_initiate_id is required")
			return
		}
		req.ClosureType = normalizeCimplrClosureType(req.ClosureType, req.ActionAtMaturity)
		if !isValidCimplrClosureType(req.ClosureType) {
			api.RespondWithError(w, http.StatusBadRequest, "closure_type must be PAYOUT, ROLLOVER or PREMATURE")
			return
		}
		if req.ClosureType == "PREMATURE" {
			api.RespondWithError(w, http.StatusBadRequest, "PREMATURE is a direct one-step flow; use /investment/fd/closure/premature/validate")
			return
		}
		src, err := loadCimplrFDSource(r.Context(), pool, req.FDID)
		if err != nil {
			api.RespondWithError(w, http.StatusNotFound, constants.ErrFDNotFound)
			return
		}
		calc, err := calculateCimplrClosure(r.Context(), pool, src, req.ClosureType, cimplrDefaultCalcDate(src, req.ClosureType, req.RequestedClosureDate), false)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrClosureCalculationFailed+err.Error())
			return
		}
		recordID := firstNonEmpty(req.ClosureInitiateID, constants.PreviewPrefix+varianceengine.NewRunID())
		summary := previewCimplrInitiateVariance(recordID, req, src, calc)
		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"fd_source":         src,
			"calculation":       cimplrCalcToMap(calc),
			"initiate_prefill":  buildCimplrInitiatePrefill(src, calc, req.ClosureType),
			"variance":          summary,
			"can_create":        true,
			"required_next_api": "/investment/fd/closure/initiate/create",
		})
	}
}

func CimplrInitiateEdit(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req cimplrClosureInitiateRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		userEmail := getUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		if req.UserID == "" {
			req.UserID = userEmail
		}
		req.ClosureInitiateID = strings.TrimSpace(req.ClosureInitiateID)
		if req.ClosureInitiateID == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrClosureInitiateIDRequired)
			return
		}

		ctx := r.Context()
		oldRow, err := loadCimplrInitiateOld(ctx, pool, req.ClosureInitiateID)
		if err != nil {
			api.RespondWithError(w, http.StatusNotFound, constants.ErrClosureInitiateRecordNotFound)
			return
		}
		if fmt.Sprint(oldRow["closure_status"]) == "CONFIRM" {
			api.RespondWithError(w, http.StatusBadRequest, "approved initiate records cannot be edited; create confirm instead")
			return
		}
		src, err := loadCimplrFDSource(ctx, pool, fmt.Sprint(oldRow["fd_id"]))
		if err != nil {
			api.RespondWithError(w, http.StatusNotFound, constants.ErrFDNotFound)
			return
		}
		if req.ClosureType == "" {
			req.ClosureType = fmt.Sprint(oldRow["closure_type"])
		}
		req.ClosureType = normalizeCimplrClosureType(req.ClosureType, req.ActionAtMaturity)
		oldClosureType := strings.ToUpper(strings.TrimSpace(fmt.Sprint(oldRow["closure_type"])))
		if oldClosureType != "" && oldClosureType != req.ClosureType {
			api.RespondWithError(w, http.StatusBadRequest,
				"closure type cannot be changed after initiate; use PREMATURE closure for early exit")
			return
		}
		calc, err := calculateCimplrClosure(ctx, pool, src, req.ClosureType, cimplrDefaultCalcDate(src, req.ClosureType, req.RequestedClosureDate), false)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrClosureCalculationFailed+err.Error())
			return
		}

		principal := chooseFloat(req.PrincipalAmount, src.Principal)
		accrued := chooseFloat(req.AccruedInterestTillDate, calc.AccruedInterest)
		tds := chooseFloat(req.TDSExpected, calc.TDSAmount)
		expectedMaturity := chooseFloat(req.ExpectedMaturityValue, calc.ExpectedMaturityValue)
		netExpected := chooseFloat(req.NetExpectedAmount, calc.NetPayout)
		rolloverNewBankID, rolloverNewBankName := cimplrResolveInitiateRolloverBank(ctx, pool, req, src)

		tx, err := pool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxStartFailed)
			return
		}
		defer tx.Rollback(ctx) //nolint:errcheck
		_, err = tx.Exec(ctx, `
			UPDATE cimplr.fd_closure_initiate
			SET closure_type=$1, action_at_maturity=NULLIF($2,''), requested_closure_date=$3::date,
			    principal_amount=$4, interest_type_code=$5, interest_rate=$6,
			    expected_maturity_value=$7, accrued_interest_till_date=$8, tds_expected=$9,
			    net_expected_amount=$10, maturity_status=$11, rollover_type=NULLIF($12,''),
			    rollover_bank_type=NULLIF($13,''), rollover_new_bank_id=NULLIF($14,''),
			    rollover_new_bank_name=NULLIF($15,''), tentative_new_tenor_days=NULLIF($16,0), remarks=$17
			WHERE closure_initiate_id=$18 AND is_deleted=false`,
			req.ClosureType, strings.ToUpper(strings.TrimSpace(req.ActionAtMaturity)), nullDateArg(req.RequestedClosureDate),
			principal, src.InterestTypeCode, src.InterestRate, expectedMaturity, accrued, tds, netExpected,
			firstNonEmpty(strings.ToUpper(strings.TrimSpace(req.MaturityStatus)), deriveCimplrMaturityStatus(src.MaturityDate)),
			strings.ToUpper(strings.TrimSpace(req.RolloverType)), strings.ToUpper(strings.TrimSpace(req.RolloverBankType)),
			nullStrOrNil(rolloverNewBankID), nullStrOrNil(rolloverNewBankName), req.TentativeNewTenorDays, req.Remarks,
			req.ClosureInitiateID,
		)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "initiate edit failed: "+err.Error())
			return
		}
		if err := insertCimplrCalculation(ctx, tx, req.ClosureInitiateID, "", src, calc); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCalculationSnapshotFailed+err.Error())
			return
		}
		if err := insertCimplrInitiateAudit(ctx, tx, initiateAuditEntry{ID: req.ClosureInitiateID, Action: constants.AuditActionEdit, Status: constants.StatusPendingEditApproval, Reason: firstNonEmpty(req.Reason, "Edit FD closure initiate"), RequestedBy: req.UserID, Old: oldRow}); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "initiate audit failed: "+err.Error())
			return
		}
		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailed)
			return
		}

		varianceSummary, _ := persistCimplrInitiateVariances(ctx, pool, req.ClosureInitiateID, req, src, calc)
		_ = approvalengine.CancelPendingInstances(ctx, pool, cimplrClosureModule, req.ClosureInitiateID, userEmail)
		instanceID, instErr := createCimplrApprovalInstance(ctx, pool, approvalInstanceRequest{TxType: getClosureTxCode("initiate", req.ClosureType, "EDIT"), Action: constants.AuditActionEdit, RecordID: req.ClosureInitiateID, RecordTable: constants.QuerryClosureInitiate, AuditTable: constants.QuerryAuditClosureInitiate, AuditIDColumn: "closure_initiate_id", EntityID: src.EntityID, Amount: principal, UserID: req.UserID, UserEmail: userEmail})
		if instErr != nil {
			api.LogError("[CimplrFDClosure] initiate edit approval failed: %v", instErr)
		}
		if instanceID != "" {
			_, _ = pool.Exec(ctx, `UPDATE cimplr.fd_closure_initiate SET approval_instance_id=$1 WHERE closure_initiate_id=$2`, instanceID, req.ClosureInitiateID)
		}

		go triggerClosureBulkNotif(context.Background(), pool, []string{req.ClosureInitiateID}, "cimplr.fd_closure_initiate", "initiate", "edit", userEmail)
		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"closure_initiate_id":  req.ClosureInitiateID,
			"approval_instance_id": instanceID,
			"variance":             varianceSummary,
		})
	}
}

func CimplrInitiateDelete(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req cimplrClosureIDsRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		userEmail := getUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		if req.UserID == "" {
			req.UserID = userEmail
		}
		ids := normalizeCimplrIDs(req.ClosureInitiateID, req.ClosureInitiateIDs)
		if len(ids) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrClosureInitiateIDRequired)
			return
		}
		results := make([]map[string]interface{}, 0, len(ids))
		for _, id := range ids {
			res := map[string]interface{}{"closure_initiate_id": id}
			oldRow, err := loadCimplrInitiateOld(r.Context(), pool, id)
			if err != nil {
				res["success"] = false
				res["error"] = "record not found"
				results = append(results, res)
				continue
			}
			if strings.ToUpper(fmt.Sprint(oldRow["closure_status"])) == "CONFIRM" {
				var confirmCount int
				_ = pool.QueryRow(r.Context(), `SELECT COUNT(*) FROM cimplr.fd_closure_confirm WHERE closure_initiate_id=$1 AND is_deleted=false`, id).Scan(&confirmCount)
				if confirmCount > 0 {
					res["success"] = false
					res["error"] = "cannot delete approved initiate after confirmation exists; delete confirmation first"
				} else {
					res["success"] = false
					res["error"] = "cannot delete approved initiate record"
				}
				results = append(results, res)
				continue
			}
			if err := insertCimplrInitiateAudit(r.Context(), pool, initiateAuditEntry{ID: id, Action: constants.AuditActionDelete, Status: constants.StatusPendingDeleteApproval, Reason: firstNonEmpty(req.Comment, "Delete FD closure initiate"), RequestedBy: req.UserID, Old: oldRow}); err != nil {
				res["success"] = false
				res["error"] = err.Error()
				results = append(results, res)
				continue
			}
			instanceID, _ := createCimplrApprovalInstance(r.Context(), pool, approvalInstanceRequest{TxType: getClosureTxCode("initiate", fmt.Sprint(oldRow["closure_type"]), "DELETE"), Action: constants.AuditActionDelete, RecordID: id, RecordTable: constants.QuerryClosureInitiate, AuditTable: constants.QuerryAuditClosureInitiate, AuditIDColumn: "closure_initiate_id", EntityID: fmt.Sprint(oldRow["entity_id"]), Amount: 0, UserID: req.UserID, UserEmail: userEmail})
			if instanceID != "" {
				_, _ = pool.Exec(r.Context(), `UPDATE cimplr.fd_closure_initiate SET approval_instance_id=$1 WHERE closure_initiate_id=$2`, instanceID, id)
			}
			res["success"] = true
			res["approval_instance_id"] = instanceID
			results = append(results, res)
		}
		ok, errStr := summarizeCimplrBatchResults(results)
		go triggerClosureBulkNotif(context.Background(), pool, ids, "cimplr.fd_closure_initiate", "initiate", "delete", userEmail)
		api.RespondWithPayload(w, ok, errStr, map[string]interface{}{"results": results})
	}
}

func CimplrInitiateApprove(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req cimplrClosureIDsRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		userEmail := getUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		if req.UserID == "" {
			req.UserID = userEmail
		}
		ids := normalizeCimplrIDs(req.ClosureInitiateID, req.ClosureInitiateIDs)
		if len(ids) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrClosureInitiateIDRequired)
			return
		}
		results := cimplrApproveInitiates(r.Context(), pool, ids, req.UserID, userEmail, req.Comment)
		ok, errStr := summarizeCimplrBatchResults(results)
		go triggerClosureBulkNotif(context.Background(), pool, ids, "cimplr.fd_closure_initiate", "initiate", "approve", userEmail)
		api.RespondWithPayload(w, ok, errStr, map[string]interface{}{"results": results})
	}
}

func CimplrInitiateReject(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req cimplrClosureIDsRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		userEmail := getUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		if req.UserID == "" {
			req.UserID = userEmail
		}
		ids := normalizeCimplrIDs(req.ClosureInitiateID, req.ClosureInitiateIDs)
		if len(ids) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrClosureInitiateIDRequired)
			return
		}
		results := cimplrRejectInitiates(r.Context(), pool, ids, req.UserID, userEmail, req.Comment)
		ok, errStr := summarizeCimplrBatchResults(results)
		go triggerClosureBulkNotif(context.Background(), pool, ids, "cimplr.fd_closure_initiate", "initiate", "reject", userEmail)
		api.RespondWithPayload(w, ok, errStr, map[string]interface{}{"results": results})
	}
}

func CimplrInitiateAll(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req cimplrClosureListRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		rows, total, err := listCimplrRecords(r.Context(), pool, "initiate", req, false)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to fetch initiate records: "+err.Error())
			return
		}
		api.RespondWithPayload(w, true, "", map[string]interface{}{"records": rows, "total": total})
	}
}

func CimplrInitiateApprovedActive(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req cimplrClosureListRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		rows, total, err := listCimplrRecords(r.Context(), pool, "initiate", req, true)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to fetch approved-active initiate records: "+err.Error())
			return
		}
		rows = enrichCimplrInitiateListFromCalculation(r.Context(), pool, rows, true)
		api.RespondWithPayload(w, true, "", map[string]interface{}{"records": rows, "total": total})
	}
}

func CimplrConfirmCreate(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req cimplrClosureConfirmRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		userEmail := getUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		if req.UserID == "" {
			req.UserID = userEmail
		}
		if strings.TrimSpace(req.ClosureInitiateID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrClosureInitiateIDRequired)
			return
		}

		ctx := r.Context()
		initiate, err := loadCimplrInitiateOld(ctx, pool, req.ClosureInitiateID)
		if err != nil {
			api.RespondWithError(w, http.StatusNotFound, "approved initiate record not found")
			return
		}
		if fmt.Sprint(initiate["closure_status"]) != "CONFIRM" {
			api.RespondWithError(w, http.StatusBadRequest, "initiate record must be approved before confirm")
			return
		}
		src, err := loadCimplrFDSource(ctx, pool, fmt.Sprint(initiate["fd_id"]))
		if err != nil {
			api.RespondWithError(w, http.StatusNotFound, constants.ErrFDNotFound)
			return
		}
		closureType := fmt.Sprint(initiate["closure_type"])
		enforceMaturity := closureType == "PAYOUT" || closureType == "ROLLOVER"
		calc, err := calculateCimplrClosure(ctx, pool, src, closureType, firstNonEmpty(req.RequestedClosureDate, fmt.Sprint(initiate["requested_closure_date"])), enforceMaturity)
		if err != nil {
			if enforceMaturity {
				api.RespondWithError(w, http.StatusBadRequest, err.Error())
				return
			}
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrClosureCalculationFailed+err.Error())
			return
		}

		// Interest baseline mirrors calc.AccruedInterest for PAYOUT/ROLLOVER and
		// calc.RevisedInterestAmount for PREMATURE. The variance rule builder
		// applies the same branching so the figures stay in sync end-to-end.
		interestBaseline := calc.AccruedInterest
		if closureType == "PREMATURE" {
			interestBaseline = calc.RevisedInterestAmount
		}
		principalExpected := chooseFloat(req.PrincipalExpected, src.Principal)
		interestExpected := chooseFloat(req.InterestExpected, interestBaseline)
		tdsExpected := chooseFloat(req.TDSExpected, calc.TDSAmount)
		netExpected := chooseFloat(req.NetExpected, calc.NetPayout)
		principalReceived := chooseFloat(req.PrincipalReceived, principalExpected)
		interestReceived := chooseFloat(req.InterestReceived, interestExpected)
		tdsDeducted := chooseFloat(req.TDSDeducted, tdsExpected)
		netReceived := chooseFloat(req.NetAmountReceived, netExpected)

		// Pre-commit variance gate (same rationale as CimplrPrematureCreate).
		varianceSummary, blockReason := cimplrAssertConfirmCreateAllowed(req, src, calc, firstNonEmpty(req.ClosureConfirmID, "PRE-CREATE-"+varianceengine.NewRunID()))
		if blockReason != "" {
			api.RespondWithPayload(w, false, blockReason, map[string]interface{}{
				"blocked":  true,
				"variance": varianceSummary,
			})
			return
		}

		tx, err := pool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxStartFailed)
			return
		}
		defer tx.Rollback(ctx) //nolint:errcheck

		var closureConfirmID string
		err = tx.QueryRow(ctx, `
			INSERT INTO cimplr.fd_closure_confirm (
				closure_initiate_id, fd_id, booking_id, confirmation_id, entity_id, entity_name,
				bank_id, bank_name, fd_ref_no, bank_fd_ref_no, closure_type,
				confirmation_mode, bank_reference_no, actual_payout_date, requested_closure_date,
				premature_reason, principal_expected, interest_expected, tds_expected, net_expected,
				principal_received, interest_received, tds_deducted, net_amount_received,
				variance_type, resolution_action, remarks
			) VALUES (
				$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,NULLIF($12,''),NULLIF($13,''),$14::date,$15::date,
				NULLIF($16,''),$17,$18,$19,$20,$21,$22,$23,$24,NULLIF($25,''),NULLIF($26,''),$27
			) RETURNING closure_confirm_id`,
			req.ClosureInitiateID, src.FDID, nullStrOrNil(src.BookingID), nullStrOrNil(src.ConfirmationID), nullStrOrNil(src.EntityID), nullStrOrNil(src.EntityName),
			nullStrOrNil(src.BankID), nullStrOrNil(src.BankName), nullStrOrNil(src.FDRefNo), nullStrOrNil(src.BankFDRefNo), closureType,
			strings.ToUpper(strings.TrimSpace(req.ConfirmationMode)), req.BankReferenceNo, nullDateArg(req.ActualPayoutDate), nullDateArg(req.RequestedClosureDate),
			req.PrematureReason, principalExpected, interestExpected, tdsExpected, netExpected,
			principalReceived, interestReceived, tdsDeducted, netReceived,
			strings.ToUpper(strings.TrimSpace(req.VarianceType)), strings.ToUpper(strings.TrimSpace(req.ResolutionAction)), req.Remarks,
		).Scan(&closureConfirmID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "confirm create failed: "+err.Error())
			return
		}
		if closureType == "PREMATURE" {
			if err := upsertCimplrPrematureConfirm(ctx, tx, closureConfirmID, src, req, calc); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "premature confirm detail failed: "+err.Error())
				return
			}
		}
		if closureType == "ROLLOVER" {
			if err := upsertCimplrRolloverConfirm(ctx, tx, closureConfirmID, src, req, calc); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "rollover confirm detail failed: "+err.Error())
				return
			}
		}
		if err := insertCimplrCalculation(ctx, tx, req.ClosureInitiateID, closureConfirmID, src, calc); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCalculationSnapshotFailed+err.Error())
			return
		}
		if err := insertCimplrConfirmAudit(ctx, tx, confirmAuditEntry{ConfirmID: closureConfirmID, InitiateID: req.ClosureInitiateID, Action: constants.AuditActionCreate, Status: constants.StatusPendingApproval, Reason: firstNonEmpty(req.Reason, "Create FD closure confirm"), RequestedBy: req.UserID, Old: nil}); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "confirm audit failed: "+err.Error())
			return
		}
		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailed)
			return
		}

		if persisted, perr := persistCimplrConfirmVariances(ctx, pool, closureConfirmID, req, src, calc); perr == nil {
			varianceSummary = persisted
		}
		instanceID, instErr := createCimplrApprovalInstance(ctx, pool, approvalInstanceRequest{TxType: getClosureTxCode(func() string{if closureType=="PREMATURE"{return "premature"}; return "confirm"}(), closureType, "CREATE"), Action: constants.AuditActionCreate, RecordID: closureConfirmID, RecordTable: constants.QuerryAuditClosureConfirm, AuditTable: constants.QuerryAuditClosureConfirmAudit, AuditIDColumn: "closure_confirm_id", EntityID: src.EntityID, Amount: principalExpected, UserID: req.UserID, UserEmail: userEmail})
		if instErr != nil {
			api.LogError("[CimplrFDClosure] confirm approval create failed: %v", instErr)
		}
		if instanceID != "" {
			_, _ = pool.Exec(ctx, `UPDATE cimplr.fd_closure_confirm SET approval_instance_id=$1 WHERE closure_confirm_id=$2`, instanceID, closureConfirmID)
		}

		go func(id, cType, email string) {
			route := fmt.Sprintf("/investment/fd/closure/confirm/%s/create", strings.ToLower(cType))
			payload := fdNotifications.BuildCimplrClosureConfirmNotifPayload(context.Background(), pool, []string{id}, "CREATE", email).ToMap()
			notifcatalog.TriggerNotification(context.Background(), pool, route, id, payload)
		}(closureConfirmID, closureType, userEmail)

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"closure_confirm_id":   closureConfirmID,
			"approval_instance_id": instanceID,
			"calculation":          cimplrCalcToMap(calc),
			"variance":             varianceSummary,
		})
	}
}

func CimplrPrematureValidate(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req cimplrClosureConfirmRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		userEmail := getUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		if req.UserID == "" {
			req.UserID = userEmail
		}
		req.FDID = strings.TrimSpace(req.FDID)
		if req.FDID == "" && req.ClosureConfirmID != "" {
			old, err := loadCimplrConfirmOld(r.Context(), pool, req.ClosureConfirmID)
			if err == nil {
				req.FDID = fmt.Sprint(old["fd_id"])
			}
		}
		if req.FDID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "fd_id or closure_confirm_id is required")
			return
		}
		src, err := loadCimplrFDSource(r.Context(), pool, req.FDID)
		if err != nil {
			api.RespondWithError(w, http.StatusNotFound, constants.ErrFDNotFound)
			return
		}
		calc, err := calculateCimplrClosure(r.Context(), pool, src, "PREMATURE", req.RequestedClosureDate, false)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "premature calculation failed: "+err.Error())
			return
		}
		recordID := firstNonEmpty(req.ClosureConfirmID, constants.PreviewPrefix+varianceengine.NewRunID())
		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"fd_source":         src,
			"calculation":       cimplrCalcToMap(calc),
			"premature_prefill": buildCimplrConfirmPrefill(src, calc, "PREMATURE"),
			"variance":          previewCimplrConfirmVariance(recordID, req, src, calc),
			"can_create":        true,
			"required_next_api": "/investment/fd/closure/premature/create",
		})
	}
}

func CimplrPrematureCreate(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req cimplrClosureConfirmRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		userEmail := getUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		if req.UserID == "" {
			req.UserID = userEmail
		}
		req.FDID = strings.TrimSpace(req.FDID)
		if req.FDID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "fd_id is required")
			return
		}
		if strings.TrimSpace(req.PrematureReason) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "premature_reason is required")
			return
		}

		ctx := r.Context()
		src, err := loadCimplrFDSource(ctx, pool, req.FDID)
		if err != nil {
			api.RespondWithError(w, http.StatusNotFound, constants.ErrFDNotFound)
			return
		}
		calc, err := calculateCimplrClosure(ctx, pool, src, "PREMATURE", req.RequestedClosureDate, false)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "premature calculation failed: "+err.Error())
			return
		}
		principalExpected := chooseFloat(req.PrincipalExpected, src.Principal)
		interestExpected := chooseFloat(req.InterestExpected, calc.RevisedInterestAmount)
		tdsExpected := chooseFloat(req.TDSExpected, calc.TDSAmount)
		netExpected := chooseFloat(req.NetExpected, calc.NetPayout)
		principalReceived := chooseFloat(req.PrincipalReceived, principalExpected)
		interestReceived := chooseFloat(req.InterestReceived, interestExpected)
		tdsDeducted := chooseFloat(req.TDSDeducted, tdsExpected)
		netReceived := chooseFloat(req.NetAmountReceived, netExpected)

		// Pre-commit variance gate — block creation if user-entered values
		// disagree with the calculator and the user has not explicitly accepted
		// the discrepancy. Stops zombie rows from entering the approval queue.
		varianceSummary, blockReason := cimplrAssertConfirmCreateAllowed(req, src, calc, firstNonEmpty(req.ClosureConfirmID, "PRE-CREATE-"+varianceengine.NewRunID()))
		if blockReason != "" {
			api.RespondWithPayload(w, false, blockReason, map[string]interface{}{
				"blocked":  true,
				"variance": varianceSummary,
			})
			return
		}

		tx, err := pool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxStartFailed)
			return
		}
		defer tx.Rollback(ctx) //nolint:errcheck

		closureInitiateID, err := ensureCimplrPrematureInitiate(ctx, tx, "", "", src, req, calc)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "premature initiate failed: "+err.Error())
			return
		}

		var closureConfirmID string
		err = tx.QueryRow(ctx, `
			INSERT INTO cimplr.fd_closure_confirm (
				closure_initiate_id, fd_id, booking_id, confirmation_id, entity_id, entity_name,
				bank_id, bank_name, fd_ref_no, bank_fd_ref_no, closure_type,
				confirmation_mode, bank_reference_no, actual_payout_date, requested_closure_date,
				premature_reason, principal_expected, interest_expected, tds_expected, net_expected,
				principal_received, interest_received, tds_deducted, net_amount_received,
				variance_type, resolution_action, remarks
			) VALUES (
				$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'PREMATURE',NULLIF($11,''),NULLIF($12,''),$13::date,$14::date,
				$15,$16,$17,$18,$19,$20,$21,$22,$23,NULLIF($24,''),NULLIF($25,''),$26
			) RETURNING closure_confirm_id`,
			closureInitiateID, src.FDID, nullStrOrNil(src.BookingID), nullStrOrNil(src.ConfirmationID), nullStrOrNil(src.EntityID), nullStrOrNil(src.EntityName),
			nullStrOrNil(src.BankID), nullStrOrNil(src.BankName), nullStrOrNil(src.FDRefNo), nullStrOrNil(src.BankFDRefNo),
			strings.ToUpper(strings.TrimSpace(req.ConfirmationMode)), req.BankReferenceNo, nullDateArg(req.ActualPayoutDate), nullDateArg(req.RequestedClosureDate),
			req.PrematureReason, principalExpected, interestExpected, tdsExpected, netExpected,
			principalReceived, interestReceived, tdsDeducted, netReceived,
			strings.ToUpper(strings.TrimSpace(req.VarianceType)), strings.ToUpper(strings.TrimSpace(req.ResolutionAction)), req.Remarks,
		).Scan(&closureConfirmID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "premature create failed: "+err.Error())
			return
		}
		if err := upsertCimplrPrematureConfirm(ctx, tx, closureConfirmID, src, req, calc); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "premature detail failed: "+err.Error())
			return
		}
		if err := insertCimplrCalculation(ctx, tx, closureInitiateID, closureConfirmID, src, calc); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCalculationSnapshotFailed+err.Error())
			return
		}
		if err := insertCimplrConfirmAudit(ctx, tx, confirmAuditEntry{ConfirmID: closureConfirmID, InitiateID: closureInitiateID, Action: constants.AuditActionCreate, Status: constants.StatusPendingApproval, Reason: firstNonEmpty(req.Reason, "Create premature closure"), RequestedBy: req.UserID, Old: nil}); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "premature audit failed: "+err.Error())
			return
		}
		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailed)
			return
		}

		// Reuse varianceSummary from the gate; persist (no-op vs preview because
		// the gate already confirmed open_count==0 or resolution_action='ACCEPT')
		// still writes the run + auto-resolves any previously-OPEN variance rows.
		if persisted, perr := persistCimplrConfirmVariances(ctx, pool, closureConfirmID, req, src, calc); perr == nil {
			varianceSummary = persisted
		}
		instanceID, instErr := createCimplrApprovalInstance(ctx, pool, approvalInstanceRequest{TxType: getClosureTxCode(func() string{if "PREMATURE"=="PREMATURE"{return "premature"}; return "confirm"}(), "PREMATURE", "CREATE"), Action: constants.AuditActionCreate, RecordID: closureConfirmID, RecordTable: constants.QuerryAuditClosureConfirm, AuditTable: constants.QuerryAuditClosureConfirmAudit, AuditIDColumn: "closure_confirm_id", EntityID: src.EntityID, Amount: principalExpected, UserID: req.UserID, UserEmail: userEmail})
		if instErr != nil {
			api.LogError("[CimplrFDClosure] premature approval create failed: %v", instErr)
		}
		if instanceID != "" {
			_, _ = pool.Exec(ctx, `UPDATE cimplr.fd_closure_confirm SET approval_instance_id=$1 WHERE closure_confirm_id=$2`, instanceID, closureConfirmID)
		}

		go func(id, email string) {
			route := "/investment/fd/closure/premature/create"
			payload := fdNotifications.BuildCimplrClosureConfirmNotifPayload(context.Background(), pool, []string{id}, "CREATE", email).ToMap()
			notifcatalog.TriggerNotification(context.Background(), pool, route, id, payload)
		}(closureConfirmID, userEmail)

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"closure_initiate_id":  closureInitiateID,
			"closure_confirm_id":   closureConfirmID,
			"approval_instance_id": instanceID,
			"calculation":          cimplrCalcToMap(calc),
			"variance":             varianceSummary,
		})
	}
}

func CimplrConfirmValidate(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req cimplrClosureConfirmRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		userEmail := getUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		if req.UserID == "" {
			req.UserID = userEmail
		}
		if req.ClosureConfirmID != "" && req.ClosureInitiateID == "" {
			old, err := loadCimplrConfirmOld(r.Context(), pool, req.ClosureConfirmID)
			if err == nil {
				req.ClosureInitiateID = fmt.Sprint(old["closure_initiate_id"])
			}
		}
		if req.ClosureInitiateID == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ClosureIDsRequired)
			return
		}
		initiate, err := loadCimplrInitiateOld(r.Context(), pool, req.ClosureInitiateID)
		if err != nil {
			api.RespondWithError(w, http.StatusNotFound, constants.ErrClosureInitiateRecordNotFound)
			return
		}
		src, err := loadCimplrFDSource(r.Context(), pool, fmt.Sprint(initiate["fd_id"]))
		if err != nil {
			api.RespondWithError(w, http.StatusNotFound, constants.ErrFDNotFound)
			return
		}
		closureType := fmt.Sprint(initiate["closure_type"])
		calcDate := cimplrDefaultCalcDate(src, closureType, firstNonEmpty(req.RequestedClosureDate, fmt.Sprint(initiate["requested_closure_date"])))
		calc, err := calculateCimplrClosure(r.Context(), pool, src, closureType, calcDate, false)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrClosureCalculationFailed+err.Error())
			return
		}
		recordID := firstNonEmpty(req.ClosureConfirmID, constants.PreviewPrefix+varianceengine.NewRunID())
		summary := previewCimplrConfirmVariance(recordID, req, src, calc)
		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"fd_source":         src,
			"initiate":          initiate,
			"calculation":       cimplrCalcToMap(calc),
			"confirm_prefill":   buildCimplrConfirmPrefill(src, calc, closureType),
			"variance":          summary,
			"can_create":        fmt.Sprint(initiate["closure_status"]) == "CONFIRM",
			"required_next_api": "/investment/fd/closure/confirm/create",
		})
	}
}

func CimplrConfirmEdit(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req cimplrClosureConfirmRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		userEmail := getUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		if req.UserID == "" {
			req.UserID = userEmail
		}
		if req.ClosureConfirmID == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ClosureIDsRequired)
			return
		}
		ctx := r.Context()
		oldRow, err := loadCimplrConfirmOld(ctx, pool, req.ClosureConfirmID)
		if err != nil {
			api.RespondWithError(w, http.StatusNotFound, constants.ConfirmRecordNotFound)
			return
		}
		if fmt.Sprint(oldRow["closure_status"]) == "POSTED" || oldRow["accounting_posted"] == true {
			api.RespondWithError(w, http.StatusBadRequest, "posted confirm records cannot be edited")
			return
		}
		src, err := loadCimplrFDSource(ctx, pool, fmt.Sprint(oldRow["fd_id"]))
		if err != nil {
			api.RespondWithError(w, http.StatusNotFound, constants.ErrFDNotFound)
			return
		}
		closureType := fmt.Sprint(oldRow["closure_type"])
		calc, err := calculateCimplrClosure(ctx, pool, src, closureType, firstNonEmpty(req.RequestedClosureDate, fmt.Sprint(oldRow["requested_closure_date"])), false)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrClosureCalculationFailed+err.Error())
			return
		}
		// See CimplrConfirmCreate — same baseline branching.
		interestBaseline := calc.AccruedInterest
		if closureType == "PREMATURE" {
			interestBaseline = calc.RevisedInterestAmount
		}
		principalExpected := chooseFloat(req.PrincipalExpected, src.Principal)
		interestExpected := chooseFloat(req.InterestExpected, interestBaseline)
		tdsExpected := chooseFloat(req.TDSExpected, calc.TDSAmount)
		netExpected := chooseFloat(req.NetExpected, calc.NetPayout)
		principalReceived := chooseFloat(req.PrincipalReceived, principalExpected)
		interestReceived := chooseFloat(req.InterestReceived, interestExpected)
		tdsDeducted := chooseFloat(req.TDSDeducted, tdsExpected)
		netReceived := chooseFloat(req.NetAmountReceived, netExpected)

		// Pre-commit variance gate — same rules as create. An edit that
		// re-introduces unresolved variance without ACCEPT is refused so the
		// approval queue stays clean.
		varianceSummary, blockReason := cimplrAssertConfirmCreateAllowed(req, src, calc, req.ClosureConfirmID)
		if blockReason != "" {
			go triggerClosureBulkNotif(context.Background(), pool, []string{req.ClosureConfirmID}, "cimplr.fd_closure_confirm", "confirm", "edit", userEmail)
			api.RespondWithPayload(w, false, blockReason, map[string]interface{}{
				"blocked":  true,
				"variance": varianceSummary,
			})
			return
		}

		tx, err := pool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxStartFailed)
			return
		}
		defer tx.Rollback(ctx) //nolint:errcheck

		closureInitiateID := cimplrMapString(oldRow, "closure_initiate_id")
		if closureType == "PREMATURE" {
			closureInitiateID, err = ensureCimplrPrematureInitiate(ctx, tx, closureInitiateID, req.ClosureConfirmID, src, req, calc)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "premature initiate failed: "+err.Error())
				return
			}
		}
		if closureInitiateID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "closure_initiate_id is missing for this confirm record")
			return
		}

		_, err = tx.Exec(ctx, `
			UPDATE cimplr.fd_closure_confirm
			SET confirmation_mode=NULLIF($1,''), bank_reference_no=NULLIF($2,''),
			    actual_payout_date=$3::date, requested_closure_date=$4::date,
			    premature_reason=NULLIF($5,''), principal_expected=$6, interest_expected=$7,
			    tds_expected=$8, net_expected=$9, principal_received=$10,
			    interest_received=$11, tds_deducted=$12, net_amount_received=$13,
			    variance_type=NULLIF($14,''), resolution_action=NULLIF($15,''), remarks=$16
			WHERE closure_confirm_id=$17 AND is_deleted=false`,
			strings.ToUpper(strings.TrimSpace(req.ConfirmationMode)), req.BankReferenceNo, nullDateArg(req.ActualPayoutDate), nullDateArg(req.RequestedClosureDate),
			req.PrematureReason, principalExpected, interestExpected, tdsExpected, netExpected, principalReceived, interestReceived, tdsDeducted, netReceived,
			strings.ToUpper(strings.TrimSpace(req.VarianceType)), strings.ToUpper(strings.TrimSpace(req.ResolutionAction)), req.Remarks, req.ClosureConfirmID,
		)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "confirm edit failed: "+err.Error())
			return
		}
		if closureType == "PREMATURE" {
			if err := upsertCimplrPrematureConfirm(ctx, tx, req.ClosureConfirmID, src, req, calc); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "premature confirm detail failed: "+err.Error())
				return
			}
		}
		if closureType == "ROLLOVER" {
			if err := upsertCimplrRolloverConfirm(ctx, tx, req.ClosureConfirmID, src, req, calc); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "rollover confirm detail failed: "+err.Error())
				return
			}
		}
		if err := insertCimplrCalculation(ctx, tx, closureInitiateID, req.ClosureConfirmID, src, calc); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCalculationSnapshotFailed+err.Error())
			return
		}
		if err := insertCimplrConfirmAudit(ctx, tx, confirmAuditEntry{ConfirmID: req.ClosureConfirmID, InitiateID: closureInitiateID, Action: constants.AuditActionEdit, Status: constants.StatusPendingEditApproval, Reason: firstNonEmpty(req.Reason, "Edit FD closure confirm"), RequestedBy: req.UserID, Old: oldRow}); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "confirm audit failed: "+err.Error())
			return
		}
		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailed)
			return
		}

		if persisted, perr := persistCimplrConfirmVariances(ctx, pool, req.ClosureConfirmID, req, src, calc); perr == nil {
			varianceSummary = persisted
		}
		_ = approvalengine.CancelPendingInstances(ctx, pool, cimplrClosureModule, req.ClosureConfirmID, userEmail)
		instanceID, instErr := createCimplrApprovalInstance(ctx, pool, approvalInstanceRequest{TxType: getClosureTxCode(func() string{if closureType=="PREMATURE"{return "premature"}; return "confirm"}(), closureType, "EDIT"), Action: constants.AuditActionEdit, RecordID: req.ClosureConfirmID, RecordTable: constants.QuerryAuditClosureConfirm, AuditTable: constants.QuerryAuditClosureConfirmAudit, AuditIDColumn: "closure_confirm_id", EntityID: src.EntityID, Amount: principalExpected, UserID: req.UserID, UserEmail: userEmail})
		if instErr != nil {
			api.LogError("[CimplrFDClosure] confirm edit approval failed: %v", instErr)
		}
		if instanceID != "" {
			_, _ = pool.Exec(ctx, `UPDATE cimplr.fd_closure_confirm SET approval_instance_id=$1 WHERE closure_confirm_id=$2`, instanceID, req.ClosureConfirmID)
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"closure_initiate_id":  closureInitiateID,
			"closure_confirm_id":   req.ClosureConfirmID,
			"approval_instance_id": instanceID,
			"variance":             varianceSummary,
		})
	}
}

func CimplrConfirmDelete(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req cimplrClosureIDsRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		userEmail := getUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		if req.UserID == "" {
			req.UserID = userEmail
		}
		ids := normalizeCimplrIDs(req.ClosureConfirmID, req.ClosureConfirmIDs)
		if len(ids) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ClosureIDsRequired)
			return
		}
		results := make([]map[string]interface{}, 0, len(ids))
		for _, id := range ids {
			res := map[string]interface{}{"closure_confirm_id": id}
			oldRow, err := loadCimplrConfirmOld(r.Context(), pool, id)
			if err != nil {
				res["success"] = false
				res["error"] = "record not found"
				results = append(results, res)
				continue
			}
			if fmt.Sprint(oldRow["closure_status"]) == "POSTED" || oldRow["accounting_posted"] == true {
				res["success"] = false
				res["error"] = "posted confirm records cannot be deleted"
				results = append(results, res)
				continue
			}
			if err := insertCimplrConfirmAudit(r.Context(), pool, confirmAuditEntry{ConfirmID: id, InitiateID: fmt.Sprint(oldRow["closure_initiate_id"]), Action: constants.AuditActionDelete, Status: constants.StatusPendingDeleteApproval, Reason: firstNonEmpty(req.Comment, "Delete FD closure confirm"), RequestedBy: req.UserID, Old: oldRow}); err != nil {
				res["success"] = false
				res["error"] = err.Error()
				results = append(results, res)
				continue
			}
			instanceID, _ := createCimplrApprovalInstance(r.Context(), pool, approvalInstanceRequest{TxType: getClosureTxCode(func() string{c:=fmt.Sprint(oldRow["closure_type"]); if c=="PREMATURE"{return "premature"}; return "confirm"}(), fmt.Sprint(oldRow["closure_type"]), "DELETE"), Action: constants.AuditActionDelete, RecordID: id, RecordTable: constants.QuerryAuditClosureConfirm, AuditTable: constants.QuerryAuditClosureConfirmAudit, AuditIDColumn: "closure_confirm_id", EntityID: fmt.Sprint(oldRow["entity_id"]), Amount: 0, UserID: req.UserID, UserEmail: userEmail})
			if instanceID != "" {
				_, _ = pool.Exec(r.Context(), `UPDATE cimplr.fd_closure_confirm SET approval_instance_id=$1 WHERE closure_confirm_id=$2`, instanceID, id)
			}
			res["success"] = true
			res["approval_instance_id"] = instanceID
			results = append(results, res)
		}
		ok, errStr := summarizeCimplrBatchResults(results)
		go triggerClosureBulkNotif(context.Background(), pool, ids, "cimplr.fd_closure_confirm", "confirm", "delete", userEmail)
		api.RespondWithPayload(w, ok, errStr, map[string]interface{}{"results": results})
	}
}

// summarizeCimplrBatchResults walks the per-record results returned by
// cimplrApprove/Reject/Delete and folds them into a top-level (success, error)
// pair so the client doesn't have to inspect the inner array to find out
// whether the call worked.
//
// Top-level success is true ONLY when every record succeeded. Errors are
// concatenated as "<id>: <reason>; <id>: <reason>; ..." so a batch failure
// surfaces a single cumulative message the UI can show in a notification
// without losing per-record granularity (the full breakdown stays in results).
func summarizeCimplrBatchResults(results []map[string]interface{}) (bool, string) {
	allOk := true
	parts := make([]string, 0, len(results))
	for _, r := range results {
		ok, _ := r["success"].(bool)
		if ok {
			continue
		}
		allOk = false
		id := strings.TrimSpace(fmt.Sprint(firstNonEmpty(
			fmt.Sprint(r["closure_confirm_id"]),
			fmt.Sprint(r["closure_initiate_id"]),
		)))
		// fmt.Sprint(nil) yields "<nil>"; suppress so we don't ship that
		// noise to the UI.
		if id == "<nil>" {
			id = ""
		}
		reason := strings.TrimSpace(fmt.Sprint(r["error"]))
		if reason == "<nil>" || reason == "" {
			reason = "no error reason returned by server"
		}
		if id != "" {
			parts = append(parts, id+": "+reason)
		} else {
			parts = append(parts, reason)
		}
	}
	if allOk {
		return true, ""
	}
	return false, strings.Join(parts, "; ")
}

func CimplrConfirmApprove(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req cimplrClosureIDsRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		userEmail := getUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		if req.UserID == "" {
			req.UserID = userEmail
		}
		ids := normalizeCimplrIDs(req.ClosureConfirmID, req.ClosureConfirmIDs)
		if len(ids) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ClosureIDsRequired)
			return
		}
		results := cimplrApproveConfirms(r.Context(), pool, ids, req.UserID, userEmail, req.Comment)
		ok, errStr := summarizeCimplrBatchResults(results)
		go triggerClosureBulkNotif(context.Background(), pool, ids, "cimplr.fd_closure_confirm", "confirm", "approve", userEmail)
		api.RespondWithPayload(w, ok, errStr, map[string]interface{}{"results": results})
	}
}

func CimplrConfirmReject(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req cimplrClosureIDsRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		userEmail := getUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		if req.UserID == "" {
			req.UserID = userEmail
		}
		ids := normalizeCimplrIDs(req.ClosureConfirmID, req.ClosureConfirmIDs)
		if len(ids) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ClosureIDsRequired)
			return
		}
		results := cimplrRejectConfirms(r.Context(), pool, ids, req.UserID, userEmail, req.Comment)
		ok, errStr := summarizeCimplrBatchResults(results)
		go triggerClosureBulkNotif(context.Background(), pool, ids, "cimplr.fd_closure_confirm", "confirm", "reject", userEmail)
		api.RespondWithPayload(w, ok, errStr, map[string]interface{}{"results": results})
	}
}

func CimplrConfirmAll(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req cimplrClosureListRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		rows, total, err := listCimplrRecords(r.Context(), pool, "confirm", req, false)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to fetch confirm records: "+err.Error())
			return
		}
		api.RespondWithPayload(w, true, "", map[string]interface{}{"records": rows, "total": total})
	}
}

func CimplrPrematureAll(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req cimplrClosureListRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		req.ClosureType = "PREMATURE"
		rows, total, err := listCimplrRecords(r.Context(), pool, "confirm", req, false)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to fetch premature records: "+err.Error())
			return
		}
		api.RespondWithPayload(w, true, "", map[string]interface{}{"records": rows, "total": total})
	}
}

func CimplrMaturitySummary(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req maturitySummaryRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}
		where := []string{"COALESCE(m.is_deleted,false)=false"}
		args := []interface{}{}
		idx := 1
		addList := func(column string, values []string) {
			if len(values) == 0 {
				return
			}
			holders := make([]string, 0, len(values))
			for _, v := range values {
				if strings.TrimSpace(v) == "" {
					continue
				}
				holders = append(holders, fmt.Sprintf("$%d", idx))
				args = append(args, strings.TrimSpace(v))
				idx++
			}
			if len(holders) > 0 {
				where = append(where, column+" IN ("+strings.Join(holders, ",")+")")
			}
		}
		addList("COALESCE(m.entity_id,b.entity_id,'')", req.EntityIDs)
		addList("m.bank_id", req.BankIDs)
		addList("m.fd_status", req.FDStatuses)
		if strings.TrimSpace(req.FromDate) != "" {
			where = append(where, fmt.Sprintf("m.maturity_date >= $%d::date", idx))
			args = append(args, req.FromDate)
			idx++
		}
		if strings.TrimSpace(req.ToDate) != "" {
			where = append(where, fmt.Sprintf("m.maturity_date <= $%d::date", idx))
			args = append(args, req.ToDate)
			idx++
		}
		if strings.TrimSpace(req.ClosureType) != "" {
			where = append(where, fmt.Sprintf("COALESCE(cc.closure_type, ci.closure_type, '') = $%d", idx))
			args = append(args, strings.ToUpper(strings.TrimSpace(req.ClosureType)))
			idx++
		}

		query := `
			SELECT
				m.fd_id,
				COALESCE(m.bank_fd_ref_no, m.fd_id, '') AS fd_ref_no,
				COALESCE(m.bank_fd_ref_no,'') AS bank_fd_ref_no,
				COALESCE(m.entity_id, b.entity_id, '') AS entity_id,
				COALESCE(m.entity_name, b.entity_name, '') AS entity_name,
				COALESCE(m.bank_id,'') AS bank_id,
				COALESCE(m.bank_name,'') AS bank_name,
				COALESCE(m.principal_amount,0) AS principal_amount,
				COALESCE(m.interest_rate,0) AS interest_rate,
				COALESCE(m.interest_type_code,'') AS interest_type_code,
				COALESCE(m.tenure_days,0) AS tenure_days,
				m.start_date::text AS start_date,
				m.maturity_date::text AS maturity_date,
				COALESCE(m.fd_status,'') AS fd_status,
				COALESCE(m.booking_id,'') AS booking_id,
				COALESCE(b.booking_status,'') AS booking_status,
				ci.closure_initiate_id,
				cc.closure_confirm_id,
				COALESCE(cc.closure_type, ci.closure_type, '') AS closure_type,
				COALESCE(cc.closure_status, ci.closure_status, '') AS closure_status,
				COALESCE(cc.posting_status, '') AS posting_status,
				COALESCE(cc.accounting_posted, false) AS accounting_posted,
				COALESCE(cc.journal_entry_id, '') AS journal_entry_id,
				COALESCE(cc.new_booking_id, '') AS new_booking_id,
				COALESCE(cc.principal_received, cc.principal_expected, ci.principal_amount, m.principal_amount, 0) AS principal_amount_final,
				COALESCE(cc.interest_received, cc.interest_expected, ci.accrued_interest_till_date, calc.accrued_interest, 0) AS accrued_interest,
				COALESCE(cc.tds_deducted, cc.tds_expected, ci.tds_expected, calc.tds_amount, 0) AS tds_deducted,
				COALESCE(cc.net_amount_received, cc.net_expected, ci.net_expected_amount, calc.net_payout, 0) AS net_payout,
				COALESCE(cc.has_variance, ci.has_variance, false) AS has_variance,
				COALESCE(cc.has_unresolved_variance, ci.has_unresolved_variance, false) AS has_unresolved_variance,
				COALESCE(
					CASE WHEN UPPER(COALESCE(cc.closure_status,''))='POSTED' THEN 'POSTED' END,
					ca.processing_status,
					ia.processing_status,
					''
				) AS latest_processing_status,
				COALESCE(ca.requested_by, ia.requested_by, '') AS latest_requested_by,
				COALESCE(ca.requested_at, ia.requested_at) AS latest_requested_at,
				COALESCE(ca.checker_by, ia.checker_by, '') AS latest_checker_by,
				COALESCE(ca.checker_at, ia.checker_at) AS latest_checker_at
			FROM investment.fd_master m
			LEFT JOIN investment.fd_booking_request b ON b.booking_id=m.booking_id
			LEFT JOIN LATERAL (
				SELECT * FROM cimplr.fd_closure_confirm c
				WHERE c.fd_id=m.fd_id AND COALESCE(c.is_deleted,false)=false
				ORDER BY
					CASE WHEN c.closure_type='PREMATURE' THEN 0 ELSE 1 END,
					c.closure_confirm_id DESC
				LIMIT 1
			) cc ON true
			LEFT JOIN LATERAL (
				SELECT * FROM cimplr.fd_closure_initiate i
				WHERE i.fd_id=m.fd_id AND COALESCE(i.is_deleted,false)=false
				ORDER BY i.closure_initiate_id DESC LIMIT 1
			) ci ON true
			LEFT JOIN LATERAL (
				SELECT * FROM cimplr.fd_closure_calculation cal
				WHERE cal.fd_id=m.fd_id
				  AND COALESCE(cal.is_deleted,false)=false
				  AND (
					(cc.closure_confirm_id IS NOT NULL AND cal.closure_confirm_id=cc.closure_confirm_id)
					OR (cc.closure_confirm_id IS NULL AND ci.closure_initiate_id IS NOT NULL AND cal.closure_initiate_id=ci.closure_initiate_id)
				  )
				ORDER BY cal.calculation_date DESC, cal.calculation_id DESC LIMIT 1
			) calc ON true
			LEFT JOIN LATERAL (
				SELECT * FROM cimplr.fd_closure_confirm_audit a
				WHERE cc.closure_confirm_id IS NOT NULL AND a.closure_confirm_id=cc.closure_confirm_id
				ORDER BY
					CASE WHEN a.action_type='POST' AND a.processing_status='POSTED' THEN 0 ELSE 1 END,
					a.requested_at DESC NULLS LAST,
					a.audit_id DESC
				LIMIT 1
			) ca ON true
			LEFT JOIN LATERAL (
				SELECT * FROM cimplr.fd_closure_initiate_audit a
				WHERE ci.closure_initiate_id IS NOT NULL AND a.closure_initiate_id=ci.closure_initiate_id
				ORDER BY a.requested_at DESC, a.audit_id DESC LIMIT 1
			) ia ON true
			WHERE ` + strings.Join(where, " AND ") + `
			ORDER BY m.maturity_date ASC, m.fd_id ASC`

		rows, err := pool.Query(r.Context(), query, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to fetch cimplr maturity summary: "+err.Error())
			return
		}
		defer rows.Close()
		records, err := pgx.CollectRows(rows, pgx.RowToMap)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to read cimplr maturity summary: "+err.Error())
			return
		}
		summary := map[string]*statusTotals{}
		grand := statusTotals{}
		for _, row := range records {
			key := strings.TrimSpace(fmt.Sprint(row["closure_type"]))
			if key == "" {
				key = "NO_ACTION"
			}
			if _, ok := summary[key]; !ok {
				summary[key] = &statusTotals{}
			}
			principal := cimplrFloat(row["principal_amount_final"])
			interest := cimplrFloat(row["accrued_interest"])
			tds := cimplrFloat(row["tds_deducted"])
			net := cimplrFloat(row["net_payout"])
			summary[key].Count++
			summary[key].TotalPrincipal += principal
			summary[key].TotalInterest += interest
			summary[key].TotalTDS += tds
			summary[key].TotalNetPayout += net
			grand.Count++
			grand.TotalPrincipal += principal
			grand.TotalInterest += interest
			grand.TotalTDS += tds
			grand.TotalNetPayout += net
		}
		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"records":     records,
			"data":        records,
			"summary":     summary,
			"grand_total": grand,
			"source":      "cimplr",
		})
	}
}

func CimplrConfirmApprovedActive(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req cimplrClosureListRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		rows, total, err := listCimplrRecords(r.Context(), pool, "confirm", req, true)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to fetch approved-active confirm records: "+err.Error())
			return
		}
		api.RespondWithPayload(w, true, "", map[string]interface{}{"records": rows, "total": total})
	}
}

func CimplrClosureAudit(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req cimplrClosureIDsRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.ClosureConfirmID != "" {
			api.RespondWithPayload(w, true, "", map[string]interface{}{
				"closure_confirm_id": req.ClosureConfirmID,
				"audit_trail":        fetchCimplrAudit(r.Context(), pool, "confirm", req.ClosureConfirmID),
			})
			return
		}
		if req.ClosureInitiateID != "" {
			api.RespondWithPayload(w, true, "", map[string]interface{}{
				"closure_initiate_id": req.ClosureInitiateID,
				"audit_trail":         fetchCimplrAudit(r.Context(), pool, "initiate", req.ClosureInitiateID),
			})
			return
		}
		api.RespondWithError(w, http.StatusBadRequest, constants.ClosureIDsRequired)
	}
}

func CimplrInitiateDetail(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req cimplrClosureIDsRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		id := strings.TrimSpace(req.ClosureInitiateID)
		if id == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrClosureInitiateIDRequired)
			return
		}
		header, err := loadCimplrInitiateOld(r.Context(), pool, id)
		if err != nil {
			api.RespondWithError(w, http.StatusNotFound, constants.ErrClosureInitiateRecordNotFound)
			return
		}
		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"initiate":          header,
			"calculation":       fetchCimplrCalculations(r.Context(), pool, "closure_initiate_id", id),
			"files":             fetchCimplrFiles(r.Context(), pool, "closure_initiate_id", id),
			"audit_trail":       fetchCimplrAudit(r.Context(), pool, "initiate", id),
			"variances":         fetchVariances(r.Context(), pool, id),
			"approval_workflow": fetchCimplrApprovalWorkflow(r.Context(), pool, id),
		})
	}
}

func CimplrConfirmDetail(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req cimplrClosureIDsRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		id := strings.TrimSpace(req.ClosureConfirmID)
		if id == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ClosureIDsRequired)
			return
		}
		header, err := loadCimplrConfirmOld(r.Context(), pool, id)
		if err != nil {
			api.RespondWithError(w, http.StatusNotFound, constants.ConfirmRecordNotFound)
			return
		}
		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"confirm":           header,
			"premature_detail":  fetchCimplrSubRows(r.Context(), pool, `SELECT * FROM cimplr.fd_closure_premature_confirm WHERE closure_confirm_id=$1 AND is_deleted=false`, id),
			"rollover_detail":   fetchCimplrSubRows(r.Context(), pool, `SELECT * FROM cimplr.fd_closure_rollover_confirm WHERE closure_confirm_id=$1 AND is_deleted=false`, id),
			"calculation":       fetchCimplrCalculations(r.Context(), pool, "closure_confirm_id", id),
			"files":             fetchCimplrFiles(r.Context(), pool, "closure_confirm_id", id),
			"audit_trail":       fetchCimplrAudit(r.Context(), pool, "confirm", id),
			"variances":         fetchVariances(r.Context(), pool, id),
			"approval_workflow": fetchCimplrApprovalWorkflow(r.Context(), pool, id),
		})
	}
}

func CimplrClosureDownload(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			FileID            string `json:"file_id"`
			ClosureInitiateID string `json:"closure_initiate_id"`
			ClosureConfirmID  string `json:"closure_confirm_id"`
			FileType          string `json:"file_type"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		files, err := loadCimplrDownloadFiles(r.Context(), pool, req.FileID, req.ClosureInitiateID, req.ClosureConfirmID, req.FileType)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "download lookup failed: "+err.Error())
			return
		}
		if len(files) == 0 {
			api.RespondWithPayload(w, false, "No file available", map[string]interface{}{"files": []map[string]interface{}{}})
			return
		}
		api.RespondWithPayload(w, true, "", map[string]interface{}{"files": files})
	}
}

func CimplrClosureUpload(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Type"))), "multipart/form-data") {
			uploadCimplrClosureMultipart(w, r, pool)
			return
		}
		var req cimplrClosureUploadRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		userEmail := getUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		if req.UserID == "" {
			req.UserID = userEmail
		}
		if req.ClosureInitiateID == "" && req.ClosureConfirmID == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ClosureIDsRequired)
			return
		}
		if req.FileType == "" || req.StoredFileName == "" || req.UploadS3Key == "" {
			api.RespondWithError(w, http.StatusBadRequest, "file_type, stored_file_name and upload_s3_key are required")
			return
		}
		var fileID string
		err := pool.QueryRow(r.Context(), `
			INSERT INTO cimplr.fd_closure_files (
				closure_initiate_id, closure_confirm_id, file_type, stored_file_name, original_file_name,
				content_type, file_size, file_hash, upload_s3_key, uploaded_by
			) VALUES (NULLIF($1,''), NULLIF($2,''), $3, $4, NULLIF($5,''), NULLIF($6,''), NULLIF($7,0), NULLIF($8,''), $9, $10)
			RETURNING file_id::text`,
			req.ClosureInitiateID, req.ClosureConfirmID, strings.ToUpper(strings.TrimSpace(req.FileType)), req.StoredFileName, req.OriginalFileName,
			req.ContentType, req.FileSize, req.FileHash, req.UploadS3Key, userEmail,
		).Scan(&fileID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "file upload metadata failed: "+err.Error())
			return
		}
		_, _ = pool.Exec(r.Context(), `
			INSERT INTO cimplr.fd_closure_files_audit (
				file_id, closure_initiate_id, closure_confirm_id, action_type, processing_status,
				reason, requested_by
			) VALUES ($1::uuid, NULLIF($2,''), NULLIF($3,''), 'CREATE', 'APPROVED', $4, $5)`,
			fileID, req.ClosureInitiateID, req.ClosureConfirmID, firstNonEmpty(req.Reason, "File uploaded"), userEmail,
		)
		api.RespondWithPayload(w, true, "", map[string]interface{}{"file_id": fileID})
	}
}

func uploadCimplrClosureMultipart(w http.ResponseWriter, r *http.Request, pool *pgxpool.Pool) {
	userEmail := getUserEmail(r.Context())
	if userEmail == "" {
		api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
		return
	}
	if err := r.ParseMultipartForm(64 << 20); err != nil {
		api.RespondWithError(w, http.StatusBadRequest, constants.ErrFailedToParseMultipartForm)
		return
	}
	closureInitiateID := strings.TrimSpace(r.FormValue("closure_initiate_id"))
	closureConfirmID := strings.TrimSpace(r.FormValue("closure_confirm_id"))
	fileType := strings.ToUpper(strings.TrimSpace(r.FormValue("file_type")))
	if closureInitiateID == "" && closureConfirmID == "" {
		api.RespondWithError(w, http.StatusBadRequest, constants.ClosureIDsRequired)
		return
	}
	if fileType == "" {
		api.RespondWithError(w, http.StatusBadRequest, "file_type is required")
		return
	}
	file, header, err := r.FormFile("file")
	if err != nil {
		api.RespondWithError(w, http.StatusBadRequest, "file is required")
		return
	}
	defer file.Close()
	body, err := io.ReadAll(file)
	if err != nil {
		api.RespondWithError(w, http.StatusBadRequest, "failed to read uploaded file")
		return
	}
	if len(body) == 0 {
		api.RespondWithError(w, http.StatusBadRequest, "uploaded file is empty")
		return
	}
	uploadedAt := time.Now().UTC()
	parentID := firstNonEmpty(closureConfirmID, closureInitiateID)
	storedFileName := s3storage.BuildUploadedFilename(header.Filename, userEmail, uploadedAt)
	module := "fd-closure-additional"
	if closureConfirmID != "" {
		module = "fd-rollover-additional"
	}
	s3Key := s3storage.BuildNamedS3Key(s3storage.GetStoragePrefix(module), parentID, storedFileName)
	contentType := header.Header.Get("Content-Type")
	if contentType == "" {
		contentType = s3storage.DetectContentType(body)
	}
	if err := s3storage.PutObjectToS3(r.Context(), s3Key, body, contentType); err != nil {
		api.RespondWithError(w, http.StatusInternalServerError, "failed to upload file to S3: "+err.Error())
		return
	}
	fileHash := s3storage.ContentHashHex(body)
	var fileID string
	err = pool.QueryRow(r.Context(), `
		INSERT INTO cimplr.fd_closure_files (
			closure_initiate_id, closure_confirm_id, file_type, stored_file_name, original_file_name,
			content_type, file_size, file_hash, upload_s3_key, uploaded_by
		) VALUES (NULLIF($1,''), NULLIF($2,''), $3, $4, $5, $6, $7, $8, $9, $10)
		RETURNING file_id::text`,
		closureInitiateID, closureConfirmID, fileType, storedFileName, header.Filename,
		contentType, int64(len(body)), fileHash, s3Key, userEmail,
	).Scan(&fileID)
	if err != nil {
		api.RespondWithError(w, http.StatusInternalServerError, "file upload metadata failed: "+err.Error())
		return
	}
	_, _ = pool.Exec(r.Context(), `
		INSERT INTO cimplr.fd_closure_files_audit (
			file_id, closure_initiate_id, closure_confirm_id, action_type, processing_status,
			reason, requested_by
		) VALUES ($1::uuid, NULLIF($2,''), NULLIF($3,''), 'CREATE', 'APPROVED', $4, $5)`,
		fileID, closureInitiateID, closureConfirmID, firstNonEmpty(r.FormValue("reason"), "File uploaded"), userEmail,
	)
	api.RespondWithPayload(w, true, "", map[string]interface{}{
		"file_id":          fileID,
		"upload_s3_key":    s3Key,
		"stored_file_name": storedFileName,
		"file_hash":        fileHash,
		"file_size":        len(body),
	})
}

func cimplrInitiatePostFinalizeHook(ctx context.Context, pool *pgxpool.Pool, recordID, transactionType, finalStatus, actorEmail, comment string) {
	if finalStatus == approvalengine.InstStatusRejected {
		if strings.HasSuffix(transactionType, "_CREATE") || strings.HasSuffix(transactionType, "_DELETE") {
			_, _ = pool.Exec(ctx, `UPDATE cimplr.fd_closure_initiate SET closure_status='REJECTED' WHERE closure_initiate_id=$1 AND is_deleted=false`, recordID)
		}
		return
	}
	if strings.HasSuffix(transactionType, "_DELETE") {
		_, _ = pool.Exec(ctx, `
			UPDATE cimplr.fd_closure_initiate
			SET closure_status='DELETED', is_deleted=true, is_active=false, approval_instance_id=NULL
			WHERE closure_initiate_id=$1`, recordID)
	} else if strings.HasSuffix(transactionType, "_CREATE") || strings.HasSuffix(transactionType, "_EDIT") {
		_, _ = pool.Exec(ctx, `UPDATE cimplr.fd_closure_initiate SET closure_status='CONFIRM' WHERE closure_initiate_id=$1 AND is_deleted=false`, recordID)
	}
}

func cimplrConfirmPostFinalizeHook(ctx context.Context, pool *pgxpool.Pool, recordID, transactionType, finalStatus, actorEmail, comment string) {
	if finalStatus == approvalengine.InstStatusRejected {
		if strings.HasSuffix(transactionType, "_CREATE") || strings.HasSuffix(transactionType, "_DELETE") {
			_, _ = pool.Exec(ctx, `UPDATE cimplr.fd_closure_confirm SET closure_status='REJECTED' WHERE closure_confirm_id=$1 AND is_deleted=false`, recordID)
		}
		if strings.HasSuffix(transactionType, "_CREATE") {
			_, _ = pool.Exec(ctx, `
				UPDATE cimplr.fd_closure_initiate i
				SET closure_status='REJECTED'
				FROM cimplr.fd_closure_confirm c
				WHERE c.closure_confirm_id=$1
				  AND c.closure_initiate_id=i.closure_initiate_id
				  AND COALESCE(i.is_deleted,false)=false`, recordID)
		}
		return
	}
	if strings.HasSuffix(transactionType, "_DELETE") {
		var initiateID, closureType string
		_ = pool.QueryRow(ctx, `SELECT COALESCE(closure_initiate_id,''), COALESCE(closure_type,'') FROM cimplr.fd_closure_confirm WHERE closure_confirm_id=$1`, recordID).Scan(&initiateID, &closureType)
		_, _ = pool.Exec(ctx, `
			UPDATE cimplr.fd_closure_confirm
			SET closure_status='DELETED', is_deleted=true, is_active=false,
			    approval_instance_id=NULL, posting_status='FAILED'
			WHERE closure_confirm_id=$1`, recordID)
		if initiateID != "" {
			if closureType == "PREMATURE" {
				_, _ = pool.Exec(ctx, `UPDATE cimplr.fd_closure_initiate SET is_deleted=true, closure_status='DELETED' WHERE closure_initiate_id=$1 AND is_deleted=false`, initiateID)
			} else {
				_, _ = pool.Exec(ctx, `UPDATE cimplr.fd_closure_initiate SET closure_status='CONFIRM' WHERE closure_initiate_id=$1 AND is_deleted=false`, initiateID)
			}
		}
	} else if strings.HasSuffix(transactionType, "_CREATE") || strings.HasSuffix(transactionType, "_EDIT") {
		if err := finalizeCimplrConfirmApproval(ctx, pool, recordID, actorEmail, comment); err != nil {
			api.LogError("[CimplrFDClosure] confirm finalize failed confirm_id=%s: %v", recordID, err)
			_, _ = pool.Exec(ctx, `UPDATE cimplr.fd_closure_confirm SET posting_status='FAILED' WHERE closure_confirm_id=$1`, recordID)
		}
	}
}

func cimplrLatestPendingAuditAction(ctx context.Context, q cimplrRowQuerier, auditTable, idColumn, recordID string) (string, error) {
	var actionType string
	err := q.QueryRow(ctx, fmt.Sprintf(`
		SELECT COALESCE(action_type,'')
		FROM %s
		WHERE %s=$1 AND processing_status LIKE 'PENDING%%'
		ORDER BY requested_at DESC NULLS LAST, audit_id DESC
		LIMIT 1`, auditTable, idColumn), recordID).Scan(&actionType)
	if err != nil {
		if err == pgx.ErrNoRows {
			return "", nil
		}
		return "", err
	}
	return strings.ToUpper(strings.TrimSpace(actionType)), nil
}

func cimplrApproveInitiates(ctx context.Context, pool *pgxpool.Pool, ids []string, userID, userEmail, comment string) []map[string]interface{} {
	results := make([]map[string]interface{}, 0, len(ids))
	for _, id := range ids {
		res := map[string]interface{}{"closure_initiate_id": id}
		if acted, err := cimplrActOnApproval(ctx, pool, id, userID, userEmail, approvalengine.ActionApproved, firstNonEmpty(comment, "Approved FD closure initiate")); err != nil {
			res["success"] = false
			res["error"] = err.Error()
		} else if acted {
			res["success"] = true
			res["approval_engine"] = true
		} else {
			pendingAction, pendingErr := cimplrLatestPendingAuditAction(ctx, pool, constants.QuerryAuditClosureInitiate, "closure_initiate_id", id)
			if pendingErr != nil {
				res["success"] = false
				res["error"] = pendingErr.Error()
				results = append(results, res)
				continue
			}
			tx, err := pool.Begin(ctx)
			if err != nil {
				res["success"] = false
				res["error"] = err.Error()
				results = append(results, res)
				continue
			}
			_, err = tx.Exec(ctx, `UPDATE cimplr.fd_closure_initiate_audit SET processing_status='APPROVED', checker_by=$1, checker_at=NOW(), checker_comment=$2 WHERE closure_initiate_id=$3 AND processing_status LIKE 'PENDING%'`, userEmail, comment, id)
			if err == nil {
				switch pendingAction {
				case constants.AuditActionDelete:
					_, err = tx.Exec(ctx, `
						UPDATE cimplr.fd_closure_initiate
						SET closure_status='DELETED', is_deleted=true, is_active=false, approval_instance_id=NULL
						WHERE closure_initiate_id=$1`, id)
				default:
					_, err = tx.Exec(ctx, `UPDATE cimplr.fd_closure_initiate SET closure_status='CONFIRM' WHERE closure_initiate_id=$1 AND is_deleted=false`, id)
				}
			}
			if err == nil {
				err = tx.Commit(ctx)
			}
			if err != nil {
				_ = tx.Rollback(ctx)
				res["success"] = false
				res["error"] = err.Error()
			} else {
				res["success"] = true
				res["approval_engine"] = false
			}
		}
		results = append(results, res)
	}
	return results
}

func cimplrRejectInitiates(ctx context.Context, pool *pgxpool.Pool, ids []string, userID, userEmail, comment string) []map[string]interface{} {
	results := make([]map[string]interface{}, 0, len(ids))
	for _, id := range ids {
		res := map[string]interface{}{"closure_initiate_id": id}
		if acted, err := cimplrActOnApproval(ctx, pool, id, userID, userEmail, approvalengine.ActionRejected, firstNonEmpty(comment, "Rejected FD closure initiate")); err != nil {
			res["success"] = false
			res["error"] = err.Error()
		} else if acted {
			res["success"] = true
			res["approval_engine"] = true
		} else {
			_, err := pool.Exec(ctx, `
				WITH upd AS (
					UPDATE cimplr.fd_closure_initiate_audit
					SET processing_status='REJECTED', checker_by=$1, checker_at=NOW(), checker_comment=$2
					WHERE closure_initiate_id=$3 AND processing_status LIKE 'PENDING%'
					RETURNING action_type
				)
				UPDATE cimplr.fd_closure_initiate
				SET closure_status=CASE WHEN EXISTS(SELECT 1 FROM upd WHERE action_type IN ('CREATE','DELETE')) THEN 'REJECTED' ELSE closure_status END
				WHERE closure_initiate_id=$3`,
				userEmail, comment, id,
			)
			if err != nil {
				res["success"] = false
				res["error"] = err.Error()
			} else {
				res["success"] = true
				res["approval_engine"] = false
			}
		}
		results = append(results, res)
	}
	return results
}

func cimplrAssertConfirmApprovable(ctx context.Context, exec cimplrRowQuerier, closureConfirmID string) error {
	var hasUnresolved, hasVariance bool
	var resolutionAction string
	err := exec.QueryRow(ctx, `
		SELECT COALESCE(has_unresolved_variance,false), COALESCE(has_variance,false),
		       COALESCE(resolution_action,'')
		FROM cimplr.fd_closure_confirm
		WHERE closure_confirm_id=$1 AND COALESCE(is_deleted,false)=false`, closureConfirmID,
	).Scan(&hasUnresolved, &hasVariance, &resolutionAction)
	if err != nil {
		return fmt.Errorf("confirm record load failed for %s: %w", closureConfirmID, err)
	}
	resolutionAction = strings.ToUpper(strings.TrimSpace(resolutionAction))
	if hasUnresolved && resolutionAction != "ACCEPT" {
		return fmt.Errorf("cannot approve: unresolved variance exists — validate amounts, resolve variances, or set resolution_action to ACCEPT")
	}
	// IMPORTANT: variance rows live in PUBLIC.variance_log (see api/varianceengine/engine.go).
	// The previous query used "investment.variance_log" which does not exist — the resulting
	// "relation does not exist" error was silently swallowed by "_ = exec.QueryRow(...)".
	// When this helper ran inside the dry-run tx that swallow aborted the tx and every
	// subsequent statement reported the cryptic SQLSTATE 25P02 instead of the real cause.
	// Both the schema name and the error swallowing are fixed below.
	var openCount int
	if err := exec.QueryRow(ctx, `
		SELECT COUNT(*)::int FROM public.variance_log
		WHERE module_code=$1 AND record_id=$2 AND status='OPEN'`,
		"FD_CLOSURE", closureConfirmID,
	).Scan(&openCount); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("variance_log open-count check failed for %s: %w", closureConfirmID, err)
	}
	if openCount > 0 && resolutionAction != "ACCEPT" {
		return fmt.Errorf("cannot approve: %d open variance(s) — edit to align with system calculation or set resolution_action=ACCEPT", openCount)
	}
	if hasVariance && openCount > 0 && resolutionAction != "ACCEPT" {
		return fmt.Errorf("cannot approve: variance flag is set with %d open item(s)", openCount)
	}
	return nil
}

func cimplrApproveConfirms(ctx context.Context, pool *pgxpool.Pool, ids []string, userID, userEmail, comment string) []map[string]interface{} {
	results := make([]map[string]interface{}, 0, len(ids))
	for _, id := range ids {
		res := map[string]interface{}{"closure_confirm_id": id}
		if err := cimplrAssertConfirmApprovable(ctx, pool, id); err != nil {
			res["success"] = false
			res["error"] = err.Error()
			results = append(results, res)
			continue
		}
		// Dry-run: simulate the full post-finalize inside a tx that always
		// rolls back. If posting would fail, refuse to approve so we never
		// produce zombie APPROVED+FAILED rows. The actor passed here is only
		// used inside the dry-run tx (which never commits), so a system
		// fallback is fine if userEmail is empty.
		dryActor := firstNonEmpty(userEmail, "approval-precheck@cimplr.system")
		if dryErr := runFinalizeCimplrConfirmDryRun(ctx, pool, id, dryActor, firstNonEmpty(comment, "approval pre-check")); dryErr != nil {
			res["success"] = false
			res["error"] = "cannot approve — posting would fail: " + dryErr.Error()
			api.LogError("[CimplrFDClosure] approve pre-check rejected confirm_id=%s reason=%v", id, dryErr)
			results = append(results, res)
			continue
		}
		if acted, err := cimplrActOnApproval(ctx, pool, id, userID, userEmail, approvalengine.ActionApproved, firstNonEmpty(comment, "Approved FD closure confirm")); err != nil {
			res["success"] = false
			res["error"] = err.Error()
		} else if acted {
			// Post-finalize hook runs async in approval engine; call finalize here too so
			// journals/posting_status update even if the hook is delayed or fails silently.
			if finErr := finalizeCimplrConfirmApproval(ctx, pool, id, userEmail, comment); finErr != nil {
				api.LogError("[CimplrFDClosure] confirm finalize after approval engine failed confirm_id=%s: %v", id, finErr)
				_, _ = pool.Exec(ctx, `UPDATE cimplr.fd_closure_confirm SET posting_status='FAILED' WHERE closure_confirm_id=$1`, id)
				res["success"] = false
				res["error"] = finErr.Error()
			} else {
				res["success"] = true
				res["approval_engine"] = true
			}
		} else {
			pendingAction, pendingErr := cimplrLatestPendingAuditAction(ctx, pool, constants.QuerryAuditClosureConfirmAudit, "closure_confirm_id", id)
			if pendingErr != nil {
				res["success"] = false
				res["error"] = pendingErr.Error()
				results = append(results, res)
				continue
			}
			_, err := pool.Exec(ctx, `UPDATE cimplr.fd_closure_confirm_audit SET processing_status='APPROVED', checker_by=$1, checker_at=NOW(), checker_comment=$2 WHERE closure_confirm_id=$3 AND processing_status LIKE 'PENDING%'`, userEmail, comment, id)
			if err == nil {
				switch pendingAction {
				case constants.AuditActionDelete:
					var initiateID, closureType string
					_ = pool.QueryRow(ctx, `SELECT COALESCE(closure_initiate_id,''), COALESCE(closure_type,'') FROM cimplr.fd_closure_confirm WHERE closure_confirm_id=$1`, id).Scan(&initiateID, &closureType)
					_, err = pool.Exec(ctx, `
						UPDATE cimplr.fd_closure_confirm
						SET closure_status='DELETED', is_deleted=true, is_active=false,
						    posting_status='PENDING', accounting_posted=false, approval_instance_id=NULL
						WHERE closure_confirm_id=$1`, id)
					if err == nil && initiateID != "" {
						if closureType == "PREMATURE" {
							_, err = pool.Exec(ctx, `UPDATE cimplr.fd_closure_initiate SET is_deleted=true, closure_status='DELETED' WHERE closure_initiate_id=$1 AND is_deleted=false`, initiateID)
						} else {
							_, err = pool.Exec(ctx, `UPDATE cimplr.fd_closure_initiate SET closure_status='CONFIRM' WHERE closure_initiate_id=$1 AND is_deleted=false`, initiateID)
						}
					}
				default:
					api.LogInfo("[CimplrFDClosure] confirm finalize starting (no approval engine) confirm_id=%s actor=%s", id, userEmail)
					err = finalizeCimplrConfirmApproval(ctx, pool, id, userEmail, comment)
					if err != nil {
						api.LogError("[CimplrFDClosure] confirm finalize failed (no approval engine) confirm_id=%s: %v", id, err)
						_, _ = pool.Exec(ctx, `UPDATE cimplr.fd_closure_confirm SET posting_status='FAILED' WHERE closure_confirm_id=$1`, id)
					} else {
						api.LogInfo("[CimplrFDClosure] confirm finalize succeeded (no approval engine) confirm_id=%s", id)
					}
				}
			}
			if err != nil {
				res["success"] = false
				res["error"] = err.Error()
			} else {
				res["success"] = true
				res["approval_engine"] = false
			}
		}
		results = append(results, res)
	}
	return results
}

func cimplrRejectConfirms(ctx context.Context, pool *pgxpool.Pool, ids []string, userID, userEmail, comment string) []map[string]interface{} {
	results := make([]map[string]interface{}, 0, len(ids))
	for _, id := range ids {
		res := map[string]interface{}{"closure_confirm_id": id}
		if acted, err := cimplrActOnApproval(ctx, pool, id, userID, userEmail, approvalengine.ActionRejected, firstNonEmpty(comment, "Rejected FD closure confirm")); err != nil {
			res["success"] = false
			res["error"] = err.Error()
		} else if acted {
			res["success"] = true
			res["approval_engine"] = true
		} else {
			_, err := pool.Exec(ctx, `
				WITH upd AS (
					UPDATE cimplr.fd_closure_confirm_audit
					SET processing_status='REJECTED', checker_by=$1, checker_at=NOW(), checker_comment=$2
					WHERE closure_confirm_id=$3 AND processing_status LIKE 'PENDING%'
					RETURNING action_type
				)
				UPDATE cimplr.fd_closure_confirm
				SET closure_status=CASE WHEN EXISTS(SELECT 1 FROM upd WHERE action_type IN ('CREATE','DELETE')) THEN 'REJECTED' ELSE closure_status END
				WHERE closure_confirm_id=$3`,
				userEmail, comment, id,
			)
			if err != nil {
				res["success"] = false
				res["error"] = err.Error()
			} else {
				res["success"] = true
				res["approval_engine"] = false
			}
		}
		results = append(results, res)
	}
	return results
}

func cimplrActOnApproval(ctx context.Context, pool *pgxpool.Pool, recordID, userID, userEmail, action, comment string) (bool, error) {
	res, err := approvalengine.ActOnPendingOrDiagnose(ctx, pool, approvalengine.ActOnPendingRequest{ModuleCode: cimplrClosureModule, RecordID: recordID, UserID: userID, UserEmail: userEmail, RoleID: "", Action: action, Comment: comment})
	if err != nil {
		return false, err
	}
	if res.Acted {
		return true, nil
	}
	if res.CancelledStale {
		api.LogInfo("[CimplrClosure] Cancelled stale approval instance for record=%s: %s", recordID, res.Reason)
		return false, nil
	}
	if res.Reason != "" {
		return false, fmt.Errorf("%s", res.Reason)
	}
	return false, nil
}

func finalizeCimplrConfirmApproval(ctx context.Context, pool *pgxpool.Pool, closureConfirmID, actorEmail, comment string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if err := finalizeCimplrConfirmApprovalTx(ctx, tx, closureConfirmID, actorEmail, comment); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// runFinalizeCimplrConfirmDryRun simulates the post-finalize logic inside a
// transaction that is always rolled back. It surfaces the real error a real
// post attempt would hit without persisting any side-effects (no journals, no
// fd_master update, no new booking, no audit row).
//
// Used as a pre-flight check in cimplrApproveConfirms so approval never
// proceeds when posting would fail — eliminates zombie APPROVED+FAILED rows.
func runFinalizeCimplrConfirmDryRun(ctx context.Context, pool *pgxpool.Pool, closureConfirmID, actorEmail, comment string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck — rollback is intentional for dry-run

	return finalizeCimplrConfirmApprovalTx(ctx, tx, closureConfirmID, actorEmail, comment)
}

func finalizeCimplrConfirmApprovalTx(ctx context.Context, tx pgx.Tx, closureConfirmID, actorEmail, comment string) error {
	var closureType string
	var accountingPosted bool
	if err := tx.QueryRow(ctx, `
		SELECT closure_type, accounting_posted
		FROM cimplr.fd_closure_confirm
		WHERE closure_confirm_id=$1 AND is_deleted=false`, closureConfirmID,
	).Scan(&closureType, &accountingPosted); err != nil {
		return err
	}
	if accountingPosted {
		return nil
	}
	if err := cimplrAssertConfirmApprovable(ctx, tx, closureConfirmID); err != nil {
		return err
	}
	if closureType == "ROLLOVER" {
		return createCimplrRolloverBookingTx(ctx, tx, closureConfirmID, actorEmail, comment)
	}
	return postCimplrClosureJournalsTx(ctx, tx, closureConfirmID, actorEmail, comment)
}

func postCimplrClosureJournalsTx(ctx context.Context, tx pgx.Tx, closureConfirmID, actorEmail, comment string) error {

	var err error
	var fdID, closureType, entityID, entityName, sourceAccountID, bookingID string
	var principal, interest, tds, penalty, netPayout float64
	var accountingPosted bool
	// fd_closure_premature_confirm has no premature_type column in this
	// schema — every premature closure is treated as FULL (the partial-vs-
	// full distinction is not modelled here). The previous SELECT against
	// pc.premature_type tripped SQLSTATE 42703 and aborted the dry-run.
	err = tx.QueryRow(ctx, `
		SELECT c.fd_id, c.closure_type, COALESCE(c.entity_id,''), COALESCE(c.entity_name,''),
		       COALESCE(c.principal_received, c.principal_expected, 0),
		       COALESCE(c.interest_received, c.interest_expected, 0),
		       COALESCE(c.tds_deducted, c.tds_expected, 0),
		       CASE WHEN c.closure_type='PREMATURE' THEN COALESCE(pc.penalty_amount,0) ELSE 0 END,
		       COALESCE(c.net_amount_received, c.net_expected, 0),
		       COALESCE(b.source_account_id,''), c.accounting_posted,
		       COALESCE(b.booking_id, '')
		FROM cimplr.fd_closure_confirm c
		LEFT JOIN cimplr.fd_closure_premature_confirm pc ON pc.closure_confirm_id=c.closure_confirm_id AND pc.is_deleted=false
		LEFT JOIN investment.fd_master m ON m.fd_id=c.fd_id
		LEFT JOIN investment.fd_booking_request b ON b.booking_id=m.booking_id
		WHERE c.closure_confirm_id=$1 AND c.is_deleted=false
		FOR UPDATE OF c`, closureConfirmID,
	).Scan(&fdID, &closureType, &entityID, &entityName, &principal, &interest, &tds, &penalty, &netPayout, &sourceAccountID, &accountingPosted, &bookingID)
	if err != nil {
		return err
	}
	if accountingPosted {
		return tx.Commit(ctx)
	}

	now := time.Now()
	accountingPeriod := fmt.Sprintf("%d-%02d", now.Year(), now.Month())
	var bankAccountNumber, bankAccountName string
	if sourceAccountID != "" {
		// ErrNoRows is fine — we fall back to defaults below. Anything else
		// (column missing, permission denied, type mismatch, etc.) MUST
		// propagate; otherwise it silently aborts the tx and the next
		// INSERT reports the cryptic SQLSTATE 25P02 instead of the real cause.
		err := tx.QueryRow(ctx,
			`SELECT COALESCE(account_number,''), COALESCE(account_nickname,'')
			 FROM public.masterbankaccount
			 WHERE account_id=$1 LIMIT 1`, sourceAccountID,
		).Scan(&bankAccountNumber, &bankAccountName)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("settlement account lookup failed for account_id=%s: %w", sourceAccountID, err)
		}
	}
	if bankAccountNumber == "" {
		bankAccountNumber = firstNonEmpty(sourceAccountID, "SETTLEMENT")
	}
	if bankAccountName == "" {
		bankAccountName = "Settlement Account"
	}
	activitySubtype := "FD_MATURITY_PAYOUT"
	if closureType == "PREMATURE" {
		activitySubtype = "FD_PREMATURE_CLOSURE"
	}

	var activityID, entryID string
	if err := tx.QueryRow(ctx, `INSERT INTO investment.accounting_activity (activity_type,activity_subtype,effective_date,accounting_period,data_source,status) VALUES ('FIXED_DEPOSIT',$1,CURRENT_DATE,$2,'FD_CLOSURE','APPROVED') RETURNING activity_id`, activitySubtype, accountingPeriod).Scan(&activityID); err != nil {
		return err
	}
	totalDebit := roundToFour(netPayout + tds + penalty)
	totalCredit := roundToFour(principal + interest)
	if totalDebit != totalCredit {
		totalCredit = totalDebit
	}
	if err := tx.QueryRow(ctx, `
		INSERT INTO investment.accounting_journal_entry (
			activity_id, entity_id, entity_name, entry_date, accounting_period, entry_type,
			description, total_debit, total_credit, status, fd_id, closure_request_id,
			is_reversal, created_by
		) VALUES ($1,$2,$3,CURRENT_DATE,$4,'CLOSURE',$5,$6,$7,'POSTED',$8,$9,false,$10)
		RETURNING entry_id`,
		activityID, nullStrOrNil(entityID), nullStrOrNil(entityName), accountingPeriod,
		fmt.Sprintf("FD %s closure - %s", closureType, fdID), totalDebit, totalCredit, fdID, closureConfirmID, actorEmail,
	).Scan(&entryID); err != nil {
		return err
	}

	lineNum := 1
	insertLine := func(acctNum, acctName, acctType string, debit, credit float64, narration string) error {
		if debit == 0 && credit == 0 {
			return nil
		}
		_, e := tx.Exec(ctx, `INSERT INTO investment.accounting_journal_entry_line (entry_id,line_number,account_number,account_name,account_type,debit_amount,credit_amount,narration,fd_id) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
			entryID, lineNum, acctNum, acctName, acctType, roundToFour(debit), roundToFour(credit), narration, fdID)
		lineNum++
		return e
	}
	if err := insertLine(bankAccountNumber, bankAccountName, "ASSET", netPayout, 0, "Cash received on FD closure"); err != nil {
		return err
	}
	if err := insertLine(constants.TDSReceivable, constants.TDSReceivableLabel, "ASSET", tds, 0, "TDS withheld at source"); err != nil {
		return err
	}
	if err := insertLine("PENALTY-EXP", "Premature Withdrawal Penalty", "EXPENSE", penalty, 0, "Premature withdrawal penalty"); err != nil {
		return err
	}
	if err := insertLine(constants.FDInvestmentPrefix+fdID, constants.FormatFDInvestment+fdID, "ASSET", 0, principal, "Close FD investment asset"); err != nil {
		return err
	}
	interestCredit := roundToFour(totalCredit - principal)
	if err := insertLine(constants.FDInterestIncome+fdID, constants.FormatInterestIncome, "INCOME", 0, interestCredit, "Interest recognised on closure"); err != nil {
		return err
	}

	newFDStatus := "MATURED"
	if closureType == "PREMATURE" {
		newFDStatus = "PREMATURELY_CLOSED"
	}
	// Only fd_master carries closure-outcome status (MATURED /
	// PREMATURELY_CLOSED / ACTIVE-for-partial). fd_booking_request uses a
	// separate lifecycle vocabulary (DRAFT/APPROVAL_PENDING/APPROVED/
	// SENT_TO_BANK/CONFIRMED/ACTIVE/REJECTED/CANCELLED) enforced by
	// fd_booking_status_chk — pushing 'MATURED' or 'PREMATURELY_CLOSED' onto
	// it trips the constraint and (with silent errors) aborts the tx with
	// SQLSTATE 25P02. Closure-state lives on fd_master.
	_, err = tx.Exec(ctx, `UPDATE investment.fd_master SET fd_status=$1, closed_at=NOW(), closed_by=$2, accounting_posted=true, closure_request_id=$3, updated_by=$4, updated_at=NOW() WHERE fd_id=$5`,
		newFDStatus, actorEmail, closureConfirmID, actorEmail, fdID)
	if err != nil {
		return err
	}
	_ = bookingID // intentionally not status-flipped — see comment above
	_, err = tx.Exec(ctx, `UPDATE cimplr.fd_closure_confirm SET closure_status='POSTED', posting_status='POSTED', accounting_posted=true, journal_entry_id=$1 WHERE closure_confirm_id=$2`, entryID, closureConfirmID)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		UPDATE cimplr.fd_closure_initiate i
		SET closure_status='CONFIRM'
		FROM cimplr.fd_closure_confirm c
		WHERE c.closure_confirm_id=$1
		  AND c.closure_initiate_id=i.closure_initiate_id
		  AND c.closure_type='PREMATURE'
		  AND i.closure_type='PREMATURE'
		  AND COALESCE(i.is_deleted,false)=false`, closureConfirmID)
	if err != nil {
		return err
	}
	if err := insertCimplrConfirmAudit(ctx, tx, confirmAuditEntry{ConfirmID: closureConfirmID, InitiateID: "", Action: "POST", Status: "POSTED", Reason: firstNonEmpty(comment, "Journals posted on approval"), RequestedBy: actorEmail, Old: map[string]interface{}{"accounting_posted": false, "journal_entry_id": ""}}); err != nil {
		return err
	}
	return nil
}

func createCimplrRolloverBookingTx(ctx context.Context, tx pgx.Tx, closureConfirmID, actorEmail, comment string) error {

	var err error
	var (
		fdID, entityID, entityName, bankID, bankName, bankConfigID, sourceAccountID string
		frequencyID, tdsPlanID, dayCountCode, interestTypeCode                      string
		newBankID, newBankName, newAccountID, amountBasis                           string
		newFDAmount, newInterestRate                                                float64
		principal, interest, tds, netPayout                                         float64
		newTenorDays                                                                int
		expectedStart, expectedMaturity                                             time.Time
		accountingPosted                                                            bool
		originalBookingID                                                           string
	)
	err = tx.QueryRow(ctx, `
		SELECT c.fd_id, COALESCE(c.entity_id,''), COALESCE(c.entity_name,''),
		       COALESCE(m.bank_id,''), COALESCE(m.bank_name,''), COALESCE(b.bank_config_id,''),
		       COALESCE(b.source_account_id,''), COALESCE(b.frequency_id,''), COALESCE(b.tds_plan_id,''),
		       COALESCE(m.day_count_code,''), COALESCE(m.interest_type_code,'SIMPLE'),
		       COALESCE(rc.new_bank_id,''), COALESCE(rc.new_bank_name,''), COALESCE(rc.new_account_id,''),
		       rc.rollover_amount_basis, rc.new_fd_amount, rc.new_tenor_days,
		       COALESCE(rc.new_interest_rate, m.interest_rate), rc.expected_start_date,
		       COALESCE(rc.expected_maturity_date, rc.expected_start_date + (rc.new_tenor_days || ' days')::interval)::date,
		       COALESCE(c.principal_received, c.principal_expected, 0),
		       COALESCE(c.interest_received, c.interest_expected, 0),
		       COALESCE(c.tds_deducted, c.tds_expected, 0),
		       COALESCE(c.net_amount_received, c.net_expected, rc.new_fd_amount, 0),
		       c.accounting_posted, COALESCE(b.booking_id, '')
		FROM cimplr.fd_closure_confirm c
		JOIN cimplr.fd_closure_rollover_confirm rc ON rc.closure_confirm_id=c.closure_confirm_id AND rc.is_deleted=false
		JOIN investment.fd_master m ON m.fd_id=c.fd_id
		LEFT JOIN investment.fd_booking_request b ON b.booking_id=m.booking_id
		WHERE c.closure_confirm_id=$1 AND c.is_deleted=false
		FOR UPDATE OF c`, closureConfirmID,
	).Scan(&fdID, &entityID, &entityName, &bankID, &bankName, &bankConfigID, &sourceAccountID, &frequencyID, &tdsPlanID, &dayCountCode, &interestTypeCode, &newBankID, &newBankName, &newAccountID, &amountBasis, &newFDAmount, &newTenorDays, &newInterestRate, &expectedStart, &expectedMaturity, &principal, &interest, &tds, &netPayout, &accountingPosted, &originalBookingID)
	if err != nil {
		return err
	}
	if accountingPosted {
		return tx.Commit(ctx)
	}
	targetBankID := firstNonEmpty(newBankID, bankID)
	targetBankName := firstNonEmpty(newBankName, bankName)
	targetAccountID := firstNonEmpty(newAccountID, sourceAccountID)
	api.LogInfo("[CimplrRollover] confirm_id=%s fd_id=%s entityID=%q entityName=%q bankID=%q bankName=%q sourceAccountID=%q newAccountID=%q targetAccountID=%q newFDAmount=%v newTenorDays=%v newInterestRate=%v amountBasis=%q expectedStart=%v",
		closureConfirmID, fdID, entityID, entityName, bankID, bankName, sourceAccountID, newAccountID, targetAccountID, newFDAmount, newTenorDays, newInterestRate, amountBasis, expectedStart)
	if targetBankID == "" || targetBankName == "" || targetAccountID == "" || entityID == "" || entityName == "" {
		api.LogError("[CimplrRollover] FAILED validation confirm_id=%s: targetBankID=%q targetBankName=%q targetAccountID=%q entityID=%q entityName=%q",
			closureConfirmID, targetBankID, targetBankName, targetAccountID, entityID, entityName)
		return fmt.Errorf("rollover booking requires entity, bank and source account details (bankID=%q bankName=%q accountID=%q)", targetBankID, targetBankName, targetAccountID)
	}
	if newFDAmount <= 0 || newTenorDays <= 0 || newInterestRate <= 0 {
		api.LogError("[CimplrRollover] FAILED amount validation confirm_id=%s: newFDAmount=%v newTenorDays=%v newInterestRate=%v",
			closureConfirmID, newFDAmount, newTenorDays, newInterestRate)
		return fmt.Errorf("rollover booking requires positive amount, tenor and interest rate (amount=%v tenor=%v rate=%v)", newFDAmount, newTenorDays, newInterestRate)
	}
	if !expectedMaturity.After(expectedStart) {
		expectedMaturity = expectedStart.AddDate(0, 0, newTenorDays)
	}
	var sourceAccountNumber string
	var sourceAccountName string
	// See postCimplrClosureJournalsTx for the same reasoning — ErrNoRows is
	// expected (we fall back to defaults), but any other DB error must surface
	// so the dry-run / real post fails with the actual cause rather than the
	// downstream 25P02 mask.
	err = tx.QueryRow(ctx,
		`SELECT COALESCE(account_number,''), COALESCE(account_nickname,'')
		 FROM public.masterbankaccount
		 WHERE account_id=$1 LIMIT 1`, targetAccountID,
	).Scan(&sourceAccountNumber, &sourceAccountName)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("rollover settlement account lookup failed for account_id=%s: %w", targetAccountID, err)
	}
	if sourceAccountName == "" {
		sourceAccountName = "Rollover Settlement Account"
	}
	if interestTypeCode == "" {
		interestTypeCode = "SIMPLE"
	}

	now := time.Now()
	accountingPeriod := fmt.Sprintf("%d-%02d", now.Year(), now.Month())
	var activityID, entryID string
	if err := tx.QueryRow(ctx, `INSERT INTO investment.accounting_activity (activity_type,activity_subtype,effective_date,accounting_period,data_source,status) VALUES ('FIXED_DEPOSIT','FD_ROLLOVER',CURRENT_DATE,$1,'FD_CLOSURE','APPROVED') RETURNING activity_id`, accountingPeriod).Scan(&activityID); err != nil {
		return err
	}
	totalDebit := roundToFour(netPayout + tds)
	totalCredit := roundToFour(principal + interest)
	if totalDebit != totalCredit {
		totalCredit = totalDebit
	}
	if err := tx.QueryRow(ctx, `
		INSERT INTO investment.accounting_journal_entry (
			activity_id, entity_id, entity_name, entry_date, accounting_period, entry_type,
			description, total_debit, total_credit, status, fd_id, closure_request_id,
			is_reversal, created_by
		) VALUES ($1,$2,$3,CURRENT_DATE,$4,'CLOSURE',$5,$6,$7,'POSTED',$8,$9,false,$10)
		RETURNING entry_id`,
		activityID, nullStrOrNil(entityID), nullStrOrNil(entityName), accountingPeriod,
		fmt.Sprintf("FD ROLLOVER closure - %s", fdID), totalDebit, totalCredit, fdID, closureConfirmID, actorEmail,
	).Scan(&entryID); err != nil {
		return err
	}
	lineNum := 1
	insertLine := func(acctNum, acctName, acctType string, debit, credit float64, narration string) error {
		if debit == 0 && credit == 0 {
			return nil
		}
		_, e := tx.Exec(ctx, `INSERT INTO investment.accounting_journal_entry_line (entry_id,line_number,account_number,account_name,account_type,debit_amount,credit_amount,narration,fd_id) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
			entryID, lineNum, acctNum, acctName, acctType, roundToFour(debit), roundToFour(credit), narration, fdID)
		lineNum++
		return e
	}
	if err := insertLine(firstNonEmpty(sourceAccountNumber, targetAccountID), sourceAccountName, "ASSET", netPayout, 0, "Rollover closure settlement value"); err != nil {
		return err
	}
	if err := insertLine(constants.TDSReceivable, constants.TDSReceivableLabel, "ASSET", tds, 0, "TDS withheld at source on rollover"); err != nil {
		return err
	}
	if err := insertLine(constants.FDInvestmentPrefix+fdID, constants.FormatFDInvestment+fdID, "ASSET", 0, principal, "Close old FD investment asset on rollover"); err != nil {
		return err
	}
	interestCredit := roundToFour(totalCredit - principal)
	if err := insertLine(constants.FDInterestIncome+fdID, constants.FormatInterestIncome, "INCOME", 0, interestCredit, "Interest recognised on rollover closure"); err != nil {
		return err
	}

	// New FD booking from a rollover. Booking status is 'SENT_TO_BANK' — the
	// bank already has the instruction (the closure approval IS the
	// instruction); ops just needs to capture the bank's confirmation slip
	// from the booking workbench, after which booking_status moves to
	// CONFIRMED / ACTIVE through the normal booking flow. This matches the
	// legacy rollover path in closure.go (line ~2880).
	//
	// Allowed booking_status values per fd_booking_status_chk:
	//   DRAFT, APPROVAL_PENDING, APPROVED, SENT_TO_BANK,
	//   CONFIRMED, ACTIVE, REJECTED, CANCELLED.
	var newBookingID string
	err = tx.QueryRow(ctx, `
		INSERT INTO investment.fd_booking_request (
			entity_id, entity_name, bank_id, bank_name, bank_config_id,
			source_account_id, source_account_number, principal_amount, interest_rate,
			tenure_days, interest_type_code, frequency_id, day_count_code, tds_plan_id,
			expected_start_date, expected_maturity_date, value_date,
			booking_status, booking_remarks, source_closure_request_id, created_by
		) VALUES (
			$1,$2,$3,$4,NULLIF($5,''),$6,NULLIF($7,''),$8,$9,$10,$11,NULLIF($12,''),NULLIF($13,''),NULLIF($14,''),
			$15::date,$16::date,$17::date,'SENT_TO_BANK',$18,$19,$20
		) RETURNING booking_id`,
		entityID, entityName, targetBankID, targetBankName, bankConfigID, targetAccountID, sourceAccountNumber,
		roundToFour(newFDAmount), newInterestRate, newTenorDays, interestTypeCode, frequencyID, dayCountCode, tdsPlanID,
		expectedStart.Format(constants.DateFormat), expectedMaturity.Format(constants.DateFormat), expectedStart.Format(constants.DateFormat),
		fmt.Sprintf("Rollover booking (%s) - source confirm %s source FD %s", amountBasis, closureConfirmID, fdID),
		nil, actorEmail,
	).Scan(&newBookingID)
	if err != nil {
		// Surface constraint-violation hints so future schema drift gives an
		// actionable message instead of a raw Postgres error.
		if strings.Contains(err.Error(), "fd_booking_status_chk") {
			return fmt.Errorf("rollover new-FD insert rejected by fd_booking_status_chk — run `SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname='fd_booking_status_chk'` to see allowed values, then update createCimplrRolloverBookingTx accordingly: %w", err)
		}
		return fmt.Errorf("rollover new-FD booking insert failed for fd_id=%s: %w", fdID, err)
	}

	// Add journal lines showing the outflow for the new FD booking.
	//   DR New FD Investment  = newFDAmount  (new FD investment created)
	//   CR Settlement Account = newFDAmount  (cash reinvested from old FD)
	// These lines are self-balancing so they don't disturb the closure journal's balance.
	// Errors here MUST propagate — silently swallowing them poisons the tx with
	// SQLSTATE 25P02 on the next statement, rolls back the new fd_booking_request
	// insert above, and we lose the new FD entirely.
	if newBookingID != "" {
		if err := insertLine("FD-INVEST-NEW-"+newBookingID, "New FD Investment (Rollover)", "ASSET",
			roundToFour(newFDAmount), 0, "New FD booking from rollover — "+newBookingID); err != nil {
			return fmt.Errorf("rollover new-FD investment line insert failed: %w", err)
		}
		if err := insertLine(firstNonEmpty(sourceAccountNumber, targetAccountID), sourceAccountName, "ASSET",
			0, roundToFour(newFDAmount), "Cash reinvested into new FD rollover — "+newBookingID); err != nil {
			return fmt.Errorf("rollover settlement reinvest line insert failed: %w", err)
		}
	}

	_, err = tx.Exec(ctx, `UPDATE cimplr.fd_closure_rollover_confirm SET new_booking_id=$1, rollover_approval_status='APPROVED' WHERE closure_confirm_id=$2`, newBookingID, closureConfirmID)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE cimplr.fd_closure_confirm SET closure_status='POSTED', posting_status='POSTED', accounting_posted=true, journal_entry_id=$1, new_booking_id=$2 WHERE closure_confirm_id=$3`, entryID, newBookingID, closureConfirmID)
	if err != nil {
		return err
	}
	// fd_master carries the closure outcome ('ROLLED_OVER' / 'MATURED' /
	// 'PREMATURELY_CLOSED'). fd_booking_request has a separate lifecycle
	// vocabulary enforced by fd_booking_status_chk and does NOT include
	// 'ROLLED_OVER', so we do not touch the original booking's status here —
	// the closure linkage is reachable via fd_master.fd_status and via
	// source_closure_request_id on the new booking.
	_, err = tx.Exec(ctx, `UPDATE investment.fd_master SET fd_status='ROLLED_OVER', closed_at=NOW(), closed_by=$1, accounting_posted=true, closure_request_id=$2, updated_by=$3, updated_at=NOW() WHERE fd_id=$4`, actorEmail, closureConfirmID, actorEmail, fdID)
	if err != nil {
		return err
	}
	_ = originalBookingID // intentionally not status-flipped — see comment above
	if err := insertCimplrConfirmAudit(ctx, tx, confirmAuditEntry{ConfirmID: closureConfirmID, InitiateID: "", Action: "POST", Status: "POSTED", Reason: firstNonEmpty(comment, "Rollover journal and booking created on approval"), RequestedBy: actorEmail, Old: map[string]interface{}{"accounting_posted": false, "journal_entry_id": "", "new_booking_id": ""}}); err != nil {
		return err
	}
	return nil
}

func loadCimplrFDSource(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...interface{}) pgx.Row
}, fdID string) (cimplrFDSource, error) {
	var src cimplrFDSource
	err := q.QueryRow(ctx, `
		SELECT m.fd_id, COALESCE(m.booking_id,''), COALESCE(m.confirmation_id,''),
		       COALESCE(b.entity_id,''), COALESCE(b.entity_name,''),
		       COALESCE(m.bank_id,''), COALESCE(m.bank_name,''),
		       COALESCE(m.bank_fd_ref_no, m.fd_id, ''), COALESCE(m.bank_fd_ref_no,''),
		       COALESCE(m.principal_amount,0), COALESCE(m.interest_rate,0),
		       COALESCE(m.interest_type_code,'SIMPLE'), m.maturity_date, m.start_date,
		       COALESCE(m.tenure_days,0), COALESCE(m.day_count_code,''),
		       COALESCE(b.frequency_id,''), COALESCE(b.tds_plan_id,''),
		       COALESCE(b.bank_config_id,''), COALESCE(b.source_account_id,'')
		FROM investment.fd_master m
		LEFT JOIN investment.fd_booking_request b ON b.booking_id=m.booking_id
		WHERE m.fd_id=$1 AND COALESCE(m.is_deleted,false)=false`,
		fdID,
	).Scan(&src.FDID, &src.BookingID, &src.ConfirmationID, &src.EntityID, &src.EntityName, &src.BankID, &src.BankName, &src.FDRefNo, &src.BankFDRefNo, &src.Principal, &src.InterestRate, &src.InterestTypeCode, &src.MaturityDate, &src.StartDate, &src.TenureDays, &src.DayCountCode, &src.FrequencyID, &src.TDSPlanID, &src.BankConfigID, &src.SourceAccountID)
	return src, err
}

func cimplrResolvePenalty(ctx context.Context, pool *pgxpool.Pool, src cimplrFDSource, accruedDays int, accruedInterest float64) (penaltyID, penaltyType string, penaltyValue, penaltyAmount float64, noInterest bool, applicable bool) {
	var minHeldDays int
	err := pool.QueryRow(ctx, `
		SELECT COALESCE(penalty_id,''), COALESCE(penalty_type,''), COALESCE(penalty_value,0),
		       COALESCE(no_interest_if_withdrawn_before,0)
		FROM investment.fd_penalty_structure_master
		WHERE bank_code=$1 AND COALESCE(is_deleted,false)=false
		  AND (effective_from IS NULL OR effective_from <= CURRENT_DATE)
		  AND (effective_to IS NULL OR effective_to >= CURRENT_DATE)
		  AND (min_held_days IS NULL OR $2 >= min_held_days)
		  AND (max_held_days IS NULL OR $2 <= max_held_days)
		  AND (min_amount_range IS NULL OR $3 >= min_amount_range)
		  AND (max_amount_range IS NULL OR $3 <= max_amount_range)
		ORDER BY COALESCE(min_held_days,0) DESC, penalty_value DESC
		LIMIT 1`,
		src.BankID, accruedDays, src.Principal,
	).Scan(&penaltyID, &penaltyType, &penaltyValue, &minHeldDays)
	if err != nil || penaltyID == "" {
		return "", "", 0, 0, false, false
	}
	applicable = true
	noInterest = minHeldDays > 0 && accruedDays < minHeldDays
	revisedInterest := accruedInterest
	if noInterest {
		revisedInterest = 0
	}
	switch strings.ToUpper(penaltyType) {
	case "FLAT_AMOUNT":
		penaltyAmount = roundToFour(penaltyValue)
	case "RATE_REDUCTION":
		rate := roundToFour(src.InterestRate - penaltyValue)
		if rate < 0 {
			rate = 0
		}
		if accruedDays > 0 && src.Principal > 0 {
			revisedInterest = roundToFour(src.Principal * rate * float64(accruedDays) / 36500)
		}
		if noInterest {
			revisedInterest = 0
		}
		penaltyAmount = roundToFour(accruedInterest - revisedInterest)
		if penaltyAmount < 0 {
			penaltyAmount = 0
		}
	default:
		penaltyAmount = roundToFour(revisedInterest * penaltyValue / 100)
	}
	return penaltyID, penaltyType, penaltyValue, penaltyAmount, noInterest, applicable
}

func cimplrTDSTill(ctx context.Context, pool *pgxpool.Pool, src cimplrFDSource, periodStart, periodEnd time.Time) float64 {
	fdID := src.FDID
	if fdID == "" || periodStart.IsZero() || periodEnd.IsZero() {
		return 0
	}
	var tds float64

	type scheduleTDSRow struct {
		eventType string
		eventDate time.Time
		tds       float64
	}
	rows, err := pool.Query(ctx, `
		SELECT COALESCE(event_type,''), event_date::date, COALESCE(tds_amount,0)
		FROM investment.fd_cashflow_schedule
		WHERE fd_id=$1 AND COALESCE(is_deleted,false)=false
		  AND event_date >= $2::date AND event_date <= $3::date
		ORDER BY event_date, id`,
		fdID, periodStart, periodEnd,
	)
	if err == nil {
		defer rows.Close()
		var scheduleRows []scheduleTDSRow
		for rows.Next() {
			var r scheduleTDSRow
			if scanErr := rows.Scan(&r.eventType, &r.eventDate, &r.tds); scanErr != nil {
				continue
			}
			scheduleRows = append(scheduleRows, r)
		}
		if len(scheduleRows) > 0 {
			for _, r := range scheduleRows {
				// ACCRUAL.tds is provisional — never count it as actually deducted.
				// Only include TDS from events where cash actually changes hands.
				if r.eventType == "ACCRUAL" {
					continue
				}
				tds += r.tds
			}
			if tds != 0 {
				return roundToFour(tds)
			}
		}
	}

	_ = pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(tds_deducted_in_period),0)
		FROM investment.fd_accrual_ledger
		WHERE fd_id=$1 AND COALESCE(is_deleted,false)=false
		  AND period_end >= $2::date AND period_end <= $3::date`, fdID, periodStart, periodEnd,
	).Scan(&tds)
	if tds == 0 {
		_ = pool.QueryRow(ctx, `
			SELECT COALESCE(SUM(tds_amount),0)
			FROM investment.fd_cashflow_schedule
			WHERE fd_id=$1 AND COALESCE(is_deleted,false)=false
			  AND event_date >= $2::date AND event_date <= $3::date
			  AND event_type != 'ACCRUAL'`,
			fdID, periodStart, periodEnd,
		).Scan(&tds)
	}
	return roundToFour(tds)
}

func cimplrAccruedInterestTill(ctx context.Context, pool *pgxpool.Pool, src cimplrFDSource, asOf time.Time) (accrued, tds float64) {
	if src.FDID == "" || src.StartDate.IsZero() || asOf.Before(src.StartDate) {
		return 0, 0
	}
	periodEnd := asOf
	if !src.MaturityDate.IsZero() && periodEnd.After(src.MaturityDate) {
		periodEnd = src.MaturityDate
	}
	if interest, _, found := fdMaster.PeriodInterestFromSchedule(ctx, pool, src.FDID, src.StartDate, periodEnd, src.InterestTypeCode); found && interest > 0 {
		accrued = roundToFour(interest)
	}
	if accrued == 0 {
		_ = pool.QueryRow(ctx, `
			SELECT COALESCE(SUM(period_interest_accrued),0), COALESCE(SUM(tds_deducted_in_period),0)
			FROM investment.fd_accrual_ledger
			WHERE fd_id=$1 AND COALESCE(is_deleted,false)=false
			  AND period_end <= $2::date`, src.FDID, periodEnd,
		).Scan(&accrued, &tds)
	}
	if accrued == 0 {
		_ = pool.QueryRow(ctx, `
			SELECT COALESCE(SUM(interest_accrued),0), COALESCE(SUM(tds_amount),0)
			FROM investment.fd_cashflow_schedule
			WHERE fd_id=$1 AND COALESCE(is_deleted,false)=false
			  AND event_date >= $2::date AND event_date <= $3::date`,
			src.FDID, src.StartDate, periodEnd,
		).Scan(&accrued, &tds)
	}
	if accrued == 0 && src.Principal > 0 && src.InterestRate > 0 {
		days := int(periodEnd.Sub(src.StartDate).Hours() / 24)
		if days < 0 {
			days = 0
		}
		if days > 0 {
			divisor := 36500.0
			if strings.Contains(strings.ToUpper(src.DayCountCode), "360") {
				divisor = 36000.0
			}
			accrued = roundToFour(src.Principal * src.InterestRate * float64(days) / divisor)
		}
	}
	if tds == 0 {
		tds = cimplrTDSTill(ctx, pool, src, src.StartDate, periodEnd)
	}
	return roundToFour(accrued), roundToFour(tds)
}

func cimplrDefaultCalcDate(src cimplrFDSource, closureType, requestedDate string) string {
	if t, ok := parseCimplrDate(requestedDate); ok {
		return t.Format(constants.DateFormat)
	}
	ct := strings.ToUpper(strings.TrimSpace(closureType))
	if (ct == "ROLLOVER" || ct == "PAYOUT") && !src.MaturityDate.IsZero() {
		return src.MaturityDate.Format(constants.DateFormat)
	}
	return ""
}

func validateCimplrMaturityTiming(src cimplrFDSource, closureType string) error {
	ct := strings.ToUpper(strings.TrimSpace(closureType))
	if ct != "PAYOUT" && ct != "ROLLOVER" {
		return nil
	}
	if src.MaturityDate.IsZero() {
		return nil
	}
	today := time.Now().Truncate(24 * time.Hour)
	mat := src.MaturityDate.Truncate(24 * time.Hour)
	if mat.After(today) {
		return fmt.Errorf(
			"Payout and rollover are only allowed on or after maturity date. Use Premature Closure before maturity",
		)
	}
	return nil
}

func calculateCimplrClosure(ctx context.Context, pool *pgxpool.Pool, src cimplrFDSource, closureType, requestedDate string, enforceMaturityTiming bool) (cimplrClosureCalc, error) {
	ct := strings.ToUpper(strings.TrimSpace(closureType))
	if enforceMaturityTiming {
		if err := validateCimplrMaturityTiming(src, ct); err != nil {
			return cimplrClosureCalc{}, err
		}
	}
	calcDate := time.Now()
	if t, ok := parseCimplrDate(requestedDate); ok {
		calcDate = t
	}
	if (ct == "PAYOUT" || ct == "ROLLOVER") && !src.MaturityDate.IsZero() {
		calcDate = src.MaturityDate
	}
	accruedDays := int(calcDate.Sub(src.StartDate).Hours() / 24)
	if accruedDays < 0 {
		accruedDays = 0
	}
	accrued, tds := cimplrAccruedInterestTill(ctx, pool, src, calcDate)
	revisedInterest := accrued
	calc := cimplrClosureCalc{
		ClosureType:           closureType,
		CalculationDate:       calcDate,
		AccruedDays:           accruedDays,
		AccruedInterest:       accrued,
		TDSAmount:             tds,
		ApplicableRate:        src.InterestRate,
		ExpectedMaturityValue: roundToFour(src.Principal + accrued),
		RevisedInterestAmount: revisedInterest,
		RevisedMaturityValue:  roundToFour(src.Principal + revisedInterest),
		NetPayout:             roundToFour(src.Principal + revisedInterest - tds),
	}
	if ct == "PREMATURE" {
		pID, pType, pVal, pAmt, noInt, pApp := cimplrResolvePenalty(ctx, pool, src, accruedDays, accrued)
		calc.PenaltyID = pID
		calc.PenaltyType = pType
		calc.PenaltyValue = pVal
		calc.PenaltyAmount = pAmt
		calc.NoInterestFlag = noInt
		calc.PenaltyApplicable = pApp
		if noInt {
			calc.RevisedInterestAmount = 0
		} else {
			calc.RevisedInterestAmount = revisedInterest
			if pType == "RATE_REDUCTION" && pApp {
				rate := roundToFour(src.InterestRate - pVal)
				if rate < 0 {
					rate = 0
				}
				calc.ApplicableRate = rate
				if accruedDays > 0 {
					divisor := 36500.0
					if strings.Contains(strings.ToUpper(src.DayCountCode), "360") {
						divisor = 36000.0
					}
					calc.RevisedInterestAmount = roundToFour(src.Principal * rate * float64(accruedDays) / divisor)
				}
			}
		}
		calc.RevisedMaturityValue = roundToFour(src.Principal + calc.RevisedInterestAmount - calc.PenaltyAmount)
		calc.NetPayout = roundToFour(src.Principal + calc.RevisedInterestAmount - calc.TDSAmount - calc.PenaltyAmount)
		calc.ExpectedMaturityValue = calc.RevisedMaturityValue
	}
	return calc, nil
}

func persistCimplrInitiateVariances(ctx context.Context, pool *pgxpool.Pool, recordID string, req cimplrClosureInitiateRequest, src cimplrFDSource, calc cimplrClosureCalc) (map[string]interface{}, error) {
	runID := varianceengine.NewRunID()
	ff := func(v float64) string { return strconv.FormatFloat(roundToFour(v), 'f', 4, 64) }
	rules := []varianceengine.Rule{
		{FieldName: "principal_amount", VarianceType: varianceengine.TypeAmount, ExpectedValue: ff(src.Principal), ActualValue: ff(chooseFloat(req.PrincipalAmount, src.Principal)), Priority: varianceengine.PriorityHigh, Tolerance: 0.01},
		{FieldName: "interest_rate", VarianceType: varianceengine.TypeRate, ExpectedValue: ff(src.InterestRate), ActualValue: ff(src.InterestRate), Priority: varianceengine.PriorityHigh, Tolerance: 0.001},
		{FieldName: "expected_maturity_value", VarianceType: varianceengine.TypeAmount, ExpectedValue: ff(calc.ExpectedMaturityValue), ActualValue: ff(chooseFloat(req.ExpectedMaturityValue, calc.ExpectedMaturityValue)), Priority: varianceengine.PriorityMedium, Tolerance: 1.0},
		{FieldName: "accrued_interest_till_date", VarianceType: varianceengine.TypeAmount, ExpectedValue: ff(calc.AccruedInterest), ActualValue: ff(chooseFloat(req.AccruedInterestTillDate, calc.AccruedInterest)), Priority: varianceengine.PriorityMedium, Tolerance: 1.0},
		{FieldName: "tds_expected", VarianceType: varianceengine.TypeAmount, ExpectedValue: ff(calc.TDSAmount), ActualValue: ff(chooseFloat(req.TDSExpected, calc.TDSAmount)), Priority: varianceengine.PriorityMedium, Tolerance: 0.5},
		{FieldName: "net_expected_amount", VarianceType: varianceengine.TypeAmount, ExpectedValue: ff(calc.NetPayout), ActualValue: ff(chooseFloat(req.NetExpectedAmount, calc.NetPayout)), Priority: varianceengine.PriorityHigh, Tolerance: 1.0},
	}
	items := varianceengine.Compare("FD_CLOSURE", recordID, src.EntityID, runID, rules)
	_ = varianceengine.AutoResolveCleared(ctx, pool, recordID, items, req.UserID, getUserEmail(ctx))
	if err := varianceengine.PersistVariances(ctx, pool, items); err != nil {
		return nil, err
	}
	if err := updateCimplrVarianceFlags(ctx, pool, constants.QuerryClosureInitiate, "closure_initiate_id", recordID, runID, items); err != nil {
		return nil, err
	}
	return cimplrVarianceSummary(runID, items), nil
}

// updateCimplrVarianceFlags stamps variance flags on cimplr closure tables.
// cimplr.fd_closure_initiate / fd_closure_confirm use variance_run_id (not
// last_variance_run_id) and have no last_validated_at column — the generic
// varianceengine.UpdateRecordFlags targets legacy investment.* tables and
// breaks auto-maturity when its UPDATE aborts the surrounding tx.
func updateCimplrVarianceFlags(ctx context.Context, exec varianceengine.QueryExecutor, table, pkCol, pkVal, runID string, items []varianceengine.VarianceItem) error {
	hasAny := false
	hasUnresolved := false
	for _, item := range items {
		if item.HasVariance {
			hasAny = true
			if item.Status == varianceengine.StatusOpen {
				hasUnresolved = true
			}
		}
	}
	sql := fmt.Sprintf(
		`UPDATE %s SET has_variance=$1, has_unresolved_variance=$2, variance_run_id=$3 WHERE %s=$4`,
		table, pkCol)
	_, err := exec.Exec(ctx, sql, hasAny, hasUnresolved, runID, pkVal)
	return err
}

// buildCimplrConfirmVarianceRules is the single source of truth for which fields
// the variance engine compares on a confirm record. Both persist (post-commit) and
// preview (validate) paths call into this so the expected baselines stay in sync.
//
// PREMATURE special case: revised_interest_amount (after penalty/rate cap) is the
// figure the user actually sees and types into interest_received. Comparing against
// calc.AccruedInterest there produces a false-positive variance on every premature
// row that has a penalty applied. Branch the baseline explicitly.
func buildCimplrConfirmVarianceRules(req cimplrClosureConfirmRequest, src cimplrFDSource, calc cimplrClosureCalc) []varianceengine.Rule {
	ff := func(v float64) string { return strconv.FormatFloat(roundToFour(v), 'f', 4, 64) }
	interestBaseline := calc.AccruedInterest
	if calc.ClosureType == "PREMATURE" {
		interestBaseline = calc.RevisedInterestAmount
	}
	rules := []varianceengine.Rule{
		{FieldName: "principal_received", VarianceType: varianceengine.TypeAmount, ExpectedValue: ff(src.Principal), ActualValue: ff(chooseFloat(req.PrincipalReceived, src.Principal)), Priority: varianceengine.PriorityHigh, Tolerance: 0.01},
		{FieldName: "interest_received", VarianceType: varianceengine.TypeAmount, ExpectedValue: ff(interestBaseline), ActualValue: ff(chooseFloat(req.InterestReceived, interestBaseline)), Priority: varianceengine.PriorityMedium, Tolerance: 1.0},
		{FieldName: "tds_deducted", VarianceType: varianceengine.TypeAmount, ExpectedValue: ff(calc.TDSAmount), ActualValue: ff(chooseFloat(req.TDSDeducted, calc.TDSAmount)), Priority: varianceengine.PriorityMedium, Tolerance: 0.5},
		{FieldName: "net_amount_received", VarianceType: varianceengine.TypeAmount, ExpectedValue: ff(calc.NetPayout), ActualValue: ff(chooseFloat(req.NetAmountReceived, calc.NetPayout)), Priority: varianceengine.PriorityHigh, Tolerance: 1.0},
	}
	if calc.ClosureType == "PREMATURE" {
		rules = append(rules,
			varianceengine.Rule{FieldName: "penalty_amount", VarianceType: varianceengine.TypeAmount, ExpectedValue: ff(calc.PenaltyAmount), ActualValue: ff(chooseFloat(req.PenaltyAmount, calc.PenaltyAmount)), Priority: varianceengine.PriorityHigh, Tolerance: 0.01},
			varianceengine.Rule{FieldName: "applicable_rate", VarianceType: varianceengine.TypeRate, ExpectedValue: ff(calc.ApplicableRate), ActualValue: ff(chooseFloat(req.ApplicableRate, calc.ApplicableRate)), Priority: varianceengine.PriorityHigh, Tolerance: 0.001},
		)
	}
	if calc.ClosureType == "ROLLOVER" {
		rules = append(rules,
			varianceengine.Rule{FieldName: "new_fd_amount", VarianceType: varianceengine.TypeAmount, ExpectedValue: ff(cimplrExpectedRolloverNewFD(src, calc, req.RolloverAmountBasis)), ActualValue: ff(req.NewFDAmount), Priority: varianceengine.PriorityHigh, Tolerance: 1.0},
			varianceengine.Rule{FieldName: "new_interest_rate", VarianceType: varianceengine.TypeRate, ExpectedValue: ff(src.InterestRate), ActualValue: ff(chooseFloat(req.NewInterestRate, src.InterestRate)), Priority: varianceengine.PriorityHigh, Tolerance: 0.001},
		)
	}
	return rules
}

// cimplrAssertConfirmCreateAllowed pre-computes variance from the request payload
// and refuses creation/edit when there's an unresolved variance unless the caller
// explicitly accepts it via resolution_action='ACCEPT'. This stops zombie records
// (unresolved variance + no acceptance) from ever entering the approval queue.
//
// Returns the variance summary either way so the caller can include it in the
// 422 response body — frontend uses it to render the variance acceptance table.
func cimplrAssertConfirmCreateAllowed(req cimplrClosureConfirmRequest, src cimplrFDSource, calc cimplrClosureCalc, recordID string) (map[string]interface{}, string) {
	summary := previewCimplrConfirmVariance(recordID, req, src, calc)
	openCount, _ := summary["open_count"].(int)
	if openCount <= 0 {
		return summary, ""
	}
	if strings.ToUpper(strings.TrimSpace(req.ResolutionAction)) == "ACCEPT" {
		return summary, ""
	}
	return summary, fmt.Sprintf("%d variance(s) detected — fix the amounts to match the calculated values, or send resolution_action='ACCEPT' (with remarks) to acknowledge the variance.", openCount)
}

func cimplrMapString(m map[string]interface{}, key string) string {
	if m == nil {
		return ""
	}
	v := strings.TrimSpace(fmt.Sprint(m[key]))
	if v == "" || v == "<nil>" {
		return ""
	}
	return v
}

func ensureCimplrPrematureInitiate(ctx context.Context, exec dbExec, existingInitiateID, confirmID string, src cimplrFDSource, req cimplrClosureConfirmRequest, calc cimplrClosureCalc) (string, error) {
	existingInitiateID = strings.TrimSpace(existingInitiateID)
	if existingInitiateID != "" && existingInitiateID != "<nil>" {
		return existingInitiateID, nil
	}

	var closureInitiateID string
	err := exec.QueryRow(ctx, `
		INSERT INTO cimplr.fd_closure_initiate (
			fd_id, booking_id, confirmation_id, entity_id, entity_name,
			bank_id, bank_name, fd_ref_no, bank_fd_ref_no,
			closure_type, closure_status, action_at_maturity,
			maturity_date, requested_closure_date, principal_amount,
			interest_type_code, interest_rate, expected_maturity_value,
			accrued_interest_till_date, tds_expected, net_expected_amount,
			maturity_status, action_required, remarks
		) VALUES (
			$1,$2,$3,$4,$5,$6,$7,$8,$9,
			'PREMATURE','INITIATE',NULL,
			$10::date,$11::date,$12,$13,$14,$15,$16,$17,$18,
			$19,true,$20
		) RETURNING closure_initiate_id`,
		src.FDID, nullStrOrNil(src.BookingID), nullStrOrNil(src.ConfirmationID), nullStrOrNil(src.EntityID), nullStrOrNil(src.EntityName),
		nullStrOrNil(src.BankID), nullStrOrNil(src.BankName), nullStrOrNil(src.FDRefNo), nullStrOrNil(src.BankFDRefNo),
		src.MaturityDate.Format(constants.DateFormat), nullDateArg(firstNonEmpty(req.RequestedClosureDate, calc.CalculationDate.Format(constants.DateFormat))),
		src.Principal, nullStrOrNil(src.InterestTypeCode), src.InterestRate,
		calc.ExpectedMaturityValue, calc.RevisedInterestAmount, calc.TDSAmount, calc.NetPayout,
		deriveCimplrMaturityStatus(src.MaturityDate), req.Remarks,
	).Scan(&closureInitiateID)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(confirmID) != "" {
		if _, err := exec.Exec(ctx, `
			UPDATE cimplr.fd_closure_confirm
			SET closure_initiate_id=$1
			WHERE closure_confirm_id=$2 AND COALESCE(is_deleted,false)=false`,
			closureInitiateID, confirmID,
		); err != nil {
			return "", err
		}
	}
	return closureInitiateID, nil
}

func persistCimplrConfirmVariances(ctx context.Context, pool varianceengine.QueryExecutor, recordID string, req cimplrClosureConfirmRequest, src cimplrFDSource, calc cimplrClosureCalc) (map[string]interface{}, error) {
	runID := varianceengine.NewRunID()
	rules := buildCimplrConfirmVarianceRules(req, src, calc)
	items := varianceengine.Compare("FD_CLOSURE", recordID, src.EntityID, runID, rules)
	_ = varianceengine.AutoResolveCleared(ctx, pool, recordID, items, req.UserID, getUserEmail(ctx))
	if err := varianceengine.PersistVariances(ctx, pool, items); err != nil {
		return nil, err
	}
	if err := updateCimplrVarianceFlags(ctx, pool, constants.QuerryAuditClosureConfirm, "closure_confirm_id", recordID, runID, items); err != nil {
		return nil, err
	}
	return cimplrVarianceSummary(runID, items), nil
}

func cimplrVarianceItemsToMaps(items []varianceengine.VarianceItem) []map[string]interface{} {
	out := make([]map[string]interface{}, 0)
	for _, it := range items {
		if !it.HasVariance {
			continue
		}
		out = append(out, map[string]interface{}{
			"variance_id":    it.VarianceID,
			"field_name":     it.FieldName,
			"variance_type":  it.VarianceType,
			"expected_value": it.ExpectedValue,
			"actual_value":   it.ActualValue,
			"delta":          it.VarianceDelta,
			"variance_delta": it.VarianceDelta,
			"priority":       it.Priority,
			"status":         it.Status,
			"system_comment": it.SystemComment,
			"has_variance":   it.HasVariance,
		})
	}
	return out
}

func cimplrVarianceSummary(runID string, items []varianceengine.VarianceItem) map[string]interface{} {
	openCount := 0
	for _, it := range items {
		if it.HasVariance && it.Status == varianceengine.StatusOpen {
			openCount++
		}
	}
	itemMaps := cimplrVarianceItemsToMaps(items)
	return map[string]interface{}{
		"run_id":         runID,
		"variance_count": countVariances(items),
		"open_count":     openCount,
		"has_variance":   openCount > 0,
		"items":          itemMaps,
		"total":          len(itemMaps),
	}
}

func insertCimplrCalculation(ctx context.Context, exec dbExec, closureInitiateID, closureConfirmID string, src cimplrFDSource, calc cimplrClosureCalc) error {
	_, err := exec.Exec(ctx, `
		INSERT INTO cimplr.fd_closure_calculation (
			closure_initiate_id, closure_confirm_id, fd_id, calculation_type, calculation_date,
			period_start, period_end, day_count_convention, principal_amount, interest_rate,
			accrued_days, accrued_interest, tds_amount, penalty_id, penalty_type, penalty_value,
			penalty_amount, applicable_rate, no_interest_flag, expected_maturity_value,
			revised_interest_amount, revised_maturity_value, net_payout, rounding_rule, precision_decimals
		) VALUES (
			NULLIF($1,''), NULLIF($2,''), $3, $4, $5::date, $6::date, $7::date, NULLIF($8,''), $9, $10,
			$11,$12,$13,NULLIF($14,''),NULLIF($15,''),$16,$17,$18,$19,$20,$21,$22,$23,'ROUND_HALF_UP',4
		)`,
		closureInitiateID, closureConfirmID, src.FDID, calc.ClosureType, calc.CalculationDate.Format(constants.DateFormat),
		src.StartDate.Format(constants.DateFormat), calc.CalculationDate.Format(constants.DateFormat), src.DayCountCode, src.Principal, src.InterestRate,
		calc.AccruedDays, calc.AccruedInterest, calc.TDSAmount, calc.PenaltyID, calc.PenaltyType, calc.PenaltyValue,
		calc.PenaltyAmount, calc.ApplicableRate, calc.NoInterestFlag, calc.ExpectedMaturityValue,
		calc.RevisedInterestAmount, calc.RevisedMaturityValue, calc.NetPayout,
	)
	return err
}

func upsertCimplrPrematureConfirm(ctx context.Context, exec dbExec, closureConfirmID string, src cimplrFDSource, req cimplrClosureConfirmRequest, calc cimplrClosureCalc) error {
	penaltyApplicable := calc.PenaltyAmount > 0
	if req.PenaltyApplicable != nil {
		penaltyApplicable = *req.PenaltyApplicable
	}
	_, err := exec.Exec(ctx, `
		INSERT INTO cimplr.fd_closure_premature_confirm (
			closure_confirm_id, fd_id, requested_closure_date, premature_reason, days_held,
			original_interest_rate, contracted_rate, applicable_rate, penalty_applicable,
			penalty_id, penalty_type, penalty_value, penalty_amount, no_interest_flag,
			revised_interest_amount, revised_maturity_value, tds_deducted, net_payout,
			bank_reference_no, remarks
		) VALUES (
			$1,$2,COALESCE($3::date,CURRENT_DATE),$4,$5,$6,$7,$8,$9,NULLIF($10,''),NULLIF($11,''),$12,$13,$14,$15,$16,$17,$18,NULLIF($19,''),$20
		)
		ON CONFLICT (closure_confirm_id) WHERE is_deleted=false DO UPDATE SET
			requested_closure_date=EXCLUDED.requested_closure_date,
			premature_reason=EXCLUDED.premature_reason,
			days_held=EXCLUDED.days_held,
			contracted_rate=EXCLUDED.contracted_rate,
			applicable_rate=EXCLUDED.applicable_rate,
			penalty_applicable=EXCLUDED.penalty_applicable,
			penalty_id=EXCLUDED.penalty_id,
			penalty_type=EXCLUDED.penalty_type,
			penalty_value=EXCLUDED.penalty_value,
			penalty_amount=EXCLUDED.penalty_amount,
			no_interest_flag=EXCLUDED.no_interest_flag,
			revised_interest_amount=EXCLUDED.revised_interest_amount,
			revised_maturity_value=EXCLUDED.revised_maturity_value,
			tds_deducted=EXCLUDED.tds_deducted,
			net_payout=EXCLUDED.net_payout,
			bank_reference_no=EXCLUDED.bank_reference_no,
			remarks=EXCLUDED.remarks`,
		closureConfirmID, src.FDID, nullDateArg(req.RequestedClosureDate), firstNonEmpty(req.PrematureReason, "Premature FD closure"),
		chooseInt(req.DaysHeld, calc.AccruedDays), src.InterestRate, chooseFloat(req.ContractedRate, src.InterestRate), chooseFloat(req.ApplicableRate, calc.ApplicableRate),
		penaltyApplicable, firstNonEmpty(req.PenaltyID, calc.PenaltyID), firstNonEmpty(strings.ToUpper(req.PenaltyType), calc.PenaltyType), chooseFloat(req.PenaltyValue, calc.PenaltyValue),
		chooseFloat(req.PenaltyAmount, calc.PenaltyAmount), req.NoInterestFlag || calc.NoInterestFlag, chooseFloat(req.RevisedInterestAmount, calc.RevisedInterestAmount),
		chooseFloat(req.RevisedMaturityValue, calc.RevisedMaturityValue), chooseFloat(req.TDSDeducted, calc.TDSAmount), chooseFloat(req.NetPayout, calc.NetPayout),
		req.BankReferenceNo, req.Remarks,
	)
	return err
}

func upsertCimplrRolloverConfirm(ctx context.Context, exec dbExec, closureConfirmID string, src cimplrFDSource, req cimplrClosureConfirmRequest, calc cimplrClosureCalc) error {
	startDate := req.ExpectedStartDate
	if startDate == "" {
		startDate = time.Now().Format(constants.DateFormat)
	}
	tenor := chooseInt(req.NewTenorDays, src.TenureDays)
	maturityDate := req.ExpectedMaturityDate
	if maturityDate == "" {
		if st, ok := parseCimplrDate(startDate); ok {
			maturityDate = st.AddDate(0, 0, tenor).Format(constants.DateFormat)
		}
	}
	_, err := exec.Exec(ctx, `
		INSERT INTO cimplr.fd_closure_rollover_confirm (
			closure_confirm_id, old_fd_id, rollover_amount_basis, old_principal,
			interest_accrued, tds_deducted, closure_amount, new_bank_id, new_bank_name,
			new_account_id, new_fd_amount, new_tenor_days, new_interest_rate,
			expected_start_date, expected_maturity_date, rate_determination,
			rollover_approval_status, new_fd_reference_no, remarks
		) VALUES (
			$1,$2,$3,$4,$5,$6,$7,NULLIF($8,''),NULLIF($9,''),NULLIF($10,''),$11,$12,$13,$14::date,$15::date,NULLIF($16,''),NULLIF($17,''),NULLIF($18,''),$19
		)
		ON CONFLICT (closure_confirm_id) WHERE is_deleted=false DO UPDATE SET
			rollover_amount_basis=EXCLUDED.rollover_amount_basis,
			interest_accrued=EXCLUDED.interest_accrued,
			tds_deducted=EXCLUDED.tds_deducted,
			closure_amount=EXCLUDED.closure_amount,
			new_bank_id=EXCLUDED.new_bank_id,
			new_bank_name=EXCLUDED.new_bank_name,
			new_account_id=EXCLUDED.new_account_id,
			new_fd_amount=EXCLUDED.new_fd_amount,
			new_tenor_days=EXCLUDED.new_tenor_days,
			new_interest_rate=EXCLUDED.new_interest_rate,
			expected_start_date=EXCLUDED.expected_start_date,
			expected_maturity_date=EXCLUDED.expected_maturity_date,
			rate_determination=EXCLUDED.rate_determination,
			rollover_approval_status=EXCLUDED.rollover_approval_status,
			new_fd_reference_no=EXCLUDED.new_fd_reference_no,
			remarks=EXCLUDED.remarks`,
		closureConfirmID, src.FDID, firstNonEmpty(strings.ToUpper(req.RolloverAmountBasis), "PRINCIPAL_PLUS_INTEREST"),
		src.Principal, calc.AccruedInterest, calc.TDSAmount, chooseFloat(req.ClosureAmount, calc.NetPayout),
		req.NewBankID, req.NewBankName, req.NewAccountID, chooseFloat(req.NewFDAmount, calc.NetPayout),
		tenor, chooseFloat(req.NewInterestRate, src.InterestRate), startDate, maturityDate,
		strings.ToUpper(req.RateDetermination), firstNonEmpty(strings.ToUpper(req.RolloverApprovalStatus), "PENDING"), req.NewFDReferenceNo, req.Remarks,
	)
	return err
}

type initiateAuditEntry struct {
	ID          string
	Action      string
	Status      string
	Reason      string
	RequestedBy string
	Old         map[string]interface{}
}

func insertCimplrInitiateAudit(ctx context.Context, exec dbExec, e initiateAuditEntry) error {
	id := e.ID
	action := e.Action
	status := e.Status
	reason := e.Reason
	requestedBy := e.RequestedBy
	old := e.Old
	_, err := exec.Exec(ctx, `
		INSERT INTO cimplr.fd_closure_initiate_audit (
			closure_initiate_id, action_type, processing_status, reason, requested_by,
			old_closure_type, old_closure_status, old_action_at_maturity, old_maturity_date,
			old_requested_closure_date, old_principal_amount, old_interest_type_code,
			old_interest_rate, old_expected_maturity_value, old_accrued_interest_till_date,
			old_tds_expected, old_net_expected_amount, old_auto_renewal_flag,
			old_maturity_status, old_action_required, old_rollover_type, old_rollover_bank_type,
			old_rollover_new_bank_id, old_rollover_new_bank_name,
			old_tentative_new_tenor_days, old_remarks, old_has_variance,
			old_has_unresolved_variance, old_variance_run_id, old_approval_instance_id,
			old_is_active, old_is_deleted
		) VALUES (
			$1,$2,$3,$4,$5,$6,$7,$8,$9::date,$10::date,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32
		)`,
		id, action, status, reason, requestedBy,
		oldValue(old, "closure_type"), oldValue(old, "closure_status"), oldValue(old, "action_at_maturity"), oldValue(old, "maturity_date"),
		oldValue(old, "requested_closure_date"), oldValue(old, "principal_amount"), oldValue(old, "interest_type_code"),
		oldValue(old, "interest_rate"), oldValue(old, "expected_maturity_value"), oldValue(old, "accrued_interest_till_date"),
		oldValue(old, "tds_expected"), oldValue(old, "net_expected_amount"), oldValue(old, "auto_renewal_flag"),
		oldValue(old, "maturity_status"), oldValue(old, "action_required"), oldValue(old, "rollover_type"), oldValue(old, "rollover_bank_type"),
		oldValue(old, "rollover_new_bank_id"), oldValue(old, "rollover_new_bank_name"),
		oldValue(old, "tentative_new_tenor_days"), oldValue(old, "remarks"), oldValue(old, "has_variance"),
		oldValue(old, "has_unresolved_variance"), oldValue(old, "variance_run_id"), oldValue(old, "approval_instance_id"),
		oldValue(old, "is_active"), oldValue(old, "is_deleted"),
	)
	return err
}

func cimplrLookupBankName(ctx context.Context, q cimplrRowQuerier, bankID string) string {
	bankID = strings.TrimSpace(bankID)
	if bankID == "" {
		return ""
	}
	for _, sql := range []string{
		`SELECT bank_name FROM masterbank WHERE bank_id = $1 LIMIT 1`,
		`SELECT bank_name FROM master_bank WHERE bank_id = $1 LIMIT 1`,
		`SELECT bank_name FROM public.bank_master WHERE bank_id = $1 LIMIT 1`,
	} {
		var name string
		if err := q.QueryRow(ctx, sql, bankID).Scan(&name); err == nil && strings.TrimSpace(name) != "" {
			return strings.TrimSpace(name)
		}
	}
	return ""
}

func cimplrResolveInitiateRolloverBank(ctx context.Context, q cimplrRowQuerier, req cimplrClosureInitiateRequest, src cimplrFDSource) (string, string) {
	ct := strings.ToUpper(strings.TrimSpace(req.ClosureType))
	if ct != "ROLLOVER" {
		return "", ""
	}
	rolloverBank := strings.ToUpper(strings.TrimSpace(req.RolloverBankType))
	if rolloverBank == "" {
		rolloverBank = "SAME_BANK"
	}
	if rolloverBank == "SAME_BANK" {
		return strings.TrimSpace(src.BankID), strings.TrimSpace(src.BankName)
	}
	if rolloverBank == "NEW_BANK" {
		id := strings.TrimSpace(req.NewBankID)
		name := strings.TrimSpace(req.NewBankName)
		if name == "" && id != "" {
			name = cimplrLookupBankName(ctx, q, id)
		}
		return id, name
	}
	return strings.TrimSpace(src.BankID), strings.TrimSpace(src.BankName)
}

type confirmAuditEntry struct {
	ConfirmID   string
	InitiateID  string
	Action      string
	Status      string
	Reason      string
	RequestedBy string
	Old         map[string]interface{}
}

func insertCimplrConfirmAudit(ctx context.Context, exec dbExec, e confirmAuditEntry) error {
	confirmID := e.ConfirmID
	initiateID := e.InitiateID
	action := e.Action
	status := e.Status
	reason := e.Reason
	requestedBy := e.RequestedBy
	old := e.Old
	if initiateID == "" {
		// This helper is sometimes called inside a tx (e.g. inside the dry-run path
		// and the real post path). A silently-swallowed error here would abort the
		// surrounding tx and surface as SQLSTATE 25P02 on the next INSERT. ErrNoRows
		// is the only acceptable error — initiate_id will simply stay empty.
		if err := exec.QueryRow(ctx, `SELECT COALESCE(closure_initiate_id,'') FROM cimplr.fd_closure_confirm WHERE closure_confirm_id=$1`, confirmID).Scan(&initiateID); err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("audit prep: initiate-id lookup failed for %s: %w", confirmID, err)
		}
	}
	if strings.TrimSpace(initiateID) == "" {
		return fmt.Errorf("audit prep: closure_initiate_id is missing for %s", confirmID)
	}
	_, err := exec.Exec(ctx, `
		INSERT INTO cimplr.fd_closure_confirm_audit (
			closure_confirm_id, closure_initiate_id, action_type, processing_status, reason, requested_by, requested_at,
			old_closure_type, old_closure_status, old_posting_status, old_confirmation_mode,
			old_bank_reference_no, old_actual_payout_date, old_requested_closure_date,
			old_premature_reason, old_principal_expected, old_interest_expected,
			old_tds_expected, old_net_expected, old_principal_received,
			old_interest_received, old_tds_deducted, old_net_amount_received,
			old_variance_type, old_resolution_action, old_remarks, old_has_variance,
			old_has_unresolved_variance, old_variance_run_id, old_approval_instance_id,
			old_accounting_posted, old_journal_entry_id, old_new_booking_id,
			old_is_active, old_is_deleted
		) VALUES (
			$1,NULLIF($2,''),$3,$4,$5,$6,clock_timestamp(),$7,$8,$9,$10,$11,$12::date,$13::date,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34
		)`,
		confirmID, initiateID, action, status, reason, requestedBy,
		oldValue(old, "closure_type"), oldValue(old, "closure_status"), oldValue(old, "posting_status"), oldValue(old, "confirmation_mode"),
		oldValue(old, "bank_reference_no"), oldValue(old, "actual_payout_date"), oldValue(old, "requested_closure_date"),
		oldValue(old, "premature_reason"), oldValue(old, "principal_expected"), oldValue(old, "interest_expected"),
		oldValue(old, "tds_expected"), oldValue(old, "net_expected"), oldValue(old, "principal_received"),
		oldValue(old, "interest_received"), oldValue(old, "tds_deducted"), oldValue(old, "net_amount_received"),
		oldValue(old, "variance_type"), oldValue(old, "resolution_action"), oldValue(old, "remarks"), oldValue(old, "has_variance"),
		oldValue(old, "has_unresolved_variance"), oldValue(old, "variance_run_id"), oldValue(old, "approval_instance_id"),
		oldValue(old, "accounting_posted"), oldValue(old, "journal_entry_id"), oldValue(old, "new_booking_id"),
		oldValue(old, "is_active"), oldValue(old, "is_deleted"),
	)
	return err
}

func loadCimplrInitiateOld(ctx context.Context, pool *pgxpool.Pool, id string) (map[string]interface{}, error) {
	rows, err := pool.Query(ctx, `SELECT * FROM cimplr.fd_closure_initiate WHERE closure_initiate_id=$1 AND is_deleted=false LIMIT 1`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	vals, err := pgx.CollectOneRow(rows, pgx.RowToMap)
	return vals, err
}

func loadCimplrConfirmOld(ctx context.Context, pool *pgxpool.Pool, id string) (map[string]interface{}, error) {
	rows, err := pool.Query(ctx, `SELECT * FROM cimplr.fd_closure_confirm WHERE closure_confirm_id=$1 AND is_deleted=false LIMIT 1`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	vals, err := pgx.CollectOneRow(rows, pgx.RowToMap)
	return vals, err
}

func listCimplrRecords(ctx context.Context, pool *pgxpool.Pool, stage string, req cimplrClosureListRequest, approvedActive bool) ([]map[string]interface{}, int, error) {
	if req.Page <= 0 {
		req.Page = 1
	}
	if req.PageSize <= 0 || req.PageSize > 200 {
		req.PageSize = 50
	}
	offset := (req.Page - 1) * req.PageSize
	var table, idCol, auditTable, statusCol string
	if stage == "confirm" {
		table, idCol, auditTable, statusCol = constants.QuerryAuditClosureConfirm, "closure_confirm_id", constants.QuerryAuditClosureConfirmAudit, "closure_status"
	} else {
		table, idCol, auditTable, statusCol = constants.QuerryClosureInitiate, "closure_initiate_id", constants.QuerryAuditClosureInitiate, "closure_status"
	}
	where := []string{"t.is_deleted=false"}
	args := []interface{}{}
	add := func(cond string, v interface{}) {
		args = append(args, v)
		where = append(where, fmt.Sprintf(cond, len(args)))
	}
	if req.Status != "" {
		add("t."+statusCol+"=$%d", strings.ToUpper(req.Status))
	}
	if req.FDID != "" {
		add("t.fd_id=$%d", req.FDID)
	}
	if req.EntityID != "" {
		add("t.entity_id=$%d", req.EntityID)
	}
	if req.ClosureType != "" {
		add("t.closure_type=$%d", strings.ToUpper(req.ClosureType))
	} else {
		where = append(where, "t.closure_type <> 'PREMATURE'")
	}
	if approvedActive {
		if stage == "confirm" {
			where = append(where, "t.closure_status='CONFIRM'")
		} else {
			where = append(where, "t.closure_status='CONFIRM'")
			where = append(where, "NOT EXISTS (SELECT 1 FROM cimplr.fd_closure_confirm c WHERE c.closure_initiate_id = t.closure_initiate_id AND c.is_deleted = false AND c.closure_status NOT IN ('REJECTED', 'CANCELLED'))")
		}
	}
	whereSQL := strings.Join(where, " AND ")
	var total int
	countSQL := fmt.Sprintf("SELECT COUNT(*) FROM %s t WHERE %s", table, whereSQL)
	if err := pool.QueryRow(ctx, countSQL, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	args = append(args, req.PageSize, offset)
	tenureExpr := `COALESCE(NULLIF(t.tentative_new_tenor_days, 0), m.tenure_days, 0)`
	penaltyExpr := `COALESCE(calc.penalty_amount, 0)`
	prematureJoins := `
		LEFT JOIN LATERAL (
			SELECT accrued_days, penalty_amount, accrued_interest, tds_amount, net_payout, expected_maturity_value
			FROM cimplr.fd_closure_calculation c
			WHERE c.closure_initiate_id = t.closure_initiate_id AND COALESCE(c.is_deleted, false) = false
			ORDER BY c.calculation_date DESC NULLS LAST
			LIMIT 1
		) calc ON true`
	if stage == "confirm" {
		tenureExpr = `COALESCE(NULLIF(pc.days_held, 0), NULLIF(calc.accrued_days, 0), m.tenure_days, 0)`
		penaltyExpr = `COALESCE(pc.penalty_amount, calc.penalty_amount, 0)`
		prematureJoins = `
		LEFT JOIN cimplr.fd_closure_premature_confirm pc
			ON pc.closure_confirm_id = t.closure_confirm_id AND COALESCE(pc.is_deleted, false) = false
		LEFT JOIN LATERAL (
			SELECT accrued_days, penalty_amount, accrued_interest, tds_amount, net_payout, expected_maturity_value
			FROM cimplr.fd_closure_calculation c
			WHERE c.closure_confirm_id = t.closure_confirm_id AND COALESCE(c.is_deleted, false) = false
			ORDER BY c.calculation_date DESC NULLS LAST
			LIMIT 1
		) calc ON true`
	}
	interestRateExpr := `COALESCE(t.interest_rate, m.interest_rate, 0)`
	maturityDateExpr := `COALESCE(m.maturity_date::text, '')`
	if stage == "confirm" {
		interestRateExpr = `COALESCE(m.interest_rate, 0)`
		maturityDateExpr = `COALESCE(m.maturity_date::text, '')`
	}
	latestStatusExpr := `COALESCE(a.processing_status,'')`
	latestActionExpr := `COALESCE(a.action_type,'')`
	auditOrderExpr := `requested_at DESC NULLS LAST, audit_id DESC`
	if stage == "confirm" {
		// Posted confirms must show POSTED in approval_status regardless of audit
		// tie-break (auto-maturity approve+post runs in one tx → same requested_at).
		latestStatusExpr = `COALESCE(CASE WHEN UPPER(t.closure_status)='POSTED' THEN 'POSTED' END, a.processing_status, '')`
		latestActionExpr = `COALESCE(CASE WHEN UPPER(t.closure_status)='POSTED' THEN 'POST' END, a.action_type, '')`
		auditOrderExpr = `CASE WHEN action_type='POST' AND processing_status='POSTED' THEN 0 ELSE 1 END, requested_at DESC NULLS LAST, audit_id DESC`
	}
	listSQL := fmt.Sprintf(`
		SELECT t.*,
		       %s AS tenure_days,
		       %s AS interest_rate,
		       %s AS maturity_date,
		       %s AS penalty_amount,
		       %s AS latest_processing_status,
		       %s AS latest_action_type,
		       COALESCE(a.requested_by,'') AS latest_requested_by,
		       a.requested_at AS latest_requested_at,
		       COALESCE(a.checker_by,'') AS latest_checker_by,
		       a.checker_at AS latest_checker_at
		FROM %s t
		LEFT JOIN investment.fd_master m ON m.fd_id = t.fd_id
		%s
		LEFT JOIN LATERAL (
			SELECT * FROM %s a WHERE a.%s=t.%s ORDER BY %s LIMIT 1
		) a ON true
		WHERE %s
		ORDER BY t.%s DESC
		LIMIT $%d OFFSET $%d`,
		tenureExpr, interestRateExpr, maturityDateExpr, penaltyExpr, latestStatusExpr, latestActionExpr, table, prematureJoins, auditTable, idCol, idCol, auditOrderExpr, whereSQL, idCol, len(args)-1, len(args),
	)
	rows, err := pool.Query(ctx, listSQL, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	out, err := pgx.CollectRows(rows, pgx.RowToMap)
	return out, total, err
}

func previewCimplrInitiateVariance(recordID string, req cimplrClosureInitiateRequest, src cimplrFDSource, calc cimplrClosureCalc) map[string]interface{} {
	runID := varianceengine.NewRunID()
	ff := func(v float64) string { return strconv.FormatFloat(roundToFour(v), 'f', 4, 64) }
	rules := []varianceengine.Rule{
		{FieldName: "principal_amount", VarianceType: varianceengine.TypeAmount, ExpectedValue: ff(src.Principal), ActualValue: ff(chooseFloat(req.PrincipalAmount, src.Principal)), Priority: varianceengine.PriorityHigh, Tolerance: 0.01},
		{FieldName: "expected_maturity_value", VarianceType: varianceengine.TypeAmount, ExpectedValue: ff(calc.ExpectedMaturityValue), ActualValue: ff(chooseFloat(req.ExpectedMaturityValue, calc.ExpectedMaturityValue)), Priority: varianceengine.PriorityMedium, Tolerance: 1.0},
		{FieldName: "accrued_interest_till_date", VarianceType: varianceengine.TypeAmount, ExpectedValue: ff(calc.AccruedInterest), ActualValue: ff(chooseFloat(req.AccruedInterestTillDate, calc.AccruedInterest)), Priority: varianceengine.PriorityMedium, Tolerance: 1.0},
		{FieldName: "tds_expected", VarianceType: varianceengine.TypeAmount, ExpectedValue: ff(calc.TDSAmount), ActualValue: ff(chooseFloat(req.TDSExpected, calc.TDSAmount)), Priority: varianceengine.PriorityMedium, Tolerance: 0.5},
		{FieldName: "net_expected_amount", VarianceType: varianceengine.TypeAmount, ExpectedValue: ff(calc.NetPayout), ActualValue: ff(chooseFloat(req.NetExpectedAmount, calc.NetPayout)), Priority: varianceengine.PriorityHigh, Tolerance: 1.0},
	}
	items := varianceengine.Compare("FD_CLOSURE", recordID, src.EntityID, runID, rules)
	return cimplrVarianceSummary(runID, items)
}

func previewCimplrConfirmVariance(recordID string, req cimplrClosureConfirmRequest, src cimplrFDSource, calc cimplrClosureCalc) map[string]interface{} {
	runID := varianceengine.NewRunID()
	rules := buildCimplrConfirmVarianceRules(req, src, calc)
	items := varianceengine.Compare("FD_CLOSURE", recordID, src.EntityID, runID, rules)
	return cimplrVarianceSummary(runID, items)
}

func cimplrExpectedRolloverNewFD(src cimplrFDSource, calc cimplrClosureCalc, basis string) float64 {
	if strings.ToUpper(strings.TrimSpace(basis)) == "PRINCIPAL_ONLY" {
		return src.Principal
	}
	return calc.NetPayout
}

func buildCimplrInitiatePrefill(src cimplrFDSource, calc cimplrClosureCalc, closureType string) map[string]interface{} {
	return map[string]interface{}{
		"fd_id":                      src.FDID,
		"booking_id":                 src.BookingID,
		"confirmation_id":            src.ConfirmationID,
		"entity_id":                  src.EntityID,
		"entity_name":                src.EntityName,
		"bank_id":                    src.BankID,
		"bank_name":                  src.BankName,
		"fd_ref_no":                  src.FDRefNo,
		"bank_fd_ref_no":             src.BankFDRefNo,
		"closure_type":               closureType,
		"action_at_maturity":         closureType,
		"maturity_date":              src.MaturityDate.Format(constants.DateFormat),
		"requested_closure_date":     calc.CalculationDate.Format(constants.DateFormat),
		"principal_amount":           src.Principal,
		"interest_type_code":         src.InterestTypeCode,
		"interest_rate":              src.InterestRate,
		"expected_maturity_value":    calc.ExpectedMaturityValue,
		"accrued_interest_till_date": calc.AccruedInterest,
		"tds_expected":               calc.TDSAmount,
		"net_expected_amount":        calc.NetPayout,
		"maturity_status":            deriveCimplrMaturityStatus(src.MaturityDate),
		"action_required":            true,
		"rollover_type":              "PRINCIPAL_PLUS_INTEREST",
		"rollover_bank_type":         "SAME_BANK",
		"tentative_new_tenor_days":   src.TenureDays,
		"supported_closure_types":    []string{"PAYOUT", "ROLLOVER", "PREMATURE"},
		"calculator_version":         "1.0",
	}
}

func buildCimplrConfirmPrefill(src cimplrFDSource, calc cimplrClosureCalc, closureType string) map[string]interface{} {
	prefill := map[string]interface{}{
		"fd_id":                  src.FDID,
		"closure_type":           closureType,
		"principal_expected":     src.Principal,
		"interest_expected":      calc.AccruedInterest,
		"tds_expected":           calc.TDSAmount,
		"net_expected":           calc.NetPayout,
		"principal_received":     src.Principal,
		"interest_received":      calc.AccruedInterest,
		"tds_deducted":           calc.TDSAmount,
		"net_amount_received":    calc.NetPayout,
		"requested_closure_date": calc.CalculationDate.Format(constants.DateFormat),
	}
	if closureType == "PREMATURE" {
		prefill["days_held"] = calc.AccruedDays
		prefill["applicable_rate"] = calc.ApplicableRate
		prefill["penalty_id"] = calc.PenaltyID
		prefill["penalty_type"] = calc.PenaltyType
		prefill["penalty_value"] = calc.PenaltyValue
		prefill["penalty_amount"] = calc.PenaltyAmount
		prefill["no_interest_flag"] = calc.NoInterestFlag
		prefill["penalty_applicable"] = calc.PenaltyApplicable
		prefill["revised_interest_amount"] = calc.RevisedInterestAmount
		prefill["revised_maturity_value"] = calc.RevisedMaturityValue
		prefill["net_payout"] = calc.NetPayout
	}
	if closureType == "ROLLOVER" {
		prefill["rollover_amount_basis"] = "PRINCIPAL_PLUS_INTEREST"
		prefill["new_fd_amount"] = calc.NetPayout
		prefill["new_tenor_days"] = src.TenureDays
		prefill["new_interest_rate"] = src.InterestRate
		prefill["expected_start_date"] = calc.CalculationDate.Format(constants.DateFormat)
		prefill["expected_maturity_date"] = calc.CalculationDate.AddDate(0, 0, src.TenureDays).Format(constants.DateFormat)
		prefill["new_bank_id"] = src.BankID
		prefill["new_bank_name"] = src.BankName
		prefill["new_account_id"] = src.SourceAccountID
	}
	return prefill
}

// enrichCimplrInitiateListFromCalculation fills interest/TDS/net on list rows.
// When forceSystemCalc is true (approved-active for confirm), always overlay fresh
// maturity calculation so confirm prefill matches /confirm/validate.
func enrichCimplrInitiateListFromCalculation(ctx context.Context, pool *pgxpool.Pool, records []map[string]interface{}, forceSystemCalc bool) []map[string]interface{} {
	for _, row := range records {
		initiateID := strings.TrimSpace(fmt.Sprint(row["closure_initiate_id"]))
		fdID := strings.TrimSpace(fmt.Sprint(row["fd_id"]))
		if initiateID == "" || fdID == "" {
			continue
		}
		src, srcErr := loadCimplrFDSource(ctx, pool, fdID)
		if srcErr != nil {
			continue
		}
		closureType := strings.ToUpper(strings.TrimSpace(fmt.Sprint(row["closure_type"])))
		reqDate := cimplrDefaultCalcDate(src, closureType, strings.TrimSpace(fmt.Sprint(row["requested_closure_date"])))
		calc, calcErr := calculateCimplrClosure(ctx, pool, src, closureType, reqDate, false)
		if calcErr != nil {
			continue
		}
		if forceSystemCalc {
			row["accrued_interest_till_date"] = calc.AccruedInterest
			row["tds_expected"] = calc.TDSAmount
			row["bank_calculated_tds"] = calc.TDSAmount
			row["net_expected_amount"] = calc.NetPayout
			row["expected_maturity_value"] = calc.ExpectedMaturityValue
			continue
		}
		var accrued, tds, net, expected float64
		err := pool.QueryRow(ctx, `
			SELECT COALESCE(accrued_interest,0), COALESCE(tds_amount,0),
			       COALESCE(net_payout,0), COALESCE(expected_maturity_value,0)
			FROM cimplr.fd_closure_calculation
			WHERE closure_initiate_id=$1 AND COALESCE(is_deleted,false)=false
			ORDER BY calculation_date DESC NULLS LAST, calculation_id DESC
			LIMIT 1`, initiateID).Scan(&accrued, &tds, &net, &expected)
		if err != nil || tds == 0 {
			if err != nil {
				accrued = calc.AccruedInterest
				tds = calc.TDSAmount
				net = calc.NetPayout
				expected = calc.ExpectedMaturityValue
			} else if tds == 0 && calc.TDSAmount > 0 {
				tds = calc.TDSAmount
			}
		}
		if chooseFloat(parseFloatMap(row, "accrued_interest_till_date"), 0) == 0 && accrued > 0 {
			row["accrued_interest_till_date"] = accrued
		}
		// Always expose the live-recalculated TDS so the frontend can prefer it.
		// The stored tds_expected may be stale (computed at initiation with an older
		// cashflow); bank_calculated_tds always reflects the current cashflow schedule.
		if tds > 0 {
			row["bank_calculated_tds"] = tds
		} else if calc.TDSAmount > 0 {
			row["bank_calculated_tds"] = calc.TDSAmount
		}
		if chooseFloat(parseFloatMap(row, "tds_expected"), 0) == 0 && tds > 0 {
			row["tds_expected"] = tds
			if chooseFloat(parseFloatMap(row, "net_expected_amount"), 0) > 0 && tds > 0 {
				principal := chooseFloat(parseFloatMap(row, "principal_amount"), 0)
				interest := chooseFloat(parseFloatMap(row, "accrued_interest_till_date"), accrued)
				row["net_expected_amount"] = roundToFour(principal + interest - tds)
			}
		}
		if chooseFloat(parseFloatMap(row, "net_expected_amount"), 0) == 0 && net > 0 {
			row["net_expected_amount"] = net
		}
		if chooseFloat(parseFloatMap(row, "expected_maturity_value"), 0) == 0 && expected > 0 {
			row["expected_maturity_value"] = expected
		}
	}
	return records
}

func parseFloatMap(row map[string]interface{}, key string) float64 {
	v, ok := row[key]
	if !ok || v == nil {
		return 0
	}
	switch t := v.(type) {
	case float64:
		return t
	case float32:
		return float64(t)
	case int:
		return float64(t)
	case int64:
		return float64(t)
	case string:
		f, _ := strconv.ParseFloat(strings.TrimSpace(t), 64)
		return f
	default:
		f, _ := strconv.ParseFloat(strings.TrimSpace(fmt.Sprint(v)), 64)
		return f
	}
}

func enrichCimplrMaturityDashboardRecords(ctx context.Context, pool *pgxpool.Pool, records []map[string]interface{}) []map[string]interface{} {
	for _, row := range records {
		fdID := strings.TrimSpace(fmt.Sprint(row["fd_id"]))
		if fdID == "" {
			continue
		}
		src, err := loadCimplrFDSource(ctx, pool, fdID)
		if err != nil {
			continue
		}
		payoutCalc, err := calculateCimplrClosure(ctx, pool, src, "PAYOUT", "", false)
		if err != nil {
			continue
		}
		prematureCalc, _ := calculateCimplrClosure(ctx, pool, src, "PREMATURE", time.Now().Format(constants.DateFormat), false)
		rolloverCalc, _ := calculateCimplrClosure(ctx, pool, src, "ROLLOVER", "", false)

		row["accrued_interest_till_date"] = payoutCalc.AccruedInterest
		row["tds_expected"] = payoutCalc.TDSAmount
		row["expected_maturity_value"] = payoutCalc.ExpectedMaturityValue
		row["net_expected_amount"] = payoutCalc.NetPayout
		row["maturity_status"] = deriveCimplrMaturityStatus(src.MaturityDate)
		row["supported_closure_types"] = []string{"PAYOUT", "ROLLOVER", "PREMATURE"}
		row["initiate_prefill"] = map[string]interface{}{
			"PAYOUT":   buildCimplrInitiatePrefill(src, payoutCalc, "PAYOUT"),
			"ROLLOVER": buildCimplrInitiatePrefill(src, rolloverCalc, "ROLLOVER"),
		}
		row["premature_prefill"] = buildCimplrConfirmPrefill(src, prematureCalc, "PREMATURE")
		row["premature_calculation"] = cimplrCalcToMap(prematureCalc)
		row["rollover_calculation"] = cimplrCalcToMap(rolloverCalc)
		row["total_interest_accrued"] = payoutCalc.AccruedInterest
		row["total_tds_accrued"] = payoutCalc.TDSAmount
		row["maturity_amount"] = payoutCalc.ExpectedMaturityValue
	}
	return records
}

func fetchCimplrSubRows(ctx context.Context, pool *pgxpool.Pool, query, id string) []map[string]interface{} {
	rows, err := pool.Query(ctx, query, id)
	if err != nil {
		return []map[string]interface{}{}
	}
	defer rows.Close()
	out, err := pgx.CollectRows(rows, pgx.RowToMap)
	if err != nil {
		return []map[string]interface{}{}
	}
	return out
}

func fetchCimplrCalculations(ctx context.Context, pool *pgxpool.Pool, col, id string) []map[string]interface{} {
	query := fmt.Sprintf(`SELECT * FROM cimplr.fd_closure_calculation WHERE %s=$1 AND is_deleted=false ORDER BY calculation_date DESC, calculation_id DESC`, col)
	return fetchCimplrSubRows(ctx, pool, query, id)
}

func fetchCimplrFiles(ctx context.Context, pool *pgxpool.Pool, col, id string) []map[string]interface{} {
	query := fmt.Sprintf(`SELECT * FROM cimplr.fd_closure_files WHERE %s=$1 AND is_deleted=false ORDER BY uploaded_at DESC`, col)
	return fetchCimplrSubRows(ctx, pool, query, id)
}

func fetchCimplrAudit(ctx context.Context, pool *pgxpool.Pool, stage, id string) []map[string]interface{} {
	if stage == "confirm" {
		return fetchCimplrSubRows(ctx, pool, `
			SELECT * FROM cimplr.fd_closure_confirm_audit
			WHERE closure_confirm_id=$1
			ORDER BY GREATEST(COALESCE(checker_at, requested_at), requested_at) DESC NULLS LAST, audit_id DESC`, id)
	}
	return fetchCimplrSubRows(ctx, pool, `
		SELECT * FROM cimplr.fd_closure_initiate_audit
		WHERE closure_initiate_id=$1
		ORDER BY GREATEST(COALESCE(checker_at, requested_at), requested_at) DESC NULLS LAST, audit_id DESC`, id)
}

func fetchCimplrApprovalWorkflow(ctx context.Context, pool *pgxpool.Pool, recordID string) []map[string]interface{} {
	return fetchCimplrSubRows(ctx, pool, `
		SELECT i.instance_id, i.transaction_type, i.action_type, i.status, i.submitted_by_email,
		       i.submitted_at, i.resolved_at, ie.instance_eye_id, ie.position, ie.status AS eye_status
		FROM uam.approval_instance i
		LEFT JOIN uam.approval_instance_eye ie ON ie.instance_id=i.instance_id
		WHERE i.record_id=$1 AND i.module_code='FIXED_DEPOSIT'
		ORDER BY i.submitted_at DESC, ie.position ASC`, recordID)
}

func loadCimplrDownloadFiles(ctx context.Context, pool *pgxpool.Pool, fileID, initiateID, confirmID, fileType string) ([]map[string]interface{}, error) {
	where := []string{"is_deleted=false"}
	args := []interface{}{}
	add := func(cond string, v interface{}) {
		args = append(args, v)
		where = append(where, fmt.Sprintf(cond, len(args)))
	}
	if strings.TrimSpace(fileID) != "" {
		add("file_id=$%d::uuid", strings.TrimSpace(fileID))
	}
	if strings.TrimSpace(initiateID) != "" {
		add("closure_initiate_id=$%d", strings.TrimSpace(initiateID))
	}
	if strings.TrimSpace(confirmID) != "" {
		add("closure_confirm_id=$%d", strings.TrimSpace(confirmID))
	}
	if strings.TrimSpace(fileType) != "" {
		add("file_type=$%d", strings.ToUpper(strings.TrimSpace(fileType)))
	}
	if len(args) == 0 {
		return nil, fmt.Errorf("file_id, closure_initiate_id or closure_confirm_id is required")
	}
	rows, err := pool.Query(ctx, `SELECT file_id::text, COALESCE(closure_initiate_id,''), COALESCE(closure_confirm_id,''), file_type, COALESCE(original_file_name,''), stored_file_name, upload_s3_key FROM cimplr.fd_closure_files WHERE `+strings.Join(where, " AND ")+` ORDER BY uploaded_at DESC`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	files := []map[string]interface{}{}
	for rows.Next() {
		var id, ci, cc, ft, original, stored, key string
		if err := rows.Scan(&id, &ci, &cc, &ft, &original, &stored, &key); err != nil {
			return nil, err
		}
		url, err := s3storage.GetDownloadPresignedURL(ctx, key, 15*time.Minute)
		if err != nil {
			return nil, err
		}
		files = append(files, map[string]interface{}{
			"file_id":             id,
			"closure_initiate_id": ci,
			"closure_confirm_id":  cc,
			"file_type":           ft,
			"original_file_name":  original,
			"stored_file_name":    stored,
			"download_url":        url,
			"expires_in_minutes":  15,
		})
	}
	return files, rows.Err()
}

type approvalInstanceRequest struct {
	TxType        string
	Action        string
	RecordID      string
	RecordTable   string
	AuditTable    string
	AuditIDColumn string
	EntityID      string
	Amount        float64
	UserID        string
	UserEmail     string
}

func createCimplrApprovalInstance(ctx context.Context, pool *pgxpool.Pool, req approvalInstanceRequest) (string, error) {
	return approvalengine.CreateInstance(ctx, pool, approvalengine.InstanceRequest{
		ModuleCode:       cimplrClosureModule,
		EntityCode:       firstNonEmpty(req.EntityID, "DEFAULT"),
		TransactionType:  req.TxType,
		RecordID:         req.RecordID,
		RecordTable:      req.RecordTable,
		AuditTable:       req.AuditTable,
		AuditIDColumn:    req.AuditIDColumn,
		ActionType:       req.Action,
		Amount:           req.Amount,
		SubmittedBy:      req.UserID,
		SubmittedByEmail: req.UserEmail,
	})
}

func normalizeCimplrClosureType(closureType, actionAtMaturity string) string {
	ct := strings.ToUpper(strings.TrimSpace(closureType))
	switch ct {
	case "MATURITY", "MATURITY_PAYOUT", "LIQUIDATE":
		return "PAYOUT"
	}
	if ct == "" {
		act := strings.ToUpper(strings.TrimSpace(actionAtMaturity))
		if act == "ROLLOVER" {
			return "ROLLOVER"
		}
		return "PAYOUT"
	}
	return ct
}

func isValidCimplrClosureType(v string) bool {
	return v == "PAYOUT" || v == "ROLLOVER" || v == "PREMATURE"
}

func deriveCimplrMaturityStatus(maturityDate time.Time) string {
	today := time.Now().Truncate(24 * time.Hour)
	md := maturityDate.Truncate(24 * time.Hour)
	if md.Before(today) {
		return "OVERDUE"
	}
	if md.Equal(today) {
		return "DUE"
	}
	return "UPCOMING"
}

func parseCimplrDate(s string) (time.Time, bool) {
	s = strings.TrimSpace(s)
	if s == "" || s == "<nil>" {
		return time.Time{}, false
	}
	for _, layout := range []string{
		time.RFC3339,
		constants.DateFormatISO,
		"2006-01-02T15:04:05Z07:00",
		constants.DateFormat,
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t, true
		}
	}
	if len(s) >= 10 {
		if t, err := time.Parse(constants.DateFormat, s[:10]); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

func nullDateArg(s string) interface{} {
	if t, ok := parseCimplrDate(s); ok {
		return t.Format(constants.DateFormat)
	}
	return nil
}

func chooseFloat(v, fallback float64) float64 {
	if v != 0 {
		return roundToFour(v)
	}
	return roundToFour(fallback)
}

func chooseInt(v, fallback int) int {
	if v != 0 {
		return v
	}
	return fallback
}

func cimplrFloat(v interface{}) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case float32:
		return float64(t)
	case int:
		return float64(t)
	case int64:
		return float64(t)
	case int32:
		return float64(t)
	case string:
		f, _ := strconv.ParseFloat(strings.TrimSpace(t), 64)
		return f
	case []byte:
		f, _ := strconv.ParseFloat(strings.TrimSpace(string(t)), 64)
		return f
	case fmt.Stringer:
		f, _ := strconv.ParseFloat(strings.TrimSpace(t.String()), 64)
		return f
	default:
		s := strings.TrimSpace(fmt.Sprint(v))
		if s == "" || s == "<nil>" {
			return 0
		}
		f, _ := strconv.ParseFloat(s, 64)
		return f
	}
}

func normalizeCimplrIDs(one string, many []string) []string {
	seen := map[string]bool{}
	out := []string{}
	for _, id := range append([]string{one}, many...) {
		id = strings.TrimSpace(id)
		if id != "" && !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	return out
}

func oldValue(m map[string]interface{}, key string) interface{} {
	if m == nil {
		return nil
	}
	if v, ok := m[key]; ok {
		return v
	}
	return nil
}

type cimplrJournalLinePreview struct {
	LineNumber  int     `json:"line_number"`
	AccountNo   string  `json:"account_number"`
	AccountName string  `json:"account_name"`
	AccountType string  `json:"account_type"`
	Debit       float64 `json:"debit_amount"`
	Credit      float64 `json:"credit_amount"`
	Narration   string  `json:"narration"`
}

type journalPreviewParams struct {
	ClosureType  string
	FDID         string
	BankAcctNum  string
	BankAcctName string
	Principal    float64
	Interest     float64
	TDS          float64
	Penalty      float64
	NetPayout    float64
}

func buildCimplrJournalPreview(p journalPreviewParams) map[string]interface{} {
	closureType := p.ClosureType
	fdID := p.FDID
	bankAcctNum := p.BankAcctNum
	bankAcctName := p.BankAcctName
	principal := p.Principal
	interest := p.Interest
	tds := p.TDS
	penalty := p.Penalty
	netPayout := p.NetPayout
	bankAcctNum = firstNonEmpty(bankAcctNum, "SETTLEMENT")
	bankAcctName = firstNonEmpty(bankAcctName, "Settlement Account")
	lines := []cimplrJournalLinePreview{}
	lineNum := 1
	add := func(acctNum, acctName, acctType string, debit, credit float64, narration string) {
		if debit == 0 && credit == 0 {
			return
		}
		lines = append(lines, cimplrJournalLinePreview{
			LineNumber: lineNum, AccountNo: acctNum, AccountName: acctName, AccountType: acctType,
			Debit: roundToFour(debit), Credit: roundToFour(credit), Narration: narration,
		})
		lineNum++
	}
	activitySubtype := "FD_MATURITY_PAYOUT"
	if closureType == "PREMATURE" {
		activitySubtype = "FD_PREMATURE_CLOSURE"
	} else if closureType == "ROLLOVER" {
		activitySubtype = "FD_ROLLOVER"
	}
	totalDebit := roundToFour(netPayout + tds + penalty)
	totalCredit := roundToFour(principal + interest)
	if totalDebit != totalCredit {
		totalCredit = totalDebit
	}
	add(bankAcctNum, bankAcctName, "ASSET", netPayout, 0, "Cash / settlement on FD closure")
	if tds > 0 {
		add(constants.TDSReceivable, constants.TDSReceivableLabel, "ASSET", tds, 0, "TDS withheld at source")
	}
	if penalty > 0 {
		add("PENALTY-EXP", "Premature Withdrawal Penalty", "EXPENSE", penalty, 0, "Premature withdrawal penalty")
	}
	add(constants.FDInvestmentPrefix+fdID, constants.FormatFDInvestment+fdID, "ASSET", 0, principal, "Close FD investment asset")
	interestCredit := roundToFour(totalCredit - principal)
	if interestCredit > 0 {
		add(constants.FDInterestIncome+fdID, constants.FormatInterestIncome, "INCOME", 0, interestCredit, "Interest recognised on closure")
	}
	newFDStatus := "MATURED"
	if closureType == "PREMATURE" {
		newFDStatus = "PREMATURELY_CLOSED"
	} else if closureType == "ROLLOVER" {
		newFDStatus = "ROLLED_OVER"
	}
	entryDate := time.Now().Format(constants.DateFormat)
	return map[string]interface{}{
		"activity_subtype":  activitySubtype,
		"entry_type":        "CLOSURE",
		"entry_date":        entryDate,
		"accounting_period": time.Now().Format("2006-01"),
		"total_debit":       totalDebit,
		"total_credit":      totalCredit,
		"principal":         roundToFour(principal),
		"interest":          roundToFour(interest),
		"tds":               roundToFour(tds),
		"penalty":           roundToFour(penalty),
		"net_payout":        roundToFour(netPayout),
		"new_fd_status":     newFDStatus,
		"lines":             lines,
	}
}

func CimplrJournalPreview(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req cimplrClosureIDsRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		ctx := r.Context()
		if strings.TrimSpace(req.ClosureConfirmID) != "" {
			id := strings.TrimSpace(req.ClosureConfirmID)
			var fdID, closureType, sourceAccountID, journalEntryID string
			var principal, interest, tds, penalty, netPayout float64
			var accountingPosted bool
			err := pool.QueryRow(ctx, `
				SELECT c.fd_id, c.closure_type, COALESCE(b.source_account_id,''),
				       COALESCE(c.principal_received, c.principal_expected, 0),
				       COALESCE(c.interest_received, c.interest_expected, 0),
				       COALESCE(c.tds_deducted, c.tds_expected, 0),
				       CASE WHEN c.closure_type='PREMATURE' THEN COALESCE(pc.penalty_amount,0) ELSE 0 END,
				       COALESCE(c.net_amount_received, c.net_expected, 0),
				       COALESCE(c.journal_entry_id,''), COALESCE(c.accounting_posted,false)
				FROM cimplr.fd_closure_confirm c
				LEFT JOIN cimplr.fd_closure_premature_confirm pc ON pc.closure_confirm_id=c.closure_confirm_id AND pc.is_deleted=false
				LEFT JOIN investment.fd_master m ON m.fd_id=c.fd_id
				LEFT JOIN investment.fd_booking_request b ON b.booking_id=m.booking_id
				WHERE c.closure_confirm_id=$1 AND c.is_deleted=false`, id,
			).Scan(&fdID, &closureType, &sourceAccountID, &principal, &interest, &tds, &penalty, &netPayout, &journalEntryID, &accountingPosted)
			if err != nil {
				api.RespondWithError(w, http.StatusNotFound, constants.ConfirmRecordNotFound)
				return
			}
			bankNum, bankName := "", ""
			if sourceAccountID != "" {
				_ = pool.QueryRow(ctx, `SELECT COALESCE(account_number,''), COALESCE(account_nickname,'') FROM public.masterbankaccount WHERE account_id=$1 LIMIT 1`, sourceAccountID).Scan(&bankNum, &bankName)
			}
			preview := buildCimplrJournalPreview(journalPreviewParams{ClosureType: closureType, FDID: fdID, BankAcctNum: bankNum, BankAcctName: bankName, Principal: principal, Interest: interest, TDS: tds, Penalty: penalty, NetPayout: netPayout})
			preview["closure_confirm_id"] = id
			preview["accounting_posted"] = accountingPosted
			preview["journal_entry_id"] = journalEntryID
			if accountingPosted && journalEntryID != "" {
				preview["posted_lines"] = fetchCimplrPostedJournalLines(ctx, pool, journalEntryID)
			}
			api.RespondWithPayload(w, true, "", preview)
			return
		}
		if strings.TrimSpace(req.ClosureInitiateID) != "" {
			initiate, err := loadCimplrInitiateOld(ctx, pool, strings.TrimSpace(req.ClosureInitiateID))
			if err != nil {
				api.RespondWithError(w, http.StatusNotFound, constants.ErrClosureInitiateRecordNotFound)
				return
			}
			src, err := loadCimplrFDSource(ctx, pool, fmt.Sprint(initiate["fd_id"]))
			if err != nil {
				api.RespondWithError(w, http.StatusNotFound, constants.ErrFDNotFound)
				return
			}
			closureType := fmt.Sprint(initiate["closure_type"])
			calc, err := calculateCimplrClosure(ctx, pool, src, closureType, fmt.Sprint(initiate["requested_closure_date"]), false)
			if err != nil {
				api.RespondWithError(w, http.StatusBadRequest, err.Error())
				return
			}
			bankNum, bankName := "", ""
			if src.SourceAccountID != "" {
				_ = pool.QueryRow(ctx, `SELECT COALESCE(account_number,''), COALESCE(account_nickname,'') FROM public.masterbankaccount WHERE account_id=$1 LIMIT 1`, src.SourceAccountID).Scan(&bankNum, &bankName)
			}
			penalty := calc.PenaltyAmount
			preview := buildCimplrJournalPreview(journalPreviewParams{ClosureType: closureType, FDID: src.FDID, BankAcctNum: bankNum, BankAcctName: bankName, Principal: src.Principal, Interest: calc.RevisedInterestAmount, TDS: calc.TDSAmount, Penalty: penalty, NetPayout: calc.NetPayout})
			preview["closure_initiate_id"] = req.ClosureInitiateID
			preview["calculation"] = cimplrCalcToMap(calc)
			api.RespondWithPayload(w, true, "", preview)
			return
		}
		api.RespondWithError(w, http.StatusBadRequest, "closure_confirm_id or closure_initiate_id is required")
	}
}

func fetchCimplrPostedJournalLines(ctx context.Context, pool *pgxpool.Pool, entryID string) []map[string]interface{} {
	rows, err := pool.Query(ctx, `
		SELECT line_number, account_number, account_name, account_type,
		       debit_amount, credit_amount, narration
		FROM investment.accounting_journal_entry_line
		WHERE entry_id=$1 ORDER BY line_number`, entryID)
	if err != nil {
		return nil
	}
	defer rows.Close()
	out, err := pgx.CollectRows(rows, pgx.RowToMap)
	if err != nil {
		return nil
	}
	return out
}

func enrichCimplrAccountingListItem(ctx context.Context, pool *pgxpool.Pool, item map[string]interface{}) {
	principal := cimplrFloat(item["principal_received"])
	if principal == 0 {
		principal = cimplrFloat(item["principal_expected"])
	}
	if principal == 0 {
		principal = cimplrFloat(item["principal_amount"])
	}
	interest := cimplrFloat(item["interest_received"])
	if interest == 0 {
		interest = cimplrFloat(item["interest_expected"])
	}
	if interest == 0 {
		interest = cimplrFloat(item["accrued_interest_till_date"])
	}
	tds := cimplrFloat(item["tds_deducted"])
	if tds == 0 {
		tds = cimplrFloat(item["tds_expected"])
	}
	penalty := cimplrFloat(item["penalty_amount"])
	confirmID := strings.TrimSpace(fmt.Sprint(item["closure_confirm_id"]))
	if confirmID != "" {
		var calcNet, calcInterest, calcPenalty float64
		if err := pool.QueryRow(ctx, `
			SELECT COALESCE(net_payout,0), COALESCE(accrued_interest,0), COALESCE(penalty_amount,0)
			FROM cimplr.fd_closure_calculation
			WHERE closure_confirm_id=$1 AND COALESCE(is_deleted,false)=false
			ORDER BY calculation_date DESC NULLS LAST, calculation_id DESC
			LIMIT 1`, confirmID,
		).Scan(&calcNet, &calcInterest, &calcPenalty); err == nil {
			if calcNet > 0 {
				item["net_expected"] = calcNet
			}
			if calcInterest > 0 && interest == 0 {
				interest = calcInterest
				item["interest_received"] = calcInterest
			}
			if calcPenalty > 0 {
				penalty = calcPenalty
			}
		}
	}
	closureAmount := roundToFour(principal + interest - tds - penalty)
	netRecv := cimplrFloat(item["net_amount_received"])
	netExp := cimplrFloat(item["net_expected"])
	displayNet := netRecv
	if displayNet <= 0 || (interest > 0 && displayNet == principal) {
		displayNet = netExp
	}
	if displayNet <= 0 {
		displayNet = closureAmount
	}
	item["display_closure_amount"] = closureAmount
	item["display_net_payout"] = roundToFour(displayNet)
	item["display_interest"] = roundToFour(interest)

	posted := false
	switch v := item["accounting_posted"].(type) {
	case bool:
		posted = v
	}
	postingStatus := strings.ToUpper(strings.TrimSpace(fmt.Sprint(item["posting_status"])))
	closureStatus := strings.ToUpper(strings.TrimSpace(fmt.Sprint(item["closure_status"])))
	switch {
	case posted || postingStatus == "POSTED" || closureStatus == "POSTED":
		item["posting_display"] = "POSTED"
	case postingStatus == "FAILED":
		item["posting_display"] = "FAILED"
	default:
		item["posting_display"] = "PENDING"
	}
}

func cimplrBuildEmbeddedPostedPreview(ctx context.Context, pool *pgxpool.Pool, journalEntryID, closureType, fdID string, row map[string]interface{}) map[string]interface{} {
	lines := fetchCimplrPostedJournalLines(ctx, pool, journalEntryID)
	var totalDebit, totalCredit float64
	for _, ln := range lines {
		totalDebit += cimplrFloat(ln["debit_amount"])
		totalCredit += cimplrFloat(ln["credit_amount"])
	}
	preview := buildCimplrJournalPreview(journalPreviewParams{
		ClosureType: closureType,
		FDID:        fdID,
		Principal:   cimplrFloat(row["principal_received"]),
		Interest:    cimplrFloat(row["interest_received"]),
		TDS:         cimplrFloat(row["tds_deducted"]),
		Penalty:     cimplrFloat(row["penalty_amount"]),
		NetPayout:   cimplrFloat(row["net_amount_received"]),
	})
	preview["lines"] = lines
	preview["posted_lines"] = lines
	preview["total_debit"] = roundToFour(totalDebit)
	preview["total_credit"] = roundToFour(totalCredit)
	preview["accounting_posted"] = true
	preview["journal_entry_id"] = journalEntryID
	return preview
}

func cimplrAccountingApprovedActiveRecords(ctx context.Context, pool *pgxpool.Pool, req cimplrClosureListRequest) ([]map[string]interface{}, error) {
	if req.PageSize <= 0 || req.PageSize > 200 {
		req.PageSize = 100
	}
	out := make([]map[string]interface{}, 0)

	// Accounting register = successfully POSTED confirm rows (payout, rollover,
	// premature) with their journal lines embedded. Approved-but-not-posted
	// rows must not appear here:
	//   • Going forward, the approve handler's dry-run pre-check (Fix #2)
	//     refuses approval if posting would fail, so we never create new
	//     APPROVED-PENDING-POST or APPROVED-FAILED zombies.
	//   • Legacy zombies from before the fix are reset via the maintenance
	//     SQL (cimplr.fd_closure_confirm posting_status reset). After that
	//     they go back to CONFIRM in the Maturity Dashboard for re-approval.
	listReq := req
	listReq.ClosureType = ""
	confirmRows, _, err := listCimplrRecords(ctx, pool, "confirm", listReq, false)
	if err != nil {
		return nil, err
	}
	premReq := req
	premReq.ClosureType = "PREMATURE"
	premRows, _, err := listCimplrRecords(ctx, pool, "confirm", premReq, false)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]struct{})
	allConfirm := make([]map[string]interface{}, 0, len(confirmRows)+len(premRows))
	for _, row := range append(confirmRows, premRows...) {
		id := strings.TrimSpace(fmt.Sprint(row["closure_confirm_id"]))
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		allConfirm = append(allConfirm, row)
	}
	for _, row := range allConfirm {
		status := strings.ToUpper(strings.TrimSpace(fmt.Sprint(row["closure_status"])))
		if status == constants.StatusRejected || status == "DELETED" {
			continue
		}
		posted := false
		switch v := row["accounting_posted"].(type) {
		case bool:
			posted = v
		}
		// Strict POSTED-only filter — see the comment block above.
		if !(posted || status == "POSTED") {
			continue
		}
		item := map[string]interface{}{}
		for k, v := range row {
			item[k] = v
		}
		ct := strings.ToUpper(strings.TrimSpace(fmt.Sprint(row["closure_type"])))
		item["record_kind"] = "CONFIRM"
		if ct == "PREMATURE" {
			item["queue_type"] = "PREMATURE_CONFIRM"
		} else {
			item["queue_type"] = "CONFIRM"
		}
		item["workflow_stage"] = "POSTED"
		item["can_generate_journals"] = false
		item["can_post"] = false
		journalEntryID := strings.TrimSpace(fmt.Sprint(row["journal_entry_id"]))
		if journalEntryID != "" {
			item["embedded_preview"] = cimplrBuildEmbeddedPostedPreview(ctx, pool, journalEntryID, fmt.Sprint(row["closure_type"]), fmt.Sprint(row["fd_id"]), row)
		}
		enrichCimplrAccountingListItem(ctx, pool, item)
		out = append(out, item)
	}
	return out, nil
}

func CimplrExecutionLogsAll(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req cimplrClosureListRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.PageSize <= 0 || req.PageSize > 200 {
			req.PageSize = 50
		}
		offset := 0
		if req.Page > 0 {
			offset = (req.Page - 1) * req.PageSize
		}
		_ = ensureCimplrExecutionLogTable(r.Context(), pool)
		rows, err := pool.Query(r.Context(), `
			SELECT log_id, closure_initiate_id, fd_id, closure_type, closure_confirm_id,
			       execution_source, status, message, created_at
			FROM cimplr.fd_closure_execution_log
			ORDER BY created_at DESC, log_id DESC
			LIMIT $1 OFFSET $2`, req.PageSize, offset)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to fetch execution logs: "+err.Error())
			return
		}
		defer rows.Close()
		records, err := pgx.CollectRows(rows, pgx.RowToMap)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to read execution logs: "+err.Error())
			return
		}
		api.RespondWithPayload(w, true, "", map[string]interface{}{"records": records, "total": len(records)})
	}
}

func CimplrAccountingApprovedActive(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req cimplrClosureListRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		records, err := cimplrAccountingApprovedActiveRecords(r.Context(), pool, req)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to fetch accounting approved-active: "+err.Error())
			return
		}
		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"records": records,
			"total":   len(records),
		})
	}
}

func CimplrAccountingQueue(pool *pgxpool.Pool) http.HandlerFunc {
	return CimplrAccountingApprovedActive(pool)
}

// CimplrClosureDetailCompat routes legacy /closure/detail to cimplr initiate/confirm detail by ID prefix.
func CimplrClosureDetailCompat(pool *pgxpool.Pool) http.HandlerFunc {
	initiateH := CimplrInitiateDetail(pool)
	confirmH := CimplrConfirmDetail(pool)
	legacyH := GetClosureDetail(pool)
	return func(w http.ResponseWriter, r *http.Request) {
		var raw map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		safeStr := func(key string) string {
			v, ok := raw[key]
			if !ok || v == nil {
				return ""
			}
			return strings.TrimSpace(fmt.Sprint(v))
		}
		id := firstNonEmpty(
			safeStr("closure_confirm_id"),
			safeStr("closure_initiate_id"),
			safeStr("closure_request_id"),
		)
		if id == "" {
			api.RespondWithError(w, http.StatusBadRequest, "closure_request_id, closure_initiate_id or closure_confirm_id is required")
			return
		}
		var target http.Handler
		switch {
		case strings.HasPrefix(id, "FCI-"):
			raw["closure_initiate_id"] = id
			target = initiateH
		case strings.HasPrefix(id, "FCC-"):
			raw["closure_confirm_id"] = id
			target = confirmH
		default:
			raw["closure_request_id"] = id
			target = legacyH
		}
		b, _ := json.Marshal(raw)
		r2 := r.Clone(r.Context())
		r2.Body = io.NopCloser(bytes.NewReader(b))
		r2.ContentLength = int64(len(b))
		target.ServeHTTP(w, r2)
	}
}

func cimplrVarianceOpenCount(summary map[string]interface{}) int {
	if summary == nil {
		return 0
	}
	switch v := summary["open_count"].(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	default:
		return int(cimplrFloat(v))
	}
}

// cimplrFormatOpenVarianceDebug renders open variance rows as a single log line
// so auto-maturity / approval failures show field, expected, actual, delta.
func cimplrFormatOpenVarianceDebug(summary map[string]interface{}) string {
	if summary == nil {
		return "no variance summary"
	}
	raw, _ := summary["items"].([]map[string]interface{})
	if len(raw) == 0 {
		if items, ok := summary["items"].([]interface{}); ok {
			for _, it := range items {
				if m, ok := it.(map[string]interface{}); ok {
					raw = append(raw, m)
				}
			}
		}
	}
	parts := make([]string, 0)
	for _, it := range raw {
		status := strings.ToUpper(strings.TrimSpace(fmt.Sprint(it["status"])))
		if status != "" && status != "OPEN" {
			continue
		}
		field := strings.TrimSpace(fmt.Sprint(it["field_name"]))
		if field == "" || field == "<nil>" {
			continue
		}
		parts = append(parts, fmt.Sprintf("%s expected=%s actual=%s delta=%s",
			field,
			strings.TrimSpace(fmt.Sprint(it["expected_value"])),
			strings.TrimSpace(fmt.Sprint(it["actual_value"])),
			strings.TrimSpace(fmt.Sprint(it["variance_delta"])),
		))
	}
	if len(parts) == 0 {
		return fmt.Sprintf("open_count=%d (no item detail in summary)", cimplrVarianceOpenCount(summary))
	}
	return strings.Join(parts, "; ")
}
