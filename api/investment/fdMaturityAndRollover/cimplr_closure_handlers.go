package fdMaturityAndRollover

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/approvalengine"
	"CimplrCorpSaas/api/constants"
	s3storage "CimplrCorpSaas/api/utils/s3storage"
	"CimplrCorpSaas/api/varianceengine"
	"context"
	"encoding/json"
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

	txCimplrInitiateCreate = "FD_CLOSURE_INITIATE_CREATE"
	txCimplrInitiateEdit   = "FD_CLOSURE_INITIATE_EDIT"
	txCimplrInitiateDelete = "FD_CLOSURE_INITIATE_DELETE"
	txCimplrConfirmCreate  = "FD_CLOSURE_CONFIRM_CREATE"
	txCimplrConfirmEdit    = "FD_CLOSURE_CONFIRM_EDIT"
	txCimplrConfirmDelete  = "FD_CLOSURE_CONFIRM_DELETE"
)

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
	ExpectedMaturityValue float64
	RevisedInterestAmount float64
	RevisedMaturityValue  float64
	NetPayout             float64
}

func init() {
	approvalengine.RegisterPostFinalizeHook(txCimplrInitiateCreate, cimplrInitiatePostFinalizeHook)
	approvalengine.RegisterPostFinalizeHook(txCimplrInitiateEdit, cimplrInitiatePostFinalizeHook)
	approvalengine.RegisterPostFinalizeHook(txCimplrInitiateDelete, cimplrInitiatePostFinalizeHook)
	approvalengine.RegisterPostFinalizeHook(txCimplrConfirmCreate, cimplrConfirmPostFinalizeHook)
	approvalengine.RegisterPostFinalizeHook(txCimplrConfirmEdit, cimplrConfirmPostFinalizeHook)
	approvalengine.RegisterPostFinalizeHook(txCimplrConfirmDelete, cimplrConfirmPostFinalizeHook)
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
			api.RespondWithError(w, http.StatusNotFound, "FD not found")
			return
		}
		calc, err := calculateCimplrClosure(ctx, pool, src, req.ClosureType, req.RequestedClosureDate)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "closure calculation failed: "+err.Error())
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

		tx, err := pool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "transaction start failed")
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
				rollover_type, rollover_bank_type, tentative_new_tenor_days, remarks
			) VALUES (
				$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,NULLIF($11,''),$12,$13::date,
				$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,NULLIF($24,''),NULLIF($25,''),NULLIF($26,0),$27
			) RETURNING closure_initiate_id`,
			src.FDID, nullStrOrNil(src.BookingID), nullStrOrNil(src.ConfirmationID), nullStrOrNil(src.EntityID), nullStrOrNil(src.EntityName),
			nullStrOrNil(src.BankID), nullStrOrNil(src.BankName), nullStrOrNil(src.FDRefNo), nullStrOrNil(src.BankFDRefNo),
			req.ClosureType, strings.ToUpper(strings.TrimSpace(req.ActionAtMaturity)), src.MaturityDate, nullDateArg(req.RequestedClosureDate),
			principal, src.InterestTypeCode, src.InterestRate, expectedMaturity,
			accrued, tds, netExpected,
			autoRenewal, maturityStatus, actionRequired,
			strings.ToUpper(strings.TrimSpace(req.RolloverType)), strings.ToUpper(strings.TrimSpace(req.RolloverBankType)), req.TentativeNewTenorDays, req.Remarks,
		).Scan(&closureInitiateID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "initiate create failed: "+err.Error())
			return
		}

		if err := insertCimplrCalculation(ctx, tx, closureInitiateID, "", src, calc); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "calculation snapshot failed: "+err.Error())
			return
		}
		if err := insertCimplrInitiateAudit(ctx, tx, closureInitiateID, "CREATE", "PENDING_APPROVAL", firstNonEmpty(req.Reason, "Create FD closure initiate"), req.UserID, nil); err != nil {
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
		instanceID, instErr := createCimplrApprovalInstance(ctx, pool, txCimplrInitiateCreate, "CREATE", closureInitiateID, "cimplr.fd_closure_initiate", "cimplr.fd_closure_initiate_audit", "closure_initiate_id", src.EntityID, principal, req.UserID, userEmail)
		if instErr != nil {
			api.LogError("[CimplrFDClosure] initiate approval create failed: %v", instErr)
		}
		if instanceID != "" {
			_, _ = pool.Exec(ctx, `UPDATE cimplr.fd_closure_initiate SET approval_instance_id=$1 WHERE closure_initiate_id=$2`, instanceID, closureInitiateID)
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"closure_initiate_id":  closureInitiateID,
			"approval_instance_id": instanceID,
			"calculation":          calc,
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
			api.RespondWithError(w, http.StatusNotFound, "FD not found")
			return
		}
		calc, err := calculateCimplrClosure(r.Context(), pool, src, req.ClosureType, req.RequestedClosureDate)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "closure calculation failed: "+err.Error())
			return
		}
		recordID := firstNonEmpty(req.ClosureInitiateID, "PREVIEW-"+varianceengine.NewRunID())
		summary := previewCimplrInitiateVariance(recordID, req, src, calc)
		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"fd_source":         src,
			"calculation":       calc,
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
			api.RespondWithError(w, http.StatusBadRequest, "closure_initiate_id is required")
			return
		}

		ctx := r.Context()
		oldRow, err := loadCimplrInitiateOld(ctx, pool, req.ClosureInitiateID)
		if err != nil {
			api.RespondWithError(w, http.StatusNotFound, "initiate record not found")
			return
		}
		if fmt.Sprint(oldRow["closure_status"]) == "CONFIRM" {
			api.RespondWithError(w, http.StatusBadRequest, "approved initiate records cannot be edited; create confirm instead")
			return
		}
		src, err := loadCimplrFDSource(ctx, pool, fmt.Sprint(oldRow["fd_id"]))
		if err != nil {
			api.RespondWithError(w, http.StatusNotFound, "FD not found")
			return
		}
		if req.ClosureType == "" {
			req.ClosureType = fmt.Sprint(oldRow["closure_type"])
		}
		req.ClosureType = normalizeCimplrClosureType(req.ClosureType, req.ActionAtMaturity)
		calc, err := calculateCimplrClosure(ctx, pool, src, req.ClosureType, req.RequestedClosureDate)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "closure calculation failed: "+err.Error())
			return
		}

		principal := chooseFloat(req.PrincipalAmount, src.Principal)
		accrued := chooseFloat(req.AccruedInterestTillDate, calc.AccruedInterest)
		tds := chooseFloat(req.TDSExpected, calc.TDSAmount)
		expectedMaturity := chooseFloat(req.ExpectedMaturityValue, calc.ExpectedMaturityValue)
		netExpected := chooseFloat(req.NetExpectedAmount, calc.NetPayout)

		tx, err := pool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "transaction start failed")
			return
		}
		defer tx.Rollback(ctx) //nolint:errcheck
		_, err = tx.Exec(ctx, `
			UPDATE cimplr.fd_closure_initiate
			SET closure_type=$1, action_at_maturity=NULLIF($2,''), requested_closure_date=$3::date,
			    principal_amount=$4, interest_type_code=$5, interest_rate=$6,
			    expected_maturity_value=$7, accrued_interest_till_date=$8, tds_expected=$9,
			    net_expected_amount=$10, maturity_status=$11, rollover_type=NULLIF($12,''),
			    rollover_bank_type=NULLIF($13,''), tentative_new_tenor_days=NULLIF($14,0), remarks=$15
			WHERE closure_initiate_id=$16 AND is_deleted=false`,
			req.ClosureType, strings.ToUpper(strings.TrimSpace(req.ActionAtMaturity)), nullDateArg(req.RequestedClosureDate),
			principal, src.InterestTypeCode, src.InterestRate, expectedMaturity, accrued, tds, netExpected,
			firstNonEmpty(strings.ToUpper(strings.TrimSpace(req.MaturityStatus)), deriveCimplrMaturityStatus(src.MaturityDate)),
			strings.ToUpper(strings.TrimSpace(req.RolloverType)), strings.ToUpper(strings.TrimSpace(req.RolloverBankType)), req.TentativeNewTenorDays, req.Remarks,
			req.ClosureInitiateID,
		)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "initiate edit failed: "+err.Error())
			return
		}
		if err := insertCimplrCalculation(ctx, tx, req.ClosureInitiateID, "", src, calc); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "calculation snapshot failed: "+err.Error())
			return
		}
		if err := insertCimplrInitiateAudit(ctx, tx, req.ClosureInitiateID, "EDIT", "PENDING_EDIT_APPROVAL", firstNonEmpty(req.Reason, "Edit FD closure initiate"), req.UserID, oldRow); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "initiate audit failed: "+err.Error())
			return
		}
		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailed)
			return
		}

		varianceSummary, _ := persistCimplrInitiateVariances(ctx, pool, req.ClosureInitiateID, req, src, calc)
		_ = approvalengine.CancelPendingInstances(ctx, pool, cimplrClosureModule, req.ClosureInitiateID, userEmail)
		instanceID, instErr := createCimplrApprovalInstance(ctx, pool, txCimplrInitiateEdit, "EDIT", req.ClosureInitiateID, "cimplr.fd_closure_initiate", "cimplr.fd_closure_initiate_audit", "closure_initiate_id", src.EntityID, principal, req.UserID, userEmail)
		if instErr != nil {
			api.LogError("[CimplrFDClosure] initiate edit approval failed: %v", instErr)
		}
		if instanceID != "" {
			_, _ = pool.Exec(ctx, `UPDATE cimplr.fd_closure_initiate SET approval_instance_id=$1 WHERE closure_initiate_id=$2`, instanceID, req.ClosureInitiateID)
		}

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
			api.RespondWithError(w, http.StatusBadRequest, "closure_initiate_id is required")
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
			if fmt.Sprint(oldRow["closure_status"]) == "CONFIRM" {
				var confirmCount int
				_ = pool.QueryRow(r.Context(), `SELECT COUNT(*) FROM cimplr.fd_closure_confirm WHERE closure_initiate_id=$1 AND is_deleted=false`, id).Scan(&confirmCount)
				if confirmCount > 0 {
					res["success"] = false
					res["error"] = "cannot delete initiate after confirm has been created"
					results = append(results, res)
					continue
				}
			}
			if err := insertCimplrInitiateAudit(r.Context(), pool, id, "DELETE", "PENDING_DELETE_APPROVAL", firstNonEmpty(req.Comment, "Delete FD closure initiate"), req.UserID, oldRow); err != nil {
				res["success"] = false
				res["error"] = err.Error()
				results = append(results, res)
				continue
			}
			instanceID, _ := createCimplrApprovalInstance(r.Context(), pool, txCimplrInitiateDelete, "DELETE", id, "cimplr.fd_closure_initiate", "cimplr.fd_closure_initiate_audit", "closure_initiate_id", fmt.Sprint(oldRow["entity_id"]), 0, req.UserID, userEmail)
			if instanceID != "" {
				_, _ = pool.Exec(r.Context(), `UPDATE cimplr.fd_closure_initiate SET approval_instance_id=$1 WHERE closure_initiate_id=$2`, instanceID, id)
			}
			res["success"] = true
			res["approval_instance_id"] = instanceID
			results = append(results, res)
		}
		api.RespondWithPayload(w, true, "", map[string]interface{}{"results": results})
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
			api.RespondWithError(w, http.StatusBadRequest, "closure_initiate_id is required")
			return
		}
		results := cimplrApproveInitiates(r.Context(), pool, ids, req.UserID, userEmail, req.Comment)
		api.RespondWithPayload(w, true, "", map[string]interface{}{"results": results})
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
			api.RespondWithError(w, http.StatusBadRequest, "closure_initiate_id is required")
			return
		}
		results := cimplrRejectInitiates(r.Context(), pool, ids, req.UserID, userEmail, req.Comment)
		api.RespondWithPayload(w, true, "", map[string]interface{}{"results": results})
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
			api.RespondWithError(w, http.StatusBadRequest, "closure_initiate_id is required")
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
			api.RespondWithError(w, http.StatusNotFound, "FD not found")
			return
		}
		closureType := fmt.Sprint(initiate["closure_type"])
		calc, err := calculateCimplrClosure(ctx, pool, src, closureType, firstNonEmpty(req.RequestedClosureDate, fmt.Sprint(initiate["requested_closure_date"])))
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "closure calculation failed: "+err.Error())
			return
		}

		principalExpected := chooseFloat(req.PrincipalExpected, src.Principal)
		interestExpected := chooseFloat(req.InterestExpected, calc.AccruedInterest)
		tdsExpected := chooseFloat(req.TDSExpected, calc.TDSAmount)
		netExpected := chooseFloat(req.NetExpected, calc.NetPayout)
		principalReceived := chooseFloat(req.PrincipalReceived, principalExpected)
		interestReceived := chooseFloat(req.InterestReceived, interestExpected)
		tdsDeducted := chooseFloat(req.TDSDeducted, tdsExpected)
		netReceived := chooseFloat(req.NetAmountReceived, netExpected)

		tx, err := pool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "transaction start failed")
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
			api.RespondWithError(w, http.StatusInternalServerError, "calculation snapshot failed: "+err.Error())
			return
		}
		if err := insertCimplrConfirmAudit(ctx, tx, closureConfirmID, req.ClosureInitiateID, "CREATE", "PENDING_APPROVAL", firstNonEmpty(req.Reason, "Create FD closure confirm"), req.UserID, nil); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "confirm audit failed: "+err.Error())
			return
		}
		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailed)
			return
		}

		varianceSummary, _ := persistCimplrConfirmVariances(ctx, pool, closureConfirmID, req, src, calc)
		instanceID, instErr := createCimplrApprovalInstance(ctx, pool, txCimplrConfirmCreate, "CREATE", closureConfirmID, "cimplr.fd_closure_confirm", "cimplr.fd_closure_confirm_audit", "closure_confirm_id", src.EntityID, principalExpected, req.UserID, userEmail)
		if instErr != nil {
			api.LogError("[CimplrFDClosure] confirm approval create failed: %v", instErr)
		}
		if instanceID != "" {
			_, _ = pool.Exec(ctx, `UPDATE cimplr.fd_closure_confirm SET approval_instance_id=$1 WHERE closure_confirm_id=$2`, instanceID, closureConfirmID)
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"closure_confirm_id":   closureConfirmID,
			"approval_instance_id": instanceID,
			"calculation":          calc,
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
			api.RespondWithError(w, http.StatusNotFound, "FD not found")
			return
		}
		calc, err := calculateCimplrClosure(r.Context(), pool, src, "PREMATURE", req.RequestedClosureDate)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "premature calculation failed: "+err.Error())
			return
		}
		recordID := firstNonEmpty(req.ClosureConfirmID, "PREVIEW-"+varianceengine.NewRunID())
		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"fd_source":         src,
			"calculation":       calc,
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
			api.RespondWithError(w, http.StatusNotFound, "FD not found")
			return
		}
		calc, err := calculateCimplrClosure(ctx, pool, src, "PREMATURE", req.RequestedClosureDate)
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

		tx, err := pool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "transaction start failed")
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
				NULL,$1,$2,$3,$4,$5,$6,$7,$8,$9,'PREMATURE',NULLIF($10,''),NULLIF($11,''),$12::date,$13::date,
				$14,$15,$16,$17,$18,$19,$20,$21,$22,NULLIF($23,''),NULLIF($24,''),$25
			) RETURNING closure_confirm_id`,
			src.FDID, nullStrOrNil(src.BookingID), nullStrOrNil(src.ConfirmationID), nullStrOrNil(src.EntityID), nullStrOrNil(src.EntityName),
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
		if err := insertCimplrCalculation(ctx, tx, "", closureConfirmID, src, calc); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "calculation snapshot failed: "+err.Error())
			return
		}
		if err := insertCimplrConfirmAudit(ctx, tx, closureConfirmID, "", "CREATE", "PENDING_APPROVAL", firstNonEmpty(req.Reason, "Create premature closure"), req.UserID, nil); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "premature audit failed: "+err.Error())
			return
		}
		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailed)
			return
		}

		varianceSummary, _ := persistCimplrConfirmVariances(ctx, pool, closureConfirmID, req, src, calc)
		instanceID, instErr := createCimplrApprovalInstance(ctx, pool, txCimplrConfirmCreate, "CREATE", closureConfirmID, "cimplr.fd_closure_confirm", "cimplr.fd_closure_confirm_audit", "closure_confirm_id", src.EntityID, principalExpected, req.UserID, userEmail)
		if instErr != nil {
			api.LogError("[CimplrFDClosure] premature approval create failed: %v", instErr)
		}
		if instanceID != "" {
			_, _ = pool.Exec(ctx, `UPDATE cimplr.fd_closure_confirm SET approval_instance_id=$1 WHERE closure_confirm_id=$2`, instanceID, closureConfirmID)
		}
		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"closure_confirm_id":   closureConfirmID,
			"approval_instance_id": instanceID,
			"calculation":          calc,
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
			api.RespondWithError(w, http.StatusBadRequest, "closure_initiate_id or closure_confirm_id is required")
			return
		}
		initiate, err := loadCimplrInitiateOld(r.Context(), pool, req.ClosureInitiateID)
		if err != nil {
			api.RespondWithError(w, http.StatusNotFound, "initiate record not found")
			return
		}
		src, err := loadCimplrFDSource(r.Context(), pool, fmt.Sprint(initiate["fd_id"]))
		if err != nil {
			api.RespondWithError(w, http.StatusNotFound, "FD not found")
			return
		}
		closureType := fmt.Sprint(initiate["closure_type"])
		calc, err := calculateCimplrClosure(r.Context(), pool, src, closureType, firstNonEmpty(req.RequestedClosureDate, fmt.Sprint(initiate["requested_closure_date"])))
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "closure calculation failed: "+err.Error())
			return
		}
		recordID := firstNonEmpty(req.ClosureConfirmID, "PREVIEW-"+varianceengine.NewRunID())
		summary := previewCimplrConfirmVariance(recordID, req, src, calc)
		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"fd_source":         src,
			"initiate":          initiate,
			"calculation":       calc,
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
			api.RespondWithError(w, http.StatusBadRequest, "closure_confirm_id is required")
			return
		}
		ctx := r.Context()
		oldRow, err := loadCimplrConfirmOld(ctx, pool, req.ClosureConfirmID)
		if err != nil {
			api.RespondWithError(w, http.StatusNotFound, "confirm record not found")
			return
		}
		if fmt.Sprint(oldRow["closure_status"]) == "POSTED" || oldRow["accounting_posted"] == true {
			api.RespondWithError(w, http.StatusBadRequest, "posted confirm records cannot be edited")
			return
		}
		src, err := loadCimplrFDSource(ctx, pool, fmt.Sprint(oldRow["fd_id"]))
		if err != nil {
			api.RespondWithError(w, http.StatusNotFound, "FD not found")
			return
		}
		closureType := fmt.Sprint(oldRow["closure_type"])
		calc, err := calculateCimplrClosure(ctx, pool, src, closureType, firstNonEmpty(req.RequestedClosureDate, fmt.Sprint(oldRow["requested_closure_date"])))
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "closure calculation failed: "+err.Error())
			return
		}
		principalExpected := chooseFloat(req.PrincipalExpected, src.Principal)
		interestExpected := chooseFloat(req.InterestExpected, calc.AccruedInterest)
		tdsExpected := chooseFloat(req.TDSExpected, calc.TDSAmount)
		netExpected := chooseFloat(req.NetExpected, calc.NetPayout)
		principalReceived := chooseFloat(req.PrincipalReceived, principalExpected)
		interestReceived := chooseFloat(req.InterestReceived, interestExpected)
		tdsDeducted := chooseFloat(req.TDSDeducted, tdsExpected)
		netReceived := chooseFloat(req.NetAmountReceived, netExpected)

		tx, err := pool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "transaction start failed")
			return
		}
		defer tx.Rollback(ctx) //nolint:errcheck
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
		if err := insertCimplrCalculation(ctx, tx, fmt.Sprint(oldRow["closure_initiate_id"]), req.ClosureConfirmID, src, calc); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "calculation snapshot failed: "+err.Error())
			return
		}
		if err := insertCimplrConfirmAudit(ctx, tx, req.ClosureConfirmID, fmt.Sprint(oldRow["closure_initiate_id"]), "EDIT", "PENDING_EDIT_APPROVAL", firstNonEmpty(req.Reason, "Edit FD closure confirm"), req.UserID, oldRow); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "confirm audit failed: "+err.Error())
			return
		}
		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailed)
			return
		}

		varianceSummary, _ := persistCimplrConfirmVariances(ctx, pool, req.ClosureConfirmID, req, src, calc)
		_ = approvalengine.CancelPendingInstances(ctx, pool, cimplrClosureModule, req.ClosureConfirmID, userEmail)
		instanceID, instErr := createCimplrApprovalInstance(ctx, pool, txCimplrConfirmEdit, "EDIT", req.ClosureConfirmID, "cimplr.fd_closure_confirm", "cimplr.fd_closure_confirm_audit", "closure_confirm_id", src.EntityID, principalExpected, req.UserID, userEmail)
		if instErr != nil {
			api.LogError("[CimplrFDClosure] confirm edit approval failed: %v", instErr)
		}
		if instanceID != "" {
			_, _ = pool.Exec(ctx, `UPDATE cimplr.fd_closure_confirm SET approval_instance_id=$1 WHERE closure_confirm_id=$2`, instanceID, req.ClosureConfirmID)
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
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
			api.RespondWithError(w, http.StatusBadRequest, "closure_confirm_id is required")
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
			if err := insertCimplrConfirmAudit(r.Context(), pool, id, fmt.Sprint(oldRow["closure_initiate_id"]), "DELETE", "PENDING_DELETE_APPROVAL", firstNonEmpty(req.Comment, "Delete FD closure confirm"), req.UserID, oldRow); err != nil {
				res["success"] = false
				res["error"] = err.Error()
				results = append(results, res)
				continue
			}
			instanceID, _ := createCimplrApprovalInstance(r.Context(), pool, txCimplrConfirmDelete, "DELETE", id, "cimplr.fd_closure_confirm", "cimplr.fd_closure_confirm_audit", "closure_confirm_id", fmt.Sprint(oldRow["entity_id"]), 0, req.UserID, userEmail)
			if instanceID != "" {
				_, _ = pool.Exec(r.Context(), `UPDATE cimplr.fd_closure_confirm SET approval_instance_id=$1 WHERE closure_confirm_id=$2`, instanceID, id)
			}
			res["success"] = true
			res["approval_instance_id"] = instanceID
			results = append(results, res)
		}
		api.RespondWithPayload(w, true, "", map[string]interface{}{"results": results})
	}
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
			api.RespondWithError(w, http.StatusBadRequest, "closure_confirm_id is required")
			return
		}
		results := cimplrApproveConfirms(r.Context(), pool, ids, req.UserID, userEmail, req.Comment)
		api.RespondWithPayload(w, true, "", map[string]interface{}{"results": results})
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
			api.RespondWithError(w, http.StatusBadRequest, "closure_confirm_id is required")
			return
		}
		results := cimplrRejectConfirms(r.Context(), pool, ids, req.UserID, userEmail, req.Comment)
		api.RespondWithPayload(w, true, "", map[string]interface{}{"results": results})
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
				COALESCE(ca.processing_status, ia.processing_status, '') AS latest_processing_status,
				COALESCE(ca.requested_by, ia.requested_by, '') AS latest_requested_by,
				COALESCE(ca.requested_at, ia.requested_at) AS latest_requested_at,
				COALESCE(ca.checker_by, ia.checker_by, '') AS latest_checker_by,
				COALESCE(ca.checker_at, ia.checker_at) AS latest_checker_at
			FROM investment.fd_master m
			LEFT JOIN investment.fd_booking_request b ON b.booking_id=m.booking_id
			LEFT JOIN LATERAL (
				SELECT * FROM cimplr.fd_closure_confirm c
				WHERE c.fd_id=m.fd_id AND COALESCE(c.is_deleted,false)=false
				ORDER BY c.closure_confirm_id DESC LIMIT 1
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
				ORDER BY a.requested_at DESC, a.audit_id DESC LIMIT 1
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
		api.RespondWithError(w, http.StatusBadRequest, "closure_initiate_id or closure_confirm_id is required")
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
			api.RespondWithError(w, http.StatusBadRequest, "closure_initiate_id is required")
			return
		}
		header, err := loadCimplrInitiateOld(r.Context(), pool, id)
		if err != nil {
			api.RespondWithError(w, http.StatusNotFound, "initiate record not found")
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
			api.RespondWithError(w, http.StatusBadRequest, "closure_confirm_id is required")
			return
		}
		header, err := loadCimplrConfirmOld(r.Context(), pool, id)
		if err != nil {
			api.RespondWithError(w, http.StatusNotFound, "confirm record not found")
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
			api.RespondWithError(w, http.StatusBadRequest, "closure_initiate_id or closure_confirm_id is required")
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
		api.RespondWithError(w, http.StatusBadRequest, "closure_initiate_id or closure_confirm_id is required")
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
		if transactionType == txCimplrInitiateCreate || transactionType == txCimplrInitiateDelete {
			_, _ = pool.Exec(ctx, `UPDATE cimplr.fd_closure_initiate SET closure_status='REJECTED' WHERE closure_initiate_id=$1 AND is_deleted=false`, recordID)
		}
		return
	}
	switch transactionType {
	case txCimplrInitiateDelete:
		_, _ = pool.Exec(ctx, `UPDATE cimplr.fd_closure_initiate SET closure_status='DELETED', is_deleted=true, is_active=false WHERE closure_initiate_id=$1`, recordID)
	case txCimplrInitiateCreate, txCimplrInitiateEdit:
		_, _ = pool.Exec(ctx, `UPDATE cimplr.fd_closure_initiate SET closure_status='CONFIRM' WHERE closure_initiate_id=$1 AND is_deleted=false`, recordID)
	}
}

func cimplrConfirmPostFinalizeHook(ctx context.Context, pool *pgxpool.Pool, recordID, transactionType, finalStatus, actorEmail, comment string) {
	if finalStatus == approvalengine.InstStatusRejected {
		if transactionType == txCimplrConfirmCreate || transactionType == txCimplrConfirmDelete {
			_, _ = pool.Exec(ctx, `UPDATE cimplr.fd_closure_confirm SET closure_status='REJECTED' WHERE closure_confirm_id=$1 AND is_deleted=false`, recordID)
		}
		return
	}
	switch transactionType {
	case txCimplrConfirmDelete:
		_, _ = pool.Exec(ctx, `UPDATE cimplr.fd_closure_confirm SET closure_status='DELETED', is_deleted=true, is_active=false WHERE closure_confirm_id=$1`, recordID)
	case txCimplrConfirmCreate, txCimplrConfirmEdit:
		if err := finalizeCimplrConfirmApproval(ctx, pool, recordID, actorEmail, comment); err != nil {
			api.LogError("[CimplrFDClosure] confirm finalize failed confirm_id=%s: %v", recordID, err)
			_, _ = pool.Exec(ctx, `UPDATE cimplr.fd_closure_confirm SET posting_status='FAILED' WHERE closure_confirm_id=$1`, recordID)
		}
	}
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
			tx, err := pool.Begin(ctx)
			if err != nil {
				res["success"] = false
				res["error"] = err.Error()
				results = append(results, res)
				continue
			}
			_, err = tx.Exec(ctx, `UPDATE cimplr.fd_closure_initiate_audit SET processing_status='APPROVED', checker_by=$1, checker_at=NOW(), checker_comment=$2 WHERE closure_initiate_id=$3 AND processing_status LIKE 'PENDING%'`, userEmail, comment, id)
			if err == nil {
				_, err = tx.Exec(ctx, `UPDATE cimplr.fd_closure_initiate SET closure_status='CONFIRM' WHERE closure_initiate_id=$1 AND is_deleted=false`, id)
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

func cimplrApproveConfirms(ctx context.Context, pool *pgxpool.Pool, ids []string, userID, userEmail, comment string) []map[string]interface{} {
	results := make([]map[string]interface{}, 0, len(ids))
	for _, id := range ids {
		res := map[string]interface{}{"closure_confirm_id": id}
		if acted, err := cimplrActOnApproval(ctx, pool, id, userID, userEmail, approvalengine.ActionApproved, firstNonEmpty(comment, "Approved FD closure confirm")); err != nil {
			res["success"] = false
			res["error"] = err.Error()
		} else if acted {
			res["success"] = true
			res["approval_engine"] = true
		} else {
			_, err := pool.Exec(ctx, `UPDATE cimplr.fd_closure_confirm_audit SET processing_status='APPROVED', checker_by=$1, checker_at=NOW(), checker_comment=$2 WHERE closure_confirm_id=$3 AND processing_status LIKE 'PENDING%'`, userEmail, comment, id)
			if err == nil {
				err = finalizeCimplrConfirmApproval(ctx, pool, id, userEmail, comment)
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
	var eyeID string
	err := pool.QueryRow(ctx, `
		SELECT ie.instance_eye_id
		FROM uam.approval_instance i
		JOIN uam.approval_instance_eye ie ON ie.instance_id=i.instance_id AND ie.status='ACTIVE'
		JOIN uam.approval_matrix_eye_member m ON m.eye_id=ie.matrix_eye_id
		  AND m.member_type='APPROVER' AND m.is_active=true AND m.is_deleted=false
		  AND m.assignment_type IN ('USER_ONLY','ROLE_USER') AND m.user_id=$2
		WHERE i.record_id=$1 AND i.module_code=$3 AND i.status='PENDING'
		ORDER BY ie.position ASC LIMIT 1`,
		recordID, userID, cimplrClosureModule,
	).Scan(&eyeID)
	if err == nil && eyeID != "" {
		return true, approvalengine.RecordAction(ctx, pool, approvalengine.ActionRequest{
			InstanceEyeID: eyeID,
			ActorUserID:   userID,
			ActorEmail:    userEmail,
			ActionType:    action,
			Comment:       comment,
		})
	}
	var pending int
	_ = pool.QueryRow(ctx, `SELECT COUNT(*) FROM uam.approval_instance WHERE record_id=$1 AND module_code=$2 AND status='PENDING'`, recordID, cimplrClosureModule).Scan(&pending)
	if pending > 0 {
		return false, fmt.Errorf("not your turn in approval sequence")
	}
	return false, nil
}

func finalizeCimplrConfirmApproval(ctx context.Context, pool *pgxpool.Pool, closureConfirmID, actorEmail, comment string) error {
	var closureType, resolutionAction string
	var hasUnresolved, accountingPosted bool
	if err := pool.QueryRow(ctx, `
		SELECT closure_type, COALESCE(resolution_action,''), has_unresolved_variance, accounting_posted
		FROM cimplr.fd_closure_confirm
		WHERE closure_confirm_id=$1 AND is_deleted=false`, closureConfirmID,
	).Scan(&closureType, &resolutionAction, &hasUnresolved, &accountingPosted); err != nil {
		return err
	}
	if accountingPosted {
		return nil
	}
	if hasUnresolved && resolutionAction != "ACCEPT" {
		return fmt.Errorf("confirm has unresolved variance; set resolution_action=ACCEPT before approval")
	}
	if closureType == "ROLLOVER" {
		return createCimplrRolloverBooking(ctx, pool, closureConfirmID, actorEmail, comment)
	}
	return postCimplrClosureJournals(ctx, pool, closureConfirmID, actorEmail, comment)
}

func postCimplrClosureJournals(ctx context.Context, pool *pgxpool.Pool, closureConfirmID, actorEmail, comment string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	var fdID, closureType, entityID, entityName, sourceAccountID string
	var principal, interest, tds, penalty, netPayout float64
	var accountingPosted bool
	err = tx.QueryRow(ctx, `
		SELECT c.fd_id, c.closure_type, COALESCE(c.entity_id,''), COALESCE(c.entity_name,''),
		       COALESCE(c.principal_received, c.principal_expected, 0),
		       COALESCE(c.interest_received, c.interest_expected, 0),
		       COALESCE(c.tds_deducted, c.tds_expected, 0),
		       CASE WHEN c.closure_type='PREMATURE' THEN COALESCE(pc.penalty_amount,0) ELSE 0 END,
		       COALESCE(c.net_amount_received, c.net_expected, 0),
		       COALESCE(b.source_account_id,''), c.accounting_posted
		FROM cimplr.fd_closure_confirm c
		LEFT JOIN cimplr.fd_closure_premature_confirm pc ON pc.closure_confirm_id=c.closure_confirm_id AND pc.is_deleted=false
		LEFT JOIN investment.fd_master m ON m.fd_id=c.fd_id
		LEFT JOIN investment.fd_booking_request b ON b.booking_id=m.booking_id
		WHERE c.closure_confirm_id=$1 AND c.is_deleted=false
		FOR UPDATE OF c`, closureConfirmID,
	).Scan(&fdID, &closureType, &entityID, &entityName, &principal, &interest, &tds, &penalty, &netPayout, &sourceAccountID, &accountingPosted)
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
		_ = tx.QueryRow(ctx, `SELECT COALESCE(account_number,''), COALESCE(account_nickname,'') FROM public.masterbankaccount WHERE account_id=$1 LIMIT 1`, sourceAccountID).Scan(&bankAccountNumber, &bankAccountName)
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
	if err := insertLine("TDS-RECEIVABLE", "TDS Receivable", "ASSET", tds, 0, "TDS withheld at source"); err != nil {
		return err
	}
	if err := insertLine("PENALTY-EXP", "Premature Withdrawal Penalty", "EXPENSE", penalty, 0, "Premature withdrawal penalty"); err != nil {
		return err
	}
	if err := insertLine("FD-INVEST-"+fdID, "FD Investment - "+fdID, "ASSET", 0, principal, "Close FD investment asset"); err != nil {
		return err
	}
	interestCredit := roundToFour(totalCredit - principal)
	if err := insertLine("FD-INT-INC-"+fdID, "Interest Income - FD", "INCOME", 0, interestCredit, "Interest recognised on closure"); err != nil {
		return err
	}

	newFDStatus := "MATURED"
	if closureType == "PREMATURE" {
		newFDStatus = "PREMATURELY_CLOSED"
	}
	_, err = tx.Exec(ctx, `UPDATE investment.fd_master SET fd_status=$1, closed_at=NOW(), closed_by=$2, accounting_posted=true, closure_request_id=$3, updated_by=$2, updated_at=NOW() WHERE fd_id=$4`,
		newFDStatus, actorEmail, closureConfirmID, fdID)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE cimplr.fd_closure_confirm SET closure_status='POSTED', posting_status='POSTED', accounting_posted=true, journal_entry_id=$1 WHERE closure_confirm_id=$2`, entryID, closureConfirmID)
	if err != nil {
		return err
	}
	if err := insertCimplrConfirmAudit(ctx, tx, closureConfirmID, "", "POST", "POSTED", firstNonEmpty(comment, "Journals posted on approval"), actorEmail, map[string]interface{}{"accounting_posted": false, "journal_entry_id": ""}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func createCimplrRolloverBooking(ctx context.Context, pool *pgxpool.Pool, closureConfirmID, actorEmail, comment string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	var (
		fdID, entityID, entityName, bankID, bankName, bankConfigID, sourceAccountID string
		frequencyID, tdsPlanID, dayCountCode, interestTypeCode                      string
		newBankID, newBankName, newAccountID, amountBasis                           string
		newFDAmount, newInterestRate                                                float64
		principal, interest, tds, netPayout                                         float64
		newTenorDays                                                                int
		expectedStart, expectedMaturity                                             time.Time
		accountingPosted                                                            bool
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
		       c.accounting_posted
		FROM cimplr.fd_closure_confirm c
		JOIN cimplr.fd_closure_rollover_confirm rc ON rc.closure_confirm_id=c.closure_confirm_id AND rc.is_deleted=false
		JOIN investment.fd_master m ON m.fd_id=c.fd_id
		LEFT JOIN investment.fd_booking_request b ON b.booking_id=m.booking_id
		WHERE c.closure_confirm_id=$1 AND c.is_deleted=false
		FOR UPDATE OF c`, closureConfirmID,
	).Scan(&fdID, &entityID, &entityName, &bankID, &bankName, &bankConfigID, &sourceAccountID, &frequencyID, &tdsPlanID, &dayCountCode, &interestTypeCode, &newBankID, &newBankName, &newAccountID, &amountBasis, &newFDAmount, &newTenorDays, &newInterestRate, &expectedStart, &expectedMaturity, &principal, &interest, &tds, &netPayout, &accountingPosted)
	if err != nil {
		return err
	}
	if accountingPosted {
		return tx.Commit(ctx)
	}
	targetBankID := firstNonEmpty(newBankID, bankID)
	targetBankName := firstNonEmpty(newBankName, bankName)
	targetAccountID := firstNonEmpty(newAccountID, sourceAccountID)
	if targetBankID == "" || targetBankName == "" || targetAccountID == "" || entityID == "" || entityName == "" {
		return fmt.Errorf("rollover booking requires entity, bank and source account details")
	}
	if newFDAmount <= 0 || newTenorDays <= 0 || newInterestRate <= 0 {
		return fmt.Errorf("rollover booking requires positive amount, tenor and interest rate")
	}
	if !expectedMaturity.After(expectedStart) {
		expectedMaturity = expectedStart.AddDate(0, 0, newTenorDays)
	}
	var sourceAccountNumber string
	var sourceAccountName string
	_ = tx.QueryRow(ctx, `SELECT COALESCE(account_number,''), COALESCE(account_nickname,'') FROM public.masterbankaccount WHERE account_id=$1 LIMIT 1`, targetAccountID).Scan(&sourceAccountNumber, &sourceAccountName)
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
	if err := insertLine("TDS-RECEIVABLE", "TDS Receivable", "ASSET", tds, 0, "TDS withheld at source on rollover"); err != nil {
		return err
	}
	if err := insertLine("FD-INVEST-"+fdID, "FD Investment - "+fdID, "ASSET", 0, principal, "Close old FD investment asset on rollover"); err != nil {
		return err
	}
	interestCredit := roundToFour(totalCredit - principal)
	if err := insertLine("FD-INT-INC-"+fdID, "Interest Income - FD", "INCOME", 0, interestCredit, "Interest recognised on rollover closure"); err != nil {
		return err
	}

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
		closureConfirmID, actorEmail,
	).Scan(&newBookingID)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE cimplr.fd_closure_rollover_confirm SET new_booking_id=$1, rollover_approval_status='APPROVED' WHERE closure_confirm_id=$2`, newBookingID, closureConfirmID)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE cimplr.fd_closure_confirm SET closure_status='POSTED', posting_status='POSTED', accounting_posted=true, journal_entry_id=$1, new_booking_id=$2 WHERE closure_confirm_id=$3`, entryID, newBookingID, closureConfirmID)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE investment.fd_master SET fd_status='ROLLED_OVER', closed_at=NOW(), closed_by=$1, accounting_posted=true, closure_request_id=$2, updated_by=$1, updated_at=NOW() WHERE fd_id=$3`, actorEmail, closureConfirmID, fdID)
	if err != nil {
		return err
	}
	if err := insertCimplrConfirmAudit(ctx, tx, closureConfirmID, "", "POST", "POSTED", firstNonEmpty(comment, "Rollover journal and booking created on approval"), actorEmail, map[string]interface{}{"accounting_posted": false, "journal_entry_id": "", "new_booking_id": ""}); err != nil {
		return err
	}
	return tx.Commit(ctx)
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

func calculateCimplrClosure(ctx context.Context, pool *pgxpool.Pool, src cimplrFDSource, closureType, requestedDate string) (cimplrClosureCalc, error) {
	calcDate := time.Now()
	if t, ok := parseCimplrDate(requestedDate); ok {
		calcDate = t
	}
	if closureType == "PAYOUT" && !src.MaturityDate.IsZero() {
		calcDate = src.MaturityDate
	}
	accruedDays := int(calcDate.Sub(src.StartDate).Hours() / 24)
	if accruedDays < 0 {
		accruedDays = 0
	}
	var accrued, tds float64
	_ = pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(period_interest_accrued),0), COALESCE(SUM(tds_deducted_in_period),0)
		FROM investment.fd_accrual_ledger
		WHERE fd_id=$1 AND COALESCE(is_deleted,false)=false`, src.FDID,
	).Scan(&accrued, &tds)
	if accrued == 0 && src.Principal > 0 && src.InterestRate > 0 && accruedDays > 0 {
		accrued = roundToFour(src.Principal * src.InterestRate * float64(accruedDays) / 36500)
	}
	calc := cimplrClosureCalc{
		ClosureType:           closureType,
		CalculationDate:       calcDate,
		AccruedDays:           accruedDays,
		AccruedInterest:       roundToFour(accrued),
		TDSAmount:             roundToFour(tds),
		ApplicableRate:        src.InterestRate,
		ExpectedMaturityValue: roundToFour(src.Principal + accrued),
		RevisedInterestAmount: roundToFour(accrued),
		RevisedMaturityValue:  roundToFour(src.Principal + accrued),
		NetPayout:             roundToFour(src.Principal + accrued - tds),
	}
	if closureType == "PREMATURE" {
		var minHeldDays int
		_ = pool.QueryRow(ctx, `
			SELECT COALESCE(penalty_id,''), COALESCE(penalty_type,''), COALESCE(penalty_value,0),
			       COALESCE(no_interest_if_withdrawn_before,0)
			FROM investment.fd_penalty_structure_master
			WHERE bank_code=$1 AND COALESCE(is_deleted,false)=false
			  AND (min_held_days IS NULL OR $2 >= min_held_days)
			  AND (max_held_days IS NULL OR $2 <= max_held_days)
			  AND (min_amount_range IS NULL OR $3 >= min_amount_range)
			  AND (max_amount_range IS NULL OR $3 <= max_amount_range)
			ORDER BY COALESCE(min_held_days,0) DESC, penalty_value DESC
			LIMIT 1`,
			src.BankID, accruedDays, src.Principal,
		).Scan(&calc.PenaltyID, &calc.PenaltyType, &calc.PenaltyValue, &minHeldDays)
		calc.NoInterestFlag = minHeldDays > 0 && accruedDays < minHeldDays
		if calc.NoInterestFlag {
			calc.RevisedInterestAmount = 0
		}
		switch calc.PenaltyType {
		case "FLAT_AMOUNT":
			calc.PenaltyAmount = roundToFour(calc.PenaltyValue)
		case "RATE_REDUCTION":
			calc.ApplicableRate = roundToFour(src.InterestRate - calc.PenaltyValue)
			if calc.ApplicableRate < 0 {
				calc.ApplicableRate = 0
			}
			calc.RevisedInterestAmount = roundToFour(src.Principal * calc.ApplicableRate * float64(accruedDays) / 36500)
			calc.PenaltyAmount = roundToFour(accrued - calc.RevisedInterestAmount)
		default:
			calc.PenaltyAmount = roundToFour(calc.RevisedInterestAmount * calc.PenaltyValue / 100)
		}
		calc.RevisedMaturityValue = roundToFour(src.Principal + calc.RevisedInterestAmount - calc.PenaltyAmount)
		calc.NetPayout = roundToFour(src.Principal + calc.RevisedInterestAmount - calc.TDSAmount - calc.PenaltyAmount)
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
	if err := varianceengine.UpdateRecordFlags(ctx, pool, "cimplr.fd_closure_initiate", "closure_initiate_id", recordID, runID, items); err != nil {
		return nil, err
	}
	return cimplrVarianceSummary(runID, items), nil
}

func persistCimplrConfirmVariances(ctx context.Context, pool *pgxpool.Pool, recordID string, req cimplrClosureConfirmRequest, src cimplrFDSource, calc cimplrClosureCalc) (map[string]interface{}, error) {
	runID := varianceengine.NewRunID()
	ff := func(v float64) string { return strconv.FormatFloat(roundToFour(v), 'f', 4, 64) }
	rules := []varianceengine.Rule{
		{FieldName: "principal_received", VarianceType: varianceengine.TypeAmount, ExpectedValue: ff(src.Principal), ActualValue: ff(chooseFloat(req.PrincipalReceived, src.Principal)), Priority: varianceengine.PriorityHigh, Tolerance: 0.01},
		{FieldName: "interest_received", VarianceType: varianceengine.TypeAmount, ExpectedValue: ff(calc.AccruedInterest), ActualValue: ff(chooseFloat(req.InterestReceived, calc.AccruedInterest)), Priority: varianceengine.PriorityMedium, Tolerance: 1.0},
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
			varianceengine.Rule{FieldName: "new_fd_amount", VarianceType: varianceengine.TypeAmount, ExpectedValue: ff(calc.NetPayout), ActualValue: ff(req.NewFDAmount), Priority: varianceengine.PriorityHigh, Tolerance: 1.0},
			varianceengine.Rule{FieldName: "new_interest_rate", VarianceType: varianceengine.TypeRate, ExpectedValue: ff(src.InterestRate), ActualValue: ff(chooseFloat(req.NewInterestRate, src.InterestRate)), Priority: varianceengine.PriorityHigh, Tolerance: 0.001},
		)
	}
	items := varianceengine.Compare("FD_CLOSURE", recordID, src.EntityID, runID, rules)
	_ = varianceengine.AutoResolveCleared(ctx, pool, recordID, items, req.UserID, getUserEmail(ctx))
	if err := varianceengine.PersistVariances(ctx, pool, items); err != nil {
		return nil, err
	}
	if err := varianceengine.UpdateRecordFlags(ctx, pool, "cimplr.fd_closure_confirm", "closure_confirm_id", recordID, runID, items); err != nil {
		return nil, err
	}
	return cimplrVarianceSummary(runID, items), nil
}

func cimplrVarianceSummary(runID string, items []varianceengine.VarianceItem) map[string]interface{} {
	openCount := 0
	for _, it := range items {
		if it.HasVariance && it.Status == varianceengine.StatusOpen {
			openCount++
		}
	}
	return map[string]interface{}{"run_id": runID, "variance_count": countVariances(items), "open_count": openCount, "has_variance": openCount > 0}
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

func insertCimplrInitiateAudit(ctx context.Context, exec dbExec, id, action, status, reason, requestedBy string, old map[string]interface{}) error {
	_, err := exec.Exec(ctx, `
		INSERT INTO cimplr.fd_closure_initiate_audit (
			closure_initiate_id, action_type, processing_status, reason, requested_by,
			old_closure_type, old_closure_status, old_action_at_maturity, old_maturity_date,
			old_requested_closure_date, old_principal_amount, old_interest_type_code,
			old_interest_rate, old_expected_maturity_value, old_accrued_interest_till_date,
			old_tds_expected, old_net_expected_amount, old_auto_renewal_flag,
			old_maturity_status, old_action_required, old_rollover_type, old_rollover_bank_type,
			old_tentative_new_tenor_days, old_remarks, old_has_variance,
			old_has_unresolved_variance, old_variance_run_id, old_approval_instance_id,
			old_is_active, old_is_deleted
		) VALUES (
			$1,$2,$3,$4,$5,$6,$7,$8,$9::date,$10::date,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30
		)`,
		id, action, status, reason, requestedBy,
		oldValue(old, "closure_type"), oldValue(old, "closure_status"), oldValue(old, "action_at_maturity"), oldValue(old, "maturity_date"),
		oldValue(old, "requested_closure_date"), oldValue(old, "principal_amount"), oldValue(old, "interest_type_code"),
		oldValue(old, "interest_rate"), oldValue(old, "expected_maturity_value"), oldValue(old, "accrued_interest_till_date"),
		oldValue(old, "tds_expected"), oldValue(old, "net_expected_amount"), oldValue(old, "auto_renewal_flag"),
		oldValue(old, "maturity_status"), oldValue(old, "action_required"), oldValue(old, "rollover_type"), oldValue(old, "rollover_bank_type"),
		oldValue(old, "tentative_new_tenor_days"), oldValue(old, "remarks"), oldValue(old, "has_variance"),
		oldValue(old, "has_unresolved_variance"), oldValue(old, "variance_run_id"), oldValue(old, "approval_instance_id"),
		oldValue(old, "is_active"), oldValue(old, "is_deleted"),
	)
	return err
}

func insertCimplrConfirmAudit(ctx context.Context, exec dbExec, confirmID, initiateID, action, status, reason, requestedBy string, old map[string]interface{}) error {
	if initiateID == "" {
		_ = exec.QueryRow(ctx, `SELECT closure_initiate_id FROM cimplr.fd_closure_confirm WHERE closure_confirm_id=$1`, confirmID).Scan(&initiateID)
	}
	_, err := exec.Exec(ctx, `
		INSERT INTO cimplr.fd_closure_confirm_audit (
			closure_confirm_id, closure_initiate_id, action_type, processing_status, reason, requested_by,
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
			$1,NULLIF($2,''),$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::date,$13::date,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34
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
		table, idCol, auditTable, statusCol = "cimplr.fd_closure_confirm", "closure_confirm_id", "cimplr.fd_closure_confirm_audit", "closure_status"
	} else {
		table, idCol, auditTable, statusCol = "cimplr.fd_closure_initiate", "closure_initiate_id", "cimplr.fd_closure_initiate_audit", "closure_status"
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
	} else if stage == "confirm" {
		where = append(where, "t.closure_type <> 'PREMATURE'")
	}
	if approvedActive {
		if stage == "confirm" {
			where = append(where, "t.closure_status='CONFIRM'")
		} else {
			where = append(where, "t.closure_status='CONFIRM'")
		}
	}
	whereSQL := strings.Join(where, " AND ")
	var total int
	countSQL := fmt.Sprintf("SELECT COUNT(*) FROM %s t WHERE %s", table, whereSQL)
	if err := pool.QueryRow(ctx, countSQL, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	args = append(args, req.PageSize, offset)
	listSQL := fmt.Sprintf(`
		SELECT t.*,
		       COALESCE(a.processing_status,'') AS latest_processing_status,
		       COALESCE(a.action_type,'') AS latest_action_type,
		       COALESCE(a.requested_by,'') AS latest_requested_by,
		       a.requested_at AS latest_requested_at,
		       COALESCE(a.checker_by,'') AS latest_checker_by,
		       a.checker_at AS latest_checker_at
		FROM %s t
		LEFT JOIN LATERAL (
			SELECT * FROM %s a WHERE a.%s=t.%s ORDER BY requested_at DESC, audit_id DESC LIMIT 1
		) a ON true
		WHERE %s
		ORDER BY t.%s DESC
		LIMIT $%d OFFSET $%d`,
		table, auditTable, idCol, idCol, whereSQL, idCol, len(args)-1, len(args),
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
	ff := func(v float64) string { return strconv.FormatFloat(roundToFour(v), 'f', 4, 64) }
	rules := []varianceengine.Rule{
		{FieldName: "principal_received", VarianceType: varianceengine.TypeAmount, ExpectedValue: ff(src.Principal), ActualValue: ff(chooseFloat(req.PrincipalReceived, src.Principal)), Priority: varianceengine.PriorityHigh, Tolerance: 0.01},
		{FieldName: "interest_received", VarianceType: varianceengine.TypeAmount, ExpectedValue: ff(calc.AccruedInterest), ActualValue: ff(chooseFloat(req.InterestReceived, calc.AccruedInterest)), Priority: varianceengine.PriorityMedium, Tolerance: 1.0},
		{FieldName: "tds_deducted", VarianceType: varianceengine.TypeAmount, ExpectedValue: ff(calc.TDSAmount), ActualValue: ff(chooseFloat(req.TDSDeducted, calc.TDSAmount)), Priority: varianceengine.PriorityMedium, Tolerance: 0.5},
		{FieldName: "net_amount_received", VarianceType: varianceengine.TypeAmount, ExpectedValue: ff(calc.NetPayout), ActualValue: ff(chooseFloat(req.NetAmountReceived, calc.NetPayout)), Priority: varianceengine.PriorityHigh, Tolerance: 1.0},
	}
	items := varianceengine.Compare("FD_CLOSURE", recordID, src.EntityID, runID, rules)
	return cimplrVarianceSummary(runID, items)
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
		payoutCalc, err := calculateCimplrClosure(ctx, pool, src, "PAYOUT", "")
		if err != nil {
			continue
		}
		prematureCalc, _ := calculateCimplrClosure(ctx, pool, src, "PREMATURE", time.Now().Format(constants.DateFormat))
		rolloverCalc, _ := calculateCimplrClosure(ctx, pool, src, "ROLLOVER", "")

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
		row["premature_calculation"] = prematureCalc
		row["rollover_calculation"] = rolloverCalc
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
		return fetchCimplrSubRows(ctx, pool, `SELECT * FROM cimplr.fd_closure_confirm_audit WHERE closure_confirm_id=$1 ORDER BY requested_at DESC, audit_id DESC`, id)
	}
	return fetchCimplrSubRows(ctx, pool, `SELECT * FROM cimplr.fd_closure_initiate_audit WHERE closure_initiate_id=$1 ORDER BY requested_at DESC, audit_id DESC`, id)
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

func createCimplrApprovalInstance(ctx context.Context, pool *pgxpool.Pool, txType, action, recordID, recordTable, auditTable, auditIDColumn, entityID string, amount float64, userID, userEmail string) (string, error) {
	return approvalengine.CreateInstance(ctx, pool, approvalengine.InstanceRequest{
		ModuleCode:       cimplrClosureModule,
		EntityCode:       firstNonEmpty(entityID, "DEFAULT"),
		TransactionType:  txType,
		RecordID:         recordID,
		RecordTable:      recordTable,
		AuditTable:       auditTable,
		AuditIDColumn:    auditIDColumn,
		ActionType:       action,
		Amount:           amount,
		SubmittedBy:      userID,
		SubmittedByEmail: userEmail,
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
	t, err := time.Parse(constants.DateFormat, s)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
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
		f, _ := strconv.ParseFloat(t, 64)
		return f
	case fmt.Stringer:
		f, _ := strconv.ParseFloat(t.String(), 64)
		return f
	default:
		return 0
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
