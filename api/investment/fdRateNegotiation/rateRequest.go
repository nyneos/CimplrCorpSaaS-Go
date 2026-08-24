package fdRateNegotiation

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/api/policyengine/common"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type rateRequestPayload struct {
	UserID               string   `json:"user_id"`
	RateRequestID        string   `json:"rate_request_id,omitempty"`
	ProposedFDAmount     float64  `json:"proposed_fd_amount"`
	CurrencyCode         string   `json:"currency_code"`
	TenureType           string   `json:"tenure_type"`
	TenureValue          int      `json:"tenure_value"`
	ExpectedStartDate    string   `json:"expected_start_date"`
	ExpectedMaturityDate string   `json:"expected_maturity_date,omitempty"`
	InterestType         string   `json:"interest_type"`
	InterestPayoutMode   string   `json:"interest_payout_mode,omitempty"`
	TargetBankIDs        []string `json:"target_bank_ids"`
	TargetBankNames      []string `json:"target_bank_names"`
	InternalNotes        string   `json:"internal_notes,omitempty"`
	EntityID             string   `json:"entity_id,omitempty"`
	EntityName           string   `json:"entity_name,omitempty"`
	Action               string   `json:"action,omitempty"` // CREATE | EDIT (compat: DRAFT|SUBMIT map to create/edit)
	Reason               string   `json:"reason,omitempty"`
}

type rateRequestRow struct {
	RateRequestID        string
	RateRequestRef       string
	RequestDate          string
	RequestStatus        string
	ProposedFDAmount     float64
	CurrencyCode         string
	TenureType           string
	TenureValue          int
	ExpectedStartDate    string
	ExpectedMaturityDate *string
	InterestType         string
	InterestPayoutMode   *string
	TargetBankIDs        []string
	TargetBankNames      []string
	InternalNotes        *string
	CreatedBy            string
	CreatedAt            string
	UpdatedBy            *string
	UpdatedAt            *string
}

func normalizeTenureType(v string) string {
	v = strings.ToUpper(strings.TrimSpace(v))
	if v == "DAY" {
		return "DAYS"
	}
	if v == "MONTH" {
		return "MONTHS"
	}
	return v
}

func normalizeInterestType(v string) string {
	return strings.ToUpper(strings.TrimSpace(v))
}

func normalizePayoutMode(v string) string {
	v = strings.ToUpper(strings.TrimSpace(v))
	v = strings.ReplaceAll(v, " ", "_")
	return v
}

func computeMaturityDate(start string, tenureType string, tenureValue int) string {
	if start == "" || tenureValue <= 0 {
		return ""
	}
	t, err := time.Parse("2006-01-02", start)
	if err != nil {
		return ""
	}
	switch normalizeTenureType(tenureType) {
	case "DAYS":
		return t.AddDate(0, 0, tenureValue).Format("2006-01-02")
	case "MONTHS":
		return t.AddDate(0, tenureValue, 0).Format("2006-01-02")
	default:
		return ""
	}
}

func validateRateRequest(req rateRequestPayload, requireBanks bool) string {
	if req.ProposedFDAmount <= 0 {
		return "proposed_fd_amount must be greater than 0"
	}
	if strings.TrimSpace(req.CurrencyCode) == "" {
		return "currency_code is required"
	}
	tt := normalizeTenureType(req.TenureType)
	if tt != "DAYS" && tt != "MONTHS" {
		return "tenure_type must be DAYS or MONTHS"
	}
	if req.TenureValue <= 0 {
		return "tenure_value must be greater than 0"
	}
	if strings.TrimSpace(req.ExpectedStartDate) == "" {
		return "expected_start_date is required"
	}
	start, err := time.Parse("2006-01-02", req.ExpectedStartDate)
	if err != nil {
		return "expected_start_date must be YYYY-MM-DD"
	}
	today := time.Now().Truncate(24 * time.Hour)
	if start.Before(today) {
		return "expected_start_date must be a future date"
	}
	it := normalizeInterestType(req.InterestType)
	if it != "SIMPLE" && it != "COMPOUND" {
		return "interest_type must be SIMPLE or COMPOUND"
	}
	pm := normalizePayoutMode(req.InterestPayoutMode)
	if it == "SIMPLE" && pm == "" {
		return "interest_payout_mode is mandatory for Simple FD"
	}
	if pm != "" && pm != "MONTHLY" && pm != "QUARTERLY" && pm != "ON_MATURITY" && pm != "REINVEST" {
		return "interest_payout_mode must be MONTHLY, QUARTERLY, ON_MATURITY or REINVEST"
	}
	if requireBanks && len(req.TargetBankIDs) == 0 {
		return "at least one target bank is mandatory"
	}
	return ""
}

func nextRateRequestRef(ctx context.Context, tx pgx.Tx) (string, error) {
	day := time.Now().Format("20060102")
	prefix := fmt.Sprintf("RATE-REQ-%s-", day)
	var count int
	err := tx.QueryRow(ctx, `
		SELECT COUNT(*) FROM investment.fd_rate_negotiation
		WHERE rate_request_ref LIKE $1`, prefix+"%").Scan(&count)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s%04d", prefix, count+1), nil
}

func nullIfEmpty(s string) interface{} {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return s
}

func replaceTargetBanks(ctx context.Context, tx pgx.Tx, rateRequestID, userEmail string, ids, names []string) error {
	_, err := tx.Exec(ctx, `
		UPDATE investment.fd_rate_target_bank
		SET is_deleted = true
		WHERE rate_request_id = $1::uuid AND COALESCE(is_deleted,false) = false`, rateRequestID)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "does not exist") {
			return nil
		}
		return err
	}
	for i, bankID := range ids {
		bankID = strings.TrimSpace(bankID)
		if bankID == "" {
			continue
		}
		name := ""
		if i < len(names) {
			name = names[i]
		}
		if _, err = tx.Exec(ctx, `
			INSERT INTO investment.fd_rate_target_bank (
				rate_request_id, bank_id, bank_name, sequence, created_by
			) VALUES ($1::uuid, $2, $3, $4, $5)`,
			rateRequestID, bankID, name, i+1, userEmail); err != nil {
			return err
		}
	}
	return nil
}

func insertCreateAudit(ctx context.Context, tx pgx.Tx, rateRequestID, userEmail, clientIP string, req rateRequestPayload, status string) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO investment.fd_audit_rate_negotiation (
			rate_request_id, action_type, processing_status, requested_by, requested_at, requested_ip,
			new_proposed_fd_amount, new_currency_code, new_tenure_type, new_tenure_value,
			new_expected_start_date, new_expected_maturity_date, new_interest_type, new_interest_payout_mode,
			new_target_bank_ids, new_target_bank_names, new_internal_notes, new_request_status, new_is_active
		) VALUES (
			$1::uuid, 'CREATE', 'PENDING_APPROVAL', $2, now(), $3,
			$4, $5, $6, $7,
			NULLIF($8,'')::date, NULLIF($9,'')::date, $10, NULLIF($11,''),
			$12, $13, NULLIF($14,''), $15, true
		)`,
		rateRequestID, api.SystemIfBlank(userEmail), api.SystemIfBlank(clientIP),
		req.ProposedFDAmount, strings.ToUpper(strings.TrimSpace(req.CurrencyCode)),
		normalizeTenureType(req.TenureType), req.TenureValue,
		req.ExpectedStartDate, req.ExpectedMaturityDate,
		normalizeInterestType(req.InterestType), normalizePayoutMode(req.InterestPayoutMode),
		req.TargetBankIDs, req.TargetBankNames, req.InternalNotes, status,
	)
	return err
}

// CreateRateRequest creates a rate request in PENDING_APPROVAL (booking-style).
func CreateRateRequest(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req rateRequestPayload
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}

		action := strings.ToUpper(strings.TrimSpace(req.Action))
		if action == "" || action == "DRAFT" || action == "SUBMIT" || action == "CREATE" {
			// all create paths land in PENDING_APPROVAL
		} else {
			api.RespondWithError(w, http.StatusBadRequest, "invalid action for create")
			return
		}

		// SUBMIT must include banks; Save Draft (DRAFT) may omit until edit/submit
		requireBanks := action == "SUBMIT" || action == "CREATE"
		if msg := validateRateRequest(req, requireBanks); msg != "" {
			api.RespondWithError(w, http.StatusBadRequest, msg)
			return
		}

		userEmail := api.GetUserEmailFromCtx(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		createOK, createMatrixID := fdEnforceMatrix(r.Context(), w, r, pgxPool, enforceCtx{
			EventCode:   common.TriggerPreCreate,
			HandlerName: "CreateRateRequest",
			APIPath:     "/investment/fd/rate-negotiation/create",
			EntityCode:  strings.TrimSpace(req.EntityID),
			Actor:       userEmail,
		}, map[string]interface{}{
			"entity_id":              strings.TrimSpace(req.EntityID),
			"entity_code":            strings.TrimSpace(req.EntityID),
			"entity_name":            strings.TrimSpace(req.EntityName),
			"proposed_fd_amount":     req.ProposedFDAmount,
			"currency_code":          strings.ToUpper(strings.TrimSpace(req.CurrencyCode)),
			"tenure_type":            normalizeTenureType(req.TenureType),
			"tenure_value":           req.TenureValue,
			"expected_start_date":    req.ExpectedStartDate,
			"expected_maturity_date": req.ExpectedMaturityDate,
			"interest_type":          normalizeInterestType(req.InterestType),
			"interest_payout_mode":   normalizePayoutMode(req.InterestPayoutMode),
		})
		if !createOK {
			return
		}

		tenureType := normalizeTenureType(req.TenureType)
		interestType := normalizeInterestType(req.InterestType)
		payoutMode := normalizePayoutMode(req.InterestPayoutMode)
		maturity := strings.TrimSpace(req.ExpectedMaturityDate)
		if maturity == "" {
			maturity = computeMaturityDate(req.ExpectedStartDate, tenureType, req.TenureValue)
		}

		status := "PENDING_APPROVAL"

		bankIDs := req.TargetBankIDs
		if bankIDs == nil {
			bankIDs = []string{}
		}
		bankNames := req.TargetBankNames
		if bankNames == nil {
			bankNames = []string{}
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to start transaction")
			return
		}
		defer tx.Rollback(ctx)

		ref, err := nextRateRequestRef(ctx, tx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to generate rate request reference")
			return
		}

		var id string
		err = tx.QueryRow(ctx, `
			INSERT INTO investment.fd_rate_negotiation (
				rate_request_ref, request_date, request_status,
				proposed_fd_amount, currency_code, tenure_type, tenure_value,
				expected_start_date, expected_maturity_date, interest_type, interest_payout_mode,
				target_bank_ids, target_bank_names, internal_notes,
				entity_id, entity_name, processing_status, created_by, created_at
			) VALUES (
				$1, CURRENT_DATE, $2,
				$3, $4, $5, $6,
				$7::date, NULLIF($8,'')::date, $9, NULLIF($10,''),
				$11, $12, NULLIF($13,''),
				NULLIF($14,''), NULLIF($15,''), 'PENDING_APPROVAL', $16, now()
			) RETURNING rate_request_id::text`,
			ref, status,
			req.ProposedFDAmount, strings.ToUpper(strings.TrimSpace(req.CurrencyCode)),
			tenureType, req.TenureValue,
			req.ExpectedStartDate, maturity, interestType, payoutMode,
			bankIDs, bankNames, req.InternalNotes,
			strings.TrimSpace(req.EntityID), strings.TrimSpace(req.EntityName), userEmail,
		).Scan(&id)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, fmt.Sprintf("Create failed: %v", err))
			return
		}

		if err = insertCreateAudit(ctx, tx, id, userEmail, api.ClientIPFromContext(ctx), req, status); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Audit insert failed")
			return
		}

		if err = replaceTargetBanks(ctx, tx, id, userEmail, bankIDs, bankNames); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Target bank insert failed")
			return
		}

		if err = tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Commit failed")
			return
		}

		userID := strings.TrimSpace(req.UserID)
		if userID == "" {
			userID = api.GetUserIDFromCtx(r.Context())
		}
		fireRateNegotiationInstance(pgxPool, id, userID, userEmail, "CREATE", createMatrixID, req.ProposedFDAmount)
		fireRateNegotiationNotification(pgxPool, id, userEmail, "CREATE")

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"rate_request_id":  id,
			"rate_request_ref": ref,
			"request_status":   status,
		})
	}
}

// UpdateRateRequest edits an existing request → PENDING_EDIT_APPROVAL with old_* audit.
func UpdateRateRequest(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req rateRequestPayload
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if strings.TrimSpace(req.RateRequestID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "rate_request_id is required")
			return
		}

		if msg := validateRateRequest(req, true); msg != "" {
			api.RespondWithError(w, http.StatusBadRequest, msg)
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
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to start transaction")
			return
		}
		defer tx.Rollback(ctx)

		var (
			oldAmount                                              float64
			oldCurrency, oldTenureType, oldInterestType, oldStatus string
			oldTenureValue                                         int
			oldStart, oldMaturity, oldPayout, oldNotes             *string
			oldBankIDs, oldBankNames                               []string
			oldIsActive                                            bool
		)
		err = tx.QueryRow(ctx, `
			SELECT proposed_fd_amount, currency_code, tenure_type, tenure_value,
				TO_CHAR(expected_start_date,'YYYY-MM-DD'),
				TO_CHAR(expected_maturity_date,'YYYY-MM-DD'),
				interest_type, interest_payout_mode,
				target_bank_ids, target_bank_names, internal_notes, request_status,
				COALESCE(is_active, true)
			FROM investment.fd_rate_negotiation
			WHERE rate_request_id = $1::uuid AND COALESCE(is_deleted,false)=false
			FOR UPDATE`, req.RateRequestID).Scan(
			&oldAmount, &oldCurrency, &oldTenureType, &oldTenureValue,
			&oldStart, &oldMaturity, &oldInterestType, &oldPayout,
			&oldBankIDs, &oldBankNames, &oldNotes, &oldStatus, &oldIsActive,
		)
		if err != nil {
			api.RespondWithError(w, http.StatusNotFound, "Rate request not found")
			return
		}
		if oldStatus == "DELETED" || oldStatus == "CONVERTED_TO_FD" {
			api.RespondWithError(w, http.StatusBadRequest, "Rate request cannot be edited in current status")
			return
		}

		editOK, editMatrixID := fdEnforceMatrix(ctx, w, r, pgxPool, enforceCtx{
			EventCode:   common.TriggerPreEdit,
			HandlerName: "UpdateRateRequest",
			APIPath:     "/investment/fd/rate-negotiation/update",
			EntityCode:  strings.TrimSpace(req.EntityID),
			Actor:       userEmail,
		}, map[string]interface{}{
			"rate_request_id":        req.RateRequestID,
			"entity_id":              strings.TrimSpace(req.EntityID),
			"entity_code":            strings.TrimSpace(req.EntityID),
			"entity_name":            strings.TrimSpace(req.EntityName),
			"proposed_fd_amount":     req.ProposedFDAmount,
			"currency_code":          strings.ToUpper(strings.TrimSpace(req.CurrencyCode)),
			"tenure_type":            normalizeTenureType(req.TenureType),
			"tenure_value":           req.TenureValue,
			"expected_start_date":    req.ExpectedStartDate,
			"expected_maturity_date": req.ExpectedMaturityDate,
			"interest_type":          normalizeInterestType(req.InterestType),
			"interest_payout_mode":   normalizePayoutMode(req.InterestPayoutMode),
			"request_status":         oldStatus,
		})
		if !editOK {
			return
		}

		tenureType := normalizeTenureType(req.TenureType)
		interestType := normalizeInterestType(req.InterestType)
		payoutMode := normalizePayoutMode(req.InterestPayoutMode)
		maturity := strings.TrimSpace(req.ExpectedMaturityDate)
		if maturity == "" {
			maturity = computeMaturityDate(req.ExpectedStartDate, tenureType, req.TenureValue)
		}
		newStatus := "PENDING_EDIT_APPROVAL"
		actionType := "EDIT"

		bankIDs := req.TargetBankIDs
		if bankIDs == nil {
			bankIDs = []string{}
		}
		bankNames := req.TargetBankNames
		if bankNames == nil {
			bankNames = []string{}
		}

		_, err = tx.Exec(ctx, `
			UPDATE investment.fd_rate_negotiation SET
				proposed_fd_amount = $2,
				currency_code = $3,
				tenure_type = $4,
				tenure_value = $5,
				expected_start_date = $6::date,
				expected_maturity_date = NULLIF($7,'')::date,
				interest_type = $8,
				interest_payout_mode = NULLIF($9,''),
				target_bank_ids = $10,
				target_bank_names = $11,
				internal_notes = NULLIF($12,''),
				entity_id = COALESCE(NULLIF($13,''), entity_id),
				entity_name = COALESCE(NULLIF($14,''), entity_name),
				request_status = $15,
				processing_status = $15,
				updated_by = $16,
				updated_at = now()
			WHERE rate_request_id = $1::uuid`,
			req.RateRequestID,
			req.ProposedFDAmount, strings.ToUpper(strings.TrimSpace(req.CurrencyCode)),
			tenureType, req.TenureValue,
			req.ExpectedStartDate, maturity, interestType, payoutMode,
			bankIDs, bankNames, req.InternalNotes,
			strings.TrimSpace(req.EntityID), strings.TrimSpace(req.EntityName),
			newStatus, userEmail,
		)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, fmt.Sprintf("Update failed: %v", err))
			return
		}

		var oldStartStr, oldMaturityStr, oldPayoutStr, oldNotesStr interface{}
		if oldStart != nil {
			oldStartStr = *oldStart
		}
		if oldMaturity != nil {
			oldMaturityStr = *oldMaturity
		}
		if oldPayout != nil {
			oldPayoutStr = *oldPayout
		}
		if oldNotes != nil {
			oldNotesStr = *oldNotes
		}

		_, err = tx.Exec(ctx, `
			INSERT INTO investment.fd_audit_rate_negotiation (
				rate_request_id, action_type, processing_status, reason,
				requested_by, requested_at, requested_ip,
				old_proposed_fd_amount, old_currency_code, old_tenure_type, old_tenure_value,
				old_expected_start_date, old_expected_maturity_date, old_interest_type,
				old_interest_payout_mode, old_target_bank_ids, old_target_bank_names,
				old_internal_notes, old_request_status, old_is_active,
				new_proposed_fd_amount, new_currency_code, new_tenure_type, new_tenure_value,
				new_expected_start_date, new_expected_maturity_date, new_interest_type,
				new_interest_payout_mode, new_target_bank_ids, new_target_bank_names,
				new_internal_notes, new_request_status
			) VALUES (
				$1::uuid, $2, $3, $4,
				$5, now(), $6,
				$7, $8, $9, $10,
				NULLIF($11,'')::date, NULLIF($12,'')::date, $13,
				$14, $15, $16,
				$17, $18, $19,
				$20, $21, $22, $23,
				NULLIF($24,'')::date, NULLIF($25,'')::date, $26,
				$27, $28, $29,
				$30, $31
			)`,
			req.RateRequestID, actionType, newStatus, nullIfEmpty(req.Reason),
			api.SystemIfBlank(userEmail), api.SystemIfBlank(api.ClientIPFromContext(ctx)),
			oldAmount, oldCurrency, oldTenureType, oldTenureValue,
			oldStartStr, oldMaturityStr, oldInterestType,
			oldPayoutStr, oldBankIDs, oldBankNames,
			oldNotesStr, oldStatus, oldIsActive,
			req.ProposedFDAmount, strings.ToUpper(strings.TrimSpace(req.CurrencyCode)),
			tenureType, req.TenureValue,
			req.ExpectedStartDate, maturity, interestType,
			nullIfEmpty(payoutMode), bankIDs, bankNames,
			nullIfEmpty(req.InternalNotes), newStatus,
		)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Audit insert failed")
			return
		}

		if err = replaceTargetBanks(ctx, tx, req.RateRequestID, userEmail, bankIDs, bankNames); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Target bank update failed")
			return
		}

		if err = tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Commit failed")
			return
		}

		userID := strings.TrimSpace(req.UserID)
		if userID == "" {
			userID = api.GetUserIDFromCtx(r.Context())
		}
		fireRateNegotiationInstance(pgxPool, req.RateRequestID, userID, userEmail, "EDIT", editMatrixID, req.ProposedFDAmount)
		fireRateNegotiationNotification(pgxPool, req.RateRequestID, userEmail, "EDIT")

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"rate_request_id": req.RateRequestID,
			"request_status":  newStatus,
		})
	}
}

func scanRateRequest(row pgx.Row) (map[string]interface{}, error) {
	var (
		id, ref, reqDate, status, currency, tenureType, interestType, entityID, entityName, createdBy, createdAt string
		amount                                                                                                   float64
		tenureValue                                                                                              int
		startDate                                                                                                string
		maturity, payout, notes, updatedBy, updatedAt                                                            *string
		bankIDs, bankNames                                                                                       []string
		processingStatus, actionType                                                                             *string
		selectedOfferID, selectedBankID, selectedBankName, selectionRemarks                                      *string
		approvalDecision, approvalRemarks, approvalDate, approvedBy, bookingID                                   *string
	)
	err := row.Scan(
		&id, &ref, &reqDate, &status, &amount, &currency, &tenureType, &tenureValue,
		&startDate, &maturity, &interestType, &payout, &bankIDs, &bankNames, &notes,
		&entityID, &entityName,
		&createdBy, &createdAt, &updatedBy, &updatedAt, &processingStatus, &actionType,
		&selectedOfferID, &selectedBankID, &selectedBankName, &selectionRemarks,
		&approvalDecision, &approvalRemarks, &approvalDate, &approvedBy, &bookingID,
	)
	if err != nil {
		return nil, err
	}
	if bankIDs == nil {
		bankIDs = []string{}
	}
	if bankNames == nil {
		bankNames = []string{}
	}
	out := map[string]interface{}{
		"rate_request_id":        id,
		"rate_request_ref":       ref,
		"request_date":           reqDate,
		"request_status":         status,
		"proposed_fd_amount":     amount,
		"currency_code":          currency,
		"tenure_type":            tenureType,
		"tenure_value":           tenureValue,
		"expected_start_date":    startDate,
		"expected_maturity_date": "",
		"interest_type":          interestType,
		"interest_payout_mode":   "",
		"target_bank_ids":        bankIDs,
		"target_bank_names":      bankNames,
		"internal_notes":         "",
		"entity_id":              entityID,
		"entity_name":            entityName,
		"created_by":             createdBy,
		"created_at":             createdAt,
		"updated_by":             "",
		"updated_at":             "",
		"processing_status":      status,
		"action_type":            "",
		"selected_offer_id":      "",
		"selected_bank_id":       "",
		"selected_bank_name":     "",
		"selection_remarks":      "",
		"approval_decision":      "",
		"approval_remarks":       "",
		"approval_date":          "",
		"approved_by":            "",
		"booking_id":             "",
	}
	if processingStatus != nil && *processingStatus != "" {
		out["processing_status"] = *processingStatus
	}
	if actionType != nil {
		out["action_type"] = *actionType
	}
	if maturity != nil {
		out["expected_maturity_date"] = *maturity
	}
	if payout != nil {
		out["interest_payout_mode"] = *payout
	}
	if notes != nil {
		out["internal_notes"] = *notes
	}
	if updatedBy != nil {
		out["updated_by"] = *updatedBy
	}
	if updatedAt != nil {
		out["updated_at"] = *updatedAt
	}
	if selectedOfferID != nil {
		out["selected_offer_id"] = *selectedOfferID
	}
	if selectedBankID != nil {
		out["selected_bank_id"] = *selectedBankID
	}
	if selectedBankName != nil {
		out["selected_bank_name"] = *selectedBankName
	}
	if selectionRemarks != nil {
		out["selection_remarks"] = *selectionRemarks
	}
	if approvalDecision != nil {
		out["approval_decision"] = *approvalDecision
	}
	if approvalRemarks != nil {
		out["approval_remarks"] = *approvalRemarks
	}
	if approvalDate != nil {
		out["approval_date"] = *approvalDate
	}
	if approvedBy != nil {
		out["approved_by"] = *approvedBy
	}
	if bookingID != nil {
		out["booking_id"] = *bookingID
	}
	return out, nil
}

const rateRequestSelect = `
	SELECT
		m.rate_request_id::text,
		m.rate_request_ref,
		TO_CHAR(m.request_date,'YYYY-MM-DD'),
		m.request_status,
		m.proposed_fd_amount,
		m.currency_code,
		m.tenure_type,
		m.tenure_value,
		TO_CHAR(m.expected_start_date,'YYYY-MM-DD'),
		TO_CHAR(m.expected_maturity_date,'YYYY-MM-DD'),
		m.interest_type,
		m.interest_payout_mode,
		m.target_bank_ids,
		m.target_bank_names,
		m.internal_notes,
		COALESCE(m.entity_id,''),
		COALESCE(m.entity_name,''),
		m.created_by,
		TO_CHAR(m.created_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
		m.updated_by,
		TO_CHAR(m.updated_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
		la.processing_status,
		la.action_type,
		m.selected_offer_id::text,
		m.selected_bank_id,
		m.selected_bank_name,
		m.selection_remarks,
		m.approval_decision,
		m.approval_remarks,
		TO_CHAR(m.approval_date,'YYYY-MM-DD'),
		m.approved_by,
		m.booking_id
	FROM investment.fd_rate_negotiation m
	LEFT JOIN LATERAL (
		SELECT a.processing_status, a.action_type, a.requested_at
		FROM investment.fd_audit_rate_negotiation a
		WHERE a.rate_request_id = m.rate_request_id
		ORDER BY a.requested_at DESC, a.audit_id DESC
		LIMIT 1
	) la ON true
`

// ListRateRequests returns all non-deleted rate requests.
func ListRateRequests(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		rows, err := pgxPool.Query(ctx, rateRequestSelect+`
			WHERE COALESCE(m.is_deleted,false)=false
			ORDER BY GREATEST(m.created_at, COALESCE(la.requested_at, m.created_at)) DESC, m.rate_request_id DESC`)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to list rate requests")
			return
		}
		defer rows.Close()

		results := make([]map[string]interface{}, 0)
		for rows.Next() {
			item, err := scanRateRequest(rows)
			if err != nil {
				continue
			}
			results = append(results, item)
		}
		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"results": results,
		})
	}
}

const comparableRateRequestFilter = `
			WHERE COALESCE(m.is_deleted,false)=false
			  AND EXISTS (
				SELECT 1
				FROM investment.fd_rate_offer o
				WHERE o.rate_request_id = m.rate_request_id
				  AND COALESCE(o.is_deleted,false)=false
				  AND UPPER(COALESCE(o.offer_status,'')) = 'APPROVED'
			  )
			  AND UPPER(COALESCE(m.request_status,'')) NOT IN (
				'PENDING_APPROVAL',
				'PENDING_EDIT_APPROVAL',
				'PENDING_DELETE_APPROVAL',
				'REJECTED',
				'CANCELLED',
				'DELETED'
			  )
			ORDER BY GREATEST(m.created_at, COALESCE(la.requested_at, m.created_at)) DESC, m.rate_request_id DESC`

// ListComparableRateRequests is the Rate Comparison feed: only requests that
// already have a checker-approved captured offer. Create/edit/delete pendings
// stay on the Rate Request page — including PENDING_EDIT_APPROVAL after an
// offer was approved. Remaining statuses (OFFERS_RECEIVED, PENDING_RATE_APPROVAL,
// APPROVED, CONVERTED_TO_FD, …) are returned as stored.
func ListComparableRateRequests(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		rows, err := pgxPool.Query(ctx, rateRequestSelect+comparableRateRequestFilter)
		if err != nil {
			api.LogErrorForResponse(w, "Failed to list comparable rate requests: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "Failed to list comparable rate requests", "")
			return
		}
		defer rows.Close()

		results := make([]map[string]interface{}, 0)
		for rows.Next() {
			item, err := scanRateRequest(rows)
			if err != nil {
				continue
			}
			results = append(results, item)
		}
		api.RespondEnvelopeSuccess(w, "Comparable rate requests fetched successfully", map[string]interface{}{
			"rows": results,
		})
	}
}

// GetRateRequestDetail returns one rate request by id.
func GetRateRequestDetail(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			RateRequestID string `json:"rate_request_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.RateRequestID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "rate_request_id is required")
			return
		}
		ctx := r.Context()
		row := pgxPool.QueryRow(ctx, rateRequestSelect+`
			WHERE m.rate_request_id = $1::uuid AND COALESCE(m.is_deleted,false)=false`, req.RateRequestID)
		item, err := scanRateRequest(row)
		if err != nil {
			api.RespondWithError(w, http.StatusNotFound, "Rate request not found")
			return
		}
		api.RespondWithPayload(w, true, "", item)
	}
}

// GetRateRequestAudit returns audit history including old_* values.
func GetRateRequestAudit(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			RateRequestID string `json:"rate_request_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.RateRequestID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "rate_request_id is required")
			return
		}
		ctx := r.Context()
		rows, err := pgxPool.Query(ctx, `
			SELECT
				audit_id,
				rate_request_id::text,
				action_type,
				processing_status,
				COALESCE(reason,'') AS reason,
				COALESCE(requested_by,'') AS requested_by,
				TO_CHAR(requested_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS requested_at,
				COALESCE(requested_ip,'') AS requested_ip,
				COALESCE(checker_by,'') AS checker_by,
				COALESCE(TO_CHAR(checker_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"'),'') AS checker_at,
				COALESCE(checker_comment,'') AS checker_comment,
				old_proposed_fd_amount,
				COALESCE(old_currency_code,'') AS old_currency_code,
				COALESCE(old_tenure_type,'') AS old_tenure_type,
				old_tenure_value,
				COALESCE(TO_CHAR(old_expected_start_date,'YYYY-MM-DD'),'') AS old_expected_start_date,
				COALESCE(TO_CHAR(old_expected_maturity_date,'YYYY-MM-DD'),'') AS old_expected_maturity_date,
				COALESCE(old_interest_type,'') AS old_interest_type,
				COALESCE(old_interest_payout_mode,'') AS old_interest_payout_mode,
				old_target_bank_ids,
				old_target_bank_names,
				COALESCE(old_internal_notes,'') AS old_internal_notes,
				COALESCE(old_request_status,'') AS old_request_status,
				old_is_active,
				new_proposed_fd_amount,
				COALESCE(new_currency_code,'') AS new_currency_code,
				COALESCE(new_tenure_type,'') AS new_tenure_type,
				new_tenure_value,
				COALESCE(TO_CHAR(new_expected_start_date,'YYYY-MM-DD'),'') AS new_expected_start_date,
				COALESCE(TO_CHAR(new_expected_maturity_date,'YYYY-MM-DD'),'') AS new_expected_maturity_date,
				COALESCE(new_interest_type,'') AS new_interest_type,
				COALESCE(new_interest_payout_mode,'') AS new_interest_payout_mode,
				new_target_bank_ids,
				new_target_bank_names,
				COALESCE(new_internal_notes,'') AS new_internal_notes,
				COALESCE(new_request_status,'') AS new_request_status,
				COALESCE(checker_ip,'') AS checker_ip
			FROM investment.fd_audit_rate_negotiation
			WHERE rate_request_id = $1::uuid
			ORDER BY requested_at DESC, audit_id DESC`, req.RateRequestID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to fetch audit history")
			return
		}
		defer rows.Close()

		fds := rows.FieldDescriptions()
		results := make([]map[string]interface{}, 0)
		for rows.Next() {
			vals, err := rows.Values()
			if err != nil {
				continue
			}
			m := make(map[string]interface{}, len(fds))
			for i, fd := range fds {
				if vals[i] == nil {
					m[string(fd.Name)] = nil
				} else {
					m[string(fd.Name)] = vals[i]
				}
			}
			results = append(results, m)
		}
		api.RespondWithPayload(w, true, "", results)
	}
}
