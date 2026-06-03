package fdBooking

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/approvalengine"
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/api/investment/uploadutil"
	notifcatalog "CimplrCorpSaas/api/notification/catalog"
	s3storage "CimplrCorpSaas/api/utils/s3storage"
	"CimplrCorpSaas/api/varianceengine"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type fdConfirmationCaptureRequest struct {
	UserID                   string           `json:"user_id"`
	BookingID                string           `json:"booking_id"`
	ConfirmedPrincipalAmount float64          `json:"confirmed_principal_amount"`
	ValueType                string           `json:"value_type"`
	ConfirmedInterestRate    float64          `json:"confirmed_interest_rate"`
	ConfirmedTenorDays       int              `json:"confirmed_tenor_days"`
	ConfirmedTenorMonths     int              `json:"confirmed_tenor_months"`
	ConfirmedTenorYears      int              `json:"confirmed_tenor_years"`
	ConfirmedTenorType       string           `json:"confirmed_tenor_type"`
	ConfirmedValueDate       string           `json:"confirmed_value_date"`
	ConfirmedMaturityDate    string           `json:"confirmed_maturity_date"`
	BankFDReference          string           `json:"bank_fd_reference"`
	BankReferenceNumber      string           `json:"bank_reference_number"`
	ReceiptDate              string           `json:"receipt_date"`
	ConfirmedInterestType    string           `json:"confirmed_interest_type"`
	ConfirmedFrequencyID     string           `json:"confirmed_frequency_id"`
	PayoutFrequencyID        string           `json:"payout_frequency_id"`
	PenaltyID                string           `json:"penalty_id"`
	PayoutDates              *json.RawMessage `json:"payout_dates"`
	CompoundingDates         *json.RawMessage `json:"compounding_dates"`
	FirstPayoutDate          string           `json:"first_payout_date"`
	FirstCapitalizationDate  string           `json:"first_capitalization_date"`
	AccrualFrequencyCode     string           `json:"accrual_frequency_code"`
	ResetType                string           `json:"reset_type"`
	PrematureClosureTerms    string           `json:"premature_closure_terms"`
	Notes                    string           `json:"notes"`
}

func fdFormFloat(r *http.Request, key string) float64 {
	raw := strings.TrimSpace(r.FormValue(key))
	if raw == "" {
		return 0
	}
	value, _ := strconv.ParseFloat(raw, 64)
	return value
}

func fdFormInt(r *http.Request, key string) int {
	raw := strings.TrimSpace(r.FormValue(key))
	if raw == "" {
		return 0
	}
	value, _ := strconv.Atoi(raw)
	return value
}

func fdFormRawJSON(r *http.Request, key string) *json.RawMessage {
	raw := strings.TrimSpace(r.FormValue(key))
	if raw == "" {
		return nil
	}
	msg := json.RawMessage(raw)
	return &msg
}

func fdConfirmationCaptureRequestFromForm(r *http.Request) fdConfirmationCaptureRequest {
	return fdConfirmationCaptureRequest{
		UserID:                   strings.TrimSpace(r.FormValue("user_id")),
		BookingID:                strings.TrimSpace(r.FormValue("booking_id")),
		ConfirmedPrincipalAmount: fdFormFloat(r, "confirmed_principal_amount"),
		ValueType:                strings.TrimSpace(r.FormValue("value_type")),
		ConfirmedInterestRate:    fdFormFloat(r, "confirmed_interest_rate"),
		ConfirmedTenorDays:       fdFormInt(r, "confirmed_tenor_days"),
		ConfirmedTenorMonths:     fdFormInt(r, "confirmed_tenor_months"),
		ConfirmedTenorYears:      fdFormInt(r, "confirmed_tenor_years"),
		ConfirmedTenorType:       strings.TrimSpace(r.FormValue("confirmed_tenor_type")),
		ConfirmedValueDate:       strings.TrimSpace(r.FormValue("confirmed_value_date")),
		ConfirmedMaturityDate:    strings.TrimSpace(r.FormValue("confirmed_maturity_date")),
		BankFDReference:          strings.TrimSpace(r.FormValue("bank_fd_reference")),
		BankReferenceNumber:      strings.TrimSpace(r.FormValue("bank_reference_number")),
		ReceiptDate:              strings.TrimSpace(r.FormValue("receipt_date")),
		ConfirmedInterestType:    strings.TrimSpace(r.FormValue("confirmed_interest_type")),
		ConfirmedFrequencyID:     strings.TrimSpace(r.FormValue("confirmed_frequency_id")),
		PayoutFrequencyID:        strings.TrimSpace(r.FormValue("payout_frequency_id")),
		PenaltyID:                strings.TrimSpace(r.FormValue("penalty_id")),
		PayoutDates:              fdFormRawJSON(r, "payout_dates"),
		CompoundingDates:         fdFormRawJSON(r, "compounding_dates"),
		FirstPayoutDate:          strings.TrimSpace(r.FormValue("first_payout_date")),
		FirstCapitalizationDate:  strings.TrimSpace(r.FormValue("first_capitalization_date")),
		AccrualFrequencyCode:     strings.TrimSpace(r.FormValue("accrual_frequency_code")),
		ResetType:                strings.TrimSpace(r.FormValue("reset_type")),
		PrematureClosureTerms:    strings.TrimSpace(r.FormValue("premature_closure_terms")),
		Notes:                    strings.TrimSpace(r.FormValue("notes")),
	}
}

// ─── CaptureConfirmation ──────────────────────────────────────────────────────
// POST /investment/fd/confirmation/capture
//
// Flow:
//  1. Load booked values from fd_booking_request.
//  2. Run variance engine (rate, principal, tenor_days, value_date, maturity_date).
//  3. If variance found → return variance items in response, NO DB ingestion.
//  4. If no variance → insert fd_confirmation (CONFIRMED) + audit + update booking CONFIRMED.
//
// To persist a confirmation that has variance, call /confirmation/variance-resolve first.
func CaptureConfirmation(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req fdConfirmationCaptureRequest
		isMultipart := strings.Contains(strings.ToLower(r.Header.Get("Content-Type")), "multipart/form-data")
		if isMultipart {
			if err := r.ParseMultipartForm(32 << 20); err != nil {
				api.RespondWithError(w, http.StatusBadRequest, "Invalid multipart form: "+err.Error())
				return
			}
			req = fdConfirmationCaptureRequestFromForm(r)
		} else {
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
				return
			}
		}
		if req.BookingID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "booking_id is required")
			return
		}
		if req.ConfirmedPrincipalAmount <= 0 || req.ConfirmedInterestRate <= 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrConfirmedPrincipalAndInterest)
			return
		}
		if req.ConfirmedValueDate == "" || req.ConfirmedMaturityDate == "" {
			api.RespondWithError(w, http.StatusBadRequest, "confirmed_value_date and confirmed_maturity_date are required")
			return
		}

		userEmail := api.GetUserEmailFromCtx(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		uploadS3Key := ""
		if isMultipart && r.MultipartForm != nil {
			if err := uploadutil.EnsureUniqueMultipartUpload(ctx, pgxPool, s3storage.CollectMultipartFiles(r.MultipartForm, "attachment", "bank_statement", "file", "files"), `
				SELECT upload_s3_key
				FROM investment.fd_confirmation
				WHERE COALESCE(is_deleted, false) = false
				  AND COALESCE(upload_s3_key, '') <> ''
			`); err != nil {
				if errors.Is(err, uploadutil.ErrFileAlreadyUploaded) {
					api.RespondWithError(w, http.StatusConflict, "This file was already uploaded earlier. Please upload a different file.")
					return
				}
				api.RespondWithError(w, http.StatusInternalServerError, "failed to check duplicate upload: "+err.Error())
				return
			}
			var uploadErr error
			uploadS3Key, uploadErr = s3storage.UploadFirstMultipartFile(ctx, "fd-bank-confirmation-capture", userEmail, s3storage.CollectMultipartFiles(r.MultipartForm, "attachment", "bank_statement", "file", "files"))
			if uploadErr != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "S3 upload failed: "+uploadErr.Error())
				return
			}
		}
		cleanupUpload := func() {
			if uploadS3Key != "" {
				_ = s3storage.DeleteFromS3(ctx, uploadS3Key)
			}
		}

		// ── Load booked values ────────────────────────────────────────────────
		var bookedPrincipal, bookedRate float64
		var bookedTenorDays, bookedTenorMonths, bookedTenorYears int
		var bookedValueDate, bookedMaturityDate, bookedInterestTypeCode, bookedFrequencyID, bookedTenorType, entityID, bookingStatus string
		var bookedAccrualFreqCode, bookedResetType, bookedPayoutFreqID, bookedValueType string
		err := pgxPool.QueryRow(ctx, `
			SELECT
				COALESCE(principal_amount,0),
				COALESCE(interest_rate,0),
				COALESCE(tenure_days,0),
				COALESCE(tenure_months,0),
				COALESCE(tenure_years,0),
				COALESCE(TO_CHAR(value_date,'YYYY-MM-DD'),''),
				COALESCE(TO_CHAR(expected_maturity_date,'YYYY-MM-DD'),''),
				COALESCE(interest_type_code,''),
				COALESCE(frequency_id,''),
				COALESCE(tenor_type,''),
				COALESCE(entity_id,''),
				COALESCE(booking_status,''),
				COALESCE(accrual_frequency_code,''),
				COALESCE(reset_type,''),
				COALESCE(NULLIF(payout_frequency_id,''), frequency_id, ''),
				COALESCE(NULLIF(value_type,''),'Actual')
			FROM investment.fd_booking_request
			WHERE booking_id = $1 AND COALESCE(is_deleted,false) = false`,
			req.BookingID,
		).Scan(&bookedPrincipal, &bookedRate, &bookedTenorDays, &bookedTenorMonths, &bookedTenorYears,
			&bookedValueDate, &bookedMaturityDate, &bookedInterestTypeCode, &bookedFrequencyID, &bookedTenorType,
			&entityID, &bookingStatus,
			&bookedAccrualFreqCode, &bookedResetType, &bookedPayoutFreqID, &bookedValueType)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				api.RespondWithError(w, http.StatusBadRequest,
					"Booking not found or was deleted — cannot capture confirmation without a valid booking_id")
				return
			}
			api.RespondWithError(w, http.StatusInternalServerError, "Fetch booking failed: "+err.Error())
			return
		}

		// Inherit booking values when the confirmation request omits these fields.
		if req.AccrualFrequencyCode == "" {
			req.AccrualFrequencyCode = bookedAccrualFreqCode
		}
		if req.ResetType == "" {
			req.ResetType = bookedResetType
		}
		if req.ConfirmedFrequencyID == "" {
			req.ConfirmedFrequencyID = bookedPayoutFreqID
		}
		if req.PayoutFrequencyID == "" {
			req.PayoutFrequencyID = bookedPayoutFreqID
		}
		if strings.TrimSpace(req.ValueType) == "" {
			req.ValueType = bookedValueType
		}
		req.ValueType = normalizePrincipalValueType(req.ValueType)

		bankFDRef := strings.TrimSpace(req.BankFDReference)
		if bankFDRef == "" {
			bankFDRef = req.BookingID
		}
		var existingConfirmationID string
		_ = pgxPool.QueryRow(ctx, `
			SELECT confirmation_id FROM investment.fd_confirmation
			WHERE booking_id = $1 AND COALESCE(is_deleted,false) = false
			LIMIT 1`, req.BookingID).Scan(&existingConfirmationID)
		if msg, status := validateFDConfirmationReferenceUniqueness(ctx, pgxPool, fdConfirmationReferenceCheck{
			EntityID:              entityID,
			BankFDRefNo:           bankFDRef,
			BankReferenceNumber:   req.BankReferenceNumber,
			ExcludeConfirmationID: existingConfirmationID,
		}); msg != "" {
			cleanupUpload()
			api.RespondWithError(w, status, msg)
			return
		}

		// ── Run variance engine ───────────────────────────────────────────────
		runID := varianceengine.NewRunID()
		bookedPayout, actualPayout := normalizedPayoutFrequency(ctx, pgxPool,
			bookedPayoutFreqID, bookedFrequencyID, req.PayoutFrequencyID, req.ConfirmedFrequencyID,
		)
		bookedAccrual, actualAccrual := normalizedAccrualFrequency(ctx, pgxPool,
			accrualFrequencyInput{BookedCode: bookedAccrualFreqCode, ActualCode: req.AccrualFrequencyCode, BookedPayout: bookedPayout, ActualPayout: actualPayout, BookedCompound: bookedFrequencyID, ActualCompound: req.ConfirmedFrequencyID},
		)
		varIn := prepareFDConfirmationVarianceInput(ctx, pgxPool, fdConfirmationVarianceInput{
			BookedPrincipal: bookedPrincipal, ActualPrincipal: req.ConfirmedPrincipalAmount,
			BookedRate: bookedRate, ActualRate: req.ConfirmedInterestRate,
			BookedTenorDays: bookedTenorDays, ActualTenorDays: req.ConfirmedTenorDays,
			BookedTenorMonths: bookedTenorMonths, ActualTenorMonths: req.ConfirmedTenorMonths,
			BookedTenorYears: bookedTenorYears, ActualTenorYears: req.ConfirmedTenorYears,
			BookedValueDate: bookedValueDate, ActualValueDate: req.ConfirmedValueDate,
			BookedMaturityDate: bookedMaturityDate, ActualMaturityDate: req.ConfirmedMaturityDate,
			BookedInterestType: bookedInterestTypeCode, ActualInterestType: req.ConfirmedInterestType,
			BookedFrequencyID: bookedFrequencyID, ActualFrequencyID: req.ConfirmedFrequencyID,
			BookedPayoutFrequencyID: bookedPayout, ActualPayoutFrequencyID: actualPayout,
			BookedResetType: bookedResetType, ActualResetType: req.ResetType,
			BookedAccrualFrequencyCode: bookedAccrual, ActualAccrualFrequencyCode: actualAccrual,
			BookedTenorType: bookedTenorType, ActualTenorType: strings.ToUpper(strings.TrimSpace(req.ConfirmedTenorType)),
		})
		varItems := varianceengine.Compare("FD_CONFIRMATION", req.BookingID, entityID, runID, buildFDConfirmationVarianceRules(varIn))

		hasVariance := false
		for _, v := range varItems {
			if v.HasVariance {
				hasVariance = true
				break
			}
		}

		// ── Variance found: return items, no DB ingestion ─────────────────────
		if hasVariance {
			cleanupUpload()
			userEmail := api.GetUserEmailFromCtx(ctx)
			_ = varianceengine.PersistVariances(ctx, pgxPool, varItems)
			_ = varianceengine.AutoResolveCleared(ctx, pgxPool, req.BookingID, varItems, req.UserID, userEmail)
			out := make([]map[string]interface{}, 0, len(varItems))
			for _, v := range varItems {
				out = append(out, serializeVarianceItem(ctx, pgxPool, v))
			}
			// Use 422 so axios throws (enabling the catch block on the frontend)
			// while avoiding misleading HTTP 500 / [ERROR] log noise for a normal
			// business-logic condition.
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnprocessableEntity)
			api.LogInfo("[FDBooking] CaptureConfirmation: variance detected booking=%s run=%s", req.BookingID, runID)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"success": false,
				"error":   "Variance detected — confirmation not saved. Resolve and Recapture.",
				"rows": map[string]interface{}{
					"booking_id":     req.BookingID,
					"has_variance":   true,
					"run_id":         runID,
					"variance_items": out,
				},
			})
			return
		}

		// ── No variance: insert confirmation and confirm directly ─────────────
		receivedDate := req.ReceiptDate
		if receivedDate == "" {
			receivedDate = time.Now().Format(constants.DateFormat)
		}
		tenorType := strings.ToUpper(strings.TrimSpace(req.ConfirmedTenorType))
		if tenorType == "" {
			tenorType = "DAYS"
		}

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			cleanupUpload()
			msg, status := getUserFriendlyFDError(err, constants.ErrTransactionFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx) //nolint:errcheck

		// Upsert: if a prior VARIANCE_PENDING confirmation exists for this booking, update it;
		// otherwise insert fresh. This lets capture succeed after a variance was resolved externally.
		var confirmationID string
		err = tx.QueryRow(ctx, `
			SELECT confirmation_id FROM investment.fd_confirmation
			WHERE booking_id = $1 AND COALESCE(is_deleted,false) = false
			LIMIT 1`, req.BookingID).Scan(&confirmationID)
		if err == nil && confirmationID != "" {
			// Update the existing row
			if _, err = tx.Exec(ctx, `
				UPDATE investment.fd_confirmation SET
					actual_principal          = $1,
					confirmed_rate            = $2,
					actual_start_date         = $3,
					actual_maturity_date      = $4,
					bank_fd_ref_no            = $5,
					confirmation_received_date = $6,
					confirmed_interest_type_code = NULLIF($7,''),
					confirmed_frequency_id    = NULLIF($8,''),
					tenor_type                = $9,
					tenor_days                = NULLIF($10,0),
					tenor_months              = NULLIF($11,0),
					tenor_years               = NULLIF($12,0),
					payout_dates              = $13,
					compounding_dates         = $14,
					variance_flag             = false,
					variance_threshold_breached = false,
					variance_details          = NULL,
					variance_action           = NULL,
					variance_resolved_by      = $16,
					variance_resolved_at      = now(),
					confirmation_status       = 'PENDING_APPROVAL',
					penalty_id                = NULLIF($15,''),
					first_payout_date         = $18,
					first_capitalization_date = $19,
					accrual_frequency_code    = NULLIF($21,''),
					reset_type                = NULLIF($22,''),
					bank_reference_number     = NULLIF($23,''),
					premature_closure_terms   = NULLIF($24,''),
					payout_frequency_id       = NULLIF($25,''),
					value_type                = $26,
					upload_s3_key             = COALESCE(NULLIF($20,''), upload_s3_key),
					updated_by                = $16,
					updated_at                = now()
				WHERE confirmation_id = $17`,
				req.ConfirmedPrincipalAmount, req.ConfirmedInterestRate,
				coerceDateValue(req.ConfirmedValueDate), coerceDateValue(req.ConfirmedMaturityDate),
				bankFDRef,
				coerceDateValue(receivedDate),
				req.ConfirmedInterestType, req.ConfirmedFrequencyID,
				tenorType,
				req.ConfirmedTenorDays, req.ConfirmedTenorMonths, req.ConfirmedTenorYears,
				payoutJSON(req.PayoutDates), payoutJSON(req.CompoundingDates),
				req.PenaltyID,
				userEmail, confirmationID,
				coerceDateValue(req.FirstPayoutDate), coerceDateValue(req.FirstCapitalizationDate),
				uploadS3Key,
				strings.ToUpper(strings.TrimSpace(req.AccrualFrequencyCode)),
				strings.ToUpper(strings.TrimSpace(req.ResetType)),
				nullIfEmpty(req.BankReferenceNumber),
				nullIfEmpty(req.PrematureClosureTerms),
				nullIfEmpty(req.PayoutFrequencyID),
				req.ValueType,
			); err != nil {
				cleanupUpload()
				msg, status := getUserFriendlyFDError(err, constants.ErrUpdateConfirmationFailed)
				api.RespondWithError(w, status, msg)
				return
			}
		} else {
			// Fresh insert
			err = tx.QueryRow(ctx, `
				INSERT INTO investment.fd_confirmation (
					booking_id,
					actual_principal, confirmed_rate,
					actual_start_date, actual_maturity_date,
					bank_fd_ref_no,
					confirmation_received_date,
					confirmation_mode,
					confirmed_interest_type_code, confirmed_frequency_id,
					tenor_type, tenor_days, tenor_months, tenor_years,
					payout_dates, compounding_dates,
					variance_flag, variance_threshold_breached,
					confirmation_status,
					penalty_id,
					first_payout_date,
					first_capitalization_date,
					accrual_frequency_code,
					reset_type,
					bank_reference_number,
					premature_closure_terms,
					payout_frequency_id,
					value_type,
					upload_s3_key,
					created_by
				) VALUES (
					$1,
					$2,$3,
					$4,$5,
					$6,
					$7,
					'MANUAL',
					NULLIF($8,''), NULLIF($9,''),
					$10, NULLIF($11,0), NULLIF($12,0), NULLIF($13,0),
					$14, $15,
					false, false,
					'PENDING_APPROVAL',
					NULLIF($16,''),
					$18,
					$19,
					NULLIF($21,''),
					NULLIF($22,''),
					NULLIF($23,''),
					NULLIF($24,''),
					NULLIF($25,''),
					$26,
					NULLIF($20,''),
					$17
				) RETURNING confirmation_id`,
				req.BookingID,
				req.ConfirmedPrincipalAmount, req.ConfirmedInterestRate,
				coerceDateValue(req.ConfirmedValueDate), coerceDateValue(req.ConfirmedMaturityDate),
				bankFDRef,
				coerceDateValue(receivedDate),
				req.ConfirmedInterestType, req.ConfirmedFrequencyID,
				tenorType,
				req.ConfirmedTenorDays, req.ConfirmedTenorMonths, req.ConfirmedTenorYears,
				payoutJSON(req.PayoutDates), payoutJSON(req.CompoundingDates),
				req.PenaltyID,
				userEmail,
				coerceDateValue(req.FirstPayoutDate), coerceDateValue(req.FirstCapitalizationDate),
				uploadS3Key,
				strings.ToUpper(strings.TrimSpace(req.AccrualFrequencyCode)),
				strings.ToUpper(strings.TrimSpace(req.ResetType)),
				nullIfEmpty(req.BankReferenceNumber),
				nullIfEmpty(req.PrematureClosureTerms),
				nullIfEmpty(req.PayoutFrequencyID),
				req.ValueType,
			).Scan(&confirmationID)
			if err != nil {
				cleanupUpload()
				msg, status := getUserFriendlyFDError(err, "Insert confirmation failed")
				api.RespondWithError(w, status, msg)
				return
			}
		}

		// Audit
		if _, err = tx.Exec(ctx, `
			INSERT INTO investment.fd_audit_confirmation (
				confirmation_id, action_type, processing_status,
				requested_by, requested_at,
				old_actual_principal, old_confirmed_rate,
				old_actual_start_date, old_actual_maturity_date,
				old_confirmation_status,
				old_tenor_type, old_tenor_days, old_tenor_months, old_tenor_years
			) VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now(),$3,$4,$5,$6,$7,$8,NULLIF($9,0),NULLIF($10,0),NULLIF($11,0))`,
			confirmationID, userEmail,
			bookedPrincipal, bookedRate,
			coerceDateValue(bookedValueDate), coerceDateValue(bookedMaturityDate), bookingStatus,
			tenorType, req.ConfirmedTenorDays, req.ConfirmedTenorMonths, req.ConfirmedTenorYears,
		); err != nil {
			cleanupUpload()
			msg, status := getUserFriendlyFDError(err, constants.ErrAuditInsertFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		if err = tx.Commit(ctx); err != nil {
			cleanupUpload()
			msg, status := getUserFriendlyFDError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}

		go func(cID, uID, uEmail, eID string, amount float64) {
			defer func() {
				if rec := recover(); rec != nil {
					api.LogError("[FDBooking] CaptureConfirmation engine panic for %s: %v", cID, rec)
				}
			}()
			bgCtx := context.Background()
			instID, err := approvalengine.CreateInstance(bgCtx, pgxPool, approvalengine.InstanceRequest{
				ModuleCode:       "FIXED_DEPOSIT",
				EntityCode:       eID,
				TransactionType:  "FD_CONFIRMATION_CREATE",
				RecordID:         cID,
				RecordTable:      constants.QuerryConfirmation,
				AuditTable:       constants.QuerryAuditConfirmation,
				AuditIDColumn:    "confirmation_id",
				ActionType:       "CREATE",
				Amount:           amount,
				SubmittedBy:      uID,
				SubmittedByEmail: uEmail,
			})
			if err != nil {
				api.LogError("[FDBooking] CaptureConfirmation CreateInstance failed for %s: %v", cID, err)
				return
			}
			if instID != "" {
				if _, uerr := pgxPool.Exec(bgCtx,
					`UPDATE investment.fd_confirmation SET confirmation_status = 'APPROVAL_PENDING' WHERE confirmation_id = $1`,
					cID); uerr != nil {
					api.LogError("[FDBooking] CaptureConfirmation status→APPROVAL_PENDING failed for %s: %v", cID, uerr)
				}
			}
		}(confirmationID, req.UserID, userEmail, entityID, req.ConfirmedPrincipalAmount)

		go func(cID, bID, eID, uEmail string, amount float64) {
			defer func() {
				if rec := recover(); rec != nil {
					api.LogError("[FDBooking] CaptureConfirmation notification panic for %s: %v", cID, rec)
				}
			}()
			notifcatalog.TriggerNotification(context.Background(), pgxPool,
				"/investment/fd/confirmation/capture", cID, map[string]interface{}{
					"entity_id":   eID,
					"record_id":   cID,
					"booking_id":  bID,
					"event":       "FD_CONFIRMATION_CAPTURED",
					"actor_email": uEmail,
					"amount":      amount,
				})
		}(confirmationID, req.BookingID, entityID, userEmail, req.ConfirmedPrincipalAmount)

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"confirmation_id":     confirmationID,
			"booking_id":          req.BookingID,
			"confirmation_status": constants.StatusPendingApproval,
			"has_variance":        false,
			"captured_by":         userEmail,
		})
		api.LogInfo("[FDBooking] CaptureConfirmation: id=%s booking=%s no-variance", confirmationID, req.BookingID)
	}
}

// ─── VarianceResolve ─────────────────────────────────────────────────────────
// POST /investment/fd/confirmation/variance-resolve  (alias: /resolve-variance)
//
// "Resolve later" — persist bank/actual values now even when they differ from booking.
// Creates or updates fd_confirmation; sets VARIANCE_PENDING when variance remains,
// PENDING_APPROVAL when values match booking. Does NOT accept/reject the business decision.
// Use /variance-exception to accept variance, or /reject to reject the line.
func GetFDConfirmationDownloadURL(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID         string `json:"user_id"`
			ConfirmationID string `json:"confirmation_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, "Invalid JSON: "+err.Error())
			return
		}
		confirmationID := strings.TrimSpace(req.ConfirmationID)
		if confirmationID == "" {
			api.RespondWithResult(w, false, "confirmation_id required")
			return
		}
		// Guard against deployments where upload_s3_key column was never added
		// — without this the SELECT below would return SQLSTATE 42703 to the user.
		if !fdConfirmationHasUploadS3Key(r.Context(), pgxPool) {
			api.RespondWithResult(w, false, "no file available")
			return
		}
		var uploadS3Key sql.NullString
		if err := pgxPool.QueryRow(r.Context(), `
			SELECT upload_s3_key
			FROM investment.fd_confirmation
			WHERE confirmation_id = $1 AND COALESCE(is_deleted,false) = false
		`, confirmationID).Scan(&uploadS3Key); err != nil {
			api.RespondWithResult(w, false, constants.ErrFileNotFound)
			return
		}
		key := strings.TrimSpace(uploadS3Key.String)
		if !uploadS3Key.Valid || key == "" {
			api.RespondWithResult(w, false, "no file available")
			return
		}
		downloadURL, err := s3storage.GetDownloadPresignedURL(r.Context(), key, 15*time.Minute)
		if err != nil {
			api.RespondWithResult(w, false, "Failed to generate download url: "+err.Error())
			return
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			constants.ValueSuccess: true,
			"data": map[string]interface{}{
				"confirmation_id": confirmationID,
				"download_url":    downloadURL,
			},
		})
	}
}

func GetFDConfirmationBulkDownloadURL(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID          string   `json:"user_id"`
			ConfirmationIDs []string `json:"confirmation_ids"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, "Invalid JSON: "+err.Error())
			return
		}
		if len(req.ConfirmationIDs) == 0 {
			api.RespondWithResult(w, false, constants.ConfirmationIDsRequired)
			return
		}
		files := make([]map[string]string, 0, len(req.ConfirmationIDs))
		failedIDs := make([]string, 0)
		// Skip the entire SELECT loop when the column doesn't exist in the live DB.
		hasUploadKey := fdConfirmationHasUploadS3Key(r.Context(), pgxPool)
		for _, rawID := range req.ConfirmationIDs {
			confirmationID := strings.TrimSpace(rawID)
			if confirmationID == "" {
				continue
			}
			if !hasUploadKey {
				failedIDs = append(failedIDs, confirmationID)
				continue
			}
			var uploadS3Key sql.NullString
			if err := pgxPool.QueryRow(r.Context(), `
				SELECT upload_s3_key
				FROM investment.fd_confirmation
				WHERE confirmation_id = $1 AND COALESCE(is_deleted,false) = false
			`, confirmationID).Scan(&uploadS3Key); err != nil {
				failedIDs = append(failedIDs, confirmationID)
				continue
			}
			key := strings.TrimSpace(uploadS3Key.String)
			if !uploadS3Key.Valid || key == "" {
				failedIDs = append(failedIDs, confirmationID)
				continue
			}
			downloadURL, err := s3storage.GetDownloadPresignedURL(r.Context(), key, 15*time.Minute)
			if err != nil {
				failedIDs = append(failedIDs, confirmationID)
				continue
			}
			files = append(files, map[string]string{
				"confirmation_id": confirmationID,
				"download_url":    downloadURL,
			})
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			constants.ValueSuccess: len(files) > 0,
			"data": map[string]interface{}{
				"files":      files,
				"failed_ids": failedIDs,
			},
		})
	}
}

func VarianceResolve(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID                   string           `json:"user_id"`
			BookingID                string           `json:"booking_id"`      // required on first call; optional on re-runs
			ConfirmationID           string           `json:"confirmation_id"` // for re-runs
			ConfirmedPrincipalAmount float64          `json:"confirmed_principal_amount"`
			ConfirmedInterestRate    float64          `json:"confirmed_interest_rate"`
			ConfirmedTenorDays       int              `json:"confirmed_tenor_days"`
			ConfirmedTenorMonths     int              `json:"confirmed_tenor_months"`
			ConfirmedTenorYears      int              `json:"confirmed_tenor_years"`
			ConfirmedTenorType       string           `json:"confirmed_tenor_type"`
			ConfirmedValueDate       string           `json:"confirmed_value_date"`
			ConfirmedMaturityDate    string           `json:"confirmed_maturity_date"`
			BankFDReference          string           `json:"bank_fd_reference"`
			BankReferenceNumber      string           `json:"bank_reference_number"`
			ReceiptDate              string           `json:"receipt_date"`
			ConfirmedInterestType    string           `json:"confirmed_interest_type"`
			ConfirmedFrequencyID     string           `json:"confirmed_frequency_id"`
			PayoutFrequencyID        string           `json:"payout_frequency_id"`
			PenaltyID                string           `json:"penalty_id"`
			PayoutDates              *json.RawMessage `json:"payout_dates"`
			CompoundingDates         *json.RawMessage `json:"compounding_dates"`
			FirstPayoutDate          string           `json:"first_payout_date"`         // YYYY-MM-DD; actual first interest credit date
			FirstCapitalizationDate  string           `json:"first_capitalization_date"` // YYYY-MM-DD; actual first capitalization date
			AccrualFrequencyCode     string           `json:"accrual_frequency_code"`
			ResetType                string           `json:"reset_type"`
			Notes                    string           `json:"notes"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.BookingID == "" && req.ConfirmationID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "booking_id or confirmation_id is required")
			return
		}
		if req.ConfirmedPrincipalAmount <= 0 || req.ConfirmedInterestRate <= 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrConfirmedPrincipalAndInterest)
			return
		}
		if req.ConfirmedValueDate == "" || req.ConfirmedMaturityDate == "" {
			api.RespondWithError(w, http.StatusBadRequest, "confirmed_value_date and confirmed_maturity_date are required")
			return
		}

		userEmail := api.GetUserEmailFromCtx(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()

		// ── Resolve booking_id / confirmation_id ──────────────────────────────
		bookingID := req.BookingID
		confirmationID := req.ConfirmationID

		if confirmationID != "" && bookingID == "" {
			_ = pgxPool.QueryRow(ctx, `SELECT booking_id FROM investment.fd_confirmation WHERE confirmation_id=$1`, confirmationID).Scan(&bookingID)
		}
		if bookingID == "" {
			api.RespondWithError(w, http.StatusBadRequest,
				"Cannot resolve booking_id — send booking_id or a confirmation_id linked to a booking")
			return
		}

		// ── APPROVED guard — once approved the FD is live and immutable ────────
		if confirmationID != "" {
			var currentConfStatus string
			_ = pgxPool.QueryRow(ctx,
				`SELECT confirmation_status FROM investment.fd_confirmation WHERE confirmation_id=$1`,
				confirmationID).Scan(&currentConfStatus)
			if currentConfStatus == constants.StatusApproved {
				api.RespondWithError(w, http.StatusBadRequest,
					"Cannot resolve variance on an APPROVED confirmation — the FD is live and immutable")
				return
			}
		}

		// ── Load booked values ────────────────────────────────────────────────
		var bookedPrincipal, bookedRate float64
		var bookedTenorDays, bookedTenorMonths, bookedTenorYears int
		var bookedValueDate, bookedMaturityDate, bookedInterestType, bookedFrequencyID, bookedTenorType, entityID, bookingStatus string
		var bookedAccrualFreqCode, bookedResetType, bookedPayoutFreqID string
		err := pgxPool.QueryRow(ctx, `
			SELECT
				COALESCE(principal_amount,0), COALESCE(interest_rate,0),
				COALESCE(tenure_days,0), COALESCE(tenure_months,0), COALESCE(tenure_years,0),
				COALESCE(TO_CHAR(value_date,'YYYY-MM-DD'),''),
				COALESCE(TO_CHAR(expected_maturity_date,'YYYY-MM-DD'),''),
				COALESCE(interest_type_code,''), COALESCE(frequency_id,''), COALESCE(tenor_type,''),
				COALESCE(entity_id,''), COALESCE(booking_status,''),
				COALESCE(accrual_frequency_code,''),
				COALESCE(reset_type,''),
				COALESCE(NULLIF(payout_frequency_id,''), frequency_id, '')
			FROM investment.fd_booking_request
			WHERE booking_id = $1 AND COALESCE(is_deleted,false) = false`, bookingID,
		).Scan(&bookedPrincipal, &bookedRate, &bookedTenorDays, &bookedTenorMonths, &bookedTenorYears,
			&bookedValueDate, &bookedMaturityDate, &bookedInterestType, &bookedFrequencyID, &bookedTenorType,
			&entityID, &bookingStatus,
			&bookedAccrualFreqCode, &bookedResetType, &bookedPayoutFreqID)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				api.RespondWithError(w, http.StatusBadRequest,
					fmt.Sprintf("Booking %s not found or was deleted — resolve-variance needs an active booking row", bookingID))
				return
			}
			api.RespondWithError(w, http.StatusInternalServerError, "Fetch booking failed: "+err.Error())
			return
		}

		tenorType := strings.ToUpper(strings.TrimSpace(req.ConfirmedTenorType))
		if tenorType == "" {
			tenorType = bookedTenorType
		}
		// Inherit booking values when the confirmation request omits these fields.
		if req.AccrualFrequencyCode == "" {
			req.AccrualFrequencyCode = bookedAccrualFreqCode
		}
		if req.ResetType == "" {
			req.ResetType = bookedResetType
		}
		if req.ConfirmedFrequencyID == "" {
			req.ConfirmedFrequencyID = bookedPayoutFreqID
		}
		if req.PayoutFrequencyID == "" {
			req.PayoutFrequencyID = bookedPayoutFreqID
		}

		bankFDRef := strings.TrimSpace(req.BankFDReference)
		if bankFDRef == "" {
			bankFDRef = bookingID
		}
		if confirmationID == "" {
			_ = pgxPool.QueryRow(ctx, `
				SELECT confirmation_id FROM investment.fd_confirmation
				WHERE booking_id = $1 AND COALESCE(is_deleted,false) = false
				LIMIT 1`, bookingID).Scan(&confirmationID)
		}
		if msg, status := validateFDConfirmationReferenceUniqueness(ctx, pgxPool, fdConfirmationReferenceCheck{
			EntityID:              entityID,
			BankFDRefNo:           bankFDRef,
			BankReferenceNumber:   req.BankReferenceNumber,
			ExcludeConfirmationID: confirmationID,
		}); msg != "" {
			api.RespondWithError(w, status, msg)
			return
		}

		runID := varianceengine.NewRunID()
		bookedPayout, actualPayout := normalizedPayoutFrequency(ctx, pgxPool,
			bookedPayoutFreqID, bookedFrequencyID, req.PayoutFrequencyID, req.ConfirmedFrequencyID,
		)
		bookedAccrual, actualAccrual := normalizedAccrualFrequency(ctx, pgxPool,
			accrualFrequencyInput{BookedCode: bookedAccrualFreqCode, ActualCode: req.AccrualFrequencyCode, BookedPayout: bookedPayout, ActualPayout: actualPayout, BookedCompound: bookedFrequencyID, ActualCompound: req.ConfirmedFrequencyID},
		)
		varItems, hasVariance := runFDConfirmationVarianceCompare(ctx, pgxPool, fdConfirmationVarianceInput{
			BookingID: bookingID, EntityID: entityID, RunID: runID,
			ResolvedBy: req.UserID, ResolvedByEmail: userEmail,
			BookedPrincipal: bookedPrincipal, ActualPrincipal: req.ConfirmedPrincipalAmount,
			BookedRate: bookedRate, ActualRate: req.ConfirmedInterestRate,
			BookedTenorDays: bookedTenorDays, ActualTenorDays: req.ConfirmedTenorDays,
			BookedTenorMonths: bookedTenorMonths, ActualTenorMonths: req.ConfirmedTenorMonths,
			BookedTenorYears: bookedTenorYears, ActualTenorYears: req.ConfirmedTenorYears,
			BookedValueDate: bookedValueDate, ActualValueDate: req.ConfirmedValueDate,
			BookedMaturityDate: bookedMaturityDate, ActualMaturityDate: req.ConfirmedMaturityDate,
			BookedInterestType: bookedInterestType, ActualInterestType: req.ConfirmedInterestType,
			BookedFrequencyID: bookedFrequencyID, ActualFrequencyID: req.ConfirmedFrequencyID,
			BookedPayoutFrequencyID: bookedPayout, ActualPayoutFrequencyID: actualPayout,
			BookedResetType: bookedResetType, ActualResetType: req.ResetType,
			BookedAccrualFrequencyCode: bookedAccrual, ActualAccrualFrequencyCode: actualAccrual,
			BookedTenorType: bookedTenorType, ActualTenorType: tenorType,
			ActualFirstCapDate:    coerceDateString(req.FirstCapitalizationDate),
			ActualFirstPayoutDate: coerceDateString(req.FirstPayoutDate),
		})

		receivedDate := req.ReceiptDate
		if receivedDate == "" {
			receivedDate = time.Now().Format(constants.DateFormat)
		}

		confStatus := "VARIANCE_PENDING"
		if !hasVariance {
			confStatus = constants.StatusPendingApproval
		}

		// Build variance_details JSON from items
		varDetailsJSON := buildVarianceDetailsJSON(varItems)

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			msg, status := getUserFriendlyFDError(err, constants.ErrTransactionFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx) //nolint:errcheck

		isUpdate := false
		if confirmationID == "" {
			_ = tx.QueryRow(ctx, `
				SELECT confirmation_id FROM investment.fd_confirmation
				WHERE booking_id = $1 AND COALESCE(is_deleted,false) = false
				LIMIT 1`, bookingID).Scan(&confirmationID)
		}

		if confirmationID != "" {
			// ── Update existing confirmation ──────────────────────────────────
			isUpdate = true
			// Snapshot old values for audit
			var oldPrincipal, oldRate float64
			var oldStatus, oldTenorType, oldPenaltyID string
			var oldTenorDays int
			_ = tx.QueryRow(ctx, `
				SELECT COALESCE(actual_principal,0), COALESCE(confirmed_rate,0),
				       COALESCE(confirmation_status,''), COALESCE(tenor_type,''), COALESCE(tenor_days,0), COALESCE(penalty_id,'')
				FROM investment.fd_confirmation WHERE confirmation_id=$1`, confirmationID).
				Scan(&oldPrincipal, &oldRate, &oldStatus, &oldTenorType, &oldTenorDays, &oldPenaltyID)

			if _, err = tx.Exec(ctx, `
				UPDATE investment.fd_confirmation SET
					actual_principal             = $1,
					confirmed_rate               = $2,
					actual_start_date            = $3,
					actual_maturity_date         = $4,
					bank_fd_ref_no               = $5,
					confirmation_received_date   = $6,
					confirmed_interest_type_code = NULLIF($7,''),
					confirmed_frequency_id       = NULLIF($8,''),
					tenor_type                   = $9,
					tenor_days                   = NULLIF($10,0),
					tenor_months                 = NULLIF($11,0),
					tenor_years                  = NULLIF($12,0),
					payout_dates                 = $13,
					compounding_dates            = $14,
					variance_flag                = $15,
					variance_threshold_breached  = false,
					variance_details             = $16,
					variance_action              = NULL,
					confirmation_status          = $17,
					penalty_id                   = NULLIF($18,''),
					first_payout_date            = $21,
					first_capitalization_date    = $22,
					accrual_frequency_code       = NULLIF($23,''),
					reset_type                   = NULLIF($24,''),
					bank_reference_number        = NULLIF($25,''),
					payout_frequency_id          = NULLIF($26,''),
					updated_by                   = $19,
					updated_at                   = now()
				WHERE confirmation_id = $20`,
				req.ConfirmedPrincipalAmount, req.ConfirmedInterestRate,
				coerceDateValue(req.ConfirmedValueDate), coerceDateValue(req.ConfirmedMaturityDate),
				bankFDRef, coerceDateValue(receivedDate),
				req.ConfirmedInterestType, req.ConfirmedFrequencyID,
				tenorType,
				req.ConfirmedTenorDays, req.ConfirmedTenorMonths, req.ConfirmedTenorYears,
				payoutJSON(req.PayoutDates), payoutJSON(req.CompoundingDates),
				hasVariance, varDetailsJSON, confStatus, req.PenaltyID, userEmail, confirmationID,
				coerceDateValue(req.FirstPayoutDate), coerceDateValue(req.FirstCapitalizationDate),
				strings.ToUpper(strings.TrimSpace(req.AccrualFrequencyCode)),
				strings.ToUpper(strings.TrimSpace(req.ResetType)),
				strings.TrimSpace(req.BankReferenceNumber),
				strings.TrimSpace(req.PayoutFrequencyID),
			); err != nil {
				api.LogError("[VarianceResolve] UPDATE fd_confirmation error: %v", err)
				msg, status := getUserFriendlyFDError(err, constants.ErrUpdateConfirmationFailed)
				api.RespondWithError(w, status, msg)
				return
			}

			// Audit the update
			if _, err = tx.Exec(ctx, `
				INSERT INTO investment.fd_audit_confirmation (
					confirmation_id, action_type, processing_status,
					requested_by, requested_at, reason,
					old_actual_principal, old_confirmed_rate,
					old_confirmation_status,
					old_tenor_type, old_tenor_days, old_penalty_id
				) VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL',$2,now(),$3,$4,$5,$6,$7,$8,$9)`,
				confirmationID, userEmail, req.Notes,
				oldPrincipal, oldRate, oldStatus, oldTenorType, oldTenorDays, oldPenaltyID,
			); err != nil {
				msg, status := getUserFriendlyFDError(err, constants.ErrAuditInsertFailed)
				api.RespondWithError(w, status, msg)
				return
			}

		} else {
			// ── Fresh insert with VARIANCE_PENDING ────────────────────────────
			err = tx.QueryRow(ctx, `
				INSERT INTO investment.fd_confirmation (
					booking_id,
					actual_principal, confirmed_rate,
					actual_start_date, actual_maturity_date,
					bank_fd_ref_no,
					confirmation_received_date,
					confirmation_mode,
					confirmed_interest_type_code, confirmed_frequency_id,
					tenor_type, tenor_days, tenor_months, tenor_years,
					payout_dates, compounding_dates,
					variance_flag, variance_threshold_breached,
					variance_details, variance_action,
					confirmation_status,
					penalty_id,
					first_payout_date,
					first_capitalization_date,
					accrual_frequency_code,
					reset_type,
					bank_reference_number,
					payout_frequency_id,
					created_by
				) VALUES (
					$1,
					$2,$3,
					$4,$5,
					$6,$7,
					'MANUAL',
					NULLIF($8,''), NULLIF($9,''),
					$10, NULLIF($11,0), NULLIF($12,0), NULLIF($13,0),
					$14, $15,
					$16, false,
					$17, NULL,
					$18,
					NULLIF($19,''),
					$21,
					$22,
					NULLIF($23,''),
					NULLIF($24,''),
					NULLIF($25,''),
					NULLIF($26,''),
					$20
				) RETURNING confirmation_id`,
				bookingID,
				req.ConfirmedPrincipalAmount, req.ConfirmedInterestRate,
				coerceDateValue(req.ConfirmedValueDate), coerceDateValue(req.ConfirmedMaturityDate),
				bankFDRef, coerceDateValue(receivedDate),
				req.ConfirmedInterestType, req.ConfirmedFrequencyID,
				tenorType,
				req.ConfirmedTenorDays, req.ConfirmedTenorMonths, req.ConfirmedTenorYears,
				payoutJSON(req.PayoutDates), payoutJSON(req.CompoundingDates),
				hasVariance, varDetailsJSON, confStatus, req.PenaltyID, userEmail,
				coerceDateValue(req.FirstPayoutDate), coerceDateValue(req.FirstCapitalizationDate),
				strings.ToUpper(strings.TrimSpace(req.AccrualFrequencyCode)),
				strings.ToUpper(strings.TrimSpace(req.ResetType)),
				strings.TrimSpace(req.BankReferenceNumber),
				strings.TrimSpace(req.PayoutFrequencyID),
			).Scan(&confirmationID)
			if err != nil {
				msg, status := getUserFriendlyFDError(err, "Insert confirmation failed")
				api.RespondWithError(w, status, msg)
				return
			}

			// Audit insert
			if _, err = tx.Exec(ctx, `
				INSERT INTO investment.fd_audit_confirmation (
					confirmation_id, action_type, processing_status,
					requested_by, requested_at, reason,
					old_actual_principal, old_confirmed_rate,
					old_actual_start_date, old_actual_maturity_date,
					old_confirmation_status,
					old_tenor_type, old_tenor_days
				) VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now(),$3,$4,$5,$6,$7,$8,$9,$10)`,
				confirmationID, userEmail, req.Notes,
				bookedPrincipal, bookedRate,
				coerceDateValue(bookedValueDate), coerceDateValue(bookedMaturityDate), bookingStatus,
				tenorType, req.ConfirmedTenorDays,
			); err != nil {
				msg, status := getUserFriendlyFDError(err, constants.ErrAuditInsertFailed)
				api.RespondWithError(w, status, msg)
				return
			}
		}

		if err = tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyFDError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}

		// Fire approval engine
		go func(cID, uID, uEmail, eID string, amount float64, update bool) {
			defer func() { recover() }() //nolint:errcheck
			bgCtx := context.Background()
			_ = approvalengine.CancelPendingInstances(bgCtx, pgxPool, "FIXED_DEPOSIT", cID, uEmail)
			txType := "FD_CONFIRMATION_CREATE"
			actionType := "CREATE"
			if update {
				txType = "FD_CONFIRMATION_VARIANCE_RESOLVE"
				actionType = "EDIT"
			}
			_, _ = approvalengine.CreateInstance(bgCtx, pgxPool, approvalengine.InstanceRequest{
				ModuleCode: "FIXED_DEPOSIT", EntityCode: eID,
				TransactionType: txType, RecordID: cID,
				RecordTable: constants.QuerryConfirmation, AuditTable: constants.QuerryAuditConfirmation,
				AuditIDColumn: "confirmation_id", ActionType: actionType,
				Amount: amount, SubmittedBy: uID, SubmittedByEmail: uEmail,
			})
		}(confirmationID, req.UserID, userEmail, entityID, req.ConfirmedPrincipalAmount, isUpdate)

		go func(cID, bID, eID, uEmail string, hasVar bool) {
			defer func() {
				if rec := recover(); rec != nil {
					api.LogError("[FDBooking] VarianceResolve notification panic for %s: %v", cID, rec)
				}
			}()
			event := "FD_CONFIRMATION_VARIANCE_RESOLVED"
			if hasVar {
				event = "FD_CONFIRMATION_VARIANCE_PENDING"
			}
			notifcatalog.TriggerNotification(context.Background(), pgxPool,
				"/investment/fd/confirmation/variance-resolve", cID, map[string]interface{}{
					"entity_id":    eID,
					"record_id":    cID,
					"booking_id":   bID,
					"event":        event,
					"actor_email":  uEmail,
					"has_variance": hasVar,
				})
		}(confirmationID, bookingID, entityID, userEmail, hasVariance)

		// Build variance items response
		varOut := make([]map[string]interface{}, 0, len(varItems))
		for _, v := range varItems {
			if v.HasVariance {
				varOut = append(varOut, serializeVarianceItem(ctx, pgxPool, v))
			}
		}

		msg := "Confirmation saved with variance pending"
		if !hasVariance {
			msg = "All variances resolved — confirmation confirmed"
		}
		api.RespondWithPayload(w, true, msg, map[string]interface{}{
			"confirmation_id":     confirmationID,
			"booking_id":          bookingID,
			"confirmation_status": confStatus,
			"has_variance":        hasVariance,
			"variance_items":      varOut,
			"run_id":              runID,
			"is_update":           isUpdate,
		})
		api.LogInfo("[FDBooking] VarianceResolve: confirmation=%s booking=%s has_variance=%v", confirmationID, bookingID, hasVariance)
	}
}

// ─── EditConfirmation ─────────────────────────────────────────────────────────
// POST /investment/fd/confirmation/edit
//
// PATCH-style update on an existing confirmation (not initial capture).
// Merges req.Fields, re-runs variance vs booking, returns variance_items for the UI.
// Re-opens VARIANCE_PENDING when an edit re-introduces variance (including from VARIANCE_ACCEPTED).
// Use /resolve-variance for full-body save on first capture after variance; use /edit for field tweaks.
//
// Blocked for CONFIRMED / APPROVED confirmations (finalized — immutable).
func EditConfirmation(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID         string                 `json:"user_id"`
			ConfirmationID string                 `json:"confirmation_id"`
			Fields         map[string]interface{} `json:"fields"`
			Reason         string                 `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.ConfirmationID == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrConfirmationIDsRequired)
			return
		}
		if len(req.Fields) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No fields to update")
			return
		}

		userEmail := api.GetUserEmailFromCtx(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		confCols, colErr := loadFDTableColumns(ctx, pgxPool, "investment", "fd_confirmation")
		if colErr != nil {
			msg, status := getUserFriendlyFDError(colErr, constants.ErrLoadConfirmationSchemaFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		editFieldMap := buildEditConfirmationFieldMap(confCols)
		missingSchemaFields := make([]string, 0, 2)
		if _, sent := req.Fields["bank_reference_number"]; sent && !confCols["bank_reference_number"] {
			missingSchemaFields = append(missingSchemaFields, "bank_reference_number")
		}
		if _, sent := req.Fields["premature_closure_terms"]; sent && !confCols["premature_closure_terms"] {
			missingSchemaFields = append(missingSchemaFields, "premature_closure_terms")
		}
		if len(missingSchemaFields) > 0 {
			api.RespondWithError(w, http.StatusBadRequest,
				"Cannot update fields missing from fd_confirmation schema: "+strings.Join(missingSchemaFields, ", ")+"; run scripts/migrations/20260520_fd_confirmation_edit_fields.sql")
			return
		}

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			msg, status := getUserFriendlyFDError(err, constants.ErrTransactionFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx) //nolint:errcheck

		// ── Load current confirmation + linked booking (FOR UPDATE) ─────────────
		var bookingID, entityID, currentStatus, oldTenorType, oldPenaltyID string
		var oldInterestTypeCode, oldFrequencyID, oldPayoutFreqID, oldAccrualFreqCode, oldResetType string
		var oldBankFDRefNo, oldBankReferenceNumber string
		var oldPrincipal, oldRate float64
		var oldTenorDays, oldTenorMonths, oldTenorYears int
		var oldValueDate, oldMaturityDate, oldFirstCapDate, oldFirstPayoutDate string

		bankRefSelect := "''"
		if confCols["bank_reference_number"] {
			bankRefSelect = "COALESCE(c.bank_reference_number::text,'')"
		}

		err = tx.QueryRow(ctx, fmt.Sprintf(`
			SELECT
				COALESCE(c.booking_id,''), COALESCE(b.entity_id,''),
				COALESCE(c.confirmation_status,''), COALESCE(c.tenor_type,''), COALESCE(c.penalty_id,''),
				COALESCE(c.confirmed_interest_type_code,''), COALESCE(c.confirmed_frequency_id,''),
				COALESCE(c.payout_frequency_id,''),
				COALESCE(c.actual_principal,0), COALESCE(c.confirmed_rate,0),
				COALESCE(c.tenor_days,0), COALESCE(c.tenor_months,0), COALESCE(c.tenor_years,0),
				COALESCE(TO_CHAR(c.actual_start_date,'YYYY-MM-DD'),''),
				COALESCE(TO_CHAR(c.actual_maturity_date,'YYYY-MM-DD'),''),
				COALESCE(TO_CHAR(c.first_capitalization_date,'YYYY-MM-DD'),''),
				COALESCE(TO_CHAR(c.first_payout_date,'YYYY-MM-DD'),''),
				COALESCE(c.accrual_frequency_code,''),
				COALESCE(c.reset_type,''),
				COALESCE(c.bank_fd_ref_no,''),
				%s
			FROM investment.fd_confirmation c
			LEFT JOIN investment.fd_booking_request b ON b.booking_id = c.booking_id
			WHERE c.confirmation_id = $1 AND COALESCE(c.is_deleted,false) = false
			FOR UPDATE OF c`, bankRefSelect), req.ConfirmationID).
			Scan(&bookingID, &entityID, &currentStatus, &oldTenorType, &oldPenaltyID,
				&oldInterestTypeCode, &oldFrequencyID, &oldPayoutFreqID,
				&oldPrincipal, &oldRate,
				&oldTenorDays, &oldTenorMonths, &oldTenorYears,
				&oldValueDate, &oldMaturityDate, &oldFirstCapDate, &oldFirstPayoutDate,
				&oldAccrualFreqCode, &oldResetType, &oldBankFDRefNo, &oldBankReferenceNumber)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				api.RespondWithError(w, http.StatusNotFound,
					"Confirmation not found, was removed, or is no longer editable. Refresh and try again.")
				return
			}
			api.LogError("[EditConfirmation] linkage query failed confirmation_id=%s: %v", req.ConfirmationID, err)
			msg, status := getUserFriendlyFDError(err, "Fetch confirmation linkage failed")
			api.RespondWithError(w, status, msg)
			return
		}

		// ── Final-state guard — confirmed/live records are immutable here ─────
		if currentStatus == "CONFIRMED" || currentStatus == constants.StatusApproved {
			api.RespondWithError(w, http.StatusBadRequest,
				"Cannot edit a "+currentStatus+" confirmation — create a new change request or reopen through the allowed workflow")
			return
		}

		effectiveBankFDRef, effectiveBankReference := resolveEditConfirmationReferenceFields(
			req.Fields, oldBankFDRefNo, oldBankReferenceNumber,
		)
		if msg, status := validateFDConfirmationReferenceUniqueness(ctx, tx, fdConfirmationReferenceCheck{
			EntityID:              entityID,
			BankFDRefNo:           effectiveBankFDRef,
			BankReferenceNumber:   effectiveBankReference,
			ExcludeConfirmationID: req.ConfirmationID,
		}); msg != "" {
			api.RespondWithError(w, status, msg)
			return
		}

		// ── Load booked baseline for variance comparison ──────────────────────
		var bookedPrincipal, bookedRate float64
		var bookedTenorDays, bookedTenorMonths, bookedTenorYears int
		var bookedValueDate, bookedMaturityDate, bookedInterestTypeCode, bookedFrequencyID, bookedTenorType string
		var bookedAccrualFreqCode, bookedResetType, bookedPayoutFreqID string
		if bookingID != "" {
			_ = pgxPool.QueryRow(ctx, `
				SELECT
					COALESCE(principal_amount,0), COALESCE(interest_rate,0),
					COALESCE(tenure_days,0), COALESCE(tenure_months,0), COALESCE(tenure_years,0),
					COALESCE(TO_CHAR(value_date,'YYYY-MM-DD'),''),
					COALESCE(TO_CHAR(expected_maturity_date,'YYYY-MM-DD'),''),
					COALESCE(interest_type_code,''), COALESCE(frequency_id,''), COALESCE(tenor_type,''),
					COALESCE(accrual_frequency_code,''),
					COALESCE(reset_type,''),
					COALESCE(NULLIF(payout_frequency_id,''), frequency_id, '')
				FROM investment.fd_booking_request
				WHERE booking_id = $1 AND COALESCE(is_deleted,false) = false`, bookingID).
				Scan(&bookedPrincipal, &bookedRate, &bookedTenorDays, &bookedTenorMonths, &bookedTenorYears,
					&bookedValueDate, &bookedMaturityDate, &bookedInterestTypeCode, &bookedFrequencyID, &bookedTenorType,
					&bookedAccrualFreqCode, &bookedResetType, &bookedPayoutFreqID)

			// Inject booking fallbacks into req.Fields for fields the caller omitted
			// that are also empty on the current confirmation.
			if _, sent := req.Fields["accrual_frequency_code"]; !sent && oldAccrualFreqCode == "" && bookedAccrualFreqCode != "" {
				req.Fields["accrual_frequency_code"] = bookedAccrualFreqCode
			}
			if _, sent := req.Fields["reset_type"]; !sent && oldResetType == "" && bookedResetType != "" {
				req.Fields["reset_type"] = bookedResetType
			}
			if _, sent := req.Fields["confirmed_frequency_id"]; !sent && oldFrequencyID == "" && bookedPayoutFreqID != "" {
				req.Fields["confirmed_frequency_id"] = bookedPayoutFreqID
			}
		}

		// ── Merge req.Fields into effective new values ────────────────────────
		// Start from current confirmed values; override with whatever the caller sent.
		effPrincipal := oldPrincipal
		effRate := oldRate
		effTenorDays := oldTenorDays
		effTenorMonths := oldTenorMonths
		effTenorYears := oldTenorYears
		effValueDate := oldValueDate
		effMaturityDate := oldMaturityDate
		effInterestTypeCode := oldInterestTypeCode
		effFrequencyID := oldFrequencyID
		effPayoutFreqID := oldPayoutFreqID
		effResetType := oldResetType
		effAccrualFreqCode := oldAccrualFreqCode
		effTenorType := oldTenorType
		effFirstCapDate := oldFirstCapDate
		effFirstPayoutDate := oldFirstPayoutDate

		if v, ok := req.Fields["actual_principal"]; ok {
			if fv, ok2 := v.(float64); ok2 {
				effPrincipal = fv
			}
		}
		if v, ok := req.Fields["confirmed_principal_amount"]; ok {
			if fv, ok2 := v.(float64); ok2 {
				effPrincipal = fv
			}
		}
		if v, ok := req.Fields["confirmed_rate"]; ok {
			if fv, ok2 := v.(float64); ok2 {
				effRate = fv
			}
		}
		if v, ok := req.Fields["confirmed_interest_rate"]; ok {
			if fv, ok2 := v.(float64); ok2 {
				effRate = fv
			}
		}
		if v, ok := req.Fields["tenor_days"]; ok {
			switch iv := v.(type) {
			case float64:
				effTenorDays = int(iv)
			case int:
				effTenorDays = iv
			}
		}
		if v, ok := req.Fields["tenor_months"]; ok {
			switch iv := v.(type) {
			case float64:
				effTenorMonths = int(iv)
			case int:
				effTenorMonths = iv
			}
		}
		if v, ok := req.Fields["tenor_years"]; ok {
			switch iv := v.(type) {
			case float64:
				effTenorYears = int(iv)
			case int:
				effTenorYears = iv
			}
		}
		if v, ok := req.Fields["actual_start_date"]; ok {
			if d := coerceDateString(v); d != "" {
				effValueDate = d
			}
		}
		if v, ok := req.Fields["confirmed_value_date"]; ok {
			if d := coerceDateString(v); d != "" {
				effValueDate = d
			}
		}
		if v, ok := req.Fields["actual_maturity_date"]; ok {
			if d := coerceDateString(v); d != "" {
				effMaturityDate = d
			}
		}
		if v, ok := req.Fields["confirmed_maturity_date"]; ok {
			if d := coerceDateString(v); d != "" {
				effMaturityDate = d
			}
		}
		if v, ok := req.Fields["confirmed_interest_type_code"]; ok {
			if sv, ok2 := v.(string); ok2 && sv != "" {
				effInterestTypeCode = sv
			}
		}
		if v, ok := req.Fields["confirmed_frequency_id"]; ok {
			if sv, ok2 := v.(string); ok2 && sv != "" {
				effFrequencyID = sv
			}
		}
		if v, ok := req.Fields["payout_frequency_id"]; ok {
			if sv, ok2 := v.(string); ok2 {
				effPayoutFreqID = sv
			}
		}
		if v, ok := req.Fields["reset_type"]; ok {
			if sv, ok2 := v.(string); ok2 && sv != "" {
				effResetType = strings.ToUpper(strings.TrimSpace(sv))
			}
		}
		if v, ok := req.Fields["accrual_frequency_code"]; ok {
			if sv, ok2 := v.(string); ok2 {
				effAccrualFreqCode = strings.ToUpper(strings.TrimSpace(sv))
			}
		}
		if v, ok := req.Fields["tenor_type"]; ok {
			if sv, ok2 := v.(string); ok2 && sv != "" {
				effTenorType = strings.ToUpper(strings.TrimSpace(sv))
			}
		}
		if v, ok := req.Fields["first_capitalization_date"]; ok {
			if d := coerceDateString(v); d != "" {
				effFirstCapDate = d
			}
		}
		if v, ok := req.Fields["first_payout_date"]; ok {
			if d := coerceDateString(v); d != "" {
				effFirstPayoutDate = d
			}
		}

		if effPrincipal <= 0 || effRate <= 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrConfirmedPrincipalAndInterest)
			return
		}

		// ── Run variance engine against booked baseline ───────────────────────
		runID := varianceengine.NewRunID()
		var varItems []varianceengine.VarianceItem
		var hasVariance bool
		if bookingID != "" {
			bookedPayout, actualPayout := normalizedPayoutFrequency(ctx, pgxPool,
				bookedPayoutFreqID, bookedFrequencyID, effPayoutFreqID, effFrequencyID,
			)
			bookedAccrual, actualAccrual := normalizedAccrualFrequency(ctx, pgxPool,
				accrualFrequencyInput{BookedCode: bookedAccrualFreqCode, ActualCode: effAccrualFreqCode, BookedPayout: bookedPayout, ActualPayout: actualPayout, BookedCompound: bookedFrequencyID, ActualCompound: effFrequencyID},
			)
			varItems, hasVariance = runFDConfirmationVarianceCompare(ctx, pgxPool, fdConfirmationVarianceInput{
				BookingID: bookingID, EntityID: entityID, RunID: runID,
				ResolvedBy: req.UserID, ResolvedByEmail: userEmail,
				BookedPrincipal: bookedPrincipal, ActualPrincipal: effPrincipal,
				BookedRate: bookedRate, ActualRate: effRate,
				BookedTenorDays: bookedTenorDays, ActualTenorDays: effTenorDays,
				BookedTenorMonths: bookedTenorMonths, ActualTenorMonths: effTenorMonths,
				BookedTenorYears: bookedTenorYears, ActualTenorYears: effTenorYears,
				BookedValueDate: bookedValueDate, ActualValueDate: effValueDate,
				BookedMaturityDate: bookedMaturityDate, ActualMaturityDate: effMaturityDate,
				BookedInterestType: bookedInterestTypeCode, ActualInterestType: effInterestTypeCode,
				BookedFrequencyID: bookedFrequencyID, ActualFrequencyID: effFrequencyID,
				BookedPayoutFrequencyID: bookedPayout, ActualPayoutFrequencyID: actualPayout,
				BookedResetType: bookedResetType, ActualResetType: effResetType,
				BookedAccrualFrequencyCode: bookedAccrual, ActualAccrualFrequencyCode: actualAccrual,
				BookedTenorType: bookedTenorType, ActualTenorType: effTenorType,
				ActualFirstCapDate: effFirstCapDate, ActualFirstPayoutDate: effFirstPayoutDate,
			})
		}

		// Derive confirmation_status from variance result
		newConfStatus := currentStatus
		if hasVariance {
			// Any edit that re-introduces variance re-opens the variance workflow
			newConfStatus = "VARIANCE_PENDING"
		} else if currentStatus == "VARIANCE_PENDING" || currentStatus == constants.StatusRejected {
			// No more variance and it was previously dirty — promote to PENDING_APPROVAL
			newConfStatus = constants.StatusPendingApproval
		}
		// VARIANCE_ACCEPTED with no new variance: keep VARIANCE_ACCEPTED (ready for /approve)
		// If caller explicitly overrides confirmation_status, allow it (unless APPROVED)
		if sv, ok := req.Fields["confirmation_status"]; ok {
			if svStr, ok2 := sv.(string); ok2 && svStr != constants.StatusApproved {
				newConfStatus = svStr
			}
		}

		// Build variance_details JSON
		varDetailsJSON := buildVarianceDetailsJSON(varItems)

		// ── Build dynamic SET clause (schema-aware + UI aliases) ───────────────
		setClauses := make([]string, 0)
		setArgs := make([]interface{}, 0)
		argIdx := 1
		updateAmount := effPrincipal
		usedColumns := make(map[string]bool)
		appliedFields := make([]string, 0)
		ignoredFields := make([]string, 0)

		for k, v := range req.Fields {
			dbCol, ok := editFieldMap[k]
			if !ok {
				ignoredFields = append(ignoredFields, k)
				continue
			}
			if usedColumns[dbCol] {
				continue
			}
			usedColumns[dbCol] = true
			appliedFields = append(appliedFields, k)
			v = editConfirmationCoerceFieldValue(k, dbCol, v)
			setClauses = append(setClauses, fmt.Sprintf("%s = $%d", dbCol, argIdx))
			setArgs = append(setArgs, v)
			argIdx++
		}

		// Always stamp variance outcome and derived status from engine (variance_action stays NULL until exception/reject)
		setClauses = append(setClauses,
			fmt.Sprintf("variance_flag = $%d", argIdx),
			fmt.Sprintf("variance_details = $%d", argIdx+1),
			fmt.Sprintf("confirmation_status = $%d", argIdx+2),
		)
		setArgs = append(setArgs, hasVariance, varDetailsJSON, newConfStatus)
		argIdx += 3
		if hasVariance {
			// Re-opened variance: clear prior exception decision so UI can exception/reject again
			setClauses = append(setClauses,
				"variance_action = NULL",
				"variance_remarks = NULL",
				"variance_resolved_by = NULL",
				"variance_resolved_at = NULL",
			)
		} else if newConfStatus == "VARIANCE_ACCEPTED" {
			setClauses = append(setClauses, "variance_action = 'ACCEPTED'")
		} else {
			setClauses = append(setClauses, "variance_action = NULL")
		}

		if len(appliedFields) == 0 && len(ignoredFields) > 0 {
			api.RespondWithError(w, http.StatusBadRequest,
				fmt.Sprintf("No editable fields in request — unknown or unsupported: %s", strings.Join(ignoredFields, ", ")))
			return
		}

		setClauses = append(setClauses, fmt.Sprintf("updated_by = $%d", argIdx))
		setArgs = append(setArgs, userEmail)
		argIdx++
		setClauses = append(setClauses, "updated_at = now()")
		setArgs = append(setArgs, req.ConfirmationID)

		updateQ := fmt.Sprintf(`UPDATE investment.fd_confirmation SET %s WHERE confirmation_id = $%d`,
			strings.Join(setClauses, ", "), argIdx)
		if _, err = tx.Exec(ctx, updateQ, setArgs...); err != nil {
			api.LogError("[EditConfirmation] UPDATE error: %v", err)
			msg, status := getUserFriendlyFDError(err, constants.ErrUpdateConfirmationFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		// Do not update booking status to CONFIRMED yet; it must go through checker approval first.

		// ── Audit the update ──────────────────────────────────────────────────
		if _, err = tx.Exec(ctx, `
			INSERT INTO investment.fd_audit_confirmation (
				confirmation_id, action_type, processing_status,
				requested_by, requested_at, reason,
				old_actual_principal, old_confirmed_rate,
				old_confirmation_status,
				old_tenor_type, old_tenor_days, old_penalty_id
			) VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL',$2,now(),$3,$4,$5,$6,$7,$8,$9)`,
			req.ConfirmationID, userEmail, req.Reason,
			oldPrincipal, oldRate, currentStatus, oldTenorType, oldTenorDays, oldPenaltyID,
		); err != nil {
			msg, status := getUserFriendlyFDError(err, constants.ErrAuditInsertFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		if err = tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyFDError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}

		// ── Trigger approval engine sequence ────────────────────────────────
		go func(cID, uID, uEmail, eID string, amount float64) {
			defer func() { recover() }() //nolint:errcheck
			bgCtx := context.Background()
			_ = approvalengine.CancelPendingInstances(bgCtx, pgxPool, "FIXED_DEPOSIT", cID, uEmail)
			_, _ = approvalengine.CreateInstance(bgCtx, pgxPool, approvalengine.InstanceRequest{
				ModuleCode: "FIXED_DEPOSIT", EntityCode: eID,
				TransactionType: "FD_CONFIRMATION_EDIT", RecordID: cID,
				RecordTable: constants.QuerryConfirmation, AuditTable: constants.QuerryAuditConfirmation,
				AuditIDColumn: "confirmation_id", ActionType: "EDIT",
				Amount: amount, SubmittedBy: uID, SubmittedByEmail: uEmail,
			})
		}(req.ConfirmationID, req.UserID, userEmail, entityID, updateAmount)

		go func(cID, bID, eID, uEmail string, hasVar bool) {
			defer func() {
				if rec := recover(); rec != nil {
					api.LogError("[FDBooking] EditConfirmation notification panic for %s: %v", cID, rec)
				}
			}()
			notifcatalog.TriggerNotification(context.Background(), pgxPool,
				"/investment/fd/confirmation/edit", cID, map[string]interface{}{
					"entity_id":    eID,
					"record_id":    cID,
					"booking_id":   bID,
					"event":        "FD_CONFIRMATION_EDIT_SUBMITTED",
					"actor_email":  uEmail,
					"has_variance": hasVar,
				})
		}(req.ConfirmationID, bookingID, entityID, userEmail, hasVariance)

		// Build variance items response — return ALL fields so UI can render full comparison table
		varOut := make([]map[string]interface{}, 0, len(varItems))
		for _, v := range varItems {
			varOut = append(varOut, serializeVarianceItem(ctx, pgxPool, v))
		}

		confirmMsg := "Confirmation updated successfully"
		if hasVariance {
			confirmMsg = "Confirmation updated with variance detected — resolve or accept as exception before approving"
		}
		api.RespondWithPayload(w, true, confirmMsg, map[string]interface{}{
			"confirmation_id":     req.ConfirmationID,
			"booking_id":          bookingID,
			"confirmation_status": newConfStatus,
			"has_variance":        hasVariance,
			"variance_items":      varOut,
			"run_id":              runID,
			"requested":           userEmail,
			"applied_fields":      appliedFields,
			"ignored_fields":      ignoredFields,
		})
		api.LogInfo("[FDBooking] EditConfirmation: id=%s status=%s has_variance=%v", req.ConfirmationID, newConfStatus, hasVariance)
	}
}

// ─── VarianceException ────────────────────────────────────────────────────────
// POST /investment/fd/confirmation/variance-exception
//
// Business "accept variance" — bank values stand; proceed to approval workflow.
// Marks OPEN variance_log rows EXCEPTION; sets status VARIANCE_ACCEPTED, variance_action ACCEPTED.
// Reject the line via POST /confirmation/reject (not this handler). Idempotent if already VARIANCE_ACCEPTED.
func VarianceException(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID         string `json:"user_id"`
			ConfirmationID string `json:"confirmation_id"`
			Reason         string `json:"reason"`
			Evidence       string `json:"evidence"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.ConfirmationID == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrConfirmationIDsRequired)
			return
		}

		userEmail := api.GetUserEmailFromCtx(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()

		// Build the entity_id / principal SELECT expressions dynamically — these
		// columns existed on older fd_confirmation schemas as fallbacks but were
		// dropped on some deployments. Without runtime detection the query
		// returns SQLSTATE 42703 (column does not exist).
		confCols, err := loadFDTableColumns(ctx, pgxPool, "investment", "fd_confirmation")
		if err != nil {
			msg, status := getUserFriendlyFDError(err, constants.ErrLoadConfirmationSchemaFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		entityIDExpr := "COALESCE(b.entity_id,'')"
		if confCols["entity_id"] {
			entityIDExpr = "COALESCE(c.entity_id, b.entity_id, '')"
		}
		principalExpr := "COALESCE(c.actual_principal,0)"
		if confCols["confirmed_principal_amount"] {
			principalExpr = "COALESCE(c.actual_principal, c.confirmed_principal_amount, 0)"
		}

		// Fetch confirmation state — LEFT JOIN so confirmations without a matching
		// booking row (e.g. directly-created VRN-* entries) still resolve correctly.
		// entity_id is taken from fd_confirmation first (when that column exists),
		// falling back to the booking row.
		var currentStatus, bookingID, entityID string
		var confPrincipal float64
		err = pgxPool.QueryRow(ctx, fmt.Sprintf(`
			SELECT c.confirmation_status,
			       COALESCE(c.booking_id,''),
			       %s,
			       %s
			FROM investment.fd_confirmation c
			LEFT JOIN investment.fd_booking_request b ON b.booking_id = c.booking_id
			WHERE c.confirmation_id = $1 AND COALESCE(c.is_deleted,false) = false`,
			entityIDExpr, principalExpr),
			req.ConfirmationID,
		).Scan(&currentStatus, &bookingID, &entityID, &confPrincipal)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Fetch confirmation failed: "+err.Error())
			return
		}
		if currentStatus == constants.StatusApproved {
			api.RespondWithError(w, http.StatusBadRequest,
				"Cannot accept variance exception on an APPROVED confirmation — the FD is live and immutable")
			return
		}
		// Already accepted — idempotent success (UI may double-submit; next step is /approve)
		if currentStatus == "VARIANCE_ACCEPTED" {
			api.RespondWithPayload(w, true,
				"Variance already accepted as exception — proceed to approval", map[string]interface{}{
					"confirmation_id":     req.ConfirmationID,
					"booking_id":          bookingID,
					"confirmation_status": "VARIANCE_ACCEPTED",
					"already_accepted":    true,
				})
			return
		}
		if currentStatus != "VARIANCE_PENDING" && currentStatus != constants.StatusRejected &&
			currentStatus != constants.StatusPendingApproval && currentStatus != constants.StatusPendingEditApproval &&
			currentStatus != "CONFIRMED" {
			api.RespondWithError(w, http.StatusBadRequest,
				fmt.Sprintf("variance-exception not allowed on confirmations with status %s — use /edit to change values or /reject to reject", currentStatus))
			return
		}

		// Mark all OPEN variance_log rows for this booking as EXCEPTION
		resolveReq := varianceengine.ResolveRequest{
			Resolution:      "EXCEPTION",
			Reason:          req.Reason,
			ResolvedBy:      req.UserID,
			ResolvedByEmail: userEmail,
			Evidence:        req.Evidence,
		}

		varIDs, hasVarianceFlag, err := ensureOpenVarianceLogForException(ctx, pgxPool, req.ConfirmationID, bookingID, entityID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Prepare variance exception failed: "+err.Error())
			return
		}
		if !hasVarianceFlag {
			api.RespondWithError(w, http.StatusBadRequest,
				"No open variance rows found for this confirmation — nothing to accept as exception")
			return
		}

		for _, vid := range varIDs {
			resolveReq.VarianceID = vid
			if err := varianceengine.ResolveVariance(ctx, pgxPool, resolveReq); err != nil {
				api.LogError("[FDBooking] VarianceException ResolveVariance %s: %v", vid, err)
			}
		}

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			msg, status := getUserFriendlyFDError(err, constants.ErrTransactionFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx) //nolint:errcheck

		// Update confirmation status to VARIANCE_ACCEPTED
		if _, err = tx.Exec(ctx, `
			UPDATE investment.fd_confirmation
			SET confirmation_status    = 'VARIANCE_ACCEPTED',
			    variance_action        = 'ACCEPTED',
			    variance_remarks       = $1,
			    variance_resolved_by   = $2,
			    variance_resolved_at   = now(),
			    updated_by             = $2,
			    updated_at             = now()
			WHERE confirmation_id = $3`,
			req.Reason, userEmail, req.ConfirmationID,
		); err != nil {
			msg, status := getUserFriendlyFDError(err, constants.ErrUpdateConfirmationFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		// Audit
		if _, err = tx.Exec(ctx, `
			INSERT INTO investment.fd_audit_confirmation (
				confirmation_id, action_type, processing_status,
				requested_by, requested_at, reason,
				old_confirmation_status, old_variance_action
			) VALUES ($1,'EDIT','PENDING_APPROVAL',$2,now(),$3,$4,'PENDING')`,
			req.ConfirmationID, userEmail, req.Reason, currentStatus,
		); err != nil {
			msg, status := getUserFriendlyFDError(err, constants.ErrAuditInsertFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		if err = tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyFDError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}

		// Refresh unresolved flag
		_ = varianceengine.RefreshUnresolvedFlag(ctx, pgxPool, "investment.fd_confirmation", "confirmation_id", req.ConfirmationID)

		go func(cID, uID, uEmail, eID string, amount float64) {
			defer func() { recover() }() //nolint:errcheck
			bgCtx := context.Background()
			_ = approvalengine.CancelPendingInstances(bgCtx, pgxPool, "FIXED_DEPOSIT", cID, uEmail)
			_, _ = approvalengine.CreateInstance(bgCtx, pgxPool, approvalengine.InstanceRequest{
				ModuleCode: "FIXED_DEPOSIT", EntityCode: eID,
				TransactionType: "FD_CONFIRMATION_VARIANCE_RESOLVE", RecordID: cID,
				RecordTable: constants.QuerryConfirmation, AuditTable: constants.QuerryAuditConfirmation,
				AuditIDColumn: "confirmation_id", ActionType: "EDIT",
				Amount: amount, SubmittedBy: uID, SubmittedByEmail: uEmail,
			})
		}(req.ConfirmationID, req.UserID, userEmail, entityID, confPrincipal)

		go func(cID, bID, eID, uEmail string) {
			defer func() {
				if rec := recover(); rec != nil {
					api.LogError("[FDBooking] VarianceException notification panic for %s: %v", cID, rec)
				}
			}()
			notifcatalog.TriggerNotification(context.Background(), pgxPool,
				"/investment/fd/confirmation/variance-exception", cID, map[string]interface{}{
					"entity_id":   eID,
					"record_id":   cID,
					"booking_id":  bID,
					"event":       "FD_CONFIRMATION_VARIANCE_EXCEPTION_RAISED",
					"actor_email": uEmail,
				})
		}(req.ConfirmationID, bookingID, entityID, userEmail)

		api.RespondWithPayload(w, true, "Variance accepted as exception — confirmation ready for approval", map[string]interface{}{
			"confirmation_id":     req.ConfirmationID,
			"booking_id":          bookingID,
			"confirmation_status": "VARIANCE_ACCEPTED",
			"exceptions_accepted": len(varIDs),
			"accepted_by":         userEmail,
		})
		api.LogInfo("[FDBooking] VarianceException: confirmation=%s exceptions=%d by=%s", req.ConfirmationID, len(varIDs), userEmail)
	}
}

// ─── ResolveVariance (kept for backward compat — delegates to VarianceResolve) ──
func ResolveVariance(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return VarianceResolve(pgxPool)
}

// confirmationPendingDeleteApproval reports whether checker approval should finalize
// a DELETE workflow (not a capture/edit confirm). Variance gates must not block that path.
func confirmationPendingDeleteApproval(ctx context.Context, pool *pgxpool.Pool, confirmationID string) bool {
	var pending bool
	_ = pool.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM investment.fd_audit_confirmation
			WHERE confirmation_id = $1
			  AND action_type = 'DELETE'
			  AND processing_status = 'PENDING_DELETE_APPROVAL'
		)`, confirmationID).Scan(&pending)
	return pending
}

// ─── BulkApproveConfirmation ─────────────────────────────────────────────────
// Engine-first: RecordAction handles eye sequence + audit stamp + status flip
// for the final eye. Falls back to direct stamp only when no engine instance.
func BulkApproveConfirmation(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID          string   `json:"user_id"`
			ConfirmationIDs []string `json:"confirmation_ids"`
			Comment         string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.ConfirmationIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrConfirmationIDsRequired)
			return
		}

		userEmail := api.GetUserEmailFromCtx(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		engineActed := 0
		directActed := 0
		var errors []string

		for _, cID := range req.ConfirmationIDs {
			// ── Load current confirmation status ─────────────────────────────────
			var confStatus string
			_ = pgxPool.QueryRow(ctx,
				`SELECT confirmation_status FROM investment.fd_confirmation WHERE confirmation_id=$1`, cID).
				Scan(&confStatus)

			// ── APPROVED guard — already approved, skip silently ─────────────────
			if confStatus == constants.StatusApproved {
				errors = append(errors, cID+": already APPROVED — skipped")
				continue
			}

			// ── Variance guard: applies to capture/edit approval only, not DELETE approval ──
			pendingDelete := confirmationPendingDeleteApproval(ctx, pgxPool, cID)
			if !pendingDelete {
				var openVarCount int
				_ = pgxPool.QueryRow(ctx,
					`SELECT COUNT(*) FROM public.variance_log WHERE record_id=$1 AND status='OPEN'`, cID).
					Scan(&openVarCount)
				if openVarCount > 0 {
					errors = append(errors, cID+": has unresolved variance — resolve or accept as exception first")
					continue
				}
				if confStatus == "VARIANCE_PENDING" {
					errors = append(errors, cID+": variance pending — resolve or accept as exception before approving")
					continue
				}
			}

			actionRes, actionErr := approvalengine.ActOnPendingOrDiagnose(ctx, pgxPool, approvalengine.ActOnPendingRequest{ModuleCode: "FIXED_DEPOSIT", RecordID: cID, UserID: req.UserID, UserEmail: userEmail, RoleID: "", Action: approvalengine.ActionApproved, Comment: req.Comment})
			if actionErr != nil {
				api.LogError("[FDConfirmation] RecordAction approve failed for %s: %v", cID, actionErr)
				errors = append(errors, cID+": "+actionErr.Error())
				continue
			}
			if actionRes.Acted {
				engineActed++
				// If the instance is now fully APPROVED, flip statuses.
				if actionRes.InstanceStatus == constants.StatusApproved {
					if _, execErr := pgxPool.Exec(ctx,
						`UPDATE investment.fd_confirmation
						 SET confirmation_status = 'CONFIRMED'
						 WHERE confirmation_id = $1
						   AND confirmation_status NOT IN ('VARIANCE_REJECTED','CONFIRMED')`, cID,
					); execErr != nil {
						api.LogError("[FDConfirmation] confirmation_status→CONFIRMED failed for %s: %v", cID, execErr)
					}
					if _, execErr := pgxPool.Exec(ctx,
						`UPDATE investment.fd_booking_request SET booking_status='CONFIRMED'
						 WHERE booking_id IN (SELECT booking_id FROM investment.fd_confirmation WHERE confirmation_id=$1)`, cID,
					); execErr != nil {
						api.LogError("[FDConfirmation] booking_status→CONFIRMED failed for %s: %v", cID, execErr)
					}
					if _, execErr := pgxPool.Exec(ctx,
						`UPDATE investment.fd_confirmation SET is_deleted=true
						 WHERE confirmation_id IN (
							 SELECT DISTINCT a.confirmation_id FROM investment.fd_audit_confirmation a
							 WHERE a.confirmation_id=$1 AND a.action_type='DELETE' AND a.processing_status='APPROVED'
						 )`, cID,
					); execErr != nil {
						api.LogError("[FDConfirmation] is_deleted flip failed for %s: %v", cID, execErr)
					}
					if _, execErr := pgxPool.Exec(ctx,
						`UPDATE investment.fd_booking_request
						 SET booking_status='SENT_TO_BANK'
						 WHERE booking_id IN (SELECT booking_id FROM investment.fd_confirmation WHERE confirmation_id=$1)
						   AND EXISTS (
							 SELECT 1 FROM investment.fd_audit_confirmation a
							 WHERE a.confirmation_id=$1 AND a.action_type='DELETE' AND a.processing_status='APPROVED'
						   )
						   AND NOT EXISTS (
							 SELECT 1 FROM investment.fd_master fm
							 WHERE (fm.confirmation_id=$1 OR fm.booking_id=investment.fd_booking_request.booking_id)
							   AND COALESCE(fm.is_deleted,false)=false
						   )`, cID,
					); execErr != nil {
						api.LogError("[FDConfirmation] booking_status→SENT_TO_BANK after delete failed for %s: %v", cID, execErr)
					}

				}
			} else {
				if actionRes.CancelledStale {
					api.LogInfo("[FDConfirmation] Cancelled stale approval instance for confirmation=%s: %s", cID, actionRes.Reason)
				} else if actionRes.Reason != "" {
					errors = append(errors, cID+": "+actionRes.Reason)
					continue
				}
				// No matrix — direct stamp.
				tx, err := pgxPool.Begin(ctx)
				if err != nil {
					errors = append(errors, cID+": tx begin failed")
					continue
				}
				tag1, err1 := tx.Exec(ctx, `UPDATE investment.fd_audit_confirmation a
					SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2,
					    old_confirmation_status=c.confirmation_status
					FROM investment.fd_confirmation c
					WHERE a.confirmation_id=c.confirmation_id AND a.confirmation_id=$3
					  AND a.processing_status LIKE '%PENDING%'`, userEmail, req.Comment, cID)
				_, err2 := tx.Exec(ctx, `UPDATE investment.fd_confirmation SET confirmation_status='CONFIRMED'
					WHERE confirmation_id=$1
					  AND confirmation_status NOT IN ('VARIANCE_REJECTED','CONFIRMED')`, cID)
				_, err3 := tx.Exec(ctx, `UPDATE investment.fd_booking_request SET booking_status='CONFIRMED'
					WHERE booking_id IN (SELECT booking_id FROM investment.fd_confirmation WHERE confirmation_id=$1)`, cID)
				_, err4 := tx.Exec(ctx, `UPDATE investment.fd_confirmation SET is_deleted=true
					WHERE confirmation_id IN (
						SELECT DISTINCT a.confirmation_id FROM investment.fd_audit_confirmation a
						WHERE a.confirmation_id=$1 AND a.action_type='DELETE' AND a.processing_status='APPROVED'
					)`, cID)
				_, err5 := tx.Exec(ctx, `UPDATE investment.fd_booking_request
					SET booking_status='SENT_TO_BANK'
					WHERE booking_id IN (SELECT booking_id FROM investment.fd_confirmation WHERE confirmation_id=$1)
					  AND EXISTS (
						SELECT 1 FROM investment.fd_audit_confirmation a
						WHERE a.confirmation_id=$1 AND a.action_type='DELETE' AND a.processing_status='APPROVED'
					  )
					  AND NOT EXISTS (
						SELECT 1 FROM investment.fd_master fm
						WHERE (fm.confirmation_id=$1 OR fm.booking_id=investment.fd_booking_request.booking_id)
						  AND COALESCE(fm.is_deleted,false)=false
					  )`, cID)
				if err1 != nil || err2 != nil || err3 != nil || err4 != nil || err5 != nil {
					_ = tx.Rollback(ctx)
					errors = append(errors, cID+": direct stamp failed")
					continue
				}
				if tag1.RowsAffected() == 0 {
					// Special case: VARIANCE_ACCEPTED confirmations may already have
					// their audit row stamped by the engine but the confirmation status
					// not yet flipped. Allow direct confirm in that case.
					if confStatus == "VARIANCE_ACCEPTED" {
						if _, execErr := pgxPool.Exec(ctx,
							`UPDATE investment.fd_confirmation SET confirmation_status='CONFIRMED',
							 updated_by=$1, updated_at=now()
							 WHERE confirmation_id=$2 AND confirmation_status='VARIANCE_ACCEPTED'`,
							userEmail, cID,
						); execErr != nil {
							_ = tx.Rollback(ctx)
							errors = append(errors, cID+": confirm after exception failed: "+execErr.Error())
							continue
						}
						if _, execErr := pgxPool.Exec(ctx,
							`UPDATE investment.fd_booking_request SET booking_status='CONFIRMED'
							 WHERE booking_id IN (SELECT booking_id FROM investment.fd_confirmation WHERE confirmation_id=$1)`, cID,
						); execErr != nil {
							api.LogError("[FDConfirmation] booking status flip failed for %s: %v", cID, execErr)
						}
						if cerr := tx.Commit(ctx); cerr != nil {
							errors = append(errors, cID+constants.ErrCommitFailed)
							continue
						}
						directActed++
						continue
					}
					_ = tx.Rollback(ctx)
					errors = append(errors, cID+": no pending audit action found (already approved or not found)")
					continue
				}
				if cerr := tx.Commit(ctx); cerr != nil {
					errors = append(errors, cID+constants.ErrCommitFailed)
					continue
				}
				directActed++
			}
		}
		totalActed := engineActed + directActed
		success := totalActed > 0 || len(errors) == 0
		msg := ""
		if !success {
			msg = "No confirmations were approved"
		}
		api.RespondWithPayload(w, success, msg, map[string]interface{}{
			"engine_acted": engineActed, "direct_acted": directActed,
			"errors": errors, "checker": userEmail,
		})
		for _, cID := range req.ConfirmationIDs {
			go func(id, uEmail string) {
				defer func() {
					if rec := recover(); rec != nil {
						api.LogError("[FDBooking] BulkApproveConfirmation notification goroutine panic for confirmation %s: %v", id, rec)
					}
				}()
				notifcatalog.TriggerNotification(context.Background(), pgxPool, "/investment/fd/confirmation/approve", id, map[string]interface{}{
					"record_id":   id,
					"event":       "FD_CONFIRMATION_APPROVED",
					"actor_email": uEmail,
				})
			}(cID, userEmail)
		}
		api.LogInfo("[FDConfirmation] BulkApproveConfirmation: engine=%d direct=%d errors=%d by=%s",
			engineActed, directActed, len(errors), userEmail)
	}
}

// ─── BulkRejectConfirmation ──────────────────────────────────────────────────

func BulkRejectConfirmation(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID          string   `json:"user_id"`
			ConfirmationIDs []string `json:"confirmation_ids"`
			Comment         string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.ConfirmationIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrConfirmationIDsRequired)
			return
		}

		userEmail := api.GetUserEmailFromCtx(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		engineActed := 0
		directActed := 0
		var errors []string

		for _, cID := range req.ConfirmationIDs {
			// ── APPROVED guard — already approved, skip silently ─────────────────
			var rejectConfStatus string
			_ = pgxPool.QueryRow(ctx,
				`SELECT confirmation_status FROM investment.fd_confirmation WHERE confirmation_id=$1`, cID).
				Scan(&rejectConfStatus)
			if rejectConfStatus == constants.StatusApproved {
				errors = append(errors, cID+": already APPROVED — cannot reject")
				continue
			}

			actionRes, actionErr := approvalengine.ActOnPendingOrDiagnose(ctx, pgxPool, approvalengine.ActOnPendingRequest{ModuleCode: "FIXED_DEPOSIT", RecordID: cID, UserID: req.UserID, UserEmail: userEmail, RoleID: "", Action: approvalengine.ActionRejected, Comment: req.Comment})
			if actionErr != nil {
				api.LogError("[FDConfirmation] RecordAction reject failed for %s: %v", cID, actionErr)
				errors = append(errors, cID+": "+actionErr.Error())
				continue
			}
			if actionRes.Acted {
				// finalizeRecord already set processing_status=REJECTED; flip confirmation status.
				if _, execErr := pgxPool.Exec(ctx,
					`UPDATE investment.fd_confirmation SET confirmation_status='REJECTED',
					 variance_action = 'REJECTED'
					 WHERE confirmation_id=$1 AND confirmation_status NOT IN ('CONFIRMED','VARIANCE_REJECTED')`, cID,
				); execErr != nil {
					api.LogError("[FDConfirmation] confirmation_status→REJECTED failed for %s: %v", cID, execErr)
				}
				if _, execErr := pgxPool.Exec(ctx,
					`UPDATE investment.fd_booking_request SET booking_status='SENT_TO_BANK'
					 WHERE booking_id IN (SELECT booking_id FROM investment.fd_confirmation WHERE confirmation_id=$1)`, cID,
				); execErr != nil {
					api.LogError("[FDConfirmation] booking_status→SENT_TO_BANK failed for %s: %v", cID, execErr)
				}
				engineActed++
			} else {
				if actionRes.CancelledStale {
					api.LogInfo("[FDConfirmation] Cancelled stale approval instance for confirmation=%s: %s", cID, actionRes.Reason)
				} else if actionRes.Reason != "" {
					errors = append(errors, cID+": "+actionRes.Reason)
					continue
				}
				tx, err := pgxPool.Begin(ctx)
				if err != nil {
					errors = append(errors, cID+": tx begin failed")
					continue
				}
				tag1, err1 := tx.Exec(ctx, `UPDATE investment.fd_audit_confirmation a
					SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2,
					    old_confirmation_status=c.confirmation_status
					FROM investment.fd_confirmation c
					WHERE a.confirmation_id=c.confirmation_id AND a.confirmation_id=$3
					  AND a.processing_status LIKE '%PENDING%'`, userEmail, req.Comment, cID)
				_, err2 := tx.Exec(ctx, `UPDATE investment.fd_confirmation SET confirmation_status='REJECTED',
					variance_action = 'REJECTED'
					WHERE confirmation_id=$1
					  AND confirmation_status NOT IN ('CONFIRMED','VARIANCE_REJECTED')`, cID)
				_, err3 := tx.Exec(ctx, `UPDATE investment.fd_booking_request SET booking_status='SENT_TO_BANK'
					WHERE booking_id IN (SELECT booking_id FROM investment.fd_confirmation WHERE confirmation_id=$1)`, cID)
				if err1 != nil || err2 != nil || err3 != nil {
					_ = tx.Rollback(ctx)
					errors = append(errors, cID+": direct stamp failed")
					continue
				}
				if tag1.RowsAffected() == 0 {
					_ = tx.Rollback(ctx)
					errors = append(errors, cID+": no pending audit action found (already rejected or not found)")
					continue
				}
				if cerr := tx.Commit(ctx); cerr != nil {
					errors = append(errors, cID+constants.ErrCommitFailed)
					continue
				}
				directActed++
			}
		}
		totalActed := engineActed + directActed
		success := totalActed > 0 || len(errors) == 0
		msg := ""
		if !success {
			msg = "No confirmations were rejected"
		}
		api.RespondWithPayload(w, success, msg, map[string]interface{}{
			"engine_acted": engineActed, "direct_acted": directActed,
			"errors": errors, "checker": userEmail,
		})
		for _, cID := range req.ConfirmationIDs {
			go func(id, uEmail string) {
				defer func() {
					if rec := recover(); rec != nil {
						api.LogError("[FDBooking] BulkRejectConfirmation notification goroutine panic for confirmation %s: %v", id, rec)
					}
				}()
				notifcatalog.TriggerNotification(context.Background(), pgxPool, "/investment/fd/confirmation/reject", id, map[string]interface{}{
					"record_id":   id,
					"event":       "FD_CONFIRMATION_REJECTED",
					"actor_email": uEmail,
				})
			}(cID, userEmail)
		}
		api.LogInfo("[FDConfirmation] BulkRejectConfirmation: engine=%d direct=%d errors=%d by=%s",
			engineActed, directActed, len(errors), userEmail)
	}
}

// ─── GetConfirmationsWithAudit ────────────────────────────────────────────────

func GetConfirmationsWithAudit(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		bookingAccountExpr, err := resolveFDBookingAccountExpression(ctx, pgxPool, "b")
		if err != nil {
			msg, status := getUserFriendlyFDError(err, constants.ErrLoadBookingSchemaFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		uploadKeyExpr := resolveFDConfirmationUploadKeyExpression(ctx, pgxPool, "c")

		q := fmt.Sprintf(`
			WITH latest_audit AS (
				SELECT DISTINCT ON (a.confirmation_id)
					a.confirmation_id,
					a.audit_id,
					a.action_type,
					a.processing_status,
					a.requested_by,
					a.requested_at,
					a.checker_by,
					a.checker_at,
					a.checker_comment,
					a.reason,
					a.old_confirmation_status,
					a.old_actual_principal,
					a.old_confirmed_rate,
					a.old_actual_start_date,
					a.old_actual_maturity_date,
					a.old_variance_flag,
					a.old_variance_action,
					a.old_bank_fd_ref_no,
					a.old_penalty_id
				FROM investment.fd_audit_confirmation a
				ORDER BY a.confirmation_id,
				         GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp),
				                  COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC
			),
			history AS (
				SELECT
					confirmation_id,
					MAX(CASE WHEN action_type='CREATE' THEN requested_by END) AS created_by,
					MAX(CASE WHEN action_type='CREATE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS created_at
				FROM investment.fd_audit_confirmation
				GROUP BY confirmation_id
			)
			SELECT
				c.confirmation_id,
				COALESCE(c.booking_id,'')                                              AS booking_id,
				COALESCE(b.entity_id,'')                                               AS entity_id,
				COALESCE(b.bank_id,'')                                                 AS bank_id,
				%s                                                                     AS bank_account_id,
				COALESCE(b.booking_status,'')                                          AS booking_status,
				COALESCE(c.actual_principal,0)                                         AS confirmed_principal_amount,
				COALESCE(c.confirmed_rate,0)                                           AS confirmed_interest_rate,
				COALESCE(TO_CHAR(c.actual_start_date,'YYYY-MM-DD'),'')                AS confirmed_value_date,
				COALESCE(TO_CHAR(c.actual_maturity_date,'YYYY-MM-DD'),'')             AS confirmed_maturity_date,
				COALESCE(c.bank_fd_ref_no,'')                                          AS bank_fd_reference,
				COALESCE(NULLIF(BTRIM(c.bank_reference_number::text),''),
				         c.bank_fd_ref_no, '')                                        AS bank_reference_number,
				COALESCE(TO_CHAR(c.confirmation_received_date,'YYYY-MM-DD'),'')       AS receipt_date,
				COALESCE(c.tenor_type,'')                                              AS confirmed_tenor_type,
				COALESCE(c.tenor_days,0)                                               AS confirmed_tenor_days,
				COALESCE(c.tenor_months,0)                                             AS confirmed_tenor_months,
				COALESCE(c.tenor_years,0)                                              AS confirmed_tenor_years,
				COALESCE(c.confirmed_interest_type_code,'')                            AS confirmed_interest_type,
				COALESCE(c.confirmed_frequency_id,'')                                  AS confirmed_frequency_id,
				COALESCE(c.variance_flag,false)                                        AS has_variance,
				COALESCE(c.variance_threshold_breached,false)                          AS variance_threshold_breached,
				COALESCE(c.variance_action,'')                                         AS variance_action,
				COALESCE(c.variance_remarks,'')                                        AS variance_remarks,
				COALESCE(c.variance_resolved_by,'')                                    AS variance_resolved_by,
				COALESCE(TO_CHAR(c.variance_resolved_at,'YYYY-MM-DD HH24:MI:SS'),'') AS variance_resolved_at,
				COALESCE(c.confirmation_status,'')                                     AS confirmation_status,
				COALESCE(c.penalty_id,'')                                              AS penalty_id,
				COALESCE(TO_CHAR(c.first_payout_date,'YYYY-MM-DD'),'')                AS first_payout_date,
				COALESCE(TO_CHAR(c.first_capitalization_date,'YYYY-MM-DD'),'')        AS first_capitalization_date,
				COALESCE(c.accrual_frequency_code,'')                                  AS confirmed_accrual_frequency_code,
				COALESCE(c.reset_type,'')                                              AS confirmed_reset_type,
				COALESCE(b.principal_amount,0)                                         AS principal_amount,
				COALESCE(NULLIF(b.value_type,''),'Actual')                             AS booking_value_type,
				COALESCE(NULLIF(c.value_type,''), NULLIF(b.value_type,''), 'Actual')  AS value_type,
				COALESCE(b.interest_rate,0)                                            AS interest_rate,
				COALESCE(b.interest_type_code,'')                                      AS interest_type,
				COALESCE(b.tenure_days,0)                                              AS tenor_days,
				COALESCE(b.tenure_months,0)                                            AS tenor_months,
				COALESCE(b.tenure_years,0)                                             AS tenor_years,
				COALESCE(TO_CHAR(b.value_date,'YYYY-MM-DD'),'')                        AS value_date,
				COALESCE(TO_CHAR(b.expected_start_date,'YYYY-MM-DD'),'')               AS expected_start_date,
				COALESCE(TO_CHAR(b.expected_maturity_date,'YYYY-MM-DD'),'')            AS expected_maturity_date,
				COALESCE(b.bank_config_id,'')                                          AS bank_config_id,
				COALESCE(b.frequency_id,'')                                            AS frequency_id,
				COALESCE(NULLIF(b.payout_frequency_id,''), b.frequency_id, '')         AS payout_frequency_id,
				COALESCE(b.day_count_code,'')                                          AS day_count_code,
				COALESCE(b.tds_plan_id,'')                                             AS tds_plan_id,
				COALESCE(b.accrual_frequency_code,'')                                  AS accrual_frequency_code,
				COALESCE(NULLIF(b.reset_type,''),'AT_MATURITY')                        AS reset_type,
				COALESCE(c.actual_principal,0)                                         AS actual_principal,
				COALESCE(c.confirmed_rate,0)                                           AS confirmed_rate,
				COALESCE(TO_CHAR(c.actual_start_date,'YYYY-MM-DD'),'')                 AS actual_start_date,
				COALESCE(TO_CHAR(c.actual_maturity_date,'YYYY-MM-DD'),'')              AS actual_maturity_date,
				COALESCE(bc.holiday_calendar_code,'')                                  AS holiday_calendar_code,
				COALESCE(bc.tds_deduction_timing,'')                                   AS tds_deduction_timing,
				COALESCE(bc.rounding_method,'')                                        AS rounding_method,
				COALESCE(bc.rounding_frequency,'')                                     AS rounding_frequency,
				COALESCE(bc.interest_rounding_decimals,2)                              AS interest_rounding_decimals,
				COALESCE(bc.broken_period_method,'')                                   AS broken_period_method,
				COALESCE(bc.broken_period_location,'')                                 AS broken_period_location,
				COALESCE(bc.grace_period_days,0)                                       AS grace_period_days,
				COALESCE(bc.grace_period_rate_type,'')                                 AS grace_period_rate_type,
				COALESCE(bc.capitalization_schedule_type,'')                           AS capitalization_schedule_type,
				COALESCE(bc.capitalization_date_adjustment,'')                         AS capitalization_date_adjustment,
				COALESCE(bc.weekend_accrual,true)                                      AS weekend_accrual,
				COALESCE(bc.holiday_accrual,true)                                      AS holiday_accrual,
				COALESCE(bc.accrual_start_convention,'')                               AS accrual_start_convention,
				COALESCE(bc.accrual_end_convention,'')                                 AS accrual_end_convention,
				COALESCE(bc.period_boundary_definition,'')                             AS period_boundary_definition,
				COALESCE(bc.quarter_definition,'')                                     AS quarter_definition,
				%s,
				COALESCE(c.is_deleted,false)                                           AS is_deleted,
				COALESCE(TO_CHAR(c.created_at,'YYYY-MM-DD HH24:MI:SS'),'')           AS record_created_at,

				COALESCE(l.audit_id::text,'')                                          AS audit_id,
				COALESCE(l.action_type,'')                                             AS action_type,
				COALESCE(l.processing_status,'')                                       AS processing_status,
				COALESCE(l.requested_by,'')                                            AS requested_by,
				COALESCE(TO_CHAR(l.requested_at,'YYYY-MM-DD HH24:MI:SS'),'')          AS requested_at,
				COALESCE(l.checker_by,'')                                              AS checker_by,
				COALESCE(TO_CHAR(l.checker_at,'YYYY-MM-DD HH24:MI:SS'),'')            AS checker_at,
				COALESCE(l.checker_comment,'')                                         AS checker_comment,
				COALESCE(l.reason,'')                                                  AS reason,
				COALESCE(l.old_confirmation_status,'')                                 AS old_confirmation_status,
				COALESCE(l.old_actual_principal,0)                                     AS old_confirmed_principal_amount,
				COALESCE(l.old_confirmed_rate,0)                                       AS old_confirmed_interest_rate,
				COALESCE(TO_CHAR(l.old_actual_start_date,'YYYY-MM-DD'),'')             AS old_confirmed_value_date,
				COALESCE(TO_CHAR(l.old_actual_maturity_date,'YYYY-MM-DD'),'')          AS old_confirmed_maturity_date,
				COALESCE(l.old_variance_flag,false)                                    AS old_has_variance,
				COALESCE(l.old_variance_action,'')                                     AS old_variance_action,
				COALESCE(l.old_bank_fd_ref_no,'')                                      AS old_bank_fd_reference,
				COALESCE(l.old_penalty_id,'')                                          AS old_penalty_id,

				COALESCE(h.created_by,'')                                              AS created_by,
				COALESCE(h.created_at,'')                                              AS created_at,

				-- Approval engine columns
				COALESCE(ai.instance_id,'')                                            AS approval_instance_id,
				COALESCE(ai.status,'')                                                 AS approval_engine_status,
				COALESCE(aie.instance_eye_id,'')                                       AS current_eye_id,
				COALESCE(aie.position::text,'')                                        AS current_eye_position,
				COALESCE(aie.approvals_required,0)                                     AS approvals_required,
				COALESCE(aie.approvals_received,0)                                     AS approvals_received,
				aie.sla_deadline                                                       AS sla_deadline,
				COALESCE(aie.is_escalated,false)                                       AS is_escalated

			FROM investment.fd_confirmation c
			LEFT JOIN investment.fd_booking_request b ON b.booking_id = c.booking_id
			LEFT JOIN investment.fd_bank_config_master bc
				ON bc.config_id = b.bank_config_id AND COALESCE(bc.is_deleted,false) = false
			LEFT JOIN latest_audit l ON l.confirmation_id = c.confirmation_id
			LEFT JOIN history h      ON h.confirmation_id = c.confirmation_id
			LEFT JOIN uam.approval_instance ai
				ON ai.record_id = c.confirmation_id
				AND ai.module_code = 'FIXED_DEPOSIT'
				AND ai.status = 'PENDING'
				AND ai.is_deleted = false
			LEFT JOIN uam.approval_instance_eye aie
				ON aie.instance_id = ai.instance_id
				AND aie.status = 'ACTIVE'
			WHERE COALESCE(c.is_deleted,false) = false
			ORDER BY GREATEST(
				COALESCE(l.requested_at,'1970-01-01'::timestamp),
				COALESCE(l.checker_at,'1970-01-01'::timestamp)
			) DESC`, bookingAccountExpr, uploadKeyExpr)

		rows, err := pgxPool.Query(ctx, q)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		fields := rows.FieldDescriptions()
		out := make([]map[string]interface{}, 0, 100)
		for rows.Next() {
			vals, _ := rows.Values()
			row := make(map[string]interface{}, len(fields))
			for i, f := range fields {
				if vals[i] == nil {
					row[string(f.Name)] = ""
				} else {
					row[string(f.Name)] = vals[i]
				}
			}
			AttachSimulateDiffInput(row)
			out = append(out, row)
		}
		if err := rows.Err(); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Row iteration error: "+err.Error())
			return
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]any{constants.ValueSuccess: true, "rows": out}) //nolint:errcheck
		api.LogInfo("[FDBooking] GetConfirmationsWithAudit: %d rows", len(out))
	}
}

// ─── GetConfirmationAuditHistory ──────────────────────────────────────────────

func GetConfirmationAuditHistory(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		confirmationID := r.URL.Query().Get("confirmation_id")

		var q string
		var args []interface{}
		if confirmationID != "" {
			q = `
				SELECT
					a.audit_id::text, a.confirmation_id, a.action_type, a.processing_status,
					COALESCE(a.requested_by,'') AS requested_by,
					COALESCE(TO_CHAR(a.requested_at,'YYYY-MM-DD HH24:MI:SS'),'') AS requested_at,
					COALESCE(a.checker_by,'') AS checker_by,
					COALESCE(TO_CHAR(a.checker_at,'YYYY-MM-DD HH24:MI:SS'),'') AS checker_at,
					COALESCE(a.checker_comment,'') AS checker_comment,
					COALESCE(a.reason,'') AS reason,
					COALESCE(a.old_confirmation_status,'') AS old_confirmation_status,
					COALESCE(a.old_actual_principal,0) AS old_confirmed_principal_amount,
					COALESCE(a.old_confirmed_rate,0) AS old_confirmed_interest_rate,
					COALESCE(TO_CHAR(a.old_actual_start_date,'YYYY-MM-DD'),'') AS old_confirmed_value_date,
					COALESCE(TO_CHAR(a.old_actual_maturity_date,'YYYY-MM-DD'),'') AS old_confirmed_maturity_date,
					COALESCE(a.old_variance_flag,false) AS old_has_variance,
					COALESCE(a.old_variance_action,'') AS old_variance_action,
					COALESCE(a.old_bank_fd_ref_no,'') AS old_bank_fd_reference,
					COALESCE(a.old_penalty_id,'') AS old_penalty_id,
					COALESCE(c.booking_id,'') AS booking_id,
					COALESCE(b.entity_id,'') AS entity_id,
					COALESCE(c.confirmation_status,'') AS confirmation_status,
					COALESCE(c.actual_principal,0) AS confirmed_principal_amount,
					COALESCE(c.confirmed_rate,0) AS confirmed_interest_rate,
					COALESCE(c.variance_flag,false) AS has_variance,
					COALESCE(c.variance_threshold_breached,false) AS variance_threshold_breached,
					COALESCE(c.variance_action,'') AS variance_action
				FROM investment.fd_audit_confirmation a
				LEFT JOIN investment.fd_confirmation c ON c.confirmation_id = a.confirmation_id
				LEFT JOIN investment.fd_booking_request b ON b.booking_id = c.booking_id
				WHERE a.confirmation_id = $1
				ORDER BY GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp),COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC`
			args = append(args, confirmationID)
		} else {
			q = `
				SELECT
					a.audit_id::text, a.confirmation_id, a.action_type, a.processing_status,
					COALESCE(a.requested_by,'') AS requested_by,
					COALESCE(TO_CHAR(a.requested_at,'YYYY-MM-DD HH24:MI:SS'),'') AS requested_at,
					COALESCE(a.checker_by,'') AS checker_by,
					COALESCE(TO_CHAR(a.checker_at,'YYYY-MM-DD HH24:MI:SS'),'') AS checker_at,
					COALESCE(a.checker_comment,'') AS checker_comment,
					COALESCE(a.reason,'') AS reason,
					COALESCE(a.old_confirmation_status,'') AS old_confirmation_status,
					COALESCE(a.old_actual_principal,0) AS old_confirmed_principal_amount,
					COALESCE(a.old_confirmed_rate,0) AS old_confirmed_interest_rate,
					COALESCE(TO_CHAR(a.old_actual_start_date,'YYYY-MM-DD'),'') AS old_confirmed_value_date,
					COALESCE(TO_CHAR(a.old_actual_maturity_date,'YYYY-MM-DD'),'') AS old_confirmed_maturity_date,
					COALESCE(a.old_variance_flag,false) AS old_has_variance,
					COALESCE(a.old_variance_action,'') AS old_variance_action,
					COALESCE(a.old_bank_fd_ref_no,'') AS old_bank_fd_reference,
					COALESCE(a.old_penalty_id,'') AS old_penalty_id,
					COALESCE(c.booking_id,'') AS booking_id,
					COALESCE(b.entity_id,'') AS entity_id,
					COALESCE(c.confirmation_status,'') AS confirmation_status,
					COALESCE(c.actual_principal,0) AS confirmed_principal_amount,
					COALESCE(c.confirmed_rate,0) AS confirmed_interest_rate,
					COALESCE(c.variance_flag,false) AS has_variance,
					COALESCE(c.variance_threshold_breached,false) AS variance_threshold_breached,
					COALESCE(c.variance_action,'') AS variance_action
				FROM investment.fd_audit_confirmation a
				LEFT JOIN investment.fd_confirmation c ON c.confirmation_id = a.confirmation_id
				LEFT JOIN investment.fd_booking_request b ON b.booking_id = c.booking_id
				ORDER BY GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp),COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC
				LIMIT 1000`
		}

		rows, err := pgxPool.Query(ctx, q, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		fields := rows.FieldDescriptions()
		out := make([]map[string]interface{}, 0)
		for rows.Next() {
			vals, _ := rows.Values()
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
		if err := rows.Err(); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrRowError+err.Error())
			return
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]any{constants.ValueSuccess: true, "audit_logs": out}) //nolint:errcheck
		api.LogInfo("[FDBooking] GetConfirmationAuditHistory: %d records", len(out))
	}
}

// ─── GetConfirmedConfirmations ────────────────────────────────────────────────

// GetConfirmedConfirmations returns confirmations that are CONFIRMED and
// do NOT yet have an fd_master record (i.e. ready for FD activation).
func GetConfirmedConfirmations(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		entityID := r.URL.Query().Get("entity_id")
		bookingAccountExpr, err := resolveFDBookingAccountExpression(ctx, pgxPool, "b")
		if err != nil {
			msg, status := getUserFriendlyFDError(err, constants.ErrLoadBookingSchemaFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		// Dynamically resolve which currency column exists to avoid parse-time errors.
		bookingCols, err := loadFDTableColumns(ctx, pgxPool, "investment", "fd_booking_request")
		if err != nil {
			msg, status := getUserFriendlyFDError(err, constants.ErrLoadBookingSchemaFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		confCols, err := loadFDTableColumns(ctx, pgxPool, "investment", "fd_confirmation")
		if err != nil {
			msg, status := getUserFriendlyFDError(err, constants.ErrLoadConfirmationSchemaFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		currencyExpr := constants.ErrEmptyString
		switch {
		case confCols["currency"]:
			currencyExpr = "COALESCE(c.currency, '')"
		case confCols["currency_code"]:
			currencyExpr = "COALESCE(c.currency_code, '')"
		case bookingCols["currency"]:
			currencyExpr = "COALESCE(b.currency, '')"
		case bookingCols["currency_code"]:
			currencyExpr = "COALESCE(b.currency_code, '')"
		}
		fpDateExpr := constants.ErrEmptyString
		if confCols["first_payout_date"] {
			fpDateExpr = "COALESCE(TO_CHAR(c.first_payout_date,'YYYY-MM-DD'),'')"
		}
		fcDateExpr := constants.ErrEmptyString
		if confCols["first_capitalization_date"] {
			fcDateExpr = "COALESCE(TO_CHAR(c.first_capitalization_date,'YYYY-MM-DD'),'')"
		}

		baseQ := fmt.Sprintf(`
			SELECT
				c.confirmation_id,
				COALESCE(c.booking_id,'') AS booking_id,
				COALESCE(b.entity_id,'') AS entity_id,
				COALESCE(b.bank_id,'') AS bank_id,
				%s AS bank_account_id,
				COALESCE(c.actual_principal,0) AS confirmed_principal_amount,
				COALESCE(c.confirmed_rate,0) AS confirmed_interest_rate,
				COALESCE(TO_CHAR(c.actual_start_date,'YYYY-MM-DD'),'') AS confirmed_value_date,
				COALESCE(TO_CHAR(c.actual_maturity_date,'YYYY-MM-DD'),'') AS confirmed_maturity_date,
				COALESCE(c.bank_fd_ref_no,'') AS bank_fd_reference,
				COALESCE(NULLIF(BTRIM(c.bank_reference_number::text),''),
				         c.bank_fd_ref_no, '') AS bank_reference_number,
				COALESCE(c.tenor_type,'') AS confirmed_tenor_type,
				COALESCE(c.tenor_days,0) AS confirmed_tenor_days,
				COALESCE(c.tenor_months,0) AS confirmed_tenor_months,
				COALESCE(c.tenor_years,0) AS confirmed_tenor_years,
				COALESCE(c.confirmed_interest_type_code,'') AS confirmed_interest_type,
				COALESCE(c.confirmed_frequency_id,'') AS confirmed_frequency_id,
				COALESCE(TO_CHAR(c.confirmation_received_date,'YYYY-MM-DD'),'') AS receipt_date,
				%s AS currency,
				COALESCE(c.confirmation_status,'') AS confirmation_status,
				%s AS first_payout_date,
				%s AS first_capitalization_date,
				EXISTS (
					SELECT 1 FROM investment.fd_master fm
					WHERE fm.confirmation_id = c.confirmation_id
					  AND COALESCE(fm.is_deleted,false) = false
				) AS fd_activated,
				-- latest audit timestamp: greatest of checker_at and requested_at from any audit row
				COALESCE((
					SELECT GREATEST(
						COALESCE(a.checker_at, '1970-01-01'::timestamptz),
						COALESCE(a.requested_at, '1970-01-01'::timestamptz)
					)
					FROM investment.fd_audit_confirmation a
					WHERE a.confirmation_id = c.confirmation_id
					ORDER BY GREATEST(
						COALESCE(a.checker_at, '1970-01-01'::timestamptz),
						COALESCE(a.requested_at, '1970-01-01'::timestamptz)
					) DESC
					LIMIT 1
				), c.created_at) AS latest_approval_at
			FROM investment.fd_confirmation c
			JOIN investment.fd_booking_request b ON b.booking_id = c.booking_id
			WHERE c.confirmation_status = 'CONFIRMED'
			  AND COALESCE(c.is_deleted,false) = false`, bookingAccountExpr, currencyExpr, fpDateExpr, fcDateExpr)

		var args []interface{}
		if entityID != "" {
			baseQ += ` AND b.entity_id = $1`
			args = append(args, entityID)
		}
		baseQ += ` ORDER BY latest_approval_at DESC`

		rows, err := pgxPool.Query(ctx, baseQ, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		fields := rows.FieldDescriptions()
		out := make([]map[string]interface{}, 0)
		for rows.Next() {
			vals, _ := rows.Values()
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
		if err := rows.Err(); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrRowError+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", out)
		api.LogInfo("[FDBooking] GetConfirmedConfirmations: %d ready for activation", len(out))
	}
}

// ─── GetConfirmationDetail ────────────────────────────────────────────────────
// Returns one confirmation row + full audit history + inline approval_workflow.
// Query: GET /investment/fd/confirmation/detail?confirmation_id=CONF-xxx&user_id=1
func GetConfirmationDetail(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		confirmationID := r.URL.Query().Get("confirmation_id")
		if confirmationID == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrConfirmationIDsRequired)
			return
		}
		viewerUserID := r.URL.Query().Get("user_id")
		ctx := r.Context()

		bookingAccountExpr, err := resolveFDBookingAccountExpression(ctx, pgxPool, "b")
		if err != nil {
			msg, status := getUserFriendlyFDError(err, constants.ErrLoadBookingSchemaFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		uploadKeyExpr := resolveFDConfirmationUploadKeyExpression(ctx, pgxPool, "c")

		confCols, err := loadFDTableColumns(ctx, pgxPool, "investment", "fd_confirmation")
		if err != nil {
			msg, status := getUserFriendlyFDError(err, constants.ErrLoadConfirmationSchemaFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		bankReferenceNumberSQL := "COALESCE(c.bank_fd_ref_no,'')"
		if confCols["bank_reference_number"] {
			bankReferenceNumberSQL = "COALESCE(NULLIF(BTRIM(c.bank_reference_number::text),''), c.bank_fd_ref_no,'')"
		}
		extraSelect := fdConfirmationDetailExtraSelect(confCols)

		// ── Confirmation row ─────────────────────────────────────────────────
		confRows, err := pgxPool.Query(ctx, fmt.Sprintf(`
			SELECT
				c.confirmation_id,
				COALESCE(c.booking_id,'')                                              AS booking_id,
				COALESCE(b.entity_id,'')                                               AS entity_id,
				COALESCE(b.bank_id,'')                                                 AS bank_id,
				%s                                                                     AS bank_account_id,
				COALESCE(b.booking_status,'')                                          AS booking_status,
				COALESCE(c.actual_principal,0)                                         AS confirmed_principal_amount,
				COALESCE(c.confirmed_rate,0)                                           AS confirmed_interest_rate,
				COALESCE(TO_CHAR(c.actual_start_date,'YYYY-MM-DD'),'')                AS confirmed_value_date,
				COALESCE(TO_CHAR(c.actual_maturity_date,'YYYY-MM-DD'),'')             AS confirmed_maturity_date,
				COALESCE(c.bank_fd_ref_no,'')                                          AS bank_fd_reference,
				%s                                                                     AS bank_reference_number,
				COALESCE(TO_CHAR(c.confirmation_received_date,'YYYY-MM-DD'),'')       AS receipt_date,
				COALESCE(c.tenor_type,'')                                              AS confirmed_tenor_type,
				COALESCE(c.tenor_days,0)                                               AS confirmed_tenor_days,
				COALESCE(c.tenor_months,0)                                             AS confirmed_tenor_months,
				COALESCE(c.tenor_years,0)                                              AS confirmed_tenor_years,
				COALESCE(c.confirmed_interest_type_code,'')                            AS confirmed_interest_type,
				COALESCE(c.confirmed_frequency_id,'')                                  AS confirmed_frequency_id,
				COALESCE(c.variance_flag,false)                                        AS has_variance,
				COALESCE(c.variance_threshold_breached,false)                          AS variance_threshold_breached,
				COALESCE(c.variance_details::text,'')                                  AS variance_details,
				COALESCE(c.variance_action,'')                                         AS variance_action,
				COALESCE(c.variance_remarks,'')                                        AS variance_remarks,
				COALESCE(c.variance_resolved_by,'')                                    AS variance_resolved_by,
				COALESCE(TO_CHAR(c.variance_resolved_at,'YYYY-MM-DD HH24:MI:SS'),'')  AS variance_resolved_at,
				COALESCE(c.confirmation_status,'')                                     AS confirmation_status,
				COALESCE(c.is_deleted,false)                                           AS is_deleted,
				COALESCE(TO_CHAR(c.first_payout_date,'YYYY-MM-DD'),'')                AS first_payout_date,
				COALESCE(TO_CHAR(c.first_capitalization_date,'YYYY-MM-DD'),'')        AS first_capitalization_date,
				%s,
				%s,
				COALESCE(TO_CHAR(c.created_at,'YYYY-MM-DD HH24:MI:SS'),'')            AS record_created_at
			FROM investment.fd_confirmation c
			LEFT JOIN investment.fd_booking_request b ON b.booking_id = c.booking_id
			WHERE c.confirmation_id = $1 AND COALESCE(c.is_deleted,false) = false`, bookingAccountExpr, bankReferenceNumberSQL, extraSelect, uploadKeyExpr),
			confirmationID)
		if err != nil {
			msg, httpStatus := getUserFriendlyFDError(err, constants.ErrQueryFailed)
			api.RespondWithError(w, httpStatus, msg)
			return
		}
		defer confRows.Close()
		if !confRows.Next() {
			api.RespondWithError(w, http.StatusNotFound, "Confirmation not found")
			return
		}
		cFields := confRows.FieldDescriptions()
		cVals, _ := confRows.Values()
		conf := make(map[string]interface{}, len(cFields))
		for i, f := range cFields {
			if cVals[i] == nil {
				conf[string(f.Name)] = ""
			} else {
				conf[string(f.Name)] = cVals[i]
			}
		}
		confRows.Close()

		// Parse JSON/text columns for the UI.
		if vd, ok := conf["variance_details"]; ok {
			if vdStr, ok := vd.(string); ok && vdStr != "" {
				var parsed interface{}
				if err := json.Unmarshal([]byte(vdStr), &parsed); err == nil {
					conf["variance_details"] = parsed
				}
			}
		}
		for _, jsonKey := range []string{"payout_dates", "compounding_dates"} {
			if raw, ok := conf[jsonKey]; ok {
				if s, ok := raw.(string); ok && s != "" {
					var parsed interface{}
					if err := json.Unmarshal([]byte(s), &parsed); err == nil {
						conf[jsonKey] = parsed
					}
				}
			}
		}

		// ── Booking data — full booked values for the linked booking ──────────
		bookingID, _ := conf["booking_id"].(string)
		var booking map[string]interface{}
		if bookingID != "" {
			bookingCols, colErr := loadFDTableColumns(ctx, pgxPool, "investment", "fd_booking_request")
			if colErr != nil {
				api.LogError("[FDBooking] GetConfirmationDetail booking schema: %v", colErr)
				bookingCols = map[string]bool{}
			}
			bookingAccountExprBk, _ := resolveFDBookingAccountExpression(ctx, pgxPool, "m")
			bookingAccountNumberExprBk, numErr := resolveFDBookingAccountNumberExpression(ctx, pgxPool, "m")
			if numErr != nil {
				api.LogError("[FDBooking] GetConfirmationDetail account number schema: %v", numErr)
				bookingAccountNumberExprBk = constants.ErrEmptyString
			}
			bkRows, bkErr := pgxPool.Query(ctx, fmt.Sprintf(`
				SELECT
					m.booking_id,
					%s                                                             AS entity_id,
					%s                                                             AS entity_name,
					%s                                                             AS bank_id,
					%s                                                             AS bank_name,
					%s                                                             AS bank_account_id,
					%s                                                             AS bank_account_number,
					%s                                                             AS bank_config_id,
					%s                                                             AS principal_amount,
					%s                                                             AS value_type,
					%s                                                             AS interest_rate,
					%s                                                             AS interest_type,
					%s                                                             AS interest_type_id,
					%s                                                             AS tenor_days,
					%s                                                             AS tenor_months,
					%s                                                             AS tenor_years,
					%s                                                             AS tenor_type,
					%s                                                             AS expected_start_date,
					%s                                                             AS value_date,
					%s                                                             AS maturity_date,
					%s                                                             AS frequency_id,
					COALESCE(NULLIF(%s,''), %s, '')                                AS payout_frequency_id,
					%s                                                             AS accrual_frequency_code,
					%s                                                             AS reset_type,
					%s                                                             AS first_payout_date,
					%s                                                             AS first_capitalization_date,
					%s                                                             AS day_count_code,
					%s                                                             AS tds_plan_id,
					%s                                                             AS holiday_calendar_code,
					%s                                                             AS product_code,
					%s                                                             AS auto_renewal,
					%s                                                             AS booking_remarks,
					%s                                                             AS booking_status,
					%s                                                             AS record_created_at,
					%s                                                             AS record_created_by
				FROM investment.fd_booking_request m
				WHERE m.booking_id = $1 AND COALESCE(m.is_deleted,false) = false`,
				fdTextExpr(bookingCols, "m", constants.ErrEmptyString, "entity_id"),
				fdTextExpr(bookingCols, "m", constants.ErrEmptyString, "entity_name"),
				fdTextExpr(bookingCols, "m", constants.ErrEmptyString, "bank_id"),
				fdTextExpr(bookingCols, "m", constants.ErrEmptyString, "bank_name"),
				bookingAccountExprBk,
				bookingAccountNumberExprBk,
				fdTextExpr(bookingCols, "m", constants.ErrEmptyString, "bank_config_id"),
				fdNumericExpr(bookingCols, "m", "0", "principal_amount"),
				fdTextExpr(bookingCols, "m", "'Actual'", "value_type"),
				fdNumericExpr(bookingCols, "m", "0", "interest_rate"),
				fdTextExpr(bookingCols, "m", constants.ErrEmptyString, "interest_type_code", "interest_type"),
				fdTextExpr(bookingCols, "m", constants.ErrEmptyString, "interest_type_id"),
				fdNumericExpr(bookingCols, "m", "0", "tenure_days", "tenor_days"),
				fdNumericExpr(bookingCols, "m", "0", "tenure_months", "tenor_months"),
				fdNumericExpr(bookingCols, "m", "0", "tenure_years", "tenor_years"),
				fdTextExpr(bookingCols, "m", constants.ErrEmptyString, "tenor_type"),
				fdDateTextExpr(bookingCols, "m", constants.ErrEmptyString, "expected_start_date", "start_date"),
				fdDateTextExpr(bookingCols, "m", constants.ErrEmptyString, "value_date", "actual_start_date"),
				fdDateTextExpr(bookingCols, "m", constants.ErrEmptyString, "expected_maturity_date", "maturity_date"),
				fdTextExpr(bookingCols, "m", constants.ErrEmptyString, "frequency_id"),
				fdTextExpr(bookingCols, "m", constants.ErrEmptyString, "payout_frequency_id"),
				fdTextExpr(bookingCols, "m", constants.ErrEmptyString, "frequency_id"),
				fdTextExpr(bookingCols, "m", constants.ErrEmptyString, "accrual_frequency_code"),
				fdTextExpr(bookingCols, "m", constants.ErrEmptyString, "reset_type"),
				fdDateTextExpr(bookingCols, "m", constants.ErrEmptyString, "first_payout_date"),
				fdDateTextExpr(bookingCols, "m", constants.ErrEmptyString, "first_capitalization_date"),
				fdTextExpr(bookingCols, "m", constants.ErrEmptyString, "day_count_code"),
				fdTextExpr(bookingCols, "m", constants.ErrEmptyString, "tds_plan_id"),
				fdTextExpr(bookingCols, "m", constants.ErrEmptyString, "holiday_calendar_code"),
				fdTextExpr(bookingCols, "m", constants.ErrEmptyString, "product_code"),
				fdBoolExpr(bookingCols, "m", "false", "auto_renewal", "auto_renewal_flag"),
				fdTextExpr(bookingCols, "m", constants.ErrEmptyString, "booking_remarks"),
				fdTextExpr(bookingCols, "m", constants.ErrEmptyString, "booking_status"),
				fdTimestampTextExpr(bookingCols, "m", constants.ErrEmptyString, "created_at"),
				fdTextExpr(bookingCols, "m", constants.ErrEmptyString, "created_by")),
				bookingID)
			if bkErr == nil {
				defer bkRows.Close()
				if bkRows.Next() {
					bkFields := bkRows.FieldDescriptions()
					bkVals, _ := bkRows.Values()
					booking = make(map[string]interface{}, len(bkFields))
					for i, f := range bkFields {
						if bkVals[i] == nil {
							booking[string(f.Name)] = ""
						} else {
							booking[string(f.Name)] = bkVals[i]
						}
					}
				}
				bkRows.Close()
			} else {
				api.LogError("[FDBooking] GetConfirmationDetail booking query: %v", bkErr)
			}
		}

		// ── Variance log — all variance_log entries for the booking ──────────
		var varianceLog []map[string]interface{}
		if bookingID != "" {
			vl, vlErr := varianceengine.GetVariances(ctx, pgxPool, bookingID)
			if vlErr != nil {
				api.LogError("[FDBooking] GetConfirmationDetail variance_log: %v", vlErr)
			} else {
				varianceLog = vl
			}
		}
		if varianceLog == nil {
			varianceLog = make([]map[string]interface{}, 0)
		}

		// ── Audit history ────────────────────────────────────────────────────
		auditRows, err := pgxPool.Query(ctx, `
			SELECT
				a.audit_id::text,
				a.confirmation_id,
				a.action_type,
				a.processing_status,
				COALESCE(a.requested_by,'')                                        AS requested_by,
				COALESCE(TO_CHAR(a.requested_at,'YYYY-MM-DD HH24:MI:SS'),'')      AS requested_at,
				COALESCE(a.checker_by,'')                                          AS checker_by,
				COALESCE(TO_CHAR(a.checker_at,'YYYY-MM-DD HH24:MI:SS'),'')        AS checker_at,
				COALESCE(a.checker_comment,'')                                     AS checker_comment,
				COALESCE(a.reason,'')                                                      AS reason,
				COALESCE(a.old_confirmation_status,'')                                     AS old_confirmation_status,
				COALESCE(a.old_actual_principal,0)                                         AS old_confirmed_principal_amount,
				COALESCE(a.old_confirmed_rate,0)                                           AS old_confirmed_interest_rate,
				COALESCE(TO_CHAR(a.old_actual_start_date,'YYYY-MM-DD'),'')                AS old_confirmed_value_date,
				COALESCE(TO_CHAR(a.old_actual_maturity_date,'YYYY-MM-DD'),'')             AS old_confirmed_maturity_date,
				COALESCE(a.old_variance_flag,false)                                        AS old_has_variance,
				COALESCE(a.old_variance_action,'')                                         AS old_variance_action,
				COALESCE(a.old_bank_fd_ref_no,'')                                          AS old_bank_fd_reference
			FROM investment.fd_audit_confirmation a
			WHERE a.confirmation_id = $1
			ORDER BY GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp),COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC`,
			confirmationID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer auditRows.Close()

		auditHistory := make([]map[string]interface{}, 0)
		auditFields := auditRows.FieldDescriptions()
		for auditRows.Next() {
			vals, _ := auditRows.Values()
			row := make(map[string]interface{}, len(auditFields))
			for i, f := range auditFields {
				if vals[i] == nil {
					row[string(f.Name)] = ""
				} else {
					row[string(f.Name)] = vals[i]
				}
			}
			auditHistory = append(auditHistory, row)
		}
		if err := auditRows.Err(); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Audit row error: "+err.Error())
			return
		}

		// ── Approval workflow ────────────────────────────────────────────────
		var approvalWorkflow interface{}
		{
			var instanceID string
			_ = pgxPool.QueryRow(ctx, `
				SELECT instance_id
				FROM uam.approval_instance
				WHERE record_id = $1 AND module_code = 'FIXED_DEPOSIT' AND is_deleted = false
				ORDER BY submitted_at DESC LIMIT 1`, confirmationID,
			).Scan(&instanceID)

			// Self-heal: if no instance but audit has PENDING row, create one now
			if instanceID == "" {
				var pendingActionType, submittedBy, entityID string
				var amount float64
				scanErr := pgxPool.QueryRow(ctx, `
					SELECT a.action_type,
					       COALESCE(a.requested_by,''),
					       COALESCE(b.entity_id,''),
					       COALESCE(c.actual_principal,0)
					FROM investment.fd_audit_confirmation a
					JOIN investment.fd_confirmation c ON c.confirmation_id = a.confirmation_id
					JOIN investment.fd_booking_request b ON b.booking_id = c.booking_id
					WHERE a.confirmation_id = $1
					  AND a.processing_status LIKE '%PENDING%'
					ORDER BY a.requested_at DESC LIMIT 1`, confirmationID,
				).Scan(&pendingActionType, &submittedBy, &entityID, &amount)

				if scanErr == nil && pendingActionType != "" {
					txType := map[string]string{
						"CREATE":           "FD_CONFIRMATION_CREATE",
						"VARIANCE_RESOLVE": "FD_CONFIRMATION_VARIANCE_RESOLVE",
					}[pendingActionType]
					if txType == "" {
						txType = "FD_CONFIRMATION_CREATE"
					}
					newInstID, instErr := approvalengine.CreateInstance(ctx, pgxPool, approvalengine.InstanceRequest{
						ModuleCode:       "FIXED_DEPOSIT",
						EntityCode:       entityID,
						TransactionType:  txType,
						RecordID:         confirmationID,
						RecordTable:      constants.QuerryConfirmation,
						AuditTable:       constants.QuerryAuditConfirmation,
						AuditIDColumn:    "confirmation_id",
						ActionType:       pendingActionType,
						Amount:           amount,
						SubmittedBy:      submittedBy,
						SubmittedByEmail: submittedBy,
					})
					if instErr != nil {
						api.LogError("[FDBooking] Self-heal CreateInstance(conf) for %s: %v", confirmationID, instErr)
					} else if newInstID != "" {
						instanceID = newInstID
						api.LogInfo("[FDBooking] Self-heal: created instance %s for confirmation %s", newInstID, confirmationID)
					}
				}
			}

			if instanceID != "" {
				richDetail, richErr := approvalengine.GetRichInstanceDetail(ctx, pgxPool, instanceID, viewerUserID)
				if richErr != nil {
					api.LogError("[FDBooking] GetRichInstanceDetail failed instance=%s confirmation=%s: %v", instanceID, confirmationID, richErr)
				} else {
					approvalWorkflow = richDetail
				}
			}
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"confirmation":      conf,
			"booking":           booking,
			"variance_log":      varianceLog,
			"audit_history":     auditHistory,
			"approval_workflow": approvalWorkflow,
		})
		api.LogInfo("[FDBooking] GetConfirmationDetail: confirmation_id=%s booking_id=%s", confirmationID, bookingID)
	}
}

// ─── DeleteConfirmation ──────────────────────────────────────────────────────

func DeleteConfirmation(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID          string   `json:"user_id"`
			ConfirmationIDs []string `json:"confirmation_ids"`
			Reason          string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.ConfirmationIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrConfirmationIDsRequired)
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
			msg, status := getUserFriendlyFDError(err, constants.ErrTransactionFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx) //nolint:errcheck

		// Allow deleting a confirmation until FD activation exists.
		// CONFIRMED may be deleted to send the booking back to SENT_TO_BANK.
		// Once fd_master exists, confirmation/booking become immutable from this flow.
		rows, err := tx.Query(ctx, `
			SELECT c.confirmation_id, b.entity_id, c.actual_principal
			FROM investment.fd_confirmation c
			JOIN investment.fd_booking_request b ON b.booking_id = c.booking_id
			WHERE c.confirmation_id = ANY($1::text[])
			  AND c.confirmation_status NOT IN ('VARIANCE_REJECTED','APPROVED')
			  AND NOT EXISTS (
				  SELECT 1 FROM investment.fd_master fm
				  WHERE (fm.confirmation_id = c.confirmation_id OR fm.booking_id = c.booking_id)
				    AND COALESCE(fm.is_deleted,false) = false
			  )
			  AND COALESCE(c.is_deleted,false) = false`,
			req.ConfirmationIDs,
		)
		if err != nil {
			msg, status := getUserFriendlyFDError(err, "Verify confirmations failed")
			api.RespondWithError(w, status, msg)
			return
		}
		defer rows.Close()

		type confMeta struct {
			id     string
			entity string
			amount float64
		}
		var validConfs []confMeta
		for rows.Next() {
			var cm confMeta
			if err := rows.Scan(&cm.id, &cm.entity, &cm.amount); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Scan error: "+err.Error())
				return
			}
			validConfs = append(validConfs, cm)
		}
		if err := rows.Err(); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrRowError+err.Error())
			return
		}
		rows.Close()

		if len(validConfs) == 0 {
			api.RespondWithPayload(w, false, "No eligible confirmations found (blocked if already activated, APPROVED, VARIANCE_REJECTED, or deleted)", nil)
			return
		}

		validIDs := make([]string, len(validConfs))
		for i, cm := range validConfs {
			validIDs[i] = cm.id
		}

		auditVals := make([]string, len(validIDs))
		auditArgs := make([]interface{}, 0, len(validIDs)*3)
		for i, id := range validIDs {
			auditVals[i] = fmt.Sprintf("($%d,'DELETE','PENDING_DELETE_APPROVAL',$%d,$%d,now())", i*3+1, i*3+2, i*3+3)
			auditArgs = append(auditArgs, id, userEmail, req.Reason)
		}
		auditQ := fmt.Sprintf(`
			INSERT INTO investment.fd_audit_confirmation
				(confirmation_id, action_type, processing_status, requested_by, reason, requested_at)
			VALUES %s`, strings.Join(auditVals, ","))
		if _, err = tx.Exec(ctx, auditQ, auditArgs...); err != nil {
			msg, status := getUserFriendlyFDError(err, constants.ErrAuditInsertFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		if err = tx.Commit(ctx); err != nil {
			logDBError(err, "DeleteConfirmation commit")
			msg, status := getUserFriendlyFDError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}

		// Fire engine goroutines after commit — cancel prior instances first (like update/delete booking)
		for _, cm := range validConfs {
			go func(cID, uID, uEmail, eID string, amount float64) {
				defer func() {
					if rec := recover(); rec != nil {
						api.LogError("[FDBooking] DeleteConfirmation engine goroutine panic for confirmation %s: %v", cID, rec)
					}
				}()
				bgCtx := context.Background()
				// Cancel any in-flight approval chain before submitting DELETE
				if err := approvalengine.CancelPendingInstances(bgCtx, pgxPool, "FIXED_DEPOSIT", cID, uEmail); err != nil {
					api.LogError("[FDBooking] CancelPendingInstances(DELETE) failed for confirmation %s: %v", cID, err)
					return
				}
				instID, err := approvalengine.CreateInstance(bgCtx, pgxPool, approvalengine.InstanceRequest{
					ModuleCode: "FIXED_DEPOSIT", EntityCode: eID,
					TransactionType: "FD_CONFIRMATION_DELETE", RecordID: cID,
					RecordTable: constants.QuerryConfirmation, AuditTable: constants.QuerryAuditConfirmation,
					AuditIDColumn: "confirmation_id", ActionType: "DELETE",
					Amount: 0, SubmittedBy: uID, SubmittedByEmail: uEmail,
				})
				if err != nil {
					api.LogError("[FDBooking] CreateInstance(DELETE) failed for confirmation %s: %v", cID, err)
					return
				}
				if instID != "" {
					if _, uerr := pgxPool.Exec(bgCtx,
						`UPDATE investment.fd_confirmation SET confirmation_status = 'APPROVAL_PENDING' WHERE confirmation_id = $1`,
						cID); uerr != nil {
						api.LogError("[FDBooking] Status→PENDING_DELETE_APPROVAL failed for confirmation %s: %v", cID, uerr)
					} else {
						api.LogInfo("[FDBooking] CreateInstance(DELETE) %s → confirmation %s PENDING_DELETE_APPROVAL", instID, cID)
					}
				}
			}(cm.id, req.UserID, userEmail, cm.entity, cm.amount)

			go func(cID, eID, uEmail string) {
				defer func() {
					if rec := recover(); rec != nil {
						api.LogError("[FDBooking] DeleteConfirmation notification goroutine panic for confirmation %s: %v", cID, rec)
					}
				}()
				notifcatalog.TriggerNotification(context.Background(), pgxPool, "/investment/fd/confirmation/delete", cID, map[string]interface{}{
					"entity_id":   eID,
					"record_id":   cID,
					"event":       "FD_CONFIRMATION_DELETE_SUBMITTED",
					"actor_email": uEmail,
				})
			}(cm.id, cm.entity, userEmail)
		}

		validSet := make(map[string]bool)
		for _, id := range validIDs {
			validSet[id] = true
		}
		results := make([]map[string]interface{}, 0, len(req.ConfirmationIDs))
		for _, id := range req.ConfirmationIDs {
			if validSet[id] {
				results = append(results, map[string]interface{}{constants.ValueSuccess: true, "confirmation_id": id})
			} else {
				results = append(results, map[string]interface{}{
					constants.ValueSuccess: false, "confirmation_id": id,
					constants.ValueError: "Not found, already activated, APPROVED/VARIANCE_REJECTED, or already deleted",
				})
			}
		}
		api.RespondWithPayload(w, len(validIDs) > 0, "", results)
		api.LogInfo("[FDBooking] Delete requests created: %d/%d confirmations", len(validIDs), len(req.ConfirmationIDs))
	}
}

// ─── local alias to avoid import cycle ───────────────────────────────────────

// _ prevents unused import lint if strings is used only in nullIfEmpty (defined in booking.go)
var _ = strings.TrimSpace

// ─── GetConfirmationPreflight ─────────────────────────────────────────────────
// GET /investment/fd/confirmation/preflight?user_id=X[&booking_id=Y][&entity_id=Z]
//
// Returns everything the UI needs before calling /confirmation/capture:
//   - bookings that are SENT_TO_BANK (ready to confirm) with full booked detail
//   - bank master + rate-card data for auto-populating defaults
//   - variance thresholds configured for the entity
//   - any existing partial confirmations for the booking

func GetConfirmationPreflight(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		bookingID := r.URL.Query().Get("booking_id")
		entityID := r.URL.Query().Get("entity_id")

		// ── 1. SENT_TO_BANK bookings waiting for confirmation ─────────────────
		bookingSQL := `
			SELECT
				br.booking_id,
				COALESCE(br.entity_id,'')                                    AS entity_id,
				COALESCE(br.entity_name,'')                                  AS entity_name,
				COALESCE(br.bank_id,'')                                      AS bank_id,
				COALESCE(br.bank_name,'')                                    AS bank_name,
				COALESCE(br.source_account_id,'')                            AS bank_account_id,
				COALESCE(br.source_account_number,'')                        AS bank_account_number,
				COALESCE(br.principal_amount,0)                              AS principal_amount,
				COALESCE(NULLIF(br.value_type,''),'Actual')                  AS value_type,
				COALESCE(br.interest_rate,0)                                 AS interest_rate,
				COALESCE(br.interest_type_code,'')                           AS interest_type,
				COALESCE(br.tenure_days,0)                                   AS tenure_days,
				COALESCE(br.tenure_months,0)                                 AS tenure_months,
				TO_CHAR(br.expected_start_date,'YYYY-MM-DD')                 AS expected_start_date,
				TO_CHAR(br.value_date,'YYYY-MM-DD')                          AS value_date,
				TO_CHAR(br.expected_maturity_date,'YYYY-MM-DD')              AS expected_maturity_date,
				COALESCE(br.tds_plan_id,'')                                  AS tds_plan_id,
				COALESCE(br.bank_config_id,'')                               AS bank_config_id,
				COALESCE(br.day_count_code,'')                               AS day_count_code,
				COALESCE(br.auto_renewal,false)                              AS auto_renewal,
				COALESCE(br.booking_status,'')                               AS booking_status,
				COALESCE(br.booking_remarks,'')                              AS booking_remarks,
				-- bank config / rate card defaults
				COALESCE(bc.minimum_amount,0)                                AS bank_min_amount,
				COALESCE(bc.maximum_amount,0)                                AS bank_max_amount,
				COALESCE(rc.interest_rate,0)                                 AS rate_card_rate,
				-- existing confirmation if any
				COALESCE(cf.confirmation_id,'')                              AS existing_confirmation_id,
				COALESCE(cf.confirmation_status,'')                          AS existing_confirmation_status,
				-- who sent to bank + when
				COALESCE(aud.requested_by,'')                                AS sent_to_bank_by,
				COALESCE(TO_CHAR(aud.requested_at,'YYYY-MM-DD HH24:MI'),'') AS sent_to_bank_at
			FROM investment.fd_booking_request br
			LEFT JOIN investment.fd_bank_config_master bc ON bc.config_id = br.bank_config_id
			LEFT JOIN investment.fd_bank_rate_card_master rc
				ON rc.bank_code = br.bank_id AND rc.is_active = true
				AND br.tenure_days BETWEEN rc.min_tenor_days AND rc.max_tenor_days
			LEFT JOIN investment.fd_confirmation cf
				ON cf.booking_id = br.booking_id AND COALESCE(cf.is_deleted,false) = false
			LEFT JOIN LATERAL (
				SELECT requested_by, requested_at
				FROM investment.fd_audit_booking_request
				WHERE booking_id = br.booking_id AND old_booking_status = 'APPROVED'
				ORDER BY requested_at DESC LIMIT 1
			) aud ON true
			WHERE br.booking_status = 'SENT_TO_BANK'
			  AND COALESCE(br.is_deleted,false) = false`

		bookingArgs := []interface{}{}
		argIdx := 1
		if bookingID != "" {
			bookingSQL += fmt.Sprintf(" AND br.booking_id = $%d", argIdx)
			bookingArgs = append(bookingArgs, bookingID)
			argIdx++
		}
		if entityID != "" {
			bookingSQL += fmt.Sprintf(" AND br.entity_id = $%d", argIdx)
			bookingArgs = append(bookingArgs, entityID)
			argIdx++
		}
		bookingSQL += " ORDER BY br.expected_maturity_date ASC"

		bookingRows, err := pgxPool.Query(ctx, bookingSQL, bookingArgs...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Booking query failed: "+err.Error())
			return
		}
		defer bookingRows.Close()
		bookings := make([]map[string]interface{}, 0)
		bFlds := bookingRows.FieldDescriptions()
		for bookingRows.Next() {
			vals, _ := bookingRows.Values()
			row := make(map[string]interface{}, len(bFlds))
			for i, f := range bFlds {
				if vals[i] == nil {
					row[string(f.Name)] = ""
				} else {
					row[string(f.Name)] = vals[i]
				}
			}
			bookings = append(bookings, row)
		}

		api.RespondWithPayload(w, true, "", bookings)
		api.LogInfo("[FDBooking] GetConfirmationPreflight: %d bookings pending confirmation (entity=%s)", len(bookings), entityID)
	}
}
