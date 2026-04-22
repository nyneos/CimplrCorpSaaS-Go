package fdBooking

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/approvalengine"
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/constants"
	notifcatalog "CimplrCorpSaas/api/notification/catalog"
	"CimplrCorpSaas/api/varianceengine"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

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
		var req struct {
			UserID                   string           `json:"user_id"`
			BookingID                string           `json:"booking_id"`
			ConfirmedPrincipalAmount float64          `json:"confirmed_principal_amount"`
			ConfirmedInterestRate    float64          `json:"confirmed_interest_rate"`
			ConfirmedTenorDays       int              `json:"confirmed_tenor_days"`
			ConfirmedTenorMonths     int              `json:"confirmed_tenor_months"`
			ConfirmedTenorYears      int              `json:"confirmed_tenor_years"`
			ConfirmedTenorType       string           `json:"confirmed_tenor_type"` // DAYS | MONTHS | YEARS
			ConfirmedValueDate       string           `json:"confirmed_value_date"`
			ConfirmedMaturityDate    string           `json:"confirmed_maturity_date"`
			BankFDReference          string           `json:"bank_fd_reference"`
			ReceiptDate              string           `json:"receipt_date"`
			ConfirmedInterestType    string           `json:"confirmed_interest_type"` // SIMPLE | COMPOUND | STEPPED
			ConfirmedFrequencyID     string           `json:"confirmed_frequency_id"`
			PenaltyID                string           `json:"penalty_id"`
			PayoutDates              *json.RawMessage `json:"payout_dates"`
			CompoundingDates         *json.RawMessage `json:"compounding_dates"`
			FirstPayoutDate          string           `json:"first_payout_date"`          // YYYY-MM-DD; actual first interest credit date
			FirstCapitalizationDate  string           `json:"first_capitalization_date"`  // YYYY-MM-DD; actual first capitalization date
			Notes                    string           `json:"notes"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.BookingID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "booking_id is required")
			return
		}
		if req.ConfirmedPrincipalAmount <= 0 || req.ConfirmedInterestRate <= 0 {
			api.RespondWithError(w, http.StatusBadRequest, "confirmed_principal_amount and confirmed_interest_rate must be positive")
			return
		}
		if req.ConfirmedValueDate == "" || req.ConfirmedMaturityDate == "" {
			api.RespondWithError(w, http.StatusBadRequest, "confirmed_value_date and confirmed_maturity_date are required")
			return
		}

		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Email
				break
			}
		}
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()

		// ── Load booked values ────────────────────────────────────────────────
		var bookedPrincipal, bookedRate float64
		var bookedTenorDays int
		var bookedValueDate, bookedMaturityDate, entityID, bookingStatus string
		err := pgxPool.QueryRow(ctx, `
			SELECT
				COALESCE(principal_amount,0),
				COALESCE(interest_rate,0),
				COALESCE(tenure_days,0),
				COALESCE(TO_CHAR(value_date,'YYYY-MM-DD'),''),
				COALESCE(TO_CHAR(expected_maturity_date,'YYYY-MM-DD'),''),
				COALESCE(entity_id,''),
				COALESCE(booking_status,'')
			FROM investment.fd_booking_request
			WHERE booking_id = $1 AND COALESCE(is_deleted,false) = false`,
			req.BookingID,
		).Scan(&bookedPrincipal, &bookedRate, &bookedTenorDays,
			&bookedValueDate, &bookedMaturityDate, &entityID, &bookingStatus)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Fetch booking failed: "+err.Error())
			return
		}

		// ── Run variance engine ───────────────────────────────────────────────
		runID := varianceengine.NewRunID()
		varRules := []varianceengine.Rule{
			{
				FieldName:     "interest_rate",
				VarianceType:  varianceengine.TypeRate,
				ExpectedValue: formatNumber(bookedRate),
				ActualValue:   formatNumber(req.ConfirmedInterestRate),
				Priority:      varianceengine.PriorityHigh,
				Tolerance:     0,
			},
			{
				FieldName:     "principal_amount",
				VarianceType:  varianceengine.TypeAmount,
				ExpectedValue: formatNumber(bookedPrincipal),
				ActualValue:   formatNumber(req.ConfirmedPrincipalAmount),
				Priority:      varianceengine.PriorityHigh,
				Tolerance:     0,
			},
			{
				FieldName:     "tenor_days",
				VarianceType:  varianceengine.TypeDays,
				ExpectedValue: fmt.Sprintf("%d", bookedTenorDays),
				ActualValue:   fmt.Sprintf("%d", req.ConfirmedTenorDays),
				Priority:      varianceengine.PriorityMedium,
				Tolerance:     0,
			},
			{
				FieldName:     "value_date",
				VarianceType:  varianceengine.TypeDate,
				ExpectedValue: bookedValueDate,
				ActualValue:   req.ConfirmedValueDate,
				Priority:      varianceengine.PriorityMedium,
				Tolerance:     0,
			},
			{
				FieldName:     "maturity_date",
				VarianceType:  varianceengine.TypeDate,
				ExpectedValue: bookedMaturityDate,
				ActualValue:   req.ConfirmedMaturityDate,
				Priority:      varianceengine.PriorityHigh,
				Tolerance:     0,
			},
		}
		varItems := varianceengine.Compare("FD_CONFIRMATION", req.BookingID, entityID, runID, varRules)

		hasVariance := false
		for _, v := range varItems {
			if v.HasVariance {
				hasVariance = true
				break
			}
		}

		// ── Variance found: return items, no DB ingestion ─────────────────────
		if hasVariance {
			out := make([]map[string]interface{}, 0, len(varItems))
			for _, v := range varItems {
				out = append(out, map[string]interface{}{
					"field_name":     v.FieldName,
					"variance_type":  v.VarianceType,
					"expected_value": v.ExpectedValue,
					"actual_value":   v.ActualValue,
					"variance_delta": v.VarianceDelta,
					"priority":       v.Priority,
					"has_variance":   v.HasVariance,
					"system_comment": v.SystemComment,
				})
			}
			api.RespondWithPayload(w, false, "Variance detected — confirmation not saved. Use /confirmation/variance-resolve to persist with variance or correct the values and re-capture.", map[string]interface{}{
				"booking_id":     req.BookingID,
				"has_variance":   true,
				"run_id":         runID,
				"variance_items": out,
			})
			return
		}

		// ── No variance: insert confirmation and confirm directly ─────────────
		bankFDRef := req.BankFDReference
		if bankFDRef == "" {
			bankFDRef = req.BookingID
		}
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
					variance_resolved_by      = $15,
					variance_resolved_at      = now(),
					confirmation_status       = 'PENDING_APPROVAL',
					penalty_id                = NULLIF($15,''),
					first_payout_date         = $18,
					first_capitalization_date = $19,
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
			); err != nil {
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
			).Scan(&confirmationID)
			if err != nil {
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
			msg, status := getUserFriendlyFDError(err, constants.ErrAuditInsertFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		if err = tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyFDError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}

		go func(cID, uID, uEmail, eID string, amount float64) {
			defer func() { recover() }() //nolint:errcheck
			bgCtx := context.Background()
			_ = approvalengine.CancelPendingInstances(bgCtx, pgxPool, "FIXED_DEPOSIT", cID, uEmail)
			_, _ = approvalengine.CreateInstance(bgCtx, pgxPool, approvalengine.InstanceRequest{
				ModuleCode: "FIXED_DEPOSIT", EntityCode: eID,
				TransactionType: "FD_CONFIRMATION_CREATE", RecordID: cID,
				RecordTable: constants.QuerryConfirmation, AuditTable: constants.QuerryAuditConfirmation,
				AuditIDColumn: "confirmation_id", ActionType: "CREATE",
				Amount: amount, SubmittedBy: uID, SubmittedByEmail: uEmail,
			})
		}(confirmationID, req.UserID, userEmail, entityID, req.ConfirmedPrincipalAmount)

		go func(cID, bID, eID, uEmail string, amount float64) {
			defer func() { recover() }() //nolint:errcheck
			notifcatalog.TriggerNotification(context.Background(), pgxPool, "/investment/fd/confirmation/capture", cID, map[string]interface{}{
				"entity_id": eID, "record_id": cID, "booking_id": bID,
				"event": "FD_CONFIRMATION_CAPTURED", "actor_email": uEmail, "amount": amount,
			})
		}(confirmationID, req.BookingID, entityID, userEmail, req.ConfirmedPrincipalAmount)

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"confirmation_id":     confirmationID,
			"booking_id":          req.BookingID,
			"confirmation_status": "PENDING_APPROVAL",
			"has_variance":        false,
			"captured_by":         userEmail,
		})
		api.LogInfo("[FDBooking] CaptureConfirmation: id=%s booking=%s no-variance", confirmationID, req.BookingID)
	}
}

// ─── VarianceResolve ─────────────────────────────────────────────────────────
// POST /investment/fd/confirmation/variance-resolve
//
// Called when capture returned variance. Two sub-cases:
//
//	(a) User corrected values: re-runs variance engine; if clean → resolve all OPEN variance_log rows.
//	(b) User submits same/different values: inserts or updates fd_confirmation with VARIANCE_PENDING.
//	    Subsequent calls with corrected values will auto-resolve cleared fields.
//
// Separate exception path: use /confirmation/variance-exception to accept as-is.
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
			ReceiptDate              string           `json:"receipt_date"`
			ConfirmedInterestType    string           `json:"confirmed_interest_type"`
			ConfirmedFrequencyID     string           `json:"confirmed_frequency_id"`
			PenaltyID                string           `json:"penalty_id"`
			PayoutDates              *json.RawMessage `json:"payout_dates"`
			CompoundingDates         *json.RawMessage `json:"compounding_dates"`
			FirstPayoutDate          string           `json:"first_payout_date"`         // YYYY-MM-DD; actual first interest credit date
			FirstCapitalizationDate  string           `json:"first_capitalization_date"` // YYYY-MM-DD; actual first capitalization date
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
			api.RespondWithError(w, http.StatusBadRequest, "confirmed_principal_amount and confirmed_interest_rate must be positive")
			return
		}
		if req.ConfirmedValueDate == "" || req.ConfirmedMaturityDate == "" {
			api.RespondWithError(w, http.StatusBadRequest, "confirmed_value_date and confirmed_maturity_date are required")
			return
		}

		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Email
				break
			}
		}
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
			api.RespondWithError(w, http.StatusBadRequest, "Cannot resolve booking_id")
			return
		}

		// ── APPROVED guard — once approved the FD is live and immutable ────────
		if confirmationID != "" {
			var currentConfStatus string
			_ = pgxPool.QueryRow(ctx,
				`SELECT confirmation_status FROM investment.fd_confirmation WHERE confirmation_id=$1`,
				confirmationID).Scan(&currentConfStatus)
			if currentConfStatus == "APPROVED" {
				api.RespondWithError(w, http.StatusBadRequest,
					"Cannot resolve variance on an APPROVED confirmation — the FD is live and immutable")
				return
			}
		}

		// ── Load booked values ────────────────────────────────────────────────
		var bookedPrincipal, bookedRate float64
		var bookedTenorDays int
		var bookedValueDate, bookedMaturityDate, entityID, bookingStatus string
		err := pgxPool.QueryRow(ctx, `
			SELECT
				COALESCE(principal_amount,0), COALESCE(interest_rate,0), COALESCE(tenure_days,0),
				COALESCE(TO_CHAR(value_date,'YYYY-MM-DD'),''),
				COALESCE(TO_CHAR(expected_maturity_date,'YYYY-MM-DD'),''),
				COALESCE(entity_id,''), COALESCE(booking_status,'')
			FROM investment.fd_booking_request
			WHERE booking_id = $1 AND COALESCE(is_deleted,false) = false`, bookingID,
		).Scan(&bookedPrincipal, &bookedRate, &bookedTenorDays,
			&bookedValueDate, &bookedMaturityDate, &entityID, &bookingStatus)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Fetch booking failed: "+err.Error())
			return
		}

		// ── Run variance engine ───────────────────────────────────────────────
		runID := varianceengine.NewRunID()
		varRules := []varianceengine.Rule{
			{FieldName: "interest_rate", VarianceType: varianceengine.TypeRate,
				ExpectedValue: formatNumber(bookedRate), ActualValue: formatNumber(req.ConfirmedInterestRate),
				Priority: varianceengine.PriorityHigh},
			{FieldName: "principal_amount", VarianceType: varianceengine.TypeAmount,
				ExpectedValue: formatNumber(bookedPrincipal), ActualValue: formatNumber(req.ConfirmedPrincipalAmount),
				Priority: varianceengine.PriorityHigh},
			{FieldName: "tenor_days", VarianceType: varianceengine.TypeDays,
				ExpectedValue: fmt.Sprintf("%d", bookedTenorDays), ActualValue: fmt.Sprintf("%d", req.ConfirmedTenorDays),
				Priority: varianceengine.PriorityMedium},
			{FieldName: "value_date", VarianceType: varianceengine.TypeDate,
				ExpectedValue: bookedValueDate, ActualValue: req.ConfirmedValueDate,
				Priority: varianceengine.PriorityMedium},
			{FieldName: "maturity_date", VarianceType: varianceengine.TypeDate,
				ExpectedValue: bookedMaturityDate, ActualValue: req.ConfirmedMaturityDate,
				Priority: varianceengine.PriorityHigh},
		}
		varItems := varianceengine.Compare("FD_CONFIRMATION", bookingID, entityID, runID, varRules)

		hasVariance := false
		for _, v := range varItems {
			if v.HasVariance {
				hasVariance = true
				break
			}
		}

		// ── Persist / update variance_log ─────────────────────────────────────
		if err := varianceengine.PersistVariances(ctx, pgxPool, varItems); err != nil {
			api.LogError("[FDBooking] VarianceResolve PersistVariances: %v", err)
		}

		// ── Auto-resolve fields that are now clean ────────────────────────────
		if err := varianceengine.AutoResolveCleared(ctx, pgxPool, bookingID, varItems, req.UserID, userEmail); err != nil {
			api.LogError("[FDBooking] VarianceResolve AutoResolveCleared: %v", err)
		}

		bankFDRef := req.BankFDReference
		if bankFDRef == "" {
			bankFDRef = bookingID
		}
		receivedDate := req.ReceiptDate
		if receivedDate == "" {
			receivedDate = time.Now().Format(constants.DateFormat)
		}
		tenorType := strings.ToUpper(strings.TrimSpace(req.ConfirmedTenorType))
		if tenorType == "" {
			tenorType = "DAYS"
		}

		confStatus := "VARIANCE_PENDING"
		if !hasVariance {
			confStatus = "PENDING_APPROVAL"
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
					variance_action              = CASE WHEN $15 THEN 'PENDING' ELSE NULL END,
					confirmation_status          = $17,
					penalty_id                   = NULLIF($18,''),
					first_payout_date            = $21,
					first_capitalization_date    = $22,
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
					$17, CASE WHEN $16 THEN 'PENDING' ELSE NULL END,
					$18,
					NULLIF($19,''),
					$21,
					$22,
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

		// Build variance items response
		varOut := make([]map[string]interface{}, 0, len(varItems))
		for _, v := range varItems {
			if v.HasVariance {
				varOut = append(varOut, map[string]interface{}{
					"field_name": v.FieldName, "variance_type": v.VarianceType,
					"expected_value": v.ExpectedValue, "actual_value": v.ActualValue,
					"variance_delta": v.VarianceDelta, "priority": v.Priority,
					"system_comment": v.SystemComment,
				})
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
// Allows modifying an existing CONFIRMED, VARIANCE_PENDING, or REJECTED confirmation.
// Merges req.Fields with the current confirmed values, re-runs the varianceengine
// against the booked baseline, and updates variance_flag / variance_details /
// confirmation_status accordingly. Returns variance items in the response.
//
// Blocked for APPROVED confirmations (FD is live — immutable).
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
			api.RespondWithError(w, http.StatusBadRequest, "confirmation_id is required")
			return
		}
		if len(req.Fields) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No fields to update")
			return
		}

		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Email
				break
			}
		}
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

		// ── Load current confirmation + linked booking (FOR UPDATE) ─────────────
		var bookingID, entityID, currentStatus, oldTenorType, oldPenaltyID string
		var oldPrincipal, oldRate float64
		var oldTenorDays, oldTenorMonths, oldTenorYears int
		var oldValueDate, oldMaturityDate string

		err = tx.QueryRow(ctx, `
			SELECT
				COALESCE(c.booking_id,''), COALESCE(b.entity_id,''),
				COALESCE(c.confirmation_status,''), COALESCE(c.tenor_type,''), COALESCE(c.penalty_id,''),
				COALESCE(c.actual_principal,0), COALESCE(c.confirmed_rate,0),
				COALESCE(c.tenor_days,0), COALESCE(c.tenor_months,0), COALESCE(c.tenor_years,0),
				COALESCE(TO_CHAR(c.actual_start_date,'YYYY-MM-DD'),''),
				COALESCE(TO_CHAR(c.actual_maturity_date,'YYYY-MM-DD'),'')
			FROM investment.fd_confirmation c
			LEFT JOIN investment.fd_booking_request b ON b.booking_id = c.booking_id
			WHERE c.confirmation_id = $1 AND COALESCE(c.is_deleted,false) = false
			FOR UPDATE`, req.ConfirmationID).
			Scan(&bookingID, &entityID, &currentStatus, &oldTenorType, &oldPenaltyID,
				&oldPrincipal, &oldRate,
				&oldTenorDays, &oldTenorMonths, &oldTenorYears,
				&oldValueDate, &oldMaturityDate)
		if err != nil {
			msg, status := getUserFriendlyFDError(err, "Fetch confirmation linkage failed")
			api.RespondWithError(w, status, msg)
			return
		}

		// ── APPROVED guard — FD is live, nothing may be mutated ───────────────
		if currentStatus == "APPROVED" {
			api.RespondWithError(w, http.StatusBadRequest,
				"Cannot edit an APPROVED confirmation — the FD is live and immutable")
			return
		}

		// ── Load booked baseline for variance comparison ──────────────────────
		var bookedPrincipal, bookedRate float64
		var bookedTenorDays int
		var bookedValueDate, bookedMaturityDate string
		if bookingID != "" {
			_ = pgxPool.QueryRow(ctx, `
				SELECT
					COALESCE(principal_amount,0), COALESCE(interest_rate,0), COALESCE(tenure_days,0),
					COALESCE(TO_CHAR(value_date,'YYYY-MM-DD'),''),
					COALESCE(TO_CHAR(expected_maturity_date,'YYYY-MM-DD'),'')
				FROM investment.fd_booking_request
				WHERE booking_id = $1 AND COALESCE(is_deleted,false) = false`, bookingID).
				Scan(&bookedPrincipal, &bookedRate, &bookedTenorDays, &bookedValueDate, &bookedMaturityDate)
		}

		// ── Merge req.Fields into effective new values ────────────────────────
		// Start from current confirmed values; override with whatever the caller sent.
		effPrincipal := oldPrincipal
		effRate := oldRate
		effTenorDays := oldTenorDays
		effValueDate := oldValueDate
		effMaturityDate := oldMaturityDate

		if v, ok := req.Fields["actual_principal"]; ok {
			if fv, ok2 := v.(float64); ok2 {
				effPrincipal = fv
			}
		}
		if v, ok := req.Fields["confirmed_rate"]; ok {
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
		if v, ok := req.Fields["actual_start_date"]; ok {
			if sv, ok2 := v.(string); ok2 && sv != "" {
				effValueDate = sv
			}
		}
		if v, ok := req.Fields["actual_maturity_date"]; ok {
			if sv, ok2 := v.(string); ok2 && sv != "" {
				effMaturityDate = sv
			}
		}

		// ── Run variance engine against booked baseline ───────────────────────
		runID := varianceengine.NewRunID()
		var varItems []varianceengine.VarianceItem
		if bookedPrincipal > 0 || bookedRate > 0 {
			varRules := []varianceengine.Rule{
				{
					FieldName:     "interest_rate",
					VarianceType:  varianceengine.TypeRate,
					ExpectedValue: formatNumber(bookedRate),
					ActualValue:   formatNumber(effRate),
					Priority:      varianceengine.PriorityHigh,
				},
				{
					FieldName:     "principal_amount",
					VarianceType:  varianceengine.TypeAmount,
					ExpectedValue: formatNumber(bookedPrincipal),
					ActualValue:   formatNumber(effPrincipal),
					Priority:      varianceengine.PriorityHigh,
				},
				{
					FieldName:     "tenor_days",
					VarianceType:  varianceengine.TypeDays,
					ExpectedValue: fmt.Sprintf("%d", bookedTenorDays),
					ActualValue:   fmt.Sprintf("%d", effTenorDays),
					Priority:      varianceengine.PriorityMedium,
				},
				{
					FieldName:     "value_date",
					VarianceType:  varianceengine.TypeDate,
					ExpectedValue: bookedValueDate,
					ActualValue:   effValueDate,
					Priority:      varianceengine.PriorityMedium,
				},
				{
					FieldName:     "maturity_date",
					VarianceType:  varianceengine.TypeDate,
					ExpectedValue: bookedMaturityDate,
					ActualValue:   effMaturityDate,
					Priority:      varianceengine.PriorityHigh,
				},
			}
			varItems = varianceengine.Compare("FD_CONFIRMATION", bookingID, entityID, runID, varRules)
			if persistErr := varianceengine.PersistVariances(ctx, pgxPool, varItems); persistErr != nil {
				api.LogError("[EditConfirmation] PersistVariances: %v", persistErr)
			}
			if autoErr := varianceengine.AutoResolveCleared(ctx, pgxPool, bookingID, varItems, req.UserID, userEmail); autoErr != nil {
				api.LogError("[EditConfirmation] AutoResolveCleared: %v", autoErr)
			}
		}

		hasVariance := false
		for _, v := range varItems {
			if v.HasVariance {
				hasVariance = true
				break
			}
		}

		// Derive confirmation_status from variance result
		newConfStatus := currentStatus
		if hasVariance {
			newConfStatus = "VARIANCE_PENDING"
		} else if currentStatus == "VARIANCE_PENDING" || currentStatus == "REJECTED" {
			// No more variance and it was previously dirty — promote to CONFIRMED
			newConfStatus = "CONFIRMED"
		}
		// If caller explicitly overrides confirmation_status, allow it (unless APPROVED)
		if sv, ok := req.Fields["confirmation_status"]; ok {
			if svStr, ok2 := sv.(string); ok2 && svStr != "APPROVED" {
				newConfStatus = svStr
			}
		}

		// Build variance_details JSON
		varDetailsJSON := buildVarianceDetailsJSON(varItems)

		// ── Build dynamic SET clause ──────────────────────────────────────────
		allowedFields := map[string]bool{
			"actual_principal": true, "confirmed_rate": true,
			"actual_start_date": true, "actual_maturity_date": true,
			"bank_fd_ref_no": true, "confirmation_received_date": true,
			"confirmed_interest_type_code": true, "confirmed_frequency_id": true,
			"tenor_type": true, "tenor_days": true, "tenor_months": true, "tenor_years": true,
			"payout_dates": true, "compounding_dates": true,
			"penalty_id": true,
		}

		setClauses := make([]string, 0)
		setArgs := make([]interface{}, 0)
		argIdx := 1
		updateAmount := effPrincipal

		for k, v := range req.Fields {
			if !allowedFields[k] {
				continue
			}
			if k == "actual_start_date" || k == "actual_maturity_date" || k == "confirmation_received_date" {
				v = coerceDateValue(v)
			}
			if k == "payout_dates" || k == "compounding_dates" {
				b, _ := json.Marshal(v)
				v = string(b)
				if string(b) == "null" {
					v = nil
				}
			}
			setClauses = append(setClauses, fmt.Sprintf("%s = $%d", k, argIdx))
			setArgs = append(setArgs, v)
			argIdx++
		}

		// Always stamp variance outcome and derived status from engine
		setClauses = append(setClauses,
			fmt.Sprintf("variance_flag = $%d", argIdx),
			fmt.Sprintf("variance_details = $%d", argIdx+1),
			fmt.Sprintf("variance_action = $%d", argIdx+2),
			fmt.Sprintf("confirmation_status = $%d", argIdx+3),
		)
		varAction := ""
		if hasVariance {
			varAction = "PENDING"
		}
		setArgs = append(setArgs, hasVariance, varDetailsJSON, varAction, newConfStatus)
		argIdx += 4

		if len(setClauses) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No valid fields to update")
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

		// Update booking status if variance now clean
		if !hasVariance && bookingID != "" && (currentStatus == "VARIANCE_PENDING" || currentStatus == "REJECTED") {
			_, _ = tx.Exec(ctx,
				`UPDATE investment.fd_booking_request SET booking_status='CONFIRMED' WHERE booking_id=$1`,
				bookingID)
		}

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

		// Build variance items response
		varOut := make([]map[string]interface{}, 0, len(varItems))
		for _, v := range varItems {
			if v.HasVariance {
				varOut = append(varOut, map[string]interface{}{
					"field_name":     v.FieldName,
					"variance_type":  v.VarianceType,
					"expected_value": v.ExpectedValue,
					"actual_value":   v.ActualValue,
					"variance_delta": v.VarianceDelta,
					"priority":       v.Priority,
					"system_comment": v.SystemComment,
				})
			}
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
		})
		api.LogInfo("[FDBooking] EditConfirmation: id=%s status=%s has_variance=%v", req.ConfirmationID, newConfStatus, hasVariance)
	}
}

// ─── VarianceException ────────────────────────────────────────────────────────
// POST /investment/fd/confirmation/variance-exception
//
// Accepts the variance as-is (marks all OPEN variance_log rows as EXCEPTION).
// Sets confirmation_status → VARIANCE_ACCEPTED so BulkApprove can proceed.
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
			api.RespondWithError(w, http.StatusBadRequest, "confirmation_id is required")
			return
		}

		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Email
				break
			}
		}
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()

		// Fetch confirmation state — LEFT JOIN so confirmations without a matching
		// booking row (e.g. directly-created VRN-* entries) still resolve correctly.
		// entity_id is taken from fd_confirmation first, falling back to the booking.
		var currentStatus, bookingID, entityID string
		var confPrincipal float64
		err := pgxPool.QueryRow(ctx, `
			SELECT c.confirmation_status,
			       COALESCE(c.booking_id,''),
			       COALESCE(c.entity_id, b.entity_id, ''),
			       COALESCE(c.actual_principal, c.confirmed_principal_amount, 0)
			FROM investment.fd_confirmation c
			LEFT JOIN investment.fd_booking_request b ON b.booking_id = c.booking_id
			WHERE c.confirmation_id = $1 AND COALESCE(c.is_deleted,false) = false`,
			req.ConfirmationID,
		).Scan(&currentStatus, &bookingID, &entityID, &confPrincipal)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Fetch confirmation failed: "+err.Error())
			return
		}
		if currentStatus == "APPROVED" {
			api.RespondWithError(w, http.StatusBadRequest,
				"Cannot accept variance exception on an APPROVED confirmation — the FD is live and immutable")
			return
		}
		if currentStatus != "VARIANCE_PENDING" {
			api.RespondWithError(w, http.StatusBadRequest,
				fmt.Sprintf("variance-exception only allowed on VARIANCE_PENDING confirmations (current: %s)", currentStatus))
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

		// Fetch open variance IDs for this booking
		vrows, err := pgxPool.Query(ctx,
			`SELECT variance_id FROM public.variance_log WHERE record_id=$1 AND status='OPEN'`, bookingID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Fetch variance_log failed: "+err.Error())
			return
		}
		var varIDs []string
		for vrows.Next() {
			var vid string
			if scanErr := vrows.Scan(&vid); scanErr == nil {
				varIDs = append(varIDs, vid)
			}
		}
		vrows.Close()

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

		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Email
				break
			}
		}
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
			if confStatus == "APPROVED" {
				errors = append(errors, cID+": already APPROVED — skipped")
				continue
			}

			// ── Variance guard: block approval if OPEN variance_log rows exist ────
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

			// Try engine path first.
			var instanceEyeID string
			engineErr := pgxPool.QueryRow(ctx, `
				SELECT ie.instance_eye_id
				FROM uam.approval_instance i
				JOIN uam.approval_instance_eye ie ON ie.instance_id = i.instance_id AND ie.status = 'ACTIVE'
				JOIN uam.approval_matrix_eye_member m ON m.eye_id = ie.matrix_eye_id
					AND m.member_type = 'APPROVER' AND m.is_active = true AND m.is_deleted = false
					AND m.assignment_type IN ('USER_ONLY','ROLE_USER') AND m.user_id = $2
				WHERE i.record_id = $1 AND i.module_code = 'FIXED_DEPOSIT' AND i.status = 'PENDING'
				ORDER BY ie.position ASC LIMIT 1`, cID, req.UserID,
			).Scan(&instanceEyeID)

			if engineErr == nil && instanceEyeID != "" {
				if err := approvalengine.RecordAction(ctx, pgxPool, approvalengine.ActionRequest{
					InstanceEyeID: instanceEyeID,
					ActorUserID:   req.UserID,
					ActorEmail:    userEmail,
					ActionType:    approvalengine.ActionApproved,
					Comment:       req.Comment,
				}); err != nil {
					api.LogError("[FDConfirmation] RecordAction approve failed for %s: %v", cID, err)
					errors = append(errors, cID+": "+err.Error())
					continue
				}
				engineActed++
				// If the instance is now fully APPROVED, flip statuses.
				var instStatus string
				_ = pgxPool.QueryRow(ctx, `SELECT i.status FROM uam.approval_instance i
					JOIN uam.approval_instance_eye ie ON ie.instance_id = i.instance_id
					WHERE ie.instance_eye_id = $1`, instanceEyeID).Scan(&instStatus)
				if instStatus == "APPROVED" {
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

				}
			} else {
				var anyInstance int
				_ = pgxPool.QueryRow(ctx, `SELECT COUNT(*) FROM uam.approval_instance
					WHERE record_id=$1 AND module_code='FIXED_DEPOSIT' AND status='PENDING'`, cID).Scan(&anyInstance)
				if anyInstance > 0 {
					errors = append(errors, cID+": not your turn in approval sequence")
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
				if err1 != nil || err2 != nil || err3 != nil || err4 != nil {
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

		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Email
				break
			}
		}
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
			if rejectConfStatus == "APPROVED" {
				errors = append(errors, cID+": already APPROVED — cannot reject")
				continue
			}

			var instanceEyeID string
			engineErr := pgxPool.QueryRow(ctx, `
				SELECT ie.instance_eye_id
				FROM uam.approval_instance i
				JOIN uam.approval_instance_eye ie ON ie.instance_id = i.instance_id AND ie.status = 'ACTIVE'
				JOIN uam.approval_matrix_eye_member m ON m.eye_id = ie.matrix_eye_id
					AND m.member_type = 'APPROVER' AND m.is_active = true AND m.is_deleted = false
					AND m.assignment_type IN ('USER_ONLY','ROLE_USER') AND m.user_id = $2
				WHERE i.record_id = $1 AND i.module_code = 'FIXED_DEPOSIT' AND i.status = 'PENDING'
				ORDER BY ie.position ASC LIMIT 1`, cID, req.UserID,
			).Scan(&instanceEyeID)

			if engineErr == nil && instanceEyeID != "" {
				if err := approvalengine.RecordAction(ctx, pgxPool, approvalengine.ActionRequest{
					InstanceEyeID: instanceEyeID,
					ActorUserID:   req.UserID,
					ActorEmail:    userEmail,
					ActionType:    approvalengine.ActionRejected,
					Comment:       req.Comment,
				}); err != nil {
					api.LogError("[FDConfirmation] RecordAction reject failed for %s: %v", cID, err)
					errors = append(errors, cID+": "+err.Error())
					continue
				}
				// finalizeRecord already set processing_status=REJECTED; flip confirmation status.
				if _, execErr := pgxPool.Exec(ctx,
					`UPDATE investment.fd_confirmation SET confirmation_status='REJECTED'
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
				var anyInstance int
				_ = pgxPool.QueryRow(ctx, `SELECT COUNT(*) FROM uam.approval_instance
					WHERE record_id=$1 AND module_code='FIXED_DEPOSIT' AND status='PENDING'`, cID).Scan(&anyInstance)
				if anyInstance > 0 {
					errors = append(errors, cID+": not your turn in approval sequence")
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
				_, err2 := tx.Exec(ctx, `UPDATE investment.fd_confirmation SET confirmation_status='REJECTED'
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
			) DESC`, bookingAccountExpr)

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
			msg, status := getUserFriendlyFDError(err, "Load confirmation schema failed")
			api.RespondWithError(w, status, msg)
			return
		}
		currencyExpr := "''::text"
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
		fpDateExpr := "''::text"
		if confCols["first_payout_date"] {
			fpDateExpr = "COALESCE(TO_CHAR(c.first_payout_date,'YYYY-MM-DD'),'')"
		}
		fcDateExpr := "''::text"
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
			api.RespondWithError(w, http.StatusBadRequest, "confirmation_id is required")
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
				COALESCE(TO_CHAR(c.created_at,'YYYY-MM-DD HH24:MI:SS'),'')            AS record_created_at
			FROM investment.fd_confirmation c
			LEFT JOIN investment.fd_booking_request b ON b.booking_id = c.booking_id
			WHERE c.confirmation_id = $1 AND COALESCE(c.is_deleted,false) = false`, bookingAccountExpr),
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

		// Parse variance_details from stored JSON string to object so callers get a proper object.
		if vd, ok := conf["variance_details"]; ok {
			if vdStr, ok := vd.(string); ok && vdStr != "" {
				var parsed interface{}
				if err := json.Unmarshal([]byte(vdStr), &parsed); err == nil {
					conf["variance_details"] = parsed
				}
			}
		}

		// ── Booking data — full booked values for the linked booking ──────────
		bookingID, _ := conf["booking_id"].(string)
		var booking map[string]interface{}
		if bookingID != "" {
			bookingAccountExprBk, _ := resolveFDBookingAccountExpression(ctx, pgxPool, "m")
			bkRows, bkErr := pgxPool.Query(ctx, fmt.Sprintf(`
				SELECT
					m.booking_id,
					COALESCE(m.entity_id,'')                                       AS entity_id,
					COALESCE(m.entity_name,'')                                     AS entity_name,
					COALESCE(m.bank_id,'')                                         AS bank_id,
					COALESCE(m.bank_name,'')                                       AS bank_name,
					%s                                                             AS bank_account_id,
					COALESCE(m.source_account_number,'')                           AS bank_account_number,
					COALESCE(m.bank_config_id,'')                                  AS bank_config_id,
					COALESCE(m.principal_amount,0)                                 AS principal_amount,
					COALESCE(m.interest_rate,0)                                    AS interest_rate,
					COALESCE(m.interest_type_code,'')                              AS interest_type,
					COALESCE(m.interest_type_id,'')                                AS interest_type_id,
					COALESCE(m.tenure_days,0)                                      AS tenor_days,
					COALESCE(m.tenure_months,0)                                    AS tenor_months,
					COALESCE(m.tenure_years,0)                                     AS tenor_years,
					COALESCE(m.tenor_type,'')                                      AS tenor_type,
					COALESCE(TO_CHAR(m.expected_start_date,'YYYY-MM-DD'),'')       AS expected_start_date,
					COALESCE(TO_CHAR(m.value_date,'YYYY-MM-DD'),'')                AS value_date,
					COALESCE(TO_CHAR(m.expected_maturity_date,'YYYY-MM-DD'),'')    AS maturity_date,
					COALESCE(m.frequency_id,'')                                    AS frequency_id,
					COALESCE(m.day_count_code,'')                                  AS day_count_code,
					COALESCE(m.tds_plan_id,'')                                     AS tds_plan_id,
					COALESCE(m.product_code,'')                                    AS product_code,
					COALESCE(m.auto_renewal,false)                                 AS auto_renewal,
					COALESCE(m.booking_remarks,'')                                 AS booking_remarks,
					COALESCE(m.booking_status,'')                                  AS booking_status,
					COALESCE(TO_CHAR(m.created_at,'YYYY-MM-DD HH24:MI:SS'),'')    AS record_created_at,
					COALESCE(m.created_by,'')                                      AS record_created_by
				FROM investment.fd_booking_request m
				WHERE m.booking_id = $1 AND COALESCE(m.is_deleted,false) = false`, bookingAccountExprBk),
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

		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Email
				break
			}
		}
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

		// CONFIRMED and APPROVED confirmations cannot be deleted (system depends on them).
		// APPROVED = FD is fully live — absolutely immutable.
		// Allow: PENDING_CONFIRMATION, VARIANCE_DETECTED, VARIANCE_PENDING, REJECTED, APPROVAL_PENDING.
		rows, err := tx.Query(ctx, `
			SELECT c.confirmation_id, b.entity_id, c.actual_principal
			FROM investment.fd_confirmation c
			JOIN investment.fd_booking_request b ON b.booking_id = c.booking_id
			WHERE c.confirmation_id = ANY($1::text[])
			  AND c.confirmation_status NOT IN ('CONFIRMED','VARIANCE_REJECTED','APPROVED')
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
			api.RespondWithPayload(w, false, "No eligible confirmations found (CONFIRMED confirmations cannot be deleted)", nil)
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
					constants.ValueError: "Not found or already CONFIRMED/VARIANCE_REJECTED (cannot be deleted)",
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
