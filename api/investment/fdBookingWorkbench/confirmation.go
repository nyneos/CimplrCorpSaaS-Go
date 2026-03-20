package fdBooking

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/approvalengine"
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/constants"
	notifcatalog "CimplrCorpSaas/api/notification/catalog"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ─── CaptureConfirmation ──────────────────────────────────────────────────────

func CaptureConfirmation(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID                   string   `json:"user_id"`
			BookingID                string   `json:"booking_id"`
			ConfirmedPrincipalAmount float64  `json:"confirmed_principal_amount"`
			ConfirmedInterestRate    float64  `json:"confirmed_interest_rate"`
			ConfirmedTenorDays       int      `json:"confirmed_tenor_days"`
			ConfirmedValueDate       string   `json:"confirmed_value_date"`
			ConfirmedMaturityDate    string   `json:"confirmed_maturity_date"`
			ConfirmedMaturityAmount  *float64 `json:"confirmed_maturity_amount"`
			BankFDReference          string   `json:"bank_fd_reference"`
			ReceiptDate              string   `json:"receipt_date"`
			Notes                    string   `json:"notes"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.BookingID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "booking_id is required")
			return
		}
		if req.ConfirmedPrincipalAmount <= 0 || req.ConfirmedInterestRate <= 0 || req.ConfirmedTenorDays <= 0 {
			api.RespondWithError(w, http.StatusBadRequest, "confirmed_principal_amount, confirmed_interest_rate and confirmed_tenor_days must be positive")
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

		// Fetch booked values before transaction
		var bookedPrincipal, bookedRate float64
		var bookedTenor int
		var bookedValueDate, bookedMaturityDate, entityID string
		var bookingStatus string
		err := pgxPool.QueryRow(ctx, `
			SELECT
				COALESCE(principal_amount,0), COALESCE(interest_rate,0), COALESCE(tenure_days,0),
				COALESCE(TO_CHAR(value_date,'YYYY-MM-DD'),''), COALESCE(TO_CHAR(expected_maturity_date,'YYYY-MM-DD'),''),
				COALESCE(entity_id,''), COALESCE(booking_status,'')
			FROM investment.fd_booking_request
			WHERE booking_id = $1 AND COALESCE(is_deleted,false) = false`,
			req.BookingID,
		).Scan(&bookedPrincipal, &bookedRate, &bookedTenor, &bookedValueDate, &bookedMaturityDate, &entityID, &bookingStatus)
		if err != nil {
			api.LogError("[FDBooking] CaptureConfirmation fetch booking error: booking_id=%s err=%v", req.BookingID, err)
			api.RespondWithError(w, http.StatusInternalServerError, "Fetch booking failed: "+err.Error())
			return
		}
		if bookingStatus != "SENT_TO_BANK" && bookingStatus != "APPROVED" {
			api.RespondWithError(w, http.StatusBadRequest,
				fmt.Sprintf("Cannot capture confirmation for booking in status '%s'. Booking must be APPROVED or SENT_TO_BANK.", bookingStatus))
			return
		}

		// Calculate variance
		variance := calculateVariance(
			bookedRate, req.ConfirmedInterestRate,
			bookedPrincipal, req.ConfirmedPrincipalAmount,
			bookedMaturityDate, req.ConfirmedMaturityDate,
		)

		// Determine confirmation status
		confirmationStatus := "CONFIRMED"
		if variance.HasVariance {
			confirmationStatus = "VARIANCE_REVIEW"
		}

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			msg, status := getUserFriendlyFDError(err, "Transaction begin failed")
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx) //nolint:errcheck

		// Check for existing confirmation for this booking
		var existingCount int
		if err = tx.QueryRow(ctx, `
			SELECT COUNT(*) FROM investment.fd_confirmation
			WHERE booking_id = $1 AND COALESCE(is_deleted,false) = false`,
			req.BookingID,
		).Scan(&existingCount); err != nil {
			msg, status := getUserFriendlyFDError(err, "Check existing confirmation failed")
			api.RespondWithError(w, status, msg)
			return
		}
		if existingCount > 0 {
			api.RespondWithError(w, http.StatusBadRequest, "A confirmation already exists for this booking.")
			return
		}

		var confirmationID string
		varianceDetailsJSON := fmt.Sprintf(
			`{"rate":{"booked":%g,"confirmed":%g,"diff":%g},"amount":{"booked":%g,"confirmed":%g,"diff":%g},"maturity_days_diff":%d}`,
			bookedRate, req.ConfirmedInterestRate, variance.RateVariance,
			bookedPrincipal, req.ConfirmedPrincipalAmount, variance.AmountVariance,
			variance.MaturityDateVariance,
		)
		// bank_fd_ref_no and bank_reference_number are NOT NULL — default to booking_id if empty
		bankFDRef := req.BankFDReference
		if bankFDRef == "" {
			bankFDRef = req.BookingID
		}
		// confirmation_received_date is NOT NULL — default to today if empty
		receivedDate := req.ReceiptDate
		if receivedDate == "" {
			receivedDate = time.Now().Format("2006-01-02")
		}

		err = tx.QueryRow(ctx, `
			INSERT INTO investment.fd_confirmation (
				booking_id,
				actual_principal, confirmed_rate,
				actual_start_date, actual_maturity_date,
				bank_fd_ref_no, bank_reference_number,
				confirmation_received_date,
				variance_flag, variance_threshold_breached,
				variance_details,
				confirmation_status,
				created_by
			) VALUES (
				$1,
				$2,$3,
				$4,$5,
				$6,$7,
				$8,
				$9,$10,
				$11,
				$12,
				$13
			) RETURNING confirmation_id`,
			req.BookingID,
			req.ConfirmedPrincipalAmount, req.ConfirmedInterestRate,
			coerceDateValue(req.ConfirmedValueDate), coerceDateValue(req.ConfirmedMaturityDate),
			bankFDRef, bankFDRef,
			coerceDateValue(receivedDate),
			variance.HasVariance, variance.IsThresholdBreached,
			varianceDetailsJSON,
			confirmationStatus,
			userEmail,
		).Scan(&confirmationID)
		if err != nil {
			msg, status := getUserFriendlyFDError(err, "Insert confirmation failed")
			api.RespondWithError(w, status, msg)
			return
		}

		// Audit record — snapshot the booked values as old_* so the trail shows booked vs confirmed
		if _, err = tx.Exec(ctx, `
			INSERT INTO investment.fd_audit_confirmation (
				confirmation_id, action_type, processing_status, requested_by, requested_at,
				old_actual_principal, old_confirmed_rate,
				old_actual_start_date, old_actual_maturity_date, old_confirmation_status
			) VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now(),$3,$4,$5,$6,$7)`,
			confirmationID, userEmail,
			bookedPrincipal, bookedRate,
			coerceDateValue(bookedValueDate), coerceDateValue(bookedMaturityDate), bookingStatus,
		); err != nil {
			msg, status := getUserFriendlyFDError(err, constants.ErrAuditInsertFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		// If no variance, also update booking status to CONFIRMED
		if !variance.HasVariance {
			if _, err = tx.Exec(ctx, `
				UPDATE investment.fd_booking_request
				SET booking_status = 'CONFIRMED'
				WHERE booking_id = $1`,
				req.BookingID,
			); err != nil {
				msg, status := getUserFriendlyFDError(err, "Booking status update failed")
				api.RespondWithError(w, status, msg)
				return
			}
		}

		if err = tx.Commit(ctx); err != nil {
			logDBError(err, "CaptureConfirmation commit")
			msg, status := getUserFriendlyFDError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}

		go func(cID, uID, uEmail, eID string, amount float64) {
			bgCtx := context.Background()
			// Cancel any prior pending instance — re-capture resets the full approval chain.
			if err := approvalengine.CancelPendingInstances(bgCtx, pgxPool, "FIXED_DEPOSIT", cID, uEmail); err != nil {
				api.LogError("[FDBooking] CancelPendingInstances failed for confirmation %s: %v", cID, err)
			}
			if _, err := approvalengine.CreateInstance(bgCtx, pgxPool, approvalengine.InstanceRequest{
				ModuleCode:       "FIXED_DEPOSIT",
				EntityCode:       eID,
				TransactionType:  "FD_CONFIRMATION_CREATE",
				RecordID:         cID,
				RecordTable:      "investment.fd_confirmation",
				AuditTable:       "investment.fd_audit_confirmation",
				AuditIDColumn:    "confirmation_id",
				ActionType:       "CREATE",
				Amount:           amount,
				SubmittedBy:      uID,
				SubmittedByEmail: uEmail,
			}); err != nil {
				api.LogError("[FDBooking] CreateInstance(CONF_CREATE) failed for confirmation %s: %v", cID, err)
			} else {
				api.LogInfo("[FDBooking] CreateInstance(CONF_CREATE) fired for confirmation %s", cID)
			}
		}(confirmationID, req.UserID, userEmail, entityID, req.ConfirmedPrincipalAmount)

		go func(cID, bID, eID, uEmail string, amount float64, hasVar bool) {
			notifcatalog.TriggerNotification(context.Background(), pgxPool, "/investment/fd/confirmation/capture", cID, map[string]interface{}{
				"entity_id":    eID,
				"record_id":    cID,
				"booking_id":   bID,
				"event":        "FD_CONFIRMATION_CAPTURED",
				"actor_email":  uEmail,
				"amount":       amount,
				"has_variance": hasVar,
			})
		}(confirmationID, req.BookingID, entityID, userEmail, req.ConfirmedPrincipalAmount, variance.HasVariance)

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"confirmation_id":      confirmationID,
			"booking_id":           req.BookingID,
			"has_variance":         variance.HasVariance,
			"threshold_breached":   variance.IsThresholdBreached,
			"confirmation_status":  confirmationStatus,
			"rate_variance":        variance.RateVariance,
			"amount_variance":      variance.AmountVariance,
			"maturity_date_variance_days": variance.MaturityDateVariance,
			"requested":            userEmail,
		})
		api.LogInfo("[FDBooking] Confirmation captured: id=%s booking=%s variance=%v", confirmationID, req.BookingID, variance.HasVariance)
	}
}

// ─── ResolveVariance ─────────────────────────────────────────────────────────

func ResolveVariance(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID         string `json:"user_id"`
			ConfirmationID string `json:"confirmation_id"`
			VarianceAction string `json:"variance_action"` // ACCEPTED or REJECTED
			Comment        string `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.ConfirmationID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "confirmation_id is required")
			return
		}
		if req.VarianceAction != "ACCEPTED" && req.VarianceAction != "REJECTED" {
			api.RespondWithError(w, http.StatusBadRequest, "variance_action must be ACCEPTED or REJECTED")
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
			msg, status := getUserFriendlyFDError(err, "Transaction begin failed")
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx) //nolint:errcheck

		// Fetch confirmation FOR UPDATE and snapshot
		var currentStatus, bookingID, entityID string
		var thresholdBreached bool
		var oldConfPrincipal, oldConfRate float64
		err = tx.QueryRow(ctx, `
			SELECT
				c.confirmation_status, c.booking_id,
				COALESCE(b.entity_id,''),
				COALESCE(c.variance_threshold_breached,false),
				COALESCE(c.actual_principal,0),
				COALESCE(c.confirmed_rate,0)
			FROM investment.fd_confirmation c
			JOIN investment.fd_booking_request b ON b.booking_id = c.booking_id
			WHERE c.confirmation_id = $1 AND COALESCE(c.is_deleted,false) = false
			FOR UPDATE OF c`,
			req.ConfirmationID,
		).Scan(&currentStatus, &bookingID, &entityID, &thresholdBreached,
			&oldConfPrincipal, &oldConfRate)
		if err != nil {
			msg, status := getUserFriendlyFDError(err, "Fetch confirmation failed")
			api.RespondWithError(w, status, msg)
			return
		}
		if currentStatus != "VARIANCE_REVIEW" {
			api.RespondWithError(w, http.StatusBadRequest,
				fmt.Sprintf("Cannot resolve variance for confirmation in status '%s'. Must be VARIANCE_REVIEW.", currentStatus))
			return
		}

		newConfStatus := "CONFIRMED"
		if req.VarianceAction == "REJECTED" {
			newConfStatus = "VARIANCE_REJECTED"
		}

		// Update confirmation
		if _, err = tx.Exec(ctx, `
			UPDATE investment.fd_confirmation
			SET variance_action = $1, confirmation_status = $2, variance_resolved_by = $3, variance_resolved_at = now()
			WHERE confirmation_id = $4`,
			req.VarianceAction, newConfStatus, userEmail, req.ConfirmationID,
		); err != nil {
			msg, status := getUserFriendlyFDError(err, "Update confirmation failed")
			api.RespondWithError(w, status, msg)
			return
		}

		// Update booking status based on variance resolution
		newBookingStatus := "CONFIRMED"
		if req.VarianceAction == "REJECTED" {
			newBookingStatus = "SENT_TO_BANK"
		}
		if _, err = tx.Exec(ctx, `
			UPDATE investment.fd_booking_request SET booking_status = $1 WHERE booking_id = $2`,
			newBookingStatus, bookingID,
		); err != nil {
			msg, status := getUserFriendlyFDError(err, "Booking status update failed")
			api.RespondWithError(w, status, msg)
			return
		}

		// Audit — snapshot the confirmation state before variance resolution
		if _, err = tx.Exec(ctx, `
			INSERT INTO investment.fd_audit_confirmation (
				confirmation_id, action_type, processing_status, requested_by, requested_at, reason,
				old_confirmation_status,
				old_actual_principal, old_confirmed_rate,
				old_variance_flag, old_variance_action
			) VALUES ($1,'EDIT','PENDING_APPROVAL',$2,now(),$3,$4,$5,$6,$7,$8)`,
			req.ConfirmationID, userEmail, req.Comment, currentStatus,
			oldConfPrincipal, oldConfRate,
			thresholdBreached, nullIfEmpty(req.VarianceAction),
		); err != nil {
			msg, status := getUserFriendlyFDError(err, constants.ErrAuditInsertFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		if err = tx.Commit(ctx); err != nil {
			logDBError(err, "ResolveVariance commit")
			msg, status := getUserFriendlyFDError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}

		// Fire engine — always cancel the prior instance and create a fresh one.
		// Even a non-threshold resolve may need approval tracking.
		go func(cID, uID, uEmail, eID string, amount float64, breached bool) {
			bgCtx := context.Background()
			// Cancel the CONF_CREATE instance that was pending — resolve resets the chain.
			if err := approvalengine.CancelPendingInstances(bgCtx, pgxPool, "FIXED_DEPOSIT", cID, uEmail); err != nil {
				api.LogError("[FDBooking] CancelPendingInstances failed for confirmation %s: %v", cID, err)
			}
			if breached {
				if _, err := approvalengine.CreateInstance(bgCtx, pgxPool, approvalengine.InstanceRequest{
					ModuleCode:       "FIXED_DEPOSIT",
					EntityCode:       eID,
					TransactionType:  "FD_CONFIRMATION_VARIANCE_RESOLVE",
					RecordID:         cID,
					RecordTable:      "investment.fd_confirmation",
					AuditTable:       "investment.fd_audit_confirmation",
					AuditIDColumn:    "confirmation_id",
					ActionType:       "EDIT",
					Amount:           amount,
					SubmittedBy:      uID,
					SubmittedByEmail: uEmail,
				}); err != nil {
					api.LogError("[FDBooking] CreateInstance(VARIANCE_RESOLVE) failed for confirmation %s: %v", cID, err)
				} else {
					api.LogInfo("[FDBooking] CreateInstance(VARIANCE_RESOLVE) fired for confirmation %s", cID)
				}
			} else {
				// No threshold breach — re-submit the original CONF_CREATE flow.
				if _, err := approvalengine.CreateInstance(bgCtx, pgxPool, approvalengine.InstanceRequest{
					ModuleCode:       "FIXED_DEPOSIT",
					EntityCode:       eID,
					TransactionType:  "FD_CONFIRMATION_CREATE",
					RecordID:         cID,
					RecordTable:      "investment.fd_confirmation",
					AuditTable:       "investment.fd_audit_confirmation",
					AuditIDColumn:    "confirmation_id",
					ActionType:       "EDIT",
					Amount:           amount,
					SubmittedBy:      uID,
					SubmittedByEmail: uEmail,
				}); err != nil {
					api.LogError("[FDBooking] CreateInstance(CONF_RESOLVE_NO_BREACH) failed for confirmation %s: %v", cID, err)
				} else {
					api.LogInfo("[FDBooking] CreateInstance(CONF_RESOLVE_NO_BREACH) fired for confirmation %s", cID)
				}
			}
		}(req.ConfirmationID, req.UserID, userEmail, entityID, oldConfPrincipal, thresholdBreached)

		go func(cID, eID, uEmail, action string, breached bool) {
			notifcatalog.TriggerNotification(context.Background(), pgxPool, "/investment/fd/confirmation/resolve-variance", cID, map[string]interface{}{
				"entity_id":         eID,
				"record_id":         cID,
				"event":             "FD_CONFIRMATION_VARIANCE_RESOLVED",
				"actor_email":       uEmail,
				"variance_action":   action,
				"threshold_breached": breached,
			})
		}(req.ConfirmationID, entityID, userEmail, req.VarianceAction, thresholdBreached)

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"confirmation_id":     req.ConfirmationID,
			"variance_action":     req.VarianceAction,
			"new_status":          newConfStatus,
			"booking_status":      newBookingStatus,
			"threshold_breached":  thresholdBreached,
			"resolved_by":         userEmail,
		})
		api.LogInfo("[FDBooking] ResolveVariance: confirmation=%s action=%s by=%s", req.ConfirmationID, req.VarianceAction, userEmail)
	}
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
			api.RespondWithError(w, http.StatusBadRequest, "confirmation_ids are required")
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
				// If the instance is now fully APPROVED, flip statuses.
				var instStatus string
				_ = pgxPool.QueryRow(ctx, `SELECT i.status FROM uam.approval_instance i
					JOIN uam.approval_instance_eye ie ON ie.instance_id = i.instance_id
					WHERE ie.instance_eye_id = $1`, instanceEyeID).Scan(&instStatus)
				if instStatus == "APPROVED" {
					_, _ = pgxPool.Exec(ctx, `UPDATE investment.fd_confirmation
						SET confirmation_status = 'CONFIRMED'
						WHERE confirmation_id = $1
						  AND confirmation_status NOT IN ('VARIANCE_REJECTED','CONFIRMED')`, cID)
					_, _ = pgxPool.Exec(ctx, `UPDATE investment.fd_booking_request SET booking_status='CONFIRMED'
						WHERE booking_id IN (SELECT booking_id FROM investment.fd_confirmation WHERE confirmation_id=$1)`, cID)
					_, _ = pgxPool.Exec(ctx, `UPDATE investment.fd_confirmation SET is_deleted=true
						WHERE confirmation_id IN (
							SELECT DISTINCT a.confirmation_id FROM investment.fd_audit_confirmation a
							WHERE a.confirmation_id=$1 AND a.action_type='DELETE' AND a.processing_status='APPROVED'
						)`, cID)
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
				// No matrix — direct stamp.
				tx, err := pgxPool.Begin(ctx)
				if err != nil {
					errors = append(errors, cID+": tx begin failed")
					continue
				}
				_, err1 := tx.Exec(ctx, `UPDATE investment.fd_audit_confirmation a
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
				if cerr := tx.Commit(ctx); cerr != nil {
					errors = append(errors, cID+": commit failed")
					continue
				}
				directActed++
			}
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"engine_acted": engineActed, "direct_acted": directActed,
			"errors": errors, "checker": userEmail,
		})
		for _, cID := range req.ConfirmationIDs {
			go func(id, uEmail string) {
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
			api.RespondWithError(w, http.StatusBadRequest, "confirmation_ids are required")
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
				_, _ = pgxPool.Exec(ctx, `UPDATE investment.fd_confirmation SET confirmation_status='REJECTED'
					WHERE confirmation_id=$1 AND confirmation_status NOT IN ('CONFIRMED','VARIANCE_REJECTED')`, cID)
				_, _ = pgxPool.Exec(ctx, `UPDATE investment.fd_booking_request SET booking_status='SENT_TO_BANK'
					WHERE booking_id IN (SELECT booking_id FROM investment.fd_confirmation WHERE confirmation_id=$1)`, cID)
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
				_, err1 := tx.Exec(ctx, `UPDATE investment.fd_audit_confirmation a
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
				if cerr := tx.Commit(ctx); cerr != nil {
					errors = append(errors, cID+": commit failed")
					continue
				}
				directActed++
			}
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"engine_acted": engineActed, "direct_acted": directActed,
			"errors": errors, "checker": userEmail,
		})
		for _, cID := range req.ConfirmationIDs {
			go func(id, uEmail string) {
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
			msg, status := getUserFriendlyFDError(err, "Load booking schema failed")
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
					a.old_bank_fd_ref_no
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
				COALESCE(c.bank_reference_number,'')                                   AS bank_reference_number,
				COALESCE(TO_CHAR(c.confirmation_received_date,'YYYY-MM-DD'),'')       AS receipt_date,
				COALESCE(c.variance_flag,false)                                        AS has_variance,
				COALESCE(c.variance_threshold_breached,false)                          AS variance_threshold_breached,
				COALESCE(c.variance_action,'')                                         AS variance_action,
				COALESCE(c.variance_remarks,'')                                        AS variance_remarks,
				COALESCE(c.variance_resolved_by,'')                                    AS variance_resolved_by,
				COALESCE(TO_CHAR(c.variance_resolved_at,'YYYY-MM-DD HH24:MI:SS'),'') AS variance_resolved_at,
				COALESCE(c.confirmation_status,'')                                     AS confirmation_status,
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
			api.RespondWithError(w, http.StatusInternalServerError, "Row error: "+err.Error())
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
			msg, status := getUserFriendlyFDError(err, "Load booking schema failed")
			api.RespondWithError(w, status, msg)
			return
		}

		// Dynamically resolve which currency column exists to avoid parse-time errors.
		bookingCols, err := loadFDTableColumns(ctx, pgxPool, "investment", "fd_booking_request")
		if err != nil {
			msg, status := getUserFriendlyFDError(err, "Load booking schema failed")
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
				COALESCE(c.bank_reference_number,'') AS bank_reference_number,
				COALESCE(TO_CHAR(c.confirmation_received_date,'YYYY-MM-DD'),'') AS receipt_date,
				%s AS currency,
				COALESCE(c.confirmation_status,'') AS confirmation_status,
				EXISTS (
					SELECT 1 FROM investment.fd_master fm
					WHERE fm.confirmation_id = c.confirmation_id
					  AND COALESCE(fm.is_deleted,false) = false
				) AS fd_activated
			FROM investment.fd_confirmation c
			JOIN investment.fd_booking_request b ON b.booking_id = c.booking_id
			WHERE c.confirmation_status = 'CONFIRMED'
			  AND COALESCE(c.is_deleted,false) = false`, bookingAccountExpr, currencyExpr)

		var args []interface{}
		if entityID != "" {
			baseQ += ` AND b.entity_id = $1`
			args = append(args, entityID)
		}
		baseQ += ` ORDER BY c.actual_maturity_date ASC`

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
			api.RespondWithError(w, http.StatusInternalServerError, "Row error: "+err.Error())
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
			msg, status := getUserFriendlyFDError(err, "Load booking schema failed")
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
				COALESCE(c.bank_reference_number,'')                                   AS bank_reference_number,
				COALESCE(TO_CHAR(c.confirmation_received_date,'YYYY-MM-DD'),'')       AS receipt_date,
				COALESCE(c.variance_flag,false)                                        AS has_variance,
				COALESCE(c.variance_threshold_breached,false)                          AS variance_threshold_breached,
				COALESCE(c.variance_details::text,'')                                  AS variance_details,
				COALESCE(c.variance_action,'')                                         AS variance_action,
				COALESCE(c.variance_remarks,'')                                        AS variance_remarks,
				COALESCE(c.variance_resolved_by,'')                                    AS variance_resolved_by,
				COALESCE(TO_CHAR(c.variance_resolved_at,'YYYY-MM-DD HH24:MI:SS'),'')  AS variance_resolved_at,
				COALESCE(c.confirmation_status,'')                                     AS confirmation_status,
				COALESCE(c.is_deleted,false)                                           AS is_deleted,
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
						RecordTable:      "investment.fd_confirmation",
						AuditTable:       "investment.fd_audit_confirmation",
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
			"audit_history":     auditHistory,
			"approval_workflow": approvalWorkflow,
		})
		api.LogInfo("[FDBooking] GetConfirmationDetail: confirmation_id=%s", confirmationID)
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
			api.RespondWithError(w, http.StatusBadRequest, "confirmation_ids are required")
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
			msg, status := getUserFriendlyFDError(err, "Transaction begin failed")
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx) //nolint:errcheck

		// CONFIRMED confirmations cannot be deleted (system depends on them, like APPROVED bookings).
		// Allow: PENDING_CONFIRMATION, VARIANCE_DETECTED, REJECTED, APPROVAL_PENDING.
		rows, err := tx.Query(ctx, `
			SELECT c.confirmation_id, b.entity_id, c.actual_principal
			FROM investment.fd_confirmation c
			JOIN investment.fd_booking_request b ON b.booking_id = c.booking_id
			WHERE c.confirmation_id = ANY($1::text[])
			  AND c.confirmation_status NOT IN ('CONFIRMED','VARIANCE_REJECTED')
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
			api.RespondWithError(w, http.StatusInternalServerError, "Row error: "+err.Error())
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
				bgCtx := context.Background()
				// Cancel any in-flight approval chain before submitting DELETE
				if err := approvalengine.CancelPendingInstances(bgCtx, pgxPool, "FIXED_DEPOSIT", cID, uEmail); err != nil {
					api.LogError("[FDBooking] CancelPendingInstances(DELETE) failed for confirmation %s: %v", cID, err)
				}
				instID, err := approvalengine.CreateInstance(bgCtx, pgxPool, approvalengine.InstanceRequest{
					ModuleCode: "FIXED_DEPOSIT", EntityCode: eID,
					TransactionType: "FD_CONFIRMATION_DELETE", RecordID: cID,
					RecordTable: "investment.fd_confirmation", AuditTable: "investment.fd_audit_confirmation",
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
