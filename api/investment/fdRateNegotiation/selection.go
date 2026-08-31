package fdRateNegotiation

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/approvalengine"
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

// Header audit action_type CHECK is CREATE|EDIT|DELETE|APPROVE|REJECT|file/DMS —
// SELECT is not allowed, so selection and booking-link rows use EDIT.
const selectionAuditAction = "EDIT"

// Selection is a lifecycle wait (request_status = PENDING_RATE_APPROVAL), not a
// Rate Request field-edit. Keep processing_status on the 5-value audit set as
// APPROVED so the list does not show "Pending Edit Approval".
const selectionAuditStatus = "APPROVED"

// Booking link is terminal (CONVERTED_TO_FD) — never leave a PENDING_* zombie audit.
const bookingLinkAuditStatus = "APPROVED"

type selectionPayload struct {
	RateRequestID    string   `json:"rate_request_id"`
	SelectedOfferID  string   `json:"selected_offer_id"`
	SelectionRemarks string   `json:"selection_remarks"`
	ComparedOfferIDs []string `json:"compared_offer_ids,omitempty"`
}

type linkBookingPayload struct {
	RateRequestID string `json:"rate_request_id"`
	BookingID     string `json:"booking_id"`
}

// SubmitSelection copies the chosen offer onto the master selected_* columns
// and sets request_status to PENDING_RATE_APPROVAL.
func SubmitSelection(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req selectionPayload
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if strings.TrimSpace(req.RateRequestID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "rate_request_id is required")
			return
		}
		if strings.TrimSpace(req.SelectedOfferID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "selected_offer_id is required")
			return
		}

		userEmail := api.GetUserEmailFromCtx(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		tx, err := pool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to start transaction")
			return
		}
		defer tx.Rollback(ctx)

		var (
			oldStatus  string
			oldOfferID *string
			oldRemarks *string
			amount     float64
			entityID   string
		)
		err = tx.QueryRow(ctx, `
			SELECT request_status,
				selected_offer_id::text,
				selection_remarks,
				COALESCE(proposed_fd_amount,0),
				COALESCE(entity_id,'')
			FROM investment.fd_rate_negotiation
			WHERE rate_request_id = $1::uuid AND COALESCE(is_deleted,false)=false
			FOR UPDATE`, req.RateRequestID).Scan(
			&oldStatus, &oldOfferID, &oldRemarks, &amount, &entityID,
		)
		if err != nil {
			if err == pgx.ErrNoRows {
				api.RespondWithError(w, http.StatusNotFound, "Rate request not found")
				return
			}
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to load rate request")
			return
		}
		switch oldStatus {
		case "PENDING_DELETE_APPROVAL", "DELETED", "CANCELLED", "CONVERTED_TO_FD":
			api.RespondWithError(w, http.StatusBadRequest, "Rate request cannot be selected in current status")
			return
		}

		if oldOfferID != nil && strings.EqualFold(strings.TrimSpace(*oldOfferID), strings.TrimSpace(req.SelectedOfferID)) {
			api.RespondWithError(w, http.StatusBadRequest, "This offer is already selected from a previous comparison")
			return
		}

		var bankID, bankName *string
		var offerStatus string
		err = tx.QueryRow(ctx, `
			SELECT bank_id, bank_name, offer_status
			FROM investment.fd_rate_offer
			WHERE offer_id = $1::uuid
			  AND rate_request_id = $2::uuid
			  AND COALESCE(is_deleted,false)=false`,
			req.SelectedOfferID, req.RateRequestID,
		).Scan(&bankID, &bankName, &offerStatus)
		if err != nil {
			if err == pgx.ErrNoRows {
				api.RespondWithError(w, http.StatusBadRequest, "Selected offer does not belong to this rate request")
				return
			}
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to load selected offer")
			return
		}
		var latestProc string
		_ = tx.QueryRow(ctx, `
			SELECT COALESCE(processing_status,'')
			FROM investment.fd_rate_offer_audit
			WHERE offer_id = $1::uuid
			ORDER BY requested_at DESC, audit_id DESC
			LIMIT 1`, req.SelectedOfferID).Scan(&latestProc)
		if !strings.EqualFold(strings.TrimSpace(latestProc), "APPROVED") {
			api.RespondWithError(w, http.StatusBadRequest, "Only capture-approved offers can be selected")
			return
		}
		st := strings.ToUpper(strings.TrimSpace(offerStatus))
		if st == "REJECTED" || st == "EXPIRED" || st == "INACTIVE" {
			api.RespondWithError(w, http.StatusBadRequest, "This offer is not active")
			return
		}

		selOK, selMatrixID := fdEnforceMatrix(ctx, w, r, pool, enforceCtx{
			EventCode:   common.TriggerPreEdit,
			HandlerName: "SubmitSelection",
			APIPath:     "/investment/fd/rate-negotiation/selection/submit",
			EntityCode:  entityID,
			Actor:       userEmail,
		}, map[string]interface{}{
			"rate_request_id":    req.RateRequestID,
			"entity_id":          entityID,
			"entity_code":        entityID,
			"proposed_fd_amount": amount,
			"selected_offer_id":  req.SelectedOfferID,
			"request_status":     oldStatus,
		})
		if !selOK {
			return
		}

		newStatus := "PENDING_RATE_APPROVAL"
		var selectedBankID, selectedBankName interface{}
		if bankID != nil {
			selectedBankID = nullIfEmpty(*bankID)
		}
		if bankName != nil {
			selectedBankName = nullIfEmpty(*bankName)
		}

		_, err = tx.Exec(ctx, `
			UPDATE investment.fd_rate_negotiation SET
				selected_offer_id = $2::uuid,
				selected_bank_id = $3,
				selected_bank_name = $4,
				selection_remarks = NULLIF($5,''),
				selection_submitted_by = $6,
				selection_submitted_at = now(),
				request_status = $7,
				processing_status = $8,
				updated_by = $6,
				updated_at = now()
			WHERE rate_request_id = $1::uuid`,
			req.RateRequestID,
			req.SelectedOfferID,
			selectedBankID,
			selectedBankName,
			req.SelectionRemarks,
			userEmail,
			newStatus,
			selectionAuditStatus,
		)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, fmt.Sprintf("Selection update failed: %v", err))
			return
		}

		var oldOfferVal, oldRemarksVal interface{}
		if oldOfferID != nil && strings.TrimSpace(*oldOfferID) != "" {
			oldOfferVal = *oldOfferID
		}
		if oldRemarks != nil {
			oldRemarksVal = *oldRemarks
		}

		_, err = tx.Exec(ctx, `
			INSERT INTO investment.fd_audit_rate_negotiation (
				rate_request_id, action_type, processing_status,
				requested_by, requested_at, requested_ip,
				old_selected_offer_id, new_selected_offer_id,
				old_selection_remarks, new_selection_remarks,
				old_request_status, new_request_status
			) VALUES (
				$1::uuid, $2, $3,
				$4, now(), $5,
				NULLIF($6,'')::uuid, $7::uuid,
				$8, NULLIF($9,''),
				$10, $11
			)`,
			req.RateRequestID, selectionAuditAction, selectionAuditStatus,
			api.SystemIfBlank(userEmail), api.SystemIfBlank(api.ClientIPFromContext(ctx)),
			oldOfferVal, req.SelectedOfferID,
			oldRemarksVal, req.SelectionRemarks,
			oldStatus, newStatus,
		)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Audit insert failed")
			return
		}

		comparedSet := map[string]struct{}{}
		for _, id := range req.ComparedOfferIDs {
			if id = strings.TrimSpace(id); id != "" {
				comparedSet[id] = struct{}{}
			}
		}
		comparedSet[req.SelectedOfferID] = struct{}{}
		comparedIDs := make([]string, 0, len(comparedSet))
		for id := range comparedSet {
			comparedIDs = append(comparedIDs, id)
		}

		_, err = tx.Exec(ctx, `
			INSERT INTO investment.fd_rate_selection_comparison (
				selection_rate_request_id, selected_offer_id, compared_offer_id,
				compared_rate_request_id, bank_id, bank_name,
				offered_interest_rate, effective_yield, is_selected,
				created_by
			)
			SELECT
				$1::uuid, $2::uuid, o.offer_id,
				o.rate_request_id, o.bank_id, o.bank_name,
				o.offered_interest_rate, o.effective_yield, (o.offer_id = $2::uuid),
				$3
			FROM investment.fd_rate_offer o
			WHERE o.offer_id = ANY($4::uuid[])
			  AND COALESCE(o.is_deleted,false) = false`,
			req.RateRequestID, req.SelectedOfferID, userEmail, comparedIDs,
		)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Comparison trail insert failed")
			return
		}

		if err = tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Commit failed")
			return
		}

		userID := api.GetUserIDFromCtx(r.Context())
		fireRateNegotiationInstance(pool, req.RateRequestID, userID, userEmail, "EDIT", selMatrixID, amount)

		outBankID, outBankName := "", ""
		if bankID != nil {
			outBankID = *bankID
		}
		if bankName != nil {
			outBankName = *bankName
		}
		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"rate_request_id":    req.RateRequestID,
			"selected_offer_id":  req.SelectedOfferID,
			"selected_bank_id":   outBankID,
			"selected_bank_name": outBankName,
			"selection_remarks":  req.SelectionRemarks,
			"request_status":     newStatus,
			"processing_status":  selectionAuditStatus,
		})
	}
}

// LinkBooking stores booking_id on the master and sets request_status CONVERTED_TO_FD.
func LinkBooking(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req linkBookingPayload
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if strings.TrimSpace(req.RateRequestID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "rate_request_id is required")
			return
		}
		if strings.TrimSpace(req.BookingID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "booking_id is required")
			return
		}

		userEmail := api.GetUserEmailFromCtx(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		tx, err := pool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to start transaction")
			return
		}
		defer tx.Rollback(ctx)

		var oldStatus string
		var oldBookingID *string
		err = tx.QueryRow(ctx, `
			SELECT request_status, booking_id
			FROM investment.fd_rate_negotiation
			WHERE rate_request_id = $1::uuid AND COALESCE(is_deleted,false)=false
			FOR UPDATE`, req.RateRequestID).Scan(&oldStatus, &oldBookingID)
		if err != nil {
			if err == pgx.ErrNoRows {
				api.RespondWithError(w, http.StatusNotFound, "Rate request not found")
				return
			}
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to load rate request")
			return
		}
		switch oldStatus {
		case "PENDING_DELETE_APPROVAL", "DELETED", "CANCELLED":
			api.RespondWithError(w, http.StatusBadRequest, "Rate request cannot be linked to a booking in current status")
			return
		}

		bookingID := strings.TrimSpace(req.BookingID)
		if err = applyBookingConversion(ctx, tx, bookingConversionParams{
			RateRequestID: req.RateRequestID, BookingID: bookingID, OldStatus: oldStatus, OldBookingID: oldBookingID,
			UserEmail: userEmail, ClientIP: api.ClientIPFromContext(ctx),
		}); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}

		if err = tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Commit failed")
			return
		}

		// Drop any leftover maker-checker instances — conversion is terminal.
		go func(id, actor string) {
			bg, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			_ = approvalengine.CancelPendingInstances(bg, pool, rateNegModule, id, actor)
		}(req.RateRequestID, userEmail)

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"rate_request_id": req.RateRequestID,
			"booking_id":      bookingID,
			"request_status":  "CONVERTED_TO_FD",
		})
	}
}

// applyBookingConversion stamps negotiation ↔ booking bidirectionally, sets
// CONVERTED_TO_FD, clears pending audits, and writes an APPROVED EDIT audit
// (never PENDING_EDIT_APPROVAL — that left zombie "pending edit" rows).
// bookingConversionParams bundles the fields needed to convert a rate
// negotiation request into a linked FD booking.
type bookingConversionParams struct {
	RateRequestID string
	BookingID     string
	OldStatus     string
	OldBookingID  *string
	UserEmail     string
	ClientIP      string
}

func applyBookingConversion(ctx context.Context, tx pgx.Tx, p bookingConversionParams) error {
	rateRequestID, bookingID, oldStatus, oldBookingID, userEmail, clientIP :=
		p.RateRequestID, p.BookingID, p.OldStatus, p.OldBookingID, p.UserEmail, p.ClientIP
	newStatus := "CONVERTED_TO_FD"
	if _, err := tx.Exec(ctx, `
		UPDATE investment.fd_rate_negotiation SET
			booking_id = $2,
			request_status = $3,
			processing_status = $4,
			updated_by = $5,
			updated_at = now()
		WHERE rate_request_id = $1::uuid`,
		rateRequestID, bookingID, newStatus, bookingLinkAuditStatus, userEmail,
	); err != nil {
		return fmt.Errorf("Booking link failed: %v", err)
	}

	if _, err := tx.Exec(ctx, `
		UPDATE investment.fd_booking_request
		SET rate_request_id = $1::uuid
		WHERE booking_id = $2
		  AND COALESCE(is_deleted, false) = false`,
		rateRequestID, bookingID,
	); err != nil {
		return fmt.Errorf("Stamp booking rate_request_id failed: %v", err)
	}

	// Clear any stuck PENDING_* audits so list/join cannot resurface pending edit.
	if _, err := tx.Exec(ctx, `
		UPDATE investment.fd_audit_rate_negotiation
		SET processing_status = 'APPROVED',
			checker_by = $2,
			checker_at = now(),
			checker_comment = COALESCE(NULLIF(checker_comment,''), 'Auto-stamped on booking conversion')
		WHERE rate_request_id = $1::uuid
		  AND processing_status LIKE 'PENDING%'`,
		rateRequestID, api.SystemIfBlank(userEmail),
	); err != nil {
		return fmt.Errorf("Stamp pending audits failed: %v", err)
	}

	var oldBookingVal interface{}
	if oldBookingID != nil && strings.TrimSpace(*oldBookingID) != "" {
		oldBookingVal = *oldBookingID
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO investment.fd_audit_rate_negotiation (
			rate_request_id, action_type, processing_status,
			requested_by, requested_at, requested_ip,
			checker_by, checker_at, checker_comment,
			old_booking_id, new_booking_id,
			old_request_status, new_request_status
		) VALUES (
			$1::uuid, $2, $3,
			$4, now(), $5,
			$4, now(), 'Booking linked — converted to FD',
			$6, $7,
			$8, $9
		)`,
		rateRequestID, selectionAuditAction, bookingLinkAuditStatus,
		api.SystemIfBlank(userEmail), api.SystemIfBlank(clientIP),
		oldBookingVal, bookingID,
		oldStatus, newStatus,
	); err != nil {
		return fmt.Errorf("Audit insert failed: %v", err)
	}
	return nil
}
