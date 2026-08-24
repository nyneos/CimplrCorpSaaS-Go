package fdRateNegotiation

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/api/policyengine/common"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Header audit action_type CHECK is CREATE|EDIT|DELETE|APPROVE|REJECT|file/DMS —
// SELECT is not allowed, so selection and booking-link rows use EDIT.
const selectionAuditAction = "EDIT"

// Selection is apply-immediately on the master (selected_* columns) and waits
// for checker on PENDING_EDIT_APPROVAL. Approve stamps the master APPROVED.
const selectionAuditStatus = "PENDING_EDIT_APPROVAL"

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
		if strings.ToUpper(strings.TrimSpace(offerStatus)) != "APPROVED" {
			api.RespondWithError(w, http.StatusBadRequest, "Only APPROVED offers can be selected for comparison")
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

		newStatus := "CONVERTED_TO_FD"
		_, err = tx.Exec(ctx, `
			UPDATE investment.fd_rate_negotiation SET
				booking_id = $2,
				request_status = $3,
				updated_by = $4,
				updated_at = now()
			WHERE rate_request_id = $1::uuid`,
			req.RateRequestID, strings.TrimSpace(req.BookingID), newStatus, userEmail,
		)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, fmt.Sprintf("Booking link failed: %v", err))
			return
		}

		// Stamp bidirectional link: booking → rate request (demo conversion path).
		if _, err = tx.Exec(ctx, `
			UPDATE investment.fd_booking_request
			SET rate_request_id = $1::uuid
			WHERE booking_id = $2
			  AND COALESCE(is_deleted, false) = false`,
			req.RateRequestID, strings.TrimSpace(req.BookingID),
		); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, fmt.Sprintf("Stamp booking rate_request_id failed: %v", err))
			return
		}

		var oldBookingVal interface{}
		if oldBookingID != nil && strings.TrimSpace(*oldBookingID) != "" {
			oldBookingVal = *oldBookingID
		}

		_, err = tx.Exec(ctx, `
			INSERT INTO investment.fd_audit_rate_negotiation (
				rate_request_id, action_type, processing_status,
				requested_by, requested_at, requested_ip,
				old_booking_id, new_booking_id,
				old_request_status, new_request_status
			) VALUES (
				$1::uuid, $2, $3,
				$4, now(), $5,
				$6, $7,
				$8, $9
			)`,
			req.RateRequestID, selectionAuditAction, selectionAuditStatus,
			api.SystemIfBlank(userEmail), api.SystemIfBlank(api.ClientIPFromContext(ctx)),
			oldBookingVal, strings.TrimSpace(req.BookingID),
			oldStatus, newStatus,
		)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Audit insert failed")
			return
		}

		if err = tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Commit failed")
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"rate_request_id": req.RateRequestID,
			"booking_id":      strings.TrimSpace(req.BookingID),
			"request_status":  newStatus,
		})
	}
}
