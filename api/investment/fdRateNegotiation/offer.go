package fdRateNegotiation

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const errOfferIDRequired = "offer_id is required"

type offerPayload struct {
	UserID              string   `json:"user_id"`
	OfferID             string   `json:"offer_id,omitempty"`
	OfferReferenceID    string   `json:"offer_reference_id,omitempty"`
	RateRequestID       string   `json:"rate_request_id,omitempty"`
	BankID              string   `json:"bank_id,omitempty"`
	BankName            string   `json:"bank_name"`
	OfferedInterestRate float64  `json:"offered_interest_rate"`
	ApplicableTenure    string   `json:"applicable_tenure,omitempty"`
	ValidTillDate       string   `json:"valid_till_date"`
	ConditionsRemarks   string   `json:"conditions_remarks,omitempty"`
	CommunicationSource string   `json:"communication_source"`
	CommunicationID     string   `json:"communication_id,omitempty"`
	EmailMessageID      string   `json:"email_message_id,omitempty"`
	OfferStatus         string   `json:"offer_status,omitempty"`
	EffectiveYield      *float64 `json:"effective_yield,omitempty"`
}

func normalizeOfferSource(v string) string {
	return strings.ToUpper(strings.TrimSpace(v))
}

func normalizeOfferStatus(v string) string {
	return strings.ToUpper(strings.TrimSpace(v))
}

func validateOffer(req offerPayload) string {
	if strings.TrimSpace(req.BankName) == "" {
		return "bank_name is required"
	}
	if req.OfferedInterestRate <= 0 {
		return "offered_interest_rate must be greater than 0"
	}
	if strings.TrimSpace(req.ValidTillDate) == "" {
		return "valid_till_date is required"
	}
	if _, err := time.Parse("2006-01-02", strings.TrimSpace(req.ValidTillDate)); err != nil {
		return "valid_till_date must be YYYY-MM-DD"
	}
	src := normalizeOfferSource(req.CommunicationSource)
	if src != "EMAIL" && src != "MANUAL" {
		return "communication_source must be EMAIL or MANUAL"
	}
	st := normalizeOfferStatus(req.OfferStatus)
	if st != "" && st != "ACTIVE" && st != "INACTIVE" && st != "EXPIRED" && st != "APPROVED" && st != "REJECTED" && st != "PENDING_APPROVAL" {
		return "offer_status must be ACTIVE, INACTIVE, EXPIRED, APPROVED or REJECTED"
	}
	return ""
}

func nextOfferRef(ctx context.Context, tx pgx.Tx) (string, error) {
	day := time.Now().Format("20060102")
	prefix := fmt.Sprintf("OFFER-%s-", day)
	var count int
	err := tx.QueryRow(ctx, `
		SELECT COUNT(*) FROM investment.fd_rate_offer
		WHERE offer_reference_id LIKE $1`, prefix+"%").Scan(&count)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s%04d", prefix, count+1), nil
}

func yieldArg(v *float64) interface{} {
	if v == nil {
		return nil
	}
	return *v
}

// offerCreateAuditParams bundles the fields needed to insert a create-audit
// row for a new bank rate offer.
type offerCreateAuditParams struct {
	OfferID       string
	RateRequestID string
	UserEmail     string
	ClientIP      string
	Req           offerPayload
	Source        string
	Status        string
}

func insertOfferCreateAudit(ctx context.Context, tx pgx.Tx, p offerCreateAuditParams) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO investment.fd_rate_offer_audit (
			offer_id, rate_request_id, action_type, processing_status,
			requested_by, requested_at, requested_ip,
			new_bank_name, new_offered_interest_rate, new_valid_till_date,
			new_conditions_remarks, new_communication_source, new_offer_status, new_effective_yield
		) VALUES (
			$1::uuid, $2::uuid, 'CREATE', 'PENDING_APPROVAL',
			$3, now(), $4,
			$5, $6, $7::date,
			NULLIF($8,''), $9, $10, $11
		)`,
		p.OfferID, p.RateRequestID,
		api.SystemIfBlank(p.UserEmail), api.SystemIfBlank(p.ClientIP),
		strings.TrimSpace(p.Req.BankName), p.Req.OfferedInterestRate, strings.TrimSpace(p.Req.ValidTillDate),
		p.Req.ConditionsRemarks, p.Source, p.Status, yieldArg(p.Req.EffectiveYield),
	)
	return err
}

// CreateOffer captures a bank rate offer in PENDING_APPROVAL (booking-style).
func CreateOffer(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req offerPayload
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if strings.TrimSpace(req.RateRequestID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "rate_request_id is required")
			return
		}
		if msg := validateOffer(req); msg != "" {
			api.RespondWithError(w, http.StatusBadRequest, msg)
			return
		}

		userEmail := api.GetUserEmailFromCtx(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		source := normalizeOfferSource(req.CommunicationSource)
		status := normalizeOfferStatus(req.OfferStatus)
		if status == "" || status == "DRAFT" {
			status = "ACTIVE"
		}
		if status == "APPROVED" || status == "REJECTED" {
			api.RespondWithError(w, http.StatusBadRequest, "new offers cannot be created as APPROVED or REJECTED")
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to start transaction")
			return
		}
		defer tx.Rollback(ctx)

		if err = assertParentStatus(ctx, tx, req.RateRequestID, "APPROVED", "SENT_TO_BANKS", "OFFERS_RECEIVED"); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, err.Error())
			return
		}

		if strings.TrimSpace(req.CommunicationID) != "" {
			var commRequestID, commStatus string
			err = tx.QueryRow(ctx, `
				SELECT rate_request_id::text, communication_status
				FROM investment.fd_rate_communication
				WHERE communication_id = $1::uuid AND COALESCE(is_deleted,false)=false`,
				req.CommunicationID).Scan(&commRequestID, &commStatus)
			if err != nil {
				api.RespondWithError(w, http.StatusBadRequest, "communication_id not found")
				return
			}
			if commRequestID != req.RateRequestID {
				api.RespondWithError(w, http.StatusBadRequest, "communication_id does not belong to this rate request")
				return
			}
			if strings.ToUpper(strings.TrimSpace(commStatus)) != "RESPONSE_RECEIVED" {
				api.RespondWithError(w, http.StatusBadRequest, "only RESPONSE_RECEIVED communications can be wired to an offer")
				return
			}
		}
		// communication_id is optional: an offer can be captured purely manually
		// (bank told the user the rate directly) without a prior Bank Communication
		// receive step. communication_source still records how the offer was learned.

		ref, err := nextOfferRef(ctx, tx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to generate offer reference")
			return
		}

		var id string
		err = tx.QueryRow(ctx, `
			INSERT INTO investment.fd_rate_offer (
				offer_reference_id, rate_request_id, bank_id, bank_name,
				offered_interest_rate, applicable_tenure, valid_till_date, conditions_remarks,
				communication_source, communication_id, email_message_id,
				offer_status, effective_yield, created_by, created_at
			) VALUES (
				$1, $2::uuid, NULLIF($3,''), $4,
				$5, NULLIF($6,''), $7::date, NULLIF($8,''),
				$9, NULLIF($10,'')::uuid, NULLIF($11,'')::uuid,
				$12, $13, $14, now()
			) RETURNING offer_id::text`,
			ref, req.RateRequestID, strings.TrimSpace(req.BankID), strings.TrimSpace(req.BankName),
			req.OfferedInterestRate, req.ApplicableTenure, strings.TrimSpace(req.ValidTillDate), req.ConditionsRemarks,
			source, strings.TrimSpace(req.CommunicationID), strings.TrimSpace(req.EmailMessageID),
			status, yieldArg(req.EffectiveYield), userEmail,
		).Scan(&id)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, fmt.Sprintf("Create failed: %v", err))
			return
		}

		if err = insertOfferCreateAudit(ctx, tx, offerCreateAuditParams{
			OfferID: id, RateRequestID: req.RateRequestID, UserEmail: userEmail, ClientIP: api.ClientIPFromContext(ctx),
			Req: req, Source: source, Status: status,
		}); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Audit insert failed")
			return
		}

		if _, err = tx.Exec(ctx, `
			UPDATE investment.fd_rate_negotiation
			SET request_status = 'OFFERS_RECEIVED', updated_by = $2, updated_at = now()
			WHERE rate_request_id = $1::uuid
			  AND request_status IN ('APPROVED','SUBMITTED','SENT_TO_BANKS')`,
			req.RateRequestID, userEmail); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to mark offers received")
			return
		}

		if err = tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Commit failed")
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"offer_id":           id,
			"offer_reference_id": ref,
			"offer_status":       status,
			"processing_status":  "PENDING_APPROVAL",
		})
	}
}

// UpdateOffer applies new values immediately → PENDING_EDIT_APPROVAL with old_/new_ audit.
func UpdateOffer(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req offerPayload
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if strings.TrimSpace(req.OfferID) == "" && strings.TrimSpace(req.OfferReferenceID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, errOfferIDRequired)
			return
		}
		if msg := validateOffer(req); msg != "" {
			api.RespondWithError(w, http.StatusBadRequest, msg)
			return
		}

		userEmail := api.GetUserEmailFromCtx(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		source := normalizeOfferSource(req.CommunicationSource)
		status := normalizeOfferStatus(req.OfferStatus)

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to start transaction")
			return
		}
		defer tx.Rollback(ctx)

		var (
			offerID, rateRequestID, oldBankName, oldValidTill, oldSource, oldStatus, oldRef string
			oldRate                                                                         float64
			oldRemarks                                                                      *string
			oldYield                                                                        *float64
		)
		lookupSQL := `
			SELECT offer_id::text, offer_reference_id, rate_request_id::text, bank_name,
				offered_interest_rate, TO_CHAR(valid_till_date,'YYYY-MM-DD'),
				conditions_remarks, communication_source, offer_status, effective_yield
			FROM investment.fd_rate_offer
			WHERE COALESCE(is_deleted,false)=false AND `
		var lookupArg string
		if strings.TrimSpace(req.OfferID) != "" {
			lookupSQL += `offer_id = $1::uuid FOR UPDATE`
			lookupArg = req.OfferID
		} else {
			lookupSQL += `offer_reference_id = $1 FOR UPDATE`
			lookupArg = req.OfferReferenceID
		}
		err = tx.QueryRow(ctx, lookupSQL, lookupArg).Scan(
			&offerID, &oldRef, &rateRequestID, &oldBankName,
			&oldRate, &oldValidTill, &oldRemarks, &oldSource, &oldStatus, &oldYield,
		)
		if err != nil {
			api.RespondWithError(w, http.StatusNotFound, "Offer not found")
			return
		}

		if status == "" {
			status = oldStatus
		}

		if strings.TrimSpace(req.CommunicationID) != "" {
			var commRequestID string
			err = tx.QueryRow(ctx, `
				SELECT rate_request_id::text FROM investment.fd_rate_communication
				WHERE communication_id = $1::uuid AND COALESCE(is_deleted,false)=false`,
				req.CommunicationID).Scan(&commRequestID)
			if err != nil {
				api.RespondWithError(w, http.StatusBadRequest, "communication_id not found")
				return
			}
			if commRequestID != rateRequestID {
				api.RespondWithError(w, http.StatusBadRequest, "communication_id does not belong to this rate request")
				return
			}
		}

		newStatus := "PENDING_EDIT_APPROVAL"
		actionType := "EDIT"

		_, err = tx.Exec(ctx, `
			UPDATE investment.fd_rate_offer SET
				bank_id = NULLIF($2,''),
				bank_name = $3,
				offered_interest_rate = $4,
				applicable_tenure = NULLIF($5,''),
				valid_till_date = $6::date,
				conditions_remarks = NULLIF($7,''),
				communication_source = $8,
				communication_id = NULLIF($9,'')::uuid,
				email_message_id = NULLIF($10,'')::uuid,
				offer_status = $11,
				effective_yield = $12,
				updated_by = $13,
				updated_at = now()
			WHERE offer_id = $1::uuid`,
			offerID,
			strings.TrimSpace(req.BankID), strings.TrimSpace(req.BankName),
			req.OfferedInterestRate, req.ApplicableTenure, strings.TrimSpace(req.ValidTillDate),
			req.ConditionsRemarks, source,
			strings.TrimSpace(req.CommunicationID), strings.TrimSpace(req.EmailMessageID),
			status, yieldArg(req.EffectiveYield), userEmail,
		)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, fmt.Sprintf("Update failed: %v", err))
			return
		}

		var oldRemarksStr interface{}
		if oldRemarks != nil {
			oldRemarksStr = *oldRemarks
		}

		_, err = tx.Exec(ctx, `
			INSERT INTO investment.fd_rate_offer_audit (
				offer_id, rate_request_id, action_type, processing_status,
				requested_by, requested_at, requested_ip,
				old_bank_name, new_bank_name,
				old_offered_interest_rate, new_offered_interest_rate,
				old_valid_till_date, new_valid_till_date,
				old_conditions_remarks, new_conditions_remarks,
				old_communication_source, new_communication_source,
				old_offer_status, new_offer_status,
				old_effective_yield, new_effective_yield
			) VALUES (
				$1::uuid, $2::uuid, $3, $4,
				$5, now(), $6,
				$7, $8,
				$9, $10,
				NULLIF($11,'')::date, $12::date,
				$13, NULLIF($14,''),
				$15, $16,
				$17, $18,
				$19, $20
			)`,
			offerID, rateRequestID, actionType, newStatus,
			api.SystemIfBlank(userEmail), api.SystemIfBlank(api.ClientIPFromContext(ctx)),
			oldBankName, strings.TrimSpace(req.BankName),
			oldRate, req.OfferedInterestRate,
			oldValidTill, strings.TrimSpace(req.ValidTillDate),
			oldRemarksStr, req.ConditionsRemarks,
			oldSource, source,
			oldStatus, status,
			yieldArg(oldYield), yieldArg(req.EffectiveYield),
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
			"offer_id":           offerID,
			"offer_reference_id": oldRef,
			"offer_status":       status,
			"processing_status":  newStatus,
		})
	}
}

func scanOffer(row pgx.Row) (map[string]interface{}, error) {
	var (
		id, ref, rateRequestID, bankName, validTill, source, status, createdBy, createdAt string
		rate                                                                              float64
		bankID, tenure, remarks, commID, emailMsgID, updatedBy, updatedAt, rateRequestRef *string
		yield                                                                             *float64
	)
	err := row.Scan(
		&id, &ref, &rateRequestID, &bankID, &bankName, &rate, &tenure, &validTill,
		&remarks, &source, &commID, &emailMsgID, &status, &yield,
		&createdBy, &createdAt, &updatedBy, &updatedAt, &rateRequestRef,
	)
	if err != nil {
		return nil, err
	}
	out := map[string]interface{}{
		"offer_id":              id,
		"offer_reference_id":    ref,
		"rate_request_id":       rateRequestID,
		"rate_request_ref":      "",
		"bank_id":               "",
		"bank_name":             bankName,
		"offered_interest_rate": rate,
		"applicable_tenure":     "",
		"valid_till_date":       validTill,
		"conditions_remarks":    "",
		"communication_source":  source,
		"communication_id":      "",
		"email_message_id":      "",
		"offer_status":          status,
		"effective_yield":       nil,
		"created_by":            createdBy,
		"created_at":            createdAt,
		"updated_by":            "",
		"updated_at":            "",
	}
	if bankID != nil {
		out["bank_id"] = *bankID
	}
	if tenure != nil {
		out["applicable_tenure"] = *tenure
	}
	if remarks != nil {
		out["conditions_remarks"] = *remarks
	}
	if commID != nil {
		out["communication_id"] = *commID
	}
	if emailMsgID != nil {
		out["email_message_id"] = *emailMsgID
	}
	if yield != nil {
		out["effective_yield"] = *yield
	}
	if updatedBy != nil {
		out["updated_by"] = *updatedBy
	}
	if updatedAt != nil {
		out["updated_at"] = *updatedAt
	}
	if rateRequestRef != nil {
		out["rate_request_ref"] = *rateRequestRef
	}
	return out, nil
}

const offerSelect = `
	SELECT
		offer_id::text,
		offer_reference_id,
		rate_request_id::text,
		bank_id,
		bank_name,
		offered_interest_rate,
		applicable_tenure,
		TO_CHAR(valid_till_date,'YYYY-MM-DD'),
		conditions_remarks,
		communication_source,
		communication_id::text,
		email_message_id::text,
		offer_status,
		effective_yield,
		created_by,
		TO_CHAR(created_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
		updated_by,
		TO_CHAR(updated_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
		(SELECT n.rate_request_ref FROM investment.fd_rate_negotiation n
		 WHERE n.rate_request_id = investment.fd_rate_offer.rate_request_id)
	FROM investment.fd_rate_offer
`

// ListOffers returns non-deleted offers for a rate request.
func ListOffers(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			RateRequestID string `json:"rate_request_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil && err != io.EOF {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}

		ctx := r.Context()
		rateRequestID := strings.TrimSpace(req.RateRequestID)
		var (
			rows pgx.Rows
			err  error
		)
		if rateRequestID == "" {
			rows, err = pgxPool.Query(ctx, offerSelect+`
			WHERE COALESCE(is_deleted,false)=false
			ORDER BY created_at DESC, offer_id DESC`)
		} else {
			rows, err = pgxPool.Query(ctx, offerSelect+`
			WHERE rate_request_id = $1::uuid AND COALESCE(is_deleted,false)=false
			ORDER BY created_at DESC, offer_id DESC`, rateRequestID)
		}
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to list offers")
			return
		}
		defer rows.Close()

		results := make([]map[string]interface{}, 0)
		ids := make([]string, 0)
		for rows.Next() {
			item, err := scanOffer(rows)
			if err != nil {
				continue
			}
			results = append(results, item)
			if id, _ := item["offer_id"].(string); id != "" {
				ids = append(ids, id)
			}
		}
		procMap := map[string]string{}
		if len(ids) > 0 {
			arows, aerr := pgxPool.Query(ctx, `
				SELECT DISTINCT ON (offer_id)
					offer_id::text, COALESCE(processing_status,'')
				FROM investment.fd_rate_offer_audit
				WHERE offer_id = ANY($1::uuid[])
				ORDER BY offer_id, requested_at DESC`, ids)
			if aerr == nil {
				defer arows.Close()
				for arows.Next() {
					var oid, ps string
					if arows.Scan(&oid, &ps) == nil {
						procMap[oid] = ps
					}
				}
			}
		}
		for _, item := range results {
			id, _ := item["offer_id"].(string)
			if ps := procMap[id]; ps != "" {
				item["processing_status"] = ps
			} else {
				item["processing_status"] = ""
			}
		}
		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"results": results,
		})
	}
}

// ListOfferAudit returns old_/new_ audit rows for one offer or request.
func ListOfferAudit(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			OfferID       string `json:"offer_id"`
			RateRequestID string `json:"rate_request_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil && err != io.EOF {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}
		offerID := strings.TrimSpace(req.OfferID)
		rateRequestID := strings.TrimSpace(req.RateRequestID)
		if offerID == "" && rateRequestID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "offer_id or rate_request_id is required")
			return
		}

		ctx := r.Context()
		query := `
			SELECT *
			FROM investment.fd_rate_offer_audit`
		var (
			rows pgx.Rows
			err  error
		)
		if offerID != "" {
			rows, err = pgxPool.Query(ctx, query+`
			WHERE offer_id = $1::uuid
			ORDER BY requested_at DESC, audit_id DESC`, offerID)
		} else {
			rows, err = pgxPool.Query(ctx, query+`
			WHERE rate_request_id = $1::uuid
			ORDER BY requested_at DESC, audit_id DESC`, rateRequestID)
		}
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to fetch offer audit")
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

func decideOffers(pgxPool *pgxpool.Pool, approve bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			OfferIDs []string `json:"offer_ids"`
			OfferID  string   `json:"offer_id"`
			Comment  string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		ids := req.OfferIDs
		if strings.TrimSpace(req.OfferID) != "" {
			ids = append(ids, req.OfferID)
		}
		if len(ids) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, errOfferIDRequired)
			return
		}
		userEmail := api.GetUserEmailFromCtx(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		processing := "APPROVED"
		newStatus := "APPROVED"
		if !approve {
			processing = "REJECTED"
			newStatus = "REJECTED"
		}
		ctx := r.Context()
		acted := 0
		for _, id := range ids {
			id = strings.TrimSpace(id)
			if id == "" {
				continue
			}
			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				continue
			}
			var actionType, oldStatus string
			err = tx.QueryRow(ctx, `
				UPDATE investment.fd_rate_offer_audit
				SET processing_status = $2,
				    checker_by = $3,
				    checker_at = now(),
				    checker_comment = NULLIF($4,''),
				    checker_ip = NULLIF($5,'')
				WHERE audit_id = (
					SELECT audit_id FROM investment.fd_rate_offer_audit
					WHERE offer_id = $1::uuid
					  AND processing_status IN ('PENDING_APPROVAL','PENDING_EDIT_APPROVAL','PENDING_DELETE_APPROVAL')
					ORDER BY requested_at DESC LIMIT 1
				)
				RETURNING COALESCE(action_type,''), COALESCE(old_offer_status,'')`,
				id, processing, userEmail, req.Comment, api.ClientIPFromContext(ctx),
			).Scan(&actionType, &oldStatus)
			if err != nil {
				tx.Rollback(ctx)
				continue
			}

			switch strings.ToUpper(actionType) {
			case "DELETE":
				if approve {
					if _, err = tx.Exec(ctx, `
						UPDATE investment.fd_rate_offer
						SET is_deleted = true, updated_by = $2, updated_at = now()
						WHERE offer_id = $1::uuid`, id, userEmail); err != nil {
						tx.Rollback(ctx)
						continue
					}
				}
			case "EDIT":
				if !approve {
					if _, err = tx.Exec(ctx, `
					UPDATE investment.fd_rate_offer m SET
						bank_name = COALESCE(a.old_bank_name, m.bank_name),
						offered_interest_rate = COALESCE(a.old_offered_interest_rate, m.offered_interest_rate),
						valid_till_date = COALESCE(a.old_valid_till_date, m.valid_till_date),
						conditions_remarks = COALESCE(a.old_conditions_remarks, m.conditions_remarks),
						communication_source = COALESCE(a.old_communication_source, m.communication_source),
						offer_status = COALESCE(NULLIF(a.old_offer_status,''), m.offer_status),
						effective_yield = COALESCE(a.old_effective_yield, m.effective_yield),
						updated_by = $2,
						updated_at = now()
					FROM (
						SELECT * FROM investment.fd_rate_offer_audit
						WHERE offer_id = $1::uuid AND action_type = 'EDIT'
						ORDER BY requested_at DESC LIMIT 1
					) a
					WHERE m.offer_id = $1::uuid`, id, userEmail); err != nil {
						tx.Rollback(ctx)
						continue
					}
				}
			default:
				if _, err = tx.Exec(ctx, `
					UPDATE investment.fd_rate_offer
					SET offer_status = $2, updated_by = $3, updated_at = now()
					WHERE offer_id = $1::uuid AND COALESCE(is_deleted,false)=false`,
					id, newStatus, userEmail); err != nil {
					tx.Rollback(ctx)
					continue
				}
			}

			if err = tx.Commit(ctx); err != nil {
				continue
			}
			acted++
		}
		msg := ""
		if acted == 0 {
			if approve {
				msg = "No offers were approved"
			} else {
				msg = "No offers were rejected"
			}
		}
		api.RespondWithPayload(w, acted > 0, msg, map[string]interface{}{
			"updated": acted,
			"status":  processing,
		})
	}
}

func ApproveOffers(pgxPool *pgxpool.Pool) http.HandlerFunc { return decideOffers(pgxPool, true) }
func RejectOffers(pgxPool *pgxpool.Pool) http.HandlerFunc  { return decideOffers(pgxPool, false) }

// DeleteOffers writes DELETE audits in PENDING_DELETE_APPROVAL.
// is_deleted is applied only when the checker approves.
func DeleteOffers(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			OfferIDs []string `json:"offer_ids"`
			OfferID  string   `json:"offer_id"`
			Comment  string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		ids := req.OfferIDs
		if strings.TrimSpace(req.OfferID) != "" {
			ids = append(ids, req.OfferID)
		}
		if len(ids) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, errOfferIDRequired)
			return
		}
		userEmail := api.GetUserEmailFromCtx(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		ctx := r.Context()
		acted := 0
		for _, id := range ids {
			id = strings.TrimSpace(id)
			if id == "" {
				continue
			}
			var rateRequestID, status string
			err := pgxPool.QueryRow(ctx, `
				SELECT rate_request_id::text, offer_status
				FROM investment.fd_rate_offer
				WHERE offer_id = $1::uuid AND COALESCE(is_deleted,false)=false`,
				id).Scan(&rateRequestID, &status)
			if err != nil {
				continue
			}
			var pending int
			_ = pgxPool.QueryRow(ctx, `
				SELECT COUNT(*) FROM investment.fd_rate_offer_audit
				WHERE offer_id = $1::uuid AND processing_status LIKE 'PENDING%'`, id).Scan(&pending)
			if pending > 0 {
				continue
			}
			var latestProc string
			_ = pgxPool.QueryRow(ctx, `
				SELECT COALESCE(processing_status,'')
				FROM investment.fd_rate_offer_audit
				WHERE offer_id = $1::uuid
				ORDER BY requested_at DESC, audit_id DESC
				LIMIT 1`, id).Scan(&latestProc)
			if strings.EqualFold(strings.TrimSpace(latestProc), "APPROVED") {
				continue
			}
			_, err = pgxPool.Exec(ctx, `
				INSERT INTO investment.fd_rate_offer_audit (
					offer_id, rate_request_id, action_type, processing_status,
					requested_by, requested_at, requested_ip,
					old_offer_status, new_offer_status
				) VALUES (
					$1::uuid, $2::uuid, 'DELETE', 'PENDING_DELETE_APPROVAL',
					$3, now(), $4, $5, $5
				)`,
				id, rateRequestID, userEmail, api.ClientIPFromContext(ctx), status)
			if err != nil {
				continue
			}
			acted++
		}
		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"updated":           acted,
			"processing_status": "PENDING_DELETE_APPROVAL",
		})
	}
}
