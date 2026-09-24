// Package investmentdashboards — FD Lifecycle Journey
//
// POST /dash/investment/fd/lifecycle-journey
//
// Body: { "entity_id": "", "key": "" }
//   - key empty  → { options: [...] } — FDs + in-flight bookings for the picker
//   - key set    → { journey: {...} } — rate negotiation → booking → confirmation
//     → activation records for that FD / booking, plus a unified audit timeline.
//
// key may be an fd_id (FD-…) or a booking_id (FDBR-…).
package investmentdashboards

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/internal/logger"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type fdJourneyRequest struct {
	EntityID string `json:"entity_id"`
	Key      string `json:"key"`
}

type fdJourneyOption struct {
	Value         string  `json:"value"`
	FDID          string  `json:"fd_id"`
	BookingID     string  `json:"booking_id"`
	Bank          string  `json:"bank"`
	Entity        string  `json:"entity"`
	Principal     float64 `json:"principal"`
	FDStatus      string  `json:"fd_status"`
	BookingStatus string  `json:"booking_status"`
}

type fdJourneyRateRequest struct {
	RateRequestID    string           `json:"rate_request_id"`
	Ref              string           `json:"rate_request_ref"`
	Status           string           `json:"request_status"`
	RequestDate      string           `json:"request_date"`
	ProposedAmount   float64          `json:"proposed_fd_amount"`
	Currency         string           `json:"currency_code"`
	Tenure           string           `json:"tenure"`
	TargetBanks      []string         `json:"target_bank_names"`
	SelectedBank     string           `json:"selected_bank_name"`
	SelectedRate     float64          `json:"selected_rate"`
	SelectionBy      string           `json:"selection_submitted_by"`
	SelectionAt      string           `json:"selection_submitted_at"`
	ApprovalDecision string           `json:"approval_decision"`
	ApprovedBy       string           `json:"approved_by"`
	ApprovalDate     string           `json:"approval_date"`
	CreatedBy        string           `json:"created_by"`
	CreatedAt        string           `json:"created_at"`
	Offers           []fdJourneyOffer `json:"offers"`
}

type fdJourneyOffer struct {
	OfferID    string  `json:"offer_id"`
	Bank       string  `json:"bank_name"`
	Rate       float64 `json:"offered_interest_rate"`
	Status     string  `json:"offer_status"`
	ValidTill  string  `json:"valid_till_date"`
	IsSelected bool    `json:"is_selected"`
}

type fdJourneyBooking struct {
	BookingID     string  `json:"booking_id"`
	Status        string  `json:"booking_status"`
	Bank          string  `json:"bank_name"`
	Entity        string  `json:"entity_name"`
	Principal     float64 `json:"principal_amount"`
	Rate          float64 `json:"interest_rate"`
	StartDate     string  `json:"expected_start_date"`
	MaturityDate  string  `json:"expected_maturity_date"`
	CreatedBy     string  `json:"created_by"`
	CreatedAt     string  `json:"created_at"`
	RateRequestID string  `json:"rate_request_id"`
}

type fdJourneyConfirmation struct {
	ConfirmationID string  `json:"confirmation_id"`
	Status         string  `json:"confirmation_status"`
	BankFDRef      string  `json:"bank_fd_ref_no"`
	ReceivedDate   string  `json:"confirmation_received_date"`
	Mode           string  `json:"confirmation_mode"`
	Principal      float64 `json:"actual_principal"`
	Rate           float64 `json:"confirmed_rate"`
	VarianceFlag   bool    `json:"variance_flag"`
	VarianceAction string  `json:"variance_action"`
	CreatedBy      string  `json:"created_by"`
	CreatedAt      string  `json:"created_at"`
}

type fdJourneyFD struct {
	FDID         string  `json:"fd_id"`
	Status       string  `json:"fd_status"`
	Principal    float64 `json:"principal_amount"`
	Rate         float64 `json:"interest_rate"`
	StartDate    string  `json:"start_date"`
	MaturityDate string  `json:"maturity_date"`
	ActivatedBy  string  `json:"activated_by"`
	ActivatedAt  string  `json:"activated_at"`
	CreatedAt    string  `json:"created_at"`
}

type fdJourneyEvent struct {
	Stage       string `json:"stage"` // RATE_NEGOTIATION | BOOKING | CONFIRMATION | FD_MASTER
	RefID       string `json:"ref_id"`
	ActionType  string `json:"action_type"`
	Status      string `json:"processing_status"`
	Reason      string `json:"reason"`
	RequestedBy string `json:"requested_by"`
	RequestedAt string `json:"requested_at"`
	CheckerBy   string `json:"checker_by"`
	CheckerAt   string `json:"checker_at"`
	Comment     string `json:"checker_comment"`
}

// GetFDLifecycleJourney returns the FD picker options or one FD's end-to-end journey.
func GetFDLifecycleJourney(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}
		var req fdJourneyRequest
		_ = json.NewDecoder(r.Body).Decode(&req)

		ctx := r.Context()
		entityFilter, scopeMsg := resolveFDDashboardEntity(ctx, req.EntityID)
		if scopeMsg != "" {
			api.RespondWithError(w, http.StatusForbidden, scopeMsg)
			return
		}

		key := strings.TrimSpace(req.Key)
		if key == "" {
			options, err := fetchFDJourneyOptions(ctx, pool, entityFilter)
			if err != nil {
				logger.LogError("fd lifecycle journey options: %v", err)
				api.RespondWithError(w, http.StatusInternalServerError, "Failed to load FD list")
				return
			}
			api.RespondWithPayload(w, true, "", map[string]interface{}{"options": options})
			return
		}

		journey, found, err := fetchFDJourney(ctx, pool, entityFilter, key)
		if err != nil {
			logger.LogError("fd lifecycle journey %s: %v", key, err)
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to load FD journey")
			return
		}
		if !found {
			api.RespondWithError(w, http.StatusNotFound, "FD / booking not found")
			return
		}
		api.RespondWithPayload(w, true, "", map[string]interface{}{"journey": journey})
	}
}

func fetchFDJourneyOptions(ctx context.Context, pool *pgxpool.Pool, entityFilter string) ([]fdJourneyOption, error) {
	rows, err := pool.Query(ctx, `
		SELECT COALESCE(m.fd_id, b.booking_id) AS value,
		  COALESCE(m.fd_id,''), b.booking_id,
		  COALESCE(m.bank_name, b.bank_name, ''), COALESCE(m.entity_name, b.entity_name, ''),
		  COALESCE(m.principal_amount, b.principal_amount, 0),
		  COALESCE(m.fd_status,''), COALESCE(b.booking_status,'')
		FROM investment.fd_booking_request b
		LEFT JOIN investment.fd_master m
		  ON m.booking_id = b.booking_id AND COALESCE(m.is_deleted,false)=false
		WHERE COALESCE(b.is_deleted,false)=false
		  AND b.entity_id = ANY(string_to_array($1, ','))
		ORDER BY COALESCE(m.created_at, b.created_at) DESC
		LIMIT 500`, entityFilter)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []fdJourneyOption{}
	for rows.Next() {
		var o fdJourneyOption
		if err := rows.Scan(&o.Value, &o.FDID, &o.BookingID, &o.Bank, &o.Entity,
			&o.Principal, &o.FDStatus, &o.BookingStatus); err != nil {
			return nil, err
		}
		o.Principal = fdRound(o.Principal, 2)
		out = append(out, o)
	}
	return out, rows.Err()
}

func fetchFDJourney(ctx context.Context, pool *pgxpool.Pool, entityFilter, key string) (map[string]interface{}, bool, error) {
	// Resolve key (fd_id or booking_id) → booking, scoped to the caller's entities.
	var bk fdJourneyBooking
	err := pool.QueryRow(ctx, `
		SELECT b.booking_id, COALESCE(b.booking_status,''), COALESCE(b.bank_name,''),
		  COALESCE(b.entity_name,''), COALESCE(b.principal_amount,0), COALESCE(b.interest_rate,0),
		  COALESCE(TO_CHAR(b.expected_start_date,'YYYY-MM-DD'),''),
		  COALESCE(TO_CHAR(b.expected_maturity_date,'YYYY-MM-DD'),''),
		  COALESCE(b.created_by,''), COALESCE(TO_CHAR(b.created_at,'YYYY-MM-DD HH24:MI'),''),
		  COALESCE(b.rate_request_id::text,'')
		FROM investment.fd_booking_request b
		WHERE COALESCE(b.is_deleted,false)=false
		  AND b.entity_id = ANY(string_to_array($2, ','))
		  AND (b.booking_id = $1 OR b.booking_id = (
		    SELECT booking_id FROM investment.fd_master
		    WHERE fd_id = $1 AND COALESCE(is_deleted,false)=false LIMIT 1))
		LIMIT 1`, key, entityFilter).
		Scan(&bk.BookingID, &bk.Status, &bk.Bank, &bk.Entity, &bk.Principal, &bk.Rate,
			&bk.StartDate, &bk.MaturityDate, &bk.CreatedBy, &bk.CreatedAt, &bk.RateRequestID)
	if err == pgx.ErrNoRows {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	bk.Principal = fdRound(bk.Principal, 2)
	bk.Rate = fdRound(bk.Rate, 4)

	// Rate negotiation — linked either way (booking.rate_request_id or negotiation.booking_id).
	var rr *fdJourneyRateRequest
	{
		var x fdJourneyRateRequest
		var tenureType string
		var tenureValue int
		err := pool.QueryRow(ctx, `
			SELECT n.rate_request_id::text, n.rate_request_ref, COALESCE(n.request_status,''),
			  COALESCE(TO_CHAR(n.request_date,'YYYY-MM-DD'),''),
			  COALESCE(n.proposed_fd_amount,0), COALESCE(n.currency_code,''),
			  COALESCE(n.tenure_type,''), COALESCE(n.tenure_value,0),
			  COALESCE(n.target_bank_names,'{}'),
			  COALESCE(n.selected_bank_name,''),
			  COALESCE(NULLIF(o.effective_yield,0), o.offered_interest_rate, 0),
			  COALESCE(n.selection_submitted_by,''),
			  COALESCE(TO_CHAR(n.selection_submitted_at,'YYYY-MM-DD HH24:MI'),''),
			  COALESCE(n.approval_decision,''), COALESCE(n.approved_by,''),
			  COALESCE(TO_CHAR(n.approval_date,'YYYY-MM-DD HH24:MI'),''),
			  COALESCE(n.created_by,''), COALESCE(TO_CHAR(n.created_at,'YYYY-MM-DD HH24:MI'),'')
			FROM investment.fd_rate_negotiation n
			LEFT JOIN investment.fd_rate_offer o
			  ON o.offer_id = n.selected_offer_id AND COALESCE(o.is_deleted,false)=false
			WHERE COALESCE(n.is_deleted,false)=false
			  AND (n.rate_request_id::text = NULLIF($1,'') OR n.booking_id = $2)
			ORDER BY (n.rate_request_id::text = NULLIF($1,'')) DESC NULLS LAST, n.created_at DESC
			LIMIT 1`, bk.RateRequestID, bk.BookingID).
			Scan(&x.RateRequestID, &x.Ref, &x.Status, &x.RequestDate, &x.ProposedAmount, &x.Currency,
				&tenureType, &tenureValue, &x.TargetBanks, &x.SelectedBank, &x.SelectedRate,
				&x.SelectionBy, &x.SelectionAt, &x.ApprovalDecision, &x.ApprovedBy, &x.ApprovalDate,
				&x.CreatedBy, &x.CreatedAt)
		if err != nil && err != pgx.ErrNoRows {
			return nil, false, err
		}
		if err == nil {
			if tenureValue > 0 {
				x.Tenure = strings.TrimSpace(strings.Join([]string{strconv.Itoa(tenureValue), strings.ToLower(tenureType)}, " "))
			}
			x.ProposedAmount = fdRound(x.ProposedAmount, 2)
			x.SelectedRate = fdRound(x.SelectedRate, 4)
			x.Offers = []fdJourneyOffer{}
			offerRows, err := pool.Query(ctx, `
				SELECT o.offer_id::text, COALESCE(o.bank_name,''), COALESCE(o.offered_interest_rate,0),
				  COALESCE(o.offer_status,''), COALESCE(TO_CHAR(o.valid_till_date,'YYYY-MM-DD'),''),
				  (o.offer_id = n.selected_offer_id)
				FROM investment.fd_rate_offer o
				JOIN investment.fd_rate_negotiation n ON n.rate_request_id = o.rate_request_id
				WHERE o.rate_request_id = $1::uuid AND COALESCE(o.is_deleted,false)=false
				ORDER BY o.offered_interest_rate DESC NULLS LAST`, x.RateRequestID)
			if err != nil {
				return nil, false, err
			}
			for offerRows.Next() {
				var of fdJourneyOffer
				var sel *bool
				if err := offerRows.Scan(&of.OfferID, &of.Bank, &of.Rate, &of.Status, &of.ValidTill, &sel); err != nil {
					offerRows.Close()
					return nil, false, err
				}
				of.Rate = fdRound(of.Rate, 4)
				of.IsSelected = sel != nil && *sel
				x.Offers = append(x.Offers, of)
			}
			offerRows.Close()
			rr = &x
		}
	}

	// Confirmation — latest non-deleted for the booking.
	var conf *fdJourneyConfirmation
	{
		var c fdJourneyConfirmation
		err := pool.QueryRow(ctx, `
			SELECT confirmation_id::text, COALESCE(confirmation_status,''), COALESCE(bank_fd_ref_no,''),
			  COALESCE(TO_CHAR(confirmation_received_date,'YYYY-MM-DD'),''), COALESCE(confirmation_mode,''),
			  COALESCE(actual_principal,0), COALESCE(confirmed_rate,0),
			  COALESCE(variance_flag,false), COALESCE(variance_action,''),
			  COALESCE(created_by,''), COALESCE(TO_CHAR(created_at,'YYYY-MM-DD HH24:MI'),'')
			FROM investment.fd_confirmation
			WHERE booking_id = $1 AND COALESCE(is_deleted,false)=false
			ORDER BY created_at DESC LIMIT 1`, bk.BookingID).
			Scan(&c.ConfirmationID, &c.Status, &c.BankFDRef, &c.ReceivedDate, &c.Mode,
				&c.Principal, &c.Rate, &c.VarianceFlag, &c.VarianceAction, &c.CreatedBy, &c.CreatedAt)
		if err != nil && err != pgx.ErrNoRows {
			return nil, false, err
		}
		if err == nil {
			c.Principal = fdRound(c.Principal, 2)
			c.Rate = fdRound(c.Rate, 4)
			conf = &c
		}
	}

	// Activation — fd_master row for the booking.
	var fd *fdJourneyFD
	{
		var f fdJourneyFD
		err := pool.QueryRow(ctx, `
			SELECT fd_id, COALESCE(fd_status,''), COALESCE(principal_amount,0), COALESCE(interest_rate,0),
			  COALESCE(TO_CHAR(start_date,'YYYY-MM-DD'),''), COALESCE(TO_CHAR(maturity_date,'YYYY-MM-DD'),''),
			  COALESCE(activated_by,''), COALESCE(TO_CHAR(activated_at,'YYYY-MM-DD HH24:MI'),''),
			  COALESCE(TO_CHAR(created_at,'YYYY-MM-DD HH24:MI'),'')
			FROM investment.fd_master
			WHERE booking_id = $1 AND COALESCE(is_deleted,false)=false
			ORDER BY created_at DESC LIMIT 1`, bk.BookingID).
			Scan(&f.FDID, &f.Status, &f.Principal, &f.Rate, &f.StartDate, &f.MaturityDate,
				&f.ActivatedBy, &f.ActivatedAt, &f.CreatedAt)
		if err != nil && err != pgx.ErrNoRows {
			return nil, false, err
		}
		if err == nil {
			f.Principal = fdRound(f.Principal, 2)
			f.Rate = fdRound(f.Rate, 4)
			fd = &f
		}
	}

	// Unified audit timeline across all four stages.
	rateReqID, confID, fdID := "", "", ""
	if rr != nil {
		rateReqID = rr.RateRequestID
	}
	if conf != nil {
		confID = conf.ConfirmationID
	}
	if fd != nil {
		fdID = fd.FDID
	}
	evRows, err := pool.Query(ctx, `
		SELECT stage, ref_id, action_type, processing_status, reason, requested_by,
		  COALESCE(TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI'),''), checker_by,
		  COALESCE(TO_CHAR(checker_at,'YYYY-MM-DD HH24:MI'),''), checker_comment
		FROM (
		  SELECT 'RATE_NEGOTIATION' AS stage, rate_request_id::text AS ref_id, action_type, processing_status,
		    COALESCE(reason,'') AS reason, COALESCE(requested_by,'') AS requested_by, requested_at,
		    COALESCE(checker_by,'') AS checker_by, checker_at, COALESCE(checker_comment,'') AS checker_comment
		  FROM investment.fd_audit_rate_negotiation WHERE rate_request_id::text = NULLIF($1,'')
		  UNION ALL
		  SELECT 'BOOKING', booking_id, action_type, processing_status, COALESCE(reason,''),
		    COALESCE(requested_by,''), requested_at, COALESCE(checker_by,''), checker_at, COALESCE(checker_comment,'')
		  FROM investment.fd_audit_booking_request WHERE booking_id = $2
		  UNION ALL
		  SELECT 'CONFIRMATION', confirmation_id::text, action_type, processing_status, COALESCE(reason,''),
		    COALESCE(requested_by,''), requested_at, COALESCE(checker_by,''), checker_at, COALESCE(checker_comment,'')
		  FROM investment.fd_audit_confirmation WHERE confirmation_id::text = NULLIF($3,'')
		  UNION ALL
		  SELECT 'FD_MASTER', fd_id, action_type, processing_status, COALESCE(reason,''),
		    COALESCE(requested_by,''), requested_at, COALESCE(checker_by,''), checker_at, COALESCE(checker_comment,'')
		  FROM investment.fd_audit_master WHERE fd_id = NULLIF($4,'')
		) ev
		ORDER BY requested_at ASC`, rateReqID, bk.BookingID, confID, fdID)
	if err != nil {
		return nil, false, err
	}
	defer evRows.Close()
	events := []fdJourneyEvent{}
	for evRows.Next() {
		var e fdJourneyEvent
		if err := evRows.Scan(&e.Stage, &e.RefID, &e.ActionType, &e.Status, &e.Reason,
			&e.RequestedBy, &e.RequestedAt, &e.CheckerBy, &e.CheckerAt, &e.Comment); err != nil {
			return nil, false, err
		}
		events = append(events, e)
	}
	if err := evRows.Err(); err != nil {
		return nil, false, err
	}

	return map[string]interface{}{
		"key":              key,
		"rate_negotiation": rr,
		"booking":          bk,
		"confirmation":     conf,
		"fd":               fd,
		"events":           events,
	}, true, nil
}
