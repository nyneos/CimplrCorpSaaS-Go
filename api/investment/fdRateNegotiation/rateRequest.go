package fdRateNegotiation

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
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

func insertCreateAudit(ctx context.Context, tx pgx.Tx, rateRequestID, userEmail, clientIP string) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO investment.fd_audit_rate_negotiation (
			rate_request_id, action_type, processing_status, requested_by, requested_at, requested_ip
		) VALUES ($1::uuid, 'CREATE', 'PENDING_APPROVAL', $2, now(), $3)`,
		rateRequestID, api.SystemIfBlank(userEmail), api.SystemIfBlank(clientIP),
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
				target_bank_ids, target_bank_names, internal_notes, created_by, created_at
			) VALUES (
				$1, CURRENT_DATE, $2,
				$3, $4, $5, $6,
				$7::date, NULLIF($8,'')::date, $9, NULLIF($10,''),
				$11, $12, NULLIF($13,''), $14, now()
			) RETURNING rate_request_id::text`,
			ref, status,
			req.ProposedFDAmount, strings.ToUpper(strings.TrimSpace(req.CurrencyCode)),
			tenureType, req.TenureValue,
			req.ExpectedStartDate, maturity, interestType, payoutMode,
			bankIDs, bankNames, req.InternalNotes, userEmail,
		).Scan(&id)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, fmt.Sprintf("Create failed: %v", err))
			return
		}

		if err = insertCreateAudit(ctx, tx, id, userEmail, api.ClientIPFromContext(ctx)); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Audit insert failed")
			return
		}

		if err = tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Commit failed")
			return
		}

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
		if oldStatus != "PENDING_APPROVAL" && oldStatus != "REJECTED" && oldStatus != "APPROVED" {
			api.RespondWithError(w, http.StatusBadRequest, "Rate request cannot be edited in current status")
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
				request_status = $13,
				updated_by = $14,
				updated_at = now()
			WHERE rate_request_id = $1::uuid`,
			req.RateRequestID,
			req.ProposedFDAmount, strings.ToUpper(strings.TrimSpace(req.CurrencyCode)),
			tenureType, req.TenureValue,
			req.ExpectedStartDate, maturity, interestType, payoutMode,
			bankIDs, bankNames, req.InternalNotes, newStatus, userEmail,
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
				old_internal_notes, old_request_status, old_is_active
			) VALUES (
				$1::uuid, $2, $3, $4,
				$5, now(), $6,
				$7, $8, $9, $10,
				NULLIF($11,'')::date, NULLIF($12,'')::date, $13,
				$14, $15, $16,
				$17, $18, $19
			)`,
			req.RateRequestID, actionType, newStatus, nullIfEmpty(req.Reason),
			api.SystemIfBlank(userEmail), api.SystemIfBlank(api.ClientIPFromContext(ctx)),
			oldAmount, oldCurrency, oldTenureType, oldTenureValue,
			oldStartStr, oldMaturityStr, oldInterestType,
			oldPayoutStr, oldBankIDs, oldBankNames,
			oldNotesStr, oldStatus, oldIsActive,
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
			"request_status":  newStatus,
		})
	}
}

func scanRateRequest(row pgx.Row) (map[string]interface{}, error) {
	var (
		id, ref, reqDate, status, currency, tenureType, interestType, createdBy, createdAt string
		amount                                                                             float64
		tenureValue                                                                        int
		startDate                                                                          string
		maturity, payout, notes, updatedBy, updatedAt                                      *string
		bankIDs, bankNames                                                                 []string
	)
	err := row.Scan(
		&id, &ref, &reqDate, &status, &amount, &currency, &tenureType, &tenureValue,
		&startDate, &maturity, &interestType, &payout, &bankIDs, &bankNames, &notes,
		&createdBy, &createdAt, &updatedBy, &updatedAt,
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
		"created_by":             createdBy,
		"created_at":             createdAt,
		"updated_by":             "",
		"updated_at":             "",
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
	return out, nil
}

const rateRequestSelect = `
	SELECT
		rate_request_id::text,
		rate_request_ref,
		TO_CHAR(request_date,'YYYY-MM-DD'),
		request_status,
		proposed_fd_amount,
		currency_code,
		tenure_type,
		tenure_value,
		TO_CHAR(expected_start_date,'YYYY-MM-DD'),
		TO_CHAR(expected_maturity_date,'YYYY-MM-DD'),
		interest_type,
		interest_payout_mode,
		target_bank_ids,
		target_bank_names,
		internal_notes,
		created_by,
		TO_CHAR(created_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
		updated_by,
		TO_CHAR(updated_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"')
	FROM investment.fd_rate_negotiation
`

// ListRateRequests returns all non-deleted rate requests.
func ListRateRequests(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		rows, err := pgxPool.Query(ctx, rateRequestSelect+`
			WHERE COALESCE(is_deleted,false)=false
			ORDER BY created_at DESC`)
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
			WHERE rate_request_id = $1::uuid AND COALESCE(is_deleted,false)=false`, req.RateRequestID)
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
		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"results": results,
		})
	}
}
