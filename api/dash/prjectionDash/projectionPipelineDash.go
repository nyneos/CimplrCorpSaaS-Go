package projectiondash

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"time"

	"CimplrCorpSaas/api"

	"github.com/jackc/pgx/v5/pgxpool"
)

type KPIResponse struct {
	TotalProposals    int     `json:"totalProposals"`
	PendingApproval   int     `json:"pendingApproval"`
	ValuePendingNet   float64 `json:"valuePendingNet"`
	ApprovedThisMonth int     `json:"approvedThisMonth"`
}

// Handler: Projection Pipeline KPI Cards
func GetProjectionPipelineKPI(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithResult(w, false, "Method not allowed")
			return
		}
		var body struct {
			UserID string `json:"user_id"`
			// Add filter fields here if needed (entity, department, status, etc.)
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.UserID == "" {
			api.RespondWithResult(w, false, "Missing or invalid user_id in body")
			return
		}

		ctx := context.Background()
		var resp KPIResponse

		// spot rates for conversion to USD (fallbacks)
		spotRates := map[string]float64{
			"USD": 1.0,
			"INR": 0.0117,
			"EUR": 1.09,
			"GBP": 1.28,
			"AUD": 0.68,
			"CAD": 0.75,
			"CHF": 1.1,
			"CNY": 0.14,
			"JPY": 0.0067,
		}

		// 1) Total proposals
		if err := pgxPool.QueryRow(ctx, `SELECT COUNT(*) FROM cashflow_proposal`).Scan(&resp.TotalProposals); err != nil {
			api.RespondWithResult(w, false, "DB error: "+err.Error())
			return
		}

		// 2) Pending approvals - use latest audit processing_status per proposal and count those that are pending
		pendingQ := `
			SELECT COUNT(*) FROM (
				SELECT p.proposal_id,
					   (SELECT a.processing_status FROM audit_action_cashflow_proposal a WHERE a.proposal_id = p.proposal_id ORDER BY requested_at DESC LIMIT 1) AS processing_status
				FROM cashflow_proposal p
			) t
			WHERE processing_status LIKE 'PENDING%'
		`
		if err := pgxPool.QueryRow(ctx, pendingQ).Scan(&resp.PendingApproval); err != nil {
			api.RespondWithResult(w, false, "DB error: "+err.Error())
			return
		}

		// 3) Value Pending (Net) - sum abs(expected_amount) converted to USD for proposals whose latest audit is PENDING_APPROVAL
		sumQ := `
			SELECT p.currency_code, i.expected_amount
			FROM cashflow_proposal p
			JOIN cashflow_proposal_item i ON i.proposal_id = p.proposal_id
			WHERE (SELECT a.processing_status FROM audit_action_cashflow_proposal a WHERE a.proposal_id = p.proposal_id ORDER BY requested_at DESC LIMIT 1) = 'PENDING_APPROVAL'
		`
		rows, err := pgxPool.Query(ctx, sumQ)
		if err != nil {
			api.RespondWithResult(w, false, "DB error: "+err.Error())
			return
		}
		defer rows.Close()
		var total float64
		for rows.Next() {
			var currency string
			var amt float64
			if err := rows.Scan(&currency, &amt); err != nil {
				continue
			}
			rate := 1.0
			if r, ok := spotRates[currency]; ok && r > 0 {
				rate = r
			}
			total += math.Abs(amt) * rate
		}
		resp.ValuePendingNet = total

		// 4) Approved this month - count distinct proposals that were approved (processing_status='APPROVED') with checker_at in current month
		now := time.Now()
		year, month := now.Year(), int(now.Month())
		if err := pgxPool.QueryRow(ctx, `
			SELECT COUNT(DISTINCT proposal_id)
			FROM audit_action_cashflow_proposal
			WHERE processing_status = 'APPROVED'
			  AND checker_at IS NOT NULL
			  AND EXTRACT(YEAR FROM checker_at) = $1
			  AND EXTRACT(MONTH FROM checker_at) = $2
		`, year, month).Scan(&resp.ApprovedThisMonth); err != nil {
			api.RespondWithResult(w, false, "DB error: "+err.Error())
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "kpi": resp})
	}
}

// DetailedPipeline represents a single row in the detailed pipeline grid returned to the client
type DetailedPipeline struct {
	ID          string  `json:"id"`
	Entity      string  `json:"entity"`
	Department  string  `json:"department"`
	SubmittedBy string  `json:"submitted_by"`
	SubmittedOn string  `json:"submitted_on"`
	Amount      float64 `json:"amount"`
	Status      string  `json:"status,omitempty"`
}

// Handler: GetDetailedPipeline - returns rows in the shape of the mock data
// Method: GET
func GetDetailedPipeline(pgxPool *pgxpool.Pool) http.HandlerFunc {
	// simple spot rates to convert native currency -> USD
	spotRates := map[string]float64{
		"USD": 1.0,
		"AUD": 0.68,
		"CAD": 0.75,
		"CHF": 1.1,
		"CNY": 0.14,
		"RMB": 0.14,
		"EUR": 1.09,
		"GBP": 1.28,
		"JPY": 0.0067,
		"SEK": 0.095,
		"INR": 0.0117,
	}

	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithResult(w, false, "Method not allowed")
			return
		}

		var body struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.UserID == "" {
			api.RespondWithResult(w, false, "Missing or invalid user_id in body")
			return
		}

		ctx := context.Background()

		// Query: join proposals -> items and left join latest audit_action per proposal
		rows, err := pgxPool.Query(ctx, `
			SELECT p.proposal_id, i.item_id, COALESCE(i.entity_name, '') AS entity_name, COALESCE(i.department_id, '') AS department_id,
				   i.expected_amount, p.currency_code,
				   a.requested_by, a.requested_at, a.processing_status
			FROM cashflow_proposal p
			JOIN cashflow_proposal_item i ON i.proposal_id = p.proposal_id
			LEFT JOIN LATERAL (
				SELECT requested_by, requested_at, processing_status
				FROM audit_action_cashflow_proposal a
				WHERE a.proposal_id = p.proposal_id
				ORDER BY requested_at DESC
				LIMIT 1
			) a ON true
			ORDER BY a.requested_at DESC NULLS LAST, p.proposal_id, i.item_id
		`)
		if err != nil {
			api.RespondWithResult(w, false, "DB error: "+err.Error())
			return
		}
		defer rows.Close()

		out := []DetailedPipeline{}
		for rows.Next() {
			var proposalID, itemID, entityName, departmentID, currencyCode string
			var expectedAmount float64
			var requestedBy *string
			var requestedAt *time.Time
			var processingStatus *string

			if err := rows.Scan(&proposalID, &itemID, &entityName, &departmentID, &expectedAmount, &currencyCode, &requestedBy, &requestedAt, &processingStatus); err != nil {
				continue
			}

			// build id as proposal:item
			id := proposalID + ":" + itemID

			// convert to USD using spotRates (fallback 1.0)
			rate := 1.0
			if rc, ok := spotRates[currencyCode]; ok && rc > 0 {
				rate = rc
			}
			amount := math.Abs(expectedAmount) * rate

			submittedBy := ""
			if requestedBy != nil {
				submittedBy = *requestedBy
			}
			submittedOn := ""
			if requestedAt != nil {
				submittedOn = requestedAt.Format("2006-01-02")
			}
			status := ""
			if processingStatus != nil {
				status = *processingStatus
			}

			out = append(out, DetailedPipeline{
				ID:          id,
				Entity:      entityName,
				Department:  departmentID,
				SubmittedBy: submittedBy,
				SubmittedOn: submittedOn,
				Amount:      amount,
				Status:      status,
			})
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "rows": out})
	}
}
