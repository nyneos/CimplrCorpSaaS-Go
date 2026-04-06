package fdNotifications

// notif_payload.go — Rich notification payload builders for all FD module events
//
// ─────────────────────────────────────────────────────────────────────────────
// DESIGN PHILOSOPHY
// ─────────────────────────────────────────────────────────────────────────────
// Each Build*NotifPayload function takes a slice of record IDs, fetches the
// full normalized rows from the DB (single query per domain), then produces:
//   • A full Records[] slice  — every affected record with all detail fields
//   • Grouped KPI slices       — by entity, by bank, by closure_type, etc.
//   • Scalar summaries         — Count, TotalAmount / TotalPrincipal / TotalNetPayout
//
// USAGE PATTERN
// ─────────────────────────────────────────────────────────────────────────────
//   payload := fdNotifications.BuildBookingNotifPayload(ctx, pool, []string{id}, "CREATE", userEmail)
//   go notifcatalog.TriggerNotification(ctx, pool, "/investment/fd/booking/create", id, payload.ToMap())
//
// ─────────────────────────────────────────────────────────────────────────────
// TEMPLATE VARIABLES — BOOKING
// ─────────────────────────────────────────────────────────────────────────────
// Scalars : Action, ActorEmail, Count, TotalPrincipal, ActionAt
// Lists   :
//   Bookings        — []map  — full booking rows incl. entity_name, bank_name,
//                              principal_amount, interest_rate, tenor_days,
//                              expected_start_date, maturity_date, booking_status
//   BookingIDs      — []string
//   ByEntityKPIs    — []map{group_name, count, total_principal}
//   ByBankKPIs      — []map{group_name, count, total_principal}
//
// ─────────────────────────────────────────────────────────────────────────────
// TEMPLATE VARIABLES — CONFIRMATION
// ─────────────────────────────────────────────────────────────────────────────
// Scalars : Action, ActorEmail, Count, ActionAt
// Lists   :
//   Confirmations   — []map  — confirmation_id, booking_id, entity_name,
//                              bank_name, bank_fd_ref_no, actual_principal,
//                              confirmed_rate, actual_start_date, actual_maturity_date,
//                              confirmation_status, variance_remarks
//   ConfirmationIDs — []string
//   ByEntityKPIs    — []map{group_name, count}
//   ByBankKPIs      — []map{group_name, count}
//
// ─────────────────────────────────────────────────────────────────────────────
// TEMPLATE VARIABLES — FD MASTER (Activation / Approve / Reject)
// ─────────────────────────────────────────────────────────────────────────────
// Scalars : Action, ActorEmail, Count, TotalPrincipal, ActionAt
// Lists   :
//   FDs             — []map  — fd_id, fd_ref_no, entity_name, bank_name,
//                              principal_amount, interest_rate, tenor_days,
//                              start_date, maturity_date, fd_status
//   FDIDs           — []string
//   ByEntityKPIs    — []map{group_name, count, total_principal}
//   ByBankKPIs      — []map{group_name, count, total_principal}
//
// ─────────────────────────────────────────────────────────────────────────────
// TEMPLATE VARIABLES — CLOSURE
// ─────────────────────────────────────────────────────────────────────────────
// Scalars : Action, ActorEmail, Count, TotalNetPayout, ActionAt
// Lists   :
//   Closures        — []map  — closure_request_id, fd_id, fd_ref_no, entity_name,
//                              bank_name, closure_type, principal_amount, accrued_interest,
//                              tds_deducted, penalty_amount, net_payout_amount,
//                              closure_status, closure_date
//   ClosureIDs      — []string
//   ByTypeKPIs      — []map{group_name, count, total_net_payout}  (PREMATURE/MATURITY/ROLLOVER)
//   ByEntityKPIs    — []map{group_name, count, total_net_payout}
//
// ─────────────────────────────────────────────────────────────────────────────
// TEMPLATE VARIABLES — ACCRUAL
// ─────────────────────────────────────────────────────────────────────────────
// Scalars : Action, ActorEmail, Count, TotalInterestAccrued, ActionAt, RunDate
// Lists   :
//   Accruals        — []map  — fd_id, fd_ref_no, entity_name, bank_name,
//                              accrual_date, daily_interest, cumulative_accrual
//   ByEntityKPIs    — []map{group_name, count, total_interest}
//
// ─────────────────────────────────────────────────────────────────────────────
// TEMPLATE VARIABLES — INTEREST RECEIPT
// ─────────────────────────────────────────────────────────────────────────────
// Scalars : Action, ActorEmail, Count, TotalGrossInterest, TotalNetAmount, ActionAt
// Lists   :
//   Receipts        — []map  — receipt_id, fd_id, fd_ref_no, entity_name,
//                              bank_name, receipt_date, period_start, period_end,
//                              gross_interest_received, tds_amount_deducted, 
//                              other_charges, net_amount_received, receipt_status,
//                              reconcile_status, bank_reference_no, narration
//   ByEntityKPIs    — []map{group_name, count, total_gross_interest}
//   ByBankKPIs      — []map{group_name, count, total_gross_interest}

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ─────────────────────────────────────────────────────────────────────────────
// Shared helpers
// ─────────────────────────────────────────────────────────────────────────────

// fdAnyToFloat64 converts pgx numeric types to float64.
func fdAnyToFloat64(v interface{}) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	case int:
		return float64(n)
	case int32:
		return float64(n)
	case int64:
		return float64(n)
	case string:
		if f, err := strconv.ParseFloat(strings.TrimSpace(n), 64); err == nil {
			return f
		}
	}
	if b, err := json.Marshal(v); err == nil {
		var f float64
		if err2 := json.Unmarshal(b, &f); err2 == nil {
			return f
		}
	}
	return 0
}

// fdStrField safely extracts a string field from a row map.
func fdStrField(row map[string]interface{}, key string) string {
	if v, ok := row[key].(string); ok {
		return v
	}
	return ""
}

// placeholders builds "$1,$2,..." for n items starting from offset.
func placeholders(n, offset int) string {
	parts := make([]string, n)
	for i := range parts {
		parts[i] = fmt.Sprintf("$%d", i+offset)
	}
	return strings.Join(parts, ",")
}

// stringsToAny converts []string to []interface{} for pgx args.
func stringsToAny(ss []string) []interface{} {
	out := make([]interface{}, len(ss))
	for i, s := range ss {
		out[i] = s
	}
	return out
}

// fdKPIToMaps serialises KPI slices for ToMap().
func fdKPIToMaps(rows []FDKPIRow) []map[string]interface{} {
	out := make([]map[string]interface{}, len(rows))
	for i, r := range rows {
		out[i] = map[string]interface{}{
			"group_name":   r.GroupName,
			"count":        r.Count,
			"total_amount": r.TotalAmount,
		}
	}
	return out
}

// FDKPIRow is a generic grouped aggregate reused across all FD domains.
type FDKPIRow struct {
	GroupName   string  `json:"group_name"`
	Count       int     `json:"count"`
	TotalAmount float64 `json:"total_amount"`
}

// computeKPIs groups rows by groupField and sums amountField.
func computeKPIs(rows []map[string]interface{}, groupField, amountField string) []FDKPIRow {
	groups := map[string]*FDKPIRow{}
	for _, row := range rows {
		key := fdStrField(row, groupField)
		if key == "" {
			key = "Unknown"
		}
		if _, ok := groups[key]; !ok {
			groups[key] = &FDKPIRow{GroupName: key}
		}
		groups[key].Count++
		if amountField != "" {
			groups[key].TotalAmount += fdAnyToFloat64(row[amountField])
		}
	}
	out := make([]FDKPIRow, 0, len(groups))
	for _, kpi := range groups {
		out = append(out, *kpi)
	}
	return out
}

// ─────────────────────────────────────────────────────────────────────────────
// BOOKING
// ─────────────────────────────────────────────────────────────────────────────

// BookingNotifPayload is the rich notification payload for FD booking events.
type BookingNotifPayload struct {
	Action         string           `json:"Action"`
	ActorEmail     string           `json:"ActorEmail"`
	Count          int              `json:"Count"`
	TotalPrincipal float64          `json:"TotalPrincipal"`
	ActionAt       string           `json:"ActionAt"`
	Bookings       []map[string]interface{} `json:"Bookings"`
	BookingIDs     []string         `json:"BookingIDs"`
	ByEntityKPIs   []FDKPIRow       `json:"ByEntityKPIs"`
	ByBankKPIs     []FDKPIRow       `json:"ByBankKPIs"`
}

// ToMap converts BookingNotifPayload to map[string]interface{} for TriggerNotification.
func (p *BookingNotifPayload) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"Action":         p.Action,
		"ActorEmail":     p.ActorEmail,
		"Count":          p.Count,
		"TotalPrincipal": p.TotalPrincipal,
		"ActionAt":       p.ActionAt,
		"Bookings":       p.Bookings,
		"BookingIDs":     p.BookingIDs,
		"ByEntityKPIs":   fdKPIToMaps(p.ByEntityKPIs),
		"ByBankKPIs":     fdKPIToMaps(p.ByBankKPIs),
	}
}

// fetchBookingRows fetches full booking detail rows for the given IDs.
func fetchBookingRows(ctx context.Context, pool *pgxpool.Pool, ids []string) ([]map[string]interface{}, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	q := fmt.Sprintf(`
		SELECT
			b.booking_id,
			COALESCE(b.entity_id,'')                                            AS entity_id,
			COALESCE(b.entity_name,'')                                          AS entity_name,
			COALESCE(b.bank_id,'')                                              AS bank_id,
			COALESCE(b.bank_name,'')                                            AS bank_name,
			COALESCE(b.principal_amount,0)                                      AS principal_amount,
			COALESCE(b.interest_rate,0)                                         AS interest_rate,
			COALESCE(b.interest_type_code,'')                                   AS interest_type_code,
			COALESCE(b.tenure_days,0)                                           AS tenor_days,
			COALESCE(b.tenure_months,0)                                         AS tenor_months,
			COALESCE(TO_CHAR(b.expected_start_date,'YYYY-MM-DD'),'')            AS expected_start_date,
			COALESCE(TO_CHAR(b.value_date,'YYYY-MM-DD'),'')                     AS value_date,
			COALESCE(TO_CHAR(b.expected_maturity_date,'YYYY-MM-DD'),'')         AS maturity_date,
			COALESCE(b.frequency_id,'')                                         AS frequency_id,
			COALESCE(b.day_count_code,'')                                       AS day_count_code,
			COALESCE(b.tds_plan_id,'')                                          AS tds_plan_id,
			COALESCE(b.product_code,'')                                         AS product_code,
			COALESCE(b.auto_renewal,false)                                      AS auto_renewal,
			COALESCE(b.booking_remarks,'')                                      AS booking_remarks,
			COALESCE(b.booking_status,'')                                       AS booking_status,
			COALESCE(b.bank_config_id,'')                                       AS bank_config_id,
			COALESCE(b.source_account_id,'')                                    AS source_account_id,
			COALESCE(TO_CHAR(b.created_at,'YYYY-MM-DD HH24:MI:SS'),'')         AS created_at,
			COALESCE(b.created_by,'')                                           AS created_by,
			-- latest audit snapshot
			COALESCE(la.processing_status,'')                                   AS processing_status,
			COALESCE(la.requested_by,'')                                        AS requested_by,
			COALESCE(TO_CHAR(la.requested_at,'YYYY-MM-DD HH24:MI:SS'),'')      AS requested_at,
			COALESCE(la.checker_by,'')                                          AS checker_by,
			COALESCE(TO_CHAR(la.checker_at,'YYYY-MM-DD HH24:MI:SS'),'')        AS checker_at,
			COALESCE(la.checker_comment,'')                                     AS checker_comment,
			COALESCE(la.reason,'')                                              AS reason
		FROM investment.fd_booking_request b
		LEFT JOIN LATERAL (
			SELECT processing_status, requested_by, requested_at,
				   checker_by, checker_at, checker_comment, reason
			FROM investment.fd_audit_booking_request
			WHERE booking_id = b.booking_id
			ORDER BY GREATEST(COALESCE(requested_at,'1970-01-01'::timestamp),
							  COALESCE(checker_at,'1970-01-01'::timestamp)) DESC
			LIMIT 1
		) la ON true
		WHERE b.booking_id IN (%s)
		  AND COALESCE(b.is_deleted,false) = false
		ORDER BY b.created_at DESC
	`, placeholders(len(ids), 1))

	rows, err := pool.Query(ctx, q, stringsToAny(ids)...)
	if err != nil {
		return nil, fmt.Errorf("fetchBookingRows: %w", err)
	}
	defer rows.Close()

	var out []map[string]interface{}
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			continue
		}
		cols := rows.FieldDescriptions()
		row := make(map[string]interface{}, len(cols))
		for i, col := range cols {
			row[string(col.Name)] = vals[i]
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// BuildBookingNotifPayload constructs a rich notification payload for booking events.
// Pass action = "CREATE" | "UPDATE" | "DELETE" | "APPROVE" | "REJECT" | "SENT_TO_BANK".
func BuildBookingNotifPayload(
	ctx context.Context,
	pool *pgxpool.Pool,
	bookingIDs []string,
	action string,
	actorEmail string,
) *BookingNotifPayload {
	p := &BookingNotifPayload{
		Action:       action,
		ActorEmail:   actorEmail,
		Count:        len(bookingIDs),
		ActionAt:     time.Now().Format(time.RFC3339),
		Bookings:     []map[string]interface{}{},
		BookingIDs:   bookingIDs,
		ByEntityKPIs: []FDKPIRow{},
		ByBankKPIs:   []FDKPIRow{},
	}
	if len(bookingIDs) == 0 {
		return p
	}
	rows, err := fetchBookingRows(ctx, pool, bookingIDs)
	if err != nil {
		fmt.Printf("[ERROR] BuildBookingNotifPayload: %v\n", err)
		return p
	}
	p.Bookings = rows
	p.ByEntityKPIs = computeKPIs(rows, "entity_name", "principal_amount")
	p.ByBankKPIs = computeKPIs(rows, "bank_name", "principal_amount")
	seen := map[string]bool{}
	ids := make([]string, 0, len(rows))
	for _, row := range rows {
		if id := fdStrField(row, "booking_id"); id != "" && !seen[id] {
			seen[id] = true
			ids = append(ids, id)
		}
		p.TotalPrincipal += fdAnyToFloat64(row["principal_amount"])
	}
	p.BookingIDs = ids
	return p
}

// ─────────────────────────────────────────────────────────────────────────────
// CONFIRMATION
// ─────────────────────────────────────────────────────────────────────────────

// ConfirmationNotifPayload is the rich notification payload for FD confirmation events.
type ConfirmationNotifPayload struct {
	Action          string                   `json:"Action"`
	ActorEmail      string                   `json:"ActorEmail"`
	Count           int                      `json:"Count"`
	ActionAt        string                   `json:"ActionAt"`
	Confirmations   []map[string]interface{} `json:"Confirmations"`
	ConfirmationIDs []string                 `json:"ConfirmationIDs"`
	ByEntityKPIs    []FDKPIRow               `json:"ByEntityKPIs"`
	ByBankKPIs      []FDKPIRow               `json:"ByBankKPIs"`
}

// ToMap converts ConfirmationNotifPayload to map[string]interface{} for TriggerNotification.
func (p *ConfirmationNotifPayload) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"Action":          p.Action,
		"ActorEmail":      p.ActorEmail,
		"Count":           p.Count,
		"ActionAt":        p.ActionAt,
		"Confirmations":   p.Confirmations,
		"ConfirmationIDs": p.ConfirmationIDs,
		"ByEntityKPIs":    fdKPIToMaps(p.ByEntityKPIs),
		"ByBankKPIs":      fdKPIToMaps(p.ByBankKPIs),
	}
}

func fetchConfirmationRows(ctx context.Context, pool *pgxpool.Pool, ids []string) ([]map[string]interface{}, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	q := fmt.Sprintf(`
		SELECT
			c.confirmation_id,
			COALESCE(c.booking_id,'')                                           AS booking_id,
			COALESCE(b.entity_id,'')                                            AS entity_id,
			COALESCE(b.entity_name,'')                                          AS entity_name,
			COALESCE(b.bank_id,'')                                              AS bank_id,
			COALESCE(b.bank_name,'')                                            AS bank_name,
			COALESCE(c.bank_fd_ref_no,'')                                       AS bank_fd_ref_no,
			COALESCE(c.actual_principal,b.principal_amount,0)                   AS actual_principal,
			COALESCE(c.confirmed_rate,b.interest_rate,0)                        AS confirmed_rate,
			COALESCE(b.interest_rate,0)                                         AS booked_interest_rate,
			COALESCE(b.principal_amount,0)                                      AS booked_principal,
			COALESCE(b.tenure_days,0)                                           AS booked_tenor_days,
			COALESCE(TO_CHAR(c.actual_start_date,'YYYY-MM-DD'),'')              AS actual_start_date,
			COALESCE(TO_CHAR(c.actual_maturity_date,'YYYY-MM-DD'),'')           AS actual_maturity_date,
			COALESCE(c.confirmation_status,'')                                  AS confirmation_status,
			COALESCE(c.variance_remarks,'')                                     AS variance_remarks,
			COALESCE(c.has_variance,false)                                      AS has_variance,
			COALESCE(c.variance_flag,'')                                        AS variance_flag,
			COALESCE(TO_CHAR(c.created_at,'YYYY-MM-DD HH24:MI:SS'),'')         AS created_at,
			COALESCE(c.created_by,'')                                           AS created_by,
			-- latest audit
			COALESCE(la.processing_status,'')                                   AS processing_status,
			COALESCE(la.requested_by,'')                                        AS requested_by,
			COALESCE(TO_CHAR(la.requested_at,'YYYY-MM-DD HH24:MI:SS'),'')      AS requested_at,
			COALESCE(la.checker_by,'')                                          AS checker_by,
			COALESCE(TO_CHAR(la.checker_at,'YYYY-MM-DD HH24:MI:SS'),'')        AS checker_at
		FROM investment.fd_confirmation c
		LEFT JOIN investment.fd_booking_request b ON b.booking_id = c.booking_id
		LEFT JOIN LATERAL (
			SELECT processing_status, requested_by, requested_at, checker_by, checker_at
			FROM investment.fd_audit_confirmation
			WHERE confirmation_id = c.confirmation_id
			ORDER BY GREATEST(COALESCE(requested_at,'1970-01-01'::timestamp),
							  COALESCE(checker_at,'1970-01-01'::timestamp)) DESC
			LIMIT 1
		) la ON true
		WHERE c.confirmation_id IN (%s)
		  AND COALESCE(c.is_deleted,false) = false
		ORDER BY c.created_at DESC
	`, placeholders(len(ids), 1))

	rows, err := pool.Query(ctx, q, stringsToAny(ids)...)
	if err != nil {
		return nil, fmt.Errorf("fetchConfirmationRows: %w", err)
	}
	defer rows.Close()

	var out []map[string]interface{}
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			continue
		}
		cols := rows.FieldDescriptions()
		row := make(map[string]interface{}, len(cols))
		for i, col := range cols {
			row[string(col.Name)] = vals[i]
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// BuildConfirmationNotifPayload constructs a rich notification payload for confirmation events.
// Pass action = "CAPTURE" | "RESOLVE_VARIANCE" | "APPROVE" | "REJECT" | "DELETE".
func BuildConfirmationNotifPayload(
	ctx context.Context,
	pool *pgxpool.Pool,
	confirmationIDs []string,
	action string,
	actorEmail string,
) *ConfirmationNotifPayload {
	p := &ConfirmationNotifPayload{
		Action:          action,
		ActorEmail:      actorEmail,
		Count:           len(confirmationIDs),
		ActionAt:        time.Now().Format(time.RFC3339),
		Confirmations:   []map[string]interface{}{},
		ConfirmationIDs: confirmationIDs,
		ByEntityKPIs:    []FDKPIRow{},
		ByBankKPIs:      []FDKPIRow{},
	}
	if len(confirmationIDs) == 0 {
		return p
	}
	rows, err := fetchConfirmationRows(ctx, pool, confirmationIDs)
	if err != nil {
		fmt.Printf("[ERROR] BuildConfirmationNotifPayload: %v\n", err)
		return p
	}
	p.Confirmations = rows
	p.ByEntityKPIs = computeKPIs(rows, "entity_name", "")
	p.ByBankKPIs = computeKPIs(rows, "bank_name", "")
	seen := map[string]bool{}
	ids := make([]string, 0, len(rows))
	for _, row := range rows {
		if id := fdStrField(row, "confirmation_id"); id != "" && !seen[id] {
			seen[id] = true
			ids = append(ids, id)
		}
	}
	p.ConfirmationIDs = ids
	return p
}

// ─────────────────────────────────────────────────────────────────────────────
// FD MASTER (Activation / Approve / Reject)
// ─────────────────────────────────────────────────────────────────────────────

// FDMasterNotifPayload is the rich notification payload for fd_master events.
type FDMasterNotifPayload struct {
	Action         string                   `json:"Action"`
	ActorEmail     string                   `json:"ActorEmail"`
	Count          int                      `json:"Count"`
	TotalPrincipal float64                  `json:"TotalPrincipal"`
	ActionAt       string                   `json:"ActionAt"`
	FDs            []map[string]interface{} `json:"FDs"`
	FDIDs          []string                 `json:"FDIDs"`
	ByEntityKPIs   []FDKPIRow               `json:"ByEntityKPIs"`
	ByBankKPIs     []FDKPIRow               `json:"ByBankKPIs"`
}

// ToMap converts FDMasterNotifPayload to map[string]interface{} for TriggerNotification.
func (p *FDMasterNotifPayload) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"Action":         p.Action,
		"ActorEmail":     p.ActorEmail,
		"Count":          p.Count,
		"TotalPrincipal": p.TotalPrincipal,
		"ActionAt":       p.ActionAt,
		"FDs":            p.FDs,
		"FDIDs":          p.FDIDs,
		"ByEntityKPIs":   fdKPIToMaps(p.ByEntityKPIs),
		"ByBankKPIs":     fdKPIToMaps(p.ByBankKPIs),
	}
}

func fetchFDMasterRows(ctx context.Context, pool *pgxpool.Pool, ids []string) ([]map[string]interface{}, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	q := fmt.Sprintf(`
		SELECT
			m.fd_id,
			COALESCE(m.booking_id,'')                                           AS booking_id,
			COALESCE(m.confirmation_id,'')                                      AS confirmation_id,
			COALESCE(m.entity_id,'')                                            AS entity_id,
			COALESCE(m.entity_name,'')                                          AS entity_name,
			COALESCE(m.bank_id,'')                                              AS bank_id,
			COALESCE(m.bank_name,'')                                            AS bank_name,
			COALESCE(m.bank_fd_ref_no,'')                                       AS bank_fd_ref_no,
			COALESCE(m.principal_amount,0)                                      AS principal_amount,
			COALESCE(m.interest_rate,0)                                         AS interest_rate,
			COALESCE(m.interest_type_code,'')                                   AS interest_type_code,
			COALESCE(m.tenure_days,0)                                           AS tenor_days,
			COALESCE(m.frequency_id,'')                                         AS frequency_id,
			COALESCE(m.day_count_code,'')                                       AS day_count_code,
			COALESCE(m.tds_plan_id,'')                                          AS tds_plan_id,
			COALESCE(TO_CHAR(m.start_date,'YYYY-MM-DD'),'')                     AS start_date,
			COALESCE(TO_CHAR(m.maturity_date,'YYYY-MM-DD'),'')                  AS maturity_date,
			COALESCE(m.fd_status,'')                                            AS fd_status,
			COALESCE(m.source_closure_id,'')                                    AS source_closure_id,
			COALESCE(m.cashflow_generated,false)                                AS cashflow_generated,
			COALESCE(TO_CHAR(m.created_at,'YYYY-MM-DD HH24:MI:SS'),'')         AS created_at,
			COALESCE(m.created_by,'')                                           AS created_by,
			-- latest audit
			COALESCE(la.processing_status,'')                                   AS processing_status,
			COALESCE(la.requested_by,'')                                        AS requested_by,
			COALESCE(TO_CHAR(la.requested_at,'YYYY-MM-DD HH24:MI:SS'),'')      AS requested_at,
			COALESCE(la.checker_by,'')                                          AS checker_by,
			COALESCE(TO_CHAR(la.checker_at,'YYYY-MM-DD HH24:MI:SS'),'')        AS checker_at,
			COALESCE(la.action_type,'')                                         AS action_type
		FROM investment.fd_master m
		LEFT JOIN LATERAL (
			SELECT processing_status, requested_by, requested_at,
				   checker_by, checker_at, action_type
			FROM investment.fd_audit_master
			WHERE fd_id = m.fd_id
			ORDER BY GREATEST(COALESCE(requested_at,'1970-01-01'::timestamp),
							  COALESCE(checker_at,'1970-01-01'::timestamp)) DESC
			LIMIT 1
		) la ON true
		WHERE m.fd_id IN (%s)
		  AND COALESCE(m.is_deleted,false) = false
		ORDER BY m.created_at DESC
	`, placeholders(len(ids), 1))

	rows, err := pool.Query(ctx, q, stringsToAny(ids)...)
	if err != nil {
		return nil, fmt.Errorf("fetchFDMasterRows: %w", err)
	}
	defer rows.Close()

	var out []map[string]interface{}
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			continue
		}
		cols := rows.FieldDescriptions()
		row := make(map[string]interface{}, len(cols))
		for i, col := range cols {
			row[string(col.Name)] = vals[i]
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// BuildFDMasterNotifPayload constructs a rich notification payload for fd_master events.
// Pass action = "ACTIVATE" | "APPROVE" | "REJECT".
func BuildFDMasterNotifPayload(
	ctx context.Context,
	pool *pgxpool.Pool,
	fdIDs []string,
	action string,
	actorEmail string,
) *FDMasterNotifPayload {
	p := &FDMasterNotifPayload{
		Action:       action,
		ActorEmail:   actorEmail,
		Count:        len(fdIDs),
		ActionAt:     time.Now().Format(time.RFC3339),
		FDs:          []map[string]interface{}{},
		FDIDs:        fdIDs,
		ByEntityKPIs: []FDKPIRow{},
		ByBankKPIs:   []FDKPIRow{},
	}
	if len(fdIDs) == 0 {
		return p
	}
	rows, err := fetchFDMasterRows(ctx, pool, fdIDs)
	if err != nil {
		fmt.Printf("[ERROR] BuildFDMasterNotifPayload: %v\n", err)
		return p
	}
	p.FDs = rows
	p.ByEntityKPIs = computeKPIs(rows, "entity_name", "principal_amount")
	p.ByBankKPIs = computeKPIs(rows, "bank_name", "principal_amount")
	seen := map[string]bool{}
	ids := make([]string, 0, len(rows))
	for _, row := range rows {
		if id := fdStrField(row, "fd_id"); id != "" && !seen[id] {
			seen[id] = true
			ids = append(ids, id)
		}
		p.TotalPrincipal += fdAnyToFloat64(row["principal_amount"])
	}
	p.FDIDs = ids
	return p
}

// ─────────────────────────────────────────────────────────────────────────────
// CLOSURE
// ─────────────────────────────────────────────────────────────────────────────

// ClosureNotifPayload is the rich notification payload for FD closure events.
type ClosureNotifPayload struct {
	Action         string                   `json:"Action"`
	ActorEmail     string                   `json:"ActorEmail"`
	Count          int                      `json:"Count"`
	TotalNetPayout float64                  `json:"TotalNetPayout"`
	ActionAt       string                   `json:"ActionAt"`
	Closures       []map[string]interface{} `json:"Closures"`
	ClosureIDs     []string                 `json:"ClosureIDs"`
	ByTypeKPIs     []FDKPIRow               `json:"ByTypeKPIs"`
	ByEntityKPIs   []FDKPIRow               `json:"ByEntityKPIs"`
}

// ToMap converts ClosureNotifPayload to map[string]interface{} for TriggerNotification.
func (p *ClosureNotifPayload) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"Action":         p.Action,
		"ActorEmail":     p.ActorEmail,
		"Count":          p.Count,
		"TotalNetPayout": p.TotalNetPayout,
		"ActionAt":       p.ActionAt,
		"Closures":       p.Closures,
		"ClosureIDs":     p.ClosureIDs,
		"ByTypeKPIs":     fdKPIToMaps(p.ByTypeKPIs),
		"ByEntityKPIs":   fdKPIToMaps(p.ByEntityKPIs),
	}
}

func fetchClosureRows(ctx context.Context, pool *pgxpool.Pool, ids []string) ([]map[string]interface{}, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	q := fmt.Sprintf(`
		SELECT
			cr.closure_request_id,
			COALESCE(cr.fd_id,'')                                               AS fd_id,
			COALESCE(m.bank_fd_ref_no,'')                                       AS fd_ref_no,
			COALESCE(cr.entity_id,'')                                           AS entity_id,
			COALESCE(cr.entity_name,'')                                         AS entity_name,
			COALESCE(m.bank_name, cr.bank_id,'')                                AS bank_name,
			COALESCE(cr.closure_type,'')                                        AS closure_type,
			COALESCE(cr.principal_amount,0)                                     AS principal_amount,
			COALESCE(cr.accrued_interest,0)                                     AS accrued_interest,
			COALESCE(cr.tds_deducted,0)                                         AS tds_deducted,
			COALESCE(cr.penalty_amount,0)                                       AS penalty_amount,
			COALESCE(cr.net_payout_amount,0)                                    AS net_payout_amount,
			COALESCE(cr.closure_status,'')                                      AS closure_status,
			COALESCE(TO_CHAR(cr.closure_date,'YYYY-MM-DD'),'')                  AS closure_date,
			COALESCE(TO_CHAR(cr.created_at,'YYYY-MM-DD HH24:MI:SS'),'')        AS created_at,
			COALESCE(cr.created_by,'')                                          AS created_by,
			COALESCE(cr.rejection_reason,'')                                    AS rejection_reason,
			COALESCE(cr.closure_remarks,'')                                     AS closure_remarks,
			-- rollover info if applicable
			COALESCE(cr.rollover_fd_id,'')                                      AS rollover_fd_id,
			-- latest audit
			COALESCE(la.processing_status,'')                                   AS processing_status,
			COALESCE(la.requested_by,'')                                        AS requested_by,
			COALESCE(TO_CHAR(la.requested_at,'YYYY-MM-DD HH24:MI:SS'),'')      AS requested_at,
			COALESCE(la.checker_by,'')                                          AS checker_by,
			COALESCE(TO_CHAR(la.checker_at,'YYYY-MM-DD HH24:MI:SS'),'')        AS checker_at,
			COALESCE(la.action_type,'')                                         AS action_type
		FROM investment.fd_closure_request cr
		LEFT JOIN investment.fd_master m ON m.fd_id = cr.fd_id AND COALESCE(m.is_deleted,false)=false
		LEFT JOIN LATERAL (
			SELECT processing_status, requested_by, requested_at,
				   checker_by, checker_at, action_type
			FROM investment.fd_audit_closure_request
			WHERE closure_request_id = cr.closure_request_id
			ORDER BY GREATEST(COALESCE(requested_at,'1970-01-01'::timestamp),
							  COALESCE(checker_at,'1970-01-01'::timestamp)) DESC
			LIMIT 1
		) la ON true
		WHERE cr.closure_request_id IN (%s)
		  AND COALESCE(cr.is_deleted,false) = false
		ORDER BY cr.created_at DESC
	`, placeholders(len(ids), 1))

	rows, err := pool.Query(ctx, q, stringsToAny(ids)...)
	if err != nil {
		return nil, fmt.Errorf("fetchClosureRows: %w", err)
	}
	defer rows.Close()

	var out []map[string]interface{}
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			continue
		}
		cols := rows.FieldDescriptions()
		row := make(map[string]interface{}, len(cols))
		for i, col := range cols {
			row[string(col.Name)] = vals[i]
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// BuildClosureNotifPayload constructs a rich notification payload for closure events.
// Pass action = "INITIATE" | "UPDATE" | "APPROVE" | "REJECT" | "DELETE".
func BuildClosureNotifPayload(
	ctx context.Context,
	pool *pgxpool.Pool,
	closureIDs []string,
	action string,
	actorEmail string,
) *ClosureNotifPayload {
	p := &ClosureNotifPayload{
		Action:       action,
		ActorEmail:   actorEmail,
		Count:        len(closureIDs),
		ActionAt:     time.Now().Format(time.RFC3339),
		Closures:     []map[string]interface{}{},
		ClosureIDs:   closureIDs,
		ByTypeKPIs:   []FDKPIRow{},
		ByEntityKPIs: []FDKPIRow{},
	}
	if len(closureIDs) == 0 {
		return p
	}
	rows, err := fetchClosureRows(ctx, pool, closureIDs)
	if err != nil {
		fmt.Printf("[ERROR] BuildClosureNotifPayload: %v\n", err)
		return p
	}
	p.Closures = rows
	p.ByTypeKPIs = computeKPIs(rows, "closure_type", "net_payout_amount")
	p.ByEntityKPIs = computeKPIs(rows, "entity_name", "net_payout_amount")
	seen := map[string]bool{}
	ids := make([]string, 0, len(rows))
	for _, row := range rows {
		if id := fdStrField(row, "closure_request_id"); id != "" && !seen[id] {
			seen[id] = true
			ids = append(ids, id)
		}
		p.TotalNetPayout += fdAnyToFloat64(row["net_payout_amount"])
	}
	p.ClosureIDs = ids
	return p
}

// ─────────────────────────────────────────────────────────────────────────────
// ACCRUAL
// ─────────────────────────────────────────────────────────────────────────────

// AccrualNotifPayload is the rich notification payload for FD accrual run events.
type AccrualNotifPayload struct {
	Action               string                   `json:"Action"`
	ActorEmail           string                   `json:"ActorEmail"`
	Count                int                      `json:"Count"`
	TotalInterestAccrued float64                  `json:"TotalInterestAccrued"`
	RunDate              string                   `json:"RunDate"`
	ActionAt             string                   `json:"ActionAt"`
	Accruals             []map[string]interface{} `json:"Accruals"`
	ByEntityKPIs         []FDKPIRow               `json:"ByEntityKPIs"`
	ByBankKPIs           []FDKPIRow               `json:"ByBankKPIs"`
}

// ToMap converts AccrualNotifPayload to map[string]interface{} for TriggerNotification.
func (p *AccrualNotifPayload) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"Action":               p.Action,
		"ActorEmail":           p.ActorEmail,
		"Count":                p.Count,
		"TotalInterestAccrued": p.TotalInterestAccrued,
		"RunDate":              p.RunDate,
		"ActionAt":             p.ActionAt,
		"Accruals":             p.Accruals,
		"ByEntityKPIs":         fdKPIToMaps(p.ByEntityKPIs),
		"ByBankKPIs":           fdKPIToMaps(p.ByBankKPIs),
	}
}

// BuildAccrualNotifPayload constructs a rich notification payload for an accrual run.
// Pass fdIDs = all FDs processed in the run, runDate = "YYYY-MM-DD".
func BuildAccrualNotifPayload(
	ctx context.Context,
	pool *pgxpool.Pool,
	fdIDs []string,
	runDate string,
	actorEmail string,
) *AccrualNotifPayload {
	p := &AccrualNotifPayload{
		Action:     "ACCRUAL_RUN",
		ActorEmail: actorEmail,
		Count:      len(fdIDs),
		RunDate:    runDate,
		ActionAt:   time.Now().Format(time.RFC3339),
		Accruals:   []map[string]interface{}{},
		ByEntityKPIs: []FDKPIRow{},
		ByBankKPIs:   []FDKPIRow{},
	}
	if len(fdIDs) == 0 || runDate == "" {
		return p
	}

	q := fmt.Sprintf(`
		SELECT
			a.fd_id,
			COALESCE(m.bank_fd_ref_no,'')                                       AS fd_ref_no,
			COALESCE(m.entity_name,'')                                          AS entity_name,
			COALESCE(m.bank_name,'')                                            AS bank_name,
			COALESCE(TO_CHAR(a.accrual_date,'YYYY-MM-DD'),'')                   AS accrual_date,
			COALESCE(a.daily_interest_amount,0)                                 AS daily_interest,
			COALESCE(a.cumulative_accrual,0)                                    AS cumulative_accrual,
			COALESCE(a.tds_accrued,0)                                           AS tds_accrued,
			COALESCE(m.principal_amount,0)                                      AS principal_amount,
			COALESCE(m.interest_rate,0)                                         AS interest_rate,
			COALESCE(TO_CHAR(m.maturity_date,'YYYY-MM-DD'),'')                  AS maturity_date
		FROM investment.fd_accrual_log a
		JOIN investment.fd_master m ON m.fd_id = a.fd_id
		WHERE a.fd_id IN (%s)
		  AND TO_CHAR(a.accrual_date,'YYYY-MM-DD') = $%d
		ORDER BY m.entity_name, m.bank_name
	`, placeholders(len(fdIDs), 1), len(fdIDs)+1)

	args := append(stringsToAny(fdIDs), runDate)
	rows, err := pool.Query(ctx, q, args...)
	if err != nil {
		fmt.Printf("[ERROR] BuildAccrualNotifPayload: %v\n", err)
		return p
	}
	defer rows.Close()

	var accruals []map[string]interface{}
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			continue
		}
		cols := rows.FieldDescriptions()
		row := make(map[string]interface{}, len(cols))
		for i, col := range cols {
			row[string(col.Name)] = vals[i]
		}
		accruals = append(accruals, row)
	}
	if rows.Err() == nil {
		p.Accruals = accruals
		p.ByEntityKPIs = computeKPIs(accruals, "entity_name", "daily_interest")
		p.ByBankKPIs = computeKPIs(accruals, "bank_name", "daily_interest")
		for _, row := range accruals {
			p.TotalInterestAccrued += fdAnyToFloat64(row["daily_interest"])
		}
	}
	return p
}

// ─────────────────────────────────────────────────────────────────────────────
// INTEREST RECEIPT
// ─────────────────────────────────────────────────────────────────────────────

// ReceiptNotifPayload is the rich notification payload for FD interest receipt events.
type ReceiptNotifPayload struct {
	Action             string                   `json:"Action"`
	ActorEmail         string                   `json:"ActorEmail"`
	Count              int                      `json:"Count"`
	TotalGrossInterest float64                  `json:"TotalGrossInterest"`
	TotalNetAmount     float64                  `json:"TotalNetAmount"`
	ActionAt           string                   `json:"ActionAt"`
	Receipts           []map[string]interface{} `json:"Receipts"`
	ReceiptIDs         []string                 `json:"ReceiptIDs"`
	ByEntityKPIs       []FDKPIRow               `json:"ByEntityKPIs"`
	ByBankKPIs         []FDKPIRow               `json:"ByBankKPIs"`
}

// ToMap converts ReceiptNotifPayload to map[string]interface{} for TriggerNotification.
func (p *ReceiptNotifPayload) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"Action":             p.Action,
		"ActorEmail":         p.ActorEmail,
		"Count":              p.Count,
		"TotalGrossInterest": p.TotalGrossInterest,
		"TotalNetAmount":     p.TotalNetAmount,
		"ActionAt":           p.ActionAt,
		"Receipts":           p.Receipts,
		"ReceiptIDs":         p.ReceiptIDs,
		"ByEntityKPIs":       fdKPIToMaps(p.ByEntityKPIs),
		"ByBankKPIs":         fdKPIToMaps(p.ByBankKPIs),
	}
}

func fetchReceiptRows(ctx context.Context, pool *pgxpool.Pool, ids []string) ([]map[string]interface{}, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	q := fmt.Sprintf(`
		SELECT
			r.receipt_id,
			COALESCE(r.fd_id,'')                                                AS fd_id,
			COALESCE(r.fd_ref_no,'')                                            AS fd_ref_no,
			COALESCE(r.entity_id,'')                                            AS entity_id,
			COALESCE(r.entity_name,'')                                          AS entity_name,
			COALESCE(r.bank_id,'')                                              AS bank_id,
			COALESCE(r.bank_name,'')                                            AS bank_name,
			COALESCE(TO_CHAR(r.receipt_date,'YYYY-MM-DD'),'')                   AS receipt_date,
			COALESCE(TO_CHAR(r.period_start,'YYYY-MM-DD'),'')                   AS period_start,
			COALESCE(TO_CHAR(r.period_end,'YYYY-MM-DD'),'')                     AS period_end,
			COALESCE(r.gross_interest_received,0)                               AS gross_interest_received,
			COALESCE(r.tds_amount_deducted,0)                                   AS tds_amount_deducted,
			COALESCE(r.other_charges,0)                                         AS other_charges,
			COALESCE(r.net_amount_received,0)                                   AS net_amount_received,
			COALESCE(r.currency,'INR')                                          AS currency,
			COALESCE(r.bank_reference_no,'')                                    AS bank_reference_no,
			COALESCE(r.narration,'')                                            AS narration,
			COALESCE(r.receipt_status,'')                                       AS receipt_status,
			COALESCE(r.reconcile_status,'')                                     AS reconcile_status,
			COALESCE(r.reconcile_run_id,'')                                     AS reconcile_run_id,
			COALESCE(r.journal_entry_id,'')                                     AS journal_entry_id,
			COALESCE(r.is_active,true)                                          AS is_active,
			COALESCE(TO_CHAR(r.created_at,'YYYY-MM-DD HH24:MI:SS'),'')         AS created_at,
			COALESCE(r.created_by,'')                                           AS created_by,
			-- latest audit
			COALESCE(la.processing_status,'')                                   AS processing_status,
			COALESCE(la.action_type,'')                                         AS action_type,
			COALESCE(la.requested_by,'')                                        AS requested_by,
			COALESCE(TO_CHAR(la.requested_at,'YYYY-MM-DD HH24:MI:SS'),'')      AS requested_at,
			COALESCE(la.checker_by,'')                                          AS checker_by,
			COALESCE(TO_CHAR(la.checker_at,'YYYY-MM-DD HH24:MI:SS'),'')        AS checker_at,
			COALESCE(la.checker_comment,'')                                     AS checker_comment
		FROM investment.fd_interest_receipt r
		LEFT JOIN LATERAL (
			SELECT processing_status, action_type, requested_by, requested_at,
				   checker_by, checker_at, checker_comment
			FROM investment.fd_interest_receipt_audit
			WHERE receipt_id = r.receipt_id
			ORDER BY GREATEST(COALESCE(requested_at,'1970-01-01'::timestamp),
							  COALESCE(checker_at,'1970-01-01'::timestamp)) DESC
			LIMIT 1
		) la ON true
		WHERE r.receipt_id IN (%s)
		  AND COALESCE(r.is_deleted,false) = false
		ORDER BY r.receipt_date DESC
	`, placeholders(len(ids), 1))

	rows, err := pool.Query(ctx, q, stringsToAny(ids)...)
	if err != nil {
		return nil, fmt.Errorf("fetchReceiptRows: %w", err)
	}
	defer rows.Close()

	var out []map[string]interface{}
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			continue
		}
		cols := rows.FieldDescriptions()
		row := make(map[string]interface{}, len(cols))
		for i, col := range cols {
			row[string(col.Name)] = vals[i]
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// BuildReceiptNotifPayload constructs a rich notification payload for receipt events.
// Pass action = "CREATE" | "UPDATE" | "APPROVE" | "REJECT" | "POST_JOURNAL" | "RECONCILE".
func BuildReceiptNotifPayload(
	ctx context.Context,
	pool *pgxpool.Pool,
	receiptIDs []string,
	action string,
	actorEmail string,
) *ReceiptNotifPayload {
	p := &ReceiptNotifPayload{
		Action:       action,
		ActorEmail:   actorEmail,
		Count:        len(receiptIDs),
		ActionAt:     time.Now().Format(time.RFC3339),
		Receipts:     []map[string]interface{}{},
		ReceiptIDs:   receiptIDs,
		ByEntityKPIs: []FDKPIRow{},
		ByBankKPIs:   []FDKPIRow{},
	}
	if len(receiptIDs) == 0 {
		return p
	}
	rows, err := fetchReceiptRows(ctx, pool, receiptIDs)
	if err != nil {
		fmt.Printf("[ERROR] BuildReceiptNotifPayload: %v\n", err)
		return p
	}
	p.Receipts = rows
	p.ByEntityKPIs = computeKPIs(rows, "entity_name", "gross_interest_received")
	p.ByBankKPIs = computeKPIs(rows, "bank_name", "gross_interest_received")
	seen := map[string]bool{}
	ids := make([]string, 0, len(rows))
	for _, row := range rows {
		if id := fdStrField(row, "receipt_id"); id != "" && !seen[id] {
			seen[id] = true
			ids = append(ids, id)
		}
		p.TotalGrossInterest += fdAnyToFloat64(row["gross_interest_received"])
		p.TotalNetAmount += fdAnyToFloat64(row["net_amount_received"])
	}
	p.ReceiptIDs = ids
	return p
}
