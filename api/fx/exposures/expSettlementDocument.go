package exposures

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strings"
	"time"

	api "CimplrCorpSaas/api"
	"CimplrCorpSaas/api/approvalengine"
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/api/fx/auditutil"
	"CimplrCorpSaas/api/policyengine/common"
	"CimplrCorpSaas/api/policyengine/runtime"
	"CimplrCorpSaas/internal/ctxutil"
	"CimplrCorpSaas/internal/logger"

	"github.com/jackc/pgx/v5/pgxpool"
)

type settlementLineInput struct {
	ExposureHeaderID string   `json:"exposure_header_id"`
	BookingID        string   `json:"booking_id"`
	ForwardRef       string   `json:"forward_ref"`
	LegType          string   `json:"leg_type"`
	SettlementAmount float64  `json:"settlement_amount"`
	SpotRate         *float64 `json:"spot_rate"`
	FwdRate          *float64 `json:"fwd_rate"`
	Margin           *float64 `json:"margin"`
	BankName         string   `json:"bank_name"`
	MaturityDate     string   `json:"maturity_date"`
	PartialAmount    *float64 `json:"partial_amount"`
	Status           string   `json:"status"`
	Comments         *string  `json:"comments"`
}

func normalizeSettlementMethod(m string) string {
	switch strings.ToUpper(strings.TrimSpace(m)) {
	case "PAYMENT", "PAY":
		return "PAYMENT"
	case "ROLLOVER", "ROLL":
		return "ROLLOVER"
	case "CANCELLATION", "CANCEL":
		return "CANCELLATION"
	default:
		return ""
	}
}

func settlementSnapshot(ctx context.Context, pool *pgxpool.Pool, settlementID string) map[string]any {
	header := auditutil.FetchRowSnapshotPGX(ctx, pool, "public.exposure_settlement_document", "settlement_id", settlementID)
	lines := make([]map[string]any, 0)
	rows, err := pool.Query(ctx, `
		SELECT row_to_json(t)
		FROM (
			SELECT * FROM public.exposure_settlement_line
			WHERE settlement_id = $1::uuid
			ORDER BY line_id
		) t
	`, settlementID)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var raw []byte
			if err := rows.Scan(&raw); err != nil {
				continue
			}
			var m map[string]any
			if json.Unmarshal(raw, &m) == nil {
				lines = append(lines, m)
			}
		}
	}
	out := map[string]any{}
	for k, v := range header {
		out[k] = v
	}
	out["lines"] = lines
	return out
}

func snapString(snap map[string]any, keys ...string) interface{} {
	for _, key := range keys {
		if snap == nil {
			return nil
		}
		v, ok := snap[key]
		if !ok || v == nil {
			continue
		}
		switch t := v.(type) {
		case string:
			if strings.TrimSpace(t) == "" {
				continue
			}
			return strings.TrimSpace(t)
		default:
			s := strings.TrimSpace(fmt.Sprint(t))
			if s == "" || s == "<nil>" {
				continue
			}
			return s
		}
	}
	return nil
}

func snapFloat(snap map[string]any, keys ...string) interface{} {
	for _, key := range keys {
		if snap == nil {
			return nil
		}
		v, ok := snap[key]
		if !ok || v == nil {
			continue
		}
		switch t := v.(type) {
		case float64:
			return t
		case float32:
			return float64(t)
		case int:
			return float64(t)
		case int64:
			return float64(t)
		case json.Number:
			f, err := t.Float64()
			if err == nil {
				return f
			}
		default:
			f, err := json.Number(fmt.Sprint(t)).Float64()
			if err == nil {
				return f
			}
		}
	}
	return nil
}

func snapDate(snap map[string]any, keys ...string) interface{} {
	v := snapString(snap, keys...)
	if v == nil {
		return nil
	}
	s := fmt.Sprint(v)
	// Accept RFC3339 / date-only; store date portion.
	if len(s) >= 10 {
		return s[:10]
	}
	return s
}

func settlementLineAmounts(snap map[string]any) (linked, additional, cash, partial float64, exposureIDs []string, lineCount int) {
	raw, _ := snap["lines"].([]map[string]any)
	if raw == nil {
		// json unmarshal may produce []any of maps
		if arr, ok := snap["lines"].([]any); ok {
			for _, item := range arr {
				if m, ok := item.(map[string]any); ok {
					raw = append(raw, m)
				}
			}
		}
	}
	seen := map[string]struct{}{}
	for _, line := range raw {
		lineCount++
		leg := strings.ToUpper(strings.TrimSpace(fmt.Sprint(line["leg_type"])))
		amt := 0.0
		if f := snapFloat(line, "settlement_amount"); f != nil {
			amt, _ = f.(float64)
		}
		switch leg {
		case "LINKED_HEDGE":
			linked += amt
		case "ADDITIONAL_FWD":
			additional += amt
		case "CASH":
			cash += amt
		case "CANCELLATION":
			if p := snapFloat(line, "partial_amount"); p != nil {
				partial, _ = p.(float64)
			}
			if partial == 0 {
				partial = amt
			}
		}
		id := strings.TrimSpace(fmt.Sprint(line["exposure_header_id"]))
		if id != "" && id != "<nil>" {
			if _, ok := seen[id]; !ok {
				seen[id] = struct{}{}
				exposureIDs = append(exposureIDs, id)
			}
		}
	}
	return
}

// recordSettlementAudit writes old_* columns (no JSONB) for Payment / Rollover / Cancellation.
func recordSettlementAudit(
	ctx context.Context,
	pool *pgxpool.Pool,
	settlementID, actionType, status, reason, actor string,
	oldSnap map[string]any,
	methodHint string,
) {
	if pool == nil || strings.TrimSpace(settlementID) == "" || strings.TrimSpace(actor) == "" {
		return
	}
	method := strings.TrimSpace(methodHint)
	if method == "" {
		if m := snapString(oldSnap, "settlement_method"); m != nil {
			method = fmt.Sprint(m)
		}
	}
	method = normalizeSettlementMethod(method)

	linked, additional, cash, partial, exposureIDs, lineCount := settlementLineAmounts(oldSnap)
	var exposureIDsVal interface{}
	if len(exposureIDs) > 0 {
		exposureIDsVal = strings.Join(exposureIDs, ",")
	}
	var lineCountVal interface{}
	if lineCount > 0 {
		lineCountVal = lineCount
	}
	var linkedVal, additionalVal, cashVal, partialVal interface{}
	if linked != 0 {
		linkedVal = linked
	}
	if additional != 0 {
		additionalVal = additional
	}
	if cash != 0 {
		cashVal = cash
	}
	if partial != 0 {
		partialVal = partial
	}

	ip := auditutil.NullIfBlank(api.ClientIPFromContext(ctx))

	_, err := pool.Exec(ctx, `
		INSERT INTO public.auditactionexposuresettlement (
			settlement_id, actiontype, processing_status, reason,
			requested_by, requested_at, requested_ip,
			settlement_method,
			old_settlement_method, old_entity, old_currency, old_settlement_date,
			old_total_open_amount, old_total_settled_amount, old_processing_status, old_comments,
			old_exposure_header_ids, old_line_count,
			old_linked_hedge_amount, old_additional_fwd_amount, old_cash_amount,
			old_new_exposure_type, old_new_maturity_date, old_new_quantity, old_new_price, old_new_amount,
			old_new_exposure_header_id, old_partial_amount
		) VALUES (
			$1, $2, $3, $4,
			$5, NOW(), $6,
			NULLIF($7,''),
			$8, $9, $10, $11::date,
			$12, $13, $14, $15,
			$16, $17,
			$18, $19, $20,
			$21, $22::date, $23, $24, $25,
			$26, $27
		)
	`, settlementID, actionType, status, auditutil.NullIfBlank(reason),
		actor, ip,
		method,
		snapString(oldSnap, "settlement_method"),
		snapString(oldSnap, "entity"),
		snapString(oldSnap, "currency"),
		snapDate(oldSnap, "settlement_date"),
		snapFloat(oldSnap, "total_open_amount"),
		snapFloat(oldSnap, "total_settled_amount"),
		snapString(oldSnap, "processing_status"),
		snapString(oldSnap, "comments"),
		exposureIDsVal,
		lineCountVal,
		linkedVal, additionalVal, cashVal,
		snapString(oldSnap, "new_exposure_type"),
		snapDate(oldSnap, "new_maturity_date"),
		snapFloat(oldSnap, "new_quantity"),
		snapFloat(oldSnap, "new_price"),
		snapFloat(oldSnap, "new_amount"),
		snapString(oldSnap, "new_exposure_header_id"),
		partialVal,
	)
	if err != nil {
		logger.LogError("settlement audit insert failed settlement=%s action=%s: %v", settlementID, actionType, err)
	}
}

func nullDate(s string) interface{} {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	return s
}

func replaceSettlementLines(ctx context.Context, pool *pgxpool.Pool, settlementID string, lines []settlementLineInput) error {
	if _, err := pool.Exec(ctx, `DELETE FROM public.exposure_settlement_line WHERE settlement_id = $1::uuid`, settlementID); err != nil {
		return err
	}
	for _, line := range lines {
		leg := strings.TrimSpace(line.LegType)
		if leg == "" {
			leg = "EXPOSURE"
		}
		status := strings.TrimSpace(line.Status)
		if status == "" {
			status = "pending"
		}
		amount := line.SettlementAmount
		if line.PartialAmount != nil && *line.PartialAmount > 0 && amount == 0 {
			amount = *line.PartialAmount
		}
		_, err := pool.Exec(ctx, `
			INSERT INTO public.exposure_settlement_line (
				settlement_id, exposure_header_id, booking_id, forward_ref, leg_type,
				settlement_amount, spot_rate, fwd_rate, margin, bank_name, maturity_date,
				partial_amount, line_status, comments, created_at
			) VALUES (
				$1::uuid, $2, NULLIF($3,''), NULLIF($4,''), $5,
				$6, $7, $8, $9, NULLIF($10,''), $11::date,
				$12, $13, $14, NOW()
			)
		`, settlementID,
			strings.TrimSpace(line.ExposureHeaderID),
			strings.TrimSpace(line.BookingID),
			strings.TrimSpace(line.ForwardRef),
			leg,
			amount,
			line.SpotRate,
			line.FwdRate,
			line.Margin,
			strings.TrimSpace(line.BankName),
			nullDate(line.MaturityDate),
			line.PartialAmount,
			status,
			line.Comments,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func reduceExposureOpenAmount(ctx context.Context, pool *pgxpool.Pool, exposureHeaderID string, amount float64) error {
	exposureHeaderID = strings.TrimSpace(exposureHeaderID)
	if exposureHeaderID == "" || amount == 0 {
		return nil
	}
	_, err := pool.Exec(ctx, `
		UPDATE public.exposure_headers
		SET total_open_amount = GREATEST(0, COALESCE(total_open_amount, 0) - $1),
		    updated_at = NOW()
		WHERE exposure_header_id::text = $2
		  AND COALESCE(is_deleted, false) = false
	`, math.Abs(amount), exposureHeaderID)
	return err
}

func applySettlementOnApprove(ctx context.Context, pool *pgxpool.Pool, settlementID, actor string) error {
	var method, entity, currency string
	var newExpType *string
	var newMaturity *time.Time
	var newQty, newPrice, newAmt *float64
	err := pool.QueryRow(ctx, `
		SELECT settlement_method, entity, currency,
		       new_exposure_type, new_maturity_date, new_quantity, new_price, new_amount
		FROM public.exposure_settlement_document
		WHERE settlement_id = $1::uuid AND COALESCE(is_deleted, false) = false
	`, settlementID).Scan(&method, &entity, &currency, &newExpType, &newMaturity, &newQty, &newPrice, &newAmt)
	if err != nil {
		return err
	}

	rows, err := pool.Query(ctx, `
		SELECT exposure_header_id, COALESCE(booking_id,''), COALESCE(forward_ref,''),
		       leg_type, settlement_amount, COALESCE(partial_amount, 0)
		FROM public.exposure_settlement_line
		WHERE settlement_id = $1::uuid
		ORDER BY line_id
	`, settlementID)
	if err != nil {
		return err
	}
	defer rows.Close()

	type lineRow struct {
		ExposureID string
		BookingID  string
		ForwardRef string
		LegType    string
		Amount     float64
		Partial    float64
	}
	lines := make([]lineRow, 0)
	for rows.Next() {
		var lr lineRow
		if err := rows.Scan(&lr.ExposureID, &lr.BookingID, &lr.ForwardRef, &lr.LegType, &lr.Amount, &lr.Partial); err != nil {
			continue
		}
		lines = append(lines, lr)
	}

	switch method {
	case "PAYMENT", "CANCELLATION":
		for _, lr := range lines {
			amt := lr.Amount
			if lr.Partial > 0 {
				amt = lr.Partial
			}
			if lr.LegType == "CASH" || lr.LegType == "ADDITIONAL_FWD" || lr.LegType == "LINKED_HEDGE" || lr.LegType == "EXPOSURE" || lr.LegType == "CANCELLATION" {
				_ = reduceExposureOpenAmount(ctx, pool, lr.ExposureID, amt)
			}
			// Release / consume hedge link when booking present
			if strings.TrimSpace(lr.BookingID) != "" && strings.TrimSpace(lr.ExposureID) != "" {
				_, _ = pool.Exec(ctx, `
					UPDATE public.exposure_hedge_links
					SET hedged_amount = GREATEST(0, COALESCE(hedged_amount, 0) - $1),
					    is_active = CASE WHEN GREATEST(0, COALESCE(hedged_amount, 0) - $1) <= 0 THEN false ELSE is_active END
					WHERE exposure_header_id::text = $2
					  AND booking_id::text = $3
				`, math.Abs(amt), lr.ExposureID, lr.BookingID)
				if method == "PAYMENT" {
					var openAmt float64
					var seq int
					_ = pool.QueryRow(ctx, `
						SELECT COALESCE(running_open_amount, 0), COALESCE(ledger_sequence, 0)
						FROM public.forward_booking_ledger
						WHERE booking_id::text = $1
						ORDER BY ledger_sequence DESC LIMIT 1
					`, lr.BookingID).Scan(&openAmt, &seq)
					newOpen := math.Max(0, openAmt-math.Abs(amt))
					_, _ = pool.Exec(ctx, `
						INSERT INTO public.forward_booking_ledger
							(booking_id, ledger_sequence, action_type, action_id, action_date, amount_changed, running_open_amount, user_id)
						VALUES ($1, $2, 'UTILIZATION', $3, CURRENT_DATE, $4, $5, $6)
					`, lr.BookingID, seq+1, settlementID, math.Abs(amt), newOpen, actor)
				} else if method == "CANCELLATION" {
					var openAmt float64
					var seq int
					_ = pool.QueryRow(ctx, `
						SELECT COALESCE(running_open_amount, 0), COALESCE(ledger_sequence, 0)
						FROM public.forward_booking_ledger
						WHERE booking_id::text = $1
						ORDER BY ledger_sequence DESC LIMIT 1
					`, lr.BookingID).Scan(&openAmt, &seq)
					newOpen := openAmt + math.Abs(amt)
					_, _ = pool.Exec(ctx, `
						INSERT INTO public.forward_booking_ledger
							(booking_id, ledger_sequence, action_type, action_id, action_date, amount_changed, running_open_amount, user_id)
						VALUES ($1, $2, 'CANCELLATION', $3, CURRENT_DATE, $4, $5, $6)
					`, lr.BookingID, seq+1, settlementID, -math.Abs(amt), newOpen, actor)
				}
			}
		}

	case "ROLLOVER":
		// Reduce old exposures + create one new exposure from form fields
		var primaryOldID string
		totalRoll := 0.0
		for _, lr := range lines {
			amt := lr.Amount
			if lr.Partial > 0 {
				amt = lr.Partial
			}
			if amt == 0 && newAmt != nil {
				amt = *newAmt
			}
			_ = reduceExposureOpenAmount(ctx, pool, lr.ExposureID, amt)
			if primaryOldID == "" && strings.TrimSpace(lr.ExposureID) != "" {
				primaryOldID = strings.TrimSpace(lr.ExposureID)
			}
			totalRoll += math.Abs(amt)
		}
		if newAmt != nil && *newAmt > 0 {
			totalRoll = *newAmt
		}

		expType := ""
		if newExpType != nil {
			expType = strings.TrimSpace(*newExpType)
		}
		var maturity interface{}
		if newMaturity != nil {
			maturity = newMaturity.Format("2006-01-02")
		}
		qty := 0.0
		price := 0.0
		if newQty != nil {
			qty = *newQty
		}
		if newPrice != nil {
			price = *newPrice
		}

		// Copy basics from primary old exposure when available
		var oldEntity, oldCurrency, oldCP, oldCompany string
		if primaryOldID != "" {
			_ = pool.QueryRow(ctx, `
				SELECT COALESCE(entity,''), COALESCE(currency,''), COALESCE(counterparty_name,''), COALESCE(company_code,'')
				FROM public.exposure_headers
				WHERE exposure_header_id::text = $1
			`, primaryOldID).Scan(&oldEntity, &oldCurrency, &oldCP, &oldCompany)
		}
		if entity == "" {
			entity = oldEntity
		}
		if currency == "" {
			currency = oldCurrency
		}
		if expType == "" {
			_ = pool.QueryRow(ctx, `SELECT COALESCE(exposure_type,'') FROM public.exposure_headers WHERE exposure_header_id::text = $1`, primaryOldID).Scan(&expType)
		}

		var newHeaderID string
		err = pool.QueryRow(ctx, `
			INSERT INTO public.exposure_headers (
				entity, currency, exposure_type, company_code, counterparty_name,
				total_original_amount, total_open_amount, value_date,
				exposure_creation_status, approval_status, is_deleted, created_at
			) VALUES (
				$1, $2, $3, NULLIF($4,''), NULLIF($5,''),
				$6, $6, COALESCE($7::date, CURRENT_DATE),
				'Approved', 'Approved', false, NOW()
			) RETURNING exposure_header_id::text
		`, entity, currency, expType, oldCompany, oldCP, totalRoll, maturity).Scan(&newHeaderID)
		if err != nil {
			return fmt.Errorf("create rollover exposure: %w", err)
		}
		_, _ = pool.Exec(ctx, `
			INSERT INTO public.exposure_line_items (
				exposure_header_id, line_number, quantity, unit_price, line_item_amount, delivery_date, created_at
			) VALUES ($1::uuid, 1, $2, $3, $4, COALESCE($5::date, CURRENT_DATE), NOW())
		`, newHeaderID, qty, price, totalRoll, maturity)

		// Move hedge links from old exposures to new exposure
		for _, lr := range lines {
			oldID := strings.TrimSpace(lr.ExposureID)
			if oldID == "" {
				continue
			}
			linkRows, lerr := pool.Query(ctx, `
				SELECT booking_id::text, COALESCE(hedged_amount, 0)
				FROM public.exposure_hedge_links
				WHERE exposure_header_id::text = $1 AND COALESCE(is_active, true) = true
			`, oldID)
			if lerr != nil {
				continue
			}
			for linkRows.Next() {
				var bid string
				var hamt float64
				if linkRows.Scan(&bid, &hamt) != nil {
					continue
				}
				_, _ = pool.Exec(ctx, `
					UPDATE public.exposure_hedge_links SET is_active = false
					WHERE exposure_header_id::text = $1 AND booking_id::text = $2
				`, oldID, bid)
				_, _ = pool.Exec(ctx, `
					INSERT INTO public.exposure_hedge_links (exposure_header_id, booking_id, hedged_amount, is_active)
					VALUES ($1::uuid, $2, $3, true)
					ON CONFLICT (exposure_header_id, booking_id)
					DO UPDATE SET hedged_amount = EXCLUDED.hedged_amount, is_active = true
				`, newHeaderID, bid, hamt)
			}
			linkRows.Close()
		}

		_, _ = pool.Exec(ctx, `
			UPDATE public.exposure_settlement_document
			SET new_exposure_header_id = $1
			WHERE settlement_id = $2::uuid
		`, newHeaderID, settlementID)
	}
	return nil
}

// SaveExposureSettlementDocument creates/updates a settlement (submit → PENDING_APPROVAL).
func SaveExposureSettlementDocument(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID             string                `json:"user_id"`
			SettlementID       string                `json:"settlement_id"`
			SettlementMethod   string                `json:"settlement_method"`
			Entity             string                `json:"entity"`
			Currency           string                `json:"currency"`
			SettlementDate     string                `json:"settlement_date"`
			TotalOpenAmount    float64               `json:"total_open_amount"`
			TotalSettledAmount float64               `json:"total_settled_amount"`
			NewExposureType    string                `json:"new_exposure_type"`
			NewMaturityDate    string                `json:"new_maturity_date"`
			NewQuantity        *float64              `json:"new_quantity"`
			NewPrice           *float64              `json:"new_price"`
			NewAmount          *float64              `json:"new_amount"`
			Comments           string                `json:"comments"`
			Submit             bool                  `json:"submit"`
			Lines              []settlementLineInput `json:"lines"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.UserID) == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrPleaseLogin)
			return
		}
		method := normalizeSettlementMethod(req.SettlementMethod)
		if method == "" {
			respondWithError(w, http.StatusBadRequest, "settlement_method must be PAYMENT, ROLLOVER, or CANCELLATION")
			return
		}
		if len(req.Lines) == 0 && method != "ROLLOVER" {
			respondWithError(w, http.StatusBadRequest, "at least one settlement line is required")
			return
		}
		if method == "ROLLOVER" && len(req.Lines) == 0 {
			respondWithError(w, http.StatusBadRequest, "at least one exposure line is required for rollover")
			return
		}

		actor := auditutil.Actor(req.UserID)
		status := "DRAFT"
		actionType := "CREATE"
		if req.Submit {
			status = constants.StatusPendingApproval
			if status == "" {
				status = "PENDING_APPROVAL"
			}
		}

		settled := req.TotalSettledAmount
		if settled == 0 {
			for _, l := range req.Lines {
				amt := l.SettlementAmount
				if l.PartialAmount != nil && *l.PartialAmount > 0 {
					amt = *l.PartialAmount
				}
				settled += math.Abs(amt)
			}
			if req.NewAmount != nil && method == "ROLLOVER" && settled == 0 {
				settled = math.Abs(*req.NewAmount)
			}
		}

		settlementID := strings.TrimSpace(req.SettlementID)
		var oldSnap map[string]any
		var triggerMatrixID string
		if req.Submit {
			eventCode := common.TriggerPreCreate
			if settlementID != "" {
				eventCode = common.TriggerPreEdit
			}
			if ok, msg, tID := runtime.EnforceInlineWithMatrix(ctx, r, pool, runtime.EnforceInput{
				EventCode:           eventCode,
				ModuleCode:          common.ModuleFX,
				SubModule:           "EXPOSURE_SETTLEMENT",
				EntityCode:          strings.TrimSpace(req.Entity),
				ActorUserID:         req.UserID,
				HandlerName:         "SaveExposureSettlementDocument",
				APIPath:             "/fx/exposures/settlements/save",
				DefaultBlockMessage: "Settlement submit blocked by policy",
				Fields: map[string]interface{}{
					"settlement_id":        settlementID,
					"settlement_method":    method,
					"entity":               strings.TrimSpace(req.Entity),
					"currency":             strings.TrimSpace(req.Currency),
					"settlement_date":      req.SettlementDate,
					"total_open_amount":    req.TotalOpenAmount,
					"total_settled_amount": settled,
					"new_exposure_type":    strings.TrimSpace(req.NewExposureType),
					"new_maturity_date":    req.NewMaturityDate,
					"new_quantity":         req.NewQuantity,
					"new_price":            req.NewPrice,
					"new_amount":           req.NewAmount,
				},
			}); !ok {
				respondWithError(w, http.StatusForbidden, msg)
				return
			} else {
				triggerMatrixID = tID
			}
		}

		if settlementID == "" {
			err := pool.QueryRow(ctx, `
				INSERT INTO public.exposure_settlement_document (
					settlement_method, entity, currency, settlement_date,
					total_open_amount, total_settled_amount, processing_status,
					new_exposure_type, new_maturity_date, new_quantity, new_price, new_amount,
					created_by, comments, created_at
				) VALUES (
					$1, $2, $3, $4::date,
					$5, $6, $7,
					NULLIF($8,''), $9::date, $10, $11, $12,
					$13, NULLIF($14,''), NOW()
				) RETURNING settlement_id::text
			`, method, strings.TrimSpace(req.Entity), strings.TrimSpace(req.Currency), nullDate(req.SettlementDate),
				req.TotalOpenAmount, settled, status,
				strings.TrimSpace(req.NewExposureType), nullDate(req.NewMaturityDate), req.NewQuantity, req.NewPrice, req.NewAmount,
				actor, strings.TrimSpace(req.Comments)).Scan(&settlementID)
			if err != nil {
				respondWithError(w, http.StatusInternalServerError, "failed to create settlement: "+err.Error())
				return
			}
			if req.Submit {
				actionType = "SUBMIT"
			}
		} else {
			oldSnap = settlementSnapshot(ctx, pool, settlementID)
			actionType = "EDIT"
			if req.Submit {
				actionType = "SUBMIT"
			}
			tag, err := pool.Exec(ctx, `
				UPDATE public.exposure_settlement_document
				SET settlement_method = $1,
				    entity = $2,
				    currency = $3,
				    settlement_date = $4::date,
				    total_open_amount = $5,
				    total_settled_amount = $6,
				    processing_status = CASE WHEN $7 THEN $8 ELSE processing_status END,
				    new_exposure_type = NULLIF($9,''),
				    new_maturity_date = $10::date,
				    new_quantity = $11,
				    new_price = $12,
				    new_amount = $13,
				    comments = NULLIF($14,''),
				    updated_by = $15,
				    updated_at = NOW()
				WHERE settlement_id = $16::uuid
				  AND COALESCE(is_deleted, false) = false
			`, method, strings.TrimSpace(req.Entity), strings.TrimSpace(req.Currency), nullDate(req.SettlementDate),
				req.TotalOpenAmount, settled, req.Submit, status,
				strings.TrimSpace(req.NewExposureType), nullDate(req.NewMaturityDate), req.NewQuantity, req.NewPrice, req.NewAmount,
				strings.TrimSpace(req.Comments), actor, settlementID)
			if err != nil || tag.RowsAffected() == 0 {
				respondWithError(w, http.StatusNotFound, "settlement not found")
				return
			}
		}

		if err := replaceSettlementLines(ctx, pool, settlementID, req.Lines); err != nil {
			respondWithError(w, http.StatusInternalServerError, "failed to save settlement lines: "+err.Error())
			return
		}

		finalStatus := status
		if !req.Submit {
			_ = pool.QueryRow(ctx, `SELECT processing_status FROM public.exposure_settlement_document WHERE settlement_id = $1::uuid`, settlementID).Scan(&finalStatus)
		}
		// Prefer pre-change snapshot for EDIT/SUBMIT; CREATE has empty oldSnap.
		auditSnap := oldSnap
		if actionType == "CREATE" || len(oldSnap) == 0 {
			auditSnap = nil
		}
		recordSettlementAudit(ctx, pool, settlementID, actionType, finalStatus, strings.TrimSpace(req.Comments), actor, auditSnap, method)

		if req.Submit {
			makerEmail := ""
			for _, s := range auth.GetActiveSessions() {
				if s.UserID == req.UserID {
					makerEmail = s.Email
					break
				}
			}
			txnType := "FX_SETTLEMENT_EDIT"
			if len(oldSnap) == 0 {
				txnType = "FX_SETTLEMENT_CREATE"
			}
			if method == "ROLLOVER" {
				txnType = "FX_SETTLEMENT_ROLLOVER"
			} else if method == "CANCELLATION" {
				txnType = "FX_SETTLEMENT_CANCELLATION"
			}
			go func(id, email, tType, tID string) {
				bgCtx := context.Background()
				_ = approvalengine.CancelPendingInstances(bgCtx, pool, "FX", id, email)
				_, _ = approvalengine.CreateInstance(bgCtx, pool, approvalengine.InstanceRequest{
					ModuleCode:       "FX",
					TransactionType:  tType,
					RecordID:         id,
					MatrixID:         tID,
					SubmittedByEmail: email,
				})
			}(settlementID, makerEmail, txnType, triggerMatrixID)
		}

		respondWithSuccess(w, http.StatusOK, "Settlement saved", map[string]any{
			"settlement_id":        settlementID,
			"settlement_method":    method,
			"processing_status":    finalStatus,
			"total_settled_amount": settled,
		})
	}
}

// ListExposureSettlementDocuments returns All Settlements rows.
func ListExposureSettlementDocuments(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.UserID) == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrPleaseLogin)
			return
		}
		_ = ctxutil.FromContext(ctx)

		rows, err := pool.Query(ctx, `
			SELECT
				settlement_id::text,
				settlement_method,
				entity,
				currency,
				settlement_date,
				total_open_amount,
				total_settled_amount,
				processing_status,
				created_by,
				created_at,
				updated_by,
				updated_at,
				comments,
				new_exposure_header_id,
				COALESCE(ai.instance_id,'')         AS approval_instance_id,
				COALESCE(ai.status,'')              AS approval_engine_status,
				COALESCE(aie.instance_eye_id,'')    AS current_eye_id,
				COALESCE(aie.position::text,'')     AS current_eye_position,
				COALESCE(aie.approvals_required,0)  AS approvals_required,
				COALESCE(aie.approvals_received,0)  AS approvals_received,
				aie.sla_deadline                    AS sla_deadline,
				COALESCE(aie.is_escalated,false)    AS is_escalated
			FROM public.exposure_settlement_document esd
			LEFT JOIN LATERAL (
				SELECT ai.* FROM uam.approval_instance ai
				WHERE ai.record_id = esd.settlement_id::text
				  AND ai.module_code = 'FX'
				  AND ai.transaction_type = CASE WHEN esd.settlement_method = 'ROLLOVER' OR esd.settlement_method = 'CANCELLATION' THEN 'FX_SETTLEMENT_' || esd.settlement_method ELSE 'FX_SETTLEMENT_CREATE' END
				  AND ai.status = 'PENDING'
				  AND ai.is_deleted = false
				ORDER BY ai.submitted_at DESC, ai.instance_id DESC
				LIMIT 1
			) ai ON true
			LEFT JOIN LATERAL (
				SELECT aie.* FROM uam.approval_instance_eye aie
				WHERE aie.instance_id = ai.instance_id
				  AND aie.status = 'ACTIVE'
				ORDER BY aie.position ASC, aie.instance_eye_id ASC
				LIMIT 1
			) aie ON true
			WHERE COALESCE(esd.is_deleted, false) = false
			ORDER BY esd.created_at DESC
		`)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "failed to list settlements")
			return
		}
		defer rows.Close()

		pageData := make([]map[string]any, 0)
		for rows.Next() {
			var (
				id, method, entity, currency, status, createdBy                            string
				updatedBy, comments, newExpID                                              *string
				settlementDate                                                             *time.Time
				createdAt                                                                  time.Time
				updatedAt                                                                  *time.Time
				openAmt, settledAmt                                                        float64
				approvalInstanceID, approvalEngineStatus, currentEyeID, currentEyePosition string
				approvalsRequired, approvalsReceived                                       int
				slaDeadline                                                                *time.Time
				isEscalated                                                                bool
			)
			if err := rows.Scan(&id, &method, &entity, &currency, &settlementDate, &openAmt, &settledAmt, &status, &createdBy, &createdAt, &updatedBy, &updatedAt, &comments, &newExpID,
				&approvalInstanceID, &approvalEngineStatus, &currentEyeID, &currentEyePosition, &approvalsRequired, &approvalsReceived, &slaDeadline, &isEscalated); err != nil {
				continue
			}
			row := map[string]any{
				"settlement_id":          id,
				"settlement_method":      method,
				"entity":                 entity,
				"currency":               currency,
				"total_open_amount":      openAmt,
				"total_settled_amount":   settledAmt,
				"processing_status":      status,
				"created_by":             createdBy,
				"created_at":             createdAt,
				"approval_instance_id":   approvalInstanceID,
				"approval_engine_status": approvalEngineStatus,
				"current_eye_id":         currentEyeID,
				"current_eye_position":   currentEyePosition,
				"approvals_required":     approvalsRequired,
				"approvals_received":     approvalsReceived,
				"sla_deadline":           slaDeadline,
				"is_escalated":           isEscalated,
			}
			if settlementDate != nil {
				row["settlement_date"] = settlementDate.Format("2006-01-02")
			}
			if updatedBy != nil {
				row["updated_by"] = *updatedBy
			}
			if updatedAt != nil {
				row["updated_at"] = *updatedAt
			}
			if comments != nil {
				row["comments"] = *comments
			}
			if newExpID != nil {
				row["new_exposure_header_id"] = *newExpID
			}
			pageData = append(pageData, row)
		}
		respondWithSuccess(w, http.StatusOK, "Success", map[string]any{"pageData": pageData})
	}
}

// GetExposureSettlementDocument returns one settlement + lines.
func GetExposureSettlementDocument(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID       string `json:"user_id"`
			SettlementID string `json:"settlement_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.UserID) == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrPleaseLogin)
			return
		}
		settlementID := strings.TrimSpace(req.SettlementID)
		if settlementID == "" {
			respondWithError(w, http.StatusBadRequest, "settlement_id is required")
			return
		}

		snap := settlementSnapshot(ctx, pool, settlementID)
		if len(snap) == 0 || snap["settlement_id"] == nil {
			respondWithError(w, http.StatusNotFound, "settlement not found")
			return
		}

		if instanceID, lookupErr := approvalengine.LookupLatestInstanceID(ctx, pool, "FX", settlementID); lookupErr == nil && instanceID != "" {
			if detail, dErr := approvalengine.GetRichInstanceDetail(ctx, pool, instanceID, req.UserID); dErr == nil {
				if detail != nil && detail.Status == "PENDING" && len(detail.Eyes) > 0 {
					snap["processing_status"] = detail.Eyes[0].Status
				}
			}
		}

		respondWithSuccess(w, http.StatusOK, "Success", snap)
	}
}

func updateExposureSettlementStatuses(
	ctx context.Context,
	pool *pgxpool.Pool,
	ids []string,
	status string,
	actor string,
	userID string,
	comments string,
	actionType string,
) (int, error) {
	count := 0
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		oldSnap := settlementSnapshot(ctx, pool, id)
		var prevStatus string
		_ = pool.QueryRow(ctx, `SELECT processing_status FROM public.exposure_settlement_document WHERE settlement_id = $1::uuid`, id).Scan(&prevStatus)

		if actionType == "CONFIRM" && strings.EqualFold(prevStatus, constants.StatusPendingDeleteApproval) {
			_, err := pool.Exec(ctx, `
				UPDATE public.exposure_settlement_document
				SET is_deleted = true,
				    processing_status = $1,
				    comments = COALESCE(NULLIF($2, ''), comments),
				    updated_by = $3,
				    updated_at = NOW()
				WHERE settlement_id = $4::uuid
			`, status, comments, actor, id)
			if err != nil {
				continue
			}
		} else {
			tag, err := pool.Exec(ctx, `
				UPDATE public.exposure_settlement_document
				SET processing_status = $1,
				    comments = COALESCE(NULLIF($2, ''), comments),
				    updated_by = $3,
				    updated_at = NOW()
				WHERE settlement_id = $4::uuid
				  AND COALESCE(is_deleted, false) = false
			`, status, comments, actor, id)
			if err != nil || tag.RowsAffected() == 0 {
				continue
			}
			if actionType == "CONFIRM" && strings.EqualFold(prevStatus, constants.StatusPendingApproval) {
				if applyErr := applySettlementOnApprove(ctx, pool, id, actor); applyErr != nil {
					// keep approved status but report via audit new values
					_ = applyErr
				}
			}
		}

		var engineActed bool
		if actionType == "CONFIRM" || actionType == "REJECT" {
			userEmail := ""
			for _, s := range auth.GetActiveSessions() {
				if s.UserID == userID {
					userEmail = s.Email
					break
				}
			}
			actionStr := "APPROVED"
			if actionType == "REJECT" {
				actionStr = "REJECTED"
			}
			res, err := approvalengine.ActOnPendingOrDiagnose(ctx, pool, approvalengine.ActOnPendingRequest{
				ModuleCode: "FX", RecordID: id, UserID: userID, UserEmail: userEmail,
				Action: actionStr, Comment: comments,
			})
			if err == nil && res.Acted {
				engineActed = true
			}
		}

		if !engineActed {
			methodHint := ""
			if m := snapString(oldSnap, "settlement_method"); m != nil {
				methodHint = fmt.Sprint(m)
			}
			recordSettlementAudit(ctx, pool, id, actionType, status, comments, actor, oldSnap, methodHint)
			if actionType == "CONFIRM" || actionType == "REJECT" {
				auditutil.RecordDecisionPGX(ctx, pool, auditutil.DecisionParams{
					TableName:    auditutil.TableExposureSettlement,
					ParentColumn: "settlement_id",
					ParentID:     id,
					Status:       status,
					CheckerBy:    actor,
					Comment:      comments,
				})
			}
		}
		count++
	}
	return count, nil
}

func ApproveExposureSettlementDocuments(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID        string   `json:"user_id"`
			SettlementIDs []string `json:"settlement_ids"`
			Comments      string   `json:"comments"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.UserID) == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrPleaseLogin)
			return
		}
		if len(req.SettlementIDs) == 0 {
			respondWithError(w, http.StatusBadRequest, "settlement_ids is required")
			return
		}
		actor := auditutil.Actor(req.UserID)
		n, err := updateExposureSettlementStatuses(ctx, pool, req.SettlementIDs, constants.StatusApproved, actor, req.UserID, strings.TrimSpace(req.Comments), "CONFIRM")
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "failed to approve settlements")
			return
		}
		respondWithSuccess(w, http.StatusOK, fmt.Sprintf("%d settlement(s) approved", n), map[string]any{"count": n})
	}
}

func RejectExposureSettlementDocuments(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID        string   `json:"user_id"`
			SettlementIDs []string `json:"settlement_ids"`
			Comments      string   `json:"comments"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.UserID) == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrPleaseLogin)
			return
		}
		if len(req.SettlementIDs) == 0 {
			respondWithError(w, http.StatusBadRequest, "settlement_ids is required")
			return
		}
		actor := auditutil.Actor(req.UserID)
		n, err := updateExposureSettlementStatuses(ctx, pool, req.SettlementIDs, constants.StatusRejected, actor, req.UserID, strings.TrimSpace(req.Comments), "REJECT")
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "failed to reject settlements")
			return
		}
		respondWithSuccess(w, http.StatusOK, fmt.Sprintf("%d settlement(s) rejected", n), map[string]any{"count": n})
	}
}

func DeleteExposureSettlementDocuments(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID        string   `json:"user_id"`
			SettlementIDs []string `json:"settlement_ids"`
			Comments      string   `json:"comments"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.UserID) == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrPleaseLogin)
			return
		}
		if len(req.SettlementIDs) == 0 {
			respondWithError(w, http.StatusBadRequest, "settlement_ids is required")
			return
		}
		actor := auditutil.Actor(req.UserID)
		triggerMatrices := make(map[string]string, len(req.SettlementIDs))
		for _, id := range req.SettlementIDs {
			snap := auditutil.FetchRowSnapshotPGX(ctx, pool, "public.exposure_settlement_document", "settlement_id", id)
			entity := ""
			if v, ok := snap["entity"]; ok && v != nil {
				entity = strings.TrimSpace(fmt.Sprint(v))
			}
			if ok, msg, tID := runtime.EnforceInlineWithMatrix(ctx, r, pool, runtime.EnforceInput{
				EventCode:           common.TriggerPreDelete,
				ModuleCode:          common.ModuleFX,
				SubModule:           "EXPOSURE_SETTLEMENT",
				EntityCode:          entity,
				ActorUserID:         req.UserID,
				HandlerName:         "DeleteExposureSettlementDocuments",
				APIPath:             "/fx/exposures/settlements/delete",
				DefaultBlockMessage: "Settlement delete blocked by policy",
				Fields:              snap,
			}); !ok {
				respondWithError(w, http.StatusUnprocessableEntity, msg)
				return
			} else {
				triggerMatrices[id] = tID
			}
		}
		n, err := updateExposureSettlementStatuses(ctx, pool, req.SettlementIDs, constants.StatusPendingDeleteApproval, actor, req.UserID, strings.TrimSpace(req.Comments), "DELETE")
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "failed to delete settlements")
			return
		}

		makerEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				makerEmail = s.Email
				break
			}
		}
		go func(ids []string, email string, matrices map[string]string) {
			bgCtx := context.Background()
			for _, id := range ids {
				_ = approvalengine.CancelPendingInstances(bgCtx, pool, "FX", id, email)
				_, _ = approvalengine.CreateInstance(bgCtx, pool, approvalengine.InstanceRequest{
					ModuleCode:       "FX",
					TransactionType:  "FX_SETTLEMENT_DELETE",
					RecordID:         id,
					MatrixID:         matrices[id],
					SubmittedByEmail: email,
				})
			}
		}(req.SettlementIDs, makerEmail, triggerMatrices)

		respondWithSuccess(w, http.StatusOK, fmt.Sprintf("%d settlement(s) marked for delete", n), map[string]any{"count": n})
	}
}
