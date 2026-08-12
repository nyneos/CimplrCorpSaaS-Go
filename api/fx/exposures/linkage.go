package exposures

import (
	api "CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/api/fx/auditutil"
	fxnotif "CimplrCorpSaas/api/fx/notification"
	"CimplrCorpSaas/api/policyengine/common"
	"CimplrCorpSaas/api/policyengine/runtime"
	"CimplrCorpSaas/internal/ctxutil"
	dmsjobs "CimplrCorpSaas/internal/jobs/dms"
	"CimplrCorpSaas/internal/logger"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Helper: send JSON error response
// func respondWithError(w http.ResponseWriter, status int, errMsg string) {
// 	w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
// 	w.WriteHeader(status)
// 	json.NewEncoder(w).Encode(map[string]interface{}{
// 		constants.ValueSuccess: false,
// 		constants.ValueError:   errMsg,
// 	})
// }

// Handler: HedgeLinksDetails
func HedgeLinksDetails(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID string `json:"user_id"`
		}
		ct := r.Header.Get(constants.ContentTypeText)
		if strings.HasPrefix(ct, constants.ContentTypeJSON) {
			_ = json.NewDecoder(r.Body).Decode(&req)
			// } else if strings.HasPrefix(ct, constants.ContentTypeMultipart) {
			// 	r.ParseMultipartForm(32 << 20)
			// 	req.UserID = r.FormValue(constants.KeyUserID)
		}
		if req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrPleaseLogin)
			return
		}
		scope := ctxutil.FromContext(ctx)
		buNames := scope.EntityNames
		if len(buNames) == 0 {
			respondWithError(w, http.StatusNotFound, constants.ErrNoAccessibleBusinessUnit)
			return
		}
		rows, err := pool.Query(ctx, `
			SELECT
				l.exposure_header_id::text AS exposure_header_id,
				l.booking_id::text AS booking_id,
				COALESCE(l.hedged_amount, 0) AS hedged_amount,
				COALESCE(l.link_date::text, '') AS link_date,
				COALESCE(l.is_active, false) AS is_active,
				COALESCE(h.document_id, '') AS document_id,
				COALESCE(f.internal_reference_id, '') AS internal_reference_id,
				COALESCE((
					SELECT a.processing_status
					FROM public.auditactionhedgelink a
					WHERE a.exposure_header_id = l.exposure_header_id::text
					  AND a.booking_id = l.booking_id::text
					ORDER BY a.requested_at DESC NULLS LAST
					LIMIT 1
				), CASE WHEN COALESCE(l.is_active, false) THEN 'APPROVED' ELSE 'PENDING_APPROVAL' END) AS processing_status
			FROM exposure_hedge_links l
			LEFT JOIN exposure_headers h ON l.exposure_header_id = h.exposure_header_id
			LEFT JOIN forward_bookings f ON l.booking_id = f.system_transaction_id
			WHERE (
				COALESCE(h.entity, '') = ANY($1)
				OR COALESCE(h.entity1, '') = ANY($1)
				OR COALESCE(h.entity2, '') = ANY($1)
				OR COALESCE(h.entity3, '') = ANY($1)
				OR COALESCE(f.entity_level_0, '') = ANY($1)
				OR COALESCE(f.entity_level_1, '') = ANY($1)
				OR COALESCE(f.entity_level_2, '') = ANY($1)
				OR COALESCE(f.entity_level_3, '') = ANY($1)
			)
			ORDER BY l.link_date DESC NULLS LAST
		`, buNames)
		if err != nil {
			logger.LogError("hedge-links-details query failed: %v", err)
			respondWithError(w, http.StatusInternalServerError, "Failed to fetch hedge links details")
			return
		}
		defer rows.Close()
		cols := pgxColumnNames(rows)
		data := []map[string]interface{}{}
		for rows.Next() {
			vals := make([]interface{}, len(cols))
			valPtrs := make([]interface{}, len(cols))
			for i := range vals {
				valPtrs[i] = &vals[i]
			}
			if err := rows.Scan(valPtrs...); err != nil {
				continue
			}
			rowMap := map[string]interface{}{}
			for i, col := range cols {
				switch v := vals[i].(type) {
				case []uint8:
					rowMap[col] = string(v)
				default:
					rowMap[col] = parseDBValue(col, vals[i])
				}
			}
			data = append(data, rowMap)
		}
		respondWithSuccess(w, http.StatusOK, "Hedge links fetched successfully", map[string]interface{}{
			"data": data,
		})
	}
}

// Handler: ExpFwdLinkingBookings
func ExpFwdLinkingBookings(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID string `json:"user_id"`
		}
		ct := r.Header.Get(constants.ContentTypeText)
		if strings.HasPrefix(ct, constants.ContentTypeJSON) {
			_ = json.NewDecoder(r.Body).Decode(&req)
			// } else if strings.HasPrefix(ct, constants.ContentTypeMultipart) {
			// 	r.ParseMultipartForm(32 << 20)
			// 	req.UserID = r.FormValue(constants.KeyUserID)
		}
		if req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrPleaseLogin)
			return
		}
		scope := ctxutil.FromContext(ctx)
		buNames := scope.EntityNames
		if len(buNames) == 0 {
			respondWithError(w, http.StatusNotFound, constants.ErrNoAccessibleBusinessUnit)
			return
		}
		bookRows, err := pool.Query(ctx, `
			SELECT system_transaction_id, entity_level_0, entity_level_1, entity_level_2, entity_level_3,
			       order_type, currency_pair, maturity_date, booking_amount, counterparty, total_rate, value_local_currency
			FROM forward_bookings
			WHERE (UPPER(COALESCE(processing_status, '')) = 'APPROVED')
			  AND COALESCE(is_deleted, false) = false
		`)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "Failed to fetch bookings")
			return
		}
		defer bookRows.Close()
		bookCols := pgxColumnNames(bookRows)
		bookings := []map[string]interface{}{}
		for bookRows.Next() {
			vals := make([]interface{}, len(bookCols))
			valPtrs := make([]interface{}, len(bookCols))
			for i := range vals {
				valPtrs[i] = &vals[i]
			}
			if err := bookRows.Scan(valPtrs...); err != nil {
				logger.LogError("hedge links bookings: scan failed: %v", err)
			}
			row := map[string]interface{}{}
			for i, col := range bookCols {
				// Convert []uint8 to string for keys we use as map keys or for lookups
				if b, ok := vals[i].([]uint8); ok {
					row[col] = string(b)
				} else {
					row[col] = parseDBValue(col, vals[i])
				}
			}
			entityStr, _ := row["entity_level_0"].(string)
			if containsString(buNames, entityStr) {
				bookings = append(bookings, row)
			}
		}
		bookingIds := []string{}
		for _, b := range bookings {
			// Always use string for bookingIds
			var bookingIDStr string
			switch v := b["system_transaction_id"].(type) {
			case string:
				bookingIDStr = v
			case []uint8:
				bookingIDStr = string(v)
			}
			bookingIds = append(bookingIds, bookingIDStr)
		}
		hedgeMap := map[interface{}]float64{}
		if len(bookingIds) > 0 {
			hedgeRows, err := pool.Query(ctx, `
				SELECT booking_id::text, SUM(hedged_amount) AS linked_amount
				FROM exposure_hedge_links
				WHERE booking_id = ANY($1::uuid[])
				GROUP BY booking_id`, bookingIds)
			if err == nil {
				for hedgeRows.Next() {
					var bookingId interface{}
					var linkedAmount float64
					if err := hedgeRows.Scan(&bookingId, &linkedAmount); err != nil {
						logger.LogError("hedge links linked amount: scan failed: %v", err)
					}
					// Convert booking_id to string for map key
					var bookingIDStr string
					switch v := bookingId.(type) {
					case string:
						bookingIDStr = v
					case []uint8:
						bookingIDStr = string(v)
					}
					hedgeMap[bookingIDStr] = linkedAmount
				}
				hedgeRows.Close()
			}
		}
		buCompliance := map[string]bool{}
		buRows, err := pool.Query(ctx, `
		SELECT me.entity_name
		FROM masterentitycash me
		JOIN LATERAL (
		  SELECT processing_status
		  FROM auditactionentity
		  WHERE entity_id = me.entity_id
		  ORDER BY requested_at DESC
		  LIMIT 1
		) a ON TRUE
		WHERE COALESCE(me.is_deleted, false) = false
		  AND (a.processing_status = 'APPROVED' OR a.processing_status = 'Approved')
		`)
		if err == nil {
			for buRows.Next() {
				var name string
				if err := buRows.Scan(&name); err != nil {
					logger.LogError("hedge links bu compliance: scan failed: %v", err)
				}
				buCompliance[name] = true
			}
			buRows.Close()
		}
		response := []map[string]interface{}{}
		for _, b := range bookings {
			// Use string for lookup
			var bookingIDStr string
			switch v := b["system_transaction_id"].(type) {
			case string:
				bookingIDStr = v
			case []uint8:
				bookingIDStr = string(v)
			}
			linkedAmount := hedgeMap[bookingIDStr]
			entityStr, _ := b["entity_level_0"].(string)
			// Format numbers as required
			// booking_amount: 2 decimals
			var bookingAmount float64
			switch v := b["booking_amount"].(type) {
			case float64:
				bookingAmount = v
			case string:
				bookingAmount, _ = strconv.ParseFloat(v, 64)
			case []uint8:
				bookingAmount, _ = strconv.ParseFloat(string(v), 64)
			}
			// linkedAmount: 2 decimals
			linkedAmountF := linkedAmount
			// total_rate: 6 decimals
			var totalRate float64
			switch v := b["total_rate"].(type) {
			case float64:
				totalRate = v
			case string:
				totalRate, _ = strconv.ParseFloat(v, 64)
			case []uint8:
				totalRate, _ = strconv.ParseFloat(string(v), 64)
			}
			// value_local_currency: 2 decimals
			var lcyAmount float64
			switch v := b["value_local_currency"].(type) {
			case float64:
				lcyAmount = v
			case string:
				lcyAmount, _ = strconv.ParseFloat(v, 64)
			case []uint8:
				lcyAmount, _ = strconv.ParseFloat(string(v), 64)
			}
			// currency_pair
			currencyPair, _ := b["currency_pair"].(string)
			// bank name
			bankName, _ := b["counterparty"].(string)
			entity1, _ := b["entity_level_1"].(string)
			entity2, _ := b["entity_level_2"].(string)
			entity3, _ := b["entity_level_3"].(string)
			response = append(response, map[string]interface{}{
				"bu":                    entityStr,
				"entity_level_0":        entityStr,
				"entity_level_1":        entity1,
				"entity_level_2":        entity2,
				"entity_level_3":        entity3,
				"system_transaction_id": bookingIDStr,
				"type":                  b["order_type"],
				"currency_pair":         currencyPair,
				"maturity_date":         b["maturity_date"],
				"amount":                strconv.FormatFloat(bookingAmount, 'f', 2, 64),
				"linked_amount":         strconv.FormatFloat(linkedAmountF, 'f', 2, 64),
				"rate":                  strconv.FormatFloat(totalRate, 'f', 6, 64),
				"lcy_amount":            strconv.FormatFloat(lcyAmount, 'f', 2, 64),
				"bu_unit_compliance":    buCompliance[entityStr],
				"bank":                  bankName,
			})
		}
		respondWithSuccess(w, http.StatusOK, "Forward bookings fetched successfully", map[string]interface{}{
			"data": response,
		})
	}
}

// Handler: ExpFwdLinking
func ExpFwdLinking(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID string `json:"user_id"`
		}
		ct := r.Header.Get(constants.ContentTypeText)
		if strings.HasPrefix(ct, constants.ContentTypeJSON) {
			_ = json.NewDecoder(r.Body).Decode(&req)
			// } else if strings.HasPrefix(ct, constants.ContentTypeMultipart) {
			// 	r.ParseMultipartForm(32 << 20)
			// 	req.UserID = r.FormValue(constants.KeyUserID)
		}
		if req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrPleaseLogin)
			return
		}
		// request decoded
		scope := ctxutil.FromContext(ctx)
		buNames := scope.EntityNames
		if len(buNames) == 0 {
			respondWithError(w, http.StatusNotFound, constants.ErrNoAccessibleBusinessUnit)
			return
		}
		headRows, err := pool.Query(ctx, `
			SELECT exposure_header_id, entity, exposure_type, currency, value_date, total_open_amount, counterparty_name
			FROM exposure_headers
			WHERE (approval_status = 'Approved' OR approval_status = 'approved' OR approval_status = 'APPROVED')
			  AND COALESCE(is_deleted, false) = false
		`)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "Failed to fetch exposure headers")
			return
		}
		defer headRows.Close()
		headCols := pgxColumnNames(headRows)
		headers := []map[string]interface{}{}
		for headRows.Next() {
			vals := make([]interface{}, len(headCols))
			valPtrs := make([]interface{}, len(headCols))
			for i := range vals {
				valPtrs[i] = &vals[i]
			}
			if err := headRows.Scan(valPtrs...); err != nil {
				logger.LogError("exposure headers linkage: scan failed: %v", err)
			}
			row := map[string]interface{}{}
			for i, col := range headCols {
				// Convert []uint8 to string for all string fields
				if b, ok := vals[i].([]uint8); ok {
					row[col] = string(b)
				} else {
					row[col] = parseDBValue(col, vals[i])
				}
			}
			// Safe entity string
			entityStr, _ := row["entity"].(string)
			if containsString(buNames, entityStr) {
				headers = append(headers, row)
			}
		}
		// headers fetched from DB and filtered by BU
		// normalize header ids to strings for safe map keys
		headerIds := []string{}
		for _, h := range headers {
			// convert possible []uint8 or string to string
			var idStr string
			switch v := h["exposure_header_id"].(type) {
			case string:
				idStr = v
			case []uint8:
				idStr = string(v)
			default:
				idStr = fmt.Sprintf("%v", v)
			}
			if idStr != "" {
				headerIds = append(headerIds, idStr)
			}
		}
		hedgeMap := map[string]float64{}
		if len(headerIds) > 0 {
			hedgeRows, err := pool.Query(ctx, `
				SELECT exposure_header_id::text, SUM(hedged_amount) AS hedge_amount
				FROM exposure_hedge_links
				WHERE exposure_header_id = ANY($1::uuid[])
				GROUP BY exposure_header_id`, headerIds)
			if err == nil {
				for hedgeRows.Next() {
					var exposureHeaderId interface{}
					var hedgeAmount float64
					if err := hedgeRows.Scan(&exposureHeaderId, &hedgeAmount); err != nil {
						logger.LogError("exposure headers hedge amount: scan failed: %v", err)
					}
					// normalize key to string
					var key string
					switch v := exposureHeaderId.(type) {
					case string:
						key = v
					case []uint8:
						key = string(v)
					default:
						key = fmt.Sprintf("%v", v)
					}
					if key != "" {
						hedgeMap[key] = hedgeAmount
					}
				}
				hedgeRows.Close()
			}
		}
		// hedge map prepared
		buCompliance := map[string]bool{}
		buRows, err := pool.Query(ctx, `
		SELECT me.entity_name
		FROM masterentitycash me
		JOIN LATERAL (
		  SELECT processing_status
		  FROM auditactionentity
		  WHERE entity_id = me.entity_id
		  ORDER BY requested_at DESC
		  LIMIT 1
		) a ON TRUE
		WHERE COALESCE(me.is_deleted, false) = false
		  AND (a.processing_status = 'APPROVED' OR a.processing_status = 'Approved')
		AND (me.is_deleted = false OR me.is_deleted IS NULL)
		`)
		if err == nil {
			for buRows.Next() {
				var name string
				if err := buRows.Scan(&name); err != nil {
					logger.LogError("exposure headers bu compliance: scan failed: %v", err)
				}
				buCompliance[name] = true
			}
			buRows.Close()
		}
		// buCompliance loaded
		response := []map[string]interface{}{}
		for _, h := range headers {
			// lookup by normalized string key
			var key string
			switch v := h["exposure_header_id"].(type) {
			case string:
				key = v
			case []uint8:
				key = string(v)
			default:
				key = fmt.Sprintf("%v", v)
			}
			hedgeAmount := hedgeMap[key]
			// Safe total_open_amount conversion
			var totalOpen float64
			switch v := h["total_open_amount"].(type) {
			case float64:
				totalOpen = v
			case string:
				totalOpen, _ = strconv.ParseFloat(v, 64)
			case []uint8:
				totalOpen, _ = strconv.ParseFloat(string(v), 64)
			}
			// Use absolute value for comparison and reporting; DB may store negative for one-sided exposures
			totalOpenAbs := math.Abs(totalOpen)
			entityStr, _ := h["entity"].(string)
			if hedgeAmount < totalOpenAbs {
				response = append(response, map[string]interface{}{
					"bu":                 entityStr,
					"entity_level_0":     entityStr,
					"entity":             entityStr,
					"exposure_header_id": h["exposure_header_id"],
					"type":               h["exposure_type"],
					"currency":           h["currency"],
					"maturity_date":      h["value_date"],
					"amount":             totalOpenAbs,
					"open_amount":        totalOpen,
					"amount_abs":         totalOpenAbs,
					"hedge_amount":       hedgeAmount,
					"bu_unit_compliance": buCompliance[entityStr],
					"Bank":               h["counterparty_name"],
				})
			}
		}
		respondWithSuccess(w, http.StatusOK, "Exposure headers fetched successfully", map[string]interface{}{
			"data": response,
		})
	}
}

// Handler: LinkExposureHedge - upsert exposure_hedge_links and log to forward_booking_ledger
func LinkExposureHedge(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID           string  `json:"user_id"`
			ExposureHeaderID string  `json:"exposure_header_id"`
			BookingID        string  `json:"booking_id"`
			HedgedAmount     float64 `json:"hedged_amount"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || req.ExposureHeaderID == "" || req.BookingID == "" || req.HedgedAmount == 0 {
			respondWithError(w, http.StatusBadRequest, "user_id, exposure_header_id, booking_id, and hedged_amount are required")
			return
		}
		// Get booking amount and prior utilization up front (read-only) so the
		// policy check below sees the same running_open_amount/amount_changed
		// values that will actually be written to forward_booking_ledger,
		// instead of a thinner field set than the real HEDGE_LINK row shape.
		var bookingAmount float64
		_ = pool.QueryRow(ctx, "SELECT Booking_Amount FROM forward_bookings WHERE system_transaction_id = $1 AND COALESCE(is_deleted, false) = false", req.BookingID).Scan(&bookingAmount)
		var totalUtilized float64
		sumQuery := `SELECT COALESCE(SUM(amount_changed), 0) FROM forward_booking_ledger WHERE booking_id = $1 AND action_type IN ('UTILIZATION', 'CANCELLATION', 'ROLLOVER')`
		_ = pool.QueryRow(ctx, sumQuery, req.BookingID).Scan(&totalUtilized)
		newOpenAmount := math.Abs(math.Abs(bookingAmount) - math.Abs(totalUtilized))

		hedgeRow := hedgeLinkRow{
			ExposureHeaderID:  req.ExposureHeaderID,
			BookingID:         req.BookingID,
			HedgedAmount:      req.HedgedAmount,
			LinkDate:          time.Now().Format("2006-01-02"),
			IsActive:          true,
			ActionType:        "UTILIZATION",
			ActionDate:        time.Now().Format("2006-01-02"),
			AmountChanged:     req.HedgedAmount,
			RunningOpenAmount: newOpenAmount,
			UserID:            req.UserID,
		}
		if !runtime.Enforce(ctx, w, r, pool, runtime.EnforceInput{
			EventCode:           common.TriggerPreCreate,
			ModuleCode:          common.ModuleFX,
			SubModule:           "HEDGE_LINK",
			EntityCode:          exposureEntityForHeader(ctx, pool, req.ExposureHeaderID),
			ActorUserID:         req.UserID,
			HandlerName:         "LinkExposureHedge",
			APIPath:             "/fx/exposures/link-exposure-hedge",
			DefaultBlockMessage: "Exposure hedge link blocked by policy",
			Fields:              buildHedgeLinkPolicyFields(hedgeRow),
		}) {
			return
		}
		var linkExisted bool
		_ = pool.QueryRow(ctx, `
			SELECT EXISTS(
				SELECT 1 FROM exposure_hedge_links
				WHERE exposure_header_id = $1 AND booking_id = $2
			)`, req.ExposureHeaderID, req.BookingID).Scan(&linkExisted)
		// Upsert exposure_hedge_links as pending until All Linkage approve
		upsertQuery := `
			INSERT INTO exposure_hedge_links (exposure_header_id, booking_id, hedged_amount, is_active)
			VALUES ($1, $2, $3, false)
			ON CONFLICT (exposure_header_id, booking_id)
			DO UPDATE SET hedged_amount = EXCLUDED.hedged_amount, is_active = false
			RETURNING exposure_header_id, booking_id, hedged_amount, is_active`
		var link struct {
			ExposureHeaderID string
			BookingID        string
			HedgedAmount     float64
			IsActive         bool
		}
		err := pool.QueryRow(ctx, upsertQuery, req.ExposureHeaderID, req.BookingID, req.HedgedAmount).Scan(
			&link.ExposureHeaderID,
			&link.BookingID,
			&link.HedgedAmount,
			&link.IsActive,
		)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "Failed to upsert exposure hedge link")
			return
		}
		linkMap := map[string]interface{}{
			"exposure_header_id": link.ExposureHeaderID,
			"booking_id":         link.BookingID,
			"hedged_amount":      link.HedgedAmount,
			"is_active":          link.IsActive,
			"processing_status":  "PENDING_APPROVAL",
		}
		if _, auditErr := pool.Exec(ctx, `
			INSERT INTO public.auditactionhedgelink
				(exposure_header_id, booking_id, actiontype, processing_status, requested_by, requested_at, requested_ip, new_values, change_summary)
			VALUES ($1, $2, 'LINK', 'PENDING_APPROVAL', $3, now(), $4, $5, $5)
		`, req.ExposureHeaderID, req.BookingID, auditutil.Actor(req.UserID), auditutil.NullIfBlank(api.ClientIPFromRequest(r)), auditutil.JSONValue(linkMap)); auditErr != nil {
			logger.LogError("fx hedge link audit failed exposure=%s booking=%s: %v", req.ExposureHeaderID, req.BookingID, auditErr)
		}
		respondWithSuccess(w, http.StatusOK, "Exposure hedge link submitted for approval", map[string]interface{}{
			"link": linkMap,
		})

		actor := auditutil.Actor(req.UserID)
		if linkExisted {
			dmsjobs.FireDmsEvent(pool, "FX", "HEDGE_LINK", "POST_EDIT", []string{req.ExposureHeaderID}, actor)
		} else {
			dmsjobs.FireDmsEvent(pool, "FX", "HEDGE_LINK", "POST_CREATE", []string{req.ExposureHeaderID}, actor)
		}

		payload := fxnotif.BuildExposureBulkActionPayload(ctx, pool, fxnotif.ExposureBulkActionInput{
			ExposureIDs: []string{req.ExposureHeaderID}, Action: fxnotif.ActionLink, RequestedBy: req.UserID,
		})
		payloadMap := payload.ToMap()
		payloadMap["UserID"] = req.UserID
		payloadMap["BookingID"] = req.BookingID
		payloadMap["HedgedAmount"] = req.HedgedAmount
		fxnotif.TriggerFX(context.WithoutCancel(ctx), pool, fxnotif.SourceRouteLinkExposureHedge, fxnotif.CorrelationID("FXLINK", req.ExposureHeaderID), payloadMap)
	}
}

type hedgeLinkPair struct {
	ExposureHeaderID string `json:"exposure_header_id"`
	BookingID        string `json:"booking_id"`
}

// ApproveHedgeLinks activates pending links and posts utilization ledger rows.
func ApproveHedgeLinks(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID          string          `json:"user_id"`
			Links           []hedgeLinkPair `json:"links"`
			ApprovalComment string          `json:"approval_comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.Links) == 0 {
			respondWithError(w, http.StatusBadRequest, "user_id and links are required")
			return
		}

		approved := 0
		for _, link := range req.Links {
			expID := strings.TrimSpace(link.ExposureHeaderID)
			bookID := strings.TrimSpace(link.BookingID)
			if expID == "" || bookID == "" {
				continue
			}
			var hedged float64
			err := pool.QueryRow(ctx, `
				UPDATE exposure_hedge_links
				   SET is_active = true
				 WHERE exposure_header_id = $1 AND booking_id = $2
				 RETURNING hedged_amount
			`, expID, bookID).Scan(&hedged)
			if err != nil {
				logger.LogError("approve hedge link failed exposure=%s booking=%s: %v", expID, bookID, err)
				continue
			}

			var bookingAmount float64
			_ = pool.QueryRow(ctx, "SELECT Booking_Amount FROM forward_bookings WHERE system_transaction_id = $1 AND COALESCE(is_deleted, false) = false", bookID).Scan(&bookingAmount)
			var totalUtilized float64
			_ = pool.QueryRow(ctx, `SELECT COALESCE(SUM(amount_changed), 0) FROM forward_booking_ledger WHERE booking_id = $1 AND action_type IN ('UTILIZATION', 'CANCELLATION', 'ROLLOVER')`, bookID).Scan(&totalUtilized)
			newOpenAmount := math.Abs(math.Abs(bookingAmount) - math.Abs(totalUtilized) - math.Abs(hedged))
			_, _ = pool.Exec(ctx, `INSERT INTO forward_booking_ledger (booking_id, action_type, action_id, action_date, amount_changed, running_open_amount, user_id) VALUES ($1, 'UTILIZATION', $2, CURRENT_DATE, $3, $4, $5)`, bookID, expID, hedged, newOpenAmount, req.UserID)

			linkMap := map[string]interface{}{
				"exposure_header_id": expID,
				"booking_id":         bookID,
				"hedged_amount":      hedged,
				"is_active":          true,
				"processing_status":  "APPROVED",
			}
			if _, auditErr := pool.Exec(ctx, `
				INSERT INTO public.auditactionhedgelink
					(exposure_header_id, booking_id, actiontype, processing_status, reason, requested_by, requested_at, requested_ip, new_values, change_summary)
				VALUES ($1, $2, 'EDIT', 'APPROVED', $3, $4, now(), $5, $6, $6)
			`, expID, bookID, strings.TrimSpace(req.ApprovalComment), auditutil.Actor(req.UserID), auditutil.NullIfBlank(api.ClientIPFromRequest(r)), auditutil.JSONValue(linkMap)); auditErr != nil {
				logger.LogError("approve hedge link audit failed exposure=%s booking=%s: %v", expID, bookID, auditErr)
			}
			approved++
		}

		respondWithSuccess(w, http.StatusOK, "Hedge links approved successfully", map[string]interface{}{
			"approved": approved,
		})
	}
}

// RejectHedgeLinks deactivates selected links.
func RejectHedgeLinks(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID           string          `json:"user_id"`
			Links            []hedgeLinkPair `json:"links"`
			RejectionComment string          `json:"rejection_comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.Links) == 0 {
			respondWithError(w, http.StatusBadRequest, "user_id and links are required")
			return
		}

		rejected := 0
		for _, link := range req.Links {
			expID := strings.TrimSpace(link.ExposureHeaderID)
			bookID := strings.TrimSpace(link.BookingID)
			if expID == "" || bookID == "" {
				continue
			}
			var hedged float64
			err := pool.QueryRow(ctx, `
				UPDATE exposure_hedge_links
				   SET is_active = false
				 WHERE exposure_header_id = $1 AND booking_id = $2
				 RETURNING hedged_amount
			`, expID, bookID).Scan(&hedged)
			if err != nil {
				logger.LogError("reject hedge link failed exposure=%s booking=%s: %v", expID, bookID, err)
				continue
			}
			linkMap := map[string]interface{}{
				"exposure_header_id": expID,
				"booking_id":         bookID,
				"hedged_amount":      hedged,
				"is_active":          false,
				"processing_status":  "REJECTED",
			}
			if _, auditErr := pool.Exec(ctx, `
				INSERT INTO public.auditactionhedgelink
					(exposure_header_id, booking_id, actiontype, processing_status, reason, requested_by, requested_at, requested_ip, new_values, change_summary)
				VALUES ($1, $2, 'UNLINK', 'REJECTED', $3, $4, now(), $5, $6, $6)
			`, expID, bookID, strings.TrimSpace(req.RejectionComment), auditutil.Actor(req.UserID), auditutil.NullIfBlank(api.ClientIPFromRequest(r)), auditutil.JSONValue(linkMap)); auditErr != nil {
				logger.LogError("reject hedge link audit failed exposure=%s booking=%s: %v", expID, bookID, auditErr)
			}
			rejected++
		}

		respondWithSuccess(w, http.StatusOK, "Hedge links rejected successfully", map[string]interface{}{
			"rejected": rejected,
		})
	}
}

// Helper: containsString
func containsString(arr []string, s string) bool {
	for _, v := range arr {
		if v == s {
			return true
		}
	}
	return false
}
