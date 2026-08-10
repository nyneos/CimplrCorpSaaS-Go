package exposures

// expBucketing.go: Handles exposure bucketing logic and APIs.

import (
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/fx/auditutil"
	fxnotif "CimplrCorpSaas/api/fx/notification"
	"CimplrCorpSaas/api/policyengine/common"
	"CimplrCorpSaas/api/policyengine/runtime"
	"CimplrCorpSaas/internal/ctxutil"
	dmsjobs "CimplrCorpSaas/internal/jobs/dms"
	"CimplrCorpSaas/internal/logger"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"CimplrCorpSaas/api/constants"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const errLoadExposureBucketingRowForPolicyCheck = "Failed to load exposure_bucketing row for policy check: "

// // Helper: send JSON error response
// func respondWithError(w http.ResponseWriter, status int, errMsg string) {
// 	w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
// 	w.WriteHeader(status)
// 	json.NewEncoder(w).Encode(map[string]interface{}{
// 		constants.ValueSuccess: false,
// 		constants.ValueError:   errMsg,
// 	})
// }

// allowedExposureHeaderCols is the set of columns that callers may update on exposure_headers.
var allowedExposureHeaderCols = map[string]bool{
	"company_code": true, "entity": true, "entity1": true, "entity2": true, "entity3": true,
	"exposure_type": true, "document_id": true, "document_date": true, "counterparty_type": true,
	"counterparty_code": true, "counterparty_name": true, "currency": true,
	"total_original_amount": true, "total_open_amount": true, "value_date": true,
	"status": true, "approval_status": true, "approval_comment": true,
	"approved_by": true, "approved_at": true, "rejected_by": true, "rejected_at": true,
	"rejection_comment": true, "delete_comment": true, "requested_by": true,
	"amount_in_local_currency": true, "posting_date": true, "text": true,
	"gl_account": true, "reference": true, "additional_header_details": true,
	"exposure_category": true, "updated_at": true,
}

// allowedExposureLineItemCols is the set of columns that callers may update on exposure_line_items.
var allowedExposureLineItemCols = map[string]bool{
	"line_number": true, "product_id": true, "product_description": true,
	"quantity": true, "unit_of_measure": true, "unit_price": true,
	"line_item_amount": true, "plant_code": true, "delivery_date": true,
	"payment_terms": true, "inco_terms": true, "additional_line_details": true,
}

// allowedExposureBucketingCols is the set of columns that callers may update on exposure_bucketing.
var allowedExposureBucketingCols = map[string]bool{
	"status": true, "status_bucketing": true, "updated_by": true, "updated_at": true,
	"comments": true, "month_1": true, "month_2": true, "month_3": true, "month_4": true,
	"month_4_6": true, "month_6plus": true, "old_month1": true, "old_month2": true,
	"old_month3": true, "old_month4": true, "old_month4to6": true, "old_month6plus": true,
	"advance": true,
}

// allowedHedgingProposalCols maps client field names to safe hedging_proposal columns.
var allowedHedgingProposalCols = map[string]string{
	"status": "status", "status_hedging": "status_hedging", "updated_by": "updated_by", "updated_at": "updated_at", "comments": "comments",
}

// filterColumns returns a copy of fields containing only keys present in allowed.
// Returns an error listing the first rejected key so callers can surface it.
func filterColumns(fields map[string]interface{}, allowed map[string]bool) (map[string]interface{}, error) {
	out := make(map[string]interface{}, len(fields))
	for k, v := range fields {
		if !allowed[k] {
			return nil, fmt.Errorf("column %q is not permitted for update", k)
		}
		out[k] = v
	}
	return out, nil
}

func mapColumns(fields map[string]interface{}, allowed map[string]string) (map[string]interface{}, error) {
	out := make(map[string]interface{}, len(fields))
	for k, v := range fields {
		col, ok := allowed[k]
		if !ok {
			return nil, fmt.Errorf("column %q is not permitted for update", k)
		}
		out[col] = v
	}
	return out, nil
}

// Handler: Update exposure headers, line items, bucketing, hedging proposal
func UpdateExposureHeadersLineItemsBucketing(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID           string                 `json:"user_id"`
			ExposureHeaderID string                 `json:"exposure_header_id"`
			HeaderFields     map[string]interface{} `json:"headerFields"`
			LineItemFields   map[string]interface{} `json:"lineItemFields"`
			BucketingFields  map[string]interface{} `json:"bucketingFields"`
			Comments         string                 `json:"comments"`
			Reason           string                 `json:"reason"`
			// legacy/alternate payload key used by some clients
			Fields        map[string]interface{} `json:"fields"`
			HedgingFields map[string]interface{} `json:"hedgingFields"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ExposureHeaderID == "" {
			respondWithError(w, http.StatusBadRequest, "Invalid request body or exposure_header_id missing")
			return
		}

		// // Middleware: Validate session
		// activeSessions := auth.GetActiveSessions()
		// found := false
		// for _, session := range activeSessions {
		// 	if session.UserID == req.UserID {
		// 		found = true
		// 		break
		// 	}
		// }
		// if !found {
		// 	respondWithError(w, http.StatusUnauthorized, "Unauthorized: invalid session")
		// 	return
		// }

		// Middleware: Get business units from context
		// if !ok {
		// 	respondWithError(w, http.StatusInternalServerError, "Business units not found in context")
		// 	return
		// }

		updated := map[string]interface{}{}

		// Support legacy payloads that send bucketing data under `fields` key.
		if len(req.BucketingFields) == 0 && len(req.Fields) > 0 {
			req.BucketingFields = req.Fields
		}

		// Ensure exposure_bucketing row exists
		if len(req.BucketingFields) > 0 {
			var exists int
			err := pool.QueryRow(ctx, "SELECT 1 FROM exposure_bucketing WHERE exposure_header_id = $1", req.ExposureHeaderID).Scan(&exists)
			if errors.Is(err, pgx.ErrNoRows) {
				_, err := pool.Exec(ctx, "INSERT INTO exposure_bucketing (exposure_header_id) VALUES ($1)", req.ExposureHeaderID)
				if err != nil {
					respondWithError(w, http.StatusInternalServerError, "Failed to create exposure_bucketing row")
					return
				}
			}
		}

		// Ensure hedging_proposal row exists
		if len(req.HedgingFields) > 0 {
			var exists int
			err := pool.QueryRow(ctx, "SELECT 1 FROM hedging_proposal WHERE exposure_header_id = $1", req.ExposureHeaderID).Scan(&exists)
			if errors.Is(err, pgx.ErrNoRows) {
				_, err := pool.Exec(ctx, "INSERT INTO hedging_proposal (exposure_header_id) VALUES ($1)", req.ExposureHeaderID)
				if err != nil {
					respondWithError(w, http.StatusInternalServerError, "Failed to create hedging_proposal row")
					return
				}
			}
		}

		oldBucketing := map[string]interface{}(nil)
		if len(req.BucketingFields) > 0 {
			oldBucketing = auditutil.FetchRowSnapshotPGX(ctx, pool, "public.exposure_bucketing", "exposure_header_id", req.ExposureHeaderID)
		}
		oldHedging := map[string]interface{}(nil)
		if len(req.HedgingFields) > 0 {
			oldHedging = auditutil.FetchRowSnapshotPGX(ctx, pool, "public.hedging_proposal", "exposure_header_id", req.ExposureHeaderID)
		}

		// Update exposure_headers
		if len(req.HeaderFields) > 0 {
			safeHeaderFields, err := filterColumns(req.HeaderFields, allowedExposureHeaderCols)
			if err != nil {
				respondWithError(w, http.StatusBadRequest, "headerFields: "+err.Error())
				return
			}
			req.HeaderFields = safeHeaderFields
			setParts := []string{}
			values := []interface{}{}
			i := 1
			for k, v := range req.HeaderFields {
				setParts = append(setParts, fmt.Sprintf(constants.FormatSQLColumnArg, k, i))
				values = append(values, v)
				i++
			}
			values = append(values, req.ExposureHeaderID)
			query := fmt.Sprintf("UPDATE exposure_headers SET %s WHERE exposure_header_id = $%d RETURNING *", strings.Join(setParts, ", "), len(values))
			rows, err := pool.Query(ctx, query, values...)
			if err == nil {
				cols := pgxColumnNames(rows)
				for rows.Next() {
					vals := make([]interface{}, len(cols))
					valPtrs := make([]interface{}, len(cols))
					for i := range vals {
						valPtrs[i] = &vals[i]
					}
					if err := rows.Scan(valPtrs...); err != nil {
						logger.LogError("update exposure_headers: scan failed: %v", err)
					}
					rowMap := map[string]interface{}{}
					for i, col := range cols {
						rowMap[col] = parseDBValue(col, vals[i])
					}
					updated["header"] = rowMap
				}
				rows.Close()
				if _, err := pool.Exec(ctx, "UPDATE exposure_bucketing SET status_bucketing = 'pending' WHERE exposure_header_id = $1", req.ExposureHeaderID); err != nil {
					logger.LogError("update exposure_bucketing status after header update: exec failed: %v", err)
				}
			}
		}

		// Update exposure_line_items
		if len(req.LineItemFields) > 0 {
			safeLineItemFields, err := filterColumns(req.LineItemFields, allowedExposureLineItemCols)
			if err != nil {
				respondWithError(w, http.StatusBadRequest, "lineItemFields: "+err.Error())
				return
			}
			req.LineItemFields = safeLineItemFields
			setParts := []string{}
			values := []interface{}{}
			i := 1
			for k, v := range req.LineItemFields {
				setParts = append(setParts, fmt.Sprintf(constants.FormatSQLColumnArg, k, i))
				values = append(values, v)
				i++
			}
			values = append(values, req.ExposureHeaderID)
			query := fmt.Sprintf("UPDATE exposure_line_items SET %s WHERE exposure_header_id = $%d RETURNING *", strings.Join(setParts, ", "), len(values))
			rows, err := pool.Query(ctx, query, values...)
			if err == nil {
				cols := pgxColumnNames(rows)
				lineItems := []map[string]interface{}{}
				for rows.Next() {
					vals := make([]interface{}, len(cols))
					valPtrs := make([]interface{}, len(cols))
					for i := range vals {
						valPtrs[i] = &vals[i]
					}
					if err := rows.Scan(valPtrs...); err != nil {
						logger.LogError("update exposure_line_items: scan failed: %v", err)
					}
					rowMap := map[string]interface{}{}
					for i, col := range cols {
						rowMap[col] = parseDBValue(col, vals[i])
					}
					lineItems = append(lineItems, rowMap)
				}
				updated["lineItems"] = lineItems
				rows.Close()
				if _, err := pool.Exec(ctx, "UPDATE exposure_bucketing SET status_bucketing = 'pending' WHERE exposure_header_id = $1", req.ExposureHeaderID); err != nil {
					logger.LogError("update exposure_bucketing status after line items update: exec failed: %v", err)
				}
			}
		}

		// Update exposure_bucketing
		if len(req.BucketingFields) > 0 {
			safeBucketingFields, err := filterColumns(req.BucketingFields, allowedExposureBucketingCols)
			if err != nil {
				respondWithError(w, http.StatusBadRequest, "bucketingFields: "+err.Error())
				return
			}
			req.BucketingFields = safeBucketingFields
			req.BucketingFields["status_bucketing"] = constants.StatusPendingEditApproval

			bucketingRow, err := loadExposureBucketingRow(ctx, pool, req.ExposureHeaderID)
			if err != nil {
				respondWithError(w, http.StatusInternalServerError, errLoadExposureBucketingRowForPolicyCheck+err.Error())
				return
			}
			bucketingRow = applyExposureBucketingEdits(bucketingRow, req.BucketingFields)
			if ok, msg := runtime.EnforceInline(ctx, r, pool, runtime.EnforceInput{
				EventCode:           common.TriggerPreEdit,
				ModuleCode:          common.ModuleFX,
				SubModule:           "EXPOSURE_BUCKETING",
				EntityCode:          exposureEntityForHeader(ctx, pool, req.ExposureHeaderID),
				ActorUserID:         req.UserID,
				HandlerName:         "UpdateExposureHeadersLineItemsBucketing",
				APIPath:             "/fx/exposures/update-bucketing",
				DefaultBlockMessage: "Exposure bucketing update blocked by policy",
				Fields:              buildExposureBucketingPolicyFields(bucketingRow),
			}); !ok {
				respondWithError(w, http.StatusUnprocessableEntity, msg)
				return
			}

			setParts := []string{}
			values := []interface{}{}
			i := 1
			for k, v := range req.BucketingFields {
				setParts = append(setParts, fmt.Sprintf(constants.FormatSQLColumnArg, k, i))
				values = append(values, v)
				i++
			}
			values = append(values, req.ExposureHeaderID)
			query := fmt.Sprintf("UPDATE exposure_bucketing SET %s WHERE exposure_header_id = $%d RETURNING *", strings.Join(setParts, ", "), len(values))
			rows, err := pool.Query(ctx, query, values...)
			if err == nil {
				cols := pgxColumnNames(rows)
				bucketing := []map[string]interface{}{}
				for rows.Next() {
					vals := make([]interface{}, len(cols))
					valPtrs := make([]interface{}, len(cols))
					for i := range vals {
						valPtrs[i] = &vals[i]
					}
					if err := rows.Scan(valPtrs...); err != nil {
						logger.LogError("update exposure_bucketing: scan failed: %v", err)
					}
					rowMap := map[string]interface{}{}
					for i, col := range cols {
						rowMap[col] = parseDBValue(col, vals[i])
					}
					bucketing = append(bucketing, rowMap)
				}
				updated["bucketing"] = bucketing
				rows.Close()
			}
		}

		// Update hedging_proposal
		if len(req.HedgingFields) > 0 {
			safeHedgingFields, err := mapColumns(req.HedgingFields, allowedHedgingProposalCols)
			if err != nil {
				respondWithError(w, http.StatusBadRequest, "hedgingFields: "+err.Error())
				return
			}
			req.HedgingFields = safeHedgingFields
			setParts := []string{}
			values := []interface{}{}
			i := 1
			for k, v := range req.HedgingFields {
				setParts = append(setParts, fmt.Sprintf(constants.FormatSQLColumnArg, k, i))
				values = append(values, v)
				i++
			}
			values = append(values, req.ExposureHeaderID)
			query := fmt.Sprintf("UPDATE hedging_proposal SET %s WHERE exposure_header_id = $%d RETURNING *", strings.Join(setParts, ", "), len(values))
			rows, err := pool.Query(ctx, query, values...)
			if err == nil {
				cols := pgxColumnNames(rows)
				hedging := []map[string]interface{}{}
				for rows.Next() {
					vals := make([]interface{}, len(cols))
					valPtrs := make([]interface{}, len(cols))
					for i := range vals {
						valPtrs[i] = &vals[i]
					}
					if err := rows.Scan(valPtrs...); err != nil {
						logger.LogError("update hedging_proposal: scan failed: %v", err)
					}
					rowMap := map[string]interface{}{}
					for i, col := range cols {
						rowMap[col] = parseDBValue(col, vals[i])
					}
					hedging = append(hedging, rowMap)
				}
				updated["hedging"] = hedging
				rows.Close()
				if _, err := pool.Exec(ctx, "UPDATE hedging_proposal SET status_hedging = 'pending' WHERE exposure_header_id = $1", req.ExposureHeaderID); err != nil {
					logger.LogError("update hedging_proposal status: exec failed: %v", err)
				}
			}
		}

		if len(updated) == 0 {
			respondWithError(w, http.StatusNotFound, "No records updated")
			return
		}
		actor := auditutil.Actor(req.UserID)
		reason := strings.TrimSpace(req.Comments)
		if reason == "" {
			reason = strings.TrimSpace(req.Reason)
		}
		if len(req.BucketingFields) > 0 {
			newValues := any(req.BucketingFields)
			if bucketingRows, ok := updated["bucketing"].([]map[string]interface{}); ok && len(bucketingRows) > 0 {
				newValues = bucketingRows[0]
			}
			auditutil.RecordActionPGX(ctx, pool, auditutil.ActionParams{TableName: auditutil.TableExposureBucketing, ParentColumn: "exposure_header_id", ParentID: req.ExposureHeaderID, ActionType: "EDIT", Status: constants.StatusPendingEditApproval, Reason: reason, RequestedBy: actor, OldValues: oldBucketing, NewValues: newValues})
		}
		if len(req.HedgingFields) > 0 {
			newValues := any(req.HedgingFields)
			if hedgingRows, ok := updated["hedging"]; ok {
				newValues = hedgingRows
			}
			auditutil.RecordActionPGX(ctx, pool, auditutil.ActionParams{TableName: auditutil.TableHedgeProposal, ParentColumn: "exposure_header_id", ParentID: req.ExposureHeaderID, ActionType: "EDIT", Status: constants.StatusPendingEditApproval, Reason: "", RequestedBy: actor, OldValues: oldHedging, NewValues: newValues})
		}
		respondWithSuccess(w, http.StatusOK, "Exposure bucketing updated successfully", map[string]interface{}{
			"updated": updated,
		})

		fxnotif.NotifyExposureBulkAction(ctx, pool, fxnotif.BulkActionNotifyInput{
			SourceRoute: fxnotif.SourceRouteBucketingUpdate, Action: fxnotif.ActionUpdate, UserID: req.UserID, RequestedBy: actor, CheckerComment: reason,
			ExposureIDs: []string{req.ExposureHeaderID}, ResultBuckets: nil,
		})
	}
}

// Handler: Get exposure headers, line items, and bucketing for accessible business units
func GetExposureHeadersLineItemsBucketing(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrPleaseLogin)
			return
		}

		// Validate session
		// activeSessions := auth.GetActiveSessions()
		// var session *auth.UserSession
		// for _, s := range activeSessions {
		// 	if s.UserID == req.UserID {
		// 		session = s
		// 		break
		// 	}
		// }
		// if session == nil {
		// 	respondWithError(w, http.StatusNotFound, "Session expired or not found. Please login again.")
		// 	return
		// }

		// Get business units from context (set by middleware)
		scope := ctxutil.FromContext(ctx)
		buNames := scope.EntityNames
		if len(buNames) == 0 {
			respondWithError(w, http.StatusNotFound, constants.ErrNoAccessibleBusinessUnit)
			return
		}

		// Ensure all exposure_header_id are present in exposure_bucketing
		_, _ = pool.Exec(ctx, `INSERT INTO exposure_bucketing (exposure_header_id)
			SELECT exposure_header_id
			FROM exposure_headers
			WHERE entity = ANY($1)
			  AND (approval_status = 'approved' OR approval_status = 'Approved')
			  AND COALESCE(is_deleted, false) = false
			  AND exposure_header_id NOT IN (
				SELECT exposure_header_id FROM exposure_bucketing
			  )`, buNames)

		// Join exposure_headers, exposure_line_items, exposure_bucketing
		rows, err := pool.Query(ctx, `SELECT h.*, l.*, b.*
			FROM exposure_headers h
			JOIN exposure_line_items l ON h.exposure_header_id = l.exposure_header_id
			LEFT JOIN exposure_bucketing b ON h.exposure_header_id = b.exposure_header_id
			WHERE h.entity = ANY($1)
			  AND (h.approval_status = 'approved' OR h.approval_status = 'Approved')
			  AND COALESCE(h.is_deleted, false) = false`, buNames)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "Failed to fetch joined exposures")
			return
		}
		defer rows.Close()
		cols := pgxColumnNames(rows)
		pageData := []map[string]interface{}{}
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
				rowMap[col] = parseDBValue(col, vals[i])
			}
			pageData = append(pageData, rowMap)
		}

		// Fetch permissions for 'exposure-bucketing' page for this role
		exposureBucketingPerms := map[string]interface{}{}
		var roleId int
		err = pool.QueryRow(ctx, "SELECT role_id FROM user_roles WHERE user_id = $1 LIMIT 1", req.UserID).Scan(&roleId)
		if err == nil {
			permRows, err := pool.Query(ctx, `
				SELECT p.page_name, p.tab_name, p.action, rp.allowed
				FROM role_permissions rp
				JOIN permissions p ON rp.permission_id = p.id
				WHERE rp.role_id = $1 AND (rp.status = 'Approved' OR rp.status = 'approved')
			`, roleId)
			if err == nil {
				defer permRows.Close()
				for permRows.Next() {
					var pageName, tabName, action string
					var allowed bool
					if err := permRows.Scan(&pageName, &tabName, &action, &allowed); err == nil {
						if pageName != constants.ExposureBucketing {
							continue
						}
						if exposureBucketingPerms[constants.ExposureBucketing] == nil {
							exposureBucketingPerms[constants.ExposureBucketing] = map[string]interface{}{}
						}
						perms := exposureBucketingPerms[constants.ExposureBucketing].(map[string]interface{})
						if tabName == "" {
							if perms["pagePermissions"] == nil {
								perms["pagePermissions"] = map[string]interface{}{}
							}
							perms["pagePermissions"].(map[string]interface{})[action] = allowed
						} else {
							if perms["tabs"] == nil {
								perms["tabs"] = map[string]interface{}{}
							}
							if perms["tabs"].(map[string]interface{})[tabName] == nil {
								perms["tabs"].(map[string]interface{})[tabName] = map[string]interface{}{}
							}
							perms["tabs"].(map[string]interface{})[tabName].(map[string]interface{})[action] = allowed
						}
					}
				}
			}
		}

		resp := map[string]interface{}{
			"buAccessible": buNames,
			"pageData":     pageData,
		}
		if perms, ok := exposureBucketingPerms[constants.ExposureBucketing]; ok {
			resp[constants.ExposureBucketing] = perms
		}
		respondWithSuccess(w, http.StatusOK, "Exposure bucketing data fetched successfully", resp)
	}
}

func DeleteBucketingStatus(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID            string   `json:"user_id"`
			ExposureHeaderIds []string `json:"exposureHeaderIds"`
			DeleteComment     string   `json:"delete_comment"`
			Reason            string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.ExposureHeaderIds) == 0 || req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, "exposureHeaderIds and user_id are required")
			return
		}

		var requestedBy string
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == req.UserID {
				requestedBy = s.Name
				break
			}
		}
		if requestedBy == "" {
			respondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		deleteComment := strings.TrimSpace(req.DeleteComment)
		if deleteComment == "" {
			deleteComment = strings.TrimSpace(req.Reason)
		}

		for _, id := range req.ExposureHeaderIds {
			bucketingRow, err := loadExposureBucketingRow(ctx, pool, id)
			if err != nil {
				respondWithError(w, http.StatusInternalServerError, errLoadExposureBucketingRowForPolicyCheck+err.Error())
				return
			}
			if ok, msg := runtime.EnforceInline(ctx, r, pool, runtime.EnforceInput{
				EventCode:           common.TriggerPreDelete,
				ModuleCode:          common.ModuleFX,
				SubModule:           "EXPOSURE_BUCKETING",
				EntityCode:          exposureEntityForHeader(ctx, pool, id),
				ActorUserID:         req.UserID,
				HandlerName:         "DeleteBucketingStatus",
				APIPath:             "/fx/exposures/bucketing/delete-multiple-headers",
				DefaultBlockMessage: "Exposure bucketing delete blocked by policy",
				Fields:              buildExposureBucketingPolicyFields(bucketingRow),
			}); !ok {
				respondWithError(w, http.StatusUnprocessableEntity, msg)
				return
			}
		}

		rows, err := pool.Query(ctx,
			`WITH target AS (
				SELECT b.exposure_header_id,
				       COALESCE(b.status_bucketing, '') AS old_status_bucketing,
				       COALESCE(b.comments, '') AS old_comments
				FROM exposure_bucketing b
				JOIN exposure_headers h ON h.exposure_header_id = b.exposure_header_id
				WHERE b.exposure_header_id = ANY($1::uuid[])
				  AND COALESCE(h.is_deleted, false) = false
			)
			UPDATE exposure_bucketing b
			SET status_bucketing = 'PENDING_DELETE_APPROVAL',
			    updated_by = $2,
			    comments = $3,
			    updated_at = NOW()
			FROM target t
			WHERE b.exposure_header_id = t.exposure_header_id
			RETURNING b.exposure_header_id, t.old_status_bucketing, t.old_comments`,
			req.ExposureHeaderIds,
			requestedBy,
			deleteComment,
		)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		deleted := []string{}
		for rows.Next() {
			var id, oldStatusBucketing, oldComments string
			if err := rows.Scan(&id, &oldStatusBucketing, &oldComments); err == nil {
				deleted = append(deleted, id)
				oldValues := map[string]interface{}{
					"status_bucketing": oldStatusBucketing,
					"comments":         oldComments,
				}
				newValues := map[string]interface{}{
					"status_bucketing": constants.StatusPendingDeleteApproval,
					"comments":         deleteComment,
				}
				auditutil.RecordActionPGX(ctx, pool, auditutil.ActionParams{TableName: auditutil.TableExposureBucketing, ParentColumn: "exposure_header_id", ParentID: id, ActionType: "DELETE", Status: constants.StatusPendingDeleteApproval, Reason: deleteComment, RequestedBy: requestedBy, OldValues: oldValues, NewValues: newValues})
			}
		}
		if len(deleted) == 0 {
			respondWithError(w, http.StatusNotFound, "No eligible exposure bucketing rows found")
			return
		}

		respondWithSuccess(w, http.StatusOK, "Exposure bucketing rows marked for delete approval", map[string]interface{}{
			"deleted": deleted,
		})

		fxnotif.NotifyExposureBulkAction(ctx, pool, fxnotif.BulkActionNotifyInput{
			SourceRoute: fxnotif.SourceRouteBucketingDelete, Action: fxnotif.ActionDelete, UserID: req.UserID, RequestedBy: requestedBy, CheckerComment: deleteComment,
			ExposureIDs: req.ExposureHeaderIds, ResultBuckets: map[string][]string{
				"deleted": deleted,
			},
		})
	}
}

func ApproveBucketingStatus(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID            string   `json:"user_id"`
			ExposureHeaderIds []string `json:"exposure_header_ids"`
			Comments          string   `json:"comments"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.ExposureHeaderIds) == 0 || req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, "exposure_header_ids and user_id are required")
			return
		}

		// Get updatedBy from session
		var updatedBy string
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == req.UserID {
				updatedBy = s.Name // or s.Email
				break
			}
		}
		if updatedBy == "" {
			respondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		tx, err := pool.Begin(ctx)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer tx.Rollback(ctx)

		statusRows, err := tx.Query(ctx,
			`SELECT exposure_header_id, COALESCE(status_bucketing, '')
			 FROM exposure_bucketing
			 WHERE exposure_header_id = ANY($1::uuid[])`,
			req.ExposureHeaderIds,
		)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		toDelete := []string{}
		toCreateApprove := []string{}
		toEditApprove := []string{}
		for statusRows.Next() {
			var id, status string
			if err := statusRows.Scan(&id, &status); err == nil {
				if strings.EqualFold(status, constants.StatusPendingDeleteApproval) {
					toDelete = append(toDelete, id)
				} else if strings.EqualFold(status, constants.StatusPendingEditApproval) {
					toEditApprove = append(toEditApprove, id)
				} else {
					toCreateApprove = append(toCreateApprove, id)
				}
			}
		}
		statusRows.Close()
		toApprove := append(append([]string{}, toCreateApprove...), toEditApprove...)

		for _, id := range req.ExposureHeaderIds {
			bucketingRow, err := loadExposureBucketingRow(ctx, pool, id)
			if err != nil {
				respondWithError(w, http.StatusInternalServerError, errLoadExposureBucketingRowForPolicyCheck+err.Error())
				return
			}
			if ok, msg := runtime.EnforceInline(ctx, r, pool, runtime.EnforceInput{
				EventCode:           common.TriggerPreApprove,
				ModuleCode:          common.ModuleFX,
				SubModule:           "EXPOSURE_BUCKETING",
				EntityCode:          exposureEntityForHeader(ctx, pool, id),
				ActorUserID:         req.UserID,
				HandlerName:         "ApproveBucketingStatus",
				APIPath:             "/fx/exposures/approve-bucketing-status",
				DefaultBlockMessage: "Exposure bucketing approval blocked by policy",
				Fields:              buildExposureBucketingPolicyFields(bucketingRow),
			}); !ok {
				respondWithError(w, http.StatusUnprocessableEntity, msg)
				return
			}
		}

		approved := []map[string]interface{}{}
		if len(toApprove) > 0 {
			rows, err := tx.Query(ctx,
				`UPDATE exposure_bucketing
	             SET status_bucketing = 'Approved', updated_by = $2, comments = $3, updated_at = NOW()
	             WHERE exposure_header_id = ANY($1::uuid[])
	             RETURNING *`,
				toApprove, updatedBy, req.Comments,
			)
			if err != nil {
				respondWithError(w, http.StatusInternalServerError, err.Error())
				return
			}
			cols := pgxColumnNames(rows)
			for rows.Next() {
				vals := make([]interface{}, len(cols))
				valPtrs := make([]interface{}, len(cols))
				for i := range vals {
					valPtrs[i] = &vals[i]
				}
				if err := rows.Scan(valPtrs...); err != nil {
					logger.LogError("approve exposure_bucketing: scan failed: %v", err)
				}
				rowMap := map[string]interface{}{}
				for i, col := range cols {
					rowMap[col] = parseDBValue(col, vals[i])
				}
				approved = append(approved, rowMap)
			}
			rows.Close()
		}

		deleted := []string{}
		if len(toDelete) > 0 {
			delRows, err := tx.Query(ctx,
				`UPDATE exposure_headers
				 SET is_deleted = TRUE,
				     deleted_at = NOW(),
				     deleted_by = $2
				 WHERE exposure_header_id = ANY($1::uuid[])
				   AND COALESCE(is_deleted, false) = false
				 RETURNING exposure_header_id`,
				toDelete, updatedBy,
			)
			if err != nil {
				respondWithError(w, http.StatusInternalServerError, err.Error())
				return
			}
			for delRows.Next() {
				var id string
				if err := delRows.Scan(&id); err == nil {
					deleted = append(deleted, id)
				}
			}
			delRows.Close()

			if len(deleted) > 0 {
				if _, err := tx.Exec(ctx,
					`UPDATE exposure_bucketing
					 SET status_bucketing = 'Approved', updated_by = $2, comments = $3, updated_at = NOW()
					 WHERE exposure_header_id = ANY($1::uuid[])`,
					deleted, updatedBy, req.Comments,
				); err != nil {
					respondWithError(w, http.StatusInternalServerError, err.Error())
					return
				}
			}
		}

		if err := tx.Commit(ctx); err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}

		for _, rowMap := range approved {
			if id, ok := rowMap["exposure_header_id"]; ok {
				auditutil.RecordDecisionPGX(ctx, pool, auditutil.DecisionParams{TableName: auditutil.TableExposureBucketing, ParentColumn: "exposure_header_id", ParentID: fmt.Sprint(id), Status: constants.StatusApproved, CheckerBy: updatedBy, Comment: req.Comments})
			}
		}
		for _, id := range deleted {
			auditutil.RecordDecisionPGX(ctx, pool, auditutil.DecisionParams{TableName: auditutil.TableExposureBucketing, ParentColumn: "exposure_header_id", ParentID: id, Status: constants.StatusApproved, CheckerBy: updatedBy, Comment: req.Comments})
		}

		approvedIDSet := map[string]struct{}{}
		for _, rowMap := range approved {
			if id, ok := rowMap["exposure_header_id"]; ok {
				approvedIDSet[fmt.Sprint(id)] = struct{}{}
			}
		}
		createApprovedIDs := filterBucketingHeaderIDs(toCreateApprove, approvedIDSet)
		editApprovedIDs := filterBucketingHeaderIDs(toEditApprove, approvedIDSet)
		if len(createApprovedIDs) > 0 {
			dmsjobs.FireDmsEvent(pool, "FX", "EXPOSURE_BUCKETING", "POST_APPROVE", createApprovedIDs, updatedBy)
		}
		if len(editApprovedIDs) > 0 {
			dmsjobs.FireDmsEvent(pool, "FX", "EXPOSURE_BUCKETING", "POST_EDIT", editApprovedIDs, updatedBy)
		}
		if len(deleted) > 0 {
			dmsjobs.FireDmsEvent(pool, "FX", "EXPOSURE_BUCKETING", "POST_DELETE", deleted, updatedBy)
		}

		respondWithSuccess(w, http.StatusOK, "Exposure bucketing rows approved successfully", map[string]interface{}{
			"Approved": approved,
			"deleted":  deleted,
		})

		notifApprovedIDs := make([]string, 0, len(approved))
		for _, rowMap := range approved {
			if id, ok := rowMap["exposure_header_id"]; ok {
				notifApprovedIDs = append(notifApprovedIDs, fmt.Sprint(id))
			}
		}
		fxnotif.NotifyExposureBulkAction(ctx, pool, fxnotif.BulkActionNotifyInput{
			SourceRoute: fxnotif.SourceRouteBucketingApprove, Action: fxnotif.ActionApprove, UserID: req.UserID, RequestedBy: updatedBy, CheckerComment: req.Comments,
			ExposureIDs: req.ExposureHeaderIds, ResultBuckets: map[string][]string{
				"approved": notifApprovedIDs,
				"deleted":  deleted,
			},
		})
	}
}

func RejectBucketingStatus(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID            string   `json:"user_id"`
			ExposureHeaderIds []string `json:"exposure_header_ids"`
			Comments          string   `json:"comments"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.ExposureHeaderIds) == 0 || req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, "exposure_header_ids and user_id are required")
			return
		}

		// Get updatedBy from session
		var updatedBy string
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == req.UserID {
				updatedBy = s.Name // or s.Email
				break
			}
		}
		if updatedBy == "" {
			respondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		for _, id := range req.ExposureHeaderIds {
			bucketingRow, err := loadExposureBucketingRow(ctx, pool, id)
			if err != nil {
				respondWithError(w, http.StatusInternalServerError, errLoadExposureBucketingRowForPolicyCheck+err.Error())
				return
			}
			if ok, msg := runtime.EnforceInline(ctx, r, pool, runtime.EnforceInput{
				EventCode:           common.TriggerPreReject,
				ModuleCode:          common.ModuleFX,
				SubModule:           "EXPOSURE_BUCKETING",
				EntityCode:          exposureEntityForHeader(ctx, pool, id),
				ActorUserID:         req.UserID,
				HandlerName:         "RejectBucketingStatus",
				APIPath:             "/fx/exposures/reject-bucketing-status",
				DefaultBlockMessage: "Exposure bucketing rejection blocked by policy",
				Fields:              buildExposureBucketingPolicyFields(bucketingRow),
			}); !ok {
				respondWithError(w, http.StatusUnprocessableEntity, msg)
				return
			}
		}

		rows, err := pool.Query(ctx,
			`UPDATE exposure_bucketing
             SET status_bucketing = 'Rejected',
		     updated_by = $2, comments = $3, updated_at = NOW()
             WHERE exposure_header_id = ANY($1::uuid[])
             RETURNING *`,
			req.ExposureHeaderIds, updatedBy, req.Comments,
		)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()
		cols := pgxColumnNames(rows)
		rejected := []map[string]interface{}{}
		for rows.Next() {
			vals := make([]interface{}, len(cols))
			valPtrs := make([]interface{}, len(cols))
			for i := range vals {
				valPtrs[i] = &vals[i]
			}
			if err := rows.Scan(valPtrs...); err != nil {
				logger.LogError("reject exposure_bucketing: scan failed: %v", err)
			}
			rowMap := map[string]interface{}{}
			for i, col := range cols {
				rowMap[col] = parseDBValue(col, vals[i])
			}
			rejected = append(rejected, rowMap)
			if id, ok := rowMap["exposure_header_id"]; ok {
				auditutil.RecordDecisionPGX(ctx, pool, auditutil.DecisionParams{TableName: auditutil.TableExposureBucketing, ParentColumn: "exposure_header_id", ParentID: fmt.Sprint(id), Status: constants.StatusRejected, CheckerBy: updatedBy, Comment: req.Comments})
			}
		}
		rejectedIDs := make([]string, 0, len(rejected))
		for _, rowMap := range rejected {
			if id, ok := rowMap["exposure_header_id"]; ok {
				rejectedIDs = append(rejectedIDs, fmt.Sprint(id))
			}
		}
		if len(rejectedIDs) > 0 {
			dmsjobs.FireDmsEvent(pool, "FX", "EXPOSURE_BUCKETING", "POST_REJECT", rejectedIDs, updatedBy)
		}

		respondWithSuccess(w, http.StatusOK, "Exposure bucketing rows rejected successfully", map[string]interface{}{
			"Rejected": rejected,
		})

		fxnotif.NotifyExposureBulkAction(ctx, pool, fxnotif.BulkActionNotifyInput{
			SourceRoute: fxnotif.SourceRouteBucketingReject, Action: fxnotif.ActionReject, UserID: req.UserID, RequestedBy: updatedBy, CheckerComment: req.Comments,
			ExposureIDs: req.ExposureHeaderIds, ResultBuckets: map[string][]string{
				"rejected": rejectedIDs,
			},
		})
	}
}

func filterBucketingHeaderIDs(candidates []string, approved map[string]struct{}) []string {
	if len(candidates) == 0 {
		return nil
	}
	out := make([]string, 0, len(candidates))
	for _, id := range candidates {
		if _, ok := approved[id]; ok {
			out = append(out, id)
		}
	}
	return out
}
