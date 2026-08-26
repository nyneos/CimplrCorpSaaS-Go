package forwards

import (
	"CimplrCorpSaas/api/approvalengine"
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/fx/auditutil"
	"CimplrCorpSaas/api/policyengine/common"
	"CimplrCorpSaas/api/policyengine/runtime"
	"CimplrCorpSaas/internal/ctxutil"
	dmsjobs "CimplrCorpSaas/internal/jobs/dms"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"CimplrCorpSaas/api/constants"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func isForwardPendingDeleteStatus(status string) bool {
	normalized := strings.ToUpper(strings.TrimSpace(status))
	return normalized == constants.FwdProcessingStatusPendingDeleteApproval || normalized == strings.ToUpper(constants.FwdProcessingStatusDeleteApproval)
}

func isForwardPendingEditStatus(status string) bool {
	return strings.EqualFold(strings.TrimSpace(status), constants.FwdProcessingStatusPendingEditApproval)
}

func normalizeForwardSystemTransactionID(value interface{}) string {
	switch v := value.(type) {
	case nil:
		return ""
	case []byte:
		return strings.TrimSpace(string(v))
	default:
		text := strings.TrimSpace(fmt.Sprint(v))
		if text == "<nil>" {
			return ""
		}
		return text
	}
}

func UpdateForwardBookingFields(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Accept system_transaction_id and fields in the JSON body
		var req struct {
			SystemTransactionID string                 `json:"system_transaction_id"`
			Fields              map[string]interface{} `json:"fields"`
			UserID              string                 `json:"user_id"`
			Reason              string                 `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.SystemTransactionID == "" || len(req.Fields) == 0 {
			respondEnvelopeError(w, http.StatusBadRequest, "system_transaction_id and at least one field to update must be provided in body")
			return
		}
		// Check if booking exists
		var exists bool
		err := pool.QueryRow(r.Context(), "SELECT EXISTS(SELECT 1 FROM forward_bookings WHERE system_transaction_id = $1)", req.SystemTransactionID).Scan(&exists)
		if err != nil || !exists {
			respondEnvelopeError(w, http.StatusNotFound, "No matching forward booking found")
			return
		}
		// Get valid columns for forward_bookings
		colRows, err := pool.Query(r.Context(), `SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'forward_bookings'`)
		if err != nil {
			respondEnvelopeError(w, http.StatusInternalServerError, "Failed to fetch columns")
			return
		}
		validCols := map[string]bool{}
		for colRows.Next() {
			var col string
			if err := colRows.Scan(&col); err == nil {
				validCols[col] = true
			}
		}
		colRows.Close()
		// Filter fields to only valid columns (ignore system_transaction_id)
		updateFields := map[string]interface{}{}
		for k, v := range req.Fields {
			if validCols[k] && k != "system_transaction_id" {
				updateFields[k] = v
			}
		}
		// Edits require checker approval before becoming approved again.
		updateFields["processing_status"] = constants.FwdProcessingStatusPendingEditApproval
		if len(updateFields) == 0 {
			respondEnvelopeError(w, http.StatusBadRequest, "No valid fields to update")
			return
		}
		oldValues := auditutil.FetchRowSnapshotPGX(r.Context(), pool, "public.forward_bookings", "system_transaction_id", req.SystemTransactionID)
		// Policy check sees the post-edit view (old row + patch), matching the FD booking edit pattern.
		mergedFields := map[string]interface{}{}
		for k, v := range oldValues {
			mergedFields[k] = v
		}
		for k, v := range updateFields {
			mergedFields[k] = v
		}
		entityCode, _ := oldValues["entity_level_0"].(string)
		ok, tID := runtime.EnforceWithMatrix(r.Context(), w, r, pool, runtime.EnforceInput{
			EventCode:           common.TriggerPreEdit,
			ModuleCode:          common.ModuleFX,
			SubModule:           "FORWARD_BOOKING",
			EntityCode:          entityCode,
			ActorUserID:         req.UserID,
			HandlerName:         "UpdateForwardBookingFields",
			APIPath:             "/fx/forwards/update-fields",
			DefaultBlockMessage: "Forward booking edit blocked by policy",
			Fields:              mergedFields,
		})
		if !ok {
			return
		}
		// Build dynamic SET clause
		keys := make([]string, 0, len(updateFields))
		values := make([]interface{}, 0, len(updateFields)+1)
		for k := range updateFields {
			keys = append(keys, k)
		}
		setClause := make([]string, len(keys))
		for i, k := range keys {
			setClause[i] = fmt.Sprintf(constants.FormatSQLColumnArg, k, i+1)
			values = append(values, updateFields[k])
		}
		values = append(values, req.SystemTransactionID)
		updateQuery := fmt.Sprintf("UPDATE forward_bookings SET %s WHERE system_transaction_id = $%d RETURNING row_to_json(forward_bookings)", strings.Join(setClause, ", "), len(values))
		var updatedRaw []byte
		if err := pool.QueryRow(r.Context(), updateQuery, values...).Scan(&updatedRaw); err != nil {
			respondEnvelopeError(w, http.StatusNotFound, "No matching forward booking found after update")
			return
		}
		result := map[string]interface{}{}
		if err := json.Unmarshal(updatedRaw, &result); err != nil {
			respondEnvelopeError(w, http.StatusInternalServerError, "Failed to parse updated forward booking")
			return
		}
		requestedBy := auditutil.Actor(req.UserID)
		if strings.TrimSpace(requestedBy) == "" {
			requestedBy = auditutil.ActorFromContext(r.Context())
		}

		makerEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				makerEmail = s.Email
				break
			}
		}

		auditutil.RecordActionPGX(r.Context(), pool, auditutil.ActionParams{TableName: auditutil.TableForwardBooking, ParentColumn: "system_transaction_id", ParentID: req.SystemTransactionID, ActionType: constants.FwdActionTypeEdit, Status: constants.FwdProcessingStatusPendingEditApproval, Reason: strings.TrimSpace(req.Reason), RequestedBy: requestedBy, OldValues: oldValues, NewValues: result})
		triggerForwardBookingNotif(r.Context(), pool, routeForwardUpdateFields, "UPDATE", requestedBy, constants.FwdProcessingStatusPendingEditApproval, []string{req.SystemTransactionID})

		go func(id, email, matrixID string) {
			bgCtx := context.Background()
			_ = approvalengine.CancelPendingInstances(bgCtx, pool, "FX", id, email)
			_, _ = approvalengine.CreateInstance(bgCtx, pool, approvalengine.InstanceRequest{
				ModuleCode:          "FX",
				TransactionType:     "FX_FORWARD_EDIT",
				RecordID:            id,
				MatrixID:            matrixID,
				RequirePinnedMatrix: true,
				AutoApplyIfUnpinned: true,
				SubmittedByEmail:    email,
			})
		}(req.SystemTransactionID, makerEmail, tID)

		respondEnvelopeSuccess(w, "Forward booking fields updated successfully", map[string]interface{}{
			"updated": result,
		})
	}
}

// Shared approve/reject decision path for forward bookings (bank.go-style split APIs).
type forwardBookingDecisionReq struct {
	UserID               string   `json:"user_id"`
	SystemTransactionIDs []string `json:"system_transaction_ids"`
	Comment              string   `json:"comment"`
}

func scanForwardBookingReturningRows(rows pgx.Rows) []map[string]interface{} {
	out := make([]map[string]interface{}, 0)
	fieldDescs := rows.FieldDescriptions()
	cols := make([]string, len(fieldDescs))
	for i, fd := range fieldDescs {
		cols[i] = fd.Name
	}
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		valPtrs := make([]interface{}, len(cols))
		for i := range vals {
			valPtrs[i] = &vals[i]
		}
		if err := rows.Scan(valPtrs...); err != nil {
			continue
		}
		rowMap := make(map[string]interface{}, len(cols))
		for i, col := range cols {
			rowMap[col] = normalizeMTMRowValue(vals[i])
		}
		out = append(out, rowMap)
	}
	return out
}

// forwardBookingDecisionMeta bundles the per-call-site metadata for
// applyForwardBookingDecision (approve/reject/etc. share the same body logic).
type forwardBookingDecisionMeta struct {
	ProcessingStatus string
	HandlerName      string
	APIPath          string
	NotifRoute       string
	SuccessMessage   string
}

func applyForwardBookingDecision(
	pool *pgxpool.Pool,
	w http.ResponseWriter,
	r *http.Request,
	req forwardBookingDecisionReq,
	meta forwardBookingDecisionMeta,
) {
	processingStatus, handlerName, apiPath, notifRoute, successMessage :=
		meta.ProcessingStatus, meta.HandlerName, meta.APIPath, meta.NotifRoute, meta.SuccessMessage
	if len(req.SystemTransactionIDs) == 0 {
		respondEnvelopeError(w, http.StatusBadRequest, "system_transaction_ids (array) required")
		return
	}
	scope := ctxutil.FromContext(r.Context())
	buNames := scope.EntityNames
	if len(buNames) == 0 {
		respondEnvelopeError(w, http.StatusForbidden, constants.ErrNoAccessibleBusinessUnit)
		return
	}
	actor := auditutil.Actor(req.UserID)
	actorEmail := ""
	for _, s := range auth.GetActiveSessions() {
		if s.UserID == req.UserID {
			actorEmail = s.Email
			break
		}
	}
	engineActedMap := make(map[string]bool)
	engineAwaitingMap := make(map[string]bool)
	for _, id := range req.SystemTransactionIDs {
		gate := fxApprovalGate(r.Context(), pool, req.UserID, actorEmail, id, processingStatus, req.Comment)
		if gate.Blocked {
			respondEnvelopeError(w, http.StatusUnprocessableEntity, gate.Reason)
			return
		}
		if gate.Acted {
			engineActedMap[id] = true
			if !gate.Finalized {
				engineAwaitingMap[id] = true
			}
		}
	}
	policyEvent := common.TriggerPreApprove
	if processingStatus == constants.FwdProcessingStatusRejected {
		policyEvent = common.TriggerPreReject
	}
	enforceForwardStatusPolicy := func(entityCode, txnID string) (bool, string) {
		fields := map[string]interface{}{
			"system_transaction_id": txnID,
			"processing_status":     processingStatus,
		}
		// Approve/Reject only ever receive an id — load the full canonical
		// row so the policy check sees the same field set Create does, not
		// just the two touched keys. Fall back to the thin map if the row
		// can't be loaded (e.g. already deleted) so the action still gets a
		// policy decision.
		if row, err := loadForwardBookingRow(r.Context(), pool, txnID); err == nil {
			fields = buildForwardBookingPolicyFields(row)
			fields["processing_status"] = processingStatus
		}
		return runtime.EnforceInline(r.Context(), r, pool, runtime.EnforceInput{
			EventCode:           policyEvent,
			ModuleCode:          common.ModuleFX,
			SubModule:           "FORWARD_BOOKING",
			EntityCode:          entityCode,
			ActorUserID:         req.UserID,
			HandlerName:         handlerName,
			APIPath:             apiPath,
			DefaultBlockMessage: "Forward booking status update blocked by policy",
			Fields:              fields,
		})
	}

	delRows, err := pool.Query(r.Context(), `
		SELECT system_transaction_id, entity_level_0
		FROM forward_bookings
		WHERE system_transaction_id = ANY($1)
		  AND processing_status IN ($2, $3)
	`, req.SystemTransactionIDs,
		constants.FwdProcessingStatusDeleteApproval,
		constants.FwdProcessingStatusPendingDeleteApproval,
	)
	if err != nil {
		respondEnvelopeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	var deletedIds []string
	var deletedRows []map[string]interface{}
	for delRows.Next() {
		var id, entityLevel0 string
		if err := delRows.Scan(&id, &entityLevel0); err == nil {
			if engineAwaitingMap[id] {
				continue
			}
			for _, bu := range buNames {
				if bu == entityLevel0 {
					deletedIds = append(deletedIds, id)
					break
				}
			}
		}
	}
	delRows.Close()

	if len(deletedIds) > 0 && processingStatus == constants.FwdProcessingStatusApproved {
		for _, id := range deletedIds {
			var entityLevel0 string
			_ = pool.QueryRow(r.Context(), `SELECT entity_level_0 FROM forward_bookings WHERE system_transaction_id = $1`, id).Scan(&entityLevel0)
			if ok, msg := enforceForwardStatusPolicy(entityLevel0, id); !ok {
				respondEnvelopeError(w, http.StatusUnprocessableEntity, msg)
				return
			}
		}
		rows, err := pool.Query(r.Context(), `
			UPDATE forward_bookings
			SET processing_status = $3,
			    is_deleted = TRUE,
			    deleted_at = now(),
			    deleted_by = $2
			WHERE system_transaction_id = ANY($1)
			  AND UPPER(COALESCE(processing_status, '')) IN ('DELETE-APPROVAL', 'PENDING_DELETE_APPROVAL')
			RETURNING *
		`, deletedIds, req.UserID, constants.FwdProcessingStatusApproved)
		if err != nil {
			respondEnvelopeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		deletedRows = append(deletedRows, scanForwardBookingReturningRows(rows)...)
		rows.Close()
	}
	if len(deletedIds) > 0 && processingStatus == constants.FwdProcessingStatusRejected {
		for _, id := range deletedIds {
			var entityLevel0 string
			_ = pool.QueryRow(r.Context(), `SELECT entity_level_0 FROM forward_bookings WHERE system_transaction_id = $1`, id).Scan(&entityLevel0)
			if ok, msg := enforceForwardStatusPolicy(entityLevel0, id); !ok {
				respondEnvelopeError(w, http.StatusUnprocessableEntity, msg)
				return
			}
		}
		rows, err := pool.Query(r.Context(), `
			UPDATE forward_bookings
			SET processing_status = $2
			WHERE system_transaction_id = ANY($1)
			  AND UPPER(COALESCE(processing_status, '')) IN ('DELETE-APPROVAL', 'PENDING_DELETE_APPROVAL')
			RETURNING *
		`, deletedIds, constants.FwdProcessingStatusRejected)
		if err != nil {
			respondEnvelopeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		deletedRows = append(deletedRows, scanForwardBookingReturningRows(rows)...)
		rows.Close()
	}

	updateIds := make([]string, 0, len(req.SystemTransactionIDs))
	deletedSet := make(map[string]bool, len(deletedIds))
	for _, id := range deletedIds {
		deletedSet[id] = true
	}
	for _, id := range req.SystemTransactionIDs {
		if !deletedSet[id] {
			updateIds = append(updateIds, id)
		}
	}

	var updatedRows []map[string]interface{}
	statusByID := map[string]string{}
	if len(updateIds) > 0 {
		statusRows, statusErr := pool.Query(r.Context(), `
				SELECT system_transaction_id, COALESCE(processing_status, '')
				FROM forward_bookings
				WHERE system_transaction_id = ANY($1)
			`, updateIds)
		if statusErr != nil {
			respondEnvelopeError(w, http.StatusInternalServerError, statusErr.Error())
			return
		}
		for statusRows.Next() {
			var id, processingStatus string
			if scanErr := statusRows.Scan(&id, &processingStatus); scanErr == nil {
				statusByID[id] = processingStatus
			}
		}
		statusRows.Close()
	}
	if len(updateIds) > 0 {
		rows, err := pool.Query(r.Context(), `SELECT system_transaction_id, entity_level_0 FROM forward_bookings WHERE system_transaction_id = ANY($1)`, updateIds)
		if err != nil {
			respondEnvelopeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		var eligibleIds []string
		for rows.Next() {
			var id, entityLevel0 string
			if err := rows.Scan(&id, &entityLevel0); err == nil {
				if engineAwaitingMap[id] {
					continue
				}
				for _, bu := range buNames {
					if bu == entityLevel0 {
						eligibleIds = append(eligibleIds, id)
						break
					}
				}
			}
		}
		rows.Close()
		if len(eligibleIds) > 0 {
			for _, id := range eligibleIds {
				var entityLevel0 string
				_ = pool.QueryRow(r.Context(), `SELECT entity_level_0 FROM forward_bookings WHERE system_transaction_id = $1`, id).Scan(&entityLevel0)
				if ok, msg := enforceForwardStatusPolicy(entityLevel0, id); !ok {
					respondEnvelopeError(w, http.StatusUnprocessableEntity, msg)
					return
				}
			}
			var resultRows pgx.Rows
			if processingStatus == constants.FwdProcessingStatusApproved {
				// Checker approval finalizes confirmation.
				resultRows, err = pool.Query(r.Context(), `
					UPDATE forward_bookings
					SET processing_status = $1, status = $2
					WHERE system_transaction_id = ANY($3)
					RETURNING *`,
					processingStatus, constants.FwdStatusConfirmed, eligibleIds)
			} else {
				resultRows, err = pool.Query(r.Context(), `UPDATE forward_bookings SET processing_status = $1 WHERE system_transaction_id = ANY($2) RETURNING *`, constants.FwdProcessingStatusRejected, eligibleIds)
			}
			if err == nil {
				updatedRows = append(updatedRows, scanForwardBookingReturningRows(resultRows)...)
				resultRows.Close()
			}
		}
	}

	if len(updatedRows) == 0 && len(deletedIds) == 0 && len(engineAwaitingMap) == 0 {
		respondEnvelopeError(w, http.StatusNotFound, "No matching forward bookings found")
		return
	}

	decisionStatus := strings.ToUpper(processingStatus)
	if processingStatus == constants.FwdProcessingStatusRejected {
		decisionStatus = constants.StatusRejected
	}
	decisionComment := strings.TrimSpace(req.Comment)
	for _, row := range deletedRows {
		id := normalizeForwardSystemTransactionID(row["system_transaction_id"])
		if id == "" {
			continue
		}
		if !engineActedMap[id] {
			auditutil.RecordDecisionPGX(r.Context(), pool, auditutil.DecisionParams{TableName: auditutil.TableForwardBooking, ParentColumn: "system_transaction_id", ParentID: id, Status: decisionStatus, CheckerBy: actor, Comment: decisionComment})
		}
	}
	for _, row := range updatedRows {
		if id, ok := row["system_transaction_id"]; ok {
			if normalizedID := normalizeForwardSystemTransactionID(id); normalizedID != "" {
				if !engineActedMap[normalizedID] {
					auditutil.RecordDecisionPGX(r.Context(), pool, auditutil.DecisionParams{TableName: auditutil.TableForwardBooking, ParentColumn: "system_transaction_id", ParentID: normalizedID, Status: decisionStatus, CheckerBy: actor, Comment: decisionComment})
				}
			}
		}
	}
	notifIDs := append(collectBookingIDsFromRows(updatedRows), deletedIds...)
	action := strings.ToUpper(processingStatus)
	if len(deletedIds) > 0 && processingStatus == constants.FwdProcessingStatusApproved {
		action = "DELETE_APPROVE"
	} else if len(deletedIds) > 0 {
		action = "DELETE_REJECT"
	}
	triggerForwardBookingNotif(r.Context(), pool, notifRoute, action, actor, processingStatus, notifIDs)
	if processingStatus == constants.FwdProcessingStatusApproved {
		deleteApprovedIDs := collectBookingIDsFromRows(deletedRows)
		if len(deleteApprovedIDs) > 0 {
			dmsjobs.FireDmsEvent(pool, "FX", "FX_CONFIRMATION", "POST_DELETE", deleteApprovedIDs, actor)
		}
	}
	if statusIDs := collectBookingIDsFromRows(updatedRows); len(statusIDs) > 0 && processingStatus == constants.FwdProcessingStatusApproved {
		createApprovedIDs := make([]string, 0, len(statusIDs))
		editApprovedIDs := make([]string, 0, len(statusIDs))
		for _, id := range statusIDs {
			if isForwardPendingEditStatus(statusByID[id]) {
				editApprovedIDs = append(editApprovedIDs, id)
			} else {
				createApprovedIDs = append(createApprovedIDs, id)
			}
		}
		if len(createApprovedIDs) > 0 {
			dmsjobs.FireDmsEvent(pool, "FX", "FX_CONFIRMATION", "POST_APPROVE", createApprovedIDs, actor)
		}
		if len(editApprovedIDs) > 0 {
			dmsjobs.FireDmsEvent(pool, "FX", "FX_CONFIRMATION", "POST_EDIT", editApprovedIDs, actor)
		}
	} else if statusIDs := collectBookingIDsFromRows(updatedRows); len(statusIDs) > 0 {
		trig := "POST_APPROVE"
		if processingStatus == constants.FwdProcessingStatusRejected {
			trig = "POST_REJECT"
		}
		dmsjobs.FireDmsEvent(pool, "FX", "FX_CONFIRMATION", trig, statusIDs, actor)
	}
	respondEnvelopeSuccess(w, successMessage, map[string]interface{}{
		"updated": updatedRows,
		"deleted": deletedIds,
	})
}

// BulkApproveForwardBookings approves pending forward bookings (and pending-delete approvals).
func BulkApproveForwardBookings(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req forwardBookingDecisionReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			respondEnvelopeError(w, http.StatusBadRequest, constants.ErrUserIDRequired)
			return
		}
		applyForwardBookingDecision(pool, w, r, req, forwardBookingDecisionMeta{
			ProcessingStatus: constants.FwdProcessingStatusApproved,
			HandlerName:      "BulkApproveForwardBookings",
			APIPath:          routeForwardBulkApprove,
			NotifRoute:       routeForwardBulkApprove,
			SuccessMessage:   "Forward bookings approved successfully",
		})
	}
}

// BulkRejectForwardBookings rejects pending forward bookings (and pending-delete approvals).
func BulkRejectForwardBookings(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req forwardBookingDecisionReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			respondEnvelopeError(w, http.StatusBadRequest, constants.ErrUserIDRequired)
			return
		}
		applyForwardBookingDecision(pool, w, r, req, forwardBookingDecisionMeta{
			ProcessingStatus: constants.FwdProcessingStatusRejected,
			HandlerName:      "BulkRejectForwardBookings",
			APIPath:          routeForwardBulkReject,
			NotifRoute:       routeForwardBulkReject,
			SuccessMessage:   "Forward bookings rejected successfully",
		})
	}
}

// Handler: BulkDeleteForwardBookings
func BulkDeleteForwardBookings(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID               string   `json:"user_id"`
			SystemTransactionIDs []string `json:"system_transaction_ids"`
			Reason               string   `json:"reason"`
			Comment              string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			respondEnvelopeError(w, http.StatusBadRequest, constants.ErrUserIDRequired)
			return
		}
		if len(req.SystemTransactionIDs) == 0 {
			respondEnvelopeError(w, http.StatusBadRequest, "system_transaction_ids (array) required")
			return
		}
		scope := ctxutil.FromContext(r.Context())
		buNames := scope.EntityNames
		if len(buNames) == 0 {
			respondEnvelopeError(w, http.StatusForbidden, constants.ErrNoAccessibleBusinessUnit)
			return
		}
		// Only update those belonging to accessible business units
		rows, err := pool.Query(r.Context(), `SELECT system_transaction_id, entity_level_0 FROM forward_bookings WHERE system_transaction_id = ANY($1)`, req.SystemTransactionIDs)
		if err != nil {
			respondEnvelopeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		var eligibleIds []string
		for rows.Next() {
			var id, entityLevel0 string
			if err := rows.Scan(&id, &entityLevel0); err == nil {
				for _, bu := range buNames {
					if bu == entityLevel0 {
						eligibleIds = append(eligibleIds, id)
						break
					}
				}
			}
		}
		rows.Close()
		if len(eligibleIds) == 0 {
			respondEnvelopeError(w, http.StatusNotFound, "No matching forward bookings found")
			return
		}
		oldValuesByID := make(map[string]map[string]interface{}, len(eligibleIds))
		policyEligible := make([]string, 0, len(eligibleIds))
		triggerMatrices := make(map[string]string)
		for _, id := range eligibleIds {
			row, err := loadForwardBookingRow(r.Context(), pool, id)
			if err != nil {
				respondEnvelopeError(w, http.StatusInternalServerError, "Failed to load forward booking for policy check: "+err.Error())
				return
			}
			var tID string
			if ok, msg, matID := runtime.EnforceInlineWithMatrix(r.Context(), r, pool, runtime.EnforceInput{
				EventCode:           common.TriggerPreDelete,
				ModuleCode:          common.ModuleFX,
				SubModule:           "FORWARD_BOOKING",
				EntityCode:          row.EntityLevel0,
				ActorUserID:         req.UserID,
				HandlerName:         "BulkDeleteForwardBookings",
				APIPath:             "/fx/forwards/bulk-delete",
				DefaultBlockMessage: "Forward booking delete blocked by policy",
				Fields:              buildForwardBookingPolicyFields(row),
			}); !ok {
				respondEnvelopeError(w, http.StatusForbidden, msg)
				return
			} else {
				tID = matID
			}
			oldValuesByID[id] = auditutil.FetchRowSnapshotPGX(r.Context(), pool, "public.forward_bookings", "system_transaction_id", id)
			policyEligible = append(policyEligible, id)
			triggerMatrices[id] = tID
		}
		eligibleIds = policyEligible
		updateQuery := `
			UPDATE forward_bookings
			SET processing_status = $2
			WHERE system_transaction_id = ANY($1)
			  AND COALESCE(is_deleted, false) = false
			RETURNING *
		`
		resultRows, err := pool.Query(r.Context(), updateQuery, eligibleIds, constants.FwdProcessingStatusPendingDeleteApproval)
		if err != nil {
			respondEnvelopeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		fieldDescs := resultRows.FieldDescriptions()
		cols := make([]string, len(fieldDescs))
		for i, fd := range fieldDescs {
			cols[i] = fd.Name
		}
		var updated []map[string]interface{}
		for resultRows.Next() {
			vals := make([]interface{}, len(cols))
			valPtrs := make([]interface{}, len(cols))
			for i := range vals {
				valPtrs[i] = &vals[i]
			}
			if err := resultRows.Scan(valPtrs...); err == nil {
				rowMap := make(map[string]interface{})
				for i, col := range cols {
					rowMap[col] = normalizeMTMRowValue(vals[i])
				}
				updated = append(updated, rowMap)
				if id, ok := rowMap["system_transaction_id"]; ok {
					if normalizedID := normalizeForwardSystemTransactionID(id); normalizedID != "" {
						reason := strings.TrimSpace(req.Reason)
						if reason == "" {
							reason = strings.TrimSpace(req.Comment)
						}
						auditutil.RecordActionPGX(r.Context(), pool, auditutil.ActionParams{TableName: auditutil.TableForwardBooking, ParentColumn: "system_transaction_id", ParentID: normalizedID, ActionType: constants.FwdActionTypeDelete, Status: constants.FwdProcessingStatusPendingDeleteApproval, Reason: reason, RequestedBy: auditutil.Actor(req.UserID), OldValues: oldValuesByID[normalizedID], NewValues: rowMap})
					}
				}
			}
		}
		resultRows.Close()
		triggerForwardBookingNotif(r.Context(), pool, routeForwardBulkDelete, "DELETE", auditutil.Actor(req.UserID), constants.FwdProcessingStatusPendingDeleteApproval, collectBookingIDsFromRows(updated))

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
					ModuleCode:          "FX",
					TransactionType:     "FX_FORWARD_DELETE",
					RecordID:            id,
					MatrixID:            matrices[id],
					SubmittedByEmail:    email,
					RequirePinnedMatrix: true,
					AutoApplyIfUnpinned: true,
				})
			}
		}(collectBookingIDsFromRows(updated), makerEmail, triggerMatrices)

		respondEnvelopeSuccess(w, "Forward bookings marked for deletion successfully", map[string]interface{}{
			"updated": updated,
		})
	}
}

// Handler: AddForwardConfirmationManualEntry
func AddForwardConfirmationManualEntry(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID               string `json:"user_id"`
			InternalReferenceID  string `json:"internal_reference_id"`
			EntityLevel0         string `json:"entity_level_0"`
			BankTransactionID    string `json:"bank_transaction_id"`
			SwiftUniqueID        string `json:"swift_unique_id"`
			BankConfirmationDate string `json:"bank_confirmation_date"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			respondEnvelopeError(w, http.StatusBadRequest, constants.ErrUserIDRequired)
			return
		}
		scope := ctxutil.FromContext(r.Context())
		buNames := scope.EntityNames
		if len(buNames) == 0 {
			respondEnvelopeError(w, http.StatusForbidden, constants.ErrNoAccessibleBusinessUnit)
			return
		}
		found := false
		for _, bu := range buNames {
			if bu == req.EntityLevel0 {
				found = true
				break
			}
		}
		if !found {
			respondEnvelopeError(w, http.StatusForbidden, "You do not have access to this business unit")
			return
		}
		var systemTransactionID string
		if err := pool.QueryRow(r.Context(), `
			SELECT system_transaction_id::text
			FROM forward_bookings
			WHERE internal_reference_id = $1 AND entity_level_0 = $2
			  AND (
			    UPPER(COALESCE(status, '')) = 'DRAFT'
			    OR status = 'Pending Confirmation'
			  )
			  AND COALESCE(is_deleted,false) = false
			LIMIT 1`,
			req.InternalReferenceID, req.EntityLevel0,
		).Scan(&systemTransactionID); err != nil || strings.TrimSpace(systemTransactionID) == "" {
			respondEnvelopeError(w, http.StatusNotFound, "No matching record found or already confirmed")
			return
		}
		bookingRow, err := loadForwardBookingRow(r.Context(), pool, systemTransactionID)
		if err != nil {
			respondEnvelopeError(w, http.StatusInternalServerError, "Failed to load forward booking for policy check: "+err.Error())
			return
		}
		bookingRow.Status = constants.FwdStatusPendingConfirmation
		bookingRow.BankTransactionID = req.BankTransactionID
		bookingRow.SwiftUniqueID = req.SwiftUniqueID
		bookingRow.BankConfirmationDate = req.BankConfirmationDate
		bookingRow.ProcessingStatus = constants.FwdProcessingStatusPendingApproval
		if !runtime.Enforce(r.Context(), w, r, pool, runtime.EnforceInput{
			EventCode:           common.TriggerPreEdit,
			ModuleCode:          common.ModuleFX,
			SubModule:           "FORWARD_BOOKING",
			EntityCode:          req.EntityLevel0,
			ActorUserID:         req.UserID,
			HandlerName:         "AddForwardConfirmationManualEntry",
			APIPath:             "/fx/forwards/manual-confirmation-entry",
			DefaultBlockMessage: "Forward confirmation blocked by policy",
			Fields:              buildForwardBookingPolicyFields(bookingRow),
		}) {
			return
		}
		// Convert empty string date fields to nil
		bankConfirmationDate := req.BankConfirmationDate
		if bankConfirmationDate == "" {
			bankConfirmationDate = "1970-01-01" // or set to nil if you want NULL
		}
		updateQuery := `UPDATE forward_bookings SET
		       status = $1,
		       bank_transaction_id = $2,
		       swift_unique_id = $3,
		       bank_confirmation_date = $4,
		       processing_status = $5
	       WHERE internal_reference_id = $6
	         AND entity_level_0 = $7
	         AND (
	           UPPER(COALESCE(status, '')) = 'DRAFT'
	           OR status = 'Pending Confirmation'
	         )
	       RETURNING internal_reference_id, entity_level_0, bank_transaction_id, swift_unique_id, bank_confirmation_date, status, processing_status`
		var bankConfirmationDateVal interface{}
		if bankConfirmationDate == "" {
			bankConfirmationDateVal = nil
		} else {
			bankConfirmationDateVal = bankConfirmationDate
		}
		updateValues := []interface{}{
			constants.FwdStatusPendingConfirmation,
			req.BankTransactionID,
			req.SwiftUniqueID,
			bankConfirmationDateVal,
			constants.FwdProcessingStatusPendingApproval,
			req.InternalReferenceID,
			req.EntityLevel0,
		}
		row := pool.QueryRow(r.Context(), updateQuery, updateValues...)
		cols := []string{"internal_reference_id", "entity_level_0", "bank_transaction_id", "swift_unique_id", "bank_confirmation_date", constants.KeyStatus, "processing_status"}
		vals := make([]interface{}, len(cols))
		valPtrs := make([]interface{}, len(cols))
		for i := range vals {
			valPtrs[i] = &vals[i]
		}
		if err := row.Scan(valPtrs...); err != nil {
			respondEnvelopeError(w, http.StatusNotFound, "No matching record found or already confirmed")
			return
		}
		result := make(map[string]interface{})
		for i, col := range cols {
			result[col] = vals[i]
		}
		if strings.TrimSpace(systemTransactionID) != "" {
			auditutil.RecordActionPGX(r.Context(), pool, auditutil.ActionParams{TableName: auditutil.TableForwardBooking, ParentColumn: "system_transaction_id", ParentID: systemTransactionID, ActionType: constants.FwdActionTypeConfirm, Status: constants.FwdProcessingStatusPendingApproval, Reason: "", RequestedBy: auditutil.Actor(req.UserID), OldValues: nil, NewValues: result})
			triggerForwardConfirmationNotif(r.Context(), pool, routeForwardManualConfirmation, "CONFIRM", auditutil.Actor(req.UserID), constants.FwdProcessingStatusPendingApproval, []string{systemTransactionID})
		}
		respondEnvelopeSuccess(w, "Forward confirmation recorded successfully", map[string]interface{}{
			"updated": result,
		})
	}
}
