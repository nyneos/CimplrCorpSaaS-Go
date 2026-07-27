package exposures

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/fx/auditutil"
	fxexposures "CimplrCorpSaas/api/fx/exposures"
	fxnotif "CimplrCorpSaas/api/fx/notification"
	"CimplrCorpSaas/api/policyengine/common"
	policyruntime "CimplrCorpSaas/api/policyengine/runtime"

	"CimplrCorpSaas/api/constants"

	"github.com/jackc/pgx/v5/pgxpool"
)

func BulkUpdateValueDates(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID string `json:"user_id"`
			Rows   []struct {
				ExposureHeaderID string `json:"exposure_header_id"`
				NewValueDate     string `json:"new_value_date"`
			} `json:"payload"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			respondEnvelopeError(w, http.StatusBadRequest, constants.ErrInvalidJSONPrefix+err.Error(), v91ErrorCode(http.StatusBadRequest))
			return
		}
		if req.UserID == "" {
			respondEnvelopeError(w, http.StatusBadRequest, constants.ErrUserIDRequired, v91ErrorCode(http.StatusBadRequest))
			return
		}
		if len(req.Rows) == 0 {
			respondEnvelopeError(w, http.StatusBadRequest, "empty payload", v91ErrorCode(http.StatusBadRequest))
			return
		}
		requester := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				requester = s.Name
				break
			}
		}
		if requester == "" {
			respondEnvelopeError(w, http.StatusUnauthorized, constants.ErrInvalidSession, v91ErrorCode(http.StatusUnauthorized))
			return
		}
		updated := make([]string, 0, len(req.Rows))

		for i, p := range req.Rows {
			if p.ExposureHeaderID == "" {
				respondEnvelopeError(w, http.StatusBadRequest, fmt.Sprintf("missing exposure_header_id at index %d", i), v91ErrorCode(http.StatusBadRequest))
				return
			}
			if p.NewValueDate == "" {
				respondEnvelopeError(w, http.StatusBadRequest, fmt.Sprintf("missing new_value_date at index %d", i), v91ErrorCode(http.StatusBadRequest))
				return
			}
			dt, err := parseFlexibleDate(p.NewValueDate)
			if err != nil {
				respondEnvelopeError(w, http.StatusBadRequest, fmt.Sprintf("invalid date at index %d: %v", i, err), v91ErrorCode(http.StatusBadRequest))
				return
			}
			creationRow, err := fxexposures.LoadExposureCreationRow(ctx, pool, p.ExposureHeaderID)
			if err != nil {
				respondEnvelopeError(w, http.StatusInternalServerError, constants.ErrDBPrefix+err.Error(), v91ErrorCode(http.StatusInternalServerError))
				return
			}
			creationRow = fxexposures.ApplyExposureCreationEdits(creationRow, map[string]interface{}{"value_date": p.NewValueDate})
			if ok, msg := policyruntime.EnforceInline(ctx, r, pool, policyruntime.EnforceInput{
				EventCode:           common.TriggerPreEdit,
				ModuleCode:          common.ModuleFX,
				SubModule:           v91ExposurePolicySubModule(r),
				EntityCode:          p.ExposureHeaderID,
				ActorUserID:         req.UserID,
				HandlerName:         "BulkUpdateValueDates",
				APIPath:             r.URL.Path,
				DefaultBlockMessage: "Exposure value-date update blocked by policy",
				Fields:              fxexposures.BuildExposureCreationPolicyFields(creationRow),
			}); !ok {
				respondEnvelopeError(w, http.StatusUnprocessableEntity, msg, v91ErrorCode(http.StatusUnprocessableEntity))
				return
			}
			q := `
				UPDATE public.exposure_headers
				SET 
					value_date = $1,
					exposure_creation_status = 'Pending',
					requested_by = $2,
					updated_at = now()
				WHERE exposure_header_id = $3
				RETURNING exposure_header_id
			`

			oldValues := auditutil.FetchRowSnapshotPGX(ctx, pool, constants.ExposureHeaders, "exposure_header_id", p.ExposureHeaderID)
			row := pool.QueryRow(ctx, q, dt, requester, p.ExposureHeaderID)

			var id string
			if err := row.Scan(&id); err != nil {
				respondEnvelopeError(w, http.StatusInternalServerError, fmt.Sprintf("db update error at index %d: %v", i, err), v91ErrorCode(http.StatusInternalServerError))
				return
			}

			updated = append(updated, id)
			newValues := auditutil.FetchRowSnapshotPGX(ctx, pool, constants.ExposureHeaders, "exposure_header_id", id)
			auditutil.RecordActionPGX(ctx, pool, auditutil.ActionParams{TableName: auditutil.TableExposure, ParentColumn: "exposure_header_id", ParentID: id, ActionType: "EDIT", Status: constants.StatusPendingEditApproval, Reason: "", RequestedBy: requester, OldValues: oldValues, NewValues: newValues})
		}
		respondEnvelopeSuccess(w, "value_date updated successfully", map[string]interface{}{
			"updated": updated,
		})

		fxnotif.NotifyExposureBulkAction(ctx, pool, fxnotif.BulkActionNotifyInput{
			SourceRoute: fxnotif.SourceRouteV91BulkUpdateValueDate, Action: fxnotif.ActionUpdate, UserID: req.UserID, RequestedBy: requester, CheckerComment: "",
			ExposureIDs: updated, ResultBuckets: map[string][]string{
				"updated": updated,
			},
		})
	}
}

func parseFlexibleDate(dateStr string) (time.Time, error) {
	dateStr = strings.TrimSpace(dateStr)
	if dateStr == "" {
		return time.Time{}, fmt.Errorf("empty date")
	}
	dateStr = regexp.MustCompile(`\s+`).ReplaceAllString(dateStr, " ")
	layouts := []string{
		constants.DateFormat,
		"2006/01/02",
		"2006.01.02",
		time.RFC3339,
		constants.DateTimeFormat,
		constants.DateFormatISO,
		"2006-01-02T15:04:05Z",
		"2006-01-02T15:04:05.000Z",
		constants.DateFormatAlt,
		"02/01/2006",
		"02.01.2006",
		"01-02-2006",
		"01/02/2006",
		"01.02.2006",
		constants.DateFormatDash,
		"2-Jan-2006",
		"02 Jan 2006",
		"Jan 02, 2006",
	}
	for _, l := range layouts {
		if t, err := time.Parse(l, dateStr); err == nil {
			y := t.Year()
			if y >= 1900 && y <= 9999 {
				return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC), nil
			}
		}
	}

	// numeric heuristics
	digits := true
	for _, r := range dateStr {
		if r < '0' || r > '9' {
			digits = false
			break
		}
	}
	if digits {
		if len(dateStr) == 8 {
			if y, err := strconv.Atoi(dateStr[0:4]); err == nil {
				if m, err := strconv.Atoi(dateStr[4:6]); err == nil {
					if d, err := strconv.Atoi(dateStr[6:8]); err == nil {
						if y >= 1900 && y <= 9999 {
							return time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.UTC), nil
						}
					}
				}
			}
		}
		if v, err := strconv.ParseInt(dateStr, 10, 64); err == nil {
			var t time.Time
			switch {
			case v >= 1e17:
				t = time.Unix(0, v)
			case v >= 1e14:
				t = time.Unix(0, v*1000)
			case v >= 1e11:
				t = time.Unix(0, v*1000000)
			case v >= 1e9:
				t = time.Unix(v, 0)
			default:
				base := time.Date(1899, 12, 30, 0, 0, 0, 0, time.UTC)
				t = base.AddDate(0, 0, int(v))
			}
			if y := t.Year(); y >= 1900 && y <= 9999 {
				return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC), nil
			}
		}
	}
	return time.Time{}, fmt.Errorf("unrecognized date format: %s", dateStr)
}
func BulkApproveExposures(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID      string   `json:"user_id"`
			ExposureIDs []string `json:"exposure_ids"`
			Comment     string   `json:"comment,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.ExposureIDs) == 0 {
			respondEnvelopeError(w, http.StatusBadRequest, constants.ErrInvalidJSON, v91ErrorCode(http.StatusBadRequest))
			return
		}
		approver := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				approver = s.Name
				break
			}
		}
		if approver == "" {
			respondEnvelopeError(w, http.StatusUnauthorized, constants.ErrInvalidSession, v91ErrorCode(http.StatusUnauthorized))
			return
		}
		for _, id := range req.ExposureIDs {
			creationRow, err := fxexposures.LoadExposureCreationRow(ctx, pool, id)
			if err != nil {
				respondEnvelopeError(w, http.StatusInternalServerError, constants.ErrDBPrefix+err.Error(), v91ErrorCode(http.StatusInternalServerError))
				return
			}
			if ok, msg := policyruntime.EnforceInline(ctx, r, pool, policyruntime.EnforceInput{
				EventCode:           common.TriggerPreApprove,
				ModuleCode:          common.ModuleFX,
				SubModule:           v91ExposurePolicySubModule(r),
				EntityCode:          id,
				ActorUserID:         req.UserID,
				HandlerName:         "BulkApproveExposures",
				APIPath:             r.URL.Path,
				DefaultBlockMessage: "Exposure approval blocked by policy",
				Fields:              fxexposures.BuildExposureCreationPolicyFields(creationRow),
			}); !ok {
				respondEnvelopeError(w, http.StatusUnprocessableEntity, msg, v91ErrorCode(http.StatusUnprocessableEntity))
				return
			}
		}

		sel := `SELECT exposure_header_id, exposure_creation_status FROM public.exposure_headers WHERE exposure_header_id = ANY($1)`
		rows, err := pool.Query(ctx, sel, req.ExposureIDs)
		if err != nil {
			respondEnvelopeError(w, http.StatusInternalServerError, constants.ErrDBPrefix+err.Error(), v91ErrorCode(http.StatusInternalServerError))
			return
		}
		defer rows.Close()

		toApprove := make([]string, 0)
		toDelete := make([]string, 0)
		for rows.Next() {
			var id string
			var status *string
			_ = rows.Scan(&id, &status)
			if status != nil && (strings.EqualFold(*status, constants.StatusCodeDeleteApproval) || strings.EqualFold(*status, constants.StatusPendingDeleteApproval)) {
				toDelete = append(toDelete, id)
			} else {
				toApprove = append(toApprove, id)
			}
		}

		approvedIDs := []string{}
		deletedIDs := []string{}
		if len(toApprove) > 0 {
			// uq := `UPDATE public.exposure_headers SET approval_status='Approved', approval_comment=$1, approved_by=$2, approved_at=now(), updated_at=now() WHERE exposure_header_id = ANY($3) RETURNING exposure_header_id`
			// r2, err := pool.Query(ctx, uq, nullifyEmpty(req.Comment), approver, toApprove)
			uq := `UPDATE public.exposure_headers SET exposure_creation_status='Approved', updated_at=now() WHERE exposure_header_id = ANY($1) RETURNING exposure_header_id`
			r2, err := pool.Query(ctx, uq, toApprove)
			if err != nil {
				respondEnvelopeError(w, http.StatusInternalServerError, constants.ErrDBPrefix+err.Error(), v91ErrorCode(http.StatusInternalServerError))
				return
			}
			defer r2.Close()
			for r2.Next() {
				var id string
				if err := r2.Scan(&id); err == nil {
					approvedIDs = append(approvedIDs, id)
					auditutil.RecordDecisionPGX(ctx, pool, auditutil.DecisionParams{TableName: auditutil.TableExposure, ParentColumn: "exposure_header_id", ParentID: id, Status: constants.StatusApproved, CheckerBy: approver, Comment: req.Comment})
				}
			}
		}

		if len(toDelete) > 0 {
			delQ := `
				UPDATE public.exposure_headers
				SET exposure_creation_status = 'APPROVED',
				    is_deleted = TRUE,
				    deleted_at = now(),
				    deleted_by = $2,
				    updated_at = now()
				WHERE exposure_header_id = ANY($1)
				  AND COALESCE(is_deleted, false) = false
				RETURNING exposure_header_id
			`
			drows, derr := pool.Query(ctx, delQ, toDelete, approver)
			if derr != nil {
				respondEnvelopeError(w, http.StatusInternalServerError, "failed to soft delete headers: "+derr.Error(), v91ErrorCode(http.StatusInternalServerError))
				return
			}
			for drows.Next() {
				var id string
				if err := drows.Scan(&id); err == nil {
					deletedIDs = append(deletedIDs, id)
					auditutil.RecordDecisionPGX(ctx, pool, auditutil.DecisionParams{TableName: auditutil.TableExposure, ParentColumn: "exposure_header_id", ParentID: id, Status: constants.StatusApproved, CheckerBy: approver, Comment: req.Comment})
				}
			}
			drows.Close()
		}

		resp := map[string]interface{}{"approved": approvedIDs, "deleted": deletedIDs}
		respondEnvelopeSuccess(w, "Exposures approved successfully", resp)

		fxnotif.NotifyExposureBulkAction(ctx, pool, fxnotif.BulkActionNotifyInput{
			SourceRoute: v91ExposureActionSourceRoute(r, fxnotif.SourceRouteV91BulkApprove), Action: fxnotif.ActionApprove, UserID: req.UserID, RequestedBy: approver, CheckerComment: req.Comment,
			ExposureIDs: req.ExposureIDs, ResultBuckets: map[string][]string{
				"approved": approvedIDs,
				"deleted":  deletedIDs,
			},
		})
	}
}
func BulkRejectExposures(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID      string   `json:"user_id"`
			ExposureIDs []string `json:"exposure_ids"`
			Comment     string   `json:"comment,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.ExposureIDs) == 0 {
			respondEnvelopeError(w, http.StatusBadRequest, constants.ErrInvalidJSON, v91ErrorCode(http.StatusBadRequest))
			return
		}
		rejector := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				rejector = s.Name
				break
			}
		}
		if rejector == "" {
			respondEnvelopeError(w, http.StatusUnauthorized, constants.ErrInvalidSession, v91ErrorCode(http.StatusUnauthorized))
			return
		}
		for _, id := range req.ExposureIDs {
			creationRow, err := fxexposures.LoadExposureCreationRow(ctx, pool, id)
			if err != nil {
				respondEnvelopeError(w, http.StatusInternalServerError, constants.ErrDBPrefix+err.Error(), v91ErrorCode(http.StatusInternalServerError))
				return
			}
			if ok, msg := policyruntime.EnforceInline(ctx, r, pool, policyruntime.EnforceInput{
				EventCode:           common.TriggerPreReject,
				ModuleCode:          common.ModuleFX,
				SubModule:           v91ExposurePolicySubModule(r),
				EntityCode:          id,
				ActorUserID:         req.UserID,
				HandlerName:         "BulkRejectExposures",
				APIPath:             r.URL.Path,
				DefaultBlockMessage: "Exposure rejection blocked by policy",
				Fields:              fxexposures.BuildExposureCreationPolicyFields(creationRow),
			}); !ok {
				respondEnvelopeError(w, http.StatusUnprocessableEntity, msg, v91ErrorCode(http.StatusUnprocessableEntity))
				return
			}
		}

		// q := `UPDATE public.exposure_headers SET approval_status='Rejected', rejection_comment=$1, rejected_by=$2, rejected_at=now(), updated_at=now() WHERE exposure_header_id = ANY($3) RETURNING exposure_header_id`
		// rows, err := pool.Query(ctx, q, nullifyEmpty(req.Comment), rejector, req.ExposureIDs)
		q := `
			UPDATE public.exposure_headers
			SET exposure_creation_status = 'Rejected',
			    updated_at = now()
			WHERE exposure_header_id = ANY($1)
			RETURNING exposure_header_id
		`
		rows, err := pool.Query(ctx, q, req.ExposureIDs)
		if err != nil {
			respondEnvelopeError(w, http.StatusInternalServerError, constants.ErrDBPrefix+err.Error(), v91ErrorCode(http.StatusInternalServerError))
			return
		}
		defer rows.Close()
		updated := []string{}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err == nil {
				updated = append(updated, id)
				auditutil.RecordDecisionPGX(ctx, pool, auditutil.DecisionParams{TableName: auditutil.TableExposure, ParentColumn: "exposure_header_id", ParentID: id, Status: constants.StatusRejected, CheckerBy: rejector, Comment: req.Comment})
			}
		}
		respondEnvelopeSuccess(w, "Exposures rejected successfully", map[string]interface{}{
			"rejected": updated,
		})

		fxnotif.NotifyExposureBulkAction(ctx, pool, fxnotif.BulkActionNotifyInput{
			SourceRoute: v91ExposureActionSourceRoute(r, fxnotif.SourceRouteV91BulkReject), Action: fxnotif.ActionReject, UserID: req.UserID, RequestedBy: rejector, CheckerComment: req.Comment,
			ExposureIDs: req.ExposureIDs, ResultBuckets: map[string][]string{
				"rejected": updated,
			},
		})
	}
}

func BulkDeleteExposures(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID      string   `json:"user_id"`
			ExposureIDs []string `json:"exposure_ids"`
			Comment     string   `json:"comment,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.ExposureIDs) == 0 {
			respondEnvelopeError(w, http.StatusBadRequest, constants.ErrInvalidJSON, v91ErrorCode(http.StatusBadRequest))
			return
		}
		deleter := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				deleter = s.Name
				break
			}
		}
		if deleter == "" {
			respondEnvelopeError(w, http.StatusUnauthorized, constants.ErrInvalidSession, v91ErrorCode(http.StatusUnauthorized))
			return
		}
		for _, id := range req.ExposureIDs {
			creationRow, err := fxexposures.LoadExposureCreationRow(ctx, pool, id)
			if err != nil {
				respondEnvelopeError(w, http.StatusInternalServerError, constants.ErrDBPrefix+err.Error(), v91ErrorCode(http.StatusInternalServerError))
				return
			}
			if ok, msg := policyruntime.EnforceInline(ctx, r, pool, policyruntime.EnforceInput{
				EventCode:           common.TriggerPreDelete,
				ModuleCode:          common.ModuleFX,
				SubModule:           v91ExposurePolicySubModule(r),
				EntityCode:          id,
				ActorUserID:         req.UserID,
				HandlerName:         "BulkDeleteExposures",
				APIPath:             r.URL.Path,
				DefaultBlockMessage: "Exposure delete blocked by policy",
				Fields:              fxexposures.BuildExposureCreationPolicyFields(creationRow),
			}); !ok {
				respondEnvelopeError(w, http.StatusUnprocessableEntity, msg, v91ErrorCode(http.StatusUnprocessableEntity))
				return
			}
		}

		// q := `UPDATE public.exposure_headers SET approval_status='Delete-approval',delete_comment=$1, is_active = FALSE, updated_at=now() WHERE exposure_header_id = ANY($2) RETURNING exposure_header_id`
		// rows, err := pool.Query(ctx, q, nullifyEmpty(req.Comment), req.ExposureIDs)
		q := `
			UPDATE public.exposure_headers
			SET exposure_creation_status='PENDING_DELETE_APPROVAL',
			    updated_at=now()
			WHERE exposure_header_id = ANY($1)
			  AND COALESCE(is_deleted, false) = false
			RETURNING exposure_header_id
		`
		rows, err := pool.Query(ctx, q, req.ExposureIDs)
		if err != nil {
			respondEnvelopeError(w, http.StatusInternalServerError, constants.ErrDBPrefix+err.Error(), v91ErrorCode(http.StatusInternalServerError))
			return
		}
		defer rows.Close()
		deleted := []string{}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err == nil {
				deleted = append(deleted, id)
				auditutil.RecordActionPGX(ctx, pool, auditutil.ActionParams{TableName: auditutil.TableExposure, ParentColumn: "exposure_header_id", ParentID: id, ActionType: "DELETE", Status: constants.StatusPendingDeleteApproval, Reason: req.Comment, RequestedBy: deleter, OldValues: nil, NewValues: nil})
			}
		}
		respondEnvelopeSuccess(w, "Exposures marked for delete approval", map[string]interface{}{
			"deleted": deleted,
		})

		fxnotif.NotifyExposureBulkAction(ctx, pool, fxnotif.BulkActionNotifyInput{
			SourceRoute: v91ExposureActionSourceRoute(r, fxnotif.SourceRouteV91BulkDelete), Action: fxnotif.ActionDelete, UserID: req.UserID, RequestedBy: deleter, CheckerComment: req.Comment,
			ExposureIDs: req.ExposureIDs, ResultBuckets: map[string][]string{
				"deleted": deleted,
			},
		})
	}
}

func v91ExposurePolicySubModule(r *http.Request) string {
	// /fx/exposures/v91/upload/bulk-* → Exposure Upload (staging checker)
	// /fx/exposures/v91/upload and /fx/exposures/v91/bulk-* → Exposure Creation (v91 SAP)
	if r != nil && strings.HasPrefix(r.URL.Path, "/fx/exposures/v91/upload/") {
		return "EXPOSURE_UPLOAD"
	}
	return "EXPOSURE_CREATION"
}

func v91ExposureActionSourceRoute(r *http.Request, fallback string) string {
	if r != nil && strings.HasPrefix(r.URL.Path, "/fx/exposures/v91/upload/") {
		return r.URL.Path
	}
	return fallback
}

// func nullifyEmpty(s string) interface{} {
// 	if s == "" {
// 		return nil
// 	}
// 	return s
// }
