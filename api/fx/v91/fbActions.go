package exposures

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	api "CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"

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
			api.RespondWithError(w, http.StatusBadRequest, "invalid json: "+err.Error())
			return
		}
		if req.UserID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "user_id required")
			return
		}
		if len(req.Rows) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "empty payload")
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
			api.RespondWithError(w, http.StatusUnauthorized, "invalid user/session")
			return
		}
		updated := make([]string, 0, len(req.Rows))

		for i, p := range req.Rows {
			if p.ExposureHeaderID == "" {
				api.RespondWithError(w, http.StatusBadRequest, fmt.Sprintf("missing exposure_header_id at index %d", i))
				return
			}
			if p.NewValueDate == "" {
				api.RespondWithError(w, http.StatusBadRequest, fmt.Sprintf("missing new_value_date at index %d", i))
				return
			}
			dt, err := parseFlexibleDate(p.NewValueDate)
			if err != nil {
				api.RespondWithError(w, http.StatusBadRequest, fmt.Sprintf("invalid date at index %d: %v", i, err))
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

			row := pool.QueryRow(ctx, q, dt, requester, p.ExposureHeaderID)

			var id string
			if err := row.Scan(&id); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, fmt.Sprintf("db update error at index %d: %v", i, err))
				return
			}

			updated = append(updated, id)
		}
		api.RespondWithPayload(w, true, "value_date updated successfully", updated)
	}
}

func parseFlexibleDate(dateStr string) (time.Time, error) {
	dateStr = strings.TrimSpace(dateStr)
	if dateStr == "" {
		return time.Time{}, fmt.Errorf("empty date")
	}
	dateStr = regexp.MustCompile(`\s+`).ReplaceAllString(dateStr, " ")
	layouts := []string{
		"2006-01-02",
		"2006/01/02",
		"2006.01.02",
		time.RFC3339,
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
		"2006-01-02T15:04:05Z",
		"2006-01-02T15:04:05.000Z",
		"02-01-2006",
		"02/01/2006",
		"02.01.2006",
		"01-02-2006",
		"01/02/2006",
		"01.02.2006",
		"02-Jan-2006",
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
			api.RespondWithError(w, http.StatusBadRequest, "invalid json or missing fields")
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
			api.RespondWithError(w, http.StatusUnauthorized, "invalid user/session")
			return
		}

		sel := `SELECT exposure_header_id, exposure_creation_status FROM public.exposure_headers WHERE exposure_header_id = ANY($1)`
		rows, err := pool.Query(ctx, sel, req.ExposureIDs)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "db error: "+err.Error())
			return
		}
		defer rows.Close()

		toApprove := make([]string, 0)
		toDelete := make([]string, 0)
		for rows.Next() {
			var id string
			var status *string
			_ = rows.Scan(&id, &status)
			if status != nil && *status == "Delete-Approval" {
				toDelete = append(toDelete, id)
			} else {
				toApprove = append(toApprove, id)
			}
		}

		approvedIDs := []string{}
		deletedIDs := []string{}
		warnings := []string{}

		if len(toApprove) > 0 {
			// uq := `UPDATE public.exposure_headers SET approval_status='Approved', approval_comment=$1, approved_by=$2, approved_at=now(), updated_at=now() WHERE exposure_header_id = ANY($3) RETURNING exposure_header_id`
			// r2, err := pool.Query(ctx, uq, nullifyEmpty(req.Comment), approver, toApprove)
			uq := `UPDATE public.exposure_headers SET exposure_creation_status='Approved', updated_at=now() WHERE exposure_header_id = ANY($1) RETURNING exposure_header_id`
			r2, err := pool.Query(ctx, uq, toApprove)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "db error: "+err.Error())
				return
			}
			defer r2.Close()
			for r2.Next() {
				var id string
				if err := r2.Scan(&id); err == nil {
					approvedIDs = append(approvedIDs, id)
				}
			}
		}

		if len(toDelete) > 0 {
			tx, err := pool.Begin(ctx)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "failed to begin tx: "+err.Error())
				return
			}
			rolledBack := true
			defer func() {
				if rolledBack {
					tx.Rollback(ctx)
				}
			}()

			depDeletes := []string{
				"DELETE FROM public.exposure_line_items WHERE exposure_header_id = ANY($1)",
				"DELETE FROM public.exposure_bucketing WHERE exposure_header_id = ANY($1)",
				// "DELETE FROM public.exposure_hedge_links WHERE exposure_header_id = ANY($1)",
				"DELETE FROM public.exposure_rollover_log WHERE exposure_header_id = ANY($1)",
				"DELETE FROM public.hedging_proposal WHERE exposure_header_id = ANY($1)",
			}
			for _, dq := range depDeletes {
				if _, err := tx.Exec(ctx, dq, toDelete); err != nil {
					warnings = append(warnings, fmt.Sprintf("failed to delete dependent rows: %v", err))
				}
			}

			linked := map[string][]string{}

			hq := `SELECT DISTINCT exposure_header_id FROM public.hedge_linkage_adjustments WHERE exposure_header_id = ANY($1)`
			hrows, herr := tx.Query(ctx, hq, toDelete)
			if herr != nil {
				tx.Rollback(ctx)
				api.RespondWithPayload(w, false, "db error while checking hedge_linkage_adjustments: "+herr.Error(), nil)
				return
			}
			defer hrows.Close()
			for hrows.Next() {
				var id *string
				if err := hrows.Scan(&id); err == nil && id != nil {
					linked["hedge_linkage_adjustments"] = append(linked["hedge_linkage_adjustments"], *id)
				}
			}

			sq := `SELECT DISTINCT exposure_header_id FROM public.settlements WHERE exposure_header_id = ANY($1)`
			srows, serr := tx.Query(ctx, sq, toDelete)
			if serr != nil {
				tx.Rollback(ctx)
				api.RespondWithPayload(w, false, "db error while checking settlements: "+serr.Error(), nil)
				return
			}
			defer srows.Close()
			for srows.Next() {
				var id *string
				if err := srows.Scan(&id); err == nil && id != nil {
					linked["settlements"] = append(linked["settlements"], *id)
				}
			}

			eq := `SELECT DISTINCT exposure_header_id FROM public.exposure_hedge_links WHERE exposure_header_id = ANY($1)`
			erows, eerr := tx.Query(ctx, eq, toDelete)
			if eerr != nil {
				tx.Rollback(ctx)
				api.RespondWithPayload(w, false, "db error while checking exposure_hedge_links: "+eerr.Error(), nil)
				return
			}
			defer erows.Close()
			for erows.Next() {
				var id *string
				if err := erows.Scan(&id); err == nil && id != nil {
					linked["exposure_hedge_links"] = append(linked["exposure_hedge_links"], *id)
				}
			}

			if len(linked) > 0 {
				tx.Rollback(ctx)
				api.RespondWithPayload(w, false, "cannot delete exposure_headers: linked rows exist in dependent tables", nil)
				return
			}
			delQ := "DELETE FROM public.exposure_headers WHERE exposure_header_id = ANY($1) RETURNING exposure_header_id"
			drows, derr := tx.Query(ctx, delQ, toDelete)
			if derr != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "failed to delete headers: "+derr.Error())
				tx.Rollback(ctx)
				return
			}
			for drows.Next() {
				var id string
				if err := drows.Scan(&id); err == nil {
					deletedIDs = append(deletedIDs, id)
				}
			}
			drows.Close()

			if err := tx.Commit(ctx); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "failed to commit delete tx: "+err.Error())
				return
			}
			rolledBack = false
		}

		resp := map[string]interface{}{"approved": approvedIDs, "deleted": deletedIDs}
		api.RespondWithPayload(w, true, "", resp)
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
			api.RespondWithError(w, http.StatusBadRequest, "invalid json or missing fields")
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
			api.RespondWithError(w, http.StatusUnauthorized, "invalid user/session")
			return
		}

		// q := `UPDATE public.exposure_headers SET approval_status='Rejected', rejection_comment=$1, rejected_by=$2, rejected_at=now(), updated_at=now() WHERE exposure_header_id = ANY($3) RETURNING exposure_header_id`
		// rows, err := pool.Query(ctx, q, nullifyEmpty(req.Comment), rejector, req.ExposureIDs)
		q := `UPDATE public.exposure_headers SET exposure_creation_status='Rejected', updated_at=now() WHERE exposure_header_id = ANY($1) RETURNING exposure_header_id`
	rows, err := pool.Query(ctx, q, req.ExposureIDs)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "db error: "+err.Error())
			return
		}
		defer rows.Close()
		updated := []string{}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err == nil {
				updated = append(updated, id)
			}
		}
		api.RespondWithPayload(w, true, "", updated)
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
			api.RespondWithError(w, http.StatusBadRequest, "invalid json or missing fields")
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
			api.RespondWithError(w, http.StatusUnauthorized, "invalid user/session")
			return
		}

		// q := `UPDATE public.exposure_headers SET approval_status='Delete-approval',delete_comment=$1, is_active = FALSE, updated_at=now() WHERE exposure_header_id = ANY($2) RETURNING exposure_header_id`
		// rows, err := pool.Query(ctx, q, nullifyEmpty(req.Comment), req.ExposureIDs)
		q := `UPDATE public.exposure_headers SET exposure_creation_status='Delete-Approval', updated_at=now() WHERE exposure_header_id = ANY($1) RETURNING exposure_header_id`
	rows, err := pool.Query(ctx, q, req.ExposureIDs)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "db error: "+err.Error())
			return
		}
		defer rows.Close()
		deleted := []string{}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err == nil {
				deleted = append(deleted, id)
			}
		}
		api.RespondWithPayload(w, true, "", deleted)
	}
}

// func nullifyEmpty(s string) interface{} {
// 	if s == "" {
// 		return nil
// 	}
// 	return s
// }
