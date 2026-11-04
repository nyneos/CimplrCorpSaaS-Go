package exposures

import (
	"CimplrCorpSaas/api/constants"
	listcommon "CimplrCorpSaas/api/common"
	"CimplrCorpSaas/internal/ctxutil"
	"CimplrCorpSaas/internal/logger"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ListExposureHeadersLineItems — POST /fx/exposures/headers-line-items/list
func ListExposureHeadersLineItems(pool *pgxpool.Pool) http.HandlerFunc {
	return listExposureHeaders(pool, false)
}

// ListPendingExposureHeadersLineItems — POST /fx/exposures/pending-headers-line-items/list
func ListPendingExposureHeadersLineItems(pool *pgxpool.Pool) http.HandlerFunc {
	return listExposureHeaders(pool, true)
}

func listExposureHeaders(pool *pgxpool.Pool, pendingOnly bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			listcommon.ListRequest
			BatchID string `json:"batch_id,omitempty"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		ctx := r.Context()
		scope := ctxutil.FromContext(ctx)
		buNames := scope.EntityNames
		if len(buNames) == 0 {
			respondWithError(w, http.StatusNotFound, constants.ErrNoAccessibleBusinessUnit)
			return
		}

		limit := listcommon.NormalizeLimit(req.Limit)
		dir := listcommon.NormalizeSortDir(req.SortDir)
		sortMap := map[string]string{
			"created_at":            "COALESCE(h.created_at::text,'')",
			"document_id":           "COALESCE(h.document_id,'')",
			"entity":                "COALESCE(h.entity,'')",
			"counterparty_name":     "COALESCE(h.counterparty_name,'')",
			"currency":              "COALESCE(h.currency,'')",
			"total_open_amount":     "COALESCE(h.total_open_amount::text,'')",
			"approval_status":       "COALESCE(h.approval_status,'')",
			"exposure_type":         "COALESCE(h.exposure_type,'')",
			"id":                    "h.exposure_header_id::text",
		}
		sortExpr := listcommon.WhitelistSort(req.SortBy, sortMap, "created_at")

		query := headersListSelectSQL + `
			WHERE h.entity = ANY($1)
			  AND h.exposure_creation_status = 'Approved'
			  AND h.is_deleted IS NOT TRUE
		`
		args := []interface{}{buNames}
		argN := 2
		if strings.TrimSpace(req.BatchID) != "" {
			query += fmt.Sprintf(` AND h.batch_id::text = $%d`, argN)
			args = append(args, strings.TrimSpace(req.BatchID))
			argN++
		}
		if pendingOnly {
			query += ` AND upper(COALESCE(i.status, h.approval_status, '')) <> 'APPROVED'`
		}
		if search := strings.TrimSpace(req.Search); search != "" {
			like := "%" + search + "%"
			col := strings.TrimSpace(req.SearchColumn)
			switch col {
			case "document_id":
				query += fmt.Sprintf(` AND h.document_id ILIKE $%d`, argN)
			case "entity":
				query += fmt.Sprintf(` AND h.entity ILIKE $%d`, argN)
			case "counterparty_name":
				query += fmt.Sprintf(` AND h.counterparty_name ILIKE $%d`, argN)
			case "currency":
				query += fmt.Sprintf(` AND h.currency ILIKE $%d`, argN)
			case "approval_status":
				query += fmt.Sprintf(` AND COALESCE(h.approval_status,'') ILIKE $%d`, argN)
			default:
				query += fmt.Sprintf(` AND (
					COALESCE(h.document_id,'') ILIKE $%d OR COALESCE(h.entity,'') ILIKE $%d
					OR COALESCE(h.counterparty_name,'') ILIKE $%d OR COALESCE(h.currency,'') ILIKE $%d
					OR COALESCE(h.approval_status,'') ILIKE $%d OR COALESCE(h.exposure_type,'') ILIKE $%d
				)`, argN, argN, argN, argN, argN, argN)
			}
			args = append(args, like)
			argN++
		}
		if st := strings.TrimSpace(req.Status); st != "" && !strings.EqualFold(st, "All") {
			query += fmt.Sprintf(` AND COALESCE(h.approval_status,'') = $%d`, argN)
			args = append(args, st)
			argN++
		}

		countQ := "SELECT COUNT(*) FROM (" + query + ") c"
		var total int64
		_ = pool.QueryRow(ctx, countQ, args...).Scan(&total)

		if cur, ok := listcommon.DecodeCursor(req.Cursor); ok {
			op := "<"
			if dir == "ASC" {
				op = ">"
			}
			query += fmt.Sprintf(` AND (%s, h.exposure_header_id::text) %s ($%d, $%d)`, sortExpr, op, argN, argN+1)
			args = append(args, cur.V, cur.ID)
			argN += 2
		}

		query += fmt.Sprintf(` ORDER BY %s %s NULLS LAST, h.exposure_header_id %s LIMIT $%d`, sortExpr, dir, dir, argN)
		args = append(args, limit+1)

		rows, err := pool.Query(ctx, query, args...)
		if err != nil {
			logger.LogError("[FX] ListExposureHeaders failed: %v", err)
			respondWithError(w, http.StatusInternalServerError, "Failed to fetch exposure headers/line items")
			return
		}
		defer rows.Close()
		joinData, err := scanHeadersLineItemsRows(rows)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "Failed to scan exposure headers")
			return
		}

		hasMore := len(joinData) > limit
		if hasMore {
			joinData = joinData[:limit]
		}
		next := ""
		if hasMore && len(joinData) > 0 {
			last := joinData[len(joinData)-1]
			id := fmt.Sprint(last["exposure_header_id"])
			next = listcommon.EncodeCursor(expCursorVal(last, req.SortBy), id)
		}

		respondWithSuccess(w, http.StatusOK, "Exposure headers fetched successfully", map[string]interface{}{
			"buAccessible": buNames,
			"pageData":     joinData,
			"next_cursor":  next,
			"has_more":     hasMore,
			"limit":        limit,
			"total_count":  total,
			"rows":         joinData,
		})
	}
}

func expCursorVal(row map[string]interface{}, sortBy string) string {
	switch strings.TrimSpace(sortBy) {
	case "document_id", "entity", "counterparty_name", "currency", "approval_status", "exposure_type":
		return fmt.Sprint(row[sortBy])
	case "total_open_amount":
		return fmt.Sprint(row["total_open_amount"])
	case "id":
		return fmt.Sprint(row["exposure_header_id"])
	default:
		return fmt.Sprint(row["created_at"])
	}
}
