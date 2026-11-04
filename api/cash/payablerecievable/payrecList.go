package payablerecievable

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/common"
	"CimplrCorpSaas/api/constants"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type payRecListKind string

const (
	listPayables    payRecListKind = "payable"
	listReceivables payRecListKind = "receivable"
)

// ListPayables — POST /cash/transactions/payables/list
func ListPayables(pool *pgxpool.Pool) http.HandlerFunc {
	return listPayRec(pool, listPayables)
}

// ListReceivables — POST /cash/transactions/receivables/list
func ListReceivables(pool *pgxpool.Pool) http.HandlerFunc {
	return listPayRec(pool, listReceivables)
}

func listPayRec(pool *pgxpool.Pool, kind payRecListKind) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req common.ListRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		limit := common.NormalizeLimit(req.Limit)
		dir := common.NormalizeSortDir(req.SortDir)

		sortMap := map[string]string{
			"invoice_number":    "invoice_number",
			"invoice_date":      "COALESCE(invoice_date::text,'')",
			"due_date":          "COALESCE(due_date::text,'')",
			"amount":            "COALESCE(amount_col::text,'')",
			"entity_name":       "entity_name",
			"counterparty_name": "counterparty_name",
			"currency_code":     "currency_code",
			"status":            "status",
			"id":                "id_col",
		}
		sortExpr := common.WhitelistSort(req.SortBy, sortMap, "id")
		idCol := "id_col"

		tableSQL, amountExpr, idExpr := payRecTableSQL(kind)
		baseFrom := fmt.Sprintf(`
			FROM %s t
			LEFT JOIN LATERAL (
				SELECT a.processing_status AS status
				FROM %s a
				WHERE a.%s = t.%s
				  AND a.actiontype IN ('CREATE','EDIT','DELETE')
				ORDER BY a.requested_at DESC NULLS LAST, a.action_id DESC
				LIMIT 1
			) la ON TRUE
		`, tableSQL, payRecAuditTable(kind), payRecAuditIDCol(kind), idExpr)

		selectInner := fmt.Sprintf(`
			SELECT
				t.%s::text AS id_col,
				COALESCE(t.entity_name, '') AS entity_name,
				COALESCE(t.counterparty_name, '') AS counterparty_name,
				COALESCE(t.invoice_number, '') AS invoice_number,
				t.invoice_date,
				t.due_date,
				%s AS amount_col,
				COALESCE(t.currency_code, '') AS currency_code,
				COALESCE(t.upload_s3_key, '') AS upload_s3_key,
				COALESCE(t.old_entity_name, '') AS old_entity_name,
				COALESCE(t.old_counterparty_name, '') AS old_counterparty_name,
				COALESCE(t.old_invoice_number, '') AS old_invoice_number,
				t.old_invoice_date,
				t.old_due_date,
				%s AS old_amount_col,
				COALESCE(t.old_currency_code, '') AS old_currency_code,
				COALESCE(la.status, '') AS status
			%s
			WHERE COALESCE(t.is_deleted, false) = false
		`, idExpr, amountExpr, payRecOldAmountExpr(kind), baseFrom)

		args := []interface{}{}
		argN := 1
		whereExtra := ""

		search := strings.TrimSpace(req.Search)
		if search != "" {
			like := "%" + search + "%"
			col := strings.TrimSpace(req.SearchColumn)
			switch col {
			case "invoice_number":
				whereExtra += fmt.Sprintf(" AND t.invoice_number ILIKE $%d", argN)
			case "entity_name":
				whereExtra += fmt.Sprintf(" AND t.entity_name ILIKE $%d", argN)
			case "counterparty_name":
				whereExtra += fmt.Sprintf(" AND t.counterparty_name ILIKE $%d", argN)
			case "currency_code":
				whereExtra += fmt.Sprintf(" AND t.currency_code ILIKE $%d", argN)
			case "status":
				whereExtra += fmt.Sprintf(" AND COALESCE(la.status,'') ILIKE $%d", argN)
			default:
				whereExtra += fmt.Sprintf(` AND (
					t.invoice_number ILIKE $%d OR t.entity_name ILIKE $%d OR t.counterparty_name ILIKE $%d
					OR t.currency_code ILIKE $%d OR COALESCE(la.status,'') ILIKE $%d
				)`, argN, argN, argN, argN, argN)
			}
			args = append(args, like)
			argN++
		}
		if st := strings.TrimSpace(req.Status); st != "" && !strings.EqualFold(st, "All") {
			whereExtra += fmt.Sprintf(" AND COALESCE(la.status,'') = $%d", argN)
			args = append(args, st)
			argN++
		}

		var total int64
		countQ := fmt.Sprintf(`SELECT COUNT(*) %s WHERE COALESCE(t.is_deleted, false) = false %s`, baseFrom, whereExtra)
		_ = pool.QueryRow(ctx, countQ, args...).Scan(&total)

		wrapped := fmt.Sprintf(`SELECT * FROM (%s %s) q`, selectInner, whereExtra)
		wherePage := ""
		if cur, ok := common.DecodeCursor(req.Cursor); ok {
			op := "<"
			if dir == "ASC" {
				op = ">"
			}
			wherePage = fmt.Sprintf(" WHERE (%s, %s) %s ($%d, $%d)", sortExpr, idCol, op, argN, argN+1)
			args = append(args, cur.V, cur.ID)
			argN += 2
		}

		orderSQL := fmt.Sprintf(" ORDER BY %s %s NULLS LAST, %s %s", sortExpr, dir, idCol, dir)
		limitSQL := fmt.Sprintf(" LIMIT $%d", argN)
		args = append(args, limit+1)

		q := wrapped + wherePage + orderSQL + limitSQL
		rows, err := pool.Query(ctx, q, args...)
		if err != nil {
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "Failed to list transactions", "")
			return
		}
		defer rows.Close()

		out := make([]map[string]interface{}, 0, limit)
		ids := make([]string, 0, limit)
		for rows.Next() {
			var (
				id, entity, cp, inv, curCode, s3, oldEnt, oldCp, oldInv, oldCur, status string
				invDate, dueDate, oldInvDate, oldDueDate                               *time.Time
				amount, oldAmount                                                      *float64
			)
			if err := rows.Scan(
				&id, &entity, &cp, &inv, &invDate, &dueDate, &amount, &curCode, &s3,
				&oldEnt, &oldCp, &oldInv, &oldInvDate, &oldDueDate, &oldAmount, &oldCur, &status,
			); err != nil {
				api.RespondEnvelopeError(w, http.StatusInternalServerError, "Failed to scan transactions", "")
				return
			}
			if msg := validatePayRecScope(ctx, entity, cp, curCode); msg != "" {
				continue
			}
			rec := map[string]interface{}{
				"entity_name": entity, "counterparty_name": cp,
				"invoice_number": inv, "currency_code": curCode,
				"upload_s3_key": s3, "status": status,
				"old_entity_name": oldEnt, "old_counterparty_name": oldCp,
				"old_invoice_number": oldInv, "old_currency_code": oldCur,
			}
			if kind == listPayables {
				rec["payable_id"] = id
				rec["amount"] = floatOrZero(amount)
				rec["old_amount"] = floatOrZero(oldAmount)
			} else {
				rec["receivable_id"] = id
				rec["invoice_amount"] = floatOrZero(amount)
				rec["old_invoice_amount"] = floatOrZero(oldAmount)
			}
			rec["invoice_date"] = formatDate(invDate)
			rec["due_date"] = formatDate(dueDate)
			rec["old_invoice_date"] = formatDate(oldInvDate)
			rec["old_due_date"] = formatDate(oldDueDate)
			out = append(out, rec)
			ids = append(ids, id)
		}
		if err := rows.Err(); err != nil {
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "Failed to list transactions", "")
			return
		}

		enrichPayRecAuditFields(ctx, pool, kind, ids, out)

		hasMore := len(out) > limit
		if hasMore {
			out = out[:limit]
			ids = ids[:limit]
		}
		nextCursor := ""
		if hasMore && len(out) > 0 {
			last := out[len(out)-1]
			sortVal := cursorSortValue(last, req.SortBy, kind)
			nextCursor = common.EncodeCursor(sortVal, ids[len(ids)-1])
		}

		api.RespondEnvelopeSuccess(w, "Success", common.ListPage{
			Rows:       out,
			NextCursor: nextCursor,
			HasMore:    hasMore,
			Limit:      limit,
			TotalCount: &total,
		})
	}
}

func payRecTableSQL(kind payRecListKind) (table, amountExpr, idExpr string) {
	if kind == listPayables {
		return "tr_payables", "t.amount", "payable_id"
	}
	return "tr_receivables", "t.invoice_amount", "receivable_id"
}

func payRecOldAmountExpr(kind payRecListKind) string {
	if kind == listPayables {
		return "t.old_amount"
	}
	return "t.old_invoice_amount"
}

func payRecAuditTable(kind payRecListKind) string {
	if kind == listPayables {
		return "auditactionpayable"
	}
	return "auditactionreceivable"
}

func payRecAuditIDCol(kind payRecListKind) string {
	if kind == listPayables {
		return "payable_id"
	}
	return "receivable_id"
}

func floatOrZero(v *float64) float64 {
	if v == nil {
		return 0
	}
	return *v
}

func formatDate(t *time.Time) string {
	if t == nil {
		return ""
	}
	return t.Format(constants.DateFormat)
}

func cursorSortValue(row map[string]interface{}, sortBy string, kind payRecListKind) string {
	key := strings.TrimSpace(sortBy)
	switch key {
	case "invoice_number", "entity_name", "counterparty_name", "currency_code", "status",
		"invoice_date", "due_date":
		return fmt.Sprint(row[key])
	case "amount":
		if kind == listPayables {
			return fmt.Sprint(row["amount"])
		}
		return fmt.Sprint(row["invoice_amount"])
	default:
		if kind == listPayables {
			return fmt.Sprint(row["payable_id"])
		}
		return fmt.Sprint(row["receivable_id"])
	}
}

func enrichPayRecAuditFields(ctx context.Context, pool *pgxpool.Pool, kind payRecListKind, ids []string, rows []map[string]interface{}) {
	if len(ids) == 0 {
		return
	}
	auditTable := payRecAuditTable(kind)
	idCol := payRecAuditIDCol(kind)
	q := fmt.Sprintf(`
		SELECT %s::text, actiontype, requested_by, requested_at, processing_status
		FROM %s
		WHERE %s = ANY($1) AND actiontype IN ('CREATE','EDIT','DELETE')
		ORDER BY %s, requested_at DESC, action_id DESC`, idCol, auditTable, idCol, idCol)
	arows, err := pool.Query(ctx, q, ids)
	if err != nil {
		return
	}
	defer arows.Close()

	type auditBits struct {
		createdBy, createdAt, editedBy, editedAt, deletedBy, deletedAt, status string
	}
	m := map[string]*auditBits{}
	for arows.Next() {
		var id, atype, by, status string
		var at *time.Time
		_ = arows.Scan(&id, &atype, &by, &at, &status)
		b, ok := m[id]
		if !ok {
			b = &auditBits{status: status}
			m[id] = b
		}
		ts := ""
		if at != nil {
			ts = api.FormatAuditTimestampIST(*at)
		}
		switch atype {
		case "CREATE":
			if b.createdBy == "" {
				b.createdBy, b.createdAt = by, ts
			}
		case constants.AuditActionEdit:
			if b.editedBy == "" {
				b.editedBy, b.editedAt = by, ts
			}
		case constants.AuditActionDelete:
			if b.deletedBy == "" {
				b.deletedBy, b.deletedAt = by, ts
			}
		}
	}
	for i := range rows {
		idKey := "payable_id"
		if kind == listReceivables {
			idKey = "receivable_id"
		}
		id, _ := rows[i][idKey].(string)
		if b := m[id]; b != nil {
			rows[i]["created_by"] = b.createdBy
			rows[i]["created_at"] = b.createdAt
			rows[i]["edited_by"] = b.editedBy
			rows[i]["edited_at"] = b.editedAt
			rows[i]["deleted_by"] = b.deletedBy
			rows[i]["deleted_at"] = b.deletedAt
			if rows[i]["status"] == "" || rows[i]["status"] == nil {
				rows[i]["status"] = b.status
			}
		}
	}
}
