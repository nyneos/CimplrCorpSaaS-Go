package bankstatement

import (
	"CimplrCorpSaas/api/common"
	"CimplrCorpSaas/api/constants"
	apictx "CimplrCorpSaas/api"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"CimplrCorpSaas/internal/ctxutil"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ListBankStatementsHandler — POST /cash/bank-statements/v2/list
// Cursor page over grouped statements (same shape as v2/get rows).
func ListBankStatementsHandler(pool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		var req common.ListRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}
		ctx := r.Context()
		scope := ctxutil.FromContext(ctx)
		entityIDs := scope.EntityIDs
		if len(entityIDs) == 0 {
			http.Error(w, constants.ErrNoAccessibleBusinessUnit, http.StatusUnauthorized)
			return
		}
		accountNumbers := scopeValues(scope.BankAccounts, "account_number")
		bankIDs := scopeValues(scope.Banks, "bank_id")
		currencyCodes := scopeValues(scope.Currencies, "currency_code")

		limit := common.NormalizeLimit(req.Limit)
		dir := common.NormalizeSortDir(req.SortDir)
		sortMap := map[string]string{
			"uploaded_at":        "COALESCE(sort_ts::text,'')",
			"entity_name":        "entity_name",
			"account_number":     "account_number",
			"bank_name":          "bank_name",
			"processing_status":  "COALESCE(processing_status,'')",
			"statement_period_start": "COALESCE(statement_period_start::text,'')",
			"id":                 "bank_statement_id",
		}
		sortExpr := common.WhitelistSort(req.SortBy, sortMap, "uploaded_at")

		baseCTE := `
			WITH scoped_statements AS (
				SELECT s.bank_statement_id, s.entity_id, e.entity_name, s.account_number, s.statement_period_start, s.statement_period_end, s.opening_balance, s.closing_balance, s.uploaded_at,
					COALESCE(mb.bank_name, '') AS bank_name,
					mba.account_nickname AS account_nickname,
					s.upload_s3_key
				FROM cimplrcorpsaas.bank_statements s
				JOIN public.masterentitycash e ON s.entity_id = e.entity_id
				LEFT JOIN public.masterbankaccount mba ON mba.account_number = s.account_number AND mba.entity_id::text = s.entity_id::text AND mba.is_deleted = false
				LEFT JOIN public.masterbank mb ON mb.bank_id = mba.bank_id
				WHERE s.entity_id = ANY($1)
				  AND (cardinality($2::text[]) = 0 OR s.account_number = ANY($2::text[]))
				  AND (cardinality($3::text[]) = 0 OR mba.bank_id = ANY($3::text[]))
				  AND (cardinality($4::text[]) = 0 OR mba.currency = ANY($4::text[]))
				  AND COALESCE(s.is_deleted, false) = false
			),
			ordered_audit AS (
				SELECT a.*,
					GREATEST(a.requested_at, COALESCE(a.checker_at, a.requested_at)) AS event_at,
					ROW_NUMBER() OVER (
						PARTITION BY a.bankstatementid
						ORDER BY GREATEST(a.requested_at, COALESCE(a.checker_at, a.requested_at)) DESC, a.action_id DESC
					) AS head_rn
				FROM cimplrcorpsaas.auditactionbankstatement a
				JOIN scoped_statements ss ON a.bankstatementid = ss.bank_statement_id
				WHERE COALESCE(a.actiontype, '') NOT IN ('UPLOAD_FILE', 'DOWNLOAD', 'DMS_TRIGGER')
			),
			head_audit AS (SELECT * FROM ordered_audit WHERE head_rn = 1),
			display_ranked AS (
				SELECT a.*,
					ROW_NUMBER() OVER (
						PARTITION BY a.bankstatementid
						ORDER BY
							CASE
								WHEN a.actiontype = 'DELETE' AND a.processing_status = 'PENDING_DELETE_APPROVAL' THEN 0
								WHEN a.actiontype = 'CREATE' AND a.processing_status = 'PENDING_APPROVAL' THEN 1
								ELSE 2
							END,
							a.event_at DESC, a.action_id DESC
					) AS disp_rn
				FROM ordered_audit a
				WHERE NOT (a.actiontype = 'RECAT' OR (a.actiontype = 'EDIT' AND a.processing_status = 'PENDING_EDIT_APPROVAL'))
			),
			display_audit AS (SELECT * FROM display_ranked WHERE disp_rn = 1),
			page_base AS (
				SELECT ss.bank_statement_id, ss.entity_id, ss.entity_name, ss.account_number, ss.statement_period_start, ss.statement_period_end,
					ss.opening_balance, ss.closing_balance, ss.uploaded_at,
					ha.actiontype,
					da.processing_status,
					da.action_id, da.requested_by, da.requested_at, da.checker_by, da.checker_at, da.checker_comment, da.reason,
					ss.bank_name, ss.account_nickname, ss.upload_s3_key,
					(ha.actiontype = 'RECAT' AND ha.processing_status = 'PENDING_EDIT_APPROVAL') AS has_pending_recat,
					GREATEST(COALESCE(ha.requested_at, ss.uploaded_at), COALESCE(ha.checker_at, ss.uploaded_at)) AS sort_ts
				FROM scoped_statements ss
				LEFT JOIN head_audit ha ON ha.bankstatementid = ss.bank_statement_id
				LEFT JOIN display_audit da ON da.bankstatementid = ss.bank_statement_id
			)
		`
		args := []interface{}{entityIDs, accountNumbers, bankIDs, currencyCodes}
		argN := 5
		whereExtra := ""
		if search := strings.TrimSpace(req.Search); search != "" {
			like := "%" + search + "%"
			col := strings.TrimSpace(req.SearchColumn)
			switch col {
			case "entity_name":
				whereExtra += fmt.Sprintf(" AND entity_name ILIKE $%d", argN)
			case "account_number":
				whereExtra += fmt.Sprintf(" AND account_number ILIKE $%d", argN)
			case "bank_name":
				whereExtra += fmt.Sprintf(" AND bank_name ILIKE $%d", argN)
			case "processing_status":
				whereExtra += fmt.Sprintf(" AND COALESCE(processing_status,'') ILIKE $%d", argN)
			default:
				whereExtra += fmt.Sprintf(` AND (
					entity_name ILIKE $%d OR account_number ILIKE $%d OR bank_name ILIKE $%d
					OR COALESCE(processing_status,'') ILIKE $%d OR COALESCE(account_nickname,'') ILIKE $%d
				)`, argN, argN, argN, argN, argN)
			}
			args = append(args, like)
			argN++
		}
		if st := strings.TrimSpace(req.Status); st != "" && !strings.EqualFold(st, "All") {
			whereExtra += fmt.Sprintf(" AND COALESCE(processing_status,'') = $%d", argN)
			args = append(args, st)
			argN++
		}

		var total int64
		_ = pool.QueryRow(ctx, baseCTE+` SELECT COUNT(*) FROM page_base WHERE TRUE `+whereExtra, args...).Scan(&total)

		if cur, ok := common.DecodeCursor(req.Cursor); ok {
			op := "<"
			if dir == "ASC" {
				op = ">"
			}
			whereExtra += fmt.Sprintf(" AND (%s, bank_statement_id) %s ($%d, $%d)", sortExpr, op, argN, argN+1)
			args = append(args, cur.V, cur.ID)
			argN += 2
		}

		q := baseCTE + fmt.Sprintf(`
			SELECT bank_statement_id, entity_id, entity_name, account_number, statement_period_start, statement_period_end,
				opening_balance, closing_balance, uploaded_at, actiontype, processing_status, action_id,
				requested_by, requested_at, checker_by, checker_at, checker_comment, reason,
				bank_name, account_nickname, upload_s3_key, has_pending_recat, sort_ts
			FROM page_base WHERE TRUE %s
			ORDER BY %s %s NULLS LAST, bank_statement_id %s
			LIMIT $%d
		`, whereExtra, sortExpr, dir, dir, argN)
		args = append(args, limit+1)

		rows, err := pool.Query(ctx, q, args...)
		if err != nil {
			http.Error(w, pqUserFriendlyMessage(err), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		resp := []map[string]interface{}{}
		for rows.Next() {
			var id, entityID, entityName, acc string
			var start, end, uploaded, sortTS time.Time
			var open, close float64
			var actionType, processingStatus, actionID, requestedBy, checkerBy, checkerComment, reason sql.NullString
			var bankName, accountNickname, uploadS3Key sql.NullString
			var requestedAt, checkerAt sql.NullTime
			var hasPendingRecat bool
			if err := rows.Scan(&id, &entityID, &entityName, &acc, &start, &end, &open, &close, &uploaded,
				&actionType, &processingStatus, &actionID, &requestedBy, &requestedAt, &checkerBy, &checkerAt, &checkerComment, &reason,
				&bankName, &accountNickname, &uploadS3Key, &hasPendingRecat, &sortTS); err != nil {
				continue
			}
			isDeletePending := processingStatus.String == constants.StatusPendingDeleteApproval
			resp = append(resp, map[string]interface{}{
				"statement_id": id, "bank_statement_id": id, "entity_id": entityID, "entity_name": entityName,
				"account_number": acc, "statement_period_start": start, "statement_period_end": end,
				"opening_balance": open, "closing_balance": close, "uploaded_at": uploaded,
				"action_type": actionType.String, "processing_status": processingStatus.String,
				"action_id": actionID.String, "requested_by": requestedBy.String, "requested_at": requestedAt.Time,
				"checker_by": checkerBy.String, "checker_at": checkerAt.Time, "checker_comment": checkerComment.String,
				"reason": reason.String, "bank_name": bankName.String, "account_nickname": accountNickname.String,
				"upload_s3_key": uploadS3Key.String, "is_delete_pending_approval": isDeletePending,
				"has_pending_recat": hasPendingRecat, "_sort_ts": sortTS,
			})
		}

		hasMore := len(resp) > limit
		if hasMore {
			resp = resp[:limit]
		}
		next := ""
		if hasMore && len(resp) > 0 {
			last := resp[len(resp)-1]
			next = common.EncodeCursor(stmtCursorVal(last, req.SortBy), fmt.Sprint(last["bank_statement_id"]))
			delete(last, "_sort_ts")
		}
		for i := range resp {
			delete(resp[i], "_sort_ts")
		}

		apictx.RespondEnvelopeSuccess(w, "Success", common.ListPage{
			Rows: resp, NextCursor: next, HasMore: hasMore, Limit: limit, TotalCount: &total,
		})
	})
}

func stmtCursorVal(row map[string]interface{}, sortBy string) string {
	switch strings.TrimSpace(sortBy) {
	case "entity_name", "account_number", "bank_name", "processing_status":
		return fmt.Sprint(row[sortBy])
	case "statement_period_start":
		return fmt.Sprint(row["statement_period_start"])
	case "id":
		return fmt.Sprint(row["bank_statement_id"])
	default:
		if ts, ok := row["_sort_ts"].(time.Time); ok {
			return ts.UTC().Format(time.RFC3339Nano)
		}
		return fmt.Sprint(row["uploaded_at"])
	}
}

// ListBankStatementTransactionsHandler — POST /cash/bank-statements/v2/transactions/list
func ListBankStatementTransactionsHandler(pool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			common.ListRequest
			BankStatementID string `json:"bank_statement_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || strings.TrimSpace(body.BankStatementID) == "" {
			http.Error(w, constants.ErrMissingUserIDOrBankStatementID, http.StatusBadRequest)
			return
		}
		ctx := r.Context()
		scope := ctxutil.FromContext(ctx)
		entityIDs := scope.EntityIDs
		if len(entityIDs) == 0 {
			http.Error(w, constants.ErrNoAccessibleBusinessUnit, http.StatusUnauthorized)
			return
		}
		accountNumbers := scopeValues(scope.BankAccounts, "account_number")
		bankIDs := scopeValues(scope.Banks, "bank_id")
		currencyCodes := scopeValues(scope.Currencies, "currency_code")
		limit := common.NormalizeLimit(body.Limit)
		dir := common.NormalizeSortDir(body.SortDir)
		sortMap := map[string]string{
			"value_date":        "COALESCE(value_date::text,'')",
			"transaction_date":  "COALESCE(transaction_date::text,'')",
			"description":       "COALESCE(description,'')",
			"withdrawal_amount": "COALESCE(withdrawal_amount::text,'')",
			"deposit_amount":    "COALESCE(deposit_amount::text,'')",
			"category_name":     "COALESCE(category_name,'')",
			"id":                "transaction_id::text",
		}
		sortExpr := common.WhitelistSort(body.SortBy, sortMap, "value_date")

		args := []interface{}{body.BankStatementID, entityIDs, accountNumbers, bankIDs, currencyCodes}
		argN := 6
		whereExtra := ""
		if search := strings.TrimSpace(body.Search); search != "" {
			like := "%" + search + "%"
			whereExtra += fmt.Sprintf(` AND (
				COALESCE(t.description,'') ILIKE $%d OR COALESCE(c.category_name,'') ILIKE $%d
				OR COALESCE(t.tran_id,'') ILIKE $%d OR COALESCE(t.narration_clean,'') ILIKE $%d
			)`, argN, argN, argN, argN)
			args = append(args, like)
			argN++
		}

		var total int64
		_ = pool.QueryRow(ctx, `
			SELECT COUNT(*)
			FROM cimplrcorpsaas.bank_statement_transactions t
			JOIN cimplrcorpsaas.bank_statements s ON t.bank_statement_id = s.bank_statement_id
			LEFT JOIN public.masterbankaccount mba ON mba.account_number = s.account_number AND mba.entity_id::text = s.entity_id::text AND mba.is_deleted = false
			LEFT JOIN public.mastercashflowcategory c ON t.category_id = c.category_id
			WHERE t.bank_statement_id = $1 AND s.entity_id = ANY($2)
			  AND (cardinality($3::text[]) = 0 OR s.account_number = ANY($3::text[]))
			  AND (cardinality($4::text[]) = 0 OR mba.bank_id = ANY($4::text[]))
			  AND (cardinality($5::text[]) = 0 OR mba.currency = ANY($5::text[]))
			`+whereExtra, args...).Scan(&total)

		if cur, ok := common.DecodeCursor(body.Cursor); ok {
			op := "<"
			if dir == "ASC" {
				op = ">"
			}
			whereExtra += fmt.Sprintf(" AND (%s, t.transaction_id::text) %s ($%d, $%d)", sortExpr, op, argN, argN+1)
			args = append(args, cur.V, cur.ID)
			argN += 2
		}

		q := fmt.Sprintf(`
			SELECT
				t.transaction_id, COALESCE(s.entity_id, '') AS entity_id, e.entity_name, t.tran_id,
				t.value_date, t.transaction_date, t.description, t.withdrawal_amount, t.deposit_amount,
				CASE WHEN COALESCE(t.balance, 0) <> 0 THEN t.balance
					ELSE ROUND(COALESCE(s.opening_balance, 0)
						+ SUM(COALESCE(t.deposit_amount, 0) - COALESCE(t.withdrawal_amount, 0))
							OVER (PARTITION BY t.bank_statement_id ORDER BY t.value_date, t.transaction_id
								ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 2)
				END AS balance,
				c.category_name, t.category_id,
				COALESCE(t.misclassified_flag, false), COALESCE(t.narration_clean, t.description, ''),
				COALESCE(t.narration_ref, ''), COALESCE(t.payment_channel, ''), t.confidence_score,
				COALESCE(t.classification_step, ''), COALESCE(cal.source_ref, ''), COALESCE(t.ai_reasoning, ''),
				COALESCE(q.status, ''), COALESCE(q.suggested_cat::text, ''), COALESCE(sq.category_name, '')
			FROM cimplrcorpsaas.bank_statement_transactions t
			JOIN cimplrcorpsaas.bank_statements s ON t.bank_statement_id = s.bank_statement_id
			JOIN public.masterentitycash e ON s.entity_id = e.entity_id
			LEFT JOIN public.masterbankaccount mba ON mba.account_number = s.account_number AND mba.entity_id::text = s.entity_id::text AND mba.is_deleted = false
			LEFT JOIN public.mastercashflowcategory c ON t.category_id = c.category_id
			LEFT JOIN LATERAL (
				SELECT source_ref FROM cimplrcorpsaas.classification_audit_log
				WHERE transaction_id = t.transaction_id ORDER BY classified_at DESC LIMIT 1
			) cal ON TRUE
			LEFT JOIN LATERAL (
				SELECT status, suggested_cat FROM cimplrcorpsaas.categorization_review_queue
				WHERE transaction_id = t.transaction_id AND status = 'PENDING' ORDER BY queue_id DESC LIMIT 1
			) q ON TRUE
			LEFT JOIN public.mastercashflowcategory sq ON sq.category_id::text = q.suggested_cat::text
			WHERE t.bank_statement_id = $1 AND s.entity_id = ANY($2)
			  AND (cardinality($3::text[]) = 0 OR s.account_number = ANY($3::text[]))
			  AND (cardinality($4::text[]) = 0 OR mba.bank_id = ANY($4::text[]))
			  AND (cardinality($5::text[]) = 0 OR mba.currency = ANY($5::text[]))
			%s
			ORDER BY %s %s NULLS LAST, t.transaction_id %s
			LIMIT $%d
		`, whereExtra, sortExpr, dir, dir, argN)
		args = append(args, limit+1)

		rows, err := pool.Query(ctx, q, args...)
		if err != nil {
			http.Error(w, "DB error: "+err.Error(), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		resp := scanTxnListRows(ctx, rows)
		hasMore := len(resp) > limit
		if hasMore {
			resp = resp[:limit]
		}
		next := ""
		if hasMore && len(resp) > 0 {
			last := resp[len(resp)-1]
			next = common.EncodeCursor(txnCursorVal(last, body.SortBy), fmt.Sprint(last["transaction_id"]))
		}
		apictx.RespondEnvelopeSuccess(w, "Success", common.ListPage{
			Rows: resp, NextCursor: next, HasMore: hasMore, Limit: limit, TotalCount: &total,
		})
	})
}

func txnCursorVal(row map[string]interface{}, sortBy string) string {
	switch strings.TrimSpace(sortBy) {
	case "description", "category_name":
		return fmt.Sprint(row[sortBy])
	case "withdrawal_amount", "deposit_amount":
		return fmt.Sprint(row[sortBy])
	case "transaction_date":
		return fmt.Sprint(row["transaction_date"])
	case "id":
		return fmt.Sprint(row["transaction_id"])
	default:
		return fmt.Sprint(row["value_date"])
	}
}

func scanTxnListRows(_ context.Context, rows interface {
	Next() bool
	Scan(dest ...any) error
}) []map[string]interface{} {
	resp := []map[string]interface{}{}
	for rows.Next() {
		var (
			tid                                                         int64
			entityID, entityName                                        string
			tranID                                                      sql.NullString
			valueDate, txnDate                                          sql.NullTime
			description                                                 sql.NullString
			withdrawal, deposit, balance, confidence                    sql.NullFloat64
			categoryName                                                sql.NullString
			categoryID                                                  sql.NullInt64
			misclassified                                               bool
			narrationClean, narrationRef, paymentChannel                string
			classificationStep, classificationBasis, aiReasoning        string
			reviewStatus, suggestedCatID, suggestedCatName              string
		)
		if err := rows.Scan(
			&tid, &entityID, &entityName, &tranID, &valueDate, &txnDate, &description,
			&withdrawal, &deposit, &balance, &categoryName, &categoryID, &misclassified,
			&narrationClean, &narrationRef, &paymentChannel, &confidence,
			&classificationStep, &classificationBasis, &aiReasoning,
			&reviewStatus, &suggestedCatID, &suggestedCatName,
		); err != nil {
			continue
		}
		rec := map[string]interface{}{
			"transaction_id": tid, "entity_id": entityID, "entity_name": entityName,
			"tran_id": tranID.String, "description": description.String,
			"withdrawal_amount": withdrawal.Float64, "deposit_amount": deposit.Float64,
			"balance": balance.Float64, "category_name": categoryName.String,
			"misclassified_flag": misclassified, "narration_clean": narrationClean,
			"narration_ref": narrationRef, "payment_channel": paymentChannel,
			"classification_step": classificationStep, "classification_basis": classificationBasis,
			"ai_reasoning": aiReasoning, "review_status": reviewStatus,
			"suggested_cat_id": suggestedCatID, "suggested_cat_name": suggestedCatName,
		}
		if categoryID.Valid {
			rec["category_id"] = categoryID.Int64
		}
		if confidence.Valid {
			rec["confidence_score"] = confidence.Float64
		}
		if valueDate.Valid {
			rec["value_date"] = valueDate.Time
		}
		if txnDate.Valid {
			rec["transaction_date"] = txnDate.Time
		}
		resp = append(resp, rec)
	}
	return resp
}
