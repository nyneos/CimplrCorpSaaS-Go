package bankstatement

import (
	apictx "CimplrCorpSaas/api"
	"CimplrCorpSaas/api/cash/bsasync"
	"CimplrCorpSaas/api/constants"
	middlewares "CimplrCorpSaas/api/middlewares"
	"CimplrCorpSaas/internal/ctxutil"
	"CimplrCorpSaas/internal/validation"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TransactionCategory represents a category master
type TransactionCategory struct {
	CategoryID   string `json:"category_id"`
	CategoryName string `json:"category_name"`
	CategoryType string `json:"category_type"`
	Description  string `json:"description"`
}

// RuleScope represents a rule scope
type RuleScope struct {
	ScopeID       int64   `json:"scope_id"`
	ScopeType     string  `json:"scope_type"`
	EntityID      *string `json:"entity_id,omitempty"`
	BankCode      *string `json:"bank_code,omitempty"`
	AccountNumber *string `json:"account_number,omitempty"`
	Currency      *string `json:"currency,omitempty"`
}

// CategoryRule represents a category rule
type CategoryRule struct {
	RuleID        int64      `json:"rule_id"`
	RuleName      string     `json:"rule_name"`
	CategoryID    string     `json:"category_id"`
	ScopeID       int64      `json:"scope_id"`
	Priority      int        `json:"priority"`
	IsActive      bool       `json:"is_active"`
	CreatedAt     time.Time  `json:"created_at"`
	EffectiveDate *time.Time `json:"effective_date,omitempty"`
}

// CategoryRuleComponent represents a rule component
type CategoryRuleComponent struct {
	ComponentID    int64    `json:"component_id"`
	RuleID         int64    `json:"rule_id"`
	ComponentType  string   `json:"component_type"`
	MatchType      *string  `json:"match_type,omitempty"`
	MatchValue     *string  `json:"match_value,omitempty"`
	AmountOperator *string  `json:"amount_operator,omitempty"`
	AmountValue    *float64 `json:"amount_value,omitempty"`
	TxnFlow        *string  `json:"txn_flow,omitempty"`
	CurrencyCode   *string  `json:"currency_code,omitempty"`
	IsActive       bool     `json:"is_active"`
}

// ruleQueryerLocal abstracts Query for both pgx pool and transactions within this file.
type ruleQueryerLocal interface {
	Query(context.Context, string, ...interface{}) (pgx.Rows, error)
}

func isFKViolation(err error) bool {
	if err == nil {
		return false
	}
	if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.Code == "23503" {
		return true
	}
	return false
}

func writeFKConflict(w http.ResponseWriter) {
	apictx.RespondEnvelopeError(w, http.StatusBadRequest, "Bank Statement Transactions with this category exists in the system. Please delete them first", "")
}

func requestedByFromCtx(ctx context.Context, fallback string) string {
	if s := middlewares.GetSessionFromContext(ctx); s != nil {
		if strings.TrimSpace(s.Name) != "" {
			return s.Name
		}
		if strings.TrimSpace(s.UserID) != "" {
			return s.UserID
		}
	}
	return fallback
}

func ctxHasApprovedBank(ctx context.Context, bankCode string) bool {
	bankCode = strings.TrimSpace(bankCode)
	if bankCode == "" {
		return false
	}
	v := ctx.Value("BankInfo")
	if v == nil {
		return true
	}
	banks, ok := v.([]map[string]string)
	if !ok {
		return true
	}
	for _, b := range banks {
		if strings.EqualFold(strings.TrimSpace(b["bank_id"]), bankCode) ||
			strings.EqualFold(strings.TrimSpace(b["bank_name"]), bankCode) ||
			strings.EqualFold(strings.TrimSpace(b["bank_short_name"]), bankCode) {
			return true
		}
	}
	return false
}

func validateScopeAccess(ctx context.Context, scopeType string, entityID, bankCode, accountNumber, currency *string) (int, string) {
	st := strings.ToUpper(strings.TrimSpace(scopeType))
	switch st {
	case "GLOBAL":
		return 0, ""
	case "ENTITY":
		if entityID == nil || strings.TrimSpace(*entityID) == "" {
			return http.StatusBadRequest, "Missing entity_id"
		}
		ids := ctxutil.FromContext(ctx).EntityIDs
		if len(ids) == 0 {
			return http.StatusForbidden, constants.ErrNoAccessibleEntitiesForRequest
		}
		for _, id := range ids {
			if id == *entityID {
				return 0, ""
			}
		}
		return http.StatusForbidden, constants.ErrUnauthorizedEntity
	case "ACCOUNT":
		if accountNumber == nil || strings.TrimSpace(*accountNumber) == "" {
			return http.StatusBadRequest, "Missing account_number"
		}
		if !ctxutil.FromContext(ctx).HasApprovedBankAccount(*accountNumber) {
			return http.StatusForbidden, constants.ErrInvalidAccount
		}
		return 0, ""
	case "BANK":
		if bankCode == nil || strings.TrimSpace(*bankCode) == "" {
			return http.StatusBadRequest, "Missing bank_code"
		}
		if !ctxHasApprovedBank(ctx, *bankCode) {
			return http.StatusForbidden, "Invalid or inactive bank"
		}
		return 0, ""
	case "CURRENCY":
		if currency == nil || strings.TrimSpace(*currency) == "" {
			return http.StatusBadRequest, "Missing currency"
		}
		if !ctxutil.FromContext(ctx).HasApprovedCurrency(*currency) {
			return http.StatusForbidden, constants.ErrCurrencyNotAllowed
		}
		return 0, ""
	default:
		return http.StatusBadRequest, "Invalid scope_type"
	}
}

func validateCashCategoryAccess(ctx context.Context, category interface{}) string {
	return validation.ValidateCashMasterReferences(ctx, map[string]interface{}{"category_id": category})
}

// loadCategoryRuleComponentsLocal mirrors the rule loader used during upload/recompute without depending on the upload file.
func loadCategoryRuleComponentsLocal(ctx context.Context, db ruleQueryerLocal, accountNumber, entityID string, accountCurrency *string) ([]categoryRuleComponent, error) {
	const q = `
		 SELECT r.rule_id, r.priority, r.category_id, c.category_name, c.category_type, comp.component_type, comp.match_type, comp.match_value, comp.amount_operator, comp.amount_value, comp.txn_flow, comp.currency_code, r.effective_date
	 FROM cimplrcorpsaas.category_rules r
	 JOIN public.mastercashflowcategory c ON r.category_id = c.category_id
		 JOIN cimplrcorpsaas.category_rule_components comp ON r.rule_id = comp.rule_id AND comp.is_active = true
		 JOIN cimplrcorpsaas.rule_scope s ON r.scope_id = s.scope_id
		 LEFT JOIN public.masterbankaccount mba ON mba.account_number = $1
		 WHERE r.is_active = true
	  AND (
			  (s.scope_type = 'ACCOUNT' AND s.account_number = $1)
			  OR (s.scope_type = 'ENTITY' AND s.entity_id = $2)
			  OR (s.scope_type = 'BANK' AND s.bank_code = mba.bank_id)
			  OR (s.scope_type = 'CURRENCY' AND s.currency = $3)
			  OR (s.scope_type = 'GLOBAL')
	  )
		 ORDER BY r.priority ASC, r.rule_id ASC, comp.component_id ASC
	`

	// pass accountCurrency (may be nil) so currency-scoped rules are matched only when account currency is known
	var acctCurrencyParam interface{}
	if accountCurrency != nil {
		acctCurrencyParam = *accountCurrency
	} else {
		acctCurrencyParam = nil
	}

	rows, err := db.Query(ctx, q, accountNumber, entityID, acctCurrencyParam)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var rules []categoryRuleComponent
	for rows.Next() {
		var rc categoryRuleComponent
		if err := rows.Scan(&rc.RuleID, &rc.Priority, &rc.CategoryID, &rc.CategoryName, &rc.CategoryType, &rc.ComponentType, &rc.MatchType, &rc.MatchValue, &rc.AmountOperator, &rc.AmountValue, &rc.TxnFlow, &rc.CurrencyCode, &rc.EffectiveDate); err != nil {
			return nil, err
		}
		rules = append(rules, rc)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return rules, nil
}

// ListCategoriesForUserHandler returns minimal category id/name list (POST expects user_id, currently unused for filtering).
func ListCategoriesForUserHandler(pool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			UserID string `json:"user_id"`
		}
		// best-effort parse; no filter yet
		_ = json.NewDecoder(r.Body).Decode(&body)

		rows, err := pool.Query(r.Context(), `SELECT category_id, category_name FROM public.mastercashflowcategory ORDER BY category_name`)
		if err != nil {
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		var out []TransactionCategory
		for rows.Next() {
			var c TransactionCategory
			if err := rows.Scan(&c.CategoryID, &c.CategoryName); err == nil {
				if msg := validateCashCategoryAccess(r.Context(), c.CategoryID); msg != "" {
					continue
				}
				out = append(out, c)
			}
		}

		apictx.RespondEnvelopeSuccess(w, "Success", out)
	})
}

// MapTransactionsToCategoryHandler assigns a category to transactions and raises pending edit approval per bank statement.
func MapTransactionsToCategoryHandler(pool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			TransactionIDs []int64 `json:"transaction_ids"`
			CategoryID     string  `json:"category_id"`
			UserID         string  `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || len(body.TransactionIDs) == 0 || strings.TrimSpace(body.CategoryID) == "" {
			http.Error(w, "Missing transaction_ids or category_id", http.StatusBadRequest)
			return
		}

		ctx := r.Context()
		if msg := validateCashCategoryAccess(ctx, body.CategoryID); msg != "" {
			http.Error(w, msg, http.StatusForbidden)
			return
		}
		entityIDs := ctxutil.FromContext(ctx).EntityIDs
		if len(entityIDs) == 0 {
			http.Error(w, constants.ErrNoAccessibleEntitiesForRequest, http.StatusForbidden)
			return
		}

		tx, err := pool.Begin(ctx)
		if err != nil {
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}
		defer func() {
			if p := recover(); p != nil {

				http.Error(w, constants.ErrInternalServer, http.StatusInternalServerError)
			}
		}()

		// Ensure all provided transaction IDs are within user's accessible entity/account scope
		scopeRows, err := tx.Query(ctx, `
	SELECT DISTINCT bs.entity_id, bs.account_number, m.currency
	FROM cimplrcorpsaas.bank_statement_transactions t
	JOIN cimplrcorpsaas.bank_statements bs ON t.bank_statement_id = bs.bank_statement_id
	LEFT JOIN public.masterbankaccount m ON bs.account_number = m.account_number
	WHERE t.transaction_id = ANY($1) AND t.bank_statement_id IS NOT NULL
	`, body.TransactionIDs)
		if err != nil {

			http.Error(w, pqUserFriendlyMessage(err), http.StatusInternalServerError)
			return
		}
		unauthorizedEntity := false
		unauthorizedAccount := false
		unauthorizedCurrency := false
		for scopeRows.Next() {
			var e, a string
			var acctCurrency sql.NullString
			if err := scopeRows.Scan(&e, &a, &acctCurrency); err != nil {
				continue
			}
			allowed := false
			for _, id := range entityIDs {
				if id == e {
					allowed = true
					break
				}
			}
			if !allowed {
				unauthorizedEntity = true
				break
			}
			if !ctxutil.FromContext(ctx).HasApprovedBankAccount(a) {
				unauthorizedAccount = true
				break
			}

			// Enforce currency: ensure account's currency is approved in context (use currency fetched in query)
			if acctCurrency.Valid && strings.TrimSpace(acctCurrency.String) != "" {
				if !ctxutil.FromContext(ctx).HasApprovedCurrency(acctCurrency.String) {
					unauthorizedCurrency = true
					break
				}
			}
		}
		scopeRows.Close()
		if unauthorizedEntity {
			http.Error(w, constants.ErrUnauthorizedEntity, http.StatusForbidden)
			return
		}
		if unauthorizedAccount {
			http.Error(w, constants.ErrInvalidAccount, http.StatusForbidden)
			return
		}
		if unauthorizedCurrency {
			http.Error(w, constants.ErrCurrencyNotAllowed, http.StatusForbidden)
			return
		}

		// Load current categories, stage RECAT audit + per-txn old/new, then apply.
		oldRows, err := tx.Query(ctx, `
			SELECT transaction_id, bank_statement_id::text, COALESCE(category_id::text, '')
			FROM cimplrcorpsaas.bank_statement_transactions
			WHERE transaction_id = ANY($1) AND bank_statement_id IS NOT NULL`, body.TransactionIDs)
		if err != nil {
			http.Error(w, pqUserFriendlyMessage(err), http.StatusInternalServerError)
			return
		}
		recatByBS := map[string][]bsasync.TxnCategoryChange{}
		for oldRows.Next() {
			var txnID int64
			var bsID, oldCat string
			if err := oldRows.Scan(&txnID, &bsID, &oldCat); err != nil {
				continue
			}
			if oldCat == body.CategoryID {
				continue
			}
			recatByBS[bsID] = append(recatByBS[bsID], bsasync.TxnCategoryChange{
				TransactionID: txnID,
				FieldName:     "category_id",
				OldValue:      oldCat,
				NewValue:      body.CategoryID,
			})
		}
		oldRows.Close()

		requestedBy := requestedByFromCtx(ctx, body.UserID)
		requestedIP := strings.TrimSpace(apictx.ClientIPFromRequest(r))
		if requestedIP == "" {
			requestedIP = "system"
		}
		for bsID, changes := range recatByBS {
			if err := bsasync.ApplyManualRecatInTx(ctx, tx, bsID, changes, requestedBy, requestedIP); err != nil {
				http.Error(w, pqUserFriendlyMessage(err), http.StatusInternalServerError)
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}

		apictx.RespondEnvelopeSuccess(w, "Transactions mapped and approval requested", nil)
	})
}

// CategorizeUncategorizedTransactionsHandler assigns the given category to all transactions with NULL category
// and raises pending edit approval for their bank statements.
func CategorizeUncategorizedTransactionsHandler(pool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			CategoryID string `json:"category_id"`
			UserID     string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || strings.TrimSpace(body.CategoryID) == "" {
			http.Error(w, "Missing category_id", http.StatusBadRequest)
			return
		}

		ctx := r.Context()
		entityIDs := ctxutil.FromContext(ctx).EntityIDs
		if len(entityIDs) == 0 {
			http.Error(w, constants.ErrNoAccessibleEntitiesForRequest, http.StatusForbidden)
			return
		}
		tx, err := pool.Begin(ctx)
		if err != nil {
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}
		defer func() {
			if p := recover(); p != nil {

				http.Error(w, constants.ErrInternalServer, http.StatusInternalServerError)
			}
		}()

		rows, err := tx.Query(ctx, `
SELECT t.transaction_id,
			 t.bank_statement_id,
			 bs.account_number,
			 bs.entity_id,
			 COALESCE(t.description, ''),
			 t.withdrawal_amount,
			 t.deposit_amount,
			 t.value_date,
			 m.currency
FROM cimplrcorpsaas.bank_statement_transactions t
JOIN cimplrcorpsaas.bank_statements bs ON t.bank_statement_id = bs.bank_statement_id
LEFT JOIN public.masterbankaccount m ON bs.account_number = m.account_number
WHERE t.category_id IS NULL
	AND t.bank_statement_id IS NOT NULL
	AND bs.entity_id = ANY($1);
`, entityIDs)
		if err != nil {

			http.Error(w, pqUserFriendlyMessage(err), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		type txnRow struct {
			id        int64
			bsID      string
			acct      string
			entity    string
			desc      string
			wd        sql.NullFloat64
			dep       sql.NullFloat64
			valueDate sql.NullTime
			currency  sql.NullString
		}

		var txns []txnRow
		for rows.Next() {
			var tr txnRow
			if err := rows.Scan(&tr.id, &tr.bsID, &tr.acct, &tr.entity, &tr.desc, &tr.wd, &tr.dep, &tr.valueDate, &tr.currency); err == nil {
				// Skip transactions whose account currency is not approved (enforce currency after account)
				if tr.currency.Valid && strings.TrimSpace(tr.currency.String) != "" {
					if !ctxutil.FromContext(ctx).HasApprovedCurrency(tr.currency.String) {
						// skip this txn
						continue
					}
				}
				txns = append(txns, tr)
			}
		}
		if err := rows.Err(); err != nil {

			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}

		ruleCache := make(map[string][]categoryRuleComponent)
		recatByBS := map[string][]bsasync.TxnCategoryChange{}
		for _, tr := range txns {
			if !ctxutil.FromContext(ctx).HasApprovedBankAccount(tr.acct) {
				continue
			}
			// Skip transactions whose account currency is not approved (we already fetched currency)
			if tr.currency.Valid && strings.TrimSpace(tr.currency.String) != "" {
				if !ctxutil.FromContext(ctx).HasApprovedCurrency(tr.currency.String) {
					continue
				}
			}
			cacheKey := tr.acct + "|" + tr.entity
			rules, ok := ruleCache[cacheKey]
			if !ok {
				var acctCurPtr *string
				if tr.currency.Valid && strings.TrimSpace(tr.currency.String) != "" {
					s := tr.currency.String
					acctCurPtr = &s
				}
				rules, err = loadCategoryRuleComponentsLocal(ctx, tx, tr.acct, tr.entity, acctCurPtr)
				if err != nil {
					continue
				}
				ruleCache[cacheKey] = rules
			}

			matched := matchCategoryForTransaction(rules, tr.desc, tr.wd, tr.dep, tr.valueDate)
			if matched.Valid && matched.String == body.CategoryID {
				recatByBS[tr.bsID] = append(recatByBS[tr.bsID], bsasync.TxnCategoryChange{
					TransactionID: tr.id,
					FieldName:     "category_id",
					OldValue:      "",
					NewValue:      matched.String,
				})
			}
		}

		updated := 0
		requestedBy := requestedByFromCtx(ctx, body.UserID)
		requestedIP := strings.TrimSpace(apictx.ClientIPFromRequest(r))
		if requestedIP == "" {
			requestedIP = "system"
		}
		for bsID, changes := range recatByBS {
			if err := bsasync.ApplyManualRecatInTx(ctx, tx, bsID, changes, requestedBy, requestedIP); err != nil {
				http.Error(w, pqUserFriendlyMessage(err), http.StatusInternalServerError)
				return
			}
			updated += len(changes)
		}

		if err := tx.Commit(ctx); err != nil {
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}

		apictx.RespondEnvelopeSuccessCompat(w, "Success", map[string]interface{}{
			"updated_transactions":     updated,
			"affected_bank_statements": len(recatByBS),
		})
	})
}

// RecomputeUncategorizedTransactionsHandler applies rules to uncategorized transactions and raises pending edit approvals.
func RecomputeUncategorizedTransactionsHandler(pool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			UserID string `json:"user_id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)

		ctx := r.Context()
		entityIDs := ctxutil.FromContext(ctx).EntityIDs
		if len(entityIDs) == 0 {
			http.Error(w, constants.ErrNoAccessibleEntitiesForRequest, http.StatusForbidden)
			return
		}
		tx, err := pool.Begin(ctx)
		if err != nil {
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}
		defer func() {
			if p := recover(); p != nil {

				http.Error(w, constants.ErrInternalServer, http.StatusInternalServerError)
			}
		}()

		rows, err := tx.Query(ctx, `
SELECT t.transaction_id,
			 t.bank_statement_id,
			 bs.account_number,
			 bs.entity_id,
			 COALESCE(t.description, ''),
			 t.withdrawal_amount,
			 t.deposit_amount,
			 t.value_date,
			 m.currency
FROM cimplrcorpsaas.bank_statement_transactions t
JOIN cimplrcorpsaas.bank_statements bs ON t.bank_statement_id = bs.bank_statement_id
LEFT JOIN public.masterbankaccount m ON bs.account_number = m.account_number
WHERE t.category_id IS NULL
	AND t.bank_statement_id IS NOT NULL
	AND bs.entity_id = ANY($1);
`, entityIDs)
		if err != nil {

			http.Error(w, pqUserFriendlyMessage(err), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		type txnRow struct {
			id        int64
			bsID      string
			acct      string
			entity    string
			desc      string
			wd        sql.NullFloat64
			dep       sql.NullFloat64
			valueDate sql.NullTime
			currency  sql.NullString
		}

		var txns []txnRow
		for rows.Next() {
			var tr txnRow
			if err := rows.Scan(&tr.id, &tr.bsID, &tr.acct, &tr.entity, &tr.desc, &tr.wd, &tr.dep, &tr.valueDate, &tr.currency); err == nil {
				txns = append(txns, tr)
			}
		}
		if err := rows.Err(); err != nil {

			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}

		ruleCache := make(map[string][]categoryRuleComponent)
		recatByBS := map[string][]bsasync.TxnCategoryChange{}

		for _, tr := range txns {
			if !ctxutil.FromContext(ctx).HasApprovedBankAccount(tr.acct) {
				continue
			}
			// skip if account currency not allowed
			if tr.currency.Valid && strings.TrimSpace(tr.currency.String) != "" {
				if !ctxutil.FromContext(ctx).HasApprovedCurrency(tr.currency.String) {
					continue
				}
			}
			cacheKey := tr.acct + "|" + tr.entity
			rules, ok := ruleCache[cacheKey]
			if !ok {
				var acctCurPtr *string
				if tr.currency.Valid && strings.TrimSpace(tr.currency.String) != "" {
					s := tr.currency.String
					acctCurPtr = &s
				}
				rules, err = loadCategoryRuleComponentsLocal(ctx, tx, tr.acct, tr.entity, acctCurPtr)
				if err != nil {
					continue
				}
				ruleCache[cacheKey] = rules
			}

			matched := matchCategoryForTransaction(rules, tr.desc, tr.wd, tr.dep, tr.valueDate)
			if matched.Valid {
				recatByBS[tr.bsID] = append(recatByBS[tr.bsID], bsasync.TxnCategoryChange{
					TransactionID: tr.id,
					FieldName:     "category_id",
					OldValue:      "",
					NewValue:      matched.String,
				})
			}
		}

		updated := 0
		requestedBy := requestedByFromCtx(ctx, body.UserID)
		requestedIP := strings.TrimSpace(apictx.ClientIPFromRequest(r))
		if requestedIP == "" {
			requestedIP = "system"
		}
		for bsID, changes := range recatByBS {
			if err := bsasync.ApplyManualRecatInTx(ctx, tx, bsID, changes, requestedBy, requestedIP); err != nil {
				http.Error(w, pqUserFriendlyMessage(err), http.StatusInternalServerError)
				return
			}
			updated += len(changes)
		}

		if err := tx.Commit(ctx); err != nil {
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}

		apictx.RespondEnvelopeSuccessCompat(w, "Success", map[string]interface{}{
			"updated_transactions":     updated,
			"affected_bank_statements": len(recatByBS),
		})
	})
}

// DeleteMultipleTransactionCategoriesHandler deletes multiple categories and cascades deletes for rules, rule scopes, and rule components
func DeleteMultipleTransactionCategoriesHandler(pool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			CategoryIDs []string `json:"category_ids"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || len(body.CategoryIDs) == 0 {
			http.Error(w, "Missing or invalid category_ids", http.StatusBadRequest)
			return
		}

		ctx := r.Context()
		if msg := validateCashCategoryAccess(ctx, body.CategoryIDs); msg != "" {
			http.Error(w, msg, http.StatusForbidden)
			return
		}
		tx, err := pool.Begin(ctx)
		if err != nil {
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}
		defer func() {
			if p := recover(); p != nil {

				http.Error(w, constants.ErrInternalServer, http.StatusInternalServerError)
			}
		}()
		// Safety check: abort if any transactions currently reference these categories
		var txnCount int
		if err := tx.QueryRow(ctx, `SELECT COUNT(*) FROM cimplrcorpsaas.bank_statement_transactions WHERE category_id = ANY($1)`, body.CategoryIDs).Scan(&txnCount); err != nil {

			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}
		if txnCount > 0 {

			http.Error(w, "Cannot delete rules: some transactions currently reference these categories. Unassign transactions first.", http.StatusBadRequest)
			return
		}

		// 1. Get all rules and scope_ids for these categories
		ruleRows, err := tx.Query(r.Context(), `SELECT rule_id, scope_id FROM cimplrcorpsaas.category_rules WHERE category_id = ANY($1)`, body.CategoryIDs)
		if err != nil {

			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}
		var ruleIDs []int64
		var scopeIDs []int64
		for ruleRows.Next() {
			var ruleID, scopeID int64
			if err := ruleRows.Scan(&ruleID, &scopeID); err == nil {
				ruleIDs = append(ruleIDs, ruleID)
				scopeIDs = append(scopeIDs, scopeID)
			}
		}
		ruleRows.Close()

		// 2. Delete all rule components for these rules
		if len(ruleIDs) > 0 {
			_, err = tx.Exec(r.Context(), `DELETE FROM cimplrcorpsaas.category_rule_components WHERE rule_id = ANY($1)`, ruleIDs)
			if err != nil {
				if isFKViolation(err) {

					writeFKConflict(w)
					return
				}

				http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
				return
			}
		}

		// 3. Delete all rules for these categories
		_, err = tx.Exec(r.Context(), `DELETE FROM cimplrcorpsaas.category_rules WHERE category_id = ANY($1)`, body.CategoryIDs)
		if err != nil {
			if isFKViolation(err) {

				writeFKConflict(w)
				return
			}

			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}

		// 4. Delete all rule scopes for these rules (if not used elsewhere)
		for _, scopeID := range scopeIDs {
			var count int
			err = tx.QueryRow(r.Context(), `SELECT COUNT(*) FROM cimplrcorpsaas.category_rules WHERE scope_id = $1`, scopeID).Scan(&count)
			if err == nil && count == 0 {
				_, _ = tx.Exec(r.Context(), `DELETE FROM cimplrcorpsaas.rule_scope WHERE scope_id = $1`, scopeID)
			}
		}

		// NOTE: Do NOT delete the master category rows here. Categories are managed
		// in `public.mastercashflowcategory` and should only be removed via the
		// category management UI/API when safe to do so. Here we only remove
		// rules, components, and orphaned scopes. The caller is expected to
		// unassign transactions first if they intend to delete the category.

		if err := tx.Commit(ctx); err != nil {
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}

		apictx.RespondEnvelopeSuccess(w, "Categories deleted successfully", nil)
	})
}

// --- Category CRUD ---
func CreateTransactionCategoryHandler(pool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			CategoryName string `json:"category_name"`
			CategoryType string `json:"category_type"`
			Description  string `json:"description"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.CategoryName == "" {
			http.Error(w, "Missing or invalid category_name", http.StatusBadRequest)
			return
		}
		if body.CategoryType == "" {
			body.CategoryType = "BOTH"
		}
		var id string
		err := pool.QueryRow(r.Context(), `INSERT INTO public.mastercashflowcategory (category_name, category_type, description) VALUES ($1, $2, $3) RETURNING category_id`, body.CategoryName, body.CategoryType, body.Description).Scan(&id)
		if err != nil {
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}
		apictx.RespondEnvelopeSuccess(w, "Success", map[string]interface{}{"category_id": id})
	})
}

func ListTransactionCategoriesHandler(pool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		catRows, err := pool.Query(r.Context(), `SELECT category_id, category_name, category_type, description FROM public.mastercashflowcategory`)
		if err != nil {
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}
		defer catRows.Close()

		type RuleWithDetails struct {
			CategoryRule
			Scope      *RuleScope              `json:"scope,omitempty"`
			Components []CategoryRuleComponent `json:"components"`
		}

		type CategoryWithRules struct {
			TransactionCategory
			Rules []RuleWithDetails `json:"rules"`
		}

		var categories []CategoryWithRules
		var catIDs []string
		catIndex := make(map[string]int)

		for catRows.Next() {
			var c TransactionCategory
			if err := catRows.Scan(&c.CategoryID, &c.CategoryName, &c.CategoryType, &c.Description); err != nil {
				continue
			}
			if msg := validateCashCategoryAccess(r.Context(), c.CategoryID); msg != "" {
				continue
			}
			catIndex[c.CategoryID] = len(categories)
			catIDs = append(catIDs, c.CategoryID)
			categories = append(categories, CategoryWithRules{TransactionCategory: c})
		}

		// Early return if no categories
		if len(categories) == 0 {
			apictx.RespondEnvelopeSuccess(w, "Success", categories)
			return
		}

		// Fetch all rules for these categories in one query (include effective_date)
		ruleRows, err := pool.Query(r.Context(), `SELECT rule_id, rule_name, category_id, scope_id, priority, is_active, created_at, effective_date FROM cimplrcorpsaas.category_rules WHERE category_id = ANY($1) ORDER BY rule_id DESC`, catIDs)
		if err != nil {
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}
		defer ruleRows.Close()

		rulesByCat := make(map[string][]CategoryRule)
		var scopeIDs []int64
		scopeSeen := make(map[int64]struct{})
		var ruleIDs []int64

		for ruleRows.Next() {
			var rule CategoryRule
			if err := ruleRows.Scan(&rule.RuleID, &rule.RuleName, &rule.CategoryID, &rule.ScopeID, &rule.Priority, &rule.IsActive, &rule.CreatedAt, &rule.EffectiveDate); err != nil {
				continue
			}
			rulesByCat[rule.CategoryID] = append(rulesByCat[rule.CategoryID], rule)
			ruleIDs = append(ruleIDs, rule.RuleID)
			if _, ok := scopeSeen[rule.ScopeID]; !ok && rule.ScopeID != 0 {
				scopeSeen[rule.ScopeID] = struct{}{}
				scopeIDs = append(scopeIDs, rule.ScopeID)
			}
		}

		// Fetch scopes in batch
		scopeMap := make(map[int64]RuleScope)
		if len(scopeIDs) > 0 {
			scopeRows, err := pool.Query(r.Context(), `SELECT scope_id, scope_type, entity_id, bank_code, account_number, currency FROM cimplrcorpsaas.rule_scope WHERE scope_id = ANY($1)`, scopeIDs)
			if err == nil {
				for scopeRows.Next() {
					var s RuleScope
					if err := scopeRows.Scan(&s.ScopeID, &s.ScopeType, &s.EntityID, &s.BankCode, &s.AccountNumber, &s.Currency); err == nil {
						scopeMap[s.ScopeID] = s
					}
				}
				scopeRows.Close()
			}
		}

		// Fetch components in batch
		compsByRule := make(map[int64][]CategoryRuleComponent)
		if len(ruleIDs) > 0 {
			compRows, err := pool.Query(r.Context(), `SELECT component_id, rule_id, component_type, match_type, match_value, amount_operator, amount_value, txn_flow, currency_code, is_active FROM cimplrcorpsaas.category_rule_components WHERE rule_id = ANY($1)`, ruleIDs)
			if err == nil {
				for compRows.Next() {
					var comp CategoryRuleComponent
					if err := compRows.Scan(&comp.ComponentID, &comp.RuleID, &comp.ComponentType, &comp.MatchType, &comp.MatchValue, &comp.AmountOperator, &comp.AmountValue, &comp.TxnFlow, &comp.CurrencyCode, &comp.IsActive); err == nil {
						compsByRule[comp.RuleID] = append(compsByRule[comp.RuleID], comp)
					}
				}
				compRows.Close()
			}
		}

		// Assemble output
		for i := range categories {
			cid := categories[i].CategoryID
			rules := rulesByCat[cid]
			for _, rule := range rules {
				var scopePtr *RuleScope
				if scope, ok := scopeMap[rule.ScopeID]; ok {
					scopeCopy := scope
					scopePtr = &scopeCopy
				}
				categories[i].Rules = append(categories[i].Rules, RuleWithDetails{
					CategoryRule: rule,
					Scope:        scopePtr,
					Components:   compsByRule[rule.RuleID],
				})
			}
		}

		apictx.RespondEnvelopeSuccess(w, "Success", categories)
	})
}

// --- Rule Scope CRUD ---
func CreateRuleScopeHandler(pool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		var body RuleScope
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.ScopeType == "" {
			http.Error(w, "Missing or invalid scope_type", http.StatusBadRequest)
			return
		}
		if code, msg := validateScopeAccess(r.Context(), body.ScopeType, body.EntityID, body.BankCode, body.AccountNumber, body.Currency); code != 0 {
			http.Error(w, msg, code)
			return
		}
		var id int64
		err := pool.QueryRow(r.Context(), `INSERT INTO cimplrcorpsaas.rule_scope (scope_type, entity_id, bank_code, account_number, currency) VALUES ($1, $2, $3, $4, $5) RETURNING scope_id`, body.ScopeType, body.EntityID, body.BankCode, body.AccountNumber, body.Currency).Scan(&id)
		if err != nil {
			http.Error(w, pqUserFriendlyMessage(err), http.StatusInternalServerError)
			return
		}
		apictx.RespondEnvelopeSuccess(w, "Success", map[string]interface{}{"scope_id": id})
	})
}

// --- Category Rule CRUD ---
func CreateCategoryRuleHandler(pool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			RuleName      string  `json:"rule_name"`
			CategoryID    string  `json:"category_id"`
			ScopeID       int64   `json:"scope_id"`
			Priority      int     `json:"priority"`
			IsActive      *bool   `json:"is_active"`
			EffectiveDate *string `json:"effective_date,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.RuleName == "" || strings.TrimSpace(body.CategoryID) == "" || body.ScopeID == 0 {
			http.Error(w, "Missing or invalid fields", http.StatusBadRequest)
			return
		}
		if msg := validateCashCategoryAccess(r.Context(), body.CategoryID); msg != "" {
			http.Error(w, msg, http.StatusForbidden)
			return
		}
		isActive := true
		if body.IsActive != nil {
			isActive = *body.IsActive
		}

		// Validate scope belongs to caller's entity/bank/account context
		var st string
		var entID, bankCode, acctNo sql.NullString
		var currency sql.NullString
		err := pool.QueryRow(r.Context(), `SELECT scope_type, entity_id, bank_code, account_number, currency FROM cimplrcorpsaas.rule_scope WHERE scope_id = $1`, body.ScopeID).Scan(&st, &entID, &bankCode, &acctNo, &currency)
		if err != nil {
			http.Error(w, pqUserFriendlyMessage(err), http.StatusBadRequest)
			return
		}
		var entPtr, bankPtr, acctPtr *string
		if entID.Valid {
			s := entID.String
			entPtr = &s
		}
		if bankCode.Valid {
			s := bankCode.String
			bankPtr = &s
		}
		if acctNo.Valid {
			s := acctNo.String
			acctPtr = &s
		}
		var curPtr *string
		if currency.Valid {
			s := currency.String
			curPtr = &s
		}
		if code, msg := validateScopeAccess(r.Context(), st, entPtr, bankPtr, acctPtr, curPtr); code != 0 {
			http.Error(w, msg, code)
			return
		}

		// Parse effective date if provided (accepts multiple formats)
		var eff *time.Time
		if body.EffectiveDate != nil && strings.TrimSpace(*body.EffectiveDate) != "" {
			t, err := parseDate(*body.EffectiveDate)
			if err != nil {
				http.Error(w, "invalid effective_date format", http.StatusBadRequest)
				return
			}
			eff = &t
		}

		var id int64
		err = pool.QueryRow(r.Context(), `INSERT INTO cimplrcorpsaas.category_rules (rule_name, category_id, scope_id, priority, is_active, effective_date) VALUES ($1, $2, $3, $4, $5, $6) RETURNING rule_id`, body.RuleName, body.CategoryID, body.ScopeID, body.Priority, isActive, eff).Scan(&id)
		if err != nil {
			if pgErr, ok := err.(*pgconn.PgError); ok {
				if strings.EqualFold(pgErr.ConstraintName, "uniq_rule_name_per_category") {
					http.Error(w, fmt.Sprintf("A rule named '%s' already exists for this category. Please choose a different rule name.", body.RuleName), http.StatusBadRequest)
					return
				}
			}
			http.Error(w, pqUserFriendlyMessage(err), http.StatusInternalServerError)
			return
		}
		apictx.RespondEnvelopeSuccess(w, "Success", map[string]interface{}{"rule_id": id})
	})
}

// --- Category Rule Component CRUD ---
func CreateCategoryRuleComponentHandler(pool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}

		var requestBody struct {
			CategoryRuleComponent
			Components []CategoryRuleComponent `json:"components,omitempty"`
		}

		if err := json.NewDecoder(r.Body).Decode(&requestBody); err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}
		ctx := r.Context()

		// If components array is provided, use bulk insert
		if len(requestBody.Components) > 0 {
			// Validate all components first
			for i, comp := range requestBody.Components {
				if comp.RuleID == 0 || comp.ComponentType == "" {
					http.Error(w, "Missing or invalid fields in component at index "+string(rune(i+'0')), http.StatusBadRequest)
					return
				}
				if comp.CurrencyCode != nil && strings.TrimSpace(*comp.CurrencyCode) != "" {
					if !ctxutil.FromContext(r.Context()).HasApprovedCurrency(*comp.CurrencyCode) {
						http.Error(w, constants.ErrCurrencyNotAllowed+" in component at index "+fmt.Sprint(i), http.StatusForbidden)
						return
					}
				}
			}

			tx, err := pool.Begin(ctx)
			if err != nil {
				http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
				return
			}
			defer func() {
				if p := recover(); p != nil {

					http.Error(w, constants.ErrInternalServer, http.StatusInternalServerError)
				}
			}()

			// Batch insert components to avoid extremely large single SQL statements
			const batchSize = 200
			var componentIDs []int64
			for start := 0; start < len(requestBody.Components); start += batchSize {
				end := start + batchSize
				if end > len(requestBody.Components) {
					end = len(requestBody.Components)
				}
				batch := requestBody.Components[start:end]
				placeholders := make([]string, 0, len(batch))
				args := make([]interface{}, 0, len(batch)*9)
				for i, comp := range batch {
					base := i * 9
					placeholders = append(placeholders, fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)", base+1, base+2, base+3, base+4, base+5, base+6, base+7, base+8, base+9))
					args = append(args, comp.RuleID, comp.ComponentType, comp.MatchType, comp.MatchValue, comp.AmountOperator, comp.AmountValue, comp.TxnFlow, comp.CurrencyCode, comp.IsActive)
				}
				query := "INSERT INTO cimplrcorpsaas.category_rule_components (rule_id, component_type, match_type, match_value, amount_operator, amount_value, txn_flow, currency_code, is_active) VALUES " + strings.Join(placeholders, ",") + " RETURNING component_id"
				rows, err := tx.Query(ctx, query, args...)
				if err != nil {

					if pgErr, ok := err.(*pgconn.PgError); ok {
						if strings.EqualFold(pgErr.ConstraintName, "uniq_active_components_per_rule") {
							http.Error(w, "One or more active components in the request duplicate an existing active component for the same rule. Please remove duplicates and try again.", http.StatusBadRequest)
							return
						}
					}
					http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
					return
				}
				for rows.Next() {
					var id int64
					if err := rows.Scan(&id); err != nil {
						rows.Close()

						http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
						return
					}
					componentIDs = append(componentIDs, id)
				}
				if err := rows.Err(); err != nil {
					rows.Close()

					http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
					return
				}
				rows.Close()
			}
			if err := tx.Commit(ctx); err != nil {
				http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
				return
			}
			apictx.RespondEnvelopeSuccess(w, "Success", map[string]interface{}{"component_ids": componentIDs})
			return
		}

		// Single component insert (backward compatible)
		if requestBody.RuleID == 0 || requestBody.ComponentType == "" {
			http.Error(w, "Missing or invalid fields", http.StatusBadRequest)
			return
		}
		if requestBody.CurrencyCode != nil && strings.TrimSpace(*requestBody.CurrencyCode) != "" {
			if !ctxutil.FromContext(r.Context()).HasApprovedCurrency(*requestBody.CurrencyCode) {
				http.Error(w, constants.ErrCurrencyNotAllowed+" in component", http.StatusForbidden)
				return
			}
		}
		var id int64
		err := pool.QueryRow(r.Context(), `INSERT INTO cimplrcorpsaas.category_rule_components (rule_id, component_type, match_type, match_value, amount_operator, amount_value, txn_flow, currency_code, is_active) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING component_id`, requestBody.RuleID, requestBody.ComponentType, requestBody.MatchType, requestBody.MatchValue, requestBody.AmountOperator, requestBody.AmountValue, requestBody.TxnFlow, requestBody.CurrencyCode, requestBody.IsActive).Scan(&id)
		if err != nil {
			if pgErr, ok := err.(*pgconn.PgError); ok {
				if strings.EqualFold(pgErr.ConstraintName, "uniq_active_components_per_rule") {
					http.Error(w, "An active component with the same parameters already exists for this rule. Please modify the component and try again.", http.StatusBadRequest)
					return
				}
			}
			http.Error(w, pqUserFriendlyMessage(err), http.StatusInternalServerError)
			return
		}
		apictx.RespondEnvelopeSuccess(w, "Success", map[string]interface{}{"component_id": id})
	})
}

// CreateCategoryRuleMasterHandler creates a rule scope, a category rule, and multiple
// components in a single transactional operation. If any step fails, the entire
// operation is rolled back and a friendly error message is returned.
func CreateCategoryRuleMasterHandler(pool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}

		var body struct {
			Scope RuleScope `json:"scope"`
			Rule  struct {
				RuleName      string  `json:"rule_name"`
				CategoryID    string  `json:"category_id"`
				Priority      int     `json:"priority"`
				IsActive      *bool   `json:"is_active"`
				EffectiveDate *string `json:"effective_date,omitempty"`
			} `json:"rule"`
			Components []CategoryRuleComponent `json:"components,omitempty"`
		}

		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}
		ctx := r.Context()

		// Basic validations
		if body.Rule.RuleName == "" || strings.TrimSpace(body.Rule.CategoryID) == "" {
			http.Error(w, "Missing required rule fields: rule_name or category_id", http.StatusBadRequest)
			return
		}
		if msg := validateCashCategoryAccess(ctx, body.Rule.CategoryID); msg != "" {
			http.Error(w, msg, http.StatusForbidden)
			return
		}

		// Validate scope access for the caller (same as CreateRuleScopeHandler)
		if code, msg := validateScopeAccess(r.Context(), body.Scope.ScopeType, body.Scope.EntityID, body.Scope.BankCode, body.Scope.AccountNumber, body.Scope.Currency); code != 0 {
			http.Error(w, msg, code)
			return
		}

		// Validate component currencies early
		for i, comp := range body.Components {
			if comp.CurrencyCode != nil && strings.TrimSpace(*comp.CurrencyCode) != "" {
				if !ctxutil.FromContext(r.Context()).HasApprovedCurrency(*comp.CurrencyCode) {
					http.Error(w, constants.ErrCurrencyNotAllowed+" in component at index "+fmt.Sprint(i), http.StatusForbidden)
					return
				}
			}
		}

		tx, err := pool.Begin(ctx)
		if err != nil {
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}
		defer func() {
			if p := recover(); p != nil {

				http.Error(w, constants.ErrInternalServer, http.StatusInternalServerError)
			}
		}()

		// 1) Insert scope
		var scopeID int64
		err = tx.QueryRow(r.Context(), `INSERT INTO cimplrcorpsaas.rule_scope (scope_type, entity_id, bank_code, account_number, currency) VALUES ($1,$2,$3,$4,$5) RETURNING scope_id`, body.Scope.ScopeType, body.Scope.EntityID, body.Scope.BankCode, body.Scope.AccountNumber, body.Scope.Currency).Scan(&scopeID)
		if err != nil {

			http.Error(w, pqUserFriendlyMessage(err), http.StatusInternalServerError)
			return
		}

		// 2) Insert rule
		isActive := true
		if body.Rule.IsActive != nil {
			isActive = *body.Rule.IsActive
		}
		var eff *time.Time
		if body.Rule.EffectiveDate != nil && strings.TrimSpace(*body.Rule.EffectiveDate) != "" {
			t, err := parseDate(*body.Rule.EffectiveDate)
			if err != nil {

				http.Error(w, "invalid effective_date format", http.StatusBadRequest)
				return
			}
			eff = &t
		}

		var ruleID int64
		err = tx.QueryRow(r.Context(), `INSERT INTO cimplrcorpsaas.category_rules (rule_name, category_id, scope_id, priority, is_active, effective_date) VALUES ($1, $2, $3, $4, $5, $6) RETURNING rule_id`, body.Rule.RuleName, body.Rule.CategoryID, scopeID, body.Rule.Priority, isActive, eff).Scan(&ruleID)
		if err != nil {

			if pgErr, ok := err.(*pgconn.PgError); ok {
				if strings.EqualFold(pgErr.ConstraintName, "uniq_rule_name_per_category") {
					http.Error(w, fmt.Sprintf("A rule named '%s' already exists for this category. Please choose a different rule name.", body.Rule.RuleName), http.StatusBadRequest)
					return
				}
			}
			http.Error(w, pqUserFriendlyMessage(err), http.StatusInternalServerError)
			return
		}

		// 3) Insert components (bulk if provided)
		var createdComponentIDs []int64
		if len(body.Components) > 0 {
			const batchSize = 200
			for start := 0; start < len(body.Components); start += batchSize {
				end := start + batchSize
				if end > len(body.Components) {
					end = len(body.Components)
				}
				batch := body.Components[start:end]
				placeholders := make([]string, 0, len(batch))
				args := make([]interface{}, 0, len(batch)*9)
				for i, comp := range batch {
					base := i * 9
					placeholders = append(placeholders, fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)", base+1, base+2, base+3, base+4, base+5, base+6, base+7, base+8, base+9))
					args = append(args, ruleID, comp.ComponentType, comp.MatchType, comp.MatchValue, comp.AmountOperator, comp.AmountValue, comp.TxnFlow, comp.CurrencyCode, comp.IsActive)
				}
				query := "INSERT INTO cimplrcorpsaas.category_rule_components (rule_id, component_type, match_type, match_value, amount_operator, amount_value, txn_flow, currency_code, is_active) VALUES " + strings.Join(placeholders, ",") + " RETURNING component_id"
				rows, err := tx.Query(r.Context(), query, args...)
				if err != nil {

					if pgErr, ok := err.(*pgconn.PgError); ok {
						if strings.EqualFold(pgErr.ConstraintName, "uniq_active_components_per_rule") {
							http.Error(w, "One or more active components duplicate an existing active component for this rule. Please remove duplicates and try again.", http.StatusBadRequest)
							return
						}
					}
					http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
					return
				}
				for rows.Next() {
					var cid int64
					if err := rows.Scan(&cid); err != nil {
						rows.Close()

						http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
						return
					}
					createdComponentIDs = append(createdComponentIDs, cid)
				}
				if err := rows.Err(); err != nil {
					rows.Close()

					http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
					return
				}
				rows.Close()
			}
		}

		if err := tx.Commit(ctx); err != nil {
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}

		apictx.RespondEnvelopeSuccess(w, "Success", map[string]interface{}{
			"scope_id":      scopeID,
			"rule_id":       ruleID,
			"component_ids": createdComponentIDs,
		})
	})
}

// DeleteTransactionCategoryHandler deletes a category and cascades deletes for rules, rule scopes, and rule components
func DeleteTransactionCategoryHandler(pool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			CategoryID string `json:"category_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || strings.TrimSpace(body.CategoryID) == "" {
			http.Error(w, "Missing or invalid category_id", http.StatusBadRequest)
			return
		}

		ctx := r.Context()
		if msg := validateCashCategoryAccess(ctx, body.CategoryID); msg != "" {
			http.Error(w, msg, http.StatusForbidden)
			return
		}
		tx, err := pool.Begin(ctx)
		if err != nil {
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}
		defer func() {
			if p := recover(); p != nil {

				http.Error(w, constants.ErrInternalServer, http.StatusInternalServerError)
			}
		}()

		// 1. Get all rules for this category
		ruleRows, err := tx.Query(r.Context(), `SELECT rule_id, scope_id FROM cimplrcorpsaas.category_rules WHERE category_id = $1`, body.CategoryID)
		if err != nil {

			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}
		var ruleIDs []int64
		var scopeIDs []int64
		for ruleRows.Next() {
			var ruleID, scopeID int64
			if err := ruleRows.Scan(&ruleID, &scopeID); err == nil {
				ruleIDs = append(ruleIDs, ruleID)
				scopeIDs = append(scopeIDs, scopeID)
			}
		}
		ruleRows.Close()

		// 2. Delete all rule components for these rules
		if len(ruleIDs) > 0 {
			_, err = tx.Exec(r.Context(), `DELETE FROM cimplrcorpsaas.category_rule_components WHERE rule_id = ANY($1)`, ruleIDs)
			if err != nil {
				if isFKViolation(err) {

					http.Error(w, constants.ErrBankStatementAlreadyExists, http.StatusBadRequest)
					return
				}

				http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
				return
			}
		}

		// 3. Delete all rules for this category
		_, err = tx.Exec(r.Context(), `DELETE FROM cimplrcorpsaas.category_rules WHERE category_id = $1`, body.CategoryID)
		if err != nil {
			if isFKViolation(err) {

				http.Error(w, constants.ErrBankStatementAlreadyExists, http.StatusBadRequest)
				return
			}

			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}

		// 4. Delete all rule scopes for these rules (if not used elsewhere)
		for _, scopeID := range scopeIDs {
			var count int
			err = tx.QueryRow(r.Context(), `SELECT COUNT(*) FROM cimplrcorpsaas.category_rules WHERE scope_id = $1`, scopeID).Scan(&count)
			if err == nil && count == 0 {
				_, _ = tx.Exec(r.Context(), `DELETE FROM cimplrcorpsaas.rule_scope WHERE scope_id = $1`, scopeID)
			}
		}

		// 5. Delete the category itself
		_, err = tx.Exec(r.Context(), `DELETE FROM public.mastercashflowcategory WHERE category_id = $1`, body.CategoryID)
		if err != nil {
			if isFKViolation(err) {

				http.Error(w, constants.ErrBankStatementAlreadyExists, http.StatusBadRequest)
				return
			}

			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}

		apictx.RespondEnvelopeSuccess(w, "Success", nil)
	})
}
