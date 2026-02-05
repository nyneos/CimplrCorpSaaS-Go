package bankstatement

import (
	apictx "CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	middlewares "CimplrCorpSaas/api/middlewares"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/lib/pq"
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

// ruleQueryerLocal abstracts QueryContext for both *sql.DB and *sql.Tx within this file.
type ruleQueryerLocal interface {
	QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error)
}

func isFKViolation(err error) bool {
	if err == nil {
		return false
	}
	if pqErr, ok := err.(*pq.Error); ok && pqErr.Code == "23503" {
		return true
	}
	return false
}

func writeFKConflict(w http.ResponseWriter) {
	w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": false,
		"message": "Bank Statement Transactions with this category exists in the system. Please delete them first",
	})
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
		ids := apictx.GetEntityIDsFromCtx(ctx)
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
		if !ctxHasApprovedBankAccount(ctx, *accountNumber) {
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
		if !ctxHasApprovedCurrency(ctx, *currency) {
			return http.StatusForbidden, constants.ErrCurrencyNotAllowed
		}
		return 0, ""
	default:
		return http.StatusBadRequest, "Invalid scope_type"
	}
}

// loadCategoryRuleComponentsLocal mirrors the rule loader used during upload/recompute without depending on the upload file.
func loadCategoryRuleComponentsLocal(ctx context.Context, db ruleQueryerLocal, accountNumber, entityID string, accountCurrency *string) ([]categoryRuleComponent, error) {
	const q = `
	   SELECT r.rule_id, r.priority, r.category_id, c.category_name, c.category_type, comp.component_type, comp.match_type, comp.match_value, comp.amount_operator, comp.amount_value, comp.txn_flow, comp.currency_code
	FROM cimplrcorpsaas.category_rules r
	JOIN public.mastercashflowcategory c ON r.category_id = c.category_id
	   JOIN cimplrcorpsaas.category_rule_components comp ON r.rule_id = comp.rule_id AND comp.is_active = true
	   JOIN cimplrcorpsaas.rule_scope s ON r.scope_id = s.scope_id
	   WHERE r.is_active = true
	 AND (
		   (s.scope_type = 'ACCOUNT' AND s.account_number = $1)
		   OR (s.scope_type = 'ENTITY' AND s.entity_id = $2)
		   OR (s.scope_type = 'BANK' AND s.bank_code IS NOT NULL)
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

	rows, err := db.QueryContext(ctx, q, accountNumber, entityID, acctCurrencyParam)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var rules []categoryRuleComponent
	for rows.Next() {
		var rc categoryRuleComponent
		if err := rows.Scan(&rc.RuleID, &rc.Priority, &rc.CategoryID, &rc.CategoryName, &rc.CategoryType, &rc.ComponentType, &rc.MatchType, &rc.MatchValue, &rc.AmountOperator, &rc.AmountValue, &rc.TxnFlow, &rc.CurrencyCode); err != nil {
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
func ListCategoriesForUserHandler(db *sql.DB) http.Handler {
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

		rows, err := db.Query(`SELECT category_id, category_name FROM public.mastercashflowcategory ORDER BY category_name`)
		if err != nil {
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		var out []TransactionCategory
		for rows.Next() {
			var c TransactionCategory
			if err := rows.Scan(&c.CategoryID, &c.CategoryName); err == nil {
				out = append(out, c)
			}
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"data":    out,
		})
	})
}

// MapTransactionsToCategoryHandler assigns a category to transactions and raises pending edit approval per bank statement.
func MapTransactionsToCategoryHandler(db *sql.DB) http.Handler {
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
		entityIDs := apictx.GetEntityIDsFromCtx(ctx)
		if len(entityIDs) == 0 {
			http.Error(w, constants.ErrNoAccessibleEntitiesForRequest, http.StatusForbidden)
			return
		}

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}
		defer func() {
			if p := recover(); p != nil {
				tx.Rollback()
				http.Error(w, constants.ErrInternalServer, http.StatusInternalServerError)
			}
		}()

		// Ensure all provided transaction IDs are within user's accessible entity/account scope
		scopeRows, err := tx.QueryContext(ctx, `
	SELECT DISTINCT bs.entity_id, bs.account_number, m.currency
	FROM cimplrcorpsaas.bank_statement_transactions t
	JOIN cimplrcorpsaas.bank_statements bs ON t.bank_statement_id = bs.bank_statement_id
	LEFT JOIN public.masterbankaccount m ON bs.account_number = m.account_number
	WHERE t.transaction_id = ANY($1) AND t.bank_statement_id IS NOT NULL
	`, pq.Array(body.TransactionIDs))
		if err != nil {
			tx.Rollback()
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
			if !ctxHasApprovedBankAccount(ctx, a) {
				unauthorizedAccount = true
				break
			}

			// Enforce currency: ensure account's currency is approved in context (use currency fetched in query)
			if acctCurrency.Valid && strings.TrimSpace(acctCurrency.String) != "" {
				if !ctxHasApprovedCurrency(ctx, acctCurrency.String) {
					unauthorizedCurrency = true
					break
				}
			}
		}
		scopeRows.Close()
		if unauthorizedEntity {
			tx.Rollback()
			http.Error(w, constants.ErrUnauthorizedEntity, http.StatusForbidden)
			return
		}
		if unauthorizedAccount {
			tx.Rollback()
			http.Error(w, constants.ErrInvalidAccount, http.StatusForbidden)
			return
		}
		if unauthorizedCurrency {
			tx.Rollback()
			http.Error(w, constants.ErrCurrencyNotAllowed, http.StatusForbidden)
			return
		}

		// Update category for given transactions
		if _, err := tx.Exec(`UPDATE cimplrcorpsaas.bank_statement_transactions SET category_id = $1 WHERE transaction_id = ANY($2)`, body.CategoryID, pq.Array(body.TransactionIDs)); err != nil {
			tx.Rollback()
			http.Error(w, pqUserFriendlyMessage(err), http.StatusInternalServerError)
			return
		}

		// Collect affected bank_statement_ids
		bsRows, err := tx.Query(`SELECT DISTINCT bank_statement_id FROM cimplrcorpsaas.bank_statement_transactions WHERE transaction_id = ANY($1) AND bank_statement_id IS NOT NULL`, pq.Array(body.TransactionIDs))
		if err != nil {
			tx.Rollback()
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}
		var bsIDs []string
		for bsRows.Next() {
			var id string
			if err := bsRows.Scan(&id); err == nil {
				bsIDs = append(bsIDs, id)
			}
		}
		bsRows.Close()

		// Insert pending edit approval for each affected statement
		for _, bsID := range bsIDs {
			_, err = tx.ExecContext(ctx, `INSERT INTO cimplrcorpsaas.auditactionbankstatement (bankstatementid, actiontype, processing_status, requested_by, requested_at) VALUES ($1, 'EDIT', 'PENDING_EDIT_APPROVAL', $2, $3)`, bsID, requestedByFromCtx(ctx, body.UserID), time.Now())
			if err != nil {
				tx.Rollback()
				http.Error(w, pqUserFriendlyMessage(err), http.StatusInternalServerError)
				return
			}
		}

		if err := tx.Commit(); err != nil {
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"message": "Transactions mapped and approval requested",
		})
	})
}

// CategorizeUncategorizedTransactionsHandler assigns the given category to all transactions with NULL category
// and raises pending edit approval for their bank statements.
func CategorizeUncategorizedTransactionsHandler(db *sql.DB) http.Handler {
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
		entityIDs := apictx.GetEntityIDsFromCtx(ctx)
		if len(entityIDs) == 0 {
			http.Error(w, constants.ErrNoAccessibleEntitiesForRequest, http.StatusForbidden)
			return
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}
		defer func() {
			if p := recover(); p != nil {
				tx.Rollback()
				http.Error(w, constants.ErrInternalServer, http.StatusInternalServerError)
			}
		}()

		rows, err := tx.QueryContext(ctx, `
SELECT t.transaction_id,
			 t.bank_statement_id,
			 bs.account_number,
			 bs.entity_id,
			 COALESCE(t.description, ''),
			 t.withdrawal_amount,
			 t.deposit_amount,
			 m.currency
FROM cimplrcorpsaas.bank_statement_transactions t
JOIN cimplrcorpsaas.bank_statements bs ON t.bank_statement_id = bs.bank_statement_id
LEFT JOIN public.masterbankaccount m ON bs.account_number = m.account_number
WHERE t.category_id IS NULL
	AND t.bank_statement_id IS NOT NULL
	AND bs.entity_id = ANY($1);
`, pq.Array(entityIDs))
		if err != nil {
			tx.Rollback()
			http.Error(w, pqUserFriendlyMessage(err), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		type txnRow struct {
			id       int64
			bsID     string
			acct     string
			entity   string
			desc     string
			wd       sql.NullFloat64
			dep      sql.NullFloat64
			currency sql.NullString
		}

		var txns []txnRow
		for rows.Next() {
			var tr txnRow
			if err := rows.Scan(&tr.id, &tr.bsID, &tr.acct, &tr.entity, &tr.desc, &tr.wd, &tr.dep, &tr.currency); err == nil {
				// Skip transactions whose account currency is not approved (enforce currency after account)
				if tr.currency.Valid && strings.TrimSpace(tr.currency.String) != "" {
					if !ctxHasApprovedCurrency(ctx, tr.currency.String) {
						// skip this txn
						continue
					}
				}
				txns = append(txns, tr)
			}
		}
		if err := rows.Err(); err != nil {
			tx.Rollback()
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}

		ruleCache := make(map[string][]categoryRuleComponent)
		bsSet := make(map[string]struct{})
		matchedByCategory := make(map[string][]int64)

		for _, tr := range txns {
			if !ctxHasApprovedBankAccount(ctx, tr.acct) {
				continue
			}
			// Skip transactions whose account currency is not approved (we already fetched currency)
			if tr.currency.Valid && strings.TrimSpace(tr.currency.String) != "" {
				if !ctxHasApprovedCurrency(ctx, tr.currency.String) {
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
				rules, err = loadCategoryRuleComponentsLocal(ctx, db, tr.acct, tr.entity, acctCurPtr)
				if err != nil {
					continue
				}
				ruleCache[cacheKey] = rules
			}

			matched := matchCategoryForTransaction(rules, tr.desc, tr.wd, tr.dep)
			if matched.Valid && matched.String == body.CategoryID {
				matchedByCategory[matched.String] = append(matchedByCategory[matched.String], tr.id)
				bsSet[tr.bsID] = struct{}{}
			}
		}

		updated := 0
		for catID, txnIDs := range matchedByCategory {
			if len(txnIDs) == 0 {
				continue
			}
			if _, err := tx.ExecContext(ctx, `UPDATE cimplrcorpsaas.bank_statement_transactions SET category_id = $1 WHERE transaction_id = ANY($2)`, catID, pq.Array(txnIDs)); err != nil {
				tx.Rollback()
				http.Error(w, pqUserFriendlyMessage(err), http.StatusInternalServerError)
				return
			}
			updated += len(txnIDs)
		}

		for bsID := range bsSet {
			_, err = tx.ExecContext(ctx, `INSERT INTO cimplrcorpsaas.auditactionbankstatement (bankstatementid, actiontype, processing_status, requested_by, requested_at) VALUES ($1, 'EDIT', 'PENDING_EDIT_APPROVAL', $2, $3)`, bsID, requestedByFromCtx(ctx, body.UserID), time.Now())
			if err != nil {
				tx.Rollback()
				http.Error(w, pqUserFriendlyMessage(err), http.StatusInternalServerError)
				return
			}
		}

		if err := tx.Commit(); err != nil {
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":                  true,
			"updated_transactions":     updated,
			"affected_bank_statements": len(bsSet),
		})
	})
}

// RecomputeUncategorizedTransactionsHandler applies rules to uncategorized transactions and raises pending edit approvals.
func RecomputeUncategorizedTransactionsHandler(db *sql.DB) http.Handler {
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
		entityIDs := apictx.GetEntityIDsFromCtx(ctx)
		if len(entityIDs) == 0 {
			http.Error(w, constants.ErrNoAccessibleEntitiesForRequest, http.StatusForbidden)
			return
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}
		defer func() {
			if p := recover(); p != nil {
				tx.Rollback()
				http.Error(w, constants.ErrInternalServer, http.StatusInternalServerError)
			}
		}()

		rows, err := tx.QueryContext(ctx, `
SELECT t.transaction_id,
			 t.bank_statement_id,
			 bs.account_number,
			 bs.entity_id,
			 COALESCE(t.description, ''),
			 t.withdrawal_amount,
			 t.deposit_amount,
			 m.currency
FROM cimplrcorpsaas.bank_statement_transactions t
JOIN cimplrcorpsaas.bank_statements bs ON t.bank_statement_id = bs.bank_statement_id
LEFT JOIN public.masterbankaccount m ON bs.account_number = m.account_number
WHERE t.category_id IS NULL
	AND t.bank_statement_id IS NOT NULL
	AND bs.entity_id = ANY($1);
`, pq.Array(entityIDs))
		if err != nil {
			tx.Rollback()
			http.Error(w, pqUserFriendlyMessage(err), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		type txnRow struct {
			id       int64
			bsID     string
			acct     string
			entity   string
			desc     string
			wd       sql.NullFloat64
			dep      sql.NullFloat64
			currency sql.NullString
		}

		var txns []txnRow
		for rows.Next() {
			var tr txnRow
			if err := rows.Scan(&tr.id, &tr.bsID, &tr.acct, &tr.entity, &tr.desc, &tr.wd, &tr.dep, &tr.currency); err == nil {
				txns = append(txns, tr)
			}
		}
		if err := rows.Err(); err != nil {
			tx.Rollback()
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}

		ruleCache := make(map[string][]categoryRuleComponent)
		bsSet := make(map[string]struct{})
		updated := 0

		for _, tr := range txns {
			if !ctxHasApprovedBankAccount(ctx, tr.acct) {
				continue
			}
			// skip if account currency not allowed
			if tr.currency.Valid && strings.TrimSpace(tr.currency.String) != "" {
				if !ctxHasApprovedCurrency(ctx, tr.currency.String) {
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
				rules, err = loadCategoryRuleComponentsLocal(ctx, db, tr.acct, tr.entity, acctCurPtr)
				if err != nil {
					continue
				}
				ruleCache[cacheKey] = rules
			}

			matched := matchCategoryForTransaction(rules, tr.desc, tr.wd, tr.dep)
			if matched.Valid {
				if _, err := tx.ExecContext(ctx, `UPDATE cimplrcorpsaas.bank_statement_transactions SET category_id = $1 WHERE transaction_id = $2`, matched.String, tr.id); err == nil {
					updated++
					bsSet[tr.bsID] = struct{}{}
				}
			}
		}

		for bsID := range bsSet {
			_, err = tx.ExecContext(ctx, `INSERT INTO cimplrcorpsaas.auditactionbankstatement (bankstatementid, actiontype, processing_status, requested_by, requested_at) VALUES ($1, 'EDIT', 'PENDING_EDIT_APPROVAL', $2, $3)`, bsID, requestedByFromCtx(ctx, body.UserID), time.Now())
			if err != nil {
				tx.Rollback()
				http.Error(w, pqUserFriendlyMessage(err), http.StatusInternalServerError)
				return
			}
		}

		if err := tx.Commit(); err != nil {
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":                  true,
			"updated_transactions":     updated,
			"affected_bank_statements": len(bsSet),
		})
	})
}

// DeleteMultipleTransactionCategoriesHandler deletes multiple categories and cascades deletes for rules, rule scopes, and rule components
func DeleteMultipleTransactionCategoriesHandler(db *sql.DB) http.Handler {
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

		tx, err := db.Begin()
		if err != nil {
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}
		defer func() {
			if p := recover(); p != nil {
				tx.Rollback()
				http.Error(w, constants.ErrInternalServer, http.StatusInternalServerError)
			}
		}()

		// 1. Get all rules and scope_ids for these categories
		ruleRows, err := tx.Query(`SELECT rule_id, scope_id FROM cimplrcorpsaas.category_rules WHERE category_id = ANY($1)`, pq.Array(body.CategoryIDs))
		if err != nil {
			tx.Rollback()
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
			_, err = tx.Exec(`DELETE FROM cimplrcorpsaas.category_rule_components WHERE rule_id = ANY($1)`, pq.Array(ruleIDs))
			if err != nil {
				if isFKViolation(err) {
					tx.Rollback()
					writeFKConflict(w)
					return
				}
				tx.Rollback()
				http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
				return
			}
		}

		// 3. Delete all rules for these categories
		_, err = tx.Exec(`DELETE FROM cimplrcorpsaas.category_rules WHERE category_id = ANY($1)`, pq.Array(body.CategoryIDs))
		if err != nil {
			if isFKViolation(err) {
				tx.Rollback()
				writeFKConflict(w)
				return
			}
			tx.Rollback()
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}

		// 4. Delete all rule scopes for these rules (if not used elsewhere)
		for _, scopeID := range scopeIDs {
			var count int
			err = tx.QueryRow(`SELECT COUNT(*) FROM cimplrcorpsaas.category_rules WHERE scope_id = $1`, scopeID).Scan(&count)
			if err == nil && count == 0 {
				_, _ = tx.Exec(`DELETE FROM cimplrcorpsaas.rule_scope WHERE scope_id = $1`, scopeID)
			}
		}

		// 5. Delete the categories themselves
		_, err = tx.Exec(`DELETE FROM public.mastercashflowcategory WHERE category_id = ANY($1)`, pq.Array(body.CategoryIDs))
		if err != nil {
			if isFKViolation(err) {
				tx.Rollback()
				writeFKConflict(w)
				return
			}
			tx.Rollback()
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}

		if err := tx.Commit(); err != nil {
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"message": "Categories deleted successfully",
		})
	})
}

// --- Category CRUD ---
func CreateTransactionCategoryHandler(db *sql.DB) http.Handler {
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
		err := db.QueryRow(`INSERT INTO public.mastercashflowcategory (category_name, category_type, description) VALUES ($1, $2, $3) RETURNING category_id`, body.CategoryName, body.CategoryType, body.Description).Scan(&id)
		if err != nil {
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"data":    map[string]interface{}{"category_id": id},
		})
	})
}

func ListTransactionCategoriesHandler(db *sql.DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		catRows, err := db.Query(`SELECT category_id, category_name, category_type, description FROM public.mastercashflowcategory`)
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
			catIndex[c.CategoryID] = len(categories)
			catIDs = append(catIDs, c.CategoryID)
			categories = append(categories, CategoryWithRules{TransactionCategory: c})
		}

		// Early return if no categories
		if len(categories) == 0 {
			w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": true,
				"data":    categories,
			})
			return
		}

		// Fetch all rules for these categories in one query (include effective_date)
		ruleRows, err := db.Query(`SELECT rule_id, rule_name, category_id, scope_id, priority, is_active, created_at, effective_date FROM cimplrcorpsaas.category_rules WHERE category_id = ANY($1)`, pq.Array(catIDs))
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
			scopeRows, err := db.Query(`SELECT scope_id, scope_type, entity_id, bank_code, account_number, currency FROM cimplrcorpsaas.rule_scope WHERE scope_id = ANY($1)`, pq.Array(scopeIDs))
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
			compRows, err := db.Query(`SELECT component_id, rule_id, component_type, match_type, match_value, amount_operator, amount_value, txn_flow, currency_code, is_active FROM cimplrcorpsaas.category_rule_components WHERE rule_id = ANY($1)`, pq.Array(ruleIDs))
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

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"data":    categories,
		})
	})
}

// --- Rule Scope CRUD ---
func CreateRuleScopeHandler(db *sql.DB) http.Handler {
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
		err := db.QueryRow(`INSERT INTO cimplrcorpsaas.rule_scope (scope_type, entity_id, bank_code, account_number, currency) VALUES ($1, $2, $3, $4, $5) RETURNING scope_id`, body.ScopeType, body.EntityID, body.BankCode, body.AccountNumber, body.Currency).Scan(&id)
		if err != nil {
			http.Error(w, pqUserFriendlyMessage(err), http.StatusInternalServerError)
			return
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"data":    map[string]interface{}{"scope_id": id},
		})
	})
}

// --- Category Rule CRUD ---
func CreateCategoryRuleHandler(db *sql.DB) http.Handler {
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
		isActive := true
		if body.IsActive != nil {
			isActive = *body.IsActive
		}

		// Validate scope belongs to caller's entity/bank/account context
		var st string
		var entID, bankCode, acctNo sql.NullString
		var currency sql.NullString
		err := db.QueryRowContext(r.Context(), `SELECT scope_type, entity_id, bank_code, account_number, currency FROM cimplrcorpsaas.rule_scope WHERE scope_id = $1`, body.ScopeID).Scan(&st, &entID, &bankCode, &acctNo, &currency)
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
		err = db.QueryRow(`INSERT INTO cimplrcorpsaas.category_rules (rule_name, category_id, scope_id, priority, is_active, effective_date) VALUES ($1, $2, $3, $4, $5, $6) RETURNING rule_id`, body.RuleName, body.CategoryID, body.ScopeID, body.Priority, isActive, eff).Scan(&id)
		if err != nil {
			http.Error(w, pqUserFriendlyMessage(err), http.StatusInternalServerError)
			return
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"data":    map[string]interface{}{"rule_id": id},
		})
	})
}

// --- Category Rule Component CRUD ---
func CreateCategoryRuleComponentHandler(db *sql.DB) http.Handler {
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

		// If components array is provided, use bulk insert
		if len(requestBody.Components) > 0 {
			// Validate all components first
			for i, comp := range requestBody.Components {
				if comp.RuleID == 0 || comp.ComponentType == "" {
					http.Error(w, "Missing or invalid fields in component at index "+string(rune(i+'0')), http.StatusBadRequest)
					return
				}
				if comp.CurrencyCode != nil && strings.TrimSpace(*comp.CurrencyCode) != "" {
					if !ctxHasApprovedCurrency(r.Context(), *comp.CurrencyCode) {
						http.Error(w, constants.ErrCurrencyNotAllowed+" in component at index "+fmt.Sprint(i), http.StatusForbidden)
						return
					}
				}
			}

			tx, err := db.Begin()
			if err != nil {
				http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
				return
			}
			defer func() {
				if p := recover(); p != nil {
					tx.Rollback()
					http.Error(w, constants.ErrInternalServer, http.StatusInternalServerError)
				}
			}()

			// Build multi-row INSERT with RETURNING component_id
			placeholders := make([]string, 0, len(requestBody.Components))
			args := make([]interface{}, 0, len(requestBody.Components)*9)
			for i, comp := range requestBody.Components {
				base := i * 9
				placeholders = append(placeholders, fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)", base+1, base+2, base+3, base+4, base+5, base+6, base+7, base+8, base+9))
				args = append(args, comp.RuleID, comp.ComponentType, comp.MatchType, comp.MatchValue, comp.AmountOperator, comp.AmountValue, comp.TxnFlow, comp.CurrencyCode, comp.IsActive)
			}
			query := "INSERT INTO cimplrcorpsaas.category_rule_components (rule_id, component_type, match_type, match_value, amount_operator, amount_value, txn_flow, currency_code, is_active) VALUES " + strings.Join(placeholders, ",") + " RETURNING component_id"
			rows, err := tx.Query(query, args...)
			if err != nil {
				tx.Rollback()
				http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
				return
			}
			var componentIDs []int64
			for rows.Next() {
				var id int64
				if err := rows.Scan(&id); err != nil {
					rows.Close()
					tx.Rollback()
					http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
					return
				}
				componentIDs = append(componentIDs, id)
			}
			if err := rows.Err(); err != nil {
				rows.Close()
				tx.Rollback()
				http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
				return
			}
			rows.Close()
			if err := tx.Commit(); err != nil {
				http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": true,
				"data":    map[string]interface{}{"component_ids": componentIDs},
			})
			return
		}

		// Single component insert (backward compatible)
		if requestBody.RuleID == 0 || requestBody.ComponentType == "" {
			http.Error(w, "Missing or invalid fields", http.StatusBadRequest)
			return
		}
		if requestBody.CurrencyCode != nil && strings.TrimSpace(*requestBody.CurrencyCode) != "" {
			if !ctxHasApprovedCurrency(r.Context(), *requestBody.CurrencyCode) {
				http.Error(w, constants.ErrCurrencyNotAllowed+" in component", http.StatusForbidden)
				return
			}
		}
		var id int64
		err := db.QueryRow(`INSERT INTO cimplrcorpsaas.category_rule_components (rule_id, component_type, match_type, match_value, amount_operator, amount_value, txn_flow, currency_code, is_active) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING component_id`, requestBody.RuleID, requestBody.ComponentType, requestBody.MatchType, requestBody.MatchValue, requestBody.AmountOperator, requestBody.AmountValue, requestBody.TxnFlow, requestBody.CurrencyCode, requestBody.IsActive).Scan(&id)
		if err != nil {
			http.Error(w, pqUserFriendlyMessage(err), http.StatusInternalServerError)
			return
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"data":    map[string]interface{}{"component_id": id},
		})
	})
}

// DeleteTransactionCategoryHandler deletes a category and cascades deletes for rules, rule scopes, and rule components
func DeleteTransactionCategoryHandler(db *sql.DB) http.Handler {
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

		tx, err := db.Begin()
		if err != nil {
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}
		defer func() {
			if p := recover(); p != nil {
				tx.Rollback()
				http.Error(w, constants.ErrInternalServer, http.StatusInternalServerError)
			}
		}()

		// 1. Get all rules for this category
		ruleRows, err := tx.Query(`SELECT rule_id, scope_id FROM cimplrcorpsaas.category_rules WHERE category_id = $1`, body.CategoryID)
		if err != nil {
			tx.Rollback()
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
			_, err = tx.Exec(`DELETE FROM cimplrcorpsaas.category_rule_components WHERE rule_id = ANY($1)`, pq.Array(ruleIDs))
			if err != nil {
				if isFKViolation(err) {
					tx.Rollback()
					http.Error(w, constants.ErrBankStatementAlreadyExists, http.StatusBadRequest)
					return
				}
				tx.Rollback()
				http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
				return
			}
		}

		// 3. Delete all rules for this category
		_, err = tx.Exec(`DELETE FROM cimplrcorpsaas.category_rules WHERE category_id = $1`, body.CategoryID)
		if err != nil {
			if isFKViolation(err) {
				tx.Rollback()
				http.Error(w, constants.ErrBankStatementAlreadyExists, http.StatusBadRequest)
				return
			}
			tx.Rollback()
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}

		// 4. Delete all rule scopes for these rules (if not used elsewhere)
		for _, scopeID := range scopeIDs {
			var count int
			err = tx.QueryRow(`SELECT COUNT(*) FROM cimplrcorpsaas.category_rules WHERE scope_id = $1`, scopeID).Scan(&count)
			if err == nil && count == 0 {
				_, _ = tx.Exec(`DELETE FROM cimplrcorpsaas.rule_scope WHERE scope_id = $1`, scopeID)
			}
		}

		// 5. Delete the category itself
		_, err = tx.Exec(`DELETE FROM public.mastercashflowcategory WHERE category_id = $1`, body.CategoryID)
		if err != nil {
			if isFKViolation(err) {
				tx.Rollback()
				http.Error(w, constants.ErrBankStatementAlreadyExists, http.StatusBadRequest)
				return
			}
			tx.Rollback()
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}

		if err := tx.Commit(); err != nil {
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
		})
	})
}
