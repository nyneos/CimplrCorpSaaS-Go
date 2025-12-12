package bankstatement

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"time"
)

// TransactionCategory represents a category master
type TransactionCategory struct {
	CategoryID   int64  `json:"category_id"`
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
}

// CategoryRule represents a category rule
type CategoryRule struct {
	RuleID     int64     `json:"rule_id"`
	RuleName   string    `json:"rule_name"`
	CategoryID int64     `json:"category_id"`
	ScopeID    int64     `json:"scope_id"`
	Priority   int       `json:"priority"`
	IsActive   bool      `json:"is_active"`
	CreatedAt  time.Time `json:"created_at"`
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

// --- Category CRUD ---
func CreateTransactionCategoryHandler(db *sql.DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
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
		var id int64
		err := db.QueryRow(`INSERT INTO cimplrcorpsaas.transaction_categories (category_name, category_type, description) VALUES ($1, $2, $3) RETURNING category_id`, body.CategoryName, body.CategoryType, body.Description).Scan(&id)
		if err != nil {
			http.Error(w, "DB error: "+err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"data":    map[string]interface{}{"category_id": id},
		})
	})
}

func ListTransactionCategoriesHandler(db *sql.DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Fetch all categories
		catRows, err := db.Query(`SELECT category_id, category_name, category_type, description FROM cimplrcorpsaas.transaction_categories`)
		if err != nil {
			http.Error(w, "DB error: "+err.Error(), http.StatusInternalServerError)
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

		for catRows.Next() {
			var c TransactionCategory
			if err := catRows.Scan(&c.CategoryID, &c.CategoryName, &c.CategoryType, &c.Description); err != nil {
				continue
			}

			// Fetch rules for this category
			ruleRows, err := db.Query(`SELECT rule_id, rule_name, category_id, scope_id, priority, is_active, created_at FROM cimplrcorpsaas.category_rules WHERE category_id = $1`, c.CategoryID)
			if err != nil {
				categories = append(categories, CategoryWithRules{TransactionCategory: c})
				continue
			}
			var rules []RuleWithDetails
			for ruleRows.Next() {
				var rule CategoryRule
				if err := ruleRows.Scan(&rule.RuleID, &rule.RuleName, &rule.CategoryID, &rule.ScopeID, &rule.Priority, &rule.IsActive, &rule.CreatedAt); err != nil {
					continue
				}

				// Fetch scope for this rule
				var scope RuleScope
				errScope := db.QueryRow(`SELECT scope_id, scope_type, entity_id, bank_code, account_number FROM cimplrcorpsaas.rule_scope WHERE scope_id = $1`, rule.ScopeID).Scan(&scope.ScopeID, &scope.ScopeType, &scope.EntityID, &scope.BankCode, &scope.AccountNumber)
				var scopePtr *RuleScope
				if errScope == nil {
					scopePtr = &scope
				}

				// Fetch components for this rule
				compRows, err := db.Query(`SELECT component_id, rule_id, component_type, match_type, match_value, amount_operator, amount_value, txn_flow, currency_code, is_active FROM cimplrcorpsaas.category_rule_components WHERE rule_id = $1`, rule.RuleID)
				var components []CategoryRuleComponent
				if err == nil {
					for compRows.Next() {
						var comp CategoryRuleComponent
						if err := compRows.Scan(&comp.ComponentID, &comp.RuleID, &comp.ComponentType, &comp.MatchType, &comp.MatchValue, &comp.AmountOperator, &comp.AmountValue, &comp.TxnFlow, &comp.CurrencyCode, &comp.IsActive); err != nil {
							continue
						}
						components = append(components, comp)
					}
					compRows.Close()
				}

				rules = append(rules, RuleWithDetails{
					CategoryRule: rule,
					Scope:        scopePtr,
					Components:   components,
				})
			}
			ruleRows.Close()

			categories = append(categories, CategoryWithRules{
				TransactionCategory: c,
				Rules:               rules,
			})
		}

		w.Header().Set("Content-Type", "application/json")
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
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var body RuleScope
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.ScopeType == "" {
			http.Error(w, "Missing or invalid scope_type", http.StatusBadRequest)
			return
		}
		var id int64
		err := db.QueryRow(`INSERT INTO cimplrcorpsaas.rule_scope (scope_type, entity_id, bank_code, account_number) VALUES ($1, $2, $3, $4) RETURNING scope_id`, body.ScopeType, body.EntityID, body.BankCode, body.AccountNumber).Scan(&id)
		if err != nil {
			http.Error(w, "DB error: "+err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
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
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			RuleName   string `json:"rule_name"`
			CategoryID int64  `json:"category_id"`
			ScopeID    int64  `json:"scope_id"`
			Priority   int    `json:"priority"`
			IsActive   *bool  `json:"is_active"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.RuleName == "" || body.CategoryID == 0 || body.ScopeID == 0 {
			http.Error(w, "Missing or invalid fields", http.StatusBadRequest)
			return
		}
		isActive := true
		if body.IsActive != nil {
			isActive = *body.IsActive
		}
		var id int64
		err := db.QueryRow(`INSERT INTO cimplrcorpsaas.category_rules (rule_name, category_id, scope_id, priority, is_active) VALUES ($1, $2, $3, $4, $5) RETURNING rule_id`, body.RuleName, body.CategoryID, body.ScopeID, body.Priority, isActive).Scan(&id)
		if err != nil {
			http.Error(w, "DB error: "+err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
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
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var body CategoryRuleComponent
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.RuleID == 0 || body.ComponentType == "" {
			http.Error(w, "Missing or invalid fields", http.StatusBadRequest)
			return
		}
		var id int64
		err := db.QueryRow(`INSERT INTO cimplrcorpsaas.category_rule_components (rule_id, component_type, match_type, match_value, amount_operator, amount_value, txn_flow, currency_code, is_active) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING component_id`, body.RuleID, body.ComponentType, body.MatchType, body.MatchValue, body.AmountOperator, body.AmountValue, body.TxnFlow, body.CurrencyCode, body.IsActive).Scan(&id)
		if err != nil {
			http.Error(w, "DB error: "+err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"data":    map[string]interface{}{"component_id": id},
		})
	})
}
