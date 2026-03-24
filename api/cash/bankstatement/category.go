package bankstatement

import (
	"context"
	"database/sql"
	"strings"
	"time"
)

// loadCategoryRuleComponents fetches all active rule components for a given
// account/entity/bank/currency/global scope, ordered by rule priority.
func loadCategoryRuleComponents(ctx context.Context, db *sql.DB, accountNumber, entityID, currencyCode string) ([]categoryRuleComponent, error) {
	q := `
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

	rowsRule, err := db.QueryContext(ctx, q, accountNumber, entityID, currencyCode)
	if err != nil {
		return nil, err
	}
	defer rowsRule.Close()

	rules := []categoryRuleComponent{}
	for rowsRule.Next() {
		var rc categoryRuleComponent
		if err := rowsRule.Scan(&rc.RuleID, &rc.Priority, &rc.CategoryID, &rc.CategoryName, &rc.CategoryType, &rc.ComponentType, &rc.MatchType, &rc.MatchValue, &rc.AmountOperator, &rc.AmountValue, &rc.TxnFlow, &rc.CurrencyCode, &rc.EffectiveDate); err != nil {
			return nil, err
		}
		rules = append(rules, rc)
	}
	if err := rowsRule.Err(); err != nil {
		return nil, err
	}
	return rules, nil
}

// matchCategoryForTransaction applies rule components to a single transaction
// and returns the matched category_id, if any. Rules use AND semantics within
// a single rule and priority-based selection across rules.
func matchCategoryForTransaction(rules []categoryRuleComponent, description string, withdrawal, deposit sql.NullFloat64, txnValueDate sql.NullTime) sql.NullString {
	type candidate struct {
		RuleID       int64
		Priority     int
		CategoryID   string
		CategoryType string
	}

	var candidates []candidate
	descLower := strings.ToLower(description)

	var refDate time.Time
	if txnValueDate.Valid {
		refDate = txnValueDate.Time
	} else {
		refDate = time.Now()
	}

	for i := 0; i < len(rules); {
		r := rules[i]
		rid := r.RuleID
		prio := r.Priority
		catID := r.CategoryID
		catType := r.CategoryType
		eff := r.EffectiveDate

		if eff.Valid && refDate.Before(eff.Time) {
			for i < len(rules) && rules[i].RuleID == rid {
				i++
			}
			continue
		}

		ruleMatches := true
		for i < len(rules) && rules[i].RuleID == rid {
			comp := rules[i]
			ct := comp.ComponentType

			switch ct {
			case "NARRATION_LOGIC":
				if comp.MatchType.Valid && comp.MatchValue.Valid {
					val := strings.ToLower(comp.MatchValue.String)
					mtrue := false
					switch comp.MatchType.String {
					case "CONTAINS", "ILIKE":
						if strings.Contains(descLower, val) {
							mtrue = true
						}
					case "EQUALS":
						if descLower == val {
							mtrue = true
						}
					case "STARTS_WITH":
						if strings.HasPrefix(descLower, val) {
							mtrue = true
						}
					case "ENDS_WITH":
						if strings.HasSuffix(descLower, val) {
							mtrue = true
						}
					case "REGEX":
						// not implemented: treat as no-match
					}
					if !mtrue {
						ruleMatches = false
					}
				} else {
					ruleMatches = false
				}

			case "AMOUNT_LOGIC":
				if comp.AmountOperator.Valid && comp.AmountValue.Valid {
					amounts := []float64{}
					if withdrawal.Valid {
						amounts = append(amounts, withdrawal.Float64)
					}
					if deposit.Valid {
						amounts = append(amounts, deposit.Float64)
					}
					sat := false
					for _, amt := range amounts {
						switch comp.AmountOperator.String {
						case ">":
							if amt > comp.AmountValue.Float64 {
								sat = true
							}
						case ">=":
							if amt >= comp.AmountValue.Float64 {
								sat = true
							}
						case "=":
							if amt == comp.AmountValue.Float64 {
								sat = true
							}
						case "<=":
							if amt <= comp.AmountValue.Float64 {
								sat = true
							}
						case "<":
							if amt < comp.AmountValue.Float64 {
								sat = true
							}
						}
						if sat {
							break
						}
					}
					if !sat {
						ruleMatches = false
					}
				} else {
					ruleMatches = false
				}

			case "TRANSACTION_LOGIC":
				if comp.TxnFlow.Valid {
					if comp.TxnFlow.String == "Outflow" {
						if !(withdrawal.Valid && withdrawal.Float64 > 0) {
							ruleMatches = false
						}
					} else if comp.TxnFlow.String == "Inflow" {
						if !(deposit.Valid && deposit.Float64 > 0) {
							ruleMatches = false
						}
					} else {
						ruleMatches = false
					}
				} else {
					ruleMatches = false
				}

			case "CURRENCY_CONDITION":
				// pass-through: currency checks left for callers

			default:
				ruleMatches = false
			}

			if !ruleMatches {
				for i < len(rules) && rules[i].RuleID == rid {
					i++
				}
				break
			}
			i++
		}

		if ruleMatches {
			if catType == "Outflow" && (!withdrawal.Valid || withdrawal.Float64 <= 0) {
				continue
			}
			if catType == "Inflow" && (!deposit.Valid || deposit.Float64 <= 0) {
				continue
			}
			candidates = append(candidates, candidate{RuleID: rid, Priority: prio, CategoryID: catID, CategoryType: catType})
		}
	}

	if len(candidates) == 0 {
		return sql.NullString{Valid: false}
	}

	best := candidates[0]
	for _, c := range candidates[1:] {
		if c.Priority < best.Priority {
			best = c
		}
	}

	return sql.NullString{String: best.CategoryID, Valid: true}
}
