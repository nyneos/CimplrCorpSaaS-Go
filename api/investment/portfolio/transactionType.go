package portfolio

import "strings"

// Canonical onboard / portfolio transaction types.
const (
	TxTypePurchase   = "Purchase"
	TxTypeRedemption   = "Redemption"
)

// NormalizeTransactionType maps buy/sell variants to Purchase or Redemption.
func NormalizeTransactionType(t string) string {
	switch strings.ToLower(strings.TrimSpace(t)) {
	case "buy", "purchase", "subscription", "bonus", "sip", "switch_in":
		return TxTypePurchase
	case "sell", "redemption", "redeem", "switch_out":
		return TxTypeRedemption
	default:
		if strings.TrimSpace(t) == "" {
			return ""
		}
		return strings.TrimSpace(t)
	}
}

// SQLNormalizeTransactionType normalizes transaction_type in SELECT lists.
const SQLNormalizeTransactionType = `
CASE
  WHEN LOWER(TRIM(transaction_type)) IN ('purchase','buy','subscription','bonus','sip','switch_in') THEN 'Purchase'
  WHEN LOWER(TRIM(transaction_type)) IN ('sell','redemption','redeem','switch_out') THEN 'Redemption'
  ELSE NULLIF(TRIM(transaction_type), '')
END`
