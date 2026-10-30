package api

type cashContextKey string

const (
	CashBankAccountsKey cashContextKey = "cashBankAccounts"
	// slice of BankInfo
	CashBanksKey     cashContextKey = "cashBanks"
	CashEntityIDsKey cashContextKey = "cashEntityIDs"
	// slice of BankAccountInfo (detailed)
	CashBankAccountsInfoKey cashContextKey = "cashBankAccountsInfo"
	CashCurrenciesKey       cashContextKey = "cashCurrencies"
	// map: currency_code -> decimal_places
	CashCurrencyDecimalsKey cashContextKey = "cashCurrencyDecimals"
	CashCategoriesKey       cashContextKey = "cashCategories"
	CashGLAccountsKey       cashContextKey = "cashGLAccounts"
)

// BankInfo holds minimal bank metadata returned to handlers
type BankInfo struct {
	ID           string `json:"bank_id"`
	Name         string `json:"bank_name"`
	ShortName    string `json:"bank_short_name"`
	SwiftBIC     string `json:"swift_bic_code"`
	Country      string `json:"country_of_headquarters"`
	Connectivity string `json:"connectivity_type"`
}

// BankAccountInfo holds an account number and its bank metadata
type BankAccountInfo struct {
	AccountNumber string `json:"account_number"`
	BankID        string `json:"bank_id"`
	BankName      string `json:"bank_name"`
}
