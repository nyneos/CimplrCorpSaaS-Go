package bankstatement

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"regexp"
	"time"

	"CimplrCorpSaas/api/constants"
)

// Sentinel errors used for mapping to user-friendly messages
var (
	ErrFileAlreadyUploaded      = errors.New("bank statement file already uploaded")
	ErrAccountNotFound          = errors.New("bank account not found in master data")
	ErrAccountNumberMissing     = errors.New("account number could not be found anywhere in the file name or file contents")
	ErrStatementPeriodExists    = errors.New("a statement for this period already exists for this account")
	ErrAllTransactionsDuplicate = errors.New("all transactions in this statement already exist")
)

var (
	// precompiled regexes for fallback extraction
	acctLabelRe  = regexp.MustCompile(`(?i)\b(?:a[/\\]?c|acct|account)[\s\.:#-]*(?:no|number)?\b`)
	acctNumberRe = regexp.MustCompile(`\d{7,}`) // prefer >=7 digits to avoid postal-code false positives
)

var (
	acNoHeader                      = "A/C No:"
	slNoHeader                      = "Sl. No."
	tranIDHeader                    = constants.TranID
	valueDateHeader                 = constants.ValueDateAlt
	transactionDateHeader           = constants.TransactionDateAlt
	transactionRemarksHeader        = constants.TransactionRemarks
	withdrawalAmtHeader             = constants.WithdrawalAmountINR
	depositAmtHeader                = constants.DepositAmountINR
	balanceHeader                   = constants.BalanceINR
	missingUserIDOrBankStatementIDs = "Missing user_id or bank_statement_ids"
)

// categoryRuleComponent represents a single rule component used for
// categorizing transactions. This is shared between the upload and recompute
// flows so that both use identical matching logic.
type categoryRuleComponent struct {
	RuleID         int64
	Priority       int
	CategoryID     string
	CategoryName   string
	CategoryType   string
	ComponentType  string
	MatchType      sql.NullString
	MatchValue     sql.NullString
	AmountOperator sql.NullString
	AmountValue    sql.NullFloat64
	TxnFlow        sql.NullString
	CurrencyCode   sql.NullString
	EffectiveDate  sql.NullTime
}

// ColumnMappings holds custom column name mappings for flexible bank statement parsing
type ColumnMappings struct {
	AccountNumber    string `json:"account_number"`
	TranID           string `json:"tran_id"`
	ValueDate        string `json:"value_date"`
	Description      string `json:"description"`
	DepositAmount    string `json:"deposit_amount"`
	WithdrawalAmount string `json:"withdrawal_amount"`
	Balance          string `json:"balance"`
	AccountName      string `json:"AccountName"`
	BankName         string `json:"BankName"`
	IFSC             string `json:"IFSC"`
	MICR             string `json:"MICR"`
}

// bytesFile implements multipart.File for a bytes.Reader
type bytesFile struct {
	*bytes.Reader
}

func (b *bytesFile) Close() error { return nil }

// PDFBankStatementJSON represents the JSON structure from PDF parsing
type PDFBankStatementJSON struct {
	AccountNumber  string               `json:"account_number"`
	AccountName    string               `json:"account_name"`
	BankName       string               `json:"bank_name"`
	IFSC           string               `json:"ifsc"`
	MICR           string               `json:"micr"`
	PeriodStart    string               `json:"period_start"`
	PeriodEnd      string               `json:"period_end"`
	OpeningBalance float64              `json:"opening_balance"`
	ClosingBalance float64              `json:"closing_balance"`
	Transactions   []PDFTransactionJSON `json:"transactions"`
}

type PDFTransactionJSON struct {
	TranID          *string `json:"tran_id"`
	TransactionDate string  `json:"transaction_date"`
	ValueDate       string  `json:"value_date"`
	Description     string  `json:"description"`
	Withdrawal      float64 `json:"withdrawal"`
	Deposit         float64 `json:"deposit"`
	Balance         float64 `json:"balance"`
}

type BankStatement struct {
	BankStatementID      string     `db:"bank_statement_id"`
	EntityID             string     `db:"entity_id"`
	AccountNumber        string     `db:"account_number"`
	StatementPeriodStart time.Time  `db:"statement_period_start"`
	StatementPeriodEnd   time.Time  `db:"statement_period_end"`
	StatementRequestDate *time.Time `db:"statement_request_date"`
	FileHash             string     `db:"file_hash"`
	UploadedAt           time.Time  `db:"uploaded_at"`
	OpeningBalance       float64    `db:"opening_balance"`
	ClosingBalance       float64    `db:"closing_balance"`
}

type BankStatementTransaction struct {
	TransactionID    int64           `db:"transaction_id"`
	BankStatementID  string          `db:"bank_statement_id"`
	AccountNumber    string          `db:"account_number"`
	TranID           sql.NullString  `db:"tran_id"`
	ValueDate        time.Time       `db:"value_date"`
	TransactionDate  time.Time       `db:"transaction_date"`
	PostedDate       sql.NullTime    `db:"posted_date"`
	ChequeNo         sql.NullString  `db:"cheque_no"`
	Description      string          `db:"description"`
	WithdrawalAmount sql.NullFloat64 `db:"withdrawal_amount"`
	DepositAmount    sql.NullFloat64 `db:"deposit_amount"`
	Balance          sql.NullFloat64 `db:"balance"`
	RawJSON          json.RawMessage `db:"raw_json"`
	CategoryID       sql.NullString  `db:"category_id"`
	CreatedAt        time.Time       `db:"created_at"`
}

// StructuredBankStatement and StructuredTransaction are canonical input types
// for structured ingestion (PDF JSON or pre-parsed CSV JSON).
type StructuredBankStatement struct {
	AccountNumber  string
	BankName       string
	PeriodStart    string
	PeriodEnd      string
	OpeningBalance float64
	ClosingBalance float64
	Transactions   []StructuredTransaction
}

type StructuredTransaction struct {
	TranID          *string
	TransactionDate string
	ValueDate       string
	Description     string
	Withdrawal      float64
	Deposit         float64
	Balance         float64
}
