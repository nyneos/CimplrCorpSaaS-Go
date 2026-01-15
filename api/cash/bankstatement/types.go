package bankstatement

// Types used by streaming and recalculate/commit handlers

type CompleteResponse struct {
	Status string    `json:"status"`
	Clean  CleanData `json:"clean"`
	Debug  DebugInfo `json:"debug"`
}

type CleanData struct {
	Metadata       Metadata       `json:"metadata"`
	OpeningBalance *float64       `json:"opening_balance"`
	Transactions   []Transaction  `json:"transactions"`
	Validation     ValidationInfo `json:"validation"`
}

type Metadata struct {
	AccountNumber  *string  `json:"account_number,omitempty"`
	AccountName    *string  `json:"account_name,omitempty"`
	BankName       *string  `json:"bank_name,omitempty"`
	IFSC           *string  `json:"ifsc,omitempty"`
	MICR           *string  `json:"micr,omitempty"`
	PeriodStart    *string  `json:"period_start,omitempty"`
	PeriodEnd      *string  `json:"period_end,omitempty"`
	OpeningBalance *float64 `json:"opening_balance,omitempty"`
	ClosingBalance *float64 `json:"closing_balance,omitempty"`
	Filename       *string  `json:"filename,omitempty"`
	PageCount      *int     `json:"page_count,omitempty"`
}
type Transaction struct {
	Date            string   `json:"date"`
	Description     string   `json:"description"`
	Amount          float64  `json:"amount"`
	Type            string   `json:"type"`
	RunningBalance  *float64 `json:"running_balance,omitempty"`
	ChequeNumber    *string  `json:"cheque_number,omitempty"`
	ReferenceNumber *string  `json:"reference_number,omitempty"`
}

// RecalculateInput matches the user's bank statement format
type RecalculateInput struct {
	UserID string               `json:"user_id"`
	Clean  RecalculateCleanData `json:"clean"`
}

type RecalculateCleanData struct {
	Metadata       Metadata                 `json:"metadata"`
	OpeningBalance *float64                 `json:"opening_balance"`
	Transactions   []RecalculateTransaction `json:"transactions"`
}

type RecalculateTransaction struct {
	TranDate   *string  `json:"tran_date"`
	ValueDate  *string  `json:"value_date"`
	Narration  *string  `json:"narration"`
	Withdrawal *float64 `json:"withdrawal"`
	Deposit    *float64 `json:"deposit"`
	Balance    *float64 `json:"balance"`
}

// RecalculateOutput response
type RecalculateOutput struct {
	Success         bool                       `json:"success"`
	Status          string                     `json:"status,omitempty"`
	ComputedClosing *float64                   `json:"computed_closing,omitempty"`
	ActualClosing   *float64                   `json:"actual_closing,omitempty"`
	IsValid         bool                       `json:"is_valid"`
	Clean           RecalculateCleanDataOutput `json:"clean,omitempty"`
	Validation      ValidationWrapper          `json:"validation"`
}

type ValidationWrapper struct {
	Status string            `json:"status"`
	Issues []ValidationIssue `json:"issues"`
}

type RecalculateCleanDataOutput struct {
	Metadata       Metadata                       `json:"metadata"`
	OpeningBalance *float64                       `json:"opening_balance"`
	Transactions   []RecalculateTransactionOutput `json:"transactions"`
}

type RecalculateTransactionOutput struct {
	TranDate       *string  `json:"tran_date"`
	ValueDate      *string  `json:"value_date"`
	Narration      string   `json:"narration"`
	Withdrawal     float64  `json:"withdrawal"`
	Deposit        float64  `json:"deposit"`
	Balance        *float64 `json:"balance"`
	RunningBalance *float64 `json:"running_balance,omitempty"`
}

type ValidationIssue struct {
	Transaction     RecalculateTransaction `json:"transaction"`
	ExpectedBalance *float64               `json:"expected_balance,omitempty"`
	ActualBalance   *float64               `json:"actual_balance,omitempty"`
}

type ValidationInfo struct {
	IsValid         bool     `json:"is_valid"`
	ErrorCount      int      `json:"error_count"`
	WarningCount    int      `json:"warning_count"`
	ErrorMessage    *string  `json:"error_message,omitempty"`
	ComputedClosing *float64 `json:"computed_closing,omitempty"`
	ActualClosing   *float64 `json:"actual_closing,omitempty"`
}

type DebugInfo struct {
	LLMCallsMade     int                    `json:"llm_calls_made"`
	LayoutLines      int                    `json:"layout_lines"`
	TableRows        int                    `json:"table_rows"`
	OCRLines         int                    `json:"ocr_lines"`
	TransactionCount int                    `json:"transaction_count"`
	LatencyMs        float64                `json:"latency_ms"`
	AIDebug          map[string]interface{} `json:"ai_debug,omitempty"`
}
