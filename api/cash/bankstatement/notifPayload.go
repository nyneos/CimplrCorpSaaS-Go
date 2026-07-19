package bankstatement

// notif_payload.go — Rich notification payload types for bank statement events.
//
// OVERVIEW
// ────────
// When a bank statement is committed (PDF path via /cash/commit) or uploaded
// and processed (CSV/XLS via /cash/preview → V2, multi=true, V2→multi fallback,
// or zip preview per inner file), we fire TriggerNotification with a payload built
// from BankStatementNotifPayload — one dispatch per successful statement write.
//
// The payload is serialised to map[string]interface{} so that the template
// engine can access every nested field by name.  All slice fields become
// []map[string]interface{} so that TABLE_HTML, GROUP_BY, ORDER_BY, FILTER,
// SUM_OF_FIELD, COUNT_OF etc. work directly in email/SMS templates.
//
// TEMPLATE AUTHOR REFERENCE
// ─────────────────────────
// Top-level scalar variables (use as {{VarName}}):
//   BankStatementID         — internal UUID of the committed statement
//   AccountNumber           — bank account number
//   AccountName             — account holder name (if available)
//   BankName                — bank name
//   IFSC                    — IFSC code
//   CurrencyCode            — e.g. "INR"
//   PeriodStart             — "2026-01-01"
//   PeriodEnd               — "2026-01-31"
//   OpeningBalance          — float
//   ClosingBalance          — float
//   TotalTransactions       — int (total uploaded)
//   TotalDebit              — float (sum of all withdrawal_amounts)
//   TotalCredit             — float (sum of all deposit_amounts)
//   CategorizedCount        — int
//   UncategorizedCount      — int
//   CategorizedPercent      — float
//   UncategorizedPercent    — float
//   UploadedBy              — user_id / email of uploader
//   ApprovedBy              — user_id / email of approver (set on approve event)
//   FileName                — original filename
//   UploadedAt              — ISO timestamp
//   EntityID                — entity UUID
//
// List variables (use with TABLE_HTML, GROUP_BY, etc.):
//   Transactions            — []TxnRow (all transactions)
//     Fields: tran_date, value_date, description, withdrawal_amount, deposit_amount,
//             balance, category_id, category_name, type (DEBIT|CREDIT|NONE), amount
//   UncategorizedTransactions — subset of Transactions with no category
//   CategoryKPIs            — []CategoryKPIRow
//     Fields: category_id, category_name, category_type, count, debit_sum, credit_sum
//   CreditTransactions      — Transactions where type == "CREDIT"
//   DebitTransactions       — Transactions where type == "DEBIT"
//
// EXAMPLE TEMPLATE SNIPPETS
// ─────────────────────────
// 1. Simple totals:
//    Opening: FORMAT_NUMBER(OpeningBalance)
//    Total Debit: FORMAT_CURRENCY(TotalDebit)
//    Total Credit: FORMAT_CURRENCY(TotalCredit)
//    Net: FORMAT_CURRENCY(SUBTRACT(TotalCredit, TotalDebit))
//
// 2. Full transaction table:
//    TABLE_HTML(Transactions, 'tran_date', 'description', 'withdrawal_amount', 'deposit_amount', 'balance', 'category_name')
//
// 3. Category KPI cards:
//    GROUP_BY(Transactions, 'category_name')
//    KPI_CARDS_HTML(__grouped_Transactions_category_name)
//
// 4. Only debit transactions, sorted high→low:
//    ORDER_BY(DebitTransactions, 'withdrawal_amount', 'DESC')
//    TABLE_HTML(__ordered_DebitTransactions_withdrawal_amount_DESC, 'tran_date', 'description', 'withdrawal_amount')
//
// 5. Category summary table:
//    GROUP_BY(Transactions, 'category_name')
//    SUMMARY_TABLE_HTML(__grouped_Transactions_category_name)
//
// 6. Total of a specific category (first filter, then sum):
//    FILTER(Transactions, 'category_name', 'Salary')
//    SUM_OF_FIELD(__filter_category_name_Salary, 'deposit_amount')
//
// 7. Badge for status:
//    BADGE_HTML(Status, '#1cc88a')
//
// 8. Count of uncategorized:
//    COUNT_OF(UncategorizedTransactions) transactions need review

import (
	"CimplrCorpSaas/api/constants"
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// All exported fields map directly to template variable names.
type TxnRow struct {
	TranDate         string  `json:"tran_date"`
	ValueDate        string  `json:"value_date"`
	Description      string  `json:"description"`
	WithdrawalAmount float64 `json:"withdrawal_amount"`
	DepositAmount    float64 `json:"deposit_amount"`
	Balance          float64 `json:"balance"`
	CategoryID       string  `json:"category_id"`
	CategoryName     string  `json:"category_name"`
	CategoryType     string  `json:"category_type"`
	TxnType          string  `json:"type"`   // "DEBIT" | "CREDIT" | "NONE"
	Amount           float64 `json:"amount"` // abs(withdrawal or deposit) for sorting
	Index            int     `json:"index"`
}

// CategoryKPIRow is one category group summary.
type CategoryKPIRow struct {
	CategoryID   string  `json:"category_id"`
	CategoryName string  `json:"category_name"`
	CategoryType string  `json:"category_type"`
	Count        int     `json:"count"`
	DebitSum     float64 `json:"debit_sum"`
	CreditSum    float64 `json:"credit_sum"`
}

// BankStatementNotifPayload is the top-level struct that carries all data
// for a bank statement notification event.  Build it with
// BuildBankStatementPayload and pass the result of ToMap() to TriggerNotification.
type BankStatementNotifPayload struct {
	// ── Scalar metadata ────────────────────────────────────────────────────────
	BankStatementID      string  `json:"BankStatementID"`
	AccountNumber        string  `json:"AccountNumber"`
	AccountName          string  `json:"AccountName"`
	BankName             string  `json:"BankName"`
	IFSC                 string  `json:"IFSC"`
	CurrencyCode         string  `json:"CurrencyCode"`
	PeriodStart          string  `json:"PeriodStart"`
	PeriodEnd            string  `json:"PeriodEnd"`
	OpeningBalance       float64 `json:"OpeningBalance"`
	ClosingBalance       float64 `json:"ClosingBalance"`
	TotalTransactions    int     `json:"TotalTransactions"`
	TotalDebit           float64 `json:"TotalDebit"`
	TotalCredit          float64 `json:"TotalCredit"`
	NetFlow              float64 `json:"NetFlow"` // TotalCredit - TotalDebit
	CategorizedCount     int     `json:"CategorizedCount"`
	UncategorizedCount   int     `json:"UncategorizedCount"`
	CategorizedPercent   float64 `json:"CategorizedPercent"`
	UncategorizedPercent float64 `json:"UncategorizedPercent"`
	UploadedBy           string  `json:"UploadedBy"`
	ApprovedBy           string  `json:"ApprovedBy"`
	FileName             string  `json:"FileName"`
	UploadedAt           string  `json:"UploadedAt"`
	EntityID             string  `json:"EntityID"`
	Status               string  `json:"Status"` // constants.StatusPendingApproval | constants.StatusApproved etc.

	// ── List fields ────────────────────────────────────────────────────────────
	Transactions              []TxnRow         `json:"Transactions"`
	UncategorizedTransactions []TxnRow         `json:"UncategorizedTransactions"`
	CreditTransactions        []TxnRow         `json:"CreditTransactions"`
	DebitTransactions         []TxnRow         `json:"DebitTransactions"`
	CategoryKPIs              []CategoryKPIRow `json:"CategoryKPIs"`
}

// ToMap converts BankStatementNotifPayload to map[string]interface{} ready for
// TriggerNotification.  List fields are converted to []map[string]interface{}
// so the template engine's TABLE_HTML / GROUP_BY / etc. functions work on them.
func (p *BankStatementNotifPayload) ToMap() map[string]interface{} {
	m := map[string]interface{}{
		"BankStatementID":      p.BankStatementID,
		"AccountNumber":        p.AccountNumber,
		"AccountName":          p.AccountName,
		"BankName":             p.BankName,
		"IFSC":                 p.IFSC,
		"CurrencyCode":         p.CurrencyCode,
		"PeriodStart":          p.PeriodStart,
		"PeriodEnd":            p.PeriodEnd,
		"OpeningBalance":       p.OpeningBalance,
		"ClosingBalance":       p.ClosingBalance,
		"TotalTransactions":    p.TotalTransactions,
		"TotalDebit":           p.TotalDebit,
		"TotalCredit":          p.TotalCredit,
		"NetFlow":              p.NetFlow,
		"CategorizedCount":     p.CategorizedCount,
		"UncategorizedCount":   p.UncategorizedCount,
		"CategorizedPercent":   p.CategorizedPercent,
		"UncategorizedPercent": p.UncategorizedPercent,
		"UploadedBy":           p.UploadedBy,
		"ApprovedBy":           p.ApprovedBy,
		"FileName":             p.FileName,
		"UploadedAt":           p.UploadedAt,
		"EntityID":             p.EntityID,
		"Status":               p.Status,
		// List fields
		"Transactions":              txnRowsToMaps(p.Transactions),
		"UncategorizedTransactions": txnRowsToMaps(p.UncategorizedTransactions),
		"CreditTransactions":        txnRowsToMaps(p.CreditTransactions),
		"DebitTransactions":         txnRowsToMaps(p.DebitTransactions),
		"CategoryKPIs":              kpiRowsToMaps(p.CategoryKPIs),
	}
	return m
}

// txnRowsToMaps converts []TxnRow → []map[string]interface{} for template engine.
func txnRowsToMaps(rows []TxnRow) []map[string]interface{} {
	out := make([]map[string]interface{}, len(rows))
	for i, r := range rows {
		out[i] = map[string]interface{}{
			"tran_date":         r.TranDate,
			"value_date":        r.ValueDate,
			"description":       r.Description,
			"withdrawal_amount": r.WithdrawalAmount,
			"deposit_amount":    r.DepositAmount,
			"balance":           r.Balance,
			"category_id":       r.CategoryID,
			"category_name":     r.CategoryName,
			"category_type":     r.CategoryType,
			"type":              r.TxnType,
			"amount":            r.Amount,
			"index":             r.Index,
		}
	}
	return out
}

// kpiRowsToMaps converts []CategoryKPIRow → []map[string]interface{}.
func kpiRowsToMaps(rows []CategoryKPIRow) []map[string]interface{} {
	out := make([]map[string]interface{}, len(rows))
	for i, r := range rows {
		out[i] = map[string]interface{}{
			"category_id":   r.CategoryID,
			"category_name": r.CategoryName,
			"category_type": r.CategoryType,
			"count":         r.Count,
			"debit_sum":     r.DebitSum,
			"credit_sum":    r.CreditSum,
		}
	}
	return out
}

// BuildBankStatementNotifPayload constructs a rich notification payload for bank statement
// events by querying the database for the full statement records and transactions.
// This follows the same pattern as BuildLimitNotifPayload, BuildUtilizationNotifPayload, etc.
//
// Parameters:
//
//	ctx            — request context
//	pool           — pgx connection pool
//	bankStatementIDs — slice of bank_statement_id UUIDs to fetch
//	action         — "APPROVE", "REJECT", "DELETE", etc. (workflow action type)
//	requestedBy    — user_id / email of the user performing the action
//
// Returns:
//
//	map[string]interface{} containing all fields needed for template evaluation.
//	Use this directly with TriggerNotification(ctx, pool, route, correlationID, payload)
func BuildBankStatementNotifPayload(
	ctx context.Context,
	pool *pgxpool.Pool,
	bankStatementIDs []string,
	action string,
	requestedBy string,
) map[string]interface{} {
	payload := make(map[string]interface{})
	payload["Action"] = action
	payload["RequestedBy"] = requestedBy
	payload["BankStatementIDs"] = bankStatementIDs
	payload["Count"] = len(bankStatementIDs)
	payload["ActionAt"] = time.Now().Format(time.RFC3339)

	// If no IDs provided, return minimal payload
	if len(bankStatementIDs) == 0 {
		return payload
	}

	// Try to query database for full bank statement records
	// Query structure matches the GET endpoint for bank statements
	query := `
		WITH latest_audit AS (
			SELECT a.*
			FROM cimplrcorpsaas.auditactionbankstatement a
			INNER JOIN (
				SELECT bankstatementid, MAX(action_id) AS max_action_id
				FROM cimplrcorpsaas.auditactionbankstatement
				GROUP BY bankstatementid
			) b ON a.bankstatementid = b.bankstatementid AND a.action_id = b.max_action_id
		)
		SELECT 
			s.bank_statement_id, 
			e.entity_name, 
			s.account_number, 
			s.statement_period_start, 
			s.statement_period_end, 
			s.opening_balance, 
			s.closing_balance, 
			s.uploaded_at,
			la.actiontype, 
			la.processing_status, 
			la.requested_by, 
			la.checker_by, 
			la.checker_comment,
			COALESCE(mb.bank_name, '') AS bank_name,
			mba.account_nickname AS account_nickname,
			s.entity_id
		FROM cimplrcorpsaas.bank_statements s
		JOIN public.masterentitycash e ON s.entity_id = e.entity_id
		LEFT JOIN public.masterbankaccount mba ON mba.account_number = s.account_number AND mba.is_deleted = false
		LEFT JOIN public.masterbank mb ON mb.bank_id = mba.bank_id
		LEFT JOIN latest_audit la ON la.bankstatementid = s.bank_statement_id
		WHERE s.bank_statement_id = ANY($1)
		ORDER BY s.uploaded_at DESC
	`

	statements := []map[string]interface{}{}

	if pool != nil {
		rows, err := pool.Query(ctx, query, bankStatementIDs)
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var id, entityName, acc, entityID string
				var start, end, uploaded time.Time
				var open, close float64
				var actionType, processingStatus, reqBy, checkBy, checkComment sql.NullString
				var bankName, accountNickname sql.NullString

				if err := rows.Scan(&id, &entityName, &acc, &start, &end, &open, &close, &uploaded,
					&actionType, &processingStatus, &reqBy, &checkBy, &checkComment, &bankName, &accountNickname, &entityID); err != nil {
					continue
				}

				statements = append(statements, map[string]interface{}{
					"bank_statement_id":      id,
					"entity_name":            entityName,
					"account_number":         acc,
					"statement_period_start": start.Format(constants.DateFormat),
					"statement_period_end":   end.Format(constants.DateFormat),
					"opening_balance":        open,
					"closing_balance":        close,
					"uploaded_at":            uploaded.Format(time.RFC3339),
					"action_type":            actionType.String,
					"processing_status":      processingStatus.String,
					"requested_by":           reqBy.String,
					"checker_by":             checkBy.String,
					"checker_comment":        checkComment.String,
					"bank_name":              bankName.String,
					"account_nickname":       accountNickname.String,
					"entity_id":              entityID,
				})
			}
		}
	}
	payload["BankStatements"] = statements

	// Compute aggregate KPIs
	totalOpening := 0.0
	totalClosing := 0.0
	for _, stmt := range statements {
		if open, ok := stmt["opening_balance"].(float64); ok {
			totalOpening += open
		}
		if close, ok := stmt["closing_balance"].(float64); ok {
			totalClosing += close
		}
	}

	payload["TotalOpeningBalance"] = totalOpening
	payload["TotalClosingBalance"] = totalClosing
	payload["NetChange"] = totalClosing - totalOpening

	return payload
}

// BuildBankStatementParams groups parameters for constructing BankStatementNotifPayload.
type BuildBankStatementParams struct {
	BSID           string
	AccountNumber  string
	Metadata       *Metadata
	OpeningBalance float64
	ClosingBalance float64
	EntityID       string
	UploadedBy     string
	FileName       string
	TXNS           []RecalculateTransaction
	KPICats        []map[string]interface{}
	CategoryRules  []categoryRuleComponent
	Status         string
}

// BuildBankStatementPayload constructs a BankStatementNotifPayload from the
// data available at CommitHandler / V2 upload handler completion time.
//
// Parameters:
//
//	bsID           — bank_statement_id (UUID string)
//	accountNumber  — account number string
//	metadata       — pointer to Metadata (from the clean struct; may be nil)
//	openingBalance — opening balance float
//	closingBalance — closing balance float
//	entityID       — entity UUID string
//	uploadedBy     — user_id / email of the uploader
//	fileName       — original uploaded filename
//	txns           — slice of RecalculateTransaction (from the commit payload)
//	kpiCats        — the category_kpis slice built during categorization
//	categoryRules  — the loaded category rule components (for name lookup)
//	status         — workflow status string (e.g. constants.StatusPendingApproval)
func BuildBankStatementPayload(
	params BuildBankStatementParams,
) *BankStatementNotifPayload {

	p := &BankStatementNotifPayload{
		BankStatementID: params.BSID,
		AccountNumber:   params.AccountNumber,
		OpeningBalance:  params.OpeningBalance,
		ClosingBalance:  params.ClosingBalance,
		EntityID:        params.EntityID,
		UploadedBy:      params.UploadedBy,
		FileName:        params.FileName,
		Status:          params.Status,
		UploadedAt:      time.Now().Format(time.RFC3339),
		CurrencyCode:    "INR",
	}

	// Extract metadata fields
	if params.Metadata != nil {
		if params.Metadata.AccountName != nil {
			p.AccountName = *params.Metadata.AccountName
		}
		if params.Metadata.BankName != nil {
			p.BankName = *params.Metadata.BankName
		}
		if params.Metadata.IFSC != nil {
			p.IFSC = *params.Metadata.IFSC
		}
		if params.Metadata.PeriodStart != nil {
			p.PeriodStart = *params.Metadata.PeriodStart
		}
		if params.Metadata.PeriodEnd != nil {
			p.PeriodEnd = *params.Metadata.PeriodEnd
		}
		if params.Metadata.ClosingBalance != nil && params.ClosingBalance == 0 {
			p.ClosingBalance = *params.Metadata.ClosingBalance
		}
	}

	// Build a category name lookup from rules
	catNameLookup := map[string]string{}
	catTypeLookup := map[string]string{}
	for _, r := range params.CategoryRules {
		catNameLookup[r.CategoryID] = r.CategoryName
		catTypeLookup[r.CategoryID] = r.CategoryType
	}

	// Build TxnRow list
	for i, t := range params.TXNS {
		row := TxnRow{Index: i}
		if t.TranDate != nil {
			row.TranDate = *t.TranDate
		}
		if t.ValueDate != nil {
			row.ValueDate = *t.ValueDate
		}
		if t.Narration != nil {
			row.Description = *t.Narration
		}
		if t.Withdrawal != nil && *t.Withdrawal > 0 {
			row.WithdrawalAmount = *t.Withdrawal
			row.TxnType = "DEBIT"
			row.Amount = *t.Withdrawal
		}
		if t.Deposit != nil && *t.Deposit > 0 {
			row.DepositAmount = *t.Deposit
			row.TxnType = "CREDIT"
			row.Amount = *t.Deposit
		}
		if t.Balance != nil {
			row.Balance = *t.Balance
		}

		p.Transactions = append(p.Transactions, row)
		p.TotalTransactions++
		p.TotalDebit += row.WithdrawalAmount
		p.TotalCredit += row.DepositAmount

		if row.TxnType == "DEBIT" {
			p.DebitTransactions = append(p.DebitTransactions, row)
		} else if row.TxnType == "CREDIT" {
			p.CreditTransactions = append(p.CreditTransactions, row)
		}
	}

	p.NetFlow = p.TotalCredit - p.TotalDebit

	// Overlay category information from kpiCats
	// Build a map: category_id → (name, type) for the KPI rows
	kpiMap := map[string]CategoryKPIRow{}
	for _, kpi := range params.KPICats {
		catID := fmt.Sprintf("%v", kpi["category_id"])
		catName := fmt.Sprintf("%v", kpi["category_name"])
		catType := ""
		if ct, ok := kpi["category_type"]; ok {
			catType = fmt.Sprintf("%v", ct)
		}
		count := 0
		if c, ok := kpi["count"]; ok {
			switch cv := c.(type) {
			case int:
				count = cv
			case float64:
				count = int(cv)
			}
		}
		debitSum := 0.0
		creditSum := 0.0
		if ds, ok := kpi["debit_sum"]; ok {
			if f, ok2 := ds.(float64); ok2 {
				debitSum = f
			}
		}
		if cs, ok := kpi["credit_sum"]; ok {
			if f, ok2 := cs.(float64); ok2 {
				creditSum = f
			}
		}
		kpiMap[catID] = CategoryKPIRow{
			CategoryID:   catID,
			CategoryName: catName,
			CategoryType: catType,
			Count:        count,
			DebitSum:     debitSum,
			CreditSum:    creditSum,
		}
		p.CategoryKPIs = append(p.CategoryKPIs, kpiMap[catID])
	}

	// Build categorized / uncategorized transaction lists with category names
	// We rely on the kpiCats.transactions field if present to know which txns are categorized.
	// Build a fast lookup: index → categoryID from kpiCats rows.
	txnCatLookup := map[int]string{}
	for _, kpi := range params.KPICats {
		catID := fmt.Sprintf("%v", kpi["category_id"])
		if txnRows, ok := kpi["transactions"]; ok {
			if trs, ok2 := txnRows.([]map[string]interface{}); ok2 {
				for _, tr := range trs {
					if idx, ok3 := tr["index"].(int); ok3 {
						txnCatLookup[idx] = catID
					}
				}
			}
		}
	}

	// Re-annotate Transactions with category info and split lists
	for i := range p.Transactions {
		if catID, ok := txnCatLookup[i]; ok && catID != "" {
			p.Transactions[i].CategoryID = catID
			p.Transactions[i].CategoryName = catNameLookup[catID]
			p.Transactions[i].CategoryType = catTypeLookup[catID]
			p.CategorizedCount++
		} else {
			p.UncategorizedTransactions = append(p.UncategorizedTransactions, p.Transactions[i])
			p.UncategorizedCount++
		}
	}

	total := p.TotalTransactions
	if total > 0 {
		p.CategorizedPercent = float64(p.CategorizedCount) * 100.0 / float64(total)
		p.UncategorizedPercent = float64(p.UncategorizedCount) * 100.0 / float64(total)
	}

	// Re-sync DebitTransactions and CreditTransactions with category info
	p.DebitTransactions = nil
	p.CreditTransactions = nil
	for _, t := range p.Transactions {
		if t.TxnType == "DEBIT" {
			p.DebitTransactions = append(p.DebitTransactions, t)
		} else if t.TxnType == "CREDIT" {
			p.CreditTransactions = append(p.CreditTransactions, t)
		}
	}

	return p
}

// BuildBankStatementPayloadFromV2Result converts the map[string]interface{} returned
// by UploadBankStatementV2WithCategorization (and the same structure built inside
// CommitHandler / RecomputeHandler) into a fully typed BankStatementNotifPayload.
//
// result keys consumed (all optional — missing keys become zero values):
//
//	bank_statement_id, account_number, bank_name, opening_balance, closing_balance,
//	transactions_uploaded_count, grouped_transaction_count, ungrouped_transaction_count,
//	grouped_transaction_percent, ungrouped_transaction_percent,
//	category_kpis  []map{category_id,category_name,category_type,count,debit_sum,credit_sum,transactions[]},
//	uncategorized  []map{description,withdrawal_amount,deposit_amount,balance,tran_date,value_date,...},
//	statement_date_coverage map{start,end}
//
// Extra scalar overrides (not in result map):
//
//	uploadedBy  — user_id / email of the uploader  (form field user_id)
//	fileName    — original file name
//	status      — workflow status (constants.StatusPendingApproval etc.)
func BuildBankStatementPayloadFromV2Result(
	result map[string]interface{},
	uploadedBy string,
	fileName string,
	status string,
) *BankStatementNotifPayload {
	p := &BankStatementNotifPayload{
		UploadedBy:   uploadedBy,
		FileName:     fileName,
		Status:       status,
		UploadedAt:   time.Now().Format(time.RFC3339),
		CurrencyCode: "INR",
	}

	// ── Scalar fields ──────────────────────────────────────────────────────────
	p.BankStatementID = mapStr(result, "bank_statement_id", "BankStatementID")
	p.AccountNumber = mapStr(result, "account_number", "AccountNumber")
	p.BankName = mapStr(result, "bank_name", "BankName")
	p.AccountName = mapStr(result, "account_name", "AccountName")
	p.IFSC = mapStr(result, "ifsc", "IFSC")
	p.EntityID = mapStr(result, "entity_id", "EntityID")

	p.OpeningBalance = mapFloat(result, "opening_balance", "OpeningBalance")
	p.ClosingBalance = mapFloat(result, "closing_balance", "ClosingBalance")

	p.TotalTransactions = mapInt(result, "transactions_uploaded_count", "TotalTransactions")
	p.CategorizedCount = mapInt(result, "grouped_transaction_count", "CategorizedCount")
	p.UncategorizedCount = mapInt(result, "ungrouped_transaction_count", "UncategorizedCount")
	p.CategorizedPercent = mapFloat(result, "grouped_transaction_percent", "CategorizedPercent")
	p.UncategorizedPercent = mapFloat(result, "ungrouped_transaction_percent", "UncategorizedPercent")

	// Statement date coverage
	if cov, ok := result["statement_date_coverage"]; ok {
		if m, ok2 := cov.(map[string]interface{}); ok2 {
			p.PeriodStart = fmt.Sprintf("%v", m["start"])
			p.PeriodEnd = fmt.Sprintf("%v", m["end"])
		}
	}

	// ── CategoryKPIs ───────────────────────────────────────────────────────────
	// category_kpis is []map{category_id,category_name,category_type,count,debit_sum,credit_sum,transactions[]}
	if raw, ok := result["category_kpis"]; ok {
		if kpis, ok2 := toMapSlice(raw); ok2 {
			for _, kpi := range kpis {
				kr := CategoryKPIRow{
					CategoryID:   fmt.Sprintf("%v", kpi["category_id"]),
					CategoryName: fmt.Sprintf("%v", kpi["category_name"]),
					CategoryType: fmt.Sprintf("%v", kpi["category_type"]),
					Count:        toInt(kpi["count"]),
					DebitSum:     toFloat64(kpi["debit_sum"]),
					CreditSum:    toFloat64(kpi["credit_sum"]),
				}
				p.CategoryKPIs = append(p.CategoryKPIs, kr)
				p.TotalDebit += kr.DebitSum
				p.TotalCredit += kr.CreditSum

				// Transactions embedded inside each KPI group
				if txnRaw, ok3 := kpi["transactions"]; ok3 {
					if txnMaps, ok4 := toMapSlice(txnRaw); ok4 {
						for idx, t := range txnMaps {
							row := TxnRow{
								Index:            toInt(t["index"]),
								Description:      fmt.Sprintf("%v", t["description"]),
								WithdrawalAmount: toFloat64(t["withdrawal_amount"]),
								DepositAmount:    toFloat64(t["deposit_amount"]),
								Balance:          toFloat64(t["balance"]),
								CategoryID:       kr.CategoryID,
								CategoryName:     kr.CategoryName,
								CategoryType:     kr.CategoryType,
							}
							row.TranDate = timeToDateStr(t["tran_date"], t["transaction_date"])
							row.ValueDate = timeToDateStr(t["value_date"])
							if row.WithdrawalAmount > 0 {
								row.TxnType = "DEBIT"
								row.Amount = row.WithdrawalAmount
							} else {
								row.TxnType = "CREDIT"
								row.Amount = row.DepositAmount
							}
							_ = idx
							p.Transactions = append(p.Transactions, row)
							p.CategorizedCount++ // recount from actual txn rows
						}
					}
				}
			}
		}
	}

	// ── Uncategorized transactions ─────────────────────────────────────────────
	// uncategorized is []map{description,withdrawal_amount,deposit_amount,balance,tran_date,...}
	if raw, ok := result["uncategorized"]; ok {
		if uncats, ok2 := toMapSlice(raw); ok2 {
			for i, t := range uncats {
				row := TxnRow{
					Index:            i,
					Description:      fmt.Sprintf("%v", t["description"]),
					WithdrawalAmount: toFloat64(t["withdrawal_amount"]),
					DepositAmount:    toFloat64(t["deposit_amount"]),
					Balance:          toFloat64(t["balance"]),
					CategoryID:       "",
					CategoryName:     "Uncategorized",
				}
				row.TranDate = timeToDateStr(t["tran_date"], t["transaction_date"])
				row.ValueDate = timeToDateStr(t["value_date"])
				if row.WithdrawalAmount > 0 {
					row.TxnType = "DEBIT"
					row.Amount = row.WithdrawalAmount
				} else {
					row.TxnType = "CREDIT"
					row.Amount = row.DepositAmount
				}
				p.UncategorizedTransactions = append(p.UncategorizedTransactions, row)
				p.Transactions = append(p.Transactions, row)
			}
		}
	}

	// If TotalTransactions was not set from result map or is still 0, derive it
	if p.TotalTransactions == 0 {
		p.TotalTransactions = len(p.Transactions)
	}
	total := p.TotalTransactions
	if total > 0 {
		p.CategorizedCount = total - len(p.UncategorizedTransactions)
		p.UncategorizedCount = len(p.UncategorizedTransactions)
		p.CategorizedPercent = float64(p.CategorizedCount) * 100.0 / float64(total)
		p.UncategorizedPercent = float64(p.UncategorizedCount) * 100.0 / float64(total)
	}

	p.NetFlow = p.TotalCredit - p.TotalDebit

	// Split Debit / Credit transaction sub-lists
	for _, t := range p.Transactions {
		if t.TxnType == "DEBIT" {
			p.DebitTransactions = append(p.DebitTransactions, t)
		} else if t.TxnType == "CREDIT" {
			p.CreditTransactions = append(p.CreditTransactions, t)
		}
	}

	return p
}

// ── private helpers ────────────────────────────────────────────────────────────

// mapStr tries several key names (case-sensitive) and returns the first string found.
func mapStr(m map[string]interface{}, keys ...string) string {
	for _, k := range keys {
		if v, ok := m[k]; ok && v != nil {
			return fmt.Sprintf("%v", v)
		}
	}
	return ""
}

func mapFloat(m map[string]interface{}, keys ...string) float64 {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			return toFloat64(v)
		}
	}
	return 0
}

func mapInt(m map[string]interface{}, keys ...string) int {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			return toInt(v)
		}
	}
	return 0
}

func toFloat64(v interface{}) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	case int:
		return float64(n)
	case int64:
		return float64(n)
	case int32:
		return float64(n)
	}
	return 0
}

func toInt(v interface{}) int {
	switch n := v.(type) {
	case int:
		return n
	case float64:
		return int(n)
	case int64:
		return int(n)
	case int32:
		return int(n)
	}
	return 0
}

// toMapSlice converts []interface{} or []map[string]interface{} to []map[string]interface{}.
func toMapSlice(v interface{}) ([]map[string]interface{}, bool) {
	switch t := v.(type) {
	case []map[string]interface{}:
		return t, true
	case []interface{}:
		out := make([]map[string]interface{}, 0, len(t))
		for _, e := range t {
			if m, ok := e.(map[string]interface{}); ok {
				out = append(out, m)
			}
		}
		return out, true
	}
	return nil, false
}

// timeToDateStr converts a time.Time, string, or nil value from a map to a date string.
// Accepts multiple candidate values (first non-empty wins).
func timeToDateStr(candidates ...interface{}) string {
	for _, v := range candidates {
		if v == nil {
			continue
		}
		switch t := v.(type) {
		case time.Time:
			if !t.IsZero() {
				return t.Format(constants.DateFormat)
			}
		case string:
			if t != "" && t != "<nil>" {
				return t
			}
		default:
			s := fmt.Sprintf("%v", v)
			if s != "" && s != "<nil>" {
				return s
			}
		}
	}
	return ""
}
