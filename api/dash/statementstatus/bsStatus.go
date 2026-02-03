package statementstatus

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// UploadStatus values
const (
	UploadStatusApproved    = "APPROVED"
	UploadStatusPending     = "PENDING"
	UploadStatusRejected    = "REJECTED"
	UploadStatusNotUploaded = "NOT_UPLOADED"
)

// AccountRow mirrors the TypeScript interface the frontend expects.
type AccountRow struct {
	Entity     string          `json:"entity"`
	Bank       string          `json:"bank"`
	Currency   string          `json:"currency"`
	Account    string          `json:"account"`
	Status     string          `json:"status"`               // UploadStatus
	UploadedAt string          `json:"uploadedAt,omitempty"` // RFC3339 string, optional
	Statements []StatementMeta `json:"statements,omitempty"`
}

type StatementMeta struct {
	StatementID          string            `json:"statementId"`
	StatementPeriodStart string            `json:"statementPeriodStart"`
	StatementPeriodEnd   string            `json:"statementPeriodEnd"`
	Status               string            `json:"status"`
	UploadedAt           string            `json:"uploadedAt,omitempty"`
	Transactions         []TransactionMeta `json:"transactions,omitempty"`
}

type TransactionMeta struct {
	TransactionID   int64   `json:"transactionId"`
	TransactionDate string  `json:"transactionDate"`
	ValueDate       string  `json:"valueDate"`
	Description     string  `json:"description"`
	Withdrawal      float64 `json:"withdrawal"`
	Deposit         float64 `json:"deposit"`
	Balance         float64 `json:"balance"`
	Category        string  `json:"category,omitempty"`
	SubCategory     string  `json:"subCategory,omitempty"`
	Misclassified   bool    `json:"misclassified_flag,omitempty"`
}

type BankComplianceItem struct {
	Bank      string  `json:"bank"`
	Completed int64   `json:"completed"`
	Total     int64   `json:"total"`
	Percent   float64 `json:"percent"`
}

type EntityCompletenessItem struct {
	Entity  string  `json:"entity"`
	Percent float64 `json:"percent"`
}

// StatementStatusResponse is the response shape.
type StatementStatusResponse struct {
	Success            bool                     `json:"success"`
	Accounts           []AccountRow             `json:"accounts"`
	BankCompliance     []BankComplianceItem     `json:"bankCompliance"`
	EntityCompleteness []EntityCompletenessItem `json:"entityCompleteness"`
}

func ctxApprovedAccountNumbers(ctx context.Context) []string {
	v := ctx.Value("ApprovedBankAccounts")
	if v == nil {
		return nil
	}
	accounts, ok := v.([]map[string]string)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(accounts))
	for _, a := range accounts {
		acct := strings.TrimSpace(a["account_number"])
		if acct != "" {
			out = append(out, acct)
		}
	}
	return out
}

func normalizeLowerTrimSlice(in []string) []string {
	out := make([]string, 0, len(in))
	for _, s := range in {
		s = strings.ToLower(strings.TrimSpace(s))
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}

func normalizeUpperTrimSlice(in []string) []string {
	out := make([]string, 0, len(in))
	for _, s := range in {
		s = strings.ToUpper(strings.TrimSpace(s))
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}

// GetStatementStatusHandler returns a handler that computes:
//   - accounts list (with status and uploadedAt) for accounts belonging to APPROVED banks
//   - compliance per APPROVED bank (completed / total / percent)
//   - completeness per entity (considering only accounts under APPROVED banks)
func GetStatementStatusHandler(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		allowedEntityIDs := api.GetEntityIDsFromCtx(ctx)
		allowedAccountNumbers := ctxApprovedAccountNumbers(ctx)
		allowedBankNamesNorm := normalizeLowerTrimSlice(api.GetBankNamesFromCtx(ctx))
		allowedCurrencyCodesNorm := normalizeUpperTrimSlice(api.GetCurrencyCodesFromCtx(ctx))

		if len(allowedEntityIDs) == 0 || len(allowedAccountNumbers) == 0 {
			http.Error(w, constants.ErrNoAccessibleBusinessUnit, http.StatusForbidden)
			return
		}
		if len(allowedBankNamesNorm) == 0 || len(allowedCurrencyCodesNorm) == 0 {
			http.Error(w, constants.ErrNoAccessibleBusinessUnit, http.StatusForbidden)
			return
		}

		// horizon: default 14 days; if query param `days` provided and >14, use it
		days := 14
		if raw := r.URL.Query().Get("days"); raw != "" {
			if v, err := strconv.Atoi(raw); err == nil && v > 14 {
				days = v
			}
		}
		fromDate := time.Now().AddDate(0, 0, -(days - 1)).Format(constants.DateFormat)

		// 1) Accounts list: only accounts whose bank is APPROVED in auditactionbank
		accountsSQL := `
SELECT
	COALESCE(me.entity_name, '')                                    AS entity_name,
	COALESCE(mba.bank_name, mb.bank_name, '')                       AS bank_name,
	COALESCE(mba.currency, '')                                      AS currency,
	mba.account_number                                              AS account_number,
	CASE
		WHEN bs_latest.bank_statement_id IS NULL THEN '` + UploadStatusNotUploaded + `'
		ELSE COALESCE(a.processing_status, '` + UploadStatusPending + `')
	END                                                              AS status,
	bs_latest.uploaded_at                                             AS uploaded_at_ts,
	COALESCE(stmts.statements_json, '[]')                              AS statements_json
FROM public.masterbankaccount mba
-- Only consider banks whose latest auditactionbank row is APPROVED
JOIN (
		SELECT DISTINCT ON (bank_id) bank_id, processing_status
		FROM public.auditactionbank
		ORDER BY bank_id, requested_at DESC
) ab ON ab.bank_id = mba.bank_id AND ab.processing_status = 'APPROVED'
LEFT JOIN public.masterbank mb ON mba.bank_id = mb.bank_id
LEFT JOIN public.masterentitycash me ON mba.entity_id = me.entity_id
-- latest APPROVED bank_statement per account in the horizon window, ordered by statement_period_end
LEFT JOIN LATERAL (
	SELECT bs2.*
	FROM cimplrcorpsaas.bank_statements bs2
	JOIN (
		SELECT DISTINCT ON (bankstatementid) bankstatementid, processing_status
		FROM cimplrcorpsaas.auditactionbankstatement
		ORDER BY bankstatementid, requested_at DESC
	) a2 ON a2.bankstatementid = bs2.bank_statement_id AND a2.processing_status = 'APPROVED'
	WHERE bs2.account_number = mba.account_number
		AND bs2.statement_period_end >= $1::date
	ORDER BY bs2.statement_period_end DESC
	LIMIT 1
) bs_latest ON true
-- latest audit row per bank_statement_id to derive status for uploaded statements
LEFT JOIN (
	SELECT DISTINCT ON (bankstatementid) bankstatementid, processing_status
	FROM cimplrcorpsaas.auditactionbankstatement
	ORDER BY bankstatementid, requested_at DESC
) a ON a.bankstatementid = bs_latest.bank_statement_id
-- all approved statements for the account in horizon (drill-down)
LEFT JOIN LATERAL (
	SELECT json_agg(
		json_build_object(
			'statementId', bs3.bank_statement_id,
			'statementPeriodStart', to_char(bs3.statement_period_start, 'YYYY-MM-DD'),
			'statementPeriodEnd', to_char(bs3.statement_period_end, 'YYYY-MM-DD'),
			'status', COALESCE(a3.processing_status, '` + UploadStatusPending + `'),
			'uploadedAt', COALESCE(to_char(bs3.uploaded_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'), ''),
			'transactions', COALESCE(txn.tx_json, '[]')
		) ORDER BY bs3.statement_period_end DESC
	) AS statements_json
	FROM cimplrcorpsaas.bank_statements bs3
	JOIN (
		SELECT DISTINCT ON (bankstatementid) bankstatementid, processing_status
		FROM cimplrcorpsaas.auditactionbankstatement
		ORDER BY bankstatementid, requested_at DESC
	) a3 ON a3.bankstatementid = bs3.bank_statement_id AND a3.processing_status = 'APPROVED'
	LEFT JOIN LATERAL (
		SELECT json_agg(
			json_build_object(
				'transactionId', tx.transaction_id,
				'transactionDate', to_char(tx.transaction_date, 'YYYY-MM-DD'),
				'valueDate', to_char(tx.value_date, 'YYYY-MM-DD'),
				'description', COALESCE(tx.description, ''),
				'withdrawal', COALESCE(tx.withdrawal_amount, 0),
				'deposit', COALESCE(tx.deposit_amount, 0),
				'balance', COALESCE(tx.balance, 0),
				'category', COALESCE(tc.category_name, ''),
				'subCategory', COALESCE(tc.description, ''),
				'misclassified_flag', COALESCE(tx.misclassified_flag, false)
			)
			ORDER BY COALESCE(tx.transaction_date, tx.value_date), tx.transaction_id
		) AS tx_json
		FROM cimplrcorpsaas.bank_statement_transactions tx
			LEFT JOIN public.mastercashflowcategory tc
				ON tx.category_id = tc.category_id
		WHERE tx.bank_statement_id = bs3.bank_statement_id
	) txn ON true
	WHERE bs3.account_number = mba.account_number
		AND bs3.statement_period_end >= $1::date
) stmts ON true
WHERE mba.is_deleted = false
  AND mba.entity_id = ANY($2)
  AND mba.account_number = ANY($3)
  AND lower(trim(COALESCE(mba.bank_name, mb.bank_name, ''))) = ANY($4)
  AND upper(trim(COALESCE(mba.currency, ''))) = ANY($5);
`

		rows, err := pgxPool.Query(ctx, accountsSQL, fromDate, allowedEntityIDs, allowedAccountNumbers, allowedBankNamesNorm, allowedCurrencyCodesNorm)
		if err != nil {
			http.Error(w, "error querying accounts: "+err.Error(), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		var accounts []AccountRow
		for rows.Next() {
			var entityName, bankName, currency, accountNumber, status string
			var uploadedAt *time.Time
			var stmtsJSON []byte
			if err := rows.Scan(&entityName, &bankName, &currency, &accountNumber, &status, &uploadedAt, &stmtsJSON); err != nil {
				// skip row on scan error
				continue
			}
			ar := AccountRow{
				Entity:   entityName,
				Bank:     bankName,
				Currency: currency,
				Account:  accountNumber,
				Status:   status,
			}
			if uploadedAt != nil {
				ar.UploadedAt = uploadedAt.UTC().Format(time.RFC3339)
			}
			if len(stmtsJSON) > 0 {
				var stmts []StatementMeta
				if err := json.Unmarshal(stmtsJSON, &stmts); err == nil {
					ar.Statements = stmts
				}
			}
			accounts = append(accounts, ar)
		}

		// 2) Bank-wise compliance: only approved banks are included (join ab)
		bankSQL := `
SELECT
	COALESCE(mba.bank_name, mb.bank_name, '') AS bank_label,
	COUNT(DISTINCT mba.account_number) AS total,
	COUNT(DISTINCT CASE WHEN bs_latest.bank_statement_id IS NOT NULL THEN mba.account_number END) AS completed,
	CASE WHEN COUNT(DISTINCT mba.account_number) = 0 THEN 0
			 ELSE ROUND(
				 (COUNT(DISTINCT CASE WHEN bs_latest.bank_statement_id IS NOT NULL THEN mba.account_number END)::numeric 
					/ COUNT(DISTINCT mba.account_number)) * 100, 2)
	END AS percent
FROM public.masterbankaccount mba
-- only approved banks
JOIN (
		SELECT DISTINCT ON (bank_id) bank_id, processing_status
		FROM public.auditactionbank
		ORDER BY bank_id, requested_at DESC
) ab ON ab.bank_id = mba.bank_id AND ab.processing_status = 'APPROVED'
LEFT JOIN public.masterbank mb ON mba.bank_id = mb.bank_id
LEFT JOIN LATERAL (
	SELECT bs2.bank_statement_id
	FROM cimplrcorpsaas.bank_statements bs2
	JOIN (
		SELECT DISTINCT ON (bankstatementid) bankstatementid, processing_status
		FROM cimplrcorpsaas.auditactionbankstatement
		ORDER BY bankstatementid, requested_at DESC
	) a2 ON a2.bankstatementid = bs2.bank_statement_id AND a2.processing_status = 'APPROVED'
	WHERE bs2.account_number = mba.account_number
		AND bs2.statement_period_end >= $1::date
	ORDER BY bs2.statement_period_end DESC
	LIMIT 1
) bs_latest ON true
WHERE mba.is_deleted = false
  AND mba.entity_id = ANY($2)
  AND mba.account_number = ANY($3)
  AND lower(trim(COALESCE(mba.bank_name, mb.bank_name, ''))) = ANY($4)
  AND upper(trim(COALESCE(mba.currency, ''))) = ANY($5)
GROUP BY bank_label
ORDER BY percent DESC;
`

		bankRows, err := pgxPool.Query(ctx, bankSQL, fromDate, allowedEntityIDs, allowedAccountNumbers, allowedBankNamesNorm, allowedCurrencyCodesNorm)
		if err != nil {
			http.Error(w, "error querying bank compliance: "+err.Error(), http.StatusInternalServerError)
			return
		}
		defer bankRows.Close()

		var bankCompliance []BankComplianceItem
		for bankRows.Next() {
			var b BankComplianceItem
			if err := bankRows.Scan(&b.Bank, &b.Total, &b.Completed, &b.Percent); err != nil {
				continue
			}
			bankCompliance = append(bankCompliance, b)
		}

		// 3) Entity completeness: entities are evaluated only over accounts whose bank is APPROVED
		entitySQL := `
SELECT
	COALESCE(me.entity_name, '') AS entity_name,
	COUNT(DISTINCT mba.account_number) AS total,
	COUNT(DISTINCT CASE WHEN bs_latest.bank_statement_id IS NOT NULL THEN mba.account_number END) AS completed,
	CASE WHEN COUNT(DISTINCT mba.account_number) = 0 THEN 0
			 ELSE ROUND(
				 (COUNT(DISTINCT CASE WHEN bs_latest.bank_statement_id IS NOT NULL THEN mba.account_number END)::numeric 
					/ COUNT(DISTINCT mba.account_number)) * 100, 2)
	END AS percent
FROM public.masterbankaccount mba
-- only approved banks
JOIN (
		SELECT DISTINCT ON (bank_id) bank_id, processing_status
		FROM public.auditactionbank
		ORDER BY bank_id, requested_at DESC
) ab ON ab.bank_id = mba.bank_id AND ab.processing_status = 'APPROVED'
LEFT JOIN public.masterentitycash me ON mba.entity_id = me.entity_id
LEFT JOIN LATERAL (
	SELECT bs2.bank_statement_id
	FROM cimplrcorpsaas.bank_statements bs2
	JOIN (
		SELECT DISTINCT ON (bankstatementid) bankstatementid, processing_status
		FROM cimplrcorpsaas.auditactionbankstatement
		ORDER BY bankstatementid, requested_at DESC
	) a2 ON a2.bankstatementid = bs2.bank_statement_id AND a2.processing_status = 'APPROVED'
	WHERE bs2.account_number = mba.account_number
		AND bs2.statement_period_end >= $1::date
	ORDER BY bs2.statement_period_end DESC
	LIMIT 1
) bs_latest ON true
WHERE mba.is_deleted = false
  AND mba.entity_id = ANY($2)
  AND mba.account_number = ANY($3)
  AND upper(trim(COALESCE(mba.currency, ''))) = ANY($4)
GROUP BY me.entity_name
ORDER BY percent DESC;
`

		entityRows, err := pgxPool.Query(ctx, entitySQL, fromDate, allowedEntityIDs, allowedAccountNumbers, allowedCurrencyCodesNorm)
		if err != nil {
			http.Error(w, "error querying entity completeness: "+err.Error(), http.StatusInternalServerError)
			return
		}
		defer entityRows.Close()

		var entityCompleteness []EntityCompletenessItem
		for entityRows.Next() {
			var name string
			var total int64
			var completed int64
			var percent float64
			if err := entityRows.Scan(&name, &total, &completed, &percent); err != nil {
				continue
			}
			entityCompleteness = append(entityCompleteness, EntityCompletenessItem{
				Entity:  name,
				Percent: percent,
			})
		}

		resp := StatementStatusResponse{
			Success:            true,
			Accounts:           accounts,
			BankCompliance:     bankCompliance,
			EntityCompleteness: entityCompleteness,
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		enc := json.NewEncoder(w)
		enc.SetEscapeHTML(false)
		if err := enc.Encode(resp); err != nil {
			http.Error(w, "error encoding response: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}
}
