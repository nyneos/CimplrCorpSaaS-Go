package statementstatus

import (
	"CimplrCorpSaas/api/constants"
	"context"
	"encoding/json"
	"net/http"
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
	Entity     string `json:"entity"`
	Bank       string `json:"bank"`
	Currency   string `json:"currency"`
	Account    string `json:"account"`
	Status     string `json:"status"`               // UploadStatus
	UploadedAt string `json:"uploadedAt,omitempty"` // RFC3339 string, optional
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

// GetStatementStatusHandler returns a handler that computes:
//   - accounts list (with status and uploadedAt) for accounts belonging to APPROVED banks
//   - compliance per APPROVED bank (completed / total / percent)
//   - completeness per entity (considering only accounts under APPROVED banks)
func GetStatementStatusHandler(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := context.Background()

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
  bs_latest.uploaded_at                                             AS uploaded_at_ts
FROM public.masterbankaccount mba
-- Only consider banks whose latest auditactionbank row is APPROVED
JOIN (
    SELECT DISTINCT ON (bank_id) bank_id, processing_status
    FROM public.auditactionbank
    ORDER BY bank_id, requested_at DESC
) ab ON ab.bank_id = mba.bank_id AND ab.processing_status = 'APPROVED'
LEFT JOIN public.masterbank mb ON mba.bank_id = mb.bank_id
LEFT JOIN public.masterentitycash me ON mba.entity_id = me.entity_id
-- latest bank_statement per account (if any)
LEFT JOIN LATERAL (
  SELECT bs2.*
  FROM cimplrcorpsaas.bank_statements bs2
  WHERE bs2.account_number = mba.account_number
  ORDER BY bs2.uploaded_at DESC NULLS LAST
  LIMIT 1
) bs_latest ON true
-- latest audit row per bank_statement_id to derive status for uploaded statements
LEFT JOIN (
  SELECT DISTINCT ON (bankstatementid) bankstatementid, processing_status
  FROM cimplrcorpsaas.auditactionbankstatement
  ORDER BY bankstatementid, requested_at DESC
) a ON a.bankstatementid = bs_latest.bank_statement_id
WHERE mba.is_deleted = false;
`

		rows, err := pgxPool.Query(ctx, accountsSQL)
		if err != nil {
			http.Error(w, "error querying accounts: "+err.Error(), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		var accounts []AccountRow
		for rows.Next() {
			var entityName, bankName, currency, accountNumber, status string
			var uploadedAt *time.Time
			if err := rows.Scan(&entityName, &bankName, &currency, &accountNumber, &status, &uploadedAt); err != nil {
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
  WHERE bs2.account_number = mba.account_number
  ORDER BY bs2.uploaded_at DESC NULLS LAST
  LIMIT 1
) bs_latest ON true
WHERE mba.is_deleted = false
GROUP BY bank_label
ORDER BY percent DESC;
`

		bankRows, err := pgxPool.Query(ctx, bankSQL)
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
  WHERE bs2.account_number = mba.account_number
  ORDER BY bs2.uploaded_at DESC NULLS LAST
  LIMIT 1
) bs_latest ON true
WHERE mba.is_deleted = false
GROUP BY me.entity_name
ORDER BY percent DESC;
`

		entityRows, err := pgxPool.Query(ctx, entitySQL)
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
