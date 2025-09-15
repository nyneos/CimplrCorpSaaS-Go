package bankbalances

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// CreateBankBalance inserts a bank balance row and creates a CREATE audit action (PENDING_APPROVAL)
func CreateBankBalance(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID         string   `json:"user_id"`
			BalanceID      string   `json:"balance_id,omitempty"`
			BankName       string   `json:"bank_name,omitempty"`
			AccountNo      string   `json:"account_no,omitempty"`
			IBAN           string   `json:"iban,omitempty"`
			CurrencyCode   string   `json:"currency_code,omitempty"`
			Nickname       string   `json:"nickname,omitempty"`
			Country        string   `json:"country,omitempty"`
			AsOfDate       string   `json:"as_of_date,omitempty"` // YYYY-MM-DD
			AsOfTime       string   `json:"as_of_time,omitempty"` // HH:MM:SS
			BalanceType    string   `json:"balance_type,omitempty"`
			BalanceAmount  *float64 `json:"balance_amount,omitempty"`
			StatementType  string   `json:"statement_type,omitempty"`
			SourceChannel  string   `json:"source_channel,omitempty"`
			OpeningBalance *float64 `json:"opening_balance,omitempty"`
			TotalCredits   *float64 `json:"total_credits,omitempty"`
			TotalDebits    *float64 `json:"total_debits,omitempty"`
			ClosingBalance *float64 `json:"closing_balance,omitempty"`
			Reason         string   `json:"reason,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, "invalid json: "+err.Error())
			return
		}

		if req.UserID == "" {
			api.RespondWithResult(w, false, "user_id required")
			return
		}

		// validate mandatory fields: bank_name, currency_code, account_no, as_of_date, balance_type, balance_amount
		missing := []string{}
		if req.BankName == "" {
			missing = append(missing, "bank_name")
		}
		if req.CurrencyCode == "" {
			missing = append(missing, "currency_code")
		}
		if req.AccountNo == "" {
			missing = append(missing, "account_no")
		}
		if req.AsOfDate == "" {
			missing = append(missing, "as_of_date")
		}
		if req.BalanceType == "" {
			missing = append(missing, "balance_type")
		}
		if req.BalanceAmount == nil {
			missing = append(missing, "balance_amount")
		}
		if len(missing) > 0 {
			api.RespondWithResult(w, false, "missing required fields: "+fmt.Sprintf("%v", missing))
			return
		}

		// resolve user name/email from active sessions
		requestedBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				requestedBy = s.Name
				break
			}
		}
		if requestedBy == "" {
			api.RespondWithResult(w, false, "invalid user_id or session")
			return
		}

		// generate balance_id if missing
		balanceID := req.BalanceID
		if balanceID == "" {
			balanceID = fmt.Sprintf("BBAL-%d", time.Now().UnixNano()%1000000)
		}

		// parse optional as_of_date and time into proper types in SQL; pass as strings
		ins := `INSERT INTO bank_balances_manual (
            balance_id, bank_name, account_no, iban, currency_code, nickname, country,
            as_of_date, as_of_time, balance_type, balance_amount, statement_type, source_channel,
            opening_balance, total_credits, total_debits, closing_balance
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)`

		// Use Exec context
		_, err := pgxPool.Exec(ctx, ins,
			balanceID,
			nullifyEmpty(req.BankName),
			nullifyEmpty(req.AccountNo),
			nullifyEmpty(req.IBAN),
			nullifyEmpty(req.CurrencyCode),
			nullifyEmpty(req.Nickname),
			nullifyEmpty(req.Country),
			nullifyEmpty(req.AsOfDate),
			nullifyEmpty(req.AsOfTime),
			nullifyEmpty(req.BalanceType),
			nullifyFloat(req.BalanceAmount),
			nullifyEmpty(req.StatementType),
			nullifyEmpty(req.SourceChannel),
			nullifyFloat(req.OpeningBalance),
			nullifyFloat(req.TotalCredits),
			nullifyFloat(req.TotalDebits),
			nullifyFloat(req.ClosingBalance),
		)
		if err != nil {
			api.RespondWithResult(w, false, "failed to insert bank balance: "+err.Error())
			return
		}

		// insert audit action
		auditQ := `INSERT INTO auditactionbankbalances (balance_id, actiontype, processing_status, reason, requested_by, requested_at) VALUES ($1,'CREATE','PENDING_APPROVAL',$2,$3,now())`
		_, err = pgxPool.Exec(ctx, auditQ, balanceID, nullifyEmpty(req.Reason), requestedBy)
		if err != nil {
			api.RespondWithResult(w, false, "failed to create audit action: "+err.Error())
			return
		}

		api.RespondWithResult(w, true, balanceID)
	}
}

// BulkApproveBankBalances approves pending audit actions for given balance_ids.
func BulkApproveBankBalances(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID     string   `json:"user_id"`
			BalanceIDs []string `json:"balance_ids"`
			Comment    string   `json:"comment,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.BalanceIDs) == 0 {
			api.RespondWithResult(w, false, "invalid json or missing fields")
			return
		}
		checkerBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				checkerBy = s.Name
				break
			}
		}
		if checkerBy == "" {
			api.RespondWithResult(w, false, "invalid user_id or session")
			return
		}

		// Fetch latest audit per balance_id
		sel := `SELECT DISTINCT ON (balance_id) action_id, balance_id, actiontype, processing_status FROM auditactionbankbalances WHERE balance_id = ANY($1) ORDER BY balance_id, requested_at DESC`
		rows, err := pgxPool.Query(ctx, sel, req.BalanceIDs)
		if err != nil {
			api.RespondWithResult(w, false, "failed to fetch latest audits: "+err.Error())
			return
		}
		defer rows.Close()

		actionIDs := make([]string, 0)
		deleteIDs := make([]string, 0)
		found := map[string]bool{}
		for rows.Next() {
			var actionID, balanceID, actionType, procStatus string
			if err := rows.Scan(&actionID, &balanceID, &actionType, &procStatus); err != nil {
				continue
			}
			found[balanceID] = true
			actionIDs = append(actionIDs, actionID)
			if actionType == "DELETE" {
				deleteIDs = append(deleteIDs, balanceID)
			}
		}

		missing := []string{}
		for _, id := range req.BalanceIDs {
			if !found[id] {
				missing = append(missing, id)
			}
		}
		if len(missing) > 0 {
			api.RespondWithResult(w, false, fmt.Sprintf("missing audit entries for: %v", missing))
			return
		}

		// Update audit actions to APPROVED
		upd := `UPDATE auditactionbankbalances SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2 WHERE action_id = ANY($3)`
		_, err = pgxPool.Exec(ctx, upd, checkerBy, nullifyEmpty(req.Comment), actionIDs)
		if err != nil {
			api.RespondWithResult(w, false, "failed to approve actions: "+err.Error())
			return
		}

		// Delete any balances requested for delete
		deleted := []string{}
		if len(deleteIDs) > 0 {
			delQ := `DELETE FROM bank_balances_manual WHERE balance_id = ANY($1) RETURNING balance_id`
			drows, derr := pgxPool.Query(ctx, delQ, deleteIDs)
			if derr == nil {
				defer drows.Close()
				for drows.Next() {
					var id string
					drows.Scan(&id)
					deleted = append(deleted, id)
				}
			}
		}

		// return structured JSON
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "approved_count": len(actionIDs), "deleted": deleted})
	}
}

// BulkRejectBankBalances rejects latest audit actions for given balance_ids.
func BulkRejectBankBalances(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID     string   `json:"user_id"`
			BalanceIDs []string `json:"balance_ids"`
			Comment    string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.BalanceIDs) == 0 {
			api.RespondWithResult(w, false, "invalid json or missing fields")
			return
		}
		checkerBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				checkerBy = s.Name
				break
			}
		}
		if checkerBy == "" {
			api.RespondWithResult(w, false, "invalid user_id or session")
			return
		}

		sel := `SELECT DISTINCT ON (balance_id) action_id, balance_id FROM auditactionbankbalances WHERE balance_id = ANY($1) ORDER BY balance_id, requested_at DESC`
		rows, err := pgxPool.Query(ctx, sel, req.BalanceIDs)
		if err != nil {
			api.RespondWithResult(w, false, "failed to fetch latest audits: "+err.Error())
			return
		}
		defer rows.Close()

		actionIDs := make([]string, 0)
		found := map[string]bool{}
		for rows.Next() {
			var actionID string
			var balanceID string
			if err := rows.Scan(&actionID, &balanceID); err != nil {
				continue
			}
			found[balanceID] = true
			actionIDs = append(actionIDs, actionID)
		}
		missing := []string{}
		for _, id := range req.BalanceIDs {
			if !found[id] {
				missing = append(missing, id)
			}
		}
		if len(missing) > 0 {
			api.RespondWithResult(w, false, fmt.Sprintf("missing audit entries for: %v", missing))
			return
		}

		upd := `UPDATE auditactionbankbalances SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2 WHERE action_id = ANY($3)`
		_, err = pgxPool.Exec(ctx, upd, checkerBy, nullifyEmpty(req.Comment), actionIDs)
		if err != nil {
			api.RespondWithResult(w, false, "failed to reject actions: "+err.Error())
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "rejected_count": len(actionIDs)})
	}
}

// BulkRequestDeleteBankBalances inserts DELETE audit actions (PENDING_DELETE_APPROVAL) for balances
func BulkRequestDeleteBankBalances(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID     string   `json:"user_id"`
			BalanceIDs []string `json:"balance_ids"`
			Reason     string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.BalanceIDs) == 0 {
			api.RespondWithResult(w, false, "invalid json or missing fields")
			return
		}
		requestedBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				requestedBy = s.Name
				break
			}
		}
		if requestedBy == "" {
			api.RespondWithResult(w, false, "invalid user_id or session")
			return
		}

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithResult(w, false, "failed to begin tx: "+err.Error())
			return
		}
		committed := false
		defer func() {
			if !committed {
				tx.Rollback(ctx)
			}
		}()

		ins := `INSERT INTO auditactionbankbalances (balance_id, actiontype, processing_status, reason, requested_by, requested_at) VALUES ($1,'DELETE','PENDING_DELETE_APPROVAL',$2,$3,now())`
		for _, id := range req.BalanceIDs {
			if _, err := tx.Exec(ctx, ins, id, nullifyEmpty(req.Reason), requestedBy); err != nil {
				api.RespondWithResult(w, false, "failed to create delete audit: "+err.Error())
				return
			}
		}
		if err := tx.Commit(ctx); err != nil {
			api.RespondWithResult(w, false, "failed to commit: "+err.Error())
			return
		}
		committed = true

		api.RespondWithResult(w, true, fmt.Sprintf("created %d delete requests", len(req.BalanceIDs)))
	}
}

// helper: convert empty string to nil interface for Exec args
func nullifyEmpty(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

// GetBankBalances returns bank_balances_manual rows joined with latest auditactionbankbalances
func GetBankBalances(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, "Invalid JSON: "+err.Error())
			return
		}
		if req.UserID == "" {
			api.RespondWithResult(w, false, "Missing user_id in body")
			return
		}

		// verify session
		valid := false
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				valid = true
				break
			}
		}
		if !valid {
			api.RespondWithResult(w, false, "Invalid user_id or session")
			return
		}

		// Build query: select all bank balances and include latest audit via LEFT JOIN LATERAL
		base := `
			SELECT b.balance_id, b.bank_name, b.account_no, b.iban, b.currency_code, b.nickname, b.country,
				   b.as_of_date, b.as_of_time, b.balance_type, b.balance_amount, b.statement_type, b.source_channel,
				   b.opening_balance, b.total_credits, b.total_debits, b.closing_balance,
				   b.old_bank_name, b.old_account_no, b.old_iban, b.old_currency_code, b.old_nickname,
				   b.old_as_of_date, b.old_as_of_time, b.old_balance_type, b.old_balance_amount, b.old_statement_type, b.old_source_channel,
				   b.old_opening_balance, b.old_total_credits, b.old_total_debits, b.old_closing_balance
			FROM bank_balances_manual b
			ORDER BY b.as_of_date DESC, b.balance_id
		`

		rows, err := pgxPool.Query(ctx, base)
		if err != nil {
			api.RespondWithResult(w, false, "DB error: "+err.Error())
			return
		}
		defer rows.Close()

		out := make([]map[string]interface{}, 0)
		for rows.Next() {
			var (
				balanceID, bankName, accountNo, iban, currency, nickname, country sqlNullString
				asOfDate                                                          sqlNullTime
				asOfTime                                                          sqlNullString
				balanceType                                                       sqlNullString
				balanceAmount                                                     sqlNullFloat
				statementType, sourceChannel                                      sqlNullString
				opening, credits, debits, closing                                 sqlNullFloat
				oldBankName, oldAccountNo, oldIban, oldCurrency, oldNickname      sqlNullString
				oldAsOfDate                                                       sqlNullTime
				oldAsOfTime                                                       sqlNullString
				oldBalanceType                                                    sqlNullString
				oldBalanceAmount, oldOpening, oldCredits, oldDebits, oldClosing   sqlNullFloat
				oldStatementType, oldSourceChannel                                sqlNullString
			)
			// use Scan with many nullable types
			err := rows.Scan(&balanceID, &bankName, &accountNo, &iban, &currency, &nickname, &country,
				&asOfDate, &asOfTime, &balanceType, &balanceAmount, &statementType, &sourceChannel,
				&opening, &credits, &debits, &closing,
				&oldBankName, &oldAccountNo, &oldIban, &oldCurrency, &oldNickname,
				&oldAsOfDate, &oldAsOfTime, &oldBalanceType, &oldBalanceAmount, &oldStatementType, &oldSourceChannel,
				&oldOpening, &oldCredits, &oldDebits, &oldClosing)
			if err != nil {
				continue
			}
			// Fetch latest single audit row (processing_status, requested_by, requested_at, actiontype, action_id, checker_by, checker_at, checker_comment, reason)
			auditLatest := `SELECT processing_status, requested_by, requested_at, actiontype, action_id, checker_by, checker_at, checker_comment, reason FROM auditactionbankbalances WHERE balance_id = $1 ORDER BY requested_at DESC LIMIT 1`
			var processingStatusPtr, requestedByPtr, actionTypePtr, actionIDPtr, checkerByPtr, checkerCommentPtr, reasonPtr *string
			var requestedAtPtr, checkerAtPtr *time.Time
			_ = pgxPool.QueryRow(ctx, auditLatest, balanceID.ValueOrZero()).Scan(&processingStatusPtr, &requestedByPtr, &requestedAtPtr, &actionTypePtr, &actionIDPtr, &checkerByPtr, &checkerAtPtr, &checkerCommentPtr, &reasonPtr)

			// Then fetch recent CREATE/EDIT/DELETE entries to build created/edited/deleted summary (use api.GetAuditInfo)
			auditDetailsQuery := `SELECT actiontype, requested_by, requested_at FROM auditactionbankbalances WHERE balance_id = $1 AND actiontype IN ('CREATE','EDIT','DELETE') ORDER BY requested_at DESC`
			auditRows, auditErr := pgxPool.Query(ctx, auditDetailsQuery, balanceID.ValueOrZero())
			var createdBy, createdAt, editedBy, editedAt, deletedBy, deletedAt string
			if auditErr == nil {
				defer auditRows.Close()
				for auditRows.Next() {
					var atype string
					var rbyPtr *string
					var ratPtr *time.Time
					if err := auditRows.Scan(&atype, &rbyPtr, &ratPtr); err == nil {
						auditInfo := api.GetAuditInfo(atype, rbyPtr, ratPtr)
						if atype == "CREATE" && createdBy == "" {
							createdBy = auditInfo.CreatedBy
							createdAt = auditInfo.CreatedAt
						} else if atype == "EDIT" && editedBy == "" {
							editedBy = auditInfo.EditedBy
							editedAt = auditInfo.EditedAt
						} else if atype == "DELETE" && deletedBy == "" {
							deletedBy = auditInfo.DeletedBy
							deletedAt = auditInfo.DeletedAt
						}
					}
				}
			}

			m := map[string]interface{}{
				"balance_id":          balanceID.ValueOrZero(),
				"bank_name":           bankName.ValueOrZero(),
				"account_no":          accountNo.ValueOrZero(),
				"iban":                iban.ValueOrZero(),
				"currency_code":       currency.ValueOrZero(),
				"nickname":            nickname.ValueOrZero(),
				"country":             country.ValueOrZero(),
				"as_of_date":          asOfDate.ValueOrZero(),
				"as_of_time":          asOfTime.ValueOrZero(),
				"balance_type":        balanceType.ValueOrZero(),
				"balance_amount":      balanceAmount.ValueOrZero(),
				"statement_type":      statementType.ValueOrZero(),
				"source_channel":      sourceChannel.ValueOrZero(),
				"opening_balance":     opening.ValueOrZero(),
				"total_credits":       credits.ValueOrZero(),
				"total_debits":        debits.ValueOrZero(),
				"closing_balance":     closing.ValueOrZero(),
				"old_bank_name":       oldBankName.ValueOrZero(),
				"old_account_no":      oldAccountNo.ValueOrZero(),
				"old_iban":            oldIban.ValueOrZero(),
				"old_currency_code":   oldCurrency.ValueOrZero(),
				"old_nickname":        oldNickname.ValueOrZero(),
				"old_as_of_date":      oldAsOfDate.ValueOrZero(),
				"old_as_of_time":      oldAsOfTime.ValueOrZero(),
				"old_balance_type":    oldBalanceType.ValueOrZero(),
				"old_balance_amount":  oldBalanceAmount.ValueOrZero(),
				"old_statement_type":  oldStatementType.ValueOrZero(),
				"old_source_channel":  oldSourceChannel.ValueOrZero(),
				"old_opening_balance": oldOpening.ValueOrZero(),
				"old_total_credits":   oldCredits.ValueOrZero(),
				"old_total_debits":    oldDebits.ValueOrZero(),
				"old_closing_balance": oldClosing.ValueOrZero(),
				"processing_status": func() string {
					if processingStatusPtr != nil {
						return *processingStatusPtr
					}
					return ""
				}(),
				"action_type": func() string {
					if actionTypePtr != nil {
						return *actionTypePtr
					}
					return ""
				}(),
				"action_id": func() string {
					if actionIDPtr != nil {
						return *actionIDPtr
					}
					return ""
				}(),
				// "requested_by": api.GetAuditInfo("", requestedByPtr, requestedAtPtr).CreatedBy,
				// "requested_at": api.GetAuditInfo("", requestedByPtr, requestedAtPtr).CreatedAt,
				"checker_by": api.GetAuditInfo("", checkerByPtr, checkerAtPtr).CreatedBy,
				"checker_at": api.GetAuditInfo("", checkerByPtr, checkerAtPtr).CreatedAt,
				"checker_comment": func() string {
					if checkerCommentPtr != nil {
						return *checkerCommentPtr
					}
					return ""
				}(),
				"reason": func() string {
					if reasonPtr != nil {
						return *reasonPtr
					}
					return ""
				}(),
				"created_by": createdBy,
				"created_at": createdAt,
				"edited_by":  editedBy,
				"edited_at":  editedAt,
				"deleted_by": deletedBy,
				"deleted_at": deletedAt,
			}
			out = append(out, m)
		}
		if rows.Err() != nil {
			api.RespondWithResult(w, false, "DB rows error: "+rows.Err().Error())
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "rows": out})
	}
}

// small nullable helpers (lightweight, limited methods) to avoid importing database/sql everywhere
type sqlNullString struct {
	Valid bool
	S     string
}

func (n *sqlNullString) Scan(v interface{}) error {
	if v == nil {
		n.Valid = false
		n.S = ""
		return nil
	}
	switch t := v.(type) {
	case string:
		n.Valid = true
		n.S = t
	case []byte:
		n.Valid = true
		n.S = string(t)
	default:
		n.Valid = true
		n.S = fmt.Sprint(v)
	}
	return nil
}
func (n sqlNullString) ValueOrZero() interface{} {
	if n.Valid {
		return n.S
	}
	return ""
}

type sqlNullFloat struct {
	Valid bool
	F     float64
}

func (n *sqlNullFloat) Scan(v interface{}) error {
	if v == nil {
		n.Valid = false
		n.F = 0
		return nil
	}
	switch t := v.(type) {
	case float64:
		n.Valid = true
		n.F = t
	case int64:
		n.Valid = true
		n.F = float64(t)
	case float32:
		n.Valid = true
		n.F = float64(t)
	case int:
		n.Valid = true
		n.F = float64(t)
	case uint64:
		n.Valid = true
		n.F = float64(t)
	case []byte:
		s := string(t)
		if s == "" {
			n.Valid = false
			n.F = 0
			return nil
		}
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			n.Valid = false
			n.F = 0
			return nil
		}
		n.Valid = true
		n.F = f
	case string:
		if t == "" {
			n.Valid = false
			n.F = 0
			return nil
		}
		f, err := strconv.ParseFloat(t, 64)
		if err != nil {
			n.Valid = false
			n.F = 0
			return nil
		}
		n.Valid = true
		n.F = f
	default:
		n.Valid = true
		n.F = 0
	}
	return nil
}
func (n sqlNullFloat) ValueOrZero() interface{} {
	if n.Valid {
		return n.F
	} else {
		return nil
	}
}

type sqlNullTime struct {
	Valid bool
	T     time.Time
}

func (n *sqlNullTime) Scan(v interface{}) error {
	if v == nil {
		n.Valid = false
		return nil
	}
	switch t := v.(type) {
	case time.Time:
		n.Valid = true
		n.T = t
	case *time.Time:
		if t != nil {
			n.Valid = true
			n.T = *t
		} else {
			n.Valid = false
		}
	default:
		n.Valid = false
	}
	return nil
}
func (n sqlNullTime) ValueOrZero() interface{} {
	if n.Valid {
		return n.T.Format("2006-01-02")
	}
	return ""
}

func nullifyFloat(f *float64) interface{} {
	if f == nil {
		return nil
	}
	return *f
}
// UpdateBankBalance updates allowed fields for a specific balance_id, copies existing values into old_* columns and creates an EDIT audit action (PENDING_EDIT_APPROVAL)
func UpdateBankBalance(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID    string                 `json:"user_id"`
			BalanceID string                 `json:"balance_id"`
			Fields    map[string]interface{} `json:"fields"`
			Reason    string                 `json:"reason,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, "invalid json: "+err.Error())
			return
		}
		if req.UserID == "" || req.BalanceID == "" {
			api.RespondWithResult(w, false, "user_id and balance_id required")
			return
		}

		// resolve requested_by name
		requestedBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				requestedBy = s.Name
				break
			}
		}
		if requestedBy == "" {
			api.RespondWithResult(w, false, "invalid user_id or session")
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithResult(w, false, "failed to begin tx: "+err.Error())
			return
		}
		defer func() {
			if tx != nil {
				tx.Rollback(ctx)
			}
		}()

		// fetch current row FOR UPDATE
		sel := `SELECT bank_name, account_no, iban, currency_code, nickname, country, as_of_date, as_of_time, balance_type, balance_amount, statement_type, source_channel, opening_balance, total_credits, total_debits, closing_balance FROM bank_balances_manual WHERE balance_id=$1 FOR UPDATE`
		var (
			curBankName, curAccountNo, curIban, curCurrency, curNickname, curCountry sqlNullString
			curAsOfDate                                                              sqlNullTime
			curAsOfTime, curBalanceType, curStatementType, curSourceChannel          sqlNullString
			curBalanceAmount, curOpening, curCredits, curDebits, curClosing          sqlNullFloat
		)
		if err := tx.QueryRow(ctx, sel, req.BalanceID).Scan(
			&curBankName, &curAccountNo, &curIban, &curCurrency, &curNickname, &curCountry,
			&curAsOfDate, &curAsOfTime, &curBalanceType, &curBalanceAmount, &curStatementType, &curSourceChannel,
			&curOpening, &curCredits, &curDebits, &curClosing,
		); err != nil {
			api.RespondWithResult(w, false, "failed to fetch existing balance: "+err.Error())
			return
		}

		// build update sets and args; when updating a field, set old_<field>=current_value
		sets := []string{}
		args := []interface{}{}
		pos := 1

		// helper to append set and old set
		addStrField := func(col, oldcol string, val interface{}, cur sqlNullString) {
			sets = append(sets, fmt.Sprintf("%s=$%d, %s=$%d", col, pos, oldcol, pos+1))
			args = append(args, nullifyEmpty(fmt.Sprint(val)))
			args = append(args, cur.ValueOrZero())
			pos += 2
		}
		addFloatField := func(col, oldcol string, val interface{}, cur sqlNullFloat) {
			sets = append(sets, fmt.Sprintf("%s=$%d, %s=$%d", col, pos, oldcol, pos+1))
			// val may be float64 or nil
			if val == nil {
				args = append(args, nil)
			} else {
				args = append(args, val)
			}
			if cur.Valid {
				args = append(args, cur.F)
			} else {
				args = append(args, nil)
			}
			pos += 2
		}

		// Allowed fields mapping
		for k, v := range req.Fields {
			switch k {
			case "bank_name":
				addStrField("bank_name", "old_bank_name", v, curBankName)
			case "account_no":
				addStrField("account_no", "old_account_no", v, curAccountNo)
			case "iban":
				addStrField("iban", "old_iban", v, curIban)
			case "currency_code":
				addStrField("currency_code", "old_currency_code", v, curCurrency)
			case "nickname":
				addStrField("nickname", "old_nickname", v, curNickname)
			case "country":
				addStrField("country", "old_country", v, curCountry)
			case "as_of_date":
				// date string
				sets = append(sets, fmt.Sprintf("as_of_date=$%d, old_as_of_date=$%d", pos, pos+1))
				args = append(args, nullifyEmpty(fmt.Sprint(v)))
				args = append(args, curAsOfDate.ValueOrZero())
				pos += 2
			case "as_of_time":
				addStrField("as_of_time", "old_as_of_time", v, curAsOfTime)
			case "balance_type":
				addStrField("balance_type", "old_balance_type", v, curBalanceType)
			case "balance_amount":
				addFloatField("balance_amount", "old_balance_amount", v, curBalanceAmount)
			case "statement_type":
				addStrField("statement_type", "old_statement_type", v, curStatementType)
			case "source_channel":
				addStrField("source_channel", "old_source_channel", v, curSourceChannel)
			case "opening_balance":
				addFloatField("opening_balance", "old_opening_balance", v, curOpening)
			case "total_credits":
				addFloatField("total_credits", "old_total_credits", v, curCredits)
			case "total_debits":
				addFloatField("total_debits", "old_total_debits", v, curDebits)
			case "closing_balance":
				addFloatField("closing_balance", "old_closing_balance", v, curClosing)
			default:
				// ignore unknown fields
			}
		}

		if len(sets) == 0 {
			api.RespondWithResult(w, false, "no valid fields to update")
			return
		}

		// build update query
		q := "UPDATE bank_balances_manual SET " + strings.Join(sets, ", ") + fmt.Sprintf(" WHERE balance_id=$%d", pos)
		args = append(args, req.BalanceID)

		if _, err := tx.Exec(ctx, q, args...); err != nil {
			api.RespondWithResult(w, false, "failed to update balance: "+err.Error())
			return
		}

		// insert audit action
		auditQ := `INSERT INTO auditactionbankbalances (balance_id, actiontype, processing_status, reason, requested_by, requested_at) VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL',$2,$3,now())`
		if _, err := tx.Exec(ctx, auditQ, req.BalanceID, nullifyEmpty(req.Reason), requestedBy); err != nil {
			api.RespondWithResult(w, false, "failed to create audit action: "+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithResult(w, false, "failed to commit: "+err.Error())
			return
		}
		// clear tx rollback defer
		tx = nil

		api.RespondWithResult(w, true, req.BalanceID)
	}
}
