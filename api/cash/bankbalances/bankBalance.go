package bankbalances

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	middlewares "CimplrCorpSaas/api/middlewares"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"CimplrCorpSaas/api/constants"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func pgUserFriendlyMessage(err error) string {
	if err == nil {
		return ""
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return err.Error()
	}
	switch pgErr.Code {
	case "23505":
		return "A record with the same unique value already exists."
	case "23503":
		return "Some referenced data was not found (please refresh and try again)."
	case "23514":
		return "Some fields have invalid values. Please check and try again."
	default:
		return "Database error while processing the request. Please try again."
	}
}

func requestedByFromCtx(ctx context.Context, userID string) string {
	if s := middlewares.GetSessionFromContext(ctx); s != nil {
		if strings.TrimSpace(s.Name) != "" {
			return s.Name
		}
		if strings.TrimSpace(s.UserID) != "" {
			return s.UserID
		}
	}
	for _, s := range auth.GetActiveSessions() {
		if s.UserID == userID {
			return s.Name
		}
	}
	return ""
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
		if strings.TrimSpace(a["account_number"]) != "" {
			out = append(out, strings.TrimSpace(a["account_number"]))
		}
	}
	return out
}

// ctxApprovedBankNames returns approved bank names from middleware context (if present)
func ctxApprovedBankNames(ctx context.Context) []string {
	v := ctx.Value("BankInfo")
	if v == nil {
		return nil
	}
	banks, ok := v.([]map[string]string)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(banks))
	for _, b := range banks {
		if s := strings.TrimSpace(b["bank_name"]); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// ctxApprovedCurrencies returns approved currency codes from middleware context (if present)
func ctxApprovedCurrencies(ctx context.Context) []string {
	v := ctx.Value("CurrencyInfo")
	if v == nil {
		// fallback to a generic ApprovedCurrencies key
		v = ctx.Value("ApprovedCurrencies")
	}
	if v == nil {
		return nil
	}
	curList, ok := v.([]map[string]string)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(curList))
	for _, c := range curList {
		if s := strings.TrimSpace(c["currency_code"]); s != "" {
			out = append(out, s)
		}
	}
	return out
}

func ctxHasApprovedCurrency(ctx context.Context, currency string) bool {
	currency = strings.TrimSpace(currency)
	if currency == "" {
		return false
	}
	v := ctx.Value("CurrencyInfo")
	if v == nil {
		v = ctx.Value("ApprovedCurrencies")
	}
	if v == nil {
		return true
	}
	curList, ok := v.([]map[string]string)
	if !ok {
		return true
	}
	for _, c := range curList {
		if strings.EqualFold(strings.TrimSpace(c["currency_code"]), currency) {
			return true
		}
	}
	return false
}

func ctxHasApprovedBankAccount(ctx context.Context, accountNumber string) bool {
	accountNumber = strings.TrimSpace(accountNumber)
	if accountNumber == "" {
		return false
	}
	v := ctx.Value("ApprovedBankAccounts")
	if v == nil {
		return true
	}
	accounts, ok := v.([]map[string]string)
	if !ok {
		return true
	}
	for _, a := range accounts {
		if strings.EqualFold(strings.TrimSpace(a["account_number"]), accountNumber) {
			return true
		}
	}
	return false
}

func ctxHasApprovedBankName(ctx context.Context, bankName string) bool {
	bankName = strings.TrimSpace(bankName)
	if bankName == "" {
		return false
	}
	v := ctx.Value("BankInfo")
	if v == nil {
		return true
	}
	banks, ok := v.([]map[string]string)
	if !ok {
		return true
	}
	for _, b := range banks {
		if strings.EqualFold(strings.TrimSpace(b["bank_name"]), bankName) ||
			strings.EqualFold(strings.TrimSpace(b["bank_short_name"]), bankName) ||
			strings.EqualFold(strings.TrimSpace(b["bank_id"]), bankName) {
			return true
		}
	}
	return false
}

func ctxEntityIDs(ctx context.Context) []string {
	// prefer the key used by the prevalidation middleware
	v := ctx.Value(api.EntityIDsKey)
	if v == nil {
		// fall back to the literal key for compatibility
		v = ctx.Value("entityIDs")
	}
	if v == nil {
		return nil
	}
	entityIDs, ok := v.([]string)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(entityIDs))
	for _, e := range entityIDs {
		if strings.TrimSpace(e) != "" {
			out = append(out, strings.TrimSpace(e))
		}
	}
	return out
}

func ensureBalanceIDsAccessible(ctx context.Context, pgxPool *pgxpool.Pool, balanceIDs []string) (int, string) {
	if len(balanceIDs) == 0 {
		return 0, ""
	}
	if ctx.Value("ApprovedBankAccounts") == nil {
		return 0, ""
	}
	rows, err := pgxPool.Query(ctx, `SELECT balance_id, account_no FROM bank_balances_manual WHERE balance_id = ANY($1)`, balanceIDs)
	if err != nil {
		return http.StatusInternalServerError, pgUserFriendlyMessage(err)
	}
	defer rows.Close()
	found := map[string]string{}
	for rows.Next() {
		var id, acct string
		if err := rows.Scan(&id, &acct); err == nil {
			found[id] = acct
		}
	}
	for _, id := range balanceIDs {
		acct, ok := found[id]
		if !ok {
			return http.StatusBadRequest, "Invalid balance_id: " + id
		}
		if !ctxHasApprovedBankAccount(ctx, acct) {
			return http.StatusForbidden, constants.ErrInvalidAccount
		}
	}
	return 0, ""
}

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
			api.RespondWithResult(w, false, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}

		if req.UserID == "" {
			api.RespondWithResult(w, false, constants.ErrUserIDRequired)
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

		if !ctxHasApprovedBankName(ctx, req.BankName) {
			api.RespondWithResult(w, false, constants.ErrBankInvalidOrInactive)
			return
		}
		if !ctxHasApprovedBankAccount(ctx, req.AccountNo) {
			api.RespondWithResult(w, false, constants.ErrInvalidAccount)
			return
		}

		// currency validation (middleware may provide approved currencies)
		if !ctxHasApprovedCurrency(ctx, req.CurrencyCode) {
			api.RespondWithResult(w, false, "Invalid or inactive currency")
			return
		}

		// resolve user name/email from prevalidated session (fallback: active sessions)
		requestedBy := requestedByFromCtx(ctx, req.UserID)
		if requestedBy == "" {
			api.RespondWithResult(w, false, constants.ErrInvalidSession)
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
			api.RespondWithResult(w, false, "failed to insert bank balance: "+pgUserFriendlyMessage(err))
			return
		}

		// insert audit action
		auditQ := `INSERT INTO auditactionbankbalances (balance_id, actiontype, processing_status, reason, requested_by, requested_at) VALUES ($1,'CREATE','PENDING_APPROVAL',$2,$3,now())`
		_, err = pgxPool.Exec(ctx, auditQ, balanceID, nullifyEmpty(req.Reason), requestedBy)
		if err != nil {
			api.RespondWithResult(w, false, "failed to create audit action: "+pgUserFriendlyMessage(err))
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
			api.RespondWithResult(w, false, constants.ErrInvalidJSON)
			return
		}
		checkerBy := requestedByFromCtx(ctx, req.UserID)
		if checkerBy == "" {
			api.RespondWithResult(w, false, constants.ErrInvalidSession)
			return
		}
		if code, msg := ensureBalanceIDsAccessible(ctx, pgxPool, req.BalanceIDs); code != 0 {
			api.RespondWithError(w, code, msg)
			return
		}

		// Fetch latest audit per balance_id
		sel := `SELECT DISTINCT ON (balance_id) action_id, balance_id, actiontype, processing_status FROM auditactionbankbalances WHERE balance_id = ANY($1) ORDER BY balance_id, requested_at DESC`
		rows, err := pgxPool.Query(ctx, sel, req.BalanceIDs)
		if err != nil {
			api.RespondWithResult(w, false, "failed to fetch latest audits: "+pgUserFriendlyMessage(err))
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
			api.RespondWithResult(w, false, "failed to approve actions: "+pgUserFriendlyMessage(err))
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
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: true, "approved_count": len(actionIDs), "deleted": deleted})
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
			api.RespondWithResult(w, false, constants.ErrInvalidJSON)
			return
		}
		checkerBy := requestedByFromCtx(ctx, req.UserID)
		if checkerBy == "" {
			api.RespondWithResult(w, false, constants.ErrInvalidSession)
			return
		}
		if code, msg := ensureBalanceIDsAccessible(ctx, pgxPool, req.BalanceIDs); code != 0 {
			api.RespondWithError(w, code, msg)
			return
		}

		sel := `SELECT DISTINCT ON (balance_id) action_id, balance_id FROM auditactionbankbalances WHERE balance_id = ANY($1) ORDER BY balance_id, requested_at DESC`
		rows, err := pgxPool.Query(ctx, sel, req.BalanceIDs)
		if err != nil {
			api.RespondWithResult(w, false, "failed to fetch latest audits: "+pgUserFriendlyMessage(err))
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
			api.RespondWithResult(w, false, "failed to reject actions: "+pgUserFriendlyMessage(err))
			return
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: true, "rejected_count": len(actionIDs)})
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
			api.RespondWithResult(w, false, constants.ErrInvalidJSON)
			return
		}
		requestedBy := requestedByFromCtx(ctx, req.UserID)
		if requestedBy == "" {
			api.RespondWithResult(w, false, constants.ErrInvalidSession)
			return
		}
		if code, msg := ensureBalanceIDsAccessible(ctx, pgxPool, req.BalanceIDs); code != 0 {
			api.RespondWithError(w, code, msg)
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
				api.RespondWithResult(w, false, "failed to create delete audit: "+pgUserFriendlyMessage(err))
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
			api.RespondWithResult(w, false, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}
		if req.UserID == "" {
			api.RespondWithResult(w, false, "Missing user_id in body")
			return
		}

		if middlewares.GetSessionFromContext(ctx) == nil {
			api.RespondWithResult(w, false, constants.ErrInvalidSessionCapitalized)
			return
		}

		accountNos := ctxApprovedAccountNumbers(ctx)
		entityIDs := ctxEntityIDs(ctx)

		bankNames := ctxApprovedBankNames(ctx)
		currencyCodes := ctxApprovedCurrencies(ctx)

		// context values captured; debugging prints removed

		// Build query: select all bank balances
		baseSelect := `
			SELECT b.balance_id, b.bank_name, b.account_no, b.iban, b.currency_code, b.nickname, b.country,
				   b.as_of_date, b.as_of_time, b.balance_type, b.balance_amount, b.statement_type, b.source_channel,
				   b.opening_balance, b.total_credits, b.total_debits, b.closing_balance,
				   b.old_bank_name, b.old_account_no, b.old_iban, b.old_currency_code, b.old_nickname,
				   b.old_as_of_date, b.old_as_of_time, b.old_balance_type, b.old_balance_amount, b.old_statement_type, b.old_source_channel,
				   b.old_opening_balance, b.old_total_credits, b.old_total_debits, b.old_closing_balance,
			   COALESCE(ec.entity_name, me.entity_name, '') AS entity_name
		   FROM bank_balances_manual b
		   JOIN masterbankaccount mba ON b.account_no = mba.account_number
		   LEFT JOIN public.masterentitycash ec ON mba.entity_id = ec.entity_id
		   LEFT JOIN public.masterentity me ON me.entity_id::text = mba.entity_id
		`
		var rows pgx.Rows
		var err error
		whereClauses := []string{}
		args := []interface{}{}
		pos := 1
		if len(entityIDs) > 0 {
			// If the values look like business unit keys (entity names) then
			// apply an inclusive filter on the joined masterentity `me.entity_name`.
			// Otherwise preserve the existing behaviour (exclude by entity_id).
			isNameList := true
			for _, e := range entityIDs {
				ee := strings.TrimSpace(e)
				if ee == "" {
					continue
				}
				// IDs in our system typically start with 'EC-' or are UUIDs/numeric; treat those as IDs
				if strings.HasPrefix(strings.ToUpper(ee), "EC-") || strings.Contains(ee, "-") {
					isNameList = false
					break
				}
			}
			if isNameList {
				// include only rows whose entity name is in the provided list
				// use case-insensitive matching by lowercasing both sides
				whereClauses = append(whereClauses, fmt.Sprintf("LOWER(me.entity_name) = ANY($%d)", pos))
				// prepare lowercased values for the query arg
				lowerNames := make([]string, 0, len(entityIDs))
				for _, e2 := range entityIDs {
					if s := strings.TrimSpace(e2); s != "" {
						lowerNames = append(lowerNames, strings.ToLower(s))
					}
				}
				args = append(args, lowerNames)
			} else {
				// existing behaviour: include only rows for the provided entity IDs
				// middleware provides entity IDs that the user has access to, so we should
				// limit results to those IDs rather than excluding them.
				whereClauses = append(whereClauses, fmt.Sprintf("mba.entity_id = ANY($%d)", pos))
				args = append(args, entityIDs)
			}
			pos++
			// If middleware provided approved bank names, filter by bank_name (case-insensitive)
			if len(bankNames) > 0 {
				whereClauses = append(whereClauses, fmt.Sprintf("LOWER(b.bank_name) = ANY($%d)", pos))
				lowerNames := make([]string, 0, len(bankNames))
				for _, bn := range bankNames {
					if s := strings.TrimSpace(bn); s != "" {
						lowerNames = append(lowerNames, strings.ToLower(s))
					}
				}
				args = append(args, lowerNames)
				pos++
			}

			if len(accountNos) > 0 {
				whereClauses = append(whereClauses, fmt.Sprintf("b.account_no = ANY($%d)", pos))
				args = append(args, accountNos)
				pos++
			}

			// If middleware provided approved currency codes, filter by currency_code (case-insensitive)
			if len(currencyCodes) > 0 {
				whereClauses = append(whereClauses, fmt.Sprintf("LOWER(b.currency_code) = ANY($%d)", pos))
				lowerCur := make([]string, 0, len(currencyCodes))
				for _, cc := range currencyCodes {
					if s := strings.TrimSpace(cc); s != "" {
						lowerCur = append(lowerCur, strings.ToLower(s))
					}
				}
				args = append(args, lowerCur)
				pos++
			}
			whereStr := ""
			if len(whereClauses) > 0 {
				whereStr = " WHERE " + strings.Join(whereClauses, " AND ")
			}

			// WHERE clause constructed; debugging prints removed
			query := baseSelect + whereStr + " ORDER BY b.as_of_date DESC, b.balance_id"
			rows, err = pgxPool.Query(ctx, query, args...)
			if err != nil {
				api.RespondWithResult(w, false, constants.ErrDBPrefix+err.Error())
				return
			}
			defer rows.Close()

			balanceIDs := make([]string, 0)
			partialRows := make([]map[string]interface{}, 0)
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
					entityName                                                        sqlNullString
				)
				// use Scan with many nullable types
				err := rows.Scan(&balanceID, &bankName, &accountNo, &iban, &currency, &nickname, &country,
					&asOfDate, &asOfTime, &balanceType, &balanceAmount, &statementType, &sourceChannel,
					&opening, &credits, &debits, &closing,
					&oldBankName, &oldAccountNo, &oldIban, &oldCurrency, &oldNickname,
					&oldAsOfDate, &oldAsOfTime, &oldBalanceType, &oldBalanceAmount, &oldStatementType, &oldSourceChannel,
					&oldOpening, &oldCredits, &oldDebits, &oldClosing, &entityName)
				if err != nil {
					continue
				}
				bid := balanceID.ValueOrZero().(string)
				balanceIDs = append(balanceIDs, bid)

				m := map[string]interface{}{
					"balance_id":          bid,
					"bank_name":           bankName.ValueOrZero(),
					"account_no":          accountNo.ValueOrZero(),
					"entity_name":         entityName.ValueOrZero(),
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
					"processing_status":   "",
					"action_type":         "",
					"action_id":           "",
					"checker_by":          "",
					"checker_at":          "",
					"checker_comment":     "",
					"reason":              "",
					"created_by":          "",
					"created_at":          "",
					"edited_by":           "",
					"edited_at":           "",
					"deleted_by":          "",
					"deleted_at":          "",
				}
				partialRows = append(partialRows, m)
			}

			if rows.Err() != nil {
				api.RespondWithResult(w, false, "DB rows error: "+rows.Err().Error())
				return
			}

			// Batch fetch audit details for all balance_ids
			auditMap := make(map[string]map[string]string)
			if len(balanceIDs) > 0 {
				auditDetailsQuery := `SELECT balance_id, actiontype, requested_by, requested_at FROM auditactionbankbalances WHERE balance_id = ANY($1) AND actiontype IN ('CREATE','EDIT','DELETE') ORDER BY balance_id, requested_at DESC`
				auditRows, auditErr := pgxPool.Query(ctx, auditDetailsQuery, balanceIDs)
				if auditErr == nil {
					defer auditRows.Close()
					for auditRows.Next() {
						var bid string
						var atype string
						var rbyPtr *string
						var ratPtr *time.Time
						if err := auditRows.Scan(&bid, &atype, &rbyPtr, &ratPtr); err != nil {
							continue
						}
						if auditMap[bid] == nil {
							auditMap[bid] = make(map[string]string)
						}
						auditInfo := api.GetAuditInfo(atype, rbyPtr, ratPtr)
						if atype == "CREATE" && auditMap[bid]["created_by"] == "" {
							auditMap[bid]["created_by"] = auditInfo.CreatedBy
							auditMap[bid]["created_at"] = auditInfo.CreatedAt
						} else if atype == "EDIT" && auditMap[bid]["edited_by"] == "" {
							auditMap[bid]["edited_by"] = auditInfo.EditedBy
							auditMap[bid]["edited_at"] = auditInfo.EditedAt
						} else if atype == "DELETE" && auditMap[bid]["deleted_by"] == "" {
							auditMap[bid]["deleted_by"] = auditInfo.DeletedBy
							auditMap[bid]["deleted_at"] = auditInfo.DeletedAt
						}
					}
				}
			}

			// Batch fetch latest audit for all balance_ids
			auditLatestMap := make(map[string]map[string]*string)
			if len(balanceIDs) > 0 {
				auditLatestQuery := `SELECT DISTINCT ON (balance_id) balance_id, processing_status, requested_by, requested_at, actiontype, action_id, checker_by, checker_at, checker_comment, reason FROM auditactionbankbalances WHERE balance_id = ANY($1) ORDER BY balance_id, requested_at DESC`
				auditLatestRows, err := pgxPool.Query(ctx, auditLatestQuery, balanceIDs)
				if err == nil {
					defer auditLatestRows.Close()
					for auditLatestRows.Next() {
						var bid string
						var ps, rb, at, aid, cb, cc, r *string
						var rat, cat *time.Time
						if err := auditLatestRows.Scan(&bid, &ps, &rb, &rat, &at, &aid, &cb, &cat, &cc, &r); err != nil {
							continue
						}
						auditLatestMap[bid] = map[string]*string{
							"processing_status": ps,
							"requested_by":      rb,
							"actiontype":        at,
							"action_id":         aid,
							"checker_by":        cb,
							"checker_comment":   cc,
							"reason":            r,
						}
						// For time, we can add later if needed, but for now, skip requested_at and checker_at as they are not in the map
					}
				}
			}

			// Add audit details to partialRows
			for _, m := range partialRows {
				bid := m["balance_id"].(string)
				if am, ok := auditMap[bid]; ok {
					m["created_by"] = am["created_by"]
					m["created_at"] = am["created_at"]
					m["edited_by"] = am["edited_by"]
					m["edited_at"] = am["edited_at"]
					m["deleted_by"] = am["deleted_by"]
					m["deleted_at"] = am["deleted_at"]
				}
				if alm, ok := auditLatestMap[bid]; ok {
					if ps := alm["processing_status"]; ps != nil {
						m["processing_status"] = *ps
					} else {
						m["processing_status"] = ""
					}
					if at := alm["actiontype"]; at != nil {
						m["action_type"] = *at
					} else {
						m["action_type"] = ""
					}
					if aid := alm["action_id"]; aid != nil {
						m["action_id"] = *aid
					} else {
						m["action_id"] = ""
					}
					if cb := alm["checker_by"]; cb != nil {
						m["checker_by"] = *cb
					} else {
						m["checker_by"] = ""
					}
					if cc := alm["checker_comment"]; cc != nil {
						m["checker_comment"] = *cc
					} else {
						m["checker_comment"] = ""
					}
					if r := alm["reason"]; r != nil {
						m["reason"] = *r
					} else {
						m["reason"] = ""
					}
				}
			}

			w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: true, "rows": partialRows})
		}
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
		return n.T.Format(constants.DateFormat)
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
			api.RespondWithResult(w, false, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}
		if req.UserID == "" || req.BalanceID == "" {
			api.RespondWithResult(w, false, "user_id and balance_id required")
			return
		}

		ctx := r.Context()
		requestedBy := requestedByFromCtx(ctx, req.UserID)
		if requestedBy == "" {
			api.RespondWithResult(w, false, constants.ErrInvalidSession)
			return
		}
		if code, msg := ensureBalanceIDsAccessible(ctx, pgxPool, []string{req.BalanceID}); code != 0 {
			api.RespondWithError(w, code, msg)
			return
		}
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
			api.RespondWithResult(w, false, "failed to fetch existing balance: "+pgUserFriendlyMessage(err))
			return
		}
		if curBankName.Valid && !ctxHasApprovedBankName(ctx, curBankName.S) {
			api.RespondWithError(w, http.StatusForbidden, constants.ErrBankInvalidOrInactive)
			return
		}
		if curAccountNo.Valid && !ctxHasApprovedBankAccount(ctx, curAccountNo.S) {
			api.RespondWithError(w, http.StatusForbidden, constants.ErrInvalidAccount)
			return
		}

		// build update sets and args; when updating a field, set old_<field>=current_value
		sets := []string{}
		args := []interface{}{}
		pos := 1

		// helper to append set and old set
		addStrField := func(col, oldcol string, val interface{}, cur sqlNullString) {
			sets = append(sets, fmt.Sprintf(constants.FormatSQLSetPair, col, pos, oldcol, pos+1))
			args = append(args, nullifyEmpty(fmt.Sprint(val)))
			args = append(args, cur.ValueOrZero())
			pos += 2
		}
		addFloatField := func(col, oldcol string, val interface{}, cur sqlNullFloat) {
			sets = append(sets, fmt.Sprintf(constants.FormatSQLSetPair, col, pos, oldcol, pos+1))
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
				if !ctxHasApprovedBankName(ctx, fmt.Sprint(v)) {
					api.RespondWithError(w, http.StatusForbidden, constants.ErrBankInvalidOrInactive)
					return
				}
				addStrField("bank_name", "old_bank_name", v, curBankName)
			case "account_no":
				if !ctxHasApprovedBankAccount(ctx, fmt.Sprint(v)) {
					api.RespondWithError(w, http.StatusForbidden, constants.ErrInvalidAccount)
					return
				}
				addStrField("account_no", "old_account_no", v, curAccountNo)
			case "iban":
				addStrField("iban", "old_iban", v, curIban)
			case "currency_code":
				if !ctxHasApprovedCurrency(ctx, fmt.Sprint(v)) {
					api.RespondWithError(w, http.StatusForbidden, "Invalid or inactive currency")
					return
				}
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
			api.RespondWithResult(w, false, "failed to update balance: "+pgUserFriendlyMessage(err))
			return
		}

		// insert audit action
		auditQ := `INSERT INTO auditactionbankbalances (balance_id, actiontype, processing_status, reason, requested_by, requested_at) VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL',$2,$3,now())`
		if _, err := tx.Exec(ctx, auditQ, req.BalanceID, nullifyEmpty(req.Reason), requestedBy); err != nil {
			api.RespondWithResult(w, false, "failed to create audit action: "+pgUserFriendlyMessage(err))
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
