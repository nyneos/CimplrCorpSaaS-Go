package allMaster

import (
	api "CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	exposures "CimplrCorpSaas/api/fx/exposures"
	"context"
	"encoding/json"
	"fmt"
	"time"

	// "mime/multipart"
	"net/http"
	"strconv"
	"strings"

	"CimplrCorpSaas/api/constants"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lib/pq"
)

// GET handler to fetch account_id, account_number, account_nickname for approved/active accounts

type ClearingCode struct {
	CodeType          string `json:"code_type"`
	CodeValue         string `json:"code_value"`
	OptionalCodeType  string `json:"optional_code_type,omitempty"`
	OptionalCodeValue string `json:"optional_code_value,omitempty"`
}

type BankAccountMasterRequest struct {
	BankID          string         `json:"bank_id"`
	EntityID        string         `json:"entity_id"`
	AccountNumber   string         `json:"account_number"`
	AccountNickname string         `json:"account_nickname,omitempty"`
	IBAN            string         `json:"iban,omitempty"`
	BranchName      string         `json:"branch_name,omitempty"`
	BankName        string         `json:"bank_name,omitempty"`
	Country         string         `json:"country,omitempty"`
	Relationship    string         `json:"relationship,omitempty"`
	Usage           string         `json:"usage,omitempty"`
	Currency        string         `json:"currency,omitempty"`
	Nickname        string         `json:"nickname,omitempty"`
	AccountNo       string         `json:"account_no,omitempty"`
	EffFrom         string         `json:"eff_from,omitempty"`
	EffTo           string         `json:"eff_to,omitempty"`
	Status          string         `json:"status,omitempty"`
	BranchEmail     string         `json:"branch_email,omitempty"`
	BranchPhone     string         `json:"branch_phone,omitempty"`
	Addr1           string         `json:"addr1,omitempty"`
	Addr2           string         `json:"addr2,omitempty"`
	City            string         `json:"city,omitempty"`
	State           string         `json:"state,omitempty"`
	Postal          string         `json:"postal,omitempty"`
	ErpType         string         `json:"erp_type,omitempty"`
	SapBukrs        string         `json:"sap_bukrs,omitempty"`
	SapHbkid        string         `json:"sap_hbkid,omitempty"`
	SapHktid        string         `json:"sap_hktid,omitempty"`
	SapBankl        string         `json:"sap_bankl,omitempty"`
	OraLedger       string         `json:"ora_ledger,omitempty"`
	OraBranch       string         `json:"ora_branch,omitempty"`
	OraAccount      string         `json:"ora_account,omitempty"`
	TallyLedger     string         `json:"tally_ledger,omitempty"`
	SageCC          string         `json:"sage_cc,omitempty"`
	CatInflow       string         `json:"cat_inflow,omitempty"`
	CatOutflow      string         `json:"cat_outflow,omitempty"`
	CatCharges      string         `json:"cat_charges,omitempty"`
	CatIntInc       string         `json:"cat_int_inc,omitempty"`
	CatIntExp       string         `json:"cat_int_exp,omitempty"`
	ConnChannel     string         `json:"conn_channel,omitempty"`
	ConnTz          string         `json:"conn_tz,omitempty"`
	ConnCutoff      string         `json:"conn_cutoff,omitempty"`
	ApiBase         string         `json:"api_base,omitempty"`
	ApiAuth         string         `json:"api_auth,omitempty"`
	SftpHost        string         `json:"sftp_host,omitempty"`
	SftpPort        *int           `json:"sftp_port,omitempty"`
	SftpUser        string         `json:"sftp_user,omitempty"`
	SftpFolder      string         `json:"sftp_folder,omitempty"`
	EbicsHostID     string         `json:"ebics_host_id,omitempty"`
	EbicsPartnerID  string         `json:"ebics_partner_id,omitempty"`
	EbicsUserID     string         `json:"ebics_user_id,omitempty"`
	SwiftBic        string         `json:"swift_bic,omitempty"`
	SwiftService    string         `json:"swift_service,omitempty"`
	PortalURL       string         `json:"portal_url,omitempty"`
	PortalNotes     string         `json:"portal_notes,omitempty"`
	ClearingCodes   []ClearingCode `json:"clearing_codes,omitempty"`
	UserID          string         `json:"user_id"`
}

func CreateBankAccountMaster(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req BankAccountMasterRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}
		userID := req.UserID
		if userID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "Missing user_id in body")
			return
		}
		createdBy := ""
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == userID {
				createdBy = s.Name
				break
			}
		}
		if createdBy == "" {
			api.RespondWithError(w, http.StatusBadRequest, "User session not found or Name missing")
			return
		}
		if req.BankID == "" || req.EntityID == "" || req.AccountNumber == "" {
			api.RespondWithError(w, http.StatusBadRequest, "Missing required fields: bank_id, entity_id, account_number")
			return
		}
		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxStartFailed+err.Error())
			return
		}
		defer func() {
			if p := recover(); p != nil {
				tx.Rollback(ctx)
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "Panic: " + p.(string)})
			}
		}()
		var accountID string
		// Normalize date fields
		var effFrom interface{}
		var effTo interface{}
		if strings.TrimSpace(req.EffFrom) != "" {
			if norm := NormalizeDate(req.EffFrom); norm != "" {
				if tval, err := time.Parse(constants.DateFormat, norm); err == nil {
					effFrom = tval
				}
			}
		}
		if strings.TrimSpace(req.EffTo) != "" {
			if norm := NormalizeDate(req.EffTo); norm != "" {
				if tval, err := time.Parse(constants.DateFormat, norm); err == nil {
					effTo = tval
				}
			}
		}

		query := `INSERT INTO masterbankaccount (
			bank_id, entity_id, account_number, account_nickname, iban, branch_name, bank_name, country, relationship, usage, currency, nickname, account_no,
			eff_from, eff_to, status, branch_email, branch_phone, addr1, addr2, city, state, postal,
			erp_type, sap_bukrs, sap_hbkid, sap_hktid, sap_bankl, ora_ledger, ora_branch, ora_account, tally_ledger, sage_cc,
			cat_inflow, cat_outflow, cat_charges, cat_int_inc, cat_int_exp,
			conn_channel, conn_tz, conn_cutoff, api_base, api_auth,
			sftp_host, sftp_port, sftp_user, sftp_folder,
			ebics_host_id, ebics_partner_id, ebics_user_id,
			swift_bic, swift_service, portal_url, portal_notes
		) VALUES (
			$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,$41,$42,$43,$44,$45,$46,$47,$48,$49,$50,$51,$52,$53,$54
		) RETURNING account_id`

		// Build argument list matching the insert order
		args := []interface{}{
			req.BankID, req.EntityID, req.AccountNumber, req.AccountNickname, req.IBAN, req.BranchName, req.BankName, req.Country, req.Relationship, req.Usage,
			req.Currency, req.Nickname, req.AccountNo, effFrom, effTo, req.Status, req.BranchEmail, req.BranchPhone, req.Addr1, req.Addr2, req.City, req.State, req.Postal,
			req.ErpType, req.SapBukrs, req.SapHbkid, req.SapHktid, req.SapBankl, req.OraLedger, req.OraBranch, req.OraAccount, req.TallyLedger, req.SageCC,
			req.CatInflow, req.CatOutflow, req.CatCharges, req.CatIntInc, req.CatIntExp,
			req.ConnChannel, req.ConnTz, req.ConnCutoff, req.ApiBase, req.ApiAuth,
			req.SftpHost, req.SftpPort, req.SftpUser, req.SftpFolder,
			req.EbicsHostID, req.EbicsPartnerID, req.EbicsUserID,
			req.SwiftBic, req.SwiftService, req.PortalURL, req.PortalNotes,
		}

		if err := tx.QueryRow(ctx, query, args...).Scan(&accountID); err != nil {
			tx.Rollback(ctx)
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if err != nil {
			tx.Rollback(ctx)
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		auditQuery := `INSERT INTO auditactionbankaccount (
			account_id, actiontype, processing_status, reason, requested_by, requested_at
		) VALUES ($1, $2, $3, $4, $5, now()) RETURNING action_id`
		var auditActionID string
		auditErr := tx.QueryRow(ctx, auditQuery,
			accountID,
			"CREATE",
			"PENDING_APPROVAL",
			nil,
			createdBy,
		).Scan(&auditActionID)
		if auditErr != nil {
			tx.Rollback(ctx)
			api.RespondWithError(w, http.StatusInternalServerError, "Account created but audit log failed: "+auditErr.Error())
			return
		}
		var clearingResults []map[string]interface{}
		for _, cc := range req.ClearingCodes {
			if cc.CodeType == "" || cc.CodeValue == "" {
				continue
			}
			// optional fields may be empty
			ccQuery := `INSERT INTO masterclearingcode (account_id, code_type, code_value, optional_code_type, optional_code_value) VALUES ($1, $2, $3, $4, $5) RETURNING clearing_id`
			var clearingID string
			var optType interface{}
			var optVal interface{}
			if strings.TrimSpace(cc.OptionalCodeType) != "" {
				optType = cc.OptionalCodeType
			}
			if strings.TrimSpace(cc.OptionalCodeValue) != "" {
				optVal = cc.OptionalCodeValue
			}
			err := tx.QueryRow(ctx, ccQuery, accountID, cc.CodeType, cc.CodeValue, optType, optVal).Scan(&clearingID)
			if err != nil {
				tx.Rollback(ctx)
				api.RespondWithError(w, http.StatusInternalServerError, err.Error())
				return
			}
			clearingResults = append(clearingResults, map[string]interface{}{constants.ValueSuccess: true, "clearing_id": clearingID, "code_type": cc.CodeType})
		}
		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxCommitFailed+err.Error())
			return
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			constants.ValueSuccess: true,
			"account_id":           accountID,
			"audit_action_id":      auditActionID,
			"clearing_codes":       clearingResults,
		})
	}
}

func UpdateBankAccountMasterBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string `json:"user_id"`
			Accounts []struct {
				AccountID string                 `json:"account_id"`
				Fields    map[string]interface{} `json:"fields"`
			} `json:"accounts"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: constants.ErrInvalidJSONShort})
			return
		}
		userID := req.UserID
		if userID == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "Missing user_id"})
			return
		}
		updatedBy := ""
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == userID {
				updatedBy = s.Name
				break
			}
		}
		if updatedBy == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "User session not found"})
			return
		}

		var results []map[string]interface{}
		ctx := r.Context()
		for _, acc := range req.Accounts {
			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: constants.ErrTxStartFailed + err.Error(), "account_id": acc.AccountID})
				continue
			}
			committed := false
			func() {
				defer func() {
					if !committed {
						tx.Rollback(ctx)
					}
					if p := recover(); p != nil {
						results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "panic: " + fmt.Sprint(p), "account_id": acc.AccountID})
					}
				}()

				// fetch current values for columns we care about (including newly added fields)
				var exBankID, exEntityID, exAccountNumber, exAccountNickname, exAccountType, exAccountCurrency, exIBAN, exBranchName, exAccountStatus *string
				var exAddr1, exAddr2, exCity, exState, exPostal *string
				var exUsage *string
				var exAccountNo, exNickname *string
				var exConnChannel, exConnTz, exConnCutoff, exApiBase, exApiAuth *string
				var exSftpHost, exSftpUser, exSftpFolder *string
				var exSftpPort *int
				var exEbicsHostID, exEbicsPartnerID, exEbicsUserID *string
				var exSwiftBic, exSwiftService, exPortalURL, exPortalNotes *string
				var exErpType, exSapBukrs, exSapHbkid, exSapHktid, exSapBankl *string
				var exOraLedger, exOraBranch, exOraAccount, exTallyLedger, exSageCC *string
				var exEffFrom, exEffTo *time.Time
				sel := `SELECT bank_id, entity_id, account_number, account_nickname, relationship, usage, currency, iban, branch_name, status,
								  account_no, nickname, eff_from, eff_to,
								  conn_channel, conn_tz, conn_cutoff, api_base, api_auth,
								  sftp_host, sftp_port, sftp_user, sftp_folder,
								  ebics_host_id, ebics_partner_id, ebics_user_id,
								  swift_bic, swift_service, portal_url, portal_notes,
								  erp_type, sap_bukrs, sap_hbkid, sap_hktid, sap_bankl,
								  ora_ledger, ora_branch, ora_account, tally_ledger, sage_cc,
								  addr1, addr2, city, state, postal
						   FROM masterbankaccount WHERE account_id=$1 FOR UPDATE`
				if err := tx.QueryRow(ctx, sel, acc.AccountID).Scan(&exBankID, &exEntityID, &exAccountNumber, &exAccountNickname, &exAccountType, &exUsage, &exAccountCurrency, &exIBAN, &exBranchName, &exAccountStatus,
					&exAccountNo, &exNickname, &exEffFrom, &exEffTo,
					&exConnChannel, &exConnTz, &exConnCutoff, &exApiBase, &exApiAuth,
					&exSftpHost, &exSftpPort, &exSftpUser, &exSftpFolder,
					&exEbicsHostID, &exEbicsPartnerID, &exEbicsUserID,
					&exSwiftBic, &exSwiftService, &exPortalURL, &exPortalNotes,
					&exErpType, &exSapBukrs, &exSapHbkid, &exSapHktid, &exSapBankl,
					&exOraLedger, &exOraBranch, &exOraAccount, &exTallyLedger, &exSageCC,
					&exAddr1, &exAddr2, &exCity, &exState, &exPostal); err != nil {
					results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "Failed to fetch existing account: " + err.Error(), "account_id": acc.AccountID})
					return
				}

				// build dynamic update based on provided fields
				var sets []string
				var args []interface{}
				pos := 1
				clearingProvided := false
				var clearingPayload interface{}

				for k, v := range acc.Fields {
					// simple single-column updates that follow the pattern: col=$%d
					simpleCols := map[string]string{
						"account_no":       "account_no",
						"nickname":         "nickname",
						"conn_channel":     "conn_channel",
						"conn_tz":          "conn_tz",
						"conn_cutoff":      "conn_cutoff",
						"api_base":         "api_base",
						"api_auth":         "api_auth",
						"sftp_host":        "sftp_host",
						"sftp_user":        "sftp_user",
						"sftp_folder":      "sftp_folder",
						"ebics_host_id":    "ebics_host_id",
						"ebics_partner_id": "ebics_partner_id",
						"ebics_user_id":    "ebics_user_id",
						"swift_bic":        "swift_bic",
						"swift_service":    "swift_service",
						"portal_url":       "portal_url",
						"portal_notes":     "portal_notes",
						"erp_type":         "erp_type",
						"sap_bukrs":        "sap_bukrs",
						"sap_hbkid":        "sap_hbkid",
						"sap_hktid":        "sap_hktid",
						"sap_bankl":        "sap_bankl",
						"ora_ledger":       "ora_ledger",
						"ora_branch":       "ora_branch",
						"ora_account":      "ora_account",
						"tally_ledger":     "tally_ledger",
						"sage_cc":          "sage_cc",
					}

					if col, ok := simpleCols[k]; ok {
						sets = append(sets, fmt.Sprintf("%s=$%d", col, pos))
						args = append(args, fmt.Sprint(v))
						pos += 1
						continue
					}

					// fallback to special-case handling for multi-column updates, type conversions and preservation of old values
					switch k {
					case "bank_id":
						sets = append(sets, fmt.Sprintf("bank_id=$%d, old_bank_id=$%d", pos, pos+1))
						oldVal := ""
						if exBankID != nil {
							oldVal = *exBankID
						}
						args = append(args, fmt.Sprint(v), oldVal)
						pos += 2
					case "entity_id":
						sets = append(sets, fmt.Sprintf("entity_id=$%d, old_entity_id=$%d", pos, pos+1))
						oldVal := ""
						if exEntityID != nil {
							oldVal = *exEntityID
						}
						args = append(args, fmt.Sprint(v), oldVal)
						pos += 2
					case "account_number":
						sets = append(sets, fmt.Sprintf("account_number=$%d, old_account_number=$%d", pos, pos+1))
						oldVal := ""
						if exAccountNumber != nil {
							oldVal = *exAccountNumber
						}
						args = append(args, fmt.Sprint(v), oldVal)
						pos += 2
					case "account_nickname":
						sets = append(sets, fmt.Sprintf("account_nickname=$%d, old_account_nickname=$%d", pos, pos+1))
						oldVal := ""
						if exAccountNickname != nil {
							oldVal = *exAccountNickname
						}
						args = append(args, fmt.Sprint(v), oldVal)
						pos += 2
					case "account_type":
						// map legacy account_type updates to relationship column, preserve old_account_type with previous relationship value
						sets = append(sets, fmt.Sprintf("relationship=$%d, old_relationship=$%d", pos, pos+1))
						oldVal := ""
						if exAccountType != nil {
							oldVal = *exAccountType
						}
						args = append(args, fmt.Sprint(v), oldVal)
						pos += 2
					case "currency":
						sets = append(sets, fmt.Sprintf("currency=$%d, old_currency=$%d", pos, pos+1))
						oldVal := ""
						if exAccountCurrency != nil {
							oldVal = *exAccountCurrency
						}
						args = append(args, fmt.Sprint(v), oldVal)
						pos += 2
					case "iban":
						sets = append(sets, fmt.Sprintf("iban=$%d, old_iban=$%d", pos, pos+1))
						oldVal := ""
						if exIBAN != nil {
							oldVal = *exIBAN
						}
						args = append(args, fmt.Sprint(v), oldVal)
						pos += 2
					case "eff_from":
						// parse date strings to time.Time using NormalizeDate
						var dt interface{}
						if s, ok := v.(string); ok && strings.TrimSpace(s) != "" {
							if norm := NormalizeDate(s); norm != "" {
								if tval, err := time.Parse(constants.DateFormat, norm); err == nil {
									dt = tval
								}
							}
						}
						sets = append(sets, fmt.Sprintf("eff_from=$%d", pos))
						args = append(args, dt)
						pos += 1
					case "eff_to":
						var dt interface{}
						if s, ok := v.(string); ok && strings.TrimSpace(s) != "" {
							if norm := NormalizeDate(s); norm != "" {
								if tval, err := time.Parse(constants.DateFormat, norm); err == nil {
									dt = tval
								}
							}
						}
						sets = append(sets, fmt.Sprintf("eff_to=$%d", pos))
						args = append(args, dt)
						pos += 1
					case "sftp_port":
						// support numeric and string ports
						if num, ok := v.(float64); ok {
							args = append(args, int(num))
							sets = append(sets, fmt.Sprintf("sftp_port=$%d", pos))
							pos += 1
						} else if s, ok := v.(string); ok {
							if sTrim := strings.TrimSpace(s); sTrim != "" {
								if pnum, err := strconv.Atoi(sTrim); err == nil {
									args = append(args, pnum)
									sets = append(sets, fmt.Sprintf("sftp_port=$%d", pos))
									pos += 1
								}
							}
						}
					case "branch_name":
						sets = append(sets, fmt.Sprintf("branch_name=$%d, old_branch_name=$%d", pos, pos+1))
						oldVal := ""
						if exBranchName != nil {
							oldVal = *exBranchName
						}
						args = append(args, fmt.Sprint(v), oldVal)
						pos += 2
					case "addr1":
						sets = append(sets, fmt.Sprintf("addr1=$%d, old_addr1=$%d", pos, pos+1))
						oldVal := ""
						if exAddr1 != nil {
							oldVal = *exAddr1
						}
						args = append(args, fmt.Sprint(v), oldVal)
						pos += 2
					case "addr2":
						sets = append(sets, fmt.Sprintf("addr2=$%d, old_addr2=$%d", pos, pos+1))
						oldVal := ""
						if exAddr2 != nil {
							oldVal = *exAddr2
						}
						args = append(args, fmt.Sprint(v), oldVal)
						pos += 2
					case "city":
						sets = append(sets, fmt.Sprintf("city=$%d, old_city=$%d", pos, pos+1))
						oldVal := ""
						if exCity != nil {
							oldVal = *exCity
						}
						args = append(args, fmt.Sprint(v), oldVal)
						pos += 2
					case "state":
						sets = append(sets, fmt.Sprintf("state=$%d, old_state=$%d", pos, pos+1))
						oldVal := ""
						if exState != nil {
							oldVal = *exState
						}
						args = append(args, fmt.Sprint(v), oldVal)
						pos += 2
					case "postal":
						sets = append(sets, fmt.Sprintf("postal=$%d, old_postal=$%d", pos, pos+1))
						oldVal := ""
						if exPostal != nil {
							oldVal = *exPostal
						}
						args = append(args, fmt.Sprint(v), oldVal)
						pos += 2
					case constants.KeyStatus:
						// DB column name is `status`; update status and preserve old_account_status
						sets = append(sets, fmt.Sprintf("status=$%d, old_status=$%d", pos, pos+1))
						oldVal := ""
						if exAccountStatus != nil {
							oldVal = *exAccountStatus
						}
						args = append(args, fmt.Sprint(v), oldVal)
						pos += 2
					case "clearing_codes":
						// handle after update; capture payload
						clearingProvided = true
						clearingPayload = v
					default:
						// ignore unknown fields
					}
				}

				var updatedAccountID string
				if len(sets) > 0 {
					// build query
					query := "UPDATE masterbankaccount SET " + strings.Join(sets, ", ") + fmt.Sprintf(" WHERE account_id=$%d RETURNING account_id", pos)
					args = append(args, acc.AccountID)
					if err := tx.QueryRow(ctx, query, args...).Scan(&updatedAccountID); err != nil {
						results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: err.Error(), "account_id": acc.AccountID})
						return
					}
				} else {
					// nothing to update in masterbankaccount
					updatedAccountID = acc.AccountID
				}

				var clearingResults []map[string]interface{}
				if clearingProvided {
					// parse clearingPayload to []ClearingCode
					var codes []ClearingCode
					// marshal then unmarshal to our struct for safety
					b, _ := json.Marshal(clearingPayload)
					if err := json.Unmarshal(b, &codes); err != nil {
						results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "invalid clearing_codes payload: " + err.Error(), "account_id": updatedAccountID})
						return
					}

					// fetch existing clearing codes to preserve old values including optional fields
					type existingClearingRow struct {
						CodeType             string
						CodeValue            string
						OptionalCodeType     string
						OptionalCodeValue    string
						OldCodeType          string
						OldCodeValue         string
						OldOptionalCodeType  string
						OldOptionalCodeValue string
					}
					existingClearing := make([]existingClearingRow, 0)
					selClearing := `SELECT code_type, code_value, optional_code_type, optional_code_value, old_code_type, old_code_value, old_optional_code_type, old_optional_code_value FROM masterclearingcode WHERE account_id=$1 ORDER BY clearing_id`
					erows, _ := tx.Query(ctx, selClearing, updatedAccountID)
					if erows != nil {
						defer erows.Close()
						for erows.Next() {
							var ctPtr, cvPtr, optPtr, optValPtr, oldCtPtr, oldCvPtr, oldOptPtr, oldOptValPtr *string
							if err := erows.Scan(&ctPtr, &cvPtr, &optPtr, &optValPtr, &oldCtPtr, &oldCvPtr, &oldOptPtr, &oldOptValPtr); err == nil {
								ct := ""
								cv := ""
								opt := ""
								optVal := ""
								oldCt := ""
								oldCv := ""
								oldOpt := ""
								oldOptVal := ""
								if ctPtr != nil {
									ct = *ctPtr
								}
								if cvPtr != nil {
									cv = *cvPtr
								}
								if optPtr != nil {
									opt = *optPtr
								}
								if optValPtr != nil {
									optVal = *optValPtr
								}
								if oldCtPtr != nil {
									oldCt = *oldCtPtr
								}
								if oldCvPtr != nil {
									oldCv = *oldCvPtr
								}
								if oldOptPtr != nil {
									oldOpt = *oldOptPtr
								}
								if oldOptValPtr != nil {
									oldOptVal = *oldOptValPtr
								}
								existingClearing = append(existingClearing, existingClearingRow{ct, cv, opt, optVal, oldCt, oldCv, oldOpt, oldOptVal})
							}
						}
					}

					if _, delErr := tx.Exec(ctx, `DELETE FROM masterclearingcode WHERE account_id=$1`, updatedAccountID); delErr != nil {
						results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "Failed to delete old clearing codes: " + delErr.Error(), "account_id": updatedAccountID})
						return
					}

					// When inserting new clearing codes, set old_code_type/old_code_value and old_optional_* from existingClearing by position if available
					for i, cc := range codes {
						if cc.CodeType == "" || cc.CodeValue == "" {
							continue
						}
						oldType := ""
						oldValue := ""
						oldOptType := ""
						oldOptValue := ""
						if i < len(existingClearing) {
							oldType = existingClearing[i].OldCodeType
							oldValue = existingClearing[i].OldCodeValue
							oldOptType = existingClearing[i].OldOptionalCodeType
							oldOptValue = existingClearing[i].OldOptionalCodeValue
						}

						var optType interface{}
						var optVal interface{}
						if strings.TrimSpace(cc.OptionalCodeType) != "" {
							optType = cc.OptionalCodeType
						}
						if strings.TrimSpace(cc.OptionalCodeValue) != "" {
							optVal = cc.OptionalCodeValue
						}

						ccQuery := `INSERT INTO masterclearingcode (account_id, code_type, code_value, old_code_type, old_code_value, optional_code_type, old_optional_code_type, optional_code_value, old_optional_code_value) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING clearing_id`
						var clearingID string
						if err := tx.QueryRow(ctx, ccQuery, updatedAccountID, cc.CodeType, cc.CodeValue, oldType, oldValue, optType, oldOptType, optVal, oldOptValue).Scan(&clearingID); err != nil {
							results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: err.Error(), "account_id": updatedAccountID, "code_type": cc.CodeType})
							return
						}
						clearingResults = append(clearingResults, map[string]interface{}{constants.ValueSuccess: true, "clearing_id": clearingID, "code_type": cc.CodeType})
					}
				}

				// insert audit action
				auditQuery := `INSERT INTO auditactionbankaccount (
					account_id, actiontype, processing_status, reason, requested_by, requested_at
				) VALUES ($1, $2, $3, $4, $5, now()) RETURNING action_id`
				var auditActionID string
				if err := tx.QueryRow(ctx, auditQuery, updatedAccountID, "EDIT", "PENDING_EDIT_APPROVAL", nil, updatedBy).Scan(&auditActionID); err != nil {
					results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "Account updated but audit log failed: " + err.Error(), "account_id": updatedAccountID, "clearing_codes": clearingResults})
					return
				}

				if err := tx.Commit(ctx); err != nil {
					results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: constants.ErrTxCommitFailed + err.Error(), "account_id": updatedAccountID})
					return
				}
				committed = true
				results = append(results, map[string]interface{}{constants.ValueSuccess: true, "account_id": updatedAccountID, "audit_action_id": auditActionID, "clearing_codes": clearingResults})
			}()
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		finalSuccess := api.IsBulkSuccess(results)
		json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: finalSuccess, "results": results})
	}
}

// Bulk delete handler for bank account audit actions
func BulkDeleteBankAccountAudit(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID     string   `json:"user_id"`
			AccountIDs []string `json:"account_ids"`
			Reason     string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.AccountIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON or missing fields")
			return
		}
		sessions := auth.GetActiveSessions()
		requestedBy := ""
		for _, s := range sessions {
			if s.UserID == req.UserID {
				requestedBy = s.Name
				break
			}
		}
		if requestedBy == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidSessionCapitalized)
			return
		}
		var results []string
		for _, accountID := range req.AccountIDs {
			query := `INSERT INTO auditactionbankaccount (
				account_id, actiontype, processing_status, reason, requested_by, requested_at
			) VALUES ($1, 'DELETE', 'PENDING_DELETE_APPROVAL', $2, $3, now()) RETURNING action_id`
			var actionID string
			err := pgxPool.QueryRow(r.Context(), query, accountID, req.Reason, requestedBy).Scan(&actionID)
			if err == nil {
				results = append(results, actionID)
			}
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			constants.ValueSuccess: true,
			"created":              results,
		})
	}
}

// Bulk reject audit actions for bank account master
func BulkRejectBankAccountAuditActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID     string   `json:"user_id"`
			AccountIDs []string `json:"account_ids"`
			Comment    string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.AccountIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON or missing fields: provide account_ids")
			return
		}
		sessions := auth.GetActiveSessions()
		checkerBy := ""
		for _, s := range sessions {
			if s.UserID == req.UserID {
				checkerBy = s.Name
				break
			}
		}
		if checkerBy == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidSessionCapitalized)
			return
		}
		var rows pgx.Rows
		var err error
		query := `UPDATE auditactionbankaccount SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2 WHERE account_id = ANY($3) RETURNING action_id,account_id`
		rows, err = pgxPool.Query(r.Context(), query, checkerBy, req.Comment, pq.Array(req.AccountIDs))
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()
		var updated []string
		for rows.Next() {
			var id, accountID string
			rows.Scan(&id, &accountID)
			updated = append(updated, id, accountID)
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			constants.ValueSuccess: true,
			"updated":              updated,
		})
	}
}

// Bulk approve audit actions for bank account master
func BulkApproveBankAccountAuditActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID     string   `json:"user_id"`
			AccountIDs []string `json:"account_ids"`
			Comment    string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.AccountIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON or missing fields: provide account_ids")
			return
		}
		sessions := auth.GetActiveSessions()
		checkerBy := ""
		for _, s := range sessions {
			if s.UserID == req.UserID {
				checkerBy = s.Name
				break
			}
		}
		if checkerBy == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidSessionCapitalized)
			return
		}
		// First, delete audit rows that requested DELETE (pending delete approval)
		var delRows pgx.Rows
		var delErr error
		var deleted []string
		var accountIDsToDelete []string
		delQuery := `DELETE FROM auditactionbankaccount WHERE account_id = ANY($1) AND processing_status = 'PENDING_DELETE_APPROVAL' RETURNING action_id, account_id`
		delRows, delErr = pgxPool.Query(r.Context(), delQuery, pq.Array(req.AccountIDs))
		if delErr == nil {
			defer delRows.Close()
			for delRows.Next() {
				var id, accountID string
				delRows.Scan(&id, &accountID)
				deleted = append(deleted, id, accountID)
				accountIDsToDelete = append(accountIDsToDelete, accountID)
			}
		}

		// Soft-delete associated master account records (mark is_deleted = true)
		if len(accountIDsToDelete) > 0 {
			// mark accounts as deleted; keep clearing codes for historical record
			_, _ = pgxPool.Exec(r.Context(), `UPDATE masterbankaccount SET is_deleted = true WHERE account_id = ANY($1)`, pq.Array(accountIDsToDelete))
		}

		// Approve remaining audit actions (exclude those that were pending delete)
		var rows pgx.Rows
		var err error
		query := `UPDATE auditactionbankaccount SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2 WHERE account_id = ANY($3) AND processing_status != 'PENDING_DELETE_APPROVAL' RETURNING action_id,account_id`
		rows, err = pgxPool.Query(r.Context(), query, checkerBy, req.Comment, pq.Array(req.AccountIDs))
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()
		var updated []string
		for rows.Next() {
			var id, accountID string
			rows.Scan(&id, &accountID)
			updated = append(updated, id, accountID)
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			constants.ValueSuccess: true,
			"updated":              updated,
			"deleted":              deleted,
		})
	}
}

// GET handler to fetch all bank_id, bank_name (bank_short_name) for banks used by accounts
func GetBankNamesWithIDForAccount(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}
		query := `
			SELECT DISTINCT m.bank_id, m.bank_name, m.bank_short_name
			FROM masterbank m
			JOIN masterbankaccount a ON a.bank_id = m.bank_id
			LEFT JOIN LATERAL (
				SELECT processing_status
				FROM auditactionbank a2
				WHERE a2.bank_id = m.bank_id
				ORDER BY requested_at DESC
				LIMIT 1
			) astatus ON TRUE
			WHERE m.active_status = 'Active' AND astatus.processing_status = 'APPROVED'
		`
		rows, err := pgxPool.Query(r.Context(), query)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		var results []map[string]interface{}
		var anyError error
		for rows.Next() {
			var bankID, bankName, bankShortName string
			if err := rows.Scan(&bankID, &bankName, &bankShortName); err != nil {
				anyError = err
				break
			}
			results = append(results, map[string]interface{}{
				"bank_id":   bankID,
				"bank_name": bankName,
				"bank_short_name": func() string {
					if bankShortName != "" {
						return bankShortName
					}
					return ""
				}(),
			})
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		if anyError != nil {
			api.RespondWithError(w, http.StatusInternalServerError, anyError.Error())
			return
		}
		if results == nil {
			results = make([]map[string]interface{}, 0)
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			constants.ValueSuccess: true,
			"results":              results,
		})
	}
}

func GetApprovedBankAccountsWithBankEntity(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		query := `
			WITH latest_audit AS (
				SELECT DISTINCT ON (aa.account_id)
					aa.account_id,
					aa.processing_status
				FROM auditactionbankaccount aa
				ORDER BY aa.account_id, aa.requested_at DESC
			),
			clearing AS (
				SELECT 
					c.account_id,
					STRING_AGG(c.code_type || ':' || c.code_value, ', ') AS clearing_codes
				FROM public.masterclearingcode c
				GROUP BY c.account_id
			)
			SELECT
				a.account_id,
				a.account_number,
				a.account_nickname,
				b.bank_id,
				b.bank_name,
				COALESCE(e.entity_id::text, ec.entity_id::text) AS entity_id,
				COALESCE(e.entity_name, ec.entity_name) AS entity_name,
				cl.clearing_codes
			FROM masterbankaccount a
			LEFT JOIN masterbank b ON a.bank_id = b.bank_id
			LEFT JOIN masterentity e ON e.entity_id::text = a.entity_id
			LEFT JOIN masterentitycash ec ON ec.entity_id::text = a.entity_id
			LEFT JOIN latest_audit la ON la.account_id = a.account_id
			LEFT JOIN clearing cl ON cl.account_id = a.account_id
			WHERE 
				la.processing_status = 'APPROVED'
				AND a.status = 'Active'
				AND COALESCE(a.is_deleted, false) = false
			ORDER BY a.account_nickname NULLS LAST, a.account_number;
		`

		rows, err := pgxPool.Query(r.Context(), query)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		var results []map[string]interface{}
		for rows.Next() {
			var accountID, accountNumber string
			var accountNickname, bankID, bankName, entityID, entityName, clearingCodes *string

			if err := rows.Scan(
				&accountID,
				&accountNumber,
				&accountNickname,
				&bankID,
				&bankName,
				&entityID,
				&entityName,
				&clearingCodes,
			); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "row scan failed: "+err.Error())
				return
			}

			results = append(results, map[string]interface{}{
				"account_id":     accountID,
				"account_number": accountNumber,
				"account_nickname": func() string {
					if accountNickname != nil {
						return *accountNickname
					}
					return ""
				}(),
				"bank_id": func() string {
					if bankID != nil {
						return *bankID
					}
					return ""
				}(),
				"bank_name": func() string {
					if bankName != nil {
						return *bankName
					}
					return ""
				}(),
				"entity_id": func() string {
					if entityID != nil {
						return *entityID
					}
					return ""
				}(),
				"entity_name": func() string {
					if entityName != nil {
						return *entityName
					}
					return ""
				}(),
				"clearing_codes": func() string {
					if clearingCodes != nil {
						return *clearingCodes
					}
					return ""
				}(),
			})
		}

		if rows.Err() != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "rows iteration failed: "+rows.Err().Error())
			return
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		if results == nil {
			results = make([]map[string]interface{}, 0)
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			constants.ValueSuccess: true,
			"results":              results,
		})
	}
}

// func GetApprovedBankAccountsWithBankEntity(pgxPool *pgxpool.Pool) http.HandlerFunc {
// 	return func(w http.ResponseWriter, r *http.Request) {
// 		var req struct {
// 			UserID string `json:"user_id"`
// 		}
// 		// accept optional user_id in body for parity with other handlers
// 		_ = json.NewDecoder(r.Body).Decode(&req)

// 		query := `
// 			SELECT
// 				a.account_id,
// 				a.account_number,
// 				a.account_nickname,
// 				b.bank_id,
// 				b.bank_name,
// 				COALESCE(e.entity_id::text, ec.entity_id::text) AS entity_id,
// 				COALESCE(e.entity_name, ec.entity_name) AS entity_name
// 			FROM masterbankaccount a
// 			LEFT JOIN masterbank b ON a.bank_id = b.bank_id
// 			LEFT JOIN masterentity e ON e.entity_id::text = a.entity_id
// 			LEFT JOIN masterentitycash ec ON ec.entity_id::text = a.entity_id
// 			LEFT JOIN LATERAL (
// 				SELECT processing_status
// 				FROM auditactionbankaccount aa
// 				WHERE aa.account_id = a.account_id
// 				ORDER BY requested_at DESC
// 				LIMIT 1
// 			) astatus ON TRUE
// 			WHERE astatus.processing_status = 'APPROVED' AND a.status = 'Active' AND COALESCE(a.is_deleted, false) = false
// 		`
// 		rows, err := pgxPool.Query(r.Context(), query)
// 		if err != nil {
// 			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
// 			return
// 		}
// 		defer rows.Close()

// 		var results []map[string]interface{}
// 		var anyErr error
// 		for rows.Next() {
// 			var accountID, accountNumber string
// 			var accountNickname *string
// 			var bankID, bankName, entityID, entityName *string
// 			if err := rows.Scan(&accountID, &accountNumber, &accountNickname, &bankID, &bankName, &entityID, &entityName); err != nil {
// 				anyErr = err
// 				break
// 			}
// 			results = append(results, map[string]interface{}{
// 				"account_id":     accountID,
// 				"account_number": accountNumber,
// 				"account_nickname": func() string {
// 					if accountNickname != nil {
// 						return *accountNickname
// 					}
// 					return ""
// 				}(),
// 				"bank_id": func() string {
// 					if bankID != nil {
// 						return *bankID
// 					}
// 					return ""
// 				}(),
// 				"bank_name": func() string {
// 					if bankName != nil {
// 						return *bankName
// 					}
// 					return ""
// 				}(),
// 				"entity_id": func() string {
// 					if entityID != nil {
// 						return *entityID
// 					}
// 					return ""
// 				}(),
// 				"entity_name": func() string {
// 					if entityName != nil {
// 						return *entityName
// 					}
// 					return ""
// 				}(),
// 			})
// 		}

// 		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
// 		if anyErr != nil {
// 			api.RespondWithError(w, http.StatusInternalServerError, anyErr.Error())
// 			return
// 		}
// 		if results == nil {
// 			results = make([]map[string]interface{}, 0)
// 		}
// 		json.NewEncoder(w).Encode(map[string]interface{}{
// 			constants.ValueSuccess: true,
// 			"results": results,
// 		})
// 	}
// }

func UploadBankAccount(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := context.Background()

		// Step 1: Get user_id
		userID := ""
		if r.Header.Get(constants.ContentTypeText) == constants.ContentTypeJSON {
			var req struct {
				UserID string `json:"user_id"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
				api.RespondWithError(w, http.StatusBadRequest, "user_id required in body")
				return
			}
			userID = req.UserID
		} else {
			userID = r.FormValue(constants.KeyUserID)
			if userID == "" {
				api.RespondWithError(w, http.StatusBadRequest, "user_id required in form")
				return
			}
		}

		// Step 2: Fetch user name from active sessions
		userName := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == userID {
				userName = s.Name
				break
			}
		}
		if userName == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}

		// Step 3: Parse multipart form
		if err := r.ParseMultipartForm(32 << 20); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrFailedToParseMultipartForm)
			return
		}
		files := r.MultipartForm.File["file"]
		if len(files) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrNoFilesUploaded)
			return
		}

		// Step 4: Single batch ID per upload session
		batchID := uuid.New().String()
		batchIDs := []string{batchID}

		// Step 5: Process files
		for _, fh := range files {
			f, err := fh.Open()
			if err != nil {
				api.RespondWithError(w, http.StatusBadRequest, "Failed to open file: "+fh.Filename)
				return
			}
			ext := getFileExt(fh.Filename)
			records, err := parseCashFlowCategoryFile(f, ext)
			f.Close()
			if err != nil || len(records) < 2 {
				api.RespondWithError(w, http.StatusBadRequest, "Invalid or empty file: "+fh.Filename)
				return
			}

			headerRow := records[0]
			dataRows := records[1:]
			colCount := len(headerRow)

			// Add batch ID to every row
			copyRows := make([][]interface{}, len(dataRows))
			for i, row := range dataRows {
				vals := make([]interface{}, colCount+1)
				vals[0] = batchID
				for j := 0; j < colCount; j++ {
					if j < len(row) {
						cell := strings.TrimSpace(row[j])
						if cell == "" {
							vals[j+1] = nil
						} else {
							vals[j+1] = cell
						}
					} else {
						vals[j+1] = nil
					}
				}
				copyRows[i] = vals
			}

			// Normalize headers (CSV header -> lookup key)
			headerNorm := make([]string, len(headerRow))
			for i, h := range headerRow {
				hn := strings.ToLower(strings.TrimSpace(h))
				hn = strings.ReplaceAll(hn, " ", "_")
				hn = strings.Trim(hn, "\"'`,")
				headerNorm[i] = hn
			}

			// Begin TX
			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxStartFailed+err.Error())
				return
			}
			committed := false
			defer func() {
				if !committed {
					tx.Rollback(ctx)
				}
			}()

			// Read mapping (use transaction to keep consistent)
			mapRows, err := tx.Query(ctx, `SELECT source_column_name, target_field_name FROM upload_mapping_bankaccount`)
			if err != nil {
				tx.Rollback(ctx)
				api.RespondWithError(w, http.StatusInternalServerError, "Mapping error: "+err.Error())
				return
			}

			mapping := make(map[string]string)
			for mapRows.Next() {
				var src, tgt string
				if err := mapRows.Scan(&src, &tgt); err == nil {
					key := strings.ToLower(strings.TrimSpace(src))
					key = strings.ReplaceAll(key, " ", "_")
					mapping[key] = strings.ToLower(strings.TrimSpace(tgt))
				}
			}
			mapRows.Close()

			// Build target column list (these must exist in input_bankaccount_table)
			var mappedTargets []string
			var selectedIdx []int
			for i := range headerRow {
				key := headerNorm[i]
				if t, ok := mapping[key]; ok {
					mappedTargets = append(mappedTargets, t)
					selectedIdx = append(selectedIdx, i)
				} else {
					// skip unmapped columns (user requested tolerant behavior)
					continue
				}
			}

			if len(mappedTargets) == 0 {
				tx.Rollback(ctx)
				api.RespondWithError(w, http.StatusBadRequest, "No mapped columns found in uploaded file")
				return
			}

			columns := append([]string{"upload_batch_id"}, mappedTargets...)

			// Build reduced copyRows which only includes selected (mapped) columns
			newCopyRows := make([][]interface{}, len(copyRows))
			for i, row := range copyRows {
				vals := make([]interface{}, len(mappedTargets)+1)
				// batch id stays at position 0
				if len(row) > 0 {
					vals[0] = row[0]
				} else {
					vals[0] = batchID
				}
				for j, origIdx := range selectedIdx {
					// original value is at origIdx+1 in row (since row[0] is batchID)
					if len(row) > origIdx+1 {
						vals[j+1] = row[origIdx+1]
					} else {
						vals[j+1] = nil
					}
				}
				newCopyRows[i] = vals
			}

			// Normalize date columns (eff_from, eff_to) in reduced rows before staging
			dateCols := map[string]bool{"eff_from": true, "eff_to": true}
			for colIdx, colName := range mappedTargets {
				if _, ok := dateCols[colName]; ok {
					for i := range newCopyRows {
						if len(newCopyRows[i]) <= colIdx+1 {
							continue
						}
						v := newCopyRows[i][colIdx+1]
						if v == nil {
							continue
						}
						if s, ok := v.(string); ok {
							norm := exposures.NormalizeDate(s)
							if norm == "" {
								newCopyRows[i][colIdx+1] = nil
							} else {
								newCopyRows[i][colIdx+1] = norm
							}
						}
					}
				}
			}

			// Stage into input table using mapped target column names
			_, err = tx.CopyFrom(ctx, pgx.Identifier{"input_bankaccount_table"}, columns, pgx.CopyFromRows(newCopyRows))
			if err != nil {
				tx.Rollback(ctx)
				api.RespondWithError(w, http.StatusInternalServerError, "Failed to stage data: "+err.Error())
				return
			}

			// Build insert/select expressions: map only columns that belong to masterbankaccount.
			// Clearing-code related columns should not be inserted into masterbankaccount; they go to masterclearingcode.
			exclude := map[string]bool{
				"clearing_code_type":  true,
				"clearing_code_value": true,
				"optional_code_type":  true,
				"optional_code_value": true,
			}
			masterCols := []string{}
			for _, col := range mappedTargets {
				if !exclude[col] {
					masterCols = append(masterCols, col)
				}
			}
			if len(masterCols) == 0 {
				tx.Rollback(ctx)
				api.RespondWithError(w, http.StatusBadRequest, "No target columns available for masterbankaccount insert")
				return
			}
			tgtColsStr := strings.Join(masterCols, ", ")
			var selectExprs []string
			for _, col := range masterCols {
				selectExprs = append(selectExprs, fmt.Sprintf("s.%s AS %s", col, col))
			}
			srcColsStr := strings.Join(selectExprs, ", ")

			// Insert into masterbankaccount
			insertSQL := fmt.Sprintf(`
				INSERT INTO masterbankaccount (%s)
				SELECT %s
				FROM input_bankaccount_table s
				WHERE s.upload_batch_id = $1
				RETURNING account_id, account_number, bank_id
			`, tgtColsStr, srcColsStr)

			rows, err := tx.Query(ctx, insertSQL, batchID)
			if err != nil {
				tx.Rollback(ctx)
				api.RespondWithError(w, http.StatusInternalServerError, "Final insert error: "+err.Error())
				return
			}

			type inserted struct {
				AccountID     string
				AccountNumber string
				BankID        string
			}
			var insertedRows []inserted
			for rows.Next() {
				var x inserted
				if err := rows.Scan(&x.AccountID, &x.AccountNumber, &x.BankID); err == nil {
					insertedRows = append(insertedRows, x)
				} else {
					// capture scan errors
					rows.Close()
					tx.Rollback(ctx)
					api.RespondWithError(w, http.StatusInternalServerError, "Failed to read inserted rows: "+err.Error())
					return
				}
			}
			if err := rows.Err(); err != nil {
				rows.Close()
				tx.Rollback(ctx)
				api.RespondWithError(w, http.StatusInternalServerError, "Error iterating inserted rows: "+err.Error())
				return
			}
			rows.Close()

			// Insert into auditactionbankaccount
			if len(insertedRows) > 0 {
				ids := make([]string, 0, len(insertedRows))
				for _, x := range insertedRows {
					ids = append(ids, x.AccountID)
				}
				auditSQL := `INSERT INTO auditactionbankaccount (account_id, actiontype, processing_status, reason, requested_by, requested_at)
					SELECT account_id, 'CREATE', 'PENDING_APPROVAL', NULL, $1, now()
					FROM masterbankaccount WHERE account_id = ANY($2)`
				if _, err := tx.Exec(ctx, auditSQL, userName, ids); err != nil {
					tx.Rollback(ctx)
					api.RespondWithError(w, http.StatusInternalServerError, "Failed to insert audit actions: "+err.Error())
					return
				}

				// Insert into masterclearingcode (NEW) only when master rows were created
				_, err = tx.Exec(ctx, `
					INSERT INTO masterclearingcode (
						account_id, code_type, code_value, optional_code_type, optional_code_value
					)
					SELECT
						m.account_id,
						i.clearing_code_type,
						i.clearing_code_value,
						i.optional_code_type,
						i.optional_code_value
					FROM input_bankaccount_table i
					JOIN masterbankaccount m
						ON m.account_number = i.account_number
						AND m.bank_id::text = i.bank_id::text
					WHERE i.upload_batch_id = $1
					AND i.clearing_code_type IS NOT NULL
				`, batchID)
				if err != nil {
					tx.Rollback(ctx)
					api.RespondWithError(w, http.StatusInternalServerError, "Failed to insert clearing codes: "+err.Error())
					return
				}
			}

			if err := tx.Commit(ctx); err != nil {
				tx.Rollback(ctx)
				api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailedCapitalized+err.Error())
				return
			}
			committed = true
		}

		// Success Response
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			constants.ValueSuccess: true,
			"batch_ids":            batchIDs,
			"uploaded":             len(files),
			"requested_by":         userName,
		})
	}
}
func GetApprovedBankAccountsSimple(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(new(struct {
			UserID string `json:"user_id"`
		}))

		query := `
			SELECT
				b.bank_name,
				a.account_number,
				a.iban,
				a.currency,
				a.account_nickname,
				a.account_id
			FROM masterbankaccount a
			LEFT JOIN masterbank b ON a.bank_id = b.bank_id
			LEFT JOIN LATERAL (
				SELECT processing_status
				FROM auditactionbankaccount aa
				WHERE aa.account_id = a.account_id
				ORDER BY requested_at DESC
				LIMIT 1
			) astatus ON TRUE
			WHERE astatus.processing_status = 'APPROVED' AND a.status = 'Active' AND COALESCE(a.is_deleted, false) = false
		`

		rows, err := pgxPool.Query(r.Context(), query)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		out := make([]map[string]interface{}, 0)
		for rows.Next() {
			var bankName *string
			var accountNumber string
			var iban *string
			var currency *string
			var nickname *string
			var accountID string
			if err := rows.Scan(&bankName, &accountNumber, &iban, &currency, &nickname, &accountID); err != nil {
				continue
			}
			out = append(out, map[string]interface{}{
				"bank_name": func() string {
					if bankName != nil {
						return *bankName
					}
					return ""
				}(),
				"account_no": accountNumber,
				"iban": func() string {
					if iban != nil {
						return *iban
					}
					return ""
				}(),
				"currency_code": func() string {
					if currency != nil {
						return *currency
					}
					return ""
				}(),
				"nickname": func() string {
					if nickname != nil {
						return *nickname
					}
					return ""
				}(),
				"account_id": accountID,
			})
		}

		if out == nil {
			out = make([]map[string]interface{}, 0)
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: true, "rows": out})
	}
}

// Get accounts related to a user (based on audit requested_by = user's email)
func GetBankAccountsForUser(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			AccountID string `json:"account_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.AccountID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "account_id required in body")
			return
		}

		ctx := r.Context()

		query := `
        SELECT 
            a.account_id,
            a.account_number,
            a.account_nickname,
            a.iban,
            a.branch_name,
            a.bank_id,
            a.entity_id,
            a.bank_name,
            a.old_bank_name,
            a.country,
            a.old_country,
            a.relationship,
            a.old_relationship,
            a.usage,
            a.old_usage,
            a.currency,
            a.old_currency,
            a.nickname,
            a.old_nickname,
            a.account_no,
            a.old_account_no,
            a.eff_from,
            a.old_eff_from,
            a.eff_to,
            a.old_eff_to,
            a.status,
            a.old_status,
            a.branch_email,
            a.old_branch_email,
            a.branch_phone,
            a.old_branch_phone,
            a.addr1,
            a.old_addr1,
            a.addr2,
            a.old_addr2,
            a.city,
            a.old_city,
            a.state,
            a.old_state,
            a.postal,
            a.old_postal,
            a.erp_type,
            a.old_erp_type,
            a.sap_bukrs,
            a.old_sap_bukrs,
            a.sap_hbkid,
            a.old_sap_hbkid,
            a.sap_hktid,
            a.old_sap_hktid,
            a.sap_bankl,
            a.old_sap_bankl,
            a.ora_ledger,
            a.old_ora_ledger,
            a.ora_branch,
            a.old_ora_branch,
            a.ora_account,
            a.old_ora_account,
            a.tally_ledger,
            a.old_tally_ledger,
            a.sage_cc,
            a.old_sage_cc,
            a.cat_inflow,
            a.old_cat_inflow,
            a.cat_outflow,
            a.old_cat_outflow,
            a.cat_charges,
            a.old_cat_charges,
            a.cat_int_inc,
            a.old_cat_int_inc,
            a.cat_int_exp,
            a.old_cat_int_exp,
            a.conn_channel,
            a.old_conn_channel,
            a.conn_tz,
            a.old_conn_tz,
            a.conn_cutoff,
            a.old_conn_cutoff,
            a.api_base,
            a.old_api_base,
            a.api_auth,
            a.old_api_auth,
            a.sftp_host,
            a.old_sftp_host,
            a.sftp_port,
            a.old_sftp_port,
            a.sftp_user,
            a.old_sftp_user,
            a.sftp_folder,
            a.old_sftp_folder,
            a.ebics_host_id,
            a.old_ebics_host_id,
            a.ebics_partner_id,
            a.old_ebics_partner_id,
            a.ebics_user_id,
            a.old_ebics_user_id,
            a.swift_bic,
            a.old_swift_bic,
            a.swift_service,
            a.old_swift_service,
            a.portal_url,
            a.old_portal_url,
            a.portal_notes,
            a.old_portal_notes,
            COALESCE(
                (
                    SELECT json_agg(json_build_object(
                        'clearing_id', c.clearing_id,
                        'code_type', c.code_type,
                        'code_value', c.code_value,
                        'optional_code_type', COALESCE(c.optional_code_type, ''),
                        'optional_code_value', COALESCE(c.optional_code_value, ''),
                        'old_code_type', COALESCE(c.old_code_type, ''),
                        'old_code_value', COALESCE(c.old_code_value, '')
                    ))
                    FROM masterclearingcode c
                    WHERE c.account_id = a.account_id
                ), '[]'::json
            ) AS clearing_codes,
            b.bank_id,
            b.bank_name,
            COALESCE(e.entity_id::text, ec.entity_id::text) AS entity_id,
            COALESCE(e.entity_name, ec.entity_name) AS entity_name
        FROM masterbankaccount a
        LEFT JOIN masterbank b ON a.bank_id = b.bank_id
        LEFT JOIN masterentity e ON e.entity_id::text = a.entity_id
        LEFT JOIN masterentitycash ec ON ec.entity_id::text = a.entity_id
	WHERE a.account_id = $1 AND COALESCE(a.is_deleted, false) = false
        `

		rows, err := pgxPool.Query(ctx, query, req.AccountID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		type AccountRow struct {
			AccountID, AccountNumber                                                                                   string
			AccountNickname, IBAN, BranchName                                                                          *string
			BankID, EntityID                                                                                           *string
			BankName, OldBankName                                                                                      *string
			Country, OldCountry                                                                                        *string
			Relationship, OldRelationship                                                                              *string
			Usage, OldUsage                                                                                            *string
			Currency, OldCurrency                                                                                      *string
			Nickname, OldNickname                                                                                      *string
			AccountNo, OldAccountNo                                                                                    *string
			EffFrom, OldEffFrom, EffTo, OldEffTo                                                                       *time.Time
			Status, OldStatus                                                                                          *string
			BranchEmail, OldBranchEmail                                                                                *string
			BranchPhone, OldBranchPhone                                                                                *string
			Addr1, OldAddr1, Addr2, OldAddr2                                                                           *string
			City, OldCity, State, OldState, Postal, OldPostal                                                          *string
			ErpType, OldErpType                                                                                        *string
			SapBukrs, OldSapBukrs, SapHbkid, OldSapHbkid, SapHktid, OldSapHktid, SapBankl, OldSapBankl                 *string
			OraLedger, OldOraLedger, OraBranch, OldOraBranch, OraAccount, OldOraAccount                                *string
			TallyLedger, OldTallyLedger, SageCC, OldSageCC                                                             *string
			CatInflow, OldCatInflow, CatOutflow, OldCatOutflow, CatCharges, OldCatCharges                              *string
			CatIntInc, OldCatIntInc, CatIntExp, OldCatIntExp                                                           *string
			ConnChannel, OldConnChannel, ConnTz, OldConnTz, ConnCutoff, OldConnCutoff                                  *string
			ApiBase, OldApiBase, ApiAuth, OldApiAuth                                                                   *string
			SftpHost, OldSftpHost, SftpUser, OldSftpUser, SftpFolder, OldSftpFolder                                    *string
			SftpPort, OldSftpPort                                                                                      *int
			EbicsHostID, OldEbicsHostID, EbicsPartnerID, OldEbicsPartnerID, EbicsUserID, OldEbicsUserID                *string
			SwiftBic, OldSwiftBic, SwiftService, OldSwiftService, PortalURL, OldPortalURL, PortalNotes, OldPortalNotes *string
			ClearingCodes                                                                                              []byte
			BankIDOut, BankNameOut, EntityIDOut, EntityNameOut                                                         *string
		}

		var row AccountRow
		if rows.Next() {
			if err := rows.Scan(
				&row.AccountID, &row.AccountNumber, &row.AccountNickname, &row.IBAN, &row.BranchName,
				&row.BankID, &row.EntityID, &row.BankName, &row.OldBankName, &row.Country, &row.OldCountry,
				&row.Relationship, &row.OldRelationship, &row.Usage, &row.OldUsage, &row.Currency, &row.OldCurrency,
				&row.Nickname, &row.OldNickname, &row.AccountNo, &row.OldAccountNo, &row.EffFrom, &row.OldEffFrom,
				&row.EffTo, &row.OldEffTo, &row.Status, &row.OldStatus, &row.BranchEmail, &row.OldBranchEmail,
				&row.BranchPhone, &row.OldBranchPhone, &row.Addr1, &row.OldAddr1, &row.Addr2, &row.OldAddr2,
				&row.City, &row.OldCity, &row.State, &row.OldState, &row.Postal, &row.OldPostal,
				&row.ErpType, &row.OldErpType, &row.SapBukrs, &row.OldSapBukrs, &row.SapHbkid, &row.OldSapHbkid,
				&row.SapHktid, &row.OldSapHktid, &row.SapBankl, &row.OldSapBankl, &row.OraLedger, &row.OldOraLedger,
				&row.OraBranch, &row.OldOraBranch, &row.OraAccount, &row.OldOraAccount, &row.TallyLedger, &row.OldTallyLedger,
				&row.SageCC, &row.OldSageCC, &row.CatInflow, &row.OldCatInflow, &row.CatOutflow, &row.OldCatOutflow,
				&row.CatCharges, &row.OldCatCharges, &row.CatIntInc, &row.OldCatIntInc, &row.CatIntExp, &row.OldCatIntExp,
				&row.ConnChannel, &row.OldConnChannel, &row.ConnTz, &row.OldConnTz, &row.ConnCutoff, &row.OldConnCutoff,
				&row.ApiBase, &row.OldApiBase, &row.ApiAuth, &row.OldApiAuth, &row.SftpHost, &row.OldSftpHost,
				&row.SftpPort, &row.OldSftpPort, &row.SftpUser, &row.OldSftpUser, &row.SftpFolder, &row.OldSftpFolder,
				&row.EbicsHostID, &row.OldEbicsHostID, &row.EbicsPartnerID, &row.OldEbicsPartnerID, &row.EbicsUserID, &row.OldEbicsUserID,
				&row.SwiftBic, &row.OldSwiftBic, &row.SwiftService, &row.OldSwiftService, &row.PortalURL, &row.OldPortalURL,
				&row.PortalNotes, &row.OldPortalNotes, &row.ClearingCodes, &row.BankIDOut, &row.BankNameOut, &row.EntityIDOut, &row.EntityNameOut,
			); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, err.Error())
				return
			}
		} else {
			api.RespondWithError(w, http.StatusNotFound, "account not found")
			return
		}

		// fetch audit info for this account
		auditMap := make(map[string]map[string]interface{})
		auditQuery := `SELECT DISTINCT ON (account_id) account_id, processing_status, requested_by, requested_at, actiontype, action_id, checker_by, checker_at, checker_comment, reason FROM auditactionbankaccount WHERE account_id=$1 ORDER BY account_id, requested_at DESC`
		arows, err := pgxPool.Query(ctx, auditQuery, req.AccountID)
		if err == nil {
			defer arows.Close()
			for arows.Next() {
				var accID string
				var processingStatusPtr, requestedByPtr, actionTypePtr, actionIDPtr, checkerByPtr, checkerCommentPtr, reasonPtr *string
				var requestedAtPtr, checkerAtPtr *time.Time
				if err := arows.Scan(&accID, &processingStatusPtr, &requestedByPtr, &requestedAtPtr, &actionTypePtr, &actionIDPtr, &checkerByPtr, &checkerAtPtr, &checkerCommentPtr, &reasonPtr); err == nil {
					proc := ""
					if processingStatusPtr != nil {
						proc = *processingStatusPtr
					}
					actType := ""
					if actionTypePtr != nil {
						actType = *actionTypePtr
					}
					actID := ""
					if actionIDPtr != nil {
						actID = *actionIDPtr
					}
					reqBy := ""
					if requestedByPtr != nil {
						reqBy = *requestedByPtr
					}
					reqAt := ""
					if requestedAtPtr != nil {
						reqAt = requestedAtPtr.Format(constants.DateTimeFormat)
					}
					chkBy := ""
					if checkerByPtr != nil {
						chkBy = *checkerByPtr
					}
					chkAt := ""
					if checkerAtPtr != nil {
						chkAt = checkerAtPtr.Format(constants.DateTimeFormat)
					}
					chkComment := ""
					if checkerCommentPtr != nil {
						chkComment = *checkerCommentPtr
					}
					reason := ""
					if reasonPtr != nil {
						reason = *reasonPtr
					}
					auditMap[accID] = map[string]interface{}{
						"processing_status": proc,
						"action_type":       actType,
						"action_id":         actID,
						"requested_by":      reqBy,
						"requested_at":      reqAt,
						"checker_by":        chkBy,
						"checker_at":        chkAt,
						"checker_comment":   chkComment,
						"reason":            reason,
					}
				}
			}
		}

		// fetch audit detail
		auditDetailMap := make(map[string]api.ActionAuditInfo)
		adQuery := `SELECT account_id, actiontype, requested_by, requested_at FROM auditactionbankaccount WHERE account_id=$1 AND actiontype IN ('CREATE','EDIT','DELETE') ORDER BY account_id, requested_at DESC`
		adRows, err := pgxPool.Query(ctx, adQuery, req.AccountID)
		if err == nil {
			defer adRows.Close()
			for adRows.Next() {
				var accID, adType string
				var adByPtr *string
				var adAtPtr *time.Time
				if err := adRows.Scan(&accID, &adType, &adByPtr, &adAtPtr); err == nil {
					info := api.GetAuditInfo(adType, adByPtr, adAtPtr)
					audit := auditDetailMap[accID]
					if info.CreatedBy != "" {
						audit.CreatedBy = info.CreatedBy
						audit.CreatedAt = info.CreatedAt
					}
					if info.EditedBy != "" {
						audit.EditedBy = info.EditedBy
						audit.EditedAt = info.EditedAt
					}
					if info.DeletedBy != "" {
						audit.DeletedBy = info.DeletedBy
						audit.DeletedAt = info.DeletedAt
					}
					auditDetailMap[accID] = audit
				}
			}
		}

		// build response with all fields
		var clearingCodes []map[string]interface{}
		_ = json.Unmarshal(row.ClearingCodes, &clearingCodes)
		audit := auditMap[row.AccountID]
		auditInfo := auditDetailMap[row.AccountID]

		// Helper function to safely get string values
		getString := func(s *string) string {
			if s != nil {
				return *s
			}
			return ""
		}

		// Helper function to safely get time values
		getDate := func(t *time.Time) string {
			if t != nil {
				return t.Format(constants.DateFormat)
			}
			return ""
		}

		// Helper function to safely get int values
		getInt := func(i *int) int {
			if i != nil {
				return *i
			}
			return 0
		}

		out := map[string]interface{}{
			// Basic account info
			"account_id":       row.AccountID,
			"account_number":   row.AccountNumber,
			"account_nickname": getString(row.AccountNickname),
			"iban":             getString(row.IBAN),
			"branch_name":      getString(row.BranchName),

			// Bank and entity info
			"bank_id":     getString(row.BankID),
			"entity_id":   getString(row.EntityID),
			"bank_name":   getString(row.BankNameOut),
			"entity_name": getString(row.EntityNameOut),

			// Location info
			"country": getString(row.Country),
			"addr1":   getString(row.Addr1),
			"addr2":   getString(row.Addr2),
			"city":    getString(row.City),
			"state":   getString(row.State),
			"postal":  getString(row.Postal),

			// Account details
			"relationship":      getString(row.Relationship),
			"usage":             getString(row.Usage),
			"currency":          getString(row.Currency),
			"nickname":          getString(row.Nickname),
			"account_no":        getString(row.AccountNo),
			constants.KeyStatus: getString(row.Status),

			// Dates
			"eff_from": getDate(row.EffFrom),
			"eff_to":   getDate(row.EffTo),

			// Contact info
			"branch_email": getString(row.BranchEmail),
			"branch_phone": getString(row.BranchPhone),

			// ERP info
			"erp_type":     getString(row.ErpType),
			"sap_bukrs":    getString(row.SapBukrs),
			"sap_hbkid":    getString(row.SapHbkid),
			"sap_hktid":    getString(row.SapHktid),
			"sap_bankl":    getString(row.SapBankl),
			"ora_ledger":   getString(row.OraLedger),
			"ora_branch":   getString(row.OraBranch),
			"ora_account":  getString(row.OraAccount),
			"tally_ledger": getString(row.TallyLedger),
			"sage_cc":      getString(row.SageCC),

			// Categories
			"cat_inflow":  getString(row.CatInflow),
			"cat_outflow": getString(row.CatOutflow),
			"cat_charges": getString(row.CatCharges),
			"cat_int_inc": getString(row.CatIntInc),
			"cat_int_exp": getString(row.CatIntExp),

			// Connection info
			"conn_channel": getString(row.ConnChannel),
			"conn_tz":      getString(row.ConnTz),
			"conn_cutoff":  getString(row.ConnCutoff),
			"api_base":     getString(row.ApiBase),
			"api_auth":     getString(row.ApiAuth),

			// SFTP info
			"sftp_host":   getString(row.SftpHost),
			"sftp_port":   getInt(row.SftpPort),
			"sftp_user":   getString(row.SftpUser),
			"sftp_folder": getString(row.SftpFolder),

			// EBICS info
			"ebics_host_id":    getString(row.EbicsHostID),
			"ebics_partner_id": getString(row.EbicsPartnerID),
			"ebics_user_id":    getString(row.EbicsUserID),

			// SWIFT info
			"swift_bic":     getString(row.SwiftBic),
			"swift_service": getString(row.SwiftService),
			"portal_url":    getString(row.PortalURL),
			"portal_notes":  getString(row.PortalNotes),

			// Old fields - all the old values
			"old_bank_name":        getString(row.OldBankName),
			"old_country":          getString(row.OldCountry),
			"old_relationship":     getString(row.OldRelationship),
			"old_usage":            getString(row.OldUsage),
			"old_currency":         getString(row.OldCurrency),
			"old_nickname":         getString(row.OldNickname),
			"old_account_no":       getString(row.OldAccountNo),
			"old_status":           getString(row.OldStatus),
			"old_eff_from":         getDate(row.OldEffFrom),
			"old_eff_to":           getDate(row.OldEffTo),
			"old_branch_email":     getString(row.OldBranchEmail),
			"old_branch_phone":     getString(row.OldBranchPhone),
			"old_addr1":            getString(row.OldAddr1),
			"old_addr2":            getString(row.OldAddr2),
			"old_city":             getString(row.OldCity),
			"old_state":            getString(row.OldState),
			"old_postal":           getString(row.OldPostal),
			"old_erp_type":         getString(row.OldErpType),
			"old_sap_bukrs":        getString(row.OldSapBukrs),
			"old_sap_hbkid":        getString(row.OldSapHbkid),
			"old_sap_hktid":        getString(row.OldSapHktid),
			"old_sap_bankl":        getString(row.OldSapBankl),
			"old_ora_ledger":       getString(row.OldOraLedger),
			"old_ora_branch":       getString(row.OldOraBranch),
			"old_ora_account":      getString(row.OldOraAccount),
			"old_tally_ledger":     getString(row.OldTallyLedger),
			"old_sage_cc":          getString(row.OldSageCC),
			"old_cat_inflow":       getString(row.OldCatInflow),
			"old_cat_outflow":      getString(row.OldCatOutflow),
			"old_cat_charges":      getString(row.OldCatCharges),
			"old_cat_int_inc":      getString(row.OldCatIntInc),
			"old_cat_int_exp":      getString(row.OldCatIntExp),
			"old_conn_channel":     getString(row.OldConnChannel),
			"old_conn_tz":          getString(row.OldConnTz),
			"old_conn_cutoff":      getString(row.OldConnCutoff),
			"old_api_base":         getString(row.OldApiBase),
			"old_api_auth":         getString(row.OldApiAuth),
			"old_sftp_host":        getString(row.OldSftpHost),
			"old_sftp_port":        getInt(row.OldSftpPort),
			"old_sftp_user":        getString(row.OldSftpUser),
			"old_sftp_folder":      getString(row.OldSftpFolder),
			"old_ebics_host_id":    getString(row.OldEbicsHostID),
			"old_ebics_partner_id": getString(row.OldEbicsPartnerID),
			"old_ebics_user_id":    getString(row.OldEbicsUserID),
			"old_swift_bic":        getString(row.OldSwiftBic),
			"old_swift_service":    getString(row.OldSwiftService),
			"old_portal_url":       getString(row.OldPortalURL),
			"old_portal_notes":     getString(row.OldPortalNotes),

			// Additional data
			"clearing_codes": clearingCodes,

			// Audit info
			"processing_status": func() interface{} {
				if audit != nil {
					return audit["processing_status"]
				}
				return ""
			}(),
			"action_type": func() interface{} {
				if audit != nil {
					return audit["action_type"]
				}
				return ""
			}(),
			"action_id": func() interface{} {
				if audit != nil {
					return audit["action_id"]
				}
				return ""
			}(),
			"requested_by": func() interface{} {
				if audit != nil {
					return audit["requested_by"]
				}
				return ""
			}(),
			"requested_at": func() interface{} {
				if audit != nil {
					return audit["requested_at"]
				}
				return ""
			}(),
			"checker_by": func() interface{} {
				if audit != nil {
					return audit["checker_by"]
				}
				return ""
			}(),
			"checker_at": func() interface{} {
				if audit != nil {
					return audit["checker_at"]
				}
				return ""
			}(),
			"checker_comment": func() interface{} {
				if audit != nil {
					return audit["checker_comment"]
				}
				return ""
			}(),
			"reason": func() interface{} {
				if audit != nil {
					return audit["reason"]
				}
				return ""
			}(),
			"created_by": auditInfo.CreatedBy,
			"created_at": auditInfo.CreatedAt,
			"edited_by":  auditInfo.EditedBy,
			"edited_at":  auditInfo.EditedAt,
			"deleted_by": auditInfo.DeletedBy,
			"deleted_at": auditInfo.DeletedAt,
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: true, "data": out})
	}
}

// Get summary/meta for all bank accounts: processing_status, status, entity, bank_name, account_nickname, account_id
func GetBankAccountMetaAll(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		ctx := r.Context()

		// build base query - include account_number and latest audit details plus action_id
		baseQuery := `
	SELECT 
    a.account_id, 
    a.account_number, 
    a.status,
    COALESCE(e.entity_id::text, ec.entity_id::text) AS entity_id,
    COALESCE(e.entity_name, ec.entity_name) AS entity_name,
    b.bank_name, 
    a.account_nickname,
    COALESCE(aa.processing_status, '') AS processing_status,
    COALESCE(aa.actiontype, '') AS action_type,
    COALESCE(aa.action_id::text, '') AS action_id,
    COALESCE(aa.requested_by, '') AS requested_by,
    COALESCE(to_char(aa.requested_at,'YYYY-MM-DD HH24:MI:SS'), '') AS requested_at,
    COALESCE(aa.checker_by, '') AS checker_by,
    COALESCE(to_char(aa.checker_at,'YYYY-MM-DD HH24:MI:SS'), '') AS checker_at,
    COALESCE(aa.checker_comment, '') AS checker_comment,
    COALESCE(aa.reason, '') AS reason
FROM masterbankaccount a
LEFT JOIN masterbank b ON a.bank_id = b.bank_id
LEFT JOIN masterentity e ON e.entity_id::text = a.entity_id
LEFT JOIN masterentitycash ec ON ec.entity_id::text = a.entity_id
LEFT JOIN LATERAL (
    SELECT *
    FROM auditactionbankaccount aa
    WHERE aa.account_id = a.account_id
    ORDER BY aa.requested_at DESC
    LIMIT 1
) aa ON TRUE;

		
		`

		// exclude soft-deleted accounts from meta by adding WHERE after the lateral join
		baseQuery = strings.Replace(baseQuery, ") aa ON TRUE;", ") aa ON TRUE WHERE COALESCE(a.is_deleted, false) = false;", 1)
		rows, err := pgxPool.Query(ctx, baseQuery)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		type AccountRow struct {
			AccountID       string
			AccountNumber   *string
			AccountStatus   *string
			EntityID        *string
			EntityName      *string
			BankName        *string
			AccountNickname *string
			ProcStatus      string
			ActionType      string
			ActionID        string
			RequestedBy     string
			RequestedAt     string
			CheckerBy       string
			CheckerAt       string
			CheckerComment  string
			Reason          string
		}

		var accountRows []AccountRow
		var accountIDs []string
		for rows.Next() {
			var row AccountRow
			if err := rows.Scan(&row.AccountID, &row.AccountNumber, &row.AccountStatus, &row.EntityID, &row.EntityName, &row.BankName, &row.AccountNickname, &row.ProcStatus, &row.ActionType, &row.ActionID, &row.RequestedBy, &row.RequestedAt, &row.CheckerBy, &row.CheckerAt, &row.CheckerComment, &row.Reason); err != nil {
				continue
			}
			accountRows = append(accountRows, row)
			if row.AccountID != "" {
				accountIDs = append(accountIDs, row.AccountID)
			}
		}

		// fetch audit detail for created/edited/deleted
		auditDetailMap := make(map[string]api.ActionAuditInfo)
		if len(accountIDs) > 0 {
			adQuery := `SELECT account_id, actiontype, requested_by, requested_at FROM auditactionbankaccount WHERE account_id::text = ANY($1) AND actiontype IN ('CREATE','EDIT','DELETE') ORDER BY account_id, requested_at DESC`
			adRows, err := pgxPool.Query(ctx, adQuery, pq.Array(accountIDs))
			if err == nil {
				defer adRows.Close()
				for adRows.Next() {
					var accID, adType string
					var adByPtr *string
					var adAtPtr *time.Time
					if err := adRows.Scan(&accID, &adType, &adByPtr, &adAtPtr); err == nil {
						info := api.GetAuditInfo(adType, adByPtr, adAtPtr)
						audit := auditDetailMap[accID]
						if info.CreatedBy != "" {
							audit.CreatedBy = info.CreatedBy
							audit.CreatedAt = info.CreatedAt
						}
						if info.EditedBy != "" {
							audit.EditedBy = info.EditedBy
							audit.EditedAt = info.EditedAt
						}
						if info.DeletedBy != "" {
							audit.DeletedBy = info.DeletedBy
							audit.DeletedAt = info.DeletedAt
						}
						auditDetailMap[accID] = audit
					}
				}
			}
		}

		var out []map[string]interface{}
		for _, row := range accountRows {
			auditInfo := auditDetailMap[row.AccountID]
			out = append(out, map[string]interface{}{
				"account_id": row.AccountID,
				"account_number": func() string {
					if row.AccountNumber != nil {
						return *row.AccountNumber
					}
					return ""
				}(),
				constants.KeyStatus: func() string {
					if row.AccountStatus != nil {
						return *row.AccountStatus
					}
					return ""
				}(),
				"entity_id": func() string {
					if row.EntityID != nil {
						return *row.EntityID
					}
					return ""
				}(),
				"entity_name": func() string {
					if row.EntityName != nil {
						return *row.EntityName
					}
					return ""
				}(),
				"bank_name": func() string {
					if row.BankName != nil {
						return *row.BankName
					}
					return ""
				}(),
				"account_nickname": func() string {
					if row.AccountNickname != nil {
						return *row.AccountNickname
					}
					return ""
				}(),
				"processing_status": row.ProcStatus,
				"action_type":       row.ActionType,
				"action_id":         row.ActionID,
				"requested_by":      row.RequestedBy,
				"requested_at":      row.RequestedAt,
				"checker_by":        row.CheckerBy,
				"checker_at":        row.CheckerAt,
				"checker_comment":   row.CheckerComment,
				"reason":            row.Reason,
				"created_by":        auditInfo.CreatedBy,
				"created_at":        auditInfo.CreatedAt,
				"edited_by":         auditInfo.EditedBy,
				"edited_at":         auditInfo.EditedAt,
				"deleted_by":        auditInfo.DeletedBy,
				"deleted_at":        auditInfo.DeletedAt,
			})
		}
		if out == nil {
			out = make([]map[string]interface{}, 0)
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: true, "data": out})
	}
}
