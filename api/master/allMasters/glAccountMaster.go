package allMaster

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"CimplrCorpSaas/api/constants"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// getUserFriendlyGLAccountError returns a user-friendly error message and HTTP status code
func getUserFriendlyGLAccountError(err error, context string) (string, int) {
	if err == nil {
		return "", http.StatusOK
	}

	errMsg := err.Error()
	errLower := strings.ToLower(errMsg)

	// Unique constraint violations
	if strings.Contains(errLower, "unique_gl_account_code_not_deleted") {
		return "GL Account code already exists and is not deleted. Please use a different code.", http.StatusOK
	}
	if strings.Contains(errLower, "unique_gl_account_name_not_deleted") {
		return "GL Account name already exists and is not deleted. Please use a different name.", http.StatusOK
	}
	if strings.Contains(errLower, "uq_parent_child_gl") {
		return "This parent-child GL account relationship already exists.", http.StatusOK
	}

	// Check constraints
	if strings.Contains(errLower, "masterglaccount_type_check") {
		return "GL Account type must be one of: Asset, Liability, Equity, Income, Expense.", http.StatusOK
	}
	if strings.Contains(errLower, "masterglaccount_status_check") {
		return "Status must be either 'Active' or 'Inactive'.", http.StatusOK
	}
	if strings.Contains(errLower, "masterglaccount_source_check") {
		return "Source must be one of: ERP, Manual, Upload.", http.StatusOK
	}
	if strings.Contains(errLower, "masterglaccount_old_type_check") {
		return "Old GL Account type must be one of: Asset, Liability, Equity, Income, Expense.", http.StatusOK
	}
	if strings.Contains(errLower, "masterglaccount_old_status_check") {
		return "Old status must be either 'Active' or 'Inactive'.", http.StatusOK
	}
	if strings.Contains(errLower, "masterglaccount_old_source_check") {
		return "Old source must be one of: ERP, Manual, Upload.", http.StatusOK
	}

	// Audit action check constraints
	if strings.Contains(errLower, "auditactionglaccount_actiontype_check") {
		return "Action type must be one of: CREATE, EDIT, DELETE.", http.StatusOK
	}
	if strings.Contains(errLower, "auditactionglaccount_processing_status_check") {
		return "Processing status must be one of: PENDING_APPROVAL, PENDING_EDIT_APPROVAL, PENDING_DELETE_APPROVAL, APPROVED, REJECTED, CANCELLED.", http.StatusOK
	}

	// Foreign key violations
	if strings.Contains(errLower, "foreign key") || strings.Contains(errLower, "fk_") || strings.Contains(errLower, "_fkey") {
		if strings.Contains(errLower, "parent_gl_account_id") {
			return "Parent GL Account does not exist or has been deleted.", http.StatusOK
		}
		if strings.Contains(errLower, "child_gl_account_id") {
			return "Child GL Account does not exist or has been deleted.", http.StatusOK
		}
		return "Referenced record does not exist. Please check your input.", http.StatusOK
	}

	// Not null violations
	if strings.Contains(errLower, "not null") || strings.Contains(errLower, "null value") {
		if strings.Contains(errLower, "gl_account_code") {
			return "GL Account code is required.", http.StatusOK
		}
		if strings.Contains(errLower, "gl_account_name") {
			return "GL Account name is required.", http.StatusOK
		}
		if strings.Contains(errLower, "gl_account_type") {
			return "GL Account type is required.", http.StatusOK
		}
		if strings.Contains(errLower, "status") {
			return "Status is required.", http.StatusOK
		}
		if strings.Contains(errLower, "source") {
			return "Source is required.", http.StatusOK
		}
		return "A required field is missing. Please check your input.", http.StatusOK
	}

	// Connection errors
	if strings.Contains(errLower, "connection") || strings.Contains(errLower, "timeout") {
		return fmt.Sprintf("%s: Database connection issue. Please try again.", context), http.StatusServiceUnavailable
	}

	// Default error
	return fmt.Sprintf("%s: %s", context, errMsg), http.StatusInternalServerError
}

// normalizeDateLocal mirrors the NormalizeDate implementations used elsewhere in masters.
func normalizeDateLocal(dateStr string) string {
	dateStr = strings.TrimSpace(dateStr)
	if dateStr == "" {
		return ""
	}

	layouts := []string{
		constants.DateFormat,
		constants.DateFormatAlt,
		"2006/01/02",
		"02/01/2006",
		"2006.01.02",
		"02.01.2006",
		time.RFC3339,
		constants.DateTimeFormat,
		constants.DateFormatISO,
	}
	layouts = append(layouts, []string{constants.DateFormatDash, "02-Jan-06", "2-Jan-2006", "2-Jan-06", "02-Jan-2006 15:04:05"}...)
	for _, l := range layouts {
		if t, err := time.Parse(l, dateStr); err == nil {
			return t.Format(constants.DateFormat)
		}
	}
	return ""
}

type GLAccountRequest struct {
	GLAccountCode          string `json:"gl_account_code"`
	GLAccountName          string `json:"gl_account_name"`
	GLAccountType          string `json:"gl_account_type"`
	Status                 string `json:"status"`
	Source                 string `json:"source"`
	ErpRef                 string `json:"erp_ref,omitempty"`
	ParentGLAccountCode    string `json:"parent_gl_account_code,omitempty"`
	AccountClass           string `json:"account_class,omitempty"`
	NormalBalance          string `json:"normal_balance,omitempty"`
	DefaultCurrency        string `json:"default_currency,omitempty"`
	EffectiveFrom          string `json:"effective_from,omitempty"`
	EffectiveTo            string `json:"effective_to,omitempty"`
	Tags                   string `json:"tags,omitempty"`
	ErpType                string `json:"erp_type,omitempty"`
	ExternalCode           string `json:"external_code,omitempty"`
	Segment                string `json:"segment,omitempty"`
	SAPBukrs               string `json:"sap_bukrs,omitempty"`
	SAPKtopl               string `json:"sap_ktopl,omitempty"`
	SAPSaknr               string `json:"sap_saknr,omitempty"`
	SAPKtoks               string `json:"sap_ktoks,omitempty"`
	OracleLedger           string `json:"oracle_ledger,omitempty"`
	OracleCoa              string `json:"oracle_coa,omitempty"`
	OracleBalancingSeg     string `json:"oracle_balancing_seg,omitempty"`
	OracleNaturalAccount   string `json:"oracle_natural_account,omitempty"`
	TallyLedgerName        string `json:"tally_ledger_name,omitempty"`
	TallyLedgerGroup       string `json:"tally_ledger_group,omitempty"`
	SageNominalCode        string `json:"sage_nominal_code,omitempty"`
	SageCostCentre         string `json:"sage_cost_centre,omitempty"`
	SageDepartment         string `json:"sage_department,omitempty"`
	PostingAllowed         *bool  `json:"posting_allowed,omitempty"`
	ReconciliationRequired *bool  `json:"reconciliation_required,omitempty"`
	IsCashBank             *bool  `json:"is_cash_bank,omitempty"`
	GLAccountLevel         int    `json:"gl_account_level,omitempty"`
	IsTopLevel             *bool  `json:"is_top_level_gl_account,omitempty"`
	IsDeleted              *bool  `json:"is_deleted,omitempty"`
}

// NormalizeDate is provided in other master files in this package; use that shared helper.
func FindParentGLAccountAtLevel(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			Level  int    `json:"level"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}
		valid := false
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				valid = true
				break
			}
		}
		if !valid {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidSessionCapitalized)
			return
		}

		parentLevel := req.Level - 1
		q := `
			SELECT m.gl_account_name, m.gl_account_id, m.gl_account_code
			FROM masterglaccount m
			LEFT JOIN LATERAL (
				SELECT processing_status
				FROM auditactionglaccount a
				WHERE a.gl_account_id = m.gl_account_id
				ORDER BY requested_at DESC
				LIMIT 1
			) a ON TRUE
			WHERE m.gl_account_level = $1
			  AND (m.is_deleted = false OR m.is_deleted IS NULL)
			  AND LOWER(m.status) = 'active'
			  AND a.processing_status = 'APPROVED'
		`
		rows, err := pgxPool.Query(context.Background(), q, parentLevel)
		if err != nil {
			errMsg, statusCode := getUserFriendlyGLAccountError(err, "Failed to fetch parent GL accounts")
			if statusCode == http.StatusOK {
				w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
				json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "error": errMsg})
			} else {
				api.RespondWithError(w, statusCode, errMsg)
			}
			return
		}
		defer rows.Close()
		results := []map[string]interface{}{}
		for rows.Next() {
			var name, id, code string
			if err := rows.Scan(&name, &id, &code); err == nil {
				results = append(results, map[string]interface{}{"gl_account_name": name, "gl_account_id": id, "gl_account_code": code})
			}
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: true, "results": results})
	}
}

func CreateGLAccounts(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string             `json:"user_id"`
			Rows   []GLAccountRequest `json:"rows"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}
		createdBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				createdBy = s.Name
				break
			}
		}
		if createdBy == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidSessionCapitalized)
			return
		}

		ctx := r.Context()
		currCodes := api.GetCurrencyCodesFromCtx(ctx)

		if len(currCodes) == 0 {
			api.RespondWithError(w, http.StatusForbidden, "No accessible currencies found for request")
			return
		}

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			errMsg, statusCode := getUserFriendlyGLAccountError(err, "Failed to start transaction")
			if statusCode == http.StatusOK {
				w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
				json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "error": errMsg})
			} else {
				api.RespondWithError(w, statusCode, errMsg)
			}
			return
		}
		defer func() {
			if tx != nil {
				tx.Rollback(ctx)
			}
		}()

		created := make([]map[string]interface{}, 0)
		codeToID := map[string]string{}
		type insertInfo struct {
			ID     string
			Code   string
			Parent string
			Level  int
			IsTop  bool
		}
		inserted := make([]insertInfo, 0, len(req.Rows))

		for i, rrow := range req.Rows {
			if strings.TrimSpace(rrow.GLAccountCode) == "" || strings.TrimSpace(rrow.GLAccountName) == "" || strings.TrimSpace(rrow.GLAccountType) == "" || strings.TrimSpace(rrow.Source) == "" {
				created = append(created, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "missing required fields", "gl_account_code": rrow.GLAccountCode})
				continue
			}

			// Validate currency
			if rrow.DefaultCurrency != "" && !api.IsCurrencyAllowed(ctx, rrow.DefaultCurrency) {
				created = append(created, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "invalid or unauthorized default_currency: " + rrow.DefaultCurrency, "gl_account_code": rrow.GLAccountCode})
				continue
			}

			// Validate parent GL account exists in approved GL accounts
			if rrow.ParentGLAccountCode != "" {
				var parentExists bool
				parentCheckQ := `SELECT EXISTS(
					SELECT 1 FROM masterglaccount m
					LEFT JOIN LATERAL (
						SELECT processing_status FROM auditactionglaccount
						WHERE gl_account_id = m.gl_account_id
						ORDER BY requested_at DESC LIMIT 1
					) a ON TRUE
					WHERE m.gl_account_code = $1
					AND UPPER(a.processing_status) = 'APPROVED'
					AND UPPER(m.status) = 'ACTIVE'
				)`
				if err := tx.QueryRow(ctx, parentCheckQ, rrow.ParentGLAccountCode).Scan(&parentExists); err != nil || !parentExists {
					created = append(created, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "parent_gl_code not found or not approved: " + rrow.ParentGLAccountCode, "gl_account_code": rrow.GLAccountCode})
					continue
				}
			}

			sp := fmt.Sprintf("sp_%d", i)
			if _, err := tx.Exec(ctx, "SAVEPOINT "+sp); err != nil {
				tx.Rollback(ctx)
				errMsg, statusCode := getUserFriendlyGLAccountError(err, "Failed to create savepoint")
				if statusCode == http.StatusOK {
					w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
					json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "error": errMsg})
				} else {
					api.RespondWithError(w, statusCode, errMsg)
				}
				return
			}
			var effectiveFrom interface{}
			var effectiveTo interface{}
			if rrow.EffectiveFrom != "" {
				if norm := NormalizeDate(rrow.EffectiveFrom); norm != "" {
					if tval, err := time.Parse(constants.DateFormat, norm); err == nil {
						effectiveFrom = tval
					}
				}
			}
			if rrow.EffectiveTo != "" {
				if norm := NormalizeDate(rrow.EffectiveTo); norm != "" {
					if tval, err := time.Parse(constants.DateFormat, norm); err == nil {
						effectiveTo = tval
					}
				}
			}

			insQ := `INSERT INTO masterglaccount (
					gl_account_code, gl_account_name, gl_account_type, status, source,
				account_class, normal_balance, default_currency, effective_from, effective_to, tags, erp_type, external_code, segment,
				sap_bukrs, sap_ktopl, sap_saknr, sap_ktoks, oracle_ledger, oracle_coa, oracle_balancing_seg, oracle_natural_account,
				tally_ledger_name, tally_ledger_group, sage_nominal_code, sage_cost_centre, sage_department, posting_allowed,
				reconciliation_required, is_cash_bank, gl_account_level, is_top_level_gl_account, is_deleted, parent_gl_code
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34) RETURNING gl_account_id`
			var id string

			if err := tx.QueryRow(ctx, insQ,
				rrow.GLAccountCode,
				rrow.GLAccountName,
				rrow.GLAccountType,
				rrow.Status,
				rrow.Source,
				rrow.AccountClass,
				rrow.NormalBalance,
				rrow.DefaultCurrency,
				effectiveFrom,
				effectiveTo,
				rrow.Tags,
				rrow.ErpType,
				rrow.ExternalCode,
				rrow.Segment,
				rrow.SAPBukrs,
				rrow.SAPKtopl,
				rrow.SAPSaknr,
				rrow.SAPKtoks,
				rrow.OracleLedger,
				rrow.OracleCoa,
				rrow.OracleBalancingSeg,
				rrow.OracleNaturalAccount,
				rrow.TallyLedgerName,
				rrow.TallyLedgerGroup,
				rrow.SageNominalCode,
				rrow.SageCostCentre,
				rrow.SageDepartment,
				rrow.PostingAllowed,
				rrow.ReconciliationRequired,
				rrow.IsCashBank,
				rrow.GLAccountLevel,
				rrow.IsTopLevel,
				rrow.IsDeleted,
				nil,
			).Scan(&id); err != nil {
				tx.Exec(ctx, "ROLLBACK TO SAVEPOINT "+sp)
				created = append(created, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: err.Error(), "gl_account_code": rrow.GLAccountCode})
				continue
			}

			auditQ := `INSERT INTO auditactionglaccount (gl_account_id, actiontype, processing_status, reason, requested_by, requested_at) VALUES ($1,'CREATE','PENDING_APPROVAL', $2, $3, now())`
			if _, err := tx.Exec(ctx, auditQ, id, nil, createdBy); err != nil {
				tx.Exec(ctx, "ROLLBACK TO SAVEPOINT "+sp)
				created = append(created, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: constants.ErrAuditInsertFailed + err.Error(), "gl_account_id": id})
				continue
			}

			if _, err := tx.Exec(ctx, "RELEASE SAVEPOINT "+sp); err != nil {
				tx.Rollback(ctx)
				errMsg, statusCode := getUserFriendlyGLAccountError(err, "Failed to release savepoint")
				if statusCode == http.StatusOK {
					w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
					json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "error": errMsg})
				} else {
					api.RespondWithError(w, statusCode, errMsg)
				}
				return
			}

			codeToID[strings.TrimSpace(rrow.GLAccountCode)] = id
			inserted = append(inserted, insertInfo{ID: id, Code: strings.TrimSpace(rrow.GLAccountCode), Parent: strings.TrimSpace(rrow.ParentGLAccountCode), Level: 0, IsTop: false})
			created = append(created, map[string]interface{}{constants.ValueSuccess: true, "gl_account_id": id, "gl_account_code": rrow.GLAccountCode})
		}

		for _, info := range inserted {
			if info.Parent == "" {
				continue
			}
			var parentID string
			if pid, ok := codeToID[info.Parent]; ok {
				parentID = pid
			} else {
				if err := tx.QueryRow(ctx, `SELECT gl_account_id FROM masterglaccount WHERE gl_account_code=$1`, info.Parent).Scan(&parentID); err != nil {
					continue
				}
			}

			relQ := `INSERT INTO glaccountrelationships (parent_gl_account_id, child_gl_account_id) VALUES ($1,$2) ON CONFLICT DO NOTHING`
			if _, err := tx.Exec(ctx, relQ, parentID, info.ID); err != nil {
				created = append(created, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: constants.ErrBulkRelationshipInsertFailed + err.Error(), "gl_account_id": info.ID})
				continue
			}

			var parentLevel int
			if err := tx.QueryRow(ctx, `SELECT gl_account_level FROM masterglaccount WHERE gl_account_id=$1`, parentID).Scan(&parentLevel); err == nil {
				_, _ = tx.Exec(ctx, `UPDATE masterglaccount SET parent_gl_code=$1, gl_account_level=$2, is_top_level_gl_account=false WHERE gl_account_id=$3`, info.Parent, parentLevel+1, info.ID)
			} else {
				_, _ = tx.Exec(ctx, `UPDATE masterglaccount SET parent_gl_code=$1 WHERE gl_account_id=$2`, info.Parent, info.ID)
			}
		}

		if err := tx.Commit(ctx); err != nil {
			errMsg, statusCode := getUserFriendlyGLAccountError(err, "Failed to save GL accounts")
			if statusCode == http.StatusOK {
				w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
				json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "error": errMsg})
			} else {
				api.RespondWithError(w, statusCode, errMsg)
			}
			return
		}
		tx = nil

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: true, "rows": created})
	}
}

// type glaAuditInfo struct{ CreatedBy, CreatedAt, EditedBy, EditedAt, DeletedBy, DeletedAt string }

func GetGLAccountNamesWithID(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}
		valid := false
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				valid = true
				break
			}
		}
		if !valid {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidSessionCapitalized)
			return
		}

		ctx := r.Context()
		currCodes := api.GetCurrencyCodesFromCtx(ctx)

		if len(currCodes) == 0 {
			w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: true, "data": []map[string]interface{}{}})
			return
		}

		mainQ := `
			SELECT 
				m.gl_account_id, m.gl_account_code, m.gl_account_name, m.gl_account_type, m.status, m.source,
				m.old_gl_account_code, m.old_gl_account_name, m.old_gl_account_type, m.old_status, m.old_source,
				l.processing_status, l.requested_by, l.requested_at, l.actiontype, l.action_id, l.checker_by, l.checker_at, l.checker_comment, l.reason,
				m.account_class, m.old_account_class, m.normal_balance, m.old_normal_balance, m.default_currency, m.old_default_currency,
				m.parent_gl_code, m.old_parent_gl_code, m.effective_from, m.old_effective_from, m.effective_to, m.old_effective_to,
				m.tags, m.old_tags, m.erp_type, m.old_erp_type, m.external_code, m.old_external_code, m.segment, m.old_segment,
				m.sap_bukrs, m.old_sap_bukrs, m.sap_ktopl, m.old_sap_ktopl, m.sap_saknr, m.old_sap_saknr, m.sap_ktoks, m.old_sap_ktoks,
				m.oracle_ledger, m.old_oracle_ledger, m.oracle_coa, m.old_oracle_coa, m.oracle_balancing_seg, m.old_oracle_balancing_seg, m.oracle_natural_account, m.old_oracle_natural_account,
				m.tally_ledger_name, m.old_tally_ledger_name, m.tally_ledger_group, m.old_tally_ledger_group, m.sage_nominal_code, m.old_sage_nominal_code,
				m.sage_cost_centre, m.old_sage_cost_centre, m.sage_department, m.old_sage_department, m.posting_allowed, m.old_posting_allowed,
				m.reconciliation_required, m.old_reconciliation_required, m.is_cash_bank, m.old_is_cash_bank, m.gl_account_level, m.old_gl_account_level, m.is_top_level_gl_account, m.is_deleted
			FROM masterglaccount m
			LEFT JOIN LATERAL (
				SELECT processing_status, requested_by, requested_at, actiontype, action_id, checker_by, checker_at, checker_comment, reason
				FROM auditactionglaccount a
				WHERE a.gl_account_id = m.gl_account_id
				ORDER BY requested_at DESC
				LIMIT 1
			) l ON TRUE
			WHERE (m.default_currency IS NULL OR m.default_currency = ANY($1))
			ORDER BY GREATEST(COALESCE(l.requested_at, '1970-01-01'::timestamp), COALESCE(l.checker_at, '1970-01-01'::timestamp)) DESC
		`

		rows, err := pgxPool.Query(ctx, mainQ, currCodes)
		if err != nil {
			errMsg, statusCode := getUserFriendlyGLAccountError(err, "Failed to fetch GL accounts")
			if statusCode == http.StatusOK {
				api.RespondWithResult(w, false, errMsg)
			} else {
				api.RespondWithError(w, statusCode, errMsg)
			}
			return
		}
		defer rows.Close()

		entityMap := map[string]map[string]interface{}{}
		glIDs := []string{}
		hideIds := map[string]bool{}

		for rows.Next() {
			var (
				id, actionIDI                                                                                                                string
				codeI, nameI, typeI, statusI, sourceI                                                                                        interface{}
				oldCodeI, oldNameI, oldTypeI, oldStatusI, oldSourceI                                                                         interface{}
				procStatusI, requestedByI, requestedAtI, actionTypeI                                                                         interface{}
				checkerByI, checkerAtI, checkerCommentI, reasonI                                                                             interface{}
				accountClassI, oldAccountClassI, normalBalanceI, oldNormalBalanceI, defaultCurrencyI, oldDefaultCurrencyI                    interface{}
				parentCodeI, oldParentCodeI, effectiveFromI, oldEffectiveFromI, effectiveToI, oldEffectiveToI                                interface{}
				tagsI, oldTagsI, erpTypeI, oldErpTypeI, externalCodeI, oldExternalCodeI, segmentI, oldSegmentI                               interface{}
				sapBukrsI, oldSapBukrsI, sapKtoplI, oldSapKtoplI, sapSaknrI, oldSapSaknrI, sapKtoksI, oldSapKtoksI                           interface{}
				oracleLedgerI, oldOracleLedgerI, oracleCoaI, oldOracleCoaI, oracleBalSegI, oldOracleBalSegI, oracleNatAccI, oldOracleNatAccI interface{}
				tallyNameI, oldTallyNameI, tallyGroupI, oldTallyGroupI, sageNominalI, oldSageNominalI                                        interface{}
				sageCostCentreI, oldSageCostCentreI, sageDeptI, oldSageDeptI, postingAllowedI, oldPostingAllowedI                            interface{}
				reconReqI, oldReconReqI, isCashBankI, oldIsCashBankI, glLevelI, oldGlLevelI                                                  interface{}
				isTopLevelBool, isDeletedBool                                                                                                bool
			)

			if err := rows.Scan(
				&id, &codeI, &nameI, &typeI, &statusI, &sourceI,
				&oldCodeI, &oldNameI, &oldTypeI, &oldStatusI, &oldSourceI,
				&procStatusI, &requestedByI, &requestedAtI, &actionTypeI, &actionIDI, &checkerByI, &checkerAtI, &checkerCommentI, &reasonI,
				&accountClassI, &oldAccountClassI, &normalBalanceI, &oldNormalBalanceI, &defaultCurrencyI, &oldDefaultCurrencyI,
				&parentCodeI, &oldParentCodeI, &effectiveFromI, &oldEffectiveFromI, &effectiveToI, &oldEffectiveToI,
				&tagsI, &oldTagsI, &erpTypeI, &oldErpTypeI, &externalCodeI, &oldExternalCodeI, &segmentI, &oldSegmentI,
				&sapBukrsI, &oldSapBukrsI, &sapKtoplI, &oldSapKtoplI, &sapSaknrI, &oldSapSaknrI, &sapKtoksI, &oldSapKtoksI,
				&oracleLedgerI, &oldOracleLedgerI, &oracleCoaI, &oldOracleCoaI, &oracleBalSegI, &oldOracleBalSegI, &oracleNatAccI, &oldOracleNatAccI,
				&tallyNameI, &oldTallyNameI, &tallyGroupI, &oldTallyGroupI, &sageNominalI, &oldSageNominalI,
				&sageCostCentreI, &oldSageCostCentreI, &sageDeptI, &oldSageDeptI, &postingAllowedI, &oldPostingAllowedI,
				&reconReqI, &oldReconReqI, &isCashBankI, &oldIsCashBankI, &glLevelI, &oldGlLevelI, &isTopLevelBool, &isDeletedBool,
			); err != nil {
				continue
			}

			entityMap[id] = map[string]interface{}{
				"id":   id,
				"name": ifaceToString(nameI),
				"data": map[string]interface{}{
					"gl_account_id":     id,
					"gl_account_code":   ifaceToString(codeI),
					"gl_account_name":   ifaceToString(nameI),
					"gl_account_type":   ifaceToString(typeI),
					constants.KeyStatus: ifaceToString(statusI),
					"source":            ifaceToString(sourceI),

					"old_gl_account_code": ifaceToString(oldCodeI),
					"old_gl_account_name": ifaceToString(oldNameI),
					"old_gl_account_type": ifaceToString(oldTypeI),
					"old_status":          ifaceToString(oldStatusI),
					"old_source":          ifaceToString(oldSourceI),

					"account_class":        ifaceToString(accountClassI),
					"old_account_class":    ifaceToString(oldAccountClassI),
					"normal_balance":       ifaceToString(normalBalanceI),
					"old_normal_balance":   ifaceToString(oldNormalBalanceI),
					"default_currency":     ifaceToString(defaultCurrencyI),
					"old_default_currency": ifaceToString(oldDefaultCurrencyI),

					"parent_gl_code":          ifaceToString(parentCodeI),
					"old_parent_gl_code":      ifaceToString(oldParentCodeI),
					"gl_account_level":        ifaceToInt(glLevelI),
					"old_gl_account_level":    ifaceToInt(oldGlLevelI),
					"is_top_level_gl_account": isTopLevelBool,
					"is_deleted":              isDeletedBool,

					"effective_from":     ifaceToDateString(effectiveFromI),
					"old_effective_from": ifaceToDateString(oldEffectiveFromI),
					"effective_to":       ifaceToDateString(effectiveToI),
					"old_effective_to":   ifaceToDateString(oldEffectiveToI),
					"tags":               ifaceToString(tagsI),
					"old_tags":           ifaceToString(oldTagsI),

					"erp_type":          ifaceToString(erpTypeI),
					"old_erp_type":      ifaceToString(oldErpTypeI),
					"external_code":     ifaceToString(externalCodeI),
					"old_external_code": ifaceToString(oldExternalCodeI),
					"segment":           ifaceToString(segmentI),
					"old_segment":       ifaceToString(oldSegmentI),

					"sap_bukrs":     ifaceToString(sapBukrsI),
					"old_sap_bukrs": ifaceToString(oldSapBukrsI),
					"sap_ktopl":     ifaceToString(sapKtoplI),
					"old_sap_ktopl": ifaceToString(oldSapKtoplI),
					"sap_saknr":     ifaceToString(sapSaknrI),
					"old_sap_saknr": ifaceToString(oldSapSaknrI),
					"sap_ktoks":     ifaceToString(sapKtoksI),
					"old_sap_ktoks": ifaceToString(oldSapKtoksI),

					"oracle_ledger":              ifaceToString(oracleLedgerI),
					"old_oracle_ledger":          ifaceToString(oldOracleLedgerI),
					"oracle_coa":                 ifaceToString(oracleCoaI),
					"old_oracle_coa":             ifaceToString(oldOracleCoaI),
					"oracle_balancing_seg":       ifaceToString(oracleBalSegI),
					"old_oracle_balancing_seg":   ifaceToString(oldOracleBalSegI),
					"oracle_natural_account":     ifaceToString(oracleNatAccI),
					"old_oracle_natural_account": ifaceToString(oldOracleNatAccI),

					"tally_ledger_name":      ifaceToString(tallyNameI),
					"old_tally_ledger_name":  ifaceToString(oldTallyNameI),
					"tally_ledger_group":     ifaceToString(tallyGroupI),
					"old_tally_ledger_group": ifaceToString(oldTallyGroupI),
					"sage_nominal_code":      ifaceToString(sageNominalI),
					"old_sage_nominal_code":  ifaceToString(oldSageNominalI),
					"sage_cost_centre":       ifaceToString(sageCostCentreI),
					"old_sage_cost_centre":   ifaceToString(oldSageCostCentreI),
					"sage_department":        ifaceToString(sageDeptI),
					"old_sage_department":    ifaceToString(oldSageDeptI),

					"posting_allowed":             postingAllowedI,
					"old_posting_allowed":         oldPostingAllowedI,
					"reconciliation_required":     reconReqI,
					"old_reconciliation_required": oldReconReqI,
					"is_cash_bank":                isCashBankI,
					"old_is_cash_bank":            oldIsCashBankI,

					"processing_status": ifaceToString(procStatusI),
					"action_type":       ifaceToString(actionTypeI),
					"action_id":         actionIDI,
					"checker_by":        ifaceToString(checkerByI),
					"checker_at":        ifaceToTimeString(checkerAtI),
					"checker_comment":   ifaceToString(checkerCommentI),
					"reason":            ifaceToString(reasonI),
					"requested_by":      ifaceToString(requestedByI),
					"requested_at":      ifaceToTimeString(requestedAtI),
					"created_by":        "", "created_at": "", "edited_by": "", "edited_at": "", "deleted_by": "", "deleted_at": "",
				},
				"children": []interface{}{},
			}

			glIDs = append(glIDs, id)
			if isDeletedBool && strings.ToUpper(ifaceToString(procStatusI)) == "APPROVED" {
				hideIds[id] = true
			}
		}

		if len(glIDs) > 0 {
			auditQ := `SELECT gl_account_id, actiontype, requested_by, requested_at FROM auditactionglaccount WHERE gl_account_id = ANY($1) AND actiontype IN ('CREATE','EDIT','DELETE') ORDER BY requested_at DESC`
			arows, aerr := pgxPool.Query(ctx, auditQ, glIDs)
			if aerr == nil {
				defer arows.Close()
				auditMap := make(map[string]map[string]string)
				for arows.Next() {
					var gid, atype string
					var rby, rat interface{}
					if err := arows.Scan(&gid, &atype, &rby, &rat); err == nil {
						if auditMap[gid] == nil {
							auditMap[gid] = map[string]string{}
						}
						switch atype {
						case "CREATE":
							if auditMap[gid]["created_by"] == "" {
								auditMap[gid]["created_by"] = ifaceToString(rby)
								auditMap[gid]["created_at"] = ifaceToTimeString(rat)
							}
						case "EDIT":
							if auditMap[gid]["edited_by"] == "" {
								auditMap[gid]["edited_by"] = ifaceToString(rby)
								auditMap[gid]["edited_at"] = ifaceToTimeString(rat)
							}
						case "DELETE":
							if auditMap[gid]["deleted_by"] == "" {
								auditMap[gid]["deleted_by"] = ifaceToString(rby)
								auditMap[gid]["deleted_at"] = ifaceToTimeString(rat)
							}
						}
					}
				}
				for gid, info := range auditMap {
					if ent, ok := entityMap[gid]; ok {
						data := ent["data"].(map[string]interface{})
						for k, v := range info {
							data[k] = v
						}
					}
				}
			}
		}

		relRows, err := pgxPool.Query(ctx, `SELECT parent_gl_account_id, child_gl_account_id FROM glaccountrelationships`)
		if err != nil {
			api.RespondWithResult(w, false, err.Error())
			return
		}
		defer relRows.Close()
		parentMap := map[string][]string{}
		for relRows.Next() {
			var p, c string
			if err := relRows.Scan(&p, &c); err == nil {
				parentMap[p] = append(parentMap[p], c)
			}
		}

		if len(hideIds) > 0 {
			getAllDescendants := func(start []string) []string {
				all := map[string]bool{}
				queue := append([]string{}, start...)
				for _, id := range start {
					all[id] = true
				}
				for len(queue) > 0 {
					cur := queue[0]
					queue = queue[1:]
					for _, child := range parentMap[cur] {
						if !all[child] {
							all[child] = true
							queue = append(queue, child)
						}
					}
				}
				res := []string{}
				for id := range all {
					res = append(res, id)
				}
				return res
			}
			start := []string{}
			for id := range hideIds {
				start = append(start, id)
			}
			toHide := getAllDescendants(start)
			for _, id := range toHide {
				delete(entityMap, id)
			}
		}

		for _, e := range entityMap {
			e["children"] = []interface{}{}
		}
		for parentID, children := range parentMap {
			if entityMap[parentID] != nil {
				for _, childID := range children {
					if entityMap[childID] != nil {
						entityMap[parentID]["children"] = append(entityMap[parentID]["children"].([]interface{}), entityMap[childID])
					}
				}
			}
		}

		childSet := map[string]bool{}
		for _, children := range parentMap {
			for _, childID := range children {
				childSet[childID] = true
			}
		}
		topLevel := []interface{}{}
		for _, e := range entityMap {
			if !childSet[e["id"].(string)] {
				topLevel = append(topLevel, e)
			}
		}

		api.RespondWithPayload(w, true, "", topLevel)
	}
}

func GetApprovedActiveGLAccounts(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}
		valid := false
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				valid = true
				break
			}
		}
		if !valid {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidSessionCapitalized)
			return
		}

		ctx := r.Context()
		currCodes := api.GetCurrencyCodesFromCtx(ctx)

		if len(currCodes) == 0 {
			w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: true, "rows": []map[string]interface{}{}})
			return
		}

		q := `
            WITH latest AS (
                SELECT DISTINCT ON (gl_account_id) gl_account_id, processing_status
                FROM auditactionglaccount
                ORDER BY gl_account_id, requested_at DESC
            )
            SELECT m.gl_account_id, m.gl_account_code, m.gl_account_name, m.gl_account_type
            FROM masterglaccount m
            JOIN latest l ON l.gl_account_id = m.gl_account_id
            WHERE UPPER(l.processing_status) = 'APPROVED' AND UPPER(m.status) = 'ACTIVE'
				AND (m.default_currency IS NULL OR m.default_currency = ANY($1))
        `
		rows, err := pgxPool.Query(ctx, q, currCodes)
		if err != nil {
			errMsg, statusCode := getUserFriendlyGLAccountError(err, "Failed to fetch approved GL accounts")
			if statusCode == http.StatusOK {
				w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
				json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "error": errMsg})
			} else {
				api.RespondWithError(w, statusCode, errMsg)
			}
			return
		}
		defer rows.Close()
		out := []map[string]interface{}{}
		for rows.Next() {
			var id string
			var code, name, typ interface{}
			if err := rows.Scan(&id, &code, &name, &typ); err != nil {
				continue
			}
			out = append(out, map[string]interface{}{"gl_account_id": id, "gl_account_code": ifaceToString(code), "gl_account_name": ifaceToString(name), "gl_account_type": ifaceToString(typ)})
		}
		if err := rows.Err(); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: true, "rows": out})
	}
}

func UpdateAndSyncGLAccounts(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			Rows   []struct {
				GLAccountID string                 `json:"gl_account_id"`
				Fields      map[string]interface{} `json:"fields"`
				Reason      string                 `json:"reason"`
			} `json:"rows"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}
		updatedBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				updatedBy = s.Name
				break
			}
		}
		if updatedBy == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidSessionCapitalized)
			return
		}

		ctx := r.Context()
		results := []map[string]interface{}{}
		for _, row := range req.Rows {
			if strings.TrimSpace(row.GLAccountID) == "" {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "missing gl_account_id", "gl_account_id": row.GLAccountID})
				continue
			}
			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "begin failed: " + err.Error(), "gl_account_id": row.GLAccountID})
				continue
			}
			committed := false
			func() {
				defer func() {
					if !committed {
						tx.Rollback(ctx)
					}
				}()

				var existingCode, existingName, existingType, existingParentCode, existingStatus, existingSource interface{}
				var existingLevel interface{}
				var existingIsTop interface{}
				var existingDefaultCurrency, existingTags, existingExtCode, existingSegment interface{}
				var existingSapBukrs, existingSapKtopl, existingSapSaknr, existingSapKtoks interface{}
				var existingOracleLedger, existingOracleCoa, existingOracleBal, existingOracleNat interface{}
				var existingTallyName, existingTallyGroup interface{}
				var existingSageDept, existingSageCost interface{}
				var existingEffFrom, existingEffTo interface{}

				sel := `SELECT gl_account_code, gl_account_name, gl_account_type, parent_gl_code, status, source, gl_account_level, is_top_level_gl_account, default_currency, tags, external_code, segment, sap_bukrs, sap_ktopl, sap_saknr, sap_ktoks, oracle_ledger, oracle_coa, oracle_balancing_seg, oracle_natural_account, tally_ledger_name, tally_ledger_group, sage_department, sage_cost_centre, effective_from, effective_to FROM masterglaccount WHERE gl_account_id=$1 FOR UPDATE`

				if err := tx.QueryRow(ctx, sel, row.GLAccountID).Scan(&existingCode, &existingName, &existingType, &existingParentCode, &existingStatus, &existingSource, &existingLevel, &existingIsTop, &existingDefaultCurrency, &existingTags, &existingExtCode, &existingSegment, &existingSapBukrs, &existingSapKtopl, &existingSapSaknr, &existingSapKtoks, &existingOracleLedger, &existingOracleCoa, &existingOracleBal, &existingOracleNat, &existingTallyName, &existingTallyGroup, &existingSageDept, &existingSageCost, &existingEffFrom, &existingEffTo); err != nil {
					results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "fetch failed: " + err.Error(), "gl_account_id": row.GLAccountID})
					return
				}

				// Validate currency if being updated
				if val, ok := row.Fields["default_currency"]; ok {
					if valStr := fmt.Sprint(val); valStr != "" && !api.IsCurrencyAllowed(ctx, valStr) {
						results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "invalid or unauthorized default_currency: " + valStr, "gl_account_id": row.GLAccountID})
						return
					}
				}

				// Validate parent GL code if being updated
				if val, ok := row.Fields["parent_gl_account_code"]; ok {
					parentCode := strings.TrimSpace(fmt.Sprint(val))
					if parentCode != "" {
						var parentExists bool
						parentCheckQ := `SELECT EXISTS(
							SELECT 1 FROM masterglaccount m
							LEFT JOIN LATERAL (
								SELECT processing_status FROM auditactionglaccount
								WHERE gl_account_id = m.gl_account_id
								ORDER BY requested_at DESC LIMIT 1
							) a ON TRUE
							WHERE m.gl_account_code = $1
							AND UPPER(a.processing_status) = 'APPROVED'
							AND UPPER(m.status) = 'ACTIVE'
						)`
						if err := tx.QueryRow(ctx, parentCheckQ, parentCode).Scan(&parentExists); err != nil || !parentExists {
							results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "parent_gl_code not found or not approved: " + parentCode, "gl_account_id": row.GLAccountID})
							return
						}
					}
				}

				var sets []string
				var args []interface{}
				pos := 1
				var newParentID interface{}
				var computedLevel interface{}
				var computedIsTop interface{}

				for k, v := range row.Fields {
					switch k {
					case "gl_account_code":
						sets = append(sets, fmt.Sprintf("gl_account_code=$%d, old_gl_account_code=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingCode))
						pos += 2
					case "gl_account_name":
						sets = append(sets, fmt.Sprintf("gl_account_name=$%d, old_gl_account_name=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingName))
						pos += 2
					case "gl_account_type":
						sets = append(sets, fmt.Sprintf("gl_account_type=$%d, old_gl_account_type=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingType))
						pos += 2
					case constants.KeyStatus:
						sets = append(sets, fmt.Sprintf("status=$%d, old_status=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingStatus))
						pos += 2
					case "source":
						sets = append(sets, fmt.Sprintf("source=$%d, old_source=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingSource))
						pos += 2
					case "erp_ref", "erp_type":
						sets = append(sets, fmt.Sprintf("erp_type=$%d, old_erp_type=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingSource))
						pos += 2
					case "gl_account_level":
						sets = append(sets, fmt.Sprintf("gl_account_level=$%d, old_gl_account_level=$%d", pos, pos+1))
						args = append(args, v, existingLevel)
						pos += 2
					case "is_top_level_gl_account":
						sets = append(sets, fmt.Sprintf("is_top_level_gl_account=$%d", pos))
						args = append(args, v)
						pos += 1
					case "parent_gl_account_code":
						pcode := strings.TrimSpace(fmt.Sprint(v))
						if pcode == "" {
							newParentID = nil
							computedLevel = 0
							computedIsTop = true
						} else {
							var pid string
							var plevel int
							if err := pgxPool.QueryRow(ctx, `SELECT gl_account_id, gl_account_level FROM masterglaccount WHERE gl_account_code=$1`, pcode).Scan(&pid, &plevel); err != nil {
								results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "parent gl account not found: " + pcode, "gl_account_id": row.GLAccountID})
								return
							}
							newParentID = pid
							computedLevel = plevel + 1
							computedIsTop = false
						}
						sets = append(sets, fmt.Sprintf("parent_gl_code=$%d, old_parent_gl_code=$%d, gl_account_level=$%d, old_gl_account_level=$%d, is_top_level_gl_account=$%d", pos, pos+1, pos+2, pos+3, pos+4))
						args = append(args, newParentID, ifaceToString(existingParentCode), computedLevel, existingLevel, computedIsTop)
						pos += 5

					case "default_currency":
						sets = append(sets, fmt.Sprintf("default_currency=$%d, old_default_currency=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingDefaultCurrency))
						pos += 2
					case "effective_from":
						dateStr := strings.TrimSpace(fmt.Sprint(v))
						var dateVal interface{}
						if dateStr != "" {
							if date, err := time.Parse(constants.DateFormat, dateStr); err == nil {
								dateVal = date
							}
						}
						sets = append(sets, fmt.Sprintf("effective_from=$%d, old_effective_from=$%d", pos, pos+1))
						args = append(args, dateVal, existingEffFrom)
						pos += 2
					case "effective_to":
						dateStr := strings.TrimSpace(fmt.Sprint(v))
						var dateVal interface{}
						if dateStr != "" {
							if date, err := time.Parse(constants.DateFormat, dateStr); err == nil {
								dateVal = date
							}
						}
						sets = append(sets, fmt.Sprintf("effective_to=$%d, old_effective_to=$%d", pos, pos+1))
						args = append(args, dateVal, existingEffTo)
						pos += 2
					case "tags":
						sets = append(sets, fmt.Sprintf("tags=$%d, old_tags=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingTags))
						pos += 2
					case "external_code":
						sets = append(sets, fmt.Sprintf("external_code=$%d, old_external_code=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingExtCode))
						pos += 2
					case "segment":
						sets = append(sets, fmt.Sprintf("segment=$%d, old_segment=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingSegment))
						pos += 2
					case "sap_bukrs":
						sets = append(sets, fmt.Sprintf("sap_bukrs=$%d, old_sap_bukrs=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingSapBukrs))
						pos += 2
					case "sap_ktopl":
						sets = append(sets, fmt.Sprintf("sap_ktopl=$%d, old_sap_ktopl=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingSapKtopl))
						pos += 2
					case "sap_saknr":
						sets = append(sets, fmt.Sprintf("sap_saknr=$%d, old_sap_saknr=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingSapSaknr))
						pos += 2
					case "sap_ktoks":
						sets = append(sets, fmt.Sprintf("sap_ktoks=$%d, old_sap_ktoks=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingSapKtoks))
						pos += 2
					case "oracle_ledger":
						sets = append(sets, fmt.Sprintf("oracle_ledger=$%d, old_oracle_ledger=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingOracleLedger))
						pos += 2
					case "oracle_coa":
						sets = append(sets, fmt.Sprintf("oracle_coa=$%d, old_oracle_coa=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingOracleCoa))
						pos += 2
					case "oracle_balancing_seg":
						sets = append(sets, fmt.Sprintf("oracle_balancing_seg=$%d, old_oracle_balancing_seg=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingOracleBal))
						pos += 2
					case "oracle_natural_account":
						sets = append(sets, fmt.Sprintf("oracle_natural_account=$%d, old_oracle_natural_account=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingOracleNat))
						pos += 2
					case "tally_ledger_name":
						sets = append(sets, fmt.Sprintf("tally_ledger_name=$%d, old_tally_ledger_name=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingTallyName))
						pos += 2
					case "tally_ledger_group":
						sets = append(sets, fmt.Sprintf("tally_ledger_group=$%d, old_tally_ledger_group=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingTallyGroup))
						pos += 2
					case "sage_department":
						sets = append(sets, fmt.Sprintf("sage_department=$%d, old_sage_department=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingSageDept))
						pos += 2
					case "sage_cost_centre":
						sets = append(sets, fmt.Sprintf("sage_cost_centre=$%d, old_sage_cost_centre=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingSageCost))
						pos += 2
					default:

					}
				}

				updatedID := row.GLAccountID
				if len(sets) > 0 {
					q := "UPDATE masterglaccount SET " + strings.Join(sets, ", ") + fmt.Sprintf(" WHERE gl_account_id=$%d RETURNING gl_account_id", pos)
					args = append(args, row.GLAccountID)
					if err := tx.QueryRow(ctx, q, args...).Scan(&updatedID); err != nil {
						results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: constants.ErrUpdateFailed + err.Error(), "gl_account_id": row.GLAccountID})
						return
					}
				}

				if newParentID != nil {
					relQ := `INSERT INTO glaccountrelationships (parent_gl_account_id, child_gl_account_id) SELECT $1, $2 WHERE NOT EXISTS (SELECT 1 FROM glaccountrelationships WHERE parent_gl_account_id=$1 AND child_gl_account_id=$2)`
					if _, err := tx.Exec(ctx, relQ, newParentID, updatedID); err != nil {
						results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: constants.ErrBulkRelationshipInsertFailed + err.Error(), "gl_account_id": updatedID})
						return
					}
				}

				auditQ := `INSERT INTO auditactionglaccount (gl_account_id, actiontype, processing_status, reason, requested_by, requested_at) VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL', $2, $3, now())`
				if _, err := tx.Exec(ctx, auditQ, updatedID, row.Reason, updatedBy); err != nil {
					results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "audit failed: " + err.Error(), "gl_account_id": updatedID})
					return
				}

				if err := tx.Commit(ctx); err != nil {
					results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: constants.ErrCommitFailed + err.Error(), "gl_account_id": updatedID})
					return
				}
				committed = true
				results = append(results, map[string]interface{}{constants.ValueSuccess: true, "gl_account_id": updatedID})
			}()
		}
		overall := true
		for _, r := range results {
			if ok, exists := r[constants.ValueSuccess]; exists {
				if b, okb := ok.(bool); okb {
					if !b {
						overall = false
						break
					}
				} else {
					overall = false
					break
				}
			} else {
				overall = false
				break
			}
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: overall, "rows": results})
	}
}

func DeleteGLAccount(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			UserID       string   `json:"user_id"`
			GLAccountIDs []string `json:"gl_account_ids"`
			Reason       string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}

		requestedBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == body.UserID {
				requestedBy = s.Name
				break
			}
		}
		if requestedBy == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidSessionCapitalized)
			return
		}

		if len(body.GLAccountIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "gl_account_ids required")
			return
		}

		ctx := r.Context()

		relRows, err := pgxPool.Query(ctx, `SELECT parent_gl_account_id, child_gl_account_id FROM glaccountrelationships`)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer relRows.Close()
		parentMap := map[string][]string{}
		for relRows.Next() {
			var parentID, childID string
			if err := relRows.Scan(&parentID, &childID); err == nil {
				parentMap[parentID] = append(parentMap[parentID], childID)
			}
		}

		allSet := map[string]bool{}
		queue := append([]string{}, body.GLAccountIDs...)
		for _, id := range body.GLAccountIDs {
			allSet[id] = true
		}
		for len(queue) > 0 {
			cur := queue[0]
			queue = queue[1:]
			for _, child := range parentMap[cur] {
				if !allSet[child] {
					allSet[child] = true
					queue = append(queue, child)
				}
			}
		}

		if len(allSet) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No GL accounts found to delete")
			return
		}

		allList := make([]string, 0, len(allSet))
		for id := range allSet {
			allList = append(allList, id)
		}

		q := `INSERT INTO auditactionglaccount (gl_account_id, actiontype, processing_status, reason, requested_by, requested_at)
			  SELECT gid, 'DELETE', 'PENDING_DELETE_APPROVAL', $1, $2, now() FROM unnest($3::text[]) AS gid`
		if _, err := pgxPool.Exec(ctx, q, body.Reason, requestedBy, allList); err != nil {
			errMsg, statusCode := getUserFriendlyGLAccountError(err, "Failed to queue GL accounts for deletion")
			if statusCode == http.StatusOK {
				w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
				json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "error": errMsg})
			} else {
				api.RespondWithError(w, statusCode, errMsg)
			}
			return
		}
		json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: true, "queued_count": len(allList)})
	}
}

func BulkRejectGLAccountActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID       string   `json:"user_id"`
			GLAccountIDs []string `json:"gl_account_ids"`
			Comment      string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
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
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidSessionCapitalized)
			return
		}
		ctx := context.Background()

		relRows, err := pgxPool.Query(ctx, `SELECT parent_gl_account_id, child_gl_account_id FROM glaccountrelationships`)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer relRows.Close()
		parentMap := map[string][]string{}
		for relRows.Next() {
			var parentID, childID string
			if err := relRows.Scan(&parentID, &childID); err == nil {
				parentMap[parentID] = append(parentMap[parentID], childID)
			}
		}

		getAllDescendants := func(ids []string) []string {
			all := map[string]bool{}
			queue := append([]string{}, ids...)
			for _, id := range ids {
				all[id] = true
			}
			for len(queue) > 0 {
				current := queue[0]
				queue = queue[1:]
				for _, child := range parentMap[current] {
					if !all[child] {
						all[child] = true
						queue = append(queue, child)
					}
				}
			}
			res := []string{}
			for id := range all {
				res = append(res, id)
			}
			return res
		}

		allToReject := getAllDescendants(req.GLAccountIDs)
		if len(allToReject) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No GL accounts found to reject")
			return
		}

		query := `UPDATE auditactionglaccount SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2 WHERE gl_account_id = ANY($3) RETURNING action_id, gl_account_id`
		rows2, err := pgxPool.Query(ctx, query, checkerBy, req.Comment, allToReject)
		if err != nil {
			errMsg, statusCode := getUserFriendlyGLAccountError(err, "Failed to reject GL account actions")
			if statusCode == http.StatusOK {
				w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
				json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "error": errMsg})
			} else {
				api.RespondWithError(w, statusCode, errMsg)
			}
			return
		}
		defer rows2.Close()

		var updated []map[string]interface{}
		for rows2.Next() {
			var actionID, gid string
			if err := rows2.Scan(&actionID, &gid); err == nil {
				updated = append(updated, map[string]interface{}{"action_id": actionID, "gl_account_id": gid})
			}
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		success := len(updated) > 0
		resp := map[string]interface{}{constants.ValueSuccess: success, "updated": updated}
		if !success {
			resp["message"] = constants.ErrNoRowsUpdated
		}
		json.NewEncoder(w).Encode(resp)
	}
}

func BulkApproveGLAccountActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID       string   `json:"user_id"`
			GLAccountIDs []string `json:"gl_account_ids"`
			Comment      string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
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
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidSessionCapitalized)
			return
		}
		ctx := context.Background()

		relRows, err := pgxPool.Query(ctx, `SELECT parent_gl_account_id, child_gl_account_id FROM glaccountrelationships`)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer relRows.Close()
		parentMap := map[string][]string{}
		for relRows.Next() {
			var parentID, childID string
			if err := relRows.Scan(&parentID, &childID); err == nil {
				parentMap[parentID] = append(parentMap[parentID], childID)
			}
		}
		getAllDescendants := func(ids []string) []string {
			all := map[string]bool{}
			queue := append([]string{}, ids...)
			for _, id := range ids {
				all[id] = true
			}
			for len(queue) > 0 {
				current := queue[0]
				queue = queue[1:]
				for _, child := range parentMap[current] {
					if !all[child] {
						all[child] = true
						queue = append(queue, child)
					}
				}
			}
			res := []string{}
			for id := range all {
				res = append(res, id)
			}
			return res
		}

		allToApprove := getAllDescendants(req.GLAccountIDs)
		if len(allToApprove) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No GL accounts found to approve")
			return
		}

		query := `UPDATE auditactionglaccount SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2 WHERE gl_account_id = ANY($3) RETURNING action_id, gl_account_id, actiontype`
		rows, err := pgxPool.Query(ctx, query, checkerBy, req.Comment, allToApprove)
		if err != nil {
			errMsg, statusCode := getUserFriendlyGLAccountError(err, "Failed to approve GL account actions")
			if statusCode == http.StatusOK {
				w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
				json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "error": errMsg})
			} else {
				api.RespondWithError(w, statusCode, errMsg)
			}
			return
		}
		defer rows.Close()

		var updated []map[string]interface{}
		var deleteIDs []string
		for rows.Next() {
			var actionID, gid, actionType string
			if err := rows.Scan(&actionID, &gid, &actionType); err == nil {
				updated = append(updated, map[string]interface{}{"action_id": actionID, "gl_account_id": gid, "action_type": actionType})
				if strings.ToUpper(strings.TrimSpace(actionType)) == "DELETE" {
					deleteIDs = append(deleteIDs, gid)
				}
			}
		}

		if len(deleteIDs) > 0 {
			updQ := `UPDATE masterglaccount SET is_deleted=true WHERE gl_account_id = ANY($1)`
			if _, err := pgxPool.Exec(ctx, updQ, deleteIDs); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Failed to set is_deleted: "+err.Error())
				return
			}
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		success := len(updated) > 0
		resp := map[string]interface{}{constants.ValueSuccess: success, "updated": updated}
		if !success {
			resp["message"] = constants.ErrNoRowsUpdated
		}
		json.NewEncoder(w).Encode(resp)
	}
}

func UploadGLAccount(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		// Step 1: Get user ID and name
		userID := r.FormValue(constants.KeyUserID)
		if userID == "" {
			var req struct {
				UserID string `json:"user_id"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			userID = req.UserID
		}
		if userID == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrUserIDRequired)
			return
		}

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

		currCodes := api.GetCurrencyCodesFromCtx(ctx)
		if len(currCodes) == 0 {
			api.RespondWithError(w, http.StatusForbidden, "No accessible currencies found for request")
			return
		}

		// Step 2: Parse uploaded file(s)
		if err := r.ParseMultipartForm(32 << 20); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrFailedToParseMultipartForm)
			return
		}
		files := r.MultipartForm.File["file"]
		if len(files) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrNoFilesUploaded)
			return
		}

		batchIDs := make([]string, 0, len(files))

		for _, fh := range files {
			f, err := fh.Open()
			if err != nil {
				api.RespondWithError(w, http.StatusBadRequest, "Failed to open file: "+fh.Filename)
				return
			}
			defer f.Close()

			ext := getFileExt(fh.Filename)
			records, err := parseCashFlowCategoryFile(f, ext)
			if err != nil || len(records) < 2 {
				api.RespondWithError(w, http.StatusBadRequest, "Invalid or empty file: "+fh.Filename)
				return
			}

			headerRow := records[0]
			dataRows := records[1:]
			batchID := uuid.New().String()
			batchIDs = append(batchIDs, batchID)

			colCount := len(headerRow)
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

			headerNorm := make([]string, len(headerRow))
			for i, h := range headerRow {
				hn := strings.TrimSpace(h)
				hn = strings.Trim(hn, ", ")
				hn = strings.ToLower(hn)
				hn = strings.ReplaceAll(hn, " ", "_")
				hn = strings.Trim(hn, "\"'`")
				headerNorm[i] = hn
			}
			columns := append([]string{"upload_batch_id"}, headerNorm...)

			// Begin transaction
			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxStartFailed+err.Error())
				return
			}
			defer tx.Rollback(ctx)

			// Normalize any date-like columns in the copyRows before staging
			// Build header -> index map
			headerIndex := map[string]int{}
			for i, h := range headerNorm {
				headerIndex[h] = i + 1 // +1 because copyRows has batchID at pos 0
			}
			// date candidate keys (lowercased & normalized)
			dateKeys := map[string]bool{"effective_from": true, "effectiveto": true, "effective_to": true, "eff_from": true, "eff_to": true, "effectivefrom": true}
			for _, row := range copyRows {
				for key := range dateKeys {
					if idx, ok := headerIndex[key]; ok {
						if idx < len(row) {
							if v := row[idx]; v != nil {
								s := fmt.Sprint(v)
								if norm := normalizeDateLocal(s); norm != "" {
									// try parse to time and store time.Time (consistent with other masters)
									if tval, err := time.Parse(constants.DateFormat, norm); err == nil {
										row[idx] = tval
									} else {
										row[idx] = norm
									}
								}
							}
						}
					}
				}
			}

			// Step 3: Stage data fast
			if _, err = tx.CopyFrom(ctx, pgx.Identifier{"input_glaccount_table"}, columns, pgx.CopyFromRows(copyRows)); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Failed to stage data: "+err.Error())
				return
			}

			// Validate staged data for currency authorization
			var hasCurrency bool
			for _, col := range columns {
				if col == "default_currency" {
					hasCurrency = true
					break
				}
			}
			if hasCurrency {
				var invalidCurr string
				checkCurrQ := `SELECT default_currency FROM input_glaccount_table WHERE upload_batch_id = $1 AND default_currency IS NOT NULL AND default_currency != '' AND NOT (UPPER(TRIM(default_currency)) = ANY($2)) LIMIT 1`
				currCodesUpper := make([]string, len(currCodes))
				for i, c := range currCodes {
					currCodesUpper[i] = strings.ToUpper(strings.TrimSpace(c))
				}
				if err := tx.QueryRow(ctx, checkCurrQ, batchID, currCodesUpper).Scan(&invalidCurr); err == nil {
					api.RespondWithError(w, http.StatusForbidden, "Invalid or unauthorized default_currency in upload: "+invalidCurr)
					return
				}
			}

			// Step 4: Map columns dynamically
			mapRows, err := tx.Query(ctx, `SELECT source_column_name, target_field_name FROM upload_mapping_glaccount`)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Mapping error")
				return
			}
			mapping := map[string]string{}
			for mapRows.Next() {
				var src, tgt string
				if err := mapRows.Scan(&src, &tgt); err == nil {
					key := strings.ToLower(strings.TrimSpace(src))
					key = strings.Trim(key, ", ")
					key = strings.ReplaceAll(key, " ", "_")
					tgt = strings.TrimSpace(tgt)
					tgt = strings.Trim(tgt, ", \"'`")
					tgt = strings.ReplaceAll(tgt, " ", "_")
					mapping[key] = tgt
				}
			}
			mapRows.Close()

			var srcCols, tgtCols []string
			for i := range headerRow {
				key := headerNorm[i]
				if t, ok := mapping[key]; ok {
					srcCols = append(srcCols, key)
					tgtCols = append(tgtCols, t)
				}
			}
			if len(tgtCols) == 0 {
				api.RespondWithError(w, http.StatusBadRequest, "No mapped columns found in file")
				return
			}

			// Step 5: Insert into masterglaccount
			tgtColsStr := strings.Join(tgtCols, ", ")
			var selectExprs []string
			for i, src := range srcCols {
				tgt := tgtCols[i]
				selectExprs = append(selectExprs, fmt.Sprintf("s.%s AS %s", src, tgt))
			}
			srcColsStr := strings.Join(selectExprs, ", ")

			insertSQL := fmt.Sprintf(`
				INSERT INTO masterglaccount (%s)
				SELECT %s FROM input_glaccount_table s
				WHERE s.upload_batch_id = $1
				ON CONFLICT (gl_account_code) DO NOTHING
				RETURNING gl_account_id
			`, tgtColsStr, srcColsStr)

			rows, err := tx.Query(ctx, insertSQL, batchID)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Final insert error: "+err.Error())
				return
			}
			var newIDs []string
			for rows.Next() {
				var id string
				if err := rows.Scan(&id); err == nil {
					newIDs = append(newIDs, id)
				}
			}
			rows.Close()

			// Step 6: Sync hierarchy levels + relationships (super fast recursive SQL)
			syncSQL := `
				-- Mark roots
				UPDATE masterglaccount m
				SET gl_account_level = 0,
				    is_top_level_gl_account = true
				WHERE parent_gl_code IS NULL
				   OR TRIM(parent_gl_code) = ''
				   OR parent_gl_code NOT IN (SELECT gl_account_code FROM masterglaccount);

				-- Build recursive hierarchy
				WITH RECURSIVE gl_hierarchy AS (
				  SELECT gl_account_id, gl_account_code, parent_gl_code, 0 AS lvl
				  FROM masterglaccount
				  WHERE parent_gl_code IS NULL OR TRIM(parent_gl_code) = ''

				  UNION ALL

				  SELECT c.gl_account_id, c.gl_account_code, c.parent_gl_code, p.lvl + 1
				  FROM masterglaccount c
				  JOIN gl_hierarchy p ON c.parent_gl_code = p.gl_account_code
				)
				UPDATE masterglaccount m
				SET gl_account_level = gh.lvl,
				    is_top_level_gl_account = (gh.lvl = 0)
				FROM gl_hierarchy gh
				WHERE m.gl_account_id = gh.gl_account_id;

				-- Insert relationships
				INSERT INTO glaccountrelationships (parent_gl_account_id, child_gl_account_id, status)
				SELECT DISTINCT p.gl_account_id, c.gl_account_id, 'Active'
				FROM masterglaccount c
				JOIN masterglaccount p ON c.parent_gl_code = p.gl_account_code
				ON CONFLICT (parent_gl_account_id, child_gl_account_id) DO NOTHING;
			`
			if _, err := tx.Exec(ctx, syncSQL); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Hierarchy sync failed: "+err.Error())
				return
			}

			// Step 7: Audit (optional but safe)
			if len(newIDs) > 0 {
				auditSQL := `
					INSERT INTO auditactionglaccount (gl_account_id, actiontype, processing_status, reason, requested_by, requested_at)
					SELECT gl_account_id, 'CREATE', 'PENDING_APPROVAL', NULL, $1, now()
					FROM masterglaccount WHERE gl_account_id = ANY($2)`
				if _, err := tx.Exec(ctx, auditSQL, userName, newIDs); err != nil {
					api.RespondWithError(w, http.StatusInternalServerError, "Failed to insert audit actions: "+err.Error())
					return
				}
			}

			if err := tx.Commit(ctx); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailedCapitalized+err.Error())
				return
			}
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]any{
			constants.ValueSuccess: true,
			"batch_ids":            batchIDs,
		})
	}
}

func UploadGLAccountSimple(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		// Get user
		userID := r.FormValue(constants.KeyUserID)
		if userID == "" {
			var req struct {
				UserID string `json:"user_id"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			userID = req.UserID
		}
		if userID == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrUserIDRequired)
			return
		}
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

		if err := r.ParseMultipartForm(32 << 20); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrFailedToParseMultipartForm)
			return
		}
		files := r.MultipartForm.File["file"]
		if len(files) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrNoFilesUploaded)
			return
		}

		batchIDs := make([]string, 0, len(files))

		// Allowed columns (subset from masterglaccount DDL). Unknown headers are ignored.
		allowed := map[string]bool{
			"gl_account_code": true, "gl_account_name": true, "gl_account_type": true,
			constants.KeyStatus: true, "source": true, "parent_gl_code": true,
			"effective_from": true, "effective_to": true,
			"tags": true, "account_class": true, "default_currency": true,
			"external_code": true, "segment": true, "erp_type": true,
			"sap_bukrs": true, "sap_ktopl": true, "sap_saknr": true, "sap_ktoks": true,
			"oracle_ledger": true, "oracle_coa": true, "oracle_balancing_seg": true, "oracle_natural_account": true,
			"tally_ledger_name": true, "tally_ledger_group": true, "sage_nominal_code": true, "sage_cost_centre": true, "sage_department": true,
			"posting_allowed": true, "reconciliation_required": true, "is_cash_bank": true,
			"gl_account_level": true, "is_top_level_gl_account": true, "is_deleted": true,
		}

		for _, fh := range files {
			f, err := fh.Open()
			if err != nil {
				api.RespondWithError(w, http.StatusBadRequest, "Failed to open file: "+fh.Filename)
				return
			}
			records, err := parseCashFlowCategoryFile(f, getFileExt(fh.Filename))
			f.Close()
			if err != nil || len(records) < 2 {
				api.RespondWithError(w, http.StatusBadRequest, "Invalid or empty file: "+fh.Filename)
				return
			}

			headerRow := records[0]
			dataRows := records[1:]
			batchID := uuid.New().String()
			batchIDs = append(batchIDs, batchID)

			// normalize header
			headerNorm := make([]string, len(headerRow))
			for i, h := range headerRow {
				hn := strings.TrimSpace(h)
				hn = strings.Trim(hn, ", ")
				hn = strings.ToLower(hn)
				hn = strings.ReplaceAll(hn, " ", "_")
				hn = strings.Trim(hn, "\"'`")
				headerNorm[i] = hn
			}

			// decide which columns map to master columns (ignore unknowns)
			tgtCols := []string{}
			srcIdx := []int{}
			for i, hn := range headerNorm {
				if allowed[hn] {
					tgtCols = append(tgtCols, hn)
					srcIdx = append(srcIdx, i)
				}
			}
			if len(tgtCols) == 0 {
				api.RespondWithError(w, http.StatusBadRequest, "No acceptable columns found in file")
				return
			}

			// Prepare copy rows for input_glaccount_table: upload_batch_id + all original headers
			colCount := len(headerRow)
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

			// Normalize date-like columns in-place for the columns that will be inserted into master
			headerIndex := map[string]int{}
			for i, h := range headerNorm {
				headerIndex[h] = i + 1 // +1 because copyRows has batchID at pos 0
			}
			dateKeys := map[string]bool{"effective_from": true, "effective_to": true, "eff_from": true, "eff_to": true, "effectivefrom": true, "effectiveto": true}
			for _, row := range copyRows {
				for key := range dateKeys {
					if idx, ok := headerIndex[key]; ok {
						if idx < len(row) {
							if v := row[idx]; v != nil {
								s := fmt.Sprint(v)
								if norm := normalizeDateLocal(s); norm != "" {
									if tval, err := time.Parse(constants.DateFormat, norm); err == nil {
										row[idx] = tval
									} else {
										row[idx] = norm
									}
								}
							}
						}
					}
				}
			}

			// Begin tx and insert directly into masterglaccount (no staging table)
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

			// Increase statement timeout locally for this transaction to avoid 57014 on large uploads
			if _, err := tx.Exec(ctx, "SET LOCAL statement_timeout = '10min'"); err != nil {
				// non-fatal: continue but log
			}

			// Build column list and ensure required columns exist
			// We'll copy directly into masterglaccount using the sanitized header names
			cols := []string{}
			for _, h := range headerNorm {
				if allowed[h] {
					cols = append(cols, h)
				}
			}
			// must have core required columns
			hasCode := false
			hasName := false
			hasType := false
			for _, c := range cols {
				switch c {
				case "gl_account_code":
					hasCode = true
				case "gl_account_name":
					hasName = true
				case "gl_account_type":
					hasType = true
				}
			}
			if !hasCode || !hasName || !hasType {
				api.RespondWithError(w, http.StatusBadRequest, "CSV must include gl_account_code, gl_account_name and gl_account_type columns")
				return
			}

			// Prepare copyRows aligned to cols order
			copyRowsMaster := make([][]interface{}, len(dataRows))
			// build header index map from headerNorm to position in original row
			headerPos := map[string]int{}
			for i, h := range headerNorm {
				headerPos[h] = i
			}
			for i, row := range dataRows {
				vals := make([]interface{}, len(cols))
				for j, col := range cols {
					if pos, ok := headerPos[col]; ok && pos < len(row) {
						cell := strings.TrimSpace(row[pos])
						if cell == "" {
							vals[j] = nil
						} else {
							// normalize dates if this is a date column
							if col == "effective_from" || col == "effective_to" || col == "eff_from" || col == "eff_to" || col == "effectivefrom" || col == "effectiveto" {
								if norm := normalizeDateLocal(cell); norm != "" {
									if tval, perr := time.Parse(constants.DateFormat, norm); perr == nil {
										vals[j] = tval
									} else {
										vals[j] = norm
									}
								} else {
									vals[j] = nil
								}
							} else {
								vals[j] = cell
							}
						}
					} else {
						vals[j] = nil
					}
				}
				copyRowsMaster[i] = vals
			}

			tStartCopy := time.Now()
			// Use CopyFrom directly into masterglaccount
			if _, err := tx.CopyFrom(ctx, pgx.Identifier{"masterglaccount"}, cols, pgx.CopyFromRows(copyRowsMaster)); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Failed to insert into masterglaccount: "+err.Error())
				return
			}
			log.Printf("UploadGLAccountSimple: COPY inserted %d rows in %v", len(copyRowsMaster), time.Since(tStartCopy))

			// Collect gl_account_code values we inserted to look up IDs for audit
			codes := make([]string, 0, len(copyRowsMaster))
			for _, row := range copyRowsMaster {
				for j, c := range cols {
					if c == "gl_account_code" {
						if row[j] != nil {
							codes = append(codes, fmt.Sprint(row[j]))
						}
						break
					}
				}
			}

			// Fetch inserted IDs (small cost) - do this inside tx to ensure we see newly inserted rows
			var newIDs []string
			var newCodes []string
			if len(codes) > 0 {
				q := `SELECT gl_account_id, gl_account_code FROM masterglaccount WHERE gl_account_code = ANY($1)`
				rrows, err := tx.Query(ctx, q, codes)
				if err != nil {
					api.RespondWithError(w, http.StatusInternalServerError, "Failed to fetch inserted IDs: "+err.Error())
					return
				}
				for rrows.Next() {
					var id, code string
					if err := rrows.Scan(&id, &code); err == nil {
						newIDs = append(newIDs, id)
						newCodes = append(newCodes, code)
					}
				}
				rrows.Close()
			}

			// Commit tx now to free locks; run heavyweight sync and audit asynchronously to avoid blocking the client
			if err := tx.Commit(ctx); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailedCapitalized+err.Error())
				return
			}
			committed = true

			// Run sync + audit async
			go func(codesForAudit []string) {
				ctx2 := context.Background()
				// open a new transaction for sync and audit
				tx2, err := pgxPool.Begin(ctx2)
				if err != nil {
					log.Printf("UploadGLAccountSimple: async sync begin failed: %v", err)
					return
				}
				defer func() {
					if tx2 != nil {
						tx2.Rollback(ctx2)
					}
				}()

				syncSQL := `
				-- Mark roots
				UPDATE masterglaccount m
				SET gl_account_level = 0,
					is_top_level_gl_account = true
				WHERE parent_gl_code IS NULL
				   OR TRIM(parent_gl_code) = ''
				   OR parent_gl_code NOT IN (SELECT gl_account_code FROM masterglaccount);

				-- Build recursive hierarchy
				WITH RECURSIVE gl_hierarchy AS (
				  SELECT gl_account_id, gl_account_code, parent_gl_code, 0 AS lvl
				  FROM masterglaccount
				  WHERE parent_gl_code IS NULL OR TRIM(parent_gl_code) = ''

				  UNION ALL

				  SELECT c.gl_account_id, c.gl_account_code, c.parent_gl_code, p.lvl + 1
				  FROM masterglaccount c
				  JOIN gl_hierarchy p ON c.parent_gl_code = p.gl_account_code
				)
				UPDATE masterglaccount m
				SET gl_account_level = gh.lvl,
					is_top_level_gl_account = (gh.lvl = 0)
				FROM gl_hierarchy gh
				WHERE m.gl_account_id = gh.gl_account_id;

				-- Insert relationships
				INSERT INTO glaccountrelationships (parent_gl_account_id, child_gl_account_id, status)
				SELECT DISTINCT p.gl_account_id, c.gl_account_id, 'Active'
				FROM masterglaccount c
				JOIN masterglaccount p ON c.parent_gl_code = p.gl_account_code
				ON CONFLICT (parent_gl_account_id, child_gl_account_id) DO NOTHING;
			`
				if _, err := tx2.Exec(ctx2, syncSQL); err != nil {
					log.Printf("UploadGLAccountSimple: async sync failed: %v", err)
					return
				}

				if len(codesForAudit) > 0 {
					auditSQL := `INSERT INTO auditactionglaccount (gl_account_id, actiontype, processing_status, reason, requested_by, requested_at) SELECT gl_account_id, 'CREATE', 'PENDING_APPROVAL', NULL, $1, now() FROM masterglaccount WHERE gl_account_code = ANY($2)`
					if _, err := tx2.Exec(ctx2, auditSQL, userName, codesForAudit); err != nil {
						log.Printf("UploadGLAccountSimple: async audit insert failed: %v", err)
						// continue to commit/close
					}
				}

				if err := tx2.Commit(ctx2); err != nil {
					log.Printf("UploadGLAccountSimple: async commit failed: %v", err)
					return
				}
				tx2 = nil
				log.Printf("UploadGLAccountSimple: async sync+audit finished for %d codes", len(codesForAudit))
			}(newCodes)
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]any{constants.ValueSuccess: true, "batch_ids": batchIDs})
	}
}
