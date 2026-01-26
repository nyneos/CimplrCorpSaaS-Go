package allMaster

import (
	"CimplrCorpSaas/api"
	exposures "CimplrCorpSaas/api/fx/exposures"
	middlewares "CimplrCorpSaas/api/middlewares"
	"context"
	"encoding/json"
	"fmt"
	"time"

	// "mime/multipart"
	"net/http"
	"strings"

	"CimplrCorpSaas/api/constants"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// getUserFriendlyPayableReceivableError returns a user-friendly error message and HTTP status code
func getUserFriendlyPayableReceivableError(err error, context string) (string, int) {
	if err == nil {
		return "", http.StatusOK
	}

	errMsg := err.Error()
	errLower := strings.ToLower(errMsg)

	// Unique constraint violations
	if strings.Contains(errLower, "unique_type_code_not_deleted") {
		return "Type code already exists and is not deleted. Please use a different code.", http.StatusOK
	}
	if strings.Contains(errLower, "unique_type_name_not_deleted") {
		return "Type name already exists and is not deleted. Please use a different name.", http.StatusOK
	}

	// Check constraints
	if strings.Contains(errLower, "masterpayablereceivabletype_status_check") {
		return "Status must be either 'Active' or 'Inactive'.", http.StatusOK
	}
	if strings.Contains(errLower, "masterpayablereceivabletype_old_status_check") {
		return "Old status must be either 'Active' or 'Inactive'.", http.StatusOK
	}

	// Audit action check constraints
	if strings.Contains(errLower, "auditactionpayablereceivable_actiontype_check") {
		return "Action type must be one of: CREATE, EDIT, DELETE.", http.StatusOK
	}
	if strings.Contains(errLower, "auditactionpayablereceivable_processing_status_check") {
		return "Processing status must be one of: PENDING_APPROVAL, PENDING_EDIT_APPROVAL, PENDING_DELETE_APPROVAL, APPROVED, REJECTED, CANCELLED.", http.StatusOK
	}

	// Foreign key violations
	if strings.Contains(errLower, "foreign key") || strings.Contains(errLower, "fk_") || strings.Contains(errLower, "_fkey") {
		if strings.Contains(errLower, "type_id") {
			return "Payable/Receivable type does not exist or has been deleted.", http.StatusOK
		}
		return "Referenced record does not exist. Please check your input.", http.StatusOK
	}

	// Not null violations
	if strings.Contains(errLower, "not null") || strings.Contains(errLower, "null value") {
		if strings.Contains(errLower, "type_name") {
			return "Type name is required.", http.StatusOK
		}
		if strings.Contains(errLower, "status") {
			return "Status is required.", http.StatusOK
		}
		if strings.Contains(errLower, "direction") {
			return "Direction is required.", http.StatusOK
		}
		if strings.Contains(errLower, "business_unit_division") {
			return "Business unit/division is required.", http.StatusOK
		}
		if strings.Contains(errLower, "default_currency") {
			return "Default currency is required.", http.StatusOK
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

type PayableReceivableRequest struct {
	Status                    string   `json:"status"`
	TypeCode                  string   `json:"type_code,omitempty"`
	TypeName                  string   `json:"type_name"`
	Direction                 string   `json:"direction"`
	BusinessUnitDivision      string   `json:"business_unit_division"`
	DefaultCurrency           string   `json:"default_currency"`
	DefaultDueDays            *int     `json:"default_due_days,omitempty"`
	PaymentTermsName          string   `json:"payment_terms_name,omitempty"`
	AllowNetting              *bool    `json:"allow_netting,omitempty"`
	SettlementDiscount        *bool    `json:"settlement_discount,omitempty"`
	SettlementDiscountPercent *float64 `json:"settlement_discount_percent,omitempty"`
	TaxApplicable             *bool    `json:"tax_applicable,omitempty"`
	TaxCode                   string   `json:"tax_code,omitempty"`
	DefaultReconGL            string   `json:"default_recon_gl,omitempty"`
	OffsetRevenueExpenseGL    string   `json:"offset_revenue_expense_gl,omitempty"`
	CashFlowCategory          string   `json:"cash_flow_category,omitempty"`
	Category                  string   `json:"category,omitempty"`
	EffectiveFrom             string   `json:"effective_from,omitempty"`
	EffectiveTo               string   `json:"effective_to,omitempty"`
	Tags                      string   `json:"tags,omitempty"`
	ErpType                   string   `json:"erp_type,omitempty"`
	SAPCompanyCode            string   `json:"sap_company_code,omitempty"`
	SAPFiDocType              string   `json:"sap_fi_doc_type,omitempty"`
	SAPPostingKeyDebit        string   `json:"sap_posting_key_debit,omitempty"`
	SAPPostingKeyCredit       string   `json:"sap_posting_key_credit,omitempty"`
	SAPReconciliationGL       string   `json:"sap_reconciliation_gl,omitempty"`
	SAPTaxCode                string   `json:"sap_tax_code,omitempty"`
	OracleLedger              string   `json:"oracle_ledger,omitempty"`
	OracleTransactionType     string   `json:"oracle_transaction_type,omitempty"`
	OracleDistributionSet     string   `json:"oracle_distribution_set,omitempty"`
	OracleSource              string   `json:"oracle_source,omitempty"`
	TallyVoucherType          string   `json:"tally_voucher_type,omitempty"`
	TallyTaxClass             string   `json:"tally_tax_class,omitempty"`
	TallyLedgerGroup          string   `json:"tally_ledger_group,omitempty"`
	SageNominalControl        string   `json:"sage_nominal_control,omitempty"`
	SageAnalysisCode          string   `json:"sage_analysis_code,omitempty"`
	ExternalCode              string   `json:"external_code,omitempty"`
	Segment                   string   `json:"segment,omitempty"`
}

func CreatePayableReceivableTypes(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string                     `json:"user_id"`
			Rows   []PayableReceivableRequest `json:"rows"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSON)
			return
		}

		session := middlewares.GetSessionFromContext(r.Context())
		if session == nil {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}
		createdBy := session.Name

		ctx := r.Context()
		// Get middleware-provided context for validation
		entities := api.GetEntityNamesFromCtx(ctx)
		currCodes := api.GetCurrencyCodesFromCtx(ctx)
		categories := api.GetCashFlowCategoryNamesFromCtx(ctx)

		if len(entities) == 0 {
			api.RespondWithError(w, http.StatusForbidden, constants.ErrNoAccessibleEntitiesForRequest)
			return
		}
		if len(currCodes) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No accessible currencies found")
			return
		}

		created := make([]map[string]interface{}, 0)

		for _, rrow := range req.Rows {
			var missing []string
			if strings.TrimSpace(rrow.TypeName) == "" {
				missing = append(missing, "type_name")
			}
			if strings.TrimSpace(rrow.Status) == "" {
				missing = append(missing, constants.KeyStatus)
			}
			if strings.TrimSpace(rrow.Direction) == "" {
				missing = append(missing, "direction")
			}
			if strings.TrimSpace(rrow.BusinessUnitDivision) == "" {
				missing = append(missing, "business_unit_division")
			}
			if strings.TrimSpace(rrow.DefaultCurrency) == "" {
				missing = append(missing, "default_currency")
			}
			if len(missing) > 0 {
				created = append(created, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: fmt.Sprintf("missing required fields: %s", strings.Join(missing, ", "))})
				continue
			}

			// Validate business_unit_division (entity)
			if !api.IsEntityAllowed(ctx, rrow.BusinessUnitDivision) {
				created = append(created, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "invalid or unauthorized business_unit_division (entity)", "type_name": rrow.TypeName})
				continue
			}

			// Validate default_currency
			if !api.IsCurrencyAllowed(ctx, rrow.DefaultCurrency) {
				created = append(created, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "invalid or unauthorized currency", "type_name": rrow.TypeName})
				continue
			}

			// Validate cash_flow_category if provided
			if strings.TrimSpace(rrow.CashFlowCategory) != "" {
				if len(categories) > 0 && !api.IsCashFlowCategoryAllowed(ctx, rrow.CashFlowCategory) {
					created = append(created, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "invalid or unauthorized cash_flow_category", "type_name": rrow.TypeName})
					continue
				}
			}

			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				created = append(created, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "failed to start tx: " + err.Error()})
				continue
			}

			var id string
			sp := "sp_" + strings.ReplaceAll(uuid.New().String(), "-", "_")
			if _, err := tx.Exec(ctx, "SAVEPOINT "+sp); err != nil {
				tx.Rollback(ctx)
				created = append(created, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "failed to create savepoint: " + err.Error()})
				continue
			}

			if err := tx.QueryRow(ctx, `SELECT 'TYP-' || LPAD(nextval('payable_receivable_seq')::text, 6, '0')`).Scan(&id); err != nil {
				tx.Exec(ctx, "ROLLBACK TO SAVEPOINT "+sp)
				created = append(created, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "failed to generate type_id: " + err.Error(), "type_name": rrow.TypeName})
				tx.Commit(ctx)
				continue
			}
			var effFrom, effTo interface{}
			if strings.TrimSpace(rrow.EffectiveFrom) != "" {
				if norm := NormalizeDate(rrow.EffectiveFrom); norm != "" {
					if tval, err := time.Parse(constants.DateFormat, norm); err == nil {
						effFrom = tval
					}
				}
			}
			if strings.TrimSpace(rrow.EffectiveTo) != "" {
				if norm := NormalizeDate(rrow.EffectiveTo); norm != "" {
					if tval, err := time.Parse(constants.DateFormat, norm); err == nil {
						effTo = tval
					}
				}
			}

			ins := `INSERT INTO masterpayablereceivabletype (
				type_id, status, type_code, type_name, direction, business_unit_division, default_currency, default_due_days,
					payment_terms_name, allow_netting, settlement_discount, settlement_discount_percent, tax_applicable, tax_code,
					default_recon_gl, offset_revenue_expense_gl, cash_flow_category, category, effective_from, effective_to, tags,
				erp_type, sap_company_code, sap_fi_doc_type, sap_posting_key_debit, sap_posting_key_credit, sap_reconciliation_gl,
				sap_tax_code, oracle_ledger, oracle_transaction_type, oracle_distribution_set, oracle_source, tally_voucher_type,
				tally_tax_class, tally_ledger_group, sage_nominal_control, sage_analysis_code
			, external_code, segment
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39) RETURNING type_id`
			// include generated id as first parameter
			if err := tx.QueryRow(ctx, ins,
				id,
				rrow.Status,
				rrow.TypeCode,
				rrow.TypeName,
				rrow.Direction,
				rrow.BusinessUnitDivision,
				rrow.DefaultCurrency,
				rrow.DefaultDueDays,
				rrow.PaymentTermsName,
				rrow.AllowNetting,
				rrow.SettlementDiscount,
				rrow.SettlementDiscountPercent,
				rrow.TaxApplicable,
				rrow.TaxCode,
				rrow.DefaultReconGL,
				rrow.OffsetRevenueExpenseGL,
				rrow.CashFlowCategory,
				rrow.Category,
				effFrom,
				effTo,
				rrow.Tags,
				rrow.ErpType,
				rrow.SAPCompanyCode,
				rrow.SAPFiDocType,
				rrow.SAPPostingKeyDebit,
				rrow.SAPPostingKeyCredit,
				rrow.SAPReconciliationGL,
				rrow.SAPTaxCode,
				rrow.OracleLedger,
				rrow.OracleTransactionType,
				rrow.OracleDistributionSet,
				rrow.OracleSource,
				rrow.TallyVoucherType,
				rrow.TallyTaxClass,
				rrow.TallyLedgerGroup,
				rrow.SageNominalControl,
				rrow.SageAnalysisCode,
				rrow.ExternalCode,
				rrow.Segment,
			).Scan(&id); err != nil {
				tx.Exec(ctx, "ROLLBACK TO SAVEPOINT "+sp)
				created = append(created, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: err.Error(), "type_name": rrow.TypeName})
				tx.Commit(ctx)
				continue
			}

			auditQ := `INSERT INTO auditactionpayablereceivable (type_id, actiontype, processing_status, reason, requested_by, requested_at) VALUES ($1,'CREATE','PENDING_APPROVAL', $2, $3, now())`
			if _, err := tx.Exec(ctx, auditQ, id, nil, createdBy); err != nil {
				tx.Rollback(ctx)
				created = append(created, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: constants.ErrAuditInsertFailed + err.Error(), "id": id})
				continue
			}

			if err := tx.Commit(ctx); err != nil {
				tx.Rollback(ctx)
				created = append(created, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: constants.ErrCommitFailed + err.Error(), "id": id})
				continue
			}
			created = append(created, map[string]interface{}{constants.ValueSuccess: true, "type_id": id, "type_name": rrow.TypeName})
		}

		overall := true
		for _, r := range created {
			if ok, found := r[constants.ValueSuccess].(bool); !found || !ok {
				overall = false
				break
			}
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: overall, "rows": created})
	}
}

type AuditInfo struct {
	CreatedBy string
	CreatedAt string
	EditedBy  string
	EditedAt  string
	DeletedBy string
	DeletedAt string
}

func GetPayableReceivableNamesWithID(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSON)
			return
		}

		session := middlewares.GetSessionFromContext(r.Context())
		if session == nil {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}

		ctx := r.Context()
		entities := api.GetEntityNamesFromCtx(ctx)
		currCodes := api.GetCurrencyCodesFromCtx(ctx)
		categories := api.GetCashFlowCategoryNamesFromCtx(ctx)

		if len(entities) == 0 {
			w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: true, "rows": []map[string]interface{}{}})
			return
		}

		query := `
			SELECT
				m.type_id,
				m.type_code, m.type_name, m.direction, m.business_unit_division, m.default_currency, m.default_due_days, m.payment_terms_name,
				m.allow_netting, m.settlement_discount, m.settlement_discount_percent, m.tax_applicable, m.tax_code,
				m.default_recon_gl, m.offset_revenue_expense_gl, m.cash_flow_category, m.category, m.effective_from, m.effective_to, m.tags,
				m.erp_type, m.sap_company_code, m.sap_fi_doc_type, m.sap_posting_key_debit, m.sap_posting_key_credit, m.sap_reconciliation_gl,
				m.sap_tax_code, m.oracle_ledger, m.oracle_transaction_type, m.oracle_distribution_set, m.oracle_source,
				m.tally_voucher_type, m.tally_tax_class, m.tally_ledger_group, m.sage_nominal_control, m.sage_analysis_code,
				m.old_type_code, m.old_type_name, m.old_direction, m.old_business_unit_division, m.old_default_currency, m.old_default_due_days,
				m.external_code, m.segment,
				m.old_payment_terms_name, m.old_allow_netting, m.old_settlement_discount, m.old_settlement_discount_percent, m.old_tax_applicable,
				m.old_tax_code, m.old_default_recon_gl, m.old_offset_revenue_expense_gl, m.old_cash_flow_category, m.old_category, m.old_effective_from, m.old_effective_to,
				m.old_tags, m.old_erp_type, m.old_sap_company_code, m.old_sap_fi_doc_type, m.old_sap_posting_key_debit, m.old_sap_posting_key_credit,
				m.old_sap_reconciliation_gl, m.old_sap_tax_code, m.old_oracle_ledger, m.old_oracle_transaction_type, m.old_oracle_distribution_set,
				m.old_oracle_source, m.old_tally_voucher_type, m.old_tally_tax_class, m.old_tally_ledger_group, m.old_sage_nominal_control, m.old_sage_analysis_code,
				m.is_deleted,
				m.old_external_code, m.old_segment,
				a.processing_status, a.requested_by, a.requested_at, a.actiontype, a.action_id,
				a.checker_by, a.checker_at, a.checker_comment, a.reason
			FROM masterpayablereceivabletype m
			LEFT JOIN LATERAL (
				SELECT processing_status, requested_by, requested_at, actiontype, action_id,
					   checker_by, checker_at, checker_comment, reason
				FROM auditactionpayablereceivable a
				WHERE a.type_id = m.type_id
				ORDER BY requested_at DESC
				LIMIT 1
			) a ON TRUE
			WHERE NOT (UPPER(a.processing_status) = 'APPROVED' AND m.is_deleted = true)
				AND m.business_unit_division = ANY($1)
				AND m.default_currency = ANY($2)
				AND (m.cash_flow_category IS NULL OR m.cash_flow_category = ANY($3))
			ORDER BY GREATEST(COALESCE(a.requested_at, '1970-01-01'::timestamp), COALESCE(a.checker_at, '1970-01-01'::timestamp)) DESC
		`

		rows, err := pgxPool.Query(ctx, query, entities, currCodes, categories)
		if err != nil {
			errMsg, statusCode := getUserFriendlyPayableReceivableError(err, "Failed to fetch payable/receivable types")
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
		typeIDs := []string{}

		for rows.Next() {
			var (
				typeID                        string
				typeCodeI                     interface{}
				typeNameI                     interface{}
				directionI                    interface{}
				businessUnitDivisionI         interface{}
				defaultCurrencyI              interface{}
				defaultDueDaysI               interface{}
				paymentTermsNameI             interface{}
				allowNettingI                 interface{}
				settlementDiscountI           interface{}
				settlementDiscountPercentI    interface{}
				taxApplicableI                interface{}
				taxCodeI                      interface{}
				defaultReconGLI               interface{}
				offsetRevenueExpenseGLI       interface{}
				cashFlowCategoryI             interface{}
				categoryI                     interface{}
				effectiveFromI                interface{}
				effectiveToI                  interface{}
				tagsI                         interface{}
				erpTypeI                      interface{}
				sapCompanyCodeI               interface{}
				sapFiDocTypeI                 interface{}
				sapPostingKeyDebitI           interface{}
				sapPostingKeyCreditI          interface{}
				sapReconciliationGLI          interface{}
				sapTaxCodeI                   interface{}
				oracleLedgerI                 interface{}
				oracleTransactionTypeI        interface{}
				oracleDistributionSetI        interface{}
				oracleSourceI                 interface{}
				tallyVoucherTypeI             interface{}
				tallyTaxClassI                interface{}
				tallyLedgerGroupI             interface{}
				sageNominalControlI           interface{}
				sageAnalysisCodeI             interface{}
				externalCodeI                 interface{}
				segmentI                      interface{}
				oldTypeCodeI                  interface{}
				oldTypeNameI                  interface{}
				oldDirectionI                 interface{}
				oldBusinessUnitDivisionI      interface{}
				oldDefaultCurrencyI           interface{}
				oldDefaultDueDaysI            interface{}
				oldPaymentTermsNameI          interface{}
				oldAllowNettingI              interface{}
				oldSettlementDiscountI        interface{}
				oldSettlementDiscountPercentI interface{}
				oldTaxApplicableI             interface{}
				oldTaxCodeI                   interface{}
				oldDefaultReconGLI            interface{}
				oldOffsetRevenueExpenseGLI    interface{}
				oldCashFlowCategoryI          interface{}
				oldCategoryI                  interface{}
				oldEffectiveFromI             interface{}
				oldEffectiveToI               interface{}
				oldTagsI                      interface{}
				oldErpTypeI                   interface{}
				oldSapCompanyCodeI            interface{}
				oldSapFiDocTypeI              interface{}
				oldSapPostingKeyDebitI        interface{}
				oldSapPostingKeyCreditI       interface{}
				oldSapReconciliationGLI       interface{}
				oldSapTaxCodeI                interface{}
				oldOracleLedgerI              interface{}
				oldOracleTransactionTypeI     interface{}
				oldOracleDistributionSetI     interface{}
				oldOracleSourceI              interface{}
				oldTallyVoucherTypeI          interface{}
				oldTallyTaxClassI             interface{}
				oldTallyLedgerGroupI          interface{}
				oldSageNominalControlI        interface{}
				oldSageAnalysisCodeI          interface{}
				oldExternalCodeI              interface{}
				oldSegmentI                   interface{}
				isDeletedI                    interface{}
				processingStatusI             interface{}
				requestedByI                  interface{}
				requestedAtI                  interface{}
				actionTypeI                   interface{}
				actionIDI                     string
				checkerByI                    interface{}
				checkerAtI                    interface{}
				checkerCommentI               interface{}
				reasonI                       interface{}
			)

			if err := rows.Scan(
				&typeID,
				&typeCodeI, &typeNameI, &directionI, &businessUnitDivisionI, &defaultCurrencyI, &defaultDueDaysI, &paymentTermsNameI,
				&allowNettingI, &settlementDiscountI, &settlementDiscountPercentI, &taxApplicableI, &taxCodeI,
				&defaultReconGLI, &offsetRevenueExpenseGLI, &cashFlowCategoryI, &categoryI, &effectiveFromI, &effectiveToI, &tagsI,
				&erpTypeI, &sapCompanyCodeI, &sapFiDocTypeI, &sapPostingKeyDebitI, &sapPostingKeyCreditI, &sapReconciliationGLI,
				&sapTaxCodeI, &oracleLedgerI, &oracleTransactionTypeI, &oracleDistributionSetI, &oracleSourceI,
				&tallyVoucherTypeI, &tallyTaxClassI, &tallyLedgerGroupI, &sageNominalControlI, &sageAnalysisCodeI,
				&externalCodeI, &segmentI,
				&oldTypeCodeI, &oldTypeNameI, &oldDirectionI, &oldBusinessUnitDivisionI, &oldDefaultCurrencyI, &oldDefaultDueDaysI,
				&oldPaymentTermsNameI, &oldAllowNettingI, &oldSettlementDiscountI, &oldSettlementDiscountPercentI, &oldTaxApplicableI,
				&oldTaxCodeI, &oldDefaultReconGLI, &oldOffsetRevenueExpenseGLI, &oldCashFlowCategoryI, &oldCategoryI, &oldEffectiveFromI, &oldEffectiveToI,
				&oldTagsI, &oldErpTypeI, &oldSapCompanyCodeI, &oldSapFiDocTypeI, &oldSapPostingKeyDebitI, &oldSapPostingKeyCreditI,
				&oldSapReconciliationGLI, &oldSapTaxCodeI, &oldOracleLedgerI, &oldOracleTransactionTypeI, &oldOracleDistributionSetI,
				&oldOracleSourceI, &oldTallyVoucherTypeI, &oldTallyTaxClassI, &oldTallyLedgerGroupI, &oldSageNominalControlI, &oldSageAnalysisCodeI,
				&isDeletedI,
				&oldExternalCodeI, &oldSegmentI,
				&processingStatusI, &requestedByI, &requestedAtI, &actionTypeI, &actionIDI, &checkerByI, &checkerAtI, &checkerCommentI, &reasonI,
			); err != nil {
				continue
			}

			record := map[string]interface{}{
				"type_id":                         typeID,
				"type_code":                       ifaceToString(typeCodeI),
				"type_name":                       ifaceToString(typeNameI),
				"direction":                       ifaceToString(directionI),
				"business_unit_division":          ifaceToString(businessUnitDivisionI),
				"default_currency":                ifaceToString(defaultCurrencyI),
				"default_due_days":                ifaceToString(defaultDueDaysI),
				"payment_terms_name":              ifaceToString(paymentTermsNameI),
				"allow_netting":                   ifaceToString(allowNettingI),
				"settlement_discount":             ifaceToString(settlementDiscountI),
				"settlement_discount_percent":     ifaceToString(settlementDiscountPercentI),
				"tax_applicable":                  ifaceToString(taxApplicableI),
				"tax_code":                        ifaceToString(taxCodeI),
				"default_recon_gl":                ifaceToString(defaultReconGLI),
				"offset_revenue_expense_gl":       ifaceToString(offsetRevenueExpenseGLI),
				"cash_flow_category":              ifaceToString(cashFlowCategoryI),
				"category":                        ifaceToString(categoryI),
				"effective_from":                  ifaceToTimeString(effectiveFromI),
				"effective_to":                    ifaceToTimeString(effectiveToI),
				"tags":                            ifaceToString(tagsI),
				"erp_type":                        ifaceToString(erpTypeI),
				"sap_company_code":                ifaceToString(sapCompanyCodeI),
				"sap_fi_doc_type":                 ifaceToString(sapFiDocTypeI),
				"sap_posting_key_debit":           ifaceToString(sapPostingKeyDebitI),
				"sap_posting_key_credit":          ifaceToString(sapPostingKeyCreditI),
				"sap_reconciliation_gl":           ifaceToString(sapReconciliationGLI),
				"sap_tax_code":                    ifaceToString(sapTaxCodeI),
				"oracle_ledger":                   ifaceToString(oracleLedgerI),
				"oracle_transaction_type":         ifaceToString(oracleTransactionTypeI),
				"oracle_distribution_set":         ifaceToString(oracleDistributionSetI),
				"oracle_source":                   ifaceToString(oracleSourceI),
				"tally_voucher_type":              ifaceToString(tallyVoucherTypeI),
				"tally_tax_class":                 ifaceToString(tallyTaxClassI),
				"tally_ledger_group":              ifaceToString(tallyLedgerGroupI),
				"sage_nominal_control":            ifaceToString(sageNominalControlI),
				"sage_analysis_code":              ifaceToString(sageAnalysisCodeI),
				"external_code":                   ifaceToString(externalCodeI),
				"segment":                         ifaceToString(segmentI),
				"old_type_code":                   ifaceToString(oldTypeCodeI),
				"old_type_name":                   ifaceToString(oldTypeNameI),
				"old_direction":                   ifaceToString(oldDirectionI),
				"old_business_unit_division":      ifaceToString(oldBusinessUnitDivisionI),
				"old_default_currency":            ifaceToString(oldDefaultCurrencyI),
				"old_default_due_days":            ifaceToString(oldDefaultDueDaysI),
				"old_payment_terms_name":          ifaceToString(oldPaymentTermsNameI),
				"old_allow_netting":               ifaceToString(oldAllowNettingI),
				"old_settlement_discount":         ifaceToString(oldSettlementDiscountI),
				"old_settlement_discount_percent": ifaceToString(oldSettlementDiscountPercentI),
				"old_tax_applicable":              ifaceToString(oldTaxApplicableI),
				"old_tax_code":                    ifaceToString(oldTaxCodeI),
				"old_default_recon_gl":            ifaceToString(oldDefaultReconGLI),
				"old_offset_revenue_expense_gl":   ifaceToString(oldOffsetRevenueExpenseGLI),
				"old_cash_flow_category":          ifaceToString(oldCashFlowCategoryI),
				"old_category":                    ifaceToString(oldCategoryI),
				"old_effective_from":              ifaceToTimeString(oldEffectiveFromI),
				"old_effective_to":                ifaceToTimeString(oldEffectiveToI),
				"old_tags":                        ifaceToString(oldTagsI),
				"old_erp_type":                    ifaceToString(oldErpTypeI),
				"old_sap_company_code":            ifaceToString(oldSapCompanyCodeI),
				"old_sap_fi_doc_type":             ifaceToString(oldSapFiDocTypeI),
				"old_sap_posting_key_debit":       ifaceToString(oldSapPostingKeyDebitI),
				"old_sap_posting_key_credit":      ifaceToString(oldSapPostingKeyCreditI),
				"old_sap_reconciliation_gl":       ifaceToString(oldSapReconciliationGLI),
				"old_sap_tax_code":                ifaceToString(oldSapTaxCodeI),
				"old_oracle_ledger":               ifaceToString(oldOracleLedgerI),
				"old_oracle_transaction_type":     ifaceToString(oldOracleTransactionTypeI),
				"old_oracle_distribution_set":     ifaceToString(oldOracleDistributionSetI),
				"old_oracle_source":               ifaceToString(oldOracleSourceI),
				"old_tally_voucher_type":          ifaceToString(oldTallyVoucherTypeI),
				"old_tally_tax_class":             ifaceToString(oldTallyTaxClassI),
				"old_tally_ledger_group":          ifaceToString(oldTallyLedgerGroupI),
				"old_sage_nominal_control":        ifaceToString(oldSageNominalControlI),
				"old_sage_analysis_code":          ifaceToString(oldSageAnalysisCodeI),
				"old_external_code":               ifaceToString(oldExternalCodeI),
				"old_segment":                     ifaceToString(oldSegmentI),
				"processing_status":               ifaceToString(processingStatusI),
				"is_deleted":                      ifaceToString(isDeletedI),
				"requested_by":                    ifaceToString(requestedByI),
				"requested_at":                    ifaceToTimeString(requestedAtI),
				"action_type":                     ifaceToString(actionTypeI),
				"action_id":                       actionIDI,
				"checker_by":                      ifaceToString(checkerByI),
				"checker_at":                      ifaceToTimeString(checkerAtI),
				"checker_comment":                 ifaceToString(checkerCommentI),
				"reason":                          ifaceToString(reasonI),
			}

			out = append(out, record)
			typeIDs = append(typeIDs, typeID)
		}
		if len(typeIDs) > 0 {
			queryAudit := `
				SELECT type_id, actiontype, requested_by, requested_at
				FROM auditactionpayablereceivable
				WHERE type_id = ANY($1) AND actiontype IN ('CREATE','EDIT','DELETE')
				ORDER BY requested_at DESC
			`
			auditRows, err := pgxPool.Query(ctx, queryAudit, typeIDs)
			if err == nil {
				defer auditRows.Close()

				auditMap := make(map[string]*AuditInfo)
				for auditRows.Next() {
					var (
						tid   string
						atype string
						rby   interface{}
						rat   interface{}
					)
					if err := auditRows.Scan(&tid, &atype, &rby, &rat); err == nil {
						if auditMap[tid] == nil {
							auditMap[tid] = &AuditInfo{}
						}
						switch atype {
						case "CREATE":
							if auditMap[tid].CreatedBy == "" {
								auditMap[tid].CreatedBy = ifaceToString(rby)
								auditMap[tid].CreatedAt = ifaceToTimeString(rat)
							}
						case "EDIT":
							if auditMap[tid].EditedBy == "" {
								auditMap[tid].EditedBy = ifaceToString(rby)
								auditMap[tid].EditedAt = ifaceToTimeString(rat)
							}
						case "DELETE":
							if auditMap[tid].DeletedBy == "" {
								auditMap[tid].DeletedBy = ifaceToString(rby)
								auditMap[tid].DeletedAt = ifaceToTimeString(rat)
							}
						}
					}
				}
				for i, rec := range out {
					tid := rec["type_id"].(string)
					if audit, ok := auditMap[tid]; ok {
						rec["created_by"] = audit.CreatedBy
						rec["created_at"] = audit.CreatedAt
						rec["edited_by"] = audit.EditedBy
						rec["edited_at"] = audit.EditedAt
						rec["deleted_by"] = audit.DeletedBy
						rec["deleted_at"] = audit.DeletedAt
						out[i] = rec
					}
				}
			}
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: true, "rows": out})
	}
}

func GetApprovedActivePayableReceivable(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSON)
			return
		}

		session := middlewares.GetSessionFromContext(r.Context())
		if session == nil {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}

		ctx := r.Context()
		entities := api.GetEntityNamesFromCtx(ctx)
		currCodes := api.GetCurrencyCodesFromCtx(ctx)
		categories := api.GetCashFlowCategoryNamesFromCtx(ctx)

		if len(entities) == 0 {
			w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: true, "rows": []map[string]interface{}{}})
			return
		}

		q := `
			WITH latest AS (
				SELECT DISTINCT ON (type_id) type_id, processing_status
				FROM auditactionpayablereceivable
				ORDER BY type_id, requested_at DESC
			)
			SELECT m.type_id, m.type_code, m.type_name, m.cash_flow_category
			FROM masterpayablereceivabletype m
			JOIN latest l ON l.type_id = m.type_id
			WHERE UPPER(l.processing_status) = 'APPROVED' AND UPPER(m.status) = 'ACTIVE' AND m.is_deleted = false
				AND m.business_unit_division = ANY($1)
				AND m.default_currency = ANY($2)
				AND (m.cash_flow_category IS NULL OR m.cash_flow_category = ANY($3))
		`

		rows, err := pgxPool.Query(ctx, q, entities, currCodes, categories)
		if err != nil {
			errMsg, statusCode := getUserFriendlyPayableReceivableError(err, "Failed to fetch approved types")
			if statusCode == http.StatusOK {
				w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
				json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "error": errMsg})
			} else {
				api.RespondWithError(w, statusCode, errMsg)
			}
			return
		}
		defer rows.Close()

		out := make([]map[string]interface{}, 0)
		for rows.Next() {
			var (
				typeID             string
				typeCodeI          interface{}
				typeNameI          interface{}
				cash_flow_category interface{}
			)
			if err := rows.Scan(&typeID, &typeCodeI, &typeNameI, &cash_flow_category); err != nil {
				continue
			}
			out = append(out, map[string]interface{}{
				"type_id":  typeID,
				"type":     ifaceToString(typeCodeI),
				"name":     ifaceToString(typeNameI),
				"category": ifaceToString(cash_flow_category),
			})
		}
		if err := rows.Err(); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: true, "rows": out})
	}
}

func UpdatePayableReceivableBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			Rows   []struct {
				TypeID string                 `json:"type_id"`
				Fields map[string]interface{} `json:"fields"`
				Reason string                 `json:"reason"`
			} `json:"rows"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSON)
			return
		}

		session := middlewares.GetSessionFromContext(r.Context())
		if session == nil {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}
		updatedBy := session.Name

		ctx := r.Context()
		entities := api.GetEntityNamesFromCtx(ctx)

		if len(entities) == 0 {
			api.RespondWithError(w, http.StatusForbidden, constants.ErrNoAccessibleEntitiesForRequest)
			return
		}

		results := []map[string]interface{}{}
		for _, row := range req.Rows {
			if row.TypeID == "" {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "missing type_id"})
				continue
			}
			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "begin failed: " + err.Error()})
				continue
			}
			committed := false
			func() {
				defer func() {
					if !committed {
						tx.Rollback(ctx)
					}
				}()

				var (
					existingTypeCode                  interface{}
					existingTypeName                  interface{}
					existingDirection                 interface{}
					existingBusinessUnitDivision      interface{}
					existingDefaultCurrency           interface{}
					existingDefaultDueDays            interface{}
					existingPaymentTermsName          interface{}
					existingAllowNetting              interface{}
					existingSettlementDiscount        interface{}
					existingSettlementDiscountPercent interface{}
					existingTaxApplicable             interface{}
					existingTaxCode                   interface{}
					existingDefaultReconGL            interface{}
					existingOffsetRevenueExpenseGL    interface{}
					existingCashFlowCategory          interface{}
					existingCategory                  interface{}
					existingEffectiveFrom             interface{}
					existingEffectiveTo               interface{}
					existingTags                      interface{}
					existingErpType                   interface{}
					existingSapCompanyCode            interface{}
					existingSapFiDocType              interface{}
					existingSapPostingKeyDebit        interface{}
					existingSapPostingKeyCredit       interface{}
					existingSapReconciliationGL       interface{}
					existingSapTaxCode                interface{}
					existingOracleLedger              interface{}
					existingOracleTransactionType     interface{}
					existingOracleDistributionSet     interface{}
					existingOracleSource              interface{}
					existingTallyVoucherType          interface{}
					existingTallyTaxClass             interface{}
					existingTallyLedgerGroup          interface{}
					existingSageNominalControl        interface{}
					existingSageAnalysisCode          interface{}
					existingExternalCode              interface{}
					existingSegment                   interface{}
					existingStatus                    interface{}
				)

				sel := `SELECT type_code, type_name, direction, business_unit_division, default_currency, default_due_days, payment_terms_name,
								allow_netting, settlement_discount, settlement_discount_percent, tax_applicable, tax_code,
								default_recon_gl, offset_revenue_expense_gl, cash_flow_category, category, effective_from, effective_to, tags,
						erp_type, sap_company_code, sap_fi_doc_type, sap_posting_key_debit, sap_posting_key_credit, sap_reconciliation_gl,
						sap_tax_code, oracle_ledger, oracle_transaction_type, oracle_distribution_set, oracle_source,
						tally_voucher_type, tally_tax_class, tally_ledger_group, sage_nominal_control, sage_analysis_code, external_code, segment, status
					FROM masterpayablereceivabletype WHERE type_id=$1 FOR UPDATE`
				if err := tx.QueryRow(ctx, sel, row.TypeID).Scan(
					&existingTypeCode, &existingTypeName, &existingDirection, &existingBusinessUnitDivision, &existingDefaultCurrency, &existingDefaultDueDays, &existingPaymentTermsName,
					&existingAllowNetting, &existingSettlementDiscount, &existingSettlementDiscountPercent, &existingTaxApplicable, &existingTaxCode,
					&existingDefaultReconGL, &existingOffsetRevenueExpenseGL, &existingCashFlowCategory, &existingCategory, &existingEffectiveFrom, &existingEffectiveTo, &existingTags,
					&existingErpType, &existingSapCompanyCode, &existingSapFiDocType, &existingSapPostingKeyDebit, &existingSapPostingKeyCredit, &existingSapReconciliationGL,
					&existingSapTaxCode, &existingOracleLedger, &existingOracleTransactionType, &existingOracleDistributionSet, &existingOracleSource,
					&existingTallyVoucherType, &existingTallyTaxClass, &existingTallyLedgerGroup, &existingSageNominalControl, &existingSageAnalysisCode, &existingExternalCode, &existingSegment, &existingStatus,
				); err != nil {
					results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "fetch failed: " + err.Error(), "type_id": row.TypeID})
					return
				}

				var sets []string
				var args []interface{}
				pos := 1

				// map of existing values keyed by the incoming field name
				existingMap := map[string]interface{}{
					"type_code":                   existingTypeCode,
					"type_name":                   existingTypeName,
					"direction":                   existingDirection,
					"business_unit_division":      existingBusinessUnitDivision,
					"default_currency":            existingDefaultCurrency,
					"default_due_days":            existingDefaultDueDays,
					"payment_terms_name":          existingPaymentTermsName,
					"allow_netting":               existingAllowNetting,
					"settlement_discount":         existingSettlementDiscount,
					"settlement_discount_percent": existingSettlementDiscountPercent,
					"tax_applicable":              existingTaxApplicable,
					"tax_code":                    existingTaxCode,
					"default_recon_gl":            existingDefaultReconGL,
					"offset_revenue_expense_gl":   existingOffsetRevenueExpenseGL,
					"cash_flow_category":          existingCashFlowCategory,
					"category":                    existingCategory,
					"effective_from":              existingEffectiveFrom,
					"effective_to":                existingEffectiveTo,
					"tags":                        existingTags,
					"erp_type":                    existingErpType,
					"sap_company_code":            existingSapCompanyCode,
					"sap_fi_doc_type":             existingSapFiDocType,
					"sap_posting_key_debit":       existingSapPostingKeyDebit,
					"sap_posting_key_credit":      existingSapPostingKeyCredit,
					"sap_reconciliation_gl":       existingSapReconciliationGL,
					"sap_tax_code":                existingSapTaxCode,
					"oracle_ledger":               existingOracleLedger,
					"oracle_transaction_type":     existingOracleTransactionType,
					"oracle_distribution_set":     existingOracleDistributionSet,
					"oracle_source":               existingOracleSource,
					"tally_voucher_type":          existingTallyVoucherType,
					"tally_tax_class":             existingTallyTaxClass,
					"tally_ledger_group":          existingTallyLedgerGroup,
					"sage_nominal_control":        existingSageNominalControl,
					"sage_analysis_code":          existingSageAnalysisCode,
					"external_code":               existingExternalCode,
					"segment":                     existingSegment,
				}

				// Validate entity, currency, category if being updated
				if val, ok := row.Fields["business_unit_division"]; ok {
					if valStr := fmt.Sprint(val); !api.IsEntityAllowed(ctx, valStr) {
						results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "invalid or unauthorized business_unit_division: " + valStr, "type_id": row.TypeID})
						return
					}
				}
				if val, ok := row.Fields["default_currency"]; ok {
					if valStr := fmt.Sprint(val); !api.IsCurrencyAllowed(ctx, valStr) {
						results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "invalid or unauthorized default_currency: " + valStr, "type_id": row.TypeID})
						return
					}
				}
				if val, ok := row.Fields["cash_flow_category"]; ok {
					if valStr := fmt.Sprint(val); valStr != "" && !api.IsCashFlowCategoryAllowed(ctx, valStr) {
						results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "invalid or unauthorized cash_flow_category: " + valStr, "type_id": row.TypeID})
						return
					}
				}

				for k, v := range row.Fields {
					if oldVal, ok := existingMap[k]; ok {
						// generic two-column update: <col>=$n, old_<col>=$n+1
						sets = append(sets, fmt.Sprintf("%s=$%d, old_%s=$%d", k, pos, k, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(oldVal))
						pos += 2
						continue
					}

					switch k {
					case constants.KeyStatus:
						sets = append(sets, fmt.Sprintf("status=$%d, old_status=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingStatus))
						pos += 2
					default:
						// unknown or intentionally ignored field
					}
				}

				updatedID := row.TypeID
				if len(sets) > 0 {
					q := "UPDATE masterpayablereceivabletype SET " + strings.Join(sets, ", ") + fmt.Sprintf(" WHERE type_id=$%d RETURNING type_id", pos)
					args = append(args, row.TypeID)
					if err := tx.QueryRow(ctx, q, args...).Scan(&updatedID); err != nil {
						results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: constants.ErrUpdateFailed + err.Error(), "type_id": row.TypeID})
						return
					}
				}

				// audit
				auditQ := `INSERT INTO auditactionpayablereceivable (type_id, actiontype, processing_status, reason, requested_by, requested_at) VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL', $2, $3, now())`
				if _, err := tx.Exec(ctx, auditQ, updatedID, row.Reason, updatedBy); err != nil {
					results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "audit failed: " + err.Error(), "type_id": updatedID})
					return
				}

				if err := tx.Commit(ctx); err != nil {
					results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: constants.ErrCommitFailed + err.Error(), "type_id": updatedID})
					return
				}
				committed = true
				results = append(results, map[string]interface{}{constants.ValueSuccess: true, "type_id": updatedID})
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

func DeletePayableReceivable(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID  string   `json:"user_id"`
			TypeIDs []string `json:"type_ids"`
			Reason  string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSON)
			return
		}

		session := middlewares.GetSessionFromContext(r.Context())
		if session == nil {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}
		requestedBy := session.Name

		if len(req.TypeIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "type_ids required")
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			errMsg, statusCode := getUserFriendlyPayableReceivableError(err, constants.ErrTxStartFailed)
			if statusCode == http.StatusOK {
				w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
				json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "error": errMsg})
			} else {
				api.RespondWithError(w, statusCode, errMsg)
			}
			return
		}
		committed := false
		defer func() {
			if !committed {
				tx.Rollback(ctx)
			}
		}()

		q := `INSERT INTO auditactionpayablereceivable (type_id, actiontype, processing_status, reason, requested_by, requested_at) VALUES ($1,'DELETE','PENDING_DELETE_APPROVAL',$2,$3,now())`
		for _, tid := range req.TypeIDs {
			if strings.TrimSpace(tid) == "" {
				tx.Rollback(ctx)
				api.RespondWithError(w, http.StatusBadRequest, "empty type_id provided")
				return
			}
			if _, err := tx.Exec(ctx, q, tid, req.Reason, requestedBy); err != nil {
				tx.Rollback(ctx)
				api.RespondWithError(w, http.StatusInternalServerError, err.Error())
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			tx.Rollback(ctx)
			errMsg, statusCode := getUserFriendlyPayableReceivableError(err, "Failed to save deletion request")
			if statusCode == http.StatusOK {
				w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
				json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "error": errMsg})
			} else {
				api.RespondWithError(w, statusCode, errMsg)
			}
			return
		}
		committed = true

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: true})
	}
}

func BulkRejectPayableReceivableActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID  string   `json:"user_id"`
			TypeIDs []string `json:"type_ids"`
			Comment string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}

		session := middlewares.GetSessionFromContext(r.Context())
		if session == nil {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}
		checkerBy := session.Name

		ctx := context.Background()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTransactionFailed+err.Error())
			return
		}
		committed := false
		defer func() {
			if !committed {
				tx.Rollback(ctx)
			}
		}()

		sel := `SELECT DISTINCT ON (type_id) action_id, type_id, processing_status FROM auditactionpayablereceivable WHERE type_id = ANY($1) ORDER BY type_id, requested_at DESC`
		rows, err := tx.Query(ctx, sel, req.TypeIDs)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to fetch audit rows: "+err.Error())
			return
		}
		defer rows.Close()

		actionIDs := make([]string, 0)
		foundTypes := map[string]bool{}
		var cannotReject []string
		for rows.Next() {
			var aid, tid, pstatus string
			if err := rows.Scan(&aid, &tid, &pstatus); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "failed to scan audit rows: "+err.Error())
				return
			}
			foundTypes[tid] = true
			if strings.ToUpper(strings.TrimSpace(pstatus)) == "APPROVED" {
				cannotReject = append(cannotReject, tid)
			}
			actionIDs = append(actionIDs, aid)
		}
		missing := []string{}
		for _, tid := range req.TypeIDs {
			if !foundTypes[tid] {
				missing = append(missing, tid)
			}
		}
		if len(missing) > 0 || len(cannotReject) > 0 {
			msg := ""
			if len(missing) > 0 {
				msg += fmt.Sprintf("no audit action found for type_ids: %v. ", missing)
			}
			if len(cannotReject) > 0 {
				msg += fmt.Sprintf("cannot reject already approved type_ids: %v", cannotReject)
			}
			api.RespondWithError(w, http.StatusBadRequest, msg)
			return
		}

		upd := `UPDATE auditactionpayablereceivable SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2 WHERE action_id = ANY($3) RETURNING action_id, type_id`
		urows, err := tx.Query(ctx, upd, checkerBy, req.Comment, actionIDs)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to update audit rows: "+err.Error())
			return
		}
		defer urows.Close()
		updated := []map[string]interface{}{}
		for urows.Next() {
			var aid, tid string
			if err := urows.Scan(&aid, &tid); err == nil {
				updated = append(updated, map[string]interface{}{"action_id": aid, "type_id": tid})
			}
		}

		if len(updated) != len(actionIDs) {
			api.RespondWithError(w, http.StatusInternalServerError, fmt.Sprintf("updated %d of %d actions, aborting", len(updated), len(actionIDs)))
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailed+err.Error())
			return
		}
		committed = true

		json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: true, "updated": updated})
	}
}

func BulkApprovePayableReceivableActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID  string   `json:"user_id"`
			TypeIDs []string `json:"type_ids"`
			Comment string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}
		session := middlewares.GetSessionFromContext(r.Context())
		if session == nil {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}
		checkerBy := session.Name

		ctx := context.Background()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTransactionFailed+err.Error())
			return
		}
		committed := false
		defer func() {
			if !committed {
				tx.Rollback(ctx)
			}
		}()
		sel := `SELECT DISTINCT ON (type_id) action_id, type_id, actiontype, processing_status FROM auditactionpayablereceivable WHERE type_id = ANY($1) ORDER BY type_id, requested_at DESC`
		rows, err := tx.Query(ctx, sel, req.TypeIDs)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to fetch audit rows: "+err.Error())
			return
		}
		defer rows.Close()

		actionIDs := make([]string, 0)
		foundTypes := map[string]bool{}
		cannotApprove := []string{}
		actionTypeByType := map[string]string{}
		for rows.Next() {
			var aid, tid, atype, pstatus string
			if err := rows.Scan(&aid, &tid, &atype, &pstatus); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "failed to scan audit rows: "+err.Error())
				return
			}
			foundTypes[tid] = true
			actionIDs = append(actionIDs, aid)
			actionTypeByType[tid] = atype
			if strings.ToUpper(strings.TrimSpace(pstatus)) == "APPROVED" {
				cannotApprove = append(cannotApprove, tid)
			}
		}

		missing := []string{}
		for _, tid := range req.TypeIDs {
			if !foundTypes[tid] {
				missing = append(missing, tid)
			}
		}
		if len(missing) > 0 || len(cannotApprove) > 0 {
			msg := ""
			if len(missing) > 0 {
				msg += fmt.Sprintf("no audit action found for type_ids: %v. ", missing)
			}
			if len(cannotApprove) > 0 {
				msg += fmt.Sprintf("cannot approve already approved type_ids: %v", cannotApprove)
			}
			api.RespondWithError(w, http.StatusBadRequest, msg)
			return
		}

		upd := `UPDATE auditactionpayablereceivable SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2 WHERE action_id = ANY($3) RETURNING action_id, type_id, actiontype`
		urows, err := tx.Query(ctx, upd, checkerBy, req.Comment, actionIDs)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to update audit rows: "+err.Error())
			return
		}
		defer urows.Close()
		updated := []map[string]interface{}{}
		deleteTypeIDs := make([]string, 0)
		for urows.Next() {
			var aid, tid, atype string
			if err := urows.Scan(&aid, &tid, &atype); err == nil {
				updated = append(updated, map[string]interface{}{"action_id": aid, "type_id": tid, "action_type": atype})
				if strings.ToUpper(strings.TrimSpace(atype)) == "DELETE" {
					deleteTypeIDs = append(deleteTypeIDs, tid)
				}
			}
		}

		if len(updated) != len(actionIDs) {
			api.RespondWithError(w, http.StatusInternalServerError, fmt.Sprintf("updated %d of %d actions, aborting", len(updated), len(actionIDs)))
			return
		}

		if len(deleteTypeIDs) > 0 {
			softDelQ := `UPDATE masterpayablereceivabletype SET is_deleted = true WHERE type_id = ANY($1)`
			if _, err := tx.Exec(ctx, softDelQ, deleteTypeIDs); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "failed to soft-delete master rows: "+err.Error())
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailed+err.Error())
			return
		}
		committed = true

		json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: true, "updated": updated})
	}
}
func UploadPayableReceivable(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
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

		session := middlewares.GetSessionFromContext(r.Context())
		if session == nil {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}
		userName := session.Name

		entities := api.GetEntityNamesFromCtx(ctx)
		currCodes := api.GetCurrencyCodesFromCtx(ctx)
		categories := api.GetCashFlowCategoryNamesFromCtx(ctx)

		if len(entities) == 0 {
			api.RespondWithError(w, http.StatusForbidden, constants.ErrNoAccessibleEntitiesForRequest)
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

			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				errMsg, statusCode := getUserFriendlyPayableReceivableError(err, constants.ErrTxStartFailed)
				if statusCode == http.StatusOK {
					w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
					json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "error": errMsg})
				} else {
					api.RespondWithError(w, statusCode, errMsg)
				}
				return
			}
			committed := false
			defer func() {
				if !committed {
					tx.Rollback(ctx)
				}
			}()
			// read mapping BEFORE staging so we can stage only mapped columns
			mapRows, err := tx.Query(ctx, `SELECT source_column_name, target_field_name FROM upload_mapping_payablereceivable`)
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
					tt := strings.TrimSpace(tgt)
					tt = strings.Trim(tt, ", \"'`")
					tt = strings.ReplaceAll(tt, " ", "_")
					mapping[key] = tt
				}
			}
			mapRows.Close()

			// Build list of mapped target columns and source indices; skip unmapped columns
			var mappedTargets []string
			var srcIndices []int
			for i := range headerRow {
				key := headerNorm[i]
				if t, ok := mapping[key]; ok {
					mappedTargets = append(mappedTargets, t)
					srcIndices = append(srcIndices, i)
				} else {
					// skip extra column
					continue
				}
			}
			if len(mappedTargets) == 0 {
				tx.Rollback(ctx)
				api.RespondWithError(w, http.StatusBadRequest, "No mapped columns found in uploaded file")
				return
			}

			// Build reduced copyRows which only includes selected (mapped) columns
			reducedRows := make([][]interface{}, len(copyRows))
			dateCols := map[string]bool{"effective_from": true, "effective_to": true}
			for rIdx, orig := range copyRows {
				vals := make([]interface{}, len(mappedTargets)+1)
				vals[0] = orig[0] // batch id
				for k, srcIdx := range srcIndices {
					// original columns are offset by 1
					if srcIdx+1 < len(orig) {
						v := orig[srcIdx+1]
						// Normalize date-like fields
						tgt := mappedTargets[k]
						if v != nil {
							if s, ok := v.(string); ok && dateCols[tgt] {
								norm := exposures.NormalizeDate(s)
								if norm == "" {
									vals[k+1] = nil
								} else {
									vals[k+1] = norm
								}
								continue
							}
						}
						vals[k+1] = v
					} else {
						vals[k+1] = nil
					}
				}
				reducedRows[rIdx] = vals
			}

			columns := append([]string{"upload_batch_id"}, mappedTargets...)

			// stage using reduced rows
			_, err = tx.CopyFrom(ctx, pgx.Identifier{"input_payablereceivable_table"}, columns, pgx.CopyFromRows(reducedRows))
			if err != nil {
				tx.Rollback(ctx)
				api.RespondWithError(w, http.StatusInternalServerError, "Failed to stage data: "+err.Error())
				return
			}

			// Validate staged data for entity, currency, category authorization
			hasBusinessUnit := false
			hasCurrency := false
			hasCategory := false
			for _, col := range mappedTargets {
				if col == "business_unit_division" {
					hasBusinessUnit = true
				}
				if col == "default_currency" {
					hasCurrency = true
				}
				if col == "cash_flow_category" {
					hasCategory = true
				}
			}

			if hasBusinessUnit {
				var invalidEntity string
				checkEntityQ := `SELECT business_unit_division FROM input_payablereceivable_table WHERE upload_batch_id = $1 AND business_unit_division IS NOT NULL AND business_unit_division != '' AND NOT (UPPER(TRIM(business_unit_division)) = ANY($2)) LIMIT 1`
				entitiesUpper := make([]string, len(entities))
				for i, e := range entities {
					entitiesUpper[i] = strings.ToUpper(strings.TrimSpace(e))
				}
				if err := tx.QueryRow(ctx, checkEntityQ, batchID, entitiesUpper).Scan(&invalidEntity); err == nil {
					tx.Rollback(ctx)
					api.RespondWithError(w, http.StatusForbidden, "Invalid or unauthorized business_unit_division in upload: "+invalidEntity)
					return
				}
			}

			if hasCurrency {
				var invalidCurr string
				checkCurrQ := `SELECT default_currency FROM input_payablereceivable_table WHERE upload_batch_id = $1 AND default_currency IS NOT NULL AND default_currency != '' AND NOT (UPPER(TRIM(default_currency)) = ANY($2)) LIMIT 1`
				currCodesUpper := make([]string, len(currCodes))
				for i, c := range currCodes {
					currCodesUpper[i] = strings.ToUpper(strings.TrimSpace(c))
				}
				if err := tx.QueryRow(ctx, checkCurrQ, batchID, currCodesUpper).Scan(&invalidCurr); err == nil {
					tx.Rollback(ctx)
					api.RespondWithError(w, http.StatusForbidden, "Invalid or unauthorized default_currency in upload: "+invalidCurr)
					return
				}
			}

			if hasCategory {
				var invalidCat string
				checkCatQ := `SELECT cash_flow_category FROM input_payablereceivable_table WHERE upload_batch_id = $1 AND cash_flow_category IS NOT NULL AND cash_flow_category != '' AND NOT (UPPER(TRIM(cash_flow_category)) = ANY($2)) LIMIT 1`
				categoriesUpper := make([]string, len(categories))
				for i, c := range categories {
					categoriesUpper[i] = strings.ToUpper(strings.TrimSpace(c))
				}
				if err := tx.QueryRow(ctx, checkCatQ, batchID, categoriesUpper).Scan(&invalidCat); err == nil {
					tx.Rollback(ctx)
					api.RespondWithError(w, http.StatusForbidden, "Invalid or unauthorized cash_flow_category in upload: "+invalidCat)
					return
				}
			}

			// Build insert/select expressions using mapped target columns
			tgtCols := mappedTargets

			// Ensure 'status' is provided (master requires NOT NULL). Use COALESCE to default to 'Active'.
			hasStatus := false
			for _, c := range tgtCols {
				if c == constants.KeyStatus {
					hasStatus = true
					break
				}
			}

			var finalCols []string
			var selectExprs []string
			if !hasStatus {
				// prepend status column with default
				finalCols = append([]string{constants.KeyStatus}, tgtCols...)
				selectExprs = append(selectExprs, "COALESCE(s.status, 'Active') AS status")
				for _, col := range tgtCols {
					selectExprs = append(selectExprs, fmt.Sprintf("s.%s AS %s", col, col))
				}
			} else {
				finalCols = tgtCols
				for _, col := range tgtCols {
					if col == constants.KeyStatus {
						selectExprs = append(selectExprs, "COALESCE(s.status, 'Active') AS status")
					} else {
						selectExprs = append(selectExprs, fmt.Sprintf("s.%s AS %s", col, col))
					}
				}
			}

			tgtColsStr := strings.Join(finalCols, ", ")
			srcColsStr := strings.Join(selectExprs, ", ")

			insertSQL := fmt.Sprintf(`
                INSERT INTO masterpayablereceivabletype (%s)
                SELECT %s
                FROM input_payablereceivable_table s
                WHERE s.upload_batch_id = $1
                RETURNING type_id
            `, tgtColsStr, srcColsStr)
			rows, err := tx.Query(ctx, insertSQL, batchID)
			if err != nil {
				tx.Rollback(ctx)
				api.RespondWithError(w, http.StatusInternalServerError, "Final insert error: "+err.Error())
				return
			}
			var newIDs []string
			for rows.Next() {
				var id string
				if err := rows.Scan(&id); err == nil {
					newIDs = append(newIDs, id)
				} else {
					rows.Close()
					tx.Rollback(ctx)
					api.RespondWithError(w, http.StatusInternalServerError, "Failed reading inserted ids: "+err.Error())
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

			if len(newIDs) > 0 {
				auditSQL := `INSERT INTO auditactionpayablereceivable (type_id, actiontype, processing_status, reason, requested_by, requested_at) SELECT type_id, 'CREATE', 'PENDING_APPROVAL', NULL, $1, now() FROM masterpayablereceivabletype WHERE type_id = ANY($2)`
				if _, err := tx.Exec(ctx, auditSQL, userName, newIDs); err != nil {
					tx.Rollback(ctx)
					api.RespondWithError(w, http.StatusInternalServerError, "Failed to insert audit actions: "+err.Error())
					return
				}
			}

			if err := tx.Commit(ctx); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailedCapitalized+err.Error())
				return
			}
			committed = true
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: true, "batch_ids": batchIDs})
	}
}
